//! Counted work with a bound key: how fast a statement is, said as the number of stored rows it
//! reads. A `$name` is its own arm of the index choice, which a script, run with no parameters,
//! cannot reach; the cases a script can state are in `tests/sql/counted_work.sql`.

mod support;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use contextdb::{Database, DatabaseOpenOptions, QueryResult, ReadLimits, ReadSession, params};

#[test]
fn a_bound_key_a_page_and_a_write_read_only_their_rows() {
    let folder = support::folder("paging-by-key");
    let db = support::open(&folder);
    support::declare(&db);
    let tx = db.transaction().expect("begin");
    support::insert(&tx, 1..=1000);
    tx.commit().expect("commit");
    for id in [1_i64, 500, 1000] {
        let found = db
            .execute(
                "SELECT id, name, tag FROM t WHERE id = $id",
                &params! { "id" => id },
            )
            .expect("look a row up by its key");
        assert_eq!(found.rows(), [support::row(id)]);
        assert_eq!(found.rows_examined(), 1, "the one row of key {id}");
    }

    // Through a reader with the default limits, beside the writer, at 1,000 rows and at 5,000.
    let page = "SELECT id, name, tag FROM t WHERE id > $last ORDER BY id LIMIT 100";
    for stored in [1000_i64, 5000] {
        if stored > 1000 {
            support::insert_together(&db, 1001..=stored);
        }
        let reader = ReadSession::open(&folder, ReadLimits::default()).expect("open for reading");
        // Every page costs a page of rows, however many pages came before it.
        for last in (0..stored).step_by(stored as usize / 10) {
            let rows = reader.execute(page, &params! { "last" => last }).unwrap();
            let wanted: Vec<_> = (last + 1..=last + 100).map(support::row).collect();
            let at = format!("the page after key {last} of {stored}");
            support::assert_same_rows(rows.rows(), &wanted, &at);
            assert_eq!(rows.rows_examined(), 100, "{at}");
        }
        // After the last row there is nothing, and finding that out reads nothing.
        let end = reader.execute(page, &params! { "last" => stored }).unwrap();
        assert!(end.rows().is_empty());
        assert_eq!(end.rows_examined(), 0);
    }
    // One stream among others: the ten before a place, whatever kinds lie beside it.
    support::run(
        &db,
        "CREATE TABLE ev (id INTEGER PRIMARY KEY, kind TEXT, seq INTEGER)",
    );
    support::run(&db, "CREATE INDEX ev_kind_seq ON ev (kind, seq)");
    let all: Vec<_> = (1..=300)
        .map(|n| format!("({n}, '{}', {n})", ["a", "b", "c"][n % 3]))
        .collect();
    support::run(&db, &format!("INSERT INTO ev VALUES {}", all.join(",")));
    for last in [300_i64, 150, 60] {
        let ten = "SELECT id FROM ev WHERE kind = 'a' AND seq < $last ORDER BY seq DESC LIMIT 10";
        let rows = db.execute(ten, &params! { "last" => last }).unwrap();
        assert_eq!(rows.rows_examined(), 10, "ten before {last}");
    }

    // A write is counted as a read is: an UPDATE by key reads its one row, a DELETE through the
    // index on `tag` the 500 rows of tag 3 (row n has tag n % 10), never the table.
    let updated = support::run(&db, "UPDATE t SET name = 'renamed' WHERE id = 500");
    assert_eq!(updated.rows_affected(), 1);
    assert_eq!(
        updated.rows_examined(),
        1,
        "the one row an UPDATE by key reads"
    );
    let deleted = support::run(&db, "DELETE FROM t WHERE tag = 3");
    assert_eq!(deleted.rows_affected(), 500);
    assert_eq!(
        deleted.rows_examined(),
        500,
        "the rows a DELETE reads through its index"
    );

    // The page is the first rows of the whole answer: forward, backward, over an IN list, and
    // over two arms that overlap.
    for (filter, order) in [
        ("tag > 3 OR (tag = 3 AND id > 0)", "tag, id"),
        ("tag > 3 OR (tag = 3 AND id > 0)", "tag DESC, id DESC"),
        ("tag IN (4, 5, 6)", "tag, id"),
        ("tag > 3 OR tag > 5", "tag, id"),
    ] {
        let sql = |tail: &str| format!("SELECT id FROM t WHERE {filter} ORDER BY {order}{tail}");
        let whole = support::rows(&db, &sql(""));
        assert_eq!(
            support::rows(&db, &sql(" LIMIT 100")),
            whole[..100],
            "{filter} by {order}"
        );
    }

    // A page ordered by a non-unique column and the key reads a page as well.
    let by_tag =
        "SELECT id FROM t WHERE tag > 3 OR (tag = 3 AND id > 0) ORDER BY tag, id LIMIT 100";
    assert_eq!(support::run(&db, by_tag).rows_examined(), 100, "tag page");
    support::run(&db, "CREATE TABLE s (id TEXT PRIMARY KEY, tag INTEGER)");
    support::run(&db, "CREATE INDEX s_tag ON s (tag)");
    let rows: Vec<_> = (0..300).map(|n| format!("('k{n:03}', 1)")).collect();
    support::run(&db, &format!("INSERT INTO s VALUES {}", rows.join(",")));
    let next = "SELECT id FROM s WHERE tag = 1 AND id > 'k150' ORDER BY id LIMIT 10";
    assert_eq!(support::run(&db, next).rows_examined(), 10, "text-key page");
}

/// A 512-row append, from a cold cache, after `few` stored rows and after `many`: the bytes of
/// the file it reads grow by less than a pass over the rows stored between the two reads. No test
/// owns the cost of a commit as a table grows, so this is a test of its own.
fn an_append_after(few: i64, many: i64) {
    let reads_of_an_append = |stored: i64| -> (u64, u64, usize) {
        let folder = support::folder(&format!("append-{few}-{many}-after-{stored}"));
        let db = support::open(&folder);
        support::declare(&db);
        support::insert_together(&db, 1..=stored);
        support::close(db);
        let (storage, bytes) = support::counting_bytes_read();
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        let db = support::open_with(&folder, options).expect("open with a counting door");
        let found = support::run(&db, "SELECT id FROM t WHERE id = 7");
        assert_eq!(found.rows_examined(), 1, "a lookup after {stored} rows");
        let opened = bytes.load(Ordering::SeqCst);
        let reader = ReadSession::open(&folder, ReadLimits::default()).expect("open for reading");
        let used = support::memory_used(&reader);
        support::insert_together(&db, stored + 1..=stored + 512);
        (opened, bytes.load(Ordering::SeqCst) - opened, used)
    };
    // The appended keys sit above every stored key, so each check of the append walks a tree
    // from its root to one leaf, and from `few` rows to ten times as many each walk grows by a
    // level at most. A pass over the stored rows reads at least the 8-byte key of each row added
    // between the two.
    let ((open_few, at_few, used_few), (open_many, at_many, used_many)) =
        (reads_of_an_append(few), reads_of_an_append(many));
    // The open and a lookup read no more as the store grows; a reader's memory in use after its
    // open is the same at both sizes.
    assert!(open_many < open_few + (many - few) as u64 * 8, "open grows");
    assert_eq!(used_few, used_many, "reader memory after open");
    assert!(
        at_many.saturating_sub(at_few) < (many - few) as u64 * 8,
        "a 512-row append read {at_few} bytes of the file after {few} rows and {at_many} after \
         {many}: its commit reads more as the table grows, as a pass over the stored rows does"
    );
}

#[test]
fn an_append_does_not_read_the_rows_stored_before_it() {
    an_append_after(5_000, 50_000);
}

/// Left to the nightly and release runs by `v2/.config/nextest.toml`.
#[test]
fn an_append_does_not_read_the_rows_stored_before_it_at_a_million_rows() {
    an_append_after(100_000, 1_000_000);
}

#[test]
fn statement_work_totals_include_hidden_reads_and_vector_distances() {
    let db = support::open(&support::folder("statement-work-totals"));
    support::run(
        &db,
        "CREATE TABLE vm (id INTEGER PRIMARY KEY, reach TEXT SCOPE_LABEL_READ ('public', 'internal') WRITE ('public', 'internal'), v VECTOR(3))",
    );
    support::run(
        &db,
        "INSERT INTO vm VALUES (1, 'public', [1.0, 0.5, 0.0]), (2, 'public', [1.0, 0.0, 0.5]), (3, 'public', [0.0, 1.0, 0.0]), (100, 'internal', [1.0, 0.2, 0.0])",
    );
    let on = support::with_labels(&db, &["public"]);
    let mut counted = Vec::new();
    for sql in [
        "SELECT id FROM vm",
        "SELECT id FROM vm ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 2",
        "UPDATE vm SET reach = 'public' WHERE id = (SELECT id FROM vm ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 1)",
    ] {
        let before = db.work_done().unwrap();
        let answer = support::run(&on, sql);
        let after = db.work_done().unwrap();
        counted.push((
            after.rows_read - before.rows_read,
            after.vectors_compared - before.vectors_compared,
            answer.rows_examined(),
            answer.distance_evaluations(),
        ));
    }
    assert_eq!(counted[..2], [(4, 0, 3, 0), (6, 3, 5, 3)]);
    let write = (counted[2].1, counted[2].3);
    assert_eq!(write, (3, 3), "a write counts its subquery's vectors");
}

// A cold 8 MiB cache at both sizes; raising the statement budget leaves that cache unchanged.
fn read_fixture(
    name: &str,
    rows: i64,
    setup: &str,
    put: impl Fn(&contextdb::Transaction<'_>, i64),
) -> (
    contextdb::Database,
    Arc<AtomicU64>,
    std::path::PathBuf,
    DatabaseOpenOptions,
) {
    let folder = support::folder(&format!("work-{name}-{rows}"));
    let clock = Arc::new(AtomicU64::new(100));
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(32 << 20);
    options.clock = Some(Arc::new(support::At(clock.clone())));
    let db = support::open_with(&folder, options.clone()).unwrap();
    for sql in setup.split(';').filter(|sql| !sql.is_empty()) {
        support::run(&db, sql);
    }
    for first in (1..=rows).step_by(1024) {
        let tx = db.transaction().unwrap();
        for id in first..(first + 1024).min(rows + 1) {
            put(&tx, id);
        }
        tx.commit().unwrap();
    }
    support::close(db);
    let (storage, bytes) = support::counting_bytes_read();
    options.storage = Some(storage);
    // A session's limit is set at open and is that session's limit "and no more" (README).
    options.memory_limit = Some(1 << 30);
    let db = support::open_with(&folder, options.clone()).unwrap();
    (db, bytes, folder, options)
}

#[test]
fn an_indexed_transaction_update_reads_in_proportion_to_its_rows() {
    // The larger store is more than twice the 8 MiB cache, so an UPDATE that went back over the
    // index entries before its place would read them from the file again.
    let reads = [4_096, 40_960].map(|rows| {
        let (db, bytes, folder, options) = read_fixture("index-place", rows,
            "CREATE TABLE resume(id INTEGER PRIMARY KEY, label TEXT NOT NULL, flag INTEGER NOT NULL);CREATE INDEX resume_label ON resume(label)",
            |tx,id| {tx.execute("INSERT INTO resume VALUES($id,$label,0)", &params!{"id"=>id,"label"=>format!("{id:016}{}",support::letters(id,221))}).unwrap();});
        let sql = "UPDATE resume SET flag = 1 WHERE label >= '0'";
        let plan = db.explain(sql).unwrap();
        assert!(plan.contains("resume_label"), "{plan}", plan = &*plan);
        let tx = db.transaction().unwrap();
        bytes.store(0, Ordering::SeqCst);
        let changed = support::run(&tx, sql);
        let read = bytes.load(Ordering::SeqCst);
        assert_eq!(changed.rows_affected(), rows as u64);
        assert_eq!(support::int(&tx,"SELECT COUNT(*) FROM resume WHERE flag = 1"),rows);
        drop(tx);
        assert_eq!(support::int(&db,"SELECT COUNT(*) FROM resume WHERE flag = 1"),0);
        support::close(db);
        let db = support::open_with(&folder, options).unwrap();
        bytes.store(0, Ordering::SeqCst);
        let purged = db.execute("PURGE FROM resume WHERE label IN (SELECT label FROM resume WHERE id <= $last)", &params!{"last"=>256 * (1 + i64::from(rows > 4_096))}).unwrap();
        let purge_read=bytes.load(Ordering::SeqCst);
        assert_eq!(purged.rows_affected(), 256 * (1 + u64::from(rows > 4_096)));
        support::close(db);
        [read,purge_read]
    });
    for (operation, factor) in [12, 3].into_iter().enumerate() {
        assert!(
            reads[0][operation] > 0 && reads[1][operation] <= factor * reads[0][operation],
            "UPDATE/PURGE bytes at 4,096/40,960 rows: {reads:?}"
        );
    }
}

#[test]
fn equal_index_values_resume_through_writes_and_a_graph_walk() {
    let (db, _, _, _) = read_fixture(
        "equal-entries",
        2050,
        "CREATE TABLE edges(id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, n INTEGER) DAG('D')",
        |tx, id| {
            tx.execute(
                "INSERT INTO edges VALUES($id,$from,$to,'D',0)",
                &params! {"id"=>id,"from"=>support::context(1),"to"=>support::context(2)},
            )
            .unwrap();
        },
    );
    let pair = |r: contextdb::QueryResult| [r.rows_affected(), r.rows_examined()];
    let tx = db.transaction().unwrap();
    let values = |from| params! {"from"=>support::context(from),"to"=>support::context(1)};
    let updated = tx
        .execute(
            "UPDATE edges SET n=n+1 WHERE source_id=$from AND n=0",
            &values(1),
        )
        .unwrap();
    assert_eq!(pair(updated), [2050; 2]);
    assert_eq!(support::int(&tx, "SELECT SUM(n) FROM edges"), 2050);
    let insert = "INSERT INTO edges VALUES(2051,$from,$to,'D',0)";
    let edge = tx.execute(insert, &values(9000)).unwrap();
    assert_eq!(pair(edge), [1, 2050]);
    let removed = tx
        .execute("DELETE FROM edges WHERE source_id=$from", &values(1))
        .unwrap();
    assert_eq!(pair(removed), [2050; 2]);
    assert_eq!(support::int(&tx, "SELECT COUNT(*) FROM edges"), 1);
    drop(tx);
    support::close(db);
}

fn membership_rows(db: &contextdb::Database, into: &str, suffix: &str, count: i64) {
    for first in (0..count).step_by(1000) {
        let rows = (first..first + 1000)
            .map(|n| format!("({n}{suffix})"))
            .collect::<Vec<_>>()
            .join(",");
        support::run(db, &format!("INSERT INTO {into} VALUES {rows}"));
    }
}

#[test]
fn removal_membership_agrees_with_the_in_memory_evaluator_for_every_kind() {
    use contextdb::{TxId, Uuid, Value};
    use std::mem::discriminant;
    let db = support::open(&support::folder("removal-membership-kinds"));
    let run = |sql: &str| support::run(&db, sql);
    run("CREATE TABLE choices(id INTEGER PRIMARY KEY, missing INTEGER)");
    run(
        "CREATE TABLE victim(id INTEGER PRIMARY KEY, missing INTEGER, b BOOLEAN, n INTEGER, r REAL, u UUID, stamp TIMESTAMP, tx TXID, s TEXT, j JSON, v VECTOR(1))",
    );
    membership_rows(&db, "choices(id)", "", 40_000);
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
    let values = [
        ("missing", Value::Null),
        ("b", Value::Bool(true)),
        ("n", Value::Int64(1)),
        ("n", Value::Int64(i64::MIN)),
        ("n", Value::Int64(i64::MAX)),
        ("n", Value::Int64(9_007_199_254_740_993)),
        ("r", Value::Float64(1.0)),
        ("r", Value::Float64(1.5)),
        ("r", Value::Float64(0.0)),
        ("r", Value::Float64(-0.0)),
        ("r", Value::Float64(f64::NAN)),
        ("r", Value::Float64(9_223_372_036_854_775_808.0)),
        ("u", Value::Uuid(uuid)),
        ("stamp", Value::Timestamp(0)),
        ("tx", Value::TxId(TxId(1))),
        ("s", Value::Text(uuid.to_string())),
        ("s", Value::Text(uuid.to_string().to_uppercase())),
        ("s", Value::Text("1970-01-01T00:00:00Z".into())),
        ("s", Value::Text("bad UUID/time".into())),
        ("j", Value::Json(serde_json::json!([1]))),
        ("s", Value::Text("[1]".into())),
        ("v", Value::Vector(vec![1.0])),
    ];
    let lhs = "COALESCE(missing,b,n,r,u,stamp,tx,s,j,v)";
    for (_, right) in &values {
        for negated in ["", "NOT "] {
            let tx = db.transaction().unwrap();
            for (id, (column, left)) in values.iter().enumerate() {
                tx.execute(
                    &format!("INSERT INTO victim(id,{column}) VALUES($id,$left)"),
                    &params! {"id"=>id as i64,"left"=>left.clone()},
                )
                .unwrap();
            }
            tx.commit().unwrap();
            let args = params! {"right"=>right.clone()};
            let rhs = "COALESCE(missing,$right)";
            let oracle = format!(
                "SELECT id FROM victim WHERE COALESCE({lhs} {negated}IN ({rhs}),FALSE)=FALSE ORDER BY id"
            );
            let oracle = db.execute(&oracle, &args);
            let condition = format!("{lhs} {negated}IN (SELECT {rhs} FROM choices)");
            let all_rows = "SELECT * FROM victim ORDER BY id";
            // Debug snapshots compare the fixture's NaN and distinguish signed zero.
            let before = format!("{:?}", support::rows(&db, all_rows));
            let delete = db.execute(&format!("DELETE FROM victim WHERE {condition}"), &args);
            match (oracle, delete) {
                (Ok(remain), Ok(_)) => {
                    let rows = support::rows(&db, "SELECT id FROM victim ORDER BY id");
                    assert_eq!(rows, remain.rows(), "{negated}IN {right:?}");
                }
                (Err(oracle), Err(delete)) if discriminant(&oracle) == discriminant(&delete) => {
                    let after = support::rows(&db, all_rows);
                    assert_eq!(format!("{after:?}"), before, "{negated}IN {right:?}");
                }
                mismatch => panic!("{negated}IN {right:?}: SELECT and DELETE differ: {mismatch:?}"),
            }
            run("DELETE FROM victim");
        }
    }
    run("INSERT INTO victim(id) VALUES(1)");
    for suffix in ["IN", "NOT IN"] {
        let sql =
            format!("DELETE FROM victim WHERE NULL {suffix} (SELECT id FROM choices WHERE id<0)");
        assert_eq!(run(&sql).rows_affected(), u64::from(suffix == "NOT IN"));
    }
}

#[test]
fn membership_preserves_truth_and_in_memory_index_work() {
    let db = support::open(&support::folder("stored-membership-index"));
    let run = |sql: &str| support::run(&db, sql);
    run("CREATE TABLE source(id INTEGER PRIMARY KEY, u UUID, spelling TEXT)");
    run("CREATE TABLE target(id INTEGER PRIMARY KEY, k INTEGER, label TEXT, u UUID)");
    for ddl in [
        "CREATE INDEX by_k ON target(k DESC,id ASC)",
        "CREATE INDEX by_label ON target(label)",
        "CREATE INDEX by_u ON target(u)",
    ] {
        run(ddl);
    }
    let spelling = "550e8400-e29b-41d4-a716-446655440001";
    // A list of 32-byte values leaves memory for the store past 8,192 of them (a quarter of the
    // 1 MiB part, kept beside three planning copies): 10,000 ids are a stored list.
    assert_eq!(
        size_of::<contextdb::Value>(),
        32,
        "the size the stored-list count rests on"
    );
    membership_rows(
        &db,
        "source(id,spelling)",
        &format!(",'{spelling}'"),
        10_000,
    );
    run(&format!("UPDATE source SET u='{spelling}' WHERE id=0"));
    run(&format!(
        "UPDATE source SET spelling='{}' WHERE id=1",
        spelling.to_uppercase()
    ));
    let long = "x".repeat(3000);
    let mut work = None;
    for stored in [500, 5_000] {
        run("DELETE FROM target");
        let rows = (50_000..50_000 + stored)
            .map(|id| format!("({id},{id},'neighbor',NULL)"))
            .collect::<Vec<_>>()
            .join(",");
        run(&format!("INSERT INTO target VALUES {rows}"));
        for (condition, oracle) in [
            ("id IN(SELECT id FROM source)", "id>0 AND id<3"),
            ("k IN(SELECT id FROM source LIMIT 2) AND id>0", "id=1"),
            ("id IN(SELECT DISTINCT id FROM source LIMIT 1)", "id=0"),
            ("k IN(SELECT id*1.0 FROM source) AND id>1", "id=2"),
            ("label IN(SELECT $long FROM source)", "label=$long"),
            ("u IN(SELECT COALESCE(u,spelling) FROM source)", "u=$uuid"),
            ("id IN(SELECT id FROM source) OR id=-1", "id<3"),
            ("k>=0 AND (id IN(SELECT id FROM source) OR id=-1)", "id<3"),
        ] {
            let args = params! {"long"=>long.clone(),"uuid"=>spelling};
            let query = |sql: &str| db.execute(sql, &args).unwrap();
            query(
                "INSERT INTO target VALUES(-1,0,$long,$uuid),(1,1,$long,$uuid),(2,2,'other',NULL)",
            );
            let selected = format!("SELECT id FROM target WHERE {oracle}");
            let expected = query(&selected).rows().len();
            let result = query(&format!("DELETE FROM target WHERE {condition}"));
            assert_eq!(result.rows_affected(), expected as u64, "{condition}");
            if condition.contains("LIMIT 2") {
                let reads = result.rows_examined();
                assert!((1..=6).contains(&reads), "{condition}: {reads}");
                assert_eq!(*work.get_or_insert(reads), reads);
            }
            run("DELETE FROM target WHERE id<3");
        }
    }
}

/// A figure: rows examined and fetched at most. `ROWS` the answer's rows, `FREE` a statement
/// with a reason to read more (it only answers as its twin does), `CTX` the Context's size and
/// `PAIRS` two rows of every Context, which share a JSON key and so an index entry.
const ROWS: u64 = u64::MAX;
const FREE: u64 = u64::MAX - 1;
const CTX: u64 = u64::MAX - 2;
const PAIRS: u64 = u64::MAX - 3;
/// Every form of section 1 and the forms served today; braces are shortened parts (`swaps`).
const PARTS: [(&str, &str); 8] = [
    ("{f}", "SELECT id FROM {t}"),
    ("{s}", "SELECT id FROM {t} WHERE"),
    (
        "{u}",
        "SELECT a.id FROM {t} a JOIN f b ON b.id = a.id WHERE a.x = {k}",
    ),
    ("{j}", "j -> 'n' = '9007199254740993'"),
    ("{p}", "ts >= '{a}' AND ts <= '{b}'"),
    ("{q}", "ts >= '{l}' AND ts <= '{h}'"),
    ("{o}", "(s BETWEEN 10 AND 20 OR s BETWEEN 15 AND 25)"),
    ("{w}", "FROM {t} WHERE x = {k}"),
];
/// A named exception, at today's figure: a join with ORDER BY and LIMIT is not built (sheet
/// section 14, LEAN-B47), so it reads its Context on each side. The row stays so that the day the
/// join's order is built, this figure is replaced by the design's 20.
const JOIN_TODAY: u64 = u64::MAX - 4;
/// The whole table: today's figure for `MAX(id)`. A MAX or MIN answered from one index row, and
/// an UPDATE that gathers its keys from an index on a column it assigns, are named exceptions at
/// today's figures: no product statement sends them (sheet section 14, LEAN-B47).
const TABLE: u64 = u64::MAX - 5;
const TWO: u64 = u64::MAX - 6;
const FORMS: [(&str, u64, u64); 42] = [
    ("SELECT id {w} ORDER BY ts DESC LIMIT 1", 1, 1),
    ("{f} ORDER BY id DESC LIMIT 10", 10, 10),
    ("SELECT MAX(s) {w}", CTX, CTX),
    ("SELECT MIN(s) {w}", CTX, CTX),
    ("SELECT MAX(id) FROM {t}", TABLE, TABLE),
    ("SELECT id {w} AND s BETWEEN 10 AND 20", 11, 11),
    ("SELECT id {w} AND {p}", 11, 11),
    ("SELECT id {w} AND s = 1 + 1", 1, 1),
    ("SELECT id {w} AND (s = 5 OR s = 7)", 2, 2),
    ("{s} (x = {k} OR x = {n}) AND {q}", ROWS, ROWS),
    ("SELECT DISTINCT st {w} LIMIT 1", 1, 1),
    ("{u} ORDER BY a.ts LIMIT 10", JOIN_TODAY, JOIN_TODAY),
    ("SELECT id {w} ORDER BY ts LIMIT 10", 10, 10),
    ("SELECT id {w} ORDER BY g DESC, id LIMIT 10", 12, 12),
    ("{s} id = 5", 1, 1),
    ("{s} id IN (3, 7, 11)", 3, 3),
    ("SELECT id {w} AND s >= {c}", 11, 11),
    ("SELECT id {w} ORDER BY s LIMIT 10", 10, 10),
    ("SELECT COUNT(*) {w}", CTX, 0),
    ("SELECT COUNT(*) {w} AND s BETWEEN 10 AND 20", 11, 0),
    ("SELECT COUNT(*) {w} AND s IN (3, 5, 7)", 3, 0),
    ("SELECT COUNT(*) FROM {t} WHERE id IN (3, 999999)", 1, 0),
    ("SELECT COUNT(*) {w} AND {o}", 16, 0),
    ("{s} (x, s) IN (({k}, 3), ({k}, 5))", 2, 2),
    ("{s} {j}", PAIRS, PAIRS),
    ("SELECT COUNT(*) FROM {t} WHERE {j}", PAIRS, PAIRS),
    ("SELECT id {w} AND s <> 5", FREE, FREE),
    ("SELECT id {w} AND s NOT IN (5, 6)", FREE, FREE),
    ("SELECT id {w} AND st LIKE 'o%'", FREE, FREE),
    ("SELECT id {w} AND s * 1 = 5", FREE, FREE),
    ("SELECT id {w} AND s = g", FREE, FREE),
    ("SELECT id {w} AND (s = 5 OR g = 7)", FREE, FREE),
    ("{s} id = 5 AND x = 0 AND st = 'closed'", 1, 1),
    ("{s} id IN (3, 7, 11) AND st = 'closed'", 3, 3),
    ("{s} x = {k}", ROWS, ROWS),
    ("{s} x = {k} AND st = 'open'", CTX, CTX),
    ("{s} x IN ({k}, {n}) AND st = 'open'", TWO, TWO),
    ("SELECT id {w} AND g = 0 ORDER BY id LIMIT 1", 1, 1),
    ("{f}", ROWS, ROWS),
    ("SELECT COUNT(*) FROM {t}", 0, 0),
    ("{u}", TWO, TWO),
    ("{u} ORDER BY b.ts", TWO, TWO),
];

/// The timestamp text of row `id`: one second a row.
fn at(id: i64) -> String {
    format!(
        "2026-01-01T{:02}:{:02}:{:02}Z",
        id / 3600,
        id / 60 % 60,
        id % 60
    )
}

fn load(db: &contextdb::Database, rows: i64, c: i64) {
    for t in ["e", "f"] {
        support::run(
            db,
            &format!(
                "CREATE TABLE {t} (id INTEGER PRIMARY KEY, x INTEGER, s INTEGER, g INTEGER, ts TIMESTAMP, st TEXT, j JSON)"
            ),
        );
    }
    for (n, ix) in ["(x, ts)", "(x, s)", "(x, g)", "((j -> 'n'))"]
        .iter()
        .enumerate()
    {
        support::run(db, &format!("CREATE INDEX e{n} ON e {ix}"));
    }
    let tx = db.transaction().unwrap();
    for id in 1..=rows {
        let seq = (id - 1) % c + 1;
        // The first two rows of each Context hold two integers above 2^53 with one key.
        let n = if seq < 3 { (1 << 53) | (seq - 1) } else { id };
        let values = params! {
            "id" => id, "x" => (id - 1) / c, "s" => seq, "g" => (seq - 1) / 2, "ts" => at(id),
            "st" => ["open", "closed"][(id % 2) as usize], "j" => format!("{{\"n\": {n}}}"),
        };
        for t in ["e", "f"] {
            let sql = format!("INSERT INTO {t} VALUES ($id, $x, $s, $g, $ts, $st, $j)");
            tx.execute(&sql, &values).unwrap();
        }
    }
    tx.commit().unwrap();
}

macro_rules! at_size {
    ($($n:ident $r:literal $c:literal)*) => {$(#[test] fn $n() { table($r, $c) })*};
}
at_size! {
    a_statement_reads_what_was_built_for_it_at_2000_rows_in_contexts_of_200 2000 200
    a_statement_reads_what_was_built_for_it_at_2000_rows_in_one_context 2000 2000
    a_statement_reads_what_was_built_for_it_at_20000_rows_in_contexts_of_200 20_000 200
    a_statement_reads_what_was_built_for_it_at_20000_rows_in_contexts_of_2000 20_000 2000
}

fn table(rows: i64, c: i64) {
    let mut over = Vec::new();
    let db = support::open(&support::folder(&format!("counted-forms-{rows}-{c}")));
    load(&db, rows, c);
    let (k, base) = (rows / c / 2, rows / c / 2 * c);
    let swaps = [
        ("{k}", k.to_string()),
        ("{n}", (k + 1).to_string()),
        ("{c}", (c - 10).to_string()),
        ("{a}", at(base + 10)),
        ("{b}", at(base + 20)),
        ("{l}", at(base + c - 4)),
        ("{h}", at(base + c + 5)),
    ];
    let fill = |sql: &str, t: &str| {
        let sql = PARTS
            .iter()
            .fold(sql.to_string(), |s, (from, to)| s.replace(from, to));
        swaps
            .iter()
            .fold(sql, |s, (from, to)| s.replace(from, to))
            .replace("{t}", t)
    };
    for (form, examined, fetched) in FORMS {
        let (sql, twin) = (fill(form, "e"), fill(form, "f"));
        let what = format!("{sql} (table {rows}, Context {c})");
        let before = db.work_done().unwrap().rows_read;
        let found = support::run(&db, &sql);
        let read = db.work_done().unwrap().rows_read - before;
        let norm = |mut r: Vec<Vec<contextdb::Value>>| {
            if !sql.contains("ORDER BY") {
                r.sort_by_key(|x| format!("{x:?}"));
            }
            r
        };
        let (got, want) = (norm(found.rows().to_vec()), norm(support::rows(&db, &twin)));
        assert_eq!(
            got, want,
            "{what}: an index may change the work, never the answer"
        );
        let answer = found.rows().len() as u64;
        let (most, from_store) = match (examined, fetched) {
            (FREE, _) => continue,
            (ROWS, _) => (answer, answer),
            (CTX, CTX) => (c as u64, c as u64),
            (CTX, f) => (c as u64, f),
            (TWO, _) => (2 * c as u64, 2 * c as u64),
            (TABLE, _) => (rows as u64, rows as u64),
            (JOIN_TODAY, _) => (2 * c as u64, 2 * c as u64),
            (PAIRS, _) => (2 * rows as u64 / c as u64, 2 * rows as u64 / c as u64),
            (e, f) => (e, f),
        };
        if found.rows_examined() > most || read > from_store {
            over.push(format!(
                "{what}: examined {} of {most}, the store fetched {read} of {from_store}",
                found.rows_examined()
            ));
        }
    }
    // A count of one Context's entries is work, charged once: a limit just above them admits it.
    let limits = ReadLimits {
        work: c as u64 + 10,
        ..Default::default()
    };
    let tight = db.read_session(limits).unwrap();
    let count = format!("SELECT COUNT(*) FROM e WHERE x = {k}");
    for sql in [
        count.clone(),
        format!("WITH c AS (SELECT 1) {count}"),
        format!("SELECT ({count})"),
    ] {
        support::run(&tight, &sql);
    }
    let write = |sql: &str| {
        let tx = db.transaction().unwrap();
        let done = support::run(&tx, sql);
        (done.rows_affected(), done.rows_examined())
    };
    let set = format!("SET s = s + 100000 WHERE x = {k} AND s BETWEEN 1 AND 5");
    let update = |t: &str| write(&format!("UPDATE {t} {set}"));
    let (twin, mine) = (update("f"), update("e"));
    assert_eq!(
        mine.0, twin.0,
        "the rows an UPDATE changes do not depend on its index"
    );
    if mine.1 > c as u64 {
        over.push(format!(
            "UPDATE of 5 rows (table {rows}, Context {c}): examined {} of its Context's {c}",
            mine.1
        ));
    }
    for (keys, named) in [("= 5", 1), ("IN (3, 7, 11)", 3)] {
        let sql = |t| format!("UPDATE {t} SET st = 'open' WHERE id {keys} AND st = 'closed'");
        let (twin, mine) = (write(&sql("f")), write(&sql("e")));
        assert_eq!([mine.0, twin.0], [named; 2], "{keys} names closed rows");
        if mine.1 > named {
            over.push(format!("UPDATE {keys} (table {rows}): examined {}", mine.1));
        }
    }
    let held = db.snapshot().unwrap();
    let gone = counted(&db, || support::run(&db, "DELETE FROM e WHERE id = 5"));
    let at = |q| db.execute_at_snapshot(q, &params! {}, &held).unwrap();
    let seen = counted(&db, || at("SELECT id FROM e WHERE id = 5"));
    assert_eq!([gone.1, gone.2, seen.1, seen.2], [1; 4], "one row read");
    assert!(
        over.is_empty(),
        "a statement reads the rows its answer needs and the entries naming them, never its Context or its table:\n{}",
        over.join("\n")
    );
}

/// A distance bound is the search's own cut-off: answers are the exact scan's, a question unlike
/// everything stored compares no vector in full and reads no row, and `INDEXED` still refuses.
#[test]
fn a_distance_bound_cuts_the_search_and_answers_as_the_exact_scan_does() {
    let (_, db) = support::built("bound-search", 16, "INDEXING_THRESHOLD 1000", 2000);
    let (stored, unlike) = (support::gathered(7, 16), vec![-1.0_f32; 16]);
    let sql = |bound: &str, extra: &str, order: &str, mode: &str| {
        let select = "SELECT id, v <=> $q AS distance FROM docs";
        format!("{select} WHERE v <=> $q < {bound}{extra} ORDER BY {order} {mode}LIMIT 8")
    };
    let (exact, indexed) = ("(v <=> $q) + 0", "USE VECTOR INDEXED ");
    for (question, extra) in [
        (&stored, ""),
        (&stored, " AND tag < 25"),
        (&unlike, ""),
        (&unlike, " AND tag < 25"),
    ] {
        let found = support::answer(&db, &sql("0.5", extra, "distance", ""), question);
        let wanted = support::answer(&db, &sql("0.5", extra, exact, ""), question);
        assert_eq!(
            found.rows(),
            wanted.rows(),
            "{extra}: the exact scan's rows"
        );
        assert_eq!(
            found.rows().is_empty(),
            question == &unlike,
            "{extra}: rows within the bound"
        );
        if question == &unlike {
            assert!(
                found.distance_evaluations() <= 2000,
                "{extra}: {} compared",
                found.distance_evaluations()
            );
            assert_eq!(
                found.rows_examined(),
                0,
                "{extra}: a search with no row to give reads none"
            );
        }
    }
    // Under INDEXED: the bare form answers; a bound that cuts a rare condition gives the exact
    // short answer; one that cuts nothing is refused as without it.
    assert!(
        !support::answer(&db, &sql("0.5", "", "distance", indexed), &stored)
            .rows()
            .is_empty()
    );
    // Rows 7, 207, 407 and 607 share the stored vector's direction (id mod 8 = 7) and its tag.
    let rare = " AND tag = 7 AND id < 700";
    let short = support::answer(&db, &sql("0.3", rare, "distance", indexed), &stored);
    let wanted = support::answer(&db, &sql("0.3", rare, exact, ""), &stored);
    assert_eq!(
        short.rows(),
        wanted.rows(),
        "a bound that cuts gives the exact short answer"
    );
    assert!(
        !short.rows().is_empty() && short.rows().len() < 8,
        "the rare rows within the bound"
    );
    // A bound above every distance cuts nothing: the answer is the unbounded search's.
    let unbounded = format!(
        "SELECT id, v <=> $q AS distance FROM docs WHERE tag = 7 AND id < 700 ORDER BY distance {indexed}LIMIT 8"
    );
    assert_eq!(
        support::search(&db, &sql("3.0", rare, "distance", indexed), &stored)
            .expect("a bound that cuts nothing")
            .rows(),
        support::search(&db, &unbounded, &stored)
            .expect("the same search without a bound")
            .rows(),
        "a bound that cuts nothing changes no row"
    );
}

/// A filter no row can pass, said by a parameter bound to FALSE as well as by what a script can
/// write, reads nothing: no row examined, none read from the store, and a write changes none.
/// `a = 5 AND a >= 5` only looks like one and returns its rows.
#[test]
fn a_filter_no_row_can_pass_reads_nothing_by_a_parameter_as_by_its_text() {
    let folder = support::folder("filter-no-row");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX t_a ON t (a)");
    support::run(
        &db,
        "INSERT INTO t (id, a) VALUES (1, 1), (2, 3), (3, 5), (4, 7), (5, 9)",
    );
    // A table with no key and no index has nothing to name a row by, and still reads none.
    support::run(&db, "CREATE TABLE nopk (a INTEGER)");
    for from in (0..2000).step_by(500) {
        let rows: Vec<String> = (from..from + 500).map(|a| format!("({a})")).collect();
        support::run(
            &db,
            &format!("INSERT INTO nopk (a) VALUES {}", rows.join(", ")),
        );
    }
    let none = params! { "no" => false };
    for sql in [
        "SELECT a FROM nopk WHERE FALSE",
        "SELECT a FROM nopk WHERE $no",
        "SELECT id FROM t WHERE $no",
        "SELECT COUNT(*) FROM t WHERE $no",
        "UPDATE t SET a = a + 1 WHERE $no",
        "DELETE FROM t WHERE $no",
        "SELECT id FROM t WHERE a > 5 AND a < 3",
        "SELECT id FROM t WHERE a BETWEEN 10 AND 5",
    ] {
        let before = db.work_done().unwrap().rows_read;
        let answer = db.execute(sql, &none).expect(sql);
        let read = db.work_done().unwrap().rows_read - before;
        assert_eq!(
            (read, answer.rows_examined()),
            (0, 0),
            "{sql}: a filter no row can pass reads nothing, from the store or by the count"
        );
        assert!(
            answer
                .rows()
                .first()
                .is_none_or(|row| row[0] == contextdb::Value::Int64(0)),
            "{sql}: it answers no row, or a count of 0"
        );
    }
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM t"),
        5,
        "no write changed a row"
    );
    let only = support::rows(&db, "SELECT id FROM t WHERE a = 5 AND a >= 5");
    assert_eq!(
        only,
        [[contextdb::Value::Int64(3)]],
        "a = 5 AND a >= 5 is not a filter that passes nothing"
    );
}

/// The record of statements: runs of one statement with other constants are one line (constants,
/// `LIMIT` numbers and vector literals are `?`), counts add up the runs', a session records to it,
/// a narrowed handle is refused, and a reset empties it.
#[test]
fn the_statement_record_has_one_line_per_form_and_adds_up_its_runs() {
    use contextdb::Value;
    let (_, db) = support::built("statement-record", 3, "", 20);
    let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
    let narrowed = db.scoped_with_contexts(support::context_set(&[1]));
    let (show, reset) = ("SHOW STATEMENT_STATISTICS", "RESET STATEMENT_STATISTICS");
    support::run(&db, reset);
    let mut ran = [0u64; 4];
    let mut note = |f: contextdb::QueryResult| {
        ran[0] += 1;
        ran[1] += f.rows().len() as u64;
        ran[2] += f.rows_examined();
        ran[3] += f.distance_evaluations();
    };
    for sql in [
        "SELECT id FROM docs WHERE tag = 3 LIMIT 4",
        "SELECT id FROM docs WHERE tag = 4 LIMIT 2",
        "SELECT id FROM docs ORDER BY v <=> [0.1, 0.2, 0.3] LIMIT 2",
        "SELECT id FROM docs ORDER BY v <=> [0.4, 0.5, 0.6] LIMIT 2",
    ] {
        note(support::run(&db, sql));
    }
    let by_session = "SELECT id FROM docs WHERE tag = 5 LIMIT 3";
    note(support::run(&session, by_session));
    let listing = support::run(&db, show);
    let has = |r: &&Vec<Value>| format!("{r:?}").contains("docs");
    let lines: Vec<_> = listing.rows().iter().filter(has).collect();
    assert_eq!(
        lines.len(),
        2,
        "one line for a form, whatever its constants"
    );
    let at = |n: &str| listing.columns().iter().position(|c| c == n).unwrap();
    let sum = |n: &str| {
        lines
            .iter()
            .map(|r| if let Value::Int64(x) = r[at(n)] { x } else { 0 })
            .sum::<i64>()
    };
    let kept = ["calls", "rows", "rows_examined", "distance_evaluations"].map(|n| sum(n) as u64);
    assert_eq!(
        kept, ran,
        "calls, rows, rows examined and vectors compared add up"
    );
    let text = |r: &&Vec<Value>| format!("{:?}", r[at("statement")]);
    let words = lines.iter().map(text).collect::<String>();
    let clean = words.contains('?') && !words.contains(['3', '4', '5']);
    assert!(clean, "constants are `?`: {words}");
    let unused = listing.columns().iter().any(|c| c == "unused_index");
    assert!(
        unused,
        "each line says which index its last call left unused"
    );
    for r in &lines {
        assert!(
            matches!(r[at("unused_index")], Value::Null | Value::Text(_)),
            "{r:?}"
        );
    }
    // An index the last call could have used and did not is named, with its reason.
    support::run(&db, "CREATE TABLE ix (id INTEGER PRIMARY KEY, tag INTEGER)");
    support::run(&db, "CREATE INDEX ix_tag ON ix (tag)");
    support::run(&db, "SELECT id FROM ix WHERE tag = 1 OR id = 2");
    let rows = support::rows(&db, show);
    let line = rows.iter().find(|r| format!("{r:?}").contains("FROM ix"));
    match &line.expect("the form over ix")[at("unused_index")] {
        Value::Text(named) => {
            let (index, why) = named.split_once(": ").expect("the index, then its reason");
            assert!(
                ["ix_tag", "__pk_id"].contains(&index) && !why.is_empty(),
                "{named}"
            );
        }
        other => panic!("the OR's index is not named as unused: {other:?}"),
    }
    for sql in [show, reset] {
        drop(support::refusal(&narrowed, sql));
    }
    support::run(&db, reset);
    let after = format!("{:?}", support::run(&db, show).rows());
    assert!(!after.contains("docs"), "a reset empties the record");
    support::run(&db, "SELECT id FROM docs");
    support::run(&db, "SELECT tag FROM docs");
    support::run(&db, "SET STATEMENT_STATISTICS_LIMIT 1");
    let kept = support::rows(&db, show).len();
    assert!(kept <= 1, "LIMIT 1 keeps {kept} forms");
}

/// A JSON value is worked out when the statement runs and no sooner: a bound text that is not
/// a JSON value answers no row and no error, and a value the statement works out itself answers
/// as the table without the index does.
#[test]
fn a_bound_value_compared_with_a_json_field_is_worked_out_as_the_row_does() {
    let db = support::open(&support::folder("json-bound"));
    for t in ["jt", "jx"] {
        support::run(
            &db,
            &format!("CREATE TABLE {t} (id INTEGER PRIMARY KEY, doc JSON)"),
        );
        support::run(
            &db,
            &format!("INSERT INTO {t} VALUES (1, '{{\"a\": 1}}'), (2, '{{\"k\": 1}}')"),
        );
    }
    support::run(&db, "CREATE INDEX jx_k ON jx ((doc -> 'k'))");
    // A text that is not JSON fails beside a JSON value on the table without an index; the table
    // with one fails with the same error, no earlier and no differently.
    let by = |t: &str, p: &str| {
        db.execute(
            &format!("SELECT id FROM {t} WHERE doc -> 'k' = $p"),
            &params! { "p" => p },
        )
    };
    let (plain, indexed) = (by("jt", "abc"), by("jx", "abc"));
    assert_eq!(
        format!("{:?}", indexed.as_ref().map(|r| r.rows().to_vec())),
        format!("{:?}", plain.as_ref().map(|r| r.rows().to_vec())),
        "the index fails where the table without it fails, with its error"
    );
    for t in ["jt", "jx"] {
        let none =
            by(t, "\"abc\"").unwrap_or_else(|e| panic!("{t}: a JSON text is no error: {e:?}"));
        assert!(none.rows().is_empty(), "{t}: no row holds that value");
    }
    let sql = |t: &str| format!("SELECT id FROM {t} WHERE doc -> 'k' = COALESCE(NULL, '1')");
    let shown = |t: &str| {
        format!(
            "{:?}",
            db.execute(&sql(t), &params! {}).map(|r| r.rows().to_vec())
        )
    };
    assert_eq!(
        shown("jx"),
        shown("jt"),
        "a value the statement works out answers, or fails, as the table without the index does"
    );
}

/// A join feeding a window's partition gives the first rows of its order as the same statement
/// without the join does: the oldest row of a Context, not the first by key.
#[test]
fn a_window_over_a_join_gives_the_first_rows_of_its_order() {
    let db = support::open(&support::folder("window-join"));
    support::run(
        &db,
        "CREATE TABLE s (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, at INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX s_ctx_at ON s (ctx, at DESC, id)");
    support::run(&db, "CREATE TABLE k (ctx TEXT PRIMARY KEY)");
    support::run(&db, "INSERT INTO k VALUES ('a'), ('b')");
    support::run(
        &db,
        "INSERT INTO s (id, ctx, at) VALUES (1, 'a', 30), (2, 'a', 10), (3, 'a', 20), (4, 'b', 5)",
    );
    let ranked = |from: &str| {
        format!(
            "WITH ranked AS (SELECT s.ctx, s.id, ROW_NUMBER() OVER (PARTITION BY s.ctx ORDER BY s.at ASC, s.id ASC) AS r FROM {from}) \
             SELECT ctx, id, r FROM ranked WHERE ctx = 'a' AND r <= 1 ORDER BY r"
        )
    };
    let joined = support::rows(&db, &ranked("s JOIN k ON k.ctx = s.ctx"));
    assert_eq!(
        joined,
        [[
            contextdb::Value::Text("a".into()),
            contextdb::Value::Int64(2),
            contextdb::Value::Int64(1)
        ]],
        "the oldest row of the Context, which is id 2 and not the first by key"
    );
    assert_eq!(
        joined,
        support::rows(&db, &ranked("s")),
        "as the statement without the join"
    );
}

fn counted(db: &Database, run: impl FnOnce() -> QueryResult) -> (usize, u64, u64) {
    let before = db.work_done().unwrap().rows_read;
    let found = run();
    let read = db.work_done().unwrap().rows_read - before;
    (found.rows().len(), found.rows_examined(), read)
}

/// A composite key, a unique column and an upsert read the rows they name. Row 507: a 5, b 7.
#[test]
fn a_composite_key_a_unique_column_and_an_upsert_read_only_the_rows_they_name() {
    let db = support::open(&support::folder("counted-keys"));
    let make = "CREATE TABLE p (a INT, b INT, v INT UNIQUE, PRIMARY KEY (a, b))";
    support::run(&db, make);
    let row = |n: i64| format!("({}, {}, {n})", n / 100, n % 100);
    let all: Vec<_> = (0..2000).map(row).collect();
    support::run(&db, &format!("INSERT INTO p VALUES {}", all.join(",")));
    // The longest matched prefix is read.
    support::run(&db, "CREATE INDEX p_a ON p (a)");
    for (sql, answer, work) in [
        ("SELECT v FROM p WHERE a = 5 AND b = 7", 1, 1),
        ("SELECT v FROM p WHERE a = 5", 100, 100),
        ("SELECT a FROM p WHERE v = 507", 1, 1),
        ("SELECT b FROM p WHERE a = 5 AND v = 507", 1, 1),
        ("SELECT COUNT(*) FROM p", 1, 0),
    ] {
        let got = counted(&db, || support::run(&db, sql));
        assert_eq!(got, (answer, work, work), "{sql}: the rows its key names");
    }
    // With a key of its own, two IN lists and an equality read the rows they match, by the index (a, b, v) and not (a).
    support::run(
        &db,
        "CREATE TABLE q (id INT PRIMARY KEY, a INT, b INT, v INT, u INT UNIQUE)",
    );
    support::run(&db, "CREATE INDEX q_abv ON q (a, b, v)");
    support::run(&db, "CREATE INDEX q_a ON q (a)");
    let rows: Vec<_> = (0..1000)
        .map(|n| format!("({n}, {}, {}, {}, {n})", n / 100, n % 10, n / 10 % 10))
        .collect();
    support::run(&db, &format!("INSERT INTO q VALUES {}", rows.join(",")));
    let sql = "SELECT id FROM q WHERE a IN (5, 6) AND b IN (7, 8) AND v = 0";
    assert_eq!(
        counted(&db, || support::run(&db, sql)),
        (4, 4, 4),
        "only the matching rows"
    );
    // A UNIQUE rule given one value, by `=` or by each joined row, is read before an index that
    // fixes more columns (q_abv: 10 rows for each a and b).
    let one = "SELECT id FROM q WHERE a = 5 AND b = 7 AND u = 517";
    let on = "q y ON y.a = x.a AND y.b = x.b AND y.u = x.u WHERE x.a = 5 AND x.b = 7";
    for (sql, rows, work) in [
        (one.to_owned(), 1, 1),
        (format!("SELECT y.id FROM q x JOIN {on}"), 10, 20),
    ] {
        let got = counted(&db, || support::run(&db, &sql));
        assert_eq!(got, (rows, work, work), "{sql}");
    }
    let tx = db.transaction().unwrap();
    let up = "INSERT INTO p VALUES (99, 1, 99999) ON CONFLICT (a, b) DO UPDATE SET v = 99999";
    for examined in [0, 1] {
        let read = support::run(&tx, up).rows_examined();
        assert_eq!(read, examined, "an upsert reads its key's row");
    }
}

/// A walk backward from one node reads the edges into it and no other (`query/graph.rs:431-560`).
#[test]
fn a_graph_walk_reads_the_edges_into_its_node_and_no_others() {
    let db = support::open(&support::folder("counted-graph"));
    let make =
        "CREATE TABLE edges (id INT PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)";
    support::run(&db, make);
    let uuid = |n: u64| format!("'{}'", contextdb::Uuid::from_u128(n as u128));
    let edge = |id: u64, to, kind| format!("({id}, {}, {}, '{kind}')", uuid(id + 9999), uuid(to));
    // 500 edges into nodes 100 to 599; node 1 has 30 BASED_ON and 10 ABOUT edges into it.
    let noise = (1..=500).map(|i| edge(i, 99 + i, "BASED_ON"));
    let mine = (0..40).map(|j| edge(3000 + j, 1, ["BASED_ON", "ABOUT"][j as usize / 30]));
    let all: Vec<_> = noise.chain(mine).collect();
    support::run(&db, &format!("INSERT INTO edges VALUES {}", all.join(",")));
    for (node, answer, most) in [(1, 30, 40), (2, 0, 0)] {
        let (walk, at) = ("edges MATCH (e)<-[:BASED_ON]-(d) WHERE e.id", uuid(node));
        let sql = format!("SELECT d_id FROM GRAPH_TABLE({walk} = {at} COLUMNS (d.id AS d_id))");
        let (len, examined, _) = counted(&db, || support::run(&db, &sql));
        assert_eq!(
            (len, examined <= most),
            (answer, true),
            "node {node}: {examined}"
        );
    }
}

#[test]
fn a_filtered_search_under_the_default_read_limits_works_in_proportion_to_the_contexts_it_names() {
    let folder = support::folder("filtered-work");
    let db = support::open(&folder);
    support::declare_docs(&db, 16, "PARTITION_KEY (ctx)");
    for c in 0..8_i64 {
        let (ctx, ids) = (format!("c{c}"), c * 1250 + 1..=(c + 1) * 1250);
        support::load_docs(&db, ids, &ctx, |n| support::gathered(n, 16));
    }
    support::maintenance(&db);
    let session = ReadSession::open(&folder, ReadLimits::default()).unwrap();
    let query = support::gathered(60_000, 16);
    let work = |filter: &str, mode: &str| {
        let sql = support::by_mode(Some(filter), mode, 10);
        support::answer(&session, &sql, &query)
    };
    let (one, two) = (work("ctx = 'c0'", "INDEXED"), work("ctx = 'c1'", "INDEXED"));
    let both = work("ctx IN ('c0', 'c1')", "INDEXED");
    let alone = one.rows_examined() + two.rows_examined();
    assert!(
        both.rows_examined() <= alone,
        "two Contexts keep one top ten, so cost no more than the two alone"
    );
    let every = "ctx IN ('c0','c1','c2','c3','c4','c5','c6','c7')";
    let exact = work(every, "EXACT");
    assert_eq!(work(every, "INDEXED").rows(), exact.rows(), "eight");
    assert_eq!(work("ctx = 'c0'", "EXACT").rows(), one.rows(), "one");
}

#[test]
fn inserting_edges_from_one_node_reads_none_of_the_stored_edges() {
    const INSERT: &str = "INSERT INTO edges VALUES ($id, $from, $to, 'E')";
    let reads_of_new_edges = |stored: i64| -> u64 {
        let db = support::open(&support::folder(&format!("edges-from-one-{stored}")));
        let create = "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL)";
        support::run(&db, create);
        let put = |ids: std::ops::RangeInclusive<i64>| {
            let tx = db.transaction().unwrap();
            for id in ids {
                let args = params! { "id" => id, "from" => support::context(1), "to" => support::context(id as u128 + 1) };
                tx.execute(INSERT, &args).unwrap();
            }
            tx.commit().unwrap();
            db.work_done().unwrap().rows_read
        };
        let base = put(1..=stored);
        put(stored + 1..=stored + 50) - base
    };
    let (few, many) = (reads_of_new_edges(1_000), reads_of_new_edges(10_000));
    assert!(many < few + 50, "50 new edges read {few} rows, then {many}");
}
