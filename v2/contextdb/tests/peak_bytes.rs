//! A statement reports the most memory it held, and that figure is the statement's own: a lookup
//! by key and a one-row commit hold the same at 10,000 stored rows as at 100,000, and a write's
//! figure covers the work of its commit.

mod support;

use contextdb::params;

#[test]
fn a_statements_peak_bytes_do_not_grow_with_the_store() {
    let folder = support::folder("peak-bytes-level");
    let db = support::open(&folder);
    support::declare(&db);
    let peaks = |stored: i64| {
        let look = support::run(&db, "SELECT id, name, tag FROM t WHERE id = 7");
        let [id, name, tag] = <[_; 3]>::try_from(support::row(stored + 1)).expect("three columns");
        let write = db
            .execute(
                "INSERT INTO t (id, name, tag) VALUES ($id, $name, $tag)",
                &params! { "id" => id, "name" => name, "tag" => tag },
            )
            .expect("a one-row commit");
        let [look, write] = [look.peak_bytes(), write.peak_bytes()].map(|peak: Option<u64>| {
            // A held answer and a staged row are counted for at least their text, 11 bytes.
            let peak = peak.expect("a whole handle is told each statement's peak_bytes");
            assert!(peak >= 11, "{peak} bytes is less than the row's text");
            peak
        });
        db.execute(
            "DELETE FROM t WHERE id = $id",
            &params! { "id" => stored + 1 },
        )
        .expect("take the written row back out");
        // Remove only the exact admitted commit bookkeeping, leaving the statement peak.
        (
            look,
            write - support::used(&db) as u64 - redb::WriteTransaction::DRAIN_BYTES as u64,
        )
    };
    support::insert_together(&db, 1..=10_000);
    // The first run of each form adds its line to the statement record, once.
    let _ = peaks(10_000);
    let small = peaks(10_000);
    support::insert_together(&db, 10_001..=100_000);
    assert_eq!(
        peaks(100_000),
        small,
        "a lookup by key and a one-row commit hold what they read and write, not what the \
         store holds: their peak_bytes at 100,000 stored rows against 10,000"
    );
    let db = support::reopen(db, &folder);
    let sql = "SELECT id, name, tag FROM t WHERE id = 7";
    let _ = support::run(&db, sql);
    assert_eq!(support::run(&db, sql).peak_bytes(), Some(small.0));
}

/// A write's figure covers its commit. A standalone delete of rows with vectors succeeds on a
/// twin store under a limit exactly equal to the first delete's reported peak.
#[test]
fn a_writes_peak_bytes_cover_its_commit() {
    let [first, twin] = ["peak-bytes-commit", "peak-bytes-commit-twin"].map(|name| {
        let db = support::open(&support::folder(name));
        support::declare_docs(&db, 4, "");
        support::load_docs(&db, 1..=1_000, "a", |n| support::vector(n, 4));
        db
    });
    let figure = |result: contextdb::QueryResult| result.peak_bytes().expect("a whole handle");
    let whole = figure(support::run(&first, "DELETE FROM docs"));
    // The twin's record is empty too, and takes its line for the statement on top of the peak.
    let limit = whole as usize + support::form_charge("DELETE FROM docs");
    twin.set_memory_limit(Some(limit))
        .expect("the figure and the record's line as the limit");
    support::run(&twin, "DELETE FROM docs");
}

/// A JSON literal beside a column is read once for the statement and kept, not read again for
/// each row: the literal's own bytes are charged once, so a statement over 2,000 rows peaks
/// less than one more reading of the literal above the same statement over 20.
#[test]
fn a_json_literal_is_read_once_not_once_per_row() {
    let db = support::open(&support::folder("peak-bytes-json-literal"));
    support::run(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON)");
    let literal = format!(r#"["{}"]"#, support::letters(1, 200_000));
    let sql = format!("SELECT id FROM docs WHERE doc = '{literal}'");
    let mut peaks = Vec::new();
    for (first, rows) in [(1, 20), (21, 2000)] {
        let tx = db.transaction().expect("begin");
        for id in first..=rows {
            let row = "INSERT INTO docs (id, doc) VALUES ($id, '{\"n\":1}')";
            tx.execute(row, &params! { "id" => id }).expect("a row");
        }
        tx.commit().expect("commit");
        let none = support::run(&db, &sql);
        assert!(none.rows().is_empty(), "no stored document is the literal");
        peaks.push(none.peak_bytes().expect("a whole handle"));
    }
    assert!(
        peaks[1] < peaks[0] + literal.len() as u64,
        "the statement peaked at {} bytes over 20 rows and {} over 2,000: the literal is read \
         again for each row",
        peaks[0],
        peaks[1]
    );
}

/// A commit and a declaration cost what they change, not the catalog's size: an INSERT peaks the
/// same at 100 declared sets as at 10; a declaration adds only a copy's slots and keys.
#[test]
fn a_commit_and_a_declaration_do_not_grow_with_the_schema() {
    let sql = [
        "CREATE TABLE late (id INTEGER PRIMARY KEY)",
        "CREATE EVENT TYPE late_added WHEN INSERT ON pre0",
        "INSERT INTO plain VALUES (2)",
        "INSERT INTO pre0 VALUES (2)",
    ];
    let peaks = |sets: usize| {
        let db = support::open(&support::folder(&format!("peak-bytes-schema-{sets}")));
        support::run(&db, "CREATE TABLE plain (id INTEGER PRIMARY KEY)");
        let run = |sql: String| drop(support::run(&db, &sql));
        for i in 0..sets {
            run(format!("CREATE TABLE pre{i} (id INTEGER PRIMARY KEY)"));
            run(format!("CREATE EVENT TYPE pe{i} WHEN INSERT ON pre{i}"));
            run(format!("CREATE SINK ps{i} TYPE callback"));
            run(format!("CREATE ROUTE pr{i} EVENT pe{i} TO ps{i}"));
        }
        support::run(&db, "INSERT INTO plain VALUES (1)");
        sql.map(|s| {
            let peak = support::run(&db, s).peak_bytes().expect("a whole handle");
            peak.saturating_sub((support::used(&db) + redb::WriteTransaction::DRAIN_BYTES) as u64)
        })
    };
    let (small, large) = (peaks(10), peaks(100));
    let node = |slot: usize| (4 * slot + 64) as u64;
    let set = node(size_of::<(String, usize)>())
        + node(size_of::<(u32, usize)>())
        + 3 * node(size_of::<((u8, String), usize)>());
    let keys: usize = (10..100).map(|i: usize| 9 + 4 * i.to_string().len()).sum();
    let bound = 90 * set + keys as u64;
    let over: Vec<_> = (0..4)
        .filter_map(|row| {
            let (grew, allowed) = (
                large[row].saturating_sub(small[row]),
                if row < 2 { bound } else { 0 },
            );
            (grew > allowed).then(|| format!("`{}` grew {grew}, allowed {allowed}", sql[row]))
        })
        .collect();
    assert!(
        over.is_empty(),
        "a schema of 100 sets peaks over 10 sets' by a copy's slots and keys only: {over:#?}"
    );
}

/// A filtered find holds its top k and a body, not the rows it passed over.
#[test]
fn a_filtered_finds_peak_bytes_do_not_grow_with_the_rows_it_passes_over() {
    let query = support::vector(7, 8);
    let peaks = [360, 3_600].map(|rows| {
        let (_, db) = support::built("peak-bytes-find", 8, "", rows);
        let sql = support::by_mode(Some("tag < 25"), "", 10);
        support::answer(&db, &sql, &query)
            .peak_bytes()
            .expect("a peak")
    });
    // A filtered find screens the vectors first, so it holds a slot (32 bytes) and the key (8
    // bytes) for each vector not yet ruled out, at most twice over; never a body it passes.
    let allowed = 2 * (32 + 8) * (3_600 - 360);
    assert!(
        peaks[1] <= peaks[0] + allowed,
        "10x the rows holds {peaks:?}: the peak grows by at most {allowed} bytes"
    );
}
