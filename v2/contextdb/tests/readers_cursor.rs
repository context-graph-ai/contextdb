mod support;
use contextdb::{DatabaseOpenOptions, Error, Value, params};
use std::{num::NonZeroUsize, sync::atomic::Ordering::SeqCst};
#[test]
fn a_cursor_page_reads_its_own_rows() {
    for file in [false, true] {
        let folder = support::folder(&format!("cursor-pages-{file}"));
        let (storage, bytes) = support::counting_bytes_read();
        let mut options = DatabaseOpenOptions::default();
        options.storage = Some(storage);
        let db = support::open_with(&folder, options).unwrap();
        support::declare(&db);
        support::insert_together(&db, 1..=40);
        let mut limits = contextdb::ReadLimits::default();
        (limits.result_rows, limits.cursor_page_rows, limits.work) = (2, 1, 3);
        let session = support::read_session(&db, &folder, file, limits.clone()).unwrap();
        let q = params! {"body" => support::letters(1, 512 << 10)};
        db.execute("UPDATE t SET name = $body", &q).unwrap();
        let sql = "SELECT id, name FROM t";
        bytes.store(0, SeqCst);
        let mut cursor = session.open_cursor(sql, &q).unwrap();
        assert!(
            cursor.has_more(),
            "an unfetched cursor has its first page to give"
        );
        let first = cursor.fetch(None).unwrap();
        let first_page_bytes = bytes.swap(0, SeqCst);
        assert!(file || first_page_bytes > 0);
        assert_eq!(
            first.rows()[0][0],
            Value::Int64(1),
            "the first fetch is the first page"
        );
        support::run(&db, "UPDATE t SET name = 'changed' WHERE id = 20");
        support::close(db);
        bytes.store(0, SeqCst);
        assert_eq!(cursor.fetch(None).unwrap().rows()[0][0], Value::Int64(2));
        assert!(file || bytes.swap(0, SeqCst) <= first_page_bytes);
        let db = support::open(&folder);
        support::run(&db, "DELETE FROM t WHERE id = 30");
        support::insert(&db, 41..=41);
        support::cycle(&db);
        let held = cursor.fetch(NonZeroUsize::new(3)).is_err();
        assert!(held, "the limit still refuses");
        bytes.store(0, SeqCst);
        for id in 3..=40_i64 {
            assert!(cursor.has_more());
            let page = cursor.fetch(None).unwrap();
            assert_eq!(page.rows()[0][0], Value::Int64(id));
            assert_eq!(page.rows()[0][1], q["body"]);
            assert_eq!(page.rows_examined(), 1, "page {id} rescanned");
            assert!(file || bytes.swap(0, SeqCst) <= first_page_bytes);
        }
        assert!(cursor.has_more() && cursor.fetch(None).unwrap().rows().is_empty());
        assert!(!cursor.has_more());
        let limited = "SELECT id FROM t LIMIT 3 OFFSET 2";
        let mut cut = session.open_cursor(limited, &q).unwrap();
        for id in [3, 4, 5] {
            assert_eq!(cut.fetch(None).unwrap().rows(), [vec![Value::Int64(id)]]);
        }
        assert!(!cut.has_more());
        let mut early = session.open_cursor(sql, &q).unwrap();
        let error = early.fetch(NonZeroUsize::new(3)).unwrap_err();
        assert!(matches!(error, Error::ReadLimitExceeded { .. }));
        assert_eq!(
            early.fetch(None).unwrap().rows()[0][0],
            Value::Int64(1),
            "a fetch refused by its limit consumed nothing: the first fetch to succeed gives the first page"
        );
        early.close().unwrap();
        let explained = session
            .open_cursor(&format!("EXPLAIN {sql}"), &q)
            .err()
            .unwrap();
        assert!(
            matches!(explained, Error::CursorInvalidStatement),
            "{explained:?}"
        );
        let words = "only a SELECT reads through a cursor; run this statement with execute";
        assert_eq!(explained.to_string(), words);
        drop(session.open_cursor(sql, &q).unwrap());
        support::run(&db, "CREATE TABLE s (i INT PRIMARY KEY,c UUID CONTEXT_ID)");
        let p = params! {"c" => support::context(1)};
        db.execute("INSERT INTO s VALUES (1, $c), (2, NULL)", &p)
            .unwrap();
        let scoped = support::in_contexts(&db, &[1]);
        let scoped = scoped.read_session(limits.clone()).unwrap();
        for removed in [0, 2] {
            support::run(&db, &format!("DELETE FROM s WHERE i = {removed}"));
            let mut cursor = scoped.open_cursor("SELECT i FROM s", &p).unwrap();
            assert_eq!(cursor.fetch(None).unwrap().rows(), [vec![Value::Int64(1)]]);
            assert!(cursor.has_more());
            assert!(cursor.fetch(None).unwrap().rows().is_empty());
            assert!(!cursor.has_more());
            assert_eq!(cursor.fetch(None).unwrap().peak_bytes(), None);
        }
        let mut memory = support::reader_options(Some(16 << 20)).limits;
        memory.cursor_page_rows = 1;
        let bounded = support::read_session(&db, &folder, file, memory).unwrap();
        // The whole answer is built by the first fetch, which is where it is refused.
        let whole = bounded
            .open_cursor("SELECT DISTINCT id, $body FROM t", &q)
            .and_then(|mut cursor| cursor.fetch(None));
        assert!(matches!(whole, Err(Error::MemoryBudgetExceeded { .. })));
        assert!(support::listing(&folder.join("readers")).is_empty());
        for table in ["a", "b", "c"] {
            support::run(&db, &format!("CREATE TABLE {table} (v VECTOR(2))"));
        }
        limits.tables = Some(["a".into(), "c".into()].into());
        (limits.result_rows, limits.cursor_page_rows) = (1, 1);
        let only = support::read_session(&db, &folder, file, limits).unwrap();
        for sql in [
            "SHOW VECTOR_INDEXES",
            "SHOW VECTOR_PARTITIONS",
            "SHOW CONTENT_SIZE",
            "SHOW MEMORY_LIMIT",
            "SHOW STORAGE_COMPACTION",
        ] {
            let expected = support::run(&db, sql);
            let answer = support::run(&bounded, sql);
            assert_eq!(answer.columns(), expected.columns(), "{sql}");
            if sql != "SHOW MEMORY_LIMIT" {
                assert_eq!(answer.rows(), expected.rows(), "{sql}");
            }
            let (columns, expected_rows) = (answer.columns().to_vec(), answer.rows().to_vec());
            // The ordinary result must not change the memory reported by the cursor.
            drop(answer);
            let mut cursor = bounded.open_cursor(sql, &params! {}).unwrap();
            let mut rows = Vec::new();
            while cursor.has_more() {
                let page = cursor.fetch(None).unwrap();
                assert_eq!(page.columns(), columns, "{sql}");
                assert!(page.rows().len() <= 1, "{sql}");
                rows.extend_from_slice(page.rows());
            }
            if sql == "SHOW MEMORY_LIMIT" {
                // The cursor itself is charged while it reports memory, so only the limit is shared.
                let limit = columns.iter().position(|name| name == "limit").unwrap();
                assert_eq!(rows[0][limit], expected_rows[0][limit], "{sql}");
            } else {
                assert_eq!(rows, expected_rows, "{sql}");
            }
        }
        let refused = only.execute("SHOW VECTOR_PARTITIONS", &q).unwrap_err();
        assert!(refused.to_string().contains("result_rows"), "{refused}");
        let mut show = only.open_cursor("SHOW VECTOR_INDEXES", &q).unwrap();
        let page = show.fetch(None).unwrap();
        assert_eq!((page.rows().len(), &page.rows()[0][0]), (1, &"a".into()));
        let page = show.fetch(None).unwrap();
        assert_eq!((page.rows().len(), &page.rows()[0][0]), (1, &"c".into()));
    }
}
/// A page is charged to the reader while it is held, and only its own charge goes back when it
/// is dropped: the cursor keeps no share of a page it gave away. The value in each row is large,
/// so that a page's payload is far above anything else a page carries.
#[test]
fn a_cursors_pages_carry_their_own_charge() {
    const BODY: usize = 64 << 10;
    for file in [false, true] {
        let folder = support::folder(&format!("cursor-page-charge-{file}"));
        let db = support::open(&folder);
        support::declare(&db);
        support::insert_together(&db, 1..=6);
        let q = params! {"body" => support::letters(1, BODY)};
        db.execute("UPDATE t SET name = $body", &q).unwrap();
        let mut limits = support::reader_options(None).limits;
        limits.cursor_page_rows = 1;
        let session = support::read_session(&db, &folder, file, limits).unwrap();
        for (whole, sql) in [
            (false, "SELECT id, name FROM t"),
            (true, "SELECT id, name FROM t ORDER BY name"),
        ] {
            let who = format!("{sql} (file session: {file})");
            let mut warm = session.open_cursor(sql, &params! {}).unwrap();
            drop(warm.fetch(None).unwrap());
            drop(warm);
            let used = || support::memory_used(&session);
            let base = used();
            let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
            assert!(
                used() - base < BODY,
                "{who}: opening a cursor produced a page; the first fetch is the first page"
            );
            let mut pages: Vec<_> = (0..3).map(|_| Some(cursor.fetch(None).unwrap())).collect();
            let mut give_back = |at: usize| {
                let before = used();
                drop(pages[at].take());
                before - used()
            };
            let middle = give_back(1);
            assert!(
                middle >= BODY,
                "{who}: dropping a page gave back {middle} bytes, less than the {BODY} it held"
            );
            if whole {
                assert!(
                    used() - base >= 5 * BODY,
                    "{who}: the five rows still held (two in pages, three not yet fetched) were \
                     uncharged when one page was drained and dropped"
                );
                cursor.close().unwrap();
                assert!(
                    used() - base >= 2 * BODY,
                    "{who}: the pages still held were uncharged when their cursor closed"
                );
                for at in [0, 2] {
                    assert!(
                        give_back(at) >= BODY,
                        "{who}: page {at} gave back less than it held"
                    );
                }
            } else {
                for at in [0, 2] {
                    assert_eq!(
                        give_back(at),
                        middle,
                        "{who}: pages of equal rows gave back different charges, so the cursor \
                         kept part of a page's charge after handing the page out"
                    );
                }
            }
            drop(cursor);
            assert_eq!(
                used(),
                base,
                "{who}: the cursor and its pages left memory charged"
            );
        }
    }
}

/// A paged read in an order with ties, and in an order by long texts (Context Graph walks every
/// event of a connector this way): its pages together are the unpaged read, and each page costs
/// its own rows and at most one run of tied keys, never the answer built at its first page.
#[test]
fn pages_in_an_order_with_ties_cost_their_rows_and_one_run() {
    let db = support::open(&support::folder("cursor-tied-pages"));
    let ddl = "CREATE TABLE p (id INTEGER PRIMARY KEY, g INTEGER, n TEXT); CREATE INDEX pg ON p (g); CREATE INDEX pn ON p (n)";
    ddl.split("; ").for_each(|sql| drop(support::run(&db, sql)));
    for id in 1..=120_i64 {
        // Ten rows share each g; four share each 3,000-byte text, which an index cuts.
        let row = params! {"id" => id, "g" => id / 10, "n" => support::letters(id / 4, 3000)};
        db.execute("INSERT INTO p VALUES ($id, $g, $n)", &row)
            .unwrap();
    }
    let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
    for (sql, run) in [
        ("SELECT id FROM p ORDER BY g, id", 10),
        ("SELECT id FROM p ORDER BY n, id", 4),
    ] {
        let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
        let mut paged = Vec::new();
        while cursor.has_more() {
            let page = cursor.fetch(NonZeroUsize::new(7)).unwrap();
            let rows = page.rows().len() as u64;
            let examined = page.rows_examined();
            assert!(
                examined <= rows + run,
                "{sql}: {rows} rows examined {examined}: one run is {run}"
            );
            paged.extend(page.rows().to_vec());
        }
        assert_eq!(
            paged,
            support::rows(&db, sql),
            "{sql}: the pages are the unpaged read"
        );
    }
}

/// A paged read backward in the rows of one indexed value is the forward read reversed, and each
/// page costs its own rows: the rows an index holds under one value are walked from their far end,
/// never the whole value's run nor the table.
#[test]
fn pages_of_one_indexed_value_read_backward_cost_their_rows() {
    let db = support::open(&support::folder("cursor-backward-prefix"));
    support::run(
        &db,
        "CREATE TABLE p (id INTEGER PRIMARY KEY, g INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX pg ON p (g)");
    let values: Vec<String> = (1..=120).map(|id| format!("({id}, {})", id / 10)).collect();
    support::run(
        &db,
        &format!("INSERT INTO p (id, g) VALUES {}", values.join(", ")),
    );
    let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
    let sql = "SELECT id FROM p WHERE g = 5 ORDER BY id DESC";
    let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
    let mut paged = Vec::new();
    while cursor.has_more() {
        let page = cursor.fetch(NonZeroUsize::new(3)).unwrap();
        assert!(
            page.rows_examined() <= page.rows().len() as u64,
            "a page of {} rows examined {}",
            page.rows().len(),
            page.rows_examined()
        );
        paged.extend(page.rows().to_vec());
    }
    assert_eq!(
        paged,
        support::rows(&db, sql),
        "the pages are the unpaged read"
    );
    assert_eq!(paged.len(), 10, "the ten rows of g = 5");
    assert_eq!(
        paged.first(),
        Some(&vec![contextdb::Value::Int64(59)]),
        "the far end first"
    );
}

/// Every charge a page takes is given back (rule 4): a long paged read in an order with ties
/// builds a run of tied rows at each page, and none of those runs stays charged on the cursor
/// until it closes. Under a limit that holds one run and a page with room to spare, but not
/// the sum over 2,000 pages, every page answers and the whole read is the read without a limit.
#[test]
fn a_long_paged_read_in_an_order_with_ties_does_not_creep_toward_the_memory_limit() {
    const VALUES: i64 = 2000;
    let mut options = contextdb::DatabaseOpenOptions::default();
    options.memory_limit = Some(256 << 20);
    let db = support::open_with(&support::folder("cursor-creep"), options).unwrap();
    support::run(
        &db,
        "CREATE TABLE p (id INTEGER PRIMARY KEY, g INTEGER NOT NULL, v INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX pg ON p (g)");
    for from in (0..VALUES).step_by(200) {
        let rows: Vec<String> = (from * 10..(from + 200) * 10)
            .map(|id| format!("({id}, {}, {})", id / 10, -id))
            .collect();
        support::run(
            &db,
            &format!("INSERT INTO p (id, g, v) VALUES {}", rows.join(", ")),
        );
    }
    let sql = "SELECT id, g FROM p ORDER BY g, v";
    let whole = support::rows(&db, sql);
    assert_eq!(whole.len() as i64, VALUES * 10);
    let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
    let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
    let mut paged = cursor.fetch(NonZeroUsize::new(7)).unwrap().rows().to_vec();
    // Room for one run, one page and the cache's moves, a small fraction of what 2,000 pages
    // would hold if each kept even a kilobyte: the sum over the pages does not fit.
    let room = 1 << 20;
    let first = support::memory_used(&session);
    db.set_memory_limit(Some(support::used(&db) + room))
        .unwrap();
    let mut pages = 1_usize;
    while cursor.has_more() {
        pages += 1;
        if pages.is_multiple_of(500) {
            let grown = support::memory_used(&session).saturating_sub(first);
            assert!(
                grown < 64 << 10,
                "after {pages} pages the cursor holds {grown} bytes more than after the first: a page's charge was not given back"
            );
        }
        let page = cursor
            .fetch(NonZeroUsize::new(7))
            .expect("every page answers: what a page charged was given back");
        paged.extend(page.rows().to_vec());
    }
    assert_eq!(paged, whole, "the pages are the read without a limit");
}
#[test]
fn a_default_cursor_gives_a_hundred_rows_a_page_and_leaves_nothing() {
    let folder = support::folder("cursor-default-page");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=101);
    let s = contextdb::ReadSession::open(&folder, Default::default()).unwrap();
    let (sql, q) = ("SELECT id FROM t", params! {});
    let idle = (drop(s.open_cursor(sql, &q)), support::memory_used(&s)).1;
    let mut c = s.open_cursor(sql, &q).unwrap();
    let pages = [(); 3].map(|()| c.fetch(None).unwrap().rows().len());
    assert_eq!(pages, [100, 1, 0], "default page, then empty");
    assert!(!c.has_more() && c.close().is_ok() && c.close().is_ok());
    assert!(support::listing(&folder.join("readers")).is_empty());
    assert_eq!(support::memory_used(&s), idle, "a closed cursor gave back");
}
