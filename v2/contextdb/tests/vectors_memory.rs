//! Memory and vectors: a search under a memory limit ends with the whole answer or with a
//! refusal by name, never with fewer rows; a reader with a small limit searches a store whose
//! blocks are several times its page cache; the page cache an open sets aside follows the
//! limit in force; a limit the session's page cache does not fit under is refused; and a
//! maintenance cycle under a tight limit seals what fits and is refused only when one
//! partition's working set does not.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::{Database, DatabaseOpenOptions, Error, ReadLimits, ReadSession, Value, params};

use support::{Call, Run};

const MIB: usize = 1 << 20;
const KIB: usize = 1 << 10;

#[test]
fn vector_search_ends_complete_or_refused() {
    const DIMENSION: usize = 64;
    let folder = support::folder("search-complete-or-refused");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "");
    // 3,600 rows: 3,000 sealed by a cycle, and of the last 600 those their commit did not
    // seal into the tail block (K1) on the pending list.
    for first in (1..=3600_i64).step_by(600) {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, first..first + 600, DIMENSION, |_| "a".to_owned());
        tx.commit().expect("commit");
        // Sealed once row 3,000 is in.
        if first == 2401 {
            assert_eq!(support::seal_all(&db), 3000);
        }
    }
    let stored = support::stored(&db, support::DOCS);
    let query = support::vector(96_000, DIMENSION);
    // Two searches: ten rows of the half of the table that passes a filter, and two thousand
    // rows of all of it.
    let half = |row: &[Value]| matches!(row[2], Value::Int64(tag) if tag < 25);
    let searches = [
        (
            support::nearest_sql("docs", "v", Some("tag < 25"), 10),
            stored.nearest(half, &query, 10),
        ),
        (
            support::nearest_sql("docs", "v", None, 2000),
            stored.nearest(|_| true, &query, 2000),
        ),
        (
            support::by_mode(None, "EXACT", 2000),
            stored.nearest(|_| true, &query, 2000),
        ),
    ];
    assert_eq!((searches[0].1.len(), searches[1].1.len()), (10, 2000));
    let every = stored.nearest(|_| true, &query, 5000);
    assert_eq!(every.len(), 3600);
    let pending = |db: &Database| support::whole(&support::the_partition(db), "pending_tuples");
    let unindexed = pending(&db);
    assert!(
        0 < unindexed && unindexed <= 600,
        "{unindexed} of the last 600 wait"
    );
    support::close(db);

    // Runs both searches and says which were answered. A search that is answered returns
    // every row of the reference; one that is not is refused by the memory limit, by name.
    // Either way the memory in use afterwards is what it was before, with the one line the
    // statement record adds for the form, once, if it had room when the statement ended: the
    // limit less what the statement still held, its answer if it gave one, is at least the
    // line's charge, and its text was read. Gives the answers and the bytes the record's lines took.
    let outcomes = |on: &dyn Run, state: &str, limit: usize| -> ([bool; 3], usize) {
        let mut kept = 0;
        let answered = [0, 1, 2].map(|number| {
            let (sql, wanted) = &searches[number];
            let before = support::memory_used(&on);
            let (answered, held, parsed) = match support::search(&on, sql, &query) {
                Ok(result) => {
                    let found = support::found(&result, sql);
                    support::assert_same_rows(&found, wanted, &format!("{state}: {sql}"));
                    (true, support::memory_used(&on), true)
                }
                // A statement refused for room to read its text has no form to keep a line for:
                // that refusal finds the whole of the limit free; planning's copy finds the text held (M5).
                Err(Error::MemoryBudgetExceeded {
                    available_bytes, ..
                }) => (false, before, available_bytes as usize != limit - before),
                Err(other) => panic!("{state}: {sql}\nwas refused as {other:?}"),
            };
            // The form is the text with each number a `?`.
            let line = support::form_charge(&support::form_of(sql));
            let room = limit.saturating_sub(held) >= line;
            let line = if parsed && room { line } else { 0 };
            kept += line;
            assert_eq!(
                support::memory_used(&on),
                before + line,
                "{state}: memory in use after {sql}: the line of its form is kept if there was room"
            );
            answered
        });
        (answered, kept)
    };

    // Readers under limits from the smallest that opens a store, where nothing is left for a
    // search, to one with room to spare.
    let opening = support::opening::opening_charge(&folder, None);
    let observation = support::statement_text_bytes("SHOW MEMORY_LIMIT");
    // The statement is charged what its text needs on this store's reader, which the public
    // refusal reports; the smallest limit leaves exactly that, and nothing for a search.
    let mut smallest = support::opening::reader_ready_limit(&folder, observation);
    loop {
        let reader = support::open_reader(&folder, Some(smallest));
        drop(reader.table_names().expect("admit the reader catalog"));
        match reader.execute("SHOW MEMORY_LIMIT", &params! {}) {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes,
                available_bytes,
                ..
            }) => {
                smallest += (requested_bytes - available_bytes) as usize;
            }
            _ => break,
        }
    }
    let limits = [
        smallest,
        smallest + 4 * KIB,
        smallest + 16 * KIB,
        opening + 64 * KIB,
        opening + 256 * KIB,
        opening + MIB,
        13 * MIB,
        64 * MIB,
    ];
    for limit in limits {
        let reader = support::open_reader(&folder, Some(limit));
        drop(reader.table_names().expect("admit the reader catalog"));
        let (answered, _) = outcomes(&reader, &format!("a reader under {limit} bytes"), limit);
        if limit == smallest {
            assert_eq!(
                answered, [false; 3],
                "searches under a limit with nothing to spare"
            );
        }
        if limit == 64 * MIB {
            assert_eq!(answered, [true; 3], "searches under a limit of 64 MiB");
        }
    }
    // The same through the writer.
    let db = support::open_limited(&folder, opening + observation);
    let (answers, kept) = outcomes(
        &db,
        "the writer under the smallest limit",
        opening + observation,
    );
    assert_eq!(answers, [false; 3]);
    assert_eq!(support::used(&db), opening + kept);
    support::close(db);
    let db = support::open_limited(&folder, 64 * MIB);
    let (answers, kept) = outcomes(&db, "the writer under 64 MiB", 64 * MIB);
    assert_eq!(answers, [true; 3]);
    assert_eq!(
        support::used(&db),
        support::opening::opening_charge(&folder, Some(64 * MIB)) + kept
    );
    support::close(db);

    // A cycle under a limit too small for it is refused by name, and nothing was sealed.
    let db = support::open_limited(&folder, opening + 64 * KIB);
    let before = support::used(&db);
    match db.run_maintenance_cycle() {
        Err(Error::MemoryBudgetExceeded { .. }) => {}
        other => panic!("a cycle with 64 KiB to work in got {other:?}"),
    }
    assert_eq!(
        support::used(&db),
        before,
        "memory in use after the refused cycle"
    );
    support::close(db);
    // Every vector is still found, those that waited still wait, and the next cycle under no
    // limit seals what the commits left, so nothing is pending.
    let db = support::open(&folder);
    assert_eq!(pending(&db), unindexed, "pending after the refused cycle");
    let sql = support::nearest_sql("docs", "v", None, 5000);
    let result = support::assert_nearest(&db, &sql, &query, &every, "after the refused cycle");
    assert!(
        result.rows_examined() >= unindexed as u64 + 3600,
        "{unindexed} pending rows and 3,600 returned rows were read as {} rows",
        result.rows_examined()
    );
    drop(result);
    let report = support::cycle(&db);
    let done = support::count(report.maintenance_vectors_done);
    assert_eq!(done, unindexed as u64, "the cycle sealed what waited");
    assert_eq!(pending(&db), 0, "vectors left pending after the cycle");
    support::assert_nearest(&db, &sql, &query, &every, "after the cycle that sealed");
}

#[test]
fn memory_stays_bounded_as_data_grows_vector() {
    const DIMENSION: usize = 768;
    // Vectors in the large partition of a plain column. In a block each takes 8 + 768 bytes of
    // slot, 4 of key end and 9 of key, so these are 10.5 MB of blocks: 1.25 times the page
    // cache of 8 MiB that the reader's limit gives it, and the fewest that are.
    const LARGE: i64 = 13_290;
    const SMALL: i64 = 200;
    const LIMIT: usize = 16 * MIB;
    const CACHE: usize = 8 * MIB;
    const BLOCK: u64 = 262_080;
    const { assert!(LARGE as u64 * (8 + 768 + 4 + 9) * 4 > 5 * CACHE as u64) };
    const { assert!((LARGE as u64 - 1) * (8 + 768 + 4 + 9) * 4 <= 5 * CACHE as u64) };

    let folder = support::folder("memory-bounded-vectors");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    for first in (1..=LARGE + SMALL).step_by(1000) {
        let tx = db.transaction().expect("begin");
        let last = (first + 999).min(LARGE + SMALL);
        support::insert_docs(&tx, first..=last, DIMENSION, |n| {
            String::from(if n <= LARGE { "large" } else { "small" })
        });
        tx.commit().expect("commit");
    }
    // Each commit seals its whole blocks, and the cycles what the commits left (K1).
    support::seal_all(&db);
    let partitions = support::shown(&db, "SHOW VECTOR_PARTITIONS FOR docs.v");
    let left = |row: &support::Shown| support::whole(row, "pending_tuples");
    assert!(
        partitions.iter().all(|row| left(row) == 0),
        "vectors left unsealed"
    );
    // The reference for the small partition, from its rows alone.
    let small = format!("SELECT id, ctx, tag, v FROM docs WHERE id > {LARGE}");
    let stored = support::stored(&db, &small);
    assert_eq!(stored.rows().len() as i64, SMALL);
    let query = support::vector(97_000, DIMENSION);
    support::close(db);

    // A reader with a limit of 16 MiB, whose storage door counts the bytes it reads.
    let read = Arc::new(AtomicU64::new(0));
    let mut options = support::reader_options(Some(LIMIT));
    options.storage = Some(support::storage({
        let read = read.clone();
        move |call| {
            if let Call::Read(_, bytes) = call {
                read.fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
        }
    }));
    let reader = ReadSession::open_with_options(&folder, options)
        .expect("open a store larger than the limit");
    drop(reader.table_names().expect("admit the reader catalog"));
    // Opening reads no block: less than one block's bytes in all.
    let at_open = read.load(Ordering::SeqCst);
    assert!(
        at_open < BLOCK,
        "opening the store read {at_open} bytes of it before any statement"
    );
    assert_eq!(
        support::memory_used(&reader),
        support::opening::reader_charge(&folder, None),
        "set aside by an open under 16 MiB"
    );

    // It searches the small partition and answers, reading a small part of the file.
    for k in [10, 1000] {
        let wanted = stored.nearest(|row| row[1] == support::text("small"), &query, k);
        let sql = support::nearest_sql("docs", "v", Some("ctx = 'small'"), k);
        support::assert_nearest(&reader, &sql, &query, &wanted, "the small partition");
        assert_eq!(
            support::memory_used(&reader),
            support::opening::reader_charge(&folder, None)
                + support::form_charge(
                    "SELECT id, v <=> $q AS distance FROM docs WHERE ctx = ? ORDER BY distance LIMIT ?"
                ),
            "memory in use once the answer is dropped: the open's and the record's line"
        );
    }
    let searching = read.load(Ordering::SeqCst) - at_open;
    assert!(
        searching < 8 * MIB as u64,
        "two searches of a partition of {SMALL} vectors read {searching} bytes of the file"
    );
    // The row count of the whole table is still one statement away.
    assert_eq!(
        support::int(&reader, "SELECT COUNT(*) FROM docs"),
        LARGE + SMALL
    );
}

#[test]
fn the_page_cache_follows_the_limit_in_force() {
    let folder = support::folder("page-cache-follows-limit");
    // An undeclared limit uses the writer default and sets aside a 1 MiB allowance,
    // an 8 MiB page cache and 1 MiB for kept plans, leaving any larger limit settable.
    let db = support::open(&folder);
    support::assert_default_limit(support::limit(&db), "a fresh store");
    assert_eq!(
        support::used(&db),
        support::opening::opening_charge(&folder, None),
        "set aside with no limit declared"
    );
    assert_eq!(
        support::memory_used(&db),
        support::opening::opening_charge(&folder, None)
    );
    support::declare(&db);
    support::insert(&db, 1..=10);
    support::close(db);

    // The cache is max(limit/4, 8 MiB); plans take limit/32, clamped between 1 and 8 MiB.
    let floor = support::opening::opening_charge(&folder, None);
    for limit in [64 * MIB, 16 * MIB, floor, 32 * MIB, 40 * MIB, 1024 * MIB] {
        let db = support::open_limited(&folder, limit);
        assert_eq!(support::limit(&db), Some(limit));
        assert_eq!(
            support::used(&db),
            support::opening::set_aside(Some(limit))
                + support::opening::opening_charge(&folder, None)
                - support::opening::open_floor(),
            "set aside under a limit of {limit} bytes"
        );
        // Under the smallest limit nothing is left for a statement; under the others it reads.
        if limit > floor {
            support::assert_rows_are(&db, 1..=10);
        }
        support::close(db);
    }

    // A reader's cache follows its own limit; no declared limit uses the 16 MiB default.
    for limit in [None, Some(64 * MIB), Some(16 * MIB), Some(1024 * MIB)] {
        let reader = match limit {
            Some(limit) => support::open_reader(&folder, Some(limit)),
            None => ReadSession::open(&folder, ReadLimits::default()).expect("default reader"),
        };
        drop(reader.table_names().expect("admit the reader catalog"));
        assert_eq!(
            support::memory_used(&reader),
            support::opening::set_aside(limit) + support::opening::reader_charge(&folder, None)
                - support::opening::open_floor(),
            "a reader under {limit:?} bytes"
        );
        support::assert_rows_are(&reader, 1..=10);
    }

    // A limit the store remembers sizes the cache of the next open that names none.
    let db = support::open(&folder);
    support::run(&db, "SET MEMORY_LIMIT 512M");
    assert_eq!(support::limit(&db), Some(512 * MIB));
    assert_eq!(
        support::used(&db),
        support::opening::opening_charge(&folder, None),
        "the cache of an open keeps its size"
    );
    let db = support::reopen(db, &folder);
    let under_512 = support::opening::set_aside(Some(512 * MIB))
        + support::opening::opening_charge(&folder, None)
        - support::opening::open_floor();
    assert_eq!(support::limit(&db), Some(512 * MIB));
    assert_eq!(
        support::used(&db),
        under_512,
        "set aside under a remembered limit of 512 MiB"
    );
    assert_eq!(support::memory_used(&db), under_512);
    assert_eq!(
        support::memory_shown(&db, "limit"),
        Value::Int64(512 * MIB as i64)
    );
    assert_eq!(
        support::memory_shown(&db, "available"),
        Value::Int64((512 * MIB - under_512) as i64)
    );
    // A reader does not take the limit the store remembers.
    let reader = ReadSession::open(&folder, ReadLimits::default()).expect("default reader");
    drop(reader.table_names().expect("admit the reader catalog"));
    assert_eq!(
        support::memory_shown(&reader, "limit"),
        Value::Int64(16 * MIB as i64)
    );
    assert_eq!(
        support::memory_used(&reader),
        support::opening::reader_charge(&folder, None)
    );
    let reader = support::open_reader(&folder, Some(16 * MIB));
    drop(reader.table_names().expect("admit the reader catalog"));
    assert_eq!(
        support::memory_used(&reader),
        support::opening::reader_charge(&folder, None)
    );
    assert_eq!(
        support::memory_shown(&reader, "available"),
        Value::Int64((16 * MIB - support::opening::reader_charge(&folder, None)) as i64)
    );
    // An open that names a limit has its own cache and leaves what the store remembers.
    support::close(db);
    let db = support::open_limited(&folder, 64 * MIB);
    assert_eq!(
        support::used(&db),
        support::opening::set_aside(Some(64 * MIB))
            + support::opening::opening_charge(&folder, None)
            - support::opening::open_floor()
    );
    let db = support::reopen(db, &folder);
    assert_eq!(support::limit(&db), Some(512 * MIB));
    assert_eq!(support::used(&db), under_512);
}

/// Asserts that a limit was refused because the session's page cache does not fit under it:
/// by the memory limit's name, for the operation `set_limit`, in a sentence that names the
/// limit asked for and the smallest limit the session can take, both in bytes, and says to
/// open the store with the limit as an open option.
fn refused_below_the_cache(refused: contextdb::Result<()>, asked: usize, smallest: usize) {
    let error = match refused {
        Err(error) => error,
        Ok(()) => panic!("a limit of {asked} bytes was taken; the smallest is {smallest}"),
    };
    let sentence = error.to_string();
    match error {
        Error::MemoryBudgetExceeded { operation, .. } => {
            assert_eq!(operation, "set_limit", "{sentence}");
        }
        other => panic!("a limit of {asked} bytes was refused as {other:?}"),
    }
    assert!(
        sentence.contains(&asked.to_string()),
        "the limit asked for: {sentence}"
    );
    assert!(
        sentence.contains(&smallest.to_string()),
        "the smallest limit: {sentence}"
    );
    assert!(
        sentence.to_lowercase().contains("open the store"),
        "the next action, to open the store with the limit as an open option: {sentence}"
    );
}

#[test]
fn a_limit_below_the_sessions_cache_is_refused_and_one_above_it_is_taken() {
    type Set = fn(&Database, usize) -> contextdb::Result<()>;
    let in_sql: Set = |db, mib| {
        db.execute(&format!("SET MEMORY_LIMIT '{mib}M'"), &params! {})
            .map(|_| ())
    };
    let by_call: Set = |db, mib| db.set_memory_limit(Some(mib * MIB));

    for (name, set) in [("sql", in_sql), ("call", by_call)] {
        // Opened with no limit: the smallest limit is set aside.
        let folder = support::folder(&format!("limit-below-cache-{name}"));
        let db = support::open(&folder);
        assert_eq!(support::used(&db), support::opening::open_floor());
        refused_below_the_cache(set(&db, 8), 8 * MIB, support::opening::open_floor());
        refused_below_the_cache(set(&db, 1), MIB, support::opening::open_floor());
        // The limit in force, the memory set aside and the limit the store remembers stay as
        // they were.
        support::assert_default_limit(support::limit(&db), "after a refused limit");
        let shown = support::limit(&db).map(|limit| Value::Int64(limit as i64));
        assert_eq!(Some(support::memory_shown(&db, "limit")), shown);
        assert_eq!(support::used(&db), support::opening::open_floor());
        let db = support::reopen(db, &folder);
        support::assert_default_limit(support::limit(&db), "a refused limit was remembered");
        assert_eq!(support::used(&db), support::opening::open_floor());

        // A limit above it is taken and shown, with the same still set aside; the next open
        // sizes its cache from it.
        set(&db, 512).expect("a limit above what is set aside");
        assert_eq!(support::limit(&db), Some(512 * MIB));
        assert_eq!(
            support::memory_shown(&db, "limit"),
            Value::Int64(512 * MIB as i64)
        );
        assert_eq!(support::used(&db), support::opening::open_floor());
        assert_eq!(support::memory_used(&db), support::opening::open_floor());
        let db = support::reopen(db, &folder);
        let under_512 = support::opening::set_aside(Some(512 * MIB));
        assert_eq!(support::limit(&db), Some(512 * MIB));
        assert_eq!(support::used(&db), under_512);
        // Now what that open set aside is the smallest.
        refused_below_the_cache(set(&db, 128), 128 * MIB, under_512);
        assert_eq!(support::limit(&db), Some(512 * MIB));
        assert_eq!(support::used(&db), under_512);
        let just_above = (under_512 + support::limit_commit_bytes()).div_ceil(MIB);
        set(&db, just_above).expect("a limit just above what is set aside");
        assert_eq!(support::limit(&db), Some(just_above * MIB));
        assert_eq!(support::used(&db), under_512);
        support::close(db);

        // Opened with options that name 16 MiB: the smallest limit is set aside. 8 MiB is
        // refused and 1 GiB is taken, and the next open with no options is under 1 GiB.
        let folder = support::folder(&format!("limit-below-small-cache-{name}"));
        let db = support::open_limited(&folder, 16 * MIB);
        assert_eq!(support::used(&db), support::opening::open_floor());
        refused_below_the_cache(set(&db, 8), 8 * MIB, support::opening::open_floor());
        assert_eq!(support::limit(&db), Some(16 * MIB));
        assert_eq!(support::used(&db), support::opening::open_floor());
        support::close(db);
        support::assert_default_limit(
            support::limit(&support::open(&folder)),
            "a refused limit was remembered",
        );
        let db = support::open_limited(&folder, 16 * MIB);
        // An opening limit is that session's limit "and no more" (README:1453-1455).
        assert!(set(&db, 1024).is_err(), "a limit above the opening option");
        assert_eq!(support::limit(&db), Some(16 * MIB));
        support::close(db);
        let db = support::open(&folder);
        set(&db, 1024).expect("a limit above what is set aside");
        assert_eq!(support::limit(&db), Some(1024 * MIB));
        assert_eq!(support::used(&db), support::opening::open_floor());
        let db = support::reopen(db, &folder);
        assert_eq!(support::limit(&db), Some(1024 * MIB));
        let under_1_gib = support::opening::set_aside(Some(1024 * MIB));
        assert_eq!(support::used(&db), under_1_gib);
        // Under the 1 GiB the store remembers the cache is 256 MiB, and 64 MiB is refused.
        refused_below_the_cache(set(&db, 64), 64 * MIB, under_1_gib);
        assert_eq!(support::limit(&db), Some(1024 * MIB));
        support::close(db);
    }

    // An open under 1 MiB, for writing and for reading, is refused by the ordinary memory
    // refusal with the bytes it needed and the limit; it leaves the store as it was.
    let folder = support::folder("open-below-1-mib");
    support::close(support::open(&folder));
    let (mut write, mut read) = (
        DatabaseOpenOptions::default(),
        support::reader_options(None),
    );
    (write.memory_limit, read.limits.memory) = (Some(MIB), MIB as u64);
    let refusals = [
        ("writing", support::open_with(&folder, write).err()),
        (
            "reading",
            ReadSession::open_with_options(&folder, read).err(),
        ),
    ];
    for (how, refusal) in refusals {
        let Some(Error::MemoryBudgetExceeded {
            requested_bytes,
            budget_limit_bytes,
            ..
        }) = refusal
        else {
            panic!("an open for {how} under 1 MiB was not a memory refusal: {refusal:?}");
        };
        assert!(
            budget_limit_bytes == MIB as u64 && requested_bytes > MIB as u64,
            "an open for {how}: the refusal names the limit ({budget_limit_bytes}) and the \
             bytes the session needs ({requested_bytes}), which are more than the limit"
        );
    }
    support::close(support::open(&folder));
}

#[test]
fn a_limit_of_64_mib_is_taken_in_a_store_opened_with_no_limit() {
    type Set = fn(&Database, usize) -> contextdb::Result<()>;
    let in_sql: Set = |db, mib| {
        db.execute(&format!("SET MEMORY_LIMIT {mib}M"), &params! {})
            .map(|_| ())
    };
    let by_call: Set = |db, mib| db.set_memory_limit(Some(mib * MIB));
    let insert = "INSERT INTO big (id, body) VALUES ($id, $body)";
    // Refused by the memory limit, which the refusal names as 64 MiB.
    let over_64_mib =
        |refused: contextdb::Result<contextdb::QueryResult>, state: &str| match refused {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                ..
            }) => {
                assert_eq!(budget_limit_bytes, 64 * MIB as u64, "{state}");
                assert!(requested_bytes > available_bytes, "{state}");
            }
            Err(other) => panic!("{state}: refused as {other:?}"),
            Ok(_) => panic!("{state}: a value larger than what the limit leaves was taken"),
        };

    for (name, set) in [("sql", in_sql), ("call", by_call)] {
        // Opened the plain way, with nothing remembered: the default, and the smallest limit
        // set aside.
        let folder = support::folder(&format!("limit-64-in-a-plain-store-{name}"));
        let db = support::open(&folder);
        support::assert_default_limit(support::limit(&db), "a fresh store");
        assert_eq!(
            support::used(&db),
            support::opening::opening_charge(&folder, None)
        );
        support::run(&db, "CREATE TABLE big (id INTEGER PRIMARY KEY, body TEXT)");
        let held_before = support::used(&db);

        // The first limit declared is taken and is in force at once, with the cache this
        // session already has.
        set(&db, 64).expect("64 MiB in a session opened with no limit");
        assert_eq!(support::limit(&db), Some(64 * MIB));
        assert_eq!(
            support::memory_shown(&db, "limit"),
            Value::Int64(64 * MIB as i64)
        );
        assert_eq!(support::used(&db), held_before);
        assert_eq!(support::memory_used(&db), held_before);
        assert_eq!(
            support::memory_shown(&db, "available"),
            Value::Int64((64 * MIB - held_before) as i64)
        );

        // It holds at once for what statements take: a value of 56 MiB is more than the
        // 54 MiB the limit leaves and is refused by the memory limit; one of 1 MiB is taken.
        let large = "x".repeat(56 * MIB);
        over_64_mib(
            db.execute(insert, &params! { "id" => 1_i64, "body" => large.as_str() }),
            "56 MiB in the session that declared the limit",
        );
        // The refused statement's form is in the record, once.
        let held_before = held_before + support::form_charge(insert);
        assert_eq!(
            support::used(&db),
            held_before,
            "a refused statement left memory counted"
        );
        let fits = "x".repeat(MIB);
        db.execute(insert, &params! { "id" => 2_i64, "body" => fits.as_str() })
            .expect("a value of 1 MiB under a limit of 64 MiB");
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM big"), 1);

        // The store remembers it: the next plain open is under 64 MiB, with a cache of a
        // quarter of it, 16 MiB, 2 MiB for kept plans and 1 MiB set aside before any statement.
        let db = support::reopen(db, &folder);
        let under_64 = support::opening::set_aside(Some(64 * MIB))
            + support::opening::opening_charge(&folder, None)
            - support::opening::open_floor();
        assert_eq!(support::limit(&db), Some(64 * MIB));
        assert_eq!(support::used(&db), under_64);
        assert_eq!(support::memory_used(&db), under_64);
        assert_eq!(
            support::memory_shown(&db, "limit"),
            Value::Int64(64 * MIB as i64)
        );
        assert_eq!(
            support::memory_shown(&db, "available"),
            Value::Int64((64 * MIB - under_64) as i64)
        );
        over_64_mib(
            db.execute(insert, &params! { "id" => 1_i64, "body" => large.as_str() }),
            "56 MiB after the reopen",
        );
        assert_eq!(support::int(&db, "SELECT COUNT(*) FROM big"), 1);
        support::close(db);

        // The refusal that stays: a session opened under 1 GiB has a cache of 256 MiB, and
        // 64 MiB is refused there with the action, to open the store with the limit.
        let folder = support::folder(&format!("limit-64-under-1-gib-{name}"));
        let db = support::open_limited(&folder, 1024 * MIB);
        let under_1_gib = support::opening::set_aside(Some(1024 * MIB))
            + support::opening::opening_charge(&folder, None)
            - support::opening::open_floor();
        assert_eq!(support::used(&db), under_1_gib);
        refused_below_the_cache(set(&db, 64), 64 * MIB, under_1_gib);
        assert_eq!(support::limit(&db), Some(1024 * MIB));
        assert_eq!(support::used(&db), under_1_gib);
        support::close(db);
        support::assert_default_limit(
            support::limit(&support::open(&folder)),
            "a refused limit was remembered",
        );
    }
}

#[test]
fn a_cycle_under_a_tight_limit_seals_what_fits_and_is_refused_only_when_one_partition_does_not() {
    const DIMENSION: usize = 16;
    // Forty partitions with a hundred pending vectors each: 4,000 in all, fewer than the
    // 4,096 that one cycle with room seals, so only memory cuts a cycle short here.
    const PARTITIONS: i64 = 40;
    const EACH: i64 = 100;
    const ROWS: i64 = PARTITIONS * EACH;
    // The bytes of the one block a cycle builds in, which it reserves whole.
    const BLOCK: usize = 262_080;
    // A limit with room for one partition's working set and not for all forty. One partition
    // takes the block being built, the keys of its 100 pending records, and what it stages:
    // its block, 100 removals and its entry, about 30 KiB together. Sealing all 4,000 at once
    // would stage 4,000 removals at 128 bytes and more each, 512,000 bytes, beside the block
    // being built: twice the 393,216 bytes this limit leaves.
    let tight: usize = support::opening::open_floor() + support::limit_commit_bytes() + 384 * KIB;
    const { assert!(BLOCK + 100 * KIB < 384 * KIB && 384 * KIB < BLOCK + ROWS as usize * 128) };
    // A limit under which no partition's working set fits: it leaves half of one block.
    let short: usize = support::opening::open_floor() + 128 * KIB;
    const { assert!(128 * KIB < BLOCK) };

    let folder = support::folder("sealing-under-a-tight-limit");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    let partition = |n: i64| format!("p{:02}", n % PARTITIONS);
    for first in (1..=ROWS).step_by(1000) {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, first..first + 1000, DIMENSION, partition);
        tx.commit().expect("commit");
    }
    support::close(db);

    // A separate 512 MiB reader finds ten rows over all partitions and over one partition.
    let reader = support::open_reader(&folder, None);
    let query = support::vector(95_000, DIMENSION);
    let searches = [None, Some("ctx = 'p07'")];
    let wanted_now = || {
        let stored = support::stored(&reader, support::DOCS);
        searches.map(|filter| {
            let passes = |row: &[Value]| filter.is_none() || row[1] == support::text("p07");
            let wanted = stored.nearest(passes, &query, 10);
            assert_eq!(wanted.len(), 10);
            wanted
        })
    };
    let searched = |wanted: &[Vec<support::Near>; 2], state: &str| -> [u64; 2] {
        [0, 1].map(|number| {
            let sql = support::nearest_sql("docs", "v", searches[number], 10);
            let result = support::assert_nearest(&reader, &sql, &query, &wanted[number], state);
            result.rows_examined()
        })
    };
    let wanted = wanted_now();
    // Every vector is pending: a search reads each pending row of the partitions it selects
    // once, and each row it returns once more.
    assert_eq!(
        searched(&wanted, "nothing sealed"),
        [ROWS as u64 + 10, EACH as u64 + 10]
    );

    // Under the tight limit every cycle seals at least one partition and none is refused.
    let db = support::open_limited(&folder, tight);
    let set_aside = support::used(&db);
    let mut remaining = ROWS as u64;
    let mut cycles = 0;
    while remaining > 0 {
        cycles += 1;
        assert!(
            cycles <= PARTITIONS,
            "{remaining} vectors still wait after {PARTITIONS} cycles over {PARTITIONS} partitions"
        );
        let report = support::cycle(&db);
        let done = support::count(report.maintenance_vectors_done);
        assert!(
            done >= EACH as u64 && done <= remaining,
            "cycle {cycles} sealed {done} of {remaining} unindexed vectors: at least one \
             partition's {EACH} were to be sealed"
        );
        assert_eq!(
            support::count(report.maintenance_vectors_remaining),
            remaining - done,
            "what cycle {cycles} reports as still unindexed"
        );
        if cycles == 1 {
            assert!(
                done < ROWS as u64,
                "the first cycle sealed all {ROWS} vectors in {} bytes of room",
                tight - support::opening::open_floor()
            );
        }
        assert_eq!(
            support::used(&db),
            set_aside,
            "memory in use after cycle {cycles}"
        );
        remaining -= done;
        // The same rows at the same distances after every cycle.
        searched(
            &wanted,
            &format!("after cycle {cycles} under the tight limit"),
        );
    }
    let position = support::watermark(&db);
    let report = support::cycle(&db);
    assert_eq!(
        (
            support::count(report.maintenance_vectors_done),
            support::count(report.maintenance_vectors_remaining)
        ),
        (0, 0)
    );
    assert_eq!(support::watermark(&db), position);
    support::close(db);

    // Two hundred more vectors come to wait in one partition.
    let db = support::open(&folder);
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, ROWS + 1..=ROWS + 200, DIMENSION, |_| "p07".to_owned());
    tx.commit().expect("commit");
    support::close(db);
    let wanted = wanted_now();
    let read_before = searched(&wanted, "two hundred unindexed");

    // Under a limit that leaves less than the block a cycle builds in, the working set of
    // that one partition does not fit, and the cycle is refused by name. Nothing was staged:
    // no commit was made and nothing is counted.
    let db = support::open_limited(&folder, short);
    let (set_aside, position) = (support::used(&db), support::watermark(&db));
    match db.run_maintenance_cycle() {
        Err(Error::MemoryBudgetExceeded {
            operation,
            budget_limit_bytes,
            ..
        }) => {
            assert_eq!(operation, "maintenance");
            assert_eq!(budget_limit_bytes, short as u64);
        }
        other => panic!("a cycle with half a block of room got {other:?}"),
    }
    assert_eq!(
        support::used(&db),
        set_aside,
        "memory in use after the refusal"
    );
    assert_eq!(
        support::watermark(&db),
        position,
        "a refused cycle committed"
    );
    // The store is unharmed: the same search answers the same and reads the same rows, the
    // two hundred that waited among them.
    assert_eq!(
        searched(&wanted, "after the refused cycle"),
        read_before,
        "the rows the searches read after the refused cycle"
    );

    // Opened under a larger limit the cycle goes through, and all two hundred were still unindexed.
    support::close(db);
    let db = support::open_limited(&folder, 64 * MIB);
    let report = support::cycle(&db);
    assert_eq!(
        (
            support::count(report.maintenance_vectors_done),
            support::count(report.maintenance_vectors_remaining)
        ),
        (200, 0)
    );
    searched(&wanted, "after the cycle under the raised limit");
}

#[test]
fn the_memory_of_a_search_is_given_back_when_its_result_is_dropped() {
    const DIMENSION: usize = 16;
    let folder = support::folder("search-gives-memory-back");
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSION, "PARTITION_KEY (ctx)");
    // Two partitions: 300 rows in blocks, then 100 on pending lists.
    let ctx = |n: i64| format!("c{}", n % 2);
    for (numbers, sealed) in [(1..=300, true), (301..=400, false)] {
        let tx = db.transaction().expect("begin");
        support::insert_docs(&tx, numbers, DIMENSION, ctx);
        tx.commit().expect("commit");
        if sealed {
            assert_eq!(support::seal_all(&db), 300);
        }
    }
    let query = support::vector(96_500, DIMENSION);
    let reader = support::open_reader(&folder, Some(16 * MIB));
    drop(reader.table_names().expect("admit the reader catalog"));
    // A transaction that has staged rows of its own: its searches read those too.
    let tx = db.transaction().expect("begin");
    support::insert_docs(&tx, 401..=420, DIMENSION, ctx);

    // Every partition, one partition, a filter on another column, and more rows than there
    // are; with only the key selected, with the distance, and with every column.
    let searches = [
        support::nearest_sql("docs", "v", None, 10),
        support::nearest_sql("docs", "v", Some("ctx = 'c1'"), 10),
        support::nearest_sql("docs", "v", Some("tag < 25"), 50),
        support::nearest_sql("docs", "v", None, 1000),
        "SELECT id FROM docs ORDER BY v <=> $q LIMIT 10".to_owned(),
        "SELECT * FROM docs ORDER BY v <=> $q LIMIT 100".to_owned(),
    ];
    let doors = [
        (&db as &dyn Run, "the writer"),
        (&tx as &dyn Run, "a transaction"),
        (&reader as &dyn Run, "a reader"),
    ];
    // The writer and its transaction add to one record, the reader to its own; a form's line
    // is added the first time it runs on a record, whatever its numbers.
    let mut lines = std::collections::HashSet::new();
    for (on, door) in doors {
        for sql in &searches {
            let before = support::memory_used(&on);
            let result = match support::search(&on, sql, &query) {
                Ok(result) => result,
                Err(error) => panic!("through {door}: {sql}\nwas refused: {error:?}"),
            };
            assert!(
                result.rows().len() >= 10,
                "through {door}: the rows of {sql}"
            );
            // While the result lives its rows are counted, so the figure does move.
            let held = support::memory_used(&on);
            assert!(
                held > before,
                "through {door}: with the result of {sql}\nin hand, {held} bytes are in use, \
                 and {before} were before it"
            );
            drop(result);
            let form = support::form_of(sql);
            let line = if lines.insert((door == "a reader", form.clone())) {
                support::form_charge(&form)
            } else {
                0
            };
            assert_eq!(
                support::memory_used(&on),
                before + line,
                "through {door}: memory in use once the result of {sql}\nis dropped, with the \
                 record's line for its form if it is the first run of it"
            );
        }
    }
    // What the transaction holds is its own rows, before and after: it commits them.
    tx.commit().expect("commit");
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM docs"), 420);
}

#[test]
fn a_commit_with_no_room_for_its_vector_records_changes_nothing() {
    const DIMENSION: usize = 16;
    let (folder, db) = support::built("commit-no-room", DIMENSION, "INDEXING_THRESHOLD 1000", 2000);
    let rows = "SELECT id, ctx, tag, v FROM docs ORDER BY id";
    let untouched = (support::rows(&db, rows), support::watermark(&db));
    support::close(db);
    let query = support::gathered(96_500, DIMENSION);
    // Every search agrees with the rows that are there: EXACT returns the reference, and
    // INDEXED rows with their true distances.
    let searches_agree = |db: &Database, state: &str| {
        let stored = support::stored(db, support::DOCS);
        let wanted = stored.nearest(|_| true, &query, 10);
        let sql = support::by_mode(None, "EXACT", 10);
        support::assert_nearest(db, &sql, &query, &wanted, state);
        let result = support::answer(db, &support::by_mode(None, "INDEXED", 10), &query);
        support::assert_sound(&result, &stored, |_| true, &query, 10, state);
    };

    // One transaction under each limit, from one with no room at all upward: 300 sealed rows
    // get another vector, which leaves a record for each slot they left and one for each
    // vector that waits, and 300 rows are new.
    let attempt = |db: &Database| -> contextdb::Result<()> {
        let tx = db.transaction()?;
        for id in 1..=300_i64 {
            tx.execute(
                "UPDATE docs SET v = $v WHERE id = $id",
                &params! { "id" => id, "v" => support::vector(96_600 + id, DIMENSION) },
            )?;
            support::insert_doc(
                &tx,
                5000 + id,
                "a",
                Some(support::gathered(5000 + id, DIMENSION)),
            )?;
        }
        tx.commit()
    };
    let mut refused = 0;
    let mut committed = false;
    let spares = (0..=32).map(|step| step * 64 * KIB).chain([55 * MIB]);
    let opening = support::opening::opening_charge(&folder, None);
    let observation = support::statement_text_bytes("SHOW MEMORY_LIMIT");
    for spare in spares {
        let limit = opening + observation + spare;
        let db = support::open_limited(&folder, limit);
        let before = support::memory_used(&db);
        let marked = support::watermark(&db);
        let outcome = attempt(&db);
        // The memory the commit held is given back; what stays is the statement record's line
        // for each form it kept (the attempt's statements run on the writer), each charged for
        // its text and its own bytes.
        let after = support::memory_used(&db);
        let mark = support::watermark(&db);
        // The opening limit is the session's and no more, so the record is read through a read
        // session made from the handle, which shares its record (README:1534) and its own room.
        let reader = db.read_session(ReadLimits::default()).expect("a reader");
        let lines = support::rows(&reader, "SHOW STATEMENT_STATISTICS");
        drop(reader);
        let kept: usize = lines
            .iter()
            .map(|line| match &line[0] {
                Value::Text(form) => support::form_charge(form),
                other => panic!("a statement is text, not {other:?}"),
            })
            .sum();
        assert_eq!(
            after,
            before + kept,
            "memory in use after a commit under {limit}: the record's lines are all that stay"
        );
        let db = support::reopen(db, &folder);
        let committed_here = outcome.is_ok();
        assert!(
            committed_here || mark == marked,
            "the committed position moved under {limit} bytes with the commit refused"
        );
        match outcome {
            Ok(()) => {
                assert_eq!(support::int(&db, "SELECT COUNT(*) FROM docs"), 2300);
                searches_agree(&db, &format!("committed under {limit} bytes"));
                committed = true;
                break;
            }
            Err(Error::MemoryBudgetExceeded { .. }) => {
                let state = format!("refused under {limit} bytes");
                assert!(
                    support::rows(&db, rows) == untouched.0,
                    "{state}: the rows changed"
                );
                searches_agree(&db, &state);
                refused += 1;
            }
            Err(other) => panic!("a commit under {limit} bytes gave {other:?}"),
        }
        support::close(db);
    }
    assert!(
        refused > 0 && committed,
        "{refused} limits refused the commit; committed at the last: {committed}"
    );
}
