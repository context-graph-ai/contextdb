//! A million rows are loaded and scanned by a writer whose limit is 64 MiB, and the process
//! never has more than that allocated on the engine's account.
//!
//! This test has a file of its own because its program counts every allocation. The count is
//! taken in a child, so that it is the count of one store and nothing else.
//!
//! The same count holds a refused read to its word: a value stored compressed whose real size
//! does not fit the reader's limit is refused before its bytes exist. And it holds a sealing
//! cycle to what it reserves: under a limit with exactly the room the cycle's reservations
//! come to, the cycle completes and the process stays within the limit. The cycle is measured
//! with the page cache already full: the limit has room for the whole cache, and a cache that
//! is half empty would hide whatever the cycle held without reserving it.

#[path = "support/streaming.rs"]
mod streaming;
mod support;
use support::count;

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use contextdb::{Database, DatabaseOpenOptions, Error, ReadSession, Uuid, Value, params};

const LIMIT: usize = 64 << 20;
const ROWS: i64 = 1_000_000;
const ROWS_IN_A_COMMIT: i64 = 10_000;

/// The most pages one UPDATE of one small column may write: the pages from the root to the row
/// and to its two index entries, the commit's own records, and room beside them.
const UPDATE_PAGES: u64 = 32;
const PAGE: u64 = 4096;

/// The system allocator, counting the bytes allocated now and the most there have been.
struct Counting {
    now: AtomicUsize,
    peak: AtomicUsize,
}

// The allocator of a program has to be a static; it is the only one in the tests.
#[global_allocator]
static ALLOCATED: Counting = Counting {
    now: AtomicUsize::new(0),
    peak: AtomicUsize::new(0),
};

impl Counting {
    fn took(&self, bytes: usize) {
        let now = self.now.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn gave_back(&self, bytes: usize) {
        self.now.fetch_sub(bytes, Ordering::Relaxed);
    }
}

// SAFETY: every call is handed to the system allocator unchanged; this only counts beside it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract is passed on as it is.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            self.took(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is passed on as it is.
        unsafe { System.dealloc(pointer, layout) };
        self.gave_back(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract is passed on as it is.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                self.took(new_size - layout.size());
            } else {
                self.gave_back(layout.size() - new_size);
            }
        }
        moved
    }
}

#[test]
fn a_million_rows_load_and_scan_under_64_mib() {
    use support::*;
    if let Some(role) = role() {
        if role.name == "purge" {
            return purge_with_one_centre_record(&role);
        }
        return load_and_scan(role);
    }
    for (victim, group) in [(17, 0), (247, 15)] {
        let path = folder(&format!("purge-one-centre-record-{victim}"));
        let db = open(&path);
        declare_docs(&db, 65_536, "INDEXING_THRESHOLD 1");
        let direction = |id| {
            let mut v = vec![0.0; 65_536];
            v[usize::from(id != victim && id != victim + 1)] = 1.0;
            if id == victim + 1 {
                v[1] = 0.25;
            }
            v
        };
        load_docs(&db, 1..=256, "a", direction);
        maintenance(&db);
        let exported = path.join("before");
        db.export_snapshot(&exported).unwrap();
        let records = vector_records(&exported);
        let ivf = records.iter().find(|(k, _)| k[0] == 3).unwrap().1;
        let starts: Vec<_> = ivf[12..]
            .chunks_exact(8)
            .map(|p| {
                3 * u32::from_le_bytes(p[..4].try_into().unwrap())
                    + u32::from_le_bytes(p[4..].try_into().unwrap())
            })
            .collect();
        assert_eq!(starts.len(), 17);
        assert_eq!(starts[group + 1] - starts[group], 2);
        if group == 15 {
            assert!(starts[1..16].iter().all(|p| *p == starts[15]));
        }
        write_the_filler(&db);
        close(db);
        let lines = Child::start(
            "a_million_rows_load_and_scan_under_64_mib",
            &format!("purge victim={victim}"),
            &path,
        )
        .end();
        assert_eq!(last_number(&lines, "refused"), Some(0));
        assert!(last_number(&lines, "rise").unwrap() <= 15 << 18);
        let db = open(&path);
        assert_eq!(int(&db, "SELECT COUNT(*) FROM docs"), 255);
        let nearest = answer(&db, &by_mode(None, "EXACT", 1), &direction(victim + 1));
        assert_eq!(nearest.rows()[0][0], Value::Int64(victim + 1));
        db.verify().unwrap();
        let exported = path.join("after");
        db.export_snapshot(&exported).unwrap();
        let records = vector_records(&exported);
        let centres: Vec<_> = records.iter().filter(|(k, _)| k[0] == 4).collect();
        let centre = centres[group].1;
        let norm = (127.0_f32.powi(2) + 32.0_f32.powi(2)).sqrt();
        for (at, bytes) in centre.chunks_exact(4).enumerate() {
            let expected = [127.0 / norm, 32.0 / norm].get(at).copied().unwrap_or(0.0);
            assert!(
                (f32::from_le_bytes(bytes.try_into().unwrap()) - expected).abs() < 1e-6,
                "wrong repaired centre"
            );
        }
        close(db);
    }
    let folder = support::folder("million-rows");
    let db = support::open(&folder);
    support::declare(&db);
    support::close(db);

    let lines = support::Child::start(
        "a_million_rows_load_and_scan_under_64_mib",
        "loader",
        &folder,
    )
    .end();
    assert_eq!(
        support::last_number(&lines, "committed"),
        Some(ROWS as u64),
        "the loader did not commit every row"
    );
    let before = support::last_number(&lines, "before").expect("the loader announces it");
    let peak = support::last_number(&lines, "peak").expect("the loader announces it");
    assert!(
        peak < before + LIMIT as u64,
        "with {before} bytes allocated before the store was opened, the process reached {peak}: \
         {} bytes on the store's account under a limit of {LIMIT}",
        peak - before
    );

    // A commit writes what it changed, not what the store holds: the hundredth commit of
    // 10,000 new rows writes about what the first did, whatever the tree has grown to.
    let first =
        support::last_number(&lines, "first_commit_bytes").expect("the loader announces it");
    let last = support::last_number(&lines, "last_commit_bytes").expect("the loader announces it");
    assert!(
        last <= 2 * first,
        "the first commit of {ROWS_IN_A_COMMIT} rows wrote {first} bytes and the hundredth wrote \
         {last}: a commit's writes grow with the size of the store"
    );
    // One UPDATE of one small column of one row, in a store of a million rows, changes the
    // pages on the way to that row and to its index entry, and nothing else.
    let update = support::last_number(&lines, "update_bytes").expect("the loader announces it");
    assert!(
        update <= UPDATE_PAGES * PAGE,
        "one UPDATE of one column of one row in a store of {ROWS} rows wrote {update} bytes, more \
         than {UPDATE_PAGES} pages of {PAGE} bytes"
    );
}

/// Opens the store with the limit, loads the rows, scans them, and announces `before <bytes>`,
/// the bytes this process had allocated when it opened the store, and `peak <bytes>`, the most
/// it had allocated at any time after that, leaving out a buffer of its own that it holds for
/// a moment to show that the store does not count it. On the way it announces `committed <rows>` after
/// every commit and `scanned <what>` after every scan, so that the parent waits for one commit
/// or one scan at a time and never for the whole load.
fn load_and_scan(role: support::Role) {
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(LIMIT);
    // The bytes written to the data file, counted by the storage door.
    let written = std::sync::Arc::new(AtomicU64::new(0));
    options.storage = Some(support::storage({
        let written = written.clone();
        move |call| {
            if let support::Call::Write(_, bytes) = call {
                written.fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
        }
    }));
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");

    for first in (1..=ROWS).step_by(ROWS_IN_A_COMMIT as usize) {
        written.store(0, Ordering::SeqCst);
        let tx = db.transaction().expect("begin");
        support::insert(&tx, first..first + ROWS_IN_A_COMMIT);
        tx.commit()
            .expect("commit ten thousand rows under the limit");
        let bytes = written.load(Ordering::SeqCst);
        if first == 1 {
            support::say(format!("first_commit_bytes {bytes}"));
        }
        if first + ROWS_IN_A_COMMIT > ROWS {
            support::say(format!("last_commit_bytes {bytes}"));
        }
        support::say(format!("committed {}", first + ROWS_IN_A_COMMIT - 1));
    }

    // Three scans that visit every row and hold none: the kept count, every name, every tag.
    assert_eq!(support::int(&db, "SELECT COUNT(*) FROM t"), ROWS);
    assert_eq!(support::int(&db, "SELECT COUNT(name) FROM t"), ROWS);
    support::say("scanned names");
    assert_eq!(support::int(&db, "SELECT SUM(tag) FROM t"), ROWS / 10 * 45);
    // A filtered sum takes each row that passes once (CDB-330): tags 0 to 2 of every ten rows,
    // and the tags of the half of the rows whose name sorts after row 500000.
    assert_eq!(
        support::int(&db, "SELECT SUM(tag) FROM t WHERE tag < 3"),
        ROWS / 10 * 3
    );
    assert_eq!(
        support::int(&db, "SELECT SUM(tag) FROM t WHERE name > 'row-0500000'"),
        ROWS / 20 * 45
    );
    support::say("scanned tags");
    // And one page from the far end of the table, each row whole.
    let last_page = db
        .execute(
            "SELECT id, name, tag FROM t WHERE id > $last ORDER BY id LIMIT 100",
            &params! { "last" => ROWS - 100 },
        )
        .expect("read the last page");
    let wanted: Vec<_> = (ROWS - 99..=ROWS).map(support::row).collect();
    support::assert_same_rows(last_page.rows(), &wanted, "the last hundred rows");

    // One UPDATE of one small column of one row, and the bytes it writes.
    written.store(0, Ordering::SeqCst);
    support::run(&db, "UPDATE t SET tag = 7 WHERE id = 500000");
    support::say(format!("update_bytes {}", written.load(Ordering::SeqCst)));

    // Memory the host takes for itself is not the store's: with twice the limit allocated
    // beside it, the store counts what it counted and still answers under its limit.
    let loaded = ALLOCATED.peak.load(Ordering::Relaxed);
    let counted = support::used(&db);
    {
        let beside = vec![1_u8; 2 * LIMIT];
        assert_eq!(
            support::used(&db),
            counted,
            "memory the host allocated was counted against the store's limit"
        );
        assert_eq!(support::int(&db, "SELECT COUNT(name) FROM t"), ROWS);
        assert_eq!(std::hint::black_box(&beside).len(), 2 * LIMIT);
    }
    // The peak reported below is the store's, so the host's own buffer is taken out of it.
    ALLOCATED.peak.store(loaded, Ordering::Relaxed);
    support::close(db);

    support::say(format!("before {before}"));
    support::say(format!("peak {}", ALLOCATED.peak.load(Ordering::Relaxed)));
}

/// The value of the test below: 1 MiB of one sentence repeated, the largest the store
/// compresses. And the reader's limit: what an open sets aside and 64 KiB, which is room
/// for the value's stored record, a little over 4 KiB, and a sixteenth of what the value
/// itself takes. The arithmetic is that of `a_compressed_value_too_large_for_the_limit_is_refused`
/// in `large_values.rs`, which holds the refusal itself.
const VALUE: usize = 1 << 20;
/// Sinks, event types and routes the wide status row declares, each name some 2400 bytes.
const WIDE: usize = 24;
fn value_limit(folder: &std::path::Path) -> usize {
    support::opening::opening_charge(folder, None) + (64 << 10)
}

/// The JSON cases of the test below: the same 1 MiB as text, in the shape that shrinks most
/// (an array of small objects), whose tree in memory is some twenty times its text. The limit
/// leaves 4 MiB beyond what the open sets aside: room for the text, none for the tree.
const JSON_ROOM: usize = 4 << 20;
fn json_limit(folder: &std::path::Path) -> usize {
    support::opening::opening_charge(folder, None) + JSON_ROOM
}

#[test]
fn a_refused_compressed_value_is_never_produced() {
    if let Some(role) = support::role() {
        if role.name.starts_with("delivery") {
            return delivery_allocation_row(&role);
        }
        refused_select(&role);
        return refused_json(&role);
    }
    let mut peaks = Vec::new();
    for door in "delivery-status delivery-metrics delivery-pass1 delivery-pass10".split_whitespace()
    {
        let path = support::folder(door);
        let count = if door.ends_with("10") { 10 } else { 1 };
        let bytes = if door.contains("pass") { 8 } else { 2 << 20 };
        let db = support::delivery_queue(&path, bytes, count);
        write_the_filler(&db);
        support::close(db);
        let lines =
            support::Child::start("a_refused_compressed_value_is_never_produced", door, &path)
                .end();
        let refused = support::last_number(&lines, "refused").unwrap();
        peaks.push((refused, support::last_number(&lines, "rise").unwrap()));
    }
    // Many declarations with long names: a status that copies them uncharged would allocate far
    // past the room the limit leaves; a charged one is refused before it copies them.
    let path = support::folder("delivery-wide");
    let db = support::delivery_queue(&path, 8, 1);
    for n in 0..WIDE {
        let name = |what: &str| {
            format!(
                "{what}_with_a_name_that_is_long_{n:05}_{}",
                "x".repeat(2400)
            )
        };
        let (event, sink) = (name("event"), name("sink"));
        support::run(
            &db,
            &format!("CREATE EVENT TYPE {event} WHEN INSERT ON queued"),
        );
        support::run(&db, &format!("CREATE SINK {sink} TYPE callback"));
        support::run(
            &db,
            &format!("CREATE ROUTE {} EVENT {event} TO {sink}", name("route")),
        );
    }
    support::close(db);
    let lines = support::Child::start(
        "a_refused_compressed_value_is_never_produced",
        "delivery-wide",
        &path,
    )
    .end();
    let (refused, rise) = (
        support::last_number(&lines, "refused").unwrap(),
        support::last_number(&lines, "rise").unwrap(),
    );
    assert!(
        refused == 1 && rise < WIDE as u64 * 1024,
        "an event bus status of {WIDE} sinks, event types and routes with long names was not \
         charged before it was copied: refused {refused}, allocated {rise} bytes"
    );
    assert!(
        peaks
            .iter()
            .all(|&(refused, peak)| refused == 0 && peak < 512 << 10),
        "inspection/pass allocation: {peaks:?}"
    );
    assert!(
        peaks[3].1 <= peaks[2].1 + (64 << 10),
        "queue length changed delivery peak: {peaks:?}"
    );
    let folder = support::folder("refused-value-never-produced");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    );
    support::run(
        &db,
        "CREATE TABLE records (id INTEGER PRIMARY KEY, meta JSON NOT NULL)",
    );
    db.execute(
        "INSERT INTO records (id, meta) VALUES ($id, $meta)",
        &params! { "id" => 1_i64, "meta" => Value::Json(support::large_json(true, 1, VALUE)) },
    )
    .expect("store the JSON value");
    let large = support::large_text(support::Kind::Repeating, 1, VALUE);
    for (id, body) in [(1_i64, large.as_str()), (2, "small")] {
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &params! { "id" => id, "body" => body },
        )
        .unwrap_or_else(|error| panic!("note {id} was refused: {error:?}"));
    }
    support::maintenance(&db);
    support::close(db);
    // The value is stored compressed: the file is too short to hold it as it is.
    let length = fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len();
    assert!(
        length < VALUE as u64,
        "the data file is {length} bytes long: a repeating text of {VALUE} bytes was stored as \
         it is, and this test would show nothing about a compressed value"
    );

    let lines = support::Child::start(
        "a_refused_compressed_value_is_never_produced",
        "reader",
        &folder,
    )
    .end();
    assert_eq!(
        support::last_number(&lines, "refused"),
        Some(1),
        "the reader's select of the large value was not refused by its limit"
    );
    // The value's bytes never existed: from just before the select to just after its refusal
    // the most the process had allocated rose by less than the value is long. A read that
    // produced the value and then counted it would have taken all of its bytes at once.
    let rise = support::last_number(&lines, "rise").expect("the reader announces it");
    assert!(
        rise < VALUE as u64,
        "while a value of {VALUE} bytes was refused, the process allocated {rise} bytes more \
         than it had before the select"
    );

    // A JSON value is counted as the tree it becomes, not as its text: a tree of some twenty
    // times the text, under a limit with room for the text only, is refused by name when it is
    // selected and when the same text is written, and is made in neither case.
    for (prefix, what) in [("select_", "selected"), ("write_", "written")] {
        assert_eq!(
            support::last_number(&lines, &format!("{prefix}refused")),
            Some(1),
            "a JSON value of {VALUE} bytes of text whose tree is far larger than the \
             {JSON_ROOM} bytes of room its limit leaves was not refused when {what}: the limit \
             counts the tree, not the text"
        );
        let rise = support::last_number(&lines, &format!("{prefix}rise")).expect("announced");
        assert!(
            rise < JSON_ROOM as u64,
            "while a JSON value was refused when {what}, the process allocated {rise} bytes more \
             than it had before: the tree was made though the limit left {JSON_ROOM} bytes"
        );
    }
}

/// Runs one statement and announces `<prefix>refused 1` when the memory limit refused it (0 when
/// it was answered) and `<prefix>rise <bytes>`: how far the most this process ever had allocated
/// rose above what it had allocated just before the statement. Any other outcome fails.
fn measured<T>(prefix: &str, statement: impl FnOnce() -> contextdb::Result<T>) -> bool {
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let outcome = statement();
    let peak = ALLOCATED.peak.load(Ordering::Relaxed);
    let refused = match outcome {
        Err(Error::MemoryBudgetExceeded { .. }) => true,
        Err(other) => panic!("the statement was refused as {other:?}"),
        Ok(_) => false,
    };
    support::say(format!("{prefix}refused {}", u8::from(refused)));
    support::say(format!("{prefix}rise {}", peak - before));
    refused
}

/// Opens the store for reading under the limit and reads the small row and the count, so that
/// whatever a session sets up with its first statements is there. Then selects the large
/// value, which must be refused by the memory limit, and announces `refused 1` and
/// `rise <bytes>`: how far the most this process ever had allocated rose above what it had
/// allocated just before the select.
fn refused_select(role: &support::Role) {
    let options = support::reader_options(Some(value_limit(&role.folder)));
    let reader = ReadSession::open_with_options(&role.folder, options)
        .expect("open the store for reading under the limit");
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM notes"), 2);
    assert_eq!(
        support::rows(&reader, "SELECT body FROM notes WHERE id = 2"),
        [[support::text("small")]]
    );

    measured("", || {
        reader.execute("SELECT body FROM notes WHERE id = 1", &params! {})
    });
}

/// Selects the JSON value under [`json_limit`] from a reader, then writes the text of a second
/// value of the same shape from a writer under the same limit, and announces each as
/// [`measured`] does, with the prefixes `select_` and `write_`. A refused write left no row.
fn refused_json(role: &support::Role) {
    let options = support::reader_options(Some(json_limit(&role.folder)));
    let reader = ReadSession::open_with_options(&role.folder, options)
        .expect("open the store for reading under the limit");
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM records"), 1);
    measured("select_", || {
        reader.execute("SELECT meta FROM records WHERE id = 1", &params! {})
    });
    drop(reader);

    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(json_limit(&role.folder));
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");
    let text = contextdb::serde_json::to_string(&support::large_json(true, 2, VALUE))
        .expect("JSON writes as text");
    measured("write_", || {
        db.execute(
            "INSERT INTO records (id, meta) VALUES ($id, $meta)",
            &params! { "id" => 2_i64, "meta" => text },
        )
    });
    assert_eq!(
        support::int(&db, "SELECT COUNT(*) FROM records"),
        1,
        "the refused write left a row"
    );
    support::close(db);
}

// ---- a full cache ----

/// Rows of a table beside the one a test measures, each with a text of 900 bytes that does not
/// shrink: 9 MiB, more than the 8 MiB cache of a store whose limit is under 32 MiB.
const FILLER_ROWS: i64 = 10_000;
const FILLER_TEXT: usize = 900;

/// Writes the filler rows, in a table of their own and one commit.
fn write_the_filler(db: &Database) {
    support::run(
        db,
        "CREATE TABLE filler (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    );
    let tx = db.transaction().expect("begin");
    for id in 1..=FILLER_ROWS {
        let body = support::large_text(support::Kind::Scattered, id, FILLER_TEXT);
        tx.execute(
            "INSERT INTO filler (id, body) VALUES ($id, $body)",
            &params! { "id" => id, "body" => body },
        )
        .unwrap_or_else(|error| panic!("filler row {id} was refused: {error:?}"));
    }
    tx.commit().expect("commit the filler rows");
}

/// Reads every filler row's text, so that the cache is full before what is measured begins and
/// nothing is measured with the room an empty cache leaves.
fn fill_the_cache(db: &Database) {
    assert_eq!(
        support::int(db, "SELECT COUNT(body) FROM filler"),
        FILLER_ROWS
    );
}

// ---- one sealing cycle with exactly the room it reserves ----

/// The numbers in a vector of the sealing test, and its rows: as many as one cycle seals, in
/// one partition, with keys of nine bytes. Their slots fill most of one block.
const SEALED_DIMENSION: usize = 40;
const SEALED: usize = 4096;
const KEY: usize = 9;

/// The bytes of the block one cycle writes for them: its count, and for each vector a slot of
/// eight bytes and one byte a number, the end of its key, and its key.
const BLOCK: usize = 4 + SEALED * (8 + SEALED_DIMENSION + 4 + KEY);
const _: () = assert!(BLOCK == 249_860 && BLOCK <= 262_080);

/// What the cycle's working memory comes to: the keys of the pending records it collected,
/// each its bytes and 24; one block being built, 262,080 bytes, taken once; the partition
/// record it adds to, the bytes of its key tuple, here none, and 64; and one row with its
/// vector, a row header and two decoded cells, and its numbers. The cycle also retains
/// the initial four-entry candidate reservation and its final one-entry work list.
// A decoded cell holds either a Value or the large-value pointer (two u64s).
type SeenCell = std::result::Result<Value, (u64, u64)>;
const LISTED: usize = 4 * size_of::<(bool, u32, u32, Vec<u8>)>() + size_of::<(u32, Vec<u8>)>();
const WORKING: usize = LISTED
    + SEALED * (KEY + 24)
    + 262_080
    + 64
    + (size_of::<(Option<u32>, Vec<SeenCell>, u64)>()
        + 2 * size_of::<SeenCell>()
        + 4 * SEALED_DIMENSION);

/// The partition record: step 4's 73 bytes, then its unindexed count (4), the bytes its pending
/// vectors will take (8), the room left in its tail block (4), and assigned count (8).
const ENTRY: usize = 73 + 4 + 8 + 4 + 8;

/// What the records the cycle stages come to, each its value, 320 bytes and twice its key: the
/// block, whole, under a key of 13 bytes; the removal of each pending record, whose key is nine
/// bytes and the row's key, and each row's slot record of 12 bytes under a key as long; the
/// partition record under a key of 5; and the store's count of pending records, 320. Its 8,195
/// undo calls need 16,384 slots of 136 bytes; growth from 8,192 slots reserves the old and new
/// buffers together.
const STAGED: usize = (BLOCK + 320 + 2 * 13)
    + SEALED * (320 + 2 * (9 + KEY))
    + SEALED * (12 + 320 + 2 * (9 + KEY))
    + (ENTRY + 320 + 2 * 5)
    + 320
    + 3 * (2 * SEALED + 3).next_power_of_two() / 2 * 136;

/// A limit that is exactly enough for that one partition: what an open sets aside, and
/// every reservation of one cycle, each once.
fn sealing_limit(folder: &std::path::Path) -> usize {
    support::opening::opening_charge(folder, None)
        + WORKING
        + STAGED
        + support::limit_commit_bytes()
}

#[test]
fn a_sealing_cycle_with_exactly_the_room_it_reserves_completes_within_the_limit() {
    if let Some(role) = support::role() {
        return one_cycle_under_the_limit(&role);
    }
    let folder = support::folder("sealing-exact-room");
    let db = support::open(&folder);
    write_the_filler(&db);
    support::run(
        &db,
        &format!("CREATE TABLE points (id INTEGER PRIMARY KEY, v VECTOR({SEALED_DIMENSION}))"),
    );
    let tx = db.transaction().expect("begin");
    for id in 1..=SEALED as i64 {
        tx.execute(
            "INSERT INTO points (id, v) VALUES ($id, $v)",
            &params! { "id" => id, "v" => support::vector(id, SEALED_DIMENSION) },
        )
        .unwrap_or_else(|error| panic!("point {id} was refused: {error:?}"));
    }
    tx.commit().expect("commit the points");
    support::close(db);

    let sealing_limit = sealing_limit(&folder);
    let lines = support::Child::start(
        "a_sealing_cycle_with_exactly_the_room_it_reserves_completes_within_the_limit",
        "sealer",
        &folder,
    )
    .end();
    // The open set its floor aside, so what the limit left the cycle is exactly its reservations.
    assert_eq!(
        support::last_number(&lines, "set_aside"),
        Some(support::opening::opening_charge(&folder, None) as u64),
        "set aside by an open under {sealing_limit} bytes"
    );
    let refused: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("refused "))
        .collect();
    assert!(
        refused.is_empty(),
        "with {} bytes for its working memory and {} for the records it stages, {} in all, a \
         cycle over one partition of {SEALED} pending vectors was {}",
        WORKING,
        STAGED,
        WORKING + STAGED,
        refused[0]
    );
    assert_eq!(
        (
            support::last_number(&lines, "done"),
            support::last_number(&lines, "remaining")
        ),
        (Some(SEALED as u64), Some(0)),
        "what the cycle sealed and left unindexed"
    );
    assert_eq!(
        support::last_number(&lines, "used_after"),
        Some(support::opening::opening_charge(&folder, None) as u64),
        "memory in use after the cycle"
    );
    // What the process really took for the store, from before the open to the end, the
    // cycle's commit included, is within the limit. The cycle began with the cache full.
    let before = support::last_number(&lines, "before").expect("the sealer announces it");
    let peak = support::last_number(&lines, "peak").expect("the sealer announces it");
    let cached = support::last_number(&lines, "cached").expect("the sealer announces it");
    assert!(
        peak <= before + sealing_limit as u64,
        "with {before} bytes allocated before the store was opened, the process reached {peak}: \
         {} bytes on the store's account under a limit of {sealing_limit}, {} past it, in a \
         cycle over {SEALED} pending vectors, begun with {cached} bytes allocated for a full \
         cache",
        peak - before,
        peak - before - sealing_limit as u64
    );

    // The cycle's commit is there: every vector is in a block, and the nearest are found.
    let db = support::open(&folder);
    let stored = support::stored(&db, "SELECT id, v FROM points");
    let query = support::vector(94_500, SEALED_DIMENSION);
    let wanted = stored.nearest(|_| true, &query, 10);
    let sql = support::nearest_sql("points", "v", None, 10);
    let result = support::assert_nearest(&db, &sql, &query, &wanted, "after the cycle");
    assert!(
        result.rows_examined() < SEALED as u64,
        "a search of {SEALED} sealed vectors read {} rows",
        result.rows_examined()
    );
}

/// Opens the store under [`sealing_limit`], reads every filler row, and runs one maintenance
/// cycle. Announces `set_aside <bytes>`, what the open counts; `cached <bytes>`, what this
/// process had allocated more than before the open once the filler rows were read; then
/// `done <vectors>` and `remaining <vectors>`, or `refused <the refusal>`; `used_after <bytes>`;
/// and, once the store is closed,
/// `before <bytes>`, what this process had allocated when it opened the store, and
/// `peak <bytes>`, the most it had allocated at any time after that.
fn one_cycle_under_the_limit(role: &support::Role) {
    let limit = sealing_limit(&role.folder);
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");
    support::say(format!("set_aside {}", support::used(&db)));
    fill_the_cache(&db);
    let cached = ALLOCATED.now.load(Ordering::Relaxed);
    support::say(format!("cached {}", cached.saturating_sub(before)));
    match db.run_maintenance_cycle() {
        Ok(report) => {
            support::say(format!(
                "done {}",
                count(report.vector.maintenance_vectors_done)
            ));
            support::say(format!(
                "remaining {}",
                count(report.vector.maintenance_vectors_remaining)
            ));
        }
        Err(error) => support::say(format!("refused {error:?}")),
    }
    // What stays after the cycle: the open's charge and the statement record's lines, each
    // charged for its text and its own bytes.
    let used_after = support::used(&db);
    let lines: usize = support::rows(&db, "SHOW STATEMENT_STATISTICS")
        .iter()
        .map(|line| match &line[0] {
            Value::Text(form) => support::form_charge(form),
            other => panic!("a statement is text, not {other:?}"),
        })
        .sum();
    support::say(format!("used_after {}", used_after - lines));
    support::close(db);
    support::say(format!("before {before}"));
    support::say(format!("peak {}", ALLOCATED.peak.load(Ordering::Relaxed)));
}

// ---- a build and an indexed search under the limit ----

/// The limit of the build test: the cache's 8 MiB, the open's ninth, and room for the steps.
const BUILD_LIMIT: usize = 16 << 20;

#[test]
fn an_indexed_search_and_a_build_stay_under_the_limit() {
    if let Some(role) = support::role() {
        return a_build_under_the_limit(&role);
    }
    let folder = support::folder("build-under-the-limit");
    let db = support::open(&folder);
    write_the_filler(&db);
    support::declare_docs(&db, 64, "INDEXING_THRESHOLD 1000");
    support::load_docs(&db, 1..=1100, "a", |n| support::gathered(n, 64));
    support::close(db);

    let lines = support::Child::start(
        "an_indexed_search_and_a_build_stay_under_the_limit",
        "builder",
        &folder,
    )
    .end();
    let refused: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("refused "))
        .collect();
    assert!(refused.is_empty(), "under {BUILD_LIMIT} bytes: {refused:?}");
    let told = |key: &str| support::last_number(&lines, key);
    assert_eq!(
        (
            told("stalled"),
            told("lists"),
            told("rows"),
            told("exact_rows")
        ),
        (Some(0), Some(33), Some(10), Some(10)),
        "the build's stalled partitions and groups, and the rows of the indexed and of the \
         exact search"
    );
    assert!(
        lines.iter().any(|line| line == "committed 200"),
        "the commit of vectors under the limit was not taken: {lines:?}"
    );
    // What the process took for the store, from before the open to the end, every step of
    // the build and the search included, is within the limit. Each began with the cache full.
    let before = told("before").expect("the builder announces it");
    let peak = told("peak").expect("the builder announces it");
    assert!(
        peak <= before + BUILD_LIMIT as u64,
        "with {before} bytes allocated before the store was opened, the process reached {peak}: \
         {} bytes on the store's account under a limit of {BUILD_LIMIT}",
        peak - before
    );
}

/// Opens the store under [`BUILD_LIMIT`] and, with the cache filled before each, runs every
/// maintenance cycle and one indexed search. Announces `refused <the refusal>` for a cycle or
/// a search that was refused, `stalled <partitions>`, `lists <groups>`, `rows <returned>`,
/// and, once the store is closed, `before <bytes>` and `peak <bytes>` as the sealing test does.
fn a_build_under_the_limit(role: &support::Role) {
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(BUILD_LIMIT);
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");
    // One commit of vectors under the limit: 200 rows take a nearby vector of the same
    // direction, so the column keeps its 1,100 vectors and the build below is the same.
    fill_the_cache(&db);
    let tx = db.transaction().expect("begin");
    for id in 1..=200_i64 {
        let values = params! { "id" => id, "v" => support::gathered(id + 8, 64) };
        if let Err(error) = tx.execute("UPDATE docs SET v = $v WHERE id = $id", &values) {
            support::say(format!("refused {error:?}"));
        }
    }
    match tx.commit() {
        Ok(_) => support::say("committed 200"),
        Err(error) => support::say(format!("refused {error:?}")),
    }
    for _ in 0..100 {
        fill_the_cache(&db);
        match db.run_maintenance_cycle() {
            Ok(report) => {
                let report = report.vector;
                support::say(format!("stalled {}", count(report.stalled_partitions)));
                if count(report.maintenance_vectors_remaining) == 0
                    && count(report.remaining_partitions) <= count(report.stalled_partitions)
                {
                    break;
                }
            }
            Err(error) => {
                support::say(format!("refused {error:?}"));
                break;
            }
        }
    }
    if let Value::Int64(lists) = support::field(&support::the_partition(&db), "lists") {
        support::say(format!("lists {lists}"));
    }
    fill_the_cache(&db);
    let query = support::gathered(93_000, 64);
    match support::search(&db, &support::by_mode(None, "INDEXED", 10), &query) {
        Ok(result) => support::say(format!("rows {}", result.rows().len())),
        Err(error) => support::say(format!("refused {error:?}")),
    }
    fill_the_cache(&db);
    match support::search(&db, &support::by_mode(None, "EXACT", 10), &query) {
        Ok(result) => support::say(format!("exact_rows {}", result.rows().len())),
        Err(error) => support::say(format!("refused {error:?}")),
    }
    support::close(db);
    support::say(format!("before {before}"));
    support::say(format!("peak {}", ALLOCATED.peak.load(Ordering::Relaxed)));
}

// ---- one more edge from a node that has many ----

const EDGES_FROM_ONE_NODE: i64 = 20_000;

/// The bytes a walk over this node's edges would hold at the least: a node id and its place in
/// the walk, 40 bytes each, for each edge. One edge written beside them must take far less.
const A_WALK_OVER_THEM: u64 = EDGES_FROM_ONE_NODE as u64 * 40;

#[test]
fn one_more_edge_from_a_node_with_many_edges_reads_none_of_them() {
    if let Some(role) = support::role() {
        return one_more_edge(&role);
    }
    let folder = support::folder("edges-from-one-node");
    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('E')",
    );
    let tx = db.transaction().expect("begin");
    for id in 1..=EDGES_FROM_ONE_NODE {
        let values = params! { "id" => id, "to" => Uuid::from_u128(id as u128 + 1) };
        tx.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($id, '00000000-0000-0000-0000-000000000001', $to, 'E')",
            &values,
        )
        .expect("an edge from the one node");
    }
    tx.commit().expect("commit the edges");
    support::close(db);

    let lines = support::Child::start(
        "one_more_edge_from_a_node_with_many_edges_reads_none_of_them",
        "edge",
        &folder,
    )
    .end();
    assert!(
        lines.iter().any(|line| line == "accepted"),
        "the edge was not accepted: {lines:?}"
    );
    let taken = support::last_number(&lines, "taken").expect("the child announces it");
    assert!(
        taken < A_WALK_OVER_THEM / 2,
        "one edge from a node with {EDGES_FROM_ONE_NODE} edges took {taken} bytes, as much as a \
         walk over the node's edges ({A_WALK_OVER_THEM}): writing an edge reads the other edges \
         of its source, so what it costs grows with how many there are"
    );
}

/// Opens the store, writes one more edge from the node, and announces `accepted` and
/// `taken <bytes>`, the most the process held above what it held before the edge.
fn one_more_edge(role: &support::Role) {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(BUILD_LIMIT);
    let db = support::open_with(&role.folder, options).expect("open the store under the limit");
    let before = ALLOCATED.now.load(Ordering::Relaxed);
    ALLOCATED.peak.store(before, Ordering::Relaxed);
    let values = params! {
        "id" => EDGES_FROM_ONE_NODE + 1,
        "to" => Uuid::from_u128(EDGES_FROM_ONE_NODE as u128 + 10)
    };
    match db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($id, '00000000-0000-0000-0000-000000000001', $to, 'E')",
        &values,
    ) {
        Ok(_) => support::say("accepted"),
        Err(error) => support::say(format!("refused {error:?}")),
    }
    let peak = ALLOCATED.peak.load(Ordering::Relaxed);
    support::say(format!("taken {}", peak - before));
    support::close(db);
}
fn delivery_allocation_row(role: &support::Role) {
    let room = if role.name == "delivery-wide" {
        // The open holds the declarations; the status copies their keys and pointers, about 50 KiB
        // here, so a headroom of a third of that cannot hold it.
        support::opening::opening_charge(&role.folder, None) - support::opening::open_floor()
            + (16 << 10)
    } else {
        2 << 20
    };
    let db = support::open_limited(&role.folder, support::opening::open_floor() + room);
    db.register_sink("outgoing", None, |_| Ok(())).unwrap();
    if !role.name.starts_with("delivery-wide") {
        fill_the_cache(&db);
    }
    measured("", || {
        if role.name == "delivery-wide" {
            return db.event_bus_status().map(drop);
        }
        let count = if role.name == "delivery-status" {
            db.event_bus_status()?.sinks[0].metrics.queued
        } else if role.name == "delivery-metrics" {
            db.sink_metrics("outgoing")?.queued
        } else {
            count(db.run_maintenance_cycle()?.delivered)
        };
        assert_eq!(count, if role.name.ends_with("10") { 10 } else { 1 });
        Ok(())
    });
}

#[test]
fn replacing_a_dirty_record_discards_its_old_bytes() {
    use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
    const TEST: &str = "replacing_a_dirty_record_discards_its_old_bytes";
    const LEN: usize = 200 << 10;
    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("wide");
    let Some(role) = support::role() else {
        let folder = support::folder(TEST);
        let lines = support::Child::start(TEST, "replace", &folder).end();
        eprintln!("allocation receipts: {lines:?}");
        assert!(support::last_number(&lines, "discard_rise").unwrap() < LEN as u64);
        assert!(support::last_number(&lines, "keep_rise").unwrap() >= LEN as u64);
        return;
    };
    let (a, b, c) = (vec![1_u8; LEN], vec![2_u8; LEN], vec![3_u8; LEN]);
    let path = role.folder.join("raw");
    let db = redb::Database::create(&path).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(TABLE).unwrap();
        table.insert(1, a.as_slice()).unwrap();
        measured("discard_", || {
            table.insert_discard(1, b.as_slice()).unwrap();
            Ok(())
        });
        measured("keep_", || {
            assert_eq!(table.insert(1, c.as_slice()).unwrap().unwrap().value(), b);
            Ok(())
        });
        assert_eq!(table.len().unwrap(), 1);
        assert_eq!(table.get(1).unwrap().unwrap().value(), c);
    }
    tx.commit().unwrap();
    drop(db);
    let db = redb::Database::open(path).unwrap();
    let tx = db.begin_read().unwrap();
    let table = tx.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!(table.get(1).unwrap().unwrap().value(), c);

    let folder = role.folder.join("sql");
    let db = support::open(&folder);
    support::run(&db, "CREATE TABLE t(id INT PRIMARY KEY, body TEXT)");
    let a = params! { "v" => "a".repeat(LEN) };
    let b = params! { "v" => "b".repeat(LEN) };
    let tx = db.transaction().unwrap();
    tx.execute("INSERT INTO t VALUES(1,$v)", &a).unwrap();
    tx.execute("UPDATE t SET body=$v WHERE id=1", &a).unwrap();
    measured("sql_", || tx.execute("UPDATE t SET body=$v WHERE id=1", &b));
    let expected = support::rows(&tx, "SELECT id,body FROM t");
    assert_eq!(
        expected,
        vec![vec![Value::Int64(1), support::text(&"b".repeat(LEN))]]
    );
    tx.commit().unwrap();
    let db = support::reopen(db, &folder);
    assert_eq!(support::rows(&db, "SELECT id,body FROM t"), expected);
}

fn purge_with_one_centre_record(role: &support::Role) {
    let db = support::open_limited(&role.folder, support::opening::open_floor() + (15 << 18));
    fill_the_cache(&db);
    measured("", || {
        db.execute(
            "PURGE FROM docs WHERE id=$id",
            &params! {"id"=>role.number("victim") as i64},
        )
        .inspect_err(|error| eprintln!("centroid PURGE: {error:?}"))
    });
}

/// A cursor over a run of 1,000 tied rows, fetched one row per page, holds the rest of the run
/// between pages (rule 4: what is held is charged). The bytes the process really holds beyond
/// what it held before the cursor opened are covered by the bytes the session reports used,
/// within room for the cursor's own small state: the list a run is held in is charged by what
/// it holds, 80 bytes for each entry of it, which is 80,000 bytes at the first page.
#[test]
fn a_run_held_across_pages_is_charged_by_what_it_holds() {
    const RUN: i64 = 1000;
    // The cursor's own small state, the plan and a page: far under one run's list.
    const ROOM: usize = 32 << 10;
    let db = support::open(&support::folder("cursor-held-run"));
    support::run(
        &db,
        "CREATE TABLE p (id INTEGER PRIMARY KEY, g INTEGER NOT NULL, v INTEGER NOT NULL)",
    );
    support::run(&db, "CREATE INDEX pg ON p (g)");
    let rows: Vec<String> = (1..=RUN).map(|id| format!("({id}, 0, {})", -id)).collect();
    support::run(
        &db,
        &format!("INSERT INTO p (id, g, v) VALUES {}", rows.join(", ")),
    );
    let sql = "SELECT id FROM p ORDER BY g, v";
    // Read once whole, so that the cache holds what the cursor will read.
    assert_eq!(support::rows(&db, sql).len() as i64, RUN);
    let session = db.read_session(contextdb::ReadLimits::default()).unwrap();
    let (really, charged) = (
        ALLOCATED.now.load(Ordering::Relaxed),
        support::memory_used(&session),
    );
    let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
    let mut over = Vec::new();
    for fetched in 1..RUN {
        cursor.fetch(std::num::NonZeroUsize::new(1)).unwrap();
        if ![1, 250, 500, 999].contains(&fetched) {
            continue;
        }
        let charged_now = support::memory_used(&session).saturating_sub(charged);
        let held_now = ALLOCATED.now.load(Ordering::Relaxed).saturating_sub(really);
        eprintln!("page {fetched}: really {held_now}, charged {charged_now}");
        if held_now > charged_now + ROOM {
            over.push(format!(
                "after {fetched} pages the cursor really holds {held_now} bytes more than before it opened, the session is charged {charged_now}"
            ));
        }
    }
    assert!(
        over.is_empty(),
        "what a cursor holds between pages is charged:\n{}",
        over.join("\n")
    );
}
