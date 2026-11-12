//! Large text and JSON values: one that the store keeps beside its row reads back byte for
//! byte whether or not the store keeps it compressed; text that repeats takes less room in
//! the data file than its own bytes and text that does not shrink takes no more than it did;
//! a value longer than 1 MiB and a vector are never compressed; the memory limit holds while
//! a compressed value is read and while one is written; and a damaged length is reported as
//! damage.
//!
//! Nothing here names compression, because nothing in the library does: a test sees it in
//! what a statement returns, in the length of the data file and in the memory counted.

mod support;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::serde_json;
use contextdb::{
    Database, DatabaseOpenOptions, Error, ReadSession, ReadSessionOptions, Value, params,
};

use support::{Call, Child, Kind, Run};

const KIB: usize = 1 << 10;
const MIB: usize = 1 << 20;

/// The longest text or JSON value the store compresses: 1 MiB. One byte more is stored as it
/// is, whatever it holds.
const LONGEST_COMPRESSED: usize = 1_048_576;

// ---- shared by the tests of this file ----

fn open_limited(folder: &Path, limit: usize) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    match support::open_with(folder, options) {
        Ok(db) => db,
        Err(error) => panic!("an open under a limit of {limit} bytes was refused: {error:?}"),
    }
}

fn read_limited(folder: &Path, limit: Option<usize>) -> ReadSession {
    let mut options = ReadSessionOptions::default();
    options.memory_limit = limit;
    match ReadSession::open_with_options(folder, options) {
        Ok(reader) => reader,
        Err(error) => panic!("a reader under a limit of {limit:?} was refused: {error:?}"),
    }
}

/// The bytes `SHOW MEMORY_LIMIT` reports as used.
fn used(on: &impl Run) -> usize {
    let report = support::run(on, "SHOW MEMORY_LIMIT");
    let at = report.columns().iter().position(|name| name == "used");
    match (at, report.rows()) {
        (Some(at), [row]) => match row[at] {
            Value::Int64(bytes) => bytes as usize,
            ref other => panic!("SHOW MEMORY_LIMIT reports {other:?} as used"),
        },
        _ => panic!("SHOW MEMORY_LIMIT did not return one row with a column `used`"),
    }
}

/// The length of the store's data file, in bytes.
fn data_file(folder: &Path) -> u64 {
    fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len()
}

/// The outcome of a statement that selects large values, with the rows left out of what a
/// failure prints: a megabyte of text does not belong in a test's output.
fn answered(on: &impl Run, sql: &str, id: i64) -> contextdb::Result<Vec<Vec<Value>>> {
    on.run(sql, &params! { "id" => id })
        .map(|result| result.rows().to_vec())
}

/// The table of every test below but the first: a key and one text.
const NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)";

/// Inserts one row of `notes`.
fn insert_note(on: &impl Run, id: i64, body: &str) {
    if let Err(error) = on.run(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &params! { "id" => id, "body" => body },
    ) {
        panic!("note {id}, of {} bytes, was refused: {error:?}", body.len());
    }
}

/// Asserts that the body of one row of `notes` reads back as exactly this text.
fn assert_note_is(on: &impl Run, id: i64, wanted: &str, what: &str) {
    let what = format!("{what}: the body of note {id}");
    match answered(on, "SELECT body FROM notes WHERE id = $id", id) {
        Ok(rows) => match rows.as_slice() {
            [row] => support::assert_text_is(&row[0], wanted, &what),
            rows => panic!("{what}: {} rows were returned, not one", rows.len()),
        },
        Err(error) => panic!("{what}: the select was refused: {error:?}"),
    }
}

/// A new store whose `notes` holds rows 1 to `rows`, the body of each `len` bytes of `kind`,
/// 500 rows to a transaction, closed. Returns the bytes put in: 8 for each id and the bytes
/// of each body.
fn store_of_notes(folder: &Path, kind: Kind, rows: i64, len: usize) -> u64 {
    let db = support::open(folder);
    support::run(&db, NOTES);
    let mut put_in = 0_u64;
    for first in (1..=rows).step_by(500) {
        let tx = db.transaction().expect("begin");
        for id in first..(first + 500).min(rows + 1) {
            let body = support::large_text(kind, id, len);
            put_in += 8 + body.len() as u64;
            insert_note(&tx, id, &body);
        }
        tx.commit().expect("commit five hundred notes");
    }
    support::close(db);
    put_in
}

/// Asserts that every row [`store_of_notes`] wrote reads back equal, and that no other row is
/// there.
fn assert_notes_are(on: &impl Run, kind: Kind, rows: i64, len: usize, what: &str) {
    assert_eq!(
        support::int(on, "SELECT COUNT(*) FROM notes"),
        rows,
        "{what}"
    );
    for id in 1..=rows {
        assert_note_is(on, id, &support::large_text(kind, id, len), what);
    }
}

// ---- every kind and size, three ways ----

/// The sizes of the first test: the last text that stays in its row and the first that does
/// not, and two between. Each is taken by every kind of text.
const SIZES: [usize; 4] = [1015, 1016, 4000, 70_000];

/// The rows after those: the first size too long to be compressed, in one kind that shrinks and
/// one that does not.
const LONGEST: [Kind; 2] = [Kind::Repeating, Kind::Scattered];

/// One row of `docs` for each kind of text at each size, and one for each kind of `LONGEST`.
const DOCS: i64 = (Kind::ALL.len() * SIZES.len() + LONGEST.len()) as i64;

/// The kind of text row `id` of `docs` is first written with, and the size of its body and
/// of its meta.
fn shape(id: i64) -> (Kind, usize) {
    let at = (id - 1) as usize;
    match SIZES.get(at / Kind::ALL.len()) {
        Some(size) => (Kind::ALL[at % Kind::ALL.len()], *size),
        None => (
            LONGEST[at - Kind::ALL.len() * SIZES.len()],
            LONGEST_COMPRESSED + 1,
        ),
    }
}

/// The body of row `id`: of the row's own kind when it is inserted, `changes` 0, and of the
/// next kind once it has been updated, `changes` 1.
fn body(id: i64, changes: usize) -> String {
    let (kind, size) = shape(id);
    support::large_text(kind.after(changes), id, size)
}

/// The meta of row `id`, of the same size as its body: of the kind that shrinks in the rows
/// with an odd id and of the kind that does not in the others, so every size has both.
fn meta(id: i64) -> serde_json::Value {
    support::large_json(id % 2 == 1, id, shape(id).1)
}

/// The rows of `docs` that are there: all of them at first, and all but every third once the
/// bodies have been changed.
fn kept(changes: usize) -> Vec<i64> {
    (1..=DOCS)
        .filter(|id| changes == 0 || id % 3 != 0)
        .collect()
}

/// Asserts that `docs` holds exactly the rows it is to hold, and that the body and the meta
/// of each read back as a `Value::Text` and a `Value::Json` equal to what was put in. Returns
/// how many rows matched.
fn assert_docs_are(on: &impl Run, changes: usize, what: &str) -> u64 {
    let kept = kept(changes);
    let ids: Vec<Vec<Value>> = kept.iter().map(|id| vec![Value::Int64(*id)]).collect();
    support::assert_same_rows(
        &support::rows(on, "SELECT id FROM docs ORDER BY id"),
        &ids,
        &format!("{what}: the ids of docs"),
    );
    assert_eq!(
        support::int(on, "SELECT COUNT(*) FROM docs"),
        kept.len() as i64,
        "{what}: the kept count of docs"
    );
    for id in &kept {
        let what = format!("{what}: row {id}, {:?}", shape(*id));
        match answered(on, "SELECT body, meta FROM docs WHERE id = $id", *id) {
            Ok(rows) => match rows.as_slice() {
                [row] => {
                    support::assert_text_is(&row[0], &body(*id, changes), &format!("{what}: body"));
                    support::assert_json_is(&row[1], &meta(*id), &format!("{what}: meta"));
                }
                rows => panic!("{what}: {} rows were returned, not one", rows.len()),
            },
            Err(error) => panic!("{what}: the select was refused: {error:?}"),
        }
    }
    kept.len() as u64
}

/// Runs one statement for each row of `docs`, four rows to a transaction.
fn four_to_a_transaction(db: &Database, each: impl Fn(&contextdb::Transaction<'_>, i64)) {
    for first in (1..=DOCS).step_by(4) {
        let tx = db.transaction().expect("begin");
        for id in first..(first + 4).min(DOCS + 1) {
            each(&tx, id);
        }
        tx.commit().expect("commit four rows");
    }
}

#[test]
fn large_text_and_json_come_back_byte_for_byte() {
    if let Some(role) = support::role() {
        return reader_of_docs(&role);
    }
    let test = "large_text_and_json_come_back_byte_for_byte";
    let folder = support::folder("large-values-come-back");
    // Read through the writer, through the writer after close and reopen, and through a
    // reader in another process, which makes the same content from the ids.
    let three_ways = |db: Database, changes: usize| -> Database {
        assert_docs_are(&db, changes, "through the writer");
        support::close(db);
        let db = support::open(&folder);
        assert_docs_are(&db, changes, "after close and reopen");
        let role = format!("reader changes={changes}");
        let lines = Child::start(test, &role, &folder).end();
        assert_eq!(
            support::last_number(&lines, "matched"),
            Some(kept(changes).len() as u64),
            "the rows a reader in another process found equal"
        );
        db
    };

    let db = support::open(&folder);
    support::run(
        &db,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT, meta JSON)",
    );
    four_to_a_transaction(&db, |tx, id| {
        let values = params! { "id" => id, "body" => body(id, 0), "meta" => meta(id) };
        if let Err(error) = tx.execute(
            "INSERT INTO docs (id, body, meta) VALUES ($id, $body, $meta)",
            &values,
        ) {
            panic!("row {id}, {:?}, was refused: {error:?}", shape(id));
        }
    });
    let db = three_ways(db, 0);

    // What a loaded value is counted as does not depend on how it is stored. Rows 13 and 15
    // are alike but for their bodies, 4,000 bytes each: one repeats and is stored compressed,
    // the other is scattered and is stored as it is. A result that holds either row holds the
    // same number of bytes.
    assert_eq!(
        (shape(13), shape(15)),
        ((Kind::Repeating, 4000), (Kind::Scattered, 4000))
    );
    let held = |id: i64| {
        let before = db.memory_used();
        let result = db.execute("SELECT * FROM docs WHERE id = $id", &params! { "id" => id });
        let rows = result.as_ref().map_or(0, |result| result.rows().len());
        assert_eq!(rows, 1, "row {id} was not returned once");
        db.memory_used() - before
    };
    let (compressed, plain) = (held(13), held(15));
    assert!(
        compressed > 4000,
        "a held row of 4,000 bytes was counted as {compressed} bytes"
    );
    assert_eq!(
        compressed, plain,
        "a row whose body is stored compressed and one whose body is not are counted alike"
    );

    // Every body gives way to one of the next kind, so that a value stored compressed is
    // replaced by one stored as it is and the other way round; then every third row goes.
    four_to_a_transaction(&db, |tx, id| {
        let values = params! { "id" => id, "body" => body(id, 1) };
        match tx.execute("UPDATE docs SET body = $body WHERE id = $id", &values) {
            Ok(result) => assert_eq!(result.rows_affected(), 1, "the update of row {id}"),
            Err(error) => panic!("the update of row {id} was refused: {error:?}"),
        }
    });
    let tx = db.transaction().expect("begin");
    for id in (3..=DOCS).step_by(3) {
        let deleted = tx.execute("DELETE FROM docs WHERE id = $id", &params! { "id" => id });
        match deleted {
            Ok(result) => assert_eq!(result.rows_affected(), 1, "the delete of row {id}"),
            Err(error) => panic!("the delete of row {id} was refused: {error:?}"),
        }
    }
    tx.commit().expect("commit the deletes");
    assert_eq!(kept(1).len() as i64, DOCS - DOCS / 3);
    three_ways(db, 1);
}

/// Opens the store for reading, checks every row of `docs` against the content made from its
/// id, and announces `matched <rows>`.
fn reader_of_docs(role: &support::Role) {
    let reader = ReadSession::open(&role.folder).expect("open the store for reading");
    let changes = role.number("changes") as usize;
    let matched = assert_docs_are(&reader, changes, "a reader in another process");
    support::say(format!("matched {matched}"));
}

// ---- the room large values take in the data file ----

#[test]
fn a_value_that_does_not_shrink_is_kept_as_it_is() {
    // The bytes put in: 1,000 ids of 8 bytes and 1,000 bodies of 4,000 bytes. And the most
    // bytes the data file may have: what the same store takes with no compression at all.
    const PUT_IN: u64 = 4_008_000;
    const RECORDED: u64 = PUT_IN * 5 / 4; // set from the first run

    let folder = support::folder("does-not-shrink");
    // Scattered text does not shrink by an eighth, so every body is stored byte for byte as
    // it is. Nothing in the content, its order or the store depends on a clock, a random
    // number or a thread, so the file has the same length on every machine and every run.
    let put_in = store_of_notes(&folder, Kind::Scattered, 1000, 4000);
    assert_eq!(put_in, PUT_IN, "the bytes this test put in");
    let length = data_file(&folder);

    let db = support::open(&folder);
    assert_notes_are(&db, Kind::Scattered, 1000, 4000, "after close and reopen");
    assert!(
        length <= RECORDED,
        "the data file is {length} bytes long; the most recorded for this store is {RECORDED} \
         bytes, and {PUT_IN} bytes were put in"
    );
}

#[test]
fn a_store_of_repeating_text_is_smaller_than_the_text() {
    // The bytes put in: 1,500 ids of 8 bytes and 1,500 bodies of 6,000 bytes. And the most
    // bytes the data file may have.
    const PUT_IN: u64 = 9_012_000;
    const RECORDED: u64 = PUT_IN - 1; // set from the first run

    let folder = support::folder("smaller-than-the-text");
    // Words from a list of 64 repeat, so every body is stored compressed, to about half. The
    // length is the same on every machine and every run, as in the test above.
    let put_in = store_of_notes(&folder, Kind::Words, 1500, 6000);
    assert_eq!(put_in, PUT_IN, "the bytes this test put in");
    let length = data_file(&folder);

    let db = support::open(&folder);
    assert_notes_are(&db, Kind::Words, 1500, 6000, "after close and reopen");
    assert!(
        length < PUT_IN,
        "the data file is {length} bytes long, which is not shorter than the {PUT_IN} bytes of \
         repeating text that were put in"
    );
    assert!(
        length <= RECORDED,
        "the data file is {length} bytes long; the most recorded for this store is {RECORDED} \
         bytes, and {PUT_IN} bytes were put in"
    );
}

#[test]
fn a_text_longer_than_one_mebibyte_and_a_vector_are_stored_as_they_are() {
    // A little over 1 MiB of one sentence repeated. Compressed it would take a few kilobytes.
    const OVER: usize = 1_100_000;
    // What a reader's limit leaves beside the value: room for the row it comes in, and a
    // quarter of what a second copy of the value would need.
    const MARGIN: usize = 256 * KIB;
    const { assert!(OVER > LONGEST_COMPRESSED && MARGIN < LONGEST_COMPRESSED / 2) };

    // One byte over the longest value that is compressed, and a good deal over.
    for len in [LONGEST_COMPRESSED + 1, OVER] {
        // A limit that fits the value once and not twice: the 9 MiB an open sets aside, the
        // value's stored record of 9 + len bytes, and the margin.
        let once = 9 * MIB + 9 + len + MARGIN;
        let folder = support::folder(&format!("longer-than-one-mebibyte-{len}"));
        let body = support::large_text(Kind::Repeating, 1, len);
        let db = support::open(&folder);
        support::run(&db, NOTES);
        insert_note(&db, 1, &body);
        support::close(db);
        // The file holds the value's own bytes: it is at least as long as the value.
        let length = data_file(&folder);
        assert!(
            length >= len as u64,
            "the data file is {length} bytes long: too short to hold a text of {len} bytes as \
             it is, so a text longer than {LONGEST_COMPRESSED} bytes was stored compressed"
        );

        // It reads back under a limit that fits it once. `SELECT *` hands the row over as it
        // was read; a select list that names the column works its value out as a copy.
        let reader = read_limited(&folder, Some(once));
        assert_eq!(used(&reader), 9 * MIB, "set aside by the open");
        match answered(&reader, "SELECT * FROM notes WHERE id = $id", 1) {
            Ok(rows) => match rows.as_slice() {
                [row] => {
                    assert_eq!(row[0], Value::Int64(1));
                    support::assert_text_is(&row[1], &body, &format!("a text of {len} bytes"));
                }
                rows => panic!("{} rows were returned, not one", rows.len()),
            },
            Err(error) => panic!(
                "a text of {len} bytes was refused under a limit that fits it once: {error:?}"
            ),
        }
        assert_eq!(
            used(&reader),
            9 * MIB,
            "memory in use once the row is dropped"
        );
    }

    // A value of exactly 1 MiB of the same text is stored compressed: its data file is too
    // short to hold it as it is. It reads back equal all the same.
    let folder = support::folder("exactly-one-mebibyte");
    let body = support::large_text(Kind::Repeating, 1, LONGEST_COMPRESSED);
    let db = support::open(&folder);
    support::run(&db, NOTES);
    insert_note(&db, 1, &body);
    support::close(db);
    let length = data_file(&folder);
    assert!(
        length < LONGEST_COMPRESSED as u64,
        "the data file is {length} bytes long: a repeating text of {LONGEST_COMPRESSED} bytes \
         was stored as it is"
    );
    assert_note_is(&read_limited(&folder, None), 1, &body, "exactly 1 MiB");

    // A vector is never compressed, whatever it holds. A thousand vectors of 1,024 equal
    // numbers are 4,096,000 bytes that would shrink to a few kilobytes; each is kept beside
    // its row, and the file holds them as they are.
    const NUMBERS: usize = 1024;
    let folder = support::folder("vectors-as-they-are");
    let db = support::open(&folder);
    support::run(
        &db,
        &format!("CREATE TABLE shapes (id INTEGER PRIMARY KEY, v VECTOR({NUMBERS}))"),
    );
    let same = vec![0.5_f32; NUMBERS];
    for first in (1..=1000_i64).step_by(500) {
        let tx = db.transaction().expect("begin");
        for id in first..first + 500 {
            tx.execute(
                "INSERT INTO shapes (id, v) VALUES ($id, $v)",
                &params! { "id" => id, "v" => same.clone() },
            )
            .unwrap_or_else(|error| panic!("shape {id} was refused: {error:?}"));
        }
        tx.commit().expect("commit five hundred shapes");
    }
    support::close(db);
    let (length, put_in) = (data_file(&folder), 1000 * 4 * NUMBERS as u64);
    assert!(
        length >= put_in,
        "the data file is {length} bytes long: too short to hold {put_in} bytes of vectors as \
         they are, so a vector was stored compressed"
    );
    let reader = read_limited(&folder, None);
    let last = support::rows(&reader, "SELECT v FROM shapes WHERE id = 1000");
    assert!(
        last == [[Value::Vector(same)]],
        "the vector of shape 1000 did not read back as it was written"
    );
}

// ---- the memory limit ----

/// A new store whose `notes` holds row 1, with this body, and row 2, with the body `small`,
/// each its own commit; closed.
fn store_of_one_large_note(folder: &Path, body: &str) {
    let db = support::open(folder);
    support::run(&db, NOTES);
    insert_note(&db, 1, body);
    insert_note(&db, 2, "small");
    support::close(db);
}

#[test]
fn a_compressed_value_too_large_for_the_limit_is_refused() {
    // The largest value the store compresses: 1 MiB of one sentence repeated.
    const LEN: usize = LONGEST_COMPRESSED;
    // The limit: the 9 MiB an open sets aside and 64 KiB. Reading the value asks for its
    // stored record first, which for this text is a little over 4 KiB (an LZ4 block is never
    // under a 255th of what it decodes to) and has room sixteen times over; then for 9 + LEN
    // bytes, the value's real size, before any of its bytes is produced, and that is sixteen
    // times what is left. So the select is refused after the stored bytes are in hand.
    const ROOM: usize = 64 * KIB;
    const LIMIT: usize = 9 * MIB + ROOM;
    const { assert!(9 + LEN.div_ceil(255) < ROOM / 2 && ROOM < 9 + LEN) };

    let folder = support::folder("compressed-over-the-limit");
    let body = support::large_text(Kind::Repeating, 1, LEN);
    store_of_one_large_note(&folder, &body);
    // The value is stored compressed: the file is too short to hold it as it is.
    let length = data_file(&folder);
    assert!(
        length < LEN as u64,
        "the data file is {length} bytes long: a repeating text of {LEN} bytes was stored as \
         it is, and this test would show nothing about a compressed value"
    );

    // Through a reader with the limit, then through a writer with it.
    let refused = |on: &dyn Run, door: &str| {
        let before = used(&on);
        assert_eq!(before, 9 * MIB, "{door}: set aside by the open");
        // Asked twice: the first refusal left nothing counted, so the second is the same.
        let mut refusals = Vec::new();
        for _ in 0..2 {
            match answered(&on, "SELECT body FROM notes WHERE id = $id", 1) {
                Err(Error::MemoryBudgetExceeded {
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    ..
                }) => {
                    assert_eq!(operation, "query", "{door}");
                    assert_eq!(budget_limit_bytes, LIMIT as u64, "{door}");
                    refusals.push((requested_bytes, available_bytes));
                }
                Err(other) => panic!("{door}: the select was refused as {other:?}"),
                Ok(rows) => panic!("{door}: the select was answered with {} rows", rows.len()),
            }
            assert_eq!(used(&on), before, "{door}: memory in use after the refusal");
        }
        assert_eq!(
            refusals[0], refusals[1],
            "{door}: a refused read left memory counted"
        );
        // What was refused is the room for the value's real bytes, and by then the statement
        // held the stored record.
        let (requested, available) = refusals[0];
        assert_eq!(
            requested,
            9 + LEN as u64,
            "{door}: the bytes that were asked for and refused"
        );
        let held = (ROOM as u64).checked_sub(available);
        assert!(
            held.is_some_and(|held| held >= 9 + (LEN as u64).div_ceil(255)),
            "{door}: with {available} of {ROOM} bytes free, the statement did not hold the \
             stored record when it was refused"
        );
        // The rest of the table answers in the same session.
        assert_note_is(&on, 2, "small", door);
        assert_eq!(support::int(&on, "SELECT COUNT(*) FROM notes"), 2, "{door}");
        assert_eq!(used(&on), before, "{door}: memory in use at the end");
    };
    refused(&read_limited(&folder, Some(LIMIT)), "a reader");
    let db = open_limited(&folder, LIMIT);
    refused(&db, "the writer");
    assert_eq!(db.memory_used(), 9 * MIB);
    support::close(db);

    // The store is unharmed: a reader with no limit returns the value byte for byte.
    assert_note_is(&read_limited(&folder, None), 1, &body, "under no limit");
}

#[test]
fn a_commit_with_no_room_to_compress_is_written_plain() {
    const LEN: usize = LONGEST_COMPRESSED;
    // What the compressor works in for a value of LEN bytes: 20 bytes more than 1.1 times
    // the value. The commit asks the budget for it and, refused, writes the value as it is.
    const BUFFER: usize = LEN + LEN / 10 + 20;
    // The room the limit leaves at the commit: enough for whatever else a commit takes, and
    // less than half of the compressor's buffer.
    const ROOM: usize = 512 * KIB;
    const { assert!(BUFFER == 1_153_453 && ROOM + KIB < BUFFER / 2) };
    let body = support::large_text(Kind::Repeating, 1, LEN);

    // With no room. The row is staged under no limit; then the limit is set, through the
    // transaction, to what is in use, the row included, rounded up to whole KiB, and ROOM.
    let folder = support::folder("no-room-to-compress");
    let db = support::open(&folder);
    support::run(&db, NOTES);
    let tx = db.transaction().expect("begin");
    insert_note(&tx, 1, &body);
    let kib = db.memory_used().div_ceil(KIB) + ROOM / KIB;
    support::run(&tx, &format!("SET MEMORY_LIMIT '{kib}K'"));
    assert_eq!(db.memory_limit(), Some(kib * KIB), "the limit was taken");
    let free = kib * KIB - db.memory_used();
    assert!(
        (ROOM..ROOM + KIB).contains(&free),
        "{free} bytes are free at the commit, where {ROOM} were to be"
    );
    // Compression never refuses a commit that would be written without it.
    if let Err(error) = tx.commit() {
        panic!("a commit with {free} bytes free, too few to compress in, was refused: {error:?}");
    }
    support::close(db);
    // The value went in as it is: the file is longer than the value.
    let length = data_file(&folder);
    assert!(
        length > LEN as u64,
        "the data file is {length} bytes long: too short to hold a text of {LEN} bytes as it \
         is, so it was compressed in a buffer the budget had no room for"
    );
    // Read by a reader, which has no limit: the store remembers the one set above for its
    // next writer.
    let reader = read_limited(&folder, None);
    assert_note_is(&reader, 1, &body, "written with no room to compress");

    // With room: the same row, committed the same way with no limit set, is compressed.
    let folder = support::folder("room-to-compress");
    let db = support::open(&folder);
    support::run(&db, NOTES);
    let tx = db.transaction().expect("begin");
    insert_note(&tx, 1, &body);
    tx.commit().expect("commit under no limit");
    support::close(db);
    let length = data_file(&folder);
    assert!(
        length < LEN as u64,
        "the data file is {length} bytes long: with room to compress, a repeating text of \
         {LEN} bytes was stored as it is"
    );
    assert_note_is(&read_limited(&folder, None), 1, &body, "written with room");
}

// ---- damage ----

#[test]
fn a_damaged_length_of_a_compressed_value_is_reported_as_damage() {
    const LEN: usize = LONGEST_COMPRESSED;
    // A compressed text record begins with the byte 0x84 and then the length of the value in
    // 8 bytes, low byte first.
    const BEGINS: [u8; 9] = [0x84, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00];
    assert_eq!(BEGINS[1..], (LEN as u64).to_le_bytes());

    let folder = support::folder("damaged-length");
    let body = support::large_text(Kind::Repeating, 1, LEN);
    let db = support::open(&folder);
    support::run(&db, NOTES);
    insert_note(&db, 1, &body);
    support::close(db);

    // A length no store holds, one byte more than the value has, one byte less, half of it, none.
    for wrong in [
        u64::MAX,
        1_u64 << 40,
        LEN as u64 + 1,
        LEN as u64 - 1,
        LEN as u64 / 2,
        0,
    ] {
        // A storage door that, in every read, puts the wrong length after that 0x84.
        let changed = Arc::new(AtomicU64::new(0));
        let mut options = ReadSessionOptions::default();
        options.memory_limit = Some(64 * MIB);
        options.storage = Some(support::storage({
            let changed = changed.clone();
            move |call| {
                let Call::Read(_, bytes) = call else {
                    return;
                };
                let mut from = 0;
                while let Some(found) = bytes[from..]
                    .windows(BEGINS.len())
                    .position(|window| window == BEGINS)
                {
                    let at = from + found;
                    bytes[at + 1..at + 9].copy_from_slice(&wrong.to_le_bytes());
                    changed.fetch_add(1, Ordering::SeqCst);
                    from = at + BEGINS.len();
                }
            }
        }));
        let reader = ReadSession::open_with_options(&folder, options)
            .unwrap_or_else(|error| panic!("the store did not open for reading: {error:?}"));
        let before = used(&reader);

        // Damage, by name: never a panic, never the memory limit's refusal, never a value.
        // Two checks stand between the changed bytes and an answer, the checksum of the
        // page they are in and the record's own length against its stored bytes; whichever
        // meets them first, the statement is told the store is damaged.
        match answered(&reader, "SELECT body FROM notes WHERE id = $id", 1) {
            Err(Error::StoreCorrupted { path, .. }) => assert_eq!(path, folder),
            Err(other) => panic!("a recorded length of {wrong} was refused as {other:?}"),
            Ok(rows) => panic!(
                "a recorded length of {wrong} was answered with {} rows",
                rows.len()
            ),
        }
        assert_eq!(
            used(&reader),
            before,
            "memory in use after a recorded length of {wrong}"
        );
        // The door did change a length, or this test shows nothing.
        assert!(
            changed.load(Ordering::SeqCst) > 0,
            "no read of the data file held the nine bytes a compressed record of this value \
             begins with, so no length was changed to {wrong}"
        );
    }

    // Through an untouched door the value is as it was written.
    assert_note_is(&read_limited(&folder, None), 1, &body, "undamaged");
}

#[test]
fn a_large_text_selected_by_name_is_held_once() {
    // A text of about 900,000 bytes: under the 1 MiB up to which a text may be compressed.
    const LEN: usize = 900_000;
    // What a reader's limit leaves beside the value: room for the row it comes in and for
    // the stored bytes of a compressed value, and well under what a second copy would need.
    const MARGIN: usize = 256 * KIB;
    const { assert!(LEN < LONGEST_COMPRESSED && MARGIN < LEN / 2 && LEN / 255 < MARGIN / 2) };
    // A limit that fits the value once and not twice: the 9 MiB an open sets aside, the
    // value's record of 9 + LEN bytes, and the margin.
    let once = 9 * MIB + 9 + LEN + MARGIN;

    // Stored as it is (scattered text does not shrink) and stored compressed (one sentence
    // repeated shrinks to a few kilobytes): the statement holds the value once either way.
    for (kind, name, as_it_is) in [
        (Kind::Scattered, "plain", true),
        (Kind::Repeating, "compressed", false),
    ] {
        let folder = support::folder(&format!("selected-by-name-{name}"));
        let body = support::large_text(kind, 1, LEN);
        store_of_one_large_note(&folder, &body);
        let length = data_file(&folder);
        assert_eq!(
            length >= LEN as u64,
            as_it_is,
            "the data file of a {kind:?} text of {LEN} bytes is {length} bytes long"
        );

        let reader = read_limited(&folder, Some(once));
        assert_eq!(used(&reader), 9 * MIB, "set aside by the open");
        // The select list names the column. The value is one text, whichever way the
        // statement asks for it, and a limit with room for it once is room enough.
        match answered(&reader, "SELECT body FROM notes WHERE id = $id", 1) {
            Ok(rows) => match rows.as_slice() {
                [row] => {
                    assert_eq!(row.len(), 1, "the columns of the row");
                    support::assert_text_is(&row[0], &body, &format!("a {kind:?} text by name"));
                }
                rows => panic!("{} rows were returned, not one", rows.len()),
            },
            Err(error) => panic!(
                "a {kind:?} text of {LEN} bytes, selected by name, was refused under a limit \
                 that fits it once: {error:?}"
            ),
        }
        assert_eq!(
            used(&reader),
            9 * MIB,
            "memory in use once the row is dropped"
        );
        // The small row beside it answers too, and the count.
        assert_note_is(&reader, 2, "small", "under the same limit");
    }
}
