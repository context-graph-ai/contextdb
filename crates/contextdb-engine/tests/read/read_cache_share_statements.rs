//! A read session declares, inspects and resets its read cache share in SQL.
//!
//! Promise: `SET READ_CACHE_SHARE = <percent>` declares, for this session
//! only, the percent of the reader's memory budget, after its fixed source
//! reservations, that its page cache takes. 0 turns the file cache off for a
//! streaming reader and 100 gives it everything after those reservations.
//! `SET READ_CACHE_SHARE = DEFAULT` returns to the cache derived from the
//! budget. `SHOW READ_CACHE_SHARE` reports the declared value (`default` when
//! none is declared) beside the effective cache size in bytes and as a
//! percent. A declaration takes effect at the next statement boundary in both
//! directions, is not persisted, and is not seen by any other session. A share
//! above 100 is refused and leaves the previous declaration in place.
//! Shrinking the share while a cursor is open does not disturb the cursor:
//! it finishes every row at the snapshot it opened on.

use super::reader_proof_support as fixture;
use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MIB: usize = 1024 * 1024;
const READER_BUDGET: usize = 256 * MIB;
const ROWS: usize = 6_000;
const PAYLOAD_BYTES: usize = 4_096;
const ROWS_PER_QUERY: usize = 1_500;
const CURSOR_PAGE_ROWS: usize = 500;

fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(PAYLOAD_BYTES / 8)
}

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn seed(path: &std::path::Path, rows: usize) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE cache_share (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..rows).step_by(500) {
        let tx = db.begin().unwrap();
        for row in start..(start + 500).min(rows) {
            db.execute_in_tx(
                tx,
                "INSERT INTO cache_share VALUES ($id, $payload)",
                &params([
                    ("id", Value::Int64(row as i64)),
                    ("payload", Value::Text(payload(row))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    db.close().unwrap();
}

fn open(path: &std::path::Path) -> ReadSession {
    fixture::open_file(
        path,
        ReadSessionOptions {
            memory_limit: Some(READER_BUDGET),
            limits: ReadLimits {
                memory: READER_BUDGET as u64,
                cursor_page_rows: CURSOR_PAGE_ROWS as u64,
                cursor_page_bytes: 4 * MIB as u64,
                ..fixture::limits()
            },
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    )
}

/// Declare a share; a refusal comes back as its message.
fn set(reader: &ReadSession, value: &str) -> Result<(), String> {
    reader
        .execute(&format!("SET READ_CACHE_SHARE = {value}"), &HashMap::new())
        .map(drop)
        .map_err(|error| format!("{error:?}"))
}

/// One `SHOW READ_CACHE_SHARE` row.
#[derive(Debug, PartialEq)]
struct Share {
    declared: Value,
    effective_bytes: i64,
    effective_percent: f64,
}

fn show(reader: &ReadSession) -> Share {
    let result = reader
        .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
        .expect("a read session inspects its read cache share");
    let column = |name: &str| {
        result
            .columns()
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| {
                panic!(
                    "SHOW READ_CACHE_SHARE has a {name} column: {:?}",
                    result.columns()
                )
            })
    };
    assert_eq!(result.rows().len(), 1, "one row describes the session");
    let row = &result.rows()[0];
    let effective_bytes = match row[column("effective_bytes")] {
        Value::Int64(bytes) => bytes,
        ref other => panic!("effective_bytes is a byte count, got {other:?}"),
    };
    let effective_percent = match row[column("effective_percent")] {
        Value::Int64(percent) => percent as f64,
        Value::Float64(percent) => percent,
        ref other => panic!("effective_percent is a number, got {other:?}"),
    };
    Share {
        declared: row[column("declared")].clone(),
        effective_bytes,
        effective_percent,
    }
}

fn derived_default() -> i64 {
    read_source_cache_reservation_for_limit_for_test(Some(READER_BUDGET)).0 as i64
}

/// The cache a declared percent gives: that share of the budget left after
/// the reader's fixed source reservations.
fn declared_bytes(percent: usize) -> usize {
    let fixed = read_source_cache_reservation_for_limit_for_test(Some(READER_BUDGET)).1;
    (READER_BUDGET - fixed) * percent / 100
}

fn round(reader: &ReadSession, rows: usize) -> u64 {
    let bytes = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&bytes);
    let observation = redb::observe_backend_reads(Arc::new(move |read| {
        observed.fetch_add(read as u64, Ordering::SeqCst);
    }));
    for start in (0..rows).step_by(ROWS_PER_QUERY) {
        let result = reader
            .execute(
                "SELECT id, payload FROM cache_share WHERE id >= $lo AND id < $hi ORDER BY id",
                &params([
                    ("lo", Value::Int64(start as i64)),
                    ("hi", Value::Int64((start + ROWS_PER_QUERY) as i64)),
                ]),
            )
            .unwrap();
        assert_eq!(result.rows().len(), ROWS_PER_QUERY.min(rows - start));
    }
    drop(observation);
    bytes.load(Ordering::SeqCst)
}

/// A session starts at the derived default, declares a share, sees it with
/// its effective size in bytes and as a percent, and returns to the default.
#[test]
fn a_declared_read_cache_share_is_inspected_as_declared_and_effective() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("inspect.db");
    seed(&path, 500);
    let reader = open(&path);
    let undeclared = show(&reader);
    assert_eq!(
        (undeclared.declared, undeclared.effective_bytes),
        (Value::Text("default".to_owned()), derived_default()),
        "an undeclared session reports the default share and the derived cache size"
    );
    set(&reader, "40").expect("a session declares a 40% read cache share");
    assert_eq!(
        show(&reader),
        Share {
            declared: Value::Int64(40),
            effective_bytes: declared_bytes(40) as i64,
            effective_percent: 40.0,
        },
        "a declared share is reported with the cache size it gives under this budget"
    );
    set(&reader, "DEFAULT").expect("a session returns to the default share");
    let reset = show(&reader);
    assert_eq!(
        (reset.declared, reset.effective_bytes),
        (Value::Text("default".to_owned()), derived_default())
    );
}

/// A declaration belongs to the session that made it: another session on
/// the same store, and a reopened session, start at the default.
#[test]
fn a_read_cache_share_is_scoped_to_its_session_and_not_persisted() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("scoped.db");
    seed(&path, 500);
    let declaring = open(&path);
    set(&declaring, "40").expect("a session declares a 40% read cache share");
    let other = open(&path);
    assert_eq!(
        show(&other).declared,
        Value::Text("default".to_owned()),
        "another session does not see the declaration"
    );
    drop(declaring);
    drop(other);
    let reopened = open(&path);
    assert_eq!(
        show(&reopened).declared,
        Value::Text("default".to_owned()),
        "the declaration is not persisted"
    );
}

/// The whole range 0-100 is accepted, 0 giving no file cache and 100 the
/// whole budget after the fixed reservations; a share above 100 is refused
/// and the session keeps what it had declared before.
#[test]
fn read_cache_shares_from_0_to_100_are_accepted_and_above_100_refused() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("range.db");
    seed(&path, 500);
    let reader = open(&path);
    set(&reader, "0").expect("a streaming session declares no file cache");
    let off = show(&reader);
    assert_eq!((off.declared, off.effective_bytes), (Value::Int64(0), 0));
    set(&reader, "100").expect("a session declares its whole remaining budget");
    let whole = show(&reader);
    assert_eq!(
        (whole.declared, whole.effective_bytes),
        (Value::Int64(100), declared_bytes(100) as i64)
    );
    set(&reader, "40").expect("a session declares a 40% read cache share");
    assert!(
        set(&reader, "101").is_err(),
        "a share above 100% is refused"
    );
    assert_eq!(
        show(&reader).declared,
        Value::Int64(40),
        "a refused declaration leaves the previous one in place"
    );
}

/// The statements after a declaration run under it, whichever way it moves
/// the cache: at 0 and at 2% a repeated round reads the working set again, at
/// 40% it reads nothing, and back at 2% it reads again.
#[test]
fn a_read_cache_share_declared_mid_session_is_honoured_by_the_statements_after_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("honoured.db");
    seed(&path, ROWS);
    let reader = open(&path);
    set(&reader, "0").expect("a streaming session declares no file cache");
    round(&reader, ROWS);
    let off = round(&reader, ROWS);
    set(&reader, "2").expect("a session declares a 2% read cache share");
    round(&reader, ROWS);
    let small = round(&reader, ROWS);
    set(&reader, "40").expect("a session declares a 40% read cache share");
    let primed = round(&reader, ROWS);
    let large = round(&reader, ROWS);
    set(&reader, "2").expect("a session declares a 2% read cache share again");
    round(&reader, ROWS);
    let shrunk = round(&reader, ROWS);
    assert!(
        off as usize >= ROWS * PAYLOAD_BYTES,
        "with no file cache every round reads the rows from the file again (read {off})"
    );
    assert!(
        small as usize >= ROWS * PAYLOAD_BYTES / 2,
        "at a 2% share a repeated round reads much of the working set again (read {small})"
    );
    assert!(
        primed as usize <= declared_bytes(40),
        "the working set one round reads ({primed} bytes of file pages) must fit the declared \
         40% share ({} bytes)",
        declared_bytes(40)
    );
    assert_eq!(
        large, 0,
        "at a 40% share a repeated round reads nothing from the file (2% read {small})"
    );
    assert!(
        shrunk as usize >= ROWS * PAYLOAD_BYTES / 2,
        "back at a 2% share the cache no longer holds the working set (read {shrunk})"
    );
}

/// Shrinking the share while a cursor is open leaves the cursor intact: it
/// still returns every row at the snapshot it opened on, although a writer
/// changed a row it had not reached yet. Once it finishes, the session runs
/// at the smaller cache.
#[test]
fn shrinking_the_read_cache_share_under_an_open_cursor_keeps_its_rows_and_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("shrink-under-cursor.db");
    seed(&path, ROWS);
    let reader = open(&path);
    set(&reader, "40").expect("a session declares a 40% read cache share");
    round(&reader, ROWS);
    let mut cursor = reader
        .open_cursor(
            "SELECT id, payload FROM cache_share ORDER BY id",
            &HashMap::new(),
        )
        .expect("a cursor opens over the warm working set");
    let mut seen: Vec<Vec<Value>> = cursor.first_page().rows().to_vec();
    assert!(
        seen.len() < ROWS,
        "the cursor is still open after its first page"
    );

    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "UPDATE cache_share SET payload = 'changed after the cursor opened' WHERE id = $id",
            &params([("id", Value::Int64((ROWS - 1) as i64))]),
        )
        .unwrap();
    writer.close().unwrap();

    set(&reader, "2").expect("a session shrinks its read cache share under an open cursor");
    loop {
        let page = cursor
            .fetch(None)
            .expect("an open cursor keeps reading after its session's share shrinks");
        seen.extend(page.rows().iter().cloned());
        if !page.has_more() {
            break;
        }
    }
    drop(cursor);
    let expected: Vec<Vec<Value>> = (0..ROWS)
        .map(|row| vec![Value::Int64(row as i64), Value::Text(payload(row))])
        .collect();
    assert!(
        seen == expected,
        "the cursor returns every row once, at the snapshot it opened on ({} rows seen)",
        seen.len()
    );
    assert_eq!(
        show(&reader).effective_bytes,
        declared_bytes(2) as i64,
        "once the cursor finishes, the session runs at the smaller cache"
    );
}
