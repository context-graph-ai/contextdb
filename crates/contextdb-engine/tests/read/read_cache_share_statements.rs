//! A read session declares, inspects and resets its read cache share in SQL.
//!
//! Promise: `SET READ_CACHE_SHARE = <percent>` declares, for this session
//! only, the percent of the reader's memory budget, after its fixed source
//! reservations, that its page cache takes. 0 turns the file cache off for a
//! streaming reader. A high share whose cache still leaves room for the
//! reader's own prepaid index and decoded state is accepted; 100 asks for the
//! whole remaining budget, which total admission refuses with the typed
//! memory-budget refusal. A share above 100 is refused as out of range. Either
//! refusal keeps the previous declaration, and inspection reports it.
//! `SET READ_CACHE_SHARE = DEFAULT` returns to the cache derived from the
//! budget. `SHOW READ_CACHE_SHARE` reports the declared value (`default` when
//! none is declared) beside the effective cache size in bytes and as a percent
//! (`Float64`), and the session's last refused declaration (`last_refusal`,
//! `Null` when there has been none). A declaration takes effect immediately,
//! in both directions: every statement after it reads under the new share. It
//! is not persisted and is not seen by any other session. Shrinking the share
//! while a cursor is open
//! does not disturb the cursor: it keeps the pages it holds and finishes every
//! row at the snapshot it opened on.

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
    last_refusal: Value,
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
        Value::Float64(percent) => percent,
        ref other => panic!("effective_percent is a Float64 percent, got {other:?}"),
    };
    Share {
        declared: row[column("declared")].clone(),
        effective_bytes,
        effective_percent,
        last_refusal: row[column("last_refusal")].clone(),
    }
}

/// A declaration the session must refuse; the refusal comes back as itself.
fn refused(reader: &ReadSession, value: &str) -> contextdb_core::Error {
    reader
        .execute(&format!("SET READ_CACHE_SHARE = {value}"), &HashMap::new())
        .map(drop)
        .expect_err("this declaration is refused")
}

/// The text of the session's last refused declaration, as inspection reports it.
fn last_refusal(share: &Share) -> &str {
    match &share.last_refusal {
        Value::Text(text) => text,
        other => panic!("SHOW READ_CACHE_SHARE reports the last refusal as text, got {other:?}"),
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
        (
            undeclared.declared,
            undeclared.effective_bytes,
            undeclared.last_refusal
        ),
        (
            Value::Text("default".to_owned()),
            derived_default(),
            Value::Null
        ),
        "an undeclared session reports the default share, the derived cache size and no refusal"
    );
    set(&reader, "40").expect("a session declares a 40% read cache share");
    assert_eq!(
        show(&reader),
        Share {
            declared: Value::Int64(40),
            effective_bytes: declared_bytes(40) as i64,
            effective_percent: 40.0,
            last_refusal: Value::Null,
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

/// 0 is accepted with no file cache and a high share of 90 with its exact
/// cache size. 100 asks for the whole budget left after the fixed reservations
/// and is refused by total admission with the typed memory-budget refusal,
/// because nothing would remain for the reader's prepaid index and decoded
/// state. 101 is refused as out of range. Each refusal keeps the previous
/// declaration, and inspection reports the refused value.
#[test]
fn read_cache_shares_up_to_the_budget_are_accepted_and_100_and_above_refused() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("range.db");
    seed(&path, 500);
    let reader = open(&path);
    set(&reader, "0").expect("a streaming session declares no file cache");
    let off = show(&reader);
    assert_eq!((off.declared, off.effective_bytes), (Value::Int64(0), 0));
    set(&reader, "90").expect("a session declares a 90% read cache share");
    let high = show(&reader);
    assert_eq!(
        (high.declared, high.effective_bytes),
        (Value::Int64(90), declared_bytes(90) as i64),
        "a 90% share is accepted with the cache size it gives under this budget"
    );
    set(&reader, "40").expect("a session declares a 40% read cache share");

    let whole_budget = refused(&reader, "100");
    assert!(
        matches!(
            whole_budget,
            contextdb_core::Error::MemoryBudgetExceeded { .. }
        ),
        "a 100% share leaves nothing for the reader's prepaid index and decoded state, so \
         total admission refuses it with the typed memory-budget refusal: {whole_budget:?}"
    );
    let after_budget_refusal = show(&reader);
    assert_eq!(
        after_budget_refusal.declared,
        Value::Int64(40),
        "a refused 100% declaration leaves the previous one in place"
    );
    assert!(
        last_refusal(&after_budget_refusal).contains("100"),
        "inspection reports the refused 100% declaration: {:?}",
        after_budget_refusal.last_refusal
    );

    let out_of_range = refused(&reader, "101");
    assert!(
        !matches!(out_of_range, contextdb_core::Error::ParseError(_)),
        "SET READ_CACHE_SHARE = 101 is a statement the session understands and refuses, \
         not one it cannot parse: {out_of_range:?}"
    );
    let message = out_of_range.to_string();
    assert!(
        message.contains("READ_CACHE_SHARE") && message.contains("100"),
        "the out-of-range refusal names READ_CACHE_SHARE and its 0 to 100 range: {message}"
    );
    let after_range_refusal = show(&reader);
    assert_eq!(
        after_range_refusal.declared,
        Value::Int64(40),
        "a refused 101% declaration leaves the previous one in place"
    );
    assert!(
        last_refusal(&after_range_refusal).contains("101"),
        "inspection reports the refused 101% declaration: {:?}",
        after_range_refusal.last_refusal
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
