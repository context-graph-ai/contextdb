//! A session's read cache share follows the session wherever it reads.
//!
//! Promise: a share declared on an open session governs every file cache the
//! session reads through after the change, including the file reader an
//! owner-route cursor opens later to continue past its first page, and the
//! file reader the session selects when its writer closes. A cursor opened
//! before a shrink therefore continues at the new, smaller share, exactly as
//! if it had been opened at that share. A session whose writer closes keeps
//! its declared share on the file route it selects, so a repeated query there
//! reads nothing from the file and the session still reports what it declared.
//!
//! The oracle is the bytes the storage backend returns on the calling thread
//! while the file reader answers. Nothing here measures time.

#![cfg(unix)]

#[path = "read/reader_proof_support.rs"]
#[allow(dead_code)]
mod reader_proof_support;

use contextdb_core::Value;
use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use reader_proof_support as fixture;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MIB: usize = 1024 * 1024;
const READER_BUDGET: usize = 256 * MIB;
const PAYLOAD_BYTES: usize = 4_096;
/// Rows behind the owner cursor: its file continuation reads them in pages.
const CURSOR_ROWS: usize = 1_000;
const CURSOR_PAGE_ROWS: u64 = 100;
/// Rows behind the turnover guard's repeated query.
const TURNOVER_ROWS: usize = 2_000;
const ROWS_PER_QUERY: usize = 1_000;
const SELECT_ALL: &str = "SELECT id, payload FROM cache_share ORDER BY id";

fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(PAYLOAD_BYTES / 8)
}

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// A writer holding a store of `rows` rows, left open so readers select the
/// owner route.
fn seeded_writer(path: &Path, rows: usize) -> Database {
    let writer = Database::open(path).unwrap();
    writer
        .execute(
            "CREATE TABLE cache_share (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .unwrap();
    for start in (0..rows).step_by(500) {
        let tx = writer.begin().unwrap();
        for row in start..(start + 500).min(rows) {
            writer
                .execute_in_tx(
                    tx,
                    "INSERT INTO cache_share VALUES ($id, $payload)",
                    &params([
                        ("id", Value::Int64(row as i64)),
                        ("payload", Value::Text(payload(row))),
                    ]),
                )
                .unwrap();
        }
        writer.commit(tx).unwrap();
    }
    writer
}

fn session_beside_the_writer(path: &Path, read_cache_share: Option<u8>) -> ReadSession {
    let session = ReadSession::open_with_observations_for_test(
        path,
        ReadSessionOptions {
            memory_limit: Some(READER_BUDGET),
            read_cache_share,
            limits: ReadLimits {
                memory: READER_BUDGET as u64,
                cursor_page_rows: CURSOR_PAGE_ROWS,
                cursor_page_bytes: 4 * MIB as u64,
                ..fixture::limits()
            },
            ..Default::default()
        },
        Arc::new(ManualDeadlineClock::at(10_000)),
        None,
        None,
    )
    .expect("a session opens beside the writer");
    assert_eq!(session.route(), ReadRoute::Owner);
    session
}

/// Bytes the storage backend returns on this thread while `read` runs.
fn backend_bytes<T>(read: impl FnOnce() -> T) -> (T, u64) {
    let bytes = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&bytes);
    let observation = redb::observe_backend_reads(Arc::new(move |read| {
        observed.fetch_add(read as u64, Ordering::SeqCst);
    }));
    let result = read();
    drop(observation);
    (result, bytes.load(Ordering::SeqCst))
}

fn declared_bytes(percent: usize) -> usize {
    let fixed = contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test(
        Some(READER_BUDGET),
    )
    .1;
    (READER_BUDGET - fixed) * percent / 100
}

/// What an owner-route cursor's file continuation reads: the cursor opens
/// beside the writer at `opened_at`, the session then declares `changed_to`
/// if given, the writer closes, and the cursor is read to its end through the
/// file. Returns the continuation's backend bytes, every row in order, and the
/// session's reported effective cache size afterwards.
fn owner_cursor_continuation(
    opened_at: u8,
    changed_to: Option<u8>,
) -> (u64, Vec<Vec<Value>>, usize) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner-cursor.db");
    let writer = seeded_writer(&path, CURSOR_ROWS);
    let session = session_beside_the_writer(&path, Some(opened_at));
    let mut cursor = session
        .open_cursor(SELECT_ALL, &HashMap::new())
        .expect("an owner cursor opens beside the writer");
    let mut rows: Vec<Vec<Value>> = cursor.first_page().rows().to_vec();
    assert!(
        cursor.first_page().has_more(),
        "the cursor continues past its first page"
    );
    if let Some(percent) = changed_to {
        session
            .set_read_cache_share(Some(percent))
            .expect("an open session changes its read cache share");
    }
    writer.close().unwrap();
    let ((), bytes) = backend_bytes(|| {
        loop {
            let page = cursor
                .fetch(None)
                .expect("the cursor continues through the file after the writer closes");
            rows.extend(page.rows().iter().cloned());
            if !page.has_more() {
                break;
            }
        }
    });
    let effective = session.read_cache_share_status().effective_bytes;
    drop(cursor);
    (bytes, rows, effective)
}

fn all_rows(rows: usize) -> Vec<Vec<Value>> {
    (0..rows)
        .map(|row| vec![Value::Int64(row as i64), Value::Text(payload(row))])
        .collect()
}

/// An owner cursor opened at 40% whose session then turns its file cache off
/// continues through the file with no cache: its continuation reads exactly
/// what a continuation opened at 0% reads, not what one at 40% reads.
#[test]
fn an_owner_cursor_continues_without_a_file_cache_after_its_session_turns_it_off() {
    let (opened_at_40, _, _) = owner_cursor_continuation(40, None);
    let (opened_at_0, _, _) = owner_cursor_continuation(0, None);
    let (turned_off, rows, effective) = owner_cursor_continuation(40, Some(0));
    assert_eq!(
        rows,
        all_rows(CURSOR_ROWS),
        "the cursor returns every row once"
    );
    assert_eq!(effective, 0, "the session reports no file cache");
    assert_ne!(
        opened_at_0, opened_at_40,
        "the fixture must distinguish a continuation with no file cache from one at 40%"
    );
    assert_eq!(
        turned_off, opened_at_0,
        "after the session turned its file cache off, the cursor's file continuation must read \
         what a continuation opened with no file cache reads ({opened_at_0} bytes), not what one \
         at the 40% share captured when the cursor opened reads ({opened_at_40} bytes); it read \
         {turned_off}"
    );
}

/// An owner cursor opened at 40% whose session then shrinks to 2% continues
/// at 2%: its continuation reads exactly what a continuation opened at 2%
/// reads.
#[test]
fn an_owner_cursor_continues_at_the_smaller_share_after_its_session_shrinks() {
    let (opened_at_40, _, _) = owner_cursor_continuation(40, None);
    let (opened_at_2, _, _) = owner_cursor_continuation(2, None);
    let (shrunk, rows, effective) = owner_cursor_continuation(40, Some(2));
    assert_eq!(
        rows,
        all_rows(CURSOR_ROWS),
        "the cursor returns every row once"
    );
    assert_eq!(
        effective,
        declared_bytes(2),
        "the session reports the 2% cache"
    );
    assert_ne!(
        opened_at_2, opened_at_40,
        "the fixture must distinguish a continuation at 2% from one at 40%"
    );
    assert_eq!(
        shrunk, opened_at_2,
        "after the session shrank to 2%, the cursor's file continuation must read what a \
         continuation opened at 2% reads ({opened_at_2} bytes), not what one at the 40% share \
         captured when the cursor opened reads ({opened_at_40} bytes); it read {shrunk}"
    );
}

/// A session that declared 40% beside its writer keeps that share on the file
/// route it selects when the writer closes: the first query there reads the
/// working set, the same query again reads nothing from the file, and the
/// session still reports 40%.
#[test]
fn a_session_keeps_its_declared_share_on_the_file_route_it_selects_when_its_writer_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("turnover.db");
    let writer = seeded_writer(&path, TURNOVER_ROWS);
    let session = session_beside_the_writer(&path, None);
    session
        .set_read_cache_share(Some(40))
        .expect("a session declares a 40% read cache share beside its writer");
    let round = || {
        for start in (0..TURNOVER_ROWS).step_by(ROWS_PER_QUERY) {
            let result = session
                .execute(
                    "SELECT id, payload FROM cache_share WHERE id >= $lo AND id < $hi ORDER BY id",
                    &params([
                        ("lo", Value::Int64(start as i64)),
                        ("hi", Value::Int64((start + ROWS_PER_QUERY) as i64)),
                    ]),
                )
                .expect("the session answers on whichever route it selected");
            assert_eq!(result.rows().len(), ROWS_PER_QUERY);
            assert_eq!(
                result.rows()[0],
                vec![Value::Int64(start as i64), Value::Text(payload(start))]
            );
        }
    };
    round();
    writer.close().unwrap();
    let ((), first) = backend_bytes(round);
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "with its writer closed the session reads through the file"
    );
    let ((), repeated) = backend_bytes(round);
    let status = session.read_cache_share_status();
    assert_eq!(
        (status.declared, status.effective_bytes),
        (Some(40), declared_bytes(40)),
        "the session still reports the share it declared"
    );
    assert!(
        first as usize >= TURNOVER_ROWS * PAYLOAD_BYTES,
        "the first query on the file route reads at least the row payload (read {first})"
    );
    assert!(
        first as usize <= declared_bytes(40),
        "the working set the first query read ({first} bytes of file pages) must fit the \
         declared 40% share ({} bytes)",
        declared_bytes(40)
    );
    assert_eq!(
        repeated, 0,
        "the same query again on the file route must read nothing from the file under the \
         declared 40% share: the first read {first} bytes, the repeat {repeated}"
    );
}
