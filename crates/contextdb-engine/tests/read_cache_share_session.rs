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
//! while the file reader answers. Byte counts are compared only between
//! byte-identical copies of one seeded store, because two stores built
//! separately lay their pages out a few pages apart. Nothing here measures time.

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
/// The owner route answers at most 500 rows per statement.
const ROWS_PER_QUERY: usize = 500;
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

/// Seed one store under `directory` and close its writer; every configuration
/// of a test reads a byte-identical copy of it.
fn seeded_store(directory: &Path, rows: usize) {
    seeded_writer(&directory.join("owner-cursor.db"), rows)
        .close()
        .unwrap();
}

/// Copy every file of the seeded store, so each configuration starts from the
/// same bytes and the same page layout.
fn copy_store(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_store(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
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

/// What an owner-route cursor's file continuation reads on a fresh copy of the
/// seeded store: a writer opens the copy, the cursor opens beside it at
/// `opened_at`, the session then declares `changed_to` if given, the writer
/// closes, and the cursor is read to its end through the file. Returns the
/// continuation's backend bytes, every row in order, and the session's
/// reported effective cache size afterwards.
fn owner_cursor_continuation(
    seeded: &Path,
    opened_at: u8,
    changed_to: Option<u8>,
) -> (u64, Vec<Vec<Value>>, usize) {
    let directory = tempfile::tempdir().unwrap();
    copy_store(seeded, directory.path());
    let path = directory.path().join("owner-cursor.db");
    let writer = Database::open(&path).unwrap();
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
    let effective = session
        .read_cache_share_status()
        .expect("the session inspects its read cache share")
        .effective_bytes;
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
/// what a continuation opened at 0% on the same store reads, not what one at
/// 40% reads.
#[test]
fn an_owner_cursor_continues_without_a_file_cache_after_its_session_turns_it_off() {
    let seeded = tempfile::tempdir().unwrap();
    seeded_store(seeded.path(), CURSOR_ROWS);
    let (opened_at_40, _, _) = owner_cursor_continuation(seeded.path(), 40, None);
    let (opened_at_0, _, _) = owner_cursor_continuation(seeded.path(), 0, None);
    let (turned_off, rows, effective) = owner_cursor_continuation(seeded.path(), 40, Some(0));
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
/// at 2%: its continuation reads exactly what a continuation opened at 2% on
/// the same store reads.
#[test]
fn an_owner_cursor_continues_at_the_smaller_share_after_its_session_shrinks() {
    let seeded = tempfile::tempdir().unwrap();
    seeded_store(seeded.path(), CURSOR_ROWS);
    let (opened_at_40, _, _) = owner_cursor_continuation(seeded.path(), 40, None);
    let (opened_at_2, _, _) = owner_cursor_continuation(seeded.path(), 2, None);
    let (shrunk, rows, effective) = owner_cursor_continuation(seeded.path(), 40, Some(2));
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
    let status = session
        .read_cache_share_status()
        .expect("the session inspects its read cache share");
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

/// A file reader's page cache, the index prepaid to track it and its fixed
/// source reservations together fit inside the session ceiling that bounds the
/// reader's loaded state. This holds when the ceiling equals the read limit
/// (16 MiB, 64 MiB) and when a 256 MiB ceiling sits above a 16 MiB or 64 MiB
/// read limit, which bounds each read's working memory instead.
#[test]
fn a_readers_cache_and_its_index_fit_inside_its_session_ceiling() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("spendable.db");
    seeded_writer(&path, 200).close().unwrap();
    for (ceiling, read_limit) in [
        (16 * MIB, 16 * MIB),
        (64 * MIB, 64 * MIB),
        (256 * MIB, 16 * MIB),
        (256 * MIB, 64 * MIB),
    ] {
        let clock: Arc<dyn contextdb_core::read_contract::DeadlineClock> =
            Arc::new(ManualDeadlineClock::default());
        let session = ReadSession::open_with_observations_for_test(
            &path,
            ReadSessionOptions {
                memory_limit: Some(ceiling),
                limits: ReadLimits {
                    memory: read_limit as u64,
                    ..fixture::limits()
                },
                ..Default::default()
            },
            clock,
            None,
            None,
        )
        .unwrap_or_else(|refusal| {
            panic!(
                "a reader with a {ceiling}-byte ceiling and a {read_limit}-byte read limit must \
                 open its file reader: {refusal:?}"
            )
        });
        assert_eq!(session.route(), ReadRoute::File);
        let status = session
            .read_cache_share_status()
            .expect("the session inspects its read cache share");
        let prepaid = status
            .prepaid_index_bytes
            .expect("an open file reader reports the index prepaid for its cache");
        assert!(
            status.effective_bytes + prepaid + status.fixed_reservation_bytes <= ceiling,
            "a reader with a {ceiling}-byte ceiling and a {read_limit}-byte read limit may hold \
             {ceiling} bytes of loaded state, but its cache ({}), prepaid index ({prepaid}) and \
             fixed reservations ({}) need more",
            status.effective_bytes,
            status.fixed_reservation_bytes
        );
    }
}

/// After its writer closes, a session that has run no statement since still
/// inspects and resizes the cache of the file route it now reads through: its
/// status reports that file reader's cache, not the departed owner route, and
/// a share it declares then is the file reader's share.
#[test]
fn after_its_writer_closes_a_session_inspects_and_resizes_its_file_route_before_any_statement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("turnover-status.db");
    let writer = seeded_writer(&path, TURNOVER_ROWS);
    let session = session_beside_the_writer(&path, None);
    session
        .execute(
            "SELECT id FROM cache_share WHERE id = $id",
            &params([("id", Value::Int64(0))]),
        )
        .expect("the session answers beside its writer");
    writer.close().unwrap();

    let status = session
        .read_cache_share_status()
        .expect("the session inspects its read cache share");
    assert!(
        status.prepaid_index_bytes.is_some() && status.held_page_bytes.is_some(),
        "with its writer gone, the session's cache inspection reports the file reader it now \
         reads through, not the departed owner route: {status:?}"
    );
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "inspecting the cache moved the session onto the file route"
    );
    session
        .set_read_cache_share(Some(40))
        .expect("the session declares a 40% share on its file route");
    let status = session
        .read_cache_share_status()
        .expect("the session inspects its read cache share");
    assert_eq!(
        (status.declared, status.effective_bytes),
        (Some(40), declared_bytes(40)),
        "the file route's cache is the declared 40%"
    );
    assert!(
        status.prepaid_index_bytes.is_some(),
        "the file reader's prepaid index is still reported after the change: {status:?}"
    );
}

/// What the rest of an owner-route cursor reads, on a fresh copy of the
/// seeded store, after its writer closed and its first file fetch opened a
/// private file reader: the cursor opens at `opened_at`, the writer closes,
/// one page is fetched through the file, the session then declares
/// `changed_to` if given, and the remaining pages are read. Returns the
/// remaining fetches' backend bytes and every row in order.
fn continued_cursor_after_a_share_change(
    seeded: &Path,
    opened_at: u8,
    changed_to: Option<u8>,
) -> (u64, Vec<Vec<Value>>) {
    let directory = tempfile::tempdir().unwrap();
    copy_store(seeded, directory.path());
    let path = directory.path().join("owner-cursor.db");
    let writer = Database::open(&path).unwrap();
    let session = session_beside_the_writer(&path, Some(opened_at));
    let mut cursor = session
        .open_cursor(SELECT_ALL, &HashMap::new())
        .expect("an owner cursor opens beside the writer");
    let mut rows: Vec<Vec<Value>> = cursor.first_page().rows().to_vec();
    writer.close().unwrap();
    let first = cursor
        .fetch(None)
        .expect("the cursor continues through its own file reader");
    rows.extend(first.rows().iter().cloned());
    assert!(
        first.has_more(),
        "the cursor has pages left after its first file fetch"
    );
    if let Some(percent) = changed_to {
        session
            .set_read_cache_share(Some(percent))
            .expect("the session changes its share while the cursor continues");
    }
    let ((), bytes) = backend_bytes(|| {
        loop {
            let page = cursor
                .fetch(None)
                .expect("the cursor keeps reading through its own file reader");
            rows.extend(page.rows().iter().cloned());
            if !page.has_more() {
                break;
            }
        }
    });
    drop(cursor);
    (bytes, rows)
}

/// A cursor already continuing on its own file reader after its writer
/// exited honours a share its session declares afterwards: turned off after
/// the first file fetch, the rest of the cursor reads like a continuation with
/// no file cache, not like one still at the 40% it opened with.
#[test]
fn a_cursor_continuing_on_its_own_file_reader_honours_a_later_share_change() {
    let seeded = tempfile::tempdir().unwrap();
    seeded_store(seeded.path(), CURSOR_ROWS);
    let (kept_at_40, _) = continued_cursor_after_a_share_change(seeded.path(), 40, None);
    let (opened_at_0, _) = continued_cursor_after_a_share_change(seeded.path(), 0, None);
    let (turned_off, rows) = continued_cursor_after_a_share_change(seeded.path(), 40, Some(0));
    assert_eq!(
        rows,
        all_rows(CURSOR_ROWS),
        "the cursor returns every row once"
    );
    assert!(
        opened_at_0 > 4 * kept_at_40,
        "the fixture must separate a continuation with no file cache ({opened_at_0} bytes) from \
         one at 40% ({kept_at_40} bytes)"
    );
    assert!(
        2 * turned_off > opened_at_0 + kept_at_40,
        "after the session turned its file cache off, the rest of the cursor must read like a \
         continuation with no cache ({opened_at_0} bytes), not like one still at the 40% its \
         private reader opened with ({kept_at_40} bytes); it read {turned_off}"
    );
}

/// After its writer closes, a session whose first call is to declare a share
/// moves onto the file route for that call: the share is the file reader's,
/// and the status that follows reports it there.
#[test]
fn after_its_writer_closes_a_session_whose_first_call_declares_a_share_applies_it_to_its_file_route()
 {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("turnover-set-first.db");
    let writer = seeded_writer(&path, TURNOVER_ROWS);
    let session = session_beside_the_writer(&path, None);
    session
        .execute(
            "SELECT id FROM cache_share WHERE id = $id",
            &params([("id", Value::Int64(0))]),
        )
        .expect("the session answers beside its writer");
    writer.close().unwrap();

    session
        .set_read_cache_share(Some(40))
        .expect("the session declares a 40% share as its first call after the writer closed");
    assert_eq!(
        session.route(),
        ReadRoute::File,
        "declaring a share moved the session onto the file route it now reads through"
    );
    let status = session
        .read_cache_share_status()
        .expect("the session inspects its read cache share");
    assert_eq!(
        (status.declared, status.effective_bytes),
        (Some(40), declared_bytes(40)),
        "the file route's cache is the declared 40%"
    );
    assert!(
        status.prepaid_index_bytes.is_some() && status.held_page_bytes.is_some(),
        "the status reports the file reader the share applies to: {status:?}"
    );
}

/// One `SHOW READ_CACHE_SHARE` column of the session.
fn shown(session: &ReadSession, column: &str) -> Value {
    let result = session
        .execute("SHOW READ_CACHE_SHARE", &HashMap::new())
        .expect("the session inspects its read cache share");
    let index = result
        .columns()
        .iter()
        .position(|name| name == column)
        .unwrap_or_else(|| panic!("SHOW READ_CACHE_SHARE has a {column} column"));
    result.rows()[0][index].clone()
}

/// A cursor continuing on its own file reader under a tight declared budget
/// keeps returning its rows when its session declares a share that reader
/// cannot admit: the fetch never fails for it, the session's declared share
/// stays what the user set, the reported refusal names the cursor that could
/// not follow, and a later shrink applies without needing any admission.
#[test]
fn a_continuing_cursor_keeps_its_rows_when_a_declared_share_cannot_be_admitted() {
    const TIGHT_BUDGET: usize = 32 * MIB;
    let seeded = tempfile::tempdir().unwrap();
    seeded_store(seeded.path(), CURSOR_ROWS);
    let directory = tempfile::tempdir().unwrap();
    copy_store(seeded.path(), directory.path());
    let path = directory.path().join("owner-cursor.db");
    let writer = Database::open(&path).unwrap();
    let session = ReadSession::open_with_observations_for_test(
        &path,
        ReadSessionOptions {
            memory_limit: Some(TIGHT_BUDGET),
            read_cache_share: Some(10),
            limits: ReadLimits {
                memory: TIGHT_BUDGET as u64,
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
    .expect("a session opens beside the writer under a 32 MiB budget");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor(SELECT_ALL, &HashMap::new())
        .expect("an owner cursor opens beside the writer");
    let mut rows: Vec<Vec<Value>> = cursor.first_page().rows().to_vec();
    writer.close().unwrap();
    let first = cursor
        .fetch(None)
        .expect("the cursor continues through its own file reader");
    rows.extend(first.rows().iter().cloned());
    assert!(first.has_more());

    session
        .execute("SET READ_CACHE_SHARE = 95", &HashMap::new())
        .expect("the session declares a 95% share");
    let after_growth = cursor.fetch(None).expect(
        "a share the cursor's own file reader cannot admit never turns its next fetch into a \
         refusal",
    );
    rows.extend(after_growth.rows().iter().cloned());
    let refusal = match shown(&session, "last_refusal") {
        Value::Text(text) => text,
        other => panic!(
            "the session reports the share the cursor's file reader could not admit, got {other:?}"
        ),
    };
    assert!(
        refusal.to_lowercase().contains("cursor"),
        "the reported refusal names the cursor that could not follow the share: {refusal}"
    );
    assert_eq!(
        shown(&session, "declared"),
        Value::Int64(95),
        "the session's own reader admitted 95%, so its declared share stays what the user set"
    );

    session
        .execute("SET READ_CACHE_SHARE = 2", &HashMap::new())
        .expect("a shrink needs no admission");
    let fixed = contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test(
        Some(TIGHT_BUDGET),
    )
    .1;
    assert_eq!(
        shown(&session, "effective_bytes"),
        Value::Int64(((TIGHT_BUDGET - fixed) * 2 / 100) as i64),
        "the shrink applies to the session's cache"
    );
    if after_growth.has_more() {
        loop {
            let page = cursor
                .fetch(None)
                .expect("the cursor keeps reading after the session shrinks its share");
            rows.extend(page.rows().iter().cloned());
            if !page.has_more() {
                break;
            }
        }
    }
    drop(cursor);
    assert_eq!(
        rows,
        all_rows(CURSOR_ROWS),
        "the cursor returns every row once, through the refused growth and the shrink"
    );
}
