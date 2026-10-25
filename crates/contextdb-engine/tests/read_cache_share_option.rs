//! A reader can declare the share of its memory budget its page cache takes.
//!
//! Promise: `ReadSessionOptions::read_cache_share`, a percent of the reader's
//! declared memory budget, is the page cache that reader opens with. A share
//! large enough for the working set keeps it cached, so the same queries
//! answered again read nothing from the file. A small share is honoured too:
//! the reader does not quietly keep a larger cache than it declared. Leaving
//! the share undeclared keeps the cache derived from the budget.
//!
//! The oracle is the bytes the storage backend returns while a real file
//! reader answers the same queries in repeated rounds.

#![cfg(unix)]

#[path = "read/reader_proof_support.rs"]
#[allow(dead_code)]
mod reader_proof_support;

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use reader_proof_support as fixture;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MIB: usize = 1024 * 1024;
/// Large enough that a 40% share holds the working set twice over and a
/// 2% share holds a small fraction of it.
const READER_BUDGET: usize = 256 * MIB;
const ROWS: usize = 6_000;
const PAYLOAD_BYTES: usize = 4_096;
const ROWS_PER_QUERY: usize = 1_500;

fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(PAYLOAD_BYTES / 8)
}

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE cache_share (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..ROWS).step_by(500) {
        let tx = db.begin().unwrap();
        for row in start..(start + 500).min(ROWS) {
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

fn options(read_cache_share: Option<u8>) -> ReadSessionOptions {
    ReadSessionOptions {
        memory_limit: Some(READER_BUDGET),
        limits: ReadLimits {
            memory: READER_BUDGET as u64,
            ..fixture::limits()
        },
        read_cache_share,
        ..Default::default()
    }
}

/// One round of the four range queries that together read every row; returns
/// the bytes the storage backend returned during it.
fn round(reader: &ReadSession) -> u64 {
    let bytes = Arc::new(AtomicU64::new(0));
    let observed = Arc::clone(&bytes);
    let observation = redb::observe_backend_reads(Arc::new(move |read| {
        observed.fetch_add(read as u64, Ordering::SeqCst);
    }));
    for start in (0..ROWS).step_by(ROWS_PER_QUERY) {
        let rows = reader
            .execute(
                "SELECT id, payload FROM cache_share WHERE id >= $lo AND id < $hi ORDER BY id",
                &params([
                    ("lo", Value::Int64(start as i64)),
                    ("hi", Value::Int64((start + ROWS_PER_QUERY) as i64)),
                ]),
            )
            .unwrap();
        assert_eq!(rows.rows().len(), ROWS_PER_QUERY);
        assert_eq!(
            rows.rows()[0],
            vec![Value::Int64(start as i64), Value::Text(payload(start))]
        );
    }
    drop(observation);
    bytes.load(Ordering::SeqCst)
}

/// A declared 40% share (about 102 MiB of a 256 MiB budget) keeps the working
/// set the first round reads from the file (about 49 MB of file pages for
/// 24 MiB of row payload) cached: the second round reads nothing from the file.
#[test]
fn a_declared_read_cache_share_that_holds_the_working_set_keeps_it_cached() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("declared-share.db");
    seed(&path);
    let routes = Arc::new(fixture::Routes::default());
    let reader = fixture::open_file(&path, options(Some(40)), routes.clone());
    let first = round(&reader);
    let second = round(&reader);
    routes.file_only();
    assert!(
        first as usize >= ROWS * PAYLOAD_BYTES,
        "the first round must read at least the {}-byte row payload (read {first})",
        ROWS * PAYLOAD_BYTES
    );
    assert!(
        first as usize <= READER_BUDGET * 40 / 100,
        "the working set the first round read ({first} bytes of file pages) must fit the \
         declared 40% share ({} bytes)",
        READER_BUDGET * 40 / 100
    );
    assert_eq!(
        second, 0,
        "with a declared 40% read cache share of a {READER_BUDGET}-byte budget, repeating the \
         same queries must read nothing from the file: first round {first} bytes, second {second}"
    );
}

/// A declared 2% share (about 5 MiB) is honoured as declared: the reader does
/// not keep the 49 MB working set, so a repeated round reads from the file
/// again, while a 40% share on the same store reads nothing.
#[test]
fn a_small_declared_read_cache_share_is_honoured_not_enlarged() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("small-share.db");
    seed(&path);
    let small = fixture::open_file(
        &path,
        options(Some(2)),
        Arc::new(fixture::Routes::default()),
    );
    round(&small);
    let small_repeat = round(&small);
    drop(small);
    let large = fixture::open_file(
        &path,
        options(Some(40)),
        Arc::new(fixture::Routes::default()),
    );
    round(&large);
    let large_repeat = round(&large);
    assert!(
        small_repeat as usize >= ROWS * PAYLOAD_BYTES / 2,
        "a 2% share cannot hold the working set, so a repeated round must read much of it again \
         (read {small_repeat} bytes)"
    );
    assert_eq!(
        large_repeat, 0,
        "the same store under a 40% share repeats the round without reading the file \
         (2% share repeated round read {small_repeat} bytes)"
    );
}
