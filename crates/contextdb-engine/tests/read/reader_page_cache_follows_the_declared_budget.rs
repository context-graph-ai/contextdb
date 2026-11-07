//! A file reader's page cache is sized from the memory budget it declares.
//!
//! Promise: a reader's working set stays cached within the reader's declared
//! memory budget. The page cache a reader opens with is therefore a share of
//! that declared budget, growing as the budget grows, not a fixed ceiling
//! applied whatever the reader declared. Under the reader budget a
//! context-graph search declares by default (1 GiB), a working set far
//! smaller than that budget is read from the file once: repeating the same
//! queries on the same reader reads nothing more from the file. The working
//! set is what the first round of queries reads from the file: about 49 MB of
//! file pages for 24 MiB of row payload, because each 4 KiB-plus row spans two
//! pages.
//!
//! Both tests are exact. One reads the cache share the production read open
//! uses for a declared budget; the other counts the bytes the storage backend
//! returns while a real file reader answers the same queries twice.

use super::reader_proof_support as fixture;
use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test;
use contextdb_engine::{Database, ReadSessionOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const MIB: usize = 1024 * 1024;
/// The reader memory budget a context-graph search declares by default.
const DEFAULT_SEARCH_READER_BUDGET: usize = 1024 * MIB;
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

fn reader_options(budget: usize) -> ReadSessionOptions {
    ReadSessionOptions {
        memory_limit: Some(budget),
        limits: ReadLimits {
            memory: budget as u64,
            ..fixture::limits()
        },
        ..Default::default()
    }
}

/// The page cache a reader opens with is a share of its declared budget:
/// a larger declared budget gives a larger cache, and the cache together with
/// the reader's fixed source reservations stays below the budget, leaving the
/// rest for results and working memory.
#[test]
fn a_readers_page_cache_share_grows_with_its_declared_memory_budget() {
    let budgets = [
        64 * MIB,
        256 * MIB,
        DEFAULT_SEARCH_READER_BUDGET,
        4096 * MIB,
    ];
    let reservations =
        budgets.map(|budget| read_source_cache_reservation_for_limit_for_test(Some(budget)));
    let shares = reservations.map(|(share, _)| share);
    for (budget, (share, fixed)) in budgets.iter().zip(reservations) {
        assert!(
            share + fixed < *budget,
            "a {budget}-byte reader's page cache ({share} bytes) plus its fixed source \
             reservations ({fixed} bytes) must leave room for the rest of its work"
        );
    }
    assert!(
        shares.windows(2).all(|pair| pair[1] > pair[0]),
        "the page cache must grow with the declared reader budget; budgets {budgets:?} give \
         cache shares {shares:?}"
    );
}

/// Under the default search reader budget, the working set the first round
/// of queries reads from the file (about 49 MB of file pages for 24 MiB of row
/// payload, well under that budget) fits the reader's derived page cache and
/// is not read again by an identical second round on the same reader.
#[test]
fn a_working_set_inside_the_default_search_budget_is_read_from_the_file_once() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cache-share.db");
    seed(&path);
    let routes = Arc::new(fixture::Routes::default());
    let reader = fixture::open_file(
        &path,
        reader_options(DEFAULT_SEARCH_READER_BUDGET),
        routes.clone(),
    );

    let round = || {
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
    };

    let first = round();
    let second = round();
    routes.file_only();
    let share =
        read_source_cache_reservation_for_limit_for_test(Some(DEFAULT_SEARCH_READER_BUDGET)).0;
    assert!(
        first as usize >= ROWS * PAYLOAD_BYTES,
        "the first round must read at least the {}-byte row payload from the file (read {first})",
        ROWS * PAYLOAD_BYTES
    );
    assert!(
        first as usize <= share,
        "the working set the first round read ({first} bytes of file pages) must fit the page \
         cache derived from a {DEFAULT_SEARCH_READER_BUDGET}-byte reader budget ({share} bytes)"
    );
    assert_eq!(
        second, 0,
        "repeating the same queries must read nothing from the file: the first round read \
         {first} bytes, the second {second}, under a {DEFAULT_SEARCH_READER_BUDGET}-byte reader \
         budget whose page cache is {share} bytes"
    );
}
