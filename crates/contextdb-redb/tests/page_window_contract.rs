//! A statement's page-residency figure is reported only when it was measured,
//! and measuring it costs work in proportion to the pages the statement read.
//!
//! Promise: `ReadOnlyDatabase::with_read_cache_page_window` reports the share
//! of a statement's pages that are resident as `Some(fraction)` only when it
//! measured that statement's own window. A window that another window
//! superseded before it was read, or one that recorded no pages at all,
//! reports `None` (shown to an operator as no figure, never as a measured 0%).
//! The residency scan after a statement examines a number of window entries
//! bounded by the pages the statement recorded, however large the cache.

#![cfg(all(feature = "contextdb-internal", feature = "test-seams"))]

use redb::{ReadableDatabase, TableDefinition};
use std::path::Path;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("page_window_rows");
const ROWS: u64 = 512;
const VALUE_BYTES: usize = 512;
/// Large enough that a scan over its whole window would examine about
/// 262,144 entries.
const LARGE_CACHE_BYTES: usize = 1 << 30;

fn write_store(path: &Path) {
    let database = redb::Builder::new()
        .create(path)
        .expect("create the fixture store");
    let write = database.begin_write().expect("begin the fixture write");
    {
        let mut table = write.open_table(TABLE).expect("open the fixture table");
        let value = vec![0x5a; VALUE_BYTES];
        for key in 0..ROWS {
            table
                .insert(key, value.as_slice())
                .expect("insert a fixture row");
        }
    }
    write.commit().expect("commit the fixture rows");
}

fn open(path: &Path, cache_bytes: usize) -> redb::ReadOnlyDatabase {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(cache_bytes);
    builder
        .open_read_only(path)
        .expect("reopen the fixture store for reading")
}

/// Read `keys` through one read transaction; every key is present.
fn read_keys(database: &redb::ReadOnlyDatabase, keys: impl Iterator<Item = u64>) {
    let read = database.begin_read().expect("begin a reader");
    let table = read.open_table(TABLE).expect("open the fixture table");
    for key in keys {
        let value = table
            .get(key)
            .expect("a fixture key is readable")
            .expect("every fixture key is present");
        assert_eq!(value.value().len(), VALUE_BYTES);
    }
}

/// Ten keys spread across the table, so they land on different leaf pages.
fn ten_spread_keys() -> impl Iterator<Item = u64> {
    (0..10u64).map(|step| step * (ROWS / 10))
}

/// A window whose statement opened a nested window before its own pages were
/// counted reports no figure; the nested window, which was measured, does.
#[test]
fn a_window_superseded_by_a_nested_window_reports_no_residency() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("nested.redb");
    write_store(&path);
    let database = open(&path, 64 << 20);
    let (inner, outer) = database.with_read_cache_page_window(|| {
        read_keys(&database, ten_spread_keys());
        let ((), inner) =
            database.with_read_cache_page_window(|| read_keys(&database, ten_spread_keys()));
        inner
    });
    assert!(
        inner.is_some(),
        "the nested window measured its own pages and reports their residency"
    );
    assert_eq!(
        outer, None,
        "the outer window was superseded before it was read, so it reports no figure rather \
         than a measured 0%"
    );
}

/// A window that recorded no pages reports no figure, whether the statement
/// read nothing or the cache had no room to record a window at all.
#[test]
fn a_window_that_recorded_no_pages_reports_no_residency() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("empty.redb");
    write_store(&path);
    let database = open(&path, 64 << 20);
    let ((), nothing_read) = database.with_read_cache_page_window(|| ());
    assert_eq!(
        nothing_read, None,
        "a statement that read no pages reports no residency figure"
    );
    let no_cache = open(&path, 0);
    let ((), no_window) =
        no_cache.with_read_cache_page_window(|| read_keys(&no_cache, ten_spread_keys()));
    assert_eq!(
        no_window, None,
        "a cache with no room for a window records no pages and reports no figure"
    );
}

/// After a statement that read about ten pages under a 1 GiB cache, the
/// residency scan examines entries in proportion to those pages, not to the
/// cache's whole window capacity.
#[test]
fn a_residency_scan_examines_entries_in_proportion_to_the_pages_recorded() {
    let directory = tempfile::tempdir().expect("fixture directory");
    let path = directory.path().join("large-cache.redb");
    write_store(&path);
    let database = open(&path, LARGE_CACHE_BYTES);
    let ((), fraction) =
        database.with_read_cache_page_window(|| read_keys(&database, ten_spread_keys()));
    assert!(
        fraction.is_some(),
        "a statement that read pages reports their residency"
    );
    let examined = database.page_window_entries_visited_for_test();
    assert!(
        examined <= 64,
        "the residency scan after a statement of about ten pages examined {examined} window \
         entries; it must be bounded by the pages recorded, not by the 1 GiB cache's capacity"
    );
}
