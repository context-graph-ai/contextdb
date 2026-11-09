#![cfg(feature = "contextdb-internal")]

use redb::{Database, ReadOnlyDatabase, ReadableDatabase, TableDefinition};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("resident_page");

#[test]
fn production_residency_probe_observes_cold_and_warm_pages_without_reading_them() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let database = Database::create(file.path()).unwrap();
    assert_eq!(database.read_cache_resident_fraction(&[]), 0.0);
    let write = database.begin_write().unwrap();
    {
        let mut table = write.open_table(TABLE).unwrap();
        table.insert(7, 9).unwrap();
    }
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    let root = read.table_root_fingerprint(TABLE).unwrap().unwrap();
    let page: [u8; 8] = root[..8].try_into().unwrap();
    drop(read);
    drop(database);

    // This production surface must compile without the test-seams feature.
    let _native_surface: fn(&redb::NativeReadView, &[[u8; 8]]) -> f64 =
        redb::NativeReadView::read_cache_resident_fraction;
    let database = ReadOnlyDatabase::open(file.path()).unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let _observer = redb::observe_backend_reads(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    for _ in 0..16 {
        assert_eq!(database.read_cache_resident_fraction(&[page]), 0.0);
    }
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    let read = database.begin_read().unwrap();
    assert_eq!(
        read.open_table(TABLE)
            .unwrap()
            .get(7)
            .unwrap()
            .unwrap()
            .value(),
        9
    );
    drop(read);
    let after_query = reads.load(Ordering::SeqCst);
    assert!(after_query > 0);
    for _ in 0..16 {
        assert_eq!(database.read_cache_resident_fraction(&[page]), 1.0);
        assert_eq!(
            database.read_cache_resident_fraction(&[page, [255; 8]]),
            0.5
        );
        assert_eq!(database.read_cache_resident_fraction(&[[255; 8]]), 0.0);
        assert_eq!(database.read_cache_resident_fraction(&[]), 0.0);
    }
    assert_eq!(reads.load(Ordering::SeqCst), after_query);
}
