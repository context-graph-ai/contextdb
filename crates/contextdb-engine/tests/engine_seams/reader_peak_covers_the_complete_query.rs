//! The same saved-index query pays for every reader reservation, including
//! per-record metadata and cursor windows that do not appear in the native
//! admission recorder.
use crate::resident_history_support::{exec, insert_vector, open};
use contextdb_core::{Value, Wallclock};
use std::collections::HashMap;

#[test]
fn first_saved_search_reader_bytes_stay_bounded_as_records_increase() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let mut observations = Vec::new();
    for (records, ceiling) in [(32usize, 75_362u64), (128, 197_275)] {
        let path = dir.path().join(format!("saved-{records}.db"));
        let db = open(&path, Some(64 * 1024 * 1024));
        exec(
            &db,
            "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(8) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE) HISTORY ALL",
        );
        for id in 0..records {
            insert_vector(&db, "items", id, Some(0), 8);
        }
        let mut ready = false;
        for _ in 0..128 {
            let report = db.run_maintenance_cycle().unwrap().vector;
            assert!(report.first_failure.is_none(), "{report:?}");
            if report.ready_partitions == 1 && report.remaining_partitions == 0 {
                ready = true;
                break;
            }
        }
        assert!(ready, "the explicit drive published the saved index");
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        let params = HashMap::from([(
            "v".into(),
            Value::Vector(crate::resident_history_support::vector(0, 8)),
        )]);
        let (answer, bytes) = db.with_reader_memory_high_water_for_test(|| {
            db.execute(
                "SELECT id FROM items ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 32",
                &params,
            )
            .unwrap()
        });
        assert_eq!(answer.rows().len(), 32);
        assert_eq!(answer.rows()[0][0], Value::Int64(0));
        assert!(
            bytes.peak >= bytes.before && bytes.peak >= bytes.after,
            "{bytes:?}"
        );
        eprintln!(
            "saved_search_reader_bytes records={records} before={} peak={} after={}",
            bytes.before, bytes.peak, bytes.after
        );
        assert!(
            bytes.peak <= ceiling,
            "{records} records: {bytes:?}, ceiling {ceiling}"
        );
        observations.push(bytes);
        drop(answer);
        db.close().unwrap();
    }
    // The query's output/cursor window is fixed. This ceiling includes every
    // real reader-parent debit, not only native-admission recorder entries.
    assert_eq!(observations.len(), 2);
}

#[test]
fn reader_peak_includes_direct_metadata_cursor_scratch_and_worker_admissions_and_restores_after_unwind()
 {
    use contextdb_core::read_memory::ReadCredit;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("reader-owners.db"), Some(64 * 1024 * 1024));
    let root = db.declared_read_memory_for_test();
    let owner = root.original_owner_for_test();
    let before = root.retained_bytes_for_test();
    let (_, outer) = db.with_reader_memory_high_water_for_test(|| {
        let metadata =
            ReadCredit::try_new(owner.clone(), 1337, "metadata", "decode", "fixture").unwrap();
        let (_, nested) = db.with_reader_memory_high_water_for_test(|| {
            let worker = owner.clone();
            let credit = std::thread::spawn(move || {
                ReadCredit::try_new(worker, 9000, "cursor", "window", "fixture").unwrap()
            })
            .join()
            .unwrap();
            assert!(
                ReadCredit::try_new(owner.clone(), usize::MAX, "cursor", "exhaustion", "fixture")
                    .is_err()
            );
            drop(credit);
        });
        assert_eq!(nested.before, before + 1337);
        assert_eq!(nested.peak, before + 1337 + 9000);
        assert_eq!(nested.after, before + 1337);
        let child = db.declared_reader_memory().unwrap();
        let window =
            ReadCredit::try_new(child.clone(), 5000, "cursor", "scratch", "fixture").unwrap();
        drop(window);
        drop(child);
        drop(metadata);
        assert!(
            catch_unwind(AssertUnwindSafe(|| db
                .with_reader_memory_high_water_for_test(|| {
                    let _held =
                        ReadCredit::try_new(owner.clone(), 7000, "cursor", "unwind", "fixture")
                            .unwrap();
                    panic!("unwind the nested observation");
                })))
            .is_err()
        );
        let last = ReadCredit::try_new(owner, 32768, "cursor", "after_unwind", "fixture").unwrap();
        drop(last);
    });
    assert_eq!(outer.before, before);
    assert_eq!(outer.peak, before + 32768);
    assert_eq!(outer.after, before);
    db.close().unwrap();
}
