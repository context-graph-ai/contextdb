//! The same saved-index query pays for every reader reservation, including
//! per-record metadata and cursor windows that do not appear in the native
//! admission recorder.
use crate::resident_history_support::open;

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
