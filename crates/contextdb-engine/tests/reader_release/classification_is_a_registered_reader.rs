//! Classifying a store's format is a registered, bounded read, and it never
//! takes the data lock past a writer that is waiting for starting readers.
//!
//! Promise (contextdb-intent, Policies 21-23): everything that grows with the
//! store -- here, listing the tables to find the format marker -- happens after
//! the reader hold and the data lock are released, on a root the classifier's
//! own registration protects; and a classifier is an ordinary entrant, so a
//! writer that has claimed the store is never overtaken by it.

use crate::writer_acquisition_support::{
    Phase, WATCHDOG, counts, next_event, open_writer, park_readers, seeded_store, watch,
};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use contextdb_engine::{Database, DatabaseOpenOptions};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// The format's admission budget in base pages.
const ADMISSION_PAGE_BUDGET: u64 = 110;

fn store_with_tables(directory: &Path, tables: usize) -> PathBuf {
    let path = directory.join(format!("tables-{tables}.db"));
    let database = Database::open(&path).expect("claim a store to seed");
    for table in 0..tables {
        database
            .execute(
                &format!("CREATE TABLE classified_{table} (id INTEGER PRIMARY KEY)"),
                &HashMap::new(),
            )
            .expect("declare a table");
    }
    database.close().expect("release the seeded store");
    path
}

/// Test-estate reason (2026-10-15): classification reads a format-bounded number of base pages
/// at 1, 64 and 256 tables, recorded as a root-capture receipt like any other reader's. Today the
/// classifier takes the data lock itself and iterates every table under it, and records no
/// receipt.
#[test]
fn classifying_a_store_reads_a_format_bounded_number_of_pages_at_any_table_count() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    for tables in [1usize, 64, 256] {
        let path = store_with_tables(directory.path(), tables);
        let (verdict, receipt) = std::thread::spawn(move || {
            let verdict = RedbPersistence::is_legacy_format_store(&path);
            (verdict, redb::last_admission_read_receipt())
        })
        .join()
        .expect("the classifying thread finishes");
        assert!(
            matches!(verdict, Ok(false)),
            "{tables} tables: a current store is not a legacy store, got {verdict:?}"
        );
        let receipt = receipt.expect("classification is a registered read and leaves a receipt");
        assert!(
            receipt.charged_pages <= ADMISSION_PAGE_BUDGET,
            "{tables} tables: classification admission charged {} base pages, over the \
             format's {ADMISSION_PAGE_BUDGET}",
            receipt.charged_pages
        );
    }
}

/// Test-estate reason (2026-10-15): a classifier that reaches the store through a hard link
/// after a writer has claimed it, while the writer waits behind a reader, takes no data-lock
/// grant past the writer, is refused rather than answering, and the writer is granted when the
/// reader leaves. Today classification takes the data lock directly, outside the gate every other
/// entrant passes.
#[test]
fn a_classifier_through_a_hard_link_makes_no_data_lock_call_past_a_queued_writer() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let p = seeded_store(directory.path(), "classified-first-name.db");
    let q = directory.path().join("classified-second-name.db");
    std::fs::hard_link(&p, &q).expect("a second name for the same physical store");

    let mut readers = park_readers(&p, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(&p);
    let writer = open_writer(&p, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "behind the reader inside the section"),
        WriterAdmissionEvent::Waiting,
        "the writer must be queued before the classifier arrives"
    );
    let before = counts(&p);

    let (tell, heard) = std::sync::mpsc::channel();
    let classified = q.clone();
    let classifier = std::thread::spawn(move || {
        let _ = tell.send(RedbPersistence::is_legacy_format_store(&classified));
    });
    let verdict = heard
        .recv_timeout(WATCHDOG)
        .expect("the classifier never answered: it waited behind a queued writer");
    classifier.join().expect("the classifier thread finishes");
    assert!(
        verdict.is_err(),
        "a live writer holds the store: classification refuses instead of answering, got {verdict:?}"
    );
    let after = counts(&p);
    assert_eq!(
        (
            after.data_shared_grants - before.data_shared_grants,
            after.hold_shared_grants - before.hold_shared_grants,
        ),
        (0, 0),
        "the classifier took the data lock or the hold past the queued writer \
         (before {before:?}, after {after:?})"
    );

    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader inside left"),
        WriterAdmissionEvent::Acquired
    );
    writer
        .finish()
        .expect("the writer is granted")
        .close()
        .expect("close");
}
