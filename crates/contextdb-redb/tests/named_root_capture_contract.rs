//! Capturing the root of one table must not touch any other pending table.
//!
//! The engine asks the storage layer for one closed table's root and length
//! many times inside a single commit. Each ask names one table, so it may
//! finalize that table's pending root without publishing a directory entry: another
//! table's pages are not checksummed, its directory entry is not rewritten,
//! and an unchanged table captured again costs no write. Every table's pending
//! root still reaches the directory when the transaction commits.
//!
//! Counters are the storage layer's own table-write observation of this
//! database instance; no time is read.
#![cfg(all(feature = "contextdb-internal", feature = "test-seams"))]

use redb::commit_work::{RootWorkKindForTest, observe_root_work_for_test};
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError,
    TableDefinition, TableWriteOperationForTest,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const SMALL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("small_a");
const WIDE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("wide_b");

fn fill(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    rows: u32,
    width: usize,
) {
    let mut table = tx.open_table(definition).unwrap();
    for n in 0..rows {
        let key = n.to_le_bytes();
        table
            .insert(key.as_slice(), vec![n as u8; width].as_slice())
            .unwrap();
    }
}

type Seen = Arc<Mutex<BTreeMap<(String, TableWriteOperationForTest), u64>>>;

fn watch(db: &Database) -> (Seen, redb::TableWriteObservationGuardForTest) {
    let seen: Seen = Arc::default();
    let sink = seen.clone();
    let guard = db.observe_table_writes_for_test(Arc::new(move |event| {
        let table = if matches!(
            event.operation,
            TableWriteOperationForTest::DirectoryInsert
                | TableWriteOperationForTest::DirectoryRemove
        ) {
            String::from_utf8(event.key.clone()).expect("native directory name")
        } else {
            event.table.clone()
        };
        *sink
            .lock()
            .unwrap()
            .entry((table, event.operation))
            .or_default() += 1;
    }));
    (seen, guard)
}

fn directory_inserts(seen: &Seen, table: &str) -> u64 {
    seen.lock()
        .unwrap()
        .get(&(
            table.to_string(),
            TableWriteOperationForTest::DirectoryInsert,
        ))
        .copied()
        .unwrap_or(0)
}

#[test]
fn capturing_one_table_publishes_no_other_pending_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("capture.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    fill(&tx, SMALL, 8, 16);
    fill(&tx, WIDE, 3_000, 512);
    let (seen, _guard) = watch(&db);
    let work: Arc<Mutex<BTreeMap<(String, String), u64>>> = Arc::default();
    let sink = work.clone();
    let root = observe_root_work_for_test(
        Arc::new(move |event| {
            *sink
                .lock()
                .unwrap()
                .entry((format!("{:?}", event.kind), event.table.to_string()))
                .or_default() += 1;
            Ok(())
        }),
        || tx.normal_table_root("small_a").unwrap(),
    );
    assert!(
        matches!(root, Some(Some(_))),
        "the captured table has a root: {root:?}"
    );
    assert_eq!(
        directory_inserts(&seen, "wide_b"),
        0,
        "capturing small_a published wide_b's pending root"
    );
    let touched_wide = work
        .lock()
        .unwrap()
        .iter()
        .filter(|((kind, table), _)| {
            table == "wide_b"
                && matches!(
                    kind.as_str(),
                    "FinalizeBegin" | "ChecksumPage" | "PublishBegin"
                )
        })
        .map(|((kind, _), count)| (kind.clone(), *count))
        .collect::<Vec<_>>();
    assert!(
        touched_wide.is_empty(),
        "capturing small_a finalized, checksummed or published wide_b: {touched_wide:?}"
    );
    tx.commit().unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(SMALL).unwrap().len().unwrap(),
        8,
        "the captured table survives the commit"
    );
    assert_eq!(
        read.open_table(WIDE).unwrap().len().unwrap(),
        3_000,
        "the table that was never captured is published by the commit, whole"
    );
}

#[test]
fn capturing_an_unchanged_table_again_writes_nothing() {
    for rows in [0, 8] {
        unchanged_capture_writes_nothing(rows);
    }
}

fn unchanged_capture_writes_nothing(rows: u32) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("again.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    fill(&tx, SMALL, rows, 16);
    let first = tx.normal_table_root("small_a").unwrap();
    let (seen, _guard) = watch(&db);
    let second = tx.normal_table_root("small_a").unwrap();
    let length = tx.normal_table_len("small_a").unwrap();
    assert_eq!(tx.table_root_fingerprint(SMALL).unwrap(), first.unwrap());
    assert_eq!(first, second, "an unchanged table keeps its exact root");
    assert_eq!(length, Some(u64::from(rows)));
    let written = seen.lock().unwrap().clone();
    assert!(
        written.is_empty(),
        "capturing an unchanged table again wrote: {written:?}"
    );
    tx.commit().unwrap();
}

#[test]
fn a_table_changed_after_capture_is_captured_exactly_again() {
    pending_header_lifecycle();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("mutated.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    fill(&tx, SMALL, 8, 16);
    let before = tx.normal_table_root("small_a").unwrap();
    {
        let mut table = tx.open_table(SMALL).unwrap();
        table
            .insert(99u32.to_le_bytes().as_slice(), [7u8; 16].as_slice())
            .unwrap();
    }
    let (seen, _guard) = watch(&db);
    let after = tx.normal_table_root("small_a").unwrap();
    assert_ne!(
        before, after,
        "a mutation after capture changes the captured root, even if a page address is reused"
    );
    {
        let mut table = tx.open_table(SMALL).unwrap();
        table
            .get_mut(0u32.to_le_bytes().as_slice())
            .unwrap()
            .unwrap()
            .insert([255u8; 16].as_slice())
            .unwrap();
    }
    let in_place = tx.normal_table_root("small_a").unwrap();
    let after_header = after.flatten().unwrap();
    let in_place_header = in_place.flatten().unwrap();
    assert_eq!(
        &after_header[..8],
        &in_place_header[..8],
        "the page address is unchanged"
    );
    assert_eq!(
        &after_header[24..],
        &in_place_header[24..],
        "the entry count is unchanged"
    );
    assert_ne!(
        after_header, in_place_header,
        "restaging must rehash same-address, same-length mutation"
    );
    assert_eq!(tx.normal_table_len("small_a").unwrap(), Some(9));
    assert_eq!(
        directory_inserts(&seen, "small_a"),
        0,
        "capturing the changed table finalizes its header without directory publication"
    );
    tx.commit().unwrap();
    assert_eq!(
        directory_inserts(&seen, "small_a"),
        1,
        "commit publishes the latest captured header exactly once"
    );
    let read = db.begin_read().unwrap();
    assert_eq!(read.open_table(SMALL).unwrap().len().unwrap(), 9);
    assert_eq!(
        read.open_table(SMALL)
            .unwrap()
            .get(0u32.to_le_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        [255u8; 16]
    );
    drop(read);
}

/// Additional rows of the named-root mutation contract: a delayed header must
/// survive native consumers that read the directory, and old readers keep the
/// entire old body. A decisive mutant is dropping the pending-header overlay
/// from compaction selection or relocation after capture stops publishing it.
fn pending_header_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lifecycle.redb");
    let db = Database::create(&path).unwrap();
    let tx = db.begin_write().unwrap();
    fill(&tx, SMALL, 8, 16);
    fill(&tx, WIDE, 16, 32);
    tx.commit().unwrap();
    let held = db.begin_read().unwrap();
    let old_small = table_rows(&held, SMALL);
    let old_wide = table_rows(&held, WIDE);
    let filler = TableDefinition::<&[u8], &[u8]>::new("00_filler");
    for round in 0..2 {
        let tx = db.begin_write().unwrap();
        // Reserve lower pages until A/B have their new roots, then free those
        // transaction-owned pages to give relocation an actual lower target.
        fill(&tx, filler, 64, 4096);
        fill(&tx, SMALL, 16 + round, 32);
        fill(&tx, WIDE, 32 + round, 512);
        let before = tx.table_root_fingerprint(SMALL).unwrap().unwrap();
        tx.delete_table(filler).unwrap();
        let mut cursor = redb::CompactionCursor::restore_checkpoint(
            1,
            Some("small_a".to_owned()),
            false,
            Vec::new(),
            true,
        )
        .unwrap();
        let progress = tx.compact_step_with_hook(&mut cursor, 8, |_| Ok(()), |write| {
            let relocated = write.table_root_fingerprint(SMALL).unwrap().unwrap();
            assert_ne!(&before[..8], &relocated[..8], "relocation must update the selected current root, not an older directory root");
            Ok(())
        }).unwrap();
        assert!(
            progress.pages_examined > 0,
            "the pending target was examined"
        );
        assert!(
            progress.pages_relocated > 0,
            "fixture must move a pending target page"
        );
        let read = db.begin_read().unwrap();
        assert_eq!(table_rows(&read, SMALL), expected_rows(16 + round, 32));
        assert_eq!(table_rows(&read, WIDE), expected_rows(32 + round, 512));
        assert_eq!(table_rows(&held, SMALL), old_small);
        assert_eq!(table_rows(&held, WIDE), old_wide);
    }
    let renamed = TableDefinition::<&[u8], &[u8]>::new("renamed_a");
    let tx = db.begin_write().unwrap();
    fill(&tx, SMALL, 24, 64);
    assert_eq!(tx.normal_table_len("small_a").unwrap(), Some(24));
    tx.table_root_fingerprint(SMALL).unwrap();
    tx.rename_table(SMALL, renamed).unwrap();
    assert_eq!(tx.normal_table_root("small_a").unwrap(), None);
    assert_eq!(tx.normal_table_len("renamed_a").unwrap(), Some(24));
    tx.delete_table(renamed).unwrap();
    fill(&tx, SMALL, 2, 8);
    tx.table_root_fingerprint(SMALL).unwrap();
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    let savepoint = tx.ephemeral_savepoint().unwrap();
    let original = tx.table_root_fingerprint(SMALL).unwrap();
    fill(&tx, SMALL, 40, 128);
    assert_ne!(tx.table_root_fingerprint(SMALL).unwrap(), original);
    tx.restore_savepoint(&savepoint).unwrap();
    assert_eq!(tx.table_root_fingerprint(SMALL).unwrap(), original);
    tx.commit().unwrap();
    drop(savepoint);
    assert_eq!(table_rows(&held, SMALL), old_small);
    assert_eq!(table_rows(&held, WIDE), old_wide);
    drop(held);
    drop(db);
    let db = Database::open(path).unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(table_rows(&read, SMALL), expected_rows(2, 8));
    assert_eq!(table_rows(&read, WIDE), expected_rows(33, 512));
}

fn table_rows(
    read: &redb::ReadTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    read.open_table(definition)
        .unwrap()
        .iter()
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

fn expected_rows(rows: u32, width: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut expected: Vec<_> = (0..rows)
        .map(|n| (n.to_le_bytes().to_vec(), vec![n as u8; width]))
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    expected
}

#[test]
fn a_caught_root_capture_failure_refuses_the_commit_and_publishes_nothing() {
    let mut failures = Vec::new();
    for typed in [false, true] {
        for publishing in [false, true] {
            if std::panic::catch_unwind(|| caught_capture_failure(typed, publishing)).is_err() {
                failures.push((typed, publishing));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "caught-failure rows failed (typed, publishing): {failures:?}"
    );
}

fn caught_capture_failure(typed: bool, publishing: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failed.redb");
    {
        let db = Database::create(&path).unwrap();
        let tx = db.begin_write().unwrap();
        fill(&tx, SMALL, 8, 16);
        fill(&tx, WIDE, 200, 128);
        let finalized = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sink = finalized.clone();
        let failed = observe_root_work_for_test(
            Arc::new(move |event| {
                if event.kind == RootWorkKindForTest::FinalizeEnd
                    && sink.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
                {
                    Err(StorageError::AdmissionRefused("injected".into()))
                } else {
                    Ok(())
                }
            }),
            || {
                if typed {
                    tx.table_root_fingerprint(SMALL)?;
                    tx.table_root_fingerprint(WIDE)?;
                } else {
                    tx.normal_table_root("small_a")?;
                    tx.normal_table_root("wide_b")?;
                }
                Ok::<_, redb::TableError>(())
            },
        );
        assert!(failed.is_err(), "the injected failure reaches the caller");
        assert_eq!(finalized.load(std::sync::atomic::Ordering::SeqCst), 2);
        let committed = if publishing {
            let marker = tx.durable_publication();
            tx.commit_publishing(&marker)
        } else {
            tx.commit()
        };
        assert!(
            committed.is_err(),
            "typed={typed}, publishing={publishing}: a caught failure after partial work must prevent commit"
        );
    }
    let db = Database::open(&path).unwrap();
    let read = db.begin_read().unwrap();
    for definition in [SMALL, WIDE] {
        match read.open_table(definition) {
            Err(_) => {}
            Ok(table) => assert_eq!(
                table.len().unwrap(),
                0,
                "nothing of the refused transaction is published"
            ),
        }
    }
}
