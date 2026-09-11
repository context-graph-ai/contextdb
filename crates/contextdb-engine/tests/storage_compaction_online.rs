#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use redb::{MultimapTableDefinition, ReadableDatabase, TableDefinition};
use std::collections::HashMap;

const PAYLOAD: TableDefinition<u64, &[u8]> = TableDefinition::new("payload");
const MEMBERS: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("members");

#[test]
fn backend_read_observation_counts_actual_bytes_and_ends_with_the_operation() {
    use redb::StorageBackend;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Debug)]
    struct CountedBackend {
        file: redb::backends::FileBackend,
        bytes: Arc<AtomicUsize>,
    }
    impl StorageBackend for CountedBackend {
        fn len(&self) -> std::io::Result<u64> {
            self.file.len()
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            self.file.set_len(len)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.file.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            self.file.write(offset, data)
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.file.read(offset, out)?;
            self.bytes.fetch_add(out.len(), Ordering::SeqCst);
            Ok(())
        }
        fn close(&self) -> std::io::Result<()> {
            self.file.close()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let bytes = Arc::new(AtomicUsize::new(0));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("counted.redb"))
        .unwrap();
    let db = redb::Builder::new()
        .set_cache_size(0)
        .create_with_backend(CountedBackend {
            file: redb::backends::FileBackend::new(file).unwrap(),
            bytes: bytes.clone(),
        })
        .unwrap();
    let write = db.begin_write().unwrap();
    write
        .open_table(PAYLOAD)
        .unwrap()
        .insert(1, &[91_u8; 8192][..])
        .unwrap();
    write.commit().unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = observed.clone();
    let baseline = bytes.load(Ordering::SeqCst);
    let observer =
        redb::observe_backend_reads(Arc::new(move |bytes| sink.lock().unwrap().push(bytes)));
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(PAYLOAD)
            .unwrap()
            .get(1)
            .unwrap()
            .unwrap()
            .value(),
        &[91_u8; 8192]
    );
    drop(read);
    let total = observed.lock().unwrap().iter().sum::<usize>();
    assert!(total >= 8192);
    assert_eq!(
        total,
        bytes.load(Ordering::SeqCst) - baseline,
        "matches successful reads at the actual backend"
    );
    drop(observer);
    let read = db.begin_read().unwrap();
    read.open_table(PAYLOAD).unwrap().get(1).unwrap().unwrap();
    assert_eq!(observed.lock().unwrap().iter().sum::<usize>(), total);
    assert!(
        bytes.load(Ordering::SeqCst) > baseline + total,
        "later backend reads are outside this operation"
    );
}

fn fragmented_store(path: &std::path::Path) -> redb::Database {
    let db = redb::Database::create(path).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(PAYLOAD).unwrap();
        let bytes = vec![91_u8; 8192];
        for id in 0..1024 {
            table.insert(id, bytes.as_slice()).unwrap();
        }
        let mut members = write.open_multimap_table(MEMBERS).unwrap();
        for value in 0..2048 {
            members.insert(1, value).unwrap();
        }
    }
    write.commit().unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(PAYLOAD).unwrap();
        for id in 0..896 {
            table.remove(id).unwrap();
        }
    }
    write.commit().unwrap();
    db
}

fn verify_payload(db: &redb::Database) {
    let read = db.begin_read().unwrap();
    let table = read.open_table(PAYLOAD).unwrap();
    for id in 896..1024 {
        assert_eq!(table.get(id).unwrap().unwrap().value(), vec![91_u8; 8192]);
    }
    let members = read.open_multimap_table(MEMBERS).unwrap();
    assert_eq!(
        members
            .get(1)
            .unwrap()
            .map(|v| v.unwrap().value())
            .collect::<Vec<_>>(),
        (0..2048).collect::<Vec<_>>()
    );
}

#[test]
fn relocation_batches_preserve_values_with_interleaved_writes_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bounded.redb");
    let db = fragmented_store(&path);
    let before = std::fs::metadata(&path).unwrap().len();
    let mut cursor = redb::CompactionCursor::default();
    let mut moved_during_writes = 0;
    let mut complete = false;
    let mut batches = 0;
    for step in 0..2048_u64 {
        let progress = db.compact_step(&mut cursor, 8, |_| Ok(())).unwrap();
        batches += 1;
        assert!(
            progress.pages_examined <= 24,
            "bounded page discovery: {progress:?}"
        );
        if step < 128 {
            moved_during_writes += progress.pages_relocated;
            let write = db.begin_write().unwrap();
            write
                .open_table(PAYLOAD)
                .unwrap()
                .insert(2048, step.to_le_bytes().as_slice())
                .unwrap();
            write.commit().unwrap();
            let read = db.begin_read().unwrap();
            assert_eq!(
                read.open_table(PAYLOAD)
                    .unwrap()
                    .get(2048)
                    .unwrap()
                    .unwrap()
                    .value(),
                step.to_le_bytes()
            );
        }
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "finite storage maintenance must finish");
    assert!(batches > 3);
    assert!(
        moved_during_writes > 0,
        "real durable relocation progresses with foreground writes"
    );
    assert!(std::fs::metadata(&path).unwrap().len() < before);
    verify_payload(&db);
    drop(db);
    let reopened = redb::Database::open(&path).unwrap();
    verify_payload(&reopened);
    println!("durable_batches={batches} pages_relocated_during_writes={moved_during_writes}");
}

#[test]
fn allocation_refusal_aborts_relocation_without_advancing_its_position() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("admitted.redb");
    let db = fragmented_store(&path);
    let mut cursor = redb::CompactionCursor::default();
    let failure = db.compact_step(&mut cursor, 8, |_| {
        Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory))
    });
    assert!(failure.is_err());
    assert!(
        !cursor.is_active(),
        "an aborted transaction publishes no continuation"
    );
    verify_payload(&db);
    drop(db);
    verify_payload(&redb::Database::open(&path).unwrap());
}

#[test]
fn paused_storage_statistics_allow_a_foreground_commit() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::{thread, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("statistics.redb");
    drop(fragmented_store(&path));
    let mut builder = redb::Builder::new();
    builder.set_cache_size(0);
    let db = Arc::new(builder.open(&path).unwrap());
    let expected = db
        .storage_statistics_snapshot()
        .unwrap()
        .stats()
        .unwrap()
        .stored_bytes();
    let reader_db = db.clone();
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let reader = thread::spawn(move || {
        let read = reader_db.storage_statistics_snapshot().unwrap();
        let first = AtomicBool::new(true);
        redb::with_read_memory_admission(
            Arc::new(move |_| {
                if first.swap(false, Ordering::SeqCst) {
                    reached_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                Ok(())
            }),
            || read.stats().unwrap().stored_bytes(),
        )
    });
    reached_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_db = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let write = writer_db.begin_write().unwrap();
        write
            .open_table(PAYLOAD)
            .unwrap()
            .insert(2048, b"recording".as_slice())
            .unwrap();
        write.commit().unwrap();
        done_tx.send(()).unwrap();
    });
    let completed_before_release = done_rx.recv_timeout(Duration::from_secs(5));
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(
        reader.join().unwrap(),
        expected,
        "statistics retain their original read snapshot"
    );
    completed_before_release
        .expect("recording finishes before storage-statistics traversal resumes");
}

#[test]
fn automatic_storage_memory_refusal_keeps_the_store_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("limited.redb");
    let initial = Database::open(&path).unwrap();
    initial.close().unwrap();
    drop(initial);
    drop(fragmented_store(&path));
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used + 1024)).unwrap();
    let error = db.run_maintenance_cycle().unwrap_err();
    assert!(
        matches!(error, contextdb_core::Error::MemoryBudgetExceeded { .. }),
        "typed compaction refusal: {error:?}"
    );
    assert!(!db.__storage_compaction_progress_for_test().0);
    db.set_memory_limit(None).unwrap();
    db.execute("INSERT INTO recordings VALUES (1)", &HashMap::new())
        .unwrap();
    assert_eq!(
        db.execute("SELECT id FROM recordings", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]]
    );
    assert!(db.run_maintenance_cycle().unwrap().compaction.ran);
    db.close().unwrap();
    drop(db);
    verify_payload(&redb::Database::open(&path).unwrap());
}

#[test]
fn automatic_storage_batches_allow_recording_and_readback_before_completion() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("online.redb");
    let initial = Database::open(&path).unwrap();
    initial.close().unwrap();
    drop(initial);
    drop(fragmented_store(&path));
    let before = std::fs::metadata(&path).unwrap().len();
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let mut total_moved = 0;
    let mut finished = false;
    let mut batches = 0;
    for id in 0..2048_i64 {
        let report = db.run_maintenance_cycle().unwrap();
        assert!(
            report.compaction.ran,
            "the admitted maintenance sweep keeps advancing"
        );
        let (active, examined, moved) = db.__storage_compaction_progress_for_test();
        assert!(examined <= 80, "one cycle has bounded page discovery");
        total_moved += moved;
        batches += 1;
        db.execute(
            "INSERT INTO recordings VALUES ($id)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
        let read = db.read_session(Default::default()).unwrap();
        assert_eq!(
            read.execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Int64(id + 1)]]
        );
        if !active {
            assert!(report.compaction.handle_recycled);
            finished = true;
            break;
        }
        assert!(
            !report.compaction.handle_recycled,
            "the handle remains owned between batches"
        );
    }
    assert!(
        finished,
        "automatic maintenance completes despite continued recording"
    );
    assert!(batches > 3 && total_moved > 0);
    assert!(std::fs::metadata(&path).unwrap().len() < before);
    assert!(
        !db.run_maintenance_cycle().unwrap().compaction.ran,
        "completion starts the existing interval gate"
    );
    db.close().unwrap();
    drop(db);
    verify_payload(&redb::Database::open(&path).unwrap());
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) FROM recordings", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(batches)]]
    );
    println!("automatic_batches={batches} pages_relocated={total_moved}");
}
