use super::*;
use redb::{ReadableDatabase, TableDefinition};
use std::{sync::mpsc, thread, time::Duration};

const PAYLOAD: TableDefinition<u64, &[u8]> = TableDefinition::new("overlap_payload");

#[test]
fn automatic_relocation_progresses_with_live_storage_reads_and_waiting_recordings() {
    assert_online_compaction(false);
}

#[test]
fn explicit_relocation_progresses_with_live_storage_reads_and_waiting_recordings() {
    assert_online_compaction(true);
}

fn assert_online_compaction(explicit: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("overlap.redb");
    Database::open(&path).unwrap().close().unwrap();
    {
        let store = redb::Database::open(&path).unwrap();
        let write = store.begin_write().unwrap();
        {
            let mut table = write.open_table(PAYLOAD).unwrap();
            for id in 0..1024 {
                table.insert(id, &[91_u8; 8192][..]).unwrap();
            }
        }
        write.commit().unwrap();
        let write = store.begin_write().unwrap();
        {
            let mut table = write.open_table(PAYLOAD).unwrap();
            for id in 0..896 {
                table.remove(id).unwrap();
            }
        }
        write.commit().unwrap();
    }
    let db = Arc::new(Database::open(&path).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE recordings (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    let persistence = db.persistence.as_ref().unwrap().clone();
    let old = persistence
        .with_db(|db| db.begin_read().map_err(RedbPersistence::storage_error))
        .unwrap();
    let old_table = old.open_table(PAYLOAD).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let maintenance_db = db.clone();
    let maintenance = thread::spawn(move || {
        crate::persistence::COMPACTION_BATCH_OBSERVER.with(|slot| {
            slot.replace(Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })));
        });
        if explicit {
            maintenance_db.compact_now().unwrap()
        } else {
            maintenance_db.run_maintenance_cycle().unwrap().compaction
        }
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let observe_writer_wait = persistence
        .with_db(|db| Ok(db.write_wait_observer()))
        .unwrap();
    let (requested_tx, requested_rx) = mpsc::channel();
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        requested_tx.send(()).unwrap();
        writer_db
            .execute("INSERT INTO recordings VALUES (0)", &HashMap::new())
            .unwrap();
    });
    requested_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // Observe the actual redb writer queue before resuming relocation. Merely
    // starting a thread permits its execute call to run after the release.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let writer_waiting = loop {
        if observe_writer_wait() {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        thread::yield_now();
    };
    // The maintenance transaction has read a real relocation source page and
    // remains open. A second storage read must finish before it resumes.
    let (read_tx, read_rx) = mpsc::channel();
    let read_persistence = persistence.clone();
    let read_db = db.clone();
    let reader = thread::spawn(move || {
        let read = read_persistence
            .with_db(|db| db.begin_read().map_err(RedbPersistence::storage_error))
            .unwrap();
        let table = read.open_table(PAYLOAD).unwrap();
        assert_eq!(table.get(1000).unwrap().unwrap().value(), &[91_u8; 8192]);
        assert_eq!(
            read_db
                .execute("SELECT id FROM recordings", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            0
        );
        read_tx.send(()).unwrap();
    });
    let read_completed = read_rx.recv_timeout(Duration::from_secs(10));
    release_tx.send(()).unwrap();
    reader.join().unwrap();
    let first = maintenance.join().unwrap();
    writer.join().unwrap();
    assert!(writer_waiting, "foreground write queued inside relocation");
    read_completed.expect("storage reads finish inside the active relocation transaction");
    assert!(first.ran);
    if explicit {
        assert_eq!(
            old_table.get(1000).unwrap().unwrap().value(),
            &[91_u8; 8192]
        );
        assert_eq!(
            db.execute("SELECT id FROM recordings", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            1
        );
        return;
    }
    let mut relocated = persistence.storage_compaction_progress_for_test().2;
    let mut complete = false;
    for id in 1..2048_i64 {
        let writer_db = db.clone();
        let writer = thread::spawn(move || {
            writer_db
                .execute(
                    "INSERT INTO recordings VALUES ($id)",
                    &HashMap::from([("id".into(), Value::Int64(id))]),
                )
                .unwrap()
        });
        let report = db.run_maintenance_cycle().unwrap();
        writer.join().unwrap();
        assert!(report.compaction.ran);
        let (active, examined, moved) = persistence.storage_compaction_progress_for_test();
        assert!(examined <= 80);
        relocated += moved;
        assert_eq!(
            old_table.get(1000).unwrap().unwrap().value(),
            &[91_u8; 8192]
        );
        assert!(
            !report.compaction.handle_recycled,
            "the old storage reader still owns its handle"
        );
        if !active {
            complete = true;
            break;
        }
    }
    assert!(
        complete && relocated > 0,
        "live readers cannot starve durable relocation"
    );
    assert!(
        !db.execute("SELECT id FROM recordings", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    drop(old_table);
    drop(old);
    db.close().unwrap();
    drop(persistence);
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        !reopened
            .execute("SELECT id FROM recordings", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
}
