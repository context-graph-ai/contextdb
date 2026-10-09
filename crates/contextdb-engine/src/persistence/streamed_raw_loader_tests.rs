use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn fixture() -> (
    tempfile::TempDir,
    Arc<RedbPersistence>,
    VectorPartitionRef,
    VectorEntry,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("streamed-raw.db");
    let database = crate::Database::open(&path).unwrap();
    database
        .execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(1))",
            &HashMap::new(),
        )
        .expect("declare the physical vector fixture table and its maintained totals");
    database.close().unwrap();
    let persistence = Arc::new(RedbPersistence::open(&path).unwrap());
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "embedding"),
        VectorPartitionKey::unpartitioned(),
    );
    let entry = VectorEntry {
        index: partition.index.clone(),
        row_id: RowId(1),
        vector: vec![3.0],
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    };
    persistence
        .append_vector_entries_batch(std::slice::from_ref(&entry), &HashMap::new())
        .unwrap();
    persistence
        .append_vector_partition_memberships_batch(&[VectorPartitionMembershipRecord {
            index: partition.index.clone(),
            partition_key: partition.partition_key.clone(),
            row_id: entry.row_id,
            row_created_tx: entry.created_tx,
            row_lsn: entry.lsn,
            vector_created_tx: entry.created_tx,
            vector_lsn: entry.lsn,
            visible_from: entry.created_tx,
            deleted_tx: None,
        }])
        .unwrap();
    (directory, persistence, partition, entry)
}

#[test]
fn streamed_raw_body_finishes_while_close_contends_for_persistence_ownership() {
    let (_directory, persistence, partition, expected) = fixture();
    let (at_body_tx, at_body_rx) = mpsc::channel();
    let (close_contending_tx, close_contending_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let source = Arc::clone(&persistence);
    let stream = std::thread::spawn(move || {
        let result = source.stream_vector_partition_memberships(
            &partition,
            1,
            VectorQuantization::F32,
            |_| Ok(Vec::new()),
            |entries, membership, load_body| {
                at_body_tx.send(()).unwrap();
                close_contending_rx.recv().unwrap();
                entries.push(load_body(&membership)?);
                Ok(())
            },
        );
        assert_eq!(
            PERSISTENCE_OWNERSHIP_READS_FOR_TEST.with(std::cell::Cell::get),
            1,
            "streamed body loading must never reacquire persistence ownership"
        );
        finished_tx.send(result).unwrap();
    });
    at_body_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("stream failed before reaching the controlled body boundary");
    let closing_source = Arc::clone(&persistence);
    let close = std::thread::spawn(move || {
        // The streamed callback owns the original shared persistence guard.
        // Announce a competing close only after observing that contention.
        assert!(matches!(
            closing_source.db.try_write(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        close_contending_tx.send(()).unwrap();
        closing_source.close();
    });
    // A timeout is solely a deadlock guard, never a behavior/timing assertion.
    let entries = finished_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("streamed body read deadlocked behind concurrent close")
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0.vector, expected.vector);
    stream.join().unwrap();
    close.join().unwrap();
    assert!(persistence.with_db(|_| Ok(())).is_err());
}

#[test]
fn streamed_membership_and_body_use_the_same_root_after_concurrent_body_rewrite() {
    let (_directory, persistence, partition, expected) = fixture();
    let mut replacement = expected.clone();
    replacement.vector = vec![9.0];
    let entries = persistence
        .stream_vector_partition_memberships(
            &partition,
            1,
            VectorQuantization::F32,
            |count| {
                assert_eq!(count, 1);
                // Publication occurs after the stream pins/counts its root, before
                // any membership/body decode. A fresh body transaction sees 9.0.
                persistence.append_vector_entries_batch(
                    std::slice::from_ref(&replacement),
                    &HashMap::new(),
                )?;
                Ok(Vec::new())
            },
            |entries, membership, load_body| {
                entries.push(load_body(&membership)?);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0.vector, expected.vector);
    let current = persistence
        .load_vector_entries_for_memberships(
            &persistence.load_vector_partition_memberships().unwrap(),
            1,
            Some(VectorQuantization::F32),
        )
        .unwrap();
    assert_eq!(current[0].vector, replacement.vector);
}
