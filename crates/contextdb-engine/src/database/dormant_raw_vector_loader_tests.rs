use super::*;
use contextdb_core::VectorPartitionComponent;

const ROWS: usize = 1024;
const LIMIT: usize = 3 * 1024 * 1024;

fn fixture() -> (
    tempfile::TempDir,
    DurableRawVectorPartitionLoader,
    Vec<RawVectorDirectoryEntry>,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dormant.db");
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("small_vectors", "embedding"),
        VectorPartitionKey::from_components(vec![VectorPartitionComponent::Text(
            "x".repeat(60 * 1024),
        )]),
    );
    let database = Database::open(&path).unwrap();
    database.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    let entries = (0..ROWS)
        .map(|ordinal| VectorEntry {
            index: partition.index.clone(),
            row_id: RowId(ordinal as u64 + 1),
            vector: vec![ordinal as f32],
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        })
        .collect::<Vec<_>>();
    persistence
        .append_vector_entries_batch(&entries, &HashMap::new())
        .unwrap();
    let memberships = entries
        .iter()
        .map(|entry| VectorPartitionMembershipRecord {
            index: entry.index.clone(),
            partition_key: partition.partition_key.clone(),
            row_id: entry.row_id,
            row_created_tx: entry.created_tx,
            row_lsn: entry.lsn,
            vector_created_tx: entry.created_tx,
            vector_lsn: entry.lsn,
            visible_from: entry.created_tx,
            deleted_tx: entry.deleted_tx,
        })
        .collect::<Vec<_>>();
    persistence
        .append_vector_partition_memberships_batch(&memberships)
        .unwrap();
    let mut raw_directory = entries
        .iter()
        .map(|entry| RawVectorDirectoryEntry {
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
        .collect::<Vec<_>>();
    raw_directory.reverse(); // Caller ordering must survive streamed table order.
    drop(memberships);
    drop(persistence);
    let persistence = Arc::new(RedbPersistence::open(&path).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(LIMIT));
    persistence.attach_memory_accountant(Arc::clone(&accountant));
    (
        directory,
        DurableRawVectorPartitionLoader {
            source: DurableRawVectorSource::Writable(persistence),
            accountant,
            partition,
            dimension: 1,
            quantization: VectorQuantization::F32,
        },
        raw_directory,
    )
}

#[test]
fn reopened_large_route_small_vectors_stream_under_same_tight_ceiling_and_release() {
    let (_directory, loader, directory) = fixture();
    // Replicating even one route per row exceeds this ceiling by 20x.
    const { assert!(ROWS * 60 * 1024 > LIMIT * 20 - LIMIT) };
    let loaded = loader.load(&directory).unwrap();
    assert_eq!(loaded.entries.len(), ROWS);
    for (entry, expected) in loaded.entries.iter().zip(&directory) {
        assert_eq!(entry.row_id, expected.row_id);
        assert_eq!(entry.vector, vec![(entry.row_id.0 - 1) as f32]);
    }
    assert!(loader.accountant.usage().used > ROWS * 8);
    assert!(loader.accountant.usage().used < LIMIT);
    let retained = loaded.resident_bytes + loaded.workspace_bytes;
    drop(loaded);
    loader.accountant.release(retained);
    assert_eq!(loader.accountant.usage().used, 0);
}

#[test]
fn reopened_route_scratch_refuses_before_decode_and_releases() {
    let (_directory, mut loader, directory) = fixture();
    loader.accountant = Arc::new(MemoryAccountant::with_budget(1024));
    let error = match loader.load(&directory) {
        Err(error) => error,
        Ok(_) => panic!("load unexpectedly admitted"),
    };
    assert!(matches!(error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes: 1024, budget_limit_bytes: 1024, ..
    } if subsystem == "vector_store" && operation == "load_raw_vector_partition" && requested_bytes > 60 * 1024));
    assert_eq!(loader.accountant.usage().used, 0);
}

#[test]
fn unknown_durable_memberships_are_counted_before_identity_allocation() {
    let (_directory, loader, directory) = fixture();
    // A small caller directory still validates and admits all durable identities.
    let one = &directory[..1];
    let full = loader.load(one).unwrap();
    let retained = full.resident_bytes + full.workspace_bytes;
    drop(full);
    loader.accountant.release(retained);
    let initial_request = match (DurableRawVectorPartitionLoader {
        accountant: Arc::new(MemoryAccountant::with_budget(1)),
        source: loader.source.clone(),
        partition: loader.partition.clone(),
        dimension: 1,
        quantization: VectorQuantization::F32,
    })
    .load(one)
    {
        Err(Error::MemoryBudgetExceeded {
            requested_bytes, ..
        }) => requested_bytes,
        _ => panic!("expected initial refusal"),
    };
    let constrained = DurableRawVectorPartitionLoader {
        accountant: Arc::new(MemoryAccountant::with_budget(initial_request)),
        source: loader.source.clone(),
        partition: loader.partition.clone(),
        dimension: 1,
        quantization: VectorQuantization::F32,
    };
    let error = match constrained.load(one) {
        Err(error) => error,
        Ok(_) => panic!("identity allocation unexpectedly admitted"),
    };
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { operation, requested_bytes, available_bytes: 0, .. }
        if operation == "load_raw_vector_partition" && requested_bytes >= ROWS * std::mem::size_of::<RawVectorDirectoryEntry>())
    );
    assert_eq!(constrained.accountant.usage().used, 0);
}

#[test]
fn streamed_unknown_membership_keeps_lifecycle_validation_and_release() {
    let (_directory, loader, directory) = fixture();
    let DurableRawVectorSource::Writable(persistence) = &loader.source else {
        unreachable!()
    };
    let unknown_bodies = [10_000, 10_001].map(|id| VectorEntry {
        index: loader.partition.index.clone(),
        row_id: RowId(id),
        vector: vec![id as f32],
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    });
    persistence
        .append_vector_entries_batch(&unknown_bodies, &HashMap::new())
        .unwrap();
    let unknown = VectorPartitionMembershipRecord {
        index: loader.partition.index.clone(),
        partition_key: loader.partition.partition_key.clone(),
        row_id: RowId(10_000),
        row_created_tx: TxId(1),
        row_lsn: Lsn(1),
        vector_created_tx: TxId(1),
        vector_lsn: Lsn(1),
        visible_from: TxId(1),
        deleted_tx: None,
    };
    persistence
        .append_vector_partition_memberships_batch(std::slice::from_ref(&unknown))
        .unwrap();
    let loaded = loader
        .load(&directory)
        .expect("a valid unknown lifecycle preserves every selected identity");
    assert_eq!(loaded.entries.len(), ROWS);
    for (entry, expected) in loaded.entries.iter().zip(&directory) {
        assert_eq!(
            (entry.row_id, entry.created_tx, entry.deleted_tx, entry.lsn),
            (
                expected.row_id,
                expected.created_tx,
                expected.deleted_tx,
                expected.lsn
            )
        );
        assert_eq!(entry.vector, vec![(entry.row_id.0 - 1) as f32]);
    }
    assert_eq!(
        (loaded.resident_bytes, loaded.workspace_bytes),
        (4096, 141312)
    );
    let returned_charge = loaded.resident_bytes + loaded.workspace_bytes;
    assert_eq!(
        loader.accountant.usage().used,
        returned_charge,
        "only returned body and workspace owners remain after membership validation"
    );
    assert_eq!(returned_charge, 145408);
    drop(loaded);
    // The partition loader transfers these owners to its caller for adoption
    // into stored residency. This caller discards the result instead.
    loader.accountant.release(returned_charge);
    assert_eq!(loader.accountant.usage().used, 0);

    // Keep visible_from equal to the authenticated birth identity. Deleting
    // at that boundary is invalid independently of envelope authentication,
    // and this unknown identity still must be checked by the streaming loader.
    let mut invalid = unknown;
    invalid.row_id = RowId(10_001);
    invalid.deleted_tx = Some(TxId(1));
    persistence
        .append_vector_partition_memberships_batch(&[invalid])
        .unwrap();
    let error = match loader.load(&directory) {
        Err(error) => error,
        Ok(loaded) => {
            let returned_charge = loaded.resident_bytes + loaded.workspace_bytes;
            drop(loaded);
            loader.accountant.release(returned_charge);
            panic!("invalid unknown lifecycle admitted");
        }
    };
    assert!(
        error.to_string().contains("invalid lifecycle boundary"),
        "unknown lifecycle refusal: {error:?}"
    );
    assert_eq!(
        loader.accountant.usage().used,
        0,
        "a failed unknown lifecycle releases every selected body and validation owner"
    );
}

#[test]
fn streamed_membership_keeps_duplicate_and_stale_identity_detection() {
    let (_directory, loader, mut directory) = fixture();
    directory[0].deleted_tx = Some(TxId(2));
    let error = match loader.load(&directory) {
        Err(error) => error,
        Ok(_) => panic!("stale identity admitted"),
    };
    assert!(
        error
            .to_string()
            .contains("identity changed outside its atomic publication")
    );
    assert_eq!(loader.accountant.usage().used, 0);
    directory[0].deleted_tx = None;
    let DurableRawVectorSource::Writable(persistence) = &loader.source else {
        unreachable!()
    };
    // Different row-version keys can name the same durable vector identity.
    let duplicate = VectorPartitionMembershipRecord {
        index: loader.partition.index.clone(),
        partition_key: loader.partition.partition_key.clone(),
        row_id: RowId(1),
        row_created_tx: TxId(2),
        row_lsn: Lsn(2),
        vector_created_tx: TxId(1),
        vector_lsn: Lsn(1),
        visible_from: TxId(2),
        deleted_tx: None,
    };
    persistence
        .append_vector_partition_memberships_batch(&[duplicate])
        .unwrap();
    let error = match loader.load(&directory) {
        Err(error) => error,
        Ok(_) => panic!("duplicate identity admitted"),
    };
    assert!(error.to_string().contains("duplicate directory identities"));
    assert_eq!(loader.accountant.usage().used, 0);
}
