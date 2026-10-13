use super::*;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge-errors.db");
    let seed = crate::Database::open(&path).unwrap();
    seed.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
    seed.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 24, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for id in 0..4 {
        seed.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    seed.run_maintenance_cycle().unwrap();
    seed.close().unwrap();
    drop(seed);
    let persistence = RedbPersistence::open(&path).unwrap();
    (directory, persistence)
}

fn write_marker(persistence: &RedbPersistence, bytes: &[u8]) {
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(PURGE_PUBLICATION_METADATA_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(PURGE_PUBLICATION_MARKER_KEY, bytes)
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}

#[test]
fn malformed_retention_deferred_edge_count_reports_typed_store_corruption() {
    let (_directory, persistence) = writer();
    let mut workspace = crate::memory_accounting::OwnedMemoryReservation::new(
        Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into(),
    );
    assert!(
        persistence
            .load_retention_deferred_edge_nodes_admitted(&mut workspace)
            .unwrap()
            .is_empty()
    );
    // The u16 count prefix is present, but both count bytes are missing.
    let malformed = [251_u8];
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(CONFIG_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(RETENTION_DEFERRED_EDGE_NODES, malformed.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let error = persistence
        .load_retention_deferred_edge_nodes_admitted(&mut workspace)
        .unwrap_err();
    assert!(
        matches!(&error, Error::StoreCorrupted { path, reason }
        if path == &persistence.path().display().to_string()
            && reason == "bincode retention count decode error: UnexpectedEnd { additional: 2 }"),
        "malformed retention count must report typed corruption, path, and decode reason: {error:?}"
    );
}

#[test]
fn malformed_durable_purge_marker_bytes_report_typed_store_corruption() {
    let (_directory, persistence) = writer();
    assert!(
        persistence
            .durable_purge_publication_marker()
            .unwrap()
            .is_none()
    );
    let token = [7_u8; 16];
    let frontier = Lsn(123);
    let mut bytes = token.to_vec();
    bytes.extend_from_slice(&frontier.0.to_le_bytes());
    write_marker(&persistence, &bytes);
    let marker = persistence
        .durable_purge_publication_marker()
        .unwrap()
        .unwrap();
    assert_eq!(marker.token, token);
    assert_eq!(marker.frontier, frontier);
    for length in [0, 15, 16, 23, 25] {
        let mut malformed = bytes.clone();
        malformed.resize(length, 0);
        write_marker(&persistence, &malformed);
        let error = persistence.durable_purge_publication_marker().unwrap_err();
        assert!(
            matches!(&error, Error::StoreCorrupted { path, reason }
            if path == &persistence.path().display().to_string()
                && reason.contains("durable purge publication marker has the wrong length")),
            "malformed marker length {length} must name typed corruption and path: {error:?}"
        );
    }
}

#[test]
fn invalid_purge_generation_candidates_report_typed_store_corruption() {
    let (_directory, persistence) = writer();
    let index = VectorIndexRef::new("items", "v");
    let partition_key = VectorPartitionKey::unpartitioned();
    let catalog = persistence
        .load_vector_partition_generation_catalog_for(&index, &partition_key)
        .unwrap()
        .unwrap();
    assert!(catalog.change.is_none());
    let base = persistence
        .load_vector_partition_base_generation(&index, &partition_key, catalog.base.generation_id)
        .unwrap();
    let candidate = VectorPartitionGenerationCandidate {
        base,
        change: None,
        catalog,
    };
    let mut projection = AuthoritativePurgePersistenceProjection {
        row_versions: Vec::new(),
        source_provenance: Vec::new(),
        vectors: Vec::new(),
        vector_partition_identities: vec![VectorPartitionVersionIdentity {
            index,
            partition_key,
            row_id: RowId(1),
            row_created_tx: TxId(1),
            row_lsn: Lsn(1),
            vector_created_tx: TxId(1),
            vector_lsn: Lsn(1),
        }],
        vector_generation_candidates: vec![candidate.clone()],
        graph_entries: Vec::new(),
        sink_entries: Vec::new(),
        change_log_entries: Vec::new(),
        config_keys_removed: Vec::new(),
        lifecycle_records: Vec::new(),
        purge_delivery_items: Vec::new(),
        blob_purge: Default::default(),
    };
    assert_eq!(
        persistence
            .prepare_authoritative_purge_vector_generations(&projection)
            .unwrap()
            .len(),
        1
    );
    projection.vector_generation_candidates.push(candidate);
    let error = persistence
        .prepare_authoritative_purge_vector_generations(&projection)
        .err()
        .unwrap();
    assert!(
        matches!(&error, Error::StoreCorrupted { path, reason }
        if path == &persistence.path().display().to_string()
            && reason.contains("two replacement generations for one partition")),
        "duplicate candidate must report typed corruption: {error:?}"
    );
    projection.vector_generation_candidates.pop();
    projection.vector_partition_identities.clear();
    let error = persistence
        .prepare_authoritative_purge_vector_generations(&projection)
        .err()
        .unwrap();
    assert!(
        matches!(&error, Error::StoreCorrupted { path, reason }
        if path == &persistence.path().display().to_string()
            && reason.contains("outside its affected partition: items.v")),
        "out-of-boundary candidate must report typed corruption: {error:?}"
    );
}
