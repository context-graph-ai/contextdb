use super::*;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("purge-errors.db");
    let seed = crate::Database::open(&path).unwrap();
    seed.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
 seed.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED )", &HashMap::new()).unwrap();
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

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
        if path == persistence.path().display().to_string().as_str()
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
            if path == persistence.path().display().to_string().as_str()
                && reason.contains("durable purge publication marker has the wrong length")),
            "malformed marker length {length} must name typed corruption and path: {error:?}"
        );
    }
}
