use super::*;
use crate::Database;
use contextdb_core::Value;
use contextdb_relational::mem::BoundedOrderedRowCursor;

fn fixture(path: &Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE postings (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX by_bucket ON postings(bucket)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..5 {
        db.execute(
            "INSERT INTO postings VALUES ($id, 1)",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn cursor(reverse: bool) -> BoundedOrderedRowCursor {
    let mut cursor = BoundedOrderedRowCursor::for_index("by_bucket".into(), reverse);
    cursor.select_identities_only();
    cursor.source_configure(1);
    cursor
}

fn pull(load: &ReadPersistenceLoad, cursor: &mut BoundedOrderedRowCursor) -> Result<Option<RowId>> {
    let meta = &load.image.table_meta["postings"];
    let declaration = RedbPersistence::relational_index_declarations(meta)
        .into_iter()
        .find(|d| d.name == "by_bucket")
        .unwrap();
    let snapshot = SnapshotId(
        load.image
            .newest_committed_tx_for_test()
            .map(|tx| tx.0)
            .unwrap(),
    );
    let persistence_fixture_parent_1 =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            load.source
                .verified_ordered_runs
                .memory
                .accountant
                .usage()
                .limit
                .map(|bytes| bytes as u64)
                .unwrap_or(u64::MAX),
        );
    let persistence_fixture_reader_1 = persistence_fixture_parent_1
        .child()
        .expect("admit the persistence fixture reader child");

    load.source
        .bounded_relational_ordered_next(
            "postings",
            meta,
            &declaration,
            snapshot,
            cursor,
            &mut || Ok(()),
            &mut || Ok(()),
            &mut |_| Ok(()),
            persistence_fixture_reader_1,
        )
        .map(|row| row.map(|row| row.row_id))
}

#[test]
#[allow(clippy::single_element_loop)] // Preserve the fixture loop surrounding unchanged assertions.
fn independent_forward_canonical_oracle() {
    for damage in ["missing", "extra", "changed", "hidden"] {
        for reverse in [false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("physical.db");
            fixture(&path);
            let db = redb::Database::open(&path).unwrap();
            let write = db.begin_write().unwrap();
            let name = RedbPersistence::relational_index_table_name("postings", "by_bucket");
            {
                let mut table = write
                    .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
                        &name,
                    ))
                    .unwrap();
                let (key, value) = {
                    let (key, value) = table.first().unwrap().unwrap();
                    (key.value(), value.value().to_vec())
                };
                let position = key.decode().unwrap();
                match damage {
                    "missing" => {
                        table.remove(key).unwrap();
                    }
                    "extra" | "changed" => {
                        if damage == "changed" {
                            table.remove(key).unwrap();
                        }
                        let other = DurableRelationalIndexKey::new(
                            position.key,
                            RowId(u64::MAX - 1),
                            position.created_tx,
                        )
                        .unwrap();
                        table.insert(other, value.as_slice()).unwrap();
                    }
                    "hidden" => {
                        let tombstone =
                            RedbPersistence::encode(&Some(position.created_tx)).unwrap();
                        table.insert(key, tombstone.as_slice()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            write.commit().unwrap();
            drop(db);
            let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
            assert!(
                matches!(
                    pull(&load, &mut cursor(reverse)),
                    Err(Error::StoreCorrupted { .. })
                ),
                "{damage}, reverse={reverse}"
            );
        }
    }
}

#[test]
#[allow(clippy::single_element_loop)] // Preserve the fixture loop surrounding unchanged assertions.
fn independent_reverse_canonical_oracle() {
    for damage in ["missing", "extra", "changed", "hidden"] {
        for reverse in [true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("physical.db");
            fixture(&path);
            let db = redb::Database::open(&path).unwrap();
            let write = db.begin_write().unwrap();
            let name = RedbPersistence::relational_index_table_name("postings", "by_bucket");
            {
                let mut table = write
                    .open_table(TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(
                        &name,
                    ))
                    .unwrap();
                let (key, value) = {
                    let (key, value) = table.first().unwrap().unwrap();
                    (key.value(), value.value().to_vec())
                };
                let position = key.decode().unwrap();
                match damage {
                    "missing" => {
                        table.remove(key).unwrap();
                    }
                    "extra" | "changed" => {
                        if damage == "changed" {
                            table.remove(key).unwrap();
                        }
                        let other = DurableRelationalIndexKey::new(
                            position.key,
                            RowId(u64::MAX - 1),
                            position.created_tx,
                        )
                        .unwrap();
                        table.insert(other, value.as_slice()).unwrap();
                    }
                    "hidden" => {
                        let tombstone =
                            RedbPersistence::encode(&Some(position.created_tx)).unwrap();
                        table.insert(key, tombstone.as_slice()).unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            write.commit().unwrap();
            drop(db);
            let load = load_read_image_with_memory(&path, Some(8 * 1024 * 1024)).unwrap();
            assert!(
                matches!(
                    pull(&load, &mut cursor(reverse)),
                    Err(Error::StoreCorrupted { .. })
                ),
                "{damage}, reverse={reverse}"
            );
        }
    }
}
