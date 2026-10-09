use super::*;
use contextdb_core::VectorPartitionComponent;
use redb::ReadableDatabase as _;

const ROWS: usize = 1024;
const LIMIT: usize = 3 * 1024 * 1024;

// Each request declares a separate reader funder; none borrows the writer accountant.
#[derive(Debug)]
struct LoaderReadBudget(Arc<MemoryAccountant>);
impl contextdb_core::read_memory::ReadMemoryBudget for LoaderReadBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.0.try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}
fn loader_read_request(
    limit: usize,
) -> (Arc<MemoryAccountant>, contextdb_vector::VectorCopyAdmission) {
    let reader = Arc::new(MemoryAccountant::with_budget(limit));
    let capability: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> =
        Arc::new(LoaderReadBudget(reader.clone()));
    (reader, crate::vector_read_memory::admission(capability))
}

fn retained_loader_metadata(
    writer: Arc<MemoryAccountant>,
    partition: &VectorPartitionRef,
) -> Arc<contextdb_vector::VectorCopyAdmission> {
    let mut retained = contextdb_vector::VectorCopyAdmission::new(writer);
    retained
        .reserve(
            std::mem::size_of::<DurableRawVectorPartitionLoader>()
                .saturating_add(std::mem::size_of::<contextdb_vector::VectorCopyAdmission>())
                .saturating_add(4 * std::mem::size_of::<usize>())
                .saturating_add(partition.index.table.len())
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )
        .expect("admit the retained loader metadata before its constructor clone");
    Arc::new(retained)
}
fn loader_metadata_bytes(loader: &DurableRawVectorPartitionLoader) -> usize {
    loader
        .metadata_copy_owner
        .as_ref()
        .expect("fixture retains its real column metadata owner")
        .retained_bytes()
}
fn assert_last_loader_owner_releases(
    loader: DurableRawVectorPartitionLoader,
    reader: &MemoryAccountant,
) {
    let writer = loader.accountant.clone();
    assert_eq!(writer.usage().used, loader_metadata_bytes(&loader));
    drop(loader);
    assert_eq!(
        writer.usage().used,
        0,
        "last loader releases its retained metadata"
    );
    assert_eq!(reader.usage().used, 0);
}

fn fixture() -> (
    tempfile::TempDir,
    DurableRawVectorPartitionLoader,
    Vec<RawVectorDirectoryEntry>,
) {
    fixture_with_rows(ROWS)
}

fn fixture_with_rows(
    row_count: usize,
) -> (
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
    database.execute(
        "CREATE TABLE small_vectors (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, embedding VECTOR(1) PARTITION_KEY(scope) MAX_PARTITIONS 1)",
        &HashMap::new(),
    ).expect("declare the loader fixture table before physical vector publication");
    database.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    let entries = (0..row_count)
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
    let mut raw_directory = memberships
        .iter()
        .map(|membership| RawVectorDirectoryEntry {
            membership: contextdb_vector::store::VectorMembershipProvenance::new(
                membership.row_created_tx,
                membership.row_lsn,
                membership.visible_from,
                membership.deleted_tx,
            ),
            row_id: membership.row_id,
            created_tx: membership.vector_created_tx,
            deleted_tx: membership.deleted_tx,
            lsn: membership.vector_lsn,
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
            metadata_copy_owner: Some(retained_loader_metadata(accountant.clone(), &partition)),
            source: DurableRawVectorSource::Writable(persistence),
            accountant,
            partition: partition.clone(),
            dimension: 1,
            quantization: VectorQuantization::F32,
        },
        raw_directory,
    )
}

#[test]
fn reopened_large_route_small_vectors_stream_under_same_tight_ceiling_and_release() {
    let (reader, mut read_owner) = loader_read_request(LIMIT);
    let (_directory, loader, directory) = fixture();
    // Replicating even one route per row exceeds this ceiling by 20x.
    const { assert!(ROWS * 60 * 1024 > LIMIT * 20 - LIMIT) };
    let loaded = loader.load(&directory, &mut read_owner).unwrap();
    assert_eq!(loaded.entries.len(), ROWS);
    for (entry, expected) in loaded.entries.iter().zip(&directory) {
        let entry = &entry.entry;
        assert_eq!(entry.row_id, expected.row_id);
        assert_eq!(entry.vector, vec![(entry.row_id.0 - 1) as f32]);
    }
    assert!(
        loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used
            > ROWS * 8
    );
    assert!(
        loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used
            < LIMIT
    );
    let retained = loaded.resident_bytes + loaded.workspace_owner.retained_bytes();
    assert_eq!(
        loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used,
        retained
    );
    drop(loaded);
    assert_eq!(reader.usage().used, 0);
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_last_loader_owner_releases(loader, &reader);
}

#[test]
fn reopened_route_scratch_refuses_before_decode_and_releases() {
    let (_directory, loader, directory) = fixture();
    let (reader, mut read_owner) = loader_read_request(1024);
    let error = match loader.load(&directory, &mut read_owner) {
        Err(error) => error,
        Ok(_) => panic!("load unexpectedly admitted"),
    };
    assert!(matches!(error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes: 1024, budget_limit_bytes: 1024, ..
    } if subsystem == "vector_copy" && operation == "materialize" && requested_bytes > 60 * 1024));
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_eq!(reader.usage().used, 0);
    assert_last_loader_owner_releases(loader, &reader);
}

#[test]
fn unknown_durable_memberships_are_counted_before_identity_allocation() {
    let (reader, mut read_owner) = loader_read_request(LIMIT);
    let (_directory, loader, directory) = fixture();
    // A small caller directory still validates and admits all durable identities.
    let one = &directory[..1];
    let full = loader.load(one, &mut read_owner).unwrap();
    let retained = full.resident_bytes + full.workspace_owner.retained_bytes();
    assert_eq!(
        loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used,
        retained
    );
    drop(full);
    assert_eq!(reader.usage().used, 0);
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_reads = reads.clone();
    let observation = redb::observe_backend_reads(Arc::new(move |_| {
        observed_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    let (one_byte_reader, mut one_byte_owner) = loader_read_request(1);
    let initial_request = match loader.load(one, &mut one_byte_owner) {
        Err(Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes: 1,
            budget_limit_bytes: 1,
            ..
        }) => requested_bytes,
        _ => panic!("expected initial one-byte reader refusal"),
    };
    assert_eq!(one_byte_reader.usage().used, 0);
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 0);
    let (constrained_reader, mut constrained_owner) = loader_read_request(initial_request);
    let error = match loader.load(one, &mut constrained_owner) {
        Err(error) => error,
        Ok(_) => panic!("identity allocation unexpectedly admitted"),
    };
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { operation, requested_bytes, available_bytes: 0, .. }
        if operation == "materialize" && requested_bytes >= ROWS * std::mem::size_of::<RawVectorDirectoryEntry>())
    );
    assert_eq!(constrained_reader.usage().used, 0);
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(observation);
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_eq!(reader.usage().used, 0);
    assert_last_loader_owner_releases(loader, &reader);
}

#[test]
fn streamed_unknown_membership_keeps_lifecycle_validation_and_release() {
    let (reader, mut read_owner) = loader_read_request(LIMIT);
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
        .load(&directory, &mut read_owner)
        .expect("a valid unknown lifecycle preserves every selected identity");
    assert_eq!(loaded.entries.len(), ROWS);
    for (entry, expected) in loaded.entries.iter().zip(&directory) {
        let entry = &entry.entry;
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
        std::mem::size_of::<contextdb_vector::store::LoadedRawVectorEntry>(),
        168,
        "route-free carrier layout must retain its measured exact membership and canonical stamp size"
    );
    let expected_workspace = ROWS
        * (std::mem::size_of::<contextdb_vector::store::LoadedRawVectorEntry>()
            + loader.partition.index.table.len()
            + loader.partition.index.column.len()
            + loader.dimension * std::mem::size_of::<f32>());
    assert_eq!(
        (
            loaded.resident_bytes,
            loaded.workspace_owner.retained_bytes()
        ),
        (4096, expected_workspace)
    );
    let returned_charge = loaded.resident_bytes + loaded.workspace_owner.retained_bytes();
    assert_eq!(
        loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used,
        returned_charge,
        "only returned body and workspace owners remain after membership validation"
    );
    assert_eq!(returned_charge, 4096 + expected_workspace);
    drop(loaded);
    // Dropping the unadopted carrier releases its distinct reader and writer owners.
    assert_eq!(reader.usage().used, 0);
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );

    // Keep visible_from equal to the authenticated birth identity. Deleting
    // at that boundary is invalid independently of envelope authentication,
    // and this unknown identity still must be checked by the streaming loader.
    let mut invalid = unknown;
    invalid.row_id = RowId(10_001);
    invalid.deleted_tx = Some(TxId(1));
    persistence
        .append_vector_partition_memberships_batch(&[invalid])
        .unwrap();
    let error = match loader.load(&directory, &mut read_owner) {
        Err(error) => error,
        Ok(loaded) => {
            drop(loaded);
            assert_eq!(reader.usage().used, 0);
            panic!("invalid unknown lifecycle admitted");
        }
    };
    assert!(
        error.to_string().contains("invalid lifecycle boundary"),
        "unknown lifecycle refusal: {error:?}"
    );
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader),
        "a failed unknown lifecycle releases every selected body and validation owner"
    );
    assert_eq!(reader.usage().used, 0);
    assert_last_loader_owner_releases(loader, &reader);
}

type DurableByteRows = Vec<(Vec<u8>, Vec<u8>)>;
type VectorPublicationRecords = Vec<(String, Option<DurableByteRows>)>;
fn vector_publication_records(persistence: &RedbPersistence) -> VectorPublicationRecords {
    use redb::ReadableTable as _;
    persistence
        .with_db(|db| {
            let read = db.begin_read().unwrap();
            let mut records = Vec::new();
            for name in [
                "vector_entries",
                "vector_partition_membership",
                "vector_open_directory",
                "vector_membership_source_pages",
                "vector_membership_reverse_pages",
                "vector_membership_summary_pages",
                "vector_partition_journal",
                "vector_partition_tombstones",
            ] {
                let rows = match read.open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name)) {
                    Ok(table) => Some(
                        table
                            .iter()
                            .unwrap()
                            .map(|row| {
                                let (key, value) = row.unwrap();
                                (key.value().to_vec(), value.value().to_vec())
                            })
                            .collect(),
                    ),
                    Err(redb::TableError::TableDoesNotExist(_)) => None,
                    Err(error) => panic!("observe {name}: {error}"),
                };
                records.push((name.to_owned(), rows));
            }
            for name in ["meta", "metadata", "compact_index_set_commitments"] {
                let rows = match read.open_table(redb::TableDefinition::<&str, &[u8]>::new(name)) {
                    Ok(table) => Some(
                        table
                            .iter()
                            .unwrap()
                            .map(|row| {
                                let (key, value) = row.unwrap();
                                (key.value().as_bytes().to_vec(), value.value().to_vec())
                            })
                            .collect(),
                    ),
                    Err(redb::TableError::TableDoesNotExist(_)) => None,
                    Err(error) => panic!("observe {name}: {error}"),
                };
                records.push((name.to_owned(), rows));
            }
            Ok(records)
        })
        .unwrap()
}

#[test]
fn streamed_membership_keeps_duplicate_and_stale_identity_detection() {
    assert_streamed_body_lifetime_neighbors();
    assert_single_membership_lifetime_admission();
    let (reader, mut read_owner) = loader_read_request(LIMIT);
    let (_directory, loader, mut directory) = fixture();
    directory[0].deleted_tx = Some(TxId(2));
    let error = match loader.load(&directory, &mut read_owner) {
        Err(error) => error,
        Ok(_) => panic!("stale identity admitted"),
    };
    assert!(
        error
            .to_string()
            .contains("identity changed outside its atomic publication")
    );
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_eq!(reader.usage().used, 0);
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
    let before_refusal = vector_publication_records(persistence);
    let error = persistence
        .append_vector_partition_memberships_batch(std::slice::from_ref(&duplicate))
        .unwrap_err();
    assert!(
        matches!(error, Error::StoreCorrupted { ref reason, .. }
            if reason.contains("overlapping placement lifetimes")),
        "{error:?}"
    );
    assert_eq!(
        vector_publication_records(persistence),
        before_refusal,
        "refused overlap changes no durable body, membership, directory, page, or publication metadata record"
    );
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_eq!(reader.usage().used, 0);
    // Independently corrupt the durable state: only this adversarial test seam
    // bypasses producer overlap validation; authenticated loading remains real.
    persistence
        .write_corrupt_store_vector_memberships_for_test(std::slice::from_ref(&duplicate))
        .expect("construct the adversarial overlapping corrupt-store fixture");
    let error = match loader.load(&directory, &mut read_owner) {
        Err(error) => error,
        Ok(_) => panic!("duplicate identity admitted"),
    };
    assert!(error.to_string().contains("duplicate directory identities"));
    assert_eq!(
        loader.accountant.usage().used,
        loader_metadata_bytes(&loader)
    );
    assert_eq!(reader.usage().used, 0);
    assert_last_loader_owner_releases(loader, &reader);
}

// Authenticated source order follows owning-row stamps, so lifetime validation
// must examine both interval neighbors across the whole durable stream.
fn assert_streamed_body_lifetime_neighbors() {
    // Only one canonical body is needed to test two placement lifetimes.
    const ROWS: usize = 1;
    for (name, first_from, first_end, second_from, second_end, accepted) in [
        ("ascending adjacent", 1, Some(3), 3, None, true),
        ("descending adjacent", 20, None, 2, Some(20), true),
        ("ascending overlap", 1, Some(4), 3, None, false),
        ("descending overlap", 20, None, 2, Some(21), false),
        ("equal starts", 3, None, 3, None, false),
    ] {
        for all in [false, true] {
            let (_temporary, loader, mut directory) = fixture_with_rows(ROWS);
            let (reader, mut owner) = loader_read_request(LIMIT);
            let DurableRawVectorSource::Writable(persistence) = &loader.source else {
                unreachable!()
            };
            let membership = |row_tx, from, end: Option<u64>| VectorPartitionMembershipRecord {
                index: loader.partition.index.clone(),
                partition_key: loader.partition.partition_key.clone(),
                row_id: RowId(1),
                row_created_tx: TxId(row_tx),
                row_lsn: Lsn(row_tx),
                vector_created_tx: TxId(1),
                vector_lsn: Lsn(1),
                visible_from: TxId(from),
                deleted_tx: end.map(TxId),
            };
            let first = membership(1, first_from, first_end);
            let second = membership(2, second_from, second_end);
            // Replace the fixture's row-1 placement and add another placement
            // of its same canonical body. This seam authenticates the source
            // while deliberately bypassing the producer's overlap refusal.
            persistence
                .write_corrupt_store_vector_memberships_for_test(&[first.clone(), second.clone()])
                .unwrap();
            let entry = |m: &VectorPartitionMembershipRecord| RawVectorDirectoryEntry {
                row_id: m.row_id,
                created_tx: m.vector_created_tx,
                deleted_tx: m.deleted_tx,
                lsn: m.vector_lsn,
                membership: contextdb_vector::store::VectorMembershipProvenance::new(
                    m.row_created_tx,
                    m.row_lsn,
                    m.visible_from,
                    m.deleted_tx,
                ),
            };
            let row_one = directory
                .iter_mut()
                .find(|item| item.row_id == RowId(1))
                .unwrap();
            *row_one = entry(&first);
            directory.push(entry(&second));
            let result = if all {
                loader.load_all(&mut owner)
            } else {
                loader.load(&directory, &mut owner)
            };
            match result {
                Ok(loaded) => {
                    assert!(accepted, "{name}, load_all={all}: overlap admitted");
                    assert_eq!(loaded.entries.len(), ROWS + 1);
                    let placements = loaded
                        .entries
                        .iter()
                        .filter(|item| item.entry.row_id == RowId(1))
                        .collect::<Vec<_>>();
                    assert_eq!(placements.len(), 2);
                    for expected in [&first, &second] {
                        let wanted = entry(expected);
                        let found = placements
                            .iter()
                            .find(|item| item.membership == wanted.membership)
                            .unwrap();
                        assert_eq!(found.entry.created_tx, TxId(1));
                        assert_eq!(found.entry.lsn, Lsn(1));
                        assert_eq!(found.entry.vector, vec![0.0]);
                        assert_eq!(found.canonical_deleted_tx, None);
                    }
                    assert_eq!(
                        reader.usage().used,
                        loaded.workspace_owner.retained_bytes(),
                        "lifetime validation credits must be gone before return"
                    );
                    drop(loaded);
                }
                Err(error) => {
                    assert!(!accepted, "{name}, load_all={all}: {error:?}");
                    assert!(
                        error.to_string().contains("duplicate directory identities"),
                        "{name}, load_all={all}: {error:?}"
                    );
                }
            }
            assert_eq!(reader.usage().used, 0, "{name}, load_all={all}");
            assert_eq!(
                loader.accountant.usage().used,
                loader_metadata_bytes(&loader)
            );
            assert_last_loader_owner_releases(loader, &reader);
        }
    }
}

// A first B-tree node has fixed allocation cost even for one membership.
fn assert_single_membership_lifetime_admission() {
    let (_temporary, loader, directory) = fixture_with_rows(1);
    for all in [false, true] {
        let (tiny, mut tiny_owner) = loader_read_request(1);
        let result = if all {
            loader.load_all(&mut tiny_owner)
        } else {
            loader.load(&directory, &mut tiny_owner)
        };
        let initial = match result {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes, ..
            }) => requested_bytes,
            _ => panic!("one-byte reader must refuse initial scratch"),
        };
        assert_eq!(tiny.usage().used, 0);
        // Selected small output is admitted before decoder scratch. Discover
        // that exact prefix independently from the refusal, not from layout
        // constants that belong to the returned-carrier proof.
        let (prefix_reader, mut prefix_owner) = loader_read_request(initial);
        let prefix_result = if all {
            loader.load_all(&mut prefix_owner)
        } else {
            loader.load(&directory, &mut prefix_owner)
        };
        let next_request = match prefix_result {
            Err(Error::MemoryBudgetExceeded {
                requested_bytes,
                available_bytes: 0,
                ..
            }) => requested_bytes,
            _ => panic!("exact first reservation must refuse the next reservation"),
        };
        assert_eq!(prefix_reader.usage().used, 0);
        let before_lifetime = if all {
            assert_eq!(
                next_request, 900,
                "load_all admits lifetime node after decoder scratch"
            );
            initial
        } else {
            assert!(
                next_request > 1024 * 1024,
                "selected load admits decoder after exact output"
            );
            initial.checked_add(next_request).unwrap()
        };
        // Native key32 + Option<TxId>16: four slots196 plus first-node704.
        let limit = before_lifetime + 900 - 1;
        let (reader, mut owner) = loader_read_request(limit);
        let result = if all {
            loader.load_all(&mut owner)
        } else {
            loader.load(&directory, &mut owner)
        };
        assert!(
            matches!(
                result,
                Err(Error::MemoryBudgetExceeded {
                    requested_bytes: 900,
                    available_bytes: 899,
                    ..
                })
            ),
            "first membership must admit its full node before allocation, load_all={all}"
        );
        assert_eq!(reader.usage().used, 0);
        assert_eq!(
            loader.accountant.usage().used,
            loader_metadata_bytes(&loader)
        );
        let (reader, mut owner) = loader_read_request(LIMIT);
        let loaded = if all {
            loader.load_all(&mut owner)
        } else {
            loader.load(&directory, &mut owner)
        }
        .unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].entry.row_id, RowId(1));
        assert_eq!(loaded.entries[0].entry.vector, vec![0.0]);
        assert_eq!(reader.usage().used, loaded.workspace_owner.retained_bytes());
        drop(loaded);
        assert_eq!(reader.usage().used, 0);
        assert_eq!(
            loader.accountant.usage().used,
            loader_metadata_bytes(&loader)
        );
    }
    let (reader, _owner) = loader_read_request(LIMIT);
    assert_last_loader_owner_releases(loader, &reader);
}
