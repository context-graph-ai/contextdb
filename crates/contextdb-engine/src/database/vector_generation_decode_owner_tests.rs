use super::*;
use std::sync::atomic::AtomicUsize;

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

fn resident_owner_bound(quantization: VectorQuantization) {
    let layout = VectorIndexLayout::unpartitioned(64, quantization);
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("decode_items", "embedding"),
        VectorPartitionKey::unpartitioned(),
    );
    let policy = layout.resolve_policy(16, 1);
    let direct =
        VectorGenerationWorkspacePlan::new(32, 16, &layout, &partition, policy, 8192).unwrap();
    let mut resident = direct;
    resident.assign_raw_decode_owner(true, false);
    assert_eq!(resident.raw_decode_scratch_bytes, 0);
    assert_eq!(
        resident.build_phase_bytes(),
        direct.build_phase_bytes() - direct.raw_decode_scratch_bytes
    );
    assert!(resident.raw_entry_bytes > resident.selected_entry_bytes);
    assert_eq!(resident.graph_build_bytes, direct.graph_build_bytes);
    assert_eq!(resident.raw_entry_bytes, direct.raw_entry_bytes);
    assert_eq!(resident.selected_entry_bytes, direct.selected_entry_bytes);
    assert_eq!(
        resident.durable_metadata_bytes,
        direct.durable_metadata_bytes
    );
    assert_eq!(
        resident.encoding_phase_bytes(4096),
        direct.encoding_phase_bytes(4096)
    );
    assert_eq!(
        resident.persistence_phase_bytes(4096),
        direct.persistence_phase_bytes(4096)
    );
    assert_eq!(
        resident.prepared_phase_bytes(4096),
        direct.prepared_phase_bytes(4096)
    );
    let accountant = MemoryAccountant::with_budget(resident.build_phase_bytes());
    assert!(
        accountant
            .try_allocate_for(
                direct.build_phase_bytes(),
                "vector_index",
                "prepare_vector_generation",
                "test"
            )
            .is_err()
    );
    assert_eq!(accountant.usage().used, 0);
    accountant
        .try_allocate_for(
            resident.build_phase_bytes(),
            "vector_index",
            "prepare_vector_generation",
            "test",
        )
        .unwrap();
    assert_eq!(accountant.usage().used, resident.build_phase_bytes());
    accountant.release(resident.build_phase_bytes());
    assert_eq!(accountant.usage().used, 0);
}

#[test]
fn resident_f32_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::F32);
}

#[test]
fn resident_sq8_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::SQ8);
}

#[test]
fn resident_sq4_generation_funds_snapshot_without_a_decoder_owner() {
    resident_owner_bound(VectorQuantization::SQ4);
}

#[test]
fn direct_cold_and_delete_carrier_generations_keep_the_decode_floor() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let layout = VectorIndexLayout::unpartitioned(1, quantization);
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("retained_items", "embedding"),
            VectorPartitionKey::unpartitioned(),
        );
        // Retained raw history has no live graph points, but still decodes.
        let direct = VectorGenerationWorkspacePlan::new(
            1,
            0,
            &layout,
            &partition,
            layout.resolve_policy(0, 1),
            0,
        )
        .unwrap();
        assert!(direct.raw_decode_scratch_bytes > 1024 * 1024);
        assert_eq!(direct.selected_entry_bytes, 0);
        for (state_present, delete_carriers) in [(false, false), (false, true), (true, true)] {
            let mut plan = direct;
            plan.assign_raw_decode_owner(state_present, delete_carriers);
            assert_eq!(
                plan.raw_decode_scratch_bytes,
                direct.raw_decode_scratch_bytes
            );
            assert_eq!(plan.build_phase_bytes(), direct.build_phase_bytes());
            let accountant = MemoryAccountant::with_budget(plan.build_phase_bytes() - 1);
            assert!(
                accountant
                    .try_allocate_for(
                        plan.build_phase_bytes(),
                        "vector_index",
                        "prepare_vector_generation",
                        "test",
                    )
                    .is_err()
            );
            assert_eq!(accountant.usage().used, 0);
        }
    }
}

#[test]
fn dormant_partition_loader_admits_its_decode_floor_before_reads_and_releases() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("decode-owner.db");
        let database = Database::open(&path).unwrap();
        database
            .execute(
                "CREATE TABLE decode_items (id INTEGER PRIMARY KEY, embedding VECTOR(1))",
                &HashMap::new(),
            )
            .expect("declare the decoder fixture table before physical vector publication");
        database.close().unwrap();
        let persistence = Arc::new(RedbPersistence::open_with_cache(&path, 0).unwrap());
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("decode_items", "embedding"),
            VectorPartitionKey::unpartitioned(),
        );
        let entry = VectorEntry {
            index: partition.index.clone(),
            row_id: RowId(1),
            vector: vec![0.25],
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        };
        persistence
            .append_vector_entries_batch(
                std::slice::from_ref(&entry),
                &HashMap::from([(partition.index.clone(), quantization)]),
            )
            .unwrap();
        let membership = VectorPartitionMembershipRecord {
            index: partition.index.clone(),
            partition_key: partition.partition_key.clone(),
            row_id: entry.row_id,
            row_created_tx: entry.created_tx,
            row_lsn: entry.lsn,
            vector_created_tx: entry.created_tx,
            vector_lsn: entry.lsn,
            visible_from: entry.created_tx,
            deleted_tx: None,
        };
        persistence
            .append_vector_partition_memberships_batch(std::slice::from_ref(&membership))
            .unwrap();
        let raw_directory = [RawVectorDirectoryEntry {
            membership: contextdb_vector::store::VectorMembershipProvenance::new(
                membership.row_created_tx,
                membership.row_lsn,
                membership.visible_from,
                membership.deleted_tx,
            ),
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: None,
            lsn: entry.lsn,
        }];
        let accountant = Arc::new(MemoryAccountant::with_budget(1024 * 1024));
        let loader = DurableRawVectorPartitionLoader {
            metadata_copy_owner: Some(retained_loader_metadata(accountant.clone(), &partition)),
            source: DurableRawVectorSource::Writable(persistence),
            accountant: accountant.into(),
            partition: partition.clone(),
            dimension: 1,
            quantization,
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&reads);
        let observation = redb::observe_backend_reads(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        let (reader, mut read_owner) = loader_read_request(1024 * 1024);
        let error = match loader.load(&raw_directory, &mut read_owner) {
            Err(error) => error,
            Ok(_) => panic!("loader omitted its independently owned decoder scratch"),
        };
        let (failed_requested, failed_available) = match error {
            Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                ..
            } if subsystem == "vector_copy"
                && operation == "materialize"
                && budget_limit_bytes == 1024 * 1024 =>
            {
                (requested_bytes, available_bytes)
            }
            other => panic!("executing reader must refuse its decoder scratch: {other:?}"),
        };
        assert!(failed_requested > 1024 * 1024);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        drop(observation);
        assert_eq!(
            loader.accountant.usage().used,
            loader_metadata_bytes(&loader)
        );
        let source = loader.source.clone();
        assert_last_loader_owner_releases(loader, &reader);
        let accountant = Arc::new(MemoryAccountant::with_budget(2 * 1024 * 1024));
        let loader = DurableRawVectorPartitionLoader {
            metadata_copy_owner: Some(retained_loader_metadata(accountant.clone(), &partition)),
            source,
            accountant: accountant.into(),
            partition: partition.clone(),
            dimension: 1,
            quantization,
        };
        assert_eq!(reader.usage().used, 0);
        let (reader, mut read_owner) = loader_read_request(2 * 1024 * 1024);
        let loaded = loader.load(&raw_directory, &mut read_owner).unwrap();
        assert_eq!(
            failed_available + loaded.workspace_owner.retained_bytes(),
            1024 * 1024,
            "only the exact returned workspace preceded the refused transient reservation"
        );
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries[0].entry.vector, vec![0.25]);
        let retained = loaded.resident_bytes + loaded.workspace_owner.retained_bytes();
        assert_eq!(
            loader.accountant.usage().used - loader_metadata_bytes(&loader) + reader.usage().used,
            retained
        );
        assert!(
            retained < 1024 * 1024,
            "decoder scratch died before publication"
        );
        drop(loaded);
        assert_eq!(reader.usage().used, 0);
        assert_eq!(
            loader.accountant.usage().used,
            loader_metadata_bytes(&loader)
        );
        assert_last_loader_owner_releases(loader, &reader);
    }
}
