use super::*;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("prior-base.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    (directory, persistence)
}

fn base(graph_len: usize) -> VectorPartitionBaseGenerationRecord {
    VectorPartitionBaseGenerationRecord {
        index: VectorIndexRef::new("items", "embedding"),
        partition_key: VectorPartitionKey::from_components(vec![
            contextdb_core::VectorPartitionComponent::Text("tenant".into()),
        ]),
        generation_id: 7,
        base_tx: TxId(4),
        base_lsn: Lsn(5),
        highest_included_tx: TxId(6),
        highest_included_lsn: Lsn(8),
        dimension: 768,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![13; graph_len]),
        state_digest: [17; 32],
    }
}

fn put(persistence: &RedbPersistence, key: &[u8], encoded: &[u8]) {
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(key, encoded)
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}

#[test]
fn prior_base_graph_above_serde_initial_cap_has_exact_owned_capacity() {
    let (_directory, persistence) = writer();
    let original = base(1024 * 1024 + 13);
    let key = RedbPersistence::vector_partition_base_generation_key(&original);
    let encoded = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::BaseGeneration,
        &original,
    )
    .unwrap();
    put(&persistence, &key, &encoded);
    let loaded = persistence
        .load_vector_partition_base_generation(
            &original.index,
            &original.partition_key,
            original.generation_id,
        )
        .unwrap();
    assert_eq!(
        loaded, original,
        "owned decoding preserves the physical format"
    );
    assert_eq!(loaded.graph.bytes.capacity(), loaded.graph.bytes.len());
    assert!(loaded.graph.bytes.capacity() <= encoded.len());
    eprintln!(
        "prior_base_graph_len={} owned_capacity={} durable_len={}",
        loaded.graph.bytes.len(),
        loaded.graph.bytes.capacity(),
        encoded.len()
    );
}

#[test]
fn prior_base_borrowed_decode_preserves_envelope_graph_and_identity_validation() {
    let (_directory, persistence) = writer();
    let original = base(17);
    let key = RedbPersistence::vector_partition_base_generation_key(&original);
    let encode = |record: &VectorPartitionBaseGenerationRecord| {
        RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            record,
        )
        .unwrap()
    };
    let assert_corruption = |encoded: &[u8], expected: &str| {
        put(&persistence, &key, encoded);
        let error = persistence
            .load_vector_partition_base_generation(
                &original.index,
                &original.partition_key,
                original.generation_id,
            )
            .unwrap_err();
        assert!(
            matches!(&error, Error::StoreCorrupted { reason, .. } if reason.contains(expected)),
            "expected {expected}: {error:?}"
        );
    };

    let mut damaged_envelope = encode(&original);
    damaged_envelope[0] ^= 1;
    assert_corruption(&damaged_envelope, "checksum mismatch");
    let mut damaged_graph = original.clone();
    damaged_graph.graph.checksum[0] ^= 1;
    assert_corruption(
        &encode(&damaged_graph),
        "immutable graph byte checksum mismatch",
    );

    let mut record = original.clone();
    record.index.column = "wrong".into();
    assert_corruption(&encode(&record), "route prefix does not match");
    let mut record = original.clone();
    record.partition_key = VectorPartitionKey::unpartitioned();
    assert_corruption(&encode(&record), "route prefix does not match");
    let mut invalid_records = Vec::new();
    let mut record = original.clone();
    record.generation_id += 1;
    invalid_records.push(record);
    let mut record = original.clone();
    record.dimension = 0;
    invalid_records.push(record);
    let mut record = original.clone();
    record.highest_included_tx = TxId(record.base_tx.0 - 1);
    invalid_records.push(record);
    let mut record = original.clone();
    record.highest_included_lsn = Lsn(record.base_lsn.0 - 1);
    invalid_records.push(record);
    for record in invalid_records {
        assert_corruption(
            &encode(&record),
            "identity, dimension, or inclusion boundary",
        );
    }

    // Authenticate a payload with trailing bytes: envelope verification alone
    // must not turn a non-exact bincode decode into a valid base.
    let mut trailing = encode(&original);
    trailing.truncate(trailing.len() - 32);
    trailing.push(0);
    let length_offset = VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1;
    let payload_length = trailing.len() - length_offset - 8;
    trailing[length_offset..length_offset + 8]
        .copy_from_slice(&(payload_length as u64).to_be_bytes());
    let checksum = *blake3::hash(&trailing).as_bytes();
    trailing.extend_from_slice(&checksum);
    assert_corruption(&trailing, "exact decode left trailing bytes");
}

#[test]
fn prior_base_descriptor_size_is_checked_before_owned_graph_conversion() {
    let (_directory, persistence) = writer();
    let original = base(1024 * 1024 + 13);
    let key = RedbPersistence::vector_partition_base_generation_key(&original);
    let encoded = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::BaseGeneration,
        &original,
    )
    .unwrap();
    put(&persistence, &key, &encoded);
    for expected in [17, original.graph.bytes.len() as u64 + 1] {
        let error = persistence
            .load_vector_partition_base_generation_with_expected_graph_bytes(
                &original.index,
                &original.partition_key,
                original.generation_id,
                expected,
            )
            .unwrap_err();
        assert!(
            matches!(&error, Error::StoreCorrupted { reason, .. }
                if reason.contains("disagrees with catalog durable byte declaration")),
            "{error:?}"
        );
    }
    let loaded = persistence
        .load_vector_partition_base_generation_with_expected_graph_bytes(
            &original.index,
            &original.partition_key,
            original.generation_id,
            original.graph.bytes.len() as u64,
        )
        .unwrap();
    assert_eq!(loaded, original);
    assert_eq!(loaded.graph.bytes.capacity(), loaded.graph.bytes.len());
}

#[test]
fn prior_base_expected_route_is_verified_before_owned_route_decode() {
    let (_directory, persistence) = writer();
    let expected = base(17);
    let key = RedbPersistence::vector_partition_base_generation_key(&expected);
    let mut wrong_index = expected.clone();
    wrong_index.index.table = "wrong".repeat(256 * 1024);
    let mut wrong_partition = expected.clone();
    wrong_partition.partition_key =
        VectorPartitionKey::from_components(vec![contextdb_core::VectorPartitionComponent::Text(
            "wrong".repeat(256 * 1024),
        )]);
    for mut corrupt in [wrong_index, wrong_partition] {
        // A full decode would reach the existing identity validation;
        // this distinct reason proves the route preflight ran first.
        corrupt.graph.checksum[0] ^= 1;
        let encoded = RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            &corrupt,
        )
        .unwrap();
        put(&persistence, &key, &encoded);
        let error = persistence
            .load_vector_partition_base_generation_with_expected_graph_bytes(
                &expected.index,
                &expected.partition_key,
                expected.generation_id,
                expected.graph.bytes.len() as u64,
            )
            .unwrap_err();
        assert!(
            matches!(&error, Error::StoreCorrupted { reason, .. }
            if reason.contains("route prefix does not match")),
            "{error:?}"
        );
    }
}

fn raw_entry(dimension: usize, quantization: VectorQuantization) -> PersistedVectorEntry {
    PersistedVectorEntry {
        // UTF-8 lengths are bytes, as in the serialized route strings.
        index: VectorIndexRef::new("資料".repeat(43), "embedding".repeat(31)),
        row_id: RowId(u64::MAX),
        vector: PersistedVector::from_f32(&vec![0.0; dimension], quantization),
        created_tx: TxId(u64::MAX),
        deleted_tx: Some(TxId(u64::MAX)),
        lsn: Lsn(u64::MAX),
    }
}

#[test]
fn raw_vector_encoded_width_accepts_all_quantizations_and_extreme_scalar_framing() {
    let (_directory, persistence) = writer();
    for dimension in [0, 1, 2, 251, 768, 65_536] {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let mut entry = raw_entry(dimension, quantization);
            if let PersistedVector::SQ8 { min, max, .. } | PersistedVector::SQ4 { min, max, .. } =
                &mut entry.vector
            {
                *min = f32::MIN;
                *max = f32::MAX;
            }
            let encoded = RedbPersistence::encode(&entry).unwrap();
            persistence
                .validate_raw_vector_entry_encoded_width(&encoded, &entry.index, dimension)
                .unwrap();
            let decoded = RedbPersistence::decode_vector_entry(&encoded).unwrap();
            assert_eq!(decoded.vector.len(), dimension);
            assert_eq!(decoded.index, entry.index);
            assert_eq!(decoded.row_id, entry.row_id);
            assert_eq!(decoded.created_tx, entry.created_tx);
            assert_eq!(decoded.deleted_tx, entry.deleted_tx);
            assert_eq!(decoded.lsn, entry.lsn);
            let loaded = write_and_load_raw_entry(
                &persistence,
                &encoded,
                &raw_membership(&entry),
                dimension,
            )
            .unwrap();
            assert_eq!(loaded.len(), 1);
            assert_eq!(loaded[0].vector.len(), dimension);
            assert_eq!(loaded[0].index, entry.index);
        }
    }
}

fn raw_membership(entry: &PersistedVectorEntry) -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index: entry.index.clone(),
        partition_key: VectorPartitionKey::unpartitioned(),
        row_id: entry.row_id,
        row_created_tx: entry.created_tx,
        row_lsn: entry.lsn,
        vector_created_tx: entry.created_tx,
        vector_lsn: entry.lsn,
        visible_from: entry.created_tx,
        deleted_tx: entry.deleted_tx,
    }
}

// Install the declaration and authenticated placement the current loader captures.
// The raw write below remains intentional damage injection, after that foundation.
fn declare_raw_fixture(
    persistence: &RedbPersistence,
    membership: &VectorPartitionMembershipRecord,
    dimension: usize,
    quantization: VectorQuantization,
) -> Result<()> {
    let fixture = crate::Database::open_memory();
    fixture.execute(
        "CREATE TABLE fixture (id INTEGER PRIMARY KEY, embedding VECTOR(1))",
        &HashMap::new(),
    )?;
    let mut meta = fixture.table_meta("fixture").unwrap();
    let column = meta
        .columns
        .iter_mut()
        .find(|column| column.name == "embedding")
        .unwrap();
    column.name = membership.index.column.clone();
    column.column_type = ColumnType::Vector(dimension);
    column.quantization = quantization;
    persistence.flush_table_meta(&membership.index.table, &meta)?;
    persistence.write_corrupt_store_vector_memberships_for_test(std::slice::from_ref(membership))
}

fn write_and_load_raw_entry(
    persistence: &RedbPersistence,
    encoded: &[u8],
    membership: &VectorPartitionMembershipRecord,
    dimension: usize,
) -> Result<Vec<VectorEntry>> {
    let persisted: PersistedVectorEntry = RedbPersistence::decode(encoded)?;
    let quantization = match &persisted.vector {
        PersistedVector::F32(_) => VectorQuantization::F32,
        PersistedVector::SQ8 { .. } => VectorQuantization::SQ8,
        PersistedVector::SQ4 { .. } => VectorQuantization::SQ4,
    };
    declare_raw_fixture(persistence, membership, dimension, quantization)?;
    let key = RedbPersistence::vector_identity_key(
        &membership.index,
        membership.row_id,
        membership.vector_created_tx,
        membership.vector_lsn,
    );
    persistence.with_db(|db| {
        let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
        {
            let mut table = write
                .open_table(VECTORS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            table
                .insert(key.as_slice(), encoded)
                .map_err(RedbPersistence::storage_error)?;
        }
        let entry = VectorEntry {
            index: persisted.index.clone(),
            row_id: persisted.row_id,
            vector: persisted.vector.to_f32(),
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
        };
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &[content_identity::decoded_vector_version_mutation(
                &entry,
                quantization,
            )],
            &mut persistence.proof_admission(),
        )?;
        write.commit().map_err(RedbPersistence::storage_error)?;
        let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
        persistence.load_vector_entries_for_memberships_in_read(
            &read,
            std::slice::from_ref(membership),
            dimension,
            Some(quantization),
        )
    })
}

#[test]
fn oversized_raw_dimension_or_route_is_refused_before_body_decode() {
    let (_directory, persistence) = writer();
    let expected = raw_entry(1, VectorQuantization::F32);
    let membership = raw_membership(&expected);
    declare_raw_fixture(&persistence, &membership, 1, VectorQuantization::F32).unwrap();
    let key = RedbPersistence::vector_identity_key(
        &membership.index,
        membership.row_id,
        membership.vector_created_tx,
        membership.vector_lsn,
    );
    let mut oversized_dimension = raw_entry(4096, VectorQuantization::F32);
    oversized_dimension.index = expected.index.clone();
    let mut oversized_route = expected.clone();
    oversized_route.index.table = "wrong".repeat(4096);
    for corrupt in [oversized_dimension, oversized_route] {
        let encoded = RedbPersistence::encode(&corrupt).unwrap();
        persistence.with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write.open_table(VECTORS_TABLE).map_err(RedbPersistence::storage_error)?;
                table.insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let error = persistence.load_vector_entries_for_memberships_in_read(
                &read,
                std::slice::from_ref(&membership),
                1,
                Some(VectorQuantization::F32),
            ).unwrap_err();
            // A successful body decode would instead reach the existing
            // dimension/identity checks. This reason is the predecode guard.
            assert!(matches!(&error, Error::StoreCorrupted { reason, .. }
                if reason.contains("encoded body width") && reason.contains("declared route/dimension bound")),
                "{error:?}");
            Ok(())
        }).unwrap();
    }
    let error = persistence
        .validate_raw_vector_entry_encoded_width(&[], &expected.index, usize::MAX)
        .unwrap_err();
    assert!(matches!(&error, Error::StoreCorrupted { reason, .. }
        if reason.contains("width bound overflow")));
}

#[test]
fn corrupt_sq4_expansion_is_refused_before_dequantization_allocation() {
    let (_directory, persistence) = writer();
    let mut corrupt = raw_entry(1, VectorQuantization::SQ4);
    let membership = raw_membership(&corrupt);
    declare_raw_fixture(&persistence, &membership, 1, VectorQuantization::SQ4).unwrap();
    corrupt.vector = PersistedVector::SQ4 {
        min: 0.0,
        max: 1.0,
        len: u32::MAX,
        payload: vec![0x12; 4],
    };
    let encoded = RedbPersistence::encode(&corrupt).unwrap();
    // Four coded bytes fit the conservative F32 encoded-width bound for a
    // one-dimensional declaration, but would produce eight F32 components.
    persistence
        .validate_raw_vector_entry_encoded_width(&encoded, &corrupt.index, 1)
        .unwrap();
    let calibration = crate::read_image_memory::ImageMemory::new(None);
    calibration
        .scope(|| RedbPersistence::decode::<PersistedVectorEntry>(&encoded))
        .unwrap();
    let decode_charge = calibration.held();
    // Admit exactly the owned coded record, with no credit for a subsequent
    // F32 clone/dequantization. Reaching the old conversion path would refuse
    // its reserve instead of returning the predicted dimension corruption.
    // The guarded decoder admits its fixed control transient before parsing.
    // That transient is refunded; the body still owns exactly decode_charge.
    let decode_control = std::mem::size_of::<PersistedVectorEntry>() * 4 + 128;
    let memory = crate::read_image_memory::ImageMemory::new(Some(decode_charge + decode_control));
    let key = RedbPersistence::vector_identity_key(
        &membership.index,
        membership.row_id,
        membership.vector_created_tx,
        membership.vector_lsn,
    );
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTORS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)?;
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let error = memory
                .scope(|| {
                    persistence.load_vector_entries_for_memberships_in_read(
                        &read,
                        std::slice::from_ref(&membership),
                        1,
                        Some(VectorQuantization::SQ4),
                    )
                })
                .unwrap_err();
            assert!(
                matches!(&error, Error::StoreCorrupted { reason, .. }
                if reason.contains("body dimension 8 does not match declared dimension 1")),
                "{error:?}"
            );
            assert_eq!(memory.held(), decode_charge);
            Ok(())
        })
        .unwrap();
}
