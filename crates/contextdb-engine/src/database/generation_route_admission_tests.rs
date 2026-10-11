use super::*;

#[test]
fn generation_route_metadata_refuses_zero_headroom_before_record_probe() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("generation-route.db");
    let database = Database::open(&path).unwrap();
    database.close().unwrap();
    let persistence = Arc::new(RedbPersistence::open(&path).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(4096));
    accountant.try_allocate(4096).unwrap();
    let descriptor = VectorPartitionGenerationDescriptor {
        generation_id: 7,
        covered_tx: TxId(1),
        covered_lsn: Lsn(1),
        resident_bytes: 1,
        durable_bytes: 1,
        policy_revision: 0,
        hnsw_m: 24,
        hnsw_ef_construction: 64,
    };
    let layout = VectorIndexLayout::unpartitioned(1, VectorQuantization::F32);
    let loader = DurableVectorGenerationLoader {
        source: DurableVectorGenerationSource::Writable(persistence),
        accountant: Arc::clone(&accountant).into(),
        partition: VectorPartitionRef::new(
            VectorIndexRef::new("missing", "v"),
            VectorPartitionKey::from_components(
                vec![contextdb_core::VectorPartitionComponent::Boolean(true); 4096],
            ),
        ),
        descriptor,
        base_generation_id: 7,
        is_base: true,
        base_covered: (TxId(1), Lsn(1)),
        dimension: 1,
        quantization: VectorQuantization::F32,
        layout: layout.clone(),
        live_rows: 1,
        declared_serving_cache: DeclaredServingCache::AsOfOpen(None),
        tail_covered_lsn: None,
    };
    let expected = loader.route_metadata_workspace().unwrap();
    assert!(expected > 4096 * std::mem::size_of::<contextdb_core::VectorPartitionComponent>() * 2);
    let generation = DormantVectorGraphGeneration {
        identity: durable_vector_generation_identity(descriptor, &layout, 1).unwrap(),
        resident_bytes: 1,
        load_bytes: 2,
        reclaim_bytes: 2,
    };
    let error = match loader.load(generation, 0, &mut |_| true) {
        Ok(_) => panic!("zero headroom loaded"),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { subsystem, operation, requested_bytes, available_bytes: 0, budget_limit_bytes: 4096, .. }
        if subsystem == "vector_index" && operation == "load_durable_vector_generation" && requested_bytes == expected)
    );
    assert_eq!(accountant.usage().used, 4096);
    accountant.release(4096);
}
