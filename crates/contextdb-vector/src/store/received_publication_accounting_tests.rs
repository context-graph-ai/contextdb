use super::*;

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
    refuse: AtomicBool,
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: hint.into(),
            });
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.used.fetch_sub(bytes, Ordering::SeqCst);
        assert!(
            previous >= bytes,
            "each received publication charge releases once"
        );
    }
}

fn entry(index: &VectorIndexRef, row: RowId, tx: TxId) -> VectorEntry {
    VectorEntry {
        index: index.clone(),
        row_id: row,
        vector: vec![1.0; 768],
        created_tx: tx,
        deleted_tx: None,
        lsn: Lsn(tx.0),
    }
}

fn install_received(
    store: &VectorStore,
    index: &VectorIndexRef,
    layout: VectorIndexLayout,
    key: VectorPartitionKey,
    budget: &Arc<Budget>,
    whole_registry: bool,
) {
    let publication = store
        .prepare_partitioned_received_schema_publication(
            vec![(index.clone(), layout)],
            vec![PartitionedVectorEntry::new(
                key,
                entry(index, RowId(1), TxId(1)),
            )],
            budget.clone(),
            VectorCopyAdmission::new(budget.clone()),
        )
        .unwrap();
    if whole_registry {
        store.publish_prepared_received_schema(publication, budget.clone());
    } else {
        store.publish_prepared_received_schema_tables(
            publication,
            &HashSet::from([index.table.clone()]),
            budget.clone(),
        );
    }
}

#[test]
fn received_publication_prepares_next_insert_and_releases_its_last_holder() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for whole_registry in [false, true] {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let budget = Arc::new(Budget::default());
            let store = VectorStore::default();
            let index = VectorIndexRef::new("received", "embedding");
            let key = VectorPartitionKey::unpartitioned();
            install_received(
                &store,
                &index,
                VectorIndexLayout::unpartitioned(768, quantization),
                key.clone(),
                &budget,
                whole_registry,
            );
            assert!(store.memory_accountant.read().is_none());
            let state = store.state(&index).unwrap();
            assert!(state.raw_residency_is_accounted());
            let before = budget.used.load(Ordering::SeqCst);
            let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
            budget
                .try_allocate_for(body_bytes, "test", "insert_body", "test")
                .unwrap();
            let inserts = vec![PartitionedVectorEntry::new(
                key,
                entry(&index, RowId(2), TxId(2)),
            )];
            let prepared = store
                .prepare_partitioned_batch(
                    vec![],
                    inserts.clone(),
                    vec![],
                    HashSet::new(),
                    VectorCopyAdmission::new(executing_accountant.clone()),
                )
                .unwrap();
            assert!(budget.used.load(Ordering::SeqCst) > before + body_bytes);
            drop(prepared);
            assert_eq!(budget.used.load(Ordering::SeqCst), before + body_bytes);
            assert_eq!(state.entry_count(), 1);

            budget.refuse.store(true, Ordering::SeqCst);
            assert!(matches!(
                store.prepare_partitioned_batch(vec![], inserts.clone(), vec![], HashSet::new(), VectorCopyAdmission::new(executing_accountant.clone())),
                Err(Error::MemoryBudgetExceeded { ref operation, .. }) if operation == "prepare_vector_directory_growth"
            ));
            assert_eq!(budget.used.load(Ordering::SeqCst), before + body_bytes);
            assert_eq!(state.entry_count(), 1);
            budget.refuse.store(false, Ordering::SeqCst);
            let prepared = store
                .prepare_partitioned_batch(
                    vec![],
                    inserts,
                    vec![],
                    HashSet::new(),
                    VectorCopyAdmission::new(executing_accountant.clone()),
                )
                .unwrap();
            store.publish_prepared_partitioned_batch(prepared, Lsn(2), Some(budget.as_ref()));
            assert_eq!(state.entry_count(), 2);
            let header_bytes = crate::quantized::shared_payload_header_bytes();
            assert_eq!(
                store.raw_payload_header_bytes.load(Ordering::SeqCst),
                header_bytes
            );
            let receipt = store.memory_ownership_snapshot_for_test();
            assert_eq!(
                budget.used.load(Ordering::SeqCst),
                receipt.raw_vector_bodies
                    + receipt.raw_reverse_directory
                    + receipt.partition_vectors
                    + receipt.typed_partition_keys
            );
            let held = state.stored_by_row_id(RowId(2)).unwrap();
            assert!(held.vector.payload_identity().is_some());
            assert!(held.vector.payload_is_charged());
            drop(state);
            drop(store);
            assert_eq!(
                budget.used.load(Ordering::SeqCst),
                body_bytes + header_bytes
            );
            drop(held);
            assert_eq!(budget.used.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn received_columns_keep_distinct_funders_and_legacy_columns_in_a_mixed_batch() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let store = VectorStore::default();
    let legacy = VectorIndexRef::new("legacy", "embedding");
    let legacy_column_owner = Arc::new(Budget::default());
    store
        .register_index(
            legacy.clone(),
            768,
            VectorQuantization::F32,
            legacy_column_owner.clone(),
        )
        .unwrap();
    store.apply_inserts(vec![entry(&legacy, RowId(1), TxId(1))]);
    let source =
        VectorPartitionKey::from_values(&[contextdb_core::Value::Text("a".into())]).unwrap();
    let target =
        VectorPartitionKey::from_values(&[contextdb_core::Value::Text("b".into())]).unwrap();
    let budgets = [Arc::new(Budget::default()), Arc::new(Budget::default())];
    let indexes = [
        VectorIndexRef::new("received_a", "embedding"),
        VectorIndexRef::new("received_b", "embedding"),
    ];
    for (index, budget) in indexes.iter().zip(&budgets) {
        install_received(
            &store,
            index,
            VectorIndexLayout::new(
                768,
                VectorQuantization::F32,
                vec!["scope".into()],
                Some(2),
                VectorSearchMode::Exact,
            ),
            source.clone(),
            budget,
            false,
        );
    }
    let movements = indexes
        .iter()
        .map(|index| {
            PartitionedVectorMove::new(
                index.clone(),
                source.clone(),
                target.clone(),
                RowId(1),
                RowId(2),
                TxId(2),
            )
        })
        .collect();
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                entry(&legacy, RowId(2), TxId(2)),
            )],
            movements,
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(2), None);
    let legacy_column = store.try_column(&legacy).unwrap();
    let declared: Arc<dyn MemoryBudget> = legacy_column_owner.clone();
    assert!(Arc::ptr_eq(
        &legacy_column.directory_accounting.lock().accountant,
        &declared
    ));
    let legacy_state = store.state(&legacy).unwrap();
    assert!(!legacy_state.raw_residency_is_accounted());
    assert_eq!(legacy_state.entry_count(), 2);
    assert!(
        legacy_state
            .with_stored_by_row_id(RowId(2), |entry| entry.vector.payload_identity().is_none()
                && !entry.vector.payload_is_charged())
            .unwrap()
    );
    let holders = indexes
        .iter()
        .map(|index| {
            let state = store.partition_state(index, &target).unwrap();
            assert!(state.raw_residency_is_accounted());
            assert_eq!(state.entry_count(), 1);
            let held = state.stored_by_row_id(RowId(2)).unwrap();
            assert!(held.vector.payload_is_charged());
            assert!(held.vector.payload_identity().is_some());
            held
        })
        .collect::<Vec<_>>();
    drop(legacy_column);
    drop(legacy_state);
    drop(store);
    let held_bytes = 768 * 4 + crate::quantized::shared_payload_header_bytes();
    for budget in &budgets {
        assert_eq!(budget.used.load(Ordering::SeqCst), held_bytes);
    }
    drop(holders);
    for budget in &budgets {
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}
