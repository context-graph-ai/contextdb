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
                subsystem: subsystem.to_owned().into(),
                operation: operation.to_owned().into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: hint.to_owned().into(),
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
        let affected_tables = HashSet::from([index.table.clone()]);
        let mut admission = VectorCopyAdmission::new(budget.clone());
        let prepared = store
            .prepare_received_schema_tables_publication(
                publication,
                &affected_tables,
                &mut admission,
            )
            .unwrap();
        store.publish_prepared_received_schema_tables(prepared, budget.clone());
    }
}

#[test]
fn received_publication_prepares_next_insert_and_releases_its_last_holder() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());

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
                    + receipt.vector_partition_controls
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
    // Insert bodies are caller-admitted; publication consumes their charge once.
    let first = entry(&legacy, RowId(1), TxId(1));
    let body_bytes = first.vector.capacity() * std::mem::size_of::<f32>();
    assert_eq!(body_bytes, 3072);
    let directory_before_body = legacy_column_owner.used.load(Ordering::SeqCst);
    let mut body_admission = VectorCopyAdmission::new(legacy_column_owner.clone());
    body_admission
        .reserve_for(body_bytes, "test", "insert_body", "test")
        .unwrap();
    assert_eq!(
        legacy_column_owner.used.load(Ordering::SeqCst),
        directory_before_body + body_bytes
    );
    store
        .apply_inserts(
            vec![first],
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
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
    let second = entry(&legacy, RowId(2), TxId(2));
    assert_eq!(
        second.vector.capacity() * std::mem::size_of::<f32>(),
        body_bytes
    );
    let legacy_state_before = store.state(&legacy).unwrap();
    let prior_raw_owner = legacy_state_before.raw_accountant.read().clone();
    let prior_raw_bytes = legacy_state_before.raw_payload_bytes();
    let directory_before = legacy_column_owner.used.load(Ordering::SeqCst);
    legacy_column_owner.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        body_admission.reserve_for(body_bytes, "test", "insert_body", "test"),
        Err(Error::MemoryBudgetExceeded { requested_bytes, available_bytes: 0, ref operation, .. })
            if requested_bytes == body_bytes && operation == "insert_body"
    ));
    assert_eq!(
        legacy_column_owner.used.load(Ordering::SeqCst),
        directory_before
    );
    assert_eq!(body_admission.retained_bytes(), body_bytes);
    assert_eq!(legacy_state_before.entry_count(), 1);
    let after_refusal_owner = legacy_state_before.raw_accountant.read();
    assert_eq!(after_refusal_owner.is_some(), prior_raw_owner.is_some());
    if let Some(prior) = &prior_raw_owner {
        assert!(Arc::ptr_eq(after_refusal_owner.as_ref().unwrap(), prior));
    }
    drop(after_refusal_owner);
    assert_eq!(legacy_state_before.raw_payload_bytes(), prior_raw_bytes);
    assert!(legacy_state_before.stored_by_row_id(RowId(2)).is_none());
    let declared: Arc<dyn MemoryBudget> = legacy_column_owner.clone();
    assert!(Arc::ptr_eq(
        &store
            .try_column(&legacy)
            .unwrap()
            .directory_accounting
            .lock()
            .accountant,
        &declared
    ));
    legacy_column_owner.refuse.store(false, Ordering::SeqCst);
    body_admission
        .reserve_for(body_bytes, "test", "insert_body", "test")
        .unwrap();
    assert_eq!(
        legacy_column_owner.used.load(Ordering::SeqCst),
        directory_before + body_bytes
    );
    assert_eq!(body_admission.retained_bytes(), 2 * body_bytes);
    drop(legacy_state_before);
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
                TxId(1),
                Lsn(1),
            )
        })
        .collect();
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                second,
            )],
            movements,
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    let legacy_prepared_bytes = legacy_column_owner.used.load(Ordering::SeqCst);
    store.publish_prepared_partitioned_batch(prepared, Lsn(2), None);
    // Raw payload tokens now own both body charges; disarm the caller token.
    body_admission.transfer();
    assert!(legacy_column_owner.used.load(Ordering::SeqCst) <= legacy_prepared_bytes);
    let legacy_column = store.try_column(&legacy).unwrap();
    let declared: Arc<dyn MemoryBudget> = legacy_column_owner.clone();
    assert!(Arc::ptr_eq(
        &legacy_column.directory_accounting.lock().accountant,
        &declared
    ));
    let legacy_state = store.state(&legacy).unwrap();
    assert!(legacy_state.raw_residency_is_accounted());
    assert!(Arc::ptr_eq(
        legacy_state.raw_accountant.read().as_ref().unwrap(),
        &declared
    ));
    assert_eq!(legacy_state.entry_count(), 2);
    assert_eq!(legacy_state.raw_payload_bytes(), 2 * body_bytes);
    assert!(
        legacy_state
            .with_stored_by_row_id(RowId(2), |entry| entry.vector.payload_identity().is_some()
                && entry.vector.payload_is_charged())
            .unwrap()
    );
    let legacy_held = legacy_state.stored_by_row_id(RowId(2)).unwrap();
    let legacy_identity = legacy_held.vector.payload_identity().unwrap();
    assert_eq!(legacy_held.vector.to_f32(), vec![1.0; 768]);
    assert_eq!(legacy_held.estimated_bytes(), body_bytes);
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
    let held_bytes = body_bytes + crate::quantized::shared_payload_header_bytes();
    assert_eq!(legacy_column_owner.used.load(Ordering::SeqCst), held_bytes);
    assert_eq!(legacy_held.vector.payload_identity(), Some(legacy_identity));
    assert_eq!(legacy_held.vector.to_f32(), vec![1.0; 768]);
    drop(legacy_held);
    assert_eq!(legacy_column_owner.used.load(Ordering::SeqCst), 0);
    for budget in &budgets {
        assert_eq!(budget.used.load(Ordering::SeqCst), held_bytes);
    }
    drop(holders);
    for budget in &budgets {
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}
// A shared canonical body keeps both disjoint row placements across export and
// restore. Received images preserve the same provenance and refuse overlap.
#[test]
fn disjoint_body_placements_survive_resident_and_received_export_restore() {
    let index = VectorIndexRef::new("placements", "embedding");
    let layout = VectorIndexLayout::new(
        768,
        VectorQuantization::F32,
        vec!["scope".into()],
        Some(2),
        VectorSearchMode::Exact,
    );
    let placements = || {
        let mut first = entry(&index, RowId(1), TxId(2));
        first.deleted_tx = Some(TxId(7));
        vec![
            PartitionedVectorEntry::with_membership(
                VectorPartitionKey::from_values(&[contextdb_core::Value::Text("a".into())])
                    .unwrap(),
                first,
                VectorMembershipProvenance::new(TxId(3), Lsn(3), TxId(4), Some(TxId(7))),
                None,
            ),
            PartitionedVectorEntry::with_membership(
                VectorPartitionKey::from_values(&[contextdb_core::Value::Text("b".into())])
                    .unwrap(),
                entry(&index, RowId(1), TxId(2)),
                VectorMembershipProvenance::new(TxId(8), Lsn(8), TxId(8), None),
                None,
            ),
        ]
    };
    let assert_image = |image: &[PartitionedVectorEntry]| {
        let expected = placements();
        assert_eq!(image.len(), 2);
        for wanted in &expected {
            let found = image
                .iter()
                .find(|item| item.partition_key == wanted.partition_key)
                .unwrap();
            assert_eq!(found.membership, wanted.membership);
            assert_eq!(found.entry.index, index);
            assert_eq!(found.entry.row_id, RowId(1));
            assert_eq!(found.entry.created_tx, TxId(2));
            assert_eq!(found.entry.lsn, Lsn(2));
            assert_eq!(found.entry.vector, wanted.entry.vector);
            assert_eq!(found.entry.deleted_tx, wanted.entry.deleted_tx);
        }
    };
    for received in [false, true] {
        let owner = Arc::new(Budget::default());
        let store = VectorStore::default();
        let mut registration = VectorCopyAdmission::new(owner.clone());
        store
            .register_index_with_layout(
                index.clone(),
                layout.clone(),
                owner.clone(),
                &mut registration,
            )
            .unwrap();
        drop(registration);
        if received {
            let prepared = store
                .prepare_partitioned_received_schema_publication(
                    vec![(index.clone(), layout.clone())],
                    placements(),
                    owner.clone(),
                    VectorCopyAdmission::new(owner.clone()),
                )
                .unwrap();
            store.publish_prepared_received_schema(prepared, owner.clone());
        } else {
            store
                .replace_loaded_partitioned_vectors(
                    placements(),
                    VectorCopyAdmission::new(owner.clone()),
                )
                .unwrap();
        }
        let exported = store
            .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
                owner.clone(),
            ))
            .unwrap();
        assert_image(exported.entries());
        let (entries, export_owner) = exported.into_parts();
        let restored_owner = Arc::new(Budget::default());
        let restored = VectorStore::default();
        let mut registration = VectorCopyAdmission::new(restored_owner.clone());
        restored
            .register_index_with_layout(
                index.clone(),
                layout.clone(),
                restored_owner.clone(),
                &mut registration,
            )
            .unwrap();
        drop(registration);
        restored
            .replace_loaded_partitioned_vectors(entries, export_owner)
            .unwrap();
        let image = restored
            .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
                restored_owner.clone(),
            ))
            .unwrap();
        assert_image(image.entries());
        drop(image);
        let mut overlapping = placements();
        overlapping[1].membership.visible_from = TxId(6);
        let rejection = restored.prepare_partitioned_received_schema_publication(
            vec![(index.clone(), layout.clone())],
            overlapping,
            restored_owner.clone(),
            VectorCopyAdmission::new(restored_owner.clone()),
        );
        assert!(
            matches!(rejection, Err(Error::Other(ref message)) if message == "canonical vector body has overlapping source memberships")
        );
        let image = restored
            .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
                restored_owner.clone(),
            ))
            .unwrap();
        assert_image(image.entries());
        drop(image);
        drop(restored);
        drop(store);
        assert_eq!(owner.used.load(Ordering::SeqCst), 0);
        assert_eq!(restored_owner.used.load(Ordering::SeqCst), 0);
    }
}
