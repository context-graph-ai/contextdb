use super::*;

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
    refuse: AtomicBool,
    refuse_headers: AtomicBool,
    refuse_workspace: AtomicBool,
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if self.refuse.load(Ordering::SeqCst)
            || (operation == "materialize" && self.refuse_workspace.load(Ordering::SeqCst))
            || (operation == "load_raw_vector_headers"
                && self.refuse_headers.load(Ordering::SeqCst))
        {
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
        assert!(previous >= bytes, "each payload reservation releases once");
    }
}

fn entry(index: &VectorIndexRef, row: u64, tx: TxId, axis: usize) -> VectorEntry {
    let mut vector = vec![0.0; 768];
    vector[axis] = 1.0;
    VectorEntry {
        index: index.clone(),
        row_id: RowId(row),
        vector,
        created_tx: tx,
        deleted_tx: None,
        lsn: Lsn(tx.0),
    }
}

#[test]
fn retained_scalar_memberships_share_one_body_and_keep_all_snapshot_intervals() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("passages", "embedding");
        let key = VectorPartitionKey::unpartitioned();
        store
            .register_index(index.clone(), 768, quantization, budget.clone())
            .unwrap();
        super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
        store.account_loaded_state(budget.clone()).unwrap();
        let body_bytes = store.memory_ownership_snapshot_for_test().raw_vector_bodies;
        assert_eq!(
            store.raw_payload_header_bytes.load(Ordering::SeqCst),
            crate::quantized::shared_payload_header_bytes()
        );
        for tx in 2..=65 {
            let prepared = store
                .prepare_partitioned_batch(
                    vec![],
                    vec![],
                    vec![
                        PartitionedVectorMove::new(
                            index.clone(),
                            key.clone(),
                            key.clone(),
                            RowId(1),
                            RowId(1),
                            TxId(tx),
                            TxId(tx - 1),
                            Lsn(tx - 1),
                        )
                        .replacing_row_version(),
                    ],
                    HashSet::new(),
                    VectorCopyAdmission::new(executing_accountant.clone()),
                )
                .unwrap();
            store.publish_prepared_partitioned_batch(prepared, Lsn(tx), Some(budget.as_ref()));
            assert_eq!(
                store.raw_payload_header_bytes.load(Ordering::SeqCst),
                crate::quantized::shared_payload_header_bytes()
            );
            assert_eq!(
                store.memory_ownership_snapshot_for_test().raw_vector_bodies,
                body_bytes
            );
        }
        let state = store.state(&index).unwrap();
        assert_eq!(
            state.entry_count(),
            65,
            "every row version retains its vector membership"
        );
        assert_eq!(
            state.vectors.read().payload_memberships.len(),
            1,
            "one physical allocation backs the distinct memberships"
        );
        for tx in 1..=65 {
            let visible = store
                .live_entry_for_row_in_partition(
                    &index,
                    &key,
                    RowId(1),
                    SnapshotId(tx),
                    &mut executing_admission,
                )
                .unwrap()
                .unwrap();
            assert_eq!(visible.created_tx, TxId(tx));
            assert_eq!(visible.vector, entry(&index, 1, TxId(tx), 0).vector);
        }
        let old = state.stored_by_row_id(RowId(1)).unwrap();
        let new_entry = entry(&index, 1, TxId(66), 1);
        budget
            .try_allocate_for(768 * 4, "test", "genuine_change", "test")
            .unwrap();
        let prepared = store
            .prepare_partitioned_batch(
                vec![PartitionedVectorDelete::new(
                    index.clone(),
                    key.clone(),
                    RowId(1),
                    TxId(66),
                    TxId(65),
                    Lsn(65),
                )],
                vec![PartitionedVectorEntry::new(key.clone(), new_entry)],
                vec![],
                HashSet::new(),
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        store.publish_prepared_partitioned_batch(prepared, Lsn(66), Some(budget.as_ref()));
        assert_ne!(
            old.vector.payload_identity(),
            state
                .stored_by_row_id(RowId(1))
                .unwrap()
                .vector
                .payload_identity()
        );
        assert_eq!(
            store
                .live_entry_for_row_in_partition(
                    &index,
                    &key,
                    RowId(1),
                    SnapshotId(65),
                    &mut executing_admission
                )
                .unwrap()
                .unwrap()
                .vector[0],
            1.0
        );
        assert_eq!(
            store
                .live_entry_for_row_in_partition(
                    &index,
                    &key,
                    RowId(1),
                    SnapshotId(66),
                    &mut executing_admission
                )
                .unwrap()
                .unwrap()
                .vector[1],
            1.0
        );
        // Genuine-change admission follows the declared stored representation.
        if quantization != VectorQuantization::F32 {
            budget.release(
                768 * 4 - crate::quantized::stored_vector_resident_bytes(768, quantization),
            );
        }
        drop(old);
        drop(state);
        drop(store);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn shared_cross_partition_body_survives_source_removal_and_final_external_holder() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passages", "embedding");
    let source =
        VectorPartitionKey::from_values(&[contextdb_core::Value::Text("a".into())]).unwrap();
    let target =
        VectorPartitionKey::from_values(&[contextdb_core::Value::Text("b".into())]).unwrap();
    store
        .register_index_with_layout(
            index.clone(),
            VectorIndexLayout::new(
                768,
                VectorQuantization::F32,
                vec!["scope".into()],
                Some(2),
                VectorSearchMode::Exact,
            ),
            budget.clone(),
            &mut executing_admission,
        )
        .unwrap();
    budget
        .try_allocate_for(
            crate::quantized::stored_vector_resident_bytes(768, VectorQuantization::F32),
            "test",
            "initial_body",
            "test",
        )
        .unwrap();
    store
        .apply_partitioned_inserts(
            vec![PartitionedVectorEntry::new(
                source.clone(),
                entry(&index, 1, TxId(1), 0),
            )],
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = store.memory_ownership_snapshot_for_test().raw_vector_bodies;
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![PartitionedVectorMove::new(
                index.clone(),
                source.clone(),
                target.clone(),
                RowId(1),
                RowId(2),
                TxId(2),
                TxId(1),
                Lsn(1),
            )],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(2), Some(budget.as_ref()));
    assert_eq!(
        store.memory_ownership_snapshot_for_test().raw_vector_bodies,
        body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        crate::quantized::shared_payload_header_bytes()
    );
    let target_state = store.partition_state(&index, &target).unwrap();
    let held = target_state.stored_by_row_id(RowId(2)).unwrap();
    let source_state = store.partition_state(&index, &source).unwrap();
    assert_eq!(
        held.vector.payload_identity(),
        source_state
            .stored_by_row_id(RowId(1))
            .unwrap()
            .vector
            .payload_identity()
    );
    drop(source_state);
    let released = store
        .prune_row_ids(
            &HashSet::from([RowId(1)]),
            budget.as_ref(),
            &mut executing_admission,
        )
        .unwrap();
    budget.release(released);
    assert_eq!(
        store.memory_ownership_snapshot_for_test().raw_vector_bodies,
        body_bytes
    );
    assert_eq!(held.vector.to_f32()[0], 1.0);
    drop(target_state);
    drop(store);
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        body_bytes + crate::quantized::shared_payload_header_bytes(),
        "a clone outlives the registry without returning its body or header charge"
    );
    drop(held);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn shared_payload_preparation_rollback_and_refusal_preserve_membership_and_charge() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passages", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
    store.account_loaded_state(budget.clone()).unwrap();
    let before = budget.used.load(Ordering::SeqCst);
    let movement = PartitionedVectorMove::new(
        index.clone(),
        key.clone(),
        key,
        RowId(1),
        RowId(1),
        TxId(2),
        TxId(1),
        Lsn(1),
    )
    .replacing_row_version();
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![movement.clone()],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    drop(prepared);
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    budget.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.prepare_partitioned_batch(
            vec![],
            vec![],
            vec![movement],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone())
        ),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    assert_eq!(store.state(&index).unwrap().entry_count(), 1);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

struct Reload {
    entries: Vec<VectorEntry>,
    budget: Arc<Budget>,
    quantization: VectorQuantization,
}

impl DormantRawVectorLoader for Reload {
    fn load_candidate(
        &self,
        _identity: &RawVectorDirectoryEntry,
        _caller_charged: bool,
        _request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        Err(Error::Other(
            "the reload fixture exercises complete partition hydration".into(),
        ))
    }
    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        load_co_created_fixture_carriers(
            &self.entries,
            Some(directory),
            self.budget.clone(),
            self.quantization,
            owner,
        )
    }
}

#[test]
fn eviction_and_reload_share_unchanged_history_and_keep_prior_holder_charged() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passages", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
    store.account_loaded_state(budget.clone()).unwrap();
    for tx in 2..=8 {
        let prepared = store
            .prepare_partitioned_batch(
                vec![],
                vec![],
                vec![
                    PartitionedVectorMove::new(
                        index.clone(),
                        key.clone(),
                        key.clone(),
                        RowId(1),
                        RowId(1),
                        TxId(tx),
                        TxId(tx - 1),
                        Lsn(tx - 1),
                    )
                    .replacing_row_version(),
                ],
                HashSet::new(),
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        store.publish_prepared_partitioned_batch(prepared, Lsn(tx), Some(budget.as_ref()));
    }
    let state = store.state(&index).unwrap();
    let held = state.stored_by_row_id(RowId(1)).unwrap();
    let entries = state.all_entries(&index, &mut executing_admission).unwrap();
    let directory = state.raw_directory.read().clone();
    state
        .attach_durable_raw_loader(
            directory,
            Arc::new(Reload {
                entries,
                budget: budget.clone(),
                quantization: VectorQuantization::F32,
            }),
        )
        .unwrap();
    assert!(state.evict_raw_vectors());
    assert_eq!(
        store.memory_ownership_snapshot_for_test().raw_vector_bodies,
        768 * 4,
        "an external holder keeps the evicted allocation charged"
    );
    state
        .ensure_raw_vectors_loaded(&mut executing_admission)
        .unwrap();
    assert_eq!(state.entry_count(), 8);
    assert_eq!(state.vectors.read().payload_memberships.len(), 1);
    assert_eq!(
        state.raw_payload_bytes(),
        768 * 4,
        "reload deduplicates identical historical payloads"
    );
    assert_eq!(
        store.memory_ownership_snapshot_for_test().raw_vector_bodies,
        2 * 768 * 4
    );
    drop(held);
    assert_eq!(
        store.memory_ownership_snapshot_for_test().raw_vector_bodies,
        768 * 4
    );
    assert_eq!(
        state
            .visible_entry_by_row(&index, RowId(1), SnapshotId(3))
            .unwrap()
            .created_tx,
        TxId(3)
    );
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn hydration_shares_only_bit_identical_history_in_every_stored_representation() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("zero_history", "embedding");
        store
            .register_index(index.clone(), 1, quantization, budget.clone())
            .unwrap();
        let entries = [0.0_f32, -0.0_f32, 0.0_f32]
            .into_iter()
            .enumerate()
            .map(|(at, value)| VectorEntry {
                index: index.clone(),
                row_id: RowId(1),
                vector: vec![value],
                created_tx: TxId(at as u64 + 1),
                deleted_tx: (at < 2).then_some(TxId(at as u64 + 2)),
                lsn: Lsn(at as u64 + 1),
            })
            .collect::<Vec<_>>();
        let state = store.state(&index).unwrap();
        state
            .install_dormant_raw_directory(
                entries
                    .iter()
                    .map(|entry| RawVectorDirectoryEntry {
                        row_id: entry.row_id,
                        created_tx: entry.created_tx,
                        deleted_tx: entry.deleted_tx,
                        lsn: entry.lsn,
                        membership: VectorMembershipProvenance::new(
                            entry.created_tx,
                            entry.lsn,
                            entry.created_tx,
                            entry.deleted_tx,
                        ),
                    })
                    .collect(),
                Arc::new(Reload {
                    entries,
                    budget: budget.clone(),
                    quantization,
                }),
            )
            .unwrap();
        store.account_loaded_state(budget.clone()).unwrap();
        state
            .ensure_raw_vectors_loaded(&mut executing_admission)
            .unwrap();
        let loaded = state.vectors.read();
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(
            loaded.payload_memberships.len(),
            2,
            "numeric equality must not coalesce signed-zero representations"
        );
        assert_eq!(
            loaded.entries[0].vector, loaded.entries[1].vector,
            "existing numeric equality remains unchanged"
        );
        assert_ne!(
            loaded.entries[0].vector.payload_identity(),
            loaded.entries[1].vector.payload_identity()
        );
        assert_eq!(
            loaded.entries[0].vector.payload_identity(),
            loaded.entries[2].vector.payload_identity()
        );
        assert_eq!(
            loaded.entries[0].vector.to_f32()[0].to_bits(),
            0.0_f32.to_bits()
        );
        assert_eq!(
            loaded.entries[1].vector.to_f32()[0].to_bits(),
            (-0.0_f32).to_bits()
        );
        drop(loaded);
        for (tx, bits) in [
            (1, 0.0_f32.to_bits()),
            (2, (-0.0_f32).to_bits()),
            (3, 0.0_f32.to_bits()),
        ] {
            let visible = state
                .visible_entry_by_row(&index, RowId(1), SnapshotId(tx))
                .unwrap();
            assert_eq!(visible.created_tx, TxId(tx));
            assert_eq!(visible.vector[0].to_bits(), bits);
        }
        drop(state);
        drop(store);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn representation_sharing_checks_quantized_bounds_length_payload_and_kind() {
    use crate::quantized::StoredVector;
    for quantization in [VectorQuantization::SQ8, VectorQuantization::SQ4] {
        let zero = StoredVector::from_f32(&[0.0], quantization);
        for bound in [0, 1] {
            let mut changed_zero = zero.clone();
            match &mut changed_zero {
                StoredVector::SQ8 { min, max, .. } | StoredVector::SQ4 { min, max, .. } => {
                    if bound == 0 {
                        *min = -0.0;
                    } else {
                        *max = -0.0;
                    }
                }
                _ => unreachable!(),
            }
            assert_eq!(
                zero, changed_zero,
                "signed-zero bounds retain numeric equality"
            );
            assert!(
                !zero.representation_eq(&changed_zero),
                "each quantized bound keeps its stored bits"
            );
        }
        let original = StoredVector::from_f32(&[0.0, 1.0], quantization);
        assert!(original.representation_eq(&original.clone()));
        let mut changed = original.clone();
        match &mut changed {
            StoredVector::SQ8 { min, .. } | StoredVector::SQ4 { min, .. } => *min = -0.0,
            _ => unreachable!(),
        }
        assert_eq!(original, changed);
        assert!(!original.representation_eq(&changed));
        match &mut changed {
            StoredVector::SQ8 { min, max, .. } | StoredVector::SQ4 { min, max, .. } => {
                *min = 0.0;
                *max = f32::from_bits(max.to_bits() + 1);
            }
            _ => unreachable!(),
        }
        assert!(!original.representation_eq(&changed));
        let mut changed = original.clone();
        match &mut changed {
            StoredVector::SQ8 { len, .. } | StoredVector::SQ4 { len, .. } => *len += 1,
            _ => unreachable!(),
        }
        assert!(!original.representation_eq(&changed));
        let mut changed = original.clone();
        match &mut changed {
            StoredVector::SQ8 { payload, .. } | StoredVector::SQ4 { payload, .. } => {
                *payload = vec![1].into()
            }
            _ => unreachable!(),
        }
        assert!(!original.representation_eq(&changed));
        assert!(!original.representation_eq(&StoredVector::from_f32(
            &[0.0, 1.0],
            VectorQuantization::F32
        )));
    }
    let sq8 = StoredVector::from_f32(&[0.0, 1.0], VectorQuantization::SQ8);
    let sq4 = StoredVector::from_f32(&[0.0, 1.0], VectorQuantization::SQ4);
    assert!(!sq8.representation_eq(&sq4));
}

#[test]
fn shared_allocation_header_stays_charged_until_final_holder_after_store_drop() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("passages", "embedding");
        store
            .register_index(index.clone(), 768, quantization, budget.clone())
            .unwrap();
        super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
        store.account_loaded_state(budget.clone()).unwrap();
        let state = store.state(&index).unwrap();
        let held = state.stored_by_row_id(RowId(1)).unwrap();
        let final_holder = held.clone();
        let bodies = store.raw_payload_bytes.clone();
        let headers = store.raw_payload_header_bytes.clone();
        let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
        let header_bytes = crate::quantized::shared_payload_header_bytes();
        assert_eq!(headers.load(Ordering::SeqCst), header_bytes);
        let receipt = store.memory_ownership_snapshot_for_test();
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            receipt.raw_vector_bodies
                + receipt.raw_reverse_directory
                + receipt.partition_vectors
                + receipt.vector_partition_controls
                + receipt.typed_partition_keys
        );
        drop(state);
        drop(store);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            body_bytes + header_bytes
        );
        drop(held);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            body_bytes + header_bytes
        );
        assert_eq!(bodies.load(Ordering::SeqCst), body_bytes);
        assert_eq!(headers.load(Ordering::SeqCst), header_bytes);
        drop(final_holder);
        assert_eq!(bodies.load(Ordering::SeqCst), 0);
        assert_eq!(headers.load(Ordering::SeqCst), 0);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn refused_shared_header_hydration_releases_body_workspace_and_retries_without_membership_loss() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());
    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("refused_headers", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    let item = entry(&index, 1, TxId(1), 0);
    let state = store.state(&index).unwrap();
    state
        .install_dormant_raw_directory(
            vec![RawVectorDirectoryEntry {
                row_id: item.row_id,
                created_tx: item.created_tx,
                deleted_tx: item.deleted_tx,
                lsn: item.lsn,
                membership: VectorMembershipProvenance::new(
                    item.created_tx,
                    item.lsn,
                    item.created_tx,
                    item.deleted_tx,
                ),
            }],
            Arc::new(Reload {
                entries: vec![item],
                budget: budget.clone(),
                quantization: VectorQuantization::F32,
            }),
        )
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let before = budget.used.load(Ordering::SeqCst);
    budget.refuse_headers.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut executing_admission), Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "load_raw_vector_headers")
    );
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    assert_eq!(state.entry_count(), 1);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
    assert!(state.vectors.read().entries.is_empty());
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    budget.refuse_headers.store(false, Ordering::SeqCst);
    state
        .ensure_raw_vectors_loaded(&mut executing_admission)
        .unwrap();
    assert_eq!(state.entry_count(), 1);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 768 * 4);
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        crate::quantized::shared_payload_header_bytes()
    );
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn empty_resident_loaded_state_attaches_raw_ownership_before_future_insert() {
    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("empty_loaded", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    let admitted = store.account_loaded_state(budget.clone()).unwrap();
    let state = store.state(&index).unwrap();
    assert!(state.raw_residency_is_accounted());
    assert_eq!(state.entry_count(), 0);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        admitted
            + store
                .registry
                .read()
                .bucket_charge
                .as_ref()
                .map_or(0, |charge| charge.bytes)
    );
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[derive(Debug, Default)]
struct FallbackBudget {
    used: AtomicUsize,
    limit: AtomicUsize,
    allocations: AtomicUsize,
    underflows: AtomicUsize,
}

impl FallbackBudget {
    fn fund_body(&self, bytes: usize) {
        self.try_allocate_for(bytes, "test", "legacy_body", "test")
            .unwrap();
    }

    fn assert_released(&self) {
        assert_eq!(self.used.load(Ordering::SeqCst), 0);
        assert_eq!(self.underflows.load(Ordering::SeqCst), 0);
    }
}

impl MemoryBudget for FallbackBudget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        let limit = self.limit.load(Ordering::SeqCst);
        let limit = if limit == 0 { usize::MAX } else { limit };
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes).filter(|next| *next <= limit)
            })
            .map_err(|used| Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: limit.saturating_sub(used),
                budget_limit_bytes: limit,
                hint: hint.into(),
            })?;
        self.allocations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        if self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_sub(bytes)
            })
            .is_err()
        {
            self.underflows.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn legacy_void_inserts_adopt_only_funded_bodies_without_shared_header_headroom() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(FallbackBudget::default());
        let declared_owner = Arc::new(FallbackBudget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("legacy_inline", "embedding");
        store
            .register_index(index.clone(), 768, quantization, declared_owner.clone())
            .unwrap();
        let directory_bytes = store.account_loaded_state(budget.clone()).unwrap();
        let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
        budget.fund_body(body_bytes);
        budget
            .limit
            .store(directory_bytes + body_bytes, Ordering::SeqCst);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
        assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
        assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
        let state = store.state(&index).unwrap();
        assert!(
            state
                .with_stored_by_row_id(RowId(1), |entry| entry.vector.payload_identity().is_none()
                    && entry.vector.payload_is_charged())
                .unwrap()
        );
        assert_eq!(
            state
                .find_by_row_id(&index, RowId(1), &mut executing_admission)
                .unwrap()
                .unwrap()
                .vector[0],
            1.0
        );

        budget
            .limit
            .store(directory_bytes + 2 * body_bytes, Ordering::SeqCst);
        budget.fund_body(body_bytes);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        let insert_loaded: fn(&VectorStore, VectorEntry, Arc<dyn MemoryBudget>) -> Result<()> =
            VectorStore::insert_loaded_vector;
        insert_loaded(&store, entry(&index, 2, TxId(2), 1), budget.clone()).unwrap();
        assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(state.entry_count(), 2);
        assert_eq!(state.vectors.read().owned_payload_bytes, 2 * body_bytes);
        assert!(state.vectors.read().payload_memberships.is_empty());
        assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(budget.used.load(Ordering::SeqCst), 2 * body_bytes);
        budget
            .limit
            .store(directory_bytes + 3 * body_bytes, Ordering::SeqCst);
        budget.fund_body(body_bytes);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        {
            let mut moved = state
                .with_stored_by_row_id(RowId(2), StoredVectorEntry::owned_copy)
                .unwrap();
            state.tombstone_row(RowId(2), TxId(3));
            moved.row_id = RowId(3);
            moved.created_tx = TxId(3);
            moved.deleted_tx = None;
            moved.lsn = Lsn(3);
            state.push_entry_with(moved, |stored| {
                store.try_column(&index).unwrap().note_move(
                    &VectorPartitionKey::unpartitioned(),
                    &VectorPartitionKey::unpartitioned(),
                    RowId(2),
                    stored.row_id,
                )
            });
        }
        {
            let column = store.try_column(&index).unwrap();
            let bytes = column.directory_retained_bytes(&index);
            let mut charge = column.directory_accounting.lock();
            charge
                .accountant
                .try_allocate_for(
                    bytes - charge.bytes,
                    "test",
                    "loaded_fixture_directory",
                    "test",
                )
                .unwrap();
            charge.bytes = bytes;
        }
        assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(state.entry_count(), 3);
        assert_eq!(state.vectors.read().owned_payload_bytes, 3 * body_bytes);
        assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(
            state
                .visible_entry_by_row(&index, RowId(2), SnapshotId(2))
                .unwrap()
                .created_tx,
            TxId(2)
        );
        assert_eq!(
            state
                .visible_entry_by_row(&index, RowId(3), SnapshotId(3))
                .unwrap()
                .vector[1],
            1.0
        );
        drop(state);
        drop(store);
        budget.assert_released();
    }
}

#[test]
fn loaded_owned_promotion_refusal_preserves_reservations_then_moves_each_body_without_cloning() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("owned_open", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    super::tests::seed_inline_loaded(
        &store,
        vec![entry(&index, 1, TxId(1), 0), entry(&index, 2, TxId(1), 1)],
    );
    let state = store.state(&index).unwrap();
    let body_bytes = 768 * 4;
    let pointers = state.with_entries(|entries| {
        entries
            .iter()
            .map(|entry| entry.vector.as_f32_slice().unwrap().as_ptr())
            .collect::<Vec<_>>()
    });
    let metadata = store
        .try_column(&index)
        .unwrap()
        .directory_retained_bytes(&index);
    let registered_bytes = budget.used.load(Ordering::SeqCst);
    let reserved = 2 * body_bytes;
    budget.fund_body(reserved);
    budget
        .limit
        .store(registered_bytes + reserved, Ordering::SeqCst);
    assert!(
        matches!(store.account_loaded_state_within(budget.clone(), reserved), Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == 2 * crate::quantized::shared_payload_header_bytes())
    );
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        registered_bytes + reserved
    );
    assert!(store.memory_accountant.read().is_none());
    assert!(!state.raw_residency_is_accounted());
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    state.with_entries(|entries| {
        for (entry, pointer) in entries.iter().zip(&pointers) {
            assert_eq!(entry.vector.as_f32_slice().unwrap().as_ptr(), *pointer);
            assert!(entry.vector.payload_identity().is_none());
            assert!(!entry.vector.payload_is_charged());
        }
    });
    let full = reserved + 2 * crate::quantized::shared_payload_header_bytes();
    budget
        .limit
        .store(registered_bytes + full, Ordering::SeqCst);
    assert_eq!(
        store
            .account_loaded_state_within(budget.clone(), reserved)
            .unwrap(),
        metadata + full
    );
    state.with_entries(|entries| {
        for (entry, pointer) in entries.iter().zip(pointers) {
            assert_eq!(entry.vector.as_f32_slice().unwrap().as_ptr(), pointer);
            assert!(entry.vector.payload_identity().is_some());
            assert!(entry.vector.payload_is_charged());
        }
    });
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        2 * crate::quantized::shared_payload_header_bytes()
    );
    assert!(store.account_loaded_state(budget.clone()).is_err());
    assert_eq!(budget.used.load(Ordering::SeqCst), registered_bytes + full);
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn mixed_already_accounted_open_admits_only_new_headers_and_transfers_live_counters() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("mixed_open", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    let state = store.state(&index).unwrap();
    let body_bytes = 768 * 4;
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    budget.fund_body(body_bytes + header_bytes);
    let mut stored = state.stored_entry(entry(&index, 1, TxId(1), 0));
    stored.vector.promote_to_shared();
    state.push_entry(stored);
    let old_bodies = Arc::new(AtomicUsize::new(0));
    let old_headers = Arc::new(AtomicUsize::new(0));
    state.attach_raw_accounting(budget.clone(), old_bodies.clone(), old_headers.clone());
    budget.fund_body(body_bytes);
    store
        .insert_loaded_vector(entry(&index, 2, TxId(2), 1), budget.clone())
        .unwrap();
    let shared_identity = state
        .with_stored_by_row_id(RowId(1), |entry| entry.vector.payload_identity())
        .unwrap();
    let owned_pointer = state
        .with_stored_by_row_id(RowId(2), |entry| {
            entry.vector.as_f32_slice().unwrap().as_ptr()
        })
        .unwrap();
    assert_eq!(old_bodies.load(Ordering::SeqCst), 2 * body_bytes);
    assert_eq!(old_headers.load(Ordering::SeqCst), header_bytes);
    let before = budget.used.load(Ordering::SeqCst);
    budget.limit.store(before, Ordering::SeqCst);
    assert!(store.account_loaded_state(budget.clone()).is_err());
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    assert_eq!(old_bodies.load(Ordering::SeqCst), 2 * body_bytes);
    assert_eq!(old_headers.load(Ordering::SeqCst), header_bytes);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert!(
        state
            .with_stored_by_row_id(RowId(2), |entry| entry.vector.payload_identity().is_none())
            .unwrap()
    );
    budget.limit.store(0, Ordering::SeqCst);
    let metadata = store
        .try_column(&index)
        .unwrap()
        .directory_retained_bytes(&index);
    let directory_credit_before = store
        .try_column(&index)
        .unwrap()
        .directory_accounting
        .lock()
        .bytes;
    assert_eq!(
        store.account_loaded_state(budget.clone()).unwrap(),
        metadata + header_bytes
    );
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        before + header_bytes + metadata - directory_credit_before
    );
    assert_eq!(old_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(old_headers.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        2 * header_bytes
    );
    assert_eq!(
        state
            .with_stored_by_row_id(RowId(1), |entry| entry.vector.payload_identity())
            .unwrap(),
        shared_identity
    );
    assert_eq!(
        state
            .with_stored_by_row_id(RowId(2), |entry| entry
                .vector
                .as_f32_slice()
                .unwrap()
                .as_ptr())
            .unwrap(),
        owned_pointer
    );
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn owned_empty_payloads_promote_to_distinct_shared_allocations() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("empty_payloads", "embedding");
    store
        .register_index(index.clone(), 0, VectorQuantization::F32, budget.clone())
        .unwrap();
    super::tests::seed_inline_loaded(
        &store,
        (1..=2)
            .map(|row| VectorEntry {
                index: index.clone(),
                row_id: RowId(row),
                vector: vec![],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            })
            .collect(),
    );
    let state = store.state(&index).unwrap();
    assert_eq!(state.entry_count(), 2);
    assert_eq!(state.raw_payload_bytes(), 0);
    assert!(state.vectors.read().payload_memberships.is_empty());
    store.account_loaded_state(budget.clone()).unwrap();
    let vectors = state.vectors.read();
    assert_eq!(vectors.payload_memberships.len(), 2);
    assert_ne!(
        vectors.entries[0].vector.payload_identity(),
        vectors.entries[1].vector.payload_identity()
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        2 * crate::quantized::shared_payload_header_bytes()
    );
    drop(vectors);
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn retained_owned_clone_requires_its_own_body_charge_and_never_copies_a_charge_token() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("owned_clone", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = 768 * 4;
    budget.fund_body(body_bytes);
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
    let state = store.state(&index).unwrap();
    budget.fund_body(body_bytes);
    let held = state.stored_by_row_id(RowId(1)).unwrap();
    assert!(!held.vector.payload_is_charged());
    assert!(held.vector.payload_identity().is_none());
    let source_pointer = state
        .with_stored_by_row_id(RowId(1), |entry| {
            entry.vector.as_f32_slice().unwrap().as_ptr()
        })
        .unwrap();
    assert_ne!(held.vector.as_f32_slice().unwrap().as_ptr(), source_pointer);
    held.vector.adopt_raw_charge(
        budget.clone(),
        store.raw_payload_bytes.clone(),
        store.raw_payload_header_bytes.clone(),
    );
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), body_bytes);
    drop(held);
    budget.assert_released();
}

#[test]
fn prepared_owned_source_copy_is_admitted_and_mixed_memberships_prune_exactly() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        prove_owned_source_copy_and_pruning(quantization);
    }
}

fn payload_address(entry: &StoredVectorEntry) -> usize {
    use crate::quantized::StoredVector;
    match &entry.vector {
        StoredVector::F32(payload) => payload.as_ptr() as usize,
        StoredVector::SQ8 { payload, .. } | StoredVector::SQ4 { payload, .. } => {
            payload.as_ptr() as usize
        }
    }
}

fn prove_owned_source_copy_and_pruning(quantization: VectorQuantization) {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("owned_move", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(index.clone(), 768, quantization, budget.clone())
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    budget.fund_body(body_bytes);
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
    let state = store.state(&index).unwrap();
    let source_pointer = state
        .with_stored_by_row_id(RowId(1), payload_address)
        .unwrap();
    let movement = PartitionedVectorMove::new(
        index.clone(),
        key.clone(),
        key.clone(),
        RowId(1),
        RowId(2),
        TxId(2),
        TxId(1),
        Lsn(1),
    );
    let before = budget.used.load(Ordering::SeqCst);
    budget.limit.store(before, Ordering::SeqCst);
    let requested = HashMap::from([(
        index.clone(),
        [key.clone()].into_iter().collect::<PartitionKeySet>(),
    )]);
    let metadata_bound = prepared_directory_growth_upper_bound(
        &[],
        std::slice::from_ref(&movement),
        &HashSet::new(),
        &requested,
    );
    assert!(
        matches!(store.prepare_partitioned_batch(vec![], vec![], vec![movement.clone()], HashSet::new(), VectorCopyAdmission::new(executing_accountant.clone())), Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == metadata_bound + body_bytes)
    );
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    assert_eq!(state.entry_count(), 1);
    assert_eq!(
        state
            .with_stored_by_row_id(RowId(1), |entry| entry.deleted_tx)
            .unwrap(),
        None
    );
    budget.limit.store(0, Ordering::SeqCst);
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![movement.clone()],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    assert_eq!(
        prepared
            .directory_reservation
            .index_charges
            .lock()
            .get(&index)
            .unwrap()
            .bytes,
        metadata_bound + body_bytes
    );
    drop(prepared);
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![movement],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(2), Some(budget.as_ref()));
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        header_bytes
    );
    state
        .with_stored_by_row_id(RowId(1), |entry| {
            assert_eq!(payload_address(entry), source_pointer);
            assert!(entry.vector.payload_identity().is_none());
            assert_eq!(entry.deleted_tx, Some(TxId(2)));
        })
        .unwrap();
    let shared_identity = state
        .with_stored_by_row_id(RowId(2), |entry| {
            assert_ne!(payload_address(entry), source_pointer);
            entry.vector.payload_identity()
        })
        .unwrap();
    assert!(shared_identity.is_some());
    assert_eq!(
        state
            .visible_entry_by_row(&index, RowId(1), SnapshotId(1))
            .unwrap()
            .vector[0],
        1.0
    );
    let shared_move = PartitionedVectorMove::new(
        index.clone(),
        key.clone(),
        key,
        RowId(2),
        RowId(3),
        TxId(3),
        TxId(2),
        Lsn(2),
    );
    let shared_metadata_bound = prepared_directory_growth_upper_bound(
        &[],
        std::slice::from_ref(&shared_move),
        &HashSet::new(),
        &requested,
    );
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![shared_move],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    assert_eq!(
        prepared
            .directory_reservation
            .index_charges
            .lock()
            .get(&index)
            .unwrap()
            .bytes,
        shared_metadata_bound,
        "a shared source requires no new body admission"
    );
    store.publish_prepared_partitioned_batch(prepared, Lsn(3), Some(budget.as_ref()));
    assert_eq!(
        state
            .with_stored_by_row_id(RowId(3), |entry| entry.vector.payload_identity())
            .unwrap(),
        shared_identity
    );
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        header_bytes
    );
    let released = store
        .prune_row_ids(
            &HashSet::from([RowId(1)]),
            budget.as_ref(),
            &mut executing_admission,
        )
        .unwrap();
    budget.release(released);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
    assert_eq!(state.vectors.read().owned_payload_bytes, 0);
    let held = state.stored_by_row_id(RowId(3)).unwrap();
    let released = store
        .prune_row_ids(
            &HashSet::from([RowId(2), RowId(3)]),
            budget.as_ref(),
            &mut executing_admission,
        )
        .unwrap();
    budget.release(released);
    assert_eq!(state.entry_count(), 0);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        header_bytes
    );
    drop(state);
    drop(store);
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        body_bytes + header_bytes
    );
    drop(held);
    budget.assert_released();
}

struct FallbackReload {
    entries: Vec<VectorEntry>,
    budget: Arc<FallbackBudget>,
}

impl DormantRawVectorLoader for FallbackReload {
    fn load_candidate(
        &self,
        _identity: &RawVectorDirectoryEntry,
        _caller_charged: bool,
        _request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        Err(Error::Other(
            "the fallback fixture hydrates the whole partition".into(),
        ))
    }

    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        load_co_created_fixture_carriers(
            &self.entries,
            Some(directory),
            self.budget.clone(),
            VectorQuantization::F32,
            owner,
        )
    }
}

#[test]
fn mixed_owned_shared_eviction_and_reload_preserve_final_holder_charges() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("mixed_eviction", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = 768 * 4;
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    budget.fund_body(body_bytes);
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![
                PartitionedVectorMove::new(
                    index.clone(),
                    key.clone(),
                    key,
                    RowId(1),
                    RowId(1),
                    TxId(2),
                    TxId(1),
                    Lsn(1),
                )
                .replacing_row_version(),
            ],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(2), Some(budget.as_ref()));
    let state = store.state(&index).unwrap();
    let held = state.stored_by_row_id(RowId(1)).unwrap();
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    let directory = state.raw_directory.read().clone();
    let entries = state.all_entries(&index, &mut executing_admission).unwrap();
    state
        .attach_durable_raw_loader(
            directory,
            Arc::new(FallbackReload {
                entries,
                budget: budget.clone(),
            }),
        )
        .unwrap();
    assert!(state.evict_raw_vectors());
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        header_bytes
    );
    state
        .ensure_raw_vectors_loaded(&mut executing_admission)
        .unwrap();
    assert_eq!(state.entry_count(), 2);
    assert_eq!(state.vectors.read().payload_memberships.len(), 1);
    assert_eq!(state.vectors.read().owned_payload_bytes, 0);
    assert_eq!(
        store.raw_payload_bytes.load(Ordering::SeqCst),
        2 * body_bytes
    );
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        2 * header_bytes
    );
    assert_eq!(
        state
            .visible_entry_by_row(&index, RowId(1), SnapshotId(1))
            .unwrap()
            .created_tx,
        TxId(1)
    );
    drop(held);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        header_bytes
    );
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn late_legacy_index_inherits_only_the_callers_funded_body_owner() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let declared_owner = Arc::new(FallbackBudget::default());
    assert_eq!(store.account_loaded_state(budget.clone()).unwrap(), 0);
    let index = VectorIndexRef::new("late_legacy", "embedding");
    store
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            declared_owner.clone(),
        )
        .unwrap();
    budget.fund_body(8);
    budget.limit.store(8, Ordering::SeqCst);
    let allocations = budget.allocations.load(Ordering::SeqCst);
    store
        .insert_loaded_vector(
            VectorEntry {
                index: index.clone(),
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            },
            budget.clone(),
        )
        .unwrap();
    assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 8);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    let state = store.state(&index).unwrap();
    assert_eq!(state.entry_count(), 1);
    assert!(state.raw_residency_is_accounted());
    assert!(
        state
            .with_stored_by_row_id(RowId(1), |entry| entry.vector.payload_identity().is_none()
                && entry.vector.payload_is_charged())
            .unwrap()
    );
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn promotion_keeps_legacy_body_and_new_header_with_their_actual_funders() {
    let body_budget = Arc::new(FallbackBudget::default());
    let open_budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("separate_funders", "embedding");
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::F32,
            open_budget.clone(),
        )
        .unwrap();
    let state = store.state(&index).unwrap();
    state.attach_raw_accounting(
        body_budget.clone(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let body_bytes = 768 * 4;
    body_budget.fund_body(body_bytes);
    store
        .insert_loaded_vector(entry(&index, 1, TxId(1), 0), open_budget.clone())
        .unwrap();
    let metadata = store
        .try_column(&index)
        .unwrap()
        .directory_retained_bytes(&index);
    let registry_bytes = store
        .registry
        .read()
        .bucket_charge
        .as_ref()
        .map_or(0, |charge| charge.bytes);
    let admitted = store.account_loaded_state(open_budget.clone()).unwrap();
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    assert_eq!(admitted, metadata + header_bytes);
    assert_eq!(body_budget.used.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        open_budget.used.load(Ordering::SeqCst),
        metadata + registry_bytes + header_bytes
    );
    open_budget.fund_body(body_bytes);
    store
        .insert_loaded_vector(entry(&index, 2, TxId(2), 1), open_budget.clone())
        .unwrap();
    assert_eq!(body_budget.used.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        open_budget.used.load(Ordering::SeqCst),
        metadata + registry_bytes + header_bytes + body_bytes
    );
    let held = state.stored_by_row_id(RowId(1)).unwrap();
    drop(state);
    drop(store);
    assert_eq!(body_budget.used.load(Ordering::SeqCst), body_bytes);
    assert_eq!(open_budget.used.load(Ordering::SeqCst), header_bytes);
    drop(held);
    body_budget.assert_released();
    open_budget.assert_released();
}

#[test]
fn already_accounted_empty_state_hydration_attaches_every_new_shared_charge() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("accounted_empty_hydration", "embedding");
    store
        .register_index(index.clone(), 768, VectorQuantization::F32, budget.clone())
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let state = store.state(&index).unwrap();
    assert!(state.raw_residency_is_accounted());
    let item = entry(&index, 1, TxId(1), 0);
    state
        .install_dormant_raw_directory(
            vec![RawVectorDirectoryEntry {
                row_id: item.row_id,
                created_tx: item.created_tx,
                deleted_tx: item.deleted_tx,
                lsn: item.lsn,
                membership: VectorMembershipProvenance::new(
                    item.created_tx,
                    item.lsn,
                    item.created_tx,
                    item.deleted_tx,
                ),
            }],
            Arc::new(FallbackReload {
                entries: vec![item],
                budget: budget.clone(),
            }),
        )
        .unwrap();
    state
        .ensure_raw_vectors_loaded(&mut executing_admission)
        .unwrap();
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 768 * 4);
    assert_eq!(
        store.raw_payload_header_bytes.load(Ordering::SeqCst),
        crate::quantized::shared_payload_header_bytes()
    );
    assert!(
        state
            .with_stored_by_row_id(RowId(1), |entry| entry.vector.payload_is_charged())
            .unwrap()
    );
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn legacy_shared_source_move_retains_its_funded_owned_copy_without_new_header() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(FallbackBudget::default());
        let declared_owner = Arc::new(FallbackBudget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("legacy_shared_source", "embedding");
        store
            .register_index(index.clone(), 768, quantization, declared_owner.clone())
            .unwrap();
        super::tests::seed_inline_loaded(&store, vec![entry(&index, 1, TxId(1), 0)]);
        store.account_loaded_state(budget.clone()).unwrap();
        let state = store.state(&index).unwrap();
        let held = state.stored_by_row_id(RowId(1)).unwrap();
        let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
        let header_bytes = crate::quantized::shared_payload_header_bytes();
        let before = budget.used.load(Ordering::SeqCst);
        budget.fund_body(body_bytes);
        budget.limit.store(before + body_bytes, Ordering::SeqCst);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        {
            let mut moved = state
                .with_stored_by_row_id(RowId(1), StoredVectorEntry::owned_copy)
                .unwrap();
            state.tombstone_row(RowId(1), TxId(2));
            moved.row_id = RowId(2);
            moved.created_tx = TxId(2);
            moved.deleted_tx = None;
            moved.lsn = Lsn(2);
            state.push_entry_with(moved, |stored| {
                store.try_column(&index).unwrap().note_move(
                    &VectorPartitionKey::unpartitioned(),
                    &VectorPartitionKey::unpartitioned(),
                    RowId(1),
                    stored.row_id,
                )
            });
        }
        {
            let column = store.try_column(&index).unwrap();
            let bytes = column.directory_retained_bytes(&index);
            let mut charge = column.directory_accounting.lock();
            charge
                .accountant
                .try_allocate_for(
                    bytes - charge.bytes,
                    "test",
                    "loaded_fixture_directory",
                    "test",
                )
                .unwrap();
            charge.bytes = bytes;
        }
        assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(budget.used.load(Ordering::SeqCst), before + body_bytes);
        assert_eq!(
            store.raw_payload_bytes.load(Ordering::SeqCst),
            2 * body_bytes
        );
        assert_eq!(
            store.raw_payload_header_bytes.load(Ordering::SeqCst),
            header_bytes
        );
        assert_eq!(state.vectors.read().owned_payload_bytes, body_bytes);
        assert_eq!(state.vectors.read().payload_memberships.len(), 1);
        state
            .with_stored_by_row_id(RowId(2), |entry| {
                assert!(entry.vector.payload_identity().is_none());
                assert!(entry.vector.payload_is_charged());
                assert_ne!(payload_address(entry), payload_address(&held));
            })
            .unwrap();
        assert_eq!(
            state
                .visible_entry_by_row(&index, RowId(1), SnapshotId(1))
                .unwrap()
                .created_tx,
            TxId(1)
        );
        assert_eq!(
            state
                .visible_entry_by_row(&index, RowId(2), SnapshotId(2))
                .unwrap()
                .vector[0],
            1.0
        );
        drop(state);
        drop(store);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            body_bytes + header_bytes
        );
        drop(held);
        budget.assert_released();
    }
}

/// The 768-dimension F32 layout partitioned by one `scope` key into at most
/// two routes, shared by the received-publication fixtures below.
fn two_scope_layout() -> VectorIndexLayout {
    VectorIndexLayout::new(
        768,
        VectorQuantization::F32,
        vec!["scope".into()],
        Some(2),
        VectorSearchMode::Exact,
    )
}

fn scope(name: &str) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[contextdb_core::Value::Text(name.into())]).unwrap()
}

/// Prepares one received-schema image with the executing statement's copy
/// admission and publishes it under the declared owner.
fn publish_received(
    store: &VectorStore,
    layouts: Vec<(VectorIndexRef, VectorIndexLayout)>,
    entries: Vec<PartitionedVectorEntry>,
    declared: &Arc<Budget>,
    executing: &Arc<Budget>,
) {
    let publication = store
        .prepare_partitioned_received_schema_publication(
            layouts,
            entries,
            declared.clone(),
            VectorCopyAdmission::new(executing.clone()),
        )
        .unwrap();
    store.publish_prepared_received_schema(publication, declared.clone());
}

#[test]
fn resident_export_retains_exact_disjoint_source_memberships_and_copy_owner() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("source_rows", "embedding");
    let a = scope("a");
    let b = scope("b");
    let body = entry(&index, 41, TxId(2), 0);
    let first = VectorMembershipProvenance::new(TxId(7), Lsn(101), TxId(7), Some(TxId(11)));
    let second = VectorMembershipProvenance::new(TxId(13), Lsn(211), TxId(13), None);
    publish_received(
        &store,
        vec![(index.clone(), two_scope_layout())],
        vec![
            PartitionedVectorEntry::with_membership(a.clone(), body.clone(), first, None),
            PartitionedVectorEntry::with_membership(b.clone(), body.clone(), second, None),
        ],
        &declared,
        &executing,
    );
    let declared_before = declared.used.load(Ordering::SeqCst);
    assert!(declared_before > 0);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let exported = store
        .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone(),
        ))
        .unwrap();
    assert_eq!(exported.entries().len(), 2);
    for (key, membership) in [(&a, first), (&b, second)] {
        let copied = exported
            .entries()
            .iter()
            .find(|entry| &entry.partition_key == key)
            .unwrap();
        assert_eq!(copied.membership, membership);
        assert_eq!(
            copied.entry, body,
            "canonical body stamps do not become row ownership stamps"
        );
    }
    assert_eq!(declared.used.load(Ordering::SeqCst), declared_before);
    assert!(executing.used.load(Ordering::SeqCst) > 0);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
    assert!(
        executing.used.load(Ordering::SeqCst) > 0,
        "export survives its source and holds its executing credit"
    );
    let (entries, credit) = exported.into_parts();
    assert_eq!(entries.len(), 2);
    drop(entries);
    assert!(executing.used.load(Ordering::SeqCst) > 0);
    drop(credit);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
}

#[test]
fn resident_export_refusal_is_atomic_and_refunds_its_executing_owner() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("source_rows", "embedding");
    let provenance = VectorMembershipProvenance::new(TxId(21), Lsn(233), TxId(22), Some(TxId(27)));
    publish_received(
        &store,
        vec![(
            index.clone(),
            VectorIndexLayout::unpartitioned(768, VectorQuantization::F32),
        )],
        vec![PartitionedVectorEntry::with_membership(
            VectorPartitionKey::unpartitioned(),
            entry(&index, 4, TxId(2), 0),
            provenance,
            None,
        )],
        &declared,
        &executing,
    );
    let declared_before = declared.used.load(Ordering::SeqCst);
    executing.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone()
        )),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(declared.used.load(Ordering::SeqCst), declared_before);
    executing.refuse.store(false, Ordering::SeqCst);
    let exported = store
        .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone(),
        ))
        .unwrap();
    assert_eq!(exported.entries()[0].membership, provenance);
    drop(exported);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

#[test]
fn ordinary_row_move_restamps_destination_body_and_preserves_exact_row_binding() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("source_rows", "embedding");
    let a = scope("a");
    let b = scope("b");
    let body = entry(&index, 41, TxId(2), 0);
    let first = VectorMembershipProvenance::new(TxId(7), Lsn(101), TxId(7), None);
    let second = VectorMembershipProvenance::new(TxId(13), Lsn(211), TxId(13), None);
    publish_received(
        &store,
        vec![(index.clone(), two_scope_layout())],
        vec![PartitionedVectorEntry::with_membership(
            a.clone(),
            body.clone(),
            first,
            None,
        )],
        &declared,
        &executing,
    );
    let prepared = store
        .prepare_partitioned_batch(
            vec![],
            vec![],
            vec![
                PartitionedVectorMove::new(
                    index.clone(),
                    a.clone(),
                    b.clone(),
                    RowId(41),
                    RowId(41),
                    TxId(13),
                    body.created_tx,
                    body.lsn,
                )
                .with_target_membership(second),
            ],
            HashSet::new(),
            VectorCopyAdmission::new(executing.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(300), Some(declared.as_ref()));
    let copied = store
        .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone(),
        ))
        .unwrap();
    assert_eq!(copied.entries().len(), 2);
    let old = copied
        .entries()
        .iter()
        .find(|entry| entry.partition_key == a)
        .unwrap();
    let new = copied
        .entries()
        .iter()
        .find(|entry| entry.partition_key == b)
        .unwrap();
    assert_eq!(
        old.membership,
        VectorMembershipProvenance {
            deleted_tx: Some(TxId(13)),
            ..first
        }
    );
    assert_eq!(new.membership, second);
    assert_eq!(old.entry.created_tx, body.created_tx);
    assert_eq!(old.entry.lsn, body.lsn);
    assert_eq!(new.entry.created_tx, TxId(13));
    assert_eq!(new.entry.lsn, Lsn(300));
    for placement in [old, new] {
        assert_eq!(placement.entry.row_id, body.row_id);
        assert_eq!(placement.entry.vector, body.vector);
    }
    drop(copied);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

#[test]
fn exact_received_memberships_refuse_overlap_before_publication() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("source_rows", "embedding");
    let a = scope("a");
    let b = scope("b");
    let body = entry(&index, 41, TxId(2), 0);
    let result = store.prepare_partitioned_received_schema_publication(
        vec![(index.clone(), two_scope_layout())],
        vec![
            PartitionedVectorEntry::with_membership(
                a,
                body.clone(),
                VectorMembershipProvenance::new(TxId(7), Lsn(101), TxId(7), Some(TxId(15))),
                None,
            ),
            PartitionedVectorEntry::with_membership(
                b,
                body,
                VectorMembershipProvenance::new(TxId(13), Lsn(211), TxId(13), None),
                None,
            ),
        ],
        declared.clone(),
        VectorCopyAdmission::new(executing.clone()),
    );
    assert!(
        matches!(result, Err(Error::Other(message)) if message.contains("overlapping source memberships"))
    );
    assert!(store.try_column(&index).is_none());
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
}

#[test]
fn selected_resident_export_ignores_giant_unselected_column_and_preserves_provenance() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let selected = VectorIndexRef::new("selected", "embedding");
    let giant = VectorIndexRef::new("unselected", "g".repeat(100_000));
    let first = VectorMembershipProvenance::new(TxId(11), Lsn(91), TxId(12), Some(TxId(13)));
    let second = VectorMembershipProvenance::new(TxId(19), Lsn(191), TxId(20), None);
    let body = entry(&selected, 4, TxId(2), 0);
    publish_received(
        &store,
        vec![
            (selected.clone(), two_scope_layout()),
            (
                giant.clone(),
                VectorIndexLayout::unpartitioned(768, VectorQuantization::F32),
            ),
        ],
        vec![
            PartitionedVectorEntry::with_membership(scope("a"), body.clone(), first, None),
            PartitionedVectorEntry::with_membership(scope("b"), body, second, None),
            PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                entry(&giant, 8, TxId(1), 0),
            ),
        ],
        &declared,
        &executing,
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let copied = store
        .try_resident_partitioned_entries_with_membership_matching(
            VectorCopyAdmission::new(executing.clone()),
            |index, layout| {
                assert_eq!(layout.dimension, 768);
                index == &selected
            },
        )
        .unwrap();
    assert_eq!(copied.entries().len(), 2);
    assert!(
        copied
            .entries()
            .iter()
            .all(|entry| entry.entry.index == selected)
    );
    assert_eq!(copied.entries()[0].membership, first);
    assert_eq!(copied.entries()[1].membership, second);
    assert!(
        executing.used.load(Ordering::SeqCst) < 100_000,
        "the giant excluded index name alone exceeds the selected credit"
    );
    drop(copied);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    executing.refuse.store(true, Ordering::SeqCst);
    let empty = store
        .try_resident_partitioned_entries_with_membership_matching(
            VectorCopyAdmission::new(executing.clone()),
            |_, _| false,
        )
        .unwrap();
    assert!(empty.entries().is_empty());
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(empty);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "test-seams")]
#[test]
fn scalar_quantization_observer_borrows_without_copy_credit_or_source_activity() {
    let declared = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("rows", "embedding");
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::SQ8,
            declared.clone(),
        )
        .unwrap();
    let before = declared.used.load(Ordering::SeqCst);
    let activity_before = store.passive_activity_counters_for_test();
    declared.refuse.store(true, Ordering::SeqCst);
    assert_eq!(
        store.index_quantization_storage_bytes_for_test(&index, 768),
        Some(VectorQuantization::SQ8.storage_bytes(768))
    );
    assert_eq!(
        store.index_quantization_storage_bytes_for_test(
            &VectorIndexRef::new("absent", "embedding"),
            768
        ),
        None
    );
    assert_eq!(declared.used.load(Ordering::SeqCst), before);
    assert_eq!(store.passive_activity_counters_for_test(), activity_before);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

struct DirectoryReentryReload {
    state: std::sync::Weak<IndexState>,
    inner: Reload,
}

impl DormantRawVectorLoader for DirectoryReentryReload {
    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        let state = self.state.upgrade().unwrap();
        assert!(
            state.raw_directory.try_write().is_some(),
            "hydration drops the borrowed directory guard before invoking its loader"
        );
        self.inner.load(directory, owner)
    }
    fn load_candidate(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.inner.load_candidate(identity, caller_charged, request)
    }
}

#[test]
fn raw_loader_hydration_preserves_disjoint_exact_row_bindings_and_body_stamps() {
    let declared = Arc::new(Budget::default());
    let loader_owner = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing.clone());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("rows", "embedding");
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::F32,
            declared.clone(),
        )
        .unwrap();
    let first = VectorMembershipProvenance::new(TxId(11), Lsn(91), TxId(12), Some(TxId(13)));
    let second = VectorMembershipProvenance::new(TxId(19), Lsn(191), TxId(20), None);
    let mut old = entry(&index, 4, TxId(2), 0);
    old.deleted_tx = first.deleted_tx;
    let new = entry(&index, 4, TxId(2), 0);
    let directory = [(&old, first), (&new, second)]
        .into_iter()
        .map(|(entry, membership)| RawVectorDirectoryEntry {
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
            membership,
        })
        .collect();
    let state = store.state(&index).unwrap();
    state
        .install_dormant_raw_directory(
            directory,
            Arc::new(DirectoryReentryReload {
                state: Arc::downgrade(&state),
                inner: Reload {
                    entries: vec![old, new],
                    budget: loader_owner.clone(),
                    quantization: VectorQuantization::F32,
                },
            }),
        )
        .unwrap();
    store.account_loaded_state(declared.clone()).unwrap();
    executing.refuse.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut executing_admission),
        Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "load_raw_vector_directory")
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(loader_owner.used.load(Ordering::SeqCst), 0);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
    executing.refuse.store(false, Ordering::SeqCst);
    state
        .ensure_raw_vectors_loaded(&mut executing_admission)
        .unwrap();
    assert!(state.directory_has_visible_row(RowId(4), SnapshotId(12)));
    assert!(!state.directory_has_visible_row(RowId(4), SnapshotId(19)));
    assert!(state.directory_has_visible_row(RowId(4), SnapshotId(20)));
    let mut inspection = VectorCopyAdmission::new(executing.clone());
    executing.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.storage_bytes_per_entry(&index, &mut inspection),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    executing.refuse.store(false, Ordering::SeqCst);
    let sizes = store
        .storage_bytes_per_entry(&index, &mut inspection)
        .unwrap();
    assert_eq!(sizes, vec![768 * 4, 768 * 4]);
    assert_eq!(
        executing.used.load(Ordering::SeqCst),
        2 * std::mem::size_of::<usize>()
    );
    drop(sizes);
    drop(inspection);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let copied = store
        .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone(),
        ))
        .unwrap();
    assert_eq!(copied.entries().len(), 2);
    assert_eq!(copied.entries()[0].membership, first);
    assert_eq!(copied.entries()[1].membership, second);
    assert!(
        copied
            .entries()
            .iter()
            .all(|entry| entry.entry.created_tx == TxId(2) && entry.entry.lsn == Lsn(2))
    );
    drop(copied);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(state);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
    assert_eq!(loader_owner.used.load(Ordering::SeqCst), 0);
}

struct ValidatedReload {
    inner: Reload,
    corrupt_identity: AtomicBool,
    corrupt_count: AtomicBool,
    corrupt_body: AtomicBool,
    decoded: AtomicUsize,
}

impl DormantRawVectorLoader for ValidatedReload {
    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        let mut loaded = self.inner.load(directory, owner)?;
        self.decoded
            .fetch_add(loaded.entries.len(), Ordering::SeqCst);
        if self.corrupt_count.load(Ordering::SeqCst) {
            loaded.entries.clear();
            return Ok(loaded);
        }
        if self.corrupt_identity.load(Ordering::SeqCst) {
            loaded.entries[0].entry.lsn = Lsn(999);
        }
        if self.corrupt_body.load(Ordering::SeqCst) {
            loaded.entries[0].entry.vector.pop();
        }
        Ok(loaded)
    }

    fn load_candidate(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.inner.load_candidate(identity, caller_charged, request)
    }
}

#[test]
fn hydration_read_workspace_refusal_errors_and_final_holders_keep_distinct_owners() {
    let declared = Arc::new(Budget::default());
    let writer = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let mut admission = VectorCopyAdmission::new(executing.clone());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("hydration", "embedding");
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::F32,
            declared.clone(),
        )
        .unwrap();
    let source = entry(&index, 7, TxId(1), 0);
    let loader = Arc::new(ValidatedReload {
        inner: Reload {
            entries: vec![source.clone()],
            budget: writer.clone(),
            quantization: VectorQuantization::F32,
        },
        corrupt_identity: AtomicBool::new(false),
        corrupt_count: AtomicBool::new(false),
        corrupt_body: AtomicBool::new(false),
        decoded: AtomicUsize::new(0),
    });
    let state = store.state(&index).unwrap();
    state
        .install_dormant_raw_directory(
            vec![RawVectorDirectoryEntry {
                row_id: source.row_id,
                created_tx: source.created_tx,
                deleted_tx: source.deleted_tx,
                lsn: source.lsn,
                membership: VectorMembershipProvenance::new(
                    source.created_tx,
                    source.lsn,
                    source.created_tx,
                    source.deleted_tx,
                ),
            }],
            loader.clone(),
        )
        .unwrap();
    store.account_loaded_state(declared.clone()).unwrap();
    let unchanged_activity = store.passive_activity_counters_for_test();
    executing.refuse_workspace.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut admission), Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "materialize")
    );
    assert_eq!(loader.decoded.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
    assert_eq!(
        store.passive_activity_counters_for_test(),
        unchanged_activity
    );
    executing.refuse_workspace.store(false, Ordering::SeqCst);

    loader.corrupt_count.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut admission), Err(Error::Other(message)) if message.contains("does not match its directory count"))
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    loader.corrupt_count.store(false, Ordering::SeqCst);
    loader.corrupt_identity.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut admission), Err(Error::Other(message)) if message.contains("disagrees with its durable directory"))
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    loader.corrupt_identity.store(false, Ordering::SeqCst);
    loader.corrupt_body.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut admission), Err(Error::Other(message)) if message.contains("charge mismatch"))
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    loader.corrupt_body.store(false, Ordering::SeqCst);
    writer.refuse_headers.store(true, Ordering::SeqCst);
    assert!(
        matches!(state.ensure_raw_vectors_loaded(&mut admission), Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "load_raw_vector_headers")
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    writer.refuse_headers.store(false, Ordering::SeqCst);
    state.ensure_raw_vectors_loaded(&mut admission).unwrap();
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let retained = 768 * 4 + crate::quantized::shared_payload_header_bytes();
    assert_eq!(writer.used.load(Ordering::SeqCst), retained);
    let final_body = state.vectors.read().entries[0].vector.clone();
    drop(state);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
    assert_eq!(writer.used.load(Ordering::SeqCst), retained);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(final_body);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
}

#[test]
fn abandoned_loaded_partition_returns_read_workspace_and_writer_reservation_once() {
    let writer = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let mut admission = VectorCopyAdmission::new(executing.clone());
    let index = VectorIndexRef::new("abandoned", "embedding");
    let source = entry(&index, 7, TxId(1), 0);
    let loaded = load_co_created_fixture_carriers(
        &[source],
        None,
        writer.clone(),
        VectorQuantization::F32,
        &mut admission,
    )
    .unwrap();
    assert_eq!(writer.used.load(Ordering::SeqCst), 768 * 4);
    assert!(executing.used.load(Ordering::SeqCst) > 768 * 4);
    drop(admission);
    assert!(executing.used.load(Ordering::SeqCst) > 768 * 4);
    drop(loaded);
    assert_eq!(writer.used.load(Ordering::SeqCst), 0);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
}

struct CanonicalReload {
    entries: Vec<VectorEntry>,
    canonical_deleted_tx: Option<TxId>,
    budget: Arc<Budget>,
}

impl DormantRawVectorLoader for CanonicalReload {
    fn load_candidate(
        &self,
        _: &RawVectorDirectoryEntry,
        _: bool,
        _: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        Err(Error::Other("fixture requires selected hydration".into()))
    }

    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        let mut loaded = load_co_created_fixture_carriers(
            &self.entries,
            Some(directory),
            self.budget.clone(),
            VectorQuantization::F32,
            owner,
        )?;
        for carrier in &mut loaded.entries {
            // Explicit producer body metadata, never the selected placement stamp.
            carrier.canonical_deleted_tx = self.canonical_deleted_tx;
        }
        Ok(loaded)
    }
}

#[test]
fn canonical_body_deletion_survives_disjoint_placements_selected_reload_and_owned_export() {
    for canonical_deleted_tx in [None, Some(TxId(99))] {
        let declared = Arc::new(Budget::default());
        let executing = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("canonical", "embedding");
        let a = scope("a");
        let b = scope("b");
        let mut first_body = entry(&index, 41, TxId(2), 0);
        first_body.deleted_tx = Some(TxId(7));
        let second_body = entry(&index, 41, TxId(2), 0);
        let first = VectorMembershipProvenance::new(TxId(2), Lsn(2), TxId(2), Some(TxId(7)));
        let second = VectorMembershipProvenance::new(TxId(7), Lsn(7), TxId(7), None);
        publish_received(
            &store,
            vec![(index.clone(), two_scope_layout())],
            vec![
                PartitionedVectorEntry::with_membership(
                    a.clone(),
                    first_body.clone(),
                    first,
                    canonical_deleted_tx,
                ),
                PartitionedVectorEntry::with_membership(
                    b.clone(),
                    second_body.clone(),
                    second,
                    canonical_deleted_tx,
                ),
            ],
            &declared,
            &executing,
        );
        for (key, body, membership) in [(&a, first_body, first), (&b, second_body, second)] {
            let state = store
                .try_column(&index)
                .unwrap()
                .partition_state(key)
                .unwrap();
            let directory = state.raw_directory.read().clone();
            state
                .attach_durable_raw_loader(
                    directory,
                    Arc::new(CanonicalReload {
                        entries: vec![body],
                        canonical_deleted_tx,
                        budget: declared.clone(),
                    }),
                )
                .unwrap();
            assert!(state.evict_raw_vectors());
            let mut owner = VectorCopyAdmission::new(executing.clone());
            state.ensure_raw_vectors_loaded(&mut owner).unwrap();
            state.with_entries(|entries| {
                assert_eq!(entries[0].canonical_deleted_tx, canonical_deleted_tx);
                assert_eq!(entries[0].membership, membership);
                assert_eq!(entries[0].deleted_tx, membership.deleted_tx);
                assert_eq!(entries[0].visible_at(SnapshotId(6)), key == &a);
                assert_eq!(entries[0].visible_at(SnapshotId(7)), key == &b);
            });
            drop(owner);
            assert_eq!(executing.used.load(Ordering::SeqCst), 0);
        }
        let exported = store
            .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
                executing.clone(),
            ))
            .unwrap();
        for (key, membership) in [(&a, first), (&b, second)] {
            let carrier = exported
                .entries()
                .iter()
                .find(|carrier| &carrier.partition_key == key)
                .unwrap();
            assert_eq!(carrier.canonical_deleted_tx, canonical_deleted_tx);
            assert_eq!(carrier.entry.deleted_tx, membership.deleted_tx);
            assert_eq!(carrier.membership, membership);
        }
        drop(store);
        assert_eq!(declared.used.load(Ordering::SeqCst), 0);
        assert!(executing.used.load(Ordering::SeqCst) > 0);
        drop(exported);
        assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn generation_price_and_admission_funder_observers_do_not_load_or_charge() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passive", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::F32,
            declared.clone(),
        )
        .unwrap();
    let state = store.state(&index).unwrap();
    let expected = state.generation_copy_bytes(&index, 3);
    state
        .attach_durable_raw_loader(
            vec![],
            Arc::new(CanonicalReload {
                entries: vec![],
                canonical_deleted_tx: None,
                budget: declared.clone(),
            }),
        )
        .unwrap();
    assert!(state.evict_raw_vectors());
    let before = declared.used.load(Ordering::SeqCst);
    declared.refuse.store(true, Ordering::SeqCst);
    executing.refuse.store(true, Ordering::SeqCst);
    assert_eq!(
        store.generation_copy_bytes_for_test(&index, &key, 3),
        Some(expected)
    );
    let unknown = VectorIndexRef::new("absent", "g".repeat(100_000));
    assert_eq!(
        store.generation_copy_bytes_for_test(&unknown, &key, 3),
        None
    );
    let absent_key = scope("absent");
    assert_eq!(
        store.generation_copy_bytes_for_test(&index, &absent_key, 3),
        None
    );
    assert!(!state.raw_vectors_resident());
    assert_eq!(declared.used.load(Ordering::SeqCst), before);
    let funder: Arc<dyn MemoryBudget> = executing.clone();
    let admission = VectorCopyAdmission::new(funder.clone());
    assert!(Arc::ptr_eq(admission.accountant_for_test(), &funder));
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(admission);
    drop(state);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

// Canonical tombstones belong to persisted body identity, independently of placement visibility.
fn canonical_identity_after_ordinary_publication(distinct_identity: bool, move_row: bool) {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("body_identity", "embedding");
    let a = scope("a");
    let b = scope("b");
    let c = scope("c");
    let mut retired = entry(&index, 41, TxId(2), 0);
    retired.deleted_tx = Some(TxId(7));
    let active = entry(
        &index,
        41,
        if distinct_identity { TxId(8) } else { TxId(2) },
        0,
    );
    let first = VectorMembershipProvenance::new(TxId(2), Lsn(2), TxId(2), Some(TxId(7)));
    let second = VectorMembershipProvenance::new(TxId(8), Lsn(8), TxId(8), None);
    let current_key = if distinct_identity {
        a.clone()
    } else {
        b.clone()
    };
    publish_received(
        &store,
        vec![(index.clone(), two_scope_layout())],
        vec![
            PartitionedVectorEntry::with_membership(a.clone(), retired, first, None),
            PartitionedVectorEntry::with_membership(current_key.clone(), active, second, None),
        ],
        &declared,
        &executing,
    );
    assert!(declared.used.load(Ordering::SeqCst) > 0);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let (deletes, moves) = if move_row {
        (
            vec![],
            vec![PartitionedVectorMove::new(
                index.clone(),
                current_key.clone(),
                c.clone(),
                RowId(41),
                RowId(41),
                TxId(9),
                if distinct_identity { TxId(8) } else { TxId(2) },
                if distinct_identity { Lsn(8) } else { Lsn(2) },
            )],
        )
    } else {
        (
            vec![PartitionedVectorDelete::new(
                index.clone(),
                current_key.clone(),
                RowId(41),
                TxId(9),
                if distinct_identity { TxId(8) } else { TxId(2) },
                if distinct_identity { Lsn(8) } else { Lsn(2) },
            )],
            vec![],
        )
    };
    let prepared = store
        .prepare_partitioned_batch(
            deletes,
            vec![],
            moves,
            HashSet::new(),
            VectorCopyAdmission::new(executing.clone()),
        )
        .unwrap();
    store.publish_prepared_partitioned_batch(prepared, Lsn(9), Some(declared.as_ref()));
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    let exported = store
        .try_resident_partitioned_entries_with_membership(VectorCopyAdmission::new(
            executing.clone(),
        ))
        .unwrap();
    assert!(executing.used.load(Ordering::SeqCst) > 0);
    let retired = exported
        .entries()
        .iter()
        .find(|e| e.membership == first)
        .unwrap();
    let closed = exported
        .entries()
        .iter()
        .find(|e| e.membership.owning_row_tx == TxId(8))
        .unwrap();
    assert_eq!(
        retired.entry.deleted_tx,
        Some(TxId(7)),
        "retired effective visibility remains closed at 7"
    );
    assert_eq!(closed.entry.deleted_tx, Some(TxId(9)));
    assert_eq!(closed.canonical_deleted_tx, Some(TxId(9)));
    if move_row {
        let target = exported
            .entries()
            .iter()
            .find(|e| e.partition_key == c)
            .unwrap();
        assert_eq!(target.canonical_deleted_tx, None);
        assert_eq!(target.entry.created_tx, TxId(9));
        assert_eq!(target.entry.lsn, Lsn(9));
    }
    assert_eq!(
        retired.canonical_deleted_tx,
        if distinct_identity {
            None
        } else {
            Some(TxId(9))
        },
        "only the actual active body identity is physically tombstoned; all placements of that identity agree"
    );
    drop(exported);
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    drop(store);
    assert_eq!(declared.used.load(Ordering::SeqCst), 0);
}

#[test]
fn canonical_identity_distinct_retired_body_delete() {
    canonical_identity_after_ordinary_publication(true, false);
}
#[test]
fn canonical_identity_distinct_retired_body_move() {
    canonical_identity_after_ordinary_publication(true, true);
}
#[test]
fn canonical_identity_shared_disjoint_body_delete() {
    canonical_identity_after_ordinary_publication(false, false);
}
#[test]
fn canonical_identity_shared_disjoint_body_move() {
    canonical_identity_after_ordinary_publication(false, true);
}

#[test]
fn canonical_source_identity_mismatch_refuses_before_publication_and_exact_source_succeeds() {
    let declared = Arc::new(Budget::default());
    let executing = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("identity_validation", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(
            index.clone(),
            768,
            VectorQuantization::F32,
            declared.clone(),
        )
        .unwrap();
    super::tests::seed_inline_loaded(&store, vec![entry(&index, 41, TxId(2), 0)]);
    store.account_loaded_state(declared.clone()).unwrap();
    let resident_before = declared.used.load(Ordering::SeqCst);
    for (created, lsn) in [(TxId(1), Lsn(2)), (TxId(2), Lsn(1))] {
        for movement in [false, true] {
            let delete = PartitionedVectorDelete::new(
                index.clone(),
                key.clone(),
                RowId(41),
                TxId(3),
                created,
                lsn,
            );
            let row_move = PartitionedVectorMove::new(
                index.clone(),
                key.clone(),
                key.clone(),
                RowId(41),
                RowId(42),
                TxId(3),
                created,
                lsn,
            );
            let result = store.prepare_partitioned_batch(
                if movement { vec![] } else { vec![delete] },
                vec![],
                if movement { vec![row_move] } else { vec![] },
                HashSet::new(),
                VectorCopyAdmission::new(executing.clone()),
            );
            assert!(matches!(result, Err(Error::VectorSourceIdentityMismatch {
                index: ref actual_index, row_id: RowId(41), expected_created_tx, expected_lsn,
                actual_created_tx: Some(TxId(2)), actual_lsn: Some(Lsn(2)),
            }) if actual_index == &index && expected_created_tx == created && expected_lsn == lsn));
            assert_eq!(executing.used.load(Ordering::SeqCst), 0);
            assert_eq!(declared.used.load(Ordering::SeqCst), resident_before);
        }
    }
    let state = store.state(&index).unwrap();
    let prepare = || {
        store
            .prepare_partitioned_batch(
                vec![PartitionedVectorDelete::new(
                    index.clone(),
                    key.clone(),
                    RowId(41),
                    TxId(3),
                    TxId(2),
                    Lsn(2),
                )],
                vec![],
                vec![],
                HashSet::new(),
                VectorCopyAdmission::new(executing.clone()),
            )
            .unwrap()
    };
    let pending = prepare();
    assert!(
        state.raw_residency_gate.try_write().is_none(),
        "preparation pins cache residency through commit"
    );
    drop(pending);
    assert!(
        state.raw_residency_gate.try_write().is_some(),
        "abort releases the residency pin"
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
    assert_eq!(
        state
            .stored_by_row_id(RowId(41))
            .unwrap()
            .canonical_deleted_tx,
        None
    );
    store.publish_prepared_partitioned_batch(prepare(), Lsn(3), Some(declared.as_ref()));
    assert_eq!(
        state
            .stored_by_row_id(RowId(41))
            .unwrap()
            .canonical_deleted_tx,
        Some(TxId(3))
    );
    assert_eq!(executing.used.load(Ordering::SeqCst), 0);
}
