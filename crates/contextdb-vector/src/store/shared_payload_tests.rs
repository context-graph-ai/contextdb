use super::*;

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
    refuse: AtomicBool,
    refuse_headers: AtomicBool,
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
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("passages", "embedding");
        let key = VectorPartitionKey::unpartitioned();
        store.register_index(index.clone(), 768, quantization);
        store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
                        )
                        .replacing_row_version(),
                    ],
                    HashSet::new(),
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
                .live_entry_for_row_in_partition(&index, &key, RowId(1), SnapshotId(tx))
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
                )],
                vec![PartitionedVectorEntry::new(key.clone(), new_entry)],
                vec![],
                HashSet::new(),
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
                .live_entry_for_row_in_partition(&index, &key, RowId(1), SnapshotId(65))
                .unwrap()
                .vector[0],
            1.0
        );
        assert_eq!(
            store
                .live_entry_for_row_in_partition(&index, &key, RowId(1), SnapshotId(66))
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
        )
        .unwrap();
    store
        .apply_partitioned_inserts(vec![PartitionedVectorEntry::new(
            source.clone(),
            entry(&index, 1, TxId(1), 0),
        )])
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
            )],
            HashSet::new(),
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
        .prune_row_ids(&HashSet::from([RowId(1)]), budget.as_ref())
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
    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passages", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
    store.account_loaded_state(budget.clone()).unwrap();
    let before = budget.used.load(Ordering::SeqCst);
    let movement =
        PartitionedVectorMove::new(index.clone(), key.clone(), key, RowId(1), RowId(1), TxId(2))
            .replacing_row_version();
    let prepared = store
        .prepare_partitioned_batch(vec![], vec![], vec![movement.clone()], HashSet::new())
        .unwrap();
    drop(prepared);
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    budget.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.prepare_partitioned_batch(vec![], vec![], vec![movement], HashSet::new()),
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
    fn load(&self, directory: &[RawVectorDirectoryEntry]) -> Result<LoadedRawVectorPartition> {
        assert_eq!(directory.len(), self.entries.len());
        let bytes = self
            .entries
            .iter()
            .map(|entry| {
                crate::quantized::stored_vector_resident_bytes(
                    entry.vector.len(),
                    self.quantization,
                )
            })
            .sum();
        self.budget
            .try_allocate_for(bytes, "vector_index", "reload", "test")?;
        Ok(LoadedRawVectorPartition {
            entries: self.entries.clone(),
            resident_bytes: bytes,
            workspace_bytes: 0,
            accountant: self.budget.clone(),
        })
    }
}

#[test]
fn eviction_and_reload_share_unchanged_history_and_keep_prior_holder_charged() {
    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("passages", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
                    )
                    .replacing_row_version(),
                ],
                HashSet::new(),
            )
            .unwrap();
        store.publish_prepared_partitioned_batch(prepared, Lsn(tx), Some(budget.as_ref()));
    }
    let state = store.state(&index).unwrap();
    let held = state.stored_by_row_id(RowId(1)).unwrap();
    let entries = state.all_entries(&index);
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
    state.ensure_raw_vectors_loaded().unwrap();
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
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("zero_history", "embedding");
        store.register_index(index.clone(), 1, quantization);
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
        state.ensure_raw_vectors_loaded().unwrap();
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
        store.register_index(index.clone(), 768, quantization);
        store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
    let budget = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("refused_headers", "embedding");
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    let item = entry(&index, 1, TxId(1), 0);
    let state = store.state(&index).unwrap();
    state
        .install_dormant_raw_directory(
            vec![RawVectorDirectoryEntry {
                row_id: item.row_id,
                created_tx: item.created_tx,
                deleted_tx: item.deleted_tx,
                lsn: item.lsn,
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
        matches!(state.ensure_raw_vectors_loaded(), Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "load_raw_vector_headers")
    );
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    assert_eq!(state.entry_count(), 1);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
    assert!(state.vectors.read().entries.is_empty());
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    budget.refuse_headers.store(false, Ordering::SeqCst);
    state.ensure_raw_vectors_loaded().unwrap();
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
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    let admitted = store.account_loaded_state(budget.clone()).unwrap();
    let state = store.state(&index).unwrap();
    assert!(state.raw_residency_is_accounted());
    assert_eq!(state.entry_count(), 0);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), admitted);
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
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(FallbackBudget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("legacy_inline", "embedding");
        store.register_index(index.clone(), 768, quantization);
        let directory_bytes = store.account_loaded_state(budget.clone()).unwrap();
        let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
        budget.fund_body(body_bytes);
        budget
            .limit
            .store(directory_bytes + body_bytes, Ordering::SeqCst);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        let apply_inserts: fn(&VectorStore, Vec<VectorEntry>, Option<&dyn MemoryBudget>) =
            VectorStore::apply_inserts_with_accountant;
        apply_inserts(
            &store,
            vec![entry(&index, 1, TxId(1), 0)],
            Some(budget.as_ref()),
        );
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
            state.find_by_row_id(&index, RowId(1)).unwrap().vector[0],
            1.0
        );

        budget
            .limit
            .store(directory_bytes + 2 * body_bytes, Ordering::SeqCst);
        budget.fund_body(body_bytes);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        let insert_loaded: fn(&VectorStore, VectorEntry) = VectorStore::insert_loaded_vector;
        insert_loaded(&store, entry(&index, 2, TxId(2), 1));
        assert_eq!(budget.allocations.load(Ordering::SeqCst), allocations);
        assert_eq!(state.entry_count(), 2);
        assert_eq!(state.vectors.read().owned_payload_bytes, 2 * body_bytes);
        assert!(state.vectors.read().payload_memberships.is_empty());
        assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            directory_bytes + 2 * body_bytes
        );
        budget
            .limit
            .store(directory_bytes + 3 * body_bytes, Ordering::SeqCst);
        budget.fund_body(body_bytes);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        store.apply_moves_with_accountant(
            vec![(index.clone(), RowId(2), RowId(3), TxId(3))],
            Lsn(3),
            Some(budget.as_ref()),
        );
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
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    store.apply_inserts(vec![
        entry(&index, 1, TxId(1), 0),
        entry(&index, 2, TxId(1), 1),
    ]);
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
    let reserved = metadata + 2 * body_bytes;
    budget.fund_body(reserved);
    budget.limit.store(reserved, Ordering::SeqCst);
    assert!(
        matches!(store.account_loaded_state_within(budget.clone(), reserved), Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == 2 * crate::quantized::shared_payload_header_bytes())
    );
    assert_eq!(budget.used.load(Ordering::SeqCst), reserved);
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
    budget.limit.store(full, Ordering::SeqCst);
    assert_eq!(
        store
            .account_loaded_state_within(budget.clone(), reserved)
            .unwrap(),
        full
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
    assert_eq!(budget.used.load(Ordering::SeqCst), full);
    drop(state);
    drop(store);
    budget.assert_released();
}

#[test]
fn mixed_already_accounted_open_admits_only_new_headers_and_transfers_live_counters() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("mixed_open", "embedding");
    store.register_index(index.clone(), 768, VectorQuantization::F32);
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
    store.insert_loaded_vector(entry(&index, 2, TxId(2), 1));
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
    assert_eq!(
        store.account_loaded_state(budget.clone()).unwrap(),
        metadata + header_bytes
    );
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        before + metadata + header_bytes
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
    store.register_index(index.clone(), 0, VectorQuantization::F32);
    store.apply_inserts(
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
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = 768 * 4;
    budget.fund_body(body_bytes);
    store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("owned_move", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store.register_index(index.clone(), 768, quantization);
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    budget.fund_body(body_bytes);
    store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
        matches!(store.prepare_partitioned_batch(vec![], vec![], vec![movement.clone()], HashSet::new()), Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == metadata_bound + body_bytes)
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
        .prepare_partitioned_batch(vec![], vec![], vec![movement.clone()], HashSet::new())
        .unwrap();
    assert_eq!(
        prepared
            .directory_reservation
            .charge
            .lock()
            .as_ref()
            .unwrap()
            .bytes,
        metadata_bound + body_bytes
    );
    drop(prepared);
    assert_eq!(budget.used.load(Ordering::SeqCst), before);
    let prepared = store
        .prepare_partitioned_batch(vec![], vec![], vec![movement], HashSet::new())
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
    let shared_move =
        PartitionedVectorMove::new(index.clone(), key.clone(), key, RowId(2), RowId(3), TxId(3));
    let shared_metadata_bound = prepared_directory_growth_upper_bound(
        &[],
        std::slice::from_ref(&shared_move),
        &HashSet::new(),
        &requested,
    );
    let prepared = store
        .prepare_partitioned_batch(vec![], vec![], vec![shared_move], HashSet::new())
        .unwrap();
    assert_eq!(
        prepared
            .directory_reservation
            .charge
            .lock()
            .as_ref()
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
        .prune_row_ids(&HashSet::from([RowId(1)]), budget.as_ref())
        .unwrap();
    budget.release(released);
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
    assert_eq!(state.vectors.read().owned_payload_bytes, 0);
    let held = state.stored_by_row_id(RowId(3)).unwrap();
    let released = store
        .prune_row_ids(&HashSet::from([RowId(2), RowId(3)]), budget.as_ref())
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

    fn load(&self, directory: &[RawVectorDirectoryEntry]) -> Result<LoadedRawVectorPartition> {
        assert_eq!(directory.len(), self.entries.len());
        let bytes = self
            .entries
            .iter()
            .map(|entry| entry.vector.len() * 4)
            .sum();
        self.budget
            .try_allocate_for(bytes, "vector_index", "fallback_reload", "test")?;
        Ok(LoadedRawVectorPartition {
            entries: self.entries.clone(),
            resident_bytes: bytes,
            workspace_bytes: 0,
            accountant: self.budget.clone(),
        })
    }
}

#[test]
fn mixed_owned_shared_eviction_and_reload_preserve_final_holder_charges() {
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("mixed_eviction", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    store.account_loaded_state(budget.clone()).unwrap();
    let body_bytes = 768 * 4;
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    budget.fund_body(body_bytes);
    store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
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
                )
                .replacing_row_version(),
            ],
            HashSet::new(),
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
    let entries = state.all_entries(&index);
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
    state.ensure_raw_vectors_loaded().unwrap();
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
    assert_eq!(store.account_loaded_state(budget.clone()).unwrap(), 0);
    let index = VectorIndexRef::new("late_legacy", "embedding");
    store.register_index(index.clone(), 2, VectorQuantization::F32);
    budget.fund_body(8);
    budget.limit.store(8, Ordering::SeqCst);
    let allocations = budget.allocations.load(Ordering::SeqCst);
    store.apply_changes_with_accountant_ref(
        &[],
        &[VectorEntry {
            index: index.clone(),
            row_id: RowId(1),
            vector: vec![1.0, 0.0],
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        }],
        &[],
        Lsn(1),
        Some(budget.as_ref()),
    );
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
    store.register_index(index.clone(), 768, VectorQuantization::F32);
    let state = store.state(&index).unwrap();
    state.attach_raw_accounting(
        body_budget.clone(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let body_bytes = 768 * 4;
    body_budget.fund_body(body_bytes);
    store.insert_loaded_vector(entry(&index, 1, TxId(1), 0));
    let metadata = store
        .try_column(&index)
        .unwrap()
        .directory_retained_bytes(&index);
    let admitted = store.account_loaded_state(open_budget.clone()).unwrap();
    let header_bytes = crate::quantized::shared_payload_header_bytes();
    assert_eq!(admitted, metadata + header_bytes);
    assert_eq!(body_budget.used.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        open_budget.used.load(Ordering::SeqCst),
        metadata + header_bytes
    );
    open_budget.fund_body(body_bytes);
    store.insert_loaded_vector(entry(&index, 2, TxId(2), 1));
    assert_eq!(body_budget.used.load(Ordering::SeqCst), body_bytes);
    assert_eq!(
        open_budget.used.load(Ordering::SeqCst),
        metadata + header_bytes + body_bytes
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
    let budget = Arc::new(FallbackBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("accounted_empty_hydration", "embedding");
    store.register_index(index.clone(), 768, VectorQuantization::F32);
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
            }],
            Arc::new(FallbackReload {
                entries: vec![item],
                budget: budget.clone(),
            }),
        )
        .unwrap();
    state.ensure_raw_vectors_loaded().unwrap();
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
        let store = VectorStore::default();
        let index = VectorIndexRef::new("legacy_shared_source", "embedding");
        store.register_index(index.clone(), 768, quantization);
        store.apply_inserts(vec![entry(&index, 1, TxId(1), 0)]);
        store.account_loaded_state(budget.clone()).unwrap();
        let state = store.state(&index).unwrap();
        let held = state.stored_by_row_id(RowId(1)).unwrap();
        let body_bytes = crate::quantized::stored_vector_resident_bytes(768, quantization);
        let header_bytes = crate::quantized::shared_payload_header_bytes();
        let before = budget.used.load(Ordering::SeqCst);
        budget.fund_body(body_bytes);
        budget.limit.store(before + body_bytes, Ordering::SeqCst);
        let allocations = budget.allocations.load(Ordering::SeqCst);
        store.apply_moves_with_accountant(
            vec![(index.clone(), RowId(1), RowId(2), TxId(2))],
            Lsn(2),
            Some(budget.as_ref()),
        );
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
