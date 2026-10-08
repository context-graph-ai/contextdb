use super::*;
use crate::quantized::{StoredVector, shared_payload_header_bytes, stored_vector_resident_bytes};

#[derive(Debug, Default)]
struct CopyBudget {
    used: AtomicUsize,
    refuse_headers: AtomicBool,
    header_requests: AtomicUsize,
    requested_header_bytes: AtomicUsize,
}

impl MemoryBudget for CopyBudget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if operation == "load_raw_vector_headers" {
            self.header_requests.fetch_add(1, Ordering::SeqCst);
            self.requested_header_bytes.store(bytes, Ordering::SeqCst);
            if self.refuse_headers.load(Ordering::SeqCst) {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.into(),
                    operation: operation.into(),
                    requested_bytes: bytes,
                    available_bytes: 0,
                    budget_limit_bytes: self.used.load(Ordering::SeqCst),
                    hint: hint.into(),
                });
            }
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        assert!(
            self.used
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                    used.checked_sub(bytes)
                })
                .is_ok(),
            "each body, header and workspace reservation releases once"
        );
    }
}

struct CopyReload {
    entries: Vec<VectorEntry>,
    budget: Arc<CopyBudget>,
    quantization: VectorQuantization,
}

impl DormantRawVectorLoader for CopyReload {
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

    fn load_candidate(
        &self,
        _identity: &RawVectorDirectoryEntry,
        _caller_charged: bool,
        _request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        Err(Error::Other(
            "the copy fixture loads a complete partition".into(),
        ))
    }
}

fn copy_entry(index: &VectorIndexRef, row: RowId, tx: TxId, vector: Vec<f32>) -> VectorEntry {
    VectorEntry {
        index: index.clone(),
        row_id: row,
        vector,
        created_tx: tx,
        deleted_tx: None,
        lsn: Lsn(tx.0),
    }
}

fn dormant_copy_store(
    index: &VectorIndexRef,
    dimension: usize,
    quantization: VectorQuantization,
    entries: Vec<VectorEntry>,
    budget: &Arc<CopyBudget>,
) -> (VectorStore, Arc<IndexState>) {
    let store = VectorStore::default();
    store
        .register_index(index.clone(), dimension, quantization, budget.clone())
        .unwrap();
    let state = store.state(index).unwrap();
    let directory = entries
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
        .collect();
    state
        .install_dormant_raw_directory(
            directory,
            Arc::new(CopyReload {
                entries,
                budget: budget.clone(),
                quantization,
            }),
        )
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    (store, state)
}

fn owned_receipt_bytes(store: &VectorStore) -> usize {
    let receipt = store.memory_ownership_snapshot_for_test();
    receipt.raw_vector_bodies
        + receipt.raw_reverse_directory
        + receipt.partition_vectors
        + receipt.vector_partition_controls
        + receipt.base_graph
        + receipt.change_graph
        + receipt.mutable_tail
        + receipt.typed_partition_keys
        + receipt.retired_pinned_graph_bytes
        + receipt.temporary_workspace
}

fn payload_pointer(vector: &StoredVector) -> usize {
    match vector {
        StoredVector::F32(payload) => payload.as_ptr() as usize,
        StoredVector::SQ8 { payload, .. } | StoredVector::SQ4 { payload, .. } => {
            payload.as_ptr() as usize
        }
    }
}

#[test]
fn detached_copy_unique_unwrap_moves_the_body_and_preserves_shared_or_charged_owners() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let mut vector = StoredVector::from_f32(&[0.0, 0.5, 1.0, 0.0], quantization);
        let pointer = payload_pointer(&vector);
        let expected = vector.to_f32();
        vector.promote_to_shared();
        assert!(vector.payload_identity().is_some());
        vector.move_unique_uncharged_to_owned();
        assert!(vector.payload_identity().is_none());
        assert_eq!(
            payload_pointer(&vector),
            pointer,
            "unwrapping does not clone a body"
        );
        assert_eq!(vector.to_f32(), expected);

        vector.promote_to_shared();
        let held = vector.clone();
        let identity = vector.payload_identity();
        vector.move_unique_uncharged_to_owned();
        assert_eq!(vector.payload_identity(), identity);
        assert_eq!(held.payload_identity(), identity);
        assert_eq!(payload_pointer(&vector), pointer);
        drop(held);

        let budget = Arc::new(CopyBudget::default());
        let bodies = Arc::new(AtomicUsize::new(0));
        let headers = Arc::new(AtomicUsize::new(0));
        let charge = vector.storage_bytes() + shared_payload_header_bytes();
        budget
            .try_allocate_for(charge, "test", "charged_owner", "test")
            .unwrap();
        vector.adopt_raw_charge(budget.clone(), bodies.clone(), headers.clone());
        vector.move_unique_uncharged_to_owned();
        assert_eq!(
            vector.payload_identity(),
            identity,
            "charged headers retain their real owner"
        );
        assert_eq!(payload_pointer(&vector), pointer);
        assert_eq!(budget.used.load(Ordering::SeqCst), charge);
        drop(vector);
        assert_eq!(bodies.load(Ordering::SeqCst), 0);
        assert_eq!(headers.load(Ordering::SeqCst), 0);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn detached_copy_hydration_retains_only_new_singleton_bodies_and_releases_headers() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(CopyBudget::default());
        let index = VectorIndexRef::new("receiver_copy", "embedding");
        let entries = (1..=120)
            .map(|row| {
                let mut vector = vec![0.0; 64];
                vector[0] = row as f32;
                copy_entry(&index, RowId(row), TxId(1), vector)
            })
            .collect();
        let (store, state) = dormant_copy_store(&index, 64, quantization, entries, &budget);
        let before = budget.used.load(Ordering::SeqCst);
        let copy_bytes = store.partitioned_copy_bytes_matching(|selected| selected == &index);
        budget
            .try_allocate_for(copy_bytes, "test", "detached_copy", "test")
            .unwrap();
        let copies = store
            .try_partitioned_entries_matching(
                |selected| selected == &index,
                &mut executing_admission,
            )
            .unwrap();
        assert_eq!(copies.len(), 120);
        let body_bytes = 120 * stored_vector_resident_bytes(64, quantization);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            before + copy_bytes + body_bytes
        );
        assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
        assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(
            budget.requested_header_bytes.load(Ordering::SeqCst),
            120 * shared_payload_header_bytes()
        );
        let loaded = state.vectors.read();
        assert!(loaded.payload_memberships.is_empty());
        assert_eq!(loaded.owned_payload_bytes, body_bytes);
        assert!(
            loaded
                .entries
                .iter()
                .all(|entry| entry.vector.payload_identity().is_none()
                    && entry.vector.payload_is_charged())
        );
        drop(loaded);
        for copy in &copies {
            let original = state
                .visible_entry_by_row(&index, copy.entry.row_id, SnapshotId(1))
                .unwrap();
            assert_eq!(copy.entry, original);
        }
        drop(copies);
        budget.release(copy_bytes);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            owned_receipt_bytes(&store)
        );
        drop(state);
        drop(store);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn detached_copy_header_refusal_releases_loader_body_workspace_and_retries() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let budget = Arc::new(CopyBudget::default());
    let index = VectorIndexRef::new("receiver_refusal", "embedding");
    let item = copy_entry(&index, RowId(1), TxId(1), vec![0.0, 1.0]);
    let (store, state) =
        dormant_copy_store(&index, 2, VectorQuantization::F32, vec![item], &budget);
    let before = budget.used.load(Ordering::SeqCst);
    let copy_bytes = store.partitioned_copy_bytes_matching(|_| true);
    budget
        .try_allocate_for(copy_bytes, "test", "detached_copy", "test")
        .unwrap();
    budget.refuse_headers.store(true, Ordering::SeqCst);
    assert!(
        matches!(store.try_partitioned_entries_matching(|_| true, &mut executing_admission), Err(Error::MemoryBudgetExceeded { operation, requested_bytes, .. }) if operation == "load_raw_vector_headers" && requested_bytes == shared_payload_header_bytes())
    );
    assert_eq!(budget.used.load(Ordering::SeqCst), before + copy_bytes);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
    assert_eq!(state.entry_count(), 1);
    assert!(state.vectors.read().entries.is_empty());
    assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    budget.refuse_headers.store(false, Ordering::SeqCst);
    let copies = store
        .try_partitioned_entries_matching(|_| true, &mut executing_admission)
        .unwrap();
    assert_eq!(copies.len(), 1);
    assert_eq!(budget.header_requests.load(Ordering::SeqCst), 2);
    assert_eq!(budget.used.load(Ordering::SeqCst), before + copy_bytes + 8);
    assert_eq!(store.raw_payload_header_bytes.load(Ordering::SeqCst), 0);
    drop(copies);
    budget.release(copy_bytes);
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        owned_receipt_bytes(&store)
    );
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn detached_copy_mixed_history_preserves_bits_and_shared_final_holder_charges() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let budget = Arc::new(CopyBudget::default());
        let index = VectorIndexRef::new("receiver_history", "embedding");
        let entries = [0.0_f32, -0.0_f32, 0.0_f32]
            .into_iter()
            .enumerate()
            .map(|(at, value)| {
                let mut entry = copy_entry(&index, RowId(1), TxId(at as u64 + 1), vec![value]);
                entry.deleted_tx = (at < 2).then_some(TxId(at as u64 + 2));
                entry
            })
            .collect();
        let (store, state) = dormant_copy_store(&index, 1, quantization, entries, &budget);
        let copy_bytes = store.partitioned_copy_bytes_matching(|_| true);
        budget
            .try_allocate_for(copy_bytes, "test", "detached_copy", "test")
            .unwrap();
        let copies = store
            .try_partitioned_entries_matching(|_| true, &mut executing_admission)
            .unwrap();
        let body_bytes = stored_vector_resident_bytes(1, quantization);
        assert_eq!(
            store.raw_payload_bytes.load(Ordering::SeqCst),
            2 * body_bytes
        );
        assert_eq!(
            store.raw_payload_header_bytes.load(Ordering::SeqCst),
            shared_payload_header_bytes()
        );
        let loaded = state.vectors.read();
        assert_eq!(loaded.payload_memberships.len(), 1);
        assert_eq!(loaded.owned_payload_bytes, body_bytes);
        let shared = loaded.entries[0].vector.payload_identity();
        assert!(shared.is_some());
        assert_eq!(loaded.entries[2].vector.payload_identity(), shared);
        assert!(loaded.entries[1].vector.payload_identity().is_none());
        let held = loaded.entries[0].clone();
        drop(loaded);
        for (at, bits) in [0.0_f32.to_bits(), (-0.0_f32).to_bits(), 0.0_f32.to_bits()]
            .into_iter()
            .enumerate()
        {
            assert_eq!(copies[at].entry.vector[0].to_bits(), bits);
            assert_eq!(
                state
                    .visible_entry_by_row(&index, RowId(1), SnapshotId(at as u64 + 1))
                    .unwrap()
                    .vector[0]
                    .to_bits(),
                bits
            );
        }
        drop(copies);
        budget.release(copy_bytes);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            owned_receipt_bytes(&store)
        );
        drop(state);
        let released = store
            .prune_row_ids(
                &HashSet::from([RowId(1)]),
                budget.as_ref(),
                &mut executing_admission,
            )
            .unwrap();
        budget.release(released);
        assert_eq!(store.raw_payload_bytes.load(Ordering::SeqCst), body_bytes);
        assert_eq!(
            store.raw_payload_header_bytes.load(Ordering::SeqCst),
            shared_payload_header_bytes()
        );
        drop(store);
        assert_eq!(
            budget.used.load(Ordering::SeqCst),
            body_bytes + shared_payload_header_bytes()
        );
        assert_eq!(held.vector.to_f32()[0].to_bits(), 0.0_f32.to_bits());
        drop(held);
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn detached_copy_already_resident_sources_keep_representation_charge_and_funder() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let loader_budget = Arc::new(CopyBudget::default());
        let copy_budget = Arc::new(CopyBudget::default());
        let index = VectorIndexRef::new("resident_copy", "embedding");
        let entry = copy_entry(&index, RowId(1), TxId(1), vec![0.0, 1.0]);
        let (store, state) =
            dormant_copy_store(&index, 2, quantization, vec![entry], &loader_budget);
        state
            .ensure_raw_vectors_loaded(&mut executing_admission)
            .unwrap();
        let before = loader_budget.used.load(Ordering::SeqCst);
        let receipt = store.memory_ownership_snapshot_for_test();
        let (identity, pointer) = state.with_entries(|entries| {
            (
                entries[0].vector.payload_identity(),
                payload_pointer(&entries[0].vector),
            )
        });
        assert!(identity.is_some());
        let copy_bytes = store.partitioned_copy_bytes_matching(|_| true);
        copy_budget
            .try_allocate_for(copy_bytes, "test", "detached_copy", "test")
            .unwrap();
        let copies = store
            .try_partitioned_entries_matching(|_| true, &mut executing_admission)
            .unwrap();
        assert_eq!(loader_budget.used.load(Ordering::SeqCst), before);
        assert_eq!(copy_budget.used.load(Ordering::SeqCst), copy_bytes);
        assert_eq!(store.memory_ownership_snapshot_for_test(), receipt);
        assert_eq!(loader_budget.header_requests.load(Ordering::SeqCst), 1);
        state.with_entries(|entries| {
            assert_eq!(entries[0].vector.payload_identity(), identity);
            assert_eq!(payload_pointer(&entries[0].vector), pointer);
        });
        drop(copies);
        copy_budget.release(copy_bytes);
        let held = state.stored_by_row_id(RowId(1)).unwrap();
        drop(state);
        drop(store);
        assert_eq!(
            loader_budget.used.load(Ordering::SeqCst),
            held.estimated_bytes() + shared_payload_header_bytes()
        );
        assert_eq!(copy_budget.used.load(Ordering::SeqCst), 0);
        drop(held);
        assert_eq!(loader_budget.used.load(Ordering::SeqCst), 0);
    }
}
