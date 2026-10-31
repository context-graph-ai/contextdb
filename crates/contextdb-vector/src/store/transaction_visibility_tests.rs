use super::*;
use roaring::RoaringTreemap;

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
    refuse_tail: AtomicBool,
    reject_tail_number: AtomicUsize,
    tail_calls: AtomicUsize,
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        let tail_refused = operation == "prepare_hnsw_tail"
            && (self.refuse_tail.load(Ordering::SeqCst)
                || self.tail_calls.fetch_add(1, Ordering::SeqCst) + 1
                    == self.reject_tail_number.load(Ordering::SeqCst));
        if tail_refused {
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
        let before = self.used.fetch_sub(bytes, Ordering::SeqCst);
        assert!(before >= bytes, "each vector reservation is released once");
    }
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}

fn entry(row: u64, tx: TxId) -> VectorEntry {
    VectorEntry {
        index: index(),
        row_id: RowId(row),
        vector: vec![1.0, row as f32 / 100.0],
        created_tx: tx,
        deleted_tx: None,
        lsn: Lsn(tx.0),
    }
}

fn layout() -> VectorIndexLayout {
    VectorIndexLayout::unpartitioned(2, VectorQuantization::F32).with_policy(
        Some(1),
        Some(32),
        Some(96),
        Some(17),
        3,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn seeded() -> (VectorStore, Arc<IndexState>, Arc<Budget>) {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let _executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let store = VectorStore::default();
    let budget = Arc::new(Budget::default());
    let mut registration_workspace = VectorCopyAdmission::new(budget.clone());
    store
        .register_index_with_layout(
            index(),
            layout(),
            budget.clone(),
            &mut registration_workspace,
        )
        .unwrap();
    drop(registration_workspace);
    store
        .insert_loaded_vector(entry(1, TxId(1)), budget.clone())
        .unwrap();
    store.account_loaded_state(budget.clone()).unwrap();
    let state = store.state(&index()).unwrap();
    VectorStore::build_prepared_hnsw(
        &index(),
        &state,
        layout().resolve_policy(1, 1),
        budget.clone(),
        VectorCopyAdmission::new(executing_accountant.clone()),
        || {},
        &mut || Ok(()),
    )
    .unwrap();
    (store, state, budget)
}

#[test]
fn refused_tail_admission_preserves_the_row_directory_and_existing_graph() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let _executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let (store, state, budget) = seeded();
    let serial = state.raw_hnsw_build_serial_for_test();
    let topology = state.raw_hnsw_topology_digest_for_test();
    let before_bytes = budget.used.load(Ordering::SeqCst);
    budget.refuse_tail.store(true, Ordering::SeqCst);
    let refusal = store
        .prepare_partitioned_batch(
            Vec::new(),
            vec![PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                entry(2, TxId(2)),
            )],
            Vec::new(),
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap_err();
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { operation, .. } if operation == "prepare_hnsw_tail")
    );
    assert_eq!(state.entry_count(), 1);
    assert!(!state.directory_has_visible_row(RowId(2), SnapshotId(2)));
    assert_eq!(state.raw_hnsw_build_serial_for_test(), serial);
    assert_eq!(state.raw_hnsw_topology_digest_for_test(), topology);
    assert_eq!(budget.used.load(Ordering::SeqCst), before_bytes);
    assert_eq!(
        state
            .raw_hnsw_search(&index(), &[1.0, 0.0], 1)
            .unwrap()
            .unwrap()[0]
            .0,
        RowId(1)
    );
    budget.refuse_tail.store(false, Ordering::SeqCst);
    let prepared = store
        .prepare_partitioned_batch(
            Vec::new(),
            vec![PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                entry(2, TxId(2)),
            )],
            Vec::new(),
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    drop(prepared);
    assert_eq!(
        budget.used.load(Ordering::SeqCst),
        before_bytes,
        "aborting preparation returns every charge"
    );
    assert_eq!(state.raw_hnsw_topology_digest_for_test(), topology);
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn snapshot_counts_and_filtered_eligibility_do_not_walk_excluded_history() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let state = IndexState::new(2, VectorQuantization::F32);
    state.push_entry(state.stored_entry(entry(1, TxId(1))));
    state.push_entry(state.stored_entry(entry(2, TxId(2))));
    let allowed = RoaringTreemap::from_iter([1]);
    for history in [0, 64, 4096] {
        for row in state.entry_count() as u64 + 1..history + 3 {
            let mut historical = entry(row, TxId(10 + row));
            historical.deleted_tx = Some(TxId(20 + row));
            state.push_entry(state.stored_entry(historical));
        }
        for snapshot in [SnapshotId(1), SnapshotId(u64::MAX)] {
            let expected = if snapshot.0 == 1 { 1 } else { 2 };
            assert_eq!(
                state.directory_visible_entry_count(snapshot, None),
                expected
            );
            assert_eq!(
                state.directory_visible_entry_count(snapshot, Some(&allowed)),
                1
            );
            let mut eligibility_visits = 0;
            let eligibility = state
                .bounded_hnsw_eligibility(
                    TxId(snapshot.0),
                    || {
                        eligibility_visits += 1;
                        Ok::<(), Error>(())
                    },
                    &mut executing_admission,
                )
                .unwrap();
            assert_eq!(eligibility.live_count, expected);
            assert_eq!(eligibility_visits, 1, "history must not add directory work");
            let mut count_visits = 0;
            assert_eq!(
                state
                    .bounded_directory_visible_entry_count(
                        snapshot,
                        Some(&[1]),
                        || {
                            count_visits += 1;
                            Ok::<(), Error>(())
                        },
                        &mut executing_admission
                    )
                    .unwrap(),
                1
            );
            assert_eq!(count_visits, 2, "only the supplied candidate is inspected");
        }
    }
    state.tombstone_row(RowId(1), TxId(5000));
    state.push_entry(state.stored_entry(entry(1, TxId(5000))));
    assert!(state.directory_has_visible_row(RowId(1), SnapshotId(4999)));
    assert_eq!(
        state
            .visible_directory_entry_by_row(RowId(1), SnapshotId(4999))
            .unwrap()
            .created_tx,
        TxId(1)
    );
    assert_eq!(
        state
            .visible_directory_entry_by_row(RowId(1), SnapshotId(5000))
            .unwrap()
            .created_tx,
        TxId(5000)
    );
    state.prune_directory_versions(|entry| entry.deleted_tx.is_none());
    assert_eq!(
        state.directory_visible_entry_count(SnapshotId(u64::MAX), None),
        2
    );
    assert_eq!(state.directory_visible_entry_count(SnapshotId(1), None), 0);
    assert!(matches!(
        state.bounded_hnsw_eligibility(
            TxId(5000),
            || Err::<(), _>(Error::ReadCancelled),
            &mut executing_admission
        ),
        Err(Error::ReadCancelled)
    ));
}

#[test]
fn graph_node_count_does_not_double_count_a_tail_becoming_a_base() {
    let (store, state, budget) = seeded();
    assert_eq!(state.hnsw_len(), Some(1));
    let (tail_read, saw_tail) = std::sync::mpsc::channel();
    let (resume, continue_count) = std::sync::mpsc::channel();
    let observed = std::thread::scope(|threads| {
        let reader_state = state.clone();
        let count = threads.spawn(move || {
            reader_state.hnsw_len_after_tail(|| {
                tail_read.send(()).unwrap();
                continue_count.recv().unwrap();
            })
        });
        saw_tail.recv().unwrap();
        // Publish at this exact interleaving if the reader does not protect
        // one generation image; otherwise publish as soon as it releases it.
        let publication_available = state.generation_publication.try_lock().is_some();
        if publication_available {
            publish_base(&state, &budget, 1);
        }
        resume.send(()).unwrap();
        let observed = count.join().unwrap();
        if !publication_available {
            publish_base(&state, &budget, 1);
        }
        observed
    });
    assert_eq!(state.hnsw_len(), Some(1));
    assert_eq!(
        state.with_resident_sealed_generation(true, |_, graph| graph
            .raw_entry_count_for_row(RowId(1))),
        Some(1)
    );
    println!("node count before=1 after=1 observed={observed:?}");
    assert_eq!(
        observed,
        Some(1),
        "publication must not count the same node as both old tail and new base"
    );
    drop(state);
    drop(store);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn overlapping_snapshots_pin_only_the_generations_they_select() {
    let state = IndexState::new(2, VectorQuantization::F32);
    let budget = Arc::new(Budget::default());
    publish_base(&state, &budget, 1);
    publish_base(&state, &budget, 2);
    publish_base(&state, &budget, 3);
    assert_eq!(
        state.reclaim_snapshot_free_graph_generations(&[
            SnapshotId(10),
            SnapshotId(25),
            SnapshotId(30)
        ]),
        0
    );
    publish_base(&state, &budget, 4);
    let all_bytes = budget.used.load(Ordering::SeqCst);
    assert_eq!(
        state.reclaim_snapshot_free_graph_generations(&[
            SnapshotId(10),
            SnapshotId(25),
            SnapshotId(40)
        ]),
        1
    );
    assert!(budget.used.load(Ordering::SeqCst) < all_bytes);
    assert_eq!(state.retained_chain_ids(), vec![(1, None), (2, None)]);
    assert_eq!(
        state
            .snapshot_serving_policy(SnapshotId(25))
            .unwrap()
            .hnsw_m,
        32
    );
    assert_eq!(
        state.reclaim_snapshot_free_graph_generations(&[SnapshotId(25), SnapshotId(40)]),
        1
    );
    assert_eq!(state.retained_chain_ids(), vec![(2, None)]);
    assert_eq!(
        state.reclaim_snapshot_free_graph_generations(&[SnapshotId(40)]),
        1
    );
    assert!(state.retained_chain_ids().is_empty());
    drop(state);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

struct DirectoryOnlyLoader;
impl DormantRawVectorLoader for DirectoryOnlyLoader {
    fn load(
        &self,
        _: &[RawVectorDirectoryEntry],
        _owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition> {
        Err(Error::Other(
            ("counting must not hydrate raw vector bodies".to_owned()).into(),
        ))
    }
    fn load_candidate(
        &self,
        _: &RawVectorDirectoryEntry,
        _: bool,
        _: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        Err(Error::Other(
            ("counting must not load a raw candidate".to_owned()).into(),
        ))
    }
}

#[test]
fn dormant_counts_keep_old_snapshot_visibility_and_sorted_row_versions() {
    let state = IndexState::new(2, VectorQuantization::F32);
    let identity = |row, created, deleted: Option<u64>| RawVectorDirectoryEntry {
        row_id: RowId(row),
        created_tx: TxId(created),
        deleted_tx: deleted.map(TxId),
        lsn: Lsn(created),
        membership: VectorMembershipProvenance::new(
            TxId(created),
            Lsn(created),
            TxId(created),
            deleted.map(TxId),
        ),
    };
    state
        .install_dormant_raw_directory(
            vec![
                identity(7, 20, None),
                identity(9, 4, Some(8)),
                identity(7, 1, Some(20)),
            ],
            Arc::new(DirectoryOnlyLoader),
        )
        .unwrap();
    assert_eq!(state.directory_visible_entry_count(SnapshotId(5), None), 2);
    assert_eq!(state.directory_visible_entry_count(SnapshotId(20), None), 1);
    assert_eq!(
        state
            .visible_directory_entry_by_row(RowId(7), SnapshotId(5))
            .unwrap()
            .created_tx,
        TxId(1)
    );
    assert_eq!(
        state
            .visible_directory_entry_by_row(RowId(7), SnapshotId(20))
            .unwrap()
            .created_tx,
        TxId(20)
    );
    state.prune_directory_versions(|entry| entry.deleted_tx.is_none());
    assert_eq!(state.directory_visible_entry_count(SnapshotId(5), None), 0);
    assert_eq!(state.directory_visible_entry_count(SnapshotId(20), None), 1);
    assert!(!state.raw_resident.load(Ordering::SeqCst));
}

#[test]
fn refusing_a_later_tail_insert_releases_the_whole_prepared_batch() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let _executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let (store, state, budget) = seeded();
    let bytes = budget.used.load(Ordering::SeqCst);
    let topology = state.raw_hnsw_topology_digest_for_test();
    budget.reject_tail_number.store(2, Ordering::SeqCst);
    let error = store
        .prepare_partitioned_batch(
            Vec::new(),
            vec![
                PartitionedVectorEntry::new(VectorPartitionKey::unpartitioned(), entry(2, TxId(2))),
                PartitionedVectorEntry::new(VectorPartitionKey::unpartitioned(), entry(3, TxId(2))),
            ],
            Vec::new(),
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap_err();
    assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
    assert_eq!(budget.tail_calls.load(Ordering::SeqCst), 2);
    assert_eq!(state.entry_count(), 1);
    assert_eq!(state.raw_hnsw_topology_digest_for_test(), topology);
    assert_eq!(state.fresh_tail_preparations.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), bytes);
}
