use super::*;
use roaring::RoaringTreemap;

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
            let mut all_visits = 0;
            assert_eq!(
                state
                    .bounded_directory_visible_entry_count(
                        snapshot,
                        None,
                        || {
                            all_visits += 1;
                            Ok::<(), Error>(())
                        },
                        &mut executing_admission,
                    )
                    .unwrap(),
                expected
            );
            assert_eq!(all_visits, 1, "history must not add directory work");
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
        state.bounded_directory_visible_entry_count(
            SnapshotId(5000),
            None,
            || Err::<(), _>(Error::ReadCancelled),
            &mut executing_admission
        ),
        Err(Error::ReadCancelled)
    ));
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
