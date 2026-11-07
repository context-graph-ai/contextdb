use super::*;

fn retained_entry(
    index: &VectorIndexRef,
    row: RowId,
    tx: TxId,
    deleted: Option<TxId>,
    changed: bool,
) -> VectorEntry {
    let mut vector = vec![0.0; 32];
    vector[usize::from(changed)] = -0.0;
    vector[2] = if changed { 0.26 } else { 0.13 };
    vector[3] = if changed { 1.74 } else { 0.87 };
    VectorEntry {
        index: index.clone(),
        row_id: row,
        vector,
        created_tx: tx,
        deleted_tx: deleted,
        lsn: Lsn(tx.0),
    }
}

fn assert_bit_identical(actual: &[VectorEntry], expected: &[VectorEntry]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.index, expected.index);
        assert_eq!(actual.row_id, expected.row_id);
        assert_eq!(actual.created_tx, expected.created_tx);
        assert_eq!(actual.deleted_tx, expected.deleted_tx);
        assert_eq!(actual.lsn, expected.lsn);
        assert_eq!(
            actual
                .vector
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            expected
                .vector
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>()
        );
    }
}

fn copied_body_bytes(entries: &[VectorEntry]) -> usize {
    entries
        .iter()
        .map(|entry| entry.vector.capacity() * 4)
        .sum()
}

#[test]
fn selected_generation_materialization_preserves_all_representations_and_snapshot_intervals() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let declared_column_owner = Arc::new(super::tests::CountingMemoryBudget::default());
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let store = VectorStore::default();
        let index = VectorIndexRef::new("selection_items", "embedding");
        let key = VectorPartitionKey::unpartitioned();
        store
            .register_index(
                index.clone(),
                32,
                quantization,
                declared_column_owner.clone(),
            )
            .unwrap();
        // Scalar repeats retain distinct identities; tx 33 changes the body
        // and its quantized bounds. Row 2 is deleted, row 3 has a future
        // tombstone, and row 4 lies beyond the tested generation frontier.
        let mut history = (1..=65)
            .map(|tx| {
                retained_entry(
                    &index,
                    RowId(1),
                    TxId(tx),
                    (tx < 65).then_some(TxId(tx + 1)),
                    tx >= 33,
                )
            })
            .collect::<Vec<_>>();
        history.push(retained_entry(
            &index,
            RowId(2),
            TxId(3),
            Some(TxId(12)),
            false,
        ));
        history.push(retained_entry(
            &index,
            RowId(3),
            TxId(4),
            Some(TxId(70)),
            true,
        ));
        history.push(retained_entry(&index, RowId(4), TxId(66), None, false));
        store
            .apply_inserts(
                history,
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        let state = store.partition_state(&index, &key).unwrap();
        let executing_workspace = store
            .reserve_workspace(
                executing_accountant.clone(),
                state.generation_copy_bytes(&index, state.vectors.read().entries.len()),
                "generation_selection_fixture",
                "fund fixture materialization with its executing accountant",
            )
            .unwrap();
        for snapshot in [0, 1, 4, 12, 32, 33, 65, 70] {
            for after_lsn in [
                None,
                Some(Lsn(4)),
                Some(Lsn(64)),
                Some(Lsn(65)),
                Some(Lsn(99)),
            ] {
                let full = store
                    .entries_for_partition_at_snapshot(
                        &index,
                        &key,
                        SnapshotId(snapshot),
                        &executing_workspace,
                    )
                    .unwrap();
                let expected = full
                    .iter()
                    .filter(|entry| {
                        entry.deleted_tx.is_none()
                            && after_lsn.is_none_or(|boundary| entry.lsn > boundary)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let selected = store
                    .entries_for_partition_generation_at_snapshot(
                        &index,
                        &key,
                        SnapshotId(snapshot),
                        after_lsn,
                        &executing_workspace,
                    )
                    .unwrap();
                assert_bit_identical(&selected, &expected);
                assert_eq!(selected.capacity(), selected.len());
                let (raw_count, graph_count) = store
                    .partition_generation_entry_counts(
                        &index,
                        &key,
                        SnapshotId(snapshot),
                        after_lsn,
                        &mut executing_admission,
                    )
                    .unwrap();
                assert_eq!(full.len(), raw_count);
                assert_eq!(selected.len(), graph_count);
                assert!(selected.iter().all(|entry| entry.deleted_tx.is_none()));
            }
        }
        let full = store
            .entries_for_partition_at_snapshot(&index, &key, SnapshotId(65), &executing_workspace)
            .unwrap();
        let selected = store
            .entries_for_partition_generation_at_snapshot(
                &index,
                &key,
                SnapshotId(65),
                None,
                &executing_workspace,
            )
            .unwrap();
        assert_eq!(full.len(), 67);
        assert_eq!(selected.len(), 2);
        assert_eq!(full.capacity(), 67);
        assert_eq!(selected.capacity(), 2);
        assert_eq!(copied_body_bytes(&full), 67 * 32 * 4);
        assert_eq!(copied_body_bytes(&selected), 2 * 32 * 4);
        assert_eq!(
            copied_body_bytes(&full) - copied_body_bytes(&selected),
            65 * 32 * 4
        );
        assert_eq!(
            (full.capacity() - selected.capacity()) * std::mem::size_of::<VectorEntry>(),
            65 * std::mem::size_of::<VectorEntry>()
        );
        assert_ne!(selected[0].vector.as_ptr(), selected[1].vector.as_ptr());
        if quantization == VectorQuantization::F32 {
            assert_eq!(selected[0].vector[1].to_bits(), (-0.0f32).to_bits());
        }
        let before_change = store
            .entries_for_partition_generation_at_snapshot(
                &index,
                &key,
                SnapshotId(32),
                None,
                &executing_workspace,
            )
            .unwrap();
        assert_eq!(before_change[0].created_tx, TxId(32));
        assert_eq!(before_change[0].deleted_tx, None);
        assert_ne!(
            before_change[0].vector[3].to_bits(),
            selected[0].vector[3].to_bits()
        );
        // Selection did not prune or rewrite any retained membership.
        let retained = store
            .entries_for_partition(&index, &key, &mut executing_admission)
            .unwrap();
        assert_eq!(retained.len(), 68);
        assert_eq!(retained[31].deleted_tx, Some(TxId(33)));
        assert_eq!(retained[66].deleted_tx, Some(TxId(70)));
        eprintln!(
            "selected_generation quantization={quantization:?} full_slots={} selected_slots={} full_body_bytes={} selected_body_bytes={}",
            full.capacity(),
            selected.capacity(),
            copied_body_bytes(&full),
            copied_body_bytes(&selected)
        );
    }
}

#[test]
fn selected_generation_refuses_inconsistent_metadata_before_body_materialization() {
    let executing_accountant = Arc::new(super::tests::CountingMemoryBudget::default());
    let _executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

    let declared_column_owner = Arc::new(super::tests::CountingMemoryBudget::default());
    let store = VectorStore::default();
    let index = VectorIndexRef::new("selection_items", "embedding");
    let key = VectorPartitionKey::unpartitioned();
    store
        .register_index(
            index.clone(),
            32,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    store
        .apply_inserts(
            vec![retained_entry(&index, RowId(1), TxId(1), None, false)],
            VectorCopyAdmission::new(executing_accountant.clone()),
        )
        .unwrap();
    let state = store.partition_state(&index, &key).unwrap();
    let executing_workspace = store
        .reserve_workspace(
            executing_accountant.clone(),
            state.generation_copy_bytes(&index, state.vectors.read().entries.len()),
            "generation_selection_fixture",
            "fund fixture materialization with its executing accountant",
        )
        .unwrap();
    state.raw_directory.write().clear();
    assert!(matches!(
        store.entries_for_partition_generation_at_snapshot(&index, &key, SnapshotId(1), None, &executing_workspace),
        Err(Error::StoreCorrupted { reason, .. })
            if reason == "generation selection differs from its raw directory"
    ));
    assert_eq!(state.vectors.read().entries.len(), 1);
}
