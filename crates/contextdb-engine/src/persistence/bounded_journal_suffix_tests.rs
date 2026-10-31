use super::*;
use std::cell::Cell;

thread_local! { static EXAMINED: Cell<usize> = const { Cell::new(0) }; }

pub(super) fn note_examined() {
    EXAMINED.with(|count| count.set(count.get() + 1));
}

fn record(
    index: &VectorIndexRef,
    key: &VectorPartitionKey,
    lsn: Lsn,
    ordinal: u64,
) -> VectorPartitionJournalRecord {
    let lsn = lsn.0;
    VectorPartitionJournalRecord {
        index: index.clone(),
        partition_key: key.clone(),
        lsn: Lsn(lsn),
        ordinal,
        change: VectorPartitionJournalChange::Upsert(VectorPartitionMembershipRecord {
            index: index.clone(),
            partition_key: key.clone(),
            row_id: RowId(lsn.wrapping_mul(4).wrapping_add(ordinal)),
            row_created_tx: TxId(lsn),
            row_lsn: Lsn(lsn),
            vector_created_tx: TxId(lsn),
            vector_lsn: Lsn(lsn),
            visible_from: TxId(lsn),
            deleted_tx: None,
        }),
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn fixture(
    history: u64,
) -> (
    tempfile::TempDir,
    RedbPersistence,
    VectorIndexRef,
    VectorPartitionKey,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("journal.db");
    crate::Database::open(&path).unwrap().close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    let index = VectorIndexRef::new("points", "embedding");
    let key = VectorPartitionKey::from_values(&[contextdb_core::Value::Int64(1)]).unwrap();
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                for lsn in 1..=history + 1 {
                    let ordinals: &[u64] = if lsn == history {
                        &[0, u64::MAX]
                    } else if lsn == history + 1 {
                        &[0, 1, 2]
                    } else {
                        &[0]
                    };
                    for &ordinal in ordinals {
                        let record = record(&index, &key, Lsn(lsn), ordinal);
                        let physical = RedbPersistence::vector_partition_journal_key(&record);
                        let bytes = RedbPersistence::encode_vector_partition_record(
                            VectorPartitionRecordKind::Journal,
                            &record,
                        )?;
                        table
                            .insert(physical.as_slice(), bytes.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    (root, persistence, index, key)
}

#[test]
fn fixed_journal_suffix_selection_does_not_examine_n_or_four_n_covered_records() {
    let work = [64, 256].map(|history| {
        let (_root, persistence, index, key) = fixture(history);
        EXAMINED.with(|count| count.set(0));
        assert_eq!(
            persistence
                .vector_partition_tail_entry_count(&index, &key, Lsn(history))
                .unwrap(),
            3
        );
        let count_work = EXAMINED.with(|count| count.replace(0));
        assert!(
            persistence
                .vector_partition_has_uncovered_journal_records(&index, &key, Lsn(history))
                .unwrap()
        );
        let probe_work = EXAMINED.with(|count| count.replace(0));
        assert_eq!(
            persistence
                .vector_partition_tail_entry_count(&index, &key, Lsn(u64::MAX))
                .unwrap(),
            0
        );
        assert!(
            !persistence
                .vector_partition_has_uncovered_journal_records(&index, &key, Lsn(u64::MAX))
                .unwrap()
        );
        (
            count_work,
            probe_work,
            EXAMINED.with(|count| count.replace(0)),
        )
    });
    eprintln!("journal_suffix covered=64/256 examined(count,probe,max_frontier)={work:?}");
    assert_eq!(work, [(3, 1, 0), (3, 1, 0)]);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn suffix_seek_includes_all_ordinals_and_excludes_later_cohorts() {
    let (_root, persistence, index, key) = fixture(64);
    let (lower, upper) = RedbPersistence::vector_partition_journal_suffix_bounds(
        &index,
        &key,
        Lsn(63),
        Some(Lsn(64)),
    )
    .unwrap()
    .unwrap();
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = read
                .open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut ordinals = Vec::new();
            for entry in table
                .range(lower.as_slice()..upper.as_slice())
                .map_err(RedbPersistence::storage_error)?
            {
                let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                let record: VectorPartitionJournalRecord = persistence
                    .decode_vector_partition_record(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                    )?;
                assert_eq!(record.lsn, Lsn(64));
                ordinals.push(record.ordinal);
            }
            assert_eq!(ordinals, [0, u64::MAX]);
            Ok(())
        })
        .unwrap();
    assert!(
        RedbPersistence::vector_partition_journal_suffix_bounds(
            &index,
            &key,
            Lsn(64),
            Some(Lsn(63))
        )
        .unwrap()
        .is_none()
    );
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn suffix_seek_still_authenticates_each_selected_record() {
    let (_root, persistence, index, key) = fixture(64);
    let physical = RedbPersistence::vector_partition_journal_key(&record(&index, &key, Lsn(65), 0));
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                let mut bytes = table
                    .get(physical.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .unwrap()
                    .value()
                    .to_vec();
                *bytes.last_mut().unwrap() ^= 1;
                table
                    .insert(physical.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    assert!(matches!(
        persistence.vector_partition_tail_entry_count(&index, &key, Lsn(64)),
        Err(Error::StoreCorrupted { .. })
    ));
    assert!(matches!(
        persistence.vector_partition_has_uncovered_journal_records(&index, &key, Lsn(64)),
        Err(Error::StoreCorrupted { .. })
    ));
}
