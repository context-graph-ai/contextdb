use super::*;

const TABLE: &str = "notes";
const COLUMN: &str = "embedding";

fn vector_meta() -> HashMap<String, TableMeta> {
    HashMap::from([(
        TABLE.to_string(),
        rough_sync_table_meta(
            &[(COLUMN.to_string(), "VECTOR(3)".to_string())],
            &[],
            &[],
            &[],
            &[],
        ),
    )])
}

fn vector_row(
    row_id: RowId,
    created_tx: u64,
    deleted_tx: Option<u64>,
    lsn: Lsn,
) -> VersionedRow {
    VersionedRow {
        row_id,
        values: HashMap::from([(COLUMN.to_string(), Value::Vector(vec![0.25, 0.5, 0.75]))]),
        created_tx: TxId(created_tx),
        deleted_tx: deleted_tx.map(TxId),
        lsn,
        created_at: None,
    }
}

fn loaded_vector(row_id: RowId, created_tx: u64, lsn: Lsn) -> VectorEntry {
    VectorEntry {
        index: VectorIndexRef::new(TABLE, COLUMN),
        row_id,
        vector: vec![0.25, 0.5, 0.75],
        created_tx: TxId(created_tx),
        deleted_tx: None,
        lsn,
    }
}

#[test]
fn body_only_newer_row_version_keeps_existing_live_vector_owner() {
    let tables = HashMap::from([(
        TABLE.to_string(),
        vec![
            vector_row(RowId(7), 11, Some(12), Lsn(11)),
            vector_row(RowId(7), 12, None, Lsn(12)),
        ],
    )]);
    let mut vectors = vec![loaded_vector(RowId(7), 11, Lsn(11))];

    assert!(!supplement_loaded_vectors_from_table_rows(
        &tables,
        &vector_meta(),
        &mut vectors,
    ));
    assert_eq!(vectors.len(), 1);
    assert_eq!(vectors[0].created_tx, TxId(11));
    assert_eq!(vectors[0].lsn, Lsn(11));
    assert_eq!(
        vectors
            .iter()
            .filter(|entry| entry.deleted_tx.is_none() && entry.row_id == RowId(7))
            .count(),
        1
    );
}

#[test]
fn absent_live_vector_owner_is_supplemented_once() {
    let tables = HashMap::from([(
        TABLE.to_string(),
        vec![vector_row(RowId(7), 12, None, Lsn(12))],
    )]);
    let mut vectors = Vec::new();

    assert!(supplement_loaded_vectors_from_table_rows(
        &tables,
        &vector_meta(),
        &mut vectors,
    ));
    assert_eq!(vectors.len(), 1);
    assert_eq!(vectors[0].created_tx, TxId(12));
    assert_eq!(vectors[0].lsn, Lsn(12));
    assert_eq!(vectors[0].deleted_tx, None);

    assert!(!supplement_loaded_vectors_from_table_rows(
        &tables,
        &vector_meta(),
        &mut vectors,
    ));
    assert_eq!(vectors.len(), 1);
}
