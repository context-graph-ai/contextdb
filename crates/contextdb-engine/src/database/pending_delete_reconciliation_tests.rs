use super::*;

fn composite_key(scope: &str) -> NaturalKey {
    NaturalKey::from_pairs(vec![
        ("id".to_string(), Value::Int64(1)),
        ("scope".to_string(), Value::Text(scope.to_string())),
    ])
    .unwrap()
}

fn row(natural_key: NaturalKey, lsn: Lsn, deleted: bool) -> RowChange {
    RowChange {
        table: "events".to_string(),
        natural_key,
        values: HashMap::new(),
        deleted,
        lsn,
        created_at: None,
    }
}

#[test]
fn pending_delete_reconciliation_retires_only_its_old_row_and_vector_payload() {
    let retired_key = composite_key("retired");
    let different_key = composite_key("different");
    let index = VectorIndexRef::new("events", "embedding");
    let changes = ChangeSet {
        rows: vec![
            row(retired_key.clone(), Lsn(2), false),
            row(retired_key.clone(), Lsn(4), false),
            row(different_key.clone(), Lsn(2), false),
            row(retired_key.clone(), Lsn(2), true),
        ],
        vectors: vec![
            VectorChange {
                index: index.clone(),
                row_id: RowId(10),
                vector: vec![1.0],
                lsn: Lsn(2),
            },
            VectorChange {
                index: index.clone(),
                row_id: RowId(10),
                vector: Vec::new(),
                lsn: Lsn(3),
            },
            VectorChange {
                index: index.clone(),
                row_id: RowId(11),
                vector: vec![2.0],
                lsn: Lsn(4),
            },
            VectorChange {
                index,
                row_id: RowId(12),
                vector: vec![3.0],
                lsn: Lsn(2),
            },
        ],
        ..ChangeSet::default()
    };
    let pending = DurablePendingDelete {
        row: row(retired_key.clone(), Lsn(3), true),
        local_row_id: Some(RowId(10)),
    };

    let reconciled = Database::reconcile_durable_pending_deletes(changes, vec![pending]);

    assert!(
        !reconciled
            .rows
            .iter()
            .any(|row| { !row.deleted && row.natural_key == retired_key && row.lsn == Lsn(2) })
    );
    assert!(
        reconciled
            .rows
            .iter()
            .any(|row| { !row.deleted && row.natural_key == retired_key && row.lsn == Lsn(4) })
    );
    assert!(
        reconciled.rows.iter().any(|row| {
            !row.deleted && row.natural_key == different_key && row.lsn == Lsn(2)
        })
    );
    assert!(
        reconciled
            .rows
            .iter()
            .any(|row| { row.deleted && row.natural_key == retired_key && row.lsn == Lsn(2) })
    );
    assert!(
        reconciled
            .rows
            .iter()
            .any(|row| { row.deleted && row.natural_key == retired_key && row.lsn == Lsn(3) })
    );
    assert!(!reconciled.vectors.iter().any(|vector| {
        vector.row_id == RowId(10) && !vector.vector.is_empty() && vector.lsn == Lsn(2)
    }));
    assert!(reconciled.vectors.iter().any(|vector| {
        vector.row_id == RowId(10) && vector.vector.is_empty() && vector.lsn == Lsn(3)
    }));
    assert!(reconciled.vectors.iter().any(|vector| {
        vector.row_id == RowId(11) && !vector.vector.is_empty() && vector.lsn == Lsn(4)
    }));
    assert!(reconciled.vectors.iter().any(|vector| {
        vector.row_id == RowId(12) && !vector.vector.is_empty() && vector.lsn == Lsn(2)
    }));
}
