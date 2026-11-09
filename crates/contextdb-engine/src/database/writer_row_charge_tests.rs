use super::*;

#[test]
fn repeated_trigger_bound_replacement_releases_prior_staged_row_charge() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE generation (id UUID PRIMARY KEY, counter INTEGER NOT NULL) SYNC OFF",
        &empty,
    )
    .unwrap();
    let id = uuid::Uuid::from_u128(7);
    db.execute(
        "INSERT INTO generation (id, counter) VALUES ($id, $counter)",
        &HashMap::from([
            ("id".into(), Value::Uuid(id)),
            ("counter".into(), Value::Int64(0)),
        ]),
    )
    .unwrap();
    let row_id = db
        .row_id_for_natural_key_full(
            "generation",
            &NaturalKey::single("id".into(), Value::Uuid(id)),
            db.snapshot_for_read(),
            db.declared_reader_memory().unwrap(),
        )
        .unwrap()
        .expect("the committed durable row exists");
    let meta = db.table_meta("generation").unwrap();
    assert_eq!(
        db.writer_memory_ownership_receipt_for_test()
            .reconciliation_gap,
        0
    );

    let tx = db.begin().unwrap();
    for counter in 1..=3 {
        let values = HashMap::from([
            ("id".into(), Value::Uuid(id)),
            ("counter".into(), Value::Int64(counter)),
        ]);
        let admitted = db
            .admit_retained_row_bytes(
                "generation",
                &values,
                &meta,
                "update",
                "row_replace",
                "test replacement",
            )
            .unwrap();
        let (replaced, _, _) = db
            .replace_row_after_update_validation_counted(
                tx,
                "generation",
                row_id,
                values,
                UpdateReplacementContext {
                    meta: &meta,
                    committed_row_exists: true,
                    created_at: Wallclock::now(),
                },
            )
            .unwrap();
        db.record_staged_row_admission(tx, "generation", replaced, admitted);
        let owners = db.writer_memory_ownership_receipt_for_test();
        assert_eq!(owners.staged_row_versions, 1);
        assert_eq!(owners.reconciliation_gap, 0, "replacement {counter}");
    }
    db.commit(tx).unwrap();
    let owners = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(owners.staged_row_versions, 0);
    assert_eq!(owners.reconciliation_gap, 0);
}
