use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests::measured;
use serial_test::serial;

const TENANT: &str = "small-delivery-tenant";
const SOURCE: &str = "small-delivery-source";
const TABLE: &str = "pending_notes";
const INCARNATION: Incarnation = Incarnation(17);

fn hold(tenant: &str, source: &str, refusal: String) -> DurablePolicyHold {
    DurablePolicyHold {
        source_node_id: source.into(),
        tenant_id: Some(TenantId::from(tenant)),
        source_incarnation: Some(INCARNATION),
        author_node_id: Some(source.into()),
        source_lsn: Lsn(1),
        table: TABLE.into(),
        refusal,
        table_absent: false,
        dag_edge_types: Vec::new(),
    }
}

fn store_hold(db: &Database, hold: &DurablePolicyHold) {
    let key = Database::policy_hold_storage_key(hold).expect("hold address");
    db.persistence
        .as_ref()
        .expect("a durable test store")
        .flush_config_value(&key, hold)
        .expect("persist hold");
}

fn create_held_table(db: &Database) {
    db.execute(
        "CREATE TABLE pending_notes (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("declare the existing table whose peer migration was refused");
}

fn one_small_delivery() -> ChangeSet {
    ChangeSet {
        rows: vec![RowChange {
            table: TABLE.into(),
            natural_key: NaturalKey::single("id".into(), Value::Int64(1)),
            values: HashMap::from([("id".into(), Value::Int64(1))]),
            deleted: false,
            lsn: Lsn(2),
            created_at: None,
        }],
        ..ChangeSet::default()
    }
}

fn split_one(db: &Database) -> Result<(ChangeSet, Vec<Conflict>)> {
    db.split_data_held_behind_refused_units(
        one_small_delivery(),
        &TenantId::from(TENANT),
        SOURCE,
        Some(INCARNATION),
        false,
    )
}

#[test]
#[serial]
fn unrelated_tenants_held_history_does_not_expand_one_deliverys_decode_or_retained_peak() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db =
        Database::open(root.path().join("scoped-policy-holds.db")).expect("open durable store");
    create_held_table(&db);
    store_hold(&db, &hold(TENANT, SOURCE, "relevant refusal".into()));

    let accountant = db.accountant();
    let admitted_before = accountant.admitted_bytes_for_test();
    let (small, small_peak, small_retained) = measured(|| split_one(&db));
    let small_admitted = accountant.admitted_bytes_for_test() - admitted_before;
    let (filtered, conflicts) = small.expect("a relevant hold filters the delivery");
    assert!(filtered.rows.is_empty(), "the relevant hold still applies");
    assert_eq!(conflicts.len(), 1, "one row has one hold conflict");
    assert!(
        conflicts[0]
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("relevant refusal"),
        "the conflict names the relevant refusal: {conflicts:?}"
    );
    drop(filtered);
    drop(conflicts);

    // These hold values are valid and much larger in total than the one
    // delivery. Their tenant and source identities cannot affect its policy.
    for index in 0..96 {
        let unrelated_tenant = format!("other-tenant-{index:03}");
        let unrelated_source = format!("other-source-{index:03}");
        store_hold(
            &db,
            &hold(
                if index % 2 == 0 {
                    &unrelated_tenant
                } else {
                    TENANT
                },
                if index % 2 == 0 {
                    SOURCE
                } else {
                    &unrelated_source
                },
                "unrelated history".repeat(1024),
            ),
        );
    }

    let admitted_before = accountant.admitted_bytes_for_test();
    let used_before = accountant.usage().used;
    let (large, large_peak, large_retained) = measured(|| split_one(&db));
    let large_admitted = accountant.admitted_bytes_for_test() - admitted_before;
    let (filtered, conflicts) = large.expect("other tenants do not affect the delivery");
    assert!(filtered.rows.is_empty());
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        accountant.usage().used,
        used_before,
        "temporary charges return"
    );
    assert!(
        large_peak <= small_peak.saturating_add(128 * 1024),
        "actual peak grew with another tenant: small={small_peak}, large={large_peak}"
    );
    assert!(
        large_retained <= small_retained.saturating_add(16 * 1024),
        "retained allocations grew with another tenant: small={small_retained}, large={large_retained}"
    );
    assert!(small_admitted > 0, "the relevant decode must be admitted");
    assert!(
        large_admitted <= small_admitted.saturating_add(16 * 1024),
        "decoded and retained bytes grew with another tenant: small={small_admitted}, large={large_admitted}"
    );
}

#[test]
#[serial]
fn oversized_relevant_hold_refuses_before_decode_and_returns_every_charge() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("large-policy-hold.db")).expect("open durable store");
    create_held_table(&db);
    let relevant = hold(TENANT, SOURCE, "oversized refusal".repeat(65536));
    let key = Database::policy_hold_storage_key(&relevant).expect("hold address");
    store_hold(&db, &relevant);
    let used_before = db.accountant().usage().used;
    db.set_memory_limit(Some(used_before.saturating_add(32 * 1024)))
        .expect("leave less room than the encoded hold needs");
    let admitted_before = db.accountant().admitted_bytes_for_test();
    let used_before = db.accountant().usage().used;

    let (result, actual_peak, retained) = measured(|| split_one(&db));
    let refusal = match result {
        Ok(_) => panic!("the oversized relevant hold must be refused"),
        Err(refusal) => refusal,
    };
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "refusal is typed: {refusal}"
    );
    assert!(
        actual_peak < 256 * 1024,
        "the oversized value was decoded or copied before refusal: peak={actual_peak}"
    );
    assert!(
        retained < 32 * 1024,
        "the refusal retained decoded hold memory: retained={retained}"
    );
    assert_eq!(db.accountant().usage().used, used_before, "charges return");
    assert!(
        db.accountant().admitted_bytes_for_test() - admitted_before < 32 * 1024,
        "the oversized value was not admitted before refusal"
    );
    db.set_memory_limit(None).expect("lift the budget");
    assert!(
        db.persistence
            .as_ref()
            .expect("durable store")
            .load_config_value::<DurablePolicyHold>(&key)
            .expect("the hold remains readable")
            .is_some(),
        "memory refusal preserves the pending hold"
    );
    let (filtered, conflicts) = split_one(&db).expect("retry with sufficient memory");
    assert!(filtered.rows.is_empty());
    assert_eq!(conflicts.len(), 1);
}

fn edge_delivery() -> ChangeSet {
    ChangeSet {
        edges: vec![EdgeChange {
            source: uuid::Uuid::from_u128(1),
            target: uuid::Uuid::from_u128(2),
            edge_type: "refused_edge".into(),
            properties: HashMap::new(),
            lsn: Lsn(2),
        }],
        ..ChangeSet::default()
    }
}

#[test]
#[serial]
fn edge_only_selection_keeps_absent_ownership_across_sources_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("edges.db");
    let db = Database::open(&path).unwrap();
    let mut relevant = hold(TENANT, SOURCE, "creation refused".into());
    relevant.table_absent = true;
    relevant.dag_edge_types.push("refused_edge".into());
    db.record_policy_holds(&[relevant.clone()]).unwrap();
    let mut irrelevant = relevant.clone();
    irrelevant.tenant_id = Some(TenantId::from("another-tenant"));
    db.record_policy_holds(&[irrelevant.clone()]).unwrap();
    let irrelevant_key = Database::policy_hold_storage_key(&irrelevant).unwrap();
    // A damaged unrelated payload must not be decoded by delivery or by
    // reopening a store whose selectors already committed with its records.
    db.persistence
        .as_ref()
        .unwrap()
        .flush_encoded_config_values(vec![(&irrelevant_key, vec![255])])
        .unwrap();
    db.close().unwrap();
    let db = Database::open(&path).unwrap();
    let (filtered, conflicts) = db
        .split_data_held_behind_refused_units(
            edge_delivery(),
            &TenantId::from(TENANT),
            "different-peer",
            Some(Incarnation(29)),
            false,
        )
        .unwrap();
    assert!(filtered.edges.is_empty());
    assert_eq!(conflicts.len(), 1);
    assert!(
        conflicts[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("creation refused")
    );
    let (allowed, conflicts) = db
        .split_data_held_behind_refused_units(
            edge_delivery(),
            &TenantId::from("tenant-with-no-hold"),
            SOURCE,
            Some(INCARNATION),
            false,
        )
        .unwrap();
    assert_eq!(allowed.edges.len(), 1);
    assert!(conflicts.is_empty());
}

#[test]
#[serial]
fn selected_holds_keep_their_memory_charge_until_the_collection_is_dropped() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(root.path().join("lease.db")).unwrap();
    create_held_table(&db);
    db.record_policy_holds(&[hold(TENANT, SOURCE, "retained".repeat(1024))])
        .unwrap();
    let before = db.accountant.usage().used;
    let selected = db
        .scoped_policy_hold_entries(
            Some(SOURCE),
            Some((&TenantId::from(TENANT), INCARNATION)),
            &BTreeSet::from([TABLE.into()]),
            &BTreeSet::new(),
        )
        .unwrap();
    assert_eq!(selected.entries.len(), 1);
    assert!(db.accountant.usage().used >= before + 8192);
    drop(selected);
    assert_eq!(db.accountant.usage().used, before);
}

#[test]
#[serial]
fn unrelated_absent_table_holds_do_not_expand_a_small_delivery() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(root.path().join("absent-budget.db")).unwrap();
    let mut relevant = hold(TENANT, SOURCE, "creation refused".into());
    relevant.table_absent = true;
    store_hold(&db, &relevant);
    let (first, small_peak, small_retained) = measured(|| split_one(&db));
    assert!(first.unwrap().0.rows.is_empty());
    for index in 0..96 {
        let mut unrelated = hold(
            &format!("other-tenant-{index:03}"),
            SOURCE,
            "unrelated".repeat(2048),
        );
        unrelated.table_absent = true;
        store_hold(&db, &unrelated);
    }
    let (second, large_peak, large_retained) = measured(|| split_one(&db));
    let (changes, conflicts) = second.unwrap();
    assert!(changes.rows.is_empty());
    assert_eq!(conflicts.len(), 1);
    assert!(
        large_peak <= small_peak + 128 * 1024,
        "{small_peak} -> {large_peak}"
    );
    assert!(
        large_retained <= small_retained + 16 * 1024,
        "{small_retained} -> {large_retained}"
    );
}
