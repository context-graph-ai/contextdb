#![allow(clippy::cloned_ref_to_slice_refs)] // Existing compatibility and test seam shape.
use super::*;

#[test]
fn an_exact_refused_outcome_lookup_decodes_only_requested_values() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("outcomes.db")).expect("open durable store");
    let tenant = TenantId::from("outcome-page-tenant");
    let pushing_node = "page-author";
    let hub_node = "page-hub";
    let pushing_incarnation = Incarnation(11);
    let hub_incarnation = db.sync_incarnation(&tenant).expect("hub life");
    let prefix = Database::refused_schema_scope(
        &tenant,
        pushing_node,
        pushing_incarnation,
        hub_node,
        hub_incarnation,
    );
    let persistence = db.persistence.as_ref().expect("file-backed store");

    for ordinal in 0..40u64 {
        let outcome = crate::protocol::RefusedSchemaUnitOutcome {
            tenant_id: tenant.clone(),
            pushing_node_id: pushing_node.into(),
            pushing_incarnation,
            author_node_id: pushing_node.into(),
            author_incarnation: pushing_incarnation,
            source_ddl_lsn: Lsn(ordinal + 1),
            unit_digest: [ordinal as u8; 32],
            hub_node_id: hub_node.into(),
            hub_incarnation,
            table: "notes".into(),
            reason: "policy refused".into(),
        };
        persistence
            .flush_config_value(
                &format!("{prefix}{}", Database::hex_component(&outcome.unit_digest)),
                &outcome,
            )
            .expect("persist one earlier outcome");
    }
    persistence
        .flush_config_value(&format!("{prefix}{}", "f".repeat(64)), &"malformed outcome")
        .expect("persist a malformed later outcome");
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let (first, _lease) = db
        .refused_schema_unit_outcomes_for_digests(
            &tenant,
            pushing_node,
            pushing_incarnation,
            hub_node,
            &[[0; 32]],
            fixture_operation_reader_1.clone(),
        )
        .expect("a one-item page does not decode later records");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].source_ddl_lsn, Lsn(1));
    let fixture_operation_reader_2 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    assert!(
        db.refused_schema_unit_outcomes_for_digests(
            &tenant,
            pushing_node,
            pushing_incarnation,
            hub_node,
            &[[0xff; 32]],
            fixture_operation_reader_2.clone()
        )
        .is_err(),
        "the malformed record is reported when its exact key is requested"
    );
}

#[test]
fn replay_of_one_peer_does_not_decode_another_peers_malformed_held_payload() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("held.db")).expect("open durable store");
    let tenant = TenantId::from("held-replay-tenant");
    let requested_node = "peer-a";
    let unrelated_node = "peer-b";
    let incarnation = Incarnation(17);
    let unrelated_key = format!(
        "{}{}.{}.{}.{}",
        Database::POLICY_HELD_DATA_PREFIX,
        Database::hex_component(tenant.as_str().as_bytes()),
        Database::hex_component(unrelated_node.as_bytes()),
        incarnation.to_hex(),
        "f".repeat(64),
    );
    db.persistence
        .as_ref()
        .expect("file-backed store")
        .flush_config_value(&unrelated_key, &"malformed held data")
        .expect("persist another peer's malformed payload");
    let fixture_operation_reader_3 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_3 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_3.clone());

    let replayed = db
        .replay_released_policy_data(
            &tenant,
            requested_node,
            incarnation,
            &mut fixture_operation_copy_3,
            fixture_operation_reader_3.clone(),
        )
        .expect("a peer replays only its own durable pending data");
    assert_eq!(replayed.applied_rows, 0);
    assert_eq!(replayed.skipped_rows, 0);
    assert!(replayed.conflicts.is_empty());
    let fixture_operation_reader_4 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_4 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_4.clone());

    assert!(
        db.replay_released_policy_data(
            &tenant,
            unrelated_node,
            incarnation,
            &mut fixture_operation_copy_4,
            fixture_operation_reader_4.clone()
        )
        .is_err(),
        "the malformed payload is reported to the peer whose replay reaches it"
    );
}

#[test]
fn a_single_large_refused_outcome_is_admitted_before_decode() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("large-outcome.db")).expect("open durable store");
    let tenant = TenantId::from("large-outcome-tenant");
    let node = "author";
    let hub = "hub";
    let incarnation = Incarnation(31);
    let hub_incarnation = db.sync_incarnation(&tenant).expect("hub life");
    let prefix = Database::refused_schema_scope(&tenant, node, incarnation, hub, hub_incarnation);
    let key = format!("{prefix}{}", Database::hex_component(&[1; 32]));
    let outcome = crate::protocol::RefusedSchemaUnitOutcome {
        tenant_id: tenant.clone(),
        pushing_node_id: node.into(),
        pushing_incarnation: incarnation,
        author_node_id: node.into(),
        author_incarnation: incarnation,
        source_ddl_lsn: Lsn(1),
        unit_digest: [1; 32],
        hub_node_id: hub.into(),
        hub_incarnation,
        table: "notes".into(),
        reason: "refused".repeat(8192),
    };
    let persistence = db.persistence.as_ref().expect("file-backed store");
    persistence
        .flush_config_value(&key, &outcome)
        .expect("persist one valid large outcome");
    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used.saturating_add(4096)))
        .expect("leave less room than the encoded outcome needs");
    let fixture_operation_reader_5 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let refused = match db.refused_schema_unit_outcomes_for_digests(
        &tenant,
        node,
        incarnation,
        hub,
        &[[1; 32]],
        fixture_operation_reader_5.clone(),
    ) {
        Ok(_) => panic!("the encoded value must be admitted before decoding"),
        Err(refused) => refused,
    };
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "the refusal is typed: {refused}"
    );
    assert!(
        persistence
            .load_config_value::<crate::protocol::RefusedSchemaUnitOutcome>(&key)
            .expect("the durable outcome remains readable")
            .is_some(),
        "memory refusal leaves the durable outcome intact"
    );
    db.set_memory_limit(None).expect("lift the budget");
    let fixture_operation_reader_6 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let (page, lease) = db
        .refused_schema_unit_outcomes_for_digests(
            &tenant,
            node,
            incarnation,
            hub,
            &[[1; 32]],
            fixture_operation_reader_6.clone(),
        )
        .expect("read after lifting the budget");
    assert!(
        db.accountant().usage().used > used,
        "the response holds its decoded charge"
    );
    assert_eq!(page, vec![outcome]);
    drop(page);
    drop(lease);
    assert_eq!(
        db.accountant().usage().used,
        used,
        "the response releases its charge"
    );
}

#[test]
fn a_large_local_refusal_fact_is_admitted_before_decode_and_survives_refusal() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("local-fact.db")).expect("open durable store");
    let tenant = TenantId::from("local-fact-tenant");
    let edge = "author";
    let hub = "hub";
    let edge_life = Incarnation(31);
    let hub_life = Incarnation(41);
    let outcome = crate::protocol::RefusedSchemaUnitOutcome {
        tenant_id: tenant.clone(),
        pushing_node_id: edge.into(),
        pushing_incarnation: edge_life,
        author_node_id: edge.into(),
        author_incarnation: edge_life,
        source_ddl_lsn: Lsn(1),
        unit_digest: [1; 32],
        hub_node_id: hub.into(),
        hub_incarnation: hub_life,
        table: "notes".into(),
        reason: "refused".repeat(8192),
    };
    db.persist_refused_schema_local_facts(
        &tenant,
        edge,
        edge_life,
        hub,
        hub_life,
        &[outcome.clone()],
    )
    .expect("persist the exact per-unit fact");
    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used.saturating_add(4096)))
        .expect("leave less room than the encoded fact needs");
    let refused = db
        .refused_schema_local_facts(
            &tenant,
            edge,
            edge_life,
            hub,
            hub_life,
            &[[1; 32]],
            db.declared_reader_memory()
                .expect("admit the actual fixture reader"),
        )
        .expect_err("the encoded fact must be admitted before decoding");
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "{refused}"
    );
    assert_eq!(
        db.accountant().usage().used,
        used,
        "the failed read releases its charge"
    );
    db.set_memory_limit(None).expect("lift the budget");
    let (facts, lease) = db
        .refused_schema_local_facts(
            &tenant,
            edge,
            edge_life,
            hub,
            hub_life,
            &[[1; 32]],
            db.declared_reader_memory()
                .expect("admit the actual fixture reader"),
        )
        .expect("read the retained fact");
    assert!(
        db.accountant().usage().used > used,
        "decoded facts retain their charge"
    );
    assert_eq!(facts, vec![outcome]);
    drop(facts);
    drop(lease);
    assert_eq!(
        db.accountant().usage().used,
        used,
        "dropping the result releases its charge"
    );
}

#[test]
fn a_large_held_replay_is_refused_without_removing_its_pending_record() {
    let root = tempfile::tempdir().expect("task-scoped durable store");
    let db = Database::open(root.path().join("large-held.db")).expect("open durable store");
    let tenant = TenantId::from("large-held-tenant");
    let node = "held-author";
    let incarnation = Incarnation(32);
    let key = format!(
        "{}{}.{}.{}.{}",
        Database::POLICY_HELD_DATA_PREFIX,
        Database::hex_component(tenant.as_str().as_bytes()),
        Database::hex_component(node.as_bytes()),
        incarnation.to_hex(),
        "a".repeat(64),
    );
    let held = DurablePolicyHeldData {
        changes: ChangeSet::default(),
        arrivals: HashMap::new(),
        receipt: SyncApplyReceipt {
            tenant_id: tenant.clone(),
            node_id: node.into(),
            incarnation,
            source_lsn: Lsn(1),
            dependency_complete: false,
            hub_contact_ms: None,
        },
        hub_node_id: "hub".repeat(16384),
        lineages: Vec::new(),
        outcome_key: SyncPushOutcomeKey {
            storage_key: format!("{SYNC_PUSH_OUTCOME_PREFIX}policy_replay.{key}"),
            incarnation,
            request_digest: [1; 32],
            diagnostic_identities: Vec::new(),
        },
        held_tables: BTreeSet::new(),
    };
    let persistence = db.persistence.as_ref().expect("file-backed store");
    persistence
        .flush_config_value(&key, &held)
        .expect("persist one valid large held unit");
    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used.saturating_add(4096)))
        .expect("leave less room than the encoded held unit needs");
    let fixture_operation_reader_7 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_7 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_7.clone());

    let refused = match db.replay_released_policy_data(
        &tenant,
        node,
        incarnation,
        &mut fixture_operation_copy_7,
        fixture_operation_reader_7.clone(),
    ) {
        Ok(_) => panic!("the encoded held unit must be admitted before replay"),
        Err(refused) => refused,
    };
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "the refusal is typed: {refused}"
    );
    assert!(
        persistence
            .load_config_value::<DurablePolicyHeldData>(&key)
            .expect("the durable pending unit remains readable")
            .is_some(),
        "memory refusal leaves the pending obligation intact"
    );
}
