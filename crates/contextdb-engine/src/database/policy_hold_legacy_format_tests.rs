use super::*;

// Serialize-only snapshots of the durable layouts before dependency metadata
// was appended. They deliberately cannot decode on behalf of the new types.
#[derive(Serialize)]
struct LegacyHold<'a> {
    source_node_id: &'a str,
    tenant_id: &'a Option<TenantId>,
    source_incarnation: &'a Option<Incarnation>,
    author_node_id: &'a Option<String>,
    source_lsn: Lsn,
    table: &'a str,
    refusal: &'a str,
    table_absent: bool,
}

#[derive(Serialize)]
struct LegacyHeldData<'a> {
    changes: &'a ChangeSet,
    arrivals: &'a HashMap<Lsn, Option<Lsn>>,
    receipt: &'a SyncApplyReceipt,
    hub_node_id: &'a str,
    lineages: &'a Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>,
    outcome_key: &'a SyncPushOutcomeKey,
}

#[test]
fn old_policy_hold_tuple_and_map_decode_with_empty_edge_types_and_stable_release_key() {
    let hold = DurablePolicyHold {
        source_node_id: "source".into(),
        tenant_id: Some(TenantId::from("tenant")),
        source_incarnation: Some(Incarnation(17)),
        author_node_id: Some("author".into()),
        source_lsn: Lsn(23),
        table: "edges".into(),
        refusal: "policy refusal".into(),
        table_absent: true,
        dag_edge_types: Vec::new(),
    };
    let old = LegacyHold {
        source_node_id: &hold.source_node_id,
        tenant_id: &hold.tenant_id,
        source_incarnation: &hold.source_incarnation,
        author_node_id: &hold.author_node_id,
        source_lsn: hold.source_lsn,
        table: &hold.table,
        refusal: &hold.refusal,
        table_absent: hold.table_absent,
    };
    let tuple = rmp_serde::to_vec(&old).expect("old hold tuple");
    let map = rmp_serde::to_vec_named(&old).expect("old hold map");
    for bytes in [&tuple, &map] {
        let decoded: DurablePolicyHold =
            rmp_serde::from_slice(bytes).expect("old hold layout remains readable");
        assert_eq!(decoded, hold);
        assert!(decoded.dag_edge_types.is_empty());
    }
    let old_digest = blake3::hash(&tuple).to_hex().to_string();
    assert_eq!(
        Database::policy_hold_release_key(&hold).expect("release key"),
        format!("{}{old_digest}", Database::POLICY_HOLD_RELEASE_PREFIX)
    );
}

#[test]
fn old_held_data_tuple_and_map_decode_with_empty_held_tables() {
    let changes = ChangeSet::default();
    let arrivals = HashMap::from([(Lsn(23), Some(Lsn(29)))]);
    let receipt = SyncApplyReceipt {
        tenant_id: TenantId::from("tenant"),
        node_id: "source".into(),
        incarnation: Incarnation(17),
        source_lsn: Lsn(23),
        dependency_complete: true,
        hub_contact_ms: Some(1234),
    };
    let lineages = Vec::new();
    let outcome_key = SyncPushOutcomeKey {
        storage_key: "legacy outcome".into(),
        incarnation: Incarnation(17),
        request_digest: [7; 32],
        diagnostic_identities: Vec::new(),
    };
    let old = LegacyHeldData {
        changes: &changes,
        arrivals: &arrivals,
        receipt: &receipt,
        hub_node_id: "hub",
        lineages: &lineages,
        outcome_key: &outcome_key,
    };
    for bytes in [
        rmp_serde::to_vec(&old).expect("old held data tuple"),
        rmp_serde::to_vec_named(&old).expect("old held data map"),
    ] {
        let decoded: DurablePolicyHeldData =
            rmp_serde::from_slice(&bytes).expect("old held data layout remains readable");
        assert_eq!(decoded.arrivals, arrivals);
        assert_eq!(decoded.receipt.source_lsn, receipt.source_lsn);
        assert_eq!(
            decoded.outcome_key.request_digest,
            outcome_key.request_digest
        );
        assert!(decoded.changes.is_empty());
        assert!(decoded.lineages.is_empty());
        assert!(decoded.held_tables.is_empty());
    }
}
