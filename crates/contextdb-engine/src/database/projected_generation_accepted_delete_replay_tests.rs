use super::*;
use crate::identity::FabricIdentity;
use std::collections::HashMap;

const TABLE: &str = "events";
const PROJECTED_GENERATION: u64 = 3;

fn projected_generations() -> HashMap<String, u64> {
    HashMap::from([(TABLE.to_string(), PROJECTED_GENERATION)])
}

fn live_row(key: NaturalKey, lsn: Lsn) -> RowChange {
    RowChange {
        table: TABLE.to_string(),
        natural_key: key,
        values: HashMap::from([("id".to_string(), Value::Int64(1))]),
        deleted: false,
        lsn,
        created_at: None,
    }
}

fn authenticated_lineage(
    tenant: &TenantId,
    key: &NaturalKey,
    position: u64,
    identity: &FabricIdentity,
) -> crate::protocol::WireRowLineage {
    let author_node_id = identity.node_id();
    let author_database_incarnation = Incarnation(7);
    let lineage_root = format!(
        "author:{author_node_id}:{}:{position}",
        author_database_incarnation.to_hex()
    );
    let attestation = identity.sign_lineage(
        &Database::lineage_attestation_bytes(
            tenant,
            TABLE,
            key,
            PROJECTED_GENERATION,
            &author_node_id,
            author_database_incarnation,
            Lsn(position),
            &lineage_root,
        )
        .expect("canonical lineage attestation bytes"),
    );
    crate::protocol::WireRowLineage {
        author_node_id,
        author_database_incarnation,
        author_local_mutation_position: Lsn(position),
        table_generation: PROJECTED_GENERATION,
        lineage_root,
        attestation,
    }
}

fn accepted_delete_record(
    receiver: &Database,
    key: &NaturalKey,
    lineage: &crate::protocol::WireRowLineage,
) {
    let record = DurableLineageRecord {
        table: TABLE.to_string(),
        natural_key: key.clone(),
        table_generation: PROJECTED_GENERATION,
        local_row_id: None,
        locally_created: false,
        lineage_root: lineage.lineage_root.clone(),
        lineage_attestation: lineage.attestation.clone(),
        author_node_id: Some(lineage.author_node_id.clone()),
        author_database_incarnation: Some(lineage.author_database_incarnation.to_hex()),
        author_local_mutation_position: lineage.author_local_mutation_position.0,
        delete_lsn: 4,
        delete_obligation: DurableDeleteObligation::Accepted,
        accepted_hub_lsn: Some(4),
        bound_hub_node_id: Some("hub".to_string()),
        purge_frontier: None,
    };
    let mut state = receiver.lineage_state_lock.lock();
    receiver
        .store_lineage_record(
            &mut state,
            &Database::durable_lineage_config_key(TABLE, key, PROJECTED_GENERATION),
            &record,
        )
        .expect("store exact accepted-delete record");
}

fn assert_receiver_has_no_durable_generation(receiver: &Database) {
    assert!(receiver.table_meta(TABLE).is_none());
    assert!(
        receiver.durable_lineage_table_generation(TABLE).is_err(),
        "the receiver has no installed durable schema generation"
    );
}

#[test]
fn drop_accepted_lineage_replays_against_generation_projection() {
    let tenant = TenantId::from("projected-generation-ordinary");
    let receiver = Database::open_memory();
    let identity = FabricIdentity::generate();
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let row = live_row(key.clone(), Lsn(19));
    let lineage = authenticated_lineage(&tenant, &key, 13, &identity);
    let lineages = vec![(TABLE.to_string(), key.clone(), row.lsn, lineage.clone())];

    assert_receiver_has_no_durable_generation(&receiver);
    let retained = receiver
        .drop_accepted_lineage_replays_against_generation_projection(
            &tenant,
            ChangeSet {
                rows: vec![row.clone()],
                ..ChangeSet::default()
            },
            &lineages,
            &projected_generations(),
        )
        .expect("a valid projected live row without an accepted delete remains");
    assert_eq!(retained.changes.rows, vec![row.clone()]);
    assert!(!retained.suppressed_live_replay);

    accepted_delete_record(&receiver, &key, &lineage);
    let suppressed = receiver
        .drop_accepted_lineage_replays_against_generation_projection(
            &tenant,
            ChangeSet {
                rows: vec![row],
                ..ChangeSet::default()
            },
            &lineages,
            &projected_generations(),
        )
        .expect("an exact accepted delete suppresses its projected replay");
    assert!(suppressed.changes.rows.is_empty());
    assert!(suppressed.suppressed_live_replay);
}

#[test]
fn reject_dependency_complete_accepted_lineage_replays_against_generation_projection() {
    let tenant = TenantId::from("projected-generation-dependency");
    let receiver = Database::open_memory();
    let identity = FabricIdentity::generate();
    let key = NaturalKey::single("id".to_string(), Value::Int64(1));
    let row = live_row(key.clone(), Lsn(29));
    let lineage = authenticated_lineage(&tenant, &key, 23, &identity);
    let lineages = vec![(TABLE.to_string(), key.clone(), row.lsn, lineage.clone())];

    assert_receiver_has_no_durable_generation(&receiver);
    let retained = receiver
        .reject_dependency_complete_accepted_lineage_replays_against_generation_projection(
            &tenant,
            ChangeSet {
                rows: vec![row.clone()],
                ..ChangeSet::default()
            },
            &lineages,
            &projected_generations(),
        )
        .expect("a valid projected dependency member without an accepted delete remains");
    assert_eq!(retained.changes.rows, vec![row.clone()]);
    assert!(!retained.suppressed_live_replay);

    accepted_delete_record(&receiver, &key, &lineage);
    let sibling_key = NaturalKey::single("id".to_string(), Value::Int64(2));
    let sibling = live_row(sibling_key.clone(), Lsn(30));
    let sibling_lineage = authenticated_lineage(&tenant, &sibling_key, 24, &identity);
    let error = receiver
        .reject_dependency_complete_accepted_lineage_replays_against_generation_projection(
            &tenant,
            ChangeSet {
                rows: vec![row, sibling],
                ..ChangeSet::default()
            },
            &[
                (TABLE.to_string(), key, Lsn(29), lineage),
                (TABLE.to_string(), sibling_key, Lsn(30), sibling_lineage),
            ],
            &projected_generations(),
        )
        .err()
        .expect("an accepted-delete replay rejects the complete dependency unit");
    let message = error.to_string();
    assert!(
        message.contains("replays a lineage the hub already terminated by an accepted delete"),
        "the terminated member rejects the entire unit instead of returning its sibling: {message}"
    );
    assert!(
        message.contains("store already agrees"),
        "the refusal must state benign convergence, not just that it terminated: {message}"
    );
}
