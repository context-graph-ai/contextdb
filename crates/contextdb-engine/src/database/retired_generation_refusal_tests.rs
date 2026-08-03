use super::*;

#[test]
fn future_generation_is_not_acknowledged_as_a_retired_generation() {
    let db = Database::open_memory();
    let tenant = TenantId::from("future-generation-refusal");
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("create notes");
    db.execute(
        "INSERT INTO notes (id, body) VALUES (1, 'future generation')",
        &HashMap::new(),
    )
    .expect("insert source row");
    let row = db
        .changes_since(Lsn(0))
        .rows
        .into_iter()
        .next()
        .expect("insert has one outbound row");
    let current_generation = db
        .durable_lineage_table_generation("notes")
        .expect("read current table generation");
    let future_generation = current_generation.saturating_add(1);
    let identity = crate::identity::FabricIdentity::generate();
    let author_node_id = identity.node_id();
    let incarnation = Incarnation(41);
    let position = row.lsn;
    let lineage_root = format!(
        "author:{author_node_id}:{}:{}",
        incarnation.to_hex(),
        position.0
    );
    let attestation = Database::lineage_attestation_bytes(
        &tenant,
        &row.table,
        &row.natural_key,
        future_generation,
        &author_node_id,
        incarnation,
        position,
        &lineage_root,
    )
    .expect("encode signed future-generation lineage");
    let lineage = crate::protocol::WireRowLineage {
        author_node_id: author_node_id.clone(),
        author_database_incarnation: incarnation,
        author_local_mutation_position: position,
        table_generation: future_generation,
        lineage_root,
        attestation: identity.sign_lineage(&attestation),
    };
    let changes = ChangeSet {
        rows: vec![row.clone()],
        ..ChangeSet::default()
    };
    let lineages = vec![(row.table.clone(), row.natural_key.clone(), row.lsn, lineage)];
    let lsn_before = db.current_lsn();

    assert!(
        db.retired_generation_refusals(&tenant, &changes, &lineages, &author_node_id, incarnation,)
            .expect("future generation is structurally authenticated")
            .is_empty(),
        "only generations older than the receiver's table can be terminally retired"
    );
    let error = db
        .validate_incoming_push_lineages(&tenant, &changes, &lineages, &author_node_id, incarnation)
        .expect_err("future generation must reach the normal fail-closed projection check");
    assert!(
        error.to_string().contains("generation"),
        "future generation is a mismatch, not an acknowledged terminal refusal: {error}"
    );
    assert_eq!(
        db.current_lsn(),
        lsn_before,
        "a future generation cannot create a terminal receipt or other receiver mutation"
    );
}
