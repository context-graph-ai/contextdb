use contextdb_core::{Error, Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::identity::FabricIdentity;
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;

const TABLE: &str = "relayed_notes";
const TENANT: &str = "unattested-schema-forwarding";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn policies() -> ConflictPolicies {
    ConflictPolicies::uniform(ConflictPolicy::LatestWins)
}

fn table_unit(changes: &ChangeSet, table: &str) -> ChangeSet {
    let index = changes
        .ddl
        .iter()
        .position(|entry| matches!(entry, DdlChange::CreateTable { name, .. } if name == table))
        .unwrap_or_else(|| panic!("schema history must contain CREATE TABLE {table}"));
    ChangeSet {
        ddl: vec![changes.ddl[index].clone()],
        ddl_lsn: vec![changes.ddl_lsn[index]],
        ..Default::default()
    }
}

fn received_then_forwarded() -> (Database, ChangeSet, FabricIdentity, Vec<WireDdlProvenance>) {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE relayed_notes (id INTEGER PRIMARY KEY, body TEXT)",
            &empty(),
        )
        .expect("author the source declaration");
    let source_unit = table_unit(
        &source
            .changes_since(Lsn(0))
            .expect("read or construct the declared fixture result"),
        TABLE,
    );

    let forwarder = Database::open_memory();
    forwarder
        .apply_changes(source_unit, &policies())
        .expect("receive the declaration through the unattested door");
    assert!(
        forwarder.table_meta(TABLE).is_some(),
        "the forwarder must actually hold the received declaration"
    );
    let forwarded = table_unit(
        &forwarder
            .changes_since(Lsn(0))
            .expect("read or construct the declared fixture result"),
        TABLE,
    );
    let identity = FabricIdentity::generate();
    let fixture_operation_reader_1 = forwarder
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = forwarder
        .outbound_ddl_provenance_for_test(
            &forwarded,
            &TenantId::from(TENANT),
            &identity.node_id(),
            Incarnation(1),
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .expect("serve the declaration the peer received");
    assert_eq!(provenance.len(), 1);
    (forwarder, forwarded, identity, provenance)
}

#[test]
fn forwarded_unattested_schema_carries_an_explicit_marker_and_no_author_signature() {
    let (_, _, _, provenance) = received_then_forwarded();
    let entry = &provenance[0];
    assert_eq!(entry.table.as_deref(), Some(TABLE));
    assert!(
        entry.author.is_none(),
        "a forwarding peer cannot claim it authored schema received without attestation: {entry:?}"
    );
    let wire = serde_json::to_string(entry).expect("encode the forwarded provenance");
    assert!(
        wire.to_ascii_lowercase().contains("unattested"),
        "the wire must explicitly mark this provenance as unattested: {wire}"
    );
}

#[test]
fn authenticated_receiver_refuses_forwarded_unattested_schema_with_its_table() {
    let (_, unit, identity, provenance) = received_then_forwarded();
    let receiver = Database::open_memory();
    let fixture_operation_reader_2 = receiver
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    let error = receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            TenantId::from(TENANT),
            &identity.node_id(),
            Incarnation(1),
            &mut fixture_operation_copy_2,
            fixture_operation_reader_2.clone(),
        )
        .expect_err("authenticated schema cannot accept an unattested declaration");
    assert!(
        matches!(error, Error::ReceivedSchemaAuthorshipUnavailable { ref table, .. } if table == TABLE),
        "the refusal must have a typed table identity: {error:?}"
    );
    assert!(receiver.table_meta(TABLE).is_none());
}

#[test]
fn unattested_receiver_still_installs_forwarded_unattested_schema() {
    let (_, unit, _, _) = received_then_forwarded();
    let receiver = Database::open_memory();
    receiver
        .apply_changes(unit, &policies())
        .expect("the unattested door accepts this declaration in its own trust class");
    assert!(receiver.table_meta(TABLE).is_some());
}

#[test]
fn forwarders_own_locally_authored_schema_remains_signed() {
    let (forwarder, _, identity, _) = received_then_forwarded();
    const LOCAL: &str = "forwarder_ledger";
    forwarder
        .execute(
            "CREATE TABLE forwarder_ledger (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect("author a separate local declaration on the forwarding peer");
    let local = table_unit(
        &forwarder
            .changes_since(Lsn(0))
            .expect("read or construct the declared fixture result"),
        LOCAL,
    );
    let fixture_operation_reader_3 = forwarder
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = forwarder
        .outbound_ddl_provenance_for_test(
            &local,
            &TenantId::from(TENANT),
            &identity.node_id(),
            Incarnation(1),
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_3.clone(),
        )
        .expect("serve the peer's own declaration");
    let author = provenance[0]
        .author
        .as_ref()
        .expect("locally authored schema keeps an author signature");
    assert_eq!(author.node_id, identity.node_id());
    assert!(!author.attestation.is_empty());
    let receiver = Database::open_memory();
    let fixture_operation_reader_4 = receiver
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_4 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_4.clone());

    receiver
        .apply_received_schema_for_test(
            local,
            provenance,
            TenantId::from(TENANT),
            &identity.node_id(),
            Incarnation(1),
            &mut fixture_operation_copy_4,
            fixture_operation_reader_4.clone(),
        )
        .expect("an authenticated receiver verifies the forwarder's own declaration");
    assert!(receiver.table_meta(LOCAL).is_some());
}
