use contextdb_core::{Error, Incarnation, TenantId};
use contextdb_engine::Database;
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::sync_types::{ChangeSet, DdlChange};
use std::collections::HashMap;

const TENANT: &str = "received-column-rename-identity";
const INCARNATION: Incarnation = Incarnation(1);

fn signed_rename(author: &Database) -> (ChangeSet, Vec<WireDdlProvenance>, String) {
    let before = author.current_lsn();
    author
        .execute("ALTER TABLE items RENAME COLUMN x TO y", &HashMap::new())
        .unwrap();
    let changes = author
        .changes_since(before)
        .expect("read or construct the declared fixture result");
    assert_eq!(changes.ddl.len(), 1);
    assert!(matches!(
        &changes.ddl[0],
        DdlChange::AlterTable { constraints, .. }
            if constraints.iter().any(|marker| marker == "COLUMN_RENAME(x,y)")
    ));
    let identity = contextdb_engine::identity::FabricIdentity::generate();
    let node_id = identity.node_id();
    let fixture_operation_reader_1 = author
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let provenance = author
        .outbound_ddl_provenance_for_test(
            &changes,
            &TenantId::from(TENANT),
            &node_id,
            INCARNATION,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .unwrap();
    (changes, provenance, node_id)
}

fn receive(
    receiver: &Database,
    changes: ChangeSet,
    provenance: Vec<WireDdlProvenance>,
    node_id: &str,
) -> contextdb_core::Result<contextdb_engine::sync_types::ApplyResult> {
    let fixture_operation_reader_2 = receiver
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    receiver.apply_received_schema_for_test(
        changes,
        provenance,
        TenantId::from(TENANT),
        node_id,
        INCARNATION,
        &mut fixture_operation_copy_2,
        fixture_operation_reader_2.clone(),
    )
}

#[test]
fn a_signed_rename_cannot_be_satisfied_by_an_independent_final_column_name() {
    let author = Database::open_memory();
    let receiver = Database::open_memory();
    author
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, x TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    receiver
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, y TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    let (changes, provenance, node_id) = signed_rename(&author);
    let before = receiver
        .changes_since(contextdb_core::Lsn(0))
        .expect("read or construct the declared fixture result")
        .ddl;

    let outcome = receive(&receiver, changes.clone(), provenance.clone(), &node_id);
    assert!(
        matches!(outcome, Err(Error::SchemaInvalid { .. })),
        "a rename with no source column must receive a typed schema refusal: {outcome:?}"
    );
    assert_eq!(
        receiver
            .changes_since(contextdb_core::Lsn(0))
            .expect("read or construct the declared fixture result")
            .ddl,
        before
    );
    assert_eq!(receiver.table_meta("items").unwrap().columns[1].name, "y");
    let retry = receive(&receiver, changes, provenance, &node_id);
    assert!(matches!(retry, Err(Error::SchemaInvalid { .. })));
}

#[test]
fn the_same_signed_rename_replays_once_by_its_recorded_identity() {
    let author = Database::open_memory();
    let receiver = Database::open_memory();
    for db in [&author, &receiver] {
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, x TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    }
    let (changes, provenance, node_id) = signed_rename(&author);
    let first = receive(&receiver, changes.clone(), provenance.clone(), &node_id).unwrap();
    let after_first = receiver
        .changes_since(contextdb_core::Lsn(0))
        .expect("read or construct the declared fixture result")
        .ddl;
    assert!(after_first.iter().any(|entry| matches!(
        entry,
        DdlChange::AlterTable { constraints, .. }
            if constraints.iter().any(|marker| marker == "COLUMN_RENAME(x,y)")
    )));
    let second = receive(&receiver, changes, provenance, &node_id).unwrap();
    assert_eq!(second.applied_rows, first.applied_rows);
    assert_eq!(second.new_lsn, first.new_lsn);
    assert_eq!(
        receiver
            .changes_since(contextdb_core::Lsn(0))
            .expect("read or construct the declared fixture result")
            .ddl,
        after_first
    );
    assert_eq!(receiver.table_meta("items").unwrap().columns[1].name, "y");
}
