//! A received drop of an edge table takes that table's links with it.
//!
//! An edge table is a table like any other: rows in, rows out, and a `DROP
//! TABLE` that arrives over sync removes it on the receiver the same way it
//! does on the writer. What the receiver must not do is keep the links. A
//! consumer that traverses the graph after the drop would otherwise be handed
//! edges belonging to a table nobody can read any more -- and be handed them
//! again after a restart, because the receiver's own file still carries them.
//!
//! The DAG-declared case was already covered: dropping a table whose DAG
//! declaration goes with it changes the declared edge types, and the receiver
//! notices that. An ORDINARY edge table -- the same three columns, no `DAG`
//! clause -- changes no declaration and carries no edge change in the arriving
//! changeset, which is exactly why its links used to survive its own table.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;
use uuid::Uuid;

const LINK_TYPE: &str = "MENTIONS";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

#[test]
fn a_received_drop_of_an_ordinary_edge_table_removes_its_links_now_and_after_a_restart() {
    let directory = tempfile::TempDir::new().expect("task-scoped sync directory");
    let path = directory.path().join("received-drop.db");
    let source = Uuid::from_u128(0x19_01);
    let target = Uuid::from_u128(0x19_02);

    {
        let database = Database::open(&path).expect("claim the receiving store");
        database
            .execute(
                "CREATE TABLE links (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
                 edge_type TEXT)",
                &empty(),
            )
            .expect("declare an ordinary edge table with no DAG clause");
        database
            .execute(
                "INSERT INTO links (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, $edge_type)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::from_u128(0x19_03))),
                    ("source".to_string(), Value::Uuid(source)),
                    ("target".to_string(), Value::Uuid(target)),
                    ("edge_type".to_string(), Value::Text(LINK_TYPE.to_string())),
                ]),
            )
            .expect("commit one link");
        assert_eq!(
            database
                .edge_count(source, LINK_TYPE, database.snapshot())
                .expect("count the link before the drop arrives"),
            1,
            "the link has to be there, or this journey proves nothing"
        );

        database
            .apply_changes(
                ChangeSet {
                    rows: vec![],
                    edges: vec![],
                    vectors: vec![],
                    ddl: vec![DdlChange::DropTable {
                        name: "links".to_string(),
                    }],
                    ddl_lsn: vec![contextdb_core::Lsn(10)],
                },
                &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
            )
            .expect("a received drop of a declared table applies");

        assert!(
            database.table_meta("links").is_none(),
            "the received drop removes the table"
        );
        assert_eq!(
            database
                .edge_count(source, LINK_TYPE, database.snapshot())
                .expect("count the link after the drop"),
            0,
            "a received drop takes the dropped table's links with it"
        );
        database.close().expect("release the receiving store");
    }

    let reopened = Database::open(&path).expect("reopen the receiving store");
    assert!(
        reopened.table_meta("links").is_none(),
        "the drop survives the restart"
    );
    assert_eq!(
        reopened
            .edge_count(source, LINK_TYPE, reopened.snapshot())
            .expect("count the link after the restart"),
        0,
        "and so does the removal of its links: the file does not bring them back"
    );
    reopened.close().expect("release the reopened store");
}

/// The same drop arriving through the authenticated received-schema door --
/// the door a real sync peer uses -- takes the links with it too, now and
/// after a restart.
#[test]
fn an_authenticated_received_drop_of_an_ordinary_edge_table_removes_its_links() {
    use contextdb_core::{Incarnation, Lsn, TenantId};

    const TENANT: &str = "received-link-table-drop";

    fn apply_received(author: &Database, receiver: &Database, changes: ChangeSet) {
        let identity = {
            static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
                std::sync::OnceLock::new();
            AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate)
        };
        let node_id = identity.node_id();
        let fixture_operation_reader_1 = author
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        let provenance = author
            .outbound_ddl_provenance_for_test(
                &changes,
                &TenantId::from(TENANT),
                &node_id,
                Incarnation(1),
                &|bytes| Ok(identity.sign_for_test(bytes)),
                fixture_operation_reader_1.clone(),
            )
            .expect("the authoring source produces canonical DDL provenance");
        let fixture_operation_reader_2 = receiver
            .declared_reader_memory()
            .expect("admit the actual fixture reader");
        let mut fixture_operation_copy_2 =
            contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

        receiver
            .apply_received_schema_for_test(
                changes,
                provenance,
                TenantId::from(TENANT),
                &node_id,
                Incarnation(1),
                &mut fixture_operation_copy_2,
                fixture_operation_reader_2.clone(),
            )
            .expect("an authenticated received schema unit applies");
    }

    let directory = tempfile::TempDir::new().expect("task-scoped sync directory");
    let path = directory.path().join("authenticated-received-drop.db");
    let source = Uuid::from_u128(0x19_11);
    let target = Uuid::from_u128(0x19_12);

    let author = Database::open_memory();
    author
        .execute(
            "CREATE TABLE links (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
             edge_type TEXT)",
            &empty(),
        )
        .expect("the author declares an ordinary edge table with no DAG clause");
    let create = author
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let before_drop = author.current_lsn();
    author
        .execute("DROP TABLE links", &empty())
        .expect("the author drops the edge table");
    let drop = author
        .changes_since(before_drop)
        .expect("read or construct the declared fixture result");

    {
        let database = Database::open(&path).expect("claim the receiving store");
        apply_received(&author, &database, create);
        database
            .execute(
                "INSERT INTO links (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, $edge_type)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::from_u128(0x19_13))),
                    ("source".to_string(), Value::Uuid(source)),
                    ("target".to_string(), Value::Uuid(target)),
                    ("edge_type".to_string(), Value::Text(LINK_TYPE.to_string())),
                ]),
            )
            .expect("commit one link");
        assert_eq!(
            database
                .edge_count(source, LINK_TYPE, database.snapshot())
                .expect("count the link before the drop arrives"),
            1,
            "the link has to be there, or this journey proves nothing"
        );
        apply_received(&author, &database, drop);
        assert!(database.table_meta("links").is_none());
        assert_eq!(
            database
                .edge_count(source, LINK_TYPE, database.snapshot())
                .expect("count the link after the drop"),
            0,
            "an authenticated received drop takes the dropped table's links with it"
        );
        database.close().expect("release the receiving store");
    }

    let reopened = Database::open(&path).expect("reopen the receiving store");
    assert_eq!(
        reopened
            .edge_count(source, LINK_TYPE, reopened.snapshot())
            .expect("count the link after the restart"),
        0,
        "and the file does not bring them back"
    );
    reopened.close().expect("release the reopened store");
}
