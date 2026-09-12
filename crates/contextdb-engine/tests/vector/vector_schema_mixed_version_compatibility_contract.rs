//! A schema upgrade must not turn an ordinary two-node sync into an all-or-nothing upgrade.
//!
//! Both endpoints speak protocol 7; a test-only sender seam models missing capabilities. The
//! sender owns the compatibility gap: an untouched table continues to move once, the table that
//! uses newer vector-schema words is held with a typed upgrade message, and that table resumes
//! automatically when the receiver upgrades.

use contextdb_core::{TenantId, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::sync_types::{DdlChange, SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_engine::{Database, QueryResult};
use contextdb_server::protocol::PROTOCOL_VERSION;
use contextdb_server::subjects::pull_subject;
use contextdb_server::{
    FabricIdentity, InProcessBroker, SyncClient, SyncServer, TransferDirection, TransferPlane,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const TENANT: &str = "vector-schema-mixed-version";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn text_column(result: QueryResult) -> Vec<String> {
    result
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Text(value)) => value,
            value => panic!("expected one text value, got {value:?}"),
        })
        .collect()
}

fn sent_sync_rows(server: &SyncServer, peer: &str) -> u64 {
    server
        .transfer_receipts()
        .into_iter()
        .find(|receipt| {
            receipt.peer_node_id == peer
                && receipt.plane == TransferPlane::Sync
                && receipt.direction == TransferDirection::Sent
        })
        .map(|receipt| receipt.counters.items)
        .unwrap_or_default()
}

#[test]
fn untouched_vector_schema_renders_and_syncs_with_the_previous_bytes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE untouched (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .expect("create a table that adopts no new schema vocabulary");

    let meta = db.table_meta("untouched").expect("table metadata exists");
    assert_eq!(
        render_table_meta("untouched", &meta),
        "CREATE TABLE untouched (\n  id UUID PRIMARY KEY,\n  embedding VECTOR(3)\n);\n",
        "the default is silence: upgrading one node must not add SEARCH_MODE AUTO, a partition key, or a partition cap to an untouched table"
    );

    let declaration = db
        .changes_since(contextdb_core::Lsn(0))
        .ddl
        .into_iter()
        .find_map(|change| match change {
            DdlChange::CreateTable { name, columns, .. } if name == "untouched" => Some(columns),
            _ => None,
        })
        .expect("the outbound snapshot carries the untouched table declaration");
    assert_eq!(
        declaration,
        vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("embedding".to_string(), "VECTOR(3)".to_string()),
        ],
        "the SQL text sent over sync must remain byte-identical for a table that adopts no new vocabulary"
    );
    assert_eq!(
        PROTOCOL_VERSION, 7,
        "the first release integrates the vocabulary on protocol 7"
    );
}

#[tokio::test]
async fn older_receiver_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_upgrade() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    let first_note = Uuid::from_u128(0xC001);
    let second_note = Uuid::from_u128(0xC002);
    let vector_id = Uuid::from_u128(0xC003);
    let scope_id = Uuid::from_u128(0xC004);

    source
        .execute(
            "CREATE TABLE ordinary_notes (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the table whose vocabulary both nodes understand");
    source
        .execute(
            "CREATE TABLE partitioned_notes (id UUID PRIMARY KEY, scope_id UUID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id)) SYNC TWO WAY",
            &empty(),
        )
        .expect("declare the table that needs the newer schema capability");
    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
            &params([
                ("id", Value::Uuid(first_note)),
                ("body", Value::Text("first".to_string())),
            ]),
        )
        .expect("write the first ordinary row");
    source
        .execute(
            "INSERT INTO partitioned_notes (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
            &params([
                ("id", Value::Uuid(vector_id)),
                ("scope", Value::Uuid(scope_id)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("write the row whose table needs the newer schema capability");

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            hub_identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(TENANT))
        .await;

    let receiver_identity = Arc::new(FabricIdentity::generate());
    let receiver_node_id = receiver_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        receiver.clone(),
        broker.client_as(&receiver_node_id),
        TenantId::from(TENANT),
        receiver_identity,
    );
    server.set_peer_vector_schema_support_for_test(&receiver_node_id, false);

    client
        .pull_default()
        .await
        .expect("a newer declaration must not refuse the whole connection");
    assert_eq!(
        text_column(
            receiver
                .execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("the ordinary table and its first row arrive")
        ),
        vec!["first".to_string()]
    );
    assert!(
        receiver.table_meta("partitioned_notes").is_none(),
        "the older parser must never be handed SQL words it cannot understand"
    );
    assert_eq!(
        server.schema_sync_holdbacks(),
        vec![SchemaSyncHoldback {
            table: "partitioned_notes".to_string(),
            capability: SchemaSyncCapability::VectorPartitioning,
            node_to_upgrade: receiver_node_id.clone(),
        }],
        "the typed message names the held table, the missing capability, and the exact node to upgrade"
    );

    source
        .execute(
            "INSERT INTO ordinary_notes (id, body) VALUES ($id, $body)",
            &params([
                ("id", Value::Uuid(second_note)),
                ("body", Value::Text("second".to_string())),
            ]),
        )
        .expect("write ordinary work while the other table remains held");
    client
        .pull_default()
        .await
        .expect("the unaffected table keeps syncing during the version gap");
    assert_eq!(
        text_column(
            receiver
                .execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("both ordinary rows are readable")
        ),
        vec!["first".to_string(), "second".to_string()]
    );
    assert_eq!(
        sent_sync_rows(&server, &receiver_node_id),
        2,
        "healthy rows cross once; holding one table must not repeatedly resend another table's history"
    );

    server.set_peer_vector_schema_support_for_test(&receiver_node_id, true);
    client.pull_default().await.expect(
        "upgrading the receiver resumes the held table without a cursor reset or repair command",
    );
    assert!(
        receiver.table_meta("partitioned_notes").is_some(),
        "the held declaration arrives after upgrade"
    );
    let vector_rows = receiver
        .execute("SELECT id FROM partitioned_notes", &empty())
        .expect("the held table's row arrives with its declaration");
    assert_eq!(vector_rows.rows, vec![vec![Value::Uuid(vector_id)]]);
    assert!(
        server.schema_sync_holdbacks().is_empty(),
        "successful upgraded sync clears the upgrade message automatically"
    );
    assert_eq!(
        text_column(
            receiver
                .execute("SELECT body FROM ordinary_notes ORDER BY body", &empty())
                .expect("ordinary data remains unchanged after held-table recovery")
        ),
        vec!["first".to_string(), "second".to_string()]
    );

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
}
