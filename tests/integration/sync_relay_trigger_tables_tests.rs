use contextdb_core::{Error, Lsn, TxId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange, SyncDirection,
};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

fn no_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn widget_params(id: Uuid, label: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("id".to_string(), Value::Uuid(id));
    p.insert("label".to_string(), Value::Text(label.to_string()));
    p
}

fn wuuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn server_wins() -> ConflictPolicies {
    ConflictPolicies::uniform(ConflictPolicy::ServerWins)
}

fn widget_label(db: &Database, id: Uuid) -> Option<String> {
    let rows = db.scan("widgets", db.snapshot()).unwrap();
    rows.into_iter()
        .find(|r| r.values.get("id") == Some(&Value::Uuid(id)))
        .and_then(|r| match r.values.get("label") {
            Some(Value::Text(s)) => Some(s.clone()),
            _ => None,
        })
}

fn row_count(db: &Database, table: &str) -> usize {
    db.scan(table, db.snapshot()).map(|r| r.len()).unwrap_or(0)
}

fn assert_table_absent(db: &Database, table: &str, reason: &str) {
    let scan = db.scan(table, db.snapshot());
    assert!(
        matches!(scan, Err(Error::TableNotFound(ref name)) if name == table),
        "{reason}: expected table {table:?} to be absent, got {scan:?}"
    );
}

fn assert_no_ddl_logged(db: &Database, reason: &str) {
    let ddl = db.changes_since(Lsn(0)).ddl;
    assert!(ddl.is_empty(), "{reason}: unexpected DDL log {ddl:?}");
}

fn single_audit_marker(db: &Database) -> Option<(Uuid, Uuid, String)> {
    db.scan("widget_audit", db.snapshot())
        .unwrap()
        .into_iter()
        .find_map(|row| {
            let id = row.values.get("id").and_then(Value::as_uuid).copied()?;
            let widget_id = row
                .values
                .get("widget_id")
                .and_then(Value::as_uuid)
                .copied()?;
            let marker = match row.values.get("marker")? {
                Value::Text(marker) => marker.clone(),
                _ => return None,
            };
            Some((id, widget_id, marker))
        })
}

fn open_relay_hub() -> Database {
    let hub = Database::open_memory();
    hub.enable_sync_relay_mode();
    hub
}

fn open_edge_with_widget_trigger() -> Database {
    let edge = Database::open_memory();
    edge.execute(
        "CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)",
        &no_params(),
    )
    .unwrap();
    edge.execute(
        "CREATE TRIGGER widget_guard ON widgets WHEN INSERT",
        &no_params(),
    )
    .unwrap();
    edge.register_trigger_callback("widget_guard", |_, _| Ok(()))
        .unwrap();
    edge.complete_initialization().unwrap();
    edge
}

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

async fn start_nats() -> NatsFixture {
    let nats_conf = format!(
        "{}/../contextdb-server/tests/nats.conf",
        env!("CARGO_MANIFEST_DIR")
    );
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let container = image
        .with_mount(Mount::bind_mount(&nats_conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"])
        .start()
        .await
        .expect("start nats");
    let host = container.get_host().await.expect("nats host");
    let port = container.get_host_port_ipv4(4222).await.expect("nats port");
    NatsFixture {
        nats_url: format!("nats://{host}:{port}"),
        _container: container,
    }
}

#[test]
fn relay_hub_applies_combined_ddl_and_trigger_table_data_in_one_batch() {
    let edge = open_edge_with_widget_trigger();
    let id = wuuid(1);
    edge.execute(
        "INSERT INTO widgets (id, label) VALUES ($id, $label)",
        &widget_params(id, "w1"),
    )
    .unwrap();
    let combined = edge.changes_since(Lsn(0));

    let hub = open_relay_hub();
    let res = hub.apply_changes(combined, &server_wins());
    assert!(
        res.is_ok(),
        "relay hub must apply a combined DDL + trigger-table-data batch, got {res:?}"
    );
    assert_eq!(
        widget_label(&hub, id).as_deref(),
        Some("w1"),
        "relay hub must file the trigger-table row verbatim"
    );
    assert_relay_still_rejects_trigger_table_schema_violation();
    assert_relay_still_rejects_trigger_table_fk_violation();
}

#[test]
fn relay_hub_applies_data_batch_after_trigger_declared_separately() {
    let edge = open_edge_with_widget_trigger();
    let ddl_only = edge.changes_since(Lsn(0));
    let after_ddl = edge.current_lsn();
    let id = wuuid(2);
    edge.execute(
        "INSERT INTO widgets (id, label) VALUES ($id, $label)",
        &widget_params(id, "w2"),
    )
    .unwrap();
    let data_only = edge.changes_since(after_ddl);

    let hub = open_relay_hub();
    let res_ddl = hub.apply_changes(ddl_only, &server_wins());
    assert!(
        res_ddl.is_ok(),
        "relay hub must apply a CREATE-TRIGGER DDL batch with no registered callback, got {res_ddl:?}"
    );
    let res_data = hub.apply_changes(data_only, &server_wins());
    assert!(
        res_data.is_ok(),
        "relay hub must apply a data-only batch on a declared callbackless trigger table, got {res_data:?}"
    );
    assert_eq!(widget_label(&hub, id).as_deref(), Some("w2"));
}

#[test]
fn strict_hub_rejects_callbackless_trigger_table_apply() {
    let edge = open_edge_with_widget_trigger();
    let id = wuuid(3);
    edge.execute(
        "INSERT INTO widgets (id, label) VALUES ($id, $label)",
        &widget_params(id, "w3"),
    )
    .unwrap();
    let combined = edge.changes_since(Lsn(0));

    let hub = Database::open_memory();
    hub.complete_initialization().unwrap();
    let res = hub.apply_changes(combined, &server_wins());
    assert!(
        matches!(res, Err(Error::EngineNotInitialized { .. })),
        "strict hub must still reject a combined callbackless trigger-table apply, got {res:?}"
    );
    assert_table_absent(
        &hub,
        "widgets",
        "strict combined rejection must not create the trigger table",
    );
    assert_no_ddl_logged(&hub, "strict combined rejection must not file trigger DDL");
    assert_eq!(
        row_count(&hub, "widgets"),
        0,
        "strict combined apply rejection must not file trigger-table rows"
    );

    let data_only_hub = Database::open_memory();
    data_only_hub
        .execute(
            "CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)",
            &no_params(),
        )
        .unwrap();
    data_only_hub
        .execute(
            "CREATE TRIGGER widget_guard ON widgets WHEN INSERT",
            &no_params(),
        )
        .unwrap();
    let data_only_id = wuuid(4);
    let data_only = ChangeSet {
        rows: vec![RowChange {
            table: "widgets".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(data_only_id),
            },
            values: widget_params(data_only_id, "strict-data-only"),
            deleted: false,
            lsn: Lsn(10),
            created_at: None,
        }],
        ..Default::default()
    };
    let data_only_res = data_only_hub.apply_changes(data_only, &server_wins());
    assert!(
        matches!(data_only_res, Err(Error::EngineNotInitialized { .. })),
        "strict hub must still reject a data-only callbackless trigger-table apply, got {data_only_res:?}"
    );
    assert_eq!(
        row_count(&data_only_hub, "widgets"),
        0,
        "strict data-only rejection must not file trigger-table rows"
    );
}

fn assert_relay_still_rejects_trigger_table_fk_violation() {
    let hub = open_relay_hub();
    hub.execute(
        "CREATE TABLE parent_rows (id UUID PRIMARY KEY)",
        &no_params(),
    )
    .unwrap();
    hub.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), label TEXT)",
        &no_params(),
    )
    .unwrap();
    hub.execute(
        "CREATE TRIGGER child_guard ON child_rows WHEN INSERT",
        &no_params(),
    )
    .unwrap();

    let child_id = wuuid(0x401);
    let parent_id = wuuid(0x402);
    let result = hub.apply_changes(
        ChangeSet {
            rows: vec![
                RowChange {
                    table: "child_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(child_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(child_id)),
                        ("parent_id".to_string(), Value::Uuid(parent_id)),
                        ("label".to_string(), Value::Text("bad-order".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(20),
                    created_at: None,
                },
                RowChange {
                    table: "parent_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(parent_id),
                    },
                    values: HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
                    deleted: false,
                    lsn: Lsn(30),
                    created_at: None,
                },
            ],
            ..Default::default()
        },
        &server_wins(),
    );
    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_id".to_string()]
        ),
        "relay mode must preserve FK validation for trigger-table rows; got {result:?}"
    );
    assert_eq!(
        row_count(&hub, "child_rows"),
        0,
        "FK-rejected relay apply must not leave the child row behind"
    );
    assert_eq!(
        row_count(&hub, "parent_rows"),
        0,
        "FK-rejected relay apply must not continue to later sender-LSN groups"
    );
}

fn assert_relay_still_rejects_trigger_table_schema_violation() {
    let hub = open_relay_hub();
    hub.execute(
        "CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)",
        &no_params(),
    )
    .unwrap();
    hub.execute(
        "CREATE TRIGGER widget_guard ON widgets WHEN INSERT",
        &no_params(),
    )
    .unwrap();

    let id = wuuid(0x601);
    let result = hub.apply_changes(
        ChangeSet {
            rows: vec![RowChange {
                table: "widgets".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(id),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("label".to_string(), Value::TxId(TxId(99))),
                ]),
                deleted: false,
                lsn: Lsn(10),
                created_at: None,
            }],
            ..Default::default()
        },
        &server_wins(),
    );
    assert!(
        matches!(
            result,
            Err(Error::ColumnTypeMismatch {
                ref table,
                ref column,
                expected: "TEXT",
                actual: "TxId",
            }) if table == "widgets" && column == "label"
        ),
        "relay mode must preserve row/schema validation for trigger-table rows; got {result:?}"
    );
    assert_eq!(
        row_count(&hub, "widgets"),
        0,
        "schema-rejected relay apply must not leave the malformed row behind"
    );
}

#[test]
fn relay_hub_still_enforces_ddl_lsn_preflight() {
    let hub = open_relay_hub();
    let malformed = ChangeSet {
        ddl: vec![DdlChange::CreateTable {
            name: "gadgets".into(),
            columns: vec![("id".into(), "UUID PRIMARY KEY".into())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }],
        ddl_lsn: Vec::new(),
        ..Default::default()
    };
    let res = hub.apply_changes(malformed, &server_wins());
    assert!(
        matches!(
            res,
            Err(Error::SyncError(ref message))
                if message == "invalid ChangeSet ddl_lsn length: got 0, expected 1 for 1 DDL entries"
        ),
        "relay mode must relax ONLY the trigger-callback gate, not ddl_lsn preflight; got {res:?}"
    );
    assert_table_absent(
        &hub,
        "gadgets",
        "malformed DDL-LSN rejection must not create schema",
    );
    assert_no_ddl_logged(
        &hub,
        "malformed DDL-LSN rejection must not file DDL history",
    );
}

#[test]
fn relay_apply_does_not_fabricate_trigger_cascade_rows() {
    let edge = Database::open_memory();
    edge.execute(
        "CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)",
        &no_params(),
    )
    .unwrap();
    edge.execute(
        "CREATE TABLE widget_audit (id UUID PRIMARY KEY, widget_id UUID, marker TEXT)",
        &no_params(),
    )
    .unwrap();
    edge.execute(
        "CREATE TRIGGER widget_guard ON widgets WHEN INSERT",
        &no_params(),
    )
    .unwrap();
    edge.register_trigger_callback("widget_guard", |db_handle, ctx| {
        let wid = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("trigger missing widget id".into()))?;
        let audit_id = wuuid(0x500);
        let mut p = HashMap::new();
        p.insert("id".to_string(), Value::Uuid(audit_id));
        p.insert("widget_id".to_string(), Value::Uuid(wid));
        p.insert(
            "marker".to_string(),
            Value::Text("origin-callback-marker".to_string()),
        );
        db_handle.execute(
            "INSERT INTO widget_audit (id, widget_id, marker) VALUES ($id, $widget_id, $marker)",
            &p,
        )?;
        Ok(())
    })
    .unwrap();
    edge.complete_initialization().unwrap();

    let id = wuuid(5);
    edge.execute(
        "INSERT INTO widgets (id, label) VALUES ($id, $label)",
        &widget_params(id, "w5"),
    )
    .unwrap();
    let combined = edge.changes_since(Lsn(0));

    let hub = open_relay_hub();
    let res = hub.apply_changes(combined, &server_wins());
    assert!(
        res.is_ok(),
        "relay hub must apply the transported batch, got {res:?}"
    );
    assert_eq!(
        row_count(&hub, "widgets"),
        1,
        "exactly the one transported widget row"
    );
    assert_eq!(
        row_count(&hub, "widget_audit"),
        1,
        "exactly the one transported audit row - relay transports, it never recomputes the trigger"
    );
    assert_eq!(
        single_audit_marker(&hub),
        Some((wuuid(0x500), id, "origin-callback-marker".to_string())),
        "relay must preserve the origin callback's audit row identity and payload"
    );
}

#[tokio::test]
async fn oss_syncserver_relay_hub_accepts_pushed_trigger_table_row() {
    let nats = start_nats().await;
    let tenant_id = "relay-trigger";

    let hub_db = Arc::new(Database::open_memory());
    hub_db
        .execute(
            "CREATE TABLE widgets (id UUID PRIMARY KEY, label TEXT)",
            &no_params(),
        )
        .unwrap();
    hub_db
        .execute(
            "CREATE TRIGGER widget_guard ON widgets WHEN INSERT",
            &no_params(),
        )
        .unwrap();

    let shutdown = Arc::new(AtomicBool::new(false));
    let server = Arc::new(SyncServer::new(
        Arc::clone(&hub_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant_id),
        server_wins(),
    ));
    let constructor_id = wuuid(6);
    let constructor_apply = hub_db.apply_changes(
        ChangeSet {
            rows: vec![RowChange {
                table: "widgets".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(constructor_id),
                },
                values: widget_params(constructor_id, "constructor-relay"),
                deleted: false,
                lsn: Lsn(10),
                created_at: None,
            }],
            ..Default::default()
        },
        &server_wins(),
    );
    assert!(
        constructor_apply.is_ok(),
        "SyncServer::new must mark its database handle as relay before run_until starts, got {constructor_apply:?}"
    );
    assert_eq!(
        widget_label(&hub_db, constructor_id).as_deref(),
        Some("constructor-relay"),
        "the constructor-marked relay hub must immediately file trigger-table rows"
    );
    let server_task = {
        let server = Arc::clone(&server);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move { server.run_until(shutdown).await })
    };
    tokio::time::sleep(Duration::from_millis(250)).await;

    let edge_db = Arc::new(open_edge_with_widget_trigger());
    let id = wuuid(7);
    edge_db
        .execute(
            "INSERT INTO widgets (id, label) VALUES ($id, $label)",
            &widget_params(id, "w7"),
        )
        .unwrap();

    let client = SyncClient::new(
        Arc::clone(&edge_db),
        &nats.nats_url,
        contextdb_core::TenantId::from(tenant_id),
    );
    client
        .set_table_direction("widgets", SyncDirection::Both)
        .expect("an ordinary table accepts any direction");
    let pushed = client.push().await;
    assert!(
        pushed.is_ok(),
        "push to a relay hub must succeed, got {pushed:?}"
    );

    assert_eq!(
        widget_label(&hub_db, id).as_deref(),
        Some("w7"),
        "the relay hub must have filed the pushed trigger-table row"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;
}
