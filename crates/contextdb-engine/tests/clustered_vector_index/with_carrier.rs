//! A vector column's engineering policy is declared through one carrier,
//! `WITH (member = value, ...)`, on CREATE and ADD COLUMN, and changed with
//! `ALTER TABLE t ALTER COLUMN c SET WITH (member = value, ...)` and
//! `... RESET WITH (member, ...)`. Every value kind (integer, decimal,
//! DEFAULT, a bracketed integer/decimal list, a table.column reference)
//! renders canonically, members render in their fixed canonical order, the
//! rendering is a fixpoint of parsing, DEFAULT declares nothing, an unknown
//! member is a typed refusal naming it, and the declarations travel with
//! synced DDL unchanged.

use super::support::empty;
use contextdb_core::{Error, TenantId, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata exists for {table}"));
    render_table_meta(table, &meta)
}

const QUESTIONS: &str =
    "CREATE TABLE carrier_questions (id INTEGER PRIMARY KEY, embedding VECTOR(3))";

#[test]
fn every_value_kind_renders_canonically_in_member_order_and_round_trips() {
    let db = Database::open_memory();
    db.execute(QUESTIONS, &empty())
        .expect("declare the calibration questions");
    db.execute(
        "CREATE TABLE carrier_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (training_seed = 0, refine_factors = [1, 4.50, 10, 20], write_buffer_bytes = DEFAULT, refine_growth = 2.50, calibration_queries = carrier_questions.embedding, calibration_max_k = 100))",
        &empty(),
    )
    .expect("every value kind is accepted, in any member order");
    let declared = schema(&db, "carrier_items");
    assert!(
        declared.contains(
            "WITH (calibration_max_k = 100, refine_factors = [1, 4.5, 10, 20], refine_growth = 2.5, calibration_queries = carrier_questions.embedding, training_seed = 0)"
        ),
        "members render in canonical order; decimals render without trailing zeros; the list \
         and the reference render as declared; zero is a valid seed; a DEFAULT member declares \
         nothing: {declared}"
    );
    assert!(
        !declared.contains("write_buffer_bytes"),
        "DEFAULT renders as silence: {declared}"
    );

    let replay = Database::open_memory();
    replay
        .execute(QUESTIONS, &empty())
        .expect("declare the questions in the replay");
    replay
        .execute(&declared, &empty())
        .expect("the rendered declaration is executable SQL");
    assert_eq!(
        schema(&replay, "carrier_items"),
        declared,
        "the rendering is a fixpoint"
    );

    db.execute(
        "CREATE TABLE silent_items (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .expect("declare a silent column");
    db.execute(
        "CREATE TABLE default_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (training_seed = DEFAULT, calibration_max_k = DEFAULT))",
        &empty(),
    )
    .expect("an all-DEFAULT carrier parses");
    assert_eq!(
        schema(&db, "default_items"),
        schema(&db, "silent_items").replace("silent_items", "default_items"),
        "an all-DEFAULT carrier declares nothing"
    );

    db.execute(
        "CREATE TABLE added_items (id INTEGER PRIMARY KEY)",
        &empty(),
    )
    .expect("declare the add-column target");
    db.execute(
        "ALTER TABLE added_items ADD COLUMN embedding VECTOR(3) WITH (max_delta_runs = 3)",
        &empty(),
    )
    .expect("ADD COLUMN accepts the carrier");
    assert!(
        schema(&db, "added_items").contains("WITH (max_delta_runs = 3)"),
        "ADD COLUMN renders its carrier: {}",
        schema(&db, "added_items")
    );
}

#[test]
fn set_with_and_reset_with_change_only_the_members_they_name() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (training_seed = 7, max_delta_runs = 3))",
        &empty(),
    )
    .expect("declare two members");
    let alter = |sql: &str| {
        db.execute(sql, &empty())
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        schema(&db, "policy_items")
    };
    let rendered =
        alter("ALTER TABLE policy_items ALTER COLUMN embedding SET WITH (training_seed = 0)");
    assert!(
        rendered.contains("WITH (training_seed = 0, max_delta_runs = 3)"),
        "SET WITH changes the named member and keeps the other: {rendered}"
    );
    let rendered =
        alter("ALTER TABLE policy_items ALTER COLUMN embedding SET WITH (ingest_batch_keys = 32)");
    assert!(
        rendered.contains("WITH (training_seed = 0, max_delta_runs = 3, ingest_batch_keys = 32)"),
        "SET WITH adds a member in its canonical place: {rendered}"
    );
    let rendered = alter(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET WITH (max_delta_runs = DEFAULT)",
    );
    assert!(
        rendered.contains("WITH (training_seed = 0, ingest_batch_keys = 32)")
            && !rendered.contains("max_delta_runs"),
        "SET WITH (member = DEFAULT) clears only that member: {rendered}"
    );
    let rendered =
        alter("ALTER TABLE policy_items ALTER COLUMN embedding RESET WITH (training_seed)");
    assert!(
        rendered.contains("WITH (ingest_batch_keys = 32)") && !rendered.contains("training_seed"),
        "RESET WITH clears the named member: {rendered}"
    );
    let rendered =
        alter("ALTER TABLE policy_items ALTER COLUMN embedding RESET WITH (ingest_batch_keys)");
    assert!(
        !rendered.contains("WITH ("),
        "resetting the last member leaves no carrier: {rendered}"
    );
}

#[test]
fn an_unknown_with_member_is_a_typed_refusal_naming_it() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (training_seed = 7))",
        &empty(),
    )
    .expect("declare the column every refusal must preserve");
    db.execute("CREATE TABLE add_target (id INTEGER PRIMARY KEY)", &empty())
        .expect("declare the add-column target");
    let before = (schema(&db, "policy_items"), schema(&db, "add_target"));
    for (sql, table) in [
        (
            "CREATE TABLE refused_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (no_such_member = 1))",
            "refused_items",
        ),
        (
            "ALTER TABLE add_target ADD COLUMN embedding VECTOR(3) WITH (no_such_member = 1)",
            "add_target",
        ),
        (
            "ALTER TABLE policy_items ALTER COLUMN embedding SET WITH (no_such_member = 1)",
            "policy_items",
        ),
        (
            "ALTER TABLE policy_items ALTER COLUMN embedding RESET WITH (no_such_member)",
            "policy_items",
        ),
    ] {
        let error = db
            .execute(sql, &empty())
            .expect_err("an unknown member is refused");
        assert!(
            matches!(
                &error,
                Error::InvalidVectorPolicyDeclaration { index, issue }
                    if index == &VectorIndexRef::new(table, "embedding")
                        && format!("{issue:?} {issue}").contains("no_such_member")
            ),
            "{sql} is a typed vector policy refusal naming the member, got {error:?}"
        );
        assert!(
            db.table_meta("refused_items").is_none(),
            "{sql} publishes no table"
        );
        assert_eq!(
            (schema(&db, "policy_items"), schema(&db, "add_target")),
            before,
            "{sql} changes nothing"
        );
    }
}

const TENANT: &str = "with-carrier-transport";

#[tokio::test]
async fn with_declarations_travel_with_synced_ddl() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let edge = Arc::new(Database::open_memory());
    source
        .execute(
            &format!("{QUESTIONS} SYNC TWO WAY SYNC CONFLICT KEEP LATEST"),
            &empty(),
        )
        .expect("declare the source questions");
    source
        .execute(
            "CREATE TABLE carrier_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (calibration_depths = [1, 10, 50], centroid_memory_fraction = 0.06, calibration_queries = carrier_questions.embedding, training_seed = 0, max_delta_runs = 3)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
            &empty(),
        )
        .expect("declare the source column with its carrier");

    let hub_identity = Arc::new(FabricIdentity::generate());
    let hub_node_id = hub_identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            source.clone(),
            broker.server_as(&hub_node_id),
            TenantId::from(TENANT),
            hub_node_id,
            hub_identity,
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    let edge_identity = Arc::new(FabricIdentity::generate());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_identity.node_id()),
        TenantId::from(TENANT),
        edge_identity,
    )
    .expect("construct the authenticated sync client");

    client.pull_default().await.expect("the first pull");
    assert_eq!(
        schema(&edge, "carrier_items"),
        schema(&source, "carrier_items"),
        "the receiver holds the sender's carrier exactly"
    );
    assert!(
        schema(&edge, "carrier_items").contains(
            "WITH (calibration_depths = [1, 10, 50], calibration_queries = carrier_questions.embedding, centroid_memory_fraction = 0.06, training_seed = 0, max_delta_runs = 3)"
        ),
        "the transported carrier renders canonically: {}",
        schema(&edge, "carrier_items")
    );

    for alter in [
        "ALTER TABLE carrier_items ALTER COLUMN embedding SET WITH (training_seed = 5, refine_growth = 1.5)",
        "ALTER TABLE carrier_items ALTER COLUMN embedding RESET WITH (max_delta_runs, calibration_queries)",
    ] {
        source
            .execute(alter, &empty())
            .unwrap_or_else(|error| panic!("{alter}: {error:?}"));
        client.pull_default().await.expect("pull the change");
        assert_eq!(
            schema(&edge, "carrier_items"),
            schema(&source, "carrier_items"),
            "after {alter}, the receiver holds the sender's carrier exactly"
        );
    }

    drop(client);
    shutdown.store(true, Ordering::SeqCst);
    server_task.await.expect("server task stops");
}
