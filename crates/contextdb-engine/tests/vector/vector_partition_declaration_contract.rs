//! Durable SQL declarations for vector partition layout and search behavior.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;
use std::{collections::HashMap, sync::Arc, thread, time::Duration};
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata must exist for {table}"));
    render_table_meta(table, &meta)
}

fn column(result: &contextdb_engine::QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == name)
        .unwrap_or_else(|| panic!("inspection includes {name}: {:?}", result.columns))
}

fn integer_partition_keys(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    let key = column(result, "partition_key");
    result
        .rows
        .iter()
        .map(|row| match row.get(key) {
            Some(Value::Json(value)) => value
                .get("bucket")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_else(|| panic!("bucket is a typed integer in {value}")),
            other => panic!("partition key is JSON, got {other:?}"),
        })
        .collect()
}

fn assert_typed_declaration_refusal(error: Error, sql: &str) {
    assert!(
        !matches!(
            &error,
            Error::ParseError(_) | Error::PlanError(_) | Error::Other(_)
        ),
        "invalid vector partition DDL must reach semantic validation and return a typed refusal, not a parse, planning, or generic error\nSQL: {sql}\nerror: {error:?}"
    );
}

#[test]
fn existing_quantized_vector_schema_survives_reopen() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("vector-control.db");
    let db = Database::open(&path).expect("open vector control store");
    db.execute(
        "CREATE TABLE vector_control (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (quantization = 'SQ8'))",
        &empty(),
    )
    .expect("existing vector DDL must remain available");

    let before = schema(&db, "vector_control");
    assert!(
        before.contains("embedding VECTOR(3) WITH (quantization = 'SQ8')"),
        "control schema must render existing vector quantization: {before}"
    );
    db.close().expect("close vector control store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen vector control store");
    assert_eq!(
        schema(&reopened, "vector_control"),
        before,
        "existing vector metadata must remain stable across reopen"
    );
}

#[test]
fn partitioned_vector_schema_survives_reopen_with_canonical_defaults() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("partitioned-vector.db");
    let db = Database::open(&path).expect("open partitioned vector store");
    db.execute(
        "CREATE TABLE vector_items (item_id UUID PRIMARY KEY, scope_id UUID NOT NULL, kind TEXT NOT NULL, embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id, kind))",
        &empty(),
    )
    .expect("declare a partitioned vector column");

    let before = schema(&db, "vector_items");
    let canonical = "embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id, kind) MAX_PARTITIONS 256";
    assert!(
        before.contains(canonical),
        "schema must render quantization, partition key, and the write-refusing effective limit in canonical order while leaving the default AUTO mode silent\nexpected: {canonical}\nrendered: {before}"
    );
    assert!(!before.contains("SEARCH_MODE AUTO"));
    db.close().expect("close partitioned vector store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen partitioned vector store");
    assert_eq!(
        schema(&reopened, "vector_items"),
        before,
        "the complete vector declaration must survive reopen"
    );
}

#[test]
fn consolidation_and_maintenance_poll_declarations_round_trip_and_reset() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("maintenance-policy.db");
    let db = Database::open(&path).expect("open maintenance-policy store");
    db.execute(
        "CREATE TABLE maintained_items (
            id UUID PRIMARY KEY,
            scope UUID NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 8
                CONSOLIDATION (CHANGE_PERCENT = 7, TOMBSTONE_PERCENT = 31)
        )",
        &empty(),
    )
    .expect("declare per-column consolidation policy");
    db.execute(
        "INSERT INTO maintained_items (id, scope, embedding) VALUES ($id, $scope, $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(0xC001))),
            ("scope".to_owned(), Value::Uuid(Uuid::from_u128(0xC002))),
            ("embedding".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("seed one inspectable partition for both SHOW surfaces");
    db.execute("SET MAINTENANCE_POLL_INTERVAL '250 MILLISECONDS'", &empty())
        .expect("declare the quoted database maintenance cadence through SQL");
    let shown = db
        .execute("SHOW MAINTENANCE_POLL_INTERVAL", &empty())
        .expect("inspect the declared maintenance cadence");
    assert_eq!(shown.columns, vec!["milliseconds"]);
    assert_eq!(shown.rows, vec![vec![Value::Int64(250)]]);
    assert_eq!(db.maintenance_poll_interval(), Duration::from_millis(250));
    let declared = schema(&db, "maintained_items");
    assert!(
        declared.contains("CONSOLIDATION (CHANGE_PERCENT = 7, TOMBSTONE_PERCENT = 31)"),
        "the explicit policy has one canonical schema spelling: {declared}"
    );
    db.close().expect("close the maintenance-policy store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen maintenance-policy store");
    assert_eq!(
        reopened.maintenance_poll_interval(),
        Duration::from_millis(250)
    );
    let meta = reopened
        .table_meta("maintained_items")
        .expect("reopened table keeps its vector declaration");
    let vector = meta
        .columns
        .iter()
        .find(|column| column.name == "embedding")
        .expect("reopened table keeps its vector column");
    assert_eq!(vector.consolidation_change_percent, Some(7));
    assert_eq!(vector.consolidation_tombstone_percent, Some(31));

    reopened
        .execute(
            "ALTER TABLE maintained_items ALTER COLUMN embedding \
             SET CONSOLIDATION (CHANGE_PERCENT = 11)",
            &empty(),
        )
        .expect("alter one consolidation axis without resetting the other");
    let changed = reopened.table_meta("maintained_items").unwrap();
    let vector = changed
        .columns
        .iter()
        .find(|column| column.name == "embedding")
        .unwrap();
    assert_eq!(vector.consolidation_change_percent, Some(11));
    assert_eq!(vector.consolidation_tombstone_percent, Some(31));
    let thresholds = reopened.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    assert_eq!(
        thresholds.rows[0][column(&thresholds, "declared_consolidation_mode")],
        Value::Text("thresholds".to_owned())
    );
    let threshold_partitions = reopened
        .execute(
            "SHOW VECTOR_PARTITIONS FOR maintained_items.embedding",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        threshold_partitions.rows[0][column(&threshold_partitions, "declared_consolidation_mode")],
        Value::Text("thresholds".to_owned())
    );
    assert_eq!(
        threshold_partitions.rows[0][column(
            &threshold_partitions,
            "declared_consolidation_change_percent"
        )],
        Value::Int64(11)
    );
    assert_eq!(
        threshold_partitions.rows[0][column(
            &threshold_partitions,
            "declared_consolidation_tombstone_percent"
        )],
        Value::Int64(31)
    );

    reopened
        .execute(
            "ALTER TABLE maintained_items ALTER COLUMN embedding SET CONSOLIDATION NONE",
            &empty(),
        )
        .expect("disable automatic consolidation without disabling maintained search");
    let disabled_schema = schema(&reopened, "maintained_items");
    assert!(
        disabled_schema.contains("CONSOLIDATION NONE"),
        "the disabled policy has one canonical schema spelling: {disabled_schema}"
    );
    let disabled = reopened
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect the disabled consolidation policy");
    assert_eq!(
        disabled.rows[0][column(&disabled, "declared_consolidation_mode")],
        Value::Text("none".to_owned())
    );
    assert_eq!(
        disabled.rows[0][column(&disabled, "effective_consolidation_mode")],
        Value::Text("none".to_owned())
    );
    for name in [
        "effective_consolidation_change_percent",
        "effective_consolidation_tombstone_percent",
    ] {
        assert_eq!(disabled.rows[0][column(&disabled, name)], Value::Null);
    }
    let disabled_partitions = reopened
        .execute(
            "SHOW VECTOR_PARTITIONS FOR maintained_items.embedding",
            &empty(),
        )
        .expect("partition inspection exposes the disabled consolidation policy");
    for name in [
        "declared_consolidation_mode",
        "declared_consolidation_change_percent",
        "declared_consolidation_tombstone_percent",
        "effective_consolidation_mode",
        "effective_consolidation_change_percent",
        "effective_consolidation_tombstone_percent",
    ] {
        assert!(
            disabled_partitions
                .columns
                .iter()
                .any(|column| column == name),
            "partition inspection exposes {name}"
        );
    }
    assert_eq!(disabled_partitions.rows.len(), 1);
    assert_eq!(
        disabled_partitions.rows[0][column(&disabled_partitions, "declared_consolidation_mode")],
        Value::Text("none".to_owned())
    );
    assert_eq!(
        disabled_partitions.rows[0][column(&disabled_partitions, "effective_consolidation_mode")],
        Value::Text("none".to_owned())
    );
    reopened.close().expect("persist CONSOLIDATION NONE");
    drop(reopened);

    let reopened = Database::open(&path).expect("reopen the disabled consolidation policy");
    assert!(schema(&reopened, "maintained_items").contains("CONSOLIDATION NONE"));
    reopened
        .execute(
            "ALTER TABLE maintained_items ALTER COLUMN embedding SET CONSOLIDATION DEFAULT",
            &empty(),
        )
        .expect("restore the silent consolidation defaults");
    let reset = reopened.table_meta("maintained_items").unwrap();
    let vector = reset
        .columns
        .iter()
        .find(|column| column.name == "embedding")
        .unwrap();
    assert_eq!(vector.consolidation_change_percent, None);
    assert_eq!(vector.consolidation_tombstone_percent, None);
    assert!(
        !schema(&reopened, "maintained_items").contains("CONSOLIDATION"),
        "default consolidation policy stays silent in canonical schema"
    );
    let defaulted = reopened.execute("SHOW VECTOR_INDEXES", &empty()).unwrap();
    assert_eq!(
        defaulted.rows[0][column(&defaulted, "declared_consolidation_mode")],
        Value::Null
    );
    assert_eq!(
        defaulted.rows[0][column(&defaulted, "effective_consolidation_mode")],
        Value::Text("thresholds".to_owned())
    );
    assert_eq!(
        defaulted.rows[0][column(&defaulted, "effective_consolidation_change_percent")],
        Value::Int64(20)
    );
    assert_eq!(
        defaulted.rows[0][column(&defaulted, "effective_consolidation_tombstone_percent")],
        Value::Int64(10)
    );
    let defaulted_partitions = reopened
        .execute(
            "SHOW VECTOR_PARTITIONS FOR maintained_items.embedding",
            &empty(),
        )
        .unwrap();
    assert_eq!(defaulted_partitions.rows.len(), 1);
    assert_eq!(
        defaulted_partitions.rows[0][column(&defaulted_partitions, "declared_consolidation_mode")],
        Value::Null
    );
    assert_eq!(
        defaulted_partitions.rows[0][column(&defaulted_partitions, "effective_consolidation_mode")],
        Value::Text("thresholds".to_owned())
    );
}

#[test]
fn concurrent_maintenance_poll_interval_setters_keep_live_worker_and_reopen_consistent() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("concurrent-maintenance-policy.db");
    let db = Arc::new(Database::open(&path).expect("open maintenance-policy store"));
    db.set_maintenance_poll_interval(Duration::from_millis(19))
        .expect("set the initial maintenance cadence");
    db.execute(
        "CREATE TABLE maintained_items (
            id INTEGER PRIMARY KEY,
            scope INTEGER NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope)
        )",
        &empty(),
    )
    .expect("start the engine-owned maintenance worker");
    let worker_clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("hold the real engine-owned maintenance clock");
    assert_eq!(
        db.__maintenance_worker_poll_interval_for_test(),
        Some(Duration::from_millis(19))
    );

    let entered_before = db.__maintenance_poll_interval_update_entries_for_test();
    let pause = db.__pause_after_maintenance_poll_interval_persist_for_test();
    let first_db = db.clone();
    let first =
        thread::spawn(move || first_db.set_maintenance_poll_interval(Duration::from_millis(37)));
    assert!(
        pause.wait_until_reached_blocking(),
        "the first setter reaches the post-persistence publication boundary"
    );
    assert!(
        db.__maintenance_poll_interval_update_lock_is_held_for_test(),
        "the durable update must hold the shared setting lock through live and worker publication"
    );

    let second_db = db.clone();
    let second =
        thread::spawn(move || second_db.set_maintenance_poll_interval(Duration::from_millis(53)));
    db.__wait_for_maintenance_poll_interval_update_entry_for_test(entered_before + 2);
    assert_eq!(
        db.maintenance_poll_interval(),
        Duration::from_millis(19),
        "the paused durable value is not live before its serialized publication"
    );
    assert_eq!(
        db.__maintenance_worker_poll_interval_for_test(),
        Some(Duration::from_millis(19)),
        "the worker still observes the last completely published setting"
    );

    pause.release();
    first
        .join()
        .expect("join the first interval setter")
        .expect("the first interval setter succeeds");
    second
        .join()
        .expect("join the second interval setter")
        .expect("the second interval setter succeeds");
    assert_eq!(db.maintenance_poll_interval(), Duration::from_millis(53));
    assert_eq!(
        db.__maintenance_worker_poll_interval_for_test(),
        Some(Duration::from_millis(53))
    );

    db.close().expect("close the maintenance-policy store");
    drop(worker_clock);
    drop(db);
    let reopened = Database::open(&path).expect("reopen the maintenance-policy store");
    assert_eq!(
        reopened.maintenance_poll_interval(),
        Duration::from_millis(53),
        "reopen restores the same value that the live database and worker published"
    );
}

#[test]
fn partition_key_component_refusal_names_the_replacement_table_journey() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE camera_vectors (
            id UUID PRIMARY KEY,
            camera_id UUID NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (camera_id) MAX_PARTITIONS 16
        )",
        &empty(),
    )
    .expect("create a partition-key declaration");

    for sql in [
        "ALTER TABLE camera_vectors DROP COLUMN camera_id",
        "ALTER TABLE camera_vectors RENAME COLUMN camera_id TO source_id",
    ] {
        let error = db
            .execute(sql, &empty())
            .expect_err("a live vector partition key cannot be changed in place");
        assert!(
            matches!(
                &error,
                Error::VectorPartitionKeyInUse { table, column, index }
                    if table == "camera_vectors"
                        && column == "camera_id"
                        && index == "embedding"
            ),
            "the refusal is typed to the blocking vector partition declaration: {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("create a replacement table")
                && message.contains("desired vector partition key")
                && message.contains("copy the wanted rows"),
            "the refusal tells the operator how to proceed: {message}"
        );
    }
}

#[test]
fn signed_required_numeric_values_keep_distinct_integer_partitions_across_reopen() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("signed-partitions.db");
    let db = Database::open(&path).expect("open signed partition store");
    db.execute(
        "CREATE TABLE signed_partition_items (\
         id INTEGER PRIMARY KEY, \
         bucket INTEGER NOT NULL, \
         weight REAL NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (bucket) MAX_PARTITIONS 8)",
        &empty(),
    )
    .expect("declare a required INTEGER partition key and REAL value");

    for (id, bucket, weight, embedding) in [
        ("-1", "-1", "-1.5", "[1,0]"),
        ("0", "0", "0.0", "[0,1]"),
        ("1", "1", "1.5", "[1,1]"),
    ] {
        db.execute(
            &format!(
                "INSERT INTO signed_partition_items (id, bucket, weight, embedding) \
                 VALUES ({id}, {bucket}, {weight}, {embedding})"
            ),
            &empty(),
        )
        .unwrap_or_else(|error| panic!("signed required numeric literals must insert: {error}"));
    }

    let null_error = db
        .execute(
            "INSERT INTO signed_partition_items (id, bucket, weight, embedding) \
             VALUES (2, NULL, -2.5, [1,0])",
            &empty(),
        )
        .expect_err("NOT NULL remains enforced for the partition key");
    assert!(
        null_error
            .to_string()
            .contains("NOT NULL constraint violated: signed_partition_items.bucket"),
        "required partition key keeps its NOT NULL error: {null_error}"
    );

    let unresolved_error = db
        .execute(
            "INSERT INTO signed_partition_items (id, bucket, weight, embedding) \
             VALUES (3, id, -3.5, [1,0])",
            &empty(),
        )
        .expect_err("a row-dependent required value is refused before insert");
    let unresolved = unresolved_error.to_string();
    assert!(
        unresolved.contains("signed_partition_items.bucket")
            && unresolved.contains("requires row context")
            && !unresolved.contains("unsupported expression in schema enforcer"),
        "the required-value refusal identifies the destination instead of an implementation detail: {unresolved}"
    );

    let before = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR signed_partition_items.embedding",
            &empty(),
        )
        .expect("inspect the typed partition keys");
    assert_eq!(
        integer_partition_keys(&before),
        vec![-1, 0, 1],
        "negative, zero, and positive INTEGER keys remain distinct and sort by typed numeric identity"
    );
    db.close().expect("close signed partition store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen signed partition store");
    let after = reopened
        .execute(
            "SHOW VECTOR_PARTITIONS FOR signed_partition_items.embedding",
            &empty(),
        )
        .expect("inspect signed partitions after reopen");
    assert_eq!(
        integer_partition_keys(&after),
        vec![-1, 0, 1],
        "typed negative partition identity survives reopen"
    );
}

#[test]
fn max_partitions_without_a_partition_key_is_a_typed_refusal() {
    let db = Database::open_memory();
    let sql = "CREATE TABLE invalid_limit_scope (id INTEGER PRIMARY KEY, embedding VECTOR(3) MAX_PARTITIONS 8)";
    let error = db
        .execute(sql, &empty())
        .expect_err("MAX_PARTITIONS without PARTITION_KEY must be refused");

    assert!(
        db.table_meta("invalid_limit_scope").is_none(),
        "the refusal must leave no partial table metadata"
    );
    assert_typed_declaration_refusal(error, sql);
}

#[test]
fn unpartitioned_vector_columns_accept_every_search_mode() {
    let db = Database::open_memory();

    for (table, mode) in [
        ("unpartitioned_auto", "AUTO"),
        ("unpartitioned_exact", "EXACT"),
        ("unpartitioned_indexed", "INDEXED"),
    ] {
        let sql = format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE {mode})"
        );
        db.execute(&sql, &empty())
            .unwrap_or_else(|error| panic!("declare {mode} on an unpartitioned vector: {error:?}"));

        let rendered = schema(&db, table);
        if mode == "AUTO" {
            assert!(
                rendered.contains("embedding VECTOR(3)") && !rendered.contains("SEARCH_MODE AUTO"),
                "the default AUTO mode is represented by silence: {rendered}"
            );
        } else {
            assert!(
                rendered.contains(&format!("embedding VECTOR(3) SEARCH_MODE {mode}")),
                "schema must preserve non-default unpartitioned mode {mode}: {rendered}"
            );
        }
        assert!(
            !rendered.contains("PARTITION_KEY") && !rendered.contains("MAX_PARTITIONS"),
            "an unpartitioned vector must not gain partition-only clauses: {rendered}"
        );
    }
}

#[test]
fn nullable_partitioned_vector_added_to_existing_rows_starts_null() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE vector_items (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, kind INTEGER NOT NULL)",
        &empty(),
    )
    .expect("create the existing table");
    db.execute(
        "INSERT INTO vector_items (id, scope_id, kind) VALUES (1, 'project-a', 7)",
        &empty(),
    )
    .expect("seed an existing row");
    db.execute(
        "ALTER TABLE vector_items ADD COLUMN embedding VECTOR(3) PARTITION_KEY (scope_id, kind) SEARCH_MODE AUTO",
        &empty(),
    )
    .expect("add a nullable partitioned vector column");

    let result = db
        .execute("SELECT embedding FROM vector_items", &empty())
        .expect("read the added vector column");
    assert_eq!(
        result.rows,
        vec![vec![Value::Null]],
        "existing rows must receive NULL and create no vector membership"
    );
    assert!(
        schema(&db, "vector_items")
            .contains("embedding VECTOR(3) PARTITION_KEY (scope_id, kind) MAX_PARTITIONS 256")
            && !schema(&db, "vector_items").contains("SEARCH_MODE AUTO"),
        "the added declaration must expose the effective write-refusing cap and leave AUTO silent"
    );
}

#[test]
fn every_exact_identity_type_is_valid_in_a_composite_partition_key() {
    let db = Database::open_memory();
    let sql = "CREATE TABLE typed_partitions (id INTEGER PRIMARY KEY, scope_uuid UUID NOT NULL, scope_text TEXT NOT NULL, scope_integer INTEGER NOT NULL, scope_boolean BOOLEAN NOT NULL, scope_time TIMESTAMP NOT NULL, scope_tx TXID NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_uuid, scope_text, scope_integer, scope_boolean, scope_time, scope_tx) MAX_PARTITIONS 64 SEARCH_MODE EXACT)";
    db.execute(sql, &empty())
        .expect("all exact identity types must be legal partition components");

    assert!(
        schema(&db, "typed_partitions").contains(
            "PARTITION_KEY (scope_uuid, scope_text, scope_integer, scope_boolean, scope_time, scope_tx) MAX_PARTITIONS 64 SEARCH_MODE EXACT"
        ),
        "schema must preserve every component in declaration order"
    );
}

#[test]
fn invalid_partition_components_are_refused_without_partial_schema() {
    let db = Database::open_memory();
    let cases = [
        (
            "missing_key",
            "CREATE TABLE missing_key (id INTEGER PRIMARY KEY, embedding VECTOR(3) PARTITION_KEY (scope_id))",
        ),
        (
            "nullable_key",
            "CREATE TABLE nullable_key (id INTEGER PRIMARY KEY, scope_id TEXT, embedding VECTOR(3) PARTITION_KEY (scope_id))",
        ),
        (
            "real_key",
            "CREATE TABLE real_key (id INTEGER PRIMARY KEY, scope_id REAL NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id))",
        ),
        (
            "json_key",
            "CREATE TABLE json_key (id INTEGER PRIMARY KEY, scope_id JSON NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id))",
        ),
        (
            "vector_key",
            "CREATE TABLE vector_key (id INTEGER PRIMARY KEY, scope_vector VECTOR(3) NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_vector))",
        ),
        (
            "duplicate_key",
            "CREATE TABLE duplicate_key (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id, scope_id))",
        ),
        (
            "self_key",
            "CREATE TABLE self_key (id INTEGER PRIMARY KEY, embedding VECTOR(3) PARTITION_KEY (embedding))",
        ),
    ];
    let mut refusals = Vec::new();

    for (table, sql) in cases {
        let error = db
            .execute(sql, &empty())
            .expect_err("an invalid partition component must be refused");
        assert!(
            db.table_meta(table).is_none(),
            "refusing {table} must leave no partial table metadata"
        );
        refusals.push((sql, error));
    }

    for (sql, error) in refusals {
        assert_typed_declaration_refusal(error, sql);
    }
}

#[test]
fn invalid_partition_limits_are_typed_refusals_without_partial_schema() {
    let db = Database::open_memory();
    let cases = [
        (
            "zero_limit",
            "CREATE TABLE zero_limit (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 0)",
        ),
        (
            "negative_limit",
            "CREATE TABLE negative_limit (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS -1)",
        ),
        (
            "overflow_limit",
            "CREATE TABLE overflow_limit (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 18446744073709551616)",
        ),
    ];
    let mut refusals = Vec::new();

    for (table, sql) in cases {
        let error = db
            .execute(sql, &empty())
            .expect_err("an invalid partition limit must be refused");
        assert!(
            db.table_meta(table).is_none(),
            "refusing {table} must leave no partial table metadata"
        );
        refusals.push((sql, error));
    }

    for (sql, error) in refusals {
        assert_typed_declaration_refusal(error, sql);
    }
}

#[test]
fn online_partition_limit_and_search_mode_changes_update_the_declaration() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE vector_items (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &empty(),
    )
    .expect("declare the initial vector policy");

    db.execute(
        "ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 16",
        &empty(),
    )
    .expect("raise the online partition limit");
    let raised = schema(&db, "vector_items");
    assert!(
        raised.contains("MAX_PARTITIONS 16") && !raised.contains("SEARCH_MODE AUTO"),
        "schema must expose the raised limit while leaving AUTO silent: {raised}"
    );
    assert!(
        !raised.contains("MAX_PARTITIONS 8"),
        "the prior limit must not remain in schema: {raised}"
    );

    for mode in ["EXACT", "INDEXED", "AUTO"] {
        let sql = format!("ALTER TABLE vector_items ALTER COLUMN embedding SET SEARCH_MODE {mode}");
        db.execute(&sql, &empty())
            .unwrap_or_else(|error| panic!("set online search mode with {sql}: {error:?}"));
        let rendered = schema(&db, "vector_items");
        if mode == "AUTO" {
            assert!(
                rendered.contains("MAX_PARTITIONS 16") && !rendered.contains("SEARCH_MODE AUTO"),
                "returning to default AUTO must remove the clause while retaining the cap: {rendered}"
            );
        } else {
            assert!(
                rendered.contains(&format!("MAX_PARTITIONS 16 SEARCH_MODE {mode}")),
                "schema must expose the active non-default search mode {mode}: {rendered}"
            );
        }
    }
}

#[test]
fn missing_insert_parameters_keep_the_existing_typed_refusal() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE required_parameters (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope))",
        &empty(),
    ).unwrap();
    for (sql, bindings, missing) in [
        (
            "INSERT INTO required_parameters (id, scope) VALUES ($id, 1)",
            empty(),
            "id",
        ),
        (
            "INSERT INTO required_parameters (id, scope) VALUES (1, $scope)",
            empty(),
            "scope",
        ),
        (
            "INSERT INTO required_parameters (id, scope) VALUES (1, -$scope)",
            empty(),
            "scope",
        ),
        (
            "INSERT INTO required_parameters (id, scope, embedding) VALUES (1, 1, $vector)",
            empty(),
            "vector",
        ),
    ] {
        let error = db
            .execute(sql, &bindings)
            .expect_err("missing INSERT binding is refused");
        assert!(
            matches!(&error, Error::NotFound(message) if message == &format!("missing parameter: {missing}")),
            "{sql}: {error:?}"
        );
        assert!(
            db.execute("SELECT id FROM required_parameters", &empty())
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(
            db.execute(
                "SHOW VECTOR_PARTITIONS FOR required_parameters.embedding",
                &empty()
            )
            .unwrap()
            .rows
            .is_empty()
        );
    }
    db.execute(
        "INSERT INTO required_parameters (id, scope) VALUES ($id, -$scope)",
        &HashMap::from([
            ("id".to_owned(), Value::Int64(1)),
            ("scope".to_owned(), Value::Int64(2)),
        ]),
    )
    .expect("bound values still reach required-value validation");
    assert_eq!(
        db.execute("SELECT scope FROM required_parameters", &empty())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(-2)]]
    );
}
