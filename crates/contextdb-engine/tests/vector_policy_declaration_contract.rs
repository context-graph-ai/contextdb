//! A schema author can declare vector crossover and graph work without losing
//! the durable SQL contract that existing vector tables already have.

use contextdb_core::{Error, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn schema(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table metadata exists for {table}"));
    render_table_meta(table, &meta)
}

fn schema_identity(db: &Database, table: &str) -> Vec<u8> {
    rmp_serde::to_vec(
        &db.table_meta(table)
            .unwrap_or_else(|| panic!("table metadata exists for {table}")),
    )
    .expect("table metadata has a stable persisted encoding")
}

fn summary_row(db: &Database, table: &str) -> (Vec<String>, Vec<Value>) {
    let summary = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the public vector inspection statement succeeds");
    let table_column = summary
        .columns
        .iter()
        .position(|column| column == "table")
        .expect("vector inspection identifies its table");
    let column_column = summary
        .columns
        .iter()
        .position(|column| column == "column")
        .expect("vector inspection identifies its column");
    let row = summary
        .rows
        .iter()
        .find(|row| {
            row[table_column] == Value::Text(table.to_owned())
                && row[column_column] == Value::Text("embedding".to_owned())
        })
        .unwrap_or_else(|| panic!("vector inspection describes {table}.embedding: {summary:?}"));
    (summary.columns, row.clone())
}

fn assert_declared_values_are_null_when_silent_and_exact_when_set(
    silent: &(Vec<String>, Vec<Value>),
    declared: &(Vec<String>, Vec<Value>),
    expected: &[i64],
) {
    assert_eq!(silent.0, declared.0, "inspection shape is stable");
    assert_eq!(
        silent.1.len(),
        declared.1.len(),
        "inspection row shape is stable"
    );

    for value in expected {
        let field = silent
            .1
            .iter()
            .zip(&declared.1)
            .position(|(silent, declared)| {
                *silent == Value::Null && *declared == Value::Int64(*value)
            });
        assert!(
            field.is_some(),
            "the public summary exposes declared policy value {value} as NULL when silent and exactly {value} when declared\nsilent: {silent:?}\ndeclared: {declared:?}"
        );
    }
}

fn assert_typed_policy_refusal(error: Error, sql: &str) {
    assert!(
        !matches!(
            &error,
            Error::ParseError(_) | Error::PlanError(_) | Error::Other(_)
        ),
        "invalid vector policy DDL reaches semantic validation and returns a typed refusal, not a parser, planner, or generic error\nSQL: {sql}\nerror: {error:?}"
    );
}

#[test]
fn create_and_add_column_declarations_render_inspect_and_survive_reopen_and_export() {
    let root = tempfile::tempdir().expect("temporary policy store");
    let path = root.path().join("policy.db");
    let export = root.path().join("policy.snapshot");
    let db = Database::open(&path).expect("open policy store");

    // Existing silent vector DDL is the control: new policy syntax must not
    // change what a peer or restored database sees when the author chose none.
    db.execute(
        "CREATE TABLE silent_f32 (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .expect("existing vector declaration remains executable");
    db.execute(
        "CREATE TABLE silent_sq8 (id INTEGER PRIMARY KEY, embedding VECTOR(3) WITH (quantization = 'SQ8'))",
        &empty(),
    )
    .expect("existing quantized vector declaration remains executable");
    assert_eq!(
        schema(&db, "silent_f32"),
        "CREATE TABLE silent_f32 (\n  id INTEGER PRIMARY KEY,\n  embedding VECTOR(3)\n);\n"
    );
    assert!(
        !schema(&db, "silent_sq8").contains("AUTO_INDEX_AT")
            && !schema(&db, "silent_sq8").contains("HNSW"),
        "silence preserves the old SQL spelling"
    );

    db.execute(
        "CREATE TABLE declared_f32 (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1000 HNSW (M = 24, EF_CONSTRUCTION = 400, EF_SEARCH = 401))",
        &empty(),
    )
    .expect("CREATE TABLE accepts the complete F32 vector policy");
    let declared_schema = schema(&db, "declared_f32");
    assert!(
        declared_schema.contains(
            "embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1000 HNSW (M = 24, EF_CONSTRUCTION = 400, EF_SEARCH = 401)"
        ),
        "schema renders every declared vector clause in canonical order: {declared_schema}"
    );
    assert_declared_values_are_null_when_silent_and_exact_when_set(
        &summary_row(&db, "silent_f32"),
        &summary_row(&db, "declared_f32"),
        &[1000, 24, 400, 401],
    );

    db.execute(
        "CREATE TABLE add_target (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL)",
        &empty(),
    )
    .expect("create the existing table for add-column control");
    db.execute(
        "INSERT INTO add_target (id, scope_id) VALUES (1, 'camera-a')",
        &empty(),
    )
    .expect("seed an existing row before adding its nullable vector");
    db.execute(
        "ALTER TABLE add_target ADD COLUMN embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 9 AUTO_INDEX_AT 5001 HNSW (M = 12, EF_CONSTRUCTION = 64)",
        &empty(),
    )
    .expect("ALTER TABLE ADD COLUMN accepts a partial SQ8 HNSW declaration");
    assert_eq!(
        db.execute("SELECT embedding FROM add_target", &empty())
            .expect("read nullable added vector")
            .rows,
        vec![vec![Value::Null]],
        "existing rows remain NULL after a nullable vector policy declaration"
    );
    let added_schema = schema(&db, "add_target");
    assert!(
        added_schema.contains(
            "embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 9 AUTO_INDEX_AT 5001 HNSW (M = 12, EF_CONSTRUCTION = 64)"
        ),
        "the add-column declaration keeps quantization before policy and only its partial HNSW members: {added_schema}"
    );
    assert_declared_values_are_null_when_silent_and_exact_when_set(
        &summary_row(&db, "silent_sq8"),
        &summary_row(&db, "add_target"),
        &[5001, 12, 64],
    );

    let before_identity = schema_identity(&db, "declared_f32");
    let before_schema = schema(&db, "declared_f32");
    db.close().expect("close durable policy store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen durable policy store");
    assert_eq!(schema(&reopened, "declared_f32"), before_schema);
    assert_eq!(schema_identity(&reopened, "declared_f32"), before_identity);
    reopened
        .export_snapshot(&export)
        .expect("export policy declarations through the public checkpoint surface");
    let restored = Database::open(&export).expect("open exported policy artifact");
    assert_eq!(schema(&restored, "declared_f32"), before_schema);
    assert_eq!(schema_identity(&restored, "declared_f32"), before_identity);
}

#[test]
fn partial_alters_member_defaults_and_group_defaults_change_only_the_declared_policy() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 1000 HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 211))",
        &empty(),
    )
    .expect("declare an explicit complete policy");

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
        &empty(),
    )
    .expect("partial HNSW ALTER keeps omitted members");
    let partial = schema(&db, "policy_items");
    assert!(
        partial.contains("HNSW (M = 24, EF_CONSTRUCTION = 400, EF_SEARCH = 211)"),
        "omitted HNSW members retain their declaration: {partial}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (EF_SEARCH = DEFAULT)",
        &empty(),
    )
    .expect("a member DEFAULT clears only that member");
    let member_default = schema(&db, "policy_items");
    assert!(
        member_default.contains("HNSW (M = 24, EF_CONSTRUCTION = 400)")
            && !member_default.contains("EF_SEARCH"),
        "member DEFAULT leaves the other declared members intact: {member_default}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT DEFAULT",
        &empty(),
    )
    .expect("AUTO_INDEX_AT DEFAULT restores the compatibility declaration silence");
    assert!(
        !schema(&db, "policy_items").contains("AUTO_INDEX_AT"),
        "the default crossover is rendered as silence"
    );
    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW DEFAULT",
        &empty(),
    )
    .expect("HNSW DEFAULT clears the entire declared group");
    let cleared = schema(&db, "policy_items");
    assert!(
        !cleared.contains("AUTO_INDEX_AT") && !cleared.contains("HNSW"),
        "clearing both declarations returns the schema to compatibility silence: {cleared}"
    );
}

#[test]
fn invalid_policy_values_refuse_atomically_without_changing_schema_or_inspection() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 1000 HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 211))",
        &empty(),
    )
    .expect("declare the policy whose state must survive each refusal");
    let original_schema = schema(&db, "policy_items");
    let original_identity = schema_identity(&db, "policy_items");
    let original_summary = summary_row(&db, "policy_items");

    for sql in [
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 0",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT -1",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET AUTO_INDEX_AT 18446744073709551616",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (M = 0)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (EF_CONSTRUCTION = -1)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (M = 401, EF_CONSTRUCTION = 400)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET HNSW (M = 18446744073709551616)",
    ] {
        let error = db
            .execute(sql, &empty())
            .expect_err("invalid policy is refused");
        assert_typed_policy_refusal(error, sql);
        assert_eq!(
            schema(&db, "policy_items"),
            original_schema,
            "{sql} is atomic"
        );
        assert_eq!(
            schema_identity(&db, "policy_items"),
            original_identity,
            "{sql} preserves schema identity"
        );
        assert_eq!(
            summary_row(&db, "policy_items").1,
            original_summary.1,
            "{sql} preserves public declared-policy inspection"
        );
    }
}

#[test]
fn invalid_create_and_add_policies_refuse_before_publishing_schema_or_rows() {
    use contextdb_core::{VectorIndexRef, VectorPolicyDeclarationIssue as Issue};

    let db = Database::open_memory();
    db.execute("CREATE TABLE add_policy (id INTEGER PRIMARY KEY)", &empty())
        .expect("create add-column target");
    db.execute("INSERT INTO add_policy VALUES (1)", &empty())
        .expect("seed the row that every refusal must preserve");
    let before = schema_identity(&db, "add_policy");
    let mut invalid = vec![
        ("AUTO_INDEX_AT 0".to_owned(), Issue::AutoIndexAtNotPositive),
        ("AUTO_INDEX_AT -1".to_owned(), Issue::AutoIndexAtNotPositive),
        (
            "AUTO_INDEX_AT 4294967296".to_owned(),
            Issue::AutoIndexAtOutOfRange,
        ),
        (
            "HNSW (M = 25, EF_CONSTRUCTION = 24)".to_owned(),
            Issue::EfConstructionBelowM,
        ),
    ];
    for member in ["M", "EF_CONSTRUCTION", "EF_SEARCH"] {
        for (value, issue) in [
            ("0", Issue::HnswValueNotPositive),
            ("-1", Issue::HnswValueNotPositive),
            ("4294967296", Issue::HnswValueOutOfRange),
        ] {
            invalid.push((format!("HNSW ({member} = {value})"), issue));
        }
    }
    for quantization in ["F32", "SQ8", "SQ4"] {
        let mut cases = invalid.clone();
        // Each partial policy is valid in the small profile but invalid in
        // another profile, even though the new column currently has no rows.
        let ef = if quantization == "F32" { 20 } else { 10 };
        cases.push((
            format!("HNSW (EF_CONSTRUCTION = {ef})"),
            Issue::EfConstructionBelowM,
        ));
        for (policy, expected_issue) in cases {
            let vector = format!("VECTOR(3) WITH (quantization = '{quantization}') {policy}");
            for (table, sql) in [
                (
                    "create_policy",
                    format!(
                        "CREATE TABLE create_policy (id INTEGER PRIMARY KEY, embedding {vector})"
                    ),
                ),
                (
                    "add_policy",
                    format!("ALTER TABLE add_policy ADD COLUMN embedding {vector}"),
                ),
            ] {
                let error = db
                    .execute(&sql, &empty())
                    .expect_err("invalid policy is refused");
                assert!(
                    matches!(&error, Error::InvalidVectorPolicyDeclaration { index, issue }
                        if index == &VectorIndexRef::new(table, "embedding") && *issue == expected_issue),
                    "{sql}: expected {expected_issue:?}, got {error:?}"
                );
                assert!(db.table_meta("create_policy").is_none());
                assert_eq!(schema_identity(&db, "add_policy"), before);
                assert_eq!(
                    db.execute("SELECT id FROM add_policy", &empty())
                        .unwrap()
                        .rows,
                    vec![vec![Value::Int64(1)]]
                );
                assert!(
                    db.execute("SHOW VECTOR_INDEXES", &empty())
                        .unwrap()
                        .rows
                        .is_empty()
                );
            }
        }
    }
    for sql in [
        "CREATE TABLE create_policy (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 4294967295 HNSW (M = 4294967295, EF_CONSTRUCTION = 4294967295, EF_SEARCH = 4294967295))",
        "ALTER TABLE add_policy ADD COLUMN embedding VECTOR(3) AUTO_INDEX_AT 4294967295 HNSW (M = 4294967295, EF_CONSTRUCTION = 4294967295, EF_SEARCH = 4294967295)",
    ] {
        db.execute(sql, &empty())
            .expect("the persisted u32 maximum has no smaller private policy cap");
    }
}

fn serving_hnsw_m(db: &Database, table: &str) -> Option<i64> {
    let detail = db
        .execute(
            &format!("SHOW VECTOR_PARTITIONS FOR {table}.embedding"),
            &empty(),
        )
        .expect("inspect the maintained vector partition");
    let column = detail
        .columns
        .iter()
        .position(|name| name == "serving_hnsw_m")
        .expect("partition inspection exposes serving_hnsw_m");
    match detail.rows.first().and_then(|row| row.get(column)) {
        Some(Value::Int64(value)) => Some(*value),
        Some(Value::Null) | None => None,
        other => panic!("serving_hnsw_m has an unexpected value: {other:?}"),
    }
}

#[test]
fn full_width_hnsw_m_builds_reloads_and_accounts_at_255_256_and_257() {
    let root = tempfile::tempdir().expect("temporary full-width HNSW store");
    let path = root.path().join("full-width-hnsw-m.db");
    let db = Database::open(&path).expect("open full-width HNSW store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for m in [255_i64, 256, 257] {
        let table = format!("wide_m_{m}");
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = {m}, EF_CONSTRUCTION = {m}, EF_SEARCH = 32))"
            ),
            &empty(),
        )
        .expect("declare a representable full-width HNSW policy");
        let tx = db.begin_or_panic();
        for ordinal in 0..8_i64 {
            db.execute_in_tx(
                tx,
                &format!("INSERT INTO {table} VALUES ($id, $embedding)"),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(ordinal)),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vec![1.0, ordinal as f32 * 0.01, m as f32 * 0.00001]),
                    ),
                ]),
            )
            .expect("insert a deterministic vector");
        }
        db.commit(tx).expect("commit the full-width HNSW fixture");
    }
    for _ in 0..64 {
        if [255_i64, 256, 257]
            .into_iter()
            .all(|m| serving_hnsw_m(&db, &format!("wide_m_{m}")) == Some(m))
        {
            break;
        }
        db.run_maintenance_cycle()
            .expect("build one caller-driven HNSW generation");
    }
    for m in [255_i64, 256, 257] {
        assert_eq!(
            serving_hnsw_m(&db, &format!("wide_m_{m}")),
            Some(m),
            "the built graph reports the declared full-width M"
        );
    }
    db.close().expect("close the full-width HNSW store");
    drop(db);

    let reopened = Database::open(&path).expect("reopen the full-width HNSW store");
    for m in [255_i64, 256, 257] {
        let table = format!("wide_m_{m}");
        let rows = reopened
            .execute(
                &format!(
                    "SELECT id FROM {table} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
                ),
                &HashMap::from([(
                    "query".to_owned(),
                    Value::Vector(vec![1.0, 0.0, 0.0]),
                )]),
            )
            .expect("reload and search the owned full-width graph");
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(
            serving_hnsw_m(&reopened, &table),
            Some(m),
            "reload and resident accounting preserve the declared M"
        );
    }
}
