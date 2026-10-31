//! A schema author can declare vector crossover and graph work without losing
//! the durable SQL contract that existing vector tables already have.

use contextdb_core::{Error, Value};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

#[test]
fn zero_dimension_vector_remains_an_accepted_schema_and_row_value() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE zero_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(0))",
        &empty(),
    )
    .expect("the existing VECTOR(0) schema contract remains accepted");
    db.execute(
        "INSERT INTO zero_vectors (id, embedding) VALUES (1, $embedding)",
        &HashMap::from([("embedding".to_owned(), Value::Vector(Vec::new()))]),
    )
    .expect("the zero-dimensional row value remains reachable");
    let meta = db
        .table_meta("zero_vectors")
        .expect("read zero-vector schema");
    assert!(matches!(
        meta.columns
            .iter()
            .find(|column| column.name == "embedding")
            .map(|column| &column.column_type),
        Some(contextdb_core::ColumnType::Vector(0))
    ));
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

fn summary_row(db: &Database, table: &str) -> (QueryResult, Vec<Value>) {
    let summary = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the public vector inspection statement succeeds");
    let table_column = summary
        .columns()
        .iter()
        .position(|column| column == "table")
        .expect("vector inspection identifies its table");
    let column_column = summary
        .columns()
        .iter()
        .position(|column| column == "column")
        .expect("vector inspection identifies its column");
    let row = summary
        .rows()
        .iter()
        .find(|row| {
            row[table_column] == Value::Text(table.to_owned())
                && row[column_column] == Value::Text("embedding".to_owned())
        })
        .unwrap_or_else(|| panic!("vector inspection describes {table}.embedding: {summary:?}"));
    let row = row.clone();
    (summary, row)
}

fn assert_declared_values_are_null_when_silent_and_exact_when_set(
    silent: &(QueryResult, Vec<Value>),
    declared: &(QueryResult, Vec<Value>),
    expected: &[i64],
) {
    assert_eq!(
        silent.0.columns(),
        declared.0.columns(),
        "inspection shape is stable"
    );
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
            && !schema(&db, "silent_sq8").contains("IVF"),
        "silence preserves the old SQL spelling"
    );

    db.execute(
 "CREATE TABLE declared_f32 (id INTEGER PRIMARY KEY, scope_id TEXT NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1000 IVF (PROBES = 401))",
        &empty(),
    )
    .expect("CREATE TABLE accepts the complete F32 vector policy");
    let declared_schema = schema(&db, "declared_f32");
    assert!(
        declared_schema.contains(
 "embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1000 IVF (PROBES = 401)"
        ),
        "schema renders every declared vector clause in canonical order: {declared_schema}"
    );
    assert_declared_values_are_null_when_silent_and_exact_when_set(
        &summary_row(&db, "silent_f32"),
        &summary_row(&db, "declared_f32"),
        &[1000, 401],
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
 "ALTER TABLE add_target ADD COLUMN embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 9 AUTO_INDEX_AT 5001 IVF (LISTS = 12, PROBES = 64)",
        &empty(),
    )
 .expect("ALTER TABLE ADD COLUMN accepts a partial SQ8 IVF declaration");
    assert_eq!(
        db.execute("SELECT embedding FROM add_target", &empty())
            .expect("read nullable added vector")
            .rows(),
        vec![vec![Value::Null]],
        "existing rows remain NULL after a nullable vector policy declaration"
    );
    let added_schema = schema(&db, "add_target");
    assert!(
        added_schema.contains(
 "embedding VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope_id) MAX_PARTITIONS 9 AUTO_INDEX_AT 5001 IVF (LISTS = 12, PROBES = 64)"
        ),
        "the add-column declaration keeps quantization before policy and only its partial IVF members: {added_schema}"
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
 "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 1000 IVF (LISTS = 32, PROBES = 211))",
        &empty(),
    )
    .expect("declare an explicit complete policy");

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (REFINE_FACTOR = 2.5)",
        &empty(),
    )
    .expect("partial IVF ALTER keeps omitted members");
    let partial = schema(&db, "policy_items");
    assert!(
        partial.contains("IVF (LISTS = 32, PROBES = 211, REFINE_FACTOR = 2.5"),
        "omitted IVF members retain their declaration: {partial}"
    );

    db.execute(
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = DEFAULT)",
        &empty(),
    )
    .expect("a member DEFAULT clears only that member");
    let member_default = schema(&db, "policy_items");
    assert!(
        member_default.contains("LISTS = 32") && !member_default.contains("PROBES"),
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
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF DEFAULT",
        &empty(),
    )
    .expect("IVF DEFAULT clears the entire declared group");
    let cleared = schema(&db, "policy_items");
    assert!(
        !cleared.contains("AUTO_INDEX_AT") && !cleared.contains("IVF"),
        "clearing both declarations returns the schema to compatibility silence: {cleared}"
    );
}

#[test]
fn invalid_policy_values_refuse_atomically_without_changing_schema_or_inspection() {
    let db = Database::open_memory();
    db.execute(
 "CREATE TABLE policy_items (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 1000 IVF (PROBES = 211))",
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
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (LISTS = 0)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (PROBES = 0)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET IVF (REFINE_FACTOR = 0.99)",
        "ALTER TABLE policy_items ALTER COLUMN embedding SET SEARCH (EXPECTED_VECTORS = 0)",
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
    use contextdb_core::VectorIndexRef;

    let db = Database::open_memory();
    db.execute("CREATE TABLE add_policy (id INTEGER PRIMARY KEY)", &empty())
        .expect("create add-column target");
    db.execute("INSERT INTO add_policy VALUES (1)", &empty())
        .expect("seed the row that every refusal must preserve");
    let before = schema_identity(&db, "add_policy");
    let invalid: Vec<(&str, &str)> = vec![
        ("AUTO_INDEX_AT 0", "AUTO_INDEX_AT must be a positive integer"),
        ("AUTO_INDEX_AT -1", "AUTO_INDEX_AT must be a positive integer"),
        (
            "AUTO_INDEX_AT 4294967296",
            "AUTO_INDEX_AT is outside the persisted numeric range",
        ),
        ("IVF (LISTS = 0)", "LISTS"),
        ("IVF (PROBES = 0)", "PROBES"),
        ("IVF (REFINE_FACTOR = 0.99)", "REFINE_FACTOR"),
        ("SEARCH (EXPECTED_VECTORS = 0)", "EXPECTED_VECTORS"),
    ];
    for quantization in ["F32", "SQ8", "SQ4"] {
        for (policy, expected) in &invalid {
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
                        if index == &VectorIndexRef::new(table, "embedding")
                            && issue.to_string().contains(expected)),
                    "{sql}: expected a refusal naming {expected}, got {error:?}"
                );
                assert!(db.table_meta("create_policy").is_none());
                assert_eq!(schema_identity(&db, "add_policy"), before);
                assert_eq!(
                    db.execute("SELECT id FROM add_policy", &empty())
                        .unwrap()
                        .rows(),
                    vec![vec![Value::Int64(1)]]
                );
                assert!(
                    db.execute("SHOW VECTOR_INDEXES", &empty())
                        .unwrap()
                        .rows()
                        .is_empty()
                );
            }
        }
    }
    for sql in [
        "CREATE TABLE create_policy (id INTEGER PRIMARY KEY, embedding VECTOR(3) AUTO_INDEX_AT 4294967295 IVF (PROBES = 4294967295))",
        "ALTER TABLE add_policy ADD COLUMN embedding VECTOR(3) AUTO_INDEX_AT 4294967295 IVF (PROBES = 4294967295)",
    ] {
        db.execute(sql, &empty())
            .expect("the persisted u32 maximum has no smaller private policy cap");
    }
}
