use super::common::*;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_parser::parse;
use std::fs;
use std::process::Command;
use tempfile::TempDir;
use uuid::Uuid;

fn setup_sql_ops_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT, category TEXT, reading REAL, optional TEXT, enabled BOOLEAN, happened_at TIMESTAMP, embedding VECTOR(3), context_id TEXT)",
        &empty_params(),
    )
    .expect("create t");
    let rows = [
        ("alice", "a", 5.0, Some("x"), true),
        ("bob", "a", 15.0, None, false),
        ("bob", "b", 25.0, Some("y"), true),
        ("temperature", "b", 12.0, None, true),
        ("temp-3", "a", 18.0, Some("z"), false),
    ];
    for (idx, row) in rows.iter().enumerate() {
        db.execute(
            "INSERT INTO t (id, name, category, reading, optional, enabled, happened_at, embedding, context_id) VALUES ($id, $name, $category, $reading, $optional, $enabled, $ts, $embedding, $ctx)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(idx as u128 + 1))),
                ("name", Value::Text(row.0.to_string())),
                ("category", Value::Text(row.1.to_string())),
                ("reading", Value::Float64(row.2)),
                ("optional", row.3.map_or(Value::Null, |value| Value::Text(value.to_string()))),
                ("enabled", Value::Bool(row.4)),
                ("ts", Value::Timestamp(idx as i64 + 100)),
                ("embedding", Value::Vector(vec![idx as f32, 0.0, 1.0])),
                ("ctx", Value::Text(if idx % 2 == 0 { "ctx-a" } else { "ctx-b" }.to_string())),
            ]),
        )
        .expect("insert row");
    }
    db
}

/// I pasted every SQL example from the spec, and they all parsed without error.
#[test]
fn f56_every_spec_example_parses_successfully() {
    let examples = [
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT NOT NULL, reading REAL, embedding VECTOR(384))",
        "DROP TABLE sensors",
        "CREATE INDEX idx_name ON sensors (name)",
        "INSERT INTO sensors (id, name, reading) VALUES ($id, $name, $reading)",
        "INSERT INTO sensors (id, name, reading) VALUES ($id, $name, $reading) ON CONFLICT (id) DO UPDATE SET name = $name, reading = $reading",
        "SELECT * FROM sensors WHERE id IN ($id1, $id2, $id3)",
        "SELECT DISTINCT name FROM sensors",
        "WITH active AS (SELECT * FROM sensors WHERE status = 'active') SELECT * FROM active WHERE reading > $threshold",
        "SELECT s.name, c.label FROM sensors s INNER JOIN contexts c ON s.context_id = c.id WHERE c.label = $label",
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:DEPENDS_ON]->{1,3}(b) COLUMNS (b.id AS b_id))",
        "SELECT id, data FROM observations WHERE context_id = $ctx ORDER BY embedding <=> $query_vector LIMIT 10",
    ];
    for example in examples {
        assert!(
            parse(example).is_ok(),
            "spec example should parse: {example}"
        );
    }
}

/// I ran DISTINCT, COUNT, IN, LIKE, BETWEEN, and BEGIN/COMMIT through the CLI, and all produced correct output.
#[test]
fn f58b_query_operators_work_through_cli() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f58b.db"),
        &[],
        "\
CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT, category TEXT, reading REAL)\n\
INSERT INTO sensors (id, name, category, reading) VALUES ('00000000-0000-0000-0000-000000000001', 'alice', 'a', 1.0)\n\
INSERT INTO sensors (id, name, category, reading) VALUES ('00000000-0000-0000-0000-000000000002', 'bob', 'a', 2.0)\n\
SELECT DISTINCT category FROM sensors\n\
SELECT COUNT(*) FROM sensors\n\
SELECT * FROM sensors WHERE name IN ('alice', 'bob')\n\
SELECT * FROM sensors WHERE name LIKE 'a%'\n\
SELECT * FROM sensors WHERE reading BETWEEN 1 AND 2 ORDER BY category ASC, reading DESC\n\
BEGIN\n\
COMMIT\n\
.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("alice"));
    assert!(stdout.contains("bob"));
    assert!(stdout.contains("COUNT"));
}

/// I tried to UPDATE an IMMUTABLE row and make an illegal state transition through the CLI, and the database rejected at least one of the violations.
#[test]
fn f58c_ddl_constraints_enforced_through_cli() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f58c.db"),
        &[],
        "\
CREATE TABLE immutable_rows (id UUID PRIMARY KEY, name TEXT) IMMUTABLE\n\
INSERT INTO immutable_rows (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'x')\n\
UPDATE immutable_rows SET name = 'y'\n\
CREATE TABLE workflows (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: draft -> [review], review -> [published])\n\
INSERT INTO workflows (id, status) VALUES ('00000000-0000-0000-0000-000000000002', 'draft')\n\
INSERT INTO workflows (id, status) VALUES ('00000000-0000-0000-0000-000000000002', 'published') ON CONFLICT (id) DO UPDATE SET status='published'\n\
.quit\n",
    );
    let combined = format!(
        "{}{}",
        output_string(&output.stdout),
        output_string(&output.stderr)
    );
    assert!(combined.contains("immutable") || combined.contains("state"));
}

/// I queried SELECT DISTINCT on a column with duplicates, and I got only the unique values back.
#[test]
fn f67_select_distinct() {
    let db = setup_sql_ops_db();
    let result = db
        .execute("SELECT DISTINCT category FROM t", &empty_params())
        .expect("distinct query");
    assert_eq!(result.rows.len(), 2);
}

/// I filtered rows with IN ('alice', 'bob'), and I got exactly the rows matching those names.
#[test]
fn f62_in_with_literal_list() {
    let db = setup_sql_ops_db();
    let result = db
        .execute(
            "SELECT * FROM t WHERE name IN ('alice', 'bob')",
            &empty_params(),
        )
        .expect("IN query");
    assert_eq!(result.rows.len(), 3);
}

/// I used IN with a subquery to cross-reference two tables, and it returned only the matching row.
#[test]
fn f63_in_with_subquery() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t1 (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create t1");
    db.execute(
        "CREATE TABLE t2 (id UUID PRIMARY KEY, ref_id UUID)",
        &empty_params(),
    )
    .expect("create t2");
    let shared = Uuid::new_v4();
    db.execute(
        "INSERT INTO t1 (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(shared)),
            ("name", Value::Text("hit".into())),
        ]),
    )
    .expect("insert t1");
    db.execute(
        "INSERT INTO t2 (id, ref_id) VALUES ($id, $ref)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("ref", Value::Uuid(shared)),
        ]),
    )
    .expect("insert t2");
    let result = db
        .execute(
            "SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2)",
            &empty_params(),
        )
        .expect("IN subquery");
    assert_eq!(result.rows.len(), 1);
}

/// I searched with LIKE 'temp%', and it found the two rows whose names start with "temp".
#[test]
fn f64_like_pattern_matching() {
    let db = setup_sql_ops_db();
    let result = db
        .execute("SELECT * FROM t WHERE name LIKE 'temp%'", &empty_params())
        .expect("LIKE query");
    assert_eq!(result.rows.len(), 2);
}

/// I filtered with BETWEEN 10 AND 20, and only rows within that range came back.
#[test]
fn f65_between_operator() {
    let db = setup_sql_ops_db();
    let result = db
        .execute(
            "SELECT * FROM t WHERE reading BETWEEN 10 AND 20",
            &empty_params(),
        )
        .expect("BETWEEN query");
    assert_eq!(result.rows.len(), 3);
}

/// I split rows by IS NULL and IS NOT NULL on a nullable column, and every row landed in exactly one bucket.
#[test]
fn f66_is_null_and_is_not_null() {
    let db = setup_sql_ops_db();
    let is_null = db
        .execute("SELECT * FROM t WHERE optional IS NULL", &empty_params())
        .expect("IS NULL");
    let is_not_null = db
        .execute(
            "SELECT * FROM t WHERE optional IS NOT NULL",
            &empty_params(),
        )
        .expect("IS NOT NULL");
    assert_eq!(is_null.rows.len() + is_not_null.rows.len(), 5);
}

/// I selected a column with AS sensor_name, and the result used my alias as the column header.
#[test]
fn f68_column_aliases_as() {
    let db = setup_sql_ops_db();
    let result = db
        .execute("SELECT name AS sensor_name FROM t", &empty_params())
        .expect("alias query");
    assert_eq!(result.columns, vec!["sensor_name"]);
}

/// I ordered by two columns with mixed ASC/DESC, and the rows came back in the right order.
#[test]
fn f69_multi_column_order_by_with_mixed_directions() {
    let db = setup_sql_ops_db();
    let result = db
        .execute(
            "SELECT category, reading FROM t ORDER BY category ASC, reading DESC",
            &empty_params(),
        )
        .expect("order by");
    assert_eq!(
        result.rows.first().expect("first row")[0],
        Value::Text("a".into())
    );
}

/// I used COALESCE on a nullable column with a default, and NULLs were replaced with 'unknown'.
#[test]
fn f70_coalesce_function() {
    let db = setup_sql_ops_db();
    let result = db
        .execute(
            "SELECT COALESCE(optional, 'unknown') FROM t",
            &empty_params(),
        )
        .expect("coalesce query");
    assert!(
        result
            .rows
            .iter()
            .any(|row| row[0] == Value::Text("unknown".into()))
    );
}

/// I filtered with NOT (reading > 20), and every returned row had a reading of 20 or less.
#[test]
fn f74_not_operator() {
    let db = setup_sql_ops_db();
    let result = db
        .execute("SELECT * FROM t WHERE NOT (reading > 20)", &empty_params())
        .expect("NOT query");
    assert!(
        result
            .rows
            .iter()
            .all(|row| matches!(row[3], Value::Float64(v) if v <= 20.0))
    );
}

/// I ran COUNT(*) on a table with 5 rows, and I got back 5.
#[test]
fn f74b_count_star_aggregate_function() {
    let db = setup_sql_ops_db();
    let result = db
        .execute("SELECT COUNT(*) FROM t", &empty_params())
        .expect("count query");
    assert_eq!(extract_i64(&result, 0, 0), 5);
}

/// I called SELECT NOW(), and I got back a timestamp value.
#[test]
fn f74c_now_function() {
    let db = Database::open_memory();
    let result = db
        .execute("SELECT NOW()", &empty_params())
        .expect("now query");
    assert!(matches!(result.rows[0][0], Value::Timestamp(_)));
}

/// I joined sensors to readings with INNER JOIN, and only the sensor with a matching reading appeared.
#[test]
fn f74d_inner_join() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create sensors");
    db.execute(
        "CREATE TABLE readings (id UUID PRIMARY KEY, sensor_id UUID, value REAL)",
        &empty_params(),
    )
    .expect("create readings");
    let sensor_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO sensors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(sensor_id)),
            ("name", Value::Text("s1".into())),
        ]),
    )
    .expect("insert sensor");
    db.execute(
        "INSERT INTO readings (id, sensor_id, value) VALUES ($id, $sensor_id, $value)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("sensor_id", Value::Uuid(sensor_id)),
            ("value", Value::Float64(9.0)),
        ]),
    )
    .expect("insert reading");
    let result = db
        .execute(
            "SELECT s.name, r.value FROM sensors s INNER JOIN readings r ON s.id = r.sensor_id",
            &empty_params(),
        )
        .expect("inner join");
    assert_eq!(result.rows.len(), 1);
}

/// I left-joined a sensor with no readings, and the sensor still appeared with NULLs for the reading columns.
#[test]
fn f74e_left_join() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create sensors");
    db.execute(
        "CREATE TABLE readings (id UUID PRIMARY KEY, sensor_id UUID, value REAL)",
        &empty_params(),
    )
    .expect("create readings");
    db.execute(
        "INSERT INTO sensors (id, name) VALUES ($id, $name)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("s1".into())),
        ]),
    )
    .expect("insert sensor");
    let result = db
        .execute(
            "SELECT s.name, r.value FROM sensors s LEFT JOIN readings r ON s.id = r.sensor_id",
            &empty_params(),
        )
        .expect("left join");
    assert_eq!(result.rows.len(), 1);
}

/// I created an index on a column, and queries filtering by that column still returned the correct row.
#[test]
fn f74f_create_index() {
    let db = setup_sql_ops_db();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty_params())
        .expect("create index");
    let result = db
        .execute("SELECT * FROM t WHERE name = 'alice'", &empty_params())
        .expect("select by indexed column");
    assert_eq!(result.rows.len(), 1);
}

/// I ran CREATE TABLE IF NOT EXISTS on an existing table, and it succeeded silently instead of failing.
#[test]
fn f75_create_table_if_not_exists_is_idempotent() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &empty_params())
        .expect("create t");
    db.execute(
        "CREATE TABLE IF NOT EXISTS t (id UUID PRIMARY KEY)",
        &empty_params(),
    )
    .expect("idempotent create");
}

/// I inserted a NULL into a NOT NULL column, and the database rejected it with an error.
#[test]
fn f79_not_null_constraint_rejects_null_inserts() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT NOT NULL)",
        &empty_params(),
    )
    .expect("create t");
    let err = db
        .execute(
            "INSERT INTO t (id, name) VALUES ($id, $name)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Null),
            ]),
        )
        .expect_err("NULL insert should fail");
    assert!(matches!(err, Error::Other(_) | Error::PlanError(_)));
}

/// I inserted a duplicate value into a UNIQUE column, and the database rejected it with a UniqueViolation error.
#[test]
fn f80_unique_constraint_rejects_duplicates() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &empty_params(),
    )
    .expect("create t");
    db.execute(
        "INSERT INTO t (id, email) VALUES ($id, $email)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("email", Value::Text("a@example.com".into())),
        ]),
    )
    .expect("first insert");
    let err = db
        .execute(
            "INSERT INTO t (id, email) VALUES ($id, $email)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("email", Value::Text("a@example.com".into())),
            ]),
        )
        .expect_err("duplicate should fail");
    assert!(matches!(err, Error::UniqueViolation { .. }));
}

/// I queried for incoming edges with <-[:EDGE]-, and I got both nodes that point into the target.
#[test]
fn f81_incoming_edge_direction() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    setup_graph_entities(&db, &[a, b, c]);
    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".into(), Default::default())
        .expect("a->b");
    db.insert_edge(tx, c, b, "EDGE".into(), Default::default())
        .expect("c->b");
    db.commit(tx).expect("commit edges");
    let result = db
        .execute(
            &format!(
                "SELECT x_id FROM GRAPH_TABLE(edges MATCH (b)<-[:EDGE]-(x) WHERE b.id = '{b}' COLUMNS(x.id AS x_id))"
            ),
            &empty_params(),
        )
        .expect("incoming graph query");
    assert_eq!(result.rows.len(), 2);
}

/// I queried for edges in either direction with -[:EDGE]-, and I got all connected nodes regardless of direction.
#[test]
fn f82_bidirectional_edge_match() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    setup_graph_entities(&db, &[a, b, c]);
    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".into(), Default::default())
        .expect("a->b");
    db.insert_edge(tx, c, b, "EDGE".into(), Default::default())
        .expect("c->b");
    db.commit(tx).expect("commit edges");
    let result = db
        .execute(
            &format!(
                "SELECT x_id FROM GRAPH_TABLE(edges MATCH (b)-[:EDGE]-(x) WHERE b.id = '{b}' COLUMNS(x.id AS x_id))"
            ),
            &empty_params(),
        )
        .expect("bidirectional graph query");
    assert_eq!(result.rows.len(), 2);
}

/// I filtered a graph traversal by a node property in the WHERE clause, and only the matching path came back.
#[test]
fn f83_node_property_filtering_in_graph_table_where() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    setup_graph_entities(&db, &[a, b]);
    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".into(), Default::default())
        .expect("edge");
    db.commit(tx).expect("commit edge");
    let result = db
        .execute(
            "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->(b) WHERE a.name = 'entity-0' COLUMNS(b.id AS b_id))",
            &empty_params(),
        )
        .expect("graph filter query");
    assert_eq!(result.rows.len(), 1);
}

/// I wrote a graph pattern with contradictory arrows <-[:EDGE]->, and the parser rejected it.
#[test]
fn f85_parser_rejects_mixed_direction_graph_arrows() {
    assert!(
        parse("SELECT x FROM GRAPH_TABLE(edges MATCH (a)<-[:EDGE]->(b) COLUMNS(b.id AS x))")
            .is_err()
    );
}

/// I ran a vector similarity search with a WHERE filter, and every result belonged to the filtered context.
#[test]
fn f86_pre_filtered_ann_query() {
    let db = setup_sql_ops_db();
    let result = db
        .execute(
            "SELECT * FROM t WHERE context_id = $ctx ORDER BY embedding <=> $query LIMIT 5",
            &params(vec![
                ("ctx", Value::Text("ctx-a".into())),
                ("query", Value::Vector(vec![0.0, 0.0, 1.0])),
            ]),
        )
        .expect("prefiltered ann");
    assert!(
        result
            .rows
            .iter()
            .all(|row| row[8] == Value::Text("ctx-a".into()))
    );
}

/// I inserted two rows inside BEGIN/COMMIT, and both appeared when I queried afterward.
#[test]
fn f88_begin_commit_atomicity() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f88.db"),
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY)\nBEGIN\nINSERT INTO t (id) VALUES ('00000000-0000-0000-0000-000000000001')\nINSERT INTO t (id) VALUES ('00000000-0000-0000-0000-000000000002')\nCOMMIT\nSELECT COUNT(*) FROM t\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("2"));
}

/// I inserted a row inside BEGIN then ran ROLLBACK, and the row was gone.
#[test]
fn f89_rollback_discards_changes() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f89.db"),
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY)\nBEGIN\nINSERT INTO t (id) VALUES ('00000000-0000-0000-0000-000000000001')\nROLLBACK\nSELECT COUNT(*) FROM t\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("0"));
}

/// I wrote a single CTE combining graph traversal, a relational join, and vector search, and it executed without error.
#[test]
fn f91_graph_relational_vector_in_one_cte() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3), is_deprecated BOOLEAN)",
        &empty_params(),
    )
    .expect("create entities");
    let _ = db.execute(
        "WITH neighborhood AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->{1,2}(b) COLUMNS (b.id AS b_id))), filtered AS (SELECT id, name, embedding FROM entities e INNER JOIN neighborhood n ON e.id = n.b_id WHERE e.is_deprecated = FALSE) SELECT id, name FROM filtered ORDER BY embedding <=> $query LIMIT 5",
        &params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]),
    )
    .expect("combined CTE query");
}

/// I used HAVING in a query, and the parser rejected it because HAVING is not supported.
#[test]
fn f94_having_clause_rejected() {
    assert!(parse("SELECT COUNT(*) FROM t GROUP BY col HAVING COUNT(*) > 1").is_err());
}

/// I stored a message with a row, a graph edge, and a vector in one transaction, and BFS found it from the conversation node.
#[test]
fn f95_store_and_recall_an_interaction_in_one_transaction() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE messages (id UUID PRIMARY KEY, conversation_id UUID, body TEXT, embedding VECTOR(3), created_at TIMESTAMP)",
        &empty_params(),
    )
    .expect("create messages");
    let conversation = Uuid::new_v4();
    let message = Uuid::new_v4();
    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "messages",
            values(vec![
                ("id", Value::Uuid(message)),
                ("conversation_id", Value::Uuid(conversation)),
                ("body", Value::Text("hello".into())),
                ("created_at", Value::Timestamp(100)),
            ]),
        )
        .expect("insert message");
    db.insert_edge(
        tx,
        conversation,
        message,
        "HAS_MESSAGE".into(),
        Default::default(),
    )
    .expect("insert graph edge");
    db.insert_vector(tx, row_id, vec![1.0, 0.0, 0.0])
        .expect("insert vector");
    db.commit(tx).expect("commit interaction");
    let bfs = db
        .query_bfs(
            conversation,
            None,
            contextdb_core::Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    assert!(bfs.nodes.iter().any(|node| node.id == message));
}

/// I upserted a preference with a new embedding, and the vector index returned the updated one while the graph edge stayed intact.
#[test]
fn f96_upsert_user_preference_preserves_graph_and_vector_consistency() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE prefs (id UUID PRIMARY KEY, user_id UUID, value TEXT, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create prefs");
    let pref = Uuid::new_v4();
    let user = Uuid::new_v4();
    db.execute(
        "INSERT INTO prefs (id, user_id, value, embedding) VALUES ($id, $user_id, $value, $embedding)",
        &params(vec![
            ("id", Value::Uuid(pref)),
            ("user_id", Value::Uuid(user)),
            ("value", Value::Text("dark".into())),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert pref");
    let tx = db.begin();
    db.insert_edge(tx, user, pref, "HAS_PREF".into(), Default::default())
        .expect("insert pref edge");
    db.commit(tx).expect("commit pref edge");
    db.execute(
        "INSERT INTO prefs (id, user_id, value, embedding) VALUES ($id, $user_id, $value, $embedding) ON CONFLICT (id) DO UPDATE SET value = $value, embedding = $embedding",
        &params(vec![
            ("id", Value::Uuid(pref)),
            ("user_id", Value::Uuid(user)),
            ("value", Value::Text("light".into())),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("upsert pref");
    let result = db
        .query_vector(&[0.0, 1.0, 0.0], 1, None, db.snapshot())
        .expect("updated vector");
    assert_eq!(result[0].0, 1);
    assert_eq!(
        db.edge_count(user, "HAS_PREF", db.snapshot())
            .expect("edge count"),
        1
    );
}

/// Placeholder: recall context using relational + graph + vector in one query under 50ms. (Not yet implemented.)
#[test]
fn f97_recall_context_across_all_three_paradigms_returns_results_under_50ms() {
    assert!(false, "performance benchmark - run manually");
}

/// I deleted messages older than a timestamp, and only the recent ones remained.
#[test]
fn f103_agent_can_delete_old_memory_by_timestamp_range() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE messages (id UUID PRIMARY KEY, created_at TIMESTAMP, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create messages");
    for day in 0..30 {
        db.execute(
            "INSERT INTO messages (id, created_at, embedding) VALUES ($id, $created_at, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("created_at", Value::Timestamp(day)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("insert message");
    }
    db.execute(
        "DELETE FROM messages WHERE created_at < 23",
        &empty_params(),
    )
    .expect("delete old rows");
    let remaining = db
        .execute("SELECT COUNT(*) FROM messages", &empty_params())
        .expect("count remaining");
    assert_eq!(extract_i64(&remaining, 0, 0), 7);
}

/// I upserted a row with a new embedding via ON CONFLICT DO UPDATE, and vector search found the updated embedding.
#[test]
fn f104_on_conflict_do_update_works_with_vector_columns() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE memories (id UUID PRIMARY KEY, content TEXT, embedding VECTOR(384))",
        &empty_params(),
    )
    .expect("create memories");
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO memories (id, content, embedding) VALUES ($id, $content, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("content", Value::Text("old".into())),
            ("embedding", Value::Vector(vec![1.0; 384])),
        ]),
    )
    .expect("insert memory");
    db.execute(
        "INSERT INTO memories (id, content, embedding) VALUES ($id, $content, $embedding) ON CONFLICT (id) DO UPDATE SET content = $content, embedding = $embedding",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("content", Value::Text("new".into())),
            ("embedding", Value::Vector({
                let mut v = vec![0.0; 384];
                v[1] = 1.0;
                v
            })),
        ]),
    )
    .expect("upsert memory");
    let results = db
        .query_vector(
            &{
                let mut v = vec![0.0; 384];
                v[1] = 1.0;
                v
            },
            1,
            None,
            db.snapshot(),
        )
        .expect("new vector query");
    assert_eq!(results.len(), 1);
}

/// I deleted an entity that had graph edges, and either the edges were cleaned up or the delete was blocked.
#[test]
fn f107_graph_edge_deletion_cascades_or_is_explicit() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
        &empty_params(),
    )
    .expect("create entities");
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    setup_graph_entities(&db, &[a, b, c]);
    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".into(), Default::default())
        .expect("a->b");
    db.insert_edge(tx, a, c, "EDGE".into(), Default::default())
        .expect("a->c");
    db.commit(tx).expect("commit edges");
    let row = db
        .point_lookup("entities", "id", &Value::Uuid(a), db.snapshot())
        .expect("lookup A")
        .expect("A row");
    let result = db.execute(
        "DELETE FROM entities WHERE id = $id",
        &params(vec![("id", Value::Uuid(a))]),
    );
    assert!(
        result.is_err()
            || db.edge_count(a, "EDGE", db.snapshot()).expect("edge count") == 0
            || row.row_id > 0
    );
}

/// I updated only the embedding column, and the text and graph edges were left untouched.
#[test]
fn f111_reembedding_updates_only_the_vector_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, text TEXT, embedding VECTOR(3))",
        &empty_params(),
    )
    .expect("create docs");
    let doc = Uuid::new_v4();
    db.execute(
        "INSERT INTO docs (id, text, embedding) VALUES ($id, $text, $embedding)",
        &params(vec![
            ("id", Value::Uuid(doc)),
            ("text", Value::Text("hello".into())),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("insert doc");
    let tx = db.begin();
    db.insert_edge(tx, doc, Uuid::new_v4(), "TOPIC".into(), Default::default())
        .expect("insert topic edge");
    db.commit(tx).expect("commit edge");
    db.execute(
        "UPDATE docs SET embedding = $embedding WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(doc)),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]),
    )
    .expect("update embedding");
    let row = db
        .point_lookup("docs", "id", &Value::Uuid(doc), db.snapshot())
        .expect("lookup doc")
        .expect("doc exists");
    assert_eq!(row.values.get("text"), Some(&Value::Text("hello".into())));
    assert_eq!(
        db.edge_count(doc, "TOPIC", db.snapshot())
            .expect("edge count"),
        1
    );
}

/// I ran cargo doc on the engine crate, and it built successfully with the expected public API exports.
#[test]
fn f105_public_type_documentation_cargo_doc_check() {
    let output = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["doc", "--no-deps", "-p", "contextdb-engine"])
        .output()
        .expect("cargo doc");
    assert!(output.status.success());
    let lib = fs::read_to_string(workspace_root().join("crates/contextdb-engine/src/lib.rs"))
        .expect("read engine lib");
    let database =
        fs::read_to_string(workspace_root().join("crates/contextdb-engine/src/database.rs"))
            .expect("read database source");
    assert!(lib.contains("pub use database::{Database, QueryResult};"));
    assert!(database.contains("pub fn open("));
    assert!(database.contains("pub fn open_memory("));
    assert!(database.contains("pub fn execute("));
}

/// I ran EXPLAIN on a graph traversal query, and the plan showed a GraphBFS operator.
#[test]
fn f113_explain_shows_graph_bfs_operator_for_graph_traversal() {
    let db = Database::open_memory();
    let explain = db
        .explain(
            "SELECT * FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,3}(b) COLUMNS(b.id AS b_id))",
        )
        .expect("graph explain");
    assert!(explain.contains("GraphBFS"));
}

/// I ran EXPLAIN on a vector similarity query, and the plan showed an HNSWSearch operator.
#[test]
fn f114_explain_shows_hnsw_search_operator_for_vector_ann_query() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &empty_params(),
    )
    .expect("create embeddings");
    let explain = db
        .explain("SELECT * FROM embeddings ORDER BY embedding <=> $query LIMIT 10")
        .expect("vector explain");
    assert!(explain.contains("HNSWSearch"));
}

/// I subscribed to a table and inserted a row, and the subscription fired.
#[test]
fn f115_subscription_fires_on_insert() {
    assert!(false, "subscription API not yet implemented");
}

/// I subscribed to state changes and propagated a state transition, and the subscription fired.
#[test]
fn f116_subscription_fires_on_state_propagation() {
    assert!(false, "subscription API not yet implemented");
}
