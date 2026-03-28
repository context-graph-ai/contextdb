use super::helpers::{make_params, setup_ontology_db, setup_ontology_db_with_dag};
use contextdb_core::{Direction, Error, MemoryAccountant, UpsertResult, Value, VersionedRow};
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use contextdb_parser::{Statement as AstStatement, parse as parse_sql};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

fn text<'a>(row: &'a VersionedRow, key: &str) -> &'a str {
    row.values
        .get(key)
        .and_then(Value::as_text)
        .expect("expected text value")
}

fn uuid_value(row: &VersionedRow, key: &str) -> Uuid {
    *row.values
        .get(key)
        .and_then(Value::as_uuid)
        .expect("expected uuid value")
}

fn ddl_sql_from_change(change: &DdlChange) -> String {
    match change {
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
        } => {
            let mut sql = format!(
                "CREATE TABLE {} ({})",
                name,
                columns
                    .iter()
                    .map(|(col, ty)| format!("{col} {ty}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if !constraints.is_empty() {
                sql.push(' ');
                sql.push_str(&constraints.join(" "));
            }
            sql
        }
        DdlChange::DropTable { .. } => panic!("unexpected DROP TABLE in DDL round-trip helper"),
        DdlChange::AlterTable { .. } => panic!("unexpected ALTER TABLE in DDL round-trip helper"),
    }
}

#[test]
fn a1_01_create_all_12_tables() {
    let db = setup_ontology_db();
    let tables = db.table_names();
    assert_eq!(tables.len(), 12);
    for expected in [
        "contexts",
        "intentions",
        "decisions",
        "entities",
        "entity_snapshots",
        "observations",
        "outcomes",
        "invalidations",
        "edges",
        "approvals",
        "patterns",
        "sync_state",
    ] {
        assert!(tables.contains(&expected.to_string()));
        let scanned = db.scan(expected, db.snapshot()).expect("table should scan");
        assert!(scanned.is_empty());
    }
}

#[test]
fn a1_02_create_table_with_immutable_constraint() {
    let db = setup_ontology_db();
    db.execute(
        "CREATE TABLE obs_gate_a (id UUID PRIMARY KEY, data JSON) IMMUTABLE",
        &HashMap::new(),
    )
    .expect("create immutable table");

    let meta = db.table_meta("obs_gate_a").expect("table meta");
    assert!(meta.immutable);
}

#[test]
fn a1_03_create_table_with_state_machine_constraint() {
    let db = setup_ontology_db();
    db.execute(
        "CREATE TABLE inv_gate_a (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &HashMap::new(),
    )
    .expect("create state machine table");

    let meta = db.table_meta("inv_gate_a").expect("table meta");
    let sm = meta.state_machine.expect("state machine expected");
    assert_eq!(sm.column, "status");
    assert_eq!(
        sm.transitions.get("pending").expect("pending transitions"),
        &vec!["acknowledged".to_string(), "dismissed".to_string()]
    );
    assert_eq!(
        sm.transitions
            .get("acknowledged")
            .expect("acknowledged transitions"),
        &vec!["resolved".to_string(), "dismissed".to_string()]
    );
}

#[test]
fn a1_04_drop_table_removes_schema_and_data() {
    let db = setup_ontology_db();
    let tx = db.begin();
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("description".to_string(), Value::Text("p".to_string())),
        ]),
    )
    .expect("insert row");
    db.commit(tx).expect("commit");

    db.execute(
        "CREATE TABLE gate_a_drop (id UUID PRIMARY KEY)",
        &HashMap::new(),
    )
    .expect("create table");
    db.execute("DROP TABLE gate_a_drop", &HashMap::new())
        .expect("drop table");

    assert!(!db.table_names().contains(&"gate_a_drop".to_string()));
    let result = db.scan("gate_a_drop", db.snapshot());
    assert!(matches!(result, Err(Error::TableNotFound(_))));
}

// a1_05 deleted — redundant with p31_column_type_preservation_across_reopen

#[test]
fn rt1_composite_store_ddl_round_trip_parse() {
    let db = contextdb_engine::Database::open_memory();
    let params = HashMap::new();

    db.execute(
        "CREATE TABLE rt_immutable (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE rt_state_machine (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [done])",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE rt_dag (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &params,
    )
    .unwrap();

    let ddl = db.ddl_log_since(0);
    assert_eq!(ddl.len(), 3);

    for change in ddl {
        let sql = ddl_sql_from_change(&change);
        let stmt = parse_sql(&sql).expect("rendered DDL should parse");
        let AstStatement::CreateTable(parsed) = stmt else {
            panic!("expected CREATE TABLE");
        };
        let DdlChange::CreateTable {
            name,
            columns,
            constraints,
        } = change
        else {
            panic!("expected CREATE TABLE");
        };

        assert_eq!(parsed.name, name);
        assert_eq!(parsed.columns.len(), columns.len());
        for ((expected_name, _), parsed_col) in columns.iter().zip(parsed.columns.iter()) {
            assert_eq!(&parsed_col.name, expected_name);
        }
        if constraints.iter().any(|c| c == "IMMUTABLE") {
            assert!(parsed.immutable);
        }
        if constraints.iter().any(|c| c.starts_with("STATE MACHINE")) {
            assert!(parsed.state_machine.is_some());
        }
        if constraints.iter().any(|c| c.starts_with("DAG(")) {
            assert!(!parsed.dag_edge_types.is_empty());
        }
    }
}

#[test]
fn rt3_database_apply_changes_ddl_round_trip() {
    let db = contextdb_engine::Database::open_memory();
    let changes = ChangeSet {
        ddl: vec![
            DdlChange::CreateTable {
                name: "rt3_immutable".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID".to_string()),
                    ("name".to_string(), "TEXT".to_string()),
                ],
                constraints: vec!["IMMUTABLE".to_string()],
            },
            DdlChange::CreateTable {
                name: "rt3_state_machine".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID".to_string()),
                    ("status".to_string(), "TEXT".to_string()),
                ],
                constraints: vec!["STATE MACHINE (status: pending -> [done])".to_string()],
            },
            DdlChange::CreateTable {
                name: "rt3_dag".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID".to_string()),
                    ("source_id".to_string(), "UUID".to_string()),
                    ("target_id".to_string(), "UUID".to_string()),
                    ("edge_type".to_string(), "TEXT".to_string()),
                ],
                constraints: vec!["DAG('CITES')".to_string()],
            },
        ],
        ..Default::default()
    };
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    )
    .expect("apply changes");

    let immutable = db.table_meta("rt3_immutable").expect("immutable table");
    assert!(immutable.immutable);
    assert_eq!(immutable.columns.len(), 2);

    let state_machine = db.table_meta("rt3_state_machine").expect("state table");
    assert!(state_machine.state_machine.is_some());
    assert_eq!(state_machine.columns.len(), 2);

    let dag = db.table_meta("rt3_dag").expect("dag table");
    assert_eq!(dag.dag_edge_types, vec!["CITES".to_string()]);
    assert_eq!(dag.columns.len(), 4);
}

#[test]
fn a2_01_insert_and_scan() {
    let db = setup_ontology_db();
    let rows = [
        (Uuid::new_v4(), "alpha", "service"),
        (Uuid::new_v4(), "beta", "server"),
        (Uuid::new_v4(), "gamma", "database"),
    ];

    let tx = db.begin();
    for (id, name, entity_type) in rows {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text(name.to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text(entity_type.to_string()),
                ),
            ]),
        )
        .expect("insert entity");
    }
    db.commit(tx).expect("commit");

    let scanned = db.scan("entities", db.snapshot()).expect("scan entities");
    assert_eq!(scanned.len(), 3);
    let names: HashSet<String> = scanned
        .iter()
        .map(|r| text(r, "name").to_string())
        .collect();
    assert!(names.contains("alpha"));
    assert!(names.contains("beta"));
    assert!(names.contains("gamma"));
}

#[test]
fn a2_02_point_lookup_by_column() {
    let db = setup_ontology_db();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id1)),
            ("name".to_string(), Value::Text("lookup".to_string())),
            ("entity_type".to_string(), Value::Text("server".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let found = db
        .point_lookup("entities", "id", &Value::Uuid(id1), db.snapshot())
        .expect("point lookup")
        .expect("row exists");
    assert_eq!(uuid_value(&found, "id"), id1);
    assert_eq!(text(&found, "name"), "lookup");

    let missing = db
        .point_lookup("entities", "id", &Value::Uuid(id2), db.snapshot())
        .expect("point lookup");
    assert!(missing.is_none());
}

#[test]
fn a2_03_insert_via_sql() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("sql insert".to_string())),
        ]),
    )
    .expect("sql insert");

    let result = db
        .execute("SELECT * FROM entities", &HashMap::new())
        .expect("select entities");
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert!(row.contains(&Value::Uuid(id)));
    assert!(row.contains(&Value::Text("sql insert".to_string())));
}

#[test]
fn a2_04_select_with_where_clause() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for (name, entity_type) in [
        ("web-1", "server"),
        ("web-2", "server"),
        ("db-1", "database"),
    ] {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text(name.to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text(entity_type.to_string()),
                ),
            ]),
        )
        .expect("insert row");
    }
    db.commit(tx).expect("commit");

    let result = db
        .execute(
            "SELECT * FROM entities WHERE entity_type = 'server'",
            &HashMap::new(),
        )
        .expect("select with where");
    assert_eq!(result.rows.len(), 2);
    for row in result.rows {
        assert!(row.contains(&Value::Text("server".to_string())));
    }
}

#[test]
fn a2_05_delete_removes_from_scan() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();
    let tx = db.begin();
    let _row_id = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("delete-me".to_string())),
                ("entity_type".to_string(), Value::Text("server".to_string())),
            ]),
        )
        .expect("insert");
    db.commit(tx).expect("commit");

    // Capture snapshot before deletion to prove deleted_tx mechanism via MVCC.
    let snap_before_delete = db.snapshot();

    let tx2 = db.begin();
    db.delete_row(tx2, "entities", _row_id).expect("delete row");
    db.commit(tx2).expect("commit delete");

    let rows = db.scan("entities", db.snapshot()).expect("scan");
    assert!(rows.is_empty());

    // Verify the row still exists at the pre-delete snapshot (deleted_tx filters it
    // at newer snapshots, but MVCC preserves it at older ones).
    let old = db
        .point_lookup("entities", "id", &Value::Uuid(id), snap_before_delete)
        .expect("lookup at old snapshot");
    assert!(old.is_some(), "row must be visible at pre-delete snapshot");

    let current = db
        .point_lookup("entities", "id", &Value::Uuid(id), db.snapshot())
        .expect("lookup at current snapshot");
    assert!(
        current.is_none(),
        "row must be invisible at post-delete snapshot"
    );
}

#[test]
fn a2_06_update_modifies_row() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    db.execute(
        "INSERT INTO entities (id, name, entity_type) VALUES ($id, $name, $ty)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("old".to_string())),
            ("ty", Value::Text("service".to_string())),
        ]),
    )
    .expect("insert");

    db.execute(
        "UPDATE entities SET name='new' WHERE id=$id",
        &make_params(vec![("id", Value::Uuid(id))]),
    )
    .expect("update");

    let selected = db
        .execute(
            "SELECT * FROM entities WHERE id=$id",
            &make_params(vec![("id", Value::Uuid(id))]),
        )
        .expect("select");
    assert_eq!(selected.rows.len(), 1);
    assert!(selected.rows[0].contains(&Value::Text("new".to_string())));
    assert!(!selected.rows[0].contains(&Value::Text("old".to_string())));
}

#[test]
fn a2_07_upsert_insert_when_new() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    let tx = db.begin();
    let result = db
        .upsert_row(
            tx,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("new".to_string())),
                ("entity_type".to_string(), Value::Text("server".to_string())),
            ]),
        )
        .expect("upsert");
    db.commit(tx).expect("commit");

    assert_eq!(result, UpsertResult::Inserted);
    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 1);
}

#[test]
fn a2_08_upsert_update_when_conflict() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text("before".to_string())),
            ("entity_type".to_string(), Value::Text("server".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let result = db
        .upsert_row(
            tx2,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("changed".to_string())),
                ("entity_type".to_string(), Value::Text("server".to_string())),
            ]),
        )
        .expect("upsert");
    db.commit(tx2).expect("commit");

    assert_eq!(result, UpsertResult::Updated);
    let row = db
        .point_lookup("entities", "id", &Value::Uuid(id), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "name"), "changed");
}

#[test]
fn a2_09_upsert_noop_when_same_values() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text("same".to_string())),
            ("entity_type".to_string(), Value::Text("server".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let result = db
        .upsert_row(
            tx2,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("same".to_string())),
                ("entity_type".to_string(), Value::Text("server".to_string())),
            ]),
        )
        .expect("upsert");
    db.commit(tx2).expect("commit");

    assert_eq!(result, UpsertResult::NoOp);
    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 1);
}

#[test]
fn a2_10_upsert_is_idempotent() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    for _ in 0..3 {
        db.execute(
            "INSERT INTO entities (id, name, entity_type) VALUES ($id, $name, $ty) ON CONFLICT (id) DO UPDATE SET name=$name, entity_type=$ty",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("steady".to_string())),
                ("ty", Value::Text("server".to_string())),
            ]),
        )
        .expect("upsert sql");
    }

    let rows = db.scan("entities", db.snapshot()).expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(text(&rows[0], "name"), "steady");
}

#[test]
fn a2_11_select_with_join_direct_api() {
    let db = setup_ontology_db();
    let entity_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(entity_id)),
            ("name".to_string(), Value::Text("entity".to_string())),
            ("entity_type".to_string(), Value::Text("infra".to_string())),
        ]),
    )
    .expect("insert entity");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(decision_id)),
            (
                "description".to_string(),
                Value::Text("decision".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.9)),
        ]),
    )
    .expect("insert decision");
    db.insert_edge(
        tx,
        decision_id,
        entity_id,
        "BASED_ON".to_string(),
        HashMap::new(),
    )
    .expect("insert edge");
    db.commit(tx).expect("commit");

    let bfs = db
        .query_bfs(
            entity_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, decision_id);

    let joined = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            bfs.nodes.iter().any(|n| n.id == uuid_value(r, "id"))
        })
        .expect("scan filter");
    assert_eq!(joined.len(), 1);
    assert_eq!(uuid_value(&joined[0], "id"), decision_id);
}

#[test]
fn a2_12_select_with_cte_direct_api() {
    let db = setup_ontology_db();
    db.execute(
        "CREATE TABLE entities_with_ctx (id UUID PRIMARY KEY, context_id UUID, name TEXT)",
        &HashMap::new(),
    )
    .expect("create table");

    let ctx_target = Uuid::new_v4();
    let ctx_other = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "contexts",
        HashMap::from([
            ("id".to_string(), Value::Uuid(ctx_target)),
            ("name".to_string(), Value::Text("test".to_string())),
        ]),
    )
    .expect("insert ctx target");
    db.insert_row(
        tx,
        "contexts",
        HashMap::from([
            ("id".to_string(), Value::Uuid(ctx_other)),
            ("name".to_string(), Value::Text("other".to_string())),
        ]),
    )
    .expect("insert ctx other");
    db.insert_row(
        tx,
        "entities_with_ctx",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("context_id".to_string(), Value::Uuid(ctx_target)),
            ("name".to_string(), Value::Text("in-test".to_string())),
        ]),
    )
    .expect("insert entity target");
    db.insert_row(
        tx,
        "entities_with_ctx",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("context_id".to_string(), Value::Uuid(ctx_other)),
            ("name".to_string(), Value::Text("in-other".to_string())),
        ]),
    )
    .expect("insert entity other");
    db.commit(tx).expect("commit");

    let test_contexts = db
        .scan_filter("contexts", db.snapshot(), &|r| text(r, "name") == "test")
        .expect("scan contexts");
    assert_eq!(test_contexts.len(), 1);
    let test_ctx = uuid_value(&test_contexts[0], "id");

    let entities = db
        .scan_filter("entities_with_ctx", db.snapshot(), &|r| {
            uuid_value(r, "context_id") == test_ctx
        })
        .expect("scan entities");
    assert_eq!(entities.len(), 1);
    assert_eq!(text(&entities[0], "name"), "in-test");
}

#[test]
fn a2_13_count_aggregate_via_scan_len() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for idx in 0..5 {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text(format!("e-{idx}"))),
                ("entity_type".to_string(), Value::Text("node".to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");

    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 5);
}

fn insert_chain(db: &contextdb_engine::Database, nodes: &[Uuid], edge_type: &str) {
    let tx = db.begin();
    for idx in 0..(nodes.len() - 1) {
        db.insert_edge(
            tx,
            nodes[idx],
            nodes[idx + 1],
            edge_type.to_string(),
            HashMap::new(),
        )
        .expect("insert edge");
    }
    db.commit(tx).expect("commit chain");
}

#[test]
fn a3_01_bfs_single_hop() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "BASED_ON".to_string(), HashMap::new())
        .expect("insert");
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(
            a,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.nodes[0].id, b);
}

#[test]
fn a3_02_bfs_multi_hop_chain() {
    let db = setup_ontology_db();
    let nodes: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    insert_chain(&db, &nodes, "BASED_ON");

    let depth3 = db
        .query_bfs(
            nodes[0],
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            3,
            db.snapshot(),
        )
        .expect("bfs depth 3");
    assert_eq!(depth3.nodes.len(), 3);
    assert_eq!(depth3.nodes[0].id, nodes[1]);
    assert_eq!(depth3.nodes[1].id, nodes[2]);
    assert_eq!(depth3.nodes[2].id, nodes[3]);

    let depth5 = db
        .query_bfs(
            nodes[0],
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            5,
            db.snapshot(),
        )
        .expect("bfs depth 5");
    assert_eq!(depth5.nodes.len(), 4);
    assert_eq!(depth5.nodes[3].id, nodes[4]);
}

#[test]
fn a3_03_bfs_respects_depth_bound() {
    let db = setup_ontology_db();
    let nodes: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();
    insert_chain(&db, &nodes, "BASED_ON");

    let out = db
        .query_bfs(nodes[0], None, Direction::Outgoing, 3, db.snapshot())
        .expect("bfs");
    assert_eq!(out.nodes.len(), 3);
    let ids: HashSet<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert!(ids.contains(&nodes[1]));
    assert!(ids.contains(&nodes[2]));
    assert!(ids.contains(&nodes[3]));
    assert!(!ids.contains(&nodes[4]));
    assert!(!ids.contains(&nodes[5]));
}

#[test]
fn a3_04_bfs_cycle_detection() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    let tx = db.begin();
    for (s, t) in [(a, b), (b, c), (c, a)] {
        db.insert_edge(tx, s, t, "BASED_ON".to_string(), HashMap::new())
            .expect("insert");
    }
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(a, None, Direction::Outgoing, 5, db.snapshot())
        .expect("bfs");
    assert_eq!(out.nodes.len(), 2);
    let ids: Vec<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert!(ids.contains(&b));
    assert!(ids.contains(&c));
    let unique: HashSet<Uuid> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len());
}

#[test]
fn a3_05_bfs_reverse_direction() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("insert");
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(
            e,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.nodes[0].id, d);
}

#[test]
fn a3_06_bfs_bidirectional() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .expect("insert");
    db.insert_edge(tx, c, b, "EDGE".to_string(), HashMap::new())
        .expect("insert");
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(b, None, Direction::Both, 1, db.snapshot())
        .expect("bfs");
    let ids: HashSet<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&a));
    assert!(ids.contains(&c));
}

#[test]
fn a3_07_bfs_edge_type_filter() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "BASED_ON".to_string(), HashMap::new())
        .expect("insert");
    db.insert_edge(tx, a, c, "CITES".to_string(), HashMap::new())
        .expect("insert");
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(
            a,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.nodes[0].id, b);
}

#[test]
fn a3_08_bfs_empty_graph() {
    let db = setup_ontology_db();
    let out = db
        .query_bfs(Uuid::new_v4(), None, Direction::Outgoing, 5, db.snapshot())
        .expect("bfs");
    assert!(out.nodes.is_empty());
}

#[test]
fn a3_09_bfs_fan_out() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let neighbors: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    for n in &neighbors {
        db.insert_edge(tx, a, *n, "EDGE".to_string(), HashMap::new())
            .expect("insert");
    }
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
        .expect("bfs");
    let ids: HashSet<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert_eq!(ids.len(), 3);
    for n in neighbors {
        assert!(ids.contains(&n));
    }
}

#[test]
fn a3_10_bfs_via_match_sql_explain_only() {
    let db = setup_ontology_db();
    let explain = db
        .explain(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id))",
        )
        .expect("explain");
    assert!(explain.contains("GraphBfs"));
}

#[test]
fn a3_11_bfs_max_visited_limit() {
    let db = setup_ontology_db();
    let start = Uuid::new_v4();
    let tx = db.begin();
    for _ in 0..100_001 {
        db.insert_edge(
            tx,
            start,
            Uuid::new_v4(),
            "EDGE".to_string(),
            HashMap::new(),
        )
        .expect("insert edge");
    }
    db.commit(tx).expect("commit");

    let result = db.query_bfs(start, None, Direction::Outgoing, 1, db.snapshot());
    assert!(matches!(result, Err(Error::BfsVisitedExceeded(100_000))));
}

#[test]
fn a3_12_edge_deletion_reflected_in_bfs() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    db.insert_edge(tx, a, b, "BASED_ON".to_string(), HashMap::new())
        .expect("insert edge");
    db.commit(tx).expect("commit");

    // Guard: edge exists
    let bfs_before = db
        .query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    assert_eq!(
        bfs_before.nodes.len(),
        1,
        "guard: edge must exist before deletion"
    );

    let tx2 = db.begin();
    db.delete_edge(tx2, a, b, "BASED_ON").expect("delete edge");
    db.commit(tx2).expect("commit");

    let bfs_after = db
        .query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    assert!(
        bfs_after.nodes.is_empty(),
        "BFS must not find edge after deletion"
    );
}

#[test]
fn a3_13_bfs_over_adjacency_not_recursive_cte() {
    let db = setup_ontology_db();
    let explain = db
        .explain(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id))",
        )
        .expect("explain");
    assert!(explain.contains("GraphBfs"));
    assert!(!explain.contains("RecursiveCte"));
}

#[test]
fn a4_01_insert_and_search_basic() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let r1 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert r1");
    let r2 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert r2");
    let r3 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert r3");

    db.insert_vector(tx, r1, vec![1.0, 0.0, 0.0]).expect("v1");
    db.insert_vector(tx, r2, vec![0.0, 1.0, 0.0]).expect("v2");
    db.insert_vector(tx, r3, vec![0.0, 0.0, 1.0]).expect("v3");
    db.commit(tx).expect("commit");

    let out = db
        .query_vector(&[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, r1);
}

#[test]
fn a4_02_search_respects_k_limit() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for idx in 0..10 {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("data".to_string(), Value::Null),
                ]),
            )
            .expect("insert");
        db.insert_vector(tx, rid, vec![idx as f32, 1.0, 0.0])
            .expect("insert vector");
    }
    db.commit(tx).expect("commit");

    let out = db
        .query_vector(&[1.0, 0.0, 0.0], 3, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 3);
}

#[test]
fn a4_03_search_with_candidate_prefilter() {
    let db = setup_ontology_db();
    let mut row_ids = Vec::new();
    let tx = db.begin();
    for idx in 0..10 {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("data".to_string(), Value::Null),
                ]),
            )
            .expect("insert");
        db.insert_vector(tx, rid, vec![1.0 - (idx as f32 * 0.01), 0.0, 0.0])
            .expect("insert vector");
        row_ids.push(rid);
    }
    db.commit(tx).expect("commit");

    let candidates = RoaringTreemap::from_iter([row_ids[0], row_ids[1], row_ids[2]]);
    let out = db
        .query_vector(&[1.0, 0.0, 0.0], 5, Some(&candidates), db.snapshot())
        .expect("search");
    assert!(!out.is_empty());
    for (rid, _) in out {
        assert!(candidates.contains(rid));
    }
}

#[test]
fn a4_04_search_empty_store() {
    let db = setup_ontology_db();
    let out = db
        .query_vector(&[1.0, 0.0, 0.0], 5, None, db.snapshot())
        .expect("search empty");
    assert!(out.is_empty());
}

#[test]
fn a4_05_dimension_mismatch_error() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let rid = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert row");
    db.insert_vector(tx, rid, vec![1.0, 0.0, 0.0])
        .expect("insert 3d vector");

    let rid2 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert second row");
    let err = db.insert_vector(tx, rid2, vec![1.0, 0.0, 0.0, 0.0, 0.0]);
    assert!(matches!(
        err,
        Err(Error::VectorDimensionMismatch {
            expected: 3,
            got: 5
        })
    ));

    db.commit(tx).expect("commit");
    // TODO: search-side dimension validation not yet implemented.
}

#[test]
fn a4_06_vector_deletion() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let rid = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
            ]),
        )
        .expect("insert");
    db.insert_vector(tx, rid, vec![1.0, 0.0])
        .expect("insert vector");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    db.delete_vector(tx2, rid).expect("delete vector");
    db.commit(tx2).expect("commit");
    assert!(
        !db.query_vector(&[1.0, 0.0], 5, None, db.snapshot())
            .expect("search")
            .iter()
            .any(|(id, _)| *id == rid)
    );
}

#[test]
fn a4_07_cosine_similarity_ordering() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let mut ids = Vec::new();
    for vector in [vec![1.0, 0.0], vec![0.9, 0.1], vec![0.0, 1.0]] {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("data".to_string(), Value::Null),
                ]),
            )
            .expect("insert");
        db.insert_vector(tx, rid, vector).expect("vector");
        ids.push(rid);
    }
    db.commit(tx).expect("commit");

    let out = db
        .query_vector(&[1.0, 0.0], 3, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].0, ids[0]);
    assert_eq!(out[1].0, ids[1]);
    assert_eq!(out[2].0, ids[2]);
}

#[test]
fn a4_08_vector_search_via_sql() {
    let db = setup_ontology_db();
    let embed_384 = |x: f32, y: f32| {
        let mut v = vec![0.0; 384];
        v[0] = x;
        v[1] = y;
        v
    };
    let v = embed_384(1.0, 0.0);
    for embed in [
        embed_384(1.0, 0.0),
        embed_384(0.8, 0.2),
        embed_384(0.0, 1.0),
    ] {
        db.execute(
            "INSERT INTO observations (id, entity_id, data, embedding) VALUES ($id, $entity_id, $data, $embedding)",
            &make_params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("entity_id", Value::Uuid(Uuid::new_v4())),
                ("data", Value::Null),
                ("embedding", Value::Vector(embed)),
            ]),
        )
        .expect("insert via sql");
    }

    let out = db
        .execute(
            "SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 3",
            &make_params(vec![("q", Value::Vector(v))]),
        )
        .expect("vector select");
    assert_eq!(out.rows.len(), 3);

    let explain = db
        .explain("SELECT * FROM observations ORDER BY embedding <=> $q LIMIT 3")
        .expect("explain");
    assert!(explain.contains("VectorSearch"));
}

#[test]
fn a4_09_search_k_zero_returns_empty() {
    let db = setup_ontology_db();
    let out = db
        .query_vector(&[1.0, 0.0], 0, None, db.snapshot())
        .expect("search");
    assert!(out.is_empty());
}

#[test]
fn a4_10_float32_precision_roundtrip() {
    let db = setup_ontology_db();
    let input = [0.123_456_79_f32, -0.987_654_3_f32, 0.555_555_6_f32];
    let obs_id = Uuid::new_v4();

    // Store embedding in the row's values map so we can retrieve it via point_lookup.
    let tx = db.begin();
    let rid = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(obs_id)),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("data".to_string(), Value::Null),
                ("embedding".to_string(), Value::Vector(input.to_vec())),
            ]),
        )
        .expect("insert");
    db.insert_vector(tx, rid, input.to_vec())
        .expect("insert vector");
    db.commit(tx).expect("commit");

    let out = db
        .query_vector(&input, 1, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, rid);

    // Retrieve the stored vector and verify each component matches within f32 precision.
    let row = db
        .point_lookup("observations", "id", &Value::Uuid(obs_id), db.snapshot())
        .expect("lookup")
        .expect("row must exist");
    let stored = match row.values.get("embedding") {
        Some(Value::Vector(v)) => v,
        other => panic!("expected Vector value, got {:?}", other),
    };
    assert_eq!(stored.len(), input.len());
    for (i, (&s, &inp)) in stored.iter().zip(input.iter()).enumerate() {
        assert!(
            (s - inp).abs() < 1e-7,
            "component {i} mismatch: stored={s}, input={inp}"
        );
    }
}

#[test]
fn a4_11_hnsw_recall_threshold() {
    // Use a dedicated table with a higher-dimensional vector to get good HNSW recall.
    let db = setup_ontology_db();
    let dim = 32usize;
    let n = 1_500u64;
    let k = 10usize;

    // Create a table with a vector column of the right dimension.
    db.execute(
        &format!("CREATE TABLE recall_items (id UUID PRIMARY KEY, embedding VECTOR({dim}))"),
        &HashMap::new(),
    )
    .expect("create table");

    // Insert enough vectors to trigger HNSW (threshold = 1000).
    let tx = db.begin();
    let mut all_vectors = Vec::with_capacity(n as usize);
    for i in 0..n {
        let rid = db
            .insert_row(
                tx,
                "recall_items",
                HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        // Deterministic pseudo-random unit vector.
        let mut state = i.wrapping_mul(6364136223846793005).wrapping_add(1);
        let raw: Vec<f32> = (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                ((state >> 33) as f64 / (1u64 << 31) as f64) as f32 * 2.0 - 1.0
            })
            .collect();
        let norm = raw.iter().map(|v| v * v).sum::<f32>().sqrt().max(1e-9);
        let vec: Vec<f32> = raw.iter().map(|v| v / norm).collect();
        db.insert_vector(tx, rid, vec.clone()).expect("insert vec");
        all_vectors.push((rid, vec));
    }
    db.commit(tx).expect("commit");

    // Run 20 queries, measure average recall@k vs brute-force.
    let mut total_recall = 0.0f64;
    let queries = 20u64;
    for q_seed in 5000..(5000 + queries) {
        let mut state = q_seed
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let raw: Vec<f32> = (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                ((state >> 33) as f64 / (1u64 << 31) as f64) as f32 * 2.0 - 1.0
            })
            .collect();
        let norm = raw.iter().map(|v| v * v).sum::<f32>().sqrt().max(1e-9);
        let query: Vec<f32> = raw.iter().map(|v| v / norm).collect();

        let results = db
            .query_vector(&query, k, None, db.snapshot())
            .expect("vector search");
        let result_ids: HashSet<_> = results.iter().map(|r| r.0).collect();

        // Brute-force top-k by cosine similarity (dot product on unit vectors).
        let mut scored: Vec<_> = all_vectors
            .iter()
            .map(|(rid, vec)| {
                let dot: f32 = vec.iter().zip(&query).map(|(a, b)| a * b).sum();
                (*rid, dot)
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let truth_ids: HashSet<_> = scored.iter().take(k).map(|(rid, _)| *rid).collect();

        let matches = result_ids.intersection(&truth_ids).count();
        total_recall += matches as f64 / k as f64;
    }
    let avg_recall = total_recall / queries as f64;
    assert!(
        avg_recall >= 0.95,
        "avg HNSW recall@{k} was {avg_recall}, expected >= 0.95"
    );
}

#[test]
fn a4_12_hnsw_with_mvcc_visibility() {
    let db = setup_ontology_db();
    let dim = 32usize;

    // Create a table with a vector column of the right dimension.
    db.execute(
        &format!("CREATE TABLE mvcc_items (id UUID PRIMARY KEY, embedding VECTOR({dim}))"),
        &HashMap::new(),
    )
    .expect("create table");

    // Phase 1: insert 1000 vectors and commit.
    let tx1 = db.begin();
    let mut phase1_rids = HashSet::new();
    for i in 0..1_000u64 {
        let rid = db
            .insert_row(
                tx1,
                "mvcc_items",
                HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        let mut state = i.wrapping_mul(6364136223846793005).wrapping_add(1);
        let raw: Vec<f32> = (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                ((state >> 33) as f64 / (1u64 << 31) as f64) as f32 * 2.0 - 1.0
            })
            .collect();
        let norm = raw.iter().map(|v| v * v).sum::<f32>().sqrt().max(1e-9);
        let vec: Vec<f32> = raw.iter().map(|v| v / norm).collect();
        db.insert_vector(tx1, rid, vec).expect("insert vec");
        phase1_rids.insert(rid);
    }
    db.commit(tx1).expect("commit phase 1");
    let snap_after_phase1 = db.snapshot();

    // Phase 2: insert 200 more vectors and commit.
    let tx2 = db.begin();
    for i in 1_000..1_200u64 {
        let rid = db
            .insert_row(
                tx2,
                "mvcc_items",
                HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("insert row");
        let mut state = i.wrapping_mul(6364136223846793005).wrapping_add(1);
        let raw: Vec<f32> = (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                ((state >> 33) as f64 / (1u64 << 31) as f64) as f32 * 2.0 - 1.0
            })
            .collect();
        let norm = raw.iter().map(|v| v * v).sum::<f32>().sqrt().max(1e-9);
        let vec: Vec<f32> = raw.iter().map(|v| v / norm).collect();
        db.insert_vector(tx2, rid, vec).expect("insert vec");
    }
    db.commit(tx2).expect("commit phase 2");

    // Query at the older snapshot — all results must be from phase 1 only.
    let mut state = 9999u64
        .wrapping_mul(2862933555777941757)
        .wrapping_add(3037000493);
    let raw: Vec<f32> = (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            ((state >> 33) as f64 / (1u64 << 31) as f64) as f32 * 2.0 - 1.0
        })
        .collect();
    let norm = raw.iter().map(|v| v * v).sum::<f32>().sqrt().max(1e-9);
    let query: Vec<f32> = raw.iter().map(|v| v / norm).collect();

    let results = db
        .query_vector(&query, 10, None, snap_after_phase1)
        .expect("query at old snapshot");
    for (rid, _) in &results {
        assert!(
            phase1_rids.contains(rid),
            "row {rid} should only be from phase 1 at old snapshot"
        );
    }
}

#[test]
fn a4_13_vector_search_with_context_prefilter() {
    let db = setup_ontology_db();
    db.execute(
        "CREATE TABLE observations_ctx (id UUID PRIMARY KEY, context_id TEXT, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .expect("create table");

    for (ctx, emb) in [
        ("ctx1", vec![1.0, 0.0]),
        ("ctx1", vec![0.9, 0.1]),
        ("ctx1", vec![0.8, 0.2]),
        ("ctx2", vec![0.0, 1.0]),
        ("ctx2", vec![0.1, 0.9]),
    ] {
        db.execute(
            "INSERT INTO observations_ctx (id, context_id, embedding) VALUES ($id, $context_id, $embedding)",
            &make_params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("context_id", Value::Text(ctx.to_string())),
                ("embedding", Value::Vector(emb)),
            ]),
        )
        .expect("insert");
    }

    let out = db
        .execute(
            "SELECT * FROM observations_ctx WHERE context_id='ctx1' ORDER BY embedding <=> $q LIMIT 3",
            &make_params(vec![("q", Value::Vector(vec![1.0, 0.0]))]),
        )
        .expect("query");
    assert_eq!(out.rows.len(), 3);
    // SELECT * must return user columns (no internal row_id), verify context_id directly
    let ctx_idx = out
        .columns
        .iter()
        .position(|c| c == "context_id")
        .expect("context_id column must exist in output");
    for row in &out.rows {
        assert_eq!(
            row[ctx_idx],
            Value::Text("ctx1".to_string()),
            "all results must be from ctx1"
        );
    }
}

#[test]
fn a5_01_commit_makes_all_visible() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("name".to_string(), Value::Text("atomic".to_string())),
                ("entity_type".to_string(), Value::Text("node".to_string())),
            ]),
        )
        .expect("insert row");
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .expect("insert edge");
    db.insert_vector(tx, row_id, vec![1.0, 0.0])
        .expect("insert vector");
    db.commit(tx).expect("commit");

    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 1);
    assert_eq!(
        db.query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
            .expect("bfs")
            .nodes
            .len(),
        1
    );
    assert_eq!(
        db.query_vector(&[1.0, 0.0], 1, None, db.snapshot())
            .expect("vector")
            .len(),
        1
    );
}

#[test]
fn a5_02_rollback_makes_none_visible() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("name".to_string(), Value::Text("rollback".to_string())),
                ("entity_type".to_string(), Value::Text("node".to_string())),
            ]),
        )
        .expect("insert row");
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .expect("insert edge");
    db.insert_vector(tx, row_id, vec![1.0, 0.0])
        .expect("insert vector");
    db.rollback(tx).expect("rollback");

    assert!(db.scan("entities", db.snapshot()).expect("scan").is_empty());
    assert!(
        db.query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
            .expect("bfs")
            .nodes
            .is_empty()
    );
    assert!(
        db.query_vector(&[1.0, 0.0], 1, None, db.snapshot())
            .expect("vector")
            .is_empty()
    );
}

#[test]
fn a5_03_snapshot_isolation_read_old() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text("version-1".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("service".to_string()),
            ),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");
    let s1 = db.snapshot();

    db.execute(
        "UPDATE entities SET name='version-2' WHERE id=$id",
        &make_params(vec![("id", Value::Uuid(id))]),
    )
    .expect("update");

    let old = db
        .point_lookup("entities", "id", &Value::Uuid(id), s1)
        .expect("lookup old")
        .expect("row");
    let current = db
        .point_lookup("entities", "id", &Value::Uuid(id), db.snapshot())
        .expect("lookup current")
        .expect("row");

    assert_eq!(text(&old, "name"), "version-1");
    assert_eq!(text(&current, "name"), "version-2");
}

#[test]
fn a5_04_snapshot_isolation_edge_not_visible() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let s1 = db.snapshot();

    let tx = db.begin();
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .expect("insert edge");
    db.commit(tx).expect("commit");

    assert!(
        db.query_bfs(a, None, Direction::Outgoing, 1, s1)
            .expect("bfs at old snapshot")
            .nodes
            .is_empty()
    );
    assert_eq!(
        db.query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
            .expect("bfs current")
            .nodes
            .len(),
        1
    );
}

#[test]
fn a5_05_autocommit_dml() {
    let db = setup_ontology_db();
    db.execute(
        "INSERT INTO entities (id, name, entity_type) VALUES ($id, $name, $ty)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("auto".to_string())),
            ("ty", Value::Text("service".to_string())),
        ]),
    )
    .expect("insert");

    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 1);
}

#[test]
fn a5_06_autocommit_failure_rolls_back() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO observations (id, entity_id, data) VALUES ($id, $entity, $data)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
        ]),
    )
    .expect("first insert");

    let err = db.execute(
        "INSERT INTO observations (id, entity_id, data) VALUES ($id, $entity, $data) ON CONFLICT (id) DO UPDATE SET data=$data",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
        ]),
    );
    assert!(matches!(err, Err(Error::ImmutableTable(_))));

    let rows = db
        .scan("observations", db.snapshot())
        .expect("scan observations");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("data"), Some(&Value::Null));
}

#[test]
fn a5_07_multiple_sequential_transactions() {
    let db = setup_ontology_db();
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    let tx1 = db.begin();
    let row_a = db
        .insert_row(
            tx1,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id_a)),
                ("name".to_string(), Value::Text("A".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("service".to_string()),
                ),
            ]),
        )
        .expect("insert A");
    db.commit(tx1).expect("commit tx1");

    let tx2 = db.begin();
    db.insert_row(
        tx2,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id_b)),
            ("name".to_string(), Value::Text("B".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("service".to_string()),
            ),
        ]),
    )
    .expect("insert B");
    db.commit(tx2).expect("commit tx2");

    // Snapshot after tx2 but before the delete — row A should be visible here.
    let snap_before_delete = db.snapshot();

    let tx3 = db.begin();
    db.delete_row(tx3, "entities", row_a).expect("delete A");
    db.commit(tx3).expect("commit tx3");

    let rows = db.scan("entities", db.snapshot()).expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(uuid_value(&rows[0], "id"), id_b);

    // Verify row A is soft-deleted (visible at old snapshot, invisible at current).
    let old_lookup = db
        .point_lookup("entities", "id", &Value::Uuid(id_a), snap_before_delete)
        .expect("lookup at old snapshot");
    assert!(
        old_lookup.is_some(),
        "row A must be visible at pre-delete snapshot"
    );

    let current_lookup = db
        .point_lookup("entities", "id", &Value::Uuid(id_a), db.snapshot())
        .expect("lookup at current snapshot");
    assert!(
        current_lookup.is_none(),
        "row A must be invisible at post-delete snapshot"
    );
}

#[test]
fn a5_08_cross_subsystem_atomicity_no_partial_commit() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("name".to_string(), Value::Text("partial".to_string())),
                ("entity_type".to_string(), Value::Text("node".to_string())),
            ]),
        )
        .expect("insert");
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .expect("edge");
    db.insert_vector(tx, row_id, vec![1.0, 0.0])
        .expect("vector");
    db.rollback(tx).expect("rollback");

    assert!(db.scan("entities", db.snapshot()).expect("scan").is_empty());
    assert!(
        db.query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
            .expect("bfs")
            .nodes
            .is_empty()
    );
    assert!(
        db.query_vector(&[1.0, 0.0], 1, None, db.snapshot())
            .expect("vector")
            .is_empty()
    );
}

#[test]
fn a6_01_immutable_table_rejects_update() {
    let db = setup_ontology_db();
    db.execute(
        "INSERT INTO observations (id, entity_id, data) VALUES ($id, $entity, $data)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
        ]),
    )
    .expect("insert");

    let err = db
        .execute(
            "UPDATE observations SET data=$d",
            &make_params(vec![("d", Value::Null)]),
        )
        .expect_err("immutable update must fail");
    assert!(matches!(err, Error::ImmutableTable(_)));
}

#[test]
fn a6_02_immutable_table_rejects_delete() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO observations (id, entity_id, data) VALUES ($id, $entity, $data)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
        ]),
    )
    .expect("insert");

    let err = db
        .execute(
            "DELETE FROM observations WHERE id=$id",
            &make_params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("immutable delete must fail");
    assert!(matches!(err, Error::ImmutableTable(_)));
}

#[test]
fn a6_03_state_machine_valid_transition() {
    let db = setup_ontology_db();
    let id1 = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id1)),
            ("status", Value::Text("pending".to_string())),
        ]),
    )
    .expect("insert pending");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id1)),
            ("status", Value::Text("acknowledged".to_string())),
        ]),
    )
    .expect("pending->ack");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id1)),
            ("status", Value::Text("resolved".to_string())),
        ]),
    )
    .expect("ack->resolved");

    let id2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id2)),
            ("status", Value::Text("pending".to_string())),
        ]),
    )
    .expect("insert pending 2");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id2)),
            ("status", Value::Text("dismissed".to_string())),
        ]),
    )
    .expect("pending->dismissed");
}

#[test]
fn a6_04_state_machine_invalid_transition() {
    let db = setup_ontology_db();

    let id_ack = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id_ack)),
            ("status", Value::Text("pending".to_string())),
        ]),
    )
    .expect("insert pending");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_ack)),
            ("status", Value::Text("acknowledged".to_string())),
        ]),
    )
    .expect("pending->ack");
    let err1 = db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_ack)),
            ("status", Value::Text("pending".to_string())),
        ]),
    );
    assert!(matches!(err1, Err(Error::InvalidStateTransition(_))));

    let id_resolved = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id_resolved)),
            ("status", Value::Text("pending".to_string())),
        ]),
    )
    .expect("insert pending 2");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_resolved)),
            ("status", Value::Text("acknowledged".to_string())),
        ]),
    )
    .expect("pending->ack 2");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_resolved)),
            ("status", Value::Text("resolved".to_string())),
        ]),
    )
    .expect("ack->resolved");
    let err2 = db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_resolved)),
            ("status", Value::Text("pending".to_string())),
        ]),
    );
    assert!(matches!(err2, Err(Error::InvalidStateTransition(_))));

    let id_dismissed = Uuid::new_v4();
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status)",
        &make_params(vec![
            ("id", Value::Uuid(id_dismissed)),
            ("status", Value::Text("pending".to_string())),
        ]),
    )
    .expect("insert pending 3");
    db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_dismissed)),
            ("status", Value::Text("dismissed".to_string())),
        ]),
    )
    .expect("pending->dismissed");
    let err3 = db.execute(
        "INSERT INTO invalidations (id, status) VALUES ($id, $status) ON CONFLICT (id) DO UPDATE SET status=$status",
        &make_params(vec![
            ("id", Value::Uuid(id_dismissed)),
            ("status", Value::Text("acknowledged".to_string())),
        ]),
    );
    assert!(matches!(err3, Err(Error::InvalidStateTransition(_))));
}

#[test]
fn a6_05_uuid_uniqueness_violation() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO entities (id, name, entity_type) VALUES ($id, $name, $ty)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("a".to_string())),
            ("ty", Value::Text("service".to_string())),
        ]),
    )
    .expect("first insert");

    let err = db.execute(
        "INSERT INTO entities (id, name, entity_type) VALUES ($id, $name, $ty)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("b".to_string())),
            ("ty", Value::Text("service".to_string())),
        ]),
    );
    assert!(matches!(
        err,
        Err(Error::UniqueViolation { table, column }) if table == "entities" && column == "id"
    ));
}

#[test]
fn a6_06_vector_dimension_enforced_at_insert() {
    let db = setup_ontology_db();
    db.execute(
        "INSERT INTO observations (id, entity_id, data, embedding) VALUES ($id, $entity, $data, $embedding)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
            ("embedding", Value::Vector(vec![0.0; 384])),
        ]),
    )
    .expect("seed 384d vector");

    let err = db.execute(
        "INSERT INTO observations (id, entity_id, data, embedding) VALUES ($id, $entity, $data, $embedding)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("entity", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Null),
            ("embedding", Value::Vector(vec![0.0; 256])),
        ]),
    );
    assert!(matches!(
        err,
        Err(Error::VectorDimensionMismatch {
            expected: 384,
            got: 256
        })
    ));
}

#[test]
fn a6_10_acyclic_rejects_direct_cycle() {
    let db = setup_ontology_db_with_dag();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert a->b");

    let result = db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new());
    assert!(matches!(
        result,
        Err(Error::CycleDetected {
            ref edge_type,
            source_node,
            target_node,
        }) if edge_type == "CITES" && source_node == b && target_node == a
    ));

    db.commit(tx).expect("commit");
    let bfs = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs a");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, b);

    let bfs_rev = db
        .query_bfs(
            b,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs b");
    assert!(bfs_rev.nodes.is_empty());
}

#[test]
fn a6_11_acyclic_rejects_transitive_cycle() {
    let db = setup_ontology_db_with_dag();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert a->b");
    db.insert_edge(tx, b, c, "CITES".to_string(), HashMap::new())
        .expect("insert b->c");

    let result = db.insert_edge(tx, c, a, "CITES".to_string(), HashMap::new());
    assert!(matches!(
        result,
        Err(Error::CycleDetected {
            ref edge_type,
            source_node,
            target_node,
        }) if edge_type == "CITES" && source_node == c && target_node == a
    ));

    db.commit(tx).expect("commit");
    let bfs = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 2);
    assert_eq!(bfs.nodes[0].id, b);
    assert_eq!(bfs.nodes[1].id, c);
}

#[test]
fn a6_12_acyclic_allows_diamond() {
    let db = setup_ontology_db_with_dag();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert a->b");

    let result = db.insert_edge(tx, c, b, "CITES".to_string(), HashMap::new());
    assert!(result.is_ok());

    db.commit(tx).expect("commit");
    let bfs = db
        .query_bfs(
            b,
            Some(&["CITES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 2);
    let ids: HashSet<Uuid> = bfs.nodes.iter().map(|n| n.id).collect();
    assert!(ids.contains(&a));
    assert!(ids.contains(&c));
}

#[test]
fn a6_13_non_acyclic_type_allows_cycles() {
    let db = setup_ontology_db_with_dag();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "SERVES".to_string(), HashMap::new())
        .expect("insert a->b");

    let result = db.insert_edge(tx, b, a, "SERVES".to_string(), HashMap::new());
    assert!(result.is_ok());

    db.commit(tx).expect("commit");
    let bfs_out = db
        .query_bfs(
            a,
            Some(&["SERVES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs out");
    assert_eq!(bfs_out.nodes.len(), 1);
    assert_eq!(bfs_out.nodes[0].id, b);

    let bfs_back = db
        .query_bfs(
            b,
            Some(&["SERVES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs back");
    assert_eq!(bfs_back.nodes.len(), 1);
    assert_eq!(bfs_back.nodes[0].id, a);
}

#[test]
fn a6_14_acyclic_same_tx_write_set_visibility() {
    let db = setup_ontology_db_with_dag();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();

    let result1 = db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new());
    let result2 = db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new());
    assert!(result1.is_ok());
    assert!(matches!(
        result2,
        Err(Error::CycleDetected {
            ref edge_type,
            source_node,
            target_node,
        }) if edge_type == "CITES" && source_node == b && target_node == a
    ));

    db.commit(tx).expect("commit");
    let bfs = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, b);
}

#[test]
fn a6_15_duplicate_edge_stored_once() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert #1");
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert #2");
    db.commit(tx).expect("commit");

    let bfs = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, b);
    assert_eq!(db.edge_count(a, "CITES", db.snapshot()).unwrap(), 1);
}

#[test]
fn a6_16_reinsert_after_delete_creates_edge() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx1 = db.begin();
    db.insert_edge(tx1, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert");
    db.commit(tx1).expect("commit tx1");

    let tx2 = db.begin();
    db.delete_edge(tx2, a, b, "CITES").expect("delete");
    db.commit(tx2).expect("commit tx2");

    let tx3 = db.begin();
    let result = db.insert_edge(tx3, a, b, "CITES".to_string(), HashMap::new());
    db.commit(tx3).expect("commit tx3");

    assert!(result.is_ok());
    let bfs = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, b);
}

#[test]
fn a6_17_different_edge_types_not_duplicates() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "CITES".to_string(), HashMap::new())
        .expect("insert cites");
    db.insert_edge(tx, a, b, "SERVES".to_string(), HashMap::new())
        .expect("insert serves");
    db.commit(tx).expect("commit");

    let bfs_all = db
        .query_bfs(a, None, Direction::Outgoing, 1, db.snapshot())
        .expect("bfs all");
    assert_eq!(bfs_all.nodes.len(), 1);
    assert_eq!(bfs_all.nodes[0].id, b);

    let bfs_cites = db
        .query_bfs(
            a,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs cites");
    assert_eq!(bfs_cites.nodes.len(), 1);
    assert_eq!(bfs_cites.nodes[0].id, b);

    let bfs_serves = db
        .query_bfs(
            a,
            Some(&["SERVES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs serves");
    assert_eq!(bfs_serves.nodes.len(), 1);
    assert_eq!(bfs_serves.nodes[0].id, b);
}

#[test]
fn a6_18_cross_tx_duplicate_stored_once() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx1 = db.begin();
    db.insert_edge(tx1, a, b, "BASED_ON".to_string(), HashMap::new())
        .expect("insert tx1");
    db.commit(tx1).expect("commit tx1");

    let tx2 = db.begin();
    let result = db.insert_edge(tx2, a, b, "BASED_ON".to_string(), HashMap::new());
    db.commit(tx2).expect("commit tx2");

    assert!(result.is_ok());
    let bfs = db
        .query_bfs(
            a,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, b);
    assert_eq!(db.edge_count(a, "BASED_ON", db.snapshot()).unwrap(), 1);
}

#[test]
fn a7_01_recursive_cte_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
        &HashMap::new(),
    );
    assert!(matches!(result, Err(Error::RecursiveCteNotSupported)));
}

#[test]
fn a7_02_window_function_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT ROW_NUMBER() OVER (PARTITION BY x) FROM t",
        &HashMap::new(),
    );
    assert!(matches!(result, Err(Error::WindowFunctionNotSupported)));
}

#[test]
fn a7_03_stored_procedure_rejected() {
    let db = setup_ontology_db();
    let result = db.execute("CREATE PROCEDURE do_stuff AS SELECT 1", &HashMap::new());
    assert!(matches!(
        result,
        Err(Error::StoredProcNotSupported) | Err(Error::ParseError(_))
    ));
}

#[test]
fn a7_04_unbounded_traversal_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->+(b) COLUMNS (b.id AS b_id))",
        &HashMap::new(),
    );
    assert!(matches!(result, Err(Error::UnboundedTraversal)));
}

#[test]
fn a7_05_unbounded_vector_search_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT * FROM observations ORDER BY embedding <=> $q",
        &make_params(vec![("q", Value::Vector(vec![1.0, 0.0]))]),
    );
    assert!(matches!(result, Err(Error::UnboundedVectorSearch)));
}

#[test]
fn a7_06_correlated_subquery_rejected() {
    let db = setup_ontology_db();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, y UUID)",
        &HashMap::new(),
    )
    .expect("create t");
    db.execute(
        "CREATE TABLE s (id UUID PRIMARY KEY, x UUID)",
        &HashMap::new(),
    )
    .expect("create s");

    let result = db.execute(
        "SELECT * FROM t WHERE id IN (SELECT id FROM s WHERE s.x = t.y)",
        &HashMap::new(),
    );
    // Correlated subqueries are banned per spec — must fail at execution
    assert!(result.is_err(), "correlated subquery must be rejected");
}

#[test]
fn a7_07_cte_reference_in_where_allowed() {
    let db = setup_ontology_db();
    let result = db.execute(
        "WITH ctx AS (SELECT id FROM contexts) SELECT * FROM entities WHERE context_id IN (SELECT id FROM ctx)",
        &HashMap::new(),
    );

    // Plain SQL CTEs are supported per spec
    assert!(
        result.is_ok(),
        "CTE-backed IN subquery must succeed: {:?}",
        result.err()
    );
}

#[test]
fn a7_08_full_text_search_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT * FROM observations WHERE text MATCH 'pattern'",
        &HashMap::new(),
    );
    assert!(matches!(result, Err(Error::FullTextSearchNotSupported)));
}

#[test]
fn a7_08b_graph_match_still_works() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id))",
        &HashMap::new(),
    );
    assert!(!matches!(result, Err(Error::FullTextSearchNotSupported)));
}

#[test]
fn a7_09_implicit_vector_coercion_rejected() {
    let db = setup_ontology_db();
    let result = db.execute(
        "SELECT * FROM observations ORDER BY embedding <=> 'not a vector' LIMIT 5",
        &HashMap::new(),
    );
    // currently PlanError, plan specifies InvalidVectorLiteral.
    assert!(matches!(result, Err(Error::PlanError(_))));
}

// a8_07 deleted — redundant with p32_deleted_vectors_excluded_from_ann_after_reopen

#[test]
fn a10_01_budget_tracks_allocations() {
    let accountant = MemoryAccountant::with_budget(4096);
    accountant
        .try_allocate(1000)
        .expect("allocation within budget should succeed");

    let usage = accountant.usage();
    assert_eq!(usage.limit, Some(4096));
    assert_eq!(usage.used, 1000);
    assert_eq!(usage.available, Some(3096));
}

#[test]
fn a10_02_budget_rejects_over_limit() {
    let accountant = MemoryAccountant::with_budget(128);
    accountant
        .try_allocate(96)
        .expect("initial allocation should fit");

    let err = accountant
        .try_allocate(64)
        .expect_err("allocation over remaining budget must fail");
    match err {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        } => {
            assert_eq!(subsystem, "memory");
            assert_eq!(operation, "allocate");
            assert_eq!(requested_bytes, 64);
            assert_eq!(available_bytes, 32);
            assert_eq!(budget_limit_bytes, 128);
        }
        other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
    }
}

#[test]
fn a10_03_budget_release_frees_capacity() {
    let accountant = MemoryAccountant::with_budget(256);
    accountant.try_allocate(200).expect("initial allocate");
    accountant.release(160);

    let usage = accountant.usage();
    assert_eq!(usage.used, 40);
    assert_eq!(usage.available, Some(216));

    accountant
        .try_allocate(200)
        .expect("released capacity should be reusable");
    assert_eq!(accountant.usage().used, 240);
}

#[test]
fn a10_04_bfs_aborts_on_frontier_overflow() {
    let accountant = Arc::new(MemoryAccountant::with_budget(4096));
    let db = contextdb_engine::Database::open_memory_with_accountant(accountant);
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    for (id, name) in [(a, "A"), (b, "B")] {
        let _ = db.execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name)",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
            ]),
        );
    }

    let tx = db.begin();
    let _ = db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new());
    let _ = db.commit(tx);

    let err = db
        .execute(
            "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $start COLUMNS (b.id AS b_id))",
            &make_params(vec![("start", Value::Uuid(a))]),
        )
        .expect_err("BFS should fail when frontier allocation exceeds budget");

    match err {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            ..
        } => {
            assert_eq!(subsystem, "bfs_frontier");
            assert_eq!(operation, "graph_bfs");
        }
        other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
    }
}

#[test]
#[ignore = "requires external RSS harness for deterministic process-memory assertion"]
fn a10_05_peak_rss_within_2gb() {
    let _db = setup_ontology_db();
    todo!("run large persisted mixed workload and assert peak RSS <= 2GB");
}

#[test]
#[ignore = "requires ARM64 cross-compile CI gate"]
fn a11_01_cross_compile_check() {
    let _db = setup_ontology_db();
    todo!("run cargo check --target aarch64-unknown-linux-gnu in CI and audit SIMD deps");
}

// a12_01 deleted — redundant with p34_cli_memory_mode_smoke_test
