use contextdb_core::{Error, Lsn, SingleColumnForeignKey, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

fn vals(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Open a fresh on-disk database, run `populate`, close it, reopen from the same
/// path, and return the full changeset since `Lsn(0)`. The reopen-and-diff
/// scaffold is identical across the ds01/02/03/04/11/12/13 tests; each supplies
/// only its populate closure and asserts on the returned changeset.
fn persist_and_reopen_changeset(db_name: &str, populate: impl FnOnce(&Database)) -> ChangeSet {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join(db_name);
    {
        let db = Database::open(&path).unwrap();
        populate(&db);
        db.close().unwrap();
    }
    let db2 = Database::open(&path).unwrap();
    db2.changes_since(Lsn(0))
}

#[test]
fn ds01_rows_available_after_reopen() {
    let alice_id = Uuid::new_v4();
    let bob_id = Uuid::new_v4();

    let cs = persist_and_reopen_changeset("ds01.db", |db| {
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, score REAL)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        db.insert_row(
            tx,
            "items",
            vals(vec![
                ("id", Value::Uuid(alice_id)),
                ("name", Value::Text("alice".into())),
                ("score", Value::Float64(1.5)),
            ]),
        )
        .unwrap();
        db.insert_row(
            tx,
            "items",
            vals(vec![
                ("id", Value::Uuid(bob_id)),
                ("name", Value::Text("bob".into())),
                ("score", Value::Float64(2.7)),
            ]),
        )
        .unwrap();
        db.commit(tx).unwrap();
    });

    assert_eq!(
        cs.rows.len(),
        2,
        "expected 2 rows in changeset after reopen"
    );
    let alice_row = cs
        .rows
        .iter()
        .find(|r| r.natural_key.value == Value::Uuid(alice_id))
        .expect("alice row missing from changeset");
    assert_eq!(alice_row.values["name"], Value::Text("alice".into()));
    assert_eq!(alice_row.values["score"], Value::Float64(1.5));
    let bob_row = cs
        .rows
        .iter()
        .find(|r| r.natural_key.value == Value::Uuid(bob_id))
        .expect("bob row missing from changeset");
    assert_eq!(bob_row.values["name"], Value::Text("bob".into()));
    assert_eq!(bob_row.values["score"], Value::Float64(2.7));
}

#[test]
fn ds02_ddl_in_changeset_after_reopen() {
    let cs = persist_and_reopen_changeset("ds02.db", |db| {
        db.execute(
            "CREATE TABLE events (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE workflows (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: draft -> [active], active -> [archived])",
            &HashMap::new(),
        )
        .unwrap();
    });

    assert_eq!(cs.ddl.len(), 2, "expected 2 DDL entries after reopen");
    let events_ddl = cs
        .ddl
        .iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "events"))
        .expect("events DDL missing");
    match events_ddl {
        DdlChange::CreateTable {
            columns,
            constraints,
            ..
        } => {
            assert!(
                columns
                    .iter()
                    .any(|(col, ty)| col == "id" && ty.starts_with("UUID")),
                "events DDL missing 'id UUID' column"
            );
            assert!(
                columns
                    .iter()
                    .any(|(col, ty)| col == "data" && ty == "TEXT"),
                "events DDL missing 'data TEXT' column"
            );
            assert!(
                constraints.iter().any(|c| c.contains("IMMUTABLE")),
                "events DDL missing IMMUTABLE constraint"
            );
        }
        DdlChange::DropTable { .. } => unreachable!(),
        DdlChange::AlterTable { .. } => unreachable!(),
        DdlChange::CreateIndex { .. }
        | DdlChange::DropIndex { .. }
        | DdlChange::CreateTrigger { .. }
        | DdlChange::DropTrigger { .. }
        | DdlChange::CreateEventType { .. }
        | DdlChange::CreateSink { .. }
        | DdlChange::CreateRoute { .. }
        | DdlChange::DropRoute { .. } => unreachable!(),
    }
    let workflows_ddl = cs
        .ddl
        .iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "workflows"))
        .expect("workflows DDL missing");
    match workflows_ddl {
        DdlChange::CreateTable { columns, .. } => {
            assert!(
                columns
                    .iter()
                    .any(|(col, ty)| col == "id" && ty.starts_with("UUID")),
                "workflows DDL missing 'id UUID' column"
            );
            assert!(
                columns
                    .iter()
                    .any(|(col, ty)| col == "status" && ty == "TEXT"),
                "workflows DDL missing 'status TEXT' column"
            );
        }
        DdlChange::DropTable { .. } => unreachable!(),
        DdlChange::AlterTable { .. } => unreachable!(),
        DdlChange::CreateIndex { .. }
        | DdlChange::DropIndex { .. }
        | DdlChange::CreateTrigger { .. }
        | DdlChange::DropTrigger { .. }
        | DdlChange::CreateEventType { .. }
        | DdlChange::CreateSink { .. }
        | DdlChange::CreateRoute { .. }
        | DdlChange::DropRoute { .. } => unreachable!(),
    }
}

#[test]
fn ds03_edges_available_after_reopen() {
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();

    let cs = persist_and_reopen_changeset("ds03.db", |db| {
        db.execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        db.insert_row(
            tx,
            "nodes",
            vals(vec![
                ("id", Value::Uuid(node_a)),
                ("name", Value::Text("node_a".into())),
            ]),
        )
        .unwrap();
        db.insert_row(
            tx,
            "nodes",
            vals(vec![
                ("id", Value::Uuid(node_b)),
                ("name", Value::Text("node_b".into())),
            ]),
        )
        .unwrap();
        db.insert_edge(
            tx,
            node_a,
            node_b,
            "RELATES_TO".into(),
            vals(vec![("weight", Value::Float64(0.9))]),
        )
        .unwrap();
        db.commit(tx).unwrap();
    });

    assert_eq!(
        cs.edges.len(),
        1,
        "expected 1 edge in changeset after reopen"
    );
    assert_eq!(cs.edges[0].source, node_a);
    assert_eq!(cs.edges[0].target, node_b);
    assert_eq!(cs.edges[0].edge_type, "RELATES_TO");
    assert_eq!(
        cs.edges[0].properties.get("weight"),
        Some(&Value::Float64(0.9))
    );
}

#[test]
fn ds04_vectors_available_after_reopen() {
    let cs = persist_and_reopen_changeset("ds04.db", |db| {
        db.execute(
            "CREATE TABLE obs (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        let row_id = db
            .insert_row(
                tx,
                "obs",
                vals(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("data", Value::Text("observation_1".into())),
                ]),
            )
            .unwrap();
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("obs", "embedding"),
            row_id,
            vec![0.6, 0.8, 0.0],
        )
        .unwrap();
        db.commit(tx).unwrap();
    });

    assert_eq!(
        cs.vectors.len(),
        1,
        "expected 1 vector in changeset after reopen"
    );
    assert_eq!(cs.vectors[0].vector, vec![0.6, 0.8, 0.0]);
}

#[test]
fn ds05_edge_restart_push_full_dataset() {
    let tmp = TempDir::new().unwrap();
    let edge_path = tmp.path().join("ds05_edge.db");
    let reading_id = Uuid::new_v4();

    {
        let edge_db = Database::open(&edge_path).unwrap();
        edge_db
            .execute(
                "CREATE TABLE readings (id UUID PRIMARY KEY, sensor TEXT, value REAL)",
                &HashMap::new(),
            )
            .unwrap();
        let tx = edge_db.begin_or_panic();
        edge_db
            .insert_row(
                tx,
                "readings",
                vals(vec![
                    ("id", Value::Uuid(reading_id)),
                    ("sensor", Value::Text("temp_1".into())),
                    ("value", Value::Float64(23.5)),
                ]),
            )
            .unwrap();
        edge_db.commit(tx).unwrap();
        edge_db.close().unwrap();
    }

    let edge_db2 = Database::open(&edge_path).unwrap();
    let cs = edge_db2.changes_since(Lsn(0));

    let server = Database::open_memory();
    server
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .unwrap();

    let rows = server.scan("readings", server.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "server should have 1 row after applying edge push"
    );
    let row = &rows[0];
    assert_eq!(
        row.values.get("sensor"),
        Some(&Value::Text("temp_1".into()))
    );
    assert_eq!(row.values.get("value"), Some(&Value::Float64(23.5)));
}

#[test]
fn ds06_idempotent_row_reapply() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    let known_id = Uuid::new_v4();
    let tx = source.begin_or_panic();
    source
        .insert_row(
            tx,
            "users",
            vals(vec![
                ("id", Value::Uuid(known_id)),
                ("name", Value::Text("alice".into())),
            ]),
        )
        .unwrap();
    source.commit(tx).unwrap();

    let cs = source.changes_since(Lsn(0));
    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            cs.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .unwrap();

    let rows = receiver.scan("users", receiver.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "rows must not duplicate on re-apply");
    assert_eq!(
        rows[0].values.get("name"),
        Some(&Value::Text("alice".into()))
    );
}

#[test]
fn ds07_idempotent_edge_reapply() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    let src_id = Uuid::new_v4();
    let tgt_id = Uuid::new_v4();
    let tx = source.begin_or_panic();
    source
        .insert_row(
            tx,
            "nodes",
            vals(vec![
                ("id", Value::Uuid(src_id)),
                ("name", Value::Text("src".into())),
            ]),
        )
        .unwrap();
    source
        .insert_row(
            tx,
            "nodes",
            vals(vec![
                ("id", Value::Uuid(tgt_id)),
                ("name", Value::Text("tgt".into())),
            ]),
        )
        .unwrap();
    source
        .insert_edge(tx, src_id, tgt_id, "LINKS_TO".into(), HashMap::new())
        .unwrap();
    source.commit(tx).unwrap();

    let cs = source.changes_since(Lsn(0));
    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            cs.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .unwrap();

    let count = receiver
        .edge_count(src_id, "LINKS_TO", receiver.snapshot())
        .unwrap();
    assert_eq!(count, 1, "edges must not duplicate on re-apply");

    let cs_after = receiver.changes_since(Lsn(0));
    let matching_edges: Vec<_> = cs_after
        .edges
        .iter()
        .filter(|e| e.source == src_id && e.target == tgt_id && e.edge_type == "LINKS_TO")
        .collect();
    assert_eq!(
        matching_edges.len(),
        1,
        "changeset must contain exactly 1 edge for this triple"
    );
}

#[test]
fn ds08_idempotent_vector_reapply() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
    let obs_id = Uuid::new_v4();
    let tx = source.begin_or_panic();
    let row_id = source
        .insert_row(tx, "obs", vals(vec![("id", Value::Uuid(obs_id))]))
        .unwrap();
    source
        .insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("obs", "embedding"),
            row_id,
            vec![1.0, 0.0, 0.0],
        )
        .unwrap();
    source.commit(tx).unwrap();

    let cs = source.changes_since(Lsn(0));
    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            cs.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .unwrap();

    let results = receiver
        .query_vector(
            contextdb_core::VectorIndexRef::new("obs", "embedding"),
            &[1.0, 0.0, 0.0],
            10,
            None,
            receiver.snapshot(),
        )
        .unwrap();
    assert_eq!(results.len(), 1, "vectors must not duplicate on re-apply");
    assert!(
        (results[0].1 - 1.0).abs() < 1e-6,
        "expected exact vector match"
    );
}

#[test]
fn ds09_future_watermark_returns_empty() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ds09.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        db.insert_row(
            tx,
            "items",
            vals(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text("something".into())),
            ]),
        )
        .unwrap();
        db.commit(tx).unwrap();
        db.close().unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let current_lsn = db2.current_lsn();
    let cs = db2.changes_since(Lsn(current_lsn.0 + 1000));

    assert!(
        cs.rows.is_empty(),
        "future watermark must return empty rows"
    );
    assert!(
        cs.edges.is_empty(),
        "future watermark must return empty edges"
    );
    assert!(
        cs.vectors.is_empty(),
        "future watermark must return empty vectors"
    );
    assert!(cs.ddl.is_empty(), "future watermark must return empty ddl");
}

#[test]
fn ds10_schema_divergence_detected() {
    let server = Database::open_memory();
    server
        .execute(
            "CREATE TABLE measurements (id UUID PRIMARY KEY, value REAL)",
            &HashMap::new(),
        )
        .unwrap();

    let divergent_cs = ChangeSet {
        rows: vec![],
        edges: vec![],
        vectors: vec![],
        ddl: vec![DdlChange::CreateTable {
            name: "measurements".into(),
            columns: vec![
                ("id".into(), "UUID".into()),
                ("value".into(), "REAL".into()),
                ("unit".into(), "TEXT".into()),
            ],
            constraints: vec![],
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }],

        ddl_lsn: vec![Lsn(1)],
    };

    server
        .apply_changes(
            divergent_cs,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let meta = server.table_meta("measurements").unwrap();
    assert_eq!(
        meta.columns.len(),
        2,
        "schema must not be overwritten by divergent DDL"
    );
    assert!(
        meta.columns.iter().all(|c| c.name != "unit"),
        "divergent column 'unit' must not appear in local schema"
    );

    let tx = server.begin_or_panic();
    server
        .insert_row(
            tx,
            "measurements",
            vals(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("value", Value::Float64(42.0)),
            ]),
        )
        .unwrap();
    server.commit(tx).unwrap();
    let rows = server.scan("measurements", server.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "table must still accept rows after divergent DDL skip"
    );
}

#[test]
fn ds10b_public_apply_changes_rejects_ddl_without_matching_lsn() {
    let db = Database::open_memory();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::CreateTable {
                    name: "missing_lsn".into(),
                    columns: vec![("id".into(), "UUID PRIMARY KEY".into())],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: Vec::new(),
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("public apply_changes must reject DDL without a matching ddl_lsn");
    assert!(
        matches!(err, Error::SyncError(ref message) if message.contains("ddl_lsn length")),
        "unexpected missing ddl_lsn error: {err}"
    );
    assert!(
        db.table_meta("missing_lsn").is_none(),
        "invalid DDL envelope must not apply schema side effects"
    );
}

#[test]
fn ds10c_apply_changes_rejects_scan_backed_exact_constraint_ddl() {
    let db = Database::open_memory();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::CreateTable {
                    name: "bad_exact_key".into(),
                    columns: vec![
                        ("id".into(), "INTEGER PRIMARY KEY".into()),
                        ("score".into(), "REAL UNIQUE".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(1)],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("sync DDL must reject exact constraints without exact key semantics");
    match err {
        Error::ColumnNotIndexable { table, column, .. } => {
            assert_eq!(table, "bad_exact_key");
            assert_eq!(column, "score");
        }
        other => panic!("expected ColumnNotIndexable for sync DDL, got {other:?}"),
    }
    assert!(
        db.table_meta("bad_exact_key").is_none(),
        "invalid sync DDL must not leave schema side effects"
    );
}

#[test]
fn ds10d_sync_alter_unique_name_collision_rebuilds_auto_index_storage() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, a TEXT NOT NULL, \
         b TEXT NOT NULL, embedding VECTOR(2), UNIQUE (a, b))",
        &empty,
    )
    .unwrap();
    db.execute("INSERT INTO p (id, a, b) VALUES (1, 'a-1', 'b-1')", &empty)
        .unwrap();

    db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::AlterTable {
                name: "p".into(),
                columns: vec![
                    ("id".into(), "INTEGER PRIMARY KEY".into()),
                    ("a_b".into(), "TEXT UNIQUE".into()),
                    ("a".into(), "TEXT NOT NULL".into()),
                    ("b".into(), "TEXT NOT NULL".into()),
                    ("embedding".into(), "VECTOR(3)".into()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(2)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();

    let meta = db.table_meta("p").unwrap();
    assert!(
        meta.indexes.iter().any(|index| {
            index.name == "__unique_a_b" && index.columns.len() == 1 && index.columns[0].0 == "a_b"
        }),
        "sync ALTER must move the colliding auto-index name onto the new exact key"
    );

    db.execute("UPDATE p SET a_b = 'key-1' WHERE id = 1", &empty)
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_key TEXT REFERENCES p(a_b))",
        &empty,
    )
    .unwrap();
    db.__reset_commit_stage_stats();
    let inserted = db
        .execute("INSERT INTO c (id, p_key) VALUES (1, 'key-1')", &empty)
        .unwrap();
    assert_eq!(inserted.rows_affected, 1);
    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 1,
        "sync-altered FK parent key should use rebuilt exact index storage, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "sync-altered UNIQUE name collision must not fall back to committed row scans"
    );
}

#[test]
fn ds10e_sync_apply_exact_natural_keys_do_not_scan_visible_tables() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, note TEXT)",
        &empty,
    )
    .unwrap();

    let tx = db.begin_or_panic();
    for id in 0..512 {
        db.insert_row(
            tx,
            "items",
            vals(vec![
                ("id", Value::Int64(id)),
                ("code", Value::Text(format!("code-{id}"))),
                ("note", Value::Text("seed".into())),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_relational_scan_rows_touched();
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![
                    RowChange {
                        table: "items".into(),
                        natural_key: NaturalKey {
                            column: "id".into(),
                            value: Value::Int64(7),
                        },
                        values: vals(vec![("id", Value::Int64(7))]),
                        deleted: true,
                        lsn: Lsn(100),
                        created_at: None,
                    },
                    RowChange {
                        table: "items".into(),
                        natural_key: NaturalKey {
                            column: "id".into(),
                            value: Value::Int64(900),
                        },
                        values: vals(vec![
                            ("id", Value::Int64(900)),
                            ("code", Value::Text("code-200".into())),
                            ("note", Value::Text("conflict".into())),
                        ]),
                        deleted: false,
                        lsn: Lsn(101),
                        created_at: None,
                    },
                    RowChange {
                        table: "items".into(),
                        natural_key: NaturalKey {
                            column: "id".into(),
                            value: Value::Int64(901),
                        },
                        values: vals(vec![
                            ("id", Value::Int64(901)),
                            ("code", Value::Text("remote-901".into())),
                            ("note", Value::Text("fresh".into())),
                        ]),
                        deleted: false,
                        lsn: Lsn(102),
                        created_at: None,
                    },
                ],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 3);
    assert_eq!(result.skipped_rows, 0);
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "sync apply exact-key lookups must not materialize visible table rows"
    );
}

#[test]
fn ds10f_sync_preflight_new_fk_column_parent_delete_does_not_scan_child_table() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty)
        .unwrap();
    db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY)", &empty)
        .unwrap();
    db.execute("INSERT INTO p (id) VALUES (1)", &empty).unwrap();

    let tx = db.begin_or_panic();
    for id in 0..512 {
        db.insert_row(tx, "c", vals(vec![("id", Value::Int64(id))]))
            .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_relational_scan_rows_touched();
    let result = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::AlterTable {
                    name: "c".into(),
                    columns: vec![("p_id".into(), "INTEGER".into())],
                    constraints: Vec::new(),
                    foreign_keys: vec![SingleColumnForeignKey {
                        child_column: "p_id".into(),
                        parent_table: "p".into(),
                        parent_column: "id".into(),
                    }],
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
                rows: vec![RowChange {
                    table: "p".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(1),
                    },
                    values: vals(vec![("id", Value::Int64(1))]),
                    deleted: true,
                    lsn: Lsn(11),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 1);
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "sync preflight for newly added FK columns must not scan committed child rows"
    );
}

#[test]
fn ds10g_sync_preflight_existing_column_new_fk_parent_delete_uses_existing_index() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty)
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER)",
        &empty,
    )
    .unwrap();
    db.execute("CREATE INDEX c_p_id_idx ON c(p_id)", &empty)
        .unwrap();
    db.execute("INSERT INTO p (id) VALUES (1)", &empty).unwrap();

    let tx = db.begin_or_panic();
    for id in 0..512 {
        db.insert_row(
            tx,
            "c",
            vals(vec![("id", Value::Int64(id)), ("p_id", Value::Null)]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_relational_scan_rows_touched();
    let result = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::AlterTable {
                    name: "c".into(),
                    columns: vec![("p_id".into(), "INTEGER".into())],
                    constraints: Vec::new(),
                    foreign_keys: vec![SingleColumnForeignKey {
                        child_column: "p_id".into(),
                        parent_table: "p".into(),
                        parent_column: "id".into(),
                    }],
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
                rows: vec![RowChange {
                    table: "p".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(1),
                    },
                    values: vals(vec![("id", Value::Int64(1))]),
                    deleted: true,
                    lsn: Lsn(11),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 1);
    let meta = db.table_meta("c").unwrap();
    let p_id = meta
        .columns
        .iter()
        .find(|column| column.name == "p_id")
        .unwrap();
    assert!(
        p_id.references.is_some(),
        "sync ALTER should install the FK on the existing child column"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "sync preflight for newly added FK relationships must not scan committed child rows"
    );
}

#[test]
fn ds10h_sync_preflight_existing_column_new_fk_parent_delete_without_index_rejects_before_schema() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty)
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER)",
        &empty,
    )
    .unwrap();
    db.execute("INSERT INTO p (id) VALUES (1)", &empty).unwrap();
    db.execute("INSERT INTO c (id, p_id) VALUES (10, 1)", &empty)
        .unwrap();

    db.__reset_relational_scan_rows_touched();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::AlterTable {
                    name: "c".into(),
                    columns: vec![("p_id".into(), "INTEGER".into())],
                    constraints: Vec::new(),
                    foreign_keys: vec![SingleColumnForeignKey {
                        child_column: "p_id".into(),
                        parent_table: "p".into(),
                        parent_column: "id".into(),
                    }],
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(10)],
                rows: vec![RowChange {
                    table: "p".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(1),
                    },
                    values: vals(vec![("id", Value::Int64(1))]),
                    deleted: true,
                    lsn: Lsn(11),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap_err();

    assert!(
        format!("{err}").contains("no covering index"),
        "expected no-covering-index preflight error, got {err}"
    );
    let meta = db.table_meta("c").unwrap();
    let p_id = meta
        .columns
        .iter()
        .find(|column| column.name == "p_id")
        .unwrap();
    assert!(
        p_id.references.is_none(),
        "failed sync preflight must not install the projected FK"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "sync preflight rejection must not fall back to scanning committed child rows"
    );
}

#[test]
fn ds10i_sync_preflight_projected_parent_unique_uses_existing_parent_index() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT)", &empty)
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT)",
        &empty,
    )
    .unwrap();
    db.execute("CREATE INDEX p_code_idx ON p(code)", &empty)
        .unwrap();
    db.execute("INSERT INTO p (id, code) VALUES (1, 'p1')", &empty)
        .unwrap();

    db.__reset_relational_scan_rows_touched();
    let result = db
        .apply_changes(
            ChangeSet {
                ddl: vec![
                    DdlChange::AlterTable {
                        name: "p".into(),
                        columns: vec![("code".into(), "TEXT UNIQUE".into())],
                        constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                    DdlChange::AlterTable {
                        name: "c".into(),
                        columns: vec![("p_code".into(), "TEXT REFERENCES p(code)".into())],
                        constraints: Vec::new(),
                        foreign_keys: vec![SingleColumnForeignKey {
                            child_column: "p_code".into(),
                            parent_table: "p".into(),
                            parent_column: "code".into(),
                        }],
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                ],
                ddl_lsn: vec![Lsn(10), Lsn(11)],
                rows: vec![RowChange {
                    table: "c".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(10),
                    },
                    values: vals(vec![
                        ("id", Value::Int64(10)),
                        ("p_code", Value::Text("p1".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(12),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 1);
    let parent_meta = db.table_meta("p").unwrap();
    let code = parent_meta
        .columns
        .iter()
        .find(|column| column.name == "code")
        .unwrap();
    assert!(code.unique, "sync ALTER should install projected UNIQUE");
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "projected parent-key preflight must not use visible table scan APIs"
    );
}

#[test]
fn ds10j_sync_preflight_projected_parent_unique_without_index_rejects_before_schema() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT)", &empty)
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT)",
        &empty,
    )
    .unwrap();
    db.execute("INSERT INTO p (id, code) VALUES (1, 'p1')", &empty)
        .unwrap();

    db.__reset_relational_scan_rows_touched();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![
                    DdlChange::AlterTable {
                        name: "p".into(),
                        columns: vec![("code".into(), "TEXT UNIQUE".into())],
                        constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                    DdlChange::AlterTable {
                        name: "c".into(),
                        columns: vec![("p_code".into(), "TEXT REFERENCES p(code)".into())],
                        constraints: Vec::new(),
                        foreign_keys: vec![SingleColumnForeignKey {
                            child_column: "p_code".into(),
                            parent_table: "p".into(),
                            parent_column: "code".into(),
                        }],
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                ],
                ddl_lsn: vec![Lsn(10), Lsn(11)],
                rows: vec![RowChange {
                    table: "c".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(10),
                    },
                    values: vals(vec![
                        ("id", Value::Int64(10)),
                        ("p_code", Value::Text("p1".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(12),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap_err();

    assert!(
        format!("{err}").contains("no covering index"),
        "expected no-covering-index preflight error, got {err}"
    );
    let parent_meta = db.table_meta("p").unwrap();
    let code = parent_meta
        .columns
        .iter()
        .find(|column| column.name == "code")
        .unwrap();
    assert!(
        !code.unique,
        "failed sync preflight must not install projected UNIQUE"
    );
    let child_meta = db.table_meta("c").unwrap();
    let p_code = child_meta
        .columns
        .iter()
        .find(|column| column.name == "p_code")
        .unwrap();
    assert!(
        p_code.references.is_none(),
        "failed sync preflight must not install projected FK"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "projected parent-key rejection must not fall back to scanning committed parent rows"
    );
}

#[test]
fn ds10k_sync_vector_shape_alter_preserves_projected_parent_unique_for_fk_preflight() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT, embedding VECTOR(2))",
        &empty,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT)",
        &empty,
    )
    .unwrap();
    db.execute("CREATE INDEX p_code_idx ON p(code)", &empty)
        .unwrap();
    db.execute("INSERT INTO p (id, code) VALUES (1, 'p1')", &empty)
        .unwrap();

    db.__reset_relational_scan_rows_touched();
    let result = db
        .apply_changes(
            ChangeSet {
                ddl: vec![
                    DdlChange::AlterTable {
                        name: "p".into(),
                        columns: vec![
                            ("id".into(), "INTEGER PRIMARY KEY".into()),
                            ("code".into(), "TEXT UNIQUE".into()),
                            ("embedding".into(), "VECTOR(3)".into()),
                        ],
                        constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                    DdlChange::AlterTable {
                        name: "c".into(),
                        columns: vec![("p_code".into(), "TEXT REFERENCES p(code)".into())],
                        constraints: Vec::new(),
                        foreign_keys: vec![SingleColumnForeignKey {
                            child_column: "p_code".into(),
                            parent_table: "p".into(),
                            parent_column: "code".into(),
                        }],
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                ],
                ddl_lsn: vec![Lsn(10), Lsn(11)],
                rows: vec![RowChange {
                    table: "c".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Int64(10),
                    },
                    values: vals(vec![
                        ("id", Value::Int64(10)),
                        ("p_code", Value::Text("p1".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(12),
                    created_at: None,
                }],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 1);
    let parent_meta = db.table_meta("p").unwrap();
    let code = parent_meta
        .columns
        .iter()
        .find(|column| column.name == "code")
        .unwrap();
    assert!(
        code.unique,
        "vector full-shape sync ALTER should install projected UNIQUE"
    );
    assert_eq!(
        db.__relational_scan_rows_touched(),
        0,
        "vector full-shape projected parent-key preflight must stay indexed"
    );
}

#[test]
fn ds11_empty_db_restart_no_crash() {
    let cs = persist_and_reopen_changeset("ds11.db", |_db| {});

    assert!(cs.rows.is_empty(), "empty DB must return empty rows");
    assert!(cs.edges.is_empty(), "empty DB must return empty edges");
    assert!(cs.vectors.is_empty(), "empty DB must return empty vectors");
    assert!(cs.ddl.is_empty(), "empty DB must return empty ddl");
}

#[test]
fn ds12_edge_only_db_after_reopen() {
    let node_id = Uuid::new_v4();
    let target_a = Uuid::new_v4();
    let target_b = Uuid::new_v4();

    let cs = persist_and_reopen_changeset("ds12.db", |db| {
        db.execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        db.insert_row(
            tx,
            "nodes",
            vals(vec![
                ("id", Value::Uuid(node_id)),
                ("name", Value::Text("root".into())),
            ]),
        )
        .unwrap();
        db.insert_edge(tx, node_id, target_a, "OBSERVED".into(), HashMap::new())
            .unwrap();
        db.insert_edge(tx, node_id, target_b, "OBSERVED".into(), HashMap::new())
            .unwrap();
        db.commit(tx).unwrap();
    });

    assert_eq!(cs.edges.len(), 2, "expected 2 edges after reopen");
    assert!(
        cs.edges.iter().all(|e| e.source == node_id),
        "all edges must have source == node_id"
    );
    assert!(
        cs.edges.iter().all(|e| e.edge_type == "OBSERVED"),
        "all edges must be OBSERVED type"
    );
    assert!(cs.vectors.is_empty(), "no vectors should be present");
}

#[test]
fn ds13_vector_only_after_reopen() {
    let cs = persist_and_reopen_changeset("ds13.db", |db| {
        db.execute(
            "CREATE TABLE embeddings (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        let row_id = db
            .insert_row(
                tx,
                "embeddings",
                vals(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .unwrap();
        db.insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("embeddings", "embedding"),
            row_id,
            vec![0.0, 1.0, 0.0],
        )
        .unwrap();
        db.commit(tx).unwrap();
    });

    assert_eq!(
        cs.vectors.len(),
        1,
        "expected 1 vector in changeset after reopen"
    );
    assert_eq!(cs.vectors[0].vector, vec![0.0, 1.0, 0.0]);
    assert!(cs.edges.is_empty(), "no edges should be present");
}

#[test]
fn ds14_both_sides_restart_bidirectional() {
    let tmp = TempDir::new().unwrap();
    let server_path = tmp.path().join("ds14_server.db");
    let edge_path = tmp.path().join("ds14_edge.db");
    let server_id = Uuid::new_v4();
    let edge_id = Uuid::new_v4();

    {
        let server = Database::open(&server_path).unwrap();
        server
            .execute(
                "CREATE TABLE config (id UUID PRIMARY KEY, key TEXT, value TEXT)",
                &HashMap::new(),
            )
            .unwrap();
        let tx = server.begin_or_panic();
        server
            .insert_row(
                tx,
                "config",
                vals(vec![
                    ("id", Value::Uuid(server_id)),
                    ("key", Value::Text("mode".into())),
                    ("value", Value::Text("production".into())),
                ]),
            )
            .unwrap();
        server.commit(tx).unwrap();
        server.close().unwrap();
    }

    {
        let edge = Database::open(&edge_path).unwrap();
        edge.execute(
            "CREATE TABLE config (id UUID PRIMARY KEY, key TEXT, value TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = edge.begin_or_panic();
        edge.insert_row(
            tx,
            "config",
            vals(vec![
                ("id", Value::Uuid(edge_id)),
                ("key", Value::Text("debug".into())),
                ("value", Value::Text("true".into())),
            ]),
        )
        .unwrap();
        edge.commit(tx).unwrap();
        edge.close().unwrap();
    }

    let server2 = Database::open(&server_path).unwrap();
    let edge2 = Database::open(&edge_path).unwrap();

    let cs_server = server2.changes_since(Lsn(0));
    edge2
        .apply_changes(
            cs_server,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let cs_edge = edge2.changes_since(Lsn(0));
    server2
        .apply_changes(
            cs_edge,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let server_rows = server2.scan("config", server2.snapshot()).unwrap();
    assert_eq!(
        server_rows.len(),
        2,
        "server must have 2 rows after bidirectional sync"
    );
    let server_mode = server2
        .point_lookup("config", "id", &Value::Uuid(server_id), server2.snapshot())
        .unwrap()
        .expect("server's own row missing");
    assert_eq!(
        server_mode.values.get("key"),
        Some(&Value::Text("mode".into()))
    );
    assert_eq!(
        server_mode.values.get("value"),
        Some(&Value::Text("production".into()))
    );
    let server_debug = server2
        .point_lookup("config", "id", &Value::Uuid(edge_id), server2.snapshot())
        .unwrap()
        .expect("edge's row missing from server");
    assert_eq!(
        server_debug.values.get("key"),
        Some(&Value::Text("debug".into()))
    );

    let edge_rows = edge2.scan("config", edge2.snapshot()).unwrap();
    assert_eq!(
        edge_rows.len(),
        2,
        "edge must have 2 rows after bidirectional sync"
    );
    let edge_mode = edge2
        .point_lookup("config", "id", &Value::Uuid(server_id), edge2.snapshot())
        .unwrap()
        .expect("server's row missing from edge");
    assert_eq!(
        edge_mode.values.get("value"),
        Some(&Value::Text("production".into()))
    );
}

#[test]
fn ds15_drop_table_syncs() {
    let server = Database::open_memory();
    let edge = Database::open_memory();

    for db in [&server, &edge] {
        db.execute(
            "CREATE TABLE temp (id UUID PRIMARY KEY, val TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin_or_panic();
        db.insert_row(
            tx,
            "temp",
            vals(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("val", Value::Text("ephemeral".into())),
            ]),
        )
        .unwrap();
        db.commit(tx).unwrap();
    }

    let lsn_before_drop = edge.current_lsn();
    edge.execute("DROP TABLE temp", &HashMap::new()).unwrap();

    let cs = edge.changes_since(lsn_before_drop);
    let has_drop = cs
        .ddl
        .iter()
        .any(|d| matches!(d, DdlChange::DropTable { name } if name == "temp"));
    assert!(
        has_drop,
        "changeset must contain DdlChange::DropTable for 'temp'"
    );

    server
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::ServerWins))
        .unwrap();

    assert!(
        server.table_meta("temp").is_none(),
        "server must have dropped 'temp' after applying DropTable DDL"
    );
    assert!(
        server.scan("temp", server.snapshot()).is_err(),
        "scanning a dropped table must fail"
    );
}

#[test]
fn ds16_deletion_tombstone_sync() {
    let server = Database::open_memory();
    let edge = Database::open_memory();
    let alice_id = Uuid::new_v4();
    let bob_id = Uuid::new_v4();
    let carol_id = Uuid::new_v4();

    edge.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = edge.begin_or_panic();
    for (id, name) in [(alice_id, "alice"), (bob_id, "bob"), (carol_id, "carol")] {
        edge.insert_row(
            tx,
            "items",
            vals(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.into())),
            ]),
        )
        .unwrap();
    }
    edge.commit(tx).unwrap();

    let cs_initial = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            cs_initial,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert_eq!(server.scan("items", server.snapshot()).unwrap().len(), 3);

    let lsn_after_sync = edge.current_lsn();

    let tx2 = edge.begin_or_panic();
    let bob_row = edge
        .point_lookup("items", "id", &Value::Uuid(bob_id), edge.snapshot())
        .unwrap()
        .expect("bob must exist");
    edge.delete_row(tx2, "items", bob_row.row_id).unwrap();
    edge.commit(tx2).unwrap();

    let cs_del = edge.changes_since(lsn_after_sync);
    let tombstone = cs_del
        .rows
        .iter()
        .find(|r| r.natural_key.value == Value::Uuid(bob_id) && r.deleted);
    assert!(
        tombstone.is_some(),
        "changeset must contain a tombstone for bob"
    );

    server
        .apply_changes(
            cs_del,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let server_rows = server.scan("items", server.snapshot()).unwrap();
    assert_eq!(
        server_rows.len(),
        2,
        "server must have 2 rows after deletion sync"
    );
    assert!(
        server
            .point_lookup("items", "id", &Value::Uuid(bob_id), server.snapshot())
            .unwrap()
            .is_none(),
        "bob must be deleted on server"
    );
    assert!(
        server
            .point_lookup("items", "id", &Value::Uuid(alice_id), server.snapshot())
            .unwrap()
            .is_some(),
        "alice must still exist on server"
    );
}

fn assert_delete_tombstone_syncs_for_scalar_id(create_sql: &str, id: Value) {
    let server = Database::open_memory();
    let edge = Database::open_memory();

    edge.execute(create_sql, &HashMap::new()).unwrap();
    let tx = edge.begin_or_panic();
    edge.insert_row(
        tx,
        "items",
        vals(vec![
            ("id", id.clone()),
            ("name", Value::Text("delete-me".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    let initial = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            initial,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup("items", "id", &id, server.snapshot())
            .unwrap()
            .is_some(),
        "initial sync must create the scalar-id row on the receiver"
    );

    let lsn_after_sync = edge.current_lsn();
    let row = edge
        .point_lookup("items", "id", &id, edge.snapshot())
        .unwrap()
        .expect("edge row must exist before delete");
    let tx = edge.begin_or_panic();
    edge.delete_row(tx, "items", row.row_id).unwrap();
    edge.commit(tx).unwrap();

    let delete_changes = edge.changes_since(lsn_after_sync);
    assert!(
        delete_changes.rows.iter().any(|row| {
            row.deleted && row.natural_key.column == "id" && row.natural_key.value == id
        }),
        "delete changeset must carry the actual scalar id value"
    );
    server
        .apply_changes(
            delete_changes,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup("items", "id", &id, server.snapshot())
            .unwrap()
            .is_none(),
        "delete tombstone must remove scalar-id row on receiver"
    );
}

#[test]
fn ds17_text_primary_key_delete_tombstone_sync() {
    assert_delete_tombstone_syncs_for_scalar_id(
        "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)",
        Value::Text("item-1".into()),
    );
}

#[test]
fn ds18_integer_primary_key_delete_tombstone_sync() {
    assert_delete_tombstone_syncs_for_scalar_id(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
        Value::Int64(42),
    );
}

#[test]
fn ds19_primary_key_wins_over_non_key_id_for_delete_tombstone_sync() {
    let server = Database::open_memory();
    let edge = Database::open_memory();

    edge.execute(
        "CREATE TABLE items (id TEXT, sku TEXT PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = edge.begin_or_panic();
    edge.insert_row(
        tx,
        "items",
        vals(vec![
            ("id", Value::Text("legacy-id".into())),
            ("sku", Value::Text("sku-1".into())),
            ("name", Value::Text("delete-me".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    let initial = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            initial,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let lsn_after_sync = edge.current_lsn();
    let row = edge
        .point_lookup(
            "items",
            "sku",
            &Value::Text("sku-1".into()),
            edge.snapshot(),
        )
        .unwrap()
        .expect("edge row must exist before delete");
    let tx = edge.begin_or_panic();
    edge.delete_row(tx, "items", row.row_id).unwrap();
    edge.commit(tx).unwrap();

    let delete_changes = edge.changes_since(lsn_after_sync);
    assert!(
        delete_changes.rows.iter().any(|row| {
            row.deleted
                && row.natural_key.column == "sku"
                && row.natural_key.value == Value::Text("sku-1".into())
        }),
        "delete changeset must prefer declared primary key over a non-key id column"
    );
    server
        .apply_changes(
            delete_changes,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup(
                "items",
                "sku",
                &Value::Text("sku-1".into()),
                server.snapshot(),
            )
            .unwrap()
            .is_none(),
        "delete tombstone must remove row by declared primary key on receiver"
    );
}

#[test]
fn ds20_delete_tombstone_uses_latest_primary_key_after_key_update() {
    let server = Database::open_memory();
    let edge = Database::open_memory();

    edge.execute(
        "CREATE TABLE items (sku TEXT PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = edge.begin_or_panic();
    edge.insert_row(
        tx,
        "items",
        vals(vec![
            ("sku", Value::Text("sku-a".into())),
            ("name", Value::Text("delete-after-key-update".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    server
        .apply_changes(
            edge.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup(
                "items",
                "sku",
                &Value::Text("sku-a".into()),
                server.snapshot(),
            )
            .unwrap()
            .is_some(),
        "initial sync must create the original primary-key row"
    );

    let lsn_after_initial = edge.current_lsn();
    edge.execute(
        "UPDATE items SET sku = 'sku-b' WHERE sku = 'sku-a'",
        &HashMap::new(),
    )
    .unwrap();
    server
        .apply_changes(
            edge.changes_since(lsn_after_initial),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup(
                "items",
                "sku",
                &Value::Text("sku-b".into()),
                server.snapshot(),
            )
            .unwrap()
            .is_some(),
        "update sync must move the receiver to the new primary key"
    );
    assert!(
        server
            .point_lookup(
                "items",
                "sku",
                &Value::Text("sku-a".into()),
                server.snapshot(),
            )
            .unwrap()
            .is_none(),
        "old primary key must not remain live after update sync"
    );

    let lsn_after_update = edge.current_lsn();
    let row = edge
        .point_lookup(
            "items",
            "sku",
            &Value::Text("sku-b".into()),
            edge.snapshot(),
        )
        .unwrap()
        .expect("edge row must exist with updated primary key before delete");
    let tx = edge.begin_or_panic();
    edge.delete_row(tx, "items", row.row_id).unwrap();
    edge.commit(tx).unwrap();

    let delete_changes = edge.changes_since(lsn_after_update);
    assert!(
        delete_changes.rows.iter().any(|row| {
            row.deleted
                && row.natural_key.column == "sku"
                && row.natural_key.value == Value::Text("sku-b".into())
        }),
        "delete changeset must carry the latest primary-key value after key update"
    );
    assert!(
        !delete_changes.rows.iter().any(|row| {
            row.deleted
                && row.natural_key.column == "sku"
                && row.natural_key.value == Value::Text("sku-a".into())
        }),
        "delete changeset must not use the stale primary-key value"
    );

    server
        .apply_changes(
            delete_changes,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();
    assert!(
        server
            .point_lookup(
                "items",
                "sku",
                &Value::Text("sku-b".into()),
                server.snapshot(),
            )
            .unwrap()
            .is_none(),
        "delete tombstone must remove the row at the latest primary key on receiver"
    );
}

#[test]
fn ds21_single_column_fk_null_child_does_not_block_parent_delete_sync() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let empty = HashMap::new();

    edge.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
        &empty,
    )
    .unwrap();
    edge.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT REFERENCES p(code))",
        &empty,
    )
    .unwrap();
    edge.execute("INSERT INTO p (id, code) VALUES (1, NULL)", &empty)
        .unwrap();
    edge.execute("INSERT INTO c (id, p_code) VALUES (1, NULL)", &empty)
        .unwrap();

    server
        .apply_changes(
            edge.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    let lsn_after_initial = edge.current_lsn();
    edge.execute("DELETE FROM p WHERE id = 1", &empty).unwrap();

    server
        .apply_changes(
            edge.changes_since(lsn_after_initial),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    assert!(
        server
            .point_lookup("p", "id", &Value::Int64(1), server.snapshot())
            .unwrap()
            .is_none(),
        "sync preflight must not treat child NULL FKs as references"
    );
}
