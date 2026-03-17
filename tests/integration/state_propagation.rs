use contextdb_core::{Error, Value, VersionedRow};
use contextdb_engine::{ConflictPolicies, ConflictPolicy, Database};
use std::collections::HashMap;
use uuid::Uuid;

fn setup_propagation_db() -> Database {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived, paused, superseded])",
        &p,
    )
    .unwrap();

    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, embedding VECTOR(128)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR",
        &p,
    )
    .unwrap();

    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES')",
        &p,
    )
    .unwrap();

    db
}

fn text(row: &VersionedRow, key: &str) -> String {
    match row.values.get(key) {
        Some(Value::Text(v)) => v.clone(),
        v => panic!("expected text for key {key}, got {v:?}"),
    }
}

fn get_row(db: &Database, table: &str, id: Uuid) -> VersionedRow {
    db.point_lookup(table, "id", &Value::Uuid(id), db.snapshot())
        .unwrap()
        .unwrap()
}

fn vec128(x: f32) -> Vec<f32> {
    vec![x; 128]
}

#[test]
fn t01_fk_propagation_basic() {
    let db = setup_propagation_db();
    let intention_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(intention_id)),
            (
                "description".to_string(),
                Value::Text("detect anomaly".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(decision_id)),
            (
                "description".to_string(),
                Value::Text("use model X".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("intention_id".to_string(), Value::Uuid(intention_id)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(intention_id)),
            (
                "description".to_string(),
                Value::Text("detect anomaly".to_string()),
            ),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(
        text(&get_row(&db, "decisions", decision_id), "status"),
        "invalidated"
    );
    assert_eq!(
        text(&get_row(&db, "intentions", intention_id), "status"),
        "archived"
    );
}

#[test]
fn t02_fk_propagation_multi_level() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sub_decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, decision_id UUID REFERENCES decisions(id) ON STATE invalidated PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated])",
        &p,
    )
    .unwrap();

    let i = Uuid::new_v4();
    let d = Uuid::new_v4();
    let sd = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("top".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("mid".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "sub_decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(sd)),
            ("description".to_string(), Value::Text("bottom".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("decision_id".to_string(), Value::Uuid(d)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("top".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "invalidated");
    assert_eq!(
        text(&get_row(&db, "sub_decisions", sd), "status"),
        "invalidated"
    );
}

#[test]
fn t03_fk_propagation_depth_limit() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE tasks (id UUID PRIMARY KEY, description TEXT, status TEXT, parent_id UUID REFERENCES tasks(id) ON STATE closed PROPAGATE SET closed MAX DEPTH 3) STATE MACHINE (status: active -> [closed])",
        &p,
    )
    .unwrap();

    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    let tx = db.begin();
    db.insert_row(
        tx,
        "tasks",
        HashMap::from([
            ("id".to_string(), Value::Uuid(ids[0])),
            ("description".to_string(), Value::Text("root".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();

    for i in 1..5 {
        db.insert_row(
            tx,
            "tasks",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ids[i])),
                ("description".to_string(), Value::Text(format!("task-{i}"))),
                ("status".to_string(), Value::Text("active".to_string())),
                ("parent_id".to_string(), Value::Uuid(ids[i - 1])),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "tasks",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(ids[0])),
            ("description".to_string(), Value::Text("root".to_string())),
            ("status".to_string(), Value::Text("closed".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    for (i, id) in ids.iter().enumerate().take(4).skip(1) {
        assert_eq!(
            text(&get_row(&db, "tasks", *id), "status"),
            "closed",
            "task at depth {i} should be closed"
        );
    }
    assert_eq!(
        text(&get_row(&db, "tasks", ids[4]), "status"),
        "active",
        "task at depth 4 should remain active (MAX DEPTH 3)"
    );
}

#[test]
fn t04_edge_propagation_basic() {
    let db = setup_propagation_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            (
                "description".to_string(),
                Value::Text("cited decision".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(b)),
            (
                "description".to_string(),
                Value::Text("citing decision".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "decisions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            (
                "description".to_string(),
                Value::Text("cited decision".to_string()),
            ),
            ("status".to_string(), Value::Text("invalidated".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", a), "status"), "invalidated");
    assert_eq!(text(&get_row(&db, "decisions", b), "status"), "invalidated");
}

#[test]
fn t05_edge_propagation_respects_direction() {
    let db = setup_propagation_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            (
                "description".to_string(),
                Value::Text("cited decision".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(b)),
            (
                "description".to_string(),
                Value::Text("cites A via CITES".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(c)),
            (
                "description".to_string(),
                Value::Text("serves A via SERVES".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    db.insert_edge(tx, c, a, "SERVES".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "decisions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            (
                "description".to_string(),
                Value::Text("cited decision".to_string()),
            ),
            ("status".to_string(), Value::Text("invalidated".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", a), "status"), "invalidated");
    assert_eq!(
        text(&get_row(&db, "decisions", b), "status"),
        "invalidated",
        "B should be invalidated -- CITES edge, INCOMING direction matches"
    );
    assert_eq!(
        text(&get_row(&db, "decisions", c), "status"),
        "active",
        "C should remain active -- SERVES edge type does not match CITES propagation rule"
    );
}

#[test]
fn t06_fk_plus_edge_combined() {
    let db = setup_propagation_db();
    let i = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            ("description".to_string(), Value::Text("A".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(b)),
            (
                "description".to_string(),
                Value::Text("B cites A".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", a), "status"), "invalidated");
    assert_eq!(text(&get_row(&db, "decisions", b), "status"), "invalidated");
}

#[test]
fn t07_vector_exclusion_basic() {
    let db = setup_propagation_db();
    let d = Uuid::new_v4();

    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(d)),
                ("description".to_string(), Value::Text("d".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
    db.insert_vector(tx, row_id, vec128(0.5)).unwrap();
    db.commit(tx).unwrap();

    assert!(
        !db.query_vector(&vec128(0.5), 10, None, db.snapshot())
            .unwrap()
            .is_empty()
    );

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "decisions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("invalidated".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    let hits = db
        .query_vector(&vec128(0.5), 10, None, db.snapshot())
        .unwrap();
    assert!(hits.iter().all(|(rid, _)| *rid != row_id));
}

#[test]
fn t08_fk_propagation_plus_vector_exclusion() {
    let db = setup_propagation_db();
    let i = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    let row_id = db
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(d)),
                (
                    "description".to_string(),
                    Value::Text("decision".to_string()),
                ),
                ("status".to_string(), Value::Text("active".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    db.insert_vector(tx, row_id, vec128(0.6)).unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "invalidated");
    let hits = db
        .query_vector(&vec128(0.6), 10, None, db.snapshot())
        .unwrap();
    assert!(hits.iter().all(|(rid, _)| *rid != row_id));
}

#[test]
fn t09_skip_and_warn_on_invalid_transition() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated) STATE MACHINE (status: paused -> [superseded])",
        &p,
    )
    .unwrap();

    let i = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("paused".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "intentions", i), "status"), "archived");
    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "paused");
}

#[test]
fn t10_cycle_detection() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT, parent_id UUID REFERENCES tasks(id) ON STATE closed PROPAGATE SET closed) STATE MACHINE (status: active -> [closed])",
        &p,
    )
    .unwrap();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "tasks",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            ("status".to_string(), Value::Text("active".to_string())),
            ("parent_id".to_string(), Value::Uuid(b)),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "tasks",
        HashMap::from([
            ("id".to_string(), Value::Uuid(b)),
            ("status".to_string(), Value::Text("active".to_string())),
            ("parent_id".to_string(), Value::Uuid(a)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "tasks",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            ("status".to_string(), Value::Text("closed".to_string())),
            ("parent_id".to_string(), Value::Uuid(b)),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "tasks", a), "status"), "closed");
    assert_eq!(text(&get_row(&db, "tasks", b), "status"), "closed");
}

#[test]
fn t11_no_matching_rows_noop() {
    let db = setup_propagation_db();
    let i = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "intentions", i), "status"), "archived");
}

#[test]
fn t12_abort_on_failure_policy() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated ABORT ON FAILURE) STATE MACHINE (status: paused -> [superseded])",
        &p,
    )
    .unwrap();

    let i = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("paused".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    let upsert_result = db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    );

    if upsert_result.is_ok() {
        let commit_result = db.commit(tx2);
        assert!(
            commit_result.is_err(),
            "abort-on-failure should prevent commit when propagation target is invalid"
        );
    } else {
        assert!(upsert_result.is_err());
    }

    assert_eq!(text(&get_row(&db, "intentions", i), "status"), "active");
}

#[test]
fn t13_propagation_immutable_interaction() {
    let p = HashMap::new();
    let db = Database::open_memory();

    let res = db.execute(
        "CREATE TABLE bad (id UUID PRIMARY KEY, status TEXT) IMMUTABLE STATE MACHINE (status: active -> [archived]) PROPAGATE ON STATE archived EXCLUDE VECTOR",
        &p,
    );
    assert!(matches!(res, Err(Error::ParseError(_))));
}

#[test]
#[ignore = "requires redb persistence"]
fn t14_persistence_across_reopen() {
    todo!("requires redb persistence");
}

#[test]
fn t15_end_to_end_fk_edge_vector_full_chain() {
    let db = setup_propagation_db();
    let i = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    let a_row = db
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("description".to_string(), Value::Text("A".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    let b_row = db
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(b)),
                (
                    "description".to_string(),
                    Value::Text("B cites A".to_string()),
                ),
                ("status".to_string(), Value::Text("active".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    db.insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    db.insert_vector(tx, a_row, vec128(0.1)).unwrap();
    db.insert_vector(tx, b_row, vec128(0.2)).unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("intent".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", a), "status"), "invalidated");
    assert_eq!(text(&get_row(&db, "decisions", b), "status"), "invalidated");

    let hits = db
        .query_vector(&vec128(0.1), 10, None, db.snapshot())
        .unwrap();
    assert!(
        hits.iter().all(|(rid, _)| *rid != a_row && *rid != b_row),
        "both vectors should be excluded after invalidation chain"
    );
}

#[test]
fn t16_supersession_with_vector_exclusion() {
    let db = setup_propagation_db();
    let d = Uuid::new_v4();

    let tx = db.begin();
    let row_id = db
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(d)),
                (
                    "description".to_string(),
                    Value::Text("decision".to_string()),
                ),
                ("status".to_string(), Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
    db.insert_vector(tx, row_id, vec128(0.7)).unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "decisions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("decision".to_string()),
            ),
            ("status".to_string(), Value::Text("superseded".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "superseded");
    let hits = db
        .query_vector(&vec128(0.7), 10, None, db.snapshot())
        .unwrap();
    assert!(hits.iter().all(|(rid, _)| *rid != row_id));
}

#[test]
fn t17_sync_propagation_server_archives_client_applies_and_propagates() {
    let server = setup_propagation_db();
    let client = setup_propagation_db();

    let i = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = server.begin();
    server
        .insert_row(
            tx,
            "intentions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(i)),
                ("description".to_string(), Value::Text("intent".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
    server
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("description".to_string(), Value::Text("A".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    server
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(b)),
                (
                    "description".to_string(),
                    Value::Text("B cites A".to_string()),
                ),
                ("status".to_string(), Value::Text("active".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    server
        .insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    server.commit(tx).unwrap();

    let bootstrap = server.changes_since(0);
    client
        .apply_changes(
            bootstrap,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let before = server.current_lsn();
    let tx2 = server.begin();
    server
        .upsert_row(
            tx2,
            "intentions",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(i)),
                ("description".to_string(), Value::Text("intent".to_string())),
                ("status".to_string(), Value::Text("archived".to_string())),
            ]),
        )
        .unwrap();
    server.commit(tx2).unwrap();

    let delta = server.changes_since(before);
    client
        .apply_changes(
            delta,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    assert_eq!(
        text(&get_row(&client, "intentions", i), "status"),
        "archived"
    );
    assert_eq!(
        text(&get_row(&client, "decisions", a), "status"),
        "invalidated"
    );
    assert_eq!(
        text(&get_row(&client, "decisions", b), "status"),
        "invalidated"
    );
}

#[test]
fn t18_sync_does_not_repropagate_already_propagated_rows() {
    let server = setup_propagation_db();
    let client = setup_propagation_db();

    let i = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = server.begin();
    server
        .insert_row(
            tx,
            "intentions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(i)),
                ("description".to_string(), Value::Text("intent".to_string())),
                ("status".to_string(), Value::Text("archived".to_string())),
            ]),
        )
        .unwrap();
    server
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(a)),
                ("description".to_string(), Value::Text("A".to_string())),
                ("status".to_string(), Value::Text("invalidated".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    server
        .insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(b)),
                (
                    "description".to_string(),
                    Value::Text("B cites A".to_string()),
                ),
                ("status".to_string(), Value::Text("invalidated".to_string())),
                ("intention_id".to_string(), Value::Uuid(i)),
            ]),
        )
        .unwrap();
    server
        .insert_edge(tx, b, a, "CITES".to_string(), HashMap::new())
        .unwrap();
    server.commit(tx).unwrap();

    let changes = server.changes_since(0);
    client
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    assert_eq!(
        text(&get_row(&client, "intentions", i), "status"),
        "archived"
    );
    assert_eq!(
        text(&get_row(&client, "decisions", a), "status"),
        "invalidated"
    );
    assert_eq!(
        text(&get_row(&client, "decisions", b), "status"),
        "invalidated"
    );
}

#[test]
fn t19_propagation_does_not_fire_without_rules() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id)) STATE MACHINE (status: active -> [invalidated])",
        &p,
    )
    .unwrap();

    let i = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "intentions", i), "status"), "archived");
    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "active");
}

#[test]
fn t20_impossible_state_transition_during_propagation_skip_and_warn() {
    let db = Database::open_memory();
    let p = HashMap::new();

    db.execute(
        "CREATE TABLE intentions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [archived])",
        &p,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated) STATE MACHINE (status: paused -> [superseded], active -> [invalidated])",
        &p,
    )
    .unwrap();

    let i = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("paused".to_string())),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();

    assert_eq!(text(&get_row(&db, "decisions", d), "status"), "paused");
}
