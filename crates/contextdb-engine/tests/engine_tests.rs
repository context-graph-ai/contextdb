use contextdb_core::*;
use contextdb_engine::Database;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn make_values(pairs: Vec<(&str, Value)>) -> HashMap<ColName, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn setup_db() -> Database {
    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, version INTEGER, context_id TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3)) IMMUTABLE",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE entity_snapshots (id UUID PRIMARY KEY, entity_id UUID, valid_from INTEGER, valid_to INTEGER)",
        &params,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &params,
    )
    .unwrap();
    db
}

#[test]
fn test_cross_subsystem_atomic_commit() {
    let eng = setup_db();
    let entity_id = Uuid::new_v4();

    let tx = eng.begin();
    let row_id = eng
        .insert_row(
            tx,
            "entities",
            make_values(vec![
                ("id", Value::Uuid(entity_id)),
                ("name", Value::Text("test".into())),
            ]),
        )
        .unwrap();
    eng.insert_edge(
        tx,
        entity_id,
        Uuid::new_v4(),
        "RELATES_TO".into(),
        HashMap::new(),
    )
    .unwrap();
    eng.insert_vector(tx, row_id, vec![1.0, 0.0, 0.0]).unwrap();
    eng.commit(tx).unwrap();

    let snap = eng.snapshot();
    assert_eq!(eng.scan("entities", snap).unwrap().len(), 1);
    assert_eq!(
        eng.query_bfs(entity_id, None, Direction::Outgoing, 1, snap)
            .unwrap()
            .nodes
            .len(),
        1
    );
    assert_eq!(
        eng.query_vector(&[1.0, 0.0, 0.0], 10, None, snap)
            .unwrap()
            .len(),
        1
    );

    let tx2 = eng.begin();
    eng.insert_row(
        tx2,
        "entities",
        make_values(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("rolled_back".into())),
        ]),
    )
    .unwrap();
    eng.insert_edge(
        tx2,
        Uuid::new_v4(),
        Uuid::new_v4(),
        "SERVES".into(),
        HashMap::new(),
    )
    .unwrap();
    eng.insert_vector(tx2, 999, vec![0.0, 1.0, 0.0]).unwrap();
    eng.rollback(tx2).unwrap();

    let snap2 = eng.snapshot();
    assert_eq!(eng.scan("entities", snap2).unwrap().len(), 1);
    let vec_results2 = eng.query_vector(&[0.0, 1.0, 0.0], 10, None, snap2).unwrap();
    assert_eq!(vec_results2.len(), 1);
    assert_eq!(vec_results2[0].0, row_id);
}

#[test]
fn test_rollback_across_all_subsystems() {
    let eng = setup_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let tx = eng.begin();
    let rid = eng
        .insert_row(
            tx,
            "decisions",
            make_values(vec![
                ("id", Value::Uuid(a)),
                ("status", Value::Text("active".into())),
            ]),
        )
        .unwrap();
    eng.insert_edge(tx, a, b, "SERVES".into(), HashMap::new())
        .unwrap();
    eng.insert_vector(tx, rid, vec![0.5, 0.5, 0.0]).unwrap();
    eng.rollback(tx).unwrap();

    let snap = eng.snapshot();
    assert_eq!(eng.scan("decisions", snap).unwrap().len(), 0);
    assert_eq!(
        eng.query_bfs(a, None, Direction::Outgoing, 1, snap)
            .unwrap()
            .nodes
            .len(),
        0
    );
    assert_eq!(
        eng.query_vector(&[0.5, 0.5, 0.0], 10, None, snap)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn test_mvcc_snapshot_isolation() {
    let eng = setup_db();
    let eid = Uuid::new_v4();

    let tx1 = eng.begin();
    let rid1 = eng
        .insert_row(
            tx1,
            "entities",
            make_values(vec![("id", Value::Uuid(eid)), ("version", Value::Int64(1))]),
        )
        .unwrap();
    eng.commit(tx1).unwrap();

    let snap1 = eng.snapshot();

    let tx2 = eng.begin();
    eng.delete_row(tx2, "entities", rid1).unwrap();
    eng.insert_row(
        tx2,
        "entities",
        make_values(vec![("id", Value::Uuid(eid)), ("version", Value::Int64(2))]),
    )
    .unwrap();
    eng.commit(tx2).unwrap();

    let rows_s1 = eng.scan("entities", snap1).unwrap();
    assert_eq!(rows_s1.len(), 1);
    assert_eq!(rows_s1[0].values.get("version"), Some(&Value::Int64(1)));

    let rows_s2 = eng.scan("entities", eng.snapshot()).unwrap();
    assert_eq!(rows_s2.len(), 1);
    assert_eq!(rows_s2[0].values.get("version"), Some(&Value::Int64(2)));
}

#[test]
fn test_bfs_under_mvcc() {
    let eng = setup_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let d = Uuid::new_v4();

    let tx1 = eng.begin();
    eng.insert_edge(tx1, a, b, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.insert_edge(tx1, b, c, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.commit(tx1).unwrap();

    let snap1 = eng.snapshot();

    let tx2 = eng.begin();
    eng.insert_edge(tx2, c, d, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.commit(tx2).unwrap();

    let ids1: HashSet<NodeId> = eng
        .query_bfs(a, None, Direction::Outgoing, 5, snap1)
        .unwrap()
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert!(ids1.contains(&b));
    assert!(ids1.contains(&c));
    assert!(!ids1.contains(&d));

    let ids2: HashSet<NodeId> = eng
        .query_bfs(a, None, Direction::Outgoing, 5, eng.snapshot())
        .unwrap()
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert!(ids2.contains(&d));
}

#[test]
fn test_bfs_cycle_detection() {
    let eng = setup_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    let tx = eng.begin();
    eng.insert_edge(tx, a, b, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.insert_edge(tx, b, c, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.insert_edge(tx, c, a, "EDGE".into(), HashMap::new())
        .unwrap();
    eng.commit(tx).unwrap();

    let ids: Vec<NodeId> = eng
        .query_bfs(a, None, Direction::Outgoing, 5, eng.snapshot())
        .unwrap()
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&b));
    assert!(ids.contains(&c));
}

#[test]
fn test_bfs_depth_bound() {
    let eng = setup_db();
    let nodes: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();

    let tx = eng.begin();
    for i in 0..5 {
        eng.insert_edge(tx, nodes[i], nodes[i + 1], "EDGE".into(), HashMap::new())
            .unwrap();
    }
    eng.commit(tx).unwrap();

    let ids: HashSet<NodeId> = eng
        .query_bfs(nodes[0], None, Direction::Outgoing, 3, eng.snapshot())
        .unwrap()
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert!(ids.contains(&nodes[1]));
    assert!(ids.contains(&nodes[2]));
    assert!(ids.contains(&nodes[3]));
    assert!(!ids.contains(&nodes[4]));
}

#[test]
fn test_bfs_depth_exceeds_max_is_accepted_in_executor() {
    let eng = setup_db();
    let snap = eng.snapshot();
    let result = eng.query_bfs(Uuid::new_v4(), None, Direction::Outgoing, 50, snap);
    assert!(result.is_ok());
}

#[test]
fn test_vector_search_with_prefilter() {
    let eng = setup_db();

    let tx = eng.begin();
    eng.insert_vector(tx, 1, vec![1.0, 0.0, 0.0]).unwrap();
    eng.insert_vector(tx, 2, vec![0.0, 1.0, 0.0]).unwrap();
    eng.insert_vector(tx, 3, vec![0.9, 0.1, 0.0]).unwrap();
    eng.insert_vector(tx, 4, vec![0.0, 0.0, 1.0]).unwrap();
    eng.insert_vector(tx, 5, vec![0.5, 0.5, 0.0]).unwrap();
    eng.insert_vector(tx, 6, vec![-1.0, 0.0, 0.0]).unwrap();
    eng.insert_vector(tx, 7, vec![0.8, 0.2, 0.0]).unwrap();
    eng.insert_vector(tx, 8, vec![0.0, -1.0, 0.0]).unwrap();
    eng.insert_vector(tx, 9, vec![0.99, 0.01, 0.0]).unwrap();
    eng.insert_vector(tx, 10, vec![0.1, 0.9, 0.0]).unwrap();
    eng.commit(tx).unwrap();

    let mut candidates = RoaringTreemap::new();
    for row_id in [1, 3, 5, 7, 9] {
        candidates.insert(row_id);
    }

    let results = eng
        .query_vector(&[1.0, 0.0, 0.0], 3, Some(&candidates), eng.snapshot())
        .unwrap();

    for (rid, _) in &results {
        assert!(candidates.contains(*rid));
    }
    assert_eq!(results.len(), 3);
}

#[test]
fn test_unified_pipeline() {
    let eng = setup_db();
    let center = Uuid::new_v4();
    let neighbor1 = Uuid::new_v4();
    let neighbor2 = Uuid::new_v4();
    let far = Uuid::new_v4();

    let tx = eng.begin();
    let rid1 = eng
        .insert_row(
            tx,
            "entities",
            make_values(vec![
                ("id", Value::Uuid(neighbor1)),
                ("context_id", Value::Text("ctx1".into())),
            ]),
        )
        .unwrap();
    let rid2 = eng
        .insert_row(
            tx,
            "entities",
            make_values(vec![
                ("id", Value::Uuid(neighbor2)),
                ("context_id", Value::Text("ctx1".into())),
            ]),
        )
        .unwrap();
    let rid3 = eng
        .insert_row(
            tx,
            "entities",
            make_values(vec![
                ("id", Value::Uuid(far)),
                ("context_id", Value::Text("ctx2".into())),
            ]),
        )
        .unwrap();

    eng.insert_edge(tx, center, neighbor1, "RELATES_TO".into(), HashMap::new())
        .unwrap();
    eng.insert_edge(tx, center, neighbor2, "RELATES_TO".into(), HashMap::new())
        .unwrap();
    eng.insert_edge(tx, center, far, "RELATES_TO".into(), HashMap::new())
        .unwrap();

    eng.insert_vector(tx, rid1, vec![1.0, 0.0, 0.0]).unwrap();
    eng.insert_vector(tx, rid2, vec![0.9, 0.1, 0.0]).unwrap();
    eng.insert_vector(tx, rid3, vec![0.0, 0.0, 1.0]).unwrap();
    eng.commit(tx).unwrap();

    let snap = eng.snapshot();
    let neighborhood_ids: HashSet<NodeId> = eng
        .query_bfs(center, None, Direction::Outgoing, 1, snap)
        .unwrap()
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();

    let filtered_row_ids: RoaringTreemap = eng
        .scan("entities", snap)
        .unwrap()
        .into_iter()
        .filter(|r| {
            r.values.get("context_id") == Some(&Value::Text("ctx1".into()))
                && r.values
                    .get("id")
                    .and_then(Value::as_uuid)
                    .is_some_and(|u| neighborhood_ids.contains(u))
        })
        .map(|r| r.row_id)
        .collect();
    assert_eq!(filtered_row_ids.len(), 2);

    let vec_results = eng
        .query_vector(&[1.0, 0.0, 0.0], 1, Some(&filtered_row_ids), snap)
        .unwrap();
    assert_eq!(vec_results.len(), 1);
    assert_eq!(vec_results[0].0, rid1);
}

#[test]
fn test_upsert_idempotent() {
    let eng = setup_db();
    let eid = Uuid::new_v4();

    let tx1 = eng.begin();
    let r1 = eng
        .upsert_row(
            tx1,
            "entities",
            "id",
            make_values(vec![("id", Value::Uuid(eid)), ("version", Value::Int64(1))]),
        )
        .unwrap();
    eng.commit(tx1).unwrap();
    assert_eq!(r1, UpsertResult::Inserted);

    let tx2 = eng.begin();
    let r2 = eng
        .upsert_row(
            tx2,
            "entities",
            "id",
            make_values(vec![("id", Value::Uuid(eid)), ("version", Value::Int64(2))]),
        )
        .unwrap();
    eng.commit(tx2).unwrap();
    assert_eq!(r2, UpsertResult::Updated);

    let tx3 = eng.begin();
    let r3 = eng
        .upsert_row(
            tx3,
            "entities",
            "id",
            make_values(vec![("id", Value::Uuid(eid)), ("version", Value::Int64(2))]),
        )
        .unwrap();
    eng.commit(tx3).unwrap();
    assert_eq!(r3, UpsertResult::NoOp);
}

#[test]
fn test_observation_immutability() {
    let eng = setup_db();

    let tx1 = eng.begin();
    let rid = eng
        .insert_row(
            tx1,
            "observations",
            make_values(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("data", Value::Text("temperature=25".into())),
            ]),
        )
        .unwrap();
    eng.commit(tx1).unwrap();

    let tx2 = eng.begin();
    let result = eng.delete_row(tx2, "observations", rid);
    assert!(matches!(result, Err(Error::ImmutableTable(_))));
    eng.rollback(tx2).unwrap();
}

#[test]
fn test_invalidation_state_machine() {
    let eng = setup_db();
    let inv_id = Uuid::new_v4();

    let tx1 = eng.begin();
    eng.insert_row(
        tx1,
        "invalidations",
        make_values(vec![
            ("id", Value::Uuid(inv_id)),
            ("status", Value::Text("pending".into())),
        ]),
    )
    .unwrap();
    eng.commit(tx1).unwrap();

    let tx2 = eng.begin();
    eng.upsert_row(
        tx2,
        "invalidations",
        "id",
        make_values(vec![
            ("id", Value::Uuid(inv_id)),
            ("status", Value::Text("acknowledged".into())),
        ]),
    )
    .unwrap();
    eng.commit(tx2).unwrap();

    let tx3 = eng.begin();
    let err = eng
        .upsert_row(
            tx3,
            "invalidations",
            "id",
            make_values(vec![
                ("id", Value::Uuid(inv_id)),
                ("status", Value::Text("pending".into())),
            ]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}

#[test]
fn test_snapshot_contiguity() {
    let eng = setup_db();
    let eid = Uuid::new_v4();

    let tx1 = eng.begin();
    eng.insert_row(
        tx1,
        "entity_snapshots",
        make_values(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("entity_id", Value::Uuid(eid)),
            ("valid_from", Value::Timestamp(100)),
            ("valid_to", Value::Null),
        ]),
    )
    .unwrap();
    eng.commit(tx1).unwrap();

    let snap1 = eng.snapshot();
    let rows = eng.scan("entity_snapshots", snap1).unwrap();
    let old_rid = rows[0].row_id;

    let tx2 = eng.begin();
    eng.delete_row(tx2, "entity_snapshots", old_rid).unwrap();
    eng.insert_row(
        tx2,
        "entity_snapshots",
        make_values(vec![
            (
                "id",
                Value::Uuid(*rows[0].values.get("id").unwrap().as_uuid().unwrap()),
            ),
            ("entity_id", Value::Uuid(eid)),
            ("valid_from", Value::Timestamp(100)),
            ("valid_to", Value::Timestamp(200)),
        ]),
    )
    .unwrap();
    eng.insert_row(
        tx2,
        "entity_snapshots",
        make_values(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("entity_id", Value::Uuid(eid)),
            ("valid_from", Value::Timestamp(200)),
            ("valid_to", Value::Null),
        ]),
    )
    .unwrap();
    eng.commit(tx2).unwrap();

    let mut snapshots: Vec<_> = eng
        .scan("entity_snapshots", eng.snapshot())
        .unwrap()
        .into_iter()
        .filter(|r| r.values.get("entity_id") == Some(&Value::Uuid(eid)))
        .collect();
    snapshots.sort_by_key(|r| match r.values.get("valid_from") {
        Some(Value::Timestamp(v)) => *v,
        _ => 0,
    });

    assert_eq!(snapshots.len(), 2);
    assert_eq!(
        snapshots[0].values.get("valid_to"),
        snapshots[1].values.get("valid_from")
    );
}

#[test]
fn test_vector_snapshot_isolation() {
    let eng = setup_db();

    let tx1 = eng.begin();
    eng.insert_vector(tx1, 1, vec![1.0, 0.0, 0.0]).unwrap();
    eng.insert_vector(tx1, 2, vec![0.0, 1.0, 0.0]).unwrap();
    eng.insert_vector(tx1, 3, vec![0.0, 0.0, 1.0]).unwrap();
    eng.commit(tx1).unwrap();

    let snap1 = eng.snapshot();

    let tx2 = eng.begin();
    eng.insert_vector(tx2, 4, vec![0.7, 0.7, 0.0]).unwrap();
    eng.insert_vector(tx2, 5, vec![0.5, 0.5, 0.5]).unwrap();
    eng.commit(tx2).unwrap();

    let rids1: HashSet<RowId> = eng
        .query_vector(&[1.0, 0.0, 0.0], 10, None, snap1)
        .unwrap()
        .iter()
        .map(|(r, _)| *r)
        .collect();
    assert_eq!(rids1.len(), 3);
    assert!(!rids1.contains(&4));
    assert!(!rids1.contains(&5));

    assert_eq!(
        eng.query_vector(&[1.0, 0.0, 0.0], 10, None, eng.snapshot())
            .unwrap()
            .len(),
        5
    );
}

#[test]
fn test_empty_database() {
    let eng = setup_db();
    let snap = eng.snapshot();

    assert_eq!(
        eng.query_bfs(Uuid::new_v4(), None, Direction::Outgoing, 5, snap)
            .unwrap()
            .nodes
            .len(),
        0
    );
    assert_eq!(
        eng.query_vector(&[1.0, 0.0, 0.0], 10, None, snap)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(eng.scan("entities", snap).unwrap().len(), 0);
}

#[test]
fn test_vector_search_requires_limit() {
    let eng = setup_db();
    let tx = eng.begin();
    eng.insert_vector(tx, 1, vec![1.0, 0.0]).unwrap();
    eng.commit(tx).unwrap();

    let snap = eng.snapshot();
    let results = eng.query_vector(&[1.0, 0.0], 0, None, snap).unwrap();
    assert_eq!(results.len(), 0);
}
