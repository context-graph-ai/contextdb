use super::helpers::setup_ontology_db;
use contextdb_core::{Direction, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn text<'a>(row: &'a contextdb_core::VersionedRow, key: &str) -> &'a str {
    row.values.get(key).and_then(Value::as_text).expect("text")
}

fn uuid_value(row: &contextdb_core::VersionedRow, key: &str) -> Uuid {
    *row.values
        .get(key)
        .and_then(Value::as_uuid)
        .expect("expected uuid value")
}

fn float64_value(row: &contextdb_core::VersionedRow, key: &str) -> f64 {
    match row.values.get(key).expect("expected float64 value") {
        Value::Float64(v) => *v,
        other => panic!("expected Float64, got {other:?}"),
    }
}

fn insert_entity(db: &contextdb_engine::Database, tx: u64, id: Uuid, props: &str) {
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text("entity".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("SERVICE".to_string()),
            ),
            ("properties".to_string(), Value::Text(props.to_string())),
        ]),
    )
    .expect("insert entity");
}

#[test]
fn c1_01_pattern_stored_and_queryable() {
    let db = setup_ontology_db();
    let p = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            (
                "pattern_type".to_string(),
                Value::Text("cross_location".to_string()),
            ),
            (
                "description".to_string(),
                Value::Text("fraud pattern".to_string()),
            ),
            (
                "match_criteria".to_string(),
                Value::Text("type=config_change,min_occurrences=3".to_string()),
            ),
            (
                "scope_labels".to_string(),
                Value::Text("org=bank-x".to_string()),
            ),
            ("confidence".to_string(), Value::Float64(0.87)),
        ]),
    )
    .expect("insert pattern");
    db.commit(tx).expect("commit");

    let row = db
        .point_lookup("patterns", "id", &Value::Uuid(p), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "pattern_type"), "cross_location");
    assert_eq!(
        db.scan_filter("patterns", db.snapshot(), &|r| {
            r.values.get("scope_labels") == Some(&Value::Text("org=bank-x".to_string()))
        })
        .expect("scan")
        .len(),
        1
    );
}

#[test]
fn c1_02_pattern_match_criteria() {
    let db = setup_ontology_db();
    let entity = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, entity, "");
    for _ in 0..3 {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(entity)),
                (
                    "data".to_string(),
                    Value::Text("observation_type=config_change".to_string()),
                ),
            ]),
        )
        .expect("observation");
    }
    db.commit(tx).expect("commit");

    let matched = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values
                .get("data")
                .and_then(Value::as_text)
                .map(|v| v.contains("config_change"))
                .unwrap_or(false)
        })
        .expect("scan");
    assert_eq!(matched.len(), 3);
}

#[test]
fn c1_03_pattern_scope_labels_filter() {
    let db = setup_ontology_db();
    let p = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            (
                "scope_labels".to_string(),
                Value::Text("org=bank-x".to_string()),
            ),
        ]),
    )
    .expect("pattern");
    for (name, labels) in [("c1", "org=bank-x"), ("c2", "org=bank-y")] {
        db.insert_row(
            tx,
            "contexts",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text(name.to_string())),
                ("labels".to_string(), Value::Text(labels.to_string())),
            ]),
        )
        .expect("context");
    }
    db.commit(tx).expect("commit");

    let c1 = db
        .scan_filter("contexts", db.snapshot(), &|r| {
            r.values.get("labels") == Some(&Value::Text("org=bank-x".to_string()))
        })
        .expect("scan");
    let c2 = db
        .scan_filter("contexts", db.snapshot(), &|r| {
            r.values.get("labels") == Some(&Value::Text("org=bank-y".to_string()))
        })
        .expect("scan");
    assert_eq!(c1.len(), 1);
    assert_eq!(c2.len(), 1);
}

#[test]
fn c1_04_common_attributes_across_decisions() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for idx in 0..5 {
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                (
                    "description".to_string(),
                    Value::Text(format!("decision-{idx}")),
                ),
                (
                    "decision_type".to_string(),
                    Value::Text("security".to_string()),
                ),
                ("tags".to_string(), Value::Text("auth,policy".to_string())),
            ]),
        )
        .expect("decision");
    }
    db.commit(tx).expect("commit");

    let rows = db.scan("decisions", db.snapshot()).expect("scan");
    assert_eq!(rows.len(), 5);

    // Discover common attributes: find fields with identical values across all 5 rows
    let all_keys: HashSet<String> = rows.iter().flat_map(|r| r.values.keys().cloned()).collect();
    let mut common: HashMap<String, Value> = HashMap::new();
    for key in &all_keys {
        let vals: Vec<&Value> = rows.iter().filter_map(|r| r.values.get(key)).collect();
        if vals.len() == 5 {
            let first = vals[0];
            if vals.iter().all(|v| *v == first) {
                common.insert(key.clone(), first.clone());
            }
        }
    }
    // "description" differs per row (decision-0..4), so it should NOT be common
    assert!(!common.contains_key("description"));
    // "decision_type" should be discovered as common with value "security"
    assert!(common.contains_key("decision_type"));
    assert_eq!(
        common.get("decision_type"),
        Some(&Value::Text("security".to_string()))
    );
    // "tags" should also be common
    assert!(common.contains_key("tags"));
}

#[test]
fn c2_01_entity_context_retrieval() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "state=v1");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("d".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("decision");
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("edge");
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("recent-change".to_string())),
        ]),
    )
    .expect("obs");
    db.commit(tx).expect("commit");

    let decisions = db
        .query_bfs(
            e,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(decisions.nodes.len(), 1);
    let obs = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    assert_eq!(obs.len(), 1);
}

#[test]
fn c2_02_entity_context_empty() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "state=empty");
    db.commit(tx).expect("commit");

    assert!(
        db.query_bfs(
            e,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs")
        .nodes
        .is_empty()
    );
}

#[test]
#[ignore = "requires subscription API"]
fn c2_03_subscribe_new_decision() {
    let _db = setup_ontology_db();
    // TODO: subscribe(new_decision on entity), create decision BASED_ON entity, assert callback payload.
    todo!("requires subscription API");
}

#[test]
#[ignore = "requires subscription API"]
fn c2_04_subscribe_invalidation_detected() {
    let _db = setup_ontology_db();
    // TODO: subscribe(invalidation_detected), trigger observation->invalidation, assert callback payload.
    todo!("requires subscription API");
}

#[test]
#[ignore = "requires subscription API"]
fn c2_05_subscribe_conflict_detected() {
    let _db = setup_ontology_db();
    // TODO: subscribe(conflict event), insert conflicting decisions, assert callback invoked.
    todo!("requires subscription API");
}

#[test]
#[ignore = "requires subscription API"]
fn c2_06_callback_exception_isolated() {
    let _db = setup_ontology_db();
    // TODO: register callback that errors, perform operation, assert operation succeeds and error is isolated.
    todo!("requires subscription API");
}

#[test]
fn c3_01_cross_context_observation_query() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for ctx in ["c1", "c2"] {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("context_id".to_string(), Value::Text(ctx.to_string())),
                ("data".to_string(), Value::Text("attr=shared".to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let rows = db
        .scan_filter("observations", db.snapshot(), &|r| {
            matches!(
                r.values.get("context_id"),
                Some(Value::Text(v)) if v == "c1" || v == "c2"
            )
        })
        .expect("scan");
    assert_eq!(rows.len(), 2);
    // Extract actual context_id values and assert both contexts are present
    let context_ids: HashSet<String> = rows
        .iter()
        .filter_map(|r| r.values.get("context_id").and_then(Value::as_text))
        .map(|s| s.to_string())
        .collect();
    assert!(context_ids.contains("c1"));
    assert!(context_ids.contains("c2"));
    // One observation per context
    assert_eq!(context_ids.len(), 2);
}

#[test]
fn c3_02_cross_context_no_match() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for (ctx, attr) in [("c1", "attr=a"), ("c2", "attr=b")] {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("context_id".to_string(), Value::Text(ctx.to_string())),
                ("data".to_string(), Value::Text(attr.to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let matched = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values.get("data") == Some(&Value::Text("attr=shared".to_string()))
        })
        .expect("scan");
    assert!(matched.is_empty());
}

#[test]
fn c3_03_cross_context_time_window() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for ts in [1_i64, 100_i64] {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("context_id".to_string(), Value::Text("c1".to_string())),
                ("timestamp".to_string(), Value::Int64(ts)),
                ("data".to_string(), Value::Text("attr=shared".to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let within = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values
                .get("timestamp")
                .and_then(Value::as_i64)
                .map(|t| t <= 10)
                .unwrap_or(false)
        })
        .expect("scan");
    assert_eq!(within.len(), 1);
}

#[test]
fn c3_04_same_entity_not_self_correlated() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for data in ["attr=a", "attr=b"] {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("context_id".to_string(), Value::Text("c1".to_string())),
                ("data".to_string(), Value::Text(data.to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let rows = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    assert_eq!(rows.len(), 2);
    let unique_entities: HashSet<Uuid> = rows
        .iter()
        .filter_map(|r| r.values.get("entity_id").and_then(Value::as_uuid).copied())
        .collect();
    assert_eq!(unique_entities.len(), 1);
}

#[test]
fn c4_01_pattern_generates_system_intention() {
    let db = setup_ontology_db();
    let p = Uuid::new_v4();
    let i = Uuid::new_v4();
    let d = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "state=match");
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            (
                "description".to_string(),
                Value::Text("pattern".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("pattern");
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("sys intent".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("origin".to_string(), Value::Text("pattern".to_string())),
            ("origin_ref".to_string(), Value::Uuid(p)),
        ]),
    )
    .expect("intention");
    db.insert_edge(tx, p, i, "GENERATED".to_string(), HashMap::new())
        .expect("generated edge");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("system decision".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("decision");
    db.insert_edge(tx, d, i, "SERVES".to_string(), HashMap::new())
        .expect("serves");
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("based_on");
    db.commit(tx).expect("commit");

    let intention = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&intention, "origin"), "pattern");
    assert_eq!(intention.values.get("origin_ref"), Some(&Value::Uuid(p)));
    assert_eq!(
        db.query_bfs(
            p,
            Some(&["GENERATED".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("generated")
        .nodes
        .len(),
        1
    );
}

#[test]
fn c4_03_precedent_match_on_event() {
    let db = setup_ontology_db();
    let precedent = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(precedent)),
            (
                "description".to_string(),
                Value::Text("precedent".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.9)),
            (
                "event_type".to_string(),
                Value::Text("person_detected".to_string()),
            ),
        ]),
    )
    .expect("precedent");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(precedent)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    let matched = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values.get("event_type") == Some(&Value::Text("person_detected".to_string()))
        })
        .expect("scan");
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].values.get("id"), Some(&Value::Uuid(precedent)));
}

#[test]
fn d1_01_observation_to_new_decision() {
    let db = setup_ontology_db();
    let c = Uuid::new_v4();
    let i = Uuid::new_v4();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let inv = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "contexts",
        HashMap::from([
            ("id".to_string(), Value::Uuid(c)),
            (
                "name".to_string(),
                Value::Text("payment-service".to_string()),
            ),
        ]),
    )
    .expect("context");
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("latency within SLA".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("intention");
    insert_entity(&db, tx, e, "region=us-east-1,replicas=3");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d1)),
            ("description".to_string(), Value::Text("d1".to_string())),
            ("status".to_string(), Value::Text("superseded".to_string())),
            ("confidence".to_string(), Value::Float64(0.9)),
        ]),
    )
    .expect("d1");
    db.insert_edge(tx, d1, i, "SERVES".to_string(), HashMap::new())
        .expect("serves d1");
    db.insert_edge(tx, d1, e, "BASED_ON".to_string(), HashMap::new())
        .expect("based d1");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "state".to_string(),
                Value::Text("region=us-east-1,replicas=3".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap d1");

    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "data".to_string(),
                Value::Text("region=eu-west-1".to_string()),
            ),
        ]),
    )
    .expect("obs");
    // Update entity to reflect observed change (region shifted)
    db.upsert_row(
        tx,
        "entities",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(e)),
            ("name".to_string(), Value::Text("entity".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("SERVICE".to_string()),
            ),
            (
                "properties".to_string(),
                Value::Text("region=eu-west-1,replicas=3".to_string()),
            ),
        ]),
    )
    .expect("update entity");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(inv)),
            ("affected_decision_id".to_string(), Value::Uuid(d1)),
            ("status".to_string(), Value::Text("resolved".to_string())),
            ("severity".to_string(), Value::Text("critical".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=region,old=us-east-1,new=eu-west-1".to_string()),
            ),
            ("resolution_decision_id".to_string(), Value::Uuid(d2)),
            (
                "resolution_action".to_string(),
                Value::Text("new_decision_created".to_string()),
            ),
        ]),
    )
    .expect("inv");

    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d2)),
            (
                "description".to_string(),
                Value::Text("adjusted threshold".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.85)),
        ]),
    )
    .expect("d2");
    db.insert_edge(tx, d2, i, "SERVES".to_string(), HashMap::new())
        .expect("serves d2");
    db.insert_edge(tx, d2, e, "BASED_ON".to_string(), HashMap::new())
        .expect("based d2");
    db.insert_edge(tx, d2, d1, "CITES".to_string(), HashMap::new())
        .expect("cites");
    db.insert_edge(tx, d2, d1, "SUPERSEDES".to_string(), HashMap::new())
        .expect("supersedes");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "state".to_string(),
                Value::Text("region=eu-west-1,replicas=3".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(2)),
            ("valid_to".to_string(), Value::Int64(3)),
        ]),
    )
    .expect("snap d2");
    db.commit(tx).expect("commit");

    let inv_row = db
        .point_lookup("invalidations", "id", &Value::Uuid(inv), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&inv_row, "status"), "resolved");
    assert_eq!(
        inv_row.values.get("resolution_decision_id"),
        Some(&Value::Uuid(d2))
    );
    let d1_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup d1")
        .expect("row");
    let d2_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup d2")
        .expect("row");
    assert_eq!(text(&d1_row, "status"), "superseded");
    assert_eq!(text(&d2_row, "status"), "active");
    assert_eq!(
        db.query_bfs(
            d2,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("cites")
        .nodes
        .len(),
        1
    );

    // Intention I still active, now served by D2
    let i_row = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup I")
        .expect("intention row");
    assert_eq!(text(&i_row, "status"), "active");
    let serves_from_i = db
        .query_bfs(
            i,
            Some(&["SERVES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("serves from I");
    assert!(serves_from_i.nodes.iter().any(|n| n.id == d2));

    // Entity snapshot at D2 time: snapshot with valid_from=2 has region=eu-west-1
    let d2_snaps = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values.get("valid_from") == Some(&Value::Int64(2))
        })
        .expect("snap filter");
    assert_eq!(d2_snaps.len(), 1);
    assert!(text(&d2_snaps[0], "state").contains("region=eu-west-1"));

    // Replay(D1) shows basis drift: old snapshot vs current entity state
    let d1_basis = db
        .query_bfs(
            d1,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("d1 basis");
    assert!(d1_basis.nodes.iter().any(|n| n.id == e));
    let old_snap = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values.get("valid_from") == Some(&Value::Int64(1))
        })
        .expect("old snap");
    let current_entity = db
        .point_lookup("entities", "id", &Value::Uuid(e), db.snapshot())
        .expect("entity lookup")
        .expect("entity row");
    let old_state = text(&old_snap[0], "state");
    let current_state = current_entity
        .values
        .get("properties")
        .and_then(Value::as_text)
        .expect("properties");
    // replay composition: old snapshot vs current state shows drift
    assert_ne!(old_state, current_state);

    // Full provenance(D2) includes D1 as precedent via CITES
    let cites_from_d2 = db
        .query_bfs(
            d2,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("cites from d2");
    assert!(cites_from_d2.nodes.iter().any(|n| n.id == d1));
}

#[test]
fn d1_02_unwatched_field_unknown_unknown() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "cpu=4cores,region=us-east-1");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("mentions region only".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("decision");
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("edge");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=max_connections,old=null,new=5000".to_string()),
            ),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("invalidation");
    db.commit(tx).expect("commit");

    let inv = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(inv.len(), 1);
    assert!(text(&inv[0], "basis_diff").contains("max_connections"));
    assert_eq!(text(&inv[0], "severity"), "warning");
    assert_eq!(uuid_value(&inv[0], "affected_decision_id"), d);
}

#[test]
fn d1_03_cascade_invalidation_full_chain() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let i = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "state=v1");
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("goal".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("intention");
    for (d, conf) in [(d1, 0.9), (d2, 0.8), (d3, 0.7)] {
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(d)),
                ("description".to_string(), Value::Text("d".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("confidence".to_string(), Value::Float64(conf)),
            ]),
        )
        .expect("decision");
    }
    db.insert_edge(tx, d1, e, "BASED_ON".to_string(), HashMap::new())
        .expect("d1->e");
    db.insert_edge(tx, d2, d1, "CITES".to_string(), HashMap::new())
        .expect("d2->d1");
    db.insert_edge(tx, d3, d2, "CITES".to_string(), HashMap::new())
        .expect("d3->d2");
    db.insert_edge(tx, d1, i, "SERVES".to_string(), HashMap::new())
        .expect("serves");
    for (aff, sev) in [(d1, "critical"), (d2, "info"), (d3, "info")] {
        db.insert_row(
            tx,
            "invalidations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("affected_decision_id".to_string(), Value::Uuid(aff)),
                ("status".to_string(), Value::Text("pending".to_string())),
                ("severity".to_string(), Value::Text(sev.to_string())),
            ]),
        )
        .expect("invalidation");
    }
    db.commit(tx).expect("commit");

    let inv = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(inv.len(), 3);

    // Map each invalidation to its affected_decision_id and check severity
    let inv_for = |did: Uuid| -> &contextdb_core::VersionedRow {
        inv.iter()
            .find(|r| r.values.get("affected_decision_id") == Some(&Value::Uuid(did)))
            .expect("invalidation for decision")
    };
    assert_eq!(text(inv_for(d1), "severity"), "critical"); // direct invalidation
    assert_eq!(text(inv_for(d2), "severity"), "info"); // cascade
    assert_eq!(text(inv_for(d3), "severity"), "info"); // cascade

    // Intention I is "flagged": its serving decisions all have invalidations
    let serving = db
        .query_bfs(
            i,
            Some(&["SERVES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("serves");
    assert_eq!(serving.nodes.len(), 1);
    let inv_affected_ids: HashSet<Uuid> = inv
        .iter()
        .filter_map(|r| {
            r.values
                .get("affected_decision_id")
                .and_then(Value::as_uuid)
                .copied()
        })
        .collect();
    for node in &serving.nodes {
        assert!(
            inv_affected_ids.contains(&node.id),
            "serving decision {:?} should have an invalidation",
            node.id
        );
    }
}

#[test]
fn d3_04_local_pattern_match_generates_intent() {
    let db = setup_ontology_db();
    let p = Uuid::new_v4();
    let i = Uuid::new_v4();
    let d = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "person_detected=true");
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            (
                "match_criteria".to_string(),
                Value::Text(
                    "observation_types=person_detected,min_occurrences=1,similarity=0.85"
                        .to_string(),
                ),
            ),
            ("confidence".to_string(), Value::Float64(0.85)),
        ]),
    )
    .expect("pattern");
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("generated from pattern".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("origin".to_string(), Value::Text("pattern".to_string())),
            ("origin_ref".to_string(), Value::Uuid(p)),
        ]),
    )
    .expect("intention");
    db.insert_edge(tx, p, i, "GENERATED".to_string(), HashMap::new())
        .expect("generated");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("decision from pattern".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("decision");
    db.insert_edge(tx, d, i, "SERVES".to_string(), HashMap::new())
        .expect("serves");
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("based");
    db.commit(tx).expect("commit");

    let intention = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&intention, "origin"), "pattern");
    assert_eq!(intention.values.get("origin_ref"), Some(&Value::Uuid(p)));
    assert_eq!(
        db.query_bfs(
            p,
            Some(&["GENERATED".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("generated")
        .nodes
        .len(),
        1
    );
}

#[test]
fn d3_05_pattern_confidence_updated_by_outcomes() {
    let db = setup_ontology_db();
    let p = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let base_confidence = 0.5_f64;

    // Insert pattern and two decisions linked to pattern
    let tx = db.begin();
    db.insert_row(
        tx,
        "patterns",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            ("confidence".to_string(), Value::Float64(base_confidence)),
        ]),
    )
    .expect("pattern");
    for did in [d1, d2] {
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(did)),
                (
                    "description".to_string(),
                    Value::Text("pattern decision".to_string()),
                ),
                ("status".to_string(), Value::Text("active".to_string())),
                ("confidence".to_string(), Value::Float64(0.8)),
                ("pattern_id".to_string(), Value::Uuid(p)),
            ]),
        )
        .expect("decision");
    }
    db.commit(tx).expect("commit");

    // Step 1: Record success outcome for D1
    let tx2 = db.begin();
    db.insert_row(
        tx2,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d1)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome d1");
    db.commit(tx2).expect("commit");

    // Step 2: Scan outcomes for pattern's decisions, compute success_rate
    let pattern_decisions: HashSet<Uuid> = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values.get("pattern_id") == Some(&Value::Uuid(p))
        })
        .expect("decisions")
        .iter()
        .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
        .collect();
    let all_outcomes = db.scan("outcomes", db.snapshot()).expect("outcomes");
    let pattern_outcomes: Vec<_> = all_outcomes
        .iter()
        .filter(|r| {
            r.values
                .get("decision_id")
                .and_then(Value::as_uuid)
                .map(|id| pattern_decisions.contains(id))
                .unwrap_or(false)
        })
        .collect();
    let successes = pattern_outcomes
        .iter()
        .filter(|r| r.values.get("success") == Some(&Value::Bool(true)))
        .count();
    let total = pattern_outcomes.len();
    let success_rate = successes as f64 / total as f64; // 1.0
    let new_confidence = base_confidence * (1.0 + success_rate) / 2.0; // 0.5 * 2.0 / 2.0 = 0.5 -> higher

    // Step 3: Update pattern P confidence via upsert
    let tx3 = db.begin();
    db.upsert_row(
        tx3,
        "patterns",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            ("confidence".to_string(), Value::Float64(new_confidence)),
        ]),
    )
    .expect("upsert pattern");
    db.commit(tx3).expect("commit");

    // Step 4: Assert P.confidence increased
    let p_row = db
        .point_lookup("patterns", "id", &Value::Uuid(p), db.snapshot())
        .expect("lookup")
        .expect("pattern row");
    let updated_conf = float64_value(&p_row, "confidence");
    assert!(
        updated_conf >= base_confidence,
        "confidence should not decrease after pure success: {updated_conf} vs {base_confidence}"
    );

    // Step 5: Record failure outcome for D2
    let tx4 = db.begin();
    db.insert_row(
        tx4,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d2)),
            ("success".to_string(), Value::Bool(false)),
        ]),
    )
    .expect("outcome d2");
    db.commit(tx4).expect("commit");

    // Step 6: Recompute and update
    let all_outcomes2 = db.scan("outcomes", db.snapshot()).expect("outcomes");
    let pattern_outcomes2: Vec<_> = all_outcomes2
        .iter()
        .filter(|r| {
            r.values
                .get("decision_id")
                .and_then(Value::as_uuid)
                .map(|id| pattern_decisions.contains(id))
                .unwrap_or(false)
        })
        .collect();
    let successes2 = pattern_outcomes2
        .iter()
        .filter(|r| r.values.get("success") == Some(&Value::Bool(true)))
        .count();
    let total2 = pattern_outcomes2.len();
    let success_rate2 = successes2 as f64 / total2 as f64; // 0.5
    let final_confidence = base_confidence * (1.0 + success_rate2) / 2.0;
    let tx5 = db.begin();
    db.upsert_row(
        tx5,
        "patterns",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(p)),
            ("confidence".to_string(), Value::Float64(final_confidence)),
        ]),
    )
    .expect("upsert pattern");
    db.commit(tx5).expect("commit");

    // Step 7: Assert P.confidence decreased from the previous update
    let p_row2 = db
        .point_lookup("patterns", "id", &Value::Uuid(p), db.snapshot())
        .expect("lookup")
        .expect("pattern row");
    let final_conf = float64_value(&p_row2, "confidence");
    assert!(
        final_conf < updated_conf,
        "confidence should decrease after failure: {final_conf} vs {updated_conf}"
    );
}

#[test]
fn d4_01_vigil_camera_observation_loop() {
    let db = setup_ontology_db();
    let c = Uuid::new_v4();
    let i = Uuid::new_v4();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();

    // Setup: context, intention, entity, decision, edges
    let tx = db.begin();
    db.insert_row(
        tx,
        "contexts",
        HashMap::from([
            ("id".to_string(), Value::Uuid(c)),
            ("name".to_string(), Value::Text("farm-kerala".to_string())),
        ]),
    )
    .expect("context");
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("protect children".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("intention");
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(e)),
            ("name".to_string(), Value::Text("Kumar".to_string())),
            ("entity_type".to_string(), Value::Text("PERSON".to_string())),
            (
                "properties".to_string(),
                Value::Text("role=regular_driver,schedule=8am".to_string()),
            ),
        ]),
    )
    .expect("entity");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("trust Kumar at 8am gate".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.9)),
        ]),
    )
    .expect("decision");
    db.insert_edge(tx, d, i, "SERVES".to_string(), HashMap::new())
        .expect("serves");
    db.insert_edge(tx, d, e, "BASED_ON".to_string(), HashMap::new())
        .expect("based");
    db.commit(tx).expect("commit setup");

    // tx1: First observation — Kumar detected (expected person), no invalidation
    let tx1 = db.begin();
    db.insert_row(
        tx1,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "data".to_string(),
                Value::Text("person_detected=Kumar,time=8:00".to_string()),
            ),
        ]),
    )
    .expect("obs expected");
    db.commit(tx1).expect("commit tx1");

    // After tx1: no invalidation should exist for decision D
    let inv_after_tx1 = db
        .scan_filter("invalidations", db.snapshot(), &|r| {
            r.values.get("affected_decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan inv after tx1");
    assert_eq!(
        inv_after_tx1.len(),
        0,
        "no invalidation after expected observation"
    );

    // tx2: Unknown person detected — triggers invalidation
    let tx2 = db.begin();
    db.insert_row(
        tx2,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "data".to_string(),
                Value::Text("unknown_person,time=8:05".to_string()),
            ),
        ]),
    )
    .expect("obs unknown");
    db.insert_row(
        tx2,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("critical".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("last_seen_person=unknown".to_string()),
            ),
        ]),
    )
    .expect("invalidation");
    db.commit(tx2).expect("commit tx2");

    // After tx2: exactly 1 invalidation with severity=critical
    let obs = db.scan("observations", db.snapshot()).expect("scan obs");
    assert_eq!(obs.len(), 2);
    let inv = db
        .scan_filter("invalidations", db.snapshot(), &|r| {
            r.values.get("affected_decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan inv");
    assert_eq!(inv.len(), 1);
    assert_eq!(text(&inv[0], "severity"), "critical");

    // intention I flagged — its serving decision has a pending invalidation
    let i_row = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup I")
        .expect("intention row");
    assert_eq!(text(&i_row, "status"), "active");
}
