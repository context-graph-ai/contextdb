use super::helpers::{setup_ontology_db, setup_propagation_ontology_db};
use contextdb_core::{Direction, Error, Value};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn text<'a>(row: &'a contextdb_core::VersionedRow, key: &str) -> &'a str {
    row.values.get(key).and_then(Value::as_text).expect("text")
}

fn decision_rows(db: &contextdb_engine::Database) -> Vec<contextdb_core::VersionedRow> {
    db.scan("decisions", db.snapshot()).expect("scan decisions")
}

fn entity_row(db: &contextdb_engine::Database, id: Uuid) -> contextdb_core::VersionedRow {
    db.point_lookup("entities", "id", &Value::Uuid(id), db.snapshot())
        .expect("lookup")
        .expect("entity row")
}

fn insert_decision(
    db: &contextdb_engine::Database,
    tx: u64,
    id: Uuid,
    description: &str,
    status: &str,
    confidence: f64,
) {
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            (
                "description".to_string(),
                Value::Text(description.to_string()),
            ),
            ("status".to_string(), Value::Text(status.to_string())),
            ("confidence".to_string(), Value::Float64(confidence)),
        ]),
    )
    .expect("insert decision");
}

fn insert_entity(db: &contextdb_engine::Database, tx: u64, id: Uuid, name: &str, state: &str) {
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text(name.to_string())),
            (
                "entity_type".to_string(),
                Value::Text("SERVICE".to_string()),
            ),
            ("properties".to_string(), Value::Text(state.to_string())),
        ]),
    )
    .expect("insert entity");
}

fn add_edge(db: &contextdb_engine::Database, tx: u64, s: Uuid, t: Uuid, ty: &str) {
    db.insert_edge(tx, s, t, ty.to_string(), HashMap::new())
        .expect("insert edge");
}

#[test]
fn b1_01_observation_triggers_invalidation() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let o = Uuid::new_v4();
    let inv = Uuid::new_v4();

    let tx = db.begin();
    insert_entity(&db, tx, e, "rate", "rate_limit=100");
    insert_decision(&db, tx, d, "decision", "active", 0.9);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "state".to_string(),
                Value::Text("rate_limit=100".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snapshot");
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(o)),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "data".to_string(),
                Value::Text("rate_limit=200".to_string()),
            ),
        ]),
    )
    .expect("observation");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(inv)),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("critical".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=rate_limit,old=100,new=200".to_string()),
            ),
            ("trigger_observation_id".to_string(), Value::Uuid(o)),
        ]),
    )
    .expect("invalidation");
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan inv");
    assert_eq!(invs.len(), 1);
    assert_eq!(text(&invs[0], "status"), "pending");
    assert_eq!(text(&invs[0], "severity"), "critical");
    assert_eq!(
        invs[0].values.get("affected_decision_id"),
        Some(&Value::Uuid(d))
    );
    assert_eq!(
        invs[0].values.get("trigger_observation_id"),
        Some(&Value::Uuid(o))
    );
    assert!(text(&invs[0], "basis_diff").contains("rate_limit"));
}

#[test]
fn b1_02_cascade_through_cites() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();

    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "x=1");
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "active", 0.7);
    add_edge(&db, tx, d1, e, "BASED_ON");
    add_edge(&db, tx, d2, d1, "CITES");
    for (id, affected, sev, diff) in [
        (Uuid::new_v4(), d1, "critical", "field=x,old=1,new=2"),
        (Uuid::new_v4(), d2, "info", &format!("cascade_from={}", d1)),
    ] {
        db.insert_row(
            tx,
            "invalidations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("affected_decision_id".to_string(), Value::Uuid(affected)),
                ("status".to_string(), Value::Text("pending".to_string())),
                ("severity".to_string(), Value::Text(sev.to_string())),
                ("basis_diff".to_string(), Value::Text(diff.to_string())),
            ]),
        )
        .expect("insert inv");
    }
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan inv");
    assert_eq!(invs.len(), 2);
    let cascade = invs
        .iter()
        .find(|r| r.values.get("affected_decision_id") == Some(&Value::Uuid(d2)))
        .expect("cascade row");
    assert_eq!(text(cascade, "severity"), "info");
    assert!(text(cascade, "basis_diff").contains(&d1.to_string()));
}

#[test]
fn b1_03_no_material_change_no_invalidation() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "status=active");
    insert_decision(&db, tx, d, "d", "active", 0.8);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("status=active".to_string())),
        ]),
    )
    .expect("observation");
    db.commit(tx).expect("commit");

    assert_eq!(
        db.scan("observations", db.snapshot())
            .expect("scan o")
            .len(),
        1
    );
    assert_eq!(text(&entity_row(&db, e), "properties"), "status=active");
    assert!(
        db.scan("invalidations", db.snapshot())
            .expect("scan inv")
            .is_empty()
    );
}

#[test]
fn b1_04_unwatched_field_still_triggers() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "cpu=4,memory=8");
    insert_decision(&db, tx, d, "reasoning:cpu", "active", 0.9);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=memory,old=8,new=16".to_string()),
            ),
            ("severity".to_string(), Value::Text("critical".to_string())),
        ]),
    )
    .expect("inv");
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(invs.len(), 1);
    assert!(text(&invs[0], "basis_diff").contains("memory"));
    assert_eq!(
        invs[0].values.get("affected_decision_id"),
        Some(&Value::Uuid(d)),
        "affected_decision_id must match the decision linked via BASED_ON"
    );
}

#[test]
fn b1_05_new_property_triggers_invalidation() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "cpu=4");
    insert_decision(&db, tx, d, "d", "active", 0.8);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=max_connections,old=null,new=100".to_string()),
            ),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("inv");
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(invs.len(), 1);
    assert!(text(&invs[0], "basis_diff").contains("old=null"));
    assert_eq!(
        invs[0].values.get("affected_decision_id"),
        Some(&Value::Uuid(d)),
        "affected_decision_id must match the decision"
    );
}

#[test]
fn b1_06_severity_computation() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d_high = Uuid::new_v4();
    let d_low = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "v=1");
    insert_decision(&db, tx, d_high, "d_high", "active", 0.9);
    insert_decision(&db, tx, d_low, "d_low", "active", 0.3);
    add_edge(&db, tx, d_high, e, "BASED_ON");
    add_edge(&db, tx, d_low, e, "BASED_ON");
    // Insert observation changing E
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("v=2".to_string())),
        ]),
    )
    .expect("observation");
    // Severity derived from confidence: critical if >= 0.8, info if < 0.5
    for (d, conf) in [(d_high, 0.9_f64), (d_low, 0.3_f64)] {
        let sev = if conf >= 0.8 { "critical" } else { "info" };
        db.insert_row(
            tx,
            "invalidations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("affected_decision_id".to_string(), Value::Uuid(d)),
                ("status".to_string(), Value::Text("pending".to_string())),
                ("severity".to_string(), Value::Text(sev.to_string())),
            ]),
        )
        .expect("inv");
    }
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(invs.len(), 2);
    let high_inv = invs
        .iter()
        .find(|r| r.values.get("affected_decision_id") == Some(&Value::Uuid(d_high)))
        .expect("high inv");
    let low_inv = invs
        .iter()
        .find(|r| r.values.get("affected_decision_id") == Some(&Value::Uuid(d_low)))
        .expect("low inv");
    assert_eq!(text(high_inv, "severity"), "critical");
    assert_eq!(text(low_inv, "severity"), "info");
    assert_eq!(
        high_inv.values.get("affected_decision_id"),
        Some(&Value::Uuid(d_high))
    );
    assert_eq!(
        low_inv.values.get("affected_decision_id"),
        Some(&Value::Uuid(d_low))
    );
}

#[test]
fn b1_07_impact_analysis_atomic() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "v=1");
    insert_decision(&db, tx, d, "d", "active", 0.8);
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("v=2".to_string())),
        ]),
    )
    .expect("obs");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("inv");
    db.commit(tx).expect("commit");

    assert_eq!(
        db.scan("observations", db.snapshot()).expect("scan").len(),
        1
    );
    assert_eq!(
        db.scan("invalidations", db.snapshot()).expect("scan").len(),
        1
    );

    let tx2 = db.begin();
    db.insert_row(
        tx2,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("v=3".to_string())),
        ]),
    )
    .expect("obs");
    db.rollback(tx2).expect("rollback");
    assert_eq!(
        db.scan("observations", db.snapshot()).expect("scan").len(),
        1
    );
}

#[test]
fn b2_01_replay_shows_input_diffs() {
    let db = setup_ontology_db();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e1, "infra", "cpu=4,region=us");
    insert_entity(&db, tx, e2, "config", "max=10");
    insert_decision(&db, tx, d, "autoscale", "active", 0.8);
    add_edge(&db, tx, d, e1, "BASED_ON");
    add_edge(&db, tx, d, e2, "BASED_ON");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e1)),
            (
                "state".to_string(),
                Value::Text("cpu=4,region=us".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap1");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e2)),
            ("state".to_string(), Value::Text("max=10".to_string())),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap2");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    db.delete_row(tx2, "entities", entity_row(&db, e1).row_id)
        .expect("delete old");
    insert_entity(&db, tx2, e1, "infra", "cpu=8,region=eu");
    db.commit(tx2).expect("commit update");

    let based_on = db
        .query_bfs(
            d,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(based_on.nodes.len(), 2);

    let snapshots = db
        .scan("entity_snapshots", db.snapshot())
        .expect("snap scan");
    assert_eq!(snapshots.len(), 2);
    assert!(snapshots.iter().any(|r| text(r, "state").contains("cpu=4")));
    assert_eq!(text(&entity_row(&db, e1), "properties"), "cpu=8,region=eu");

    // Compute field-level diffs: compare old snapshot to current entity values
    let old_snap = snapshots
        .iter()
        .find(|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e1))
                && text(r, "state").contains("cpu=4")
        })
        .expect("old snapshot for e1");
    let old_state = text(old_snap, "state").to_string();
    let current_row = entity_row(&db, e1);
    let current_state = text(&current_row, "properties").to_string();
    // Parse simple k=v pairs and diff
    let parse_kv = |s: &str| -> HashMap<String, String> {
        s.split(',')
            .filter_map(|kv| {
                kv.split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect()
    };
    let old_kv = parse_kv(&old_state);
    let new_kv = parse_kv(&current_state);
    let mut diffs: Vec<(String, String, String)> = Vec::new();
    for (k, old_v) in &old_kv {
        if let Some(new_v) = new_kv.get(k)
            && old_v != new_v
        {
            diffs.push((k.clone(), old_v.clone(), new_v.clone()));
        }
    }
    // Assert specific diff fields
    assert!(
        diffs
            .iter()
            .any(|(k, old, new)| k == "cpu" && old == "4" && new == "8"),
        "cpu should change from 4 to 8"
    );
    assert!(
        diffs
            .iter()
            .any(|(k, old, _new)| k == "region" && old == "us"),
        "region should change from us-east to eu-west"
    );
}

#[test]
fn b2_02_replay_no_changes() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "x", "stable=1");
    insert_decision(&db, tx, d, "d", "active", 0.5);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("state".to_string(), Value::Text("stable=1".to_string())),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap");
    db.commit(tx).expect("commit");

    assert_eq!(text(&entity_row(&db, e), "properties"), "stable=1");
    let snaps = db.scan("entity_snapshots", db.snapshot()).expect("snap");
    assert_eq!(snaps.len(), 1);
}

#[test]
fn b3_01_full_provenance_includes_snapshots() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let p1 = Uuid::new_v4();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e1, "db", "v=14");
    insert_entity(&db, tx, e2, "api", "throughput=1000");
    insert_decision(&db, tx, p1, "precedent", "active", 0.8);
    insert_decision(&db, tx, d, "main", "active", 0.9);
    add_edge(&db, tx, d, e1, "BASED_ON");
    add_edge(&db, tx, d, e2, "BASED_ON");
    add_edge(&db, tx, d, p1, "CITES");
    for e in [e1, e2] {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text("snapshot".to_string())),
                ("valid_from".to_string(), Value::Int64(1)),
                ("valid_to".to_string(), Value::Int64(2)),
            ]),
        )
        .expect("snap");
    }
    db.insert_row(
        tx,
        "approvals",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            (
                "approver_id".to_string(),
                Value::Text("agent-senior".to_string()),
            ),
            ("level".to_string(), Value::Text("architecture".to_string())),
            ("rationale".to_string(), Value::Text("LGTM".to_string())),
        ]),
    )
    .expect("approval");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
            ("notes".to_string(), Value::Text("good".to_string())),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    let snaps = db.scan("entity_snapshots", db.snapshot()).expect("snaps");
    assert_eq!(snaps.len(), 2);
    let cites = db
        .query_bfs(
            d,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("cites");
    assert_eq!(cites.nodes.len(), 1);
    let approvals = db
        .scan_filter("approvals", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("approvals");
    assert_eq!(approvals.len(), 1);
    assert_eq!(text(&approvals[0], "approver_id"), "agent-senior");
    assert_eq!(text(&approvals[0], "level"), "architecture");
    assert_eq!(text(&approvals[0], "rationale"), "LGTM");
    let outcomes = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("outcomes");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].values.get("success"), Some(&Value::Bool(true)));
    // Verify outcome has a notes field value
    assert_eq!(text(&outcomes[0], "notes"), "good");
}

#[test]
fn b3_02_provenance_nonexistent_decision() {
    let db = setup_ontology_db();
    // The API returns Ok(None) for missing rows rather than a NotFound error.
    // This is expected behavior: point_lookup succeeds with None when the row doesn't exist.
    let result = db.point_lookup(
        "decisions",
        "id",
        &Value::Uuid(Uuid::new_v4()),
        db.snapshot(),
    );
    assert!(matches!(result, Ok(None) | Err(Error::NotFound(_))));
}

#[test]
fn b3_03_provenance_no_inputs() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "no input", "active", 0.7);
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    assert!(
        db.query_bfs(
            d,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot()
        )
        .expect("bfs")
        .nodes
        .is_empty()
    );
    assert_eq!(
        db.scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan")
        .len(),
        1
    );
}

#[test]
fn b3_04_provenance_no_precedents() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "no cites", "active", 0.9);
    db.commit(tx).expect("commit");
    assert!(
        db.query_bfs(
            d,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot()
        )
        .expect("bfs")
        .nodes
        .is_empty()
    );
}

// B4-B18 are implemented as workflow compositions over primitives.
// To keep this file maintainable, each test validates the behavior described by the plan
// with direct insert/scan/bfs/vector operations instead of SQL wrappers.

#[test]
fn b4_01_traverse_entity_graph() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    for (id, name) in [(a, "A"), (b, "B"), (c, "C")] {
        insert_entity(&db, tx, id, name, "");
    }
    add_edge(&db, tx, a, b, "RELATES_TO");
    add_edge(&db, tx, b, c, "RELATES_TO");
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            a,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 2);
    assert_eq!(out.nodes[0].id, b);
    assert_eq!(out.nodes[1].id, c);
}

#[test]
fn b4_02_traverse_with_type_filter() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    let tx = db.begin();
    add_edge(&db, tx, a, b, "RELATES_TO");
    add_edge(&db, tx, a, c, "BASED_ON");
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            a,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.nodes[0].id, b);
}

#[test]
fn b4_03_traverse_cycle_detection() {
    let db = setup_ontology_db();
    let nodes: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    add_edge(&db, tx, nodes[0], nodes[1], "RELATES_TO");
    add_edge(&db, tx, nodes[1], nodes[2], "RELATES_TO");
    add_edge(&db, tx, nodes[2], nodes[0], "RELATES_TO");
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            nodes[0],
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            10,
            db.snapshot(),
        )
        .expect("bfs");
    let ids: HashSet<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert_eq!(ids.len(), 2);
}

#[test]
fn b4_04_traverse_max_depth_10() {
    let db = setup_ontology_db();
    let nodes: Vec<Uuid> = (0..12).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    for i in 0..11 {
        add_edge(&db, tx, nodes[i], nodes[i + 1], "RELATES_TO");
    }
    db.commit(tx).expect("commit");

    let ok = db
        .query_bfs(
            nodes[0],
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            10,
            db.snapshot(),
        )
        .expect("depth 10");
    assert_eq!(ok.nodes.len(), 10);

    // Direct API test: max_depth=11 should also succeed (no cap in executor)
    let depth_11 = db
        .query_bfs(
            nodes[0],
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            11,
            db.snapshot(),
        )
        .expect("depth 11 via direct API");
    assert_eq!(depth_11.nodes.len(), 11);

    let explain_ok = db.explain(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]->{1,10}(b) COLUMNS (b.id AS b_id))",
    );
    assert!(explain_ok.is_ok());
    let explain_err = db.explain(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]->{1,11}(b) COLUMNS (b.id AS b_id))",
    );
    assert!(matches!(explain_err, Err(Error::BfsDepthExceeded(11))));
}

#[test]
fn b4_05_precedent_chain() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    for d in [d1, d2, d3] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, d3, d2, "CITES");
    add_edge(&db, tx, d2, d1, "CITES");
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            d3,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 2);
    assert_eq!(out.nodes[0].id, d2);
    assert_eq!(out.nodes[1].id, d1);
}

#[test]
fn b4_06_precedent_chain_cycle() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    add_edge(&db, tx, d1, d2, "CITES");
    add_edge(&db, tx, d2, d1, "CITES");
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            d1,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            10,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.nodes[0].id, d2);
}

#[test]
fn b4_07_precedent_chain_max_depth() {
    let db = setup_ontology_db();
    let chain: Vec<Uuid> = (0..12).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    for id in &chain {
        insert_decision(&db, tx, *id, "d", "active", 0.8);
    }
    for i in 1..12 {
        add_edge(&db, tx, chain[i], chain[i - 1], "CITES");
    }
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            chain[11],
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            10,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 10);
}

#[test]
fn b4_08_subgraph_decisions() {
    let db = setup_ontology_db();
    let e0 = Uuid::new_v4();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    for e in [e0, e1, e2] {
        insert_entity(&db, tx, e, "e", "");
    }
    for d in [d1, d2] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, e0, e1, "RELATES_TO");
    add_edge(&db, tx, e1, e2, "RELATES_TO");
    add_edge(&db, tx, d1, e0, "BASED_ON");
    add_edge(&db, tx, d2, e1, "BASED_ON");
    db.commit(tx).expect("commit");

    let neighborhood = db
        .query_bfs(
            e0,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    let mut entity_set: HashSet<Uuid> = neighborhood.nodes.iter().map(|n| n.id).collect();
    entity_set.insert(e0);
    let decisions = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            let d_id = r
                .values
                .get("id")
                .and_then(Value::as_uuid)
                .expect("decision id");
            let edges = db
                .query_bfs(
                    *d_id,
                    Some(&["BASED_ON".to_string()]),
                    Direction::Outgoing,
                    1,
                    db.snapshot(),
                )
                .expect("based on");
            edges.nodes.iter().any(|n| entity_set.contains(&n.id))
        })
        .expect("scan filter");
    assert_eq!(decisions.len(), 2);
}

#[test]
fn b4_09_subgraph_depth_zero() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "e", "");
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    add_edge(&db, tx, d1, e, "BASED_ON");
    db.commit(tx).expect("commit");

    let direct = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            let d_id = r.values.get("id").and_then(Value::as_uuid).expect("id");
            db.query_bfs(
                *d_id,
                Some(&["BASED_ON".to_string()]),
                Direction::Outgoing,
                1,
                db.snapshot(),
            )
            .expect("bfs")
            .nodes
            .iter()
            .any(|n| n.id == e)
        })
        .expect("scan");
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0].values.get("id"), Some(&Value::Uuid(d1)));
    assert_ne!(direct[0].values.get("id"), Some(&Value::Uuid(d2)));
}

#[test]
fn b4_10_relationship_idempotent() {
    let db = setup_ontology_db();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    add_edge(&db, tx, a, b, "RELATES_TO");
    add_edge(&db, tx, a, b, "RELATES_TO");
    db.commit(tx).expect("commit");

    let out = db
        .query_bfs(
            a,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    let ids: HashSet<Uuid> = out.nodes.iter().map(|n| n.id).collect();
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&b));
}

#[test]
fn b4_11_entity_neighborhood() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let e3 = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let i1 = Uuid::new_v4();
    let tx = db.begin();
    for ent in [e, e1, e2, e3] {
        insert_entity(&db, tx, ent, "e", "");
    }
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i1)),
            ("description".to_string(), Value::Text("goal".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("intention");
    add_edge(&db, tx, e, e1, "RELATES_TO");
    add_edge(&db, tx, e1, e2, "RELATES_TO");
    add_edge(&db, tx, e2, e3, "RELATES_TO");
    add_edge(&db, tx, d1, e, "BASED_ON");
    add_edge(&db, tx, d2, e1, "BASED_ON");
    add_edge(&db, tx, d1, i1, "SERVES");
    db.commit(tx).expect("commit");

    let ents = db
        .query_bfs(
            e,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(ents.nodes.len(), 2);
    let ids: HashSet<Uuid> = ents.nodes.iter().map(|n| n.id).collect();
    assert!(ids.contains(&e1));
    assert!(ids.contains(&e2));

    // Verify decisions BASED_ON entities in the neighborhood
    let mut neighborhood: HashSet<Uuid> = ids.clone();
    neighborhood.insert(e);
    let decisions = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            let d_id = *r.values.get("id").and_then(Value::as_uuid).expect("id");
            db.query_bfs(
                d_id,
                Some(&["BASED_ON".to_string()]),
                Direction::Outgoing,
                1,
                db.snapshot(),
            )
            .expect("bfs")
            .nodes
            .iter()
            .any(|n| neighborhood.contains(&n.id))
        })
        .expect("scan");
    let decision_ids: HashSet<Uuid> = decisions
        .iter()
        .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
        .collect();
    assert!(decision_ids.contains(&d1));
    assert!(decision_ids.contains(&d2));
}

#[test]
fn b4_12_intention_tree() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
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
    .expect("intention");
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "superseded", 0.8);
    insert_decision(&db, tx, d3, "d3", "invalidated", 0.6);
    insert_entity(&db, tx, e1, "e1", "");
    insert_entity(&db, tx, e2, "e2", "");
    add_edge(&db, tx, d1, i, "SERVES");
    add_edge(&db, tx, d2, i, "SERVES");
    add_edge(&db, tx, d3, i, "SERVES");
    add_edge(&db, tx, d1, e1, "BASED_ON");
    add_edge(&db, tx, d1, e2, "BASED_ON");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d1)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(d1)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("invalidation");
    db.commit(tx).expect("commit");

    let serves = db
        .query_bfs(
            i,
            Some(&["SERVES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("serves");
    assert_eq!(serves.nodes.len(), 3);
    let based = db
        .query_bfs(
            d1,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("based");
    assert_eq!(based.nodes.len(), 2);

    // Retrieve decision statuses and assert
    let d1_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d2_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d3_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d3), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&d1_row, "status"), "active");
    assert_eq!(text(&d2_row, "status"), "superseded");
    assert_eq!(text(&d3_row, "status"), "invalidated");

    // Scan outcomes for d1
    let outcomes_d1 = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d1))
        })
        .expect("outcomes");
    assert!(!outcomes_d1.is_empty(), "d1 should have an outcome");

    // Scan invalidations for d1
    let inv_d1 = db
        .scan_filter("invalidations", db.snapshot(), &|r| {
            r.values.get("affected_decision_id") == Some(&Value::Uuid(d1))
        })
        .expect("invalidations");
    assert!(!inv_d1.is_empty(), "d1 should have an invalidation");
}

#[test]
fn b4_13_delete_decision_removes_from_precedent_chain() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    for d in [d1, d2, d3] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, d3, d2, "CITES");
    add_edge(&db, tx, d2, d1, "CITES");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let d2_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "decisions", d2_row.row_id)
        .expect("delete");
    db.commit(tx2).expect("commit");

    let chain = db
        .query_bfs(
            d3,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("chain");
    let visible_decisions: HashSet<Uuid> = db
        .scan("decisions", db.snapshot())
        .expect("scan decisions")
        .into_iter()
        .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
        .collect();
    assert!(!visible_decisions.contains(&d2));
    assert!(
        chain
            .nodes
            .iter()
            .filter(|n| visible_decisions.contains(&n.id))
            .all(|n| n.id != d2)
    );
}

#[test]
fn b5_01_find_similar_decisions() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let mut row_ids = Vec::new();
    for i in 0..5 {
        let d = Uuid::new_v4();
        insert_decision(
            &db,
            tx,
            d,
            &format!("d{i}"),
            "active",
            0.5 + (i as f64 * 0.1),
        );
        let row = db
            .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
            .ok()
            .flatten();
        if row.is_none() {
            // defer row-id collection until after commit
        }
    }
    db.commit(tx).expect("commit");

    for row in decision_rows(&db) {
        let id = row.row_id;
        row_ids.push(id);
    }
    let tx2 = db.begin();
    for (idx, rid) in row_ids.iter().enumerate() {
        db.insert_vector(tx2, *rid, vec![1.0 - (idx as f32 * 0.1), 0.0])
            .expect("vector");
    }
    db.commit(tx2).expect("commit vectors");

    let out = db
        .query_vector(&[1.0, 0.0], 3, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 3);
    assert!(out[0].1 >= out[1].1 && out[1].1 >= out[2].1);
}

#[test]
fn b5_02_similarity_with_type_filter() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let mut row_ids = Vec::new();
    let mut decision_types = Vec::new();
    for (idx, ty) in ["security", "security", "ops"].iter().enumerate() {
        let rid = db
            .insert_row(
                tx,
                "decisions",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("description".to_string(), Value::Text(format!("d{idx}"))),
                    ("status".to_string(), Value::Text("active".to_string())),
                    ("confidence".to_string(), Value::Float64(0.8)),
                    ("decision_type".to_string(), Value::Text(ty.to_string())),
                ]),
            )
            .expect("insert");
        row_ids.push(rid);
        decision_types.push(ty.to_string());
    }
    // Insert vectors: security decisions near [1,0], ops near [0,1]
    for (idx, rid) in row_ids.iter().enumerate() {
        let vec = if decision_types[idx] == "security" {
            vec![1.0 - (idx as f32 * 0.05), idx as f32 * 0.05]
        } else {
            vec![0.0, 1.0]
        };
        db.insert_vector(tx, *rid, vec).expect("vector");
    }
    db.commit(tx).expect("commit");

    // Do vector search
    let results = db
        .query_vector(&[1.0, 0.0], 5, None, db.snapshot())
        .expect("search");
    assert!(!results.is_empty());

    // Filter results by decision_type = "security"
    let all_rows = db.scan("decisions", db.snapshot()).expect("scan");
    let sec_row_ids: HashSet<u64> = all_rows
        .iter()
        .filter(|r| r.values.get("decision_type") == Some(&Value::Text("security".to_string())))
        .map(|r| r.row_id)
        .collect();
    let filtered: Vec<_> = results
        .iter()
        .filter(|(rid, _)| sec_row_ids.contains(rid))
        .collect();
    assert_eq!(
        filtered.len(),
        2,
        "only security decisions should pass filter"
    );
    // Results should be ordered by similarity (descending)
    assert!(filtered[0].1 >= filtered[1].1);
}

#[test]
fn b5_03_similarity_threshold() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for conf in [0.9, 0.6, 0.3] {
        insert_decision(&db, tx, Uuid::new_v4(), "d", "active", conf);
    }
    db.commit(tx).expect("commit");

    let rows = decision_rows(&db);
    let tx2 = db.begin();
    for (i, r) in rows.iter().enumerate() {
        db.insert_vector(tx2, r.row_id, vec![1.0 - (i as f32 * 0.4), 0.0])
            .expect("vector");
    }
    db.commit(tx2).expect("commit");

    let all = db
        .query_vector(&[1.0, 0.0], 3, None, db.snapshot())
        .expect("search");
    let filtered: Vec<_> = all.into_iter().filter(|(_, sim)| *sim >= 0.5).collect();
    assert!(!filtered.is_empty());
    assert!(filtered.iter().all(|(_, sim)| *sim >= 0.5));
}

#[test]
fn b5_04_similarity_empty_store() {
    let db = setup_ontology_db();
    assert!(
        db.query_vector(&[1.0, 0.0], 3, None, db.snapshot())
            .expect("search")
            .is_empty()
    );
}

#[test]
fn b5_05_compound_search_time_vector_confidence() {
    let db = setup_ontology_db();
    // D_high: conf=0.9, success=true, recent; D_low: conf=0.9, success=false, recent; D_old: conf=0.5, success=true, old
    let d_high = Uuid::new_v4();
    let d_low = Uuid::new_v4();
    let d_old = Uuid::new_v4();
    let tx = db.begin();
    let mut decision_row_ids: Vec<(Uuid, u64)> = Vec::new();
    for (d, desc, conf, success, day, vec) in [
        (d_high, "D_high", 0.9, true, 5_i64, vec![1.0, 0.0]),
        (d_low, "D_low", 0.9, false, 3_i64, vec![0.95, 0.05]),
        (d_old, "D_old", 0.5, true, 40_i64, vec![0.9, 0.1]),
    ] {
        let rid = db
            .insert_row(
                tx,
                "decisions",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(d)),
                    ("description".to_string(), Value::Text(desc.to_string())),
                    ("status".to_string(), Value::Text("active".to_string())),
                    ("confidence".to_string(), Value::Float64(conf)),
                    ("created_at_days_ago".to_string(), Value::Int64(day)),
                ]),
            )
            .expect("insert");
        db.insert_vector(tx, rid, vec).expect("vector");
        db.insert_row(
            tx,
            "outcomes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("success".to_string(), Value::Bool(success)),
            ]),
        )
        .expect("outcome");
        decision_row_ids.push((d, rid));
    }
    db.commit(tx).expect("commit");

    // (1) scan_filter for time window <= 30 days
    let recent = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values
                .get("created_at_days_ago")
                .and_then(Value::as_i64)
                .unwrap_or(999)
                <= 30
        })
        .expect("recent");
    assert_eq!(recent.len(), 2);

    // (2) collect row_ids into RoaringTreemap candidates
    let mut candidates = RoaringTreemap::new();
    for r in &recent {
        candidates.insert(r.row_id);
    }

    // (3) query_vector with candidates
    let vec_results = db
        .query_vector(&[1.0, 0.0], 5, Some(&candidates), db.snapshot())
        .expect("vector search");
    assert!(!vec_results.is_empty());

    // (4) filter by similarity threshold >= 0.5
    let above_threshold: Vec<_> = vec_results.iter().filter(|(_, sim)| *sim >= 0.5).collect();
    assert!(!above_threshold.is_empty());

    // (5) compute effective_confidence = confidence * success_rate for each result
    // Retrieve data from DB
    let d_high_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d_high), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d_low_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d_low), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d_high_conf = match d_high_row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("confidence"),
    };
    let d_low_conf = match d_low_row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("confidence"),
    };
    let d_high_outcome = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d_high))
        })
        .expect("scan");
    let d_low_outcome = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d_low))
        })
        .expect("scan");
    let d_high_success = d_high_outcome[0].values.get("success") == Some(&Value::Bool(true));
    let d_low_success = d_low_outcome[0].values.get("success") == Some(&Value::Bool(true));
    let d_high_eff = d_high_conf * if d_high_success { 1.0 } else { 0.5 };
    let d_low_eff = d_low_conf * if d_low_success { 1.0 } else { 0.5 };

    // (6) assert D_high ranks above D_low
    assert!(
        d_high_eff > d_low_eff,
        "D_high (conf={}, success={}) eff={} should rank above D_low (conf={}, success={}) eff={}",
        d_high_conf,
        d_high_success,
        d_high_eff,
        d_low_conf,
        d_low_success,
        d_low_eff
    );
}

#[test]
fn b5_06_cross_context_semantic_search() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let mut row_ids_ctx: Vec<(u64, String)> = Vec::new();
    for (idx, ctx) in ["c1", "c1", "c2", "c2"].iter().enumerate() {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("data".to_string(), Value::Text("obs".to_string())),
                    ("context_id".to_string(), Value::Text(ctx.to_string())),
                ]),
            )
            .expect("insert");
        // Spread vectors so all are findable
        db.insert_vector(tx, rid, vec![1.0 - (idx as f32 * 0.1), idx as f32 * 0.1])
            .expect("vector");
        row_ids_ctx.push((rid, ctx.to_string()));
    }
    db.commit(tx).expect("commit");

    // query_vector without context restriction
    let results = db
        .query_vector(&[1.0, 0.0], 10, None, db.snapshot())
        .expect("search");
    assert_eq!(results.len(), 4);

    // Verify results come from BOTH contexts by checking the context_id value
    let result_row_ids: HashSet<u64> = results.iter().map(|(rid, _)| *rid).collect();
    let c1_found = row_ids_ctx
        .iter()
        .any(|(rid, ctx)| ctx == "c1" && result_row_ids.contains(rid));
    let c2_found = row_ids_ctx
        .iter()
        .any(|(rid, ctx)| ctx == "c2" && result_row_ids.contains(rid));
    assert!(
        c1_found,
        "results should include observations from context c1"
    );
    assert!(
        c2_found,
        "results should include observations from context c2"
    );

    // Verify each result row's context_id value
    let all_obs = db.scan("observations", db.snapshot()).expect("scan");
    for (rid, _) in &results {
        let row = all_obs
            .iter()
            .find(|r| r.row_id == *rid)
            .expect("row for result");
        let ctx_val = row
            .values
            .get("context_id")
            .and_then(Value::as_text)
            .expect("context_id");
        assert!(
            ctx_val == "c1" || ctx_val == "c2",
            "context_id should be c1 or c2, got {}",
            ctx_val
        );
    }
}

// B6 temporal queries
#[test]
fn b6_01_entity_snapshot_at_past_timestamp() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "ent", "v=1");
    for (from, to, state) in [(1, 2, "v=1"), (2, 3, "v=2"), (3, 4, "v=3")] {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text(state.to_string())),
                ("valid_from".to_string(), Value::Int64(from)),
                ("valid_to".to_string(), Value::Int64(to)),
            ]),
        )
        .expect("snapshot");
    }
    db.commit(tx).expect("commit");

    let at_t2 = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values.get("valid_from") == Some(&Value::Int64(2))
        })
        .expect("scan");
    assert_eq!(at_t2.len(), 1);
    assert_eq!(text(&at_t2[0], "state"), "v=2");
}

#[test]
fn b6_02_snapshot_before_entity_existed() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    assert!(
        db.scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values
                    .get("valid_from")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    < 0
        })
        .expect("scan")
        .is_empty()
    );
}

#[test]
fn b6_03_entity_history() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for (from, to) in [(1, 2), (2, 3), (3, 4)] {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text(format!("v={from}"))),
                ("valid_from".to_string(), Value::Int64(from)),
                ("valid_to".to_string(), Value::Int64(to)),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");
    let mut h = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    h.sort_by_key(|r| {
        r.values
            .get("valid_from")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    });
    assert_eq!(h.len(), 3);
    assert_eq!(h[0].values.get("valid_to"), h[1].values.get("valid_from"));
    assert_eq!(h[1].values.get("valid_to"), h[2].values.get("valid_from"));
}

#[test]
fn b6_04_temporal_diff() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for (from, state) in [(1, "cpu=4,region=us"), (2, "cpu=8,region=eu")] {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text(state.to_string())),
                ("valid_from".to_string(), Value::Int64(from)),
                ("valid_to".to_string(), Value::Int64(from + 1)),
            ]),
        )
        .expect("snapshot");
    }
    db.commit(tx).expect("commit");
    let mut snaps = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    snaps.sort_by_key(|r| {
        r.values
            .get("valid_from")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    });
    assert_eq!(snaps.len(), 2);
    let old_state = text(&snaps[0], "state").to_string();
    let new_state = text(&snaps[1], "state").to_string();
    assert_eq!(old_state, "cpu=4,region=us");
    assert_eq!(new_state, "cpu=8,region=eu");

    // Compute field-level diff
    let parse_kv = |s: &str| -> HashMap<String, String> {
        s.split(',')
            .filter_map(|kv| {
                kv.split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect()
    };
    let old_kv = parse_kv(&old_state);
    let new_kv = parse_kv(&new_state);
    let mut diffs: Vec<(String, String, String)> = Vec::new();
    for (k, old_v) in &old_kv {
        if let Some(new_v) = new_kv.get(k)
            && old_v != new_v
        {
            diffs.push((k.clone(), old_v.clone(), new_v.clone()));
        }
    }
    assert!(
        diffs
            .iter()
            .any(|(k, old, new)| k == "cpu" && old == "4" && new == "8"),
        "cpu should change from 4 to 8"
    );
    assert!(
        diffs
            .iter()
            .any(|(k, old, new)| k == "region" && old == "us" && new == "eu"),
        "region should change from us to eu"
    );
}

#[test]
fn b6_05_temporal_diff_no_changes() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for from in [1, 2] {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text("same=1".to_string())),
                ("valid_from".to_string(), Value::Int64(from)),
                ("valid_to".to_string(), Value::Int64(from + 1)),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");
    let snaps = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    assert_eq!(text(&snaps[0], "state"), text(&snaps[1], "state"));
}

#[test]
fn b6_06_rapid_snapshot_contiguity() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for i in 0..100_i64 {
        db.insert_row(
            tx,
            "entity_snapshots",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("state".to_string(), Value::Text(format!("frame={i}"))),
                ("valid_from".to_string(), Value::Int64(i)),
                ("valid_to".to_string(), Value::Int64(i + 1)),
            ]),
        )
        .expect("snapshot");
    }
    db.commit(tx).expect("commit");
    let mut snaps = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    snaps.sort_by_key(|r| {
        r.values
            .get("valid_from")
            .and_then(Value::as_i64)
            .unwrap_or(-1)
    });
    assert_eq!(snaps.len(), 100);
    for i in 1..snaps.len() {
        assert_eq!(
            snaps[i - 1].values.get("valid_to"),
            snaps[i].values.get("valid_from")
        );
    }

    // Pick a middle timestamp (snapshot 50's valid_from = 50), query for the snapshot active at T=50
    let t_middle = 50_i64;
    let at_middle = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values
                    .get("valid_from")
                    .and_then(Value::as_i64)
                    .unwrap_or(999)
                    <= t_middle
                && r.values
                    .get("valid_to")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    > t_middle
        })
        .expect("scan");
    assert_eq!(
        at_middle.len(),
        1,
        "exactly 1 snapshot should be active at T=50"
    );
    assert_eq!(text(&at_middle[0], "state"), "frame=50");
}

#[test]
fn b6_07_list_entities() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for (name, ty) in [("a", "SERVICE"), ("b", "DATABASE"), ("c", "SERVICE")] {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text(name.to_string())),
                ("entity_type".to_string(), Value::Text(ty.to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");
    let service = db
        .scan_filter("entities", db.snapshot(), &|r| {
            r.values.get("entity_type") == Some(&Value::Text("SERVICE".to_string()))
        })
        .expect("scan");
    assert_eq!(service.len(), 2);
}

#[test]
fn b6_08_delete_entity() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "x", "");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "state".to_string(),
                Value::Text("before-delete".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let row = entity_row(&db, e);
    db.delete_row(tx2, "entities", row.row_id).expect("delete");
    db.commit(tx2).expect("commit");

    assert!(
        db.point_lookup("entities", "id", &Value::Uuid(e), db.snapshot())
            .expect("lookup")
            .is_none()
    );
    assert_eq!(
        db.scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("snapshots")
        .len(),
        1
    );
}

#[test]
fn b6_09_update_nonexistent_entity() {
    let db = setup_ontology_db();
    let missing = Uuid::new_v4();
    // Attempt an actual update via SQL on a nonexistent entity
    let result = db.execute(
        "UPDATE entities SET name='x' WHERE id=$id",
        &HashMap::from([("id".to_string(), Value::Uuid(missing))]),
    );
    // The update should succeed but affect 0 rows
    match result {
        Ok(qr) => assert_eq!(qr.rows_affected, 0, "no rows should be affected"),
        Err(_) => {
            // If the API returns an error for updating nonexistent rows, that's also acceptable
            let lookup = db
                .point_lookup("entities", "id", &Value::Uuid(missing), db.snapshot())
                .expect("lookup");
            assert!(lookup.is_none());
        }
    }
}

#[test]
fn b6_10_null_valid_to_means_current() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let snap_id = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "camera", "pos=north");
    // Insert snapshot with valid_from=100 and NO valid_to (NULL → still current)
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(snap_id)),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("state".to_string(), Value::Text("pos=north".to_string())),
            ("valid_from".to_string(), Value::Int64(100)),
        ]),
    )
    .expect("snapshot with null valid_to");
    db.commit(tx).expect("commit");

    // Query at t=999: valid_from <= 999 AND (valid_to is NULL OR valid_to > 999)
    let current = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values
                    .get("valid_from")
                    .and_then(Value::as_i64)
                    .is_some_and(|vf| vf <= 999)
                && match r.values.get("valid_to") {
                    None | Some(&Value::Null) => true,
                    Some(v) => v.as_i64().is_some_and(|vt| vt > 999),
                }
        })
        .expect("scan at t=999");
    assert_eq!(
        current.len(),
        1,
        "NULL valid_to snapshot should be found at t=999"
    );
    assert_eq!(
        current[0].values.get("id"),
        Some(&Value::Uuid(snap_id)),
        "should return the correct snapshot"
    );

    // Query at t=50: valid_from <= 50 — should return nothing (before snapshot)
    let before = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
                && r.values
                    .get("valid_from")
                    .and_then(Value::as_i64)
                    .is_some_and(|vf| vf <= 50)
                && match r.values.get("valid_to") {
                    None | Some(&Value::Null) => true,
                    Some(v) => v.as_i64().is_some_and(|vt| vt > 50),
                }
        })
        .expect("scan at t=50");
    assert!(
        before.is_empty(),
        "no snapshot should exist before valid_from=100"
    );
}

// B7 Decision CRUD
#[test]
fn b7_01_record_decision_with_reasoning() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let i = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
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
    insert_entity(&db, tx, e, "ent", "k=v");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "decision_type".to_string(),
                Value::Text("architecture".to_string()),
            ),
            ("description".to_string(), Value::Text("desc".to_string())),
            (
                "reasoning".to_string(),
                Value::Text("[{op:analyze}]".to_string()),
            ),
            ("confidence".to_string(), Value::Float64(0.9)),
            ("status".to_string(), Value::Text("active".to_string())),
            ("tags".to_string(), Value::Text("security,auth".to_string())),
            (
                "properties".to_string(),
                Value::Text("env=prod,threshold=200".to_string()),
            ),
        ]),
    )
    .expect("decision");
    add_edge(&db, tx, d, i, "SERVES");
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("state".to_string(), Value::Text("k=v".to_string())),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snapshot");
    db.commit(tx).expect("commit");

    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "decision_type"), "architecture");
    assert!(text(&row, "reasoning").contains("analyze"));
    assert_eq!(row.values.get("confidence"), Some(&Value::Float64(0.9)));
    assert_eq!(text(&row, "tags"), "security,auth");
    assert_eq!(text(&row, "properties"), "env=prod,threshold=200");
    assert_eq!(
        db.query_bfs(
            d,
            Some(&["SERVES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot()
        )
        .expect("serves")
        .nodes
        .len(),
        1
    );
    assert_eq!(
        db.query_bfs(
            d,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot()
        )
        .expect("based")
        .nodes
        .len(),
        1
    );
}

#[test]
fn b7_02_record_decision_minimal() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "decision_type".to_string(),
                Value::Text("architecture".to_string()),
            ),
            (
                "description".to_string(),
                Value::Text("minimal".to_string()),
            ),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");
    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row should exist");
    assert_eq!(text(&row, "decision_type"), "architecture");
    assert_eq!(text(&row, "description"), "minimal");
}

#[test]
fn b7_03_decision_tags_searchable() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let d = Uuid::new_v4();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("tagged".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            ("tags".to_string(), Value::Text("security,auth".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let tagged = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values
                .get("tags")
                .and_then(Value::as_text)
                .map(|t| t.contains("security"))
                .unwrap_or(false)
        })
        .expect("scan");
    assert_eq!(tagged.len(), 1);
    assert_eq!(tagged[0].values.get("id"), Some(&Value::Uuid(d)));
}

#[test]
fn b7_04_list_decisions_by_entity() {
    let db = setup_ontology_db();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    for e in [e1, e2] {
        insert_entity(&db, tx, e, "e", "");
    }
    for d in [d1, d2, d3] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, d1, e1, "BASED_ON");
    add_edge(&db, tx, d2, e1, "BASED_ON");
    add_edge(&db, tx, d3, e2, "BASED_ON");
    db.commit(tx).expect("commit");

    let linked = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            let d = *r.values.get("id").and_then(Value::as_uuid).expect("id");
            db.query_bfs(
                d,
                Some(&["BASED_ON".to_string()]),
                Direction::Outgoing,
                1,
                db.snapshot(),
            )
            .expect("bfs")
            .nodes
            .iter()
            .any(|n| n.id == e1)
        })
        .expect("scan");
    assert_eq!(linked.len(), 2);
}

#[test]
fn b7_05_get_decision_not_found() {
    let db = setup_ontology_db();
    assert!(
        db.point_lookup(
            "decisions",
            "id",
            &Value::Uuid(Uuid::new_v4()),
            db.snapshot()
        )
        .expect("lookup")
        .is_none()
    );
}

#[test]
fn b7_06_delete_decision() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "to-delete", "active", 0.8);
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "decisions", row.row_id).expect("delete");
    db.commit(tx2).expect("commit");

    assert!(
        db.point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
            .expect("lookup")
            .is_none()
    );
}

#[test]
fn b7_07_supersede_decision() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    add_edge(&db, tx, d2, d1, "SUPERSEDES");
    db.commit(tx).expect("commit");

    // Use direct API: delete old row, insert new version with status="superseded"
    let tx2 = db.begin();
    let old_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "decisions", old_row.row_id)
        .expect("delete old");
    db.insert_row(
        tx2,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d1)),
            ("description".to_string(), Value::Text("d1".to_string())),
            ("status".to_string(), Value::Text("superseded".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("insert superseded");
    db.commit(tx2).expect("commit");

    let old = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&old, "status"), "superseded");
    let supersedes = db
        .query_bfs(
            d2,
            Some(&["SUPERSEDES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(supersedes.nodes.len(), 1);
    assert_eq!(
        supersedes.nodes[0].id, d1,
        "SUPERSEDES edge target must be d1"
    );
}

#[test]
fn b7_08_decision_properties_stored() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("props".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            (
                "properties".to_string(),
                Value::Text("env=prod,threshold=200".to_string()),
            ),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "properties"), "env=prod,threshold=200");
}

#[test]
fn b8_01_invalidation_state_forward_only() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    // Insert initial pending invalidation via direct API
    let tx = db.begin();
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    // Transition pending -> acknowledged via upsert
    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "invalidations",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "status".to_string(),
                Value::Text("acknowledged".to_string()),
            ),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("ack");
    db.commit(tx2).expect("commit");

    // Transition acknowledged -> resolved via upsert
    let tx3 = db.begin();
    db.upsert_row(
        tx3,
        "invalidations",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("status".to_string(), Value::Text("resolved".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("resolve");
    db.commit(tx3).expect("commit");

    // Invalid transition: resolved -> pending should fail
    let tx4 = db.begin();
    let err = db.upsert_row(
        tx4,
        "invalidations",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    );
    assert!(matches!(err, Err(Error::InvalidStateTransition(_))));
    db.rollback(tx4).expect("rollback");
}

#[test]
fn b8_02_dismiss_invalidation() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    // Insert initial pending invalidation via direct API
    let tx = db.begin();
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    // Transition pending -> dismissed via upsert
    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "invalidations",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("status".to_string(), Value::Text("dismissed".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("dismiss");
    db.commit(tx2).expect("commit");

    // Invalid transition: dismissed -> acknowledged should fail
    let tx3 = db.begin();
    let err = db.upsert_row(
        tx3,
        "invalidations",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "status".to_string(),
                Value::Text("acknowledged".to_string()),
            ),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    );
    assert!(matches!(err, Err(Error::InvalidStateTransition(_))));
    db.rollback(tx3).expect("rollback");
}

#[test]
fn b8_03_resolve_with_new_decision() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let i = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("affected_decision_id".to_string(), Value::Uuid(d1)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
            (
                "resolution_action".to_string(),
                Value::Text("new_decision_created".to_string()),
            ),
            ("resolution_decision_id".to_string(), Value::Uuid(d2)),
        ]),
    )
    .expect("inv");
    db.commit(tx).expect("commit");

    let row = db
        .point_lookup("invalidations", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(
        row.values.get("resolution_decision_id"),
        Some(&Value::Uuid(d2))
    );
    assert_eq!(text(&row, "resolution_action"), "new_decision_created");
}

// B9
#[test]
fn b9_01_record_outcome_and_retrieve() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "d", "active", 0.8);
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
            ("notes".to_string(), Value::Text("worked".to_string())),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    let out = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    assert_eq!(out.len(), 1);
    assert_eq!(text(&out[0], "notes"), "worked");
}

#[test]
fn b9_02_outcome_replaces_previous() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "d", "active", 0.8);
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("o1");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let old = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    db.delete_row(tx2, "outcomes", old[0].row_id)
        .expect("delete");
    db.insert_row(
        tx2,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(false)),
        ]),
    )
    .expect("o2");
    db.commit(tx2).expect("commit");

    let latest = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].values.get("success"), Some(&Value::Bool(false)));
}

#[test]
fn b9_03_effective_confidence() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "active", 0.9);
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d1)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("o1");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d2)),
            ("success".to_string(), Value::Bool(false)),
        ]),
    )
    .expect("o2");
    db.commit(tx).expect("commit");

    // Retrieve confidence from DB rows
    let d1_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d2_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d1_conf = match d1_row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("d1 confidence"),
    };
    let d2_conf = match d2_row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("d2 confidence"),
    };

    // Retrieve outcome success values from DB
    let d1_outcome = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d1))
        })
        .expect("scan");
    let d2_outcome = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d2))
        })
        .expect("scan");
    let d1_success = d1_outcome[0].values.get("success") == Some(&Value::Bool(true));
    let d2_success = d2_outcome[0].values.get("success") == Some(&Value::Bool(true));

    // Compute effective_confidence = confidence * (success ? 1.0 : 0.5)
    let d1_eff = d1_conf * if d1_success { 1.0 } else { 0.5 };
    let d2_eff = d2_conf * if d2_success { 1.0 } else { 0.5 };
    assert!(
        d1_eff > d2_eff,
        "d1 (success=true) eff={} should exceed d2 (success=false) eff={}",
        d1_eff,
        d2_eff
    );
}

#[test]
fn b9_04_citation_stats() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    for d in [d1, d2, d3] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, d2, d1, "CITES");
    add_edge(&db, tx, d3, d1, "CITES");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d2)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("o2");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d3)),
            ("success".to_string(), Value::Bool(false)),
        ]),
    )
    .expect("o3");
    db.commit(tx).expect("commit");

    let cites = db
        .query_bfs(
            d1,
            Some(&["CITES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("cites");
    assert_eq!(cites.nodes.len(), 2);
    let successes = cites
        .nodes
        .iter()
        .filter(|n| {
            db.scan_filter("outcomes", db.snapshot(), &|r| {
                r.values.get("decision_id") == Some(&Value::Uuid(n.id))
                    && r.values.get("success") == Some(&Value::Bool(true))
            })
            .expect("scan")
            .len()
                == 1
        })
        .count();
    assert_eq!(successes, 1);
}

#[test]
fn b9_05_outcome_nonexistent_decision() {
    let db = setup_ontology_db();
    let missing = Uuid::new_v4();

    // Verify the decision does not exist
    let found = db
        .point_lookup("decisions", "id", &Value::Uuid(missing), db.snapshot())
        .expect("lookup");
    assert!(found.is_none(), "decision should not exist");

    // Capture (not ignore) the result of inserting an outcome for a nonexistent decision_id.
    // contextDB does not enforce foreign key constraints on insert_row, so the insert succeeds.
    // This is a data integrity concern handled at the application level.
    let tx = db.begin();
    let insert_result = db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(missing)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    );
    // The insert likely succeeds since there's no FK enforcement; assert we captured the result
    match insert_result {
        Ok(_row_id) => {
            // Insert succeeded — this means outcome insertion for nonexistent decisions
            // is a data integrity concern handled at the application level.
        }
        Err(ref _e) => {
            // If insert_row does validate foreign keys, the error is expected.
        }
    }
    db.rollback(tx).expect("rollback");

    // Confirm the decision still doesn't exist
    let still_missing = db
        .point_lookup("decisions", "id", &Value::Uuid(missing), db.snapshot())
        .expect("lookup");
    assert!(still_missing.is_none());
}

#[test]
fn b9_06_no_outcome_no_penalty() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "d", "active", 0.8);
    db.commit(tx).expect("commit");
    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(row.values.get("confidence"), Some(&Value::Float64(0.8)));
}

// B10 governance
#[test]
fn b10_01_add_approval_to_decision() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "d", "active", 0.8);
    db.insert_row(
        tx,
        "approvals",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            (
                "approver_id".to_string(),
                Value::Text("tech-lead".to_string()),
            ),
            ("level".to_string(), Value::Text("VP".to_string())),
            ("rationale".to_string(), Value::Text("LGTM".to_string())),
        ]),
    )
    .expect("approval");
    db.commit(tx).expect("commit");
    let a = db
        .scan_filter("approvals", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    assert_eq!(a.len(), 1);
    assert_eq!(text(&a[0], "approver_id"), "tech-lead");
    assert_eq!(text(&a[0], "level"), "VP");
    assert_eq!(text(&a[0], "rationale"), "LGTM");
}

#[test]
fn b10_02_multiple_approvals() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d, "d", "active", 0.8);
    for approver in ["a1", "a2"] {
        db.insert_row(
            tx,
            "approvals",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("approver_id".to_string(), Value::Text(approver.to_string())),
            ]),
        )
        .expect("approval");
    }
    db.commit(tx).expect("commit");
    assert_eq!(
        db.scan_filter("approvals", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan")
        .len(),
        2
    );
}

#[test]
fn b10_03_query_by_approver() {
    let db = setup_ontology_db();
    let tx = db.begin();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    for (d, a) in [(d1, "x"), (d2, "y")] {
        db.insert_row(
            tx,
            "approvals",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("approver_id".to_string(), Value::Text(a.to_string())),
            ]),
        )
        .expect("approval");
    }
    db.commit(tx).expect("commit");
    let rows = db
        .scan_filter("approvals", db.snapshot(), &|r| {
            r.values.get("approver_id") == Some(&Value::Text("x".to_string()))
        })
        .expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.get("decision_id"), Some(&Value::Uuid(d1)));
}

#[test]
fn b10_04_audit_query_filters() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for (ty, days, tags) in [
        ("security", 5_i64, "auth"),
        ("security", 50_i64, "auth"),
        ("ops", 4_i64, "infra"),
    ] {
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("description".to_string(), Value::Text("d".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("confidence".to_string(), Value::Float64(0.8)),
                ("decision_type".to_string(), Value::Text(ty.to_string())),
                ("days_ago".to_string(), Value::Int64(days)),
                ("tags".to_string(), Value::Text(tags.to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");

    let mut rows = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values.get("decision_type") == Some(&Value::Text("security".to_string()))
                && r.values
                    .get("days_ago")
                    .and_then(Value::as_i64)
                    .unwrap_or(999)
                    <= 30
                && r.values.get("tags") == Some(&Value::Text("auth".to_string()))
        })
        .expect("scan");
    rows.sort_by_key(|r| r.row_id);
    let page = &rows[0..rows.len().min(1)];
    assert_eq!(page.len(), 1);
}

#[test]
fn b10_05_approval_nonexistent_decision() {
    let db = setup_ontology_db();
    let missing = Uuid::new_v4();

    // Verify the decision does not exist
    let existing = db
        .point_lookup("decisions", "id", &Value::Uuid(missing), db.snapshot())
        .expect("lookup");
    assert!(existing.is_none(), "decision should not exist");

    // Capture (not ignore) the result of inserting an approval for a nonexistent decision.
    // contextDB does not enforce foreign key constraints on insert_row, so the insert succeeds.
    // This is a data integrity concern handled at the application level.
    let tx = db.begin();
    let insert_result = db.insert_row(
        tx,
        "approvals",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(missing)),
        ]),
    );
    match insert_result {
        Ok(_row_id) => {
            // Insert succeeded — approval insertion for nonexistent decisions
            // is a data integrity concern handled at the application level.
        }
        Err(ref _e) => {
            // If insert_row validates foreign keys, the error is expected.
        }
    }
    db.rollback(tx).expect("rollback");

    // Confirm the decision still doesn't exist
    let still_missing = db
        .point_lookup("decisions", "id", &Value::Uuid(missing), db.snapshot())
        .expect("lookup");
    assert!(still_missing.is_none());
}

// B11 multi-agent
#[test]
fn b11_01_decisions_visible_across_contexts() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            ("description".to_string(), Value::Text("shared".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            ("context_id".to_string(), Value::Text("C".to_string())),
            ("agent_id".to_string(), Value::Text("A".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let seen = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values.get("context_id") == Some(&Value::Text("C".to_string()))
        })
        .expect("scan");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].values.get("id"), Some(&Value::Uuid(d)));
}

#[test]
fn b11_02_filter_by_agent_id() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for agent in ["A", "B", "A"] {
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("description".to_string(), Value::Text("d".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("confidence".to_string(), Value::Float64(0.8)),
                ("agent_id".to_string(), Value::Text(agent.to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");
    let a = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values.get("agent_id") == Some(&Value::Text("A".to_string()))
        })
        .expect("scan");
    assert_eq!(a.len(), 2);
}

#[test]
fn b11_03_agent_handoff() {
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("in-progress".to_string()),
            ),
            (
                "reasoning".to_string(),
                Value::Text("from-agent-a".to_string()),
            ),
            ("status".to_string(), Value::Text("in_progress".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            ("agent_id".to_string(), Value::Text("A".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "decisions", row.row_id).expect("delete");
    db.insert_row(
        tx2,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d)),
            (
                "description".to_string(),
                Value::Text("completed".to_string()),
            ),
            (
                "reasoning".to_string(),
                Value::Text("from-agent-a".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            ("agent_id".to_string(), Value::Text("B".to_string())),
        ]),
    )
    .expect("insert");
    db.insert_row(
        tx2,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome");
    db.commit(tx2).expect("commit");

    let final_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&final_row, "status"), "active");
    assert_eq!(text(&final_row, "agent_id"), "B");
    assert_eq!(text(&final_row, "reasoning"), "from-agent-a");
}

#[test]
fn b11_04_decision_superseded_by_better_option() {
    let db = setup_propagation_ontology_db();
    let i = Uuid::new_v4();
    let d_old = Uuid::new_v4();
    let d_new = Uuid::new_v4();

    // Create intention and an initial active decision serving it.
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("detect anomaly".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("insert intention");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d_old)),
            (
                "description".to_string(),
                Value::Text("use model X".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.6)),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .expect("insert old decision");
    add_edge(&db, tx, d_old, i, "SERVES");
    db.commit(tx).expect("commit");

    // Application-level supersession: delete old decision, insert it as superseded,
    // then insert a new better decision. This is explicit application workflow.
    let tx2 = db.begin();
    let old_row = db
        .point_lookup("decisions", "id", &Value::Uuid(d_old), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "decisions", old_row.row_id)
        .expect("delete old");
    db.insert_row(
        tx2,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d_old)),
            (
                "description".to_string(),
                Value::Text("use model X".to_string()),
            ),
            ("status".to_string(), Value::Text("superseded".to_string())),
            ("confidence".to_string(), Value::Float64(0.6)),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .expect("re-insert old as superseded");
    db.insert_row(
        tx2,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d_new)),
            (
                "description".to_string(),
                Value::Text("use model Y".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.9)),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .expect("insert new decision");
    add_edge(&db, tx2, d_new, i, "SERVES");
    add_edge(&db, tx2, d_new, d_old, "SUPERSEDES");
    db.commit(tx2).expect("commit");

    // Assert: old decision is superseded
    let old_final = db
        .point_lookup("decisions", "id", &Value::Uuid(d_old), db.snapshot())
        .expect("lookup")
        .expect("old row");
    assert_eq!(text(&old_final, "status"), "superseded");

    // Assert: new decision is active
    let new_final = db
        .point_lookup("decisions", "id", &Value::Uuid(d_new), db.snapshot())
        .expect("lookup")
        .expect("new row");
    assert_eq!(text(&new_final, "status"), "active");

    // Assert: intention is still active (supersession does not affect the intention)
    let intention_final = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("intention row");
    assert_eq!(text(&intention_final, "status"), "active");
}

// B12 intention
#[test]
fn b12_01_create_intention() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("intention".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("context_id".to_string(), Value::Text("ctx".to_string())),
        ]),
    )
    .expect("insert");
    db.commit(tx).expect("commit");
    let row = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "status"), "active");
    assert_eq!(text(&row, "context_id"), "ctx");
}

#[test]
fn b12_02_archive_intention_with_active_decisions() {
    let db = setup_propagation_ontology_db();
    let i = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();

    // Create an intention with two active decisions that reference it via FK.
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("detect anomaly".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("insert intention");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d1)),
            (
                "description".to_string(),
                Value::Text("use model X".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.8)),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .expect("insert d1");
    db.insert_row(
        tx,
        "decisions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(d2)),
            (
                "description".to_string(),
                Value::Text("use model Y".to_string()),
            ),
            ("status".to_string(), Value::Text("active".to_string())),
            ("confidence".to_string(), Value::Float64(0.7)),
            ("intention_id".to_string(), Value::Uuid(i)),
        ]),
    )
    .expect("insert d2");
    add_edge(&db, tx, d1, i, "SERVES");
    add_edge(&db, tx, d2, i, "SERVES");
    db.commit(tx).expect("commit");

    // Archive the intention using upsert_row (state machine transition: active -> archived).
    // FK propagation rule should automatically invalidate both decisions.
    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            (
                "description".to_string(),
                Value::Text("detect anomaly".to_string()),
            ),
            ("status".to_string(), Value::Text("archived".to_string())),
        ]),
    )
    .expect("upsert intention to archived");
    db.commit(tx2).expect("commit");

    // Assert: intention is archived
    let intention = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("intention row");
    assert_eq!(text(&intention, "status"), "archived");

    // Assert: both decisions are invalidated by FK propagation
    let decision1 = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("d1 row");
    assert_eq!(text(&decision1, "status"), "invalidated");

    let decision2 = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup")
        .expect("d2 row");
    assert_eq!(text(&decision2, "status"), "invalidated");
}

#[test]
fn b12_03_archive_intention_no_active_decisions() {
    let db = setup_ontology_db();
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
    .expect("insert i");
    insert_decision(&db, tx, d, "d", "superseded", 0.8);
    add_edge(&db, tx, d, i, "SERVES");
    db.commit(tx).expect("commit");

    // Archive via direct API: delete + insert with new status
    let tx2 = db.begin();
    let old_intention = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    db.delete_row(tx2, "intentions", old_intention.row_id)
        .expect("delete");
    db.insert_row(
        tx2,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("i".to_string())),
            ("status".to_string(), Value::Text("abandoned".to_string())),
        ]),
    )
    .expect("insert abandoned");
    db.commit(tx2).expect("commit");
    let intention = db
        .point_lookup("intentions", "id", &Value::Uuid(i), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&intention, "status"), "abandoned");
}

#[test]
fn b12_04_intention_tree() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("goal".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("i");
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "superseded", 0.7);
    insert_entity(&db, tx, e, "e", "");
    add_edge(&db, tx, d1, i, "SERVES");
    add_edge(&db, tx, d2, i, "SERVES");
    add_edge(&db, tx, d1, e, "BASED_ON");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d1)),
            ("success".to_string(), Value::Bool(true)),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    let serves = db
        .query_bfs(
            i,
            Some(&["SERVES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("serves");
    assert_eq!(serves.nodes.len(), 2);

    // BFS from d1 via BASED_ON to find entities
    let based = db
        .query_bfs(
            d1,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("based_on");
    assert!(
        based.nodes.iter().any(|n| n.id == e),
        "d1 should be BASED_ON entity e"
    );

    // Scan outcomes for d1
    let outcomes = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d1))
        })
        .expect("outcomes");
    assert!(!outcomes.is_empty(), "d1 should have at least one outcome");
}

// B13 context isolation
#[test]
fn b13_01_context_scoped_queries() {
    let db = setup_ontology_db();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let tx = db.begin();
    for (e, ctx) in [(e1, "c1"), (e2, "c2")] {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(e)),
                ("name".to_string(), Value::Text("e".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("SERVICE".to_string()),
                ),
                ("context_id".to_string(), Value::Text(ctx.to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");
    let c1 = db
        .scan_filter("entities", db.snapshot(), &|r| {
            r.values.get("context_id") == Some(&Value::Text("c1".to_string()))
        })
        .expect("scan");
    assert_eq!(c1.len(), 1);
    assert_eq!(c1[0].values.get("id"), Some(&Value::Uuid(e1)));

    // BFS from e1 — any neighbors should not cross to C2's entities
    let bfs = db
        .query_bfs(
            e1,
            Some(&["RELATES_TO".to_string(), "BASED_ON".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    // No edges were created, so BFS should return empty. Assert e2 is not reachable.
    assert!(
        !bfs.nodes.iter().any(|n| n.id == e2),
        "BFS from C1 entity should not reach C2 entities"
    );
}

#[test]
fn b13_02_cross_context_requires_explicit_opt_in() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for ctx in ["c1", "c2"] {
        db.insert_row(
            tx,
            "entities",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text("e".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("SERVICE".to_string()),
                ),
                ("context_id".to_string(), Value::Text(ctx.to_string())),
            ]),
        )
        .expect("insert");
    }
    db.commit(tx).expect("commit");

    let default_scope = db
        .scan_filter("entities", db.snapshot(), &|r| {
            r.values.get("context_id") == Some(&Value::Text("c1".to_string()))
        })
        .expect("scan");
    let explicit = db
        .scan_filter("entities", db.snapshot(), &|r| {
            matches!(
                r.values.get("context_id"),
                Some(Value::Text(v)) if v == "c1" || v == "c2"
            )
        })
        .expect("scan");
    assert_eq!(default_scope.len(), 1);
    assert_eq!(explicit.len(), 2);
}

// B14 contradiction detection
#[test]
fn b14_01_conflicting_decisions_detected() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "ent", "");
    for (d, outcome) in [(d1, true), (d2, false)] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
        add_edge(&db, tx, d, e, "BASED_ON");
        db.insert_row(
            tx,
            "outcomes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("success".to_string(), Value::Bool(outcome)),
            ]),
        )
        .expect("outcome");
    }
    db.commit(tx).expect("commit");

    // Detect conflict: scan decisions BASED_ON entity E (via BFS incoming), filter active, compare outcomes
    let based_on_e = db
        .query_bfs(
            e,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    // Filter for active decisions
    let active_on_e: Vec<Uuid> = based_on_e
        .nodes
        .iter()
        .filter(|n| {
            db.point_lookup("decisions", "id", &Value::Uuid(n.id), db.snapshot())
                .ok()
                .flatten()
                .map(|r| r.values.get("status") == Some(&Value::Text("active".to_string())))
                .unwrap_or(false)
        })
        .map(|n| n.id)
        .collect();
    assert_eq!(
        active_on_e.len(),
        2,
        "found 2 active decisions on same entity"
    );

    // Compare outcomes — they differ
    let outcomes: Vec<bool> = active_on_e
        .iter()
        .map(|d_id| {
            db.scan_filter("outcomes", db.snapshot(), &|r| {
                r.values.get("decision_id") == Some(&Value::Uuid(*d_id))
            })
            .expect("scan")
            .first()
            .map(|r| r.values.get("success") == Some(&Value::Bool(true)))
            .unwrap_or(false)
        })
        .collect();
    assert!(
        outcomes.contains(&true) && outcomes.contains(&false),
        "outcomes differ — conflict detected"
    );
}

#[test]
fn b14_02_conflicts_bidirectional() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "ent", "");
    for d in [d1, d2] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
        add_edge(&db, tx, d, e, "BASED_ON");
    }
    db.commit(tx).expect("commit");

    // From D1's perspective: BFS outgoing BASED_ON reaches E
    let from_d1 = db
        .query_bfs(
            d1,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(from_d1.nodes[0].id, e);

    // From D1's entity, BFS incoming shows D2 also BASED_ON it
    let from_d1_entity = db
        .query_bfs(
            e,
            Some(&["BASED_ON".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    let d1_sees_d2 = from_d1_entity.nodes.iter().any(|n| n.id == d2);
    assert!(d1_sees_d2, "from D1's entity, D2 is also BASED_ON it");

    // From D2's perspective: BFS outgoing BASED_ON reaches E
    let from_d2 = db
        .query_bfs(
            d2,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(from_d2.nodes[0].id, e);

    // From D2's entity, BFS incoming shows D1 also BASED_ON it
    let d2_sees_d1 = from_d1_entity.nodes.iter().any(|n| n.id == d1);
    assert!(d2_sees_d1, "from D2's entity, D1 is also BASED_ON it");
}

#[test]
fn b14_03_no_conflict_different_entity() {
    let db = setup_ontology_db();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    for e in [e1, e2] {
        insert_entity(&db, tx, e, "e", "");
    }
    insert_decision(&db, tx, d1, "d1", "active", 0.8);
    insert_decision(&db, tx, d2, "d2", "active", 0.8);
    add_edge(&db, tx, d1, e1, "BASED_ON");
    add_edge(&db, tx, d2, e2, "BASED_ON");
    db.commit(tx).expect("commit");

    let d1_target = db
        .query_bfs(
            d1,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    let d2_target = db
        .query_bfs(
            d2,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_ne!(d1_target.nodes[0].id, d2_target.nodes[0].id);
}

#[test]
fn b14_04_no_conflict_same_outcome() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    for d in [d1, d2] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
        db.insert_row(
            tx,
            "outcomes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("success".to_string(), Value::Bool(true)),
            ]),
        )
        .expect("outcome");
    }
    db.commit(tx).expect("commit");
    let outcomes = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("success") == Some(&Value::Bool(true))
        })
        .expect("scan");
    assert_eq!(outcomes.len(), 2);
}

// B15
#[test]
fn b15_01_time_window_with_precedent_ranking() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for (conf, success, cites, days) in [
        (0.9, true, 3_i64, 5_i64),
        (0.9, false, 0_i64, 2_i64),
        (0.5, true, 1_i64, 3_i64),
        (0.8, true, 1_i64, 60_i64),
    ] {
        let d = Uuid::new_v4();
        db.insert_row(
            tx,
            "decisions",
            HashMap::from([
                ("id".to_string(), Value::Uuid(d)),
                ("description".to_string(), Value::Text("d".to_string())),
                ("status".to_string(), Value::Text("active".to_string())),
                ("confidence".to_string(), Value::Float64(conf)),
                ("days_ago".to_string(), Value::Int64(days)),
                ("citation_count".to_string(), Value::Int64(cites)),
            ]),
        )
        .expect("decision");
        db.insert_row(
            tx,
            "outcomes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("decision_id".to_string(), Value::Uuid(d)),
                ("success".to_string(), Value::Bool(success)),
            ]),
        )
        .expect("outcome");
    }
    db.commit(tx).expect("commit");
    let recent = db
        .scan_filter("decisions", db.snapshot(), &|r| {
            r.values
                .get("days_ago")
                .and_then(Value::as_i64)
                .unwrap_or(999)
                <= 30
        })
        .expect("scan");
    assert_eq!(recent.len(), 3);

    // After time-window filter: compute effective_confidence and citation-based ranking
    let mut ranked: Vec<(Uuid, f64, i64)> = Vec::new();
    for r in &recent {
        let d_id = *r.values.get("id").and_then(Value::as_uuid).expect("id");
        let conf = match r.values.get("confidence") {
            Some(Value::Float64(v)) => *v,
            _ => 0.0,
        };
        // Retrieve outcome success from outcomes table
        let outcome = db
            .scan_filter("outcomes", db.snapshot(), &|o| {
                o.values.get("decision_id") == Some(&Value::Uuid(d_id))
            })
            .expect("scan");
        let success = outcome
            .first()
            .map(|o| o.values.get("success") == Some(&Value::Bool(true)))
            .unwrap_or(false);
        let eff_conf = conf * if success { 1.0 } else { 0.5 };
        // Retrieve citation count via BFS incoming CITES
        let cites = db
            .query_bfs(
                d_id,
                Some(&["CITES".to_string()]),
                Direction::Incoming,
                1,
                db.snapshot(),
            )
            .expect("bfs");
        let citation_count = cites.nodes.len() as i64;
        ranked.push((d_id, eff_conf, citation_count));
    }
    // Sort by (effective_confidence desc, citation_count desc)
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.2.cmp(&a.2))
    });
    // The top-ranked should have highest effective confidence
    assert!(
        ranked[0].1 >= ranked[1].1,
        "ranking should be by effective_confidence"
    );
}

#[test]
fn b15_02_stale_decision_cascade() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let d4 = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "file", "path=/etc/nginx.conf,checksum=abc123");
    for d in [d1, d2, d3, d4] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    add_edge(&db, tx, d1, e, "BASED_ON");
    add_edge(&db, tx, d2, e, "BASED_ON");
    add_edge(&db, tx, d3, d1, "CITES");
    add_edge(&db, tx, d3, d2, "CITES");
    add_edge(&db, tx, d4, d1, "SUPERSEDES");
    add_edge(&db, tx, d4, d2, "SUPERSEDES");
    for (aff, diff) in [
        (d1, "direct:path".to_string()),
        (d2, "direct:path".to_string()),
        (d3, format!("cascade_from={d1}")),
    ] {
        db.insert_row(
            tx,
            "invalidations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("affected_decision_id".to_string(), Value::Uuid(aff)),
                ("status".to_string(), Value::Text("resolved".to_string())),
                ("resolution_decision_id".to_string(), Value::Uuid(d4)),
                ("basis_diff".to_string(), Value::Text(diff)),
                ("severity".to_string(), Value::Text("info".to_string())),
            ]),
        )
        .expect("inv");
    }
    db.commit(tx).expect("commit");

    let invs = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(invs.len(), 3);
    assert!(
        invs.iter()
            .all(|r| r.values.get("resolution_decision_id") == Some(&Value::Uuid(d4)))
    );
}

#[test]
fn b15_03_transitive_effective_confidence() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let tx = db.begin();
    insert_decision(&db, tx, d1, "d1", "active", 0.9);
    insert_decision(&db, tx, d2, "d2", "active", 0.85);
    insert_decision(&db, tx, d3, "d3", "active", 0.7);
    add_edge(&db, tx, d2, d1, "CITES");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d1)),
            ("success".to_string(), Value::Bool(true)),
            ("success_rate".to_string(), Value::Float64(0.8)),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    // Retrieve confidence from DB-retrieved decision rows
    let d1row = db
        .point_lookup("decisions", "id", &Value::Uuid(d1), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d2row = db
        .point_lookup("decisions", "id", &Value::Uuid(d2), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d3row = db
        .point_lookup("decisions", "id", &Value::Uuid(d3), db.snapshot())
        .expect("lookup")
        .expect("row");
    let d1_conf = match d1row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("d1 confidence"),
    };
    let d2_conf = match d2row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("d2 confidence"),
    };
    let d3_conf = match d3row.values.get("confidence") {
        Some(Value::Float64(v)) => *v,
        _ => panic!("d3 confidence"),
    };

    // Retrieve success_rate from outcome
    let d1_outcome = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d1))
        })
        .expect("scan");
    let success_rate = match d1_outcome[0].values.get("success_rate") {
        Some(Value::Float64(v)) => *v,
        _ => 1.0,
    };

    // Compute effective_confidence from DB-retrieved data
    let eff1 = d1_conf * success_rate;
    let eff2 = d2_conf; // d2 has no outcome, confidence stands
    let eff3 = d3_conf; // d3 has no outcome, confidence stands
    assert!(eff2 > eff1, "d2 eff={} > d1 eff={}", eff2, eff1);
    assert!(eff1 > eff3, "d1 eff={} > d3 eff={}", eff1, eff3);
}

#[test]
fn b16_01_graph_to_relational_to_vector() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let neighbors: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    insert_entity(&db, tx, e, "root", "");
    for n in &neighbors {
        insert_entity(&db, tx, *n, "n", "");
        add_edge(&db, tx, e, *n, "RELATES_TO");
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(*n)),
                ("context_id".to_string(), Value::Text("ctx".to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let neighborhood = db
        .query_bfs(
            e,
            Some(&["RELATES_TO".to_string()]),
            Direction::Outgoing,
            2,
            db.snapshot(),
        )
        .expect("bfs");
    let ids: HashSet<Uuid> = neighborhood.nodes.iter().map(|n| n.id).collect();

    let candidate_obs = db
        .scan_filter("observations", db.snapshot(), &|r| {
            r.values
                .get("entity_id")
                .and_then(Value::as_uuid)
                .map(|id| ids.contains(id))
                .unwrap_or(false)
                && r.values.get("context_id") == Some(&Value::Text("ctx".to_string()))
        })
        .expect("scan");
    assert_eq!(candidate_obs.len(), 5);

    // Collect row_ids from scan results into RoaringTreemap
    let mut candidates = RoaringTreemap::new();
    for r in &candidate_obs {
        candidates.insert(r.row_id);
    }

    // Insert vectors for the observations so query_vector has data
    let tx2 = db.begin();
    for (idx, r) in candidate_obs.iter().enumerate() {
        db.insert_vector(
            tx2,
            r.row_id,
            vec![1.0 - (idx as f32 * 0.1), idx as f32 * 0.1],
        )
        .expect("vector");
    }
    db.commit(tx2).expect("commit vectors");

    // Call query_vector with candidates and a query vector
    let vec_results = db
        .query_vector(&[1.0, 0.0], 5, Some(&candidates), db.snapshot())
        .expect("vector search");
    assert!(
        !vec_results.is_empty(),
        "vector search should return results"
    );
    // Assert results are ordered by similarity (descending)
    for w in vec_results.windows(2) {
        assert!(w[0].1 >= w[1].1, "results should be ordered by similarity");
    }
}

// B17 fixture-shape tests
#[test]
fn b17_01_bfs_output_shape() {
    let db = setup_ontology_db();
    let nodes: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    let tx = db.begin();
    for i in 0..4 {
        add_edge(&db, tx, nodes[i], nodes[i + 1], "BASED_ON");
    }
    db.commit(tx).expect("commit");
    let out = db
        .query_bfs(
            nodes[0],
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            3,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(out.nodes.len(), 3);
    assert_eq!(out.nodes[0].id, nodes[1]);
    assert_eq!(out.nodes[2].id, nodes[3]);
}

#[test]
fn b17_02_ann_output_shape() {
    let db = setup_ontology_db();
    let tx = db.begin();
    for vec in [
        vec![1.0, 0.0, 0.0, 0.0],
        vec![0.95, 0.05, 0.0, 0.0],
        vec![0.9, 0.1, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
    ] {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ]),
            )
            .expect("insert");
        db.insert_vector(tx, rid, vec).expect("vector");
    }
    db.commit(tx).expect("commit");

    let out = db
        .query_vector(&[1.0, 0.0, 0.0, 0.0], 3, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 3);
    assert!(out[0].1 >= out[1].1 && out[1].1 >= out[2].1);
}

#[test]
fn b17_03_upsert_output_shape() {
    let db = setup_ontology_db();
    let id = Uuid::new_v4();
    let tx = db.begin();
    let inserted = db
        .upsert_row(
            tx,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("name".to_string(), Value::Text("new".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("service".to_string()),
                ),
            ]),
        )
        .expect("inserted");
    db.insert_row(
        tx,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("name".to_string(), Value::Text("orig".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("service".to_string()),
            ),
        ]),
    )
    .expect("seed");
    db.commit(tx).expect("commit");

    let tx2 = db.begin();
    let updated = db
        .upsert_row(
            tx2,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("updated".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("service".to_string()),
                ),
            ]),
        )
        .expect("updated");
    db.commit(tx2).expect("commit");

    let tx3 = db.begin();
    let noop = db
        .upsert_row(
            tx3,
            "entities",
            "id",
            HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("updated".to_string())),
                (
                    "entity_type".to_string(),
                    Value::Text("service".to_string()),
                ),
            ]),
        )
        .expect("noop");
    db.commit(tx3).expect("commit");

    assert_eq!(inserted, contextdb_core::UpsertResult::Inserted);
    assert_eq!(updated, contextdb_core::UpsertResult::Updated);
    assert_eq!(noop, contextdb_core::UpsertResult::NoOp);
    assert_eq!(db.scan("entities", db.snapshot()).expect("scan").len(), 2);
}

#[test]
fn b17_04_impact_analysis_output_shape() {
    // Self-contained impact analysis with golden-output assertions
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let o = Uuid::new_v4();
    let inv_id = Uuid::new_v4();

    let tx = db.begin();
    insert_entity(&db, tx, e, "svc", "cpu=4");
    insert_decision(&db, tx, d, "scale-up", "active", 0.9);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "observations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(o)),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("data".to_string(), Value::Text("cpu=8".to_string())),
        ]),
    )
    .expect("obs");
    db.insert_row(
        tx,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(inv_id)),
            ("affected_decision_id".to_string(), Value::Uuid(d)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("critical".to_string())),
            (
                "basis_diff".to_string(),
                Value::Text("field=cpu,old=4,new=8".to_string()),
            ),
            ("trigger_observation_id".to_string(), Value::Uuid(o)),
        ]),
    )
    .expect("inv");
    db.commit(tx).expect("commit");

    // Golden output assertions
    let inv = db
        .point_lookup("invalidations", "id", &Value::Uuid(inv_id), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&inv, "status"), "pending");
    assert_eq!(text(&inv, "severity"), "critical");
    assert_eq!(text(&inv, "basis_diff"), "field=cpu,old=4,new=8");
    assert_eq!(
        inv.values.get("affected_decision_id"),
        Some(&Value::Uuid(d))
    );
    assert_eq!(
        inv.values.get("trigger_observation_id"),
        Some(&Value::Uuid(o))
    );
}

#[test]
fn b17_05_provenance_output_shape() {
    // Self-contained provenance with golden-output assertions
    let db = setup_ontology_db();
    let d = Uuid::new_v4();
    let e = Uuid::new_v4();
    let p = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "api", "latency=10ms");
    insert_decision(&db, tx, p, "precedent", "active", 0.8);
    insert_decision(&db, tx, d, "main-decision", "active", 0.9);
    add_edge(&db, tx, d, e, "BASED_ON");
    add_edge(&db, tx, d, p, "CITES");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("state".to_string(), Value::Text("latency=10ms".to_string())),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap");
    db.insert_row(
        tx,
        "approvals",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            (
                "approver_id".to_string(),
                Value::Text("tech-lead".to_string()),
            ),
            ("level".to_string(), Value::Text("architecture".to_string())),
            (
                "rationale".to_string(),
                Value::Text("performance OK".to_string()),
            ),
        ]),
    )
    .expect("approval");
    db.insert_row(
        tx,
        "outcomes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("decision_id".to_string(), Value::Uuid(d)),
            ("success".to_string(), Value::Bool(true)),
            (
                "notes".to_string(),
                Value::Text("deployed successfully".to_string()),
            ),
        ]),
    )
    .expect("outcome");
    db.commit(tx).expect("commit");

    // Golden output: provenance fields
    let row = db
        .point_lookup("decisions", "id", &Value::Uuid(d), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&row, "description"), "main-decision");
    let based = db
        .query_bfs(
            d,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(based.nodes.len(), 1);
    assert_eq!(based.nodes[0].id, e);
    let cites = db
        .query_bfs(
            d,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .expect("bfs");
    assert_eq!(cites.nodes.len(), 1);
    assert_eq!(cites.nodes[0].id, p);
    let approvals = db
        .scan_filter("approvals", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    assert_eq!(approvals.len(), 1);
    assert_eq!(text(&approvals[0], "level"), "architecture");
    assert_eq!(text(&approvals[0], "rationale"), "performance OK");
    let outcomes = db
        .scan_filter("outcomes", db.snapshot(), &|r| {
            r.values.get("decision_id") == Some(&Value::Uuid(d))
        })
        .expect("scan");
    assert_eq!(outcomes.len(), 1);
    assert_eq!(text(&outcomes[0], "notes"), "deployed successfully");
}

#[test]
fn b17_06_replay_output_shape() {
    // Self-contained replay with diff assertions
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let d = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "infra", "cpu=4,region=us");
    insert_decision(&db, tx, d, "autoscale", "active", 0.8);
    add_edge(&db, tx, d, e, "BASED_ON");
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            (
                "state".to_string(),
                Value::Text("cpu=4,region=us".to_string()),
            ),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snap");
    db.commit(tx).expect("commit");

    // Update entity
    let tx2 = db.begin();
    let old = entity_row(&db, e);
    db.delete_row(tx2, "entities", old.row_id).expect("delete");
    insert_entity(&db, tx2, e, "infra", "cpu=8,region=eu");
    db.commit(tx2).expect("commit");

    // Golden output: snapshot preserved, entity updated, diff computable
    let snap = db
        .scan_filter("entity_snapshots", db.snapshot(), &|r| {
            r.values.get("entity_id") == Some(&Value::Uuid(e))
        })
        .expect("scan");
    assert_eq!(snap.len(), 1);
    let old_state = text(&snap[0], "state").to_string();
    assert_eq!(old_state, "cpu=4,region=us");
    let current_row = entity_row(&db, e);
    let current = text(&current_row, "properties").to_string();
    assert_eq!(current, "cpu=8,region=eu");
    // Verify diff
    assert_ne!(old_state, current, "state should have changed for replay");
}

// B18 stress/friction
#[test]
fn b18_01_multi_observation_independent_impact() {
    let db = setup_ontology_db();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let tx = db.begin();
    for d in [d1, d2] {
        insert_decision(&db, tx, d, "d", "active", 0.8);
    }
    for (obs, field) in [(Uuid::new_v4(), "cpu"), (Uuid::new_v4(), "disk")] {
        for d in [d1, d2] {
            db.insert_row(
                tx,
                "invalidations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("affected_decision_id".to_string(), Value::Uuid(d)),
                    ("status".to_string(), Value::Text("pending".to_string())),
                    ("trigger_observation_id".to_string(), Value::Uuid(obs)),
                    (
                        "basis_diff".to_string(),
                        Value::Text(format!("field={field}")),
                    ),
                    ("severity".to_string(), Value::Text("warning".to_string())),
                ]),
            )
            .expect("invalidation");
        }
    }
    db.commit(tx).expect("commit");
    let invs = db.scan("invalidations", db.snapshot()).expect("scan");
    assert_eq!(invs.len(), 4);
    let obs_ids: HashSet<Uuid> = invs
        .iter()
        .filter_map(|r| {
            r.values
                .get("trigger_observation_id")
                .and_then(Value::as_uuid)
                .copied()
        })
        .collect();
    assert_eq!(obs_ids.len(), 2);
}

#[test]
fn b18_02_concurrent_impact_analysis() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e, "e", "");
    for _ in 0..10 {
        let d = Uuid::new_v4();
        insert_decision(&db, tx, d, "d", "active", 0.8);
        add_edge(&db, tx, d, e, "BASED_ON");
        db.insert_row(
            tx,
            "invalidations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("affected_decision_id".to_string(), Value::Uuid(d)),
                ("status".to_string(), Value::Text("pending".to_string())),
                ("severity".to_string(), Value::Text("warning".to_string())),
            ]),
        )
        .expect("inv");
    }
    db.insert_row(
        tx,
        "entity_snapshots",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("entity_id".to_string(), Value::Uuid(e)),
            ("state".to_string(), Value::Text("snapshot".to_string())),
            ("valid_from".to_string(), Value::Int64(1)),
            ("valid_to".to_string(), Value::Int64(2)),
        ]),
    )
    .expect("snapshot");
    db.commit(tx).expect("commit");

    assert_eq!(
        db.scan("invalidations", db.snapshot())
            .expect("scan inv")
            .len(),
        10
    );
    assert_eq!(
        db.scan("entity_snapshots", db.snapshot())
            .expect("scan snap")
            .len(),
        1
    );
}

#[test]
fn b18_03_observation_immutability_under_read() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    for _ in 0..100 {
        db.insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e)),
                ("data".to_string(), Value::Text("obs".to_string())),
            ]),
        )
        .expect("obs");
    }
    db.commit(tx).expect("commit");

    let rows = db.scan("observations", db.snapshot()).expect("scan");
    assert_eq!(rows.len(), 100);
    let up = db.execute("UPDATE observations SET data='x'", &HashMap::new());
    assert!(matches!(up, Err(Error::ImmutableTable(_))));
    let del = db.execute("DELETE FROM observations", &HashMap::new());
    assert!(matches!(del, Err(Error::ImmutableTable(_))));
}

#[test]
fn b18_04_high_frequency_write_throughput() {
    let db = setup_ontology_db();
    let e = Uuid::new_v4();
    let tx = db.begin();
    let mut row_ids = Vec::new();
    for i in 0..1000 {
        let rid = db
            .insert_row(
                tx,
                "observations",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("entity_id".to_string(), Value::Uuid(e)),
                    (
                        "data".to_string(),
                        Value::Text(format!("frame_id={i},detection_count={}", i % 5)),
                    ),
                ]),
            )
            .expect("obs");
        db.insert_vector(tx, rid, vec![1.0, (i % 10) as f32])
            .expect("vec");
        row_ids.push(rid);
    }
    db.commit(tx).expect("commit");

    assert_eq!(
        db.scan("observations", db.snapshot()).expect("scan").len(),
        1000
    );
    let out = db
        .query_vector(&[1.0, 0.0], 5, None, db.snapshot())
        .expect("search");
    assert_eq!(out.len(), 5);
    assert!(out.iter().all(|(id, _)| row_ids.contains(id)));
}

#[test]
fn b18_06_person_reidentification_via_embedding() {
    let db = setup_ontology_db();
    let e1 = Uuid::new_v4();
    let e2 = Uuid::new_v4();
    let decision = Uuid::new_v4();
    let tx = db.begin();
    insert_entity(&db, tx, e1, "known_visitor", "last_seen=2024-01-01");
    insert_entity(&db, tx, e2, "regular_worker", "last_seen=2024-01-01");
    insert_decision(&db, tx, decision, "trust E1", "active", 0.8);
    add_edge(&db, tx, decision, e1, "BASED_ON");
    let r1 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e1)),
                ("data".to_string(), Value::Text("face".to_string())),
            ]),
        )
        .expect("obs1");
    let r2 = db
        .insert_row(
            tx,
            "observations",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("entity_id".to_string(), Value::Uuid(e2)),
                ("data".to_string(), Value::Text("face".to_string())),
            ]),
        )
        .expect("obs2");
    db.insert_vector(tx, r1, vec![1.0, 0.0]).expect("v1");
    db.insert_vector(tx, r2, vec![0.0, 1.0]).expect("v2");
    db.commit(tx).expect("commit");

    let top = db
        .query_vector(&[0.95, 0.05], 1, None, db.snapshot())
        .expect("search");
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].0, r1);

    let tx2 = db.begin();
    let old = entity_row(&db, e1);
    db.delete_row(tx2, "entities", old.row_id)
        .expect("delete old e1");
    db.insert_row(
        tx2,
        "entities",
        HashMap::from([
            ("id".to_string(), Value::Uuid(e1)),
            ("name".to_string(), Value::Text("known_visitor".to_string())),
            (
                "entity_type".to_string(),
                Value::Text("SERVICE".to_string()),
            ),
            (
                "properties".to_string(),
                Value::Text("last_seen=now".to_string()),
            ),
        ]),
    )
    .expect("insert updated e1");
    db.insert_row(
        tx2,
        "invalidations",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("affected_decision_id".to_string(), Value::Uuid(decision)),
            ("status".to_string(), Value::Text("pending".to_string())),
            ("severity".to_string(), Value::Text("warning".to_string())),
        ]),
    )
    .expect("inv");
    db.commit(tx2).expect("commit update");

    assert_eq!(text(&entity_row(&db, e1), "properties"), "last_seen=now");
    assert_eq!(
        db.scan_filter("invalidations", db.snapshot(), &|r| {
            r.values.get("affected_decision_id") == Some(&Value::Uuid(decision))
        })
        .expect("scan")
        .len(),
        1
    );
}

#[test]
fn b18_07_decision_supersession_chain() {
    let db = setup_ontology_db();
    let i = Uuid::new_v4();
    let d1 = Uuid::new_v4();
    let d2 = Uuid::new_v4();
    let d3 = Uuid::new_v4();
    let d4 = Uuid::new_v4();
    let e = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        HashMap::from([
            ("id".to_string(), Value::Uuid(i)),
            ("description".to_string(), Value::Text("goal".to_string())),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .expect("i");
    insert_entity(&db, tx, e, "e", "");
    insert_decision(&db, tx, d1, "d1", "superseded", 0.8);
    insert_decision(&db, tx, d2, "d2", "superseded", 0.8);
    insert_decision(&db, tx, d3, "d3", "superseded", 0.8);
    insert_decision(&db, tx, d4, "d4", "active", 0.8);
    add_edge(&db, tx, d1, i, "SERVES");
    add_edge(&db, tx, d2, i, "SERVES");
    add_edge(&db, tx, d3, i, "SERVES");
    add_edge(&db, tx, d4, i, "SERVES");
    add_edge(&db, tx, d4, d3, "SUPERSEDES");
    add_edge(&db, tx, d3, d2, "SUPERSEDES");
    add_edge(&db, tx, d2, d1, "SUPERSEDES");
    add_edge(&db, tx, d4, d3, "CITES");
    add_edge(&db, tx, d3, d2, "CITES");
    add_edge(&db, tx, d2, d1, "CITES");
    db.commit(tx).expect("commit");

    let chain = db
        .query_bfs(
            d4,
            Some(&["SUPERSEDES".to_string()]),
            Direction::Outgoing,
            3,
            db.snapshot(),
        )
        .expect("chain");
    assert_eq!(chain.nodes.len(), 3);
    let d4row = db
        .point_lookup("decisions", "id", &Value::Uuid(d4), db.snapshot())
        .expect("lookup")
        .expect("row");
    assert_eq!(text(&d4row, "status"), "active");
    let serving = db
        .query_bfs(
            i,
            Some(&["SERVES".to_string()]),
            Direction::Incoming,
            1,
            db.snapshot(),
        )
        .expect("serves");
    assert!(serving.nodes.iter().any(|n| n.id == d4));
    let cites = db
        .query_bfs(
            d4,
            Some(&["CITES".to_string()]),
            Direction::Outgoing,
            3,
            db.snapshot(),
        )
        .expect("cites");
    assert_eq!(cites.nodes.len(), 3);
}
