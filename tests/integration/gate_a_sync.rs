//! Gate A9 — Sync Engine Tests + NT — NATS Integration Tests
//!
//! All 25 tests define the sync contract for contextDB.
//! A9-xx tests verify in-process sync mechanics (ChangeTracking + ChangeApplication traits).
//! NT-xx tests verify NATS wire-protocol round-trips.
//!
//! Sync contract tests. A9-xx test engine sync mechanics. NT-xx test NATS transport.

use contextdb_core::{Direction, MemoryAccountant, Value};
use contextdb_core::{Lsn, RowId};
use contextdb_engine::Database;
#[allow(unused_imports)]
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange,
    NaturalKey, RowChange, SyncDirection, VectorChange,
};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

async fn start_nats() -> NatsFixture {
    let nats_conf = format!(
        "{}/../contextdb-server/tests/nats.conf",
        env!("CARGO_MANIFEST_DIR")
    );

    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(9222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));

    let request = image
        .with_mount(Mount::bind_mount(&nats_conf, "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);

    let container: ContainerAsync<GenericImage> = request.start().await.unwrap();
    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();

    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

fn make_params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn setup_sync_db_with_tables(tables: &[&str]) -> Database {
    let db = Database::open_memory();
    let params = HashMap::new();
    for table in tables {
        match *table {
            "observations" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, source TEXT, embedding VECTOR(3)) IMMUTABLE",
                    &params,
                ).unwrap();
            }
            "observations_384" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, embedding VECTOR(384)) IMMUTABLE",
                    &params,
                ).unwrap();
            }
            "observations_seq" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, seq INTEGER, data TEXT) IMMUTABLE",
                    &params,
                ).unwrap();
            }
            "observations_text" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
                    &params,
                )
                .unwrap();
            }
            "observations_src" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, source TEXT, context_id UUID, embedding VECTOR(3)) IMMUTABLE",
                    &params,
                ).unwrap();
            }
            "entities" => {
                db.execute(
                    "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, entity_type TEXT)",
                    &params,
                )
                .unwrap();
            }
            "entities_status" => {
                db.execute(
                    "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, status TEXT)",
                    &params,
                )
                .unwrap();
            }
            "decisions" => {
                db.execute(
                    "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
                    &params,
                )
                .unwrap();
            }
            "decisions_sm" => {
                db.execute(
                    "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT) STATE MACHINE (status: active -> [superseded, invalidated], superseded -> [invalidated])",
                    &params,
                ).unwrap();
            }
            "patterns" => {
                db.execute(
                    "CREATE TABLE patterns (id UUID PRIMARY KEY, description TEXT)",
                    &params,
                )
                .unwrap();
            }
            "invalidations" => {
                db.execute(
                    "CREATE TABLE invalidations (id UUID PRIMARY KEY, affected_decision_id UUID, trigger_observation_id UUID, basis_diff TEXT, status TEXT, severity TEXT, resolution_decision_id UUID) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
                    &params,
                ).unwrap();
            }
            "scratch" => {
                db.execute(
                    "CREATE TABLE scratch (id UUID PRIMARY KEY, temp TEXT)",
                    &params,
                )
                .unwrap();
            }
            "items" => {
                db.execute(
                    "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
                    &params,
                )
                .unwrap();
            }
            "observations_simple" => {
                db.execute(
                    "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
                    &params,
                )
                .unwrap();
            }
            _ => panic!("Unknown table template: {}", table),
        }
    }
    db
}

// =========================================================================
// A9-01: push_returns_rows_since_watermark
// =========================================================================
#[test]
fn a9_01_push_returns_rows_since_watermark() {
    let edge = setup_sync_db_with_tables(&["observations"]);

    let uuid_a = Uuid::new_v4();
    let uuid_b = Uuid::new_v4();
    let uuid_c = Uuid::new_v4();
    let uuid_d = Uuid::new_v4();
    let uuid_e = Uuid::new_v4();

    // Commit 1: 2 rows
    let tx1 = edge.begin();
    edge.insert_row(
        tx1,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_a)),
            ("data", Value::Text(r#"{"sensor":1}"#.into())),
            ("source", Value::Text("cam_1".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx1,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_b)),
            ("data", Value::Text(r#"{"sensor":2}"#.into())),
            ("source", Value::Text("cam_2".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx1).unwrap();

    let lsn_after_commit_1 = edge.current_lsn();

    // Commit 2: 3 rows
    let tx2 = edge.begin();
    edge.insert_row(
        tx2,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_c)),
            ("data", Value::Text(r#"{"sensor":3}"#.into())),
            ("source", Value::Text("cam_3".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx2,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_d)),
            ("data", Value::Text(r#"{"sensor":4}"#.into())),
            ("source", Value::Text("cam_4".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx2,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_e)),
            ("data", Value::Text(r#"{"sensor":5}"#.into())),
            ("source", Value::Text("cam_5".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx2).unwrap();

    // Guard: LSN must have advanced
    assert!(
        edge.current_lsn() > lsn_after_commit_1,
        "LSN must advance after second commit"
    );

    // All changes since LSN 0
    let cs_all = edge.changes_since(Lsn(0));
    assert_eq!(
        cs_all.rows.len(),
        5,
        "changes_since(0) must return all 5 rows"
    );
    let all_uuids: Vec<Uuid> = cs_all
        .rows
        .iter()
        .filter_map(|r| {
            if let Value::Uuid(u) = &r.natural_key.value {
                Some(*u)
            } else {
                None
            }
        })
        .collect();
    for expected in [uuid_a, uuid_b, uuid_c, uuid_d, uuid_e] {
        assert!(
            all_uuids.contains(&expected),
            "changes_since(0) must contain UUID {}",
            expected
        );
    }
    // Verify field values round-trip — check presence AND actual values
    for row in &cs_all.rows {
        assert!(
            row.values.contains_key("data"),
            "each row must have 'data' field"
        );
        assert!(
            row.values.contains_key("source"),
            "each row must have 'source' field"
        );
    }
    // Verify at least one row's actual value matches the inserted data
    let row_a = cs_all
        .rows
        .iter()
        .find(|r| r.natural_key.value == Value::Uuid(uuid_a))
        .expect("must find row with uuid_a's natural key");
    assert_eq!(
        row_a.values.get("data").and_then(Value::as_text),
        Some(r#"{"sensor":1}"#),
        "uuid_a's data field must match inserted value"
    );
    assert_eq!(
        row_a.values.get("source").and_then(Value::as_text),
        Some("cam_1"),
        "uuid_a's source field must match inserted value"
    );

    // Only second commit's changes
    let cs_partial = edge.changes_since(lsn_after_commit_1);
    assert_eq!(
        cs_partial.rows.len(),
        3,
        "changes_since(lsn_after_commit_1) must return exactly 3 rows from commit 2"
    );
    let partial_uuids: Vec<Uuid> = cs_partial
        .rows
        .iter()
        .filter_map(|r| {
            if let Value::Uuid(u) = &r.natural_key.value {
                Some(*u)
            } else {
                None
            }
        })
        .collect();
    assert!(partial_uuids.contains(&uuid_c));
    assert!(partial_uuids.contains(&uuid_d));
    assert!(partial_uuids.contains(&uuid_e));
    // Negative: commit 1 rows must NOT appear
    assert!(
        !partial_uuids.contains(&uuid_a),
        "commit 1 row uuid_a must NOT appear in changes_since(lsn_after_commit_1)"
    );
    assert!(
        !partial_uuids.contains(&uuid_b),
        "commit 1 row uuid_b must NOT appear in changes_since(lsn_after_commit_1)"
    );

    // Nothing new at current LSN
    let cs_empty = edge.changes_since(edge.current_lsn());
    assert!(
        cs_empty.rows.is_empty(),
        "changes_since(current_lsn) must return empty rows"
    );
    assert!(
        cs_empty.edges.is_empty(),
        "changes_since(current_lsn) must return empty edges"
    );
    assert!(
        cs_empty.vectors.is_empty(),
        "changes_since(current_lsn) must return empty vectors"
    );
}

// =========================================================================
// A9-02: pull_inserts_new_rows
// =========================================================================
#[test]
fn a9_02_pull_inserts_new_rows() {
    let edge = setup_sync_db_with_tables(&["observations"]);
    let server = setup_sync_db_with_tables(&["observations"]);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();

    // Guard: server starts empty
    let server_rows_before = server.scan("observations", server.snapshot()).unwrap();
    assert_eq!(
        server_rows_before.len(),
        0,
        "server must start with 0 observations"
    );

    // Edge inserts 3 observations
    let tx = edge.begin();
    edge.insert_row(
        tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_1)),
            ("data", Value::Text(r#"{"temp":25}"#.into())),
            ("source", Value::Text("cam_1".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_2)),
            ("data", Value::Text(r#"{"temp":30}"#.into())),
            ("source", Value::Text("cam_2".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_3)),
            ("data", Value::Text(r#"{"temp":22}"#.into())),
            ("source", Value::Text("cam_3".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    let changeset = edge.changes_since(Lsn(0));
    let result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    // Verify each row by point lookup — field-level fidelity
    for (uuid, expected_source) in [(uuid_1, "cam_1"), (uuid_2, "cam_2"), (uuid_3, "cam_3")] {
        let row = server
            .point_lookup("observations", "id", &Value::Uuid(uuid), server.snapshot())
            .unwrap()
            .unwrap_or_else(|| panic!("server must have row for UUID {}", uuid));
        assert_eq!(
            row.values.get("source").and_then(Value::as_text),
            Some(expected_source),
            "source field must match for UUID {}",
            uuid
        );
        assert!(
            row.values.contains_key("data"),
            "data field must be present for UUID {}",
            uuid
        );
    }

    // Verify scan returns exactly 3
    let all_rows = server.scan("observations", server.snapshot()).unwrap();
    assert_eq!(
        all_rows.len(),
        3,
        "server scan must return exactly 3 observations"
    );
    let scanned_uuids: HashSet<Uuid> = all_rows
        .iter()
        .map(|r| *r.values.get("id").and_then(Value::as_uuid).unwrap())
        .collect();
    assert_eq!(scanned_uuids, HashSet::from([uuid_1, uuid_2, uuid_3]));

    assert_eq!(result.applied_rows, 3);
    assert_eq!(result.skipped_rows, 0);
}

// =========================================================================
// A9-03: pull_conflict_server_wins
// =========================================================================
#[test]
fn a9_03_pull_conflict_server_wins() {
    let edge = setup_sync_db_with_tables(&["entities"]);
    let server = setup_sync_db_with_tables(&["entities"]);

    let shared_uuid = Uuid::new_v4();

    // Edge version
    let tx_e = edge.begin();
    edge.insert_row(
        tx_e,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(shared_uuid)),
            ("name", Value::Text("edge-version".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_e).unwrap();

    // Server version (different data, same UUID)
    let tx_s = server.begin();
    server
        .insert_row(
            tx_s,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(shared_uuid)),
                ("name", Value::Text("server-version".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s).unwrap();

    // Guard: both sides have 1 row with the same UUID
    assert!(
        server
            .point_lookup(
                "entities",
                "id",
                &Value::Uuid(shared_uuid),
                server.snapshot()
            )
            .unwrap()
            .is_some(),
        "server must have the row before apply"
    );

    let changeset = edge.changes_since(Lsn(0));
    let result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .unwrap();

    // Server kept its version
    let row = server
        .point_lookup(
            "entities",
            "id",
            &Value::Uuid(shared_uuid),
            server.snapshot(),
        )
        .unwrap()
        .expect("row must still exist");
    assert_eq!(
        row.values.get("name").and_then(Value::as_text),
        Some("server-version"),
        "ServerWins: server must keep its own value"
    );

    // Conflict recorded
    assert_eq!(result.skipped_rows, 1, "one row must be skipped");
    assert_eq!(result.conflicts.len(), 1, "one conflict must be recorded");
    assert_eq!(result.conflicts[0].resolution, ConflictPolicy::ServerWins);
    assert_eq!(
        result.conflicts[0].natural_key.value,
        Value::Uuid(shared_uuid)
    );

    // Scan still returns exactly 1 row (not 2)
    let all = server.scan("entities", server.snapshot()).unwrap();
    assert_eq!(all.len(), 1, "server must still have exactly 1 entity row");
}

// =========================================================================
// A9-04: pull_conflict_latest_wins
// =========================================================================
#[test]
fn a9_04_pull_conflict_latest_wins() {
    // --- Scenario 1: edge LSN > server LSN -> edge wins ---
    let edge1 = setup_sync_db_with_tables(&["entities"]);
    let server1 = setup_sync_db_with_tables(&["entities"]);
    let uuid_1 = Uuid::new_v4();

    // Server inserts first (lower LSN)
    let tx_s = server1.begin();
    server1
        .insert_row(
            tx_s,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                ("name", Value::Text("server-old".into())),
            ]),
        )
        .unwrap();
    server1.commit(tx_s).unwrap();

    // Edge inserts later — bump LSN with a padding commit first
    let padding_uuid = Uuid::new_v4();
    let tx_e1 = edge1.begin();
    edge1
        .insert_row(
            tx_e1,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(padding_uuid)),
                ("name", Value::Text("padding".into())),
            ]),
        )
        .unwrap();
    edge1.commit(tx_e1).unwrap();
    let tx_e2 = edge1.begin();
    edge1
        .insert_row(
            tx_e2,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                ("name", Value::Text("edge-newer".into())),
            ]),
        )
        .unwrap();
    edge1.commit(tx_e2).unwrap();

    // Guard: edge LSN must be higher than server LSN
    assert!(
        edge1.current_lsn() > server1.current_lsn(),
        "edge must have higher LSN than server for scenario 1"
    );

    let changeset = edge1.changes_since(Lsn(0));
    let _result = server1
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let row = server1
        .point_lookup("entities", "id", &Value::Uuid(uuid_1), server1.snapshot())
        .unwrap()
        .expect("row must exist after apply");
    assert_eq!(
        row.values.get("name").and_then(Value::as_text),
        Some("edge-newer"),
        "LatestWins scenario 1: edge had higher LSN, so edge value must win"
    );

    // --- Scenario 2: server LSN > edge LSN -> server wins ---
    let edge2 = setup_sync_db_with_tables(&["entities"]);
    let server2 = setup_sync_db_with_tables(&["entities"]);
    let uuid_2 = Uuid::new_v4();

    // Edge inserts first (lower LSN)
    let tx_e = edge2.begin();
    edge2
        .insert_row(
            tx_e,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(uuid_2)),
                ("name", Value::Text("edge-old".into())),
            ]),
        )
        .unwrap();
    edge2.commit(tx_e).unwrap();

    // Server inserts later (higher LSN via extra commits)
    let padding2_uuid = Uuid::new_v4();
    let tx_s1 = server2.begin();
    server2
        .insert_row(
            tx_s1,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(padding2_uuid)),
                ("name", Value::Text("padding".into())),
            ]),
        )
        .unwrap();
    server2.commit(tx_s1).unwrap();
    let tx_s2 = server2.begin();
    server2
        .insert_row(
            tx_s2,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(uuid_2)),
                ("name", Value::Text("server-newer".into())),
            ]),
        )
        .unwrap();
    server2.commit(tx_s2).unwrap();

    // Guard: server LSN must be higher than edge LSN
    assert!(
        server2.current_lsn() > edge2.current_lsn(),
        "server must have higher LSN than edge for scenario 2"
    );

    let changeset2 = edge2.changes_since(Lsn(0));
    let result2 = server2
        .apply_changes(
            changeset2,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let row2 = server2
        .point_lookup("entities", "id", &Value::Uuid(uuid_2), server2.snapshot())
        .unwrap()
        .expect("row must exist after apply");
    assert_eq!(
        row2.values.get("name").and_then(Value::as_text),
        Some("server-newer"),
        "LatestWins scenario 2: server had higher LSN, so server value must win"
    );
    assert!(
        result2.skipped_rows >= 1,
        "at least 1 row should be skipped when server wins"
    );
}

// =========================================================================
// A9-05: pull_conflict_edge_wins
// =========================================================================
#[test]
fn a9_05_pull_conflict_edge_wins() {
    let edge = setup_sync_db_with_tables(&["entities_status"]);
    let server = setup_sync_db_with_tables(&["entities_status"]);

    let shared_uuid = Uuid::new_v4();

    // Server version
    let tx_s = server.begin();
    server
        .insert_row(
            tx_s,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(shared_uuid)),
                ("name", Value::Text("server-version".into())),
                ("status", Value::Text("stale".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s).unwrap();

    // Edge version
    let tx_e = edge.begin();
    edge.insert_row(
        tx_e,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(shared_uuid)),
            ("name", Value::Text("edge-version".into())),
            ("status", Value::Text("fresh".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_e).unwrap();

    // Guard: server has its version
    let before = server
        .point_lookup(
            "entities",
            "id",
            &Value::Uuid(shared_uuid),
            server.snapshot(),
        )
        .unwrap()
        .expect("server must have the row before apply");
    assert_eq!(
        before.values.get("name").and_then(Value::as_text),
        Some("server-version")
    );

    let changeset = edge.changes_since(Lsn(0));
    let result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    // Server now has edge's values
    let row = server
        .point_lookup(
            "entities",
            "id",
            &Value::Uuid(shared_uuid),
            server.snapshot(),
        )
        .unwrap()
        .expect("row must still exist");
    assert_eq!(
        row.values.get("name").and_then(Value::as_text),
        Some("edge-version"),
        "EdgeWins: server must adopt edge's name"
    );
    assert_eq!(
        row.values.get("status").and_then(Value::as_text),
        Some("fresh"),
        "EdgeWins: server must adopt edge's status"
    );

    // Conflict recorded with EdgeWins resolution
    assert_eq!(result.conflicts.len(), 1);
    assert_eq!(result.conflicts[0].resolution, ConflictPolicy::EdgeWins);
    assert_eq!(
        result.conflicts[0].natural_key.value,
        Value::Uuid(shared_uuid)
    );

    // Still exactly 1 row (replaced, not duplicated)
    let all = server.scan("entities", server.snapshot()).unwrap();
    assert_eq!(all.len(), 1, "EdgeWins must replace, not duplicate");
}

// =========================================================================
// A9-06: observations_never_conflict
// =========================================================================
#[test]
fn a9_06_observations_never_conflict() {
    let edge = setup_sync_db_with_tables(&["observations"]);
    let server = setup_sync_db_with_tables(&["observations"]);

    let uuid_s1 = Uuid::new_v4();
    let uuid_s2 = Uuid::new_v4();
    let uuid_e1 = Uuid::new_v4();
    let uuid_e2 = Uuid::new_v4();
    let uuid_e3 = Uuid::new_v4();

    // Server has 2 existing observations
    let tx_s = server.begin();
    server
        .insert_row(
            tx_s,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_s1)),
                ("data", Value::Text(r#"{"event":"motion"}"#.into())),
                ("source", Value::Text("cam_a".into())),
            ]),
        )
        .unwrap();
    server
        .insert_row(
            tx_s,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_s2)),
                ("data", Value::Text(r#"{"event":"door"}"#.into())),
                ("source", Value::Text("cam_b".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s).unwrap();

    // Guard: server starts with exactly 2
    assert_eq!(
        server
            .scan("observations", server.snapshot())
            .unwrap()
            .len(),
        2
    );

    // Edge has 3 different observations
    let tx_e = edge.begin();
    edge.insert_row(
        tx_e,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_e1)),
            ("data", Value::Text(r#"{"event":"person"}"#.into())),
            ("source", Value::Text("cam_1".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx_e,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_e2)),
            ("data", Value::Text(r#"{"event":"vehicle"}"#.into())),
            ("source", Value::Text("cam_2".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx_e,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_e3)),
            ("data", Value::Text(r#"{"event":"package"}"#.into())),
            ("source", Value::Text("cam_3".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_e).unwrap();

    // First push: 3 new observations arrive
    let cs = edge.changes_since(Lsn(0));
    let result1 = server
        .apply_changes(
            cs.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();
    assert_eq!(
        result1.applied_rows, 3,
        "first push must insert all 3 edge observations"
    );
    assert_eq!(result1.skipped_rows, 0, "first push must skip 0 rows");

    let all = server.scan("observations", server.snapshot()).unwrap();
    assert_eq!(
        all.len(),
        5,
        "server must have 5 total observations after first push"
    );
    let all_uuids: HashSet<Uuid> = all
        .iter()
        .map(|r| *r.values.get("id").and_then(Value::as_uuid).unwrap())
        .collect();
    for expected in [uuid_s1, uuid_s2, uuid_e1, uuid_e2, uuid_e3] {
        assert!(
            all_uuids.contains(&expected),
            "server must contain UUID {}",
            expected
        );
    }

    // Re-push: same 3 observations are skipped (idempotent)
    let result2 = server
        .apply_changes(
            cs,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();
    assert_eq!(result2.applied_rows, 0, "re-push must apply 0 rows");
    assert_eq!(
        result2.skipped_rows, 3,
        "re-push must skip all 3 already-present rows"
    );

    let all2 = server.scan("observations", server.snapshot()).unwrap();
    assert_eq!(
        all2.len(),
        5,
        "server must still have exactly 5 observations (no duplicates)"
    );
}

// =========================================================================
// A9-07: watermark_advances_after_sync
// =========================================================================
#[test]
fn a9_07_watermark_advances_after_sync() {
    let edge = setup_sync_db_with_tables(&["entities"]);
    let server = setup_sync_db_with_tables(&["entities"]);

    let uuid_a = Uuid::new_v4();
    let uuid_b = Uuid::new_v4();

    let tx = edge.begin();
    edge.insert_row(
        tx,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(uuid_a)),
            ("name", Value::Text("entity_a".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(uuid_b)),
            ("name", Value::Text("entity_b".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    let server_lsn_before = server.current_lsn();

    // Guard: server starts empty
    assert_eq!(server.scan("entities", server.snapshot()).unwrap().len(), 0);

    let changeset = edge.changes_since(Lsn(0));
    let _result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    // Server's LSN must advance
    let server_lsn_after = server.current_lsn();
    assert!(
        server_lsn_after > server_lsn_before,
        "server LSN must advance after applying changes"
    );
    assert!(
        server_lsn_after > Lsn(0),
        "server LSN must be > 0 after apply"
    );

    // Server can now serve these rows to other pullers
    let server_cs = server.changes_since(Lsn(0));
    assert_eq!(
        server_cs.rows.len(),
        2,
        "server changes_since(0) must return exactly 2 received rows"
    );
    let server_uuids: Vec<Uuid> = server_cs
        .rows
        .iter()
        .filter_map(|r| {
            if let Value::Uuid(u) = &r.natural_key.value {
                Some(*u)
            } else {
                None
            }
        })
        .collect();
    assert!(
        server_uuids.contains(&uuid_a),
        "server must be able to re-serve uuid_a to downstream pullers"
    );
    assert!(
        server_uuids.contains(&uuid_b),
        "server must be able to re-serve uuid_b to downstream pullers"
    );

    // Server's changes_since(current) returns empty
    let server_cs_empty = server.changes_since(server.current_lsn());
    assert!(
        server_cs_empty.rows.is_empty(),
        "server changes_since(current_lsn) must return empty"
    );
}

// =========================================================================
// A9-08: sync_is_deterministic
// =========================================================================
#[test]
fn a9_08_sync_is_deterministic() {
    let uuids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

    // Create 3 edges with identical data
    let mut changesets = Vec::new();
    for _ in 0..3 {
        let db = setup_sync_db_with_tables(&["observations_simple"]);
        let tx = db.begin();
        for &uuid in &uuids {
            db.insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(uuid)),
                    ("data", Value::Text(r#"{"event":"same"}"#.into())),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        changesets.push(db.changes_since(Lsn(0)));
    }

    // Server 1: apply in order 0, 1, 2
    let server1 = setup_sync_db_with_tables(&["observations_simple"]);
    for cs in &changesets {
        server1
            .apply_changes(
                cs.clone(),
                &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
            )
            .unwrap();
    }
    let rows1 = server1.scan("observations", server1.snapshot()).unwrap();
    assert_eq!(
        rows1.len(),
        3,
        "3 identical UUIDs from 3 edges must coalesce to 3 rows, not 9"
    );

    // Server 2: apply in reverse order 2, 1, 0
    let server2 = setup_sync_db_with_tables(&["observations_simple"]);
    for cs in changesets.iter().rev() {
        server2
            .apply_changes(
                cs.clone(),
                &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
            )
            .unwrap();
    }
    let rows2 = server2.scan("observations", server2.snapshot()).unwrap();
    assert_eq!(
        rows2.len(),
        3,
        "reverse order must also produce exactly 3 rows"
    );

    // Both servers have identical UUID sets
    let mut uuids1: Vec<Uuid> = rows1
        .iter()
        .map(|r| *r.values.get("id").and_then(Value::as_uuid).unwrap())
        .collect();
    uuids1.sort();
    let mut uuids2: Vec<Uuid> = rows2
        .iter()
        .map(|r| *r.values.get("id").and_then(Value::as_uuid).unwrap())
        .collect();
    uuids2.sort();
    assert_eq!(uuids1, uuids2, "both servers must have identical UUID sets");

    // Both servers have identical field values for each UUID
    for &uuid in &uuids {
        let r1 = server1
            .point_lookup("observations", "id", &Value::Uuid(uuid), server1.snapshot())
            .unwrap()
            .expect("server1 must have row");
        let r2 = server2
            .point_lookup("observations", "id", &Value::Uuid(uuid), server2.snapshot())
            .unwrap()
            .expect("server2 must have row");
        assert_eq!(
            r1.values.get("data"),
            r2.values.get("data"),
            "field values must match for UUID {} across both servers",
            uuid
        );
    }
}

// =========================================================================
// A9-09: memory_to_memory_sync
// =========================================================================
#[test]
fn a9_09_memory_to_memory_sync() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let params = HashMap::new();

    // Create schema on both sides
    for db in [&edge, &server] {
        db.execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
            &params,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
            &params,
        )
        .unwrap();
        db.execute("CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3)) IMMUTABLE", &params).unwrap();
    }

    let entity_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();
    let obs_id = Uuid::new_v4();

    // Edge: insert relational rows, graph edge, and vector
    let tx = edge.begin();
    edge.insert_row(
        tx,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(entity_id)),
            ("name", Value::Text("Camera-3".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(decision_id)),
            ("description", Value::Text("alert on motion".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    let obs_row_id = edge
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(obs_id)),
                ("data", Value::Text(r#"{"event":"motion"}"#.into())),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    edge.insert_edge(
        tx,
        decision_id,
        entity_id,
        "BASED_ON".to_string(),
        HashMap::new(),
    )
    .unwrap();
    edge.insert_vector(
        tx,
        contextdb_core::VectorIndexRef::new("observations", "embedding"),
        obs_row_id,
        vec![1.0, 0.0, 0.0],
    )
    .unwrap();
    edge.commit(tx).unwrap();

    // Guard: server starts empty
    assert_eq!(server.scan("entities", server.snapshot()).unwrap().len(), 0);
    assert_eq!(
        server.scan("decisions", server.snapshot()).unwrap().len(),
        0
    );
    assert_eq!(
        server
            .scan("observations", server.snapshot())
            .unwrap()
            .len(),
        0
    );

    let changeset = edge.changes_since(Lsn(0));

    // Guard: changeset has all subsystems
    assert!(!changeset.rows.is_empty(), "changeset must contain rows");
    assert!(
        !changeset.edges.is_empty(),
        "changeset must contain graph edges"
    );
    assert!(
        !changeset.vectors.is_empty(),
        "changeset must contain vectors"
    );

    let _result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    // Relational: point lookups match
    let e_row = server
        .point_lookup("entities", "id", &Value::Uuid(entity_id), server.snapshot())
        .unwrap()
        .expect("entity must be on server");
    assert_eq!(
        e_row.values.get("name").and_then(Value::as_text),
        Some("Camera-3")
    );

    let d_row = server
        .point_lookup(
            "decisions",
            "id",
            &Value::Uuid(decision_id),
            server.snapshot(),
        )
        .unwrap()
        .expect("decision must be on server");
    assert_eq!(
        d_row.values.get("description").and_then(Value::as_text),
        Some("alert on motion")
    );

    // Graph: BFS from decision finds entity
    let bfs_result = server
        .query_bfs(
            decision_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            server.snapshot(),
        )
        .unwrap();
    assert_eq!(
        bfs_result.nodes.len(),
        1,
        "BFS from decision via BASED_ON must find exactly 1 node (the entity)"
    );
    assert_eq!(
        bfs_result.nodes[0].id, entity_id,
        "BFS must find the correct entity"
    );

    // Vector: ANN search finds the observation embedding
    let vec_result = server
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &[1.0, 0.0, 0.0],
            1,
            None,
            server.snapshot(),
        )
        .unwrap();
    assert_eq!(vec_result.len(), 1, "vector search must return 1 result");
}

// a9_10 deleted — redundant with p33_bidirectional_sync_with_persistence

// =========================================================================
// A9-11: conflict_resolution_respects_state_machine
// =========================================================================
#[test]
fn a9_11_conflict_resolution_respects_state_machine() {
    let edge = setup_sync_db_with_tables(&["decisions_sm"]);
    let server = setup_sync_db_with_tables(&["decisions_sm"]);

    let uuid_d = Uuid::new_v4();

    // Server: decision is active, then transitions to superseded
    let tx_s = server.begin();
    server
        .insert_row(
            tx_s,
            "decisions",
            make_params(vec![
                ("id", Value::Uuid(uuid_d)),
                ("description", Value::Text("alert rule".into())),
                ("status", Value::Text("active".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s).unwrap();

    // Transition to superseded
    let tx_s2 = server.begin();
    server
        .upsert_row(
            tx_s2,
            "decisions",
            "id",
            make_params(vec![
                ("id", Value::Uuid(uuid_d)),
                ("description", Value::Text("alert rule".into())),
                ("status", Value::Text("superseded".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s2).unwrap();

    // Guard: server decision is superseded
    let server_row = server
        .point_lookup("decisions", "id", &Value::Uuid(uuid_d), server.snapshot())
        .unwrap()
        .expect("server must have decision");
    assert_eq!(
        server_row.values.get("status").and_then(Value::as_text),
        Some("superseded"),
        "guard: server decision must be 'superseded' before sync"
    );

    // Edge: same decision still active (stale copy, higher LSN via extra commits)
    let padding_id = Uuid::new_v4();
    let tx_pad = edge.begin();
    edge.insert_row(
        tx_pad,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(padding_id)),
            ("description", Value::Text("padding".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_pad).unwrap();
    let tx_pad2 = edge.begin();
    edge.insert_row(
        tx_pad2,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("description", Value::Text("padding2".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_pad2).unwrap();
    let tx_e = edge.begin();
    edge.insert_row(
        tx_e,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(uuid_d)),
            ("description", Value::Text("alert rule".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_e).unwrap();

    // Guard: edge has higher LSN
    assert!(
        edge.current_lsn() > server.current_lsn(),
        "guard: edge must have higher LSN for LatestWins to select edge version"
    );

    let changeset = edge.changes_since(Lsn(0));
    let result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    // Decision must NOT regress from superseded to active
    let row = server
        .point_lookup("decisions", "id", &Value::Uuid(uuid_d), server.snapshot())
        .unwrap()
        .expect("decision must still exist");
    assert_eq!(
        row.values.get("status").and_then(Value::as_text),
        Some("superseded"),
        "state machine must prevent regression: decision must stay 'superseded', not revert to 'active'"
    );

    // Conflict must record the state machine violation reason
    let sm_conflicts: Vec<&Conflict> = result
        .conflicts
        .iter()
        .filter(|c| c.natural_key.value == Value::Uuid(uuid_d))
        .collect();
    assert!(
        !sm_conflicts.is_empty(),
        "a conflict must be recorded for the state-machine-blocked decision"
    );
    assert!(
        sm_conflicts[0]
            .reason
            .as_ref()
            .map(|r| r.contains("state_machine") || r.contains("state machine"))
            .unwrap_or(false),
        "conflict reason must mention state machine violation"
    );
}

// =========================================================================
// A9-12: edge_temporal_validity_syncs
// =========================================================================
#[test]
fn a9_12_edge_temporal_validity_syncs() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let params = HashMap::new();

    for db in [&edge, &server] {
        db.execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
            &params,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
            &params,
        )
        .unwrap();
    }

    let decision_id = Uuid::new_v4();
    let entity_id = Uuid::new_v4();

    // Edge: create decision, entity, and BASED_ON edge
    let tx = edge.begin();
    edge.insert_row(
        tx,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(decision_id)),
            ("description", Value::Text("alert rule".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "entities",
        make_params(vec![
            ("id", Value::Uuid(entity_id)),
            ("name", Value::Text("plate_ABC123".into())),
        ]),
    )
    .unwrap();
    // BASED_ON edge — valid_from is effectively the creation tx
    edge.insert_edge(
        tx,
        decision_id,
        entity_id,
        "BASED_ON".to_string(),
        HashMap::new(),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    // Snapshot after edge creation — BFS should find the edge
    let snap_t1 = edge.snapshot();
    let bfs_before_close = edge
        .query_bfs(
            decision_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            snap_t1,
        )
        .unwrap();
    assert_eq!(
        bfs_before_close.nodes.len(),
        1,
        "guard: BFS on edge must find entity via BASED_ON before closing"
    );

    // Sync the OPEN edge to server — verify BFS finds it (positive assertion)
    let changeset_open = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            changeset_open,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    let server_snap_open = server.snapshot();
    let bfs_server_open = server
        .query_bfs(
            decision_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            server_snap_open,
        )
        .unwrap();
    assert_eq!(
        bfs_server_open.nodes.len(),
        1,
        "server BFS must find the BASED_ON edge BEFORE closure (positive assertion)"
    );

    // Delete the BASED_ON edge on the edge side, sync again, assert NOT found
    let tx2 = edge.begin();
    edge.delete_edge(tx2, decision_id, entity_id, "BASED_ON")
        .unwrap();
    edge.commit(tx2).unwrap();

    let changeset_closed = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            changeset_closed,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    let server_snap_closed = server.snapshot();
    let bfs_server_closed = server
        .query_bfs(
            decision_id,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            server_snap_closed,
        )
        .unwrap();
    assert_eq!(
        bfs_server_closed.nodes.len(),
        0,
        "server BFS must NOT find the BASED_ON edge AFTER deletion sync"
    );

    // Verify the relational data DID arrive
    let server_snap = server.snapshot();
    let d_row = server
        .point_lookup("decisions", "id", &Value::Uuid(decision_id), server_snap)
        .unwrap();
    assert!(d_row.is_some(), "decision row must be synced to server");
    let e_row = server
        .point_lookup("entities", "id", &Value::Uuid(entity_id), server_snap)
        .unwrap();
    assert!(e_row.is_some(), "entity row must be synced to server");
}

// =========================================================================
// A9-13: apply_changes_concurrent_with_local_writes
// =========================================================================
#[test]
fn a9_13_apply_changes_concurrent_with_local_writes() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let params = HashMap::new();

    for db in [&edge, &server] {
        db.execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &params,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE patterns (id UUID PRIMARY KEY, description TEXT)",
            &params,
        )
        .unwrap();
    }

    let uuid_obs = Uuid::new_v4();
    let uuid_pattern = Uuid::new_v4();

    // Edge: insert observation
    let tx_e = edge.begin();
    edge.insert_row(
        tx_e,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_obs)),
            ("data", Value::Text(r#"{"event":"motion_event"}"#.into())),
        ]),
    )
    .unwrap();
    edge.commit(tx_e).unwrap();

    // Server: start local write (pattern creation) — DO NOT commit yet
    let tx_local = server.begin();
    server
        .insert_row(
            tx_local,
            "patterns",
            make_params(vec![
                ("id", Value::Uuid(uuid_pattern)),
                ("description", Value::Text("recurring_motion".into())),
            ]),
        )
        .unwrap();

    // While local tx is open, apply edge changes
    let changeset = edge.changes_since(Lsn(0));
    let result = server
        .apply_changes(
            changeset,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();
    assert_eq!(
        result.applied_rows, 1,
        "apply_changes must succeed while local tx is open"
    );

    // Now commit local tx
    server.commit(tx_local).unwrap();

    let snap = server.snapshot();

    // Both must be visible
    let obs = server
        .point_lookup("observations", "id", &Value::Uuid(uuid_obs), snap)
        .unwrap();
    assert!(
        obs.is_some(),
        "observation from edge must be visible after both commits"
    );

    let pattern = server
        .point_lookup("patterns", "id", &Value::Uuid(uuid_pattern), snap)
        .unwrap();
    assert!(
        pattern.is_some(),
        "pattern from local tx must be visible after commit"
    );

    // Scan returns correct counts — no lost writes
    let all_obs = server.scan("observations", snap).unwrap();
    assert_eq!(all_obs.len(), 1, "server must have exactly 1 observation");
    let all_patterns = server.scan("patterns", snap).unwrap();
    assert_eq!(all_patterns.len(), 1, "server must have exactly 1 pattern");
}

// =========================================================================
// A9-14: selective_sync_direction_filtering
// =========================================================================
#[test]
fn a9_14_selective_sync_direction_filtering() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let params = HashMap::new();

    // Create tables on both sides (except scratch which is edge-only)
    edge.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
        &params,
    )
    .unwrap();
    edge.execute(
        "CREATE TABLE patterns (id UUID PRIMARY KEY, pattern_type TEXT)",
        &params,
    )
    .unwrap();
    edge.execute(
        "CREATE TABLE scratch (id UUID PRIMARY KEY, temp TEXT)",
        &params,
    )
    .unwrap();
    server
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &params,
        )
        .unwrap();
    server
        .execute(
            "CREATE TABLE patterns (id UUID PRIMARY KEY, pattern_type TEXT)",
            &params,
        )
        .unwrap();

    let uuid_obs = Uuid::new_v4();
    let uuid_pat_local = Uuid::new_v4();
    let uuid_scratch = Uuid::new_v4();

    // Direction map
    let mut directions = HashMap::new();
    directions.insert("observations".to_string(), SyncDirection::Push);
    directions.insert("patterns".to_string(), SyncDirection::Pull);
    directions.insert("scratch".to_string(), SyncDirection::None);

    // Edge inserts into all 3 tables
    let tx = edge.begin();
    edge.insert_row(
        tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(uuid_obs)),
            ("data", Value::Text("motion_event".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "patterns",
        make_params(vec![
            ("id", Value::Uuid(uuid_pat_local)),
            ("pattern_type", Value::Text("local_guess".into())),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "scratch",
        make_params(vec![
            ("id", Value::Uuid(uuid_scratch)),
            ("temp", Value::Text("working_data".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx).unwrap();

    // Push with direction filter: only Push and Both tables
    let full_cs = edge.changes_since(Lsn(0));
    let push_cs =
        full_cs.filter_by_direction(&directions, &[SyncDirection::Push, SyncDirection::Both]);
    server
        .apply_changes(
            push_cs,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    // Server got observations
    let obs_row = server
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(uuid_obs),
            server.snapshot(),
        )
        .unwrap();
    assert!(
        obs_row.is_some(),
        "server must receive observations (Push direction)"
    );

    // Server did NOT get scratch (table doesn't even exist on server)
    assert!(
        !server.table_names().contains(&"scratch".to_string()),
        "scratch table must NOT be on server"
    );

    // Server did NOT get edge's local pattern guess
    let pat_local = server
        .point_lookup(
            "patterns",
            "id",
            &Value::Uuid(uuid_pat_local),
            server.snapshot(),
        )
        .unwrap();
    assert!(
        pat_local.is_none(),
        "server must NOT receive patterns from edge (patterns is Pull-only)"
    );

    // Server inserts real patterns
    let uuid_pat_real = Uuid::new_v4();
    let tx_s = server.begin();
    server
        .insert_row(
            tx_s,
            "patterns",
            make_params(vec![
                ("id", Value::Uuid(uuid_pat_real)),
                ("pattern_type", Value::Text("collective_signal".into())),
            ]),
        )
        .unwrap();
    server.commit(tx_s).unwrap();

    // Edge pulls with direction filter: only Pull and Both tables
    let server_full_cs = server.changes_since(Lsn(0));
    let pull_cs = server_full_cs
        .filter_by_direction(&directions, &[SyncDirection::Pull, SyncDirection::Both]);

    // Negative assertion: pull changeset must NOT contain observations
    let pull_obs: Vec<&RowChange> = pull_cs
        .rows
        .iter()
        .filter(|r| r.table == "observations")
        .collect();
    assert!(
        pull_obs.is_empty(),
        "pull changeset must NOT contain observations (Push-only table)"
    );

    edge.apply_changes(
        pull_cs,
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap();

    // Edge got server's pattern
    let pat_real = edge
        .point_lookup(
            "patterns",
            "id",
            &Value::Uuid(uuid_pat_real),
            edge.snapshot(),
        )
        .unwrap();
    assert!(
        pat_real.is_some(),
        "edge must receive server's pattern via pull"
    );

    // Edge's scratch data was never sent anywhere and still exists locally
    let scratch = edge
        .point_lookup("scratch", "id", &Value::Uuid(uuid_scratch), edge.snapshot())
        .unwrap();
    assert!(scratch.is_some(), "scratch data must remain on edge");
}

// =========================================================================
// A9-15: archive-not-delete — status transitions sync as row updates
// =========================================================================
//
// Ontology Invariant 3: rows are archived, never deleted. When a row
// transitions status (e.g. active → superseded), the ChangeSet must carry
// the row with populated values reflecting the new status. An empty values
// map must NEVER represent a status transition. This test pins the
// archive-not-delete contract so that no implementation can treat
// empty values as a delete signal for status-transitioned rows.
// =========================================================================
#[test]
fn a9_15_archive_not_delete_status_transition_sync() {
    let edge = Database::open_memory();
    let server = Database::open_memory();
    let params = HashMap::new();

    // Create decisions table on both sides
    edge.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
        &params,
    )
    .unwrap();
    server
        .execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
            &params,
        )
        .unwrap();

    let uuid_d = Uuid::new_v4();

    // Edge: insert a decision with status=active
    let tx1 = edge.begin();
    edge.insert_row(
        tx1,
        "decisions",
        make_params(vec![
            ("id", Value::Uuid(uuid_d)),
            ("description", Value::Text("initial alert rule".into())),
            ("status", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx1).unwrap();

    // Record watermark after initial insert
    let lsn_after_insert = edge.current_lsn();

    // Sync initial insert to server
    let initial_cs = edge.changes_since(Lsn(0));
    server
        .apply_changes(
            initial_cs,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();

    // Guard: server has the row with status=active
    let server_row = server
        .point_lookup("decisions", "id", &Value::Uuid(uuid_d), server.snapshot())
        .unwrap()
        .expect("server must have decision after initial sync");
    assert_eq!(
        server_row.values.get("status").and_then(Value::as_text),
        Some("active"),
        "guard: server decision must be 'active' after initial sync"
    );

    // Edge: transition status from active → superseded (archive, not delete)
    let tx2 = edge.begin();
    edge.upsert_row(
        tx2,
        "decisions",
        "id",
        make_params(vec![
            ("id", Value::Uuid(uuid_d)),
            ("description", Value::Text("initial alert rule".into())),
            ("status", Value::Text("superseded".into())),
        ]),
    )
    .unwrap();
    edge.commit(tx2).unwrap();

    // Get changeset since the watermark (only the status transition)
    let delta_cs = edge.changes_since(lsn_after_insert);

    // CRITICAL ASSERTION: The changeset must contain the row
    assert!(
        !delta_cs.rows.is_empty(),
        "changeset must contain the status-transitioned row"
    );

    // Find our decision in the changeset
    let decision_changes: Vec<&RowChange> = delta_cs
        .rows
        .iter()
        .filter(|r| r.table == "decisions")
        .collect();
    assert_eq!(
        decision_changes.len(),
        1,
        "exactly one decision row change expected in delta"
    );

    // CRITICAL ASSERTION: values must NOT be empty
    // This pins archive-not-delete: a status transition is a row UPDATE with
    // populated values, never an empty map that could be misinterpreted as delete.
    let change = decision_changes[0];
    assert!(
        !change.values.is_empty(),
        "status-transitioned row must have populated values — empty values would \
         violate archive-not-delete (Ontology Invariant 3)"
    );

    // The values must contain the updated status
    assert_eq!(
        change.values.get("status").and_then(Value::as_text),
        Some("superseded"),
        "changeset must carry the new status value 'superseded'"
    );

    // Apply the delta to server with EdgeWins (edge is source of truth for this update)
    let result = server
        .apply_changes(
            delta_cs,
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    // Server must have applied exactly 1 change (not skipped, not double-applied)
    assert_eq!(
        result.applied_rows, 1,
        "server must apply exactly 1 status transition row"
    );
    assert_eq!(
        result.skipped_rows, 0,
        "no rows should be skipped for EdgeWins update"
    );

    // Server row must exist with updated status — NOT deleted
    let server_updated = server
        .point_lookup("decisions", "id", &Value::Uuid(uuid_d), server.snapshot())
        .unwrap()
        .expect("server row must still exist after status transition sync — archive, not delete");
    assert_eq!(
        server_updated.values.get("status").and_then(Value::as_text),
        Some("superseded"),
        "server must reflect the superseded status after sync"
    );
    assert_eq!(
        server_updated
            .values
            .get("description")
            .and_then(Value::as_text),
        Some("initial alert rule"),
        "server must retain all row fields — not just status"
    );

    // Negative assertion: a full scan of decisions must find exactly 1 row,
    // not 0 (deleted) and not 2 (duplicate from misinterpreted update)
    let all_decisions = server.scan("decisions", server.snapshot()).unwrap();
    assert_eq!(
        all_decisions.len(),
        1,
        "server must have exactly 1 decision row — archive-not-delete means the \
         row is updated in place, not deleted and re-inserted"
    );

    // CRITICAL: Pin apply_changes input behavior — an empty-values RowChange
    // must NOT delete the row. This closes the gap where Codex could make
    // changes_since() always produce populated values but still have
    // apply_changes treat empty values as a delete signal.
    let empty_values_change = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(uuid_d),
        },
        values: HashMap::new(), // deliberately empty
        deleted: false,
        lsn: Lsn(999),
    };
    let poison_cs = ChangeSet {
        rows: vec![empty_values_change],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };
    let poison_result = server
        .apply_changes(
            poison_cs,
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();

    // After applying empty-values RowChange, the row must still exist
    let after_poison = server
        .point_lookup("decisions", "id", &Value::Uuid(uuid_d), server.snapshot())
        .unwrap()
        .expect("row must survive an empty-values RowChange — empty values is NOT a delete signal");
    assert_eq!(
        after_poison.values.get("status").and_then(Value::as_text),
        Some("superseded"),
        "row must retain its status after empty-values RowChange — no silent deletion"
    );

    // The empty-values change should be skipped (no-op), not treated as a delete
    assert_eq!(
        poison_result.applied_rows, 0,
        "empty-values RowChange must be treated as no-op, not as delete"
    );
    assert_eq!(
        poison_result.skipped_rows, 1,
        "empty-values RowChange must be skipped"
    );

    // Final count: still exactly 1 row
    let final_decisions = server.scan("decisions", server.snapshot()).unwrap();
    assert_eq!(
        final_decisions.len(),
        1,
        "exactly 1 decision row must remain — empty-values must never cause deletion"
    );
}

// =========================================================================
// NT-01: push round-trip via NATS
// =========================================================================
#[tokio::test]
async fn nt_01_push_round_trip_via_nats() {
    let nats = start_nats().await;
    let client_db = setup_sync_db_with_tables(&["observations"]);

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();

    let tx = client_db.begin();
    // NT-01 edge-case data: Arabic Unicode in data field, f32 precision boundary in embedding
    client_db
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                (
                    "data",
                    Value::Text(r#"{"site":"موقع البناء","temp":25}"#.into()),
                ),
                ("source", Value::Text("cam_east".into())),
                (
                    "embedding",
                    Value::Vector(vec![1.1754944e-38_f32, 1.0 + f32::EPSILON, -0.0]),
                ),
            ]),
        )
        .unwrap();
    client_db
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_2)),
                ("data", Value::Text(r#"{"temp":28}"#.into())),
                ("source", Value::Text("cam_west".into())),
            ]),
        )
        .unwrap();
    client_db
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_3)),
                (
                    "data",
                    Value::Text(r#"{"note":"مراقبة الأمن","temp":22}"#.into()),
                ),
                ("source", Value::Text("cam_north".into())),
            ]),
        )
        .unwrap();
    client_db.commit(tx).unwrap();

    // Guard: client DB has 3 rows
    assert_eq!(
        client_db
            .scan("observations", client_db.snapshot())
            .unwrap()
            .len(),
        3
    );

    let server_db = Arc::new(setup_sync_db_with_tables(&["observations"]));
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client_db = Arc::new(client_db);
    let client = SyncClient::new(client_db.clone(), &nats.nats_url, "test_tenant");
    let result = client.push().await.unwrap();
    assert_eq!(result.applied_rows, 3);

    // Point-lookup each UUID on server with exact value checks
    let row_1 = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(uuid_1),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("server must have row for uuid_1");
    assert_eq!(
        row_1.values.get("data").and_then(Value::as_text),
        Some(r#"{"site":"موقع البناء","temp":25}"#),
        "Arabic Unicode data must survive NATS round-trip"
    );
    assert_eq!(
        row_1.values.get("source").and_then(Value::as_text),
        Some("cam_east")
    );
    // Verify f32 precision edge cases in the embedding
    if let Some(Value::Vector(v)) = row_1.values.get("embedding") {
        assert_eq!(v[0], 1.1754944e-38_f32, "smallest normal f32 must survive");
        assert_eq!(v[1], 1.0 + f32::EPSILON, "f32 near epsilon must survive");
        assert!(
            v[2].is_sign_negative() || v[2] == 0.0,
            "negative zero must survive as zero"
        );
    } else {
        panic!("embedding must be present as Vector");
    }

    let row_2 = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(uuid_2),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("server must have row for uuid_2");
    assert_eq!(
        row_2.values.get("data").and_then(Value::as_text),
        Some(r#"{"temp":28}"#)
    );
    assert_eq!(
        row_2.values.get("source").and_then(Value::as_text),
        Some("cam_west")
    );

    let row_3 = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(uuid_3),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("server must have row for uuid_3");
    assert_eq!(
        row_3.values.get("data").and_then(Value::as_text),
        Some(r#"{"note":"مراقبة الأمن","temp":22}"#),
        "Arabic Unicode data must survive NATS round-trip"
    );
    assert_eq!(
        row_3.values.get("source").and_then(Value::as_text),
        Some("cam_north")
    );
}

// =========================================================================
// NT-02: pull round-trip via NATS
// =========================================================================
#[tokio::test]
async fn nt_02_pull_round_trip_via_nats() {
    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE patterns (id UUID PRIMARY KEY, pattern_type TEXT, confidence REAL)",
            &params,
        )
        .unwrap();

    let mut pat_uuids = Vec::new();
    let tx = server_db.begin();
    for _i in 0..5 {
        let id = Uuid::new_v4();
        server_db
            .insert_row(
                tx,
                "patterns",
                make_params(vec![
                    ("id", Value::Uuid(id)),
                    ("pattern_type", Value::Text("correlation".into())),
                    ("confidence", Value::Float64(0.8)),
                ]),
            )
            .unwrap();
        pat_uuids.push(id);
    }
    server_db.commit(tx).unwrap();

    // Guard: server has 5 patterns
    assert_eq!(
        server_db
            .scan("patterns", server_db.snapshot())
            .unwrap()
            .len(),
        5
    );

    // Client DB starts WITHOUT the patterns table — DDL sync must create it
    let client_db = Arc::new(Database::open_memory());

    // Guard: client has no patterns table
    assert!(
        !client_db.table_names().contains(&"patterns".to_string()),
        "guard: client must NOT have patterns table before pull"
    );

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(client_db.clone(), &nats.nats_url, "test_tenant");
    let _result = client.pull(&policies).await.unwrap();

    // Table was created via DDL sync
    assert!(
        client_db.table_names().contains(&"patterns".to_string()),
        "DDL sync must create patterns table on edge"
    );

    // All 5 rows arrived with field-level checks
    for uuid in &pat_uuids {
        let row = client_db
            .point_lookup("patterns", "id", &Value::Uuid(*uuid), client_db.snapshot())
            .unwrap()
            .expect("client must have pattern row after NATS pull");
        assert_eq!(
            row.values.get("pattern_type").and_then(Value::as_text),
            Some("correlation")
        );
        assert_eq!(
            row.values.get("confidence"),
            Some(&Value::Float64(0.8)),
            "confidence must be 0.8"
        );
    }
}

// =========================================================================
// NT-03: bidirectional sync via NATS
// =========================================================================
#[tokio::test]
async fn nt_03_bidirectional_sync_via_nats() {
    let nats = start_nats().await;
    let edge_db = Arc::new(setup_sync_db_with_tables(&["items"]));
    let server_db = Arc::new(setup_sync_db_with_tables(&["items"]));

    let uuid_a = Uuid::new_v4();
    let uuid_b = Uuid::new_v4();
    let uuid_c = Uuid::new_v4();
    let uuid_d = Uuid::new_v4();

    // Edge has A, B
    let tx_e = edge_db.begin();
    edge_db
        .insert_row(
            tx_e,
            "items",
            make_params(vec![
                ("id", Value::Uuid(uuid_a)),
                ("name", Value::Text("obs_a".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx_e,
            "items",
            make_params(vec![
                ("id", Value::Uuid(uuid_b)),
                ("name", Value::Text("obs_b".into())),
            ]),
        )
        .unwrap();
    edge_db.commit(tx_e).unwrap();

    // Server has C, D
    let tx_s = server_db.begin();
    server_db
        .insert_row(
            tx_s,
            "items",
            make_params(vec![
                ("id", Value::Uuid(uuid_c)),
                ("name", Value::Text("pattern_c".into())),
            ]),
        )
        .unwrap();
    server_db
        .insert_row(
            tx_s,
            "items",
            make_params(vec![
                ("id", Value::Uuid(uuid_d)),
                ("name", Value::Text("pattern_d".into())),
            ]),
        )
        .unwrap();
    server_db.commit(tx_s).unwrap();

    // Guards
    assert_eq!(edge_db.scan("items", edge_db.snapshot()).unwrap().len(), 2);
    assert_eq!(
        server_db.scan("items", server_db.snapshot()).unwrap().len(),
        2
    );

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");

    // Push: edge -> server
    client.push().await.unwrap();
    // Pull: server -> edge
    client.pull(&policies).await.unwrap();

    // Both sides have all 4
    let edge_rows = edge_db.scan("items", edge_db.snapshot()).unwrap();
    let server_rows = server_db.scan("items", server_db.snapshot()).unwrap();
    assert_eq!(edge_rows.len(), 4);
    assert_eq!(server_rows.len(), 4);

    let expected = HashSet::from([uuid_a, uuid_b, uuid_c, uuid_d]);
    let edge_uuids: HashSet<Uuid> = edge_rows
        .iter()
        .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
        .collect();
    let server_uuids: HashSet<Uuid> = server_rows
        .iter()
        .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
        .collect();
    assert_eq!(edge_uuids, expected, "edge must have all 4 UUIDs");
    assert_eq!(server_uuids, expected, "server must have all 4 UUIDs");
}

// =========================================================================
// NT-04: chunking (payload > 1MB)
// =========================================================================
#[tokio::test]
async fn nt_04_chunking_large_vector_payload() {
    let nats = start_nats().await;
    let edge_db = Arc::new(setup_sync_db_with_tables(&["observations_384"]));

    // Generate a known vector for later search assertion
    let known_vector: Vec<f32> = (0..384).map(|i| (i as f32) / 384.0).collect();

    let tx = edge_db.begin();
    for i in 0..200 {
        let uuid = Uuid::new_v4();
        let vec = if i == 0 {
            known_vector.clone()
        } else {
            // Generate distinct random-ish vectors
            (0..384).map(|j| ((i * 384 + j) as f32).sin()).collect()
        };
        let row_id = edge_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(uuid)),
                    ("embedding", Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        edge_db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    edge_db.commit(tx).unwrap();

    // Guard: edge has 200 rows
    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        200
    );

    let server_db = Arc::new(setup_sync_db_with_tables(&["observations_384"]));
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    let result = client.push().await.unwrap();
    assert_eq!(result.applied_rows, 200);

    // All 200 rows arrived
    let all = server_db
        .scan("observations", server_db.snapshot())
        .unwrap();
    assert_eq!(all.len(), 200, "server must have all 200 rows after push");

    // Vector search on server finds the planted vector (precision check)
    let search_result = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &known_vector,
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(search_result.len(), 1);
    assert!(
        search_result[0].1 > 0.999,
        "planted vector must be found with near-perfect cosine similarity, got {}",
        search_result[0].1
    );
}

// =========================================================================
// NT-05: Reconnection after NATS restart
//
// TODO(sync): This test currently validates watermark-based delta push
// (only new rows sent on second push). The full NT-05 scenario requires:
//   1. Push batch_1 (succeeds, watermark advances)
//   2. Insert batch_2
//   3. Stop NATS container
//   4. Push fails (assert Err — connection error)
//   5. Watermark does NOT advance on failed push
//   6. Restart NATS container
//   7. Push succeeds — sends batch_2 only (watermark-based delta)
//   8. No duplicate rows on server
// Steps 3-6 are deferred until testcontainers or Docker helper is available.
// =========================================================================
#[tokio::test]
async fn nt_05_reconnection_after_nats_restart() {
    let nats = start_nats().await;
    let edge_db = Arc::new(setup_sync_db_with_tables(&["observations_text"]));

    let uuid_1 = Uuid::new_v4();
    let uuid_2 = Uuid::new_v4();
    let uuid_3 = Uuid::new_v4();

    // Insert first batch
    let tx1 = edge_db.begin();
    edge_db
        .insert_row(
            tx1,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                ("data", Value::Text("batch_1_a".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx1,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_2)),
                ("data", Value::Text("batch_1_b".into())),
            ]),
        )
        .unwrap();
    edge_db.commit(tx1).unwrap();

    let server_db = Arc::new(setup_sync_db_with_tables(&["observations_text"]));
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");

    // First push succeeds
    let result1 = client.push().await.unwrap();
    assert_eq!(result1.applied_rows, 2);
    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        2
    );

    // Insert second batch
    let tx2 = edge_db.begin();
    edge_db
        .insert_row(
            tx2,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_3)),
                ("data", Value::Text("batch_2_a".into())),
            ]),
        )
        .unwrap();
    edge_db.commit(tx2).unwrap();

    // The push below simulates the retry after reconnection.

    // Push again — should only add the new row from batch_2
    let result2 = client.push().await.unwrap();
    assert_eq!(
        result2.applied_rows, 1,
        "reconnected push must only send the new row"
    );
    let total = server_db
        .scan("observations", server_db.snapshot())
        .unwrap()
        .len();
    assert_eq!(total, 3, "server must have 3 rows (not 5 — no duplicates)");
}

// =========================================================================
// NT-06: initial sync (empty edge)
// =========================================================================
#[tokio::test]
async fn nt_06_initial_sync_empty_edge() {
    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();

    // Server has 3 tables
    server_db
        .execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT)",
            &params,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT)",
            &params,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, embedding VECTOR(3)) IMMUTABLE",
            &params,
        )
        .unwrap();

    let known_entity = Uuid::new_v4();
    let known_decision = Uuid::new_v4();

    let tx = server_db.begin();
    // Insert entities
    server_db
        .insert_row(
            tx,
            "entities",
            make_params(vec![
                ("id", Value::Uuid(known_entity)),
                ("name", Value::Text("Camera-1".into())),
            ]),
        )
        .unwrap();
    for _ in 0..19 {
        server_db
            .insert_row(
                tx,
                "entities",
                make_params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("name", Value::Text("Camera-X".into())),
                ]),
            )
            .unwrap();
    }
    // Insert decisions
    server_db
        .insert_row(
            tx,
            "decisions",
            make_params(vec![
                ("id", Value::Uuid(known_decision)),
                ("description", Value::Text("alert rule".into())),
            ]),
        )
        .unwrap();
    for _ in 0..9 {
        server_db
            .insert_row(
                tx,
                "decisions",
                make_params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("description", Value::Text("other rule".into())),
                ]),
            )
            .unwrap();
    }
    // Insert observations with vectors
    for i in 0..20 {
        let obs_id = Uuid::new_v4();
        let vec = vec![(i as f32) / 20.0, 1.0 - (i as f32) / 20.0, 0.5];
        let row_id = server_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(obs_id)),
                    ("embedding", Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        server_db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    // Graph edges
    server_db
        .insert_edge(
            tx,
            known_decision,
            known_entity,
            "BASED_ON".to_string(),
            HashMap::new(),
        )
        .unwrap();
    for _ in 0..9 {
        let d = Uuid::new_v4();
        let e = Uuid::new_v4();
        server_db
            .insert_edge(tx, d, e, "SERVES".to_string(), HashMap::new())
            .unwrap();
    }
    server_db.commit(tx).unwrap();

    // Guard: server state
    assert_eq!(
        server_db
            .scan("entities", server_db.snapshot())
            .unwrap()
            .len(),
        20
    );
    assert_eq!(
        server_db
            .scan("decisions", server_db.snapshot())
            .unwrap()
            .len(),
        10
    );
    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        20
    );

    // Edge starts completely empty
    let edge_db = Arc::new(Database::open_memory());
    assert!(
        edge_db.table_names().is_empty(),
        "guard: edge must start with no tables"
    );

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    let _result = client.initial_sync(&policies).await.unwrap();

    // DDL synced — all 3 tables created
    let edge_tables = edge_db.table_names();
    assert!(edge_tables.contains(&"entities".to_string()));
    assert!(edge_tables.contains(&"decisions".to_string()));
    assert!(edge_tables.contains(&"observations".to_string()));

    // Row counts match
    assert_eq!(
        edge_db.scan("entities", edge_db.snapshot()).unwrap().len(),
        20
    );
    assert_eq!(
        edge_db.scan("decisions", edge_db.snapshot()).unwrap().len(),
        10
    );
    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        20
    );

    // Graph intact: BFS from decision finds entity
    let bfs_edge = edge_db
        .query_bfs(
            known_decision,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            edge_db.snapshot(),
        )
        .unwrap();
    let bfs_server = server_db
        .query_bfs(
            known_decision,
            Some(&["BASED_ON".to_string()]),
            Direction::Outgoing,
            1,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        bfs_edge.nodes.len(),
        bfs_server.nodes.len(),
        "BFS results must match between edge and server"
    );

    // Vectors intact: same top-3 ANN results
    let query_vec = [1.0_f32, 0.0, 0.0];
    let top3_edge = edge_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &query_vec,
            3,
            None,
            edge_db.snapshot(),
        )
        .unwrap();
    let top3_server = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &query_vec,
            3,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    // Compare similarity scores, NOT row_ids — row_ids are instance-local
    // and will differ between edge and server (design deviation #3)
    let edge_scores: Vec<f32> = top3_edge.iter().map(|r| r.1).collect();
    let server_scores: Vec<f32> = top3_server.iter().map(|r| r.1).collect();
    assert_eq!(
        edge_scores.len(),
        server_scores.len(),
        "edge and server must return same number of top-3 vector results"
    );
    for (i, (e, s)) in edge_scores.iter().zip(&server_scores).enumerate() {
        assert!(
            (e - s).abs() < 1e-6,
            "top-3 similarity score {i} must match: edge={e}, server={s}"
        );
    }
}

// =========================================================================
// NT-07: offline accumulate + batch sync
// =========================================================================
#[tokio::test]
async fn nt_07_offline_accumulate_batch_sync() {
    let nats = start_nats().await;
    let edge_db = Arc::new(setup_sync_db_with_tables(&["observations_seq"]));

    // Simulate 8 hours of offline recording: 1000 observations across separate txns
    let mut uuids = Vec::with_capacity(1000);
    for i in 0..1000 {
        let id = Uuid::new_v4();
        let tx = edge_db.begin();
        edge_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(id)),
                    ("seq", Value::Int64(i)),
                    ("data", Value::Text(format!("event_{}", i))),
                ]),
            )
            .unwrap();
        edge_db.commit(tx).unwrap();
        uuids.push(id);
    }

    // Guard: edge has 1000 rows
    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        1000
    );

    // Guard: changes_since(0) returns all 1000
    let cs = edge_db.changes_since(Lsn(0));
    assert_eq!(
        cs.rows.len(),
        1000,
        "changes_since(0) must return all 1000 observations without truncation"
    );

    let server_db = Arc::new(setup_sync_db_with_tables(&["observations_seq"]));
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    let result = client.push().await.unwrap();
    assert_eq!(result.applied_rows, 1000);

    // All 1000 arrived
    let all = server_db
        .scan("observations", server_db.snapshot())
        .unwrap();
    assert_eq!(all.len(), 1000, "server must have all 1000 observations");

    // LSN ordering preserved — seq values must be in ascending order
    let server_cs = server_db.changes_since(Lsn(0));
    let seqs: Vec<i64> = server_cs
        .rows
        .iter()
        .filter(|r| r.table == "observations")
        .filter_map(|r| r.values.get("seq").and_then(Value::as_i64))
        .collect();
    for window in seqs.windows(2) {
        assert!(
            window[0] <= window[1],
            "LSN ordering must be preserved: seq {} must come before {}",
            window[0],
            window[1]
        );
    }
}

// =========================================================================
// NT-08: DDL sync
// =========================================================================
#[tokio::test]
async fn nt_08_ddl_sync_with_state_machine() {
    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();

    // Server creates a table with STATE MACHINE constraint
    server_db
        .execute(
            "CREATE TABLE configs (id UUID PRIMARY KEY, name TEXT, status TEXT) STATE MACHINE (status: active -> [deactivated], deactivated -> [active])",
            &params,
        )
        .unwrap();

    let uuid_1 = Uuid::new_v4();
    let tx = server_db.begin();
    server_db
        .insert_row(
            tx,
            "configs",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                ("name", Value::Text("alert_rule".into())),
                ("status", Value::Text("active".into())),
            ]),
        )
        .unwrap();
    server_db.commit(tx).unwrap();

    // Guard: server has the table with state machine
    let meta = server_db.table_meta("configs").unwrap();
    assert!(
        meta.state_machine.is_some(),
        "guard: server configs table must have state machine"
    );

    let edge_db = Arc::new(Database::open_memory());
    // Guard: edge has NO configs table
    assert!(!edge_db.table_names().contains(&"configs".to_string()));

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    client.pull(&policies).await.unwrap();

    // Table exists with state machine
    assert!(edge_db.table_names().contains(&"configs".to_string()));
    let edge_meta = edge_db.table_meta("configs").unwrap();
    assert!(
        edge_meta.state_machine.is_some(),
        "DDL sync must transfer state machine constraint"
    );
    let sm = edge_meta.state_machine.unwrap();
    assert_eq!(sm.column, "status");
    assert!(
        sm.transitions
            .get("active")
            .unwrap()
            .contains(&"deactivated".to_string())
    );
    assert!(
        sm.transitions
            .get("deactivated")
            .unwrap()
            .contains(&"active".to_string())
    );

    // Row synced
    let row = edge_db
        .point_lookup("configs", "id", &Value::Uuid(uuid_1), edge_db.snapshot())
        .unwrap()
        .expect("row must be synced");
    assert_eq!(
        row.values.get("status").and_then(Value::as_text),
        Some("active")
    );

    // State machine enforced: valid transition (active -> deactivated)
    let tx2 = edge_db.begin();
    edge_db
        .upsert_row(
            tx2,
            "configs",
            "id",
            make_params(vec![
                ("id", Value::Uuid(uuid_1)),
                ("name", Value::Text("alert_rule".into())),
                ("status", Value::Text("deactivated".into())),
            ]),
        )
        .unwrap();
    edge_db.commit(tx2).unwrap();

    // State machine enforced: INVALID transition (deactivated -> "bogus")
    let tx3 = edge_db.begin();
    let invalid_result = edge_db.upsert_row(
        tx3,
        "configs",
        "id",
        make_params(vec![
            ("id", Value::Uuid(uuid_1)),
            ("name", Value::Text("alert_rule".into())),
            ("status", Value::Text("bogus".into())),
        ]),
    );
    assert!(
        invalid_result.is_err(),
        "invalid state transition must fail on synced table"
    );
}

// =========================================================================
// NT-09: cross-edge embedding clustering
// =========================================================================
#[tokio::test]
async fn nt_09_cross_edge_embedding_clustering() {
    let nats = start_nats().await;
    // 3 edges with similar face embeddings push to 1 server
    let base_embedding = vec![0.9f32, 0.1, 0.1];
    let edge_embeddings = [
        vec![0.91f32, 0.09, 0.11],
        vec![0.89, 0.12, 0.10],
        vec![0.90, 0.10, 0.09],
    ];
    let context_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let mut obs_uuids = Vec::new();

    let server_db = Arc::new(Database::open_memory());
    server_db
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, source TEXT, context_id UUID, embedding VECTOR(3)) IMMUTABLE",
            &HashMap::new(),
        )
        .unwrap();

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Set up 3 edge DBs and push each
    for i in 0..3 {
        let edge_db = Arc::new(Database::open_memory());
        let params = HashMap::new();
        edge_db
            .execute(
                "CREATE TABLE observations (id UUID PRIMARY KEY, source TEXT, context_id UUID, embedding VECTOR(3)) IMMUTABLE",
                &params,
            )
            .unwrap();

        let obs_id = Uuid::new_v4();
        let tx = edge_db.begin();
        let row_id = edge_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(obs_id)),
                    ("source", Value::Text(format!("cam_{}", i))),
                    ("context_id", Value::Uuid(context_ids[i])),
                    ("embedding", Value::Vector(edge_embeddings[i].clone())),
                ]),
            )
            .unwrap();
        edge_db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                edge_embeddings[i].clone(),
            )
            .unwrap();
        edge_db.commit(tx).unwrap();
        obs_uuids.push(obs_id);

        // Guard: each edge has 1 observation
        assert_eq!(
            edge_db
                .scan("observations", edge_db.snapshot())
                .unwrap()
                .len(),
            1
        );

        let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
        client.push().await.unwrap();
    }

    // Server now has 3 observations from 3 edges
    let all_obs = server_db
        .scan("observations", server_db.snapshot())
        .unwrap();
    assert_eq!(
        all_obs.len(),
        3,
        "server must have 3 observations from 3 edges"
    );

    // Cross-context vector query finds cluster
    let search_result = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &base_embedding,
            3,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        search_result.len(),
        3,
        "ANN search must find all 3 similar embeddings"
    );

    // All results should have high cosine similarity (similar vectors)
    for (_, similarity) in &search_result {
        assert!(
            *similarity > 0.85,
            "all 3 embeddings must have cosine similarity > 0.85 with base, got {}",
            similarity
        );
    }

    // Verify that result row_ids map back to the 3 pushed observations.
    // Since the server has exactly 3 observations total and we search with k=3,
    // all results must be the 3 pushed observations — no other rows exist.
    let result_row_ids: HashSet<u64> = search_result.iter().map(|(rid, _)| rid.0).collect();
    assert_eq!(
        result_row_ids.len(),
        3,
        "all 3 search results must be distinct rows"
    );
}

// =========================================================================
// NT-10: invalidation sync round-trip
// =========================================================================
#[tokio::test]
async fn nt_10_invalidation_sync_round_trip() {
    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();

    for db in [&edge_db, &server_db] {
        db.execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &params,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT)",
            &params,
        )
        .unwrap();
        db.execute(
            "CREATE TABLE invalidations (id UUID PRIMARY KEY, affected_decision_id UUID, trigger_observation_id UUID, basis_diff TEXT, status TEXT, severity TEXT, resolution_decision_id UUID) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved, dismissed])",
            &params,
        ).unwrap();
    }

    let obs_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();
    let inv_id = Uuid::new_v4();

    // Edge: create observation, decision, and invalidation with graph edges
    let tx = edge_db.begin();
    edge_db
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(obs_id)),
                ("data", Value::Text("contradicting evidence".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx,
            "decisions",
            make_params(vec![
                ("id", Value::Uuid(decision_id)),
                ("description", Value::Text("original alert rule".into())),
                ("status", Value::Text("active".into())),
            ]),
        )
        .unwrap();
    edge_db
        .insert_row(
            tx,
            "invalidations",
            make_params(vec![
                ("id", Value::Uuid(inv_id)),
                ("affected_decision_id", Value::Uuid(decision_id)),
                ("trigger_observation_id", Value::Uuid(obs_id)),
                (
                    "basis_diff",
                    Value::Text(
                        r#"{"field": "location", "expected": "A", "observed": "B"}"#.into(),
                    ),
                ),
                ("status", Value::Text("pending".into())),
                ("severity", Value::Text("high".into())),
            ]),
        )
        .unwrap();
    // Graph edges: TRIGGERED_BY and INVALIDATES
    edge_db
        .insert_edge(
            tx,
            obs_id,
            decision_id,
            "TRIGGERED_BY".to_string(),
            HashMap::new(),
        )
        .unwrap();
    edge_db
        .insert_edge(
            tx,
            inv_id,
            decision_id,
            "INVALIDATES".to_string(),
            HashMap::new(),
        )
        .unwrap();
    edge_db.commit(tx).unwrap();

    // Guard: edge has the full invalidation graph
    assert!(
        edge_db
            .point_lookup(
                "invalidations",
                "id",
                &Value::Uuid(inv_id),
                edge_db.snapshot()
            )
            .unwrap()
            .is_some()
    );
    let triggered_by = edge_db
        .query_bfs(
            obs_id,
            Some(&["TRIGGERED_BY".to_string()]),
            Direction::Outgoing,
            1,
            edge_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        triggered_by.nodes.len(),
        1,
        "guard: TRIGGERED_BY edge must exist"
    );
    let invalidates = edge_db
        .query_bfs(
            inv_id,
            Some(&["INVALIDATES".to_string()]),
            Direction::Outgoing,
            1,
            edge_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        invalidates.nodes.len(),
        1,
        "guard: INVALIDATES edge must exist"
    );

    // Guard: server starts empty
    assert_eq!(
        server_db
            .scan("invalidations", server_db.snapshot())
            .unwrap()
            .len(),
        0
    );

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");

    // Push to server
    client.push().await.unwrap();

    // Verify server has invalidation with correct fields
    let inv = server_db
        .point_lookup(
            "invalidations",
            "id",
            &Value::Uuid(inv_id),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("server must have invalidation");
    assert_eq!(
        inv.values.get("status").and_then(Value::as_text),
        Some("pending")
    );
    assert_eq!(
        inv.values.get("severity").and_then(Value::as_text),
        Some("high")
    );

    // Assert basis_diff contains "location"
    let basis_diff = inv
        .values
        .get("basis_diff")
        .and_then(Value::as_text)
        .expect("basis_diff must be present");
    assert!(
        basis_diff.contains("location"),
        "basis_diff must contain 'location'"
    );

    // Verify graph edges synced
    let srv_triggered = server_db
        .query_bfs(
            obs_id,
            Some(&["TRIGGERED_BY".to_string()]),
            Direction::Outgoing,
            1,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        srv_triggered.nodes.len(),
        1,
        "TRIGGERED_BY must sync to server"
    );
    assert_eq!(srv_triggered.nodes[0].id, decision_id);

    let srv_invalidates = server_db
        .query_bfs(
            inv_id,
            Some(&["INVALIDATES".to_string()]),
            Direction::Outgoing,
            1,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        srv_invalidates.nodes.len(),
        1,
        "INVALIDATES must sync to server"
    );

    // Server resolves the invalidation (pending -> acknowledged -> resolved)
    let resolution_id = Uuid::new_v4();
    let tx_s = server_db.begin();
    server_db
        .upsert_row(
            tx_s,
            "invalidations",
            "id",
            make_params(vec![
                ("id", Value::Uuid(inv_id)),
                ("affected_decision_id", Value::Uuid(decision_id)),
                ("trigger_observation_id", Value::Uuid(obs_id)),
                (
                    "basis_diff",
                    Value::Text(
                        r#"{"field": "location", "expected": "A", "observed": "B"}"#.into(),
                    ),
                ),
                ("status", Value::Text("acknowledged".into())),
                ("severity", Value::Text("high".into())),
            ]),
        )
        .unwrap();
    server_db.commit(tx_s).unwrap();

    let tx_s2 = server_db.begin();
    server_db
        .upsert_row(
            tx_s2,
            "invalidations",
            "id",
            make_params(vec![
                ("id", Value::Uuid(inv_id)),
                ("affected_decision_id", Value::Uuid(decision_id)),
                ("trigger_observation_id", Value::Uuid(obs_id)),
                (
                    "basis_diff",
                    Value::Text(
                        r#"{"field": "location", "expected": "A", "observed": "B"}"#.into(),
                    ),
                ),
                ("status", Value::Text("resolved".into())),
                ("severity", Value::Text("high".into())),
                ("resolution_decision_id", Value::Uuid(resolution_id)),
            ]),
        )
        .unwrap();
    server_db.commit(tx_s2).unwrap();

    // Edge pulls resolution
    let pull_policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    client.pull(&pull_policies).await.unwrap();

    let inv_edge = edge_db
        .point_lookup(
            "invalidations",
            "id",
            &Value::Uuid(inv_id),
            edge_db.snapshot(),
        )
        .unwrap()
        .expect("edge must still have invalidation");
    assert_eq!(
        inv_edge.values.get("status").and_then(Value::as_text),
        Some("resolved"),
        "edge must receive server's resolved status"
    );
    assert_eq!(
        inv_edge
            .values
            .get("resolution_decision_id")
            .and_then(Value::as_uuid),
        Some(&resolution_id),
        "edge must receive resolution_decision_id"
    );
}

// =========================================================================
// NT-11: push large mixed payload (400 rows × 384-dim vectors + 4KB text)
// Distinct from nt_04: uses text+vectors to guarantee >1MB total wire size
// =========================================================================
#[tokio::test]
async fn nt_11_push_large_mixed_payload_chunked() {
    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let params = HashMap::new();
    edge_db
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(384)) IMMUTABLE",
            &params,
        )
        .unwrap();

    let known_vector: Vec<f32> = (0..384).map(|i| (i as f32) / 384.0).collect();
    let mut known_uuid: Option<Uuid> = None;

    let tx = edge_db.begin();
    for i in 0..400usize {
        let uuid = Uuid::new_v4();
        if i == 0 {
            known_uuid = Some(uuid);
        }
        let vec: Vec<f32> = if i == 0 {
            known_vector.clone()
        } else {
            (0..384).map(|j| ((i * 384 + j) as f32).sin()).collect()
        };
        let row_id = edge_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(uuid)),
                    ("data", Value::Text("x".repeat(4_000))),
                    ("embedding", Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        edge_db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    edge_db.commit(tx).unwrap();

    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        400
    );

    {
        use contextdb_server::protocol::WireChangeSet;
        let wire = WireChangeSet::from(edge_db.changes_since(Lsn(0)));
        let encoded_len = rmp_serde::to_vec(&wire).unwrap().len();
        assert!(
            encoded_len > 1_048_576,
            "guard: changeset must exceed 1MB to validate chunking, got {} bytes",
            encoded_len
        );
    }

    let server_db = Arc::new(Database::open_memory());
    server_db
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(384)) IMMUTABLE",
            &params,
        )
        .unwrap();
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    let result = client.push().await.unwrap();

    assert_eq!(
        result.applied_rows, 400,
        "all 400 rows must reach the server"
    );
    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        400,
        "server must have all 400 rows"
    );

    let row = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(known_uuid.unwrap()),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("planted row must be present on server");
    assert_eq!(
        row.values.get("data").and_then(Value::as_text),
        Some("x".repeat(4_000).as_str()),
        "text content must arrive intact"
    );

    let search = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &known_vector,
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(search.len(), 1);
    assert!(
        search[0].1 > 0.999,
        "planted vector must be found with cosine > 0.999, got {}",
        search[0].1
    );
}

// =========================================================================
// NT-12: push single row with 1.1MB text blob, requires chunking
// =========================================================================
#[tokio::test]
async fn nt_12_push_single_oversized_text_blob() {
    let nats = start_nats().await;
    let edge_db = Arc::new(setup_sync_db_with_tables(&["observations_text"]));

    let blob = "x".repeat(1_126_400);
    let uuid_blob = Uuid::new_v4();

    let tx = edge_db.begin();
    edge_db
        .insert_row(
            tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(uuid_blob)),
                ("data", Value::Text(blob.clone())),
            ]),
        )
        .unwrap();
    edge_db.commit(tx).unwrap();

    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        1
    );

    {
        use contextdb_server::protocol::WireChangeSet;
        let wire = WireChangeSet::from(edge_db.changes_since(Lsn(0)));
        let encoded_len = rmp_serde::to_vec(&wire).unwrap().len();
        assert!(
            encoded_len > 1_048_576,
            "guard: 1-row blob changeset must exceed 1MB, got {} bytes",
            encoded_len
        );
    }

    let server_db = Arc::new(setup_sync_db_with_tables(&["observations_text"]));
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies,
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    let result = client.push().await.unwrap();

    assert_eq!(result.applied_rows, 1, "the blob row must reach the server");

    let row = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(uuid_blob),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("blob row must be present on server");

    assert_eq!(
        row.values.get("data").and_then(Value::as_text),
        Some(blob.as_str()),
        "blob must arrive byte-for-byte intact — full content equality"
    );
    assert!(result.conflicts.is_empty(), "no conflicts expected");
}

// =========================================================================
// NT-13: pull large server-side dataset (600 rows × 2.5KB text), requires chunking
// Uses 600 rows (not 500) to avoid PULL_PAGE_SIZE=500 exact boundary
// =========================================================================
#[tokio::test]
async fn nt_13_pull_large_dataset_chunked() {
    let nats = start_nats().await;
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();
    server_db
        .execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT) IMMUTABLE",
            &params,
        )
        .unwrap();

    let mut sentinel_uuids: Vec<Uuid> = Vec::new();
    let tx = server_db.begin();
    for i in 0..600usize {
        let id = Uuid::new_v4();
        if i % 60 == 0 {
            sentinel_uuids.push(id);
        }
        server_db
            .insert_row(
                tx,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(id)),
                    ("data", Value::Text("x".repeat(2_500))),
                ]),
            )
            .unwrap();
    }
    server_db.commit(tx).unwrap();

    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        600
    );

    {
        use contextdb_server::protocol::WireChangeSet;
        let wire = WireChangeSet::from(server_db.changes_since(Lsn(0)));
        let encoded_len = rmp_serde::to_vec(&wire).unwrap().len();
        assert!(
            encoded_len > 1_048_576,
            "guard: 600-row server changeset must exceed 1MB, got {} bytes",
            encoded_len
        );
    }

    let edge_db = Arc::new(Database::open_memory());
    assert!(
        edge_db.table_names().is_empty(),
        "guard: edge must start with no tables"
    );

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");
    client.pull(&policies).await.unwrap();

    assert!(
        edge_db.table_names().contains(&"observations".to_string()),
        "DDL must be synced — edge must have observations table"
    );
    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        600,
        "all 600 rows must arrive at edge"
    );

    let expected_text = "x".repeat(2_500);
    for uuid in &sentinel_uuids {
        let row = edge_db
            .point_lookup(
                "observations",
                "id",
                &Value::Uuid(*uuid),
                edge_db.snapshot(),
            )
            .unwrap()
            .unwrap_or_else(|| panic!("sentinel row {} must be present on edge", uuid));
        assert_eq!(
            row.values.get("data").and_then(Value::as_text),
            Some(expected_text.as_str()),
            "sentinel blob must arrive byte-for-byte intact for row {}",
            uuid
        );
    }
}

// =========================================================================
// NT-14: large bidirectional sync — push 400 rows (vectors+text), pull 400 rows
// Uses same schema as NT-11 to guarantee >1MB per direction
// =========================================================================
#[tokio::test]
async fn nt_14_large_bidirectional_vector_sync() {
    let nats = start_nats().await;
    let edge_db = Arc::new(Database::open_memory());
    let server_db = Arc::new(Database::open_memory());
    let params = HashMap::new();
    let ddl = "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(384)) IMMUTABLE";
    edge_db.execute(ddl, &params).unwrap();
    server_db.execute(ddl, &params).unwrap();

    let known_edge_vector: Vec<f32> = (0..384).map(|i| (i as f32) / 384.0).collect();
    let known_server_vector: Vec<f32> = (0..384).map(|i| 1.0 - (i as f32) / 384.0).collect();
    let mut known_edge_uuid: Option<Uuid> = None;

    let tx_e = edge_db.begin();
    for i in 0..400usize {
        let uuid = Uuid::new_v4();
        if i == 0 {
            known_edge_uuid = Some(uuid);
        }
        let vec: Vec<f32> = if i == 0 {
            known_edge_vector.clone()
        } else {
            (0..384).map(|j| ((i * 384 + j) as f32).sin()).collect()
        };
        let row_id = edge_db
            .insert_row(
                tx_e,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(uuid)),
                    ("data", Value::Text("x".repeat(4_000))),
                    ("embedding", Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        edge_db
            .insert_vector(
                tx_e,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    edge_db.commit(tx_e).unwrap();

    let tx_s = server_db.begin();
    for i in 0..400usize {
        let uuid = Uuid::new_v4();
        let vec: Vec<f32> = if i == 0 {
            known_server_vector.clone()
        } else {
            (0..384)
                .map(|j| ((i * 384 + j + 1_000) as f32).cos())
                .collect()
        };
        let row_id = server_db
            .insert_row(
                tx_s,
                "observations",
                make_params(vec![
                    ("id", Value::Uuid(uuid)),
                    ("data", Value::Text("x".repeat(4_000))),
                    ("embedding", Value::Vector(vec.clone())),
                ]),
            )
            .unwrap();
        server_db
            .insert_vector(
                tx_s,
                contextdb_core::VectorIndexRef::new("observations", "embedding"),
                row_id,
                vec,
            )
            .unwrap();
    }
    server_db.commit(tx_s).unwrap();

    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        400,
        "guard: edge starts with 400 rows"
    );
    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        400,
        "guard: server starts with 400 rows"
    );

    {
        use contextdb_server::protocol::WireChangeSet;
        let wire = WireChangeSet::from(edge_db.changes_since(Lsn(0)));
        let encoded_len = rmp_serde::to_vec(&wire).unwrap().len();
        assert!(
            encoded_len > 1_048_576,
            "guard: edge changeset must exceed 1MB, got {} bytes",
            encoded_len
        );
    }

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "test_tenant",
        policies.clone(),
    ));
    let server_handle = server.clone();
    tokio::spawn(async move { server_handle.run().await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "test_tenant");

    let push_result = client.push().await.unwrap();
    assert_eq!(
        push_result.applied_rows, 400,
        "push must deliver all 400 edge rows to server"
    );

    client.pull(&policies).await.unwrap();

    assert_eq!(
        server_db
            .scan("observations", server_db.snapshot())
            .unwrap()
            .len(),
        800,
        "server must have 800 rows after push"
    );
    assert_eq!(
        edge_db
            .scan("observations", edge_db.snapshot())
            .unwrap()
            .len(),
        800,
        "edge must have 800 rows after pull"
    );

    let push_search = server_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &known_edge_vector,
            1,
            None,
            server_db.snapshot(),
        )
        .unwrap();
    assert_eq!(push_search.len(), 1);
    assert!(
        push_search[0].1 > 0.999,
        "edge's planted vector must survive push, similarity={}",
        push_search[0].1
    );

    let pull_search = edge_db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &known_server_vector,
            1,
            None,
            edge_db.snapshot(),
        )
        .unwrap();
    assert_eq!(pull_search.len(), 1);
    assert!(
        pull_search[0].1 > 0.999,
        "server's planted vector must survive pull, similarity={}",
        pull_search[0].1
    );

    let edge_row_on_server = server_db
        .point_lookup(
            "observations",
            "id",
            &Value::Uuid(known_edge_uuid.unwrap()),
            server_db.snapshot(),
        )
        .unwrap()
        .expect("known edge row must be present on server after push");
    assert_eq!(
        edge_row_on_server
            .values
            .get("data")
            .and_then(Value::as_text),
        Some("x".repeat(4_000).as_str()),
        "text data must arrive intact on server after push"
    );
}

// ---------------------------------------------------------------------------
// CP — Sync Conflict Policy SQL Surface Tests
// ---------------------------------------------------------------------------

/// SET SYNC_CONFLICT_POLICY sets the default policy and SHOW returns it.
#[test]
fn cp01_set_and_show_sync_conflict_policy() {
    let db = Database::open_memory();
    db.execute("SET SYNC_CONFLICT_POLICY 'latest_wins'", &HashMap::new())
        .expect("SET must succeed");
    let result = db
        .execute("SHOW SYNC_CONFLICT_POLICY", &HashMap::new())
        .expect("SHOW must succeed");
    assert!(!result.rows.is_empty(), "SHOW must return a row");
    let policy = &result.rows[0][0];
    assert_eq!(
        *policy,
        Value::Text("latest_wins".to_string()),
        "default policy must be latest_wins"
    );
}

/// SET SYNC_CONFLICT_POLICY accepts all valid policy names.
#[test]
fn cp02_all_policy_names_accepted() {
    let db = Database::open_memory();
    for policy in &["latest_wins", "server_wins", "edge_wins"] {
        db.execute(
            &format!("SET SYNC_CONFLICT_POLICY '{policy}'"),
            &HashMap::new(),
        )
        .unwrap_or_else(|e| panic!("SET '{policy}' must succeed: {e}"));
    }
}

/// SET SYNC_CONFLICT_POLICY rejects invalid policy names.
#[test]
fn cp03_invalid_policy_rejected() {
    let db = Database::open_memory();
    let result = db.execute("SET SYNC_CONFLICT_POLICY 'bogus'", &HashMap::new());
    assert!(result.is_err(), "invalid policy name must be rejected");
}

/// ALTER TABLE SET SYNC_CONFLICT_POLICY sets a per-table override.
#[test]
fn cp04_per_table_conflict_policy() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE sensors SET SYNC_CONFLICT_POLICY 'server_wins'",
        &HashMap::new(),
    )
    .expect("ALTER TABLE SET policy must succeed");
    let result = db
        .execute("SHOW SYNC_CONFLICT_POLICY", &HashMap::new())
        .expect("SHOW must succeed");
    // SHOW must include the per-table override
    let output = format!("{:?}", result.rows);
    assert!(
        output.contains("server_wins"),
        "SHOW must reflect per-table policy, got: {output}"
    );
}

/// ALTER TABLE DROP SYNC_CONFLICT_POLICY removes a per-table override.
#[test]
fn cp05_drop_per_table_policy() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE sensors SET SYNC_CONFLICT_POLICY 'server_wins'",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE sensors DROP SYNC_CONFLICT_POLICY",
        &HashMap::new(),
    )
    .expect("DROP policy must succeed");
}

/// SET SYNC_CONFLICT_POLICY actually determines conflict resolution behavior during apply_changes.
/// This test sets 'server_wins', creates a conflict, and verifies the server's value is kept.
/// Then sets 'edge_wins' on the same scenario and verifies the edge's value overwrites.
#[test]
fn cp06_conflict_policy_wired_to_apply_changes() {
    let id = uuid::Uuid::from_u128(1);
    let key = NaturalKey {
        column: "id".to_string(),
        value: Value::Uuid(id),
    };

    // Scenario: server has row with name="server_value", edge pushes name="edge_value"
    let changeset = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: key.clone(),
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("edge_value".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(100),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };

    // Test with server_wins — server value must be kept
    {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("server_value".to_string())),
            ]),
        )
        .unwrap();

        let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
        let result = db.apply_changes(changeset.clone(), &policies).unwrap();
        assert!(
            result.skipped_rows > 0,
            "server_wins must skip the edge row"
        );

        let row = db
            .point_lookup("items", "id", &Value::Uuid(id), db.snapshot())
            .unwrap()
            .expect("row must exist");
        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("server_value".to_string())),
            "server_wins must keep server's value"
        );
    }

    // Test with edge_wins — edge value must overwrite
    {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("server_value".to_string())),
            ]),
        )
        .unwrap();

        let policies = ConflictPolicies::uniform(ConflictPolicy::EdgeWins);
        let result = db.apply_changes(changeset.clone(), &policies).unwrap();
        assert!(
            result.applied_rows > 0,
            "edge_wins must apply the edge's row"
        );

        let row = db
            .point_lookup("items", "id", &Value::Uuid(id), db.snapshot())
            .unwrap()
            .expect("row must exist");
        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("edge_value".to_string())),
            "edge_wins must overwrite with edge's value"
        );
    }
}

/// Plain row UPDATE (non-state-machine) propagates through sync end-to-end.
/// Edge inserts a row, pushes, updates the row, pushes again. Server must have the updated value.
#[tokio::test]
async fn cp07_row_update_propagates_through_sync() {
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;

    let tmp = tempfile::TempDir::new().unwrap();
    let edge_path = tmp.path().join("cp07-edge.db");
    let server_path = tmp.path().join("cp07-server.db");

    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());

    edge_db
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();

    let id = uuid::Uuid::new_v4();
    edge_db
        .execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("original".to_string())),
            ]),
        )
        .unwrap();

    // Start server, push initial data
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        nats_url,
        "cp07",
        policies,
    ));
    let handle = tokio::spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let client = SyncClient::new(edge_db.clone(), nats_url, "cp07");
    client.push().await.unwrap();

    // Verify initial value on server
    let server_row = server_db
        .point_lookup("items", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap()
        .expect("row must exist on server after first push");
    assert_eq!(
        server_row.values.get("name"),
        Some(&Value::Text("original".to_string())),
    );

    // UPDATE on edge
    edge_db
        .execute(
            "UPDATE items SET name = $name WHERE id = $id",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("updated".to_string())),
            ]),
        )
        .unwrap();

    // Push again — server must accept the update
    client.push().await.unwrap();

    let updated_row = server_db
        .point_lookup("items", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap()
        .expect("row must still exist");
    assert_eq!(
        updated_row.values.get("name"),
        Some(&Value::Text("updated".to_string())),
        "server must have updated value after second push"
    );

    handle.abort();
    drop(client);
    edge_db.close().unwrap();
    server_db.close().unwrap();
}

/// Push retry succeeds when server subscribes late.
/// Start CLI push BEFORE server is running. Server starts during retry window.
/// Push must succeed, not fail.
#[tokio::test]
async fn cp08_push_retry_succeeds_when_server_starts_late() {
    let nats = start_nats().await;
    let nats_url = &nats.nats_url;

    let tmp = tempfile::TempDir::new().unwrap();
    let edge_path = tmp.path().join("cp08-edge.db");
    let server_path = tmp.path().join("cp08-server.db");

    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());

    edge_db
        .execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    let id = uuid::Uuid::new_v4();
    edge_db
        .execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("name".to_string(), Value::Text("retry_test".to_string())),
            ]),
        )
        .unwrap();

    // Start push BEFORE server is running — it must retry
    let push_handle = {
        tokio::spawn({
            let nats_url = nats_url.to_string();
            let edge_db = edge_db.clone();
            async move {
                let client = SyncClient::new(edge_db, &nats_url, "cp08");
                client.push().await
            }
        })
    };

    // Wait 1 second, THEN start server — push is retrying during this time
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        nats_url,
        "cp08",
        policies,
    ));
    let server_handle = tokio::spawn({
        let server = server.clone();
        async move { server.run().await }
    });

    // Push must succeed (retried and found server)
    let push_result = tokio::time::timeout(std::time::Duration::from_secs(15), push_handle)
        .await
        .expect("push must complete within 15s")
        .expect("push task must not panic");
    assert!(
        push_result.is_ok(),
        "push must succeed after server starts late: {:?}",
        push_result.err()
    );

    // Verify data arrived
    let row = server_db
        .point_lookup("items", "id", &Value::Uuid(id), server_db.snapshot())
        .unwrap();
    assert!(
        row.is_some(),
        "row must be on server after retry-succeeded push"
    );

    server_handle.abort();
    edge_db.close().unwrap();
    server_db.close().unwrap();
}

#[test]
fn integrity_11_sync_apply_respects_memory_limit() {
    let accountant = Arc::new(MemoryAccountant::with_budget(8 * 1024));
    let db = Database::open_memory_with_accountant(accountant);
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, data TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("data".to_string(), Value::Text("x".repeat(4096))),
            ]),
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],
    };

    let result = db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    );
    assert!(
        result.is_err(),
        "sync apply must reject rows that exceed the server memory budget"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("MEMORY_LIMIT") || err.to_lowercase().contains("memory"),
        "memory-limit rejection must mention memory, got: {err}"
    );
}

#[test]
fn integrity_12_sync_delete_removes_vectors() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();

    let filler_id = Uuid::new_v4();
    let filler_tx = db.begin();
    db.insert_row(
        filler_tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(filler_id)),
            ("data", Value::Text("filler".into())),
        ]),
    )
    .unwrap();
    db.commit(filler_tx).unwrap();

    let target_id = Uuid::new_v4();
    let target_tx = db.begin();
    let target_row_id = db
        .insert_row(
            target_tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(target_id)),
                ("data", Value::Text("target".into())),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    db.insert_vector(
        target_tx,
        contextdb_core::VectorIndexRef::new("observations", "embedding"),
        target_row_id,
        vec![1.0, 0.0, 0.0],
    )
    .unwrap();
    db.commit(target_tx).unwrap();

    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "observations".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(target_id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(target_id)),
                ("data".to_string(), Value::Text("target".into())),
            ]),
            deleted: true,
            lsn: Lsn(10),
        }],
        edges: vec![],
        vectors: vec![VectorChange {
            index: contextdb_core::VectorIndexRef::new("observations", "embedding"),
            row_id: RowId(1),
            vector: Vec::new(),
            lsn: Lsn(10),
        }],
        ddl: vec![],
    };

    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();

    assert!(
        db.point_lookup("observations", "id", &Value::Uuid(target_id), db.snapshot(),)
            .unwrap()
            .is_none(),
        "deleted row must disappear after sync apply"
    );
    assert!(
        db.live_vector_entry(target_row_id, db.snapshot()).is_none(),
        "sync delete must also remove the local vector entry"
    );
}

#[test]
fn integrity_13_sync_upsert_refreshes_vector() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();

    let filler_tx = db.begin();
    db.insert_row(
        filler_tx,
        "observations",
        make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Text("filler".into())),
        ]),
    )
    .unwrap();
    db.commit(filler_tx).unwrap();

    let target_id = Uuid::new_v4();
    let target_tx = db.begin();
    let original_row_id = db
        .insert_row(
            target_tx,
            "observations",
            make_params(vec![
                ("id", Value::Uuid(target_id)),
                ("data", Value::Text("old".into())),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    db.insert_vector(
        target_tx,
        contextdb_core::VectorIndexRef::new("observations", "embedding"),
        original_row_id,
        vec![1.0, 0.0, 0.0],
    )
    .unwrap();
    db.commit(target_tx).unwrap();

    let new_vector = vec![0.0, 1.0, 0.0];
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "observations".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(target_id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(target_id)),
                ("data".to_string(), Value::Text("new".into())),
                ("embedding".to_string(), Value::Vector(new_vector.clone())),
            ]),
            deleted: false,
            lsn: Lsn(10),
        }],
        edges: vec![],
        vectors: vec![VectorChange {
            index: contextdb_core::VectorIndexRef::new("observations", "embedding"),
            row_id: RowId(1),
            vector: new_vector.clone(),
            lsn: Lsn(10),
        }],
        ddl: vec![],
    };

    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();

    let row = db
        .point_lookup("observations", "id", &Value::Uuid(target_id), db.snapshot())
        .unwrap()
        .expect("upserted row must still exist");
    assert_eq!(
        row.values.get("data"),
        Some(&Value::Text("new".into())),
        "sync upsert must update relational values"
    );
    let vector_hits = db
        .query_vector(
            contextdb_core::VectorIndexRef::new("observations", "embedding"),
            &new_vector,
            1,
            None,
            db.snapshot(),
        )
        .expect("vector search after sync upsert");
    assert_eq!(
        vector_hits.len(),
        1,
        "sync upsert must keep the row searchable by its new embedding"
    );
}

// ======== T28 ========
// Partition-heal uses the real in-process pattern from gate_a_sync.rs
// (see a9_04_pull_conflict_latest_wins): two Database instances write
// independently (simulating partition — neither sees the other's changes),
// then "heal" is a bidirectional exchange of `changes_since(0)` applied
// via `apply_changes(cs, ConflictPolicies::uniform(ConflictPolicy::LatestWins))`.
// No partition helper type is required; partition = withholding the
// changeset exchange until the write phase is complete.
#[test]
fn sync_partition_heal_value_txid_lww_deterministic() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::Database;
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use std::collections::HashMap;

    // Fixed primary key; identical in every iteration.
    let pk = uuid::Uuid::from_u128(0x1234_5678_9ABC_DEF0_1122_3344_5566_7788);

    for iteration in 0..10u32 {
        // Fresh databases every iteration — no shared state carries across runs.
        let edge_a = Database::open_memory();
        let edge_b = Database::open_memory();

        let empty: HashMap<String, Value> = HashMap::new();
        for db in [&edge_a, &edge_b] {
            db.execute(
                "CREATE TABLE t (pk UUID PRIMARY KEY, x TXID NOT NULL)",
                &empty,
            )
            .expect("CREATE TABLE must succeed");

            // Drive the committed watermark past 200 via unrelated auto-commit SQL,
            // mirroring T12's pattern. Library-API insert_row enforces the plan's
            // B7 bound (n <= current_tx_max) on the library path, so the TxId(100)
            // and TxId(200) targets below must fall inside the watermark.
            db.execute("CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)", &empty)
                .expect("CREATE TABLE bump must succeed");
            for n in 0..210i64 {
                let mut row: HashMap<String, Value> = HashMap::new();
                row.insert("id".to_string(), Value::Uuid(uuid::Uuid::new_v4()));
                row.insert("n".to_string(), Value::Int64(n));
                db.execute("INSERT INTO bump (id, n) VALUES ($id, $n)", &row)
                    .unwrap_or_else(|e| panic!("bump insert {n} must succeed: {e:?}"));
            }
            let wm = db.committed_watermark();
            assert!(
                wm.0 >= 200,
                "committed_watermark must be >= 200 after bump commits, got TxId({})",
                wm.0,
            );
        }

        // Partition: each edge writes the same pk with a distinct Value::TxId.
        // No changeset is exchanged during this phase.
        {
            let mut row_a = HashMap::new();
            row_a.insert("pk".to_string(), Value::Uuid(pk));
            row_a.insert("x".to_string(), Value::TxId(TxId(100)));
            let tx_a = edge_a.begin();
            edge_a
                .insert_row(tx_a, "t", row_a)
                .expect("edge_a insert must succeed");
            edge_a.commit(tx_a).expect("edge_a commit must succeed");
        }
        {
            let mut row_b = HashMap::new();
            row_b.insert("pk".to_string(), Value::Uuid(pk));
            row_b.insert("x".to_string(), Value::TxId(TxId(200)));
            let tx_b = edge_b.begin();
            edge_b
                .insert_row(tx_b, "t", row_b)
                .expect("edge_b insert must succeed");
            edge_b.commit(tx_b).expect("edge_b commit must succeed");
        }

        // Heal + bidirectional sync to quiescence under uniform LWW:
        // apply each peer's changes_since(0) on the other, in both directions,
        // and repeat until neither side changes. Two rounds suffice for a
        // single conflicting row under LatestWins.
        let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
        for _round in 0..2 {
            let a_to_b = edge_a.changes_since(contextdb_core::Lsn(0));
            edge_b
                .apply_changes(a_to_b, &policies)
                .expect("apply edge_a → edge_b must succeed");
            let b_to_a = edge_b.changes_since(contextdb_core::Lsn(0));
            edge_a
                .apply_changes(b_to_a, &policies)
                .expect("apply edge_b → edge_a must succeed");
        }

        // Both edges must converge to exactly Value::TxId(TxId(200)).
        let mut select_params: HashMap<String, Value> = HashMap::new();
        select_params.insert("pk".to_string(), Value::Uuid(pk));
        for (name, db) in [("edge-A", &edge_a), ("edge-B", &edge_b)] {
            let result = db
                .execute("SELECT x FROM t WHERE pk = $pk", &select_params)
                .expect("bound select must succeed");
            assert_eq!(
                result.rows.len(),
                1,
                "{name} must have exactly 1 row for pk on iteration {iteration}; got {}",
                result.rows.len()
            );
            assert_eq!(
                result.rows[0][0],
                Value::TxId(TxId(200)),
                "{name} must converge to Value::TxId(TxId(200)) on iteration {iteration}; got {:?}",
                result.rows[0][0]
            );
        }
    }
}
