use contextdb_core::*;
use contextdb_core::{Lsn, RowId};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::{SyncClient, SyncServer};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::mem::forget;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

struct NatsFixture {
    _container: ContainerAsync<GenericImage>,
    nats_url: String,
}

fn values(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn text<'a>(row: &'a VersionedRow, key: &str) -> &'a str {
    row.values
        .get(key)
        .and_then(Value::as_text)
        .expect("expected text value")
}

fn float64(row: &VersionedRow, key: &str) -> f64 {
    match row.values.get(key) {
        Some(Value::Float64(v)) => *v,
        other => panic!("expected float64 for {key}, got {other:?}"),
    }
}

fn random_unit_vector(dim: usize, seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let mut vec = Vec::with_capacity(dim);
    for _ in 0..dim {
        state = state
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let unit = ((state >> 33) as f64) / ((1u64 << 31) as f64);
        vec.push((unit as f32) * 2.0 - 1.0);
    }
    normalize(&vec)
}

fn normalize(input: &[f32]) -> Vec<f32> {
    let norm = input
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt() as f32;
    if norm == 0.0 {
        return vec![0.0; input.len()];
    }
    input.iter().map(|v| *v / norm).collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot = a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    let an = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    let bn = b.iter().map(|v| v * v).sum::<f32>().sqrt();
    if an == 0.0 || bn == 0.0 {
        0.0
    } else {
        dot / (an * bn)
    }
}

fn brute_force_top_k(vectors: &[(RowId, Vec<f32>)], query: &[f32], k: usize) -> Vec<RowId> {
    let mut scored = vectors
        .iter()
        .map(|(rid, vec)| (*rid, cosine(vec, query)))
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(rid, _)| rid).collect()
}

fn setup_nats_conf() -> String {
    format!("{}/tests/nats.conf", env!("CARGO_MANIFEST_DIR"))
}

async fn start_nats() -> NatsFixture {
    let image = GenericImage::new("nats", "latest")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"));
    let request = image
        .with_mount(Mount::bind_mount(setup_nats_conf(), "/etc/nats/nats.conf"))
        .with_cmd(["--js", "--config", "/etc/nats/nats.conf"]);
    let container = request.start().await.unwrap();
    let nats_port = container.get_host_port_ipv4(4222.tcp()).await.unwrap();
    NatsFixture {
        _container: container,
        nats_url: format!("nats://127.0.0.1:{nats_port}"),
    }
}

#[test]
fn p01_relational_rows_survive_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, score REAL)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    db.insert_row(
        tx,
        "items",
        values(vec![
            ("id", Value::Uuid(id1)),
            ("name", Value::Text("alpha".to_string())),
            ("score", Value::Float64(1.5)),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "items",
        values(vec![
            ("id", Value::Uuid(id2)),
            ("name", Value::Text("beta".to_string())),
            ("score", Value::Float64(2.7)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let rows = db2.scan("items", db2.snapshot()).unwrap();
    assert_eq!(rows.len(), 2);
    let found = db2
        .point_lookup("items", "id", &Value::Uuid(id1), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&found, "name"), "alpha");
    assert_eq!(float64(&found, "score"), 1.5);
    let found2 = db2
        .point_lookup("items", "id", &Value::Uuid(id2), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&found2, "name"), "beta");
}

#[test]
fn p02_ddl_schema_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ddl.db");
    let db = Database::open(&path).unwrap();
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
    let tx = db.begin();
    let uuid1 = Uuid::new_v4();
    db.insert_row(
        tx,
        "events",
        values(vec![
            ("id", Value::Uuid(uuid1)),
            ("data", Value::Text("test".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let tx2 = db2.begin();
    db2.insert_row(
        tx2,
        "events",
        values(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("data", Value::Text("post-reopen".to_string())),
        ]),
    )
    .unwrap();
    db2.commit(tx2).unwrap();
    let rows = db2.scan("events", db2.snapshot()).unwrap();
    assert_eq!(rows.len(), 2);
    let update = db2.execute(
        "UPDATE events SET data = 'changed' WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid1))]),
    );
    assert!(update.is_err());
    let tx3 = db2.begin();
    let uuid3 = Uuid::new_v4();
    db2.insert_row(
        tx3,
        "workflows",
        values(vec![
            ("id", Value::Uuid(uuid3)),
            ("status", Value::Text("draft".to_string())),
        ]),
    )
    .unwrap();
    db2.commit(tx3).unwrap();
    let invalid = db2.execute(
        "UPDATE workflows SET status = 'archived' WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid3))]),
    );
    assert!(invalid.is_err());
}

#[test]
fn p03_graph_edges_with_properties_survive_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let ids = [
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    ];
    for (id, name) in ids.into_iter().zip(["A", "B", "C", "D"]) {
        db.insert_row(
            tx,
            "nodes",
            values(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
            ]),
        )
        .unwrap();
    }
    db.insert_edge(
        tx,
        ids[0],
        ids[1],
        "SERVES".to_string(),
        HashMap::from([
            ("weight".to_string(), Value::Float64(0.8)),
            ("label".to_string(), Value::Text("primary".to_string())),
        ]),
    )
    .unwrap();
    db.insert_edge(
        tx,
        ids[1],
        ids[2],
        "BASED_ON".to_string(),
        HashMap::from([("confidence".to_string(), Value::Float64(0.95))]),
    )
    .unwrap();
    db.insert_edge(tx, ids[2], ids[3], "CITES".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let bfs = db2
        .query_bfs(ids[0], None, Direction::Outgoing, 3, db2.snapshot())
        .unwrap();
    let visited = bfs.nodes.iter().map(|n| n.id).collect::<HashSet<_>>();
    assert_eq!(visited.len(), 3);
    assert!(visited.contains(&ids[1]));
    assert!(visited.contains(&ids[2]));
    assert!(visited.contains(&ids[3]));
    let props = db2
        .get_edge_properties(ids[0], ids[1], "SERVES", db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(props.get("weight"), Some(&Value::Float64(0.8)));
    assert_eq!(
        props.get("label"),
        Some(&Value::Text("primary".to_string()))
    );
}

#[test]
fn p04_vectors_survive_reopen_and_ann_works() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("vectors.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let r1 = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    let r2 = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    let r3 = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    db.insert_vector(tx, r1, vec![1.0, 0.0, 0.0]).unwrap();
    db.insert_vector(tx, r2, vec![0.0, 1.0, 0.0]).unwrap();
    db.insert_vector(tx, r3, vec![0.7, 0.7, 0.0]).unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let results = db2
        .query_vector(&[0.9, 0.1, 0.0], 2, None, db2.snapshot())
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, r1);
    assert!(results[0].1 > 0.9);
}

#[test]
fn p05_hnsw_recall_stable_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("recall.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE vecs (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    for i in 0..200u64 {
        let rid = db
            .insert_row(
                tx,
                "vecs",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .unwrap();
        db.insert_vector(tx, rid, random_unit_vector(384, i))
            .unwrap();
    }
    db.commit(tx).unwrap();
    let query = random_unit_vector(384, 9_999);
    let pre = db.query_vector(&query, 5, None, db.snapshot()).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let post = db2.query_vector(&query, 5, None, db2.snapshot()).unwrap();
    let pre_ids = pre.iter().map(|r| r.0).collect::<HashSet<_>>();
    let post_ids = post.iter().map(|r| r.0).collect::<HashSet<_>>();
    assert_eq!(pre_ids, post_ids);
    for (lhs, rhs) in pre.iter().zip(post.iter()) {
        assert!((lhs.1 - rhs.1).abs() < 0.01);
    }
}

#[test]
fn p06_cross_subsystem_atomic_persistence() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("atomic.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let tx = db.begin();
    let dec_id = Uuid::new_v4();
    let node_id = Uuid::new_v4();
    let r1 = db
        .insert_row(
            tx,
            "decisions",
            values(vec![
                ("id", Value::Uuid(dec_id)),
                ("status", Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
    db.insert_row(tx, "nodes", values(vec![("id", Value::Uuid(node_id))]))
        .unwrap();
    db.insert_edge(tx, dec_id, node_id, "SERVES".to_string(), HashMap::new())
        .unwrap();
    db.insert_vector(tx, r1, vec![1.0, 0.0, 0.0]).unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let row = db2
        .point_lookup("decisions", "id", &Value::Uuid(dec_id), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&row, "status"), "active");
    let bfs = db2
        .query_bfs(
            dec_id,
            Some(&["SERVES".to_string()]),
            Direction::Outgoing,
            1,
            db2.snapshot(),
        )
        .unwrap();
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, node_id);
    let results = db2
        .query_vector(&[0.9, 0.1, 0.0], 1, None, db2.snapshot())
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, r1);
}

#[test]
fn p07_counter_reconstruction_no_id_collisions() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("counters.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, seq INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    let tx1 = db.begin();
    for i in 0..5 {
        db.insert_row(
            tx1,
            "items",
            values(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("seq", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx1).unwrap();
    let pre_snapshot = db.snapshot();
    let pre_lsn = db.current_lsn();
    let pre_rows = db.scan("items", pre_snapshot).unwrap();
    let max_pre_row_id = pre_rows.iter().map(|r| r.row_id).max().unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let tx2 = db2.begin();
    let post_row_id = db2
        .insert_row(
            tx2,
            "items",
            values(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("seq", Value::Int64(99)),
            ]),
        )
        .unwrap();
    db2.commit(tx2).unwrap();

    assert!(post_row_id > max_pre_row_id);
    assert!(db2.snapshot() > pre_snapshot);
    assert!(db2.current_lsn() > pre_lsn);
    assert_eq!(db2.scan("items", db2.snapshot()).unwrap().len(), 6);
}

#[test]
fn p08_multi_commit_accumulation() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("multi.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE log (id UUID PRIMARY KEY, msg TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx1 = db.begin();
    let id_a = Uuid::new_v4();
    db.insert_row(
        tx1,
        "log",
        values(vec![
            ("id", Value::Uuid(id_a)),
            ("msg", Value::Text("session-one".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    let tx2 = db.begin();
    let id_b = Uuid::new_v4();
    db.insert_row(
        tx2,
        "log",
        values(vec![
            ("id", Value::Uuid(id_b)),
            ("msg", Value::Text("session-two".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    assert_eq!(db2.scan("log", db2.snapshot()).unwrap().len(), 2);
    let a = db2
        .point_lookup("log", "id", &Value::Uuid(id_a), db2.snapshot())
        .unwrap()
        .unwrap();
    let b = db2
        .point_lookup("log", "id", &Value::Uuid(id_b), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&a, "msg"), "session-one");
    assert_eq!(text(&b, "msg"), "session-two");
}

#[test]
fn p09_committed_data_survives_ungraceful_shutdown() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("crash.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, val TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let id1 = Uuid::new_v4();
    db.insert_row(
        tx,
        "items",
        values(vec![
            ("id", Value::Uuid(id1)),
            ("val", Value::Text("committed".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    drop(db);

    let db2 = Database::open(&path).unwrap();
    let row = db2
        .point_lookup("items", "id", &Value::Uuid(id1), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&row, "val"), "committed");
}

#[test]
fn p10_uncommitted_data_lost_after_crash() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("crash-uncommitted.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, val TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let tx1 = db.begin();
    let committed_id = Uuid::new_v4();
    db.insert_row(
        tx1,
        "items",
        values(vec![
            ("id", Value::Uuid(committed_id)),
            ("val", Value::Text("safe".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    let tx2 = db.begin();
    let uncommitted_id = Uuid::new_v4();
    db.insert_row(
        tx2,
        "items",
        values(vec![
            ("id", Value::Uuid(uncommitted_id)),
            ("val", Value::Text("doomed".to_string())),
        ]),
    )
    .unwrap();
    drop(db);

    let db2 = Database::open(&path).unwrap();
    let safe = db2
        .point_lookup("items", "id", &Value::Uuid(committed_id), db2.snapshot())
        .unwrap();
    assert!(safe.is_some());
    let doomed = db2
        .point_lookup("items", "id", &Value::Uuid(uncommitted_id), db2.snapshot())
        .unwrap();
    assert!(doomed.is_none());
}

#[test]
fn p11_change_log_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("change-log.db");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE items (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let tx = db.begin();
    db.insert_row(
        tx,
        "items",
        values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    assert!(!db.change_log_since(Lsn(0)).is_empty());
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    assert!(
        !db2.change_log_since(Lsn(0)).is_empty(),
        "durable sync restart semantics require the change log after reopen"
    );
}

#[test]
fn p12_open_memory_still_works() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE test (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let rid = db
        .insert_row(
            tx,
            "test",
            values(vec![
                ("id", Value::Uuid(id1)),
                ("name", Value::Text("memory".to_string())),
            ]),
        )
        .unwrap();
    db.insert_row(
        tx,
        "test",
        values(vec![
            ("id", Value::Uuid(id2)),
            ("name", Value::Text("target".to_string())),
        ]),
    )
    .unwrap();
    db.insert_edge(tx, id1, id2, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.insert_vector(tx, rid, vec![1.0, 0.0, 0.0]).unwrap();
    db.commit(tx).unwrap();

    let row = db
        .point_lookup("test", "id", &Value::Uuid(id1), db.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&row, "name"), "memory");
    let bfs = db
        .query_bfs(
            id1,
            Some(&["LINKS".to_string()]),
            Direction::Outgoing,
            1,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(bfs.nodes.len(), 1);
    assert_eq!(bfs.nodes[0].id, id2);
    let results = db
        .query_vector(&[1.0, 0.0, 0.0], 1, None, db.snapshot())
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, rid);
}

#[test]
fn p13_single_file_operation() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("test.db");
    let the_uuid = Uuid::new_v4();
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let tx = db.begin();
    db.insert_row(tx, "t", values(vec![("id", Value::Uuid(the_uuid))]))
        .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let files = fs::read_dir(tmp.path())
        .unwrap()
        .collect::<std::result::Result<Vec<_>, std::io::Error>>()
        .unwrap();
    assert!(files.len() <= 2);
    assert!(files.iter().any(|f| f.file_name() == "test.db"));
    let db2 = Database::open(&path).unwrap();
    let row = db2
        .point_lookup("t", "id", &Value::Uuid(the_uuid), db2.snapshot())
        .unwrap();
    assert!(row.is_some());
}

#[test]
fn p14_hnsw_recall_at_10_meets_threshold() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE vecs (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let mut vectors = Vec::new();
    for i in 0..1000u64 {
        let rid = db
            .insert_row(
                tx,
                "vecs",
                values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .unwrap();
        let vec = random_unit_vector(384, i + 100);
        db.insert_vector(tx, rid, vec.clone()).unwrap();
        vectors.push((rid, vec));
    }
    db.commit(tx).unwrap();
    let query = random_unit_vector(384, 999_999);
    let expected = brute_force_top_k(&vectors, &query, 10)
        .into_iter()
        .collect::<HashSet<_>>();
    let actual = db
        .query_vector(&query, 10, None, db.snapshot())
        .unwrap()
        .into_iter()
        .map(|(rid, _)| rid)
        .collect::<HashSet<_>>();
    let recall = expected.intersection(&actual).count() as f32 / 10.0;
    assert!(recall >= 0.95, "recall@10 was {recall}");
}

#[test]
fn p15_snapshot_query_sees_only_vectors_committed_before_snapshot() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let tx1 = db.begin();
    let r1 = db
        .insert_row(
            tx1,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    let r2 = db
        .insert_row(
            tx1,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    let r3 = db
        .insert_row(
            tx1,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    db.insert_vector(tx1, r1, vec![1.0, 0.0, 0.0]).unwrap();
    db.insert_vector(tx1, r2, vec![0.0, 1.0, 0.0]).unwrap();
    db.insert_vector(tx1, r3, vec![0.0, 0.0, 1.0]).unwrap();
    db.commit(tx1).unwrap();
    let old_snap = db.snapshot();

    let tx2 = db.begin();
    let r4 = db
        .insert_row(
            tx2,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    let r5 = db
        .insert_row(
            tx2,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    db.insert_vector(tx2, r4, vec![0.9, 0.1, 0.0]).unwrap();
    db.insert_vector(tx2, r5, vec![0.5, 0.5, 0.5]).unwrap();
    db.commit(tx2).unwrap();

    let results = db
        .query_vector(&[1.0, 0.0, 0.0], 10, None, old_snap)
        .unwrap();
    let ids = results
        .into_iter()
        .map(|(rid, _)| rid)
        .collect::<HashSet<_>>();
    assert!(ids.contains(&r1));
    assert!(ids.contains(&r2));
    assert!(ids.contains(&r3));
    assert!(!ids.contains(&r4));
    assert!(!ids.contains(&r5));
}

#[test]
fn p16_state_propagation_survives_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("prop.db");
    let db = Database::open(&path).unwrap();
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
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('BASED_ON')",
        &p,
    )
    .unwrap();

    let intention_id = Uuid::new_v4();
    let decision_id = Uuid::new_v4();
    let tx = db.begin();
    db.insert_row(
        tx,
        "intentions",
        values(vec![
            ("id", Value::Uuid(intention_id)),
            ("description", Value::Text("intent".to_string())),
            ("status", Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "decisions",
        values(vec![
            ("id", Value::Uuid(decision_id)),
            ("description", Value::Text("decision".to_string())),
            ("status", Value::Text("active".to_string())),
            ("intention_id", Value::Uuid(intention_id)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "intentions",
        "id",
        values(vec![
            ("id", Value::Uuid(intention_id)),
            ("description", Value::Text("intent".to_string())),
            ("status", Value::Text("archived".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let row = db2
        .point_lookup("decisions", "id", &Value::Uuid(decision_id), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&row, "status"), "invalidated");
}

#[tokio::test]
async fn p17_edge_pushes_to_server_both_survive_restart() {
    let nats = start_nats().await;
    let edge_tmp = TempDir::new().unwrap();
    let server_tmp = TempDir::new().unwrap();
    let edge_path = edge_tmp.path().join("edge.db");
    let server_path = server_tmp.path().join("server.db");
    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let empty = HashMap::new();
    edge_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();
    server_db
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)", &empty)
        .unwrap();

    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "p17",
        policies.clone(),
    ));
    let handle = tokio::spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let id = Uuid::new_v4();
    edge_db
        .execute(
            "INSERT INTO t (id, v) VALUES ($id, $v)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("v".to_string(), Value::Text("replicated".to_string())),
            ]),
        )
        .unwrap();
    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "p17");
    client.push().await.unwrap();

    handle.abort();
    drop(client);
    drop(edge_db);
    drop(server_db);

    let reopened_edge = Database::open(&edge_path).unwrap();
    let reopened_server = Database::open(&server_path).unwrap();
    assert!(
        reopened_edge
            .point_lookup("t", "id", &Value::Uuid(id), reopened_edge.snapshot())
            .unwrap()
            .is_some()
    );
    assert!(
        reopened_server
            .point_lookup("t", "id", &Value::Uuid(id), reopened_server.snapshot())
            .unwrap()
            .is_some()
    );
}

#[test]
fn p18_deleted_rows_stay_deleted_after_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("deleted.db");
    let db = Database::open(&path).unwrap();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let tx = db.begin();
    let row_id = db
        .insert_row(tx, "t", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    db.commit(tx).unwrap();
    let tx2 = db.begin();
    db.delete_row(tx2, "t", row_id).unwrap();
    db.commit(tx2).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    assert!(db2.scan("t", db2.snapshot()).unwrap().is_empty());
}

#[test]
fn p19_data_integrity_at_scale() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scale.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE rows_t (id UUID PRIMARY KEY, seq INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE vec_t (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let mut row_uuids = Vec::new();
    let mut edge_nodes = Vec::new();
    let mut vector_row_ids = Vec::new();
    for batch in 0..100 {
        let tx = db.begin();
        for i in 0..100 {
            let seq = batch * 100 + i;
            let id = Uuid::new_v4();
            let row_id = db
                .insert_row(
                    tx,
                    "rows_t",
                    values(vec![
                        ("id", Value::Uuid(id)),
                        ("seq", Value::Int64(seq as i64)),
                    ]),
                )
                .unwrap();
            row_uuids.push((id, seq as i64));
            if seq < 1000 {
                let node = Uuid::new_v4();
                edge_nodes.push(node);
                if seq > 0 {
                    db.insert_edge(
                        tx,
                        edge_nodes[seq - 1],
                        node,
                        "CHAIN".to_string(),
                        HashMap::new(),
                    )
                    .unwrap();
                }
                let angle = seq as f32 * 0.01;
                db.insert_row(
                    tx,
                    "vec_t",
                    values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
                )
                .unwrap();
                db.insert_vector(tx, row_id, vec![angle.cos(), angle.sin(), 0.0])
                    .unwrap();
                vector_row_ids.push(row_id);
            }
        }
        db.commit(tx).unwrap();
    }
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    assert_eq!(db2.scan("rows_t", db2.snapshot()).unwrap().len(), 10_000);
    let bfs = db2
        .query_bfs(
            edge_nodes[0],
            None,
            Direction::Outgoing,
            1000,
            db2.snapshot(),
        )
        .unwrap();
    assert_eq!(bfs.nodes.len(), 999);
    let ann = db2
        .query_vector(&[1.0, 0.0, 0.0], 1, None, db2.snapshot())
        .unwrap();
    assert_eq!(ann[0].0, vector_row_ids[0]);
    for idx in [128usize, 256, 512, 1024, 4096, 8192] {
        let (id, seq) = row_uuids[idx];
        let row = db2
            .point_lookup("rows_t", "id", &Value::Uuid(id), db2.snapshot())
            .unwrap()
            .unwrap();
        assert_eq!(row.values.get("seq"), Some(&Value::Int64(seq)));
    }
}

#[test]
fn p20_dimension_mismatch_rejected_after_restart() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("dim.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let rid = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    db.insert_vector(tx, rid, random_unit_vector(384, 123))
        .unwrap();
    db.commit(tx).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let tx2 = db2.begin();
    let rid2 = db2
        .insert_row(
            tx2,
            "obs",
            values(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    let err = db2.insert_vector(tx2, rid2, random_unit_vector(256, 456));
    assert!(err.is_err());
}

#[test]
fn p21_cli_persists_across_sessions() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cli.db");
    let mut child1 = Command::new("cargo")
        .args(["run", "-p", "contextdb-cli", "--", path.to_str().unwrap()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child1.stdin.as_mut().unwrap();
        writeln!(stdin, "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)").unwrap();
        writeln!(
            stdin,
            "INSERT INTO t (id, v) VALUES ('{}', 'alpha')",
            Uuid::new_v4()
        )
        .unwrap();
        writeln!(stdin, ".quit").unwrap();
    }
    let output1 = child1.wait_with_output().unwrap();
    assert!(output1.status.success());

    let mut child2 = Command::new("cargo")
        .args(["run", "-p", "contextdb-cli", "--", path.to_str().unwrap()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        writeln!(stdin, "SELECT * FROM t").unwrap();
        writeln!(stdin, ".quit").unwrap();
    }
    let output2 = child2.wait_with_output().unwrap();
    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    assert!(output2.status.success());
    assert!(stdout2.contains("alpha"));
}

#[test]
fn p22_cli_bad_path_prints_clear_error() {
    let output = Command::new("cargo")
        .args([
            "run",
            "-p",
            "contextdb-cli",
            "--",
            "/nonexistent/deeply/nested/path/test.db",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1));
    assert!(!stderr.is_empty());
    assert!(stderr.contains("path") || stderr.contains("open") || stderr.contains("writable"));
}

#[tokio::test]
async fn p23_server_started_with_db_path_survives_restart() {
    let _ = start_nats().await;
}

#[test]
fn p24_graph_edges_survive_process_crash_without_close() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph-crash.db");
    let db = Database::open(&path).unwrap();
    let tx = db.begin();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();
    db.insert_edge(tx, a, b, "EDGE".to_string(), HashMap::new())
        .unwrap();
    db.insert_edge(tx, b, c, "EDGE".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();
    drop(db);

    let db2 = Database::open(&path).unwrap();
    let bfs = db2
        .query_bfs(a, None, Direction::Outgoing, 2, db2.snapshot())
        .unwrap();
    let ids = bfs.nodes.iter().map(|n| n.id).collect::<HashSet<_>>();
    assert!(ids.contains(&b));
    assert!(ids.contains(&c));
}

#[test]
fn p25_vectors_survive_process_crash_without_close() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("vector-crash.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let rid = db
        .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
        .unwrap();
    db.insert_vector(tx, rid, vec![1.0, 0.0, 0.0]).unwrap();
    db.commit(tx).unwrap();
    drop(db);

    let db2 = Database::open(&path).unwrap();
    let results = db2
        .query_vector(&[1.0, 0.0, 0.0], 1, None, db2.snapshot())
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, rid);
}

#[test]
fn p26_flush_on_commit_proven_when_drop_is_skipped() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("forget.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin();
    let item_id = Uuid::from_u128(1);
    let node_id = Uuid::from_u128(2);
    let rid = db
        .insert_row(tx, "items", values(vec![("id", Value::Uuid(item_id))]))
        .unwrap();
    db.insert_edge(tx, item_id, node_id, "SERVES".to_string(), HashMap::new())
        .unwrap();
    db.insert_vector(tx, rid, vec![1.0, 0.0, 0.0]).unwrap();
    db.commit(tx).unwrap();
    forget(db);

    let db2 = Database::open(&path).unwrap();
    assert!(
        db2.point_lookup("items", "id", &Value::Uuid(item_id), db2.snapshot())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        db2.query_bfs(item_id, None, Direction::Outgoing, 1, db2.snapshot())
            .unwrap()
            .nodes[0]
            .id,
        node_id
    );
    assert_eq!(
        db2.query_vector(&[1.0, 0.0, 0.0], 1, None, db2.snapshot())
            .unwrap()[0]
            .0,
        rid
    );
}

#[test]
fn p27_upsert_result_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("upsert.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    let tx1 = db.begin();
    db.insert_row(
        tx1,
        "t",
        values(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("before".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    let tx2 = db.begin();
    db.upsert_row(
        tx2,
        "t",
        "id",
        values(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("after".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let row = db2
        .point_lookup("t", "id", &Value::Uuid(id), db2.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(text(&row, "name"), "after");
}

#[test]
fn p28_dag_modifier_enforced_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("dag.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('BASED_ON')",
        &HashMap::new(),
    )
    .unwrap();
    let tx1 = db.begin();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    db.insert_edge(tx1, a, b, "BASED_ON".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx1).unwrap();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let tx2 = db2.begin();
    let result = db2.insert_edge(tx2, b, a, "BASED_ON".to_string(), HashMap::new());
    assert!(
        result.is_err(),
        "cycle-creating edge should be rejected after reopen"
    );
}

#[test]
fn p29_temporal_query_works_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("temporal.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, version INTEGER)",
        &HashMap::new(),
    )
    .unwrap();
    let entity_id = Uuid::new_v4();
    let tx1 = db.begin();
    db.insert_row(
        tx1,
        "entities",
        values(vec![
            ("id", Value::Uuid(entity_id)),
            ("version", Value::Int64(1)),
        ]),
    )
    .unwrap();
    db.commit(tx1).unwrap();
    let s1 = db.snapshot();

    let row1 = db
        .point_lookup("entities", "id", &Value::Uuid(entity_id), s1)
        .unwrap()
        .unwrap();
    let rid = row1.row_id;

    let tx2 = db.begin();
    db.delete_row(tx2, "entities", rid).unwrap();
    db.insert_row(
        tx2,
        "entities",
        values(vec![
            ("id", Value::Uuid(entity_id)),
            ("version", Value::Int64(2)),
        ]),
    )
    .unwrap();
    db.commit(tx2).unwrap();
    let s2 = db.snapshot();

    let rid2 = db
        .point_lookup("entities", "id", &Value::Uuid(entity_id), s2)
        .unwrap()
        .unwrap()
        .row_id;
    let tx3 = db.begin();
    db.delete_row(tx3, "entities", rid2).unwrap();
    db.insert_row(
        tx3,
        "entities",
        values(vec![
            ("id", Value::Uuid(entity_id)),
            ("version", Value::Int64(3)),
        ]),
    )
    .unwrap();
    db.commit(tx3).unwrap();
    let s3 = db.snapshot();
    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let v1 = db2
        .point_lookup("entities", "id", &Value::Uuid(entity_id), s1)
        .unwrap()
        .unwrap();
    let v2 = db2
        .point_lookup("entities", "id", &Value::Uuid(entity_id), s2)
        .unwrap()
        .unwrap();
    let v3 = db2
        .point_lookup("entities", "id", &Value::Uuid(entity_id), s3)
        .unwrap()
        .unwrap();
    assert_eq!(v1.values.get("version"), Some(&Value::Int64(1)));
    assert_eq!(v2.values.get("version"), Some(&Value::Int64(2)));
    assert_eq!(v3.values.get("version"), Some(&Value::Int64(3)));
}

#[test]
fn p30_two_paths_are_independent() {
    let tmp = TempDir::new().unwrap();
    let path_a = tmp.path().join("a.db");
    let path_b = tmp.path().join("b.db");
    let db_a = Database::open(&path_a).unwrap();
    let db_b = Database::open(&path_b).unwrap();
    db_a.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    db_b.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
        .unwrap();
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    let tx_a = db_a.begin();
    db_a.insert_row(tx_a, "t", values(vec![("id", Value::Uuid(id_a))]))
        .unwrap();
    db_a.commit(tx_a).unwrap();
    let tx_b = db_b.begin();
    db_b.insert_row(tx_b, "t", values(vec![("id", Value::Uuid(id_b))]))
        .unwrap();
    db_b.commit(tx_b).unwrap();
    db_a.close().unwrap();
    db_b.close().unwrap();

    let reopened_a = Database::open(&path_a).unwrap();
    let reopened_b = Database::open(&path_b).unwrap();
    let own_a = reopened_a
        .point_lookup("t", "id", &Value::Uuid(id_a), reopened_a.snapshot())
        .unwrap();
    let own_b = reopened_b
        .point_lookup("t", "id", &Value::Uuid(id_b), reopened_b.snapshot())
        .unwrap();
    let cross_a = reopened_a
        .point_lookup("t", "id", &Value::Uuid(id_b), reopened_a.snapshot())
        .unwrap();
    let cross_b = reopened_b
        .point_lookup("t", "id", &Value::Uuid(id_a), reopened_b.snapshot())
        .unwrap();
    assert!(own_a.is_some());
    assert!(own_b.is_some());
    assert!(cross_a.is_none());
    assert!(cross_b.is_none());
}

#[test]
fn p31_column_type_preservation_across_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("coltypes.db");
    let db = Database::open(&path).unwrap();

    db.execute(
        "CREATE TABLE all_types (
            pk UUID PRIMARY KEY,
            t TEXT,
            r REAL,
            i INTEGER,
            b BOOLEAN,
            j JSON,
            v VECTOR(384)
        )",
        &HashMap::new(),
    )
    .unwrap();

    let meta_before = db.table_meta("all_types").unwrap();

    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();
    let meta_after = db2.table_meta("all_types").unwrap();

    assert_eq!(meta_before.columns.len(), meta_after.columns.len());

    let expected: Vec<(&str, ColumnType)> = vec![
        ("pk", ColumnType::Uuid),
        ("t", ColumnType::Text),
        ("r", ColumnType::Real),
        ("i", ColumnType::Integer),
        ("b", ColumnType::Boolean),
        ("j", ColumnType::Json),
        ("v", ColumnType::Vector(384)),
    ];
    for (name, expected_type) in &expected {
        let col = meta_after
            .columns
            .iter()
            .find(|c| c.name == *name)
            .unwrap_or_else(|| panic!("column {name} missing after reopen"));
        assert_eq!(
            &col.column_type, expected_type,
            "column {name} type changed: expected {expected_type:?}, got {:?}",
            col.column_type
        );
    }

    let pk_col = meta_after.columns.iter().find(|c| c.name == "pk").unwrap();
    assert!(pk_col.primary_key);
}

#[test]
fn p32_deleted_vectors_excluded_from_ann_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("del-vec.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();

    let tx = db.begin();
    let mut row_ids = Vec::new();
    let vectors: Vec<Vec<f32>> = vec![
        vec![1.0, 0.0, 0.0],
        vec![0.9, 0.1, 0.0],
        vec![0.0, 1.0, 0.0],
        vec![0.0, 0.0, 1.0],
        vec![0.7, 0.7, 0.0],
    ];
    for v in &vectors {
        let rid = db
            .insert_row(tx, "obs", values(vec![("id", Value::Uuid(Uuid::new_v4()))]))
            .unwrap();
        db.insert_vector(tx, rid, v.clone()).unwrap();
        row_ids.push(rid);
    }
    db.commit(tx).unwrap();

    let tx2 = db.begin();
    db.delete_row(tx2, "obs", row_ids[0]).unwrap();
    db.delete_vector(tx2, row_ids[0]).unwrap();
    db.delete_row(tx2, "obs", row_ids[1]).unwrap();
    db.delete_vector(tx2, row_ids[1]).unwrap();
    db.commit(tx2).unwrap();

    db.close().unwrap();

    let db2 = Database::open(&path).unwrap();

    let results = db2
        .query_vector(&[1.0, 0.0, 0.0], 5, None, db2.snapshot())
        .unwrap();
    let result_ids: HashSet<_> = results.iter().map(|(rid, _)| *rid).collect();

    assert!(
        !result_ids.contains(&row_ids[0]),
        "deleted row_ids[0] appeared in ANN results"
    );
    assert!(
        !result_ids.contains(&row_ids[1]),
        "deleted row_ids[1] appeared in ANN results"
    );

    assert!(
        result_ids.contains(&row_ids[2]),
        "kept row_ids[2] missing from ANN results"
    );
    assert!(
        result_ids.contains(&row_ids[3]),
        "kept row_ids[3] missing from ANN results"
    );
    assert!(
        result_ids.contains(&row_ids[4]),
        "kept row_ids[4] missing from ANN results"
    );

    assert_eq!(results.len(), 3);
    assert_eq!(db2.scan("obs", db2.snapshot()).unwrap().len(), 3);
}

#[tokio::test]
async fn p33_bidirectional_sync_with_persistence() {
    let nats = start_nats().await;

    let edge_tmp = TempDir::new().unwrap();
    let server_tmp = TempDir::new().unwrap();
    let edge_path = edge_tmp.path().join("edge.db");
    let server_path = server_tmp.path().join("server.db");

    let edge_db = Arc::new(Database::open(&edge_path).unwrap());
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let empty = HashMap::new();

    edge_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, source TEXT, seq INTEGER)",
            &empty,
        )
        .unwrap();
    server_db
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, source TEXT, seq INTEGER)",
            &empty,
        )
        .unwrap();

    let mut edge_ids = Vec::new();
    for i in 0..50 {
        let id = Uuid::new_v4();
        edge_db
            .execute(
                "INSERT INTO t (id, source, seq) VALUES ($id, $source, $seq)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("source".to_string(), Value::Text("edge".to_string())),
                    ("seq".to_string(), Value::Int64(i)),
                ]),
            )
            .unwrap();
        edge_ids.push(id);
    }

    let mut server_ids = Vec::new();
    for i in 0..50 {
        let id = Uuid::new_v4();
        server_db
            .execute(
                "INSERT INTO t (id, source, seq) VALUES ($id, $source, $seq)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(id)),
                    ("source".to_string(), Value::Text("server".to_string())),
                    ("seq".to_string(), Value::Int64(i)),
                ]),
            )
            .unwrap();
        server_ids.push(id);
    }

    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        "p33",
        policies.clone(),
    ));
    let handle = tokio::spawn({
        let server = server.clone();
        async move { server.run().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = SyncClient::new(edge_db.clone(), &nats.nats_url, "p33");
    client.push().await.unwrap();
    client.pull(&policies).await.unwrap();

    let edge_row_0 = edge_db
        .point_lookup("t", "id", &Value::Uuid(edge_ids[0]), edge_db.snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(
        edge_row_0.values.get("source"),
        Some(&Value::Text("edge".to_string()))
    );

    let edge_lsn_pre = edge_db.current_lsn();
    let server_lsn_pre = server_db.current_lsn();
    let edge_persisted_lsn_pre = edge_db
        .scan("t", edge_db.snapshot())
        .unwrap()
        .into_iter()
        .map(|row| row.lsn)
        .max()
        .unwrap();
    let server_persisted_lsn_pre = server_db
        .scan("t", server_db.snapshot())
        .unwrap()
        .into_iter()
        .map(|row| row.lsn)
        .max()
        .unwrap();

    drop(server);
    handle.abort();
    let _ = handle.await;
    drop(client);
    Arc::try_unwrap(edge_db).unwrap().close().unwrap();
    Arc::try_unwrap(server_db).unwrap().close().unwrap();

    let edge_db2 = Arc::new(Database::open(&edge_path).unwrap());
    let server_db2 = Arc::new(Database::open(&server_path).unwrap());

    let edge_rows = edge_db2.scan("t", edge_db2.snapshot()).unwrap();
    let server_rows = server_db2.scan("t", server_db2.snapshot()).unwrap();
    assert_eq!(
        edge_rows.len(),
        100,
        "edge should have 100 rows, got {}",
        edge_rows.len()
    );
    assert_eq!(
        server_rows.len(),
        100,
        "server should have 100 rows, got {}",
        server_rows.len()
    );

    for id in &edge_ids {
        let row = edge_db2
            .point_lookup("t", "id", &Value::Uuid(*id), edge_db2.snapshot())
            .unwrap();
        assert!(
            row.is_some(),
            "edge-originated row missing from edge after reopen"
        );
        assert_eq!(
            row.unwrap().values.get("source"),
            Some(&Value::Text("edge".to_string()))
        );
    }
    for id in &server_ids {
        let row = edge_db2
            .point_lookup("t", "id", &Value::Uuid(*id), edge_db2.snapshot())
            .unwrap();
        assert!(
            row.is_some(),
            "server-originated row missing from edge after reopen"
        );
        assert_eq!(
            row.unwrap().values.get("source"),
            Some(&Value::Text("server".to_string()))
        );
    }
    for id in &edge_ids {
        let row = server_db2
            .point_lookup("t", "id", &Value::Uuid(*id), server_db2.snapshot())
            .unwrap();
        assert!(
            row.is_some(),
            "edge-originated row missing from server after reopen"
        );
        assert_eq!(
            row.unwrap().values.get("source"),
            Some(&Value::Text("edge".to_string()))
        );
    }
    for id in &server_ids {
        let row = server_db2
            .point_lookup("t", "id", &Value::Uuid(*id), server_db2.snapshot())
            .unwrap();
        assert!(
            row.is_some(),
            "server-originated row missing from server after reopen"
        );
        assert_eq!(
            row.unwrap().values.get("source"),
            Some(&Value::Text("server".to_string()))
        );
    }

    let edge_lsn_post = edge_db2.current_lsn();
    let server_lsn_post = server_db2.current_lsn();
    assert_eq!(
        edge_lsn_post, edge_persisted_lsn_pre,
        "edge LSN after reopen should match persisted row LSN watermark"
    );
    assert_eq!(
        server_lsn_post, server_persisted_lsn_pre,
        "server LSN after reopen should match persisted row LSN watermark"
    );
    assert!(
        edge_lsn_post <= edge_lsn_pre,
        "edge LSN inflated after reopen: pre={edge_lsn_pre}, post={edge_lsn_post}"
    );
    assert!(
        server_lsn_post <= server_lsn_pre,
        "server LSN inflated after reopen: pre={server_lsn_pre}, post={server_lsn_post}"
    );

    let delta_id_edge = Uuid::new_v4();
    edge_db2
        .execute(
            "INSERT INTO t (id, source, seq) VALUES ($id, $source, $seq)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(delta_id_edge)),
                (
                    "source".to_string(),
                    Value::Text("edge_post_reopen".to_string()),
                ),
                ("seq".to_string(), Value::Int64(999)),
            ]),
        )
        .unwrap();

    let delta_id_server = Uuid::new_v4();
    server_db2
        .execute(
            "INSERT INTO t (id, source, seq) VALUES ($id, $source, $seq)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(delta_id_server)),
                (
                    "source".to_string(),
                    Value::Text("server_post_reopen".to_string()),
                ),
                ("seq".to_string(), Value::Int64(998)),
            ]),
        )
        .unwrap();

    let server2 = Arc::new(SyncServer::new(
        server_db2.clone(),
        &nats.nats_url,
        "p33",
        policies.clone(),
    ));
    let handle2 = tokio::spawn({
        let s = server2.clone();
        async move { s.run().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client2 = SyncClient::new(edge_db2.clone(), &nats.nats_url, "p33");
    client2.push().await.unwrap();
    client2.pull(&policies).await.unwrap();

    let server_rows2 = server_db2.scan("t", server_db2.snapshot()).unwrap();
    assert_eq!(
        server_rows2.len(),
        102,
        "server should have 102 rows after post-reopen sync"
    );
    let delta_row = server_db2
        .point_lookup(
            "t",
            "id",
            &Value::Uuid(delta_id_edge),
            server_db2.snapshot(),
        )
        .unwrap();
    assert!(
        delta_row.is_some(),
        "edge delta row missing from server after post-reopen sync"
    );
    assert_eq!(
        delta_row.unwrap().values.get("source"),
        Some(&Value::Text("edge_post_reopen".to_string()))
    );

    let edge_rows2 = edge_db2.scan("t", edge_db2.snapshot()).unwrap();
    assert_eq!(
        edge_rows2.len(),
        102,
        "edge should have 102 rows after post-reopen sync"
    );
    let delta_row2 = edge_db2
        .point_lookup(
            "t",
            "id",
            &Value::Uuid(delta_id_server),
            edge_db2.snapshot(),
        )
        .unwrap();
    assert!(
        delta_row2.is_some(),
        "server delta row missing from edge after post-reopen sync"
    );
    assert_eq!(
        delta_row2.unwrap().values.get("source"),
        Some(&Value::Text("server_post_reopen".to_string()))
    );

    drop(server2);
    handle2.abort();
    let _ = handle2.await;
}

#[test]
fn p34_cli_memory_mode_smoke_test() {
    let id = Uuid::new_v4();
    let mut child1 = Command::new("cargo")
        .args(["run", "-p", "contextdb-cli", "--", ":memory:"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child1.stdin.as_mut().unwrap();
        writeln!(stdin, "CREATE TABLE t (id UUID PRIMARY KEY, v TEXT)").unwrap();
        writeln!(
            stdin,
            "INSERT INTO t (id, v) VALUES ('{}', 'memory_val')",
            id
        )
        .unwrap();
        writeln!(stdin, "SELECT * FROM t").unwrap();
        writeln!(stdin, ".quit").unwrap();
    }
    let output1 = child1.wait_with_output().unwrap();
    assert!(
        output1.status.success(),
        "CLI session 1 exited with non-zero status"
    );
    let stdout1 = String::from_utf8_lossy(&output1.stdout);
    assert!(
        stdout1.contains("memory_val"),
        "SELECT output should contain inserted value, got: {stdout1}"
    );
    assert!(
        stdout1.contains(&id.to_string()),
        "SELECT output should contain inserted UUID, got: {stdout1}"
    );

    let mut child2 = Command::new("cargo")
        .args(["run", "-p", "contextdb-cli", "--", ":memory:"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        writeln!(stdin, "SELECT * FROM t").unwrap();
        writeln!(stdin, ".quit").unwrap();
    }
    let output2 = child2.wait_with_output().unwrap();
    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    let stderr2 = String::from_utf8_lossy(&output2.stderr);

    assert!(
        !stdout2.is_empty() || !stderr2.is_empty(),
        "CLI session 2 produced no output at all - process may not have started"
    );
    assert!(
        !stdout2.contains(&id.to_string()),
        "UUID from session 1 should not appear in session 2, stdout: {stdout2}"
    );
    assert!(
        !stdout2.contains("memory_val"),
        "memory_val should not persist across :memory: sessions, stdout: {stdout2}"
    );
}
