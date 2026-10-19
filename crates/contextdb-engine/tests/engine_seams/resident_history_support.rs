#![allow(dead_code)]
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{collections::HashMap, path::Path};

pub fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
pub fn open(path: &Path, limit: Option<usize>) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: limit,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the task-scoped writer")
}
pub fn exec(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.execute(sql, &empty())
        .unwrap_or_else(|e| panic!("fixture SQL {sql}: {e:?}"))
        .rows
}
pub fn partition(scope: i64) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap()
}
pub fn index(table: &str) -> VectorIndexRef {
    VectorIndexRef::new(table, "v")
}
pub fn vector(id: usize, dimension: usize) -> Vec<f32> {
    let mut v = vec![0.0; dimension];
    // Unique directions: a top-one identity oracle must not rely on tie order.
    v[0] = 1.0;
    v[1] = (id + 1) as f32 / 1024.0;
    v[2] = ((id * 17) % 251) as f32 / 1024.0;
    v
}
pub fn insert_vector(db: &Database, table: &str, id: usize, scope: Option<i64>, dim: usize) {
    let mut params = HashMap::from([
        ("id".into(), Value::Int64(id as i64)),
        ("v".into(), Value::Vector(vector(id, dim))),
    ]);
    let sql = if let Some(scope) = scope {
        params.insert("scope".into(), Value::Int64(scope));
        format!("INSERT INTO {table} VALUES ($id, $scope, $v)")
    } else {
        format!("INSERT INTO {table} VALUES ($id, $v)")
    };
    db.execute(&sql, &params)
        .expect("commit one vector fixture identity");
}
pub fn query_vector(
    db: &Database,
    table: &str,
    id: usize,
    dim: usize,
    mode: &str,
) -> Vec<Vec<Value>> {
    db.execute(
        &format!("SELECT id FROM {table} ORDER BY v <=> $v USE VECTOR {mode} LIMIT 1"),
        &HashMap::from([("v".into(), Value::Vector(vector(id, dim)))]),
    )
    .expect("vector query")
    .rows
}
#[cfg(target_os = "linux")]
pub fn anonymous_bytes() -> u64 {
    let rollup =
        std::fs::read_to_string("/proc/self/smaps_rollup").expect("read own anonymous RSS");
    rollup
        .lines()
        .find_map(|l| l.strip_prefix("Anonymous:"))
        .expect("anonymous RSS field")
        .trim()
        .trim_end_matches(" kB")
        .parse::<u64>()
        .unwrap()
        * 1024
}

// JSON objects canonicalize map keys; arrays preserve log/DDL occurrence order.
pub fn canonical<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("serialize exact fixture oracle")
}
pub fn vector_rows(
    db: &Database,
    table: &str,
    snapshot: contextdb_core::SnapshotId,
    scope: Option<i64>,
) -> Vec<Vec<Value>> {
    let predicate = scope.map_or(String::new(), |scope| format!(" WHERE scope = {scope}"));
    db.execute_at_snapshot(
        &format!("SELECT id, v FROM {table}{predicate} ORDER BY id"),
        &empty(),
        snapshot,
    )
    .expect("read every vector body at the fixture frontier")
    .rows
}
pub fn vector_oracle(ids: impl IntoIterator<Item = usize>, dim: usize) -> Vec<Vec<Value>> {
    ids.into_iter()
        .map(|id| vec![Value::Int64(id as i64), Value::Vector(vector(id, dim))])
        .collect()
}
pub fn exact_partition_ids(
    db: &Database,
    table: &str,
    snapshot: contextdb_core::SnapshotId,
    scope: Option<i64>,
    dim: usize,
    count: usize,
) -> Vec<i64> {
    let predicate = scope.map_or(String::new(), |scope| format!(" WHERE scope = {scope}"));
    let mut ids = db
        .execute_at_snapshot(
            &format!(
                "SELECT id FROM {table}{predicate} ORDER BY v <=> $v USE VECTOR EXACT LIMIT {}",
                count.max(1)
            ),
            &HashMap::from([("v".into(), Value::Vector(vector(0, dim)))]),
            snapshot,
        )
        .expect("exhaustive vector membership query")
        .rows
        .into_iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            _ => panic!("integer fixture id"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Run allocation-sensitive proofs alone even under the parallel cargo-test gate.
pub fn isolated_test(module: &str, name: &str, flag: &str) -> bool {
    if std::env::var_os(flag).is_some() {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(module, name),
            "--nocapture",
        ])
        .env(flag, "1")
        .output()
        .expect("launch exact isolated allocation arm");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("running 1 test"),
        "exact isolated filter must execute one test: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.status.success(),
        "isolated allocation arm failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
