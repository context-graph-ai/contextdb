//! FIX 3 — the sync-apply memory preflight must probe the WHOLE natural key,
//! not just its leading column.
//!
//! `preflight_sync_apply_memory` resolved the possibly-existing row by the
//! LEADING natural-key column. A table-level composite PRIMARY KEY auto-indexes
//! only the whole tuple, so a single-leading-column probe finds no covering
//! index and returns an error — and the `?` aborts the ENTIRE sync apply
//! whenever a memory limit is set (the multi-tenant / enterprise case). The
//! frozen composite-key sync tests miss it because they open the engine with no
//! memory limit; this pins it with a limit set.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.
//! Deterministic apply, asserted by value.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::*;
use std::collections::HashMap;
use std::sync::Arc;

const COMPOSITE_DDL: &str = "CREATE TABLE metric_windows (\
     machine_id TEXT NOT NULL, \
     sensor_id TEXT NOT NULL, \
     metric TEXT NOT NULL, \
     window_start INTEGER NOT NULL, \
     value INTEGER NOT NULL, \
     PRIMARY KEY (machine_id, sensor_id, metric, window_start))";

fn window_row(
    machine: &str,
    sensor: &str,
    metric: &str,
    window: i64,
    value: i64,
    lsn: u64,
) -> RowChange {
    let mut values = HashMap::new();
    values.insert("machine_id".to_string(), Value::Text(machine.to_string()));
    values.insert("sensor_id".to_string(), Value::Text(sensor.to_string()));
    values.insert("metric".to_string(), Value::Text(metric.to_string()));
    values.insert("window_start".to_string(), Value::Int64(window));
    values.insert("value".to_string(), Value::Int64(value));
    RowChange {
        table: "metric_windows".to_string(),
        natural_key: NaturalKey::from_pairs(vec![
            ("machine_id".to_string(), Value::Text(machine.to_string())),
            ("sensor_id".to_string(), Value::Text(sensor.to_string())),
            ("metric".to_string(), Value::Text(metric.to_string())),
            ("window_start".to_string(), Value::Int64(window)),
        ])
        .expect("composite natural key"),
        values,
        deleted: false,
        lsn: Lsn(lsn),
        created_at: None,
    }
}

fn value_of(db: &Database, machine: &str) -> Option<i64> {
    let mut params = HashMap::new();
    params.insert("m".to_string(), Value::Text(machine.to_string()));
    let result = db
        .execute(
            "SELECT value FROM metric_windows WHERE machine_id = $m",
            &params,
        )
        .expect("scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Int64(v) => *v,
        other => panic!("value must be an integer, got {other:?}"),
    })
}

/// A composite-PK sync apply with a memory limit set must not hard-error, and the
/// row must land. Pre-fix, the leading-column probe finds no covering index and
/// the `?` aborts the whole apply.
#[test]
fn fix3_composite_key_sync_apply_survives_a_memory_limit() {
    let db = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    db.execute(COMPOSITE_DDL, &empty).expect("composite table");
    db.execute("SET MEMORY_LIMIT '1G'", &empty)
        .expect("set a generous memory limit");

    let changeset = ChangeSet {
        rows: vec![
            window_row("machine-a", "cam-1", "motion", 1_000, 11, 10),
            window_row("machine-b", "cam-1", "motion", 1_000, 22, 11),
        ],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);

    let result = db.apply_changes(changeset, &policies);
    assert!(
        result.is_ok(),
        "a composite-key sync apply under a memory limit must not hard-error: {result:?}"
    );
    let applied = result.unwrap();
    assert_eq!(
        applied.applied_rows, 2,
        "both composite-key rows apply: {applied:?}"
    );
    assert_eq!(
        value_of(&db, "machine-a"),
        Some(11),
        "machine-a row landed by value"
    );
    assert_eq!(
        value_of(&db, "machine-b"),
        Some(22),
        "machine-b row landed by value"
    );
}
