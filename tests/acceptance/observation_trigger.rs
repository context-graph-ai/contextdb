use super::common::{count_rows, empty_params, start_nats, wait_for_sync_server_ready};
use contextdb_core::{
    ContextId, Error, Lsn, Principal, Result, RowId, ScopeLabel, SortDirection, TxId, UpsertResult,
    Value, VectorIndexRef, VersionedRow,
};
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange, NaturalKey, RowChange,
    VectorChange,
};
use contextdb_engine::{
    Database, TriggerAuditFilter, TriggerAuditStatus, TriggerAuditStatusFilter, TriggerEvent,
};
use contextdb_server::{SyncClient, SyncServer, protocol::WireChangeSet};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn insert_host_params(id: Uuid, content: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("content".to_string(), Value::Text(content.to_string())),
    ])
}

fn setup_host_data_tables(db: &Database) {
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE host_audits (id UUID PRIMARY KEY, write_id UUID UNIQUE, note TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
}

fn setup_host_tables(db: &Database) {
    setup_host_data_tables(db);
    db.execute(
        "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();
}

fn setup_ready_host_write_trigger(db: &Database) {
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();
    db.register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
}

fn register_audit_callback(db: &Database, fire_count: Arc<AtomicUsize>) {
    db.register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
        if ctx.trigger_name != "host_write_trigger"
            || ctx.table != "host_writes"
            || ctx.event != TriggerEvent::Insert
        {
            return Err(Error::Other(format!(
                "unexpected trigger context identity: {ctx:?}"
            )));
        }
        fire_count.fetch_add(1, Ordering::SeqCst);
        let write_id = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("trigger row missing UUID id".into()))?;
        let content = ctx
            .row_values
            .get("content")
            .and_then(Value::as_text)
            .ok_or_else(|| Error::Other("trigger row missing post-image content".into()))?
            .to_string();
        let audit_id = Uuid::new_v4();
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(audit_id));
        p.insert("write_id".into(), Value::Uuid(write_id));
        p.insert("note".into(), Value::Text(format!("cascade:{content}")));
        p.insert("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
        db_handle.execute(
            "INSERT INTO host_audits (id, write_id, note, embedding) VALUES ($id, $write_id, $note, $embedding)",
            &p,
        )?;
        let mut edge_props = HashMap::new();
        edge_props.insert("trigger_tx".into(), Value::TxId(ctx.tx));
        db_handle.insert_edge(ctx.tx, write_id, audit_id, "AUDITED_BY".into(), edge_props)?;
        Ok(())
    })
    .unwrap();
}

fn host_insert_sql() -> &'static str {
    "INSERT INTO host_writes (id, content) VALUES ($id, $content)"
}

fn row_change(table: &str, id: Uuid, content: &str, lsn: Lsn) -> RowChange {
    RowChange {
        table: table.to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("content".to_string(), Value::Text(content.to_string())),
        ]),
        deleted: false,
        lsn,
    }
}

fn row_id_for_column_uuid(
    db: &Database,
    table: &str,
    column: &str,
    id: Uuid,
) -> Option<contextdb_core::RowId> {
    row_for_column_uuid(db, table, column, id).map(|row| row.row_id)
}

fn row_for_column_uuid(db: &Database, table: &str, column: &str, id: Uuid) -> Option<VersionedRow> {
    db.scan(table, db.snapshot())
        .ok()?
        .into_iter()
        .find(|row| matches!(row.values.get(column), Some(Value::Uuid(value)) if *value == id))
}

fn changeset_has_data(changes: &ChangeSet) -> bool {
    !changes.rows.is_empty()
        || !changes.edges.is_empty()
        || !changes.vectors.is_empty()
        || !changes.ddl.is_empty()
}

fn rollback_reason_contains_any(status: &TriggerAuditStatus, needles: &[&str]) -> bool {
    let TriggerAuditStatus::RolledBack { reason } = status else {
        return false;
    };
    let reason = reason.to_ascii_lowercase();
    needles
        .iter()
        .any(|needle| reason.contains(&needle.to_ascii_lowercase()))
}

fn rollback_audit_failure(db: &Database, label: &str, needles: &[&str]) -> Option<String> {
    let history = db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::RolledBack),
        })
        .unwrap();
    if history.len() != 1 {
        return Some(format!(
            "{label} rollback must create exactly one durable rolled-back audit entry; got {history:?}"
        ));
    }
    let entry = &history[0];
    if entry.trigger_name != "host_write_trigger"
        || entry.firing_tx <= TxId(0)
        || entry.firing_lsn != Lsn(0)
        || entry.depth != 1
        || entry.cascade_row_count != 0
        || !rollback_reason_contains_any(&entry.status, needles)
    {
        return Some(format!(
            "{label} rollback audit must retain trigger, tx, depth, zero committed cascade count, and a useful typed reason; got {entry:?}"
        ));
    }
    None
}

fn reopened_rollback_audit_failure(
    path: &std::path::Path,
    label: &str,
    needles: &[&str],
) -> Option<String> {
    let reopened = Database::open(path).unwrap();
    let register = reopened.register_trigger_callback("host_write_trigger", |_, _| Ok(()));
    let ready = reopened.complete_initialization();
    let failure = if register.is_err() || ready.is_err() {
        Some(format!(
            "{label} rollback audit reopen preflight failed; register={register:?}, ready={ready:?}, triggers={:?}",
            reopened.list_triggers()
        ))
    } else {
        rollback_audit_failure(&reopened, label, needles)
    };
    reopened.close().unwrap();
    failure.map(|message| format!("{label} rollback audit must survive durable reopen; {message}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HnswBaseline {
    point_count: usize,
    layer0_points: usize,
    dimension: usize,
    has_layer0_edges: bool,
    raw_entry_counts: Vec<(RowId, usize)>,
}

impl HnswBaseline {
    fn row_ids(&self) -> Vec<RowId> {
        self.raw_entry_counts
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect()
    }
}

fn hnsw_baseline_failure(
    db: &Database,
    index: VectorIndexRef,
    expected: &HnswBaseline,
    label: &str,
) -> Option<String> {
    let warm = db.query_vector(index.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot());
    let Some(stats) = db.__debug_vector_hnsw_stats(index.clone()) else {
        return Some(format!("{label} raw HNSW graph is missing for {index:?}"));
    };
    let actual = HnswBaseline {
        point_count: stats.point_count,
        layer0_points: stats.layer0_points,
        dimension: stats.dimension,
        has_layer0_edges: stats.layer0_neighbor_edges > 0,
        raw_entry_counts: expected
            .row_ids()
            .into_iter()
            .map(|row_id| {
                (
                    row_id,
                    db.__debug_vector_hnsw_raw_entry_count_for_row_for_test(index.clone(), row_id)
                        .unwrap_or(0),
                )
            })
            .collect(),
    };
    if warm.as_ref().map(|hits| hits.is_empty()).unwrap_or(true) || &actual != expected {
        let raw_mismatches = expected
            .raw_entry_counts
            .iter()
            .zip(actual.raw_entry_counts.iter())
            .filter(|(expected, actual)| expected != actual)
            .take(8)
            .collect::<Vec<_>>();
        return Some(format!(
            "{label} raw HNSW baseline changed for {index:?}; warm_empty={}, expected={expected:?}, actual={actual:?}, raw_mismatches_sample={raw_mismatches:?}",
            warm.as_ref().map(|hits| hits.is_empty()).unwrap_or(true)
        ));
    }
    None
}

fn reopened_rollback_absence_failure(
    path: &std::path::Path,
    label: &str,
    since: Lsn,
    table_counts: &[(&str, usize)],
    edge_absence: Option<(Uuid, &str)>,
    vector_absence: Option<(VectorIndexRef, Vec<RowId>, HnswBaseline)>,
) -> Option<String> {
    let reopened = Database::open(path).unwrap();
    let mut failures = Vec::new();
    for (table, expected) in table_counts {
        let actual = count_rows(&reopened, table);
        if actual != *expected {
            failures.push(format!("{table} rows: expected {expected}, got {actual}"));
        }
    }
    if let Some((source, edge_type)) = edge_absence {
        let edge_count = reopened
            .edge_count(source, edge_type, reopened.snapshot())
            .unwrap();
        if edge_count != 0 {
            failures.push(format!("{edge_type} edges for {source}: got {edge_count}"));
        }
    }
    if let Some((index, row_ids, expected_hnsw)) = vector_absence {
        let live_vector_leaks = live_vector_rows(&reopened, row_ids.iter().copied());
        let raw_hnsw_leaks = hnsw_raw_leaks(&reopened, index.clone(), row_ids);
        let hnsw_failure = hnsw_baseline_failure(
            &reopened,
            index.clone(),
            &expected_hnsw,
            &format!("{label} durable reopen"),
        );
        if !live_vector_leaks.is_empty() || !raw_hnsw_leaks.is_empty() || hnsw_failure.is_some() {
            failures.push(format!(
                "vector leaks for {index:?}: live={live_vector_leaks:?}, raw_hnsw={raw_hnsw_leaks:?}, hnsw_failure={hnsw_failure:?}"
            ));
        }
    }
    let leaked_changes = reopened.changes_since(since);
    if changeset_has_data(&leaked_changes) {
        failures.push(format!("changes_since({since}) leaked {leaked_changes:?}"));
    }
    reopened.close().unwrap();
    if failures.is_empty() {
        None
    } else {
        Some(format!(
            "{label} rollback side effects must remain absent after durable reopen; {}",
            failures.join("; ")
        ))
    }
}

fn live_vector_rows<I>(db: &Database, row_ids: I) -> Vec<RowId>
where
    I: IntoIterator<Item = RowId>,
{
    row_ids
        .into_iter()
        .filter(|row_id| db.has_live_vector(*row_id, db.snapshot()))
        .collect()
}

fn live_vector_count(db: &Database, table: &str) -> usize {
    db.scan(table, db.snapshot())
        .unwrap()
        .iter()
        .filter(|row| db.live_vector_entry(row.row_id, db.snapshot()).is_some())
        .count()
}

fn audited_edge_count(db: &Database, ids: &[Uuid]) -> usize {
    ids.iter()
        .copied()
        .map(|id| db.edge_count(id, "AUDITED_BY", db.snapshot()).unwrap())
        .sum()
}

fn fired_trigger_history(
    db: &Database,
    trigger_name: &str,
) -> Vec<contextdb_engine::TriggerAuditEntry> {
    db.trigger_audit_history(TriggerAuditFilter {
        trigger_name: Some(trigger_name.to_string()),
        status: Some(TriggerAuditStatusFilter::Fired),
    })
    .unwrap()
}

const HNSW_FIXTURE_ROWS: usize = 1_001;

fn warm_hnsw_fixture(
    db: &Database,
    index: VectorIndexRef,
    expected_rows: usize,
    baseline_row_ids: Vec<RowId>,
    label: &str,
) -> HnswBaseline {
    let warm = db
        .query_vector(index.clone(), &[0.0, 0.0, 1.0], 10, None, db.snapshot())
        .unwrap();
    assert!(
        !warm.is_empty(),
        "{label} HNSW warmup query must return fixture rows"
    );
    let stats = db
        .__debug_vector_hnsw_stats(index.clone())
        .expect("fixture must build a raw HNSW graph");
    assert_eq!(
        stats.point_count, expected_rows,
        "{label} fixture must exercise raw HNSW, not just MVCC vector entries; stats={stats:?}"
    );
    assert_eq!(
        stats.layer0_points, expected_rows,
        "{label} fixture must put every point into layer 0; stats={stats:?}"
    );
    assert!(
        stats.layer0_neighbor_edges > 0,
        "{label} fixture must expose real HNSW neighbor edges; stats={stats:?}"
    );
    let baseline = HnswBaseline {
        point_count: stats.point_count,
        layer0_points: stats.layer0_points,
        dimension: stats.dimension,
        has_layer0_edges: true,
        raw_entry_counts: baseline_row_ids
            .into_iter()
            .map(|row_id| {
                (
                    row_id,
                    db.__debug_vector_hnsw_raw_entry_count_for_row_for_test(index.clone(), row_id)
                        .expect("fixture row must be present in raw HNSW graph"),
                )
            })
            .collect(),
    };
    assert!(
        baseline
            .raw_entry_counts
            .iter()
            .all(|(_, count)| *count == 1),
        "{label} every fixture row must have exactly one raw HNSW entry; baseline={baseline:?}"
    );
    baseline
}

fn seed_sibling_hnsw_vectors(db: &Database) -> (usize, u64, HnswBaseline) {
    for idx in 0..HNSW_FIXTURE_ROWS {
        db.execute(
            "INSERT INTO sibling_vectors (id, content, embedding) VALUES ($id, $content, $embedding)",
            &HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(uuid(0xA000_u128 + idx as u128)),
                ),
                (
                    "content".to_string(),
                    Value::Text(format!("hnsw-baseline-{idx}")),
                ),
                ("embedding".to_string(), Value::Vector(vec![0.0, 0.2, 0.98])),
            ]),
        )
        .unwrap();
    }
    let index = VectorIndexRef::new("sibling_vectors", "embedding");
    let rows = db
        .scan("sibling_vectors", db.snapshot())
        .expect("HNSW fixture must create sibling vector rows");
    let max_row_id = rows
        .iter()
        .map(|row| row.row_id.0)
        .max()
        .expect("HNSW fixture must create sibling vector rows");
    let baseline = warm_hnsw_fixture(
        db,
        index,
        HNSW_FIXTURE_ROWS,
        rows.into_iter().map(|row| row.row_id).collect(),
        "cold apply rollback",
    );
    (HNSW_FIXTURE_ROWS, max_row_id, baseline)
}

fn seed_host_audit_hnsw(db: &Database) -> (usize, u64, HnswBaseline) {
    for idx in 0..HNSW_FIXTURE_ROWS {
        db.execute(
            "INSERT INTO host_audits (id, write_id, note, embedding) VALUES ($id, $write_id, $note, $embedding)",
            &HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(uuid(0xB000_u128 + idx as u128)),
                ),
                (
                    "write_id".to_string(),
                    Value::Uuid(uuid(0xC000_u128 + idx as u128)),
                ),
                ("note".to_string(), Value::Text(format!("baseline-{idx}"))),
                ("embedding".to_string(), Value::Vector(vec![0.0, 0.2, 0.98])),
            ]),
        )
        .unwrap();
    }
    let index = VectorIndexRef::new("host_audits", "embedding");
    let rows = db
        .scan("host_audits", db.snapshot())
        .expect("HNSW fixture must create host audit vector rows");
    let max_row_id = rows
        .iter()
        .map(|row| row.row_id.0)
        .max()
        .expect("HNSW fixture must create host audit vector rows");
    let baseline = warm_hnsw_fixture(
        db,
        index,
        HNSW_FIXTURE_ROWS,
        rows.into_iter().map(|row| row.row_id).collect(),
        "cascade rollback",
    );
    (HNSW_FIXTURE_ROWS, max_row_id, baseline)
}

fn hnsw_raw_leaks<I>(db: &Database, index: VectorIndexRef, row_ids: I) -> Vec<(RowId, usize)>
where
    I: IntoIterator<Item = RowId>,
{
    row_ids
        .into_iter()
        .filter_map(|row_id| {
            db.__debug_vector_hnsw_raw_entry_count_for_row_for_test(index.clone(), row_id)
                .filter(|count| *count > 0)
                .map(|count| (row_id, count))
        })
        .collect()
}

fn candidate_row_ids_after(max_row_id: u64) -> Vec<RowId> {
    ((max_row_id + 1)..=(max_row_id + 32))
        .map(RowId::from_raw_wire)
        .collect()
}

fn value_map_signature(values: &HashMap<String, Value>) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), format!("{value:?}")))
        .collect()
}

fn row_group_signatures(db: &Database, changes: ChangeSet) -> Vec<BTreeSet<String>> {
    let mut by_lsn = BTreeMap::<Lsn, BTreeSet<String>>::new();
    for row in changes.rows {
        let Some(Value::Uuid(id)) = row.values.get("id") else {
            continue;
        };
        let detail = value_map_signature(&row.values);
        by_lsn
            .entry(row.lsn)
            .or_default()
            .insert(format!("row:{}:{}:{detail:?}", row.table, id));
    }
    for edge in changes.edges {
        let props = edge
            .properties
            .iter()
            .map(|(key, value)| format!("{key}={value:?}"))
            .collect::<BTreeSet<_>>();
        by_lsn.entry(edge.lsn).or_default().insert(format!(
            "edge:{}:{}:{}:{props:?}",
            edge.source, edge.edge_type, edge.target
        ));
    }
    for vector in changes.vectors {
        let row_identity = db
            .scan(&vector.index.table, db.snapshot_at(vector.lsn))
            .unwrap_or_default()
            .into_iter()
            .find(|row| row.row_id == vector.row_id)
            .and_then(|row| {
                row.values
                    .get("write_id")
                    .or_else(|| row.values.get("id"))
                    .cloned()
            })
            .map(|value| format!("{value:?}"))
            .unwrap_or_else(|| format!("row_id:{}", vector.row_id));
        by_lsn.entry(vector.lsn).or_default().insert(format!(
            "vector:{}.{}:{}:{:?}",
            vector.index.table, vector.index.column, row_identity, vector.vector
        ));
    }
    by_lsn.into_values().collect()
}

#[test]
fn t3_gate_writes_to_trigger_table_rejected_until_callback_ready() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("cold_gate.redb");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE sibling_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sibling_vectors (id UUID PRIMARY KEY, content TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let (sibling_hnsw_count, sibling_hnsw_max_row_id, sibling_hnsw_baseline) =
        seed_sibling_hnsw_vectors(&db);
    let sibling_hnsw_index = VectorIndexRef::new("sibling_vectors", "embedding");
    let cold_remote_vector_row_id = RowId::from_raw_wire(sibling_hnsw_max_row_id + 64);
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();

    assert!(
        db.execute(
            "INSERT INTO sibling_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000001', 'ok')",
            &empty(),
        )
        .is_ok(),
        "cold-start gate must allow tables without trigger declarations"
    );

    let mut failures = Vec::new();
    let sync_since = db.current_lsn();
    let sync_gated = db.apply_changes(
        ChangeSet {
            rows: vec![
                row_change(
                    "sibling_writes",
                    uuid(0x199),
                    "sync-sibling-must-rollback",
                    Lsn(1),
                ),
                RowChange {
                    table: "sibling_vectors".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(uuid(0x198)),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(uuid(0x198))),
                        (
                            "content".to_string(),
                            Value::Text("sync-vector-must-rollback".into()),
                        ),
                        ("embedding".to_string(), Value::Vector(vec![0.1, 0.2, 0.3])),
                    ]),
                    deleted: false,
                    lsn: Lsn(1),
                },
                row_change("host_writes", uuid(0x200), "sync-blocked", Lsn(1)),
            ],
            edges: vec![EdgeChange {
                source: uuid(0x199),
                target: uuid(0x198),
                edge_type: "COLD_APPLY_LEAK".into(),
                properties: HashMap::from([("should_rollback".into(), Value::Bool(true))]),
                lsn: Lsn(1),
            }],
            vectors: vec![VectorChange {
                index: VectorIndexRef::new("sibling_vectors", "embedding"),
                row_id: cold_remote_vector_row_id,
                vector: vec![0.1, 0.2, 0.3],
                lsn: Lsn(1),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let cold_row_id = row_id_for_column_uuid(&db, "sibling_vectors", "id", uuid(0x198));
    let mut cold_vector_probe_rows = candidate_row_ids_after(sibling_hnsw_max_row_id);
    cold_vector_probe_rows.extend(cold_row_id);
    cold_vector_probe_rows.push(cold_remote_vector_row_id);
    let cold_vector_leaks = live_vector_rows(&db, cold_vector_probe_rows.iter().copied());
    let cold_hnsw_raw_leaks = hnsw_raw_leaks(
        &db,
        sibling_hnsw_index.clone(),
        cold_vector_probe_rows.iter().copied(),
    );
    let cold_hnsw_failure = hnsw_baseline_failure(
        &db,
        sibling_hnsw_index.clone(),
        &sibling_hnsw_baseline,
        "cold gate apply rollback",
    );
    if !matches!(
        sync_gated,
        Err(Error::EngineNotInitialized { ref operation }) if operation.contains("apply_changes")
    ) || count_rows(&db, "sibling_writes") != 1
        || count_rows(&db, "sibling_vectors") != sibling_hnsw_count
        || count_rows(&db, "host_writes") != 0
        || db
            .edge_count(uuid(0x199), "COLD_APPLY_LEAK", db.snapshot())
            .unwrap()
            != 0
        || !cold_vector_leaks.is_empty()
        || !cold_hnsw_raw_leaks.is_empty()
        || cold_hnsw_failure.is_some()
        || changeset_has_data(&db.changes_since(sync_since))
    {
        failures.push(format!(
            "sync apply into trigger-attached tables must fail closed atomically until callbacks are ready; got {sync_gated:?}, sibling_rows={}, sibling_vector_rows={}, host_rows={}, leaked_edges={}, leaked_vector_rows={cold_vector_leaks:?}, leaked_hnsw_rows={cold_hnsw_raw_leaks:?}, hnsw_failure={cold_hnsw_failure:?}, leaked_changes={:?}",
            count_rows(&db, "sibling_writes"),
            count_rows(&db, "sibling_vectors"),
            count_rows(&db, "host_writes"),
            db.edge_count(uuid(0x199), "COLD_APPLY_LEAK", db.snapshot())
                .unwrap(),
            db.changes_since(sync_since)
        ));
    }

    let global_apply_since = db.current_lsn();
    let non_trigger_apply = db.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "sibling_writes",
                uuid(0x201),
                "non-trigger-sync-still-blocked-while-booting",
                Lsn(2),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let ddl_only_apply = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTable {
                name: "cold_sync_ddl".into(),
                columns: vec![("id".into(), "UUID PRIMARY KEY".into())],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(2)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let leaked_global_apply_changes = db.changes_since(global_apply_since);
    if !matches!(
        non_trigger_apply,
        Err(Error::EngineNotInitialized { ref operation }) if operation.contains("apply_changes")
    ) || !matches!(
        ddl_only_apply,
        Err(Error::EngineNotInitialized { ref operation }) if operation.contains("apply_changes")
    ) || count_rows(&db, "sibling_writes") != 1
        || changeset_has_data(&leaked_global_apply_changes)
        || !leaked_global_apply_changes.ddl.is_empty()
    {
        failures.push(format!(
            "a booting trigger handle must reject sync apply with data or non-tombstone DDL-only batches; non_trigger={non_trigger_apply:?}, ddl_only={ddl_only_apply:?}, sibling_rows={}, leaked_changes={leaked_global_apply_changes:?}",
            count_rows(&db, "sibling_writes")
        ));
    }

    let gated = db.execute(
        "INSERT INTO host_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000002', 'blocked')",
        &empty(),
    );
    if !matches!(
        gated,
        Err(Error::EngineNotInitialized { ref operation }) if operation.contains("host_writes")
    ) {
        failures.push(format!(
            "trigger-attached writes must fail closed until callbacks are ready; got {gated:?}"
        ));
    }

    let init = db.complete_initialization();
    if !matches!(
        init,
        Err(Error::TriggerCallbackMissing { ref trigger_name }) if trigger_name == "host_write_trigger"
    ) {
        failures.push(format!(
            "initialization must name the missing callback; got {init:?}"
        ));
    }
    db.close().unwrap();
    if let Some(failure) = reopened_rollback_absence_failure(
        &path,
        "cold gate apply",
        sync_since,
        &[
            ("sibling_writes", 1),
            ("sibling_vectors", sibling_hnsw_count),
            ("host_writes", 0),
        ],
        Some((uuid(0x199), "COLD_APPLY_LEAK")),
        Some((
            sibling_hnsw_index.clone(),
            cold_vector_probe_rows,
            sibling_hnsw_baseline,
        )),
    ) {
        failures.push(failure);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    let ready_db = Database::open(&path).unwrap();
    ready_db
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    ready_db
        .complete_initialization()
        .expect("initialization must succeed once callback is registered");
    ready_db.close().unwrap();

    let no_trigger_db = Database::open_memory();
    no_trigger_db
        .execute(
            "CREATE TABLE plain (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    no_trigger_db
        .complete_initialization()
        .expect("complete_initialization must be a no-op success when no triggers are declared");
    no_trigger_db
        .execute(
            "INSERT INTO plain (id, content) VALUES ('00000000-0000-0000-0000-000000000111', 'plain')",
            &empty(),
        )
        .expect("zero-trigger database must remain writable after initialization");
}

#[test]
fn t3_sync_apply_requires_complete_initialization_after_callback_registration() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("callback_registered_not_initialized.redb");
    {
        let db = Database::open(&path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }

    let db = Database::open(&path).unwrap();
    db.register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    let since = db.current_lsn();
    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateIndex {
                table: "host_writes".into(),
                name: "host_content_idx".into(),
                columns: vec![("content".into(), SortDirection::Asc)],
            }],
            ddl_lsn: vec![Lsn(10)],
            rows: vec![row_change(
                "host_writes",
                uuid(0x203),
                "registered-callback-is-not-ready",
                Lsn(10),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let leaked_changes = db.changes_since(since);
    let has_user_index = db
        .table_meta("host_writes")
        .unwrap()
        .indexes
        .iter()
        .any(|index| index.name == "host_content_idx");

    assert!(
        matches!(
            result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("apply_changes")
        ) && count_rows(&db, "host_writes") == 0
            && !has_user_index
            && leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "registering callbacks must not bypass explicit complete_initialization for sync apply; result={result:?}, has_user_index={has_user_index}, leaked_changes={leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_tombstones_can_clear_cold_reopened_declarations_without_callbacks() {
    let tmp = TempDir::new().unwrap();

    let unknown_drop_trigger_path = tmp.path().join("cold_unknown_drop_trigger.redb");
    {
        let db = Database::open(&unknown_drop_trigger_path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }
    let unknown_drop_trigger_db = Database::open(&unknown_drop_trigger_path).unwrap();
    let unknown_drop_trigger_since = unknown_drop_trigger_db.current_lsn();
    let unknown_drop_trigger_result = unknown_drop_trigger_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "unknown_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let unknown_drop_trigger_ddl = unknown_drop_trigger_db
        .changes_since(unknown_drop_trigger_since)
        .ddl;
    assert!(
        matches!(
            unknown_drop_trigger_result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("apply_changes")
        ) && unknown_drop_trigger_db
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "host_write_trigger")
            && unknown_drop_trigger_ddl.is_empty(),
        "unknown sync DropTrigger must not bypass the cold durable-trigger callback gate or append tombstones; result={unknown_drop_trigger_result:?}, triggers={:?}, ddl={unknown_drop_trigger_ddl:?}",
        unknown_drop_trigger_db.list_triggers()
    );
    unknown_drop_trigger_db.close().unwrap();

    let data_then_drop_trigger_path = tmp.path().join("cold_data_then_drop_trigger.redb");
    {
        let db = Database::open(&data_then_drop_trigger_path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }
    let data_then_drop_trigger_db = Database::open(&data_then_drop_trigger_path).unwrap();
    let data_then_drop_trigger_since = data_then_drop_trigger_db.current_lsn();
    let data_then_drop_trigger_result = data_then_drop_trigger_db.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "host_writes",
                uuid(0x217),
                "must-not-bypass-cold-gate-before-drop",
                Lsn(99),
            )],
            ddl: vec![DdlChange::DropTrigger {
                name: "host_write_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let data_then_drop_changes =
        data_then_drop_trigger_db.changes_since(data_then_drop_trigger_since);
    assert!(
        matches!(
            data_then_drop_trigger_result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("host_write_trigger")
        ) && count_rows(&data_then_drop_trigger_db, "host_writes") == 0
            && data_then_drop_trigger_db
                .list_triggers()
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger")
            && data_then_drop_changes.rows.is_empty()
            && data_then_drop_changes.ddl.is_empty(),
        "trigger-table data before a same-batch DropTrigger must not bypass a cold persisted trigger gate; result={data_then_drop_trigger_result:?}, triggers={:?}, changes={data_then_drop_changes:?}",
        data_then_drop_trigger_db.list_triggers()
    );
    data_then_drop_trigger_db.close().unwrap();

    let same_lsn_data_drop_trigger_path = tmp.path().join("cold_same_lsn_data_drop_trigger.redb");
    {
        let db = Database::open(&same_lsn_data_drop_trigger_path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }
    let same_lsn_data_drop_trigger_db = Database::open(&same_lsn_data_drop_trigger_path).unwrap();
    let same_lsn_data_drop_trigger_since = same_lsn_data_drop_trigger_db.current_lsn();
    let same_lsn_data_drop_trigger_result = same_lsn_data_drop_trigger_db.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "host_writes",
                uuid(0x218),
                "must-not-bypass-cold-gate-at-drop-lsn",
                Lsn(100),
            )],
            ddl: vec![DdlChange::DropTrigger {
                name: "host_write_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let same_lsn_data_drop_changes =
        same_lsn_data_drop_trigger_db.changes_since(same_lsn_data_drop_trigger_since);
    assert!(
        matches!(
            same_lsn_data_drop_trigger_result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("host_write_trigger")
        ) && count_rows(&same_lsn_data_drop_trigger_db, "host_writes") == 0
            && same_lsn_data_drop_trigger_db
                .list_triggers()
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger")
            && same_lsn_data_drop_changes.rows.is_empty()
            && same_lsn_data_drop_changes.ddl.is_empty(),
        "trigger-table data at the same sender LSN as DropTrigger must not bypass a cold persisted trigger gate; result={same_lsn_data_drop_trigger_result:?}, triggers={:?}, changes={same_lsn_data_drop_changes:?}",
        same_lsn_data_drop_trigger_db.list_triggers()
    );
    same_lsn_data_drop_trigger_db.close().unwrap();

    let partial_drop_trigger_path = tmp.path().join("cold_partial_drop_trigger.redb");
    {
        let db = Database::open(&partial_drop_trigger_path).unwrap();
        setup_host_tables(&db);
        db.execute(
            "CREATE TABLE other_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TRIGGER other_write_trigger ON other_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
        db.close().unwrap();
    }
    let partial_drop_trigger_db = Database::open(&partial_drop_trigger_path).unwrap();
    let partial_drop_trigger_since = partial_drop_trigger_db.current_lsn();
    let partial_drop_trigger_result = partial_drop_trigger_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "host_write_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let partial_drop_trigger_ddl = partial_drop_trigger_db
        .changes_since(partial_drop_trigger_since)
        .ddl;
    let partial_triggers = partial_drop_trigger_db.list_triggers();
    let still_booting = partial_drop_trigger_db.complete_initialization();
    assert!(
        partial_drop_trigger_result.is_ok()
            && partial_triggers
                .iter()
                .all(|trigger| trigger.name != "host_write_trigger")
            && partial_triggers
                .iter()
                .any(|trigger| trigger.name == "other_write_trigger")
            && matches!(
                still_booting,
                Err(Error::TriggerCallbackMissing { ref trigger_name })
                    if trigger_name == "other_write_trigger"
            )
            && partial_drop_trigger_ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
            ),
        "known tombstone-only sync cleanup must be allowed to remove one cold trigger while another trigger remains fail-closed; result={partial_drop_trigger_result:?}, triggers={partial_triggers:?}, init={still_booting:?}, ddl={partial_drop_trigger_ddl:?}"
    );
    partial_drop_trigger_db.close().unwrap();

    let registered_partial_drop_path = tmp.path().join("cold_registered_partial_drop_trigger.redb");
    {
        let db = Database::open(&registered_partial_drop_path).unwrap();
        setup_host_tables(&db);
        db.execute(
            "CREATE TABLE other_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TRIGGER other_write_trigger ON other_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
        db.close().unwrap();
    }
    let registered_partial_drop_db = Database::open(&registered_partial_drop_path).unwrap();
    registered_partial_drop_db
        .register_trigger_callback("other_write_trigger", |_, _| Ok(()))
        .unwrap();
    let registered_partial_drop_result = registered_partial_drop_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "host_write_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let still_requires_explicit_init = registered_partial_drop_db.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "other_writes",
                uuid(0x219),
                "must-not-bypass-explicit-initialization",
                Lsn(101),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    assert!(
        registered_partial_drop_result.is_ok()
            && matches!(
                still_requires_explicit_init,
                Err(Error::EngineNotInitialized { ref operation })
                    if operation.contains("apply_changes")
            )
            && count_rows(&registered_partial_drop_db, "other_writes") == 0
            && registered_partial_drop_db.complete_initialization().is_ok(),
        "tombstone cleanup must not infer readiness for a cold handle that has registered remaining callbacks but has not explicitly completed initialization; cleanup={registered_partial_drop_result:?}, write={still_requires_explicit_init:?}, triggers={:?}",
        registered_partial_drop_db.list_triggers()
    );
    registered_partial_drop_db.close().unwrap();

    let drop_trigger_path = tmp.path().join("cold_drop_trigger.redb");
    {
        let db = Database::open(&drop_trigger_path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }
    let drop_trigger_db = Database::open(&drop_trigger_path).unwrap();
    let drop_trigger_since = drop_trigger_db.current_lsn();
    let drop_trigger_result = drop_trigger_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "host_write_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let drop_trigger_ddl = drop_trigger_db.changes_since(drop_trigger_since).ddl;
    assert!(
        drop_trigger_result.is_ok()
            && drop_trigger_db.list_triggers().is_empty()
            && drop_trigger_db.complete_initialization().is_ok()
            && drop_trigger_ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
            ),
        "cold reopened trigger handle must accept a sync DropTrigger tombstone without requiring the deleted callback; result={drop_trigger_result:?}, triggers={:?}, ddl={drop_trigger_ddl:?}",
        drop_trigger_db.list_triggers()
    );
    drop_trigger_db.close().unwrap();

    let drop_table_path = tmp.path().join("cold_drop_table.redb");
    {
        let db = Database::open(&drop_table_path).unwrap();
        setup_host_tables(&db);
        db.close().unwrap();
    }
    let drop_table_db = Database::open(&drop_table_path).unwrap();
    let drop_table_since = drop_table_db.current_lsn();
    let drop_table_result = drop_table_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTable {
                name: "host_writes".into(),
            }],
            ddl_lsn: vec![Lsn(100)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let drop_table_ddl = drop_table_db.changes_since(drop_table_since).ddl;
    assert!(
        drop_table_result.is_ok()
            && drop_table_db.table_meta("host_writes").is_none()
            && drop_table_db.list_triggers().is_empty()
            && drop_table_db.complete_initialization().is_ok()
            && drop_table_ddl
                .iter()
                .any(|change| matches!(change, DdlChange::DropTable { name } if name == "host_writes"))
            && drop_table_ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
            ),
        "cold reopened trigger handle must accept a sync DropTable tombstone and emit the implicit DropTrigger without requiring the deleted callback; result={drop_table_result:?}, triggers={:?}, ddl={drop_table_ddl:?}",
        drop_table_db.list_triggers()
    );
}

#[test]
fn t3_sync_trigger_ddl_rejects_invalid_event_bus_batch_without_trigger_leak() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "trigger_before_bad_event_route".into(),
                    table: "host_writes".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::CreateSink {
                    name: "partial".into(),
                    sink_type: "CALLBACK".into(),
                    url: None,
                },
                DdlChange::CreateRoute {
                    name: "bad_route".into(),
                    event_type: "missing_event".into(),
                    sink: "partial".into(),
                    table: "host_writes".into(),
                    where_in: None,
                },
            ],
            ddl_lsn: vec![Lsn(7), Lsn(7), Lsn(7)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(result.is_err(), "invalid mixed DDL batch must fail");
    assert!(
        db.list_triggers()
            .iter()
            .all(|trigger| trigger.name != "trigger_before_bad_event_route"),
        "failed sync batch must not persist trigger declarations"
    );
    assert!(
        db.changes_since(since).ddl.iter().all(|ddl| {
            !matches!(
                ddl,
                DdlChange::CreateTrigger { name, .. }
                    if name == "trigger_before_bad_event_route"
            )
        }),
        "failed sync batch must not append trigger DDL history"
    );
    assert!(
        db.register_sink("partial", None, |_| Ok(())).is_err(),
        "failed sync batch must not leave an EventBus sink behind"
    );
}

#[test]
fn t3_sync_trigger_ddl_with_same_batch_rows_fails_closed_until_callback_ready() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "synced_trigger".into(),
                table: "host_writes".into(),
                on_events: vec!["INSERT".into()],
            }],
            ddl_lsn: vec![Lsn(7)],
            rows: vec![row_change(
                "host_writes",
                uuid(0x7001),
                "must-not-ingest-before-callback",
                Lsn(7),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("synced_trigger")
        ),
        "sync must fail closed before ingesting rows for trigger-attached tables without callbacks; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "failed mixed sync batch must not persist trigger declarations"
    );
    assert_eq!(
        count_rows(&db, "host_writes"),
        0,
        "failed mixed sync batch must not ingest trigger-attached rows"
    );
    assert!(
        db.changes_since(since).ddl.iter().all(|ddl| {
            !matches!(
                ddl,
                DdlChange::CreateTrigger { name, .. } if name == "synced_trigger"
            )
        }),
        "failed mixed sync batch must not append trigger DDL history"
    );
}

#[test]
fn t3_sync_trigger_ddl_with_same_batch_unrelated_data_fails_closed_until_callback_ready() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE plain_rows (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "synced_trigger".into(),
                table: "host_writes".into(),
                on_events: vec!["INSERT".into()],
            }],
            ddl_lsn: vec![Lsn(7)],
            rows: vec![row_change(
                "plain_rows",
                uuid(0x7002),
                "must-not-ingest-unrelated-data-before-callback",
                Lsn(7),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("synced_trigger")
                    && operation.contains("apply_changes")
        ),
        "sync must fail closed for any same-batch data once trigger DDL would require callbacks; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "failed mixed sync batch must not persist trigger declarations"
    );
    assert_eq!(
        count_rows(&db, "plain_rows"),
        0,
        "failed mixed sync batch must not ingest unrelated data while the handle would become booting"
    );
    assert!(
        !changeset_has_data(&db.changes_since(since))
            && db.changes_since(since).ddl.iter().all(|ddl| {
                !matches!(
                    ddl,
                    DdlChange::CreateTrigger { name, .. } if name == "synced_trigger"
                )
            }),
        "failed mixed sync batch must not append data or trigger DDL history"
    );
}

#[test]
fn t3_sync_trigger_ddl_with_same_batch_vector_data_fails_before_trigger_leak() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_vectors (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "vector_trigger".into(),
                table: "host_vectors".into(),
                on_events: vec!["INSERT".into()],
            }],
            ddl_lsn: vec![Lsn(7)],
            vectors: vec![VectorChange {
                index: VectorIndexRef::new("host_vectors", "embedding"),
                row_id: RowId(7003),
                vector: vec![1.0, 0.0, 0.0],
                lsn: Lsn(7),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_vectors")
                    && operation.contains("vector_trigger")
                    && operation.contains("apply_changes")
        ),
        "sync must reject vector-only data before durable trigger DDL side effects; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "failed vector-only mixed sync batch must not persist trigger declarations"
    );
    assert!(
        !changeset_has_data(&db.changes_since(since))
            && db.changes_since(since).ddl.iter().all(|ddl| {
                !matches!(
                    ddl,
                    DdlChange::CreateTrigger { name, .. } if name == "vector_trigger"
                )
            }),
        "failed vector-only mixed sync batch must not append data or trigger DDL history"
    );
}

#[test]
fn t3_sync_mixed_ddl_preflight_uses_sender_lsn_order_before_trigger_side_effects() {
    let db = Database::open_memory();
    setup_ready_host_write_trigger(&db);
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::DropTrigger {
                    name: "host_write_trigger".into(),
                },
                DdlChange::CreateTable {
                    name: "future_rows".into(),
                    columns: vec![
                        ("id".into(), "UUID PRIMARY KEY".into()),
                        ("content".into(), "TEXT".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(30)],
            rows: vec![row_change(
                "future_rows",
                uuid(0x7004),
                "row-before-its-schema-must-not-leak-earlier-ddl",
                Lsn(20),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(result, Err(Error::TableNotFound(ref table)) if table == "future_rows"),
        "preflight must reject rows against the schema visible at their sender LSN; got {result:?}"
    );
    assert!(
        db.list_triggers()
            .iter()
            .any(|trigger| trigger.name == "host_write_trigger"),
        "earlier trigger DDL must not leak when a later sender-LSN row is invalid"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "chronological preflight rejection must leave no durable sync side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_mixed_ddl_preflight_rejects_invalid_table_ddl_before_side_effects() {
    let invalid_type_db = Database::open_memory();
    let invalid_type_since = invalid_type_db.current_lsn();
    let invalid_type_result = invalid_type_db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "leaked_bad_type_prefix".into(),
                    columns: vec![("id".into(), "UUID PRIMARY KEY".into())],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTable {
                    name: "bad_type_table".into(),
                    columns: vec![("id".into(), "NOT_A_TYPE".into())],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![Lsn(5), Lsn(5)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );
    let invalid_type_leaked_changes = invalid_type_db.changes_since(invalid_type_since);
    assert!(
        matches!(
            invalid_type_result,
            Err(Error::ParseError(ref message)) if message.contains("NOT_A_TYPE")
        ) && invalid_type_db
            .table_meta("leaked_bad_type_prefix")
            .is_none()
            && invalid_type_db.table_meta("bad_type_table").is_none()
            && invalid_type_leaked_changes.ddl.is_empty()
            && invalid_type_leaked_changes.rows.is_empty()
            && invalid_type_leaked_changes.edges.is_empty()
            && invalid_type_leaked_changes.vectors.is_empty(),
        "sync DDL preflight must reject invalid CreateTable type before applying earlier same-LSN DDL; result={invalid_type_result:?}, leaked_changes={invalid_type_leaked_changes:?}"
    );

    let db = Database::open_memory();
    let create_since = db.current_lsn();
    let create_result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "leaked_sync_ddl".into(),
                    columns: vec![
                        ("id".into(), "UUID PRIMARY KEY".into()),
                        ("content".into(), "TEXT".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateIndex {
                    table: "missing_index_table".into(),
                    name: "missing_content_idx".into(),
                    columns: vec![("content".into(), SortDirection::Asc)],
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(10)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(create_result, Err(Error::TableNotFound(ref table)) if table == "missing_index_table"),
        "sync DDL preflight must reject CreateIndex for a missing projected table before applying earlier same-LSN DDL; got {create_result:?}"
    );
    let create_leaked_changes = db.changes_since(create_since);
    assert!(
        db.table_meta("leaked_sync_ddl").is_none()
            && create_leaked_changes.ddl.is_empty()
            && create_leaked_changes.rows.is_empty()
            && create_leaked_changes.edges.is_empty()
            && create_leaked_changes.vectors.is_empty(),
        "invalid same-LSN sync DDL must leave no durable create-table side effects; got {create_leaked_changes:?}"
    );

    db.execute(
        "CREATE TABLE doomed_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    let drop_since = db.current_lsn();
    let drop_result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::DropTable {
                    name: "doomed_writes".into(),
                },
                DdlChange::CreateIndex {
                    table: "doomed_writes".into(),
                    name: "doomed_content_idx".into(),
                    columns: vec![("content".into(), SortDirection::Asc)],
                },
            ],
            ddl_lsn: vec![Lsn(20), Lsn(20)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(drop_result, Err(Error::TableNotFound(ref table)) if table == "doomed_writes"),
        "sync DDL preflight must reject CreateIndex after a same-LSN DropTable before dropping the local table; got {drop_result:?}"
    );
    let drop_leaked_changes = db.changes_since(drop_since);
    assert!(
        db.table_meta("doomed_writes").is_some()
            && drop_leaked_changes.ddl.is_empty()
            && drop_leaked_changes.rows.is_empty()
            && drop_leaked_changes.edges.is_empty()
            && drop_leaked_changes.vectors.is_empty(),
        "invalid same-LSN sync DDL must leave no durable drop-table side effects; got {drop_leaked_changes:?}"
    );

    let bad_row_db = Database::open_memory();
    bad_row_db
        .execute(
            "CREATE TABLE type_checked_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    let bad_row_since = bad_row_db.current_lsn();
    let bad_row_result = bad_row_db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "bad_row_trigger".into(),
                table: "type_checked_writes".into(),
                on_events: vec!["INSERT".into()],
            }],
            ddl_lsn: vec![Lsn(30)],
            rows: vec![RowChange {
                table: "type_checked_writes".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(uuid(0x1530)),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x1530))),
                    ("content".to_string(), Value::TxId(TxId(30))),
                ]),
                deleted: false,
                lsn: Lsn(30),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let bad_row_leaked_changes = bad_row_db.changes_since(bad_row_since);
    assert!(
        matches!(
            bad_row_result,
            Err(Error::ColumnTypeMismatch {
                ref table,
                ref column,
                ..
            }) if table == "type_checked_writes" && column == "content"
        ) && bad_row_db
            .list_triggers()
            .iter()
            .all(|trigger| trigger.name != "bad_row_trigger")
            && count_rows(&bad_row_db, "type_checked_writes") == 0
            && bad_row_leaked_changes.ddl.is_empty()
            && bad_row_leaked_changes.rows.is_empty()
            && bad_row_leaked_changes.edges.is_empty()
            && bad_row_leaked_changes.vectors.is_empty(),
        "mixed sync DDL/data preflight must reject invalid row values before trigger DDL or row side effects; result={bad_row_result:?}, leaked={bad_row_leaked_changes:?}, triggers={:?}",
        bad_row_db.list_triggers()
    );
}

#[test]
fn t3_sync_mixed_ddl_preflight_rejects_noncanonical_natural_key_envelopes_without_trigger_leak() {
    let cases = [
        (
            "column mismatch",
            RowChange {
                table: "host_writes".into(),
                natural_key: NaturalKey {
                    column: "content".into(),
                    value: Value::Text("content-as-key".into()),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x7005))),
                    ("content".to_string(), Value::Text("content-as-key".into())),
                ]),
                deleted: false,
                lsn: Lsn(20),
            },
        ),
        (
            "value mismatch",
            RowChange {
                table: "host_writes".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(uuid(0x7006)),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(uuid(0x7007))),
                    ("content".to_string(), Value::Text("wrong-envelope".into())),
                ]),
                deleted: false,
                lsn: Lsn(20),
            },
        ),
    ];

    for (expected_message, row) in cases {
        let db = Database::open_memory();
        setup_ready_host_write_trigger(&db);
        let since = db.current_lsn();

        let result = db.apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::DropTrigger {
                    name: "host_write_trigger".into(),
                }],
                ddl_lsn: vec![Lsn(10)],
                rows: vec![row],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        );

        let Err(Error::SyncError(message)) = result else {
            panic!(
                "malformed sync natural key envelope must fail before DDL apply; got {result:?}"
            );
        };
        assert!(
            message.contains("natural key") && message.contains(expected_message),
            "natural key rejection should explain {expected_message}; got {message}"
        );
        assert!(
            db.list_triggers()
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger"),
            "trigger drop must not leak for malformed natural-key envelope"
        );
        assert_eq!(
            count_rows(&db, "host_writes"),
            0,
            "malformed natural-key envelope must not ingest host rows"
        );
        let leaked_changes = db.changes_since(since);
        assert!(
            leaked_changes.ddl.is_empty()
                && leaked_changes.rows.is_empty()
                && leaked_changes.edges.is_empty()
                && leaked_changes.vectors.is_empty(),
            "malformed natural-key rejection must leave no durable sync side effects; got {leaked_changes:?}"
        );
    }
}

#[test]
fn t3_drop_table_removes_attached_triggers_and_persists_tombstones() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("drop_table_trigger_cleanup.redb");
    let db = Database::open(&path).unwrap();
    setup_host_tables(&db);
    let since = db.current_lsn();

    db.execute("DROP TABLE host_writes", &empty()).unwrap();

    assert!(
        db.list_triggers().is_empty(),
        "dropping a trigger-attached table must detach its trigger declarations"
    );
    let ddl = db.changes_since(since).ddl;
    assert!(
        ddl.iter()
            .any(|change| matches!(change, DdlChange::DropTable { name } if name == "host_writes"))
            && ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
            ),
        "DROP TABLE must emit both the table tombstone and implicit trigger tombstone; got {ddl:?}"
    );
    db.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    assert!(
        reopened.list_triggers().is_empty() && reopened.complete_initialization().is_ok(),
        "implicit trigger cleanup must survive reopen without cold-starting dead schema; triggers={:?}",
        reopened.list_triggers()
    );
    let reopened_ddl = reopened.changes_since(Lsn(0)).ddl;
    assert!(
        reopened_ddl.iter().any(
            |change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
        ),
        "implicit trigger tombstone must be durable for downstream sync; got {reopened_ddl:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_preflight_rejects_fk_invalid_data_before_any_trigger_ddl_leaks() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent_rows (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), content TEXT)",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();
    let child_id = uuid(0x7008);
    let missing_parent = uuid(0x7009);

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "child_trigger".into(),
                    table: "child_rows".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "child_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(30)],
            rows: vec![RowChange {
                table: "child_rows".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(child_id),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(child_id)),
                    ("parent_id".to_string(), Value::Uuid(missing_parent)),
                    ("content".to_string(), Value::Text("bad-fk".into())),
                ]),
                deleted: false,
                lsn: Lsn(20),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_id".to_string()]
        ),
        "FK-invalid sync data must fail during preflight before trigger DDL is applied; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "preflight rejection must not leave a trigger declaration behind"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "FK preflight rejection must leave no durable DDL/data side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_fk_preflight_respects_sender_lsn_order() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent_rows (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), content TEXT)",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();
    let child_id = uuid(0x7010);
    let parent_id = uuid(0x7011);

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "child_trigger".into(),
                    table: "child_rows".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "child_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(40)],
            rows: vec![
                RowChange {
                    table: "child_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(child_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(child_id)),
                        ("parent_id".to_string(), Value::Uuid(parent_id)),
                        ("content".to_string(), Value::Text("bad-order".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(20),
                },
                RowChange {
                    table: "parent_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(parent_id),
                    },
                    values: HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
                    deleted: false,
                    lsn: Lsn(30),
                },
            ],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_id".to_string()]
        ),
        "a later sender-LSN parent must not validate an earlier child row; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "chronological FK preflight rejection must not leave a trigger declaration behind"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "chronological FK preflight rejection must leave no durable side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_fk_preflight_rejects_parent_delete_before_trigger_leak() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent_rows (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), content TEXT)",
        &empty(),
    )
    .unwrap();
    let parent_id = uuid(0x7012);
    let child_id = uuid(0x7013);
    db.execute(
        "INSERT INTO parent_rows (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO child_rows (id, parent_id, content) VALUES ($id, $parent_id, $content)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(child_id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            (
                "content".to_string(),
                Value::Text("still-live-child".into()),
            ),
        ]),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "child_trigger".into(),
                    table: "child_rows".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "child_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(30)],
            rows: vec![RowChange {
                table: "parent_rows".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(parent_id),
                },
                values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                deleted: true,
                lsn: Lsn(20),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_id".to_string()]
        ),
        "FK-invalid parent delete must fail during preflight before trigger DDL is applied; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "reverse-FK preflight rejection must not leave a trigger declaration behind"
    );
    assert_eq!(
        count_rows(&db, "parent_rows"),
        1,
        "rejected parent delete must not mutate committed parent rows"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "reverse-FK preflight rejection must leave no durable side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_fk_preflight_rejects_stale_parent_prefix_after_update() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parent_rows (id UUID PRIMARY KEY, ref_id UUID UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_ref UUID REFERENCES parent_rows(ref_id), content TEXT)",
        &empty(),
    )
    .unwrap();
    let parent_id = uuid(0x7014);
    let old_ref = uuid(0x7015);
    let new_ref = uuid(0x7016);
    let child_id = uuid(0x7017);
    db.execute(
        "INSERT INTO parent_rows (id, ref_id) VALUES ($id, $ref_id)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(parent_id)),
            ("ref_id".to_string(), Value::Uuid(old_ref)),
        ]),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "child_trigger".into(),
                    table: "child_rows".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "child_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(40)],
            rows: vec![
                RowChange {
                    table: "parent_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(parent_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(parent_id)),
                        ("ref_id".to_string(), Value::Uuid(new_ref)),
                    ]),
                    deleted: false,
                    lsn: Lsn(20),
                },
                RowChange {
                    table: "child_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(child_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(child_id)),
                        ("parent_ref".to_string(), Value::Uuid(old_ref)),
                        (
                            "content".to_string(),
                            Value::Text("stale-parent-ref".into()),
                        ),
                    ]),
                    deleted: false,
                    lsn: Lsn(30),
                },
            ],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_ref".to_string()]
        ),
        "FK preflight must treat parent updates as replacing the old referenced value; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "stale-prefix FK rejection must not leave a trigger declaration behind"
    );
    assert_eq!(
        count_rows(&db, "child_rows"),
        0,
        "stale-prefix rejection must not insert the invalid child row"
    );
    let parent = row_for_column_uuid(&db, "parent_rows", "id", parent_id)
        .expect("parent row must remain at its committed pre-sync value");
    assert!(
        matches!(parent.values.get("ref_id"), Some(Value::Uuid(value)) if *value == old_ref),
        "stale-prefix rejection must not update the committed parent row; got {parent:?}"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "stale-prefix FK preflight rejection must leave no durable side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_fk_preflight_accepts_child_update_before_parent_delete() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent_rows (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), content TEXT)",
        &empty(),
    )
    .unwrap();
    let parent_id = uuid(0x7018);
    let child_id = uuid(0x7019);
    db.execute(
        "INSERT INTO parent_rows (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO child_rows (id, parent_id, content) VALUES ($id, $parent_id, $content)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(child_id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            ("content".to_string(), Value::Text("attached".into())),
        ]),
    )
    .unwrap();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "child_trigger".into(),
                    table: "child_rows".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "child_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(40)],
            rows: vec![
                RowChange {
                    table: "child_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(child_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(child_id)),
                        ("parent_id".to_string(), Value::Null),
                        ("content".to_string(), Value::Text("detached".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(20),
                },
                RowChange {
                    table: "parent_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(parent_id),
                    },
                    values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                    deleted: true,
                    lsn: Lsn(30),
                },
            ],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        result.is_ok(),
        "reverse-FK preflight must honor earlier child updates that remove a reference; got {result:?}"
    );
    assert!(
        db.list_triggers().is_empty(),
        "create/drop trigger sync should leave no live trigger after successful apply"
    );
    assert_eq!(
        count_rows(&db, "parent_rows"),
        0,
        "parent delete should commit after the child no longer references it"
    );
    let child = row_for_column_uuid(&db, "child_rows", "id", child_id)
        .expect("child row should survive as detached");
    assert!(
        matches!(child.values.get("parent_id"), Some(Value::Null)),
        "child update should leave a durable NULL parent reference; got {child:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_fk_preflight_rejects_skipped_child_update_before_parent_delete() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent_rows (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child_rows (id UUID PRIMARY KEY, parent_id UUID REFERENCES parent_rows(id), content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER child_trigger ON child_rows WHEN INSERT",
        &empty(),
    )
    .unwrap();
    db.register_trigger_callback("child_trigger", |_, _| Ok(()))
        .unwrap();
    db.complete_initialization().unwrap();
    let parent_id = uuid(0x701A);
    let child_id = uuid(0x701B);
    db.execute(
        "INSERT INTO parent_rows (id) VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO child_rows (id, parent_id, content) VALUES ($id, $parent_id, $content)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(child_id)),
            ("parent_id".to_string(), Value::Uuid(parent_id)),
            (
                "content".to_string(),
                Value::Text("server-owned-child".into()),
            ),
        ]),
    )
    .unwrap();
    let since = db.current_lsn();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "child_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(10)],
            rows: vec![
                RowChange {
                    table: "child_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(child_id),
                    },
                    values: HashMap::from([
                        ("id".to_string(), Value::Uuid(child_id)),
                        ("parent_id".to_string(), Value::Null),
                        ("content".to_string(), Value::Text("skipped-detach".into())),
                    ]),
                    deleted: false,
                    lsn: Lsn(20),
                },
                RowChange {
                    table: "parent_rows".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(parent_id),
                    },
                    values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                    deleted: true,
                    lsn: Lsn(30),
                },
            ],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    assert!(
        matches!(
            result,
            Err(Error::ForeignKeyViolation {
                ref child_table,
                ref child_columns,
                ..
            }) if child_table == "child_rows" && child_columns == &vec!["parent_id".to_string()]
        ),
        "reverse-FK preflight must not trust child updates that sync policy will skip; got {result:?}"
    );
    assert!(
        db.list_triggers()
            .iter()
            .any(|trigger| trigger.name == "child_trigger"),
        "skipped-update FK rejection must not leak the earlier trigger drop"
    );
    let child = row_for_column_uuid(&db, "child_rows", "id", child_id)
        .expect("committed child must remain attached");
    assert!(
        matches!(child.values.get("parent_id"), Some(Value::Uuid(value)) if *value == parent_id),
        "skipped child update must not detach the committed child; got {child:?}"
    );
    let leaked_changes = db.changes_since(since);
    assert!(
        leaked_changes.ddl.is_empty()
            && leaked_changes.rows.is_empty()
            && leaked_changes.edges.is_empty()
            && leaked_changes.vectors.is_empty(),
        "skipped-update reverse-FK preflight rejection must leave no durable sync side effects; got {leaked_changes:?}"
    );
}

#[test]
fn t3_sync_trigger_table_create_drop_history_replays_chronologically() {
    let db = Database::open_memory();

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "replay_writes".into(),
                    columns: vec![
                        ("id".into(), "UUID PRIMARY KEY".into()),
                        ("content".into(), "TEXT".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTrigger {
                    name: "replay_trigger".into(),
                    table: "replay_writes".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTable {
                    name: "replay_writes".into(),
                },
                DdlChange::DropTrigger {
                    name: "replay_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(20), Lsn(30), Lsn(30)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    assert!(
        result.is_ok()
            && db.table_meta("replay_writes").is_none()
            && db.list_triggers().is_empty()
            && db.complete_initialization().is_ok(),
        "sync must replay create-table/create-trigger/drop-table trigger history chronologically; result={result:?}, tables={:?}, triggers={:?}",
        db.table_names(),
        db.list_triggers()
    );
    let ddl = db.changes_since(Lsn(0)).ddl;
    assert!(
        ddl.iter()
            .any(|change| matches!(change, DdlChange::CreateTrigger { name, table, .. } if name == "replay_trigger" && table == "replay_writes"))
            && ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "replay_trigger")
            )
            && ddl
                .iter()
                .any(|change| matches!(change, DdlChange::DropTable { name } if name == "replay_writes")),
        "chronological trigger table replay must preserve downstream-visible trigger/table tombstones; ddl={ddl:?}"
    );

    let same_lsn_since = db.current_lsn();
    let same_lsn_result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "same_lsn_replay_writes".into(),
                    columns: vec![
                        ("id".into(), "UUID PRIMARY KEY".into()),
                        ("content".into(), "TEXT".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTrigger {
                    name: "same_lsn_replay_trigger".into(),
                    table: "same_lsn_replay_writes".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTable {
                    name: "same_lsn_replay_writes".into(),
                },
            ],
            ddl_lsn: vec![Lsn(40), Lsn(40), Lsn(40)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let same_lsn_ddl = db.changes_since(same_lsn_since).ddl;
    let same_lsn_create_trigger_pos = same_lsn_ddl
        .iter()
        .position(|change| matches!(change, DdlChange::CreateTrigger { name, table, .. } if name == "same_lsn_replay_trigger" && table == "same_lsn_replay_writes"));
    let same_lsn_drop_table_pos = same_lsn_ddl
        .iter()
        .position(|change| matches!(change, DdlChange::DropTable { name } if name == "same_lsn_replay_writes"));
    let same_lsn_drop_trigger_pos = same_lsn_ddl
        .iter()
        .position(|change| matches!(change, DdlChange::DropTrigger { name } if name == "same_lsn_replay_trigger"));
    assert!(
        same_lsn_result.is_ok()
            && db.table_meta("same_lsn_replay_writes").is_none()
            && db
                .list_triggers()
                .iter()
                .all(|trigger| trigger.name != "same_lsn_replay_trigger")
            && same_lsn_create_trigger_pos.is_some()
            && same_lsn_drop_table_pos.is_some()
            && same_lsn_drop_trigger_pos.is_some()
            && same_lsn_create_trigger_pos < same_lsn_drop_table_pos,
        "same-sender-LSN CreateTrigger followed by DropTable must replay atomically and emit the implicit trigger tombstone; result={same_lsn_result:?}, ddl={same_lsn_ddl:?}, triggers={:?}",
        db.list_triggers()
    );

    db.execute(
        "CREATE TABLE same_lsn_marker (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    let mixed_lsn_since = db.current_lsn();
    let mixed_marker_id = uuid(0x7020);
    let mixed_lsn_result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "mixed_lsn_replay_writes".into(),
                    columns: vec![
                        ("id".into(), "UUID PRIMARY KEY".into()),
                        ("content".into(), "TEXT".into()),
                    ],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTrigger {
                    name: "mixed_lsn_replay_trigger".into(),
                    table: "mixed_lsn_replay_writes".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTable {
                    name: "mixed_lsn_replay_writes".into(),
                },
            ],
            ddl_lsn: vec![Lsn(50), Lsn(50), Lsn(50)],
            rows: vec![row_change(
                "same_lsn_marker",
                mixed_marker_id,
                "forces-mixed-branch",
                Lsn(50),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let mixed_lsn_marker_row = row_for_column_uuid(&db, "same_lsn_marker", "id", mixed_marker_id);
    let mixed_lsn_changes = db.changes_since(mixed_lsn_since);
    let mixed_lsn_create_trigger_pos = mixed_lsn_changes
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::CreateTrigger { name, table, .. } if name == "mixed_lsn_replay_trigger" && table == "mixed_lsn_replay_writes"));
    let mixed_lsn_drop_table_pos = mixed_lsn_changes
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::DropTable { name } if name == "mixed_lsn_replay_writes"));
    let mixed_lsn_drop_trigger_pos = mixed_lsn_changes
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::DropTrigger { name } if name == "mixed_lsn_replay_trigger"));
    let mixed_lsn_create_trigger_lsn =
        mixed_lsn_create_trigger_pos.and_then(|pos| mixed_lsn_changes.ddl_lsn.get(pos).copied());
    let mixed_lsn_drop_table_lsn =
        mixed_lsn_drop_table_pos.and_then(|pos| mixed_lsn_changes.ddl_lsn.get(pos).copied());
    let mixed_lsn_drop_trigger_lsn =
        mixed_lsn_drop_trigger_pos.and_then(|pos| mixed_lsn_changes.ddl_lsn.get(pos).copied());
    assert!(
        mixed_lsn_result.is_ok()
            && mixed_lsn_marker_row.is_some()
            && db.table_meta("mixed_lsn_replay_writes").is_none()
            && db
                .list_triggers()
                .iter()
                .all(|trigger| trigger.name != "mixed_lsn_replay_trigger")
            && mixed_lsn_create_trigger_pos.is_some()
            && mixed_lsn_drop_table_pos.is_some()
            && mixed_lsn_drop_trigger_pos.is_some()
            && mixed_lsn_create_trigger_pos < mixed_lsn_drop_table_pos
            && mixed_lsn_create_trigger_lsn == mixed_lsn_drop_table_lsn
            && mixed_lsn_create_trigger_lsn == mixed_lsn_drop_trigger_lsn,
        "same-sender-LSN CreateTrigger followed by DropTable must fold queued trigger DDL into the durable table-drop transaction even when the group also has data; result={mixed_lsn_result:?}, marker={mixed_lsn_marker_row:?}, ddl={:?}, ddl_lsn={:?}, triggers={:?}",
        mixed_lsn_changes.ddl,
        mixed_lsn_changes.ddl_lsn,
        db.list_triggers()
    );
}

#[test]
fn t3_sync_trigger_vector_create_data_drop_history_replays_without_callbacks() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_vectors (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let since = db.current_lsn();
    let vector_id = uuid(0x7005);

    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "vector_history_trigger".into(),
                    table: "host_vectors".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "vector_history_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(10), Lsn(30)],
            rows: vec![RowChange {
                table: "host_vectors".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(vector_id),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(vector_id)),
                    ("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0])),
                ]),
                deleted: false,
                lsn: Lsn(20),
            }],
            vectors: vec![VectorChange {
                index: VectorIndexRef::new("host_vectors", "embedding"),
                row_id: RowId(7005),
                vector: vec![1.0, 0.0, 0.0],
                lsn: Lsn(20),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );

    let row = row_for_column_uuid(&db, "host_vectors", "id", vector_id);
    let ddl = db.changes_since(since).ddl;
    let fired = fired_trigger_history(&db, "vector_history_trigger");
    assert!(
        result.is_ok()
            && row.is_some()
            && live_vector_count(&db, "host_vectors") == 1
            && db.list_triggers().is_empty()
            && db.complete_initialization().is_ok()
            && fired.len() == 1
            && ddl.iter().any(
                |change| matches!(change, DdlChange::CreateTrigger { name, table, .. } if name == "vector_history_trigger" && table == "host_vectors")
            )
            && ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "vector_history_trigger")
            ),
        "sync must replay CreateTrigger -> vector data -> DropTrigger history without host callbacks or partial cold-gate leakage; result={result:?}, row={row:?}, live_vectors={}, triggers={:?}, fired={fired:?}, ddl={ddl:?}",
        live_vector_count(&db, "host_vectors"),
        db.list_triggers()
    );

    let same_lsn_id = uuid(0x7006);
    let same_lsn_since = db.current_lsn();
    let same_lsn_result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "same_lsn_vector_history_trigger".into(),
                    table: "host_vectors".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::DropTrigger {
                    name: "same_lsn_vector_history_trigger".into(),
                },
            ],
            ddl_lsn: vec![Lsn(40), Lsn(40)],
            rows: vec![RowChange {
                table: "host_vectors".into(),
                natural_key: NaturalKey {
                    column: "id".into(),
                    value: Value::Uuid(same_lsn_id),
                },
                values: HashMap::from([
                    ("id".to_string(), Value::Uuid(same_lsn_id)),
                    ("embedding".to_string(), Value::Vector(vec![0.0, 1.0, 0.0])),
                ]),
                deleted: false,
                lsn: Lsn(40),
            }],
            vectors: vec![VectorChange {
                index: VectorIndexRef::new("host_vectors", "embedding"),
                row_id: RowId(7006),
                vector: vec![0.0, 1.0, 0.0],
                lsn: Lsn(40),
            }],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let same_lsn_row = row_for_column_uuid(&db, "host_vectors", "id", same_lsn_id);
    let same_lsn_ddl = db.changes_since(same_lsn_since).ddl;
    let same_lsn_fired = fired_trigger_history(&db, "same_lsn_vector_history_trigger");
    assert!(
        same_lsn_result.is_ok()
            && same_lsn_row.is_some()
            && db
                .list_triggers()
                .iter()
                .all(|trigger| trigger.name != "same_lsn_vector_history_trigger")
            && same_lsn_fired.len() == 1
            && same_lsn_ddl.iter().any(
                |change| matches!(change, DdlChange::CreateTrigger { name, table, .. } if name == "same_lsn_vector_history_trigger" && table == "host_vectors")
            )
            && same_lsn_ddl.iter().any(
                |change| matches!(change, DdlChange::DropTrigger { name } if name == "same_lsn_vector_history_trigger")
            ),
        "same-sender-LSN CreateTrigger -> data -> DropTrigger replay must preserve committed data, tombstone final state, and fired audit without callbacks; result={same_lsn_result:?}, row={same_lsn_row:?}, fired={same_lsn_fired:?}, ddl={same_lsn_ddl:?}, triggers={:?}",
        db.list_triggers()
    );
}

#[test]
fn t3_reg_introspection_and_delete_event_rejection() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE update_audits (id UUID PRIMARY KEY, write_id UUID UNIQUE, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER host_update_trigger ON host_writes WHEN UPDATE",
        &empty(),
    )
    .unwrap();

    let mut failures = Vec::new();
    let race_tmp = TempDir::new().unwrap();
    let race_path = race_tmp.path().join("concurrent_trigger_ddl.redb");
    let race_db = Arc::new(Database::open(&race_path).unwrap());
    race_db
        .execute(
            "CREATE TABLE race_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    let race_barrier = Arc::new(Barrier::new(3));
    let mut race_joins = Vec::new();
    for (name, event) in [
        ("race_insert_trigger".to_string(), "INSERT".to_string()),
        ("race_update_trigger".to_string(), "UPDATE".to_string()),
    ] {
        let db = race_db.clone();
        let barrier = race_barrier.clone();
        race_joins.push(std::thread::spawn(move || {
            barrier.wait();
            db.execute(
                &format!("CREATE TRIGGER {name} ON race_writes WHEN {event}"),
                &HashMap::new(),
            )
        }));
    }
    race_barrier.wait();
    let race_results = race_joins
        .into_iter()
        .map(|join| join.join().expect("concurrent trigger DDL thread panicked"))
        .collect::<Vec<_>>();
    let race_trigger_names = race_db
        .list_triggers()
        .into_iter()
        .map(|trigger| trigger.name)
        .collect::<BTreeSet<_>>();
    let race_ddl_names = race_db
        .changes_since(Lsn(0))
        .ddl
        .into_iter()
        .filter_map(|change| match change {
            DdlChange::CreateTrigger { name, .. } => Some(name),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    race_db.close().unwrap();
    let reopened_race_db = Database::open(&race_path).unwrap();
    let reopened_race_trigger_names = reopened_race_db
        .list_triggers()
        .into_iter()
        .map(|trigger| trigger.name)
        .collect::<BTreeSet<_>>();
    let expected_race_triggers = BTreeSet::from([
        "race_insert_trigger".to_string(),
        "race_update_trigger".to_string(),
    ]);
    if race_results.iter().any(|result| result.is_err())
        || race_trigger_names != expected_race_triggers
        || race_ddl_names != expected_race_triggers
        || reopened_race_trigger_names != expected_race_triggers
    {
        failures.push(format!(
            "concurrent CREATE TRIGGER commits must merge durable declaration state and DDL history without losing either trigger; results={race_results:?}, triggers={race_trigger_names:?}, ddl={race_ddl_names:?}, reopened={reopened_race_trigger_names:?}"
        ));
    }
    reopened_race_db.close().unwrap();

    let unknown = db.register_trigger_callback("missing_trigger", |_, _| Ok(()));
    if !matches!(
        unknown,
        Err(Error::TriggerNotDeclared { ref trigger_name }) if trigger_name == "missing_trigger"
    ) {
        failures.push(format!(
            "registering an undeclared trigger must fail with typed error; got {unknown:?}"
        ));
    }

    let delete_trigger = db.execute(
        "CREATE TRIGGER delete_trigger ON host_writes WHEN DELETE",
        &empty(),
    );
    if !matches!(
        delete_trigger,
        Err(Error::TriggerEventUnsupported { ref event }) if event == "DELETE"
    ) {
        failures.push(format!(
            "DELETE trigger must be rejected with a typed error; got {delete_trigger:?}"
        ));
    }

    let trigger_map = db
        .list_triggers()
        .into_iter()
        .map(|trigger| (trigger.name.clone(), trigger))
        .collect::<BTreeMap<_, _>>();
    if trigger_map.len() != 2
        || !matches!(
            trigger_map.get("host_write_trigger"),
            Some(trigger)
                if trigger.table == "host_writes"
                    && trigger.on_events == vec![TriggerEvent::Insert]
        )
        || !matches!(
            trigger_map.get("host_update_trigger"),
            Some(trigger)
                if trigger.table == "host_writes"
                    && trigger.on_events == vec![TriggerEvent::Update]
        )
    {
        failures.push(format!(
            "list_triggers must expose durable INSERT and UPDATE declarations; got {trigger_map:?}"
        ));
    }

    let insert_fires = Arc::new(AtomicUsize::new(0));
    let captured_insert_fires = insert_fires.clone();
    let insert_contexts = Arc::new(Mutex::new(Vec::new()));
    let captured_insert_contexts = insert_contexts.clone();
    db.register_trigger_callback("host_write_trigger", move |_, ctx| {
        if ctx.trigger_name != "host_write_trigger"
            || ctx.table != "host_writes"
            || ctx.event != TriggerEvent::Insert
        {
            return Err(Error::Other(format!(
                "unexpected INSERT trigger context identity: {ctx:?}"
            )));
        }
        captured_insert_contexts.lock().unwrap().push((
            ctx.event,
            ctx.row_values.get("id").cloned(),
            ctx.row_values.get("content").cloned(),
        ));
        captured_insert_fires.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("first callback registration should bind declared trigger");
    let update_contexts = Arc::new(Mutex::new(Vec::new()));
    let captured_updates = update_contexts.clone();
    db.register_trigger_callback("host_update_trigger", move |db_handle, ctx| {
        let write_id = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("UPDATE trigger row missing UUID id".into()))?;
        let content = ctx
            .row_values
            .get("content")
            .and_then(Value::as_text)
            .ok_or_else(|| Error::Other("UPDATE trigger row missing post-image content".into()))?
            .to_string();
        captured_updates.lock().unwrap().push((
            ctx.trigger_name.clone(),
            ctx.table.clone(),
            ctx.event,
            ctx.row_values.get("id").cloned(),
            ctx.row_values.get("content").cloned(),
        ));
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("write_id".into(), Value::Uuid(write_id));
        p.insert("content".into(), Value::Text(content));
        db_handle.execute(
            "INSERT INTO update_audits (id, write_id, content) VALUES ($id, $write_id, $content)",
            &p,
        )?;
        Ok(())
    })
    .expect("UPDATE callback registration should bind declared trigger");
    let duplicate = db.register_trigger_callback("host_write_trigger", |_, _| Ok(()));
    if !matches!(
        duplicate,
        Err(Error::TriggerAlreadyRegistered { ref trigger_name }) if trigger_name == "host_write_trigger"
    ) {
        failures.push(format!(
            "duplicate registration must fail with typed error; got {duplicate:?}"
        ));
    }

    let registered = db
        .registered_trigger_callbacks()
        .into_iter()
        .collect::<BTreeSet<_>>();
    if registered
        != BTreeSet::from([
            "host_write_trigger".to_string(),
            "host_update_trigger".to_string(),
        ])
    {
        failures.push(format!(
            "registered callbacks must be introspectable; got {registered:?}"
        ));
    }

    let complete = db.complete_initialization();
    if complete.is_err() {
        failures.push(format!(
            "initialization should succeed once INSERT and UPDATE callbacks are registered; got {complete:?}"
        ));
    }
    let mut p = insert_host_params(uuid(0x310), "before-update");
    let insert = db.execute(host_insert_sql(), &p);
    if insert.is_err() {
        failures.push(format!(
            "setup INSERT for UPDATE trigger failed: {insert:?}"
        ));
    }
    if insert_fires.load(Ordering::SeqCst) != 1 {
        failures.push(format!(
            "WHEN INSERT callback must fire once with valid context identity; got {}",
            insert_fires.load(Ordering::SeqCst)
        ));
    }
    if !update_contexts.lock().unwrap().is_empty() {
        failures.push(format!(
            "WHEN UPDATE callback must not fire for INSERT; got {:?}",
            update_contexts.lock().unwrap()
        ));
    }
    if insert_contexts.lock().unwrap().last()
        != Some(&(
            TriggerEvent::Insert,
            Some(Value::Uuid(uuid(0x310))),
            Some(Value::Text("before-update".into())),
        ))
    {
        failures.push(format!(
            "WHEN INSERT callback must see post-image row values; got {:?}",
            insert_contexts.lock().unwrap()
        ));
    }
    p.insert("content".into(), Value::Text("after-update".into()));
    let update_since = db.current_lsn();
    let update_rx = db.subscribe();
    let update = db.execute(
        "UPDATE host_writes SET content = $content WHERE id = $id",
        &p,
    );
    if update.is_err() {
        failures.push(format!("UPDATE trigger setup update failed: {update:?}"));
    }
    let update_contexts_snapshot = update_contexts.lock().unwrap().clone();
    if update_contexts_snapshot
        != vec![(
            "host_update_trigger".to_string(),
            "host_writes".to_string(),
            TriggerEvent::Update,
            Some(Value::Uuid(uuid(0x310))),
            Some(Value::Text("after-update".into())),
        )]
    {
        failures.push(format!(
            "WHEN UPDATE callback must fire once with trigger/table identity and post-image row values; got {update_contexts_snapshot:?}"
        ));
    }
    if update.is_ok() {
        let update_changes = db.changes_since(update_since);
        let update_host_lsns = update_changes
            .rows
            .iter()
            .filter(|row| row.table == "host_writes")
            .map(|row| row.lsn)
            .collect::<HashSet<_>>();
        let update_audit_lsns = update_changes
            .rows
            .iter()
            .filter(|row| row.table == "update_audits")
            .map(|row| row.lsn)
            .collect::<HashSet<_>>();
        let update_event = update_rx.recv_timeout(Duration::from_secs(1));
        let update_audit_row = row_for_column_uuid(&db, "update_audits", "write_id", uuid(0x310));
        let update_host_row = row_for_column_uuid(&db, "host_writes", "id", uuid(0x310));
        if update_host_lsns.len() != 1
            || update_host_lsns != update_audit_lsns
            || !matches!(
                &update_event,
                Ok(event)
                    if update_host_lsns.contains(&event.lsn)
                        && event.row_count == 2
                        && event.tables_changed.contains(&"host_writes".to_string())
                        && event.tables_changed.contains(&"update_audits".to_string())
            )
            || !matches!(
                &update_audit_row,
                Some(row)
                    if row.values.get("content")
                        == Some(&Value::Text("after-update".into()))
                        && Some(row.created_tx)
                            == update_host_row.as_ref().map(|host| host.created_tx)
            )
        {
            failures.push(format!(
                "WHEN UPDATE callback must write cascade data inside the same firing tx; host_lsns={update_host_lsns:?}, audit_lsns={update_audit_lsns:?}, event={update_event:?}, host_row={update_host_row:?}, audit_row={update_audit_row:?}, changes={update_changes:?}"
            ));
        }
    }

    let insert_then_update_insert_count_before = insert_fires.load(Ordering::SeqCst);
    let insert_then_update_context_count_before = insert_contexts.lock().unwrap().len();
    let insert_then_update_update_count_before = update_contexts.lock().unwrap().len();
    let insert_then_update_begin = db.execute("BEGIN", &empty());
    let mut insert_then_update_params = insert_host_params(uuid(0x313), "insert-then-update-draft");
    let insert_then_update_insert = db.execute(host_insert_sql(), &insert_then_update_params);
    insert_then_update_params.insert(
        "content".into(),
        Value::Text("insert-then-update-final".into()),
    );
    let insert_then_update_update = db.execute(
        "UPDATE host_writes SET content = $content WHERE id = $id",
        &insert_then_update_params,
    );
    let insert_then_update_commit = db.execute("COMMIT", &empty());
    let insert_then_update_contexts = insert_contexts.lock().unwrap().clone();
    let insert_then_update_updates = update_contexts.lock().unwrap().clone();
    if insert_then_update_begin.is_err()
        || insert_then_update_insert.is_err()
        || insert_then_update_update.is_err()
        || insert_then_update_commit.is_err()
        || insert_fires.load(Ordering::SeqCst) != insert_then_update_insert_count_before + 1
        || insert_then_update_contexts.len() != insert_then_update_context_count_before + 1
        || insert_then_update_updates.len() != insert_then_update_update_count_before
        || !matches!(
            insert_then_update_contexts.last(),
            Some((TriggerEvent::Insert, Some(Value::Uuid(id)), Some(Value::Text(content))))
                if *id == uuid(0x313) && content == "insert-then-update-final"
        )
    {
        failures.push(format!(
            "insert then update of a new row in one tx must fire the INSERT trigger once on the final post-image and must not misroute to UPDATE; begin={insert_then_update_begin:?}, insert={insert_then_update_insert:?}, update={insert_then_update_update:?}, commit={insert_then_update_commit:?}, insert_fires_before={insert_then_update_insert_count_before}, insert_fires_after={}, insert_contexts_before={insert_then_update_context_count_before}, insert_contexts={insert_then_update_contexts:?}, update_count_before={insert_then_update_update_count_before}, updates={insert_then_update_updates:?}",
            insert_fires.load(Ordering::SeqCst)
        ));
    }

    let drop_trigger = db.execute("DROP TRIGGER host_write_trigger", &empty());
    let triggers_after_insert_drop = db.list_triggers();
    let callbacks_after_insert_drop = db.registered_trigger_callbacks();
    let update_count_before_partial_drop = update_contexts.lock().unwrap().len();
    let audit_count_before_partial_drop = count_rows(&db, "update_audits");
    let insert_count_before_partial_drop = insert_fires.load(Ordering::SeqCst);
    let mut partial_drop_params = insert_host_params(uuid(0x311), "partial-drop");
    let partial_drop_insert = db.execute(host_insert_sql(), &partial_drop_params);
    partial_drop_params.insert("content".into(), Value::Text("partial-drop-update".into()));
    let partial_drop_update = db.execute(
        "UPDATE host_writes SET content = $content WHERE id = $id",
        &partial_drop_params,
    );
    let update_contexts_after_partial_drop = update_contexts.lock().unwrap().clone();
    if drop_trigger.is_err()
        || triggers_after_insert_drop.len() != 1
        || !triggers_after_insert_drop.iter().any(|trigger| {
            trigger.name == "host_update_trigger"
                && trigger.table == "host_writes"
                && trigger.on_events == vec![TriggerEvent::Update]
        })
        || callbacks_after_insert_drop != vec!["host_update_trigger".to_string()]
        || partial_drop_insert.is_err()
        || partial_drop_update.is_err()
        || insert_fires.load(Ordering::SeqCst) != insert_count_before_partial_drop
        || count_rows(&db, "update_audits") != audit_count_before_partial_drop + 1
        || update_contexts_after_partial_drop.len() != update_count_before_partial_drop + 1
        || !matches!(
            update_contexts_after_partial_drop.last(),
            Some((trigger_name, table, TriggerEvent::Update, Some(Value::Uuid(id)), Some(Value::Text(content))))
                if trigger_name == "host_update_trigger"
                    && table == "host_writes"
                    && *id == uuid(0x311)
                    && content == "partial-drop-update"
        )
    {
        failures.push(format!(
            "dropping one same-table trigger must not clear sibling trigger metadata or dispatch; drop={drop_trigger:?}, triggers={triggers_after_insert_drop:?}, callbacks={callbacks_after_insert_drop:?}, insert={partial_drop_insert:?}, update={partial_drop_update:?}, insert_fires_before={insert_count_before_partial_drop}, insert_fires_after={}, audit_rows_before={audit_count_before_partial_drop}, audit_rows_after={}, updates={update_contexts_after_partial_drop:?}",
            insert_fires.load(Ordering::SeqCst),
            count_rows(&db, "update_audits")
        ));
    }

    let drop_update = db.execute("DROP TRIGGER host_update_trigger", &empty());
    if drop_update.is_err() || !db.list_triggers().is_empty() {
        failures.push(format!(
            "DROP TRIGGER must remove durable declaration metadata one declaration at a time; drop_update={drop_update:?}, triggers={:?}",
            db.list_triggers()
        ));
    }
    let after_drop_registered = db.registered_trigger_callbacks();
    if !after_drop_registered.is_empty() {
        failures.push(format!(
            "DROP TRIGGER must detach registered callbacks; got {after_drop_registered:?}"
        ));
    }
    let register_dropped = db.register_trigger_callback("host_write_trigger", |_, _| Ok(()));
    if !matches!(
        register_dropped,
        Err(Error::TriggerNotDeclared { ref trigger_name }) if trigger_name == "host_write_trigger"
    ) {
        failures.push(format!(
            "registering a dropped trigger must fail with typed error; got {register_dropped:?}"
        ));
    }
    let insert_count_after_drop = insert_fires.load(Ordering::SeqCst);
    let update_contexts_after_drop = update_contexts.lock().unwrap().clone();
    let mut after_drop_insert_params = insert_host_params(uuid(0x312), "after-drop");
    let after_drop_insert = db.execute(host_insert_sql(), &after_drop_insert_params);
    after_drop_insert_params.insert("content".into(), Value::Text("after-drop-update".into()));
    let after_drop_update = db.execute(
        "UPDATE host_writes SET content = $content WHERE id = $id",
        &after_drop_insert_params,
    );
    let update_contexts_after_dispatch_check = update_contexts.lock().unwrap().clone();
    if after_drop_insert.is_err()
        || after_drop_update.is_err()
        || count_rows(&db, "update_audits") != audit_count_before_partial_drop + 1
        || insert_fires.load(Ordering::SeqCst) != insert_count_after_drop
        || update_contexts_after_dispatch_check != update_contexts_after_drop
    {
        failures.push(format!(
            "DROP TRIGGER must remove active dispatch entries as well as metadata; insert={after_drop_insert:?}, update={after_drop_update:?}, update_audits={}, insert_fires_before={insert_count_after_drop}, insert_fires_after={}, updates_before={update_contexts_after_drop:?}, updates_after={:?}",
            count_rows(&db, "update_audits"),
            insert_fires.load(Ordering::SeqCst),
            update_contexts_after_dispatch_check
        ));
    }

    let sync_tmp = TempDir::new().unwrap();
    let sync_path = sync_tmp.path().join("sync_applied_trigger_ddl.redb");
    let sync_admin = Database::open(&sync_path).unwrap();
    sync_admin
        .execute(
            "CREATE TABLE sync_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    let sync_since = sync_admin.current_lsn();
    let sync_delete = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "sync_delete_trigger".into(),
                table: "sync_writes".into(),
                on_events: vec!["DELETE".into()],
            }],
            ddl_lsn: vec![Lsn(31)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_mixed_delete = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "sync_mixed_delete_trigger".into(),
                table: "sync_writes".into(),
                on_events: vec!["INSERT".into(), "DELETE".into()],
            }],
            ddl_lsn: vec![Lsn(32)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_partial_batch = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTrigger {
                    name: "sync_partial_valid_trigger".into(),
                    table: "sync_writes".into(),
                    on_events: vec!["INSERT".into()],
                },
                DdlChange::CreateTrigger {
                    name: "sync_partial_delete_trigger".into(),
                    table: "sync_writes".into(),
                    on_events: vec!["DELETE".into()],
                },
            ],
            ddl_lsn: vec![Lsn(33), Lsn(33)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_mixed_bad_row = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "sync_bad_row_trigger".into(),
                table: "sync_writes".into(),
                on_events: vec!["INSERT".into()],
            }],
            rows: vec![row_change(
                "missing_sync_rows",
                uuid(0x325),
                "must-not-persist-trigger",
                Lsn(41),
            )],
            ddl_lsn: vec![Lsn(41)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_create = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTrigger {
                name: "sync_trigger".into(),
                table: "sync_writes".into(),
                on_events: vec!["INSERT".into(), "UPDATE".into()],
            }],
            ddl_lsn: vec![Lsn(34)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_contexts = Arc::new(Mutex::new(Vec::new()));
    let sync_contexts_for_callback = sync_contexts.clone();
    let sync_register = sync_admin.register_trigger_callback("sync_trigger", move |_, ctx| {
        if ctx.trigger_name != "sync_trigger"
            || ctx.table != "sync_writes"
            || !matches!(ctx.event, TriggerEvent::Insert | TriggerEvent::Update)
            || !matches!(
                ctx.row_values.get("content"),
                Some(Value::Text(content))
                    if content == "before-drop" || content == "before-drop-update"
            )
        {
            return Err(Error::Other(format!(
                "unexpected sync trigger context: {ctx:?}"
            )));
        }
        sync_contexts
            .lock()
            .unwrap()
            .push((ctx.event, ctx.row_values.get("content").cloned()));
        Ok(())
    });
    let sync_ready = sync_admin.complete_initialization();
    let sync_triggers_after_create = sync_admin.list_triggers();
    let sync_pull_callbacks_before = sync_contexts_for_callback.lock().unwrap().clone();
    let sync_pull_audits_before = sync_admin
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("sync_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let sync_pull_insert = sync_admin.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "sync_writes",
                uuid(0x322),
                "remote-insert",
                Lsn(50),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_pull_update = sync_admin.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "sync_writes",
                uuid(0x322),
                "remote-update",
                Lsn(51),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_pull_stale = sync_admin.apply_changes(
        ChangeSet {
            rows: vec![row_change(
                "sync_writes",
                uuid(0x322),
                "stale-remote",
                Lsn(1),
            )],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_pull_callbacks_after = sync_contexts_for_callback.lock().unwrap().clone();
    let sync_pull_remote_row = row_for_column_uuid(&sync_admin, "sync_writes", "id", uuid(0x322));
    let sync_pull_audits_after = sync_admin
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("sync_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let sync_pull_new_audits = sync_pull_audits_after
        .iter()
        .skip(sync_pull_audits_before.len())
        .cloned()
        .collect::<Vec<_>>();
    let sync_insert_before_drop = sync_admin.execute(
        "INSERT INTO sync_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000320', 'before-drop')",
        &empty_params(),
    );
    let sync_update_before_drop = sync_admin.execute(
        "UPDATE sync_writes SET content = 'before-drop-update' WHERE id = '00000000-0000-0000-0000-000000000320'",
        &empty_params(),
    );
    let sync_drop = sync_admin.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTrigger {
                name: "sync_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(35)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    let sync_insert_after_drop = sync_admin.execute(
        "INSERT INTO sync_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000321', 'after-drop')",
        &empty_params(),
    );
    let sync_update_after_drop = sync_admin.execute(
        "UPDATE sync_writes SET content = 'after-drop-update' WHERE id = '00000000-0000-0000-0000-000000000321'",
        &empty_params(),
    );
    let sync_contexts_snapshot = sync_contexts_for_callback.lock().unwrap().clone();
    let sync_ddl_history = sync_admin.changes_since(sync_since).ddl;
    let sync_fired_history = sync_admin
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("sync_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    if sync_create.is_err()
        || sync_register.is_err()
        || sync_ready.is_err()
        || !matches!(
            &sync_partial_batch,
            Err(Error::TriggerEventUnsupported { event }) if event == "DELETE"
        )
        || !matches!(
            &sync_mixed_bad_row,
            Err(Error::TableNotFound(table)) if table == "missing_sync_rows"
        )
        || !matches!(
            sync_triggers_after_create.as_slice(),
            [trigger]
                if trigger.name == "sync_trigger"
                    && trigger.table == "sync_writes"
                    && trigger.on_events == vec![TriggerEvent::Insert, TriggerEvent::Update]
        )
        || sync_pull_insert.is_err()
        || sync_pull_update.is_err()
        || sync_pull_stale.is_err()
        || sync_pull_callbacks_after != sync_pull_callbacks_before
        || sync_pull_new_audits.len() != 2
        || !sync_pull_new_audits.iter().all(|entry| {
            entry.trigger_name == "sync_trigger"
                && entry.status == TriggerAuditStatus::Fired
                && entry.depth == 1
                && entry.cascade_row_count == 0
                && entry.firing_lsn != Lsn(0)
                && entry.firing_tx != TxId(0)
        })
        || !matches!(
            sync_pull_remote_row,
            Some(ref row)
                if row.values.get("content") == Some(&Value::Text("remote-update".into()))
        )
        || sync_insert_before_drop.is_err()
        || sync_update_before_drop.is_err()
        || sync_contexts_snapshot
            != vec![
                (
                    TriggerEvent::Insert,
                    Some(Value::Text("before-drop".into())),
                ),
                (
                    TriggerEvent::Update,
                    Some(Value::Text("before-drop-update".into())),
                ),
            ]
        || sync_drop.is_err()
        || sync_insert_after_drop.is_err()
        || sync_update_after_drop.is_err()
        || *sync_contexts_for_callback.lock().unwrap() != sync_contexts_snapshot
        || !matches!(
            sync_ddl_history.as_slice(),
            [
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                },
                DdlChange::DropTrigger { name: drop_name },
            ] if name == "sync_trigger"
                && table == "sync_writes"
                && on_events == &vec!["INSERT".to_string(), "UPDATE".to_string()]
                && drop_name == "sync_trigger"
        )
        || sync_admin
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "sync_trigger")
        || sync_admin
            .registered_trigger_callbacks()
            .contains(&"sync_trigger".to_string())
        || sync_fired_history.len() != 4
    {
        failures.push(format!(
            "admin apply_changes must preserve INSERT/UPDATE trigger events, reject unsupported DELETE and invalid mixed DDL/data batches atomically, emit durable create/drop DDL, suppress SyncPull callbacks while auditing committed receiver writes, and detach dispatch; delete={sync_delete:?}, mixed_delete={sync_mixed_delete:?}, partial_batch={sync_partial_batch:?}, mixed_bad_row={sync_mixed_bad_row:?}, create={sync_create:?}, register={sync_register:?}, ready={sync_ready:?}, triggers_after_create={sync_triggers_after_create:?}, sync_pull_insert={sync_pull_insert:?}, sync_pull_update={sync_pull_update:?}, sync_pull_stale={sync_pull_stale:?}, sync_pull_callbacks_before={sync_pull_callbacks_before:?}, sync_pull_callbacks_after={sync_pull_callbacks_after:?}, sync_pull_row={sync_pull_remote_row:?}, sync_pull_new_audits={sync_pull_new_audits:?}, fired_history={sync_fired_history:?}, before={sync_insert_before_drop:?}, update_before={sync_update_before_drop:?}, drop={sync_drop:?}, after={sync_insert_after_drop:?}, update_after={sync_update_after_drop:?}, contexts={sync_contexts_snapshot:?}, ddl_history={sync_ddl_history:?}, triggers={:?}, callbacks={:?}",
            sync_admin.list_triggers(),
            sync_admin.registered_trigger_callbacks()
        ));
    }
    if !matches!(
        sync_delete,
        Err(Error::TriggerEventUnsupported { ref event }) if event == "DELETE"
    ) {
        failures.push(format!(
            "sync-applied CREATE TRIGGER with DELETE must be rejected with a typed event error; got {sync_delete:?}"
        ));
    }
    if !matches!(
        sync_mixed_delete,
        Err(Error::TriggerEventUnsupported { ref event }) if event == "DELETE"
    ) {
        failures.push(format!(
            "sync-applied CREATE TRIGGER with mixed INSERT+DELETE must reject the whole declaration with a typed event error; got {sync_mixed_delete:?}"
        ));
    }
    if !matches!(
        &sync_partial_batch,
        Err(Error::TriggerEventUnsupported { event }) if event == "DELETE"
    ) || sync_admin
        .changes_since(sync_since)
        .ddl
        .iter()
        .any(|change| {
            matches!(
                change,
                DdlChange::CreateTrigger { name, .. }
                    if name == "sync_partial_valid_trigger"
                        || name == "sync_partial_delete_trigger"
                        || name == "sync_bad_row_trigger"
            )
        })
        || sync_admin.list_triggers().iter().any(|trigger| {
            trigger.name == "sync_partial_valid_trigger"
                || trigger.name == "sync_partial_delete_trigger"
                || trigger.name == "sync_bad_row_trigger"
        })
    {
        failures.push(format!(
            "sync-applied trigger DDL batch must validate all trigger declarations and mixed DDL/data row schemas before persisting any trigger declaration; partial_batch={sync_partial_batch:?}, mixed_bad_row={sync_mixed_bad_row:?}, ddl={:?}, triggers={:?}",
            sync_admin.changes_since(sync_since).ddl,
            sync_admin.list_triggers()
        ));
    }
    sync_admin.close().unwrap();
    let reopened_sync_admin = Database::open(&sync_path).unwrap();
    let reopened_sync_ready = reopened_sync_admin.complete_initialization();
    let reopened_sync_ddl = reopened_sync_admin.changes_since(sync_since).ddl;
    let reopened_sync_create_triggers = reopened_sync_ddl
        .iter()
        .filter(|change| {
            matches!(
                change,
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                } if name == "sync_trigger"
                    && table == "sync_writes"
                    && on_events == &vec!["INSERT".to_string(), "UPDATE".to_string()]
            )
        })
        .count();
    let reopened_sync_drop_triggers = reopened_sync_ddl
        .iter()
        .filter(
            |change| matches!(change, DdlChange::DropTrigger { name } if name == "sync_trigger"),
        )
        .count();
    let reopened_sync_delete_triggers = reopened_sync_ddl
        .iter()
        .filter(|change| {
            matches!(
                change,
                DdlChange::CreateTrigger { name, .. }
                    if name == "sync_delete_trigger"
                        || name == "sync_mixed_delete_trigger"
                        || name == "sync_partial_valid_trigger"
                        || name == "sync_partial_delete_trigger"
                        || name == "sync_bad_row_trigger"
            )
        })
        .count();
    let reopened_sync_fired_history = reopened_sync_admin
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("sync_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    if reopened_sync_ready.is_err()
        || !reopened_sync_admin.list_triggers().is_empty()
        || reopened_sync_create_triggers != 1
        || reopened_sync_drop_triggers != 1
        || reopened_sync_delete_triggers != 0
        || reopened_sync_fired_history.len() != 4
    {
        failures.push(format!(
            "sync-applied trigger DDL create/drop, fired trigger audit history, and unsupported DELETE rejection must survive durable reopen; ready={reopened_sync_ready:?}, triggers={:?}, ddl={reopened_sync_ddl:?}, fired={reopened_sync_fired_history:?}",
            reopened_sync_admin.list_triggers()
        ));
    }
    reopened_sync_admin.close().unwrap();

    let durable_tmp = TempDir::new().unwrap();
    let durable_path = durable_tmp.path().join("durable_drop.redb");
    let durable_drop_history;
    {
        let durable = Database::open(&durable_path).unwrap();
        let since = durable.current_lsn();
        durable
            .execute(
                "CREATE TABLE durable_writes (id UUID PRIMARY KEY, content TEXT)",
                &empty(),
            )
            .unwrap();
        let create = durable.execute(
            "CREATE TRIGGER durable_trigger ON durable_writes WHEN INSERT",
            &empty(),
        );
        let listed_after_create = durable.list_triggers();
        durable
            .register_trigger_callback("durable_trigger", |_, _| Ok(()))
            .unwrap();
        durable.complete_initialization().unwrap();
        let drop = durable.execute("DROP TRIGGER durable_trigger", &empty());
        durable_drop_history = durable.changes_since(since).ddl;
        if create.is_err()
            || drop.is_err()
            || !matches!(
                durable_drop_history.as_slice(),
                [
                    DdlChange::CreateTable { name, .. },
                    DdlChange::CreateTrigger {
                        name: trigger_name,
                        table,
                        on_events,
                    },
                    DdlChange::DropTrigger { name: drop_name },
                ] if name == "durable_writes"
                    && trigger_name == "durable_trigger"
                    && table == "durable_writes"
                    && on_events == &vec!["INSERT".to_string()]
                    && drop_name == "durable_trigger"
            )
            || listed_after_create
                .iter()
                .all(|trigger| trigger.name != "durable_trigger")
            || durable
                .list_triggers()
                .iter()
                .any(|trigger| trigger.name == "durable_trigger")
        {
            failures.push(format!(
                "DROP TRIGGER must persist and emit a durable DDL tombstone; create={create:?}, drop={drop:?}, listed_after_create={listed_after_create:?}, final_triggers={:?}, ddl_history={durable_drop_history:?}",
                durable.list_triggers()
            ));
        }
        durable.close().unwrap();
    }
    let reopened_durable = Database::open(&durable_path).unwrap();
    let reopened_ready = reopened_durable.complete_initialization();
    if reopened_ready.is_err()
        || reopened_durable
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "durable_trigger")
        || reopened_durable
            .changes_since(Lsn(0))
            .ddl
            .iter()
            .filter(|change| matches!(change, DdlChange::DropTrigger { name } if name == "durable_trigger"))
            .count()
            != 1
    {
        failures.push(format!(
            "reopen must not resurrect a dropped durable trigger and must retain the drop tombstone; ready={reopened_ready:?}, triggers={:?}, ddl={:?}, original_drop_history={durable_drop_history:?}",
            reopened_durable.list_triggers(),
            reopened_durable.changes_since(Lsn(0)).ddl
        ));
    }
    reopened_durable.close().unwrap();

    let inflight = Database::open_memory();
    inflight
        .execute(
            "CREATE TABLE inflight_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    inflight
        .execute(
            "CREATE TABLE inflight_log (id UUID PRIMARY KEY, note TEXT)",
            &empty(),
        )
        .unwrap();
    inflight
        .execute(
            "CREATE TRIGGER inflight_trigger ON inflight_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
    let inflight_drop_attempts = Arc::new(Mutex::new(Vec::new()));
    let captured_drop_attempts = inflight_drop_attempts.clone();
    inflight
        .register_trigger_callback("inflight_trigger", move |db_handle, _ctx| {
            match db_handle.execute("DROP TRIGGER inflight_trigger", &empty()) {
                Err(Error::TriggerRequiresAdmin { operation })
                    if operation.contains("DROP TRIGGER") && operation.contains("callback") =>
                {
                    captured_drop_attempts.lock().unwrap().push(operation);
                }
                other => {
                    return Err(Error::Other(format!(
                        "in-flight DROP TRIGGER must be blocked with a typed callback/admin error; got {other:?}"
                    )));
                }
            }

            db_handle.execute(
                "INSERT INTO inflight_log (id, note) VALUES ('00000000-0000-0000-0000-000000000340', 'survived')",
                &empty(),
            )?;
            Ok(())
        })
        .unwrap();
    inflight.complete_initialization().unwrap();
    let inflight_insert = inflight.execute(
        "INSERT INTO inflight_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000341', 'active')",
        &empty(),
    );
    let inflight_drop_attempts_snapshot = inflight_drop_attempts.lock().unwrap().clone();
    if inflight_insert.is_err()
        || inflight_drop_attempts_snapshot.len() != 1
        || count_rows(&inflight, "inflight_log") != 1
        || !inflight
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "inflight_trigger")
        || !inflight
            .registered_trigger_callbacks()
            .contains(&"inflight_trigger".to_string())
    {
        failures.push(format!(
            "DROP TRIGGER attempted from an in-flight cascade must fail typed without deadlock, preserve the active trigger, and let the firing tx commit; insert={inflight_insert:?}, attempts={inflight_drop_attempts_snapshot:?}, log_rows={}, triggers={:?}, callbacks={:?}",
            count_rows(&inflight, "inflight_log"),
            inflight.list_triggers(),
            inflight.registered_trigger_callbacks()
        ));
    }

    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scoped_trigger_ddl.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE scoped_writes (id UUID PRIMARY KEY, context_id UUID)",
                &empty(),
            )
            .unwrap();
        admin.close().unwrap();
    }
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(uuid(0xA))])).unwrap();
    let scoped_create = scoped.execute(
        "CREATE TRIGGER scoped_trigger ON scoped_writes WHEN INSERT",
        &empty(),
    );
    if !matches!(
        scoped_create,
        Err(Error::TriggerRequiresAdmin { ref operation })
            if operation.contains("CREATE TRIGGER")
    ) {
        failures.push(format!(
            "context-scoped handles must not create engine-wide trigger DDL and must return a typed admin error; got {scoped_create:?}"
        ));
    }
    let scoped_drop = scoped.execute("DROP TRIGGER scoped_trigger", &empty());
    if !matches!(
        scoped_drop,
        Err(Error::TriggerRequiresAdmin { ref operation })
            if operation.contains("DROP TRIGGER")
    ) {
        failures.push(format!(
            "context-scoped handles must not drop engine-wide trigger DDL and must return a typed admin error; got {scoped_drop:?}"
        ));
    }
    let scoped_apply = scoped.apply_changes(
        ChangeSet {
            ddl: vec![contextdb_engine::sync_types::DdlChange::CreateTrigger {
                name: "sync_trigger".into(),
                table: "scoped_writes".into(),
                on_events: vec!["INSERT".into()],
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    if !matches!(
        scoped_apply,
        Err(Error::TriggerRequiresAdmin { ref operation })
            if operation.contains("apply_changes") && operation.contains("CREATE TRIGGER")
    ) {
        failures.push(format!(
            "context-scoped handles must not apply trigger DDL from sync and must return a typed admin error; got {scoped_apply:?}"
        ));
    }
    let scoped_apply_drop = scoped.apply_changes(
        ChangeSet {
            ddl: vec![contextdb_engine::sync_types::DdlChange::DropTrigger {
                name: "sync_trigger".into(),
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    if !matches!(
        scoped_apply_drop,
        Err(Error::TriggerRequiresAdmin { ref operation })
            if operation.contains("apply_changes") && operation.contains("DROP TRIGGER")
    ) {
        failures.push(format!(
            "context-scoped handles must not apply DROP TRIGGER DDL from sync and must return a typed admin error; got {scoped_apply_drop:?}"
        ));
    }
    scoped.close().unwrap();

    let scoped_fire_tmp = TempDir::new().unwrap();
    let scoped_fire_path = scoped_fire_tmp.path().join("scoped_trigger_fire.redb");
    let ctx_a = uuid(0xA);
    let ctx_b = uuid(0xB);
    let allowed_write_id = uuid(0xA10);
    let rel_denied_write_id = uuid(0xA11);
    let graph_denied_write_id = uuid(0xA12);
    let vector_denied_write_id = uuid(0xA13);
    let allowed_audit_id = uuid(0xA20);
    let graph_denied_audit_id = uuid(0xA21);
    let vector_denied_audit_id = uuid(0xA22);
    let hidden_node_id = uuid(0xB10);
    let hidden_audit_id = uuid(0xB20);
    let hidden_audit_row_id;
    {
        let admin = Database::open(&scoped_fire_path).unwrap();
        admin
            .execute(
                "CREATE TABLE scoped_writes (id UUID PRIMARY KEY, content TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE scoped_audits (id UUID PRIMARY KEY, write_id UUID, note TEXT, embedding VECTOR(3), context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE scoped_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE scoped_nodes (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TRIGGER scoped_fire_trigger ON scoped_writes WHEN INSERT",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO scoped_nodes (id, label, context_id) VALUES ($id, 'hidden', $ctx)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(hidden_node_id)),
                    ("ctx".to_string(), Value::Uuid(ctx_b)),
                ]),
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO scoped_audits (id, write_id, note, embedding, context_id) VALUES ($id, $write_id, 'hidden', $embedding, $ctx)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(hidden_audit_id)),
                    ("write_id".to_string(), Value::Uuid(uuid(0xB21))),
                    ("embedding".to_string(), Value::Vector(vec![0.0, 1.0, 0.0])),
                    ("ctx".to_string(), Value::Uuid(ctx_b)),
                ]),
            )
            .unwrap();
        hidden_audit_row_id =
            row_id_for_column_uuid(&admin, "scoped_audits", "id", hidden_audit_id)
                .expect("hidden ctx-b vector row must exist");
        admin.close().unwrap();
    }
    let scoped_fire =
        Database::open_with_contexts(&scoped_fire_path, BTreeSet::from([ContextId::new(ctx_a)]))
            .unwrap();
    let scoped_callback_attempts = Arc::new(Mutex::new(Vec::new()));
    let scoped_callback_attempts_for_callback = scoped_callback_attempts.clone();
    let scoped_register =
        scoped_fire.register_trigger_callback("scoped_fire_trigger", move |db_handle, ctx| {
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("scoped trigger row missing UUID id".into()))?;
            let content = ctx
                .row_values
                .get("content")
                .and_then(Value::as_text)
                .ok_or_else(|| Error::Other("scoped trigger row missing content".into()))?;
            let (audit_id, note) = match content {
                "allowed" => (allowed_audit_id, "allowed"),
                "graph-denied" => (graph_denied_audit_id, "graph-denied"),
                "vector-denied" => (vector_denied_audit_id, "vector-denied"),
                "rel-denied" => {
                    scoped_callback_attempts_for_callback
                        .lock()
                        .unwrap()
                        .push("rel-denied".to_string());
                    return db_handle.execute(
                        "INSERT INTO scoped_audits (id, write_id, note, embedding, context_id) VALUES ($id, $write_id, 'rel-denied', $embedding, $ctx)",
                        &HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid(0xA23))),
                            ("write_id".to_string(), Value::Uuid(write_id)),
                            ("embedding".to_string(), Value::Vector(vec![0.2, 0.3, 0.4])),
                            ("ctx".to_string(), Value::Uuid(ctx_b)),
                        ]),
                    ).map(|_| ());
                }
                other => return Err(Error::Other(format!("unexpected scoped branch {other}"))),
            };
            db_handle.execute(
                "INSERT INTO scoped_audits (id, write_id, note, embedding, context_id) VALUES ($id, $write_id, $note, $embedding, $ctx)",
                &HashMap::from([
                    ("id".to_string(), Value::Uuid(audit_id)),
                    ("write_id".to_string(), Value::Uuid(write_id)),
                    ("note".to_string(), Value::Text(note.to_string())),
                    ("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0])),
                    ("ctx".to_string(), Value::Uuid(ctx_a)),
                ]),
            )?;
            match content {
                "allowed" => {
                    db_handle.execute(
                        "INSERT INTO scoped_edges (id, source_id, target_id, edge_type, context_id) VALUES ($id, $source, $target, 'SCOPED_AUDITED_BY', $ctx)",
                        &HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid(0xA30))),
                            ("source".to_string(), Value::Uuid(write_id)),
                            ("target".to_string(), Value::Uuid(audit_id)),
                            ("ctx".to_string(), Value::Uuid(ctx_a)),
                        ]),
                    )?;
                    db_handle.insert_edge(
                        ctx.tx,
                        write_id,
                        audit_id,
                        "SCOPED_AUDITED_BY".into(),
                        HashMap::from([("scope".to_string(), Value::Text("ctx-a".into()))]),
                    )?;
                }
                "graph-denied" => {
                    scoped_callback_attempts_for_callback
                        .lock()
                        .unwrap()
                        .push("graph-denied".to_string());
                    db_handle.insert_edge(
                        ctx.tx,
                        write_id,
                        hidden_node_id,
                        "SCOPED_DENIED".into(),
                        HashMap::new(),
                    )?;
                }
                "vector-denied" => {
                    scoped_callback_attempts_for_callback
                        .lock()
                        .unwrap()
                        .push("vector-denied".to_string());
                    db_handle.insert_vector(
                        ctx.tx,
                        VectorIndexRef::new("scoped_audits", "embedding"),
                        hidden_audit_row_id,
                        vec![0.5, 0.5, 0.0],
                    )?;
                }
                _ => {}
            }
            Ok(())
        });
    let scoped_ready = scoped_fire.complete_initialization();
    let scoped_insert = |id: Uuid, content: &str| {
        scoped_fire.execute(
            "INSERT INTO scoped_writes (id, content, context_id) VALUES ($id, $content, $ctx)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("content".to_string(), Value::Text(content.to_string())),
                ("ctx".to_string(), Value::Uuid(ctx_a)),
            ]),
        )
    };
    let scoped_allowed = scoped_insert(allowed_write_id, "allowed");
    let scoped_rel_denied = scoped_insert(rel_denied_write_id, "rel-denied");
    let scoped_graph_denied = scoped_insert(graph_denied_write_id, "graph-denied");
    let scoped_vector_denied = scoped_insert(vector_denied_write_id, "vector-denied");
    let allowed_audit_row =
        row_for_column_uuid(&scoped_fire, "scoped_audits", "write_id", allowed_write_id);
    let allowed_edge_count = scoped_fire
        .edge_count(
            allowed_write_id,
            "SCOPED_AUDITED_BY",
            scoped_fire.snapshot(),
        )
        .unwrap();
    let scoped_allowed_vector = allowed_audit_row
        .as_ref()
        .and_then(|row| scoped_fire.live_vector_entry(row.row_id, scoped_fire.snapshot()));
    let expected_scope = BTreeSet::from([ContextId::new(ctx_a)]);
    let scoped_denied_ok = |result: &Result<contextdb_engine::QueryResult>| {
        matches!(
            result,
            Err(Error::ContextScopeViolation { requested, allowed })
                if requested == &ContextId::new(ctx_b) && allowed == &expected_scope
        )
    };
    if scoped_register.is_err()
        || scoped_ready.is_err()
        || scoped_allowed.is_err()
        || !scoped_denied_ok(&scoped_rel_denied)
        || !scoped_denied_ok(&scoped_graph_denied)
        || !scoped_denied_ok(&scoped_vector_denied)
        || count_rows(&scoped_fire, "scoped_writes") != 1
        || count_rows(&scoped_fire, "scoped_audits") != 1
        || count_rows(&scoped_fire, "scoped_edges") != 1
        || allowed_edge_count != 1
        || scoped_allowed_vector.is_none()
        || row_for_column_uuid(
            &scoped_fire,
            "scoped_audits",
            "write_id",
            graph_denied_write_id,
        )
        .is_some()
        || row_for_column_uuid(
            &scoped_fire,
            "scoped_audits",
            "write_id",
            vector_denied_write_id,
        )
        .is_some()
        || scoped_callback_attempts.lock().unwrap().as_slice()
            != ["rel-denied", "graph-denied", "vector-denied"]
    {
        failures.push(format!(
            "scoped firing writes must run callbacks through the caller's constrained handle, not an admin bypass; register={scoped_register:?}, ready={scoped_ready:?}, allowed={scoped_allowed:?}, rel={scoped_rel_denied:?}, graph={scoped_graph_denied:?}, vector={scoped_vector_denied:?}, visible_counts=({}, {}, {}), edge_count={allowed_edge_count}, vector={scoped_allowed_vector:?}, attempts={:?}",
            count_rows(&scoped_fire, "scoped_writes"),
            count_rows(&scoped_fire, "scoped_audits"),
            count_rows(&scoped_fire, "scoped_edges"),
            scoped_callback_attempts.lock().unwrap()
        ));
    }
    scoped_fire.close().unwrap();
    let scoped_fire_admin = Database::open(&scoped_fire_path).unwrap();
    if count_rows(&scoped_fire_admin, "scoped_writes") != 1
        || count_rows(&scoped_fire_admin, "scoped_audits") != 2
        || count_rows(&scoped_fire_admin, "scoped_edges") != 1
        || scoped_fire_admin
            .edge_count(
                graph_denied_write_id,
                "SCOPED_DENIED",
                scoped_fire_admin.snapshot(),
            )
            .unwrap()
            != 0
        || row_for_column_uuid(
            &scoped_fire_admin,
            "scoped_audits",
            "write_id",
            rel_denied_write_id,
        )
        .is_some()
    {
        failures.push(format!(
            "scoped callback denials must roll back firing and prior callback side effects durably; admin_counts=({}, {}, {}), denied_edge_count={}, denied_rel_row={:?}",
            count_rows(&scoped_fire_admin, "scoped_writes"),
            count_rows(&scoped_fire_admin, "scoped_audits"),
            count_rows(&scoped_fire_admin, "scoped_edges"),
            scoped_fire_admin
                .edge_count(
                    graph_denied_write_id,
                    "SCOPED_DENIED",
                    scoped_fire_admin.snapshot()
                )
                .unwrap(),
            row_for_column_uuid(
                &scoped_fire_admin,
                "scoped_audits",
                "write_id",
                rel_denied_write_id
            )
        ));
    }
    scoped_fire_admin.close().unwrap();

    let scope_tmp = TempDir::new().unwrap();
    let scope_path = scope_tmp.path().join("scope_trigger_fire.redb");
    {
        let admin = Database::open(&scope_path).unwrap();
        admin
            .execute(
                "CREATE TABLE scope_writes (id UUID PRIMARY KEY, content TEXT, scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'))",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE scope_audits (id UUID PRIMARY KEY, write_id UUID, note TEXT, scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'))",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TRIGGER scope_fire_trigger ON scope_writes WHEN INSERT",
                &empty(),
            )
            .unwrap();
        admin.close().unwrap();
    }
    let scope_handle =
        Database::open_with_scope_labels(&scope_path, BTreeSet::from([ScopeLabel::new("edge")]))
            .unwrap();
    let scope_register = scope_handle.register_trigger_callback(
        "scope_fire_trigger",
        move |db_handle, ctx| {
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("scope trigger row missing UUID id".into()))?;
            let content = ctx
                .row_values
                .get("content")
                .and_then(Value::as_text)
                .ok_or_else(|| Error::Other("scope trigger row missing content".into()))?;
            let scope = if content == "scope-allowed" {
                "edge"
            } else {
                "server"
            };
            db_handle
                .execute(
                    "INSERT INTO scope_audits (id, write_id, note, scope_label) VALUES ($id, $write_id, $note, $scope)",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("write_id".to_string(), Value::Uuid(write_id)),
                        ("note".to_string(), Value::Text(content.to_string())),
                        ("scope".to_string(), Value::Text(scope.to_string())),
                    ]),
                )
                .map(|_| ())
        },
    );
    let scope_ready = scope_handle.complete_initialization();
    let scope_insert = |id: Uuid, content: &str| {
        scope_handle.execute(
            "INSERT INTO scope_writes (id, content, scope_label) VALUES ($id, $content, 'edge')",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("content".to_string(), Value::Text(content.to_string())),
            ]),
        )
    };
    let scope_allowed = scope_insert(uuid(0xC10), "scope-allowed");
    let scope_denied = scope_insert(uuid(0xC11), "scope-denied");
    if scope_register.is_err()
        || scope_ready.is_err()
        || scope_allowed.is_err()
        || !matches!(
            scope_denied,
            Err(Error::ScopeLabelViolation { ref requested, ref allowed })
                if requested == &ScopeLabel::new("server")
                    && allowed == &BTreeSet::from([ScopeLabel::new("edge")])
        )
        || count_rows(&scope_handle, "scope_writes") != 1
        || count_rows(&scope_handle, "scope_audits") != 1
    {
        failures.push(format!(
            "scope-labelled firing writes must run callbacks through the caller's scope gate; register={scope_register:?}, ready={scope_ready:?}, allowed={scope_allowed:?}, denied={scope_denied:?}, visible_counts=({}, {})",
            count_rows(&scope_handle, "scope_writes"),
            count_rows(&scope_handle, "scope_audits")
        ));
    }
    scope_handle.close().unwrap();
    let scope_admin = Database::open(&scope_path).unwrap();
    if count_rows(&scope_admin, "scope_writes") != 1
        || count_rows(&scope_admin, "scope_audits") != 1
        || row_for_column_uuid(&scope_admin, "scope_writes", "id", uuid(0xC11)).is_some()
    {
        failures.push(format!(
            "scope-labelled callback denial must durably roll back the firing row; admin_counts=({}, {}), denied_row={:?}",
            count_rows(&scope_admin, "scope_writes"),
            count_rows(&scope_admin, "scope_audits"),
            row_for_column_uuid(&scope_admin, "scope_writes", "id", uuid(0xC11))
        ));
    }
    scope_admin.close().unwrap();

    let acl_tmp = TempDir::new().unwrap();
    let acl_path = acl_tmp.path().join("acl_trigger_fire.redb");
    let acl_a = uuid(0xD10);
    let acl_b = uuid(0xD11);
    {
        let admin = Database::open(&acl_path).unwrap();
        admin
            .execute(
                "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE acl_writes (id UUID PRIMARY KEY, content TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE acl_audits (id UUID PRIMARY KEY, write_id UUID, note TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TRIGGER acl_fire_trigger ON acl_writes WHEN INSERT",
                &empty(),
            )
            .unwrap();
        for (grant_id, principal, acl) in [(uuid(0xD20), "a1", acl_a), (uuid(0xD21), "a2", acl_b)] {
            admin
                .execute(
                    "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(grant_id)),
                        ("principal".to_string(), Value::Text(principal.to_string())),
                        ("acl".to_string(), Value::Uuid(acl)),
                    ]),
                )
                .unwrap();
        }
        admin.close().unwrap();
    }
    let acl_handle = Database::open_as_principal(&acl_path, Principal::Agent("a1".into())).unwrap();
    let acl_register = acl_handle.register_trigger_callback(
        "acl_fire_trigger",
        move |db_handle, ctx| {
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("ACL trigger row missing UUID id".into()))?;
            let content = ctx
                .row_values
                .get("content")
                .and_then(Value::as_text)
                .ok_or_else(|| Error::Other("ACL trigger row missing content".into()))?;
            let acl = if content == "acl-allowed" {
                acl_a
            } else {
                acl_b
            };
            db_handle
                .execute(
                    "INSERT INTO acl_audits (id, write_id, note, acl_id) VALUES ($id, $write_id, $note, $acl)",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                        ("write_id".to_string(), Value::Uuid(write_id)),
                        ("note".to_string(), Value::Text(content.to_string())),
                        ("acl".to_string(), Value::Uuid(acl)),
                    ]),
                )
                .map(|_| ())
        },
    );
    let acl_ready = acl_handle.complete_initialization();
    let acl_insert = |id: Uuid, content: &str| {
        acl_handle.execute(
            "INSERT INTO acl_writes (id, content, acl_id) VALUES ($id, $content, $acl)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("content".to_string(), Value::Text(content.to_string())),
                ("acl".to_string(), Value::Uuid(acl_a)),
            ]),
        )
    };
    let acl_allowed = acl_insert(uuid(0xD30), "acl-allowed");
    let acl_denied = acl_insert(uuid(0xD31), "acl-denied");
    if acl_register.is_err()
        || acl_ready.is_err()
        || acl_allowed.is_err()
        || !matches!(
            acl_denied,
            Err(Error::AclDenied { ref table, ref principal, .. })
                if table == "acl_audits"
                    && principal == &Principal::Agent("a1".into())
        )
        || count_rows(&acl_handle, "acl_writes") != 1
        || count_rows(&acl_handle, "acl_audits") != 1
    {
        failures.push(format!(
            "principal-scoped firing writes must run callbacks through the caller's ACL gate; register={acl_register:?}, ready={acl_ready:?}, allowed={acl_allowed:?}, denied={acl_denied:?}, visible_counts=({}, {})",
            count_rows(&acl_handle, "acl_writes"),
            count_rows(&acl_handle, "acl_audits")
        ));
    }
    acl_handle.close().unwrap();
    let acl_admin = Database::open(&acl_path).unwrap();
    if count_rows(&acl_admin, "acl_writes") != 1
        || count_rows(&acl_admin, "acl_audits") != 1
        || row_for_column_uuid(&acl_admin, "acl_writes", "id", uuid(0xD31)).is_some()
    {
        failures.push(format!(
            "principal-scoped callback denial must durably roll back the firing row; admin_counts=({}, {}), denied_row={:?}",
            count_rows(&acl_admin, "acl_writes"),
            count_rows(&acl_admin, "acl_audits"),
            row_for_column_uuid(&acl_admin, "acl_writes", "id", uuid(0xD31))
        ));
    }
    acl_admin.close().unwrap();

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn t3_callback_writes_are_pinned_to_supplied_tx_bound_handle() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE guard_writes (id UUID PRIMARY KEY, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE guard_audits (id UUID PRIMARY KEY, write_id UUID UNIQUE, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER guard_trigger ON guard_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();

    let other_db = Arc::new(Database::open_memory());
    other_db
        .execute(
            "CREATE TABLE escape_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    let wrong_tx = db.begin();
    let other_wrong_tx = other_db.begin();
    other_db
        .insert_row(
            other_wrong_tx,
            "escape_writes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xE0A))),
                (
                    "content".to_string(),
                    Value::Text("pre-staged-escape".into()),
                ),
            ]),
        )
        .unwrap();
    let (third_owner_db, third_owner_wrong_tx) = std::thread::spawn(|| {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE third_owner_escape (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        let tx = db.begin();
        db.insert_row(
            tx,
            "third_owner_escape",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xE0B))),
                (
                    "content".to_string(),
                    Value::Text("pre-staged-third-owner-escape".into()),
                ),
            ]),
        )
        .unwrap();
        (db, tx)
    })
    .join()
    .unwrap();
    let third_owner_db = Arc::new(third_owner_db);
    let attempts = Arc::new(Mutex::new(Vec::<(String, bool, String)>::new()));
    let callback_attempts = attempts.clone();
    let callback_other_db = other_db.clone();
    let callback_third_owner_db = third_owner_db.clone();
    db.register_trigger_callback("guard_trigger", move |db_handle, ctx| {
        let wrong_direct = db_handle.insert_row(
            wrong_tx,
            "guard_audits",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xE01))),
                (
                    "write_id".to_string(),
                    ctx.row_values
                        .get("id")
                        .cloned()
                        .unwrap_or(Value::Uuid(uuid(0xE00))),
                ),
                ("note".to_string(), Value::Text("wrong-tx".into())),
            ]),
        );
        callback_attempts.lock().unwrap().push((
            "same_handle_wrong_tx".to_string(),
            wrong_direct.is_err(),
            format!("{wrong_direct:?}"),
        ));
        let wrong_execute_in_tx = db_handle.execute_in_tx(
            wrong_tx,
            "INSERT INTO guard_audits (id, write_id, note) VALUES ($id, $write_id, 'wrong-execute-in-tx')",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xE05))),
                (
                    "write_id".to_string(),
                    ctx.row_values
                        .get("id")
                        .cloned()
                        .unwrap_or(Value::Uuid(uuid(0xE06))),
                ),
            ]),
        );
        callback_attempts.lock().unwrap().push((
            "same_handle_execute_in_tx_wrong_tx".to_string(),
            wrong_execute_in_tx.is_err(),
            format!("{wrong_execute_in_tx:?}"),
        ));

        let other_sql = callback_other_db.execute(
            "INSERT INTO escape_writes (id, content) VALUES ($id, 'escape')",
            &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE02)))]),
        );
        callback_attempts.lock().unwrap().push((
            "other_handle_sql".to_string(),
            other_sql.is_err(),
            format!("{other_sql:?}"),
        ));

        let other_apply_changes = callback_other_db.apply_changes(
            ChangeSet {
                rows: vec![row_change(
                    "escape_writes",
                    uuid(0xE09),
                    "sync-escape",
                    Lsn(1),
                )],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        );
        callback_attempts.lock().unwrap().push((
            "other_handle_apply_changes".to_string(),
            other_apply_changes.is_err(),
            format!("{other_apply_changes:?}"),
        ));

        let other_commit = callback_other_db.commit(other_wrong_tx);
        callback_attempts.lock().unwrap().push((
            "other_handle_commit_staged_tx".to_string(),
            other_commit.is_err(),
            format!("{other_commit:?}"),
        ));

        let write_id = ctx
            .row_values
            .get("id")
            .cloned()
            .unwrap_or(Value::Uuid(uuid(0xE06)));
        let cross_thread_direct = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    db_handle.insert_row(
                        ctx.tx,
                        "guard_audits",
                        HashMap::from([
                            ("id".to_string(), Value::Uuid(uuid(0xE07))),
                            ("write_id".to_string(), write_id),
                            ("note".to_string(), Value::Text("cross-thread".into())),
                        ]),
                    )
                })
                .join()
                .unwrap()
        });
        callback_attempts.lock().unwrap().push((
            "same_handle_cross_thread_ctx_tx".to_string(),
            cross_thread_direct.is_err(),
            format!("{cross_thread_direct:?}"),
        ));

        let other_for_thread = callback_other_db.clone();
        let cross_thread_other_handle = std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    other_for_thread.execute(
                        "INSERT INTO escape_writes (id, content) VALUES ($id, 'thread-escape')",
                        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE08)))]),
                    )
                })
                .join()
                .unwrap()
        });
        callback_attempts.lock().unwrap().push((
            "other_handle_cross_thread_sql".to_string(),
            cross_thread_other_handle.is_err(),
            format!("{cross_thread_other_handle:?}"),
        ));

        let third_owner_for_thread = callback_third_owner_db.clone();
        let third_owner_cross_thread_sql = std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    third_owner_for_thread.execute(
                        "INSERT INTO third_owner_escape (id, content) VALUES ($id, 'thread-escape')",
                        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE0C)))]),
                    )
                })
                .join()
                .unwrap()
        });
        callback_attempts.lock().unwrap().push((
            "third_owner_handle_cross_thread_sql".to_string(),
            third_owner_cross_thread_sql.is_err(),
            format!("{third_owner_cross_thread_sql:?}"),
        ));

        let third_owner_for_commit = callback_third_owner_db.clone();
        let third_owner_cross_thread_commit = std::thread::scope(|scope| {
            scope
                .spawn(move || third_owner_for_commit.commit(third_owner_wrong_tx))
                .join()
                .unwrap()
        });
        callback_attempts.lock().unwrap().push((
            "third_owner_handle_cross_thread_commit".to_string(),
            third_owner_cross_thread_commit.is_err(),
            format!("{third_owner_cross_thread_commit:?}"),
        ));

        db_handle.insert_row(
            ctx.tx,
            "guard_audits",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(0xE03))),
                (
                    "write_id".to_string(),
                    ctx.row_values
                        .get("id")
                        .cloned()
                        .unwrap_or(Value::Uuid(uuid(0xE04))),
                ),
                ("note".to_string(), Value::Text("valid".into())),
            ]),
        )?;
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let fire = db.execute(
        "INSERT INTO guard_writes (id, content) VALUES ($id, 'fire')",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE10)))]),
    );
    let rollback_wrong_tx = db.rollback(wrong_tx);
    let rollback_other_wrong_tx = other_db.rollback(other_wrong_tx);
    let attempts = attempts.lock().unwrap().clone();
    assert!(
        fire.is_ok()
            && rollback_wrong_tx.is_ok()
            && rollback_other_wrong_tx.is_ok()
            && attempts.len() == 9
            && attempts.iter().all(|(_, rejected, message)| {
                *rejected
                    && (message.contains(
                        "trigger callback writes must use the supplied tx-bound database handle",
                    ) || message.contains("trigger callback is active")
                        || message.contains("inside trigger callbacks"))
            })
            && count_rows(&db, "guard_audits") == 1
            && count_rows(&other_db, "escape_writes") == 0
            && count_rows(&third_owner_db, "third_owner_escape") == 0,
        "trigger callbacks must not escape the firing tx or supplied handle; fire={fire:?}, rollback={rollback_wrong_tx:?}, rollback_other={rollback_other_wrong_tx:?}, attempts={attempts:?}, guard_audits={}, escape_rows={}, third_owner_escape_rows={}",
        count_rows(&db, "guard_audits"),
        count_rows(&other_db, "escape_writes"),
        count_rows(&third_owner_db, "third_owner_escape")
    );
}

#[test]
fn t3_trigger_dispatch_uses_commit_final_write_set() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE final_writes (id UUID PRIMARY KEY, slug TEXT UNIQUE, status TEXT, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE final_audits (id UUID PRIMARY KEY, slug TEXT, event TEXT, content TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER final_insert_trigger ON final_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER final_update_trigger ON final_writes WHEN UPDATE",
        &empty(),
    )
    .unwrap();

    let events = Arc::new(Mutex::new(Vec::<(TriggerEvent, String, String)>::new()));
    for (trigger_name, expected_event) in [
        ("final_insert_trigger", TriggerEvent::Insert),
        ("final_update_trigger", TriggerEvent::Update),
    ] {
        let events = events.clone();
        db.register_trigger_callback(trigger_name, move |db_handle, ctx| {
            if ctx.event != expected_event {
                return Err(Error::Other(format!(
                    "{trigger_name} saw wrong event {:?}",
                    ctx.event
                )));
            }
            let slug = ctx
                .row_values
                .get("slug")
                .and_then(Value::as_text)
                .ok_or_else(|| Error::Other("missing slug".into()))?
                .to_string();
            let content = ctx
                .row_values
                .get("content")
                .and_then(Value::as_text)
                .ok_or_else(|| Error::Other("missing content".into()))?
                .to_string();
            events
                .lock()
                .unwrap()
                .push((ctx.event, slug.clone(), content.clone()));
            db_handle.insert_row(
                ctx.tx,
                "final_audits",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("slug".to_string(), Value::Text(slug)),
                    (
                        "event".to_string(),
                        Value::Text(
                            match ctx.event {
                                TriggerEvent::Insert => "INSERT",
                                TriggerEvent::Update => "UPDATE",
                            }
                            .into(),
                        ),
                    ),
                    ("content".to_string(), Value::Text(content)),
                ]),
            )?;
            Ok(())
        })
        .unwrap();
    }
    db.complete_initialization().unwrap();

    db.execute(
        "INSERT INTO final_writes (id, slug, status, content) VALUES ($id, 'stale-key', 'pending', 'base')",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE20)))]),
    )
    .unwrap();
    let stale_tx = db.begin();
    db.execute_in_tx(
        stale_tx,
        "UPDATE final_writes SET status = 'ack', content = 'stale-cascade' WHERE id = $id AND status = 'pending'",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE20)))]),
    )
    .unwrap();
    db.execute(
        "UPDATE final_writes SET status = 'done', content = 'winner' WHERE id = $id AND status = 'pending'",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE20)))]),
    )
    .unwrap();
    let stale_commit = db.commit(stale_tx);

    let upsert_tx = db.begin();
    db.execute_in_tx(
        upsert_tx,
        "INSERT INTO final_writes (id, slug, status, content) VALUES ($id, 'upsert-key', 'open', $content) ON CONFLICT (slug) DO UPDATE SET content = $content",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0xE21))),
            ("content".to_string(), Value::Text("from-upsert".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO final_writes (id, slug, status, content) VALUES ($id, 'upsert-key', 'open', 'committed-before-upsert')",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xE22)))]),
    )
    .unwrap();
    let upsert_commit = db.commit(upsert_tx);

    let api_insert_tx = db.begin();
    let api_insert = db.upsert_row(
        api_insert_tx,
        "final_writes",
        "slug",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0xE23))),
            ("slug".to_string(), Value::Text("api-upsert-key".into())),
            ("status".to_string(), Value::Text("open".into())),
            ("content".to_string(), Value::Text("api-insert".into())),
        ]),
    );
    let api_insert_commit = db.commit(api_insert_tx);
    let api_update_tx = db.begin();
    let api_update = db.upsert_row(
        api_update_tx,
        "final_writes",
        "slug",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(0xE23))),
            ("slug".to_string(), Value::Text("api-upsert-key".into())),
            ("status".to_string(), Value::Text("open".into())),
            ("content".to_string(), Value::Text("api-update".into())),
        ]),
    );
    let api_update_commit = db.commit(api_update_tx);

    let events = events.lock().unwrap().clone();
    let stale_row = row_for_column_uuid(&db, "final_writes", "id", uuid(0xE20))
        .expect("stale row must still exist");
    let stale_content = stale_row
        .values
        .get("content")
        .and_then(Value::as_text)
        .unwrap_or("<missing>");
    let audit_rows = db.scan("final_audits", db.snapshot()).unwrap();
    assert!(
        stale_commit.is_ok()
            && upsert_commit.is_ok()
            && matches!(api_insert, Ok(UpsertResult::Inserted))
            && api_insert_commit.is_ok()
            && matches!(api_update, Ok(UpsertResult::Updated))
            && api_update_commit.is_ok()
            && stale_content == "winner"
            && events.iter().filter(|(_, slug, _)| slug == "stale-key").count() == 2
            && !events
                .iter()
                .any(|(_, slug, content)| slug == "stale-key" && content == "stale-cascade")
            && events.iter().any(|(event, slug, content)| {
                *event == TriggerEvent::Update
                    && slug == "upsert-key"
                    && content == "from-upsert"
            })
            && events
                .iter()
                .filter(|(event, slug, _)| {
                    *event == TriggerEvent::Insert && slug == "upsert-key"
                })
                .count()
                == 1
            && events
                .iter()
                .filter(|(event, slug, _)| {
                    *event == TriggerEvent::Insert && slug == "api-upsert-key"
                })
                .count()
                == 1
            && events.iter().any(|(event, slug, content)| {
                *event == TriggerEvent::Update
                    && slug == "api-upsert-key"
                    && content == "api-update"
            })
            && events
                .iter()
                .filter(|(event, slug, content)| {
                    *event == TriggerEvent::Insert
                        && slug == "api-upsert-key"
                        && content == "api-update"
                })
                .count()
                == 0
            && audit_rows
                .iter()
                .filter(|row| matches!(row.values.get("slug"), Some(Value::Text(slug)) if slug == "upsert-key"))
                .count()
                == 2,
        "trigger dispatch must use the final commit-normalized write set: stale_commit={stale_commit:?}, upsert_commit={upsert_commit:?}, api_insert={api_insert:?}, api_insert_commit={api_insert_commit:?}, api_update={api_update:?}, api_update_commit={api_update_commit:?}, stale_content={stale_content}, events={events:?}, audits={audit_rows:?}"
    );
}

#[test]
fn t3_sync_trigger_ddl_replay_respects_sender_lsn_order() {
    let origin = Database::open_memory();
    setup_host_tables(&origin);
    register_audit_callback(&origin, Arc::new(AtomicUsize::new(0)));
    origin.complete_initialization().unwrap();
    origin
        .execute(
            host_insert_sql(),
            &insert_host_params(uuid(0xE40), "ordered"),
        )
        .unwrap();
    origin
        .execute("DROP TRIGGER host_write_trigger", &empty())
        .unwrap();

    let changes = origin.changes_since(Lsn(0));
    let trigger_create_lsn = changes
        .ddl
        .iter()
        .zip(changes.ddl_lsn.iter())
        .find_map(|(ddl, lsn)| {
            matches!(ddl, DdlChange::CreateTrigger { name, .. } if name == "host_write_trigger")
                .then_some(*lsn)
        })
        .expect("origin history must carry trigger create LSN");
    let trigger_drop_lsn = changes
        .ddl
        .iter()
        .zip(changes.ddl_lsn.iter())
        .find_map(|(ddl, lsn)| {
            matches!(ddl, DdlChange::DropTrigger { name } if name == "host_write_trigger")
                .then_some(*lsn)
        })
        .expect("origin history must carry trigger drop LSN");
    let data_lsn = changes
        .rows
        .iter()
        .find(|row| row.table == "host_writes")
        .map(|row| row.lsn)
        .expect("origin history must carry firing data");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    let fired = receiver
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert!(
        trigger_create_lsn < data_lsn
            && data_lsn < trigger_drop_lsn
            && receiver.list_triggers().is_empty()
            && fired.len() == 1
            && fired[0].trigger_name == "host_write_trigger"
            && fired[0].firing_lsn == data_lsn,
        "sync replay must apply trigger DDL chronologically so create->data->drop audits historical fires without leaving the trigger active; create_lsn={trigger_create_lsn:?}, data_lsn={data_lsn:?}, drop_lsn={trigger_drop_lsn:?}, triggers={:?}, fired={fired:?}",
        receiver.list_triggers()
    );
}

#[test]
fn t3_sibling_callback_cascades_validate_after_all_triggers_stage() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE sibling_sources (id UUID PRIMARY KEY, kind TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sibling_parents (id UUID PRIMARY KEY)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sibling_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES sibling_parents(id))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER sibling_trigger ON sibling_sources WHEN INSERT",
        &empty(),
    )
    .unwrap();
    let parent_id = uuid(0xEAA);
    db.register_trigger_callback("sibling_trigger", move |db_handle, ctx| {
        let kind = ctx
            .row_values
            .get("kind")
            .and_then(Value::as_text)
            .ok_or_else(|| Error::Other("missing sibling source kind".into()))?;
        match kind {
            "child-first" => {
                db_handle.execute(
                    "INSERT INTO sibling_children (id, parent_id) VALUES ($id, $parent_id)",
                    &HashMap::from([
                        ("id".to_string(), Value::Uuid(uuid(0xEAB))),
                        ("parent_id".to_string(), Value::Uuid(parent_id)),
                    ]),
                )?;
            }
            "parent-second" => {
                db_handle.execute(
                    "INSERT INTO sibling_parents (id) VALUES ($id)",
                    &HashMap::from([("id".to_string(), Value::Uuid(parent_id))]),
                )?;
            }
            other => {
                return Err(Error::Other(format!(
                    "unexpected sibling source kind {other}"
                )));
            }
        }
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let tx = db.begin();
    db.execute_in_tx(
        tx,
        "INSERT INTO sibling_sources (id, kind) VALUES ($id, 'child-first')",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xEA1)))]),
    )
    .unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO sibling_sources (id, kind) VALUES ($id, 'parent-second')",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid(0xEA2)))]),
    )
    .unwrap();
    let commit = db.commit(tx);

    assert!(
        commit.is_ok()
            && count_rows(&db, "sibling_sources") == 2
            && count_rows(&db, "sibling_parents") == 1
            && count_rows(&db, "sibling_children") == 1,
        "trigger callback cascades from sibling firing rows must validate after all callbacks stage, not after each callback; commit={commit:?}, counts=({}, {}, {})",
        count_rows(&db, "sibling_sources"),
        count_rows(&db, "sibling_parents"),
        count_rows(&db, "sibling_children")
    );
}

#[test]
fn t3_atomicity_cascade_rows_share_lsn_and_event() {
    let db = Database::open_memory();
    setup_host_tables(&db);
    let fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&db, fires.clone());
    db.complete_initialization().unwrap();

    let since = db.current_lsn();
    let rx = db.subscribe();
    db.execute(host_insert_sql(), &insert_host_params(uuid(10), "first"))
        .unwrap();

    let changes = db.changes_since(since);
    let host_lsns: HashSet<Lsn> = changes
        .rows
        .iter()
        .filter(|row| row.table == "host_writes")
        .map(|row| row.lsn)
        .collect();
    let audit_lsns: HashSet<Lsn> = changes
        .rows
        .iter()
        .filter(|row| row.table == "host_audits")
        .map(|row| row.lsn)
        .collect();
    let edge_lsns: HashSet<Lsn> = changes
        .edges
        .iter()
        .filter(|edge| edge.edge_type == "AUDITED_BY")
        .map(|edge| edge.lsn)
        .collect();
    let vector_lsns: HashSet<Lsn> = changes
        .vectors
        .iter()
        .filter(|vector| vector.index == VectorIndexRef::new("host_audits", "embedding"))
        .map(|vector| vector.lsn)
        .collect();
    let event = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("host write must publish one commit event");

    assert_eq!(fires.load(Ordering::SeqCst), 1, "callback must fire once");
    let commit_lsn = *host_lsns
        .iter()
        .next()
        .expect("firing row must produce one commit LSN");
    assert_eq!(
        event.lsn, commit_lsn,
        "commit event LSN must match the durable firing/cascade LSN; event={event:?}, changes={changes:?}"
    );
    let commit_tx = TxId(db.snapshot_at(commit_lsn).0);
    let firing_row = row_for_column_uuid(&db, "host_writes", "id", uuid(10))
        .expect("firing row must be visible");
    let audit_row = row_for_column_uuid(&db, "host_audits", "write_id", uuid(10))
        .expect("audit row must be visible");
    assert_eq!(
        audit_row.values.get("note"),
        Some(&Value::Text("cascade:first".into())),
        "INSERT trigger context must expose the full post-image row, including non-key content"
    );
    let edge_change = changes
        .edges
        .iter()
        .find(|edge| edge.source == uuid(10) && edge.edge_type == "AUDITED_BY")
        .expect("graph cascade edge change must be present");
    assert_eq!(
        host_lsns.len(),
        1,
        "firing row must commit under exactly one LSN; changes={changes:?}"
    );
    assert_eq!(
        host_lsns, audit_lsns,
        "firing row and cascade row must share one commit LSN; changes={changes:?}"
    );
    assert_eq!(
        host_lsns, edge_lsns,
        "graph cascade edge must share the firing commit LSN; changes={changes:?}"
    );
    assert_eq!(
        host_lsns, vector_lsns,
        "vector cascade entry must share the firing commit LSN; changes={changes:?}"
    );
    assert!(
        event.tables_changed.contains(&"host_writes".to_string())
            && event.tables_changed.contains(&"host_audits".to_string()),
        "commit event must include both firing and cascade tables; got {event:?}"
    );
    assert_eq!(
        event.row_count, 4,
        "firing row, cascade row, graph edge, and vector entry must publish as one atomic commit"
    );
    assert_eq!(
        db.edge_count(uuid(10), "AUDITED_BY", db.snapshot())
            .unwrap(),
        1,
        "trigger callback graph edge must be visible after commit"
    );
    let audit_row_id = row_id_for_column_uuid(&db, "host_audits", "write_id", uuid(10))
        .expect("audit row must be visible");
    let vector_entry = db
        .live_vector_entry(audit_row_id, db.snapshot())
        .expect("trigger callback vector entry must be visible");
    assert_eq!(vector_entry.vector, vec![1.0, 0.0, 0.0]);
    assert_eq!(
        firing_row.created_tx, commit_tx,
        "firing row TxId must resolve from the commit LSN"
    );
    assert_eq!(
        audit_row.created_tx, firing_row.created_tx,
        "relational cascade row must share the firing TxId"
    );
    assert_eq!(
        vector_entry.created_tx, firing_row.created_tx,
        "vector cascade entry must share the firing TxId"
    );
    assert_eq!(
        edge_change.properties.get("trigger_tx"),
        Some(&Value::TxId(firing_row.created_tx)),
        "graph cascade edge must be created from the same trigger TxId; edge_change={edge_change:?}"
    );
    let audit_id = audit_row
        .values
        .get("id")
        .and_then(Value::as_uuid)
        .copied()
        .expect("audit row must have UUID id");
    assert_eq!(
        db.get_edge_properties(uuid(10), audit_id, "AUDITED_BY", db.snapshot())
            .unwrap_or_default()
            .and_then(|props| props.get("trigger_tx").cloned()),
        Some(Value::TxId(firing_row.created_tx)),
        "visible graph edge properties must retain the firing TxId"
    );
    assert!(
        rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "trigger firing must publish exactly one commit event"
    );

    let api_db = Database::open_memory();
    setup_host_tables(&api_db);
    let api_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&api_db, api_fires.clone());
    api_db.complete_initialization().unwrap();
    let api_since = api_db.current_lsn();
    let api_rx = api_db.subscribe();
    let api_tx = api_db.begin();
    api_db
        .insert_row(
            api_tx,
            "host_writes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(uuid(11))),
                ("content".to_string(), Value::Text("direct-api".into())),
            ]),
        )
        .expect("direct insert_row into a trigger-attached table must be a firing write route");
    api_db.commit(api_tx).unwrap();
    let api_changes = api_db.changes_since(api_since);
    let api_event = api_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("direct insert_row firing must publish one event");
    let api_audit_row = row_for_column_uuid(&api_db, "host_audits", "write_id", uuid(11));
    if api_fires.load(Ordering::SeqCst) != 1
        || count_rows(&api_db, "host_writes") != 1
        || count_rows(&api_db, "host_audits") != 1
        || api_db
            .edge_count(uuid(11), "AUDITED_BY", api_db.snapshot())
            .unwrap()
            != 1
        || !matches!(
            &api_audit_row,
            Some(row)
                if row.values.get("note") == Some(&Value::Text("cascade:direct-api".into()))
                    && api_db.live_vector_entry(row.row_id, api_db.snapshot()).is_some()
        )
        || api_event.row_count != 4
        || row_group_signatures(&api_db, api_changes).len() != 1
    {
        panic!(
            "public insert_row must participate in the same relational trigger hook as SQL INSERT; fires={}, event={api_event:?}, audit_row={api_audit_row:?}, host_rows={}, audit_rows={}, edge_count={}",
            api_fires.load(Ordering::SeqCst),
            count_rows(&api_db, "host_writes"),
            count_rows(&api_db, "host_audits"),
            api_db
                .edge_count(uuid(11), "AUDITED_BY", api_db.snapshot())
                .unwrap()
        );
    }
}

#[test]
fn t3_tier2_cascade_unique_violation_rolls_back_firing() {
    let mut failures = Vec::new();

    {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("unique_rollback.redb");
        let db = Database::open(&path).unwrap();
        setup_host_tables(&db);
        let (audit_hnsw_count, audit_hnsw_max_row_id, audit_hnsw_baseline) =
            seed_host_audit_hnsw(&db);
        let audit_hnsw_index = VectorIndexRef::new("host_audits", "embedding");
        let rollback_vector_rows = Arc::new(Mutex::new(Vec::new()));
        let captured_rollback_vector_rows = rollback_vector_rows.clone();
        db.register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("unique rollback row missing UUID id".into()))?;
            let first_audit_id = Uuid::new_v4();
            let first_audit_row = db_handle.insert_row(
                ctx.tx,
                "host_audits",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(first_audit_id)),
                    ("write_id".to_string(), Value::Uuid(write_id)),
                    ("note".to_string(), Value::Text("dup-0".into())),
                    ("embedding".to_string(), Value::Vector(vec![0.4, 0.5, 0.6])),
                ]),
            )?;
            captured_rollback_vector_rows
                .lock()
                .unwrap()
                .push(first_audit_row);
            db_handle.insert_edge(
                ctx.tx,
                write_id,
                first_audit_id,
                "ROLLBACK_EDGE".into(),
                HashMap::from([("must_rollback".into(), Value::Bool(true))]),
            )?;
            db_handle.insert_vector(
                ctx.tx,
                VectorIndexRef::new("host_audits", "embedding"),
                first_audit_row,
                vec![0.4, 0.5, 0.6],
            )?;
            let mut duplicate = HashMap::new();
            duplicate.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            duplicate.insert("write_id".into(), Value::Uuid(write_id));
            duplicate.insert("note".into(), Value::Text("dup-1".into()));
            db_handle.execute(
                "INSERT INTO host_audits (id, write_id, note) VALUES ($id, $write_id, $note)",
                &duplicate,
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let since = db.current_lsn();
        let result = db.execute(host_insert_sql(), &insert_host_params(uuid(20), "dup"));
        let leaked_changes = db.changes_since(since);
        let mut rollback_probe_rows = candidate_row_ids_after(audit_hnsw_max_row_id);
        rollback_probe_rows.extend(rollback_vector_rows.lock().unwrap().iter().copied());
        let leaked_vector_rows = live_vector_rows(&db, rollback_probe_rows.iter().copied());
        let leaked_hnsw_rows = hnsw_raw_leaks(
            &db,
            audit_hnsw_index.clone(),
            rollback_probe_rows.iter().copied(),
        );
        let hnsw_failure = hnsw_baseline_failure(
            &db,
            audit_hnsw_index.clone(),
            &audit_hnsw_baseline,
            "cascade UNIQUE violation rollback",
        );
        if !matches!(
            result,
            Err(Error::UniqueViolation { ref table, ref column })
                if table == "host_audits" && column == "write_id"
        ) || count_rows(&db, "host_writes") != 0
            || count_rows(&db, "host_audits") != audit_hnsw_count
            || db
                .edge_count(uuid(20), "ROLLBACK_EDGE", db.snapshot())
                .unwrap()
                != 0
            || !leaked_vector_rows.is_empty()
            || !leaked_hnsw_rows.is_empty()
            || hnsw_failure.is_some()
            || changeset_has_data(&leaked_changes)
        {
            failures.push(format!(
                "cascade UNIQUE violation must abort firing plus relational/graph/vector cascade writes without historical leakage; result={result:?}, host_rows={}, audit_rows={}, leaked_edges={}, leaked_vector_rows={leaked_vector_rows:?}, leaked_hnsw_rows={leaked_hnsw_rows:?}, hnsw_failure={hnsw_failure:?}, leaked_changes={leaked_changes:?}",
                count_rows(&db, "host_writes"),
                count_rows(&db, "host_audits"),
                db.edge_count(uuid(20), "ROLLBACK_EDGE", db.snapshot())
                    .unwrap()
            ));
        }
        if let Some(failure) = rollback_audit_failure(
            &db,
            "cascade UNIQUE violation",
            &["host_audits", "write_id", "unique"],
        ) {
            failures.push(failure);
        }
        db.close().unwrap();
        if let Some(failure) = reopened_rollback_absence_failure(
            &path,
            "cascade UNIQUE violation",
            since,
            &[("host_writes", 0), ("host_audits", audit_hnsw_count)],
            Some((uuid(20), "ROLLBACK_EDGE")),
            Some((audit_hnsw_index, rollback_probe_rows, audit_hnsw_baseline)),
        ) {
            failures.push(failure);
        }
        if let Some(failure) = reopened_rollback_audit_failure(
            &path,
            "cascade UNIQUE violation",
            &["host_audits", "write_id", "unique"],
        ) {
            failures.push(failure);
        }
    }

    {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("callback_err_rollback.redb");
        let db = Database::open(&path).unwrap();
        setup_host_tables(&db);
        let (audit_hnsw_count, audit_hnsw_max_row_id, audit_hnsw_baseline) =
            seed_host_audit_hnsw(&db);
        let audit_hnsw_index = VectorIndexRef::new("host_audits", "embedding");
        let rollback_vector_rows = Arc::new(Mutex::new(Vec::new()));
        let captured_rollback_vector_rows = rollback_vector_rows.clone();
        db.register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("callback-error row missing UUID id".into()))?;
            let audit_id = uuid(0xE100);
            let audit_row = db_handle.insert_row(
                ctx.tx,
                "host_audits",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(audit_id)),
                    ("write_id".to_string(), Value::Uuid(write_id)),
                    (
                        "note".to_string(),
                        Value::Text("side-effect-before-callback-error".into()),
                    ),
                    ("embedding".to_string(), Value::Vector(vec![0.6, 0.7, 0.8])),
                ]),
            )?;
            captured_rollback_vector_rows
                .lock()
                .unwrap()
                .push(audit_row);
            db_handle.insert_edge(
                ctx.tx,
                write_id,
                audit_id,
                "ERR_ROLLBACK_EDGE".into(),
                HashMap::from([("must_rollback".into(), Value::Bool(true))]),
            )?;
            db_handle.insert_vector(
                ctx.tx,
                VectorIndexRef::new("host_audits", "embedding"),
                audit_row,
                vec![0.6, 0.7, 0.8],
            )?;
            Err(Error::TriggerCallbackFailed {
                trigger_name: ctx.trigger_name.clone(),
                reason: "callback-error-after-side-effects".into(),
            })
        })
        .unwrap();
        db.complete_initialization().unwrap();

        let since = db.current_lsn();
        let result = db.execute(
            host_insert_sql(),
            &insert_host_params(uuid(25), "callback-error"),
        );
        let leaked_changes = db.changes_since(since);
        let mut rollback_probe_rows = candidate_row_ids_after(audit_hnsw_max_row_id);
        rollback_probe_rows.extend(rollback_vector_rows.lock().unwrap().iter().copied());
        let leaked_vector_rows = live_vector_rows(&db, rollback_probe_rows.iter().copied());
        let leaked_hnsw_rows = hnsw_raw_leaks(
            &db,
            audit_hnsw_index.clone(),
            rollback_probe_rows.iter().copied(),
        );
        let hnsw_failure = hnsw_baseline_failure(
            &db,
            audit_hnsw_index.clone(),
            &audit_hnsw_baseline,
            "callback Err rollback",
        );
        if !matches!(
            result,
            Err(Error::TriggerCallbackFailed { ref trigger_name, ref reason })
                if trigger_name == "host_write_trigger"
                    && reason.contains("callback-error-after-side-effects")
        ) || count_rows(&db, "host_writes") != 0
            || count_rows(&db, "host_audits") != audit_hnsw_count
            || db
                .edge_count(uuid(25), "ERR_ROLLBACK_EDGE", db.snapshot())
                .unwrap()
                != 0
            || !leaked_vector_rows.is_empty()
            || !leaked_hnsw_rows.is_empty()
            || hnsw_failure.is_some()
            || changeset_has_data(&leaked_changes)
        {
            failures.push(format!(
                "callback-returned Err after relational/graph/vector side effects must roll back the firing tx without live, raw-HNSW, or changelog leakage; result={result:?}, host_rows={}, audit_rows={}, leaked_edges={}, leaked_vector_rows={leaked_vector_rows:?}, leaked_hnsw_rows={leaked_hnsw_rows:?}, hnsw_failure={hnsw_failure:?}, leaked_changes={leaked_changes:?}",
                count_rows(&db, "host_writes"),
                count_rows(&db, "host_audits"),
                db.edge_count(uuid(25), "ERR_ROLLBACK_EDGE", db.snapshot())
                    .unwrap()
            ));
        }
        if let Some(failure) = rollback_audit_failure(
            &db,
            "callback Err after side effects",
            &["callback-error-after-side-effects"],
        ) {
            failures.push(failure);
        }
        db.close().unwrap();
        if let Some(failure) = reopened_rollback_absence_failure(
            &path,
            "callback Err after side effects",
            since,
            &[("host_writes", 0), ("host_audits", audit_hnsw_count)],
            Some((uuid(25), "ERR_ROLLBACK_EDGE")),
            Some((audit_hnsw_index, rollback_probe_rows, audit_hnsw_baseline)),
        ) {
            failures.push(failure);
        }
        if let Some(failure) = reopened_rollback_audit_failure(
            &path,
            "callback Err after side effects",
            &["callback-error-after-side-effects"],
        ) {
            failures.push(failure);
        }
    }

    {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fk_rollback.redb");
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE TABLE parents (id UUID PRIMARY KEY)", &empty())
            .unwrap();
        db.execute(
            "CREATE TABLE cascade_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id))",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
        db.register_trigger_callback("host_write_trigger", move |db_handle, _| {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            p.insert("parent_id".into(), Value::Uuid(Uuid::new_v4()));
            db_handle.execute(
                "INSERT INTO cascade_children (id, parent_id) VALUES ($id, $parent_id)",
                &p,
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let since = db.current_lsn();
        let result = db.execute(host_insert_sql(), &insert_host_params(uuid(21), "fk"));
        let leaked_changes = db.changes_since(since);
        if !matches!(
            result,
            Err(Error::ForeignKeyViolation { ref child_table, ref child_columns, .. })
                if child_table == "cascade_children"
                    && child_columns == &vec!["parent_id".to_string()]
        ) || count_rows(&db, "host_writes") != 0
            || count_rows(&db, "cascade_children") != 0
            || changeset_has_data(&leaked_changes)
        {
            failures.push(format!(
                "cascade FK violation must run through commit_validate and roll back firing row without historical leakage; result={result:?}, host_rows={}, child_rows={}, leaked_changes={leaked_changes:?}",
                count_rows(&db, "host_writes"),
                count_rows(&db, "cascade_children")
            ));
        }
        if let Some(failure) = rollback_audit_failure(
            &db,
            "cascade FK violation",
            &["cascade_children", "parent_id", "foreign"],
        ) {
            failures.push(failure);
        }
        db.close().unwrap();
        if let Some(failure) = reopened_rollback_absence_failure(
            &path,
            "cascade FK violation",
            since,
            &[("host_writes", 0), ("cascade_children", 0)],
            None,
            None,
        ) {
            failures.push(failure);
        }
        if let Some(failure) = reopened_rollback_audit_failure(
            &path,
            "cascade FK violation",
            &["cascade_children", "parent_id", "foreign"],
        ) {
            failures.push(failure);
        }
    }

    {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state_rollback.redb");
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE cascade_states (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending') STATE MACHINE (status: pending -> [acknowledged], acknowledged -> [resolved])",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
        db.register_trigger_callback("host_write_trigger", move |db_handle, _| {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            db_handle.execute(
                "INSERT INTO cascade_states (id, status) VALUES ($id, 'pending')",
                &p,
            )?;
            db_handle.execute(
                "UPDATE cascade_states SET status = 'resolved' WHERE id = $id",
                &p,
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let since = db.current_lsn();
        let result = db.execute(host_insert_sql(), &insert_host_params(uuid(22), "state"));
        let leaked_changes = db.changes_since(since);
        if !matches!(result, Err(Error::InvalidStateTransition(_)))
            || count_rows(&db, "host_writes") != 0
            || count_rows(&db, "cascade_states") != 0
            || changeset_has_data(&leaked_changes)
        {
            failures.push(format!(
                "cascade state-machine violation must run through commit_validate and roll back firing row without historical leakage; result={result:?}, host_rows={}, state_rows={}, leaked_changes={leaked_changes:?}",
                count_rows(&db, "host_writes"),
                count_rows(&db, "cascade_states")
            ));
        }
        if let Some(failure) = rollback_audit_failure(
            &db,
            "cascade state-machine violation",
            &["cascade_states", "state"],
        ) {
            failures.push(failure);
        }
        db.close().unwrap();
        if let Some(failure) = reopened_rollback_absence_failure(
            &path,
            "cascade state-machine violation",
            since,
            &[("host_writes", 0), ("cascade_states", 0)],
            None,
            None,
        ) {
            failures.push(failure);
        }
        if let Some(failure) = reopened_rollback_audit_failure(
            &path,
            "cascade state-machine violation",
            &["cascade_states", "state"],
        ) {
            failures.push(failure);
        }
    }

    {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE host_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE cascade_counters (id UUID PRIMARY KEY, value INTEGER)",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
        let counter_id = uuid(23);
        db.register_trigger_callback("host_write_trigger", move |db_handle, _| {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(counter_id));
            db_handle.execute(
                "INSERT INTO cascade_counters (id, value) VALUES ($id, 0)",
                &p,
            )?;
            db_handle.execute(
                "UPDATE cascade_counters SET value = 1 WHERE id = $id AND value = 0",
                &p,
            )?;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        let result = db.execute(
            host_insert_sql(),
            &insert_host_params(uuid(24), "conditional"),
        );
        let rows = db
            .execute(
                "SELECT value FROM cascade_counters WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(counter_id))]),
            )
            .map(|result| result.rows)
            .unwrap_or_default();
        if result.is_err() || rows != vec![vec![Value::Int64(1)]] {
            failures.push(format!(
                "cascade conditional UPDATE must evaluate over the full staged writeset; result={result:?}, rows={rows:?}"
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn t3_persist_trigger_declaration_audit_and_no_replay_refire() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("trigger_persistence.redb");
    let expected_fire_lsn;
    {
        let db = Database::open(&path).unwrap();
        setup_host_tables(&db);
        let fires = Arc::new(AtomicUsize::new(0));
        register_audit_callback(&db, fires.clone());
        db.complete_initialization().unwrap();
        let since = db.current_lsn();
        db.execute(host_insert_sql(), &insert_host_params(uuid(30), "persist"))
            .unwrap();
        assert_eq!(fires.load(Ordering::SeqCst), 1);
        expected_fire_lsn = db
            .changes_since(since)
            .rows
            .iter()
            .find(|row| row.table == "host_writes")
            .map(|row| row.lsn)
            .expect("firing row change must be recorded");
        db.close().unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    let cold_gate_since = reopened.current_lsn();
    let cold_reopen_write = reopened.execute(
        host_insert_sql(),
        &insert_host_params(uuid(31), "cold-reopen"),
    );
    assert!(
        matches!(
            cold_reopen_write,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
        ),
        "reopened handles with persisted trigger declarations must re-enter the cold-start gate; got {cold_reopen_write:?}"
    );
    let cold_reopen_apply = reopened.apply_changes(
        ChangeSet {
            rows: vec![row_change("host_writes", uuid(32), "cold-apply", Lsn(3032))],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    assert!(
        matches!(
            cold_reopen_apply,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("apply_changes")
        ),
        "reopened handles must reject sync apply until callbacks are ready; got {cold_reopen_apply:?}"
    );
    let cold_direct_tx = reopened.begin();
    let cold_direct_insert = reopened.insert_row(
        cold_direct_tx,
        "host_writes",
        HashMap::from([
            ("id".to_string(), Value::Uuid(uuid(34))),
            ("content".to_string(), Value::Text("cold-direct-api".into())),
        ]),
    );
    let _ = reopened.rollback(cold_direct_tx);
    assert!(
        matches!(
            cold_direct_insert,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
        ) && count_rows(&reopened, "host_writes") == 1
            && count_rows(&reopened, "host_audits") == 1
            && !changeset_has_data(&reopened.changes_since(cold_gate_since)),
        "direct Database::insert_row must also enter the persisted-trigger cold gate before callback registration and must not leak staged data; result={cold_direct_insert:?}, host_rows={}, audit_rows={}, leaked_changes={:?}",
        count_rows(&reopened, "host_writes"),
        count_rows(&reopened, "host_audits"),
        reopened.changes_since(cold_gate_since)
    );
    let cold_vector_tx = reopened.begin();
    let cold_vector_insert = reopened.insert_vector(
        cold_vector_tx,
        VectorIndexRef::new("host_writes", "embedding"),
        RowId(999_001),
        vec![1.0, 0.0, 0.0],
    );
    let cold_vector_delete = reopened.delete_vector(
        cold_vector_tx,
        VectorIndexRef::new("host_writes", "embedding"),
        RowId(999_001),
    );
    let _ = reopened.rollback(cold_vector_tx);
    assert!(
        matches!(
            cold_vector_insert,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("insert_vector")
        ) && matches!(
            cold_vector_delete,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
                    && operation.contains("delete_vector")
        ) && !changeset_has_data(&reopened.changes_since(cold_gate_since)),
        "direct vector APIs must also fail closed for trigger-attached tables before callback registration; insert={cold_vector_insert:?}, delete={cold_vector_delete:?}, leaked_changes={:?}",
        reopened.changes_since(cold_gate_since)
    );
    let replay_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&reopened, replay_fires.clone());
    reopened.complete_initialization().unwrap();

    assert_eq!(
        replay_fires.load(Ordering::SeqCst),
        0,
        "opening persisted trigger state must not replay user callbacks"
    );
    assert!(
        reopened
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "host_write_trigger" && trigger.table == "host_writes"),
        "trigger declaration must persist across reopen"
    );
    assert_eq!(
        count_rows(&reopened, "host_audits"),
        1,
        "cascade row must persist with the firing row"
    );
    let audit = reopened
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        audit.len(),
        1,
        "persistent trigger audit must record exactly one committed firing; got {audit:?}"
    );
    let entry = &audit[0];
    assert_eq!(entry.trigger_name, "host_write_trigger");
    assert_eq!(entry.status, TriggerAuditStatus::Fired);
    assert_eq!(entry.firing_lsn, expected_fire_lsn);
    assert_eq!(
        entry.firing_tx,
        TxId(reopened.snapshot_at(expected_fire_lsn).0),
        "audit firing_tx must resolve to the committed firing LSN"
    );
    assert_eq!(entry.depth, 1);
    assert_eq!(
        entry.cascade_row_count, 3,
        "audit must count the callback's relational, graph, and vector cascade effects"
    );

    let post_reopen_since = reopened.current_lsn();
    reopened
        .execute(
            host_insert_sql(),
            &insert_host_params(uuid(33), "post-reopen"),
        )
        .expect("post-reopen local writes must fire persisted trigger declarations once callbacks are ready");
    assert_eq!(
        replay_fires.load(Ordering::SeqCst),
        1,
        "post-reopen local write must fire the callback exactly once"
    );
    assert_eq!(
        count_rows(&reopened, "host_writes"),
        2,
        "post-reopen firing row must persist alongside pre-reopen row"
    );
    assert_eq!(
        count_rows(&reopened, "host_audits"),
        2,
        "post-reopen trigger cascade row must persist"
    );
    let post_reopen_changes = reopened.changes_since(post_reopen_since);
    let post_reopen_lsn = post_reopen_changes
        .rows
        .iter()
        .find(|row| row.table == "host_writes")
        .map(|row| row.lsn)
        .expect("post-reopen firing row change must be recorded");
    let post_reopen_audit = row_for_column_uuid(&reopened, "host_audits", "write_id", uuid(33))
        .expect("post-reopen trigger cascade row must be visible");
    assert_eq!(
        post_reopen_audit.values.get("note"),
        Some(&Value::Text("cascade:post-reopen".into())),
        "post-reopen trigger context must preserve full INSERT post-image content"
    );
    assert!(
        reopened
            .live_vector_entry(post_reopen_audit.row_id, reopened.snapshot())
            .is_some(),
        "post-reopen trigger cascade vector must be visible"
    );
    let post_reopen_audit_id = post_reopen_audit
        .values
        .get("id")
        .and_then(Value::as_uuid)
        .copied()
        .expect("post-reopen audit row must have UUID id");
    assert_eq!(
        reopened
            .edge_count(uuid(33), "AUDITED_BY", reopened.snapshot())
            .unwrap(),
        1,
        "post-reopen trigger cascade edge must be visible"
    );
    assert!(
        reopened
            .get_edge_properties(
                uuid(33),
                post_reopen_audit_id,
                "AUDITED_BY",
                reopened.snapshot()
            )
            .unwrap_or_default()
            .is_some(),
        "post-reopen trigger cascade edge properties must persist"
    );
    let post_reopen_history = reopened
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        post_reopen_history.len(),
        2,
        "post-reopen local firing must append to durable trigger audit history; got {post_reopen_history:?}"
    );
    assert!(
        post_reopen_history
            .iter()
            .any(|entry| entry.firing_lsn == post_reopen_lsn
                && entry.status == TriggerAuditStatus::Fired),
        "post-reopen firing audit must point to the local post-reopen commit LSN; got {post_reopen_history:?}"
    );
    reopened.close().unwrap();
}

#[test]
fn t3_sync_originator_fires_receiver_does_not_refire() {
    let origin = Database::open_memory();
    let receiver = Database::open_memory();
    setup_host_tables(&origin);
    let origin_schema = origin.changes_since(Lsn(0));
    receiver
        .apply_changes(
            origin_schema,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert!(
        receiver
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "host_write_trigger" && trigger.table == "host_writes"),
        "receiver must learn trigger declarations from sync DDL before registering callbacks"
    );
    let receiver_pre_ready = receiver.execute(
        host_insert_sql(),
        &insert_host_params(uuid(39), "receiver-pre-ready"),
    );
    assert!(
        matches!(
            receiver_pre_ready,
            Err(Error::EngineNotInitialized { ref operation })
                if operation.contains("host_writes")
        ),
        "synced trigger DDL must put the receiver into fail-closed booting state; got {receiver_pre_ready:?}"
    );

    let origin_fires = Arc::new(AtomicUsize::new(0));
    let receiver_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&origin, origin_fires.clone());
    register_audit_callback(&receiver, receiver_fires.clone());
    origin.complete_initialization().unwrap();
    receiver.complete_initialization().unwrap();

    let receiver_since = receiver.current_lsn();
    receiver
        .execute(
            host_insert_sql(),
            &insert_host_params(uuid(41), "receiver-local"),
        )
        .unwrap();
    assert_eq!(
        receiver_fires.load(Ordering::SeqCst),
        1,
        "receiver local writes must still fire receiver callbacks"
    );
    let receiver_fires_before_sync = receiver_fires.load(Ordering::SeqCst);
    let receiver_local_history = receiver.changes_since(receiver_since);
    let since = origin.current_lsn();
    origin
        .execute(host_insert_sql(), &insert_host_params(uuid(40), "sync"))
        .unwrap();
    receiver
        .apply_changes(
            origin.changes_since(since),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    assert_eq!(
        origin_fires.load(Ordering::SeqCst),
        1,
        "originator must fire callback for local write"
    );
    assert_eq!(
        receiver_fires.load(Ordering::SeqCst),
        receiver_fires_before_sync,
        "SyncPull must apply originator cascades as data and must not re-fire callbacks"
    );
    assert_eq!(count_rows(&receiver, "host_writes"), 2);
    assert_eq!(count_rows(&receiver, "host_audits"), 2);
    assert_eq!(
        receiver
            .edge_count(uuid(40), "AUDITED_BY", receiver.snapshot())
            .unwrap(),
        1,
        "originator graph cascade must sync as data"
    );
    let audit_row_id = row_id_for_column_uuid(&receiver, "host_audits", "write_id", uuid(40))
        .expect("synced audit row must be visible");
    assert!(
        receiver
            .live_vector_entry(audit_row_id, receiver.snapshot())
            .is_some(),
        "originator vector cascade must sync as data"
    );
    assert_eq!(
        receiver
            .edge_count(uuid(41), "AUDITED_BY", receiver.snapshot())
            .unwrap(),
        1,
        "receiver local positive-control cascade must remain visible"
    );

    let origin_fires_before_reverse_sync = origin_fires.load(Ordering::SeqCst);
    origin
        .apply_changes(
            receiver_local_history,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(
        origin_fires.load(Ordering::SeqCst),
        origin_fires_before_reverse_sync,
        "reverse SyncPull must apply receiver-originated cascades as data and must not re-fire origin callbacks"
    );
    assert_eq!(count_rows(&origin, "host_writes"), 2);
    assert_eq!(count_rows(&origin, "host_audits"), 2);
    assert_eq!(
        origin
            .edge_count(uuid(41), "AUDITED_BY", origin.snapshot())
            .unwrap(),
        1,
        "receiver-originated graph cascade must sync back to origin as data"
    );
    let reverse_audit_row_id = row_id_for_column_uuid(&origin, "host_audits", "write_id", uuid(41))
        .expect("reverse-synced audit row must be visible");
    assert!(
        origin
            .live_vector_entry(reverse_audit_row_id, origin.snapshot())
            .is_some(),
        "receiver-originated vector cascade must sync back to origin as data"
    );
}

#[tokio::test]
async fn t3_sync_client_push_pull_carries_trigger_ddl_and_drop() {
    let nats = start_nats().await;
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let tenant_id = "t3-trigger-ddl-sync";

    let db_tmp = TempDir::new().unwrap();
    let server_path = db_tmp.path().join("server.redb");
    let edge_a_path = db_tmp.path().join("edge_a.redb");
    let edge_b_path = db_tmp.path().join("edge_b.redb");
    let server_db = Arc::new(Database::open(&server_path).unwrap());
    let edge_a_db = Arc::new(Database::open(&edge_a_path).unwrap());
    let mut edge_b_db = Arc::new(Database::open(&edge_b_path).unwrap());
    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        tenant_id,
        policies.clone(),
    ));
    let server_task = server.clone();
    tokio::spawn(async move { server_task.run().await });
    assert!(
        wait_for_sync_server_ready(&nats.nats_url, tenant_id, Duration::from_secs(5)).await,
        "sync server must be ready before trigger DDL push/pull assertions"
    );

    setup_host_tables(&edge_a_db);
    let edge_a_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&edge_a_db, edge_a_fires.clone());
    edge_a_db.complete_initialization().unwrap();

    let edge_a = SyncClient::new(edge_a_db.clone(), &nats.nats_url, tenant_id);
    let mut edge_b = SyncClient::new(edge_b_db.clone(), &nats.nats_url, tenant_id);
    let create_push = edge_a.push().await;
    let create_pull = edge_b.pull(&policies).await;
    let server_trigger_after_create = server_db.list_triggers();
    let edge_b_trigger_after_create = edge_b_db.list_triggers();
    let edge_b_missing_callback = edge_b_db.complete_initialization();
    assert!(
        create_push.is_ok()
            && create_pull.is_ok()
            && server_trigger_after_create
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger"
                    && trigger.table == "host_writes"
                    && trigger.on_events == vec![TriggerEvent::Insert])
            && edge_b_trigger_after_create
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger"
                    && trigger.table == "host_writes"
                    && trigger.on_events == vec![TriggerEvent::Insert])
            && matches!(
                edge_b_missing_callback,
                Err(Error::TriggerCallbackMissing { ref trigger_name })
                    if trigger_name == "host_write_trigger"
            ),
        "real SyncClient push/pull must carry CREATE TRIGGER DDL and put fresh receivers into cold gate; push={create_push:?}, pull={create_pull:?}, server_triggers={server_trigger_after_create:?}, edge_b_triggers={edge_b_trigger_after_create:?}, init={edge_b_missing_callback:?}"
    );
    edge_b_db.close().unwrap();
    let reopened_edge_b_after_create = Database::open(&edge_b_path).unwrap();
    let reopened_edge_b_create_triggers = reopened_edge_b_after_create.list_triggers();
    let reopened_edge_b_create_gate = reopened_edge_b_after_create.complete_initialization();
    assert!(
        reopened_edge_b_create_triggers.iter().any(|trigger| {
            trigger.name == "host_write_trigger"
                && trigger.table == "host_writes"
                && trigger.on_events == vec![TriggerEvent::Insert]
        }) && matches!(
            reopened_edge_b_create_gate,
            Err(Error::TriggerCallbackMissing { ref trigger_name })
                if trigger_name == "host_write_trigger"
        ),
        "SyncClient-pulled CREATE TRIGGER must persist across receiver reopen and re-enter callback cold gate; triggers={reopened_edge_b_create_triggers:?}, ready={reopened_edge_b_create_gate:?}"
    );
    reopened_edge_b_after_create.close().unwrap();
    edge_b_db = Arc::new(Database::open(&edge_b_path).unwrap());
    edge_b = SyncClient::new(edge_b_db.clone(), &nats.nats_url, tenant_id);

    let server_fires = Arc::new(AtomicUsize::new(0));
    let edge_b_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&server_db, server_fires.clone());
    register_audit_callback(&edge_b_db, edge_b_fires.clone());
    server_db.complete_initialization().unwrap();
    edge_b_db.complete_initialization().unwrap();

    let data_since = edge_a_db.current_lsn();
    let expected_ids = (0..260)
        .map(|idx| uuid(0xD00_u128 + idx as u128))
        .collect::<Vec<_>>();
    for (idx, id) in expected_ids.iter().copied().enumerate() {
        edge_a_db
            .execute(
                host_insert_sql(),
                &insert_host_params(id, &format!("sync-pull-{idx}")),
            )
            .unwrap();
    }
    assert_eq!(
        edge_a_fires.load(Ordering::SeqCst),
        expected_ids.len(),
        "origin edge must generate one local cascade per host write before SyncClient push"
    );
    let edge_a_groups = row_group_signatures(&edge_a_db, edge_a_db.changes_since(data_since));
    assert_eq!(
        edge_a_groups.len(),
        expected_ids.len(),
        "SyncClient mixed-history fixture must produce one source group per local trigger fire"
    );
    let server_data_since = server_db.current_lsn();
    let data_push = edge_a.push().await;
    let server_groups =
        row_group_signatures(&server_db, server_db.changes_since(server_data_since));
    assert!(
        data_push.is_ok()
            && server_fires.load(Ordering::SeqCst) == 0
            && server_groups == edge_a_groups
            && count_rows(&server_db, "host_writes") == expected_ids.len()
            && count_rows(&server_db, "host_audits") == expected_ids.len(),
        "SyncClient push must carry mixed originator trigger history to the server as data without server re-fire; push={data_push:?}, server_fires={}, server_groups={}, expected_groups={}, server_counts=({}, {})",
        server_fires.load(Ordering::SeqCst),
        server_groups.len(),
        edge_a_groups.len(),
        count_rows(&server_db, "host_writes"),
        count_rows(&server_db, "host_audits")
    );

    let edge_b_data_since = edge_b_db.current_lsn();
    let edge_b_rx = edge_b_db.subscribe();
    let data_pull = edge_b.pull(&policies).await;
    let edge_b_groups =
        row_group_signatures(&edge_b_db, edge_b_db.changes_since(edge_b_data_since));
    let mut edge_b_events = Vec::new();
    for _ in 0..server_groups.len() + 1 {
        match edge_b_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(event) => edge_b_events.push(event),
            Err(_) => break,
        }
    }
    let edge_b_edge_count = expected_ids
        .iter()
        .copied()
        .map(|id| {
            edge_b_db
                .edge_count(id, "AUDITED_BY", edge_b_db.snapshot())
                .unwrap()
        })
        .sum::<usize>();
    let edge_b_vector_count = edge_b_db
        .scan("host_audits", edge_b_db.snapshot())
        .unwrap()
        .iter()
        .filter(|row| {
            edge_b_db
                .live_vector_entry(row.row_id, edge_b_db.snapshot())
                .is_some()
        })
        .count();
    assert!(
        data_pull.is_ok()
            && edge_b_fires.load(Ordering::SeqCst) == 0
            && edge_b_groups == server_groups
            && edge_b_events.len() == server_groups.len()
            && edge_b_events.iter().all(|event| event.row_count == 4)
            && count_rows(&edge_b_db, "host_writes") == expected_ids.len()
            && count_rows(&edge_b_db, "host_audits") == expected_ids.len()
            && edge_b_edge_count == expected_ids.len()
            && edge_b_vector_count == expected_ids.len(),
        "real SyncClient fresh pull must preserve mixed row/edge/vector history across server pull paging without receiver re-fire; pull={data_pull:?}, receiver_fires={}, groups={} expected={}, events={}, counts=({}, {}, edges {edge_b_edge_count}, vectors {edge_b_vector_count})",
        edge_b_fires.load(Ordering::SeqCst),
        edge_b_groups.len(),
        server_groups.len(),
        edge_b_events.len(),
        count_rows(&edge_b_db, "host_writes"),
        count_rows(&edge_b_db, "host_audits")
    );

    let server_history_before_noop = fired_trigger_history(&server_db, "host_write_trigger");
    let server_noop_since = server_db.current_lsn();
    let server_noop_rx = server_db.subscribe();
    let data_push_noop = edge_a.push().await;
    let server_noop_changes = server_db.changes_since(server_noop_since);
    let server_noop_event = server_noop_rx.recv_timeout(Duration::from_millis(300));
    assert!(
        data_push_noop.is_ok()
            && !changeset_has_data(&server_noop_changes)
            && server_noop_event.is_err()
            && server_fires.load(Ordering::SeqCst) == 0
            && fired_trigger_history(&server_db, "host_write_trigger")
                == server_history_before_noop
            && count_rows(&server_db, "host_writes") == expected_ids.len()
            && count_rows(&server_db, "host_audits") == expected_ids.len()
            && audited_edge_count(&server_db, &expected_ids) == expected_ids.len()
            && live_vector_count(&server_db, "host_audits") == expected_ids.len(),
        "repeating SyncClient push after data is already applied must be a no-op: no events, audit entries, rows, edges, vectors, or DDL; push={data_push_noop:?}, changes={server_noop_changes:?}, event={server_noop_event:?}, fires={}, history_before={server_history_before_noop:?}, history_after={:?}, counts=({}, {}, edges {}, vectors {})",
        server_fires.load(Ordering::SeqCst),
        fired_trigger_history(&server_db, "host_write_trigger"),
        count_rows(&server_db, "host_writes"),
        count_rows(&server_db, "host_audits"),
        audited_edge_count(&server_db, &expected_ids),
        live_vector_count(&server_db, "host_audits")
    );

    let edge_b_history_before_noop = fired_trigger_history(&edge_b_db, "host_write_trigger");
    let edge_b_ddl_before_noop = format!("{:?}", edge_b_db.changes_since(Lsn(0)).ddl);
    let edge_b_noop_since = edge_b_db.current_lsn();
    let edge_b_noop_rx = edge_b_db.subscribe();
    let data_pull_noop = edge_b.pull(&policies).await;
    let edge_b_noop_changes = edge_b_db.changes_since(edge_b_noop_since);
    let edge_b_noop_event = edge_b_noop_rx.recv_timeout(Duration::from_millis(300));
    assert!(
        data_pull_noop.is_ok()
            && !changeset_has_data(&edge_b_noop_changes)
            && edge_b_noop_event.is_err()
            && edge_b_fires.load(Ordering::SeqCst) == 0
            && fired_trigger_history(&edge_b_db, "host_write_trigger")
                == edge_b_history_before_noop
            && format!("{:?}", edge_b_db.changes_since(Lsn(0)).ddl) == edge_b_ddl_before_noop
            && count_rows(&edge_b_db, "host_writes") == expected_ids.len()
            && count_rows(&edge_b_db, "host_audits") == expected_ids.len()
            && audited_edge_count(&edge_b_db, &expected_ids) == expected_ids.len()
            && live_vector_count(&edge_b_db, "host_audits") == expected_ids.len(),
        "repeating SyncClient pull after data is already applied must be a no-op with no receiver re-fire or duplicate history; pull={data_pull_noop:?}, changes={edge_b_noop_changes:?}, event={edge_b_noop_event:?}, fires={}, ddl_before={edge_b_ddl_before_noop:?}, ddl_after={:?}, history_before={edge_b_history_before_noop:?}, history_after={:?}, counts=({}, {}, edges {}, vectors {})",
        edge_b_fires.load(Ordering::SeqCst),
        edge_b_db.changes_since(Lsn(0)).ddl,
        fired_trigger_history(&edge_b_db, "host_write_trigger"),
        count_rows(&edge_b_db, "host_writes"),
        count_rows(&edge_b_db, "host_audits"),
        audited_edge_count(&edge_b_db, &expected_ids),
        live_vector_count(&edge_b_db, "host_audits")
    );

    let drop = edge_a_db.execute("DROP TRIGGER host_write_trigger", &empty());
    let drop_push = edge_a.push().await;
    let drop_pull = edge_b.pull(&policies).await;
    let edge_b_ready_after_drop = edge_b_db.complete_initialization();
    let edge_b_ddl_history = edge_b_db.changes_since(Lsn(0)).ddl;
    let edge_b_tables = edge_b_ddl_history
        .iter()
        .filter_map(|change| match change {
            DdlChange::CreateTable { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let edge_b_create_triggers = edge_b_ddl_history
        .iter()
        .filter(|change| {
            matches!(
                change,
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                } if name == "host_write_trigger"
                    && table == "host_writes"
                    && on_events == &vec!["INSERT".to_string()]
            )
        })
        .count();
    let edge_b_drop_triggers = edge_b_ddl_history
        .iter()
        .filter(|change| {
            matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger")
        })
        .count();
    assert!(
        drop.is_ok()
            && drop_push.is_ok()
            && drop_pull.is_ok()
            && server_db.list_triggers().is_empty()
            && edge_b_db.list_triggers().is_empty()
            && edge_b_ready_after_drop.is_ok()
            && edge_b_tables
                == BTreeSet::from(["host_writes".to_string(), "host_audits".to_string()])
            && edge_b_create_triggers == 1
            && edge_b_drop_triggers == 1,
        "real SyncClient push/pull must carry DROP TRIGGER DDL, detach trigger state, and preserve durable DDL history; drop={drop:?}, push={drop_push:?}, pull={drop_pull:?}, server_triggers={:?}, edge_b_triggers={:?}, ready={edge_b_ready_after_drop:?}, ddl={edge_b_ddl_history:?}",
        server_db.list_triggers(),
        edge_b_db.list_triggers()
    );

    let server_drop_noop_since = server_db.current_lsn();
    let server_drop_noop_rx = server_db.subscribe();
    let edge_b_drop_ddl_before_noop = format!("{:?}", edge_b_db.changes_since(Lsn(0)).ddl);
    let edge_b_drop_history_before_noop = fired_trigger_history(&edge_b_db, "host_write_trigger");
    let edge_b_drop_noop_since = edge_b_db.current_lsn();
    let edge_b_drop_noop_rx = edge_b_db.subscribe();
    let drop_push_noop = edge_a.push().await;
    let drop_pull_noop = edge_b.pull(&policies).await;
    let server_drop_noop_changes = server_db.changes_since(server_drop_noop_since);
    let edge_b_drop_noop_changes = edge_b_db.changes_since(edge_b_drop_noop_since);
    let server_drop_noop_event = server_drop_noop_rx.recv_timeout(Duration::from_millis(300));
    let edge_b_drop_noop_event = edge_b_drop_noop_rx.recv_timeout(Duration::from_millis(300));
    let edge_b_drop_ddl_after_noop = format!("{:?}", edge_b_db.changes_since(Lsn(0)).ddl);
    assert!(
        drop_push_noop.is_ok()
            && drop_pull_noop.is_ok()
            && !changeset_has_data(&server_drop_noop_changes)
            && !changeset_has_data(&edge_b_drop_noop_changes)
            && server_drop_noop_event.is_err()
            && edge_b_drop_noop_event.is_err()
            && server_db.list_triggers().is_empty()
            && edge_b_db.list_triggers().is_empty()
            && edge_b_drop_ddl_after_noop == edge_b_drop_ddl_before_noop
            && fired_trigger_history(&edge_b_db, "host_write_trigger")
                == edge_b_drop_history_before_noop
            && count_rows(&edge_b_db, "host_writes") == expected_ids.len()
            && count_rows(&edge_b_db, "host_audits") == expected_ids.len()
            && audited_edge_count(&edge_b_db, &expected_ids) == expected_ids.len()
            && live_vector_count(&edge_b_db, "host_audits") == expected_ids.len(),
        "repeating SyncClient push/pull after trigger tombstone is already applied must be idempotent: no duplicate tombstone, events, audit entries, rows, edges, or vectors; push={drop_push_noop:?}, pull={drop_pull_noop:?}, server_changes={server_drop_noop_changes:?}, edge_changes={edge_b_drop_noop_changes:?}, server_event={server_drop_noop_event:?}, edge_event={edge_b_drop_noop_event:?}, ddl_before={edge_b_drop_ddl_before_noop:?}, ddl_after={edge_b_drop_ddl_after_noop:?}, history_before={edge_b_drop_history_before_noop:?}, history_after={:?}, counts=({}, {}, edges {}, vectors {})",
        fired_trigger_history(&edge_b_db, "host_write_trigger"),
        count_rows(&edge_b_db, "host_writes"),
        count_rows(&edge_b_db, "host_audits"),
        audited_edge_count(&edge_b_db, &expected_ids),
        live_vector_count(&edge_b_db, "host_audits")
    );
    edge_b_db.close().unwrap();
    let reopened_edge_b = Database::open(&edge_b_path).unwrap();
    let reopened_edge_b_ready = reopened_edge_b.complete_initialization();
    let reopened_edge_b_ddl = reopened_edge_b.changes_since(Lsn(0)).ddl;
    let reopened_edge_b_create_triggers = reopened_edge_b_ddl
        .iter()
        .filter(|change| {
            matches!(
                change,
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                } if name == "host_write_trigger"
                    && table == "host_writes"
                    && on_events == &vec!["INSERT".to_string()]
            )
        })
        .count();
    let reopened_edge_b_drop_triggers = reopened_edge_b_ddl
        .iter()
        .filter(|change| matches!(change, DdlChange::DropTrigger { name } if name == "host_write_trigger"))
        .count();
    let reopened_edge_b_vector_count = reopened_edge_b
        .scan("host_audits", reopened_edge_b.snapshot())
        .unwrap()
        .iter()
        .filter(|row| {
            reopened_edge_b
                .live_vector_entry(row.row_id, reopened_edge_b.snapshot())
                .is_some()
        })
        .count();
    let reopened_edge_b_edge_count = expected_ids
        .iter()
        .copied()
        .map(|id| {
            reopened_edge_b
                .edge_count(id, "AUDITED_BY", reopened_edge_b.snapshot())
                .unwrap()
        })
        .sum::<usize>();
    let reopened_edge_b_edge_props_count = expected_ids
        .iter()
        .copied()
        .filter(|id| {
            let Some(audit_row) =
                row_for_column_uuid(&reopened_edge_b, "host_audits", "write_id", *id)
            else {
                return false;
            };
            let Some(audit_id) = audit_row.values.get("id").and_then(Value::as_uuid).copied()
            else {
                return false;
            };
            reopened_edge_b
                .get_edge_properties(*id, audit_id, "AUDITED_BY", reopened_edge_b.snapshot())
                .unwrap_or_default()
                .and_then(|props| props.get("trigger_tx").cloned())
                .is_some()
        })
        .count();
    assert!(
        reopened_edge_b_ready.is_ok()
            && reopened_edge_b.list_triggers().is_empty()
            && reopened_edge_b_create_triggers == 1
            && reopened_edge_b_drop_triggers == 1
            && count_rows(&reopened_edge_b, "host_writes") == expected_ids.len()
            && count_rows(&reopened_edge_b, "host_audits") == expected_ids.len()
            && reopened_edge_b_vector_count == expected_ids.len()
            && reopened_edge_b_edge_count == expected_ids.len()
            && reopened_edge_b_edge_props_count == expected_ids.len(),
        "SyncClient-pulled trigger DDL tombstone and mixed row/edge/vector history must survive receiver reopen; ready={reopened_edge_b_ready:?}, triggers={:?}, ddl={reopened_edge_b_ddl:?}, counts=({}, {}, edges {reopened_edge_b_edge_count}, edge_props {reopened_edge_b_edge_props_count}, vectors {reopened_edge_b_vector_count})",
        reopened_edge_b.list_triggers(),
        count_rows(&reopened_edge_b, "host_writes"),
        count_rows(&reopened_edge_b, "host_audits")
    );
    reopened_edge_b.close().unwrap();
}

#[test]
fn t3_d7_wire_split_preserves_sender_lsn_under_byte_pressure() {
    let trigger_ddl = vec![
        DdlChange::CreateTrigger {
            name: "wire_trigger".into(),
            table: "host_writes".into(),
            on_events: vec!["INSERT".into(), "UPDATE".into()],
        },
        DdlChange::DropTrigger {
            name: "wire_trigger".into(),
        },
    ];
    let wire_changes = ChangeSet {
        ddl: trigger_ddl.clone(),
        ddl_lsn: vec![Lsn(5), Lsn(9)],
        ..Default::default()
    };
    let encoded = rmp_serde::to_vec(&WireChangeSet::from(wire_changes))
        .expect("trigger DDL WireChangeSet must encode");
    let decoded_wire: WireChangeSet =
        rmp_serde::from_slice(&encoded).expect("trigger DDL WireChangeSet must decode");
    let decoded_changes =
        ChangeSet::try_from(decoded_wire).expect("valid trigger DDL WireChangeSet must convert");
    assert!(
        matches!(
            decoded_changes.ddl.as_slice(),
            [
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                },
                DdlChange::DropTrigger { name: drop_name },
            ] if name == "wire_trigger"
                && table == "host_writes"
                && on_events == &vec!["INSERT".to_string(), "UPDATE".to_string()]
                && drop_name == "wire_trigger"
        ),
        "trigger CreateTrigger/DropTrigger DDL must survive the actual sync wire round-trip"
    );
    assert_eq!(
        decoded_changes.ddl_lsn,
        vec![Lsn(5), Lsn(9)],
        "trigger DDL LSNs must survive the actual sync wire round-trip"
    );

    let mut rows = Vec::new();
    let mut edges = Vec::new();
    let mut vectors = Vec::new();
    for (lsn, count, offset) in [(Lsn(7), 8, 0x700_u128), (Lsn(8), 5, 0x800_u128)] {
        for i in 0..count {
            let id = uuid(offset + i as u128);
            let audit_id = uuid(offset + 0x10_000 + i as u128);
            rows.push(row_change(
                "host_writes",
                id,
                &format!("{}-{lsn}-{i}", "x".repeat(200 * 1024)),
                lsn,
            ));
            edges.push(contextdb_engine::sync_types::EdgeChange {
                source: id,
                target: audit_id,
                edge_type: "AUDITED_BY".into(),
                properties: HashMap::from([("sender_lsn".into(), Value::Int64(lsn.0 as i64))]),
                lsn,
            });
            vectors.push(contextdb_engine::sync_types::VectorChange {
                index: VectorIndexRef::new("host_audits", "embedding"),
                row_id: RowId::from_raw_wire(offset as u64 + i as u64),
                vector: vec![lsn.0 as f32, i as f32, 1.0],
                lsn,
            });
        }
    }
    let batches = contextdb_server::split_changeset_for_test(ChangeSet {
        rows,
        edges,
        vectors,
        ..Default::default()
    });
    let batch_lsn_counts: Vec<BTreeMap<Lsn, [usize; 3]>> = batches
        .iter()
        .map(|batch| {
            let mut counts = BTreeMap::new();
            for row in &batch.rows {
                counts.entry(row.lsn).or_insert([0, 0, 0])[0] += 1;
            }
            for edge in &batch.edges {
                counts.entry(edge.lsn).or_insert([0, 0, 0])[1] += 1;
            }
            for vector in &batch.vectors {
                counts.entry(vector.lsn).or_insert([0, 0, 0])[2] += 1;
            }
            counts
        })
        .collect();
    let containing_lsn_7 = batch_lsn_counts
        .iter()
        .filter(|counts| counts.contains_key(&Lsn(7)))
        .count();
    let containing_lsn_8 = batch_lsn_counts
        .iter()
        .filter(|counts| counts.contains_key(&Lsn(8)))
        .count();
    assert_eq!(
        containing_lsn_7, 1,
        "wire batch splitter must never tear sender LSN 7; batch_lsn_counts={batch_lsn_counts:?}"
    );
    assert_eq!(
        containing_lsn_8, 1,
        "wire batch splitter must never tear sender LSN 8; batch_lsn_counts={batch_lsn_counts:?}"
    );
    let expected = vec![
        BTreeMap::from([(Lsn(7), [8usize, 8usize, 8usize])]),
        BTreeMap::from([(Lsn(8), [5usize, 5usize, 5usize])]),
    ];
    assert_eq!(
        batch_lsn_counts, expected,
        "byte-pressure splitter must split only on complete sender-LSN boundaries across rows, graph edges, and vectors"
    );
}

#[test]
fn t3_d8_apply_changes_groups_rows_by_sender_lsn() {
    let origin = Database::open_memory();
    let receiver = Database::open_memory();
    setup_host_data_tables(&origin);
    setup_host_data_tables(&receiver);
    let rx = receiver.subscribe();

    let origin_since = origin.current_lsn();
    let sender_groups = [3usize, 129usize, 257usize];
    for (group_idx, count) in sender_groups.iter().copied().enumerate() {
        let tx = origin.begin();
        for n in 0..count {
            let write_id = uuid(0x900_u128 + group_idx as u128 * 10_000 + n as u128);
            let audit_id = uuid(0xA00_u128 + group_idx as u128 * 10_000 + n as u128);
            let content = format!("sender-group-{group_idx}-{n}");
            let vector = vec![group_idx as f32, n as f32, 1.0];
            origin
                .insert_row(
                    tx,
                    "host_writes",
                    HashMap::from([
                        ("id".to_string(), Value::Uuid(write_id)),
                        ("content".to_string(), Value::Text(content.clone())),
                    ]),
                )
                .expect("origin host row insert");
            let audit_row_id = origin
                .insert_row(
                    tx,
                    "host_audits",
                    HashMap::from([
                        ("id".to_string(), Value::Uuid(audit_id)),
                        ("write_id".to_string(), Value::Uuid(write_id)),
                        (
                            "note".to_string(),
                            Value::Text(format!("cascade:{content}")),
                        ),
                        ("embedding".to_string(), Value::Vector(vector.clone())),
                    ]),
                )
                .expect("origin audit row insert");
            origin
                .insert_edge(
                    tx,
                    write_id,
                    audit_id,
                    "AUDITED_BY".into(),
                    HashMap::from([(
                        "sender_group".to_string(),
                        Value::Text(format!("group-{group_idx}")),
                    )]),
                )
                .expect("origin edge insert");
            origin
                .insert_vector(
                    tx,
                    VectorIndexRef::new("host_audits", "embedding"),
                    audit_row_id,
                    vector,
                )
                .expect("origin vector insert");
        }
        origin.commit(tx).expect("origin sender group commit");
    }

    let origin_changes = origin.changes_since(origin_since);
    let expected_groups = row_group_signatures(&origin, origin_changes.clone());
    assert_eq!(
        expected_groups.len(),
        sender_groups.len(),
        "origin setup must create one mixed row/edge/vector sender group per source tx; groups={expected_groups:?}"
    );
    assert_eq!(
        (
            origin_changes.rows.len(),
            origin_changes.edges.len(),
            origin_changes.vectors.len()
        ),
        (
            sender_groups.iter().sum::<usize>() * 2,
            sender_groups.iter().sum::<usize>(),
            sender_groups.iter().sum::<usize>()
        ),
        "D8 fixture must exercise mixed relational, graph, and vector changes"
    );

    receiver
        .apply_changes(
            origin_changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let mut events = Vec::new();
    for _ in 0..sender_groups.len() + 1 {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => events.push(event),
            Err(_) => break,
        }
    }
    let receiver_groups = row_group_signatures(&receiver, receiver.changes_since(Lsn(0)));

    assert_eq!(
        events.len(),
        sender_groups.len(),
        "apply_changes must commit once per sender LSN group; events={events:?}"
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.row_count)
            .collect::<Vec<_>>(),
        sender_groups
            .iter()
            .map(|count| count * 4)
            .collect::<Vec<_>>(),
        "each receiver commit must preserve one mixed sender LSN group, including groups above the existing 128-row fast path"
    );
    assert_eq!(
        receiver_groups, expected_groups,
        "receiver changelog must preserve exact sender-LSN row, graph, and vector identity, not byte or row-count chunks"
    );
    let mut cumulative_count = 0usize;
    for (group_index, (event, group_count)) in
        events.iter().zip(sender_groups.iter().copied()).enumerate()
    {
        cumulative_count += group_count;
        let snapshot = receiver.snapshot_at(event.lsn);
        let host_rows = receiver.scan("host_writes", snapshot).unwrap();
        let audit_rows = receiver.scan("host_audits", snapshot).unwrap();
        let edge_count = host_rows
            .iter()
            .filter_map(|row| row.values.get("id").and_then(Value::as_uuid).copied())
            .map(|id| receiver.edge_count(id, "AUDITED_BY", snapshot).unwrap())
            .sum::<usize>();
        let vector_count = audit_rows
            .iter()
            .filter(|row| receiver.live_vector_entry(row.row_id, snapshot).is_some())
            .count();
        let snapshot_rows = ["host_writes", "host_audits"]
            .into_iter()
            .flat_map(|table| {
                receiver
                    .scan(table, snapshot)
                    .unwrap()
                    .into_iter()
                    .filter_map(move |row| {
                        let Some(Value::Uuid(id)) = row.values.get("id") else {
                            return None;
                        };
                        let detail = value_map_signature(&row.values);
                        Some(format!("row:{table}:{id}:{detail:?}"))
                    })
            })
            .collect::<BTreeSet<_>>();
        let expected_rows = expected_groups
            .iter()
            .take(group_index + 1)
            .flat_map(|group| {
                group
                    .iter()
                    .filter(|entry| entry.starts_with("row:"))
                    .cloned()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            (snapshot_rows, edge_count, vector_count),
            (expected_rows, cumulative_count, cumulative_count),
            "snapshot_at({}) must expose exactly the mixed sender groups committed through that receiver LSN",
            event.lsn
        );
    }
}

#[tokio::test]
async fn t3_sync_client_fresh_pull_bootstraps_trigger_ddl_before_history_data() {
    let nats = start_nats().await;
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let tenant_id = "t3-trigger-bootstrap-pull";

    let db_tmp = TempDir::new().unwrap();
    let server_path = db_tmp.path().join("server.redb");
    let fresh_path = db_tmp.path().join("fresh.redb");
    let server_seed = Database::open(&server_path).unwrap();
    let fresh_db = Arc::new(Database::open(&fresh_path).unwrap());

    setup_host_tables(&server_seed);
    let server_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&server_seed, server_fires.clone());
    server_seed.complete_initialization().unwrap();

    let expected_ids = [uuid(0xB001), uuid(0xB002), uuid(0xB003)];
    for (idx, id) in expected_ids.iter().copied().enumerate() {
        server_seed
            .execute(
                host_insert_sql(),
                &insert_host_params(id, &format!("bootstrap-history-{idx}")),
            )
            .unwrap();
    }
    assert_eq!(server_fires.load(Ordering::SeqCst), expected_ids.len());
    let expected_groups = row_group_signatures(&server_seed, server_seed.changes_since(Lsn(0)));
    server_seed.close().unwrap();
    let server_db = Arc::new(Database::open(&server_path).unwrap());

    let server = Arc::new(SyncServer::new(
        server_db.clone(),
        &nats.nats_url,
        tenant_id,
        policies.clone(),
    ));
    let server_task = server.clone();
    tokio::spawn(async move { server_task.run().await });
    assert!(
        wait_for_sync_server_ready(&nats.nats_url, tenant_id, Duration::from_secs(5)).await,
        "sync server must be ready before fresh trigger bootstrap pull"
    );

    let fresh = SyncClient::new(fresh_db.clone(), &nats.nats_url, tenant_id);
    let first_pull = fresh.pull(&policies).await;
    let missing_callback = fresh_db.complete_initialization();
    assert!(
        first_pull.is_ok()
            && fresh_db
                .list_triggers()
                .iter()
                .any(|trigger| trigger.name == "host_write_trigger"
                    && trigger.table == "host_writes")
            && matches!(
                missing_callback,
                Err(Error::TriggerCallbackMissing { ref trigger_name })
                    if trigger_name == "host_write_trigger"
            )
            && count_rows(&fresh_db, "host_writes") == 0
            && count_rows(&fresh_db, "host_audits") == 0,
        "fresh full-history pull must stop after trigger DDL so callbacks can be registered before data replay; first_pull={first_pull:?}, init={missing_callback:?}, triggers={:?}, counts=({}, {})",
        fresh_db.list_triggers(),
        count_rows(&fresh_db, "host_writes"),
        count_rows(&fresh_db, "host_audits")
    );

    let fresh_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&fresh_db, fresh_fires.clone());
    fresh_db.complete_initialization().unwrap();
    let fresh_since = fresh_db.current_lsn();
    let second_pull = fresh.pull(&policies).await;

    assert!(
        second_pull.is_ok()
            && fresh_fires.load(Ordering::SeqCst) == 0
            && count_rows(&fresh_db, "host_writes") == expected_ids.len()
            && count_rows(&fresh_db, "host_audits") == expected_ids.len()
            && row_group_signatures(&fresh_db, fresh_db.changes_since(fresh_since))
                == expected_groups,
        "fresh full-history pull must replay data only after callback registration and must not re-fire receiver callbacks; second_pull={second_pull:?}, fires={}, groups={:?} expected={:?}, counts=({}, {})",
        fresh_fires.load(Ordering::SeqCst),
        row_group_signatures(&fresh_db, fresh_db.changes_since(fresh_since)),
        expected_groups,
        count_rows(&fresh_db, "host_writes"),
        count_rows(&fresh_db, "host_audits")
    );
}

#[test]
fn t3_freshdisk_pull_replays_server_history_atomic_and_no_refire() {
    let tmp = TempDir::new().unwrap();
    let server = Database::open(tmp.path().join("server.redb")).unwrap();
    setup_host_tables(&server);
    let schema_snapshot = server.changes_since(Lsn(0));
    let server_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&server, server_fires.clone());
    server.complete_initialization().unwrap();

    let since = server.current_lsn();
    let expected_ids = [uuid(90), uuid(91), uuid(92)];
    for id in expected_ids {
        server
            .execute(host_insert_sql(), &insert_host_params(id, "history"))
            .unwrap();
    }
    assert_eq!(server_fires.load(Ordering::SeqCst), 3);
    let server_history = server.changes_since(since);
    let expected_groups = row_group_signatures(&server, server_history.clone());
    assert_eq!(
        expected_groups.len(),
        3,
        "server history must contain one atomic group per firing tx; groups={expected_groups:?}"
    );

    let fresh = Database::open(tmp.path().join("fresh.redb")).unwrap();
    fresh
        .apply_changes(
            schema_snapshot,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert!(
        fresh
            .list_triggers()
            .iter()
            .any(|trigger| trigger.name == "host_write_trigger" && trigger.table == "host_writes"),
        "fresh-disk receiver must learn trigger declarations from the server DDL stream"
    );
    let missing_fresh_callback = fresh.complete_initialization();
    assert!(
        matches!(
            missing_fresh_callback,
            Err(Error::TriggerCallbackMissing { ref trigger_name })
                if trigger_name == "host_write_trigger"
        ),
        "fresh-disk receiver must fail closed after synced trigger DDL until callback registration; got {missing_fresh_callback:?}"
    );
    let fresh_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&fresh, fresh_fires.clone());
    fresh.complete_initialization().unwrap();
    let fresh_since = fresh.current_lsn();
    let rx = fresh.subscribe();
    fresh
        .apply_changes(
            server_history,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let audit_write_ids: HashSet<Uuid> = fresh
        .scan("host_audits", fresh.snapshot())
        .unwrap()
        .iter()
        .filter_map(|row| match row.values.get("write_id") {
            Some(Value::Uuid(id)) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        fresh_fires.load(Ordering::SeqCst),
        0,
        "fresh-disk pull must not re-fire receiver callbacks"
    );
    assert_eq!(count_rows(&fresh, "host_writes"), 3);
    assert_eq!(count_rows(&fresh, "host_audits"), 3);
    assert_eq!(audit_write_ids, expected_ids.into_iter().collect());
    let fresh_audit_rows = fresh.scan("host_audits", fresh.snapshot()).unwrap();
    for expected_id in expected_ids {
        let audit_row = fresh_audit_rows
            .iter()
            .find(|row| matches!(row.values.get("write_id"), Some(Value::Uuid(id)) if *id == expected_id))
            .expect("fresh audit row must be present for each source write");
        assert_eq!(
            audit_row.values.get("note"),
            Some(&Value::Text("cascade:history".into())),
            "fresh audit row for {expected_id} must preserve full non-key relational state"
        );
        assert_eq!(
            audit_row.values.get("embedding"),
            Some(&Value::Vector(vec![1.0, 0.0, 0.0])),
            "fresh audit row for {expected_id} must preserve vector column value as relational state"
        );
        assert!(
            fresh
                .live_vector_entry(audit_row.row_id, fresh.snapshot())
                .is_some(),
            "fresh audit row for {expected_id} must retain its own vector entry"
        );
    }
    let mut events = Vec::new();
    for _ in 0..4 {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => events.push(event),
            Err(_) => break,
        }
    }
    assert_eq!(
        events.len(),
        expected_groups.len(),
        "fresh-disk pull must replay one receiver commit per server sender LSN; events={events:?}"
    );
    let fired_history = fresh
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let receiver_event_lsns = events
        .iter()
        .map(|event| event.lsn)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        fired_history.len(),
        events.len(),
        "fresh-disk receiver must reconstruct trigger audit history for replayed originator firings; history={fired_history:?}"
    );
    for entry in &fired_history {
        assert_eq!(entry.trigger_name, "host_write_trigger");
        assert_eq!(entry.status, TriggerAuditStatus::Fired);
        assert!(
            receiver_event_lsns.contains(&entry.firing_lsn),
            "replayed firing audit LSN must point at a receiver replay commit; entry={entry:?}, events={events:?}"
        );
        assert_eq!(
            entry.firing_tx,
            TxId(fresh.snapshot_at(entry.firing_lsn).0),
            "replayed firing audit must remain tied to the receiver tx that materialized the sender group"
        );
        assert_eq!(entry.depth, 1);
        assert_eq!(
            entry.cascade_row_count, 3,
            "replayed audit must retain cascade cardinality for relational, graph, and vector side effects"
        );
    }
    assert_eq!(
        row_group_signatures(&fresh, fresh.changes_since(fresh_since)),
        expected_groups,
        "fresh-disk receiver history must match server atomic row/edge/vector groups by content"
    );
    server.close().unwrap();
    fresh.close().unwrap();
}

#[test]
fn t3_depth_self_cascade_cap_rolls_back_typed() {
    let tmp = TempDir::new().unwrap();
    let depth_path = tmp.path().join("depth_audit.redb");
    let db = Database::open(&depth_path).unwrap();
    db.execute(
        "CREATE TABLE host_writes (id UUID PRIMARY KEY, step INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
        &empty(),
    )
    .unwrap();
    db.register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
        let step = ctx
            .row_values
            .get("step")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        if step <= db_handle.trigger_cascade_depth_cap() as i64 {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            p.insert("step".into(), Value::Int64(step + 1));
            db_handle.execute("INSERT INTO host_writes (id, step) VALUES ($id, $step)", &p)?;
        }
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(uuid(100)));
    p.insert("step".into(), Value::Int64(0));
    let result = db.execute("INSERT INTO host_writes (id, step) VALUES ($id, $step)", &p);
    let audit = db.trigger_audit_log();

    assert_eq!(
        db.trigger_cascade_depth_cap(),
        16,
        "cascade depth cap must be explicit and stable"
    );
    assert!(
        matches!(
            result,
            Err(Error::TriggerCascadeDepthExceeded { ref trigger_name, depth })
                if trigger_name == "host_write_trigger" && depth == 17
        ),
        "self-cascade beyond cap must fail with typed rollback error; got {result:?}"
    );
    assert_eq!(
        count_rows(&db, "host_writes"),
        0,
        "depth-cap failure must roll back the firing row and all cascades"
    );
    let depth_audit = audit
        .iter()
        .find(|entry| matches!(entry.status, TriggerAuditStatus::DepthExceeded));
    assert!(
        matches!(
            depth_audit,
            Some(entry)
                if entry.trigger_name == "host_write_trigger"
                    && entry.depth == 17
                    && entry.firing_lsn == Lsn(0)
                    && entry.cascade_row_count == 0
        ),
        "depth-cap rollback must be audited with concrete trigger/depth fields; got {audit:?}"
    );
    let depth_history = db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::DepthExceeded),
        })
        .unwrap();
    assert_eq!(
        depth_history.len(),
        1,
        "depth-cap rollback must be available from durable filtered audit history"
    );
    assert!(
        matches!(
            depth_history.first(),
            Some(entry)
                if entry.trigger_name == "host_write_trigger"
                    && entry.depth == 17
                    && entry.firing_lsn == Lsn(0)
                    && entry.cascade_row_count == 0
        ),
        "durable depth audit must retain concrete trigger/depth fields; got {depth_history:?}"
    );
    db.close().unwrap();
    let reopened_depth = Database::open(&depth_path).unwrap();
    reopened_depth
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_depth.complete_initialization().unwrap();
    let reopened_depth_history = reopened_depth
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::DepthExceeded),
        })
        .unwrap();
    assert_eq!(
        reopened_depth_history, depth_history,
        "depth-exceeded audit history must survive reopen"
    );
    reopened_depth.close().unwrap();

    let cap_path = tmp.path().join("cap_audit.redb");
    let cap_db = Database::open(&cap_path).unwrap();
    cap_db
        .execute(
            "CREATE TABLE host_writes (id UUID PRIMARY KEY, step INTEGER)",
            &empty(),
        )
        .unwrap();
    cap_db
        .execute(
            "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
    let at_cap_depths = Arc::new(Mutex::new(Vec::new()));
    let callback_at_cap_depths = at_cap_depths.clone();
    cap_db
        .register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
            callback_at_cap_depths.lock().unwrap().push(ctx.depth);
            let step = ctx
                .row_values
                .get("step")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            if step < db_handle.trigger_cascade_depth_cap() as i64 {
                let mut p = HashMap::new();
                p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
                p.insert("step".into(), Value::Int64(step + 1));
                db_handle.execute("INSERT INTO host_writes (id, step) VALUES ($id, $step)", &p)?;
            }
            Ok(())
        })
        .unwrap();
    cap_db.complete_initialization().unwrap();
    assert_eq!(
        cap_db.trigger_cascade_depth_cap(),
        16,
        "cascade depth cap must also allow self-cascade through the configured limit"
    );
    let mut at_cap_params = HashMap::new();
    at_cap_params.insert("id".into(), Value::Uuid(uuid(101)));
    at_cap_params.insert("step".into(), Value::Int64(1));
    cap_db
        .execute(
            "INSERT INTO host_writes (id, step) VALUES ($id, $step)",
            &at_cap_params,
        )
        .expect("self-cascade through exactly the configured cap must commit");
    assert_eq!(
        count_rows(&cap_db, "host_writes"),
        cap_db.trigger_cascade_depth_cap() as usize,
        "self-cascade at the cap must commit every firing row"
    );
    let expected_at_cap_depths = (1..=cap_db.trigger_cascade_depth_cap()).collect::<Vec<_>>();
    assert_eq!(
        at_cap_depths.lock().unwrap().as_slice(),
        expected_at_cap_depths.as_slice(),
        "self-cascade at the cap must report monotonically nested callback depths"
    );
    let at_cap_history = cap_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let at_cap_ring = cap_db.trigger_audit_log();
    let at_cap_audit_depths = at_cap_history
        .iter()
        .map(|entry| entry.depth)
        .collect::<Vec<_>>();
    let at_cap_audit_lsns = at_cap_history
        .iter()
        .map(|entry| entry.firing_lsn)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        (
            at_cap_history.len(),
            at_cap_ring.len(),
            at_cap_audit_depths.as_slice(),
            at_cap_audit_lsns.len(),
        ),
        (
            cap_db.trigger_cascade_depth_cap() as usize,
            cap_db.trigger_cascade_depth_cap() as usize,
            expected_at_cap_depths.as_slice(),
            1,
        ),
        "successful self-cascade must write one audit entry per trigger fire, not one per outer transaction; history={at_cap_history:?}, ring={at_cap_ring:?}"
    );
    assert!(
        at_cap_history.iter().all(|entry| {
            entry.trigger_name == "host_write_trigger"
                && entry.status == TriggerAuditStatus::Fired
                && entry.firing_tx == TxId(cap_db.snapshot_at(entry.firing_lsn).0)
        }),
        "each at-cap fire audit must retain trigger name, fired status, and canonical tx/LSN; history={at_cap_history:?}"
    );
    cap_db.close().unwrap();
    let reopened_cap = Database::open(&cap_path).unwrap();
    reopened_cap
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_cap.complete_initialization().unwrap();
    let reopened_at_cap_history = reopened_cap
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let reopened_at_cap_depths = reopened_at_cap_history
        .iter()
        .map(|entry| entry.depth)
        .collect::<Vec<_>>();
    let reopened_at_cap_lsns = reopened_at_cap_history
        .iter()
        .map(|entry| entry.firing_lsn)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        (
            reopened_at_cap_history.len(),
            reopened_at_cap_depths.as_slice(),
            reopened_at_cap_lsns.len(),
        ),
        (
            expected_at_cap_depths.len(),
            expected_at_cap_depths.as_slice(),
            1,
        ),
        "successful self-cascade must persist one fired audit entry per nested trigger fire, not one per outer transaction; history={reopened_at_cap_history:?}"
    );
    reopened_cap.close().unwrap();

    let sibling_path = tmp.path().join("sibling_audit.redb");
    let sibling_db = Database::open(&sibling_path).unwrap();
    sibling_db
        .execute(
            "CREATE TABLE host_writes (id UUID PRIMARY KEY, step INTEGER)",
            &empty(),
        )
        .unwrap();
    sibling_db
        .execute(
            "CREATE TABLE non_trigger_log (id UUID PRIMARY KEY, source_step INTEGER)",
            &empty(),
        )
        .unwrap();
    sibling_db
        .execute(
            "CREATE TRIGGER host_write_trigger ON host_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
    let depths = Arc::new(Mutex::new(Vec::new()));
    let callback_depths = depths.clone();
    sibling_db
        .register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
            callback_depths.lock().unwrap().push(ctx.depth);
            let step = ctx
                .row_values
                .get("step")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            p.insert("step".into(), Value::Int64(step));
            db_handle.execute(
                "INSERT INTO non_trigger_log (id, source_step) VALUES ($id, $step)",
                &p,
            )?;
            Ok(())
        })
        .unwrap();
    sibling_db.complete_initialization().unwrap();
    sibling_db.execute("BEGIN", &empty()).unwrap();
    for step in [1_i64, 2_i64] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("step".into(), Value::Int64(step));
        sibling_db
            .execute("INSERT INTO host_writes (id, step) VALUES ($id, $step)", &p)
            .unwrap();
    }
    sibling_db.execute("COMMIT", &empty()).unwrap();
    for step in [3_i64, 4_i64] {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("step".into(), Value::Int64(step));
        sibling_db
            .execute("INSERT INTO host_writes (id, step) VALUES ($id, $step)", &p)
            .unwrap();
    }
    assert_eq!(
        depths.lock().unwrap().as_slice(),
        &[1, 1, 1, 1],
        "sibling fires and sequential transactions must start at call-frame depth 1"
    );
    assert_eq!(
        count_rows(&sibling_db, "non_trigger_log"),
        4,
        "cascades into non-trigger tables must not recursively increment trigger depth"
    );
    let sibling_history = sibling_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let sibling_ring = sibling_db.trigger_audit_log();
    let sibling_depths = sibling_history
        .iter()
        .map(|entry| entry.depth)
        .collect::<Vec<_>>();
    let sibling_lsn_group_sizes = {
        let mut by_lsn = BTreeMap::<Lsn, usize>::new();
        for entry in &sibling_history {
            *by_lsn.entry(entry.firing_lsn).or_default() += 1;
        }
        by_lsn.into_values().collect::<Vec<_>>()
    };
    assert_eq!(
        (
            sibling_history.len(),
            sibling_ring.len(),
            sibling_depths.as_slice(),
            sibling_lsn_group_sizes.as_slice(),
        ),
        (4, 4, &[1_u32, 1, 1, 1][..], &[2_usize, 1, 1][..]),
        "audit must record every sibling fire, including two fires in one explicit transaction; history={sibling_history:?}, ring={sibling_ring:?}"
    );
    sibling_db.close().unwrap();
    let reopened_sibling = Database::open(&sibling_path).unwrap();
    reopened_sibling
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_sibling.complete_initialization().unwrap();
    let reopened_sibling_history = reopened_sibling
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    let reopened_sibling_depths = reopened_sibling_history
        .iter()
        .map(|entry| entry.depth)
        .collect::<Vec<_>>();
    let reopened_sibling_lsn_group_sizes = {
        let mut by_lsn = BTreeMap::<Lsn, usize>::new();
        for entry in &reopened_sibling_history {
            *by_lsn.entry(entry.firing_lsn).or_default() += 1;
        }
        by_lsn.into_values().collect::<Vec<_>>()
    };
    assert_eq!(
        (
            reopened_sibling_history.len(),
            reopened_sibling_depths.as_slice(),
            reopened_sibling_lsn_group_sizes.as_slice(),
        ),
        (4, &[1_u32, 1, 1, 1][..], &[2_usize, 1, 1][..]),
        "sibling and same-transaction multi-fire audit entries must survive durable reopen; history={reopened_sibling_history:?}"
    );
    reopened_sibling.close().unwrap();
}

#[test]
fn t3_cron_callback_write_can_fire_trigger_without_aliasing_flags() {
    let db = Arc::new(Database::open_memory());
    setup_host_tables(&db);
    let trigger_fires = Arc::new(AtomicUsize::new(0));
    let trigger_apply_results = Arc::new(Mutex::new(Vec::new()));
    let callback_apply_results = trigger_apply_results.clone();
    let trigger_fires_for_callback = trigger_fires.clone();
    db.register_trigger_callback("host_write_trigger", move |db_handle, ctx| {
        trigger_fires_for_callback.fetch_add(1, Ordering::SeqCst);
        let apply_result = db_handle.apply_changes(
            ChangeSet::default(),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        );
        callback_apply_results
            .lock()
            .unwrap()
            .push(format!("{apply_result:?}"));
        if !matches!(
            &apply_result,
            Err(Error::Other(reason))
                if reason.contains("cron callbacks") && !reason.contains("trigger")
        ) {
            return Err(Error::Other(format!(
                "apply_changes inside cron-trigger callback must be rejected for the cron-active reason only; got {apply_result:?}"
            )));
        }
        let write_id = ctx
            .row_values
            .get("id")
            .and_then(Value::as_uuid)
            .copied()
            .ok_or_else(|| Error::Other("cron trigger row missing UUID id".into()))?;
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        p.insert("write_id".into(), Value::Uuid(write_id));
        p.insert("note".into(), Value::Text("cron-cascade".into()));
        db_handle.execute(
            "INSERT INTO host_audits (id, write_id, note) VALUES ($id, $write_id, $note)",
            &p,
        )?;
        Ok(())
    })
    .unwrap();
    db.execute(
        "CREATE SCHEDULE cron_fire EVERY '500 MILLISECONDS' TX (cron_cb)",
        &empty(),
    )
    .unwrap();
    db.register_cron_callback("cron_cb", move |db_handle| {
        db_handle.execute(
            host_insert_sql(),
            &insert_host_params(Uuid::new_v4(), "cron"),
        )?;
        let apply_result = db_handle.apply_changes(
            ChangeSet::default(),
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        );
        if !matches!(
            &apply_result,
            Err(Error::Other(reason))
                if reason.contains("cron callbacks") && !reason.contains("trigger")
        ) {
            return Err(Error::Other(format!(
                "apply_changes after cron-fired trigger returns must still be rejected for the cron-active reason only; got {apply_result:?}"
            )));
        }
        Ok(())
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let _pause = db.pause_cron_tickler_for_test();
    std::thread::sleep(Duration::from_millis(700));
    let cron_since = db.current_lsn();
    let cron_rx = db.subscribe();
    let fires = db.cron_run_due_now_for_test().unwrap();
    assert_eq!(fires, 1);
    assert_eq!(
        trigger_fires.load(Ordering::SeqCst),
        1,
        "cron callback writes must fire observation triggers in the same cron tx"
    );
    assert_eq!(
        trigger_apply_results.lock().unwrap().len(),
        1,
        "trigger callback must exercise cron-active apply_changes rejection once"
    );
    assert_eq!(count_rows(&db, "host_audits"), 1);
    let cron_event = cron_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cron-origin trigger cascade must publish one atomic commit event");
    assert!(
        cron_rx.try_recv().is_err(),
        "cron-origin firing row and trigger cascade must not publish separate commit events"
    );
    let cron_changes = db.changes_since(cron_since);
    let host_row = db
        .scan("host_writes", db.snapshot())
        .unwrap()
        .into_iter()
        .find(|row| row.values.get("content") == Some(&Value::Text("cron".into())))
        .expect("cron callback must create the firing host row");
    let audit_row = db
        .scan("host_audits", db.snapshot())
        .unwrap()
        .into_iter()
        .find(|row| row.values.get("note") == Some(&Value::Text("cron-cascade".into())))
        .expect("cron-origin trigger callback must create the cascade audit row");
    let cron_group_count = row_group_signatures(&db, cron_changes).len();
    assert_eq!(
        (
            host_row.lsn,
            audit_row.lsn,
            host_row.created_tx,
            audit_row.created_tx,
            cron_event.lsn,
            cron_event
                .tables_changed
                .contains(&"host_writes".to_string()),
            cron_event
                .tables_changed
                .contains(&"host_audits".to_string()),
            cron_group_count,
        ),
        (
            audit_row.lsn,
            host_row.lsn,
            audit_row.created_tx,
            host_row.created_tx,
            host_row.lsn,
            true,
            true,
            1,
        ),
        "cron-origin firing row and trigger cascade must share one LSN/Tx/event; event={cron_event:?}, host={host_row:?}, audit={audit_row:?}"
    );
}

#[test]
fn t3_audit_records_panic_rollback_and_engine_survives() {
    let tmp = TempDir::new().unwrap();
    let panic_path = tmp.path().join("panic_audit.redb");
    let db = Database::open(&panic_path).unwrap();
    setup_host_tables(&db);
    db.execute(
        "CREATE TABLE after_panic (id UUID PRIMARY KEY, note TEXT)",
        &empty(),
    )
    .unwrap();
    db.register_trigger_callback("host_write_trigger", move |_, _| -> Result<()> {
        panic!("panic from trigger callback")
    })
    .unwrap();
    db.complete_initialization().unwrap();

    let result = db.execute(host_insert_sql(), &insert_host_params(uuid(120), "panic"));
    let audit = db.trigger_audit_log();
    assert!(
        matches!(
            result,
            Err(Error::TriggerCallbackFailed { ref trigger_name, ref reason })
                if trigger_name == "host_write_trigger" && reason.contains("panic")
        ),
        "panic must be caught, typed, and rolled back; got {result:?}"
    );
    assert_eq!(
        count_rows(&db, "host_writes"),
        0,
        "panic rollback must leave no firing row"
    );
    let panic_audit = audit.iter().find(|entry| {
        matches!(
            &entry.status,
            TriggerAuditStatus::RolledBack { reason } if reason.contains("panic")
        )
    });
    assert!(
        matches!(
            panic_audit,
            Some(entry)
                if entry.trigger_name == "host_write_trigger"
                    && entry.firing_tx > TxId(0)
                    && entry.firing_lsn == Lsn(0)
                    && entry.depth == 1
                    && entry.cascade_row_count == 0
        ),
        "panic rollback must be audited with concrete trigger/status/tx/depth fields; got {audit:?}"
    );

    db.execute(
        "INSERT INTO after_panic (id, note) VALUES ('00000000-0000-0000-0000-000000000121', 'alive')",
        &empty_params(),
    )
    .expect("engine must remain usable after callback panic");
    assert_eq!(count_rows(&db, "after_panic"), 1);

    let rolled_back_history = db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::RolledBack),
        })
        .unwrap();
    assert_eq!(
        rolled_back_history.len(),
        1,
        "rolled-back trigger audits must be available from durable filtered history"
    );
    assert!(
        matches!(
            &rolled_back_history[0].status,
            TriggerAuditStatus::RolledBack { reason } if reason.contains("panic")
        ),
        "rolled-back history must retain callback failure reason; got {rolled_back_history:?}"
    );
    assert!(
        db.trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap()
        .is_empty(),
        "failed callback must not also appear as a fired audit"
    );
    db.close().unwrap();
    let reopened_panic = Database::open(&panic_path).unwrap();
    reopened_panic
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_panic.complete_initialization().unwrap();
    let reopened_rolled_back_history = reopened_panic
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::RolledBack),
        })
        .unwrap();
    assert_eq!(
        reopened_rolled_back_history, rolled_back_history,
        "callback panic rollback audit history must survive reopen"
    );
    reopened_panic.close().unwrap();

    let audit_path = tmp.path().join("audit_ring_history.redb");
    let audit_db = Database::open(&audit_path).unwrap();
    setup_host_tables(&audit_db);
    audit_db
        .execute(
            "CREATE TABLE other_writes (id UUID PRIMARY KEY, content TEXT)",
            &empty(),
        )
        .unwrap();
    audit_db
        .execute(
            "CREATE TABLE other_audits (id UUID PRIMARY KEY, write_id UUID)",
            &empty(),
        )
        .unwrap();
    audit_db
        .execute(
            "CREATE TRIGGER other_trigger ON other_writes WHEN INSERT",
            &empty(),
        )
        .unwrap();
    let ring_host_fires = Arc::new(AtomicUsize::new(0));
    let other_fires = Arc::new(AtomicUsize::new(0));
    register_audit_callback(&audit_db, ring_host_fires.clone());
    let other_fires_for_callback = other_fires.clone();
    audit_db
        .register_trigger_callback("other_trigger", move |db_handle, ctx| {
            other_fires_for_callback.fetch_add(1, Ordering::SeqCst);
            let write_id = ctx
                .row_values
                .get("id")
                .and_then(Value::as_uuid)
                .copied()
                .ok_or_else(|| Error::Other("other trigger row missing UUID id".into()))?;
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::new_v4()));
            p.insert("write_id".into(), Value::Uuid(write_id));
            db_handle.execute(
                "INSERT INTO other_audits (id, write_id) VALUES ($id, $write_id)",
                &p,
            )?;
            Ok(())
        })
        .unwrap();
    audit_db.complete_initialization().unwrap();
    audit_db
        .execute(
            "INSERT INTO other_writes (id, content) VALUES ('00000000-0000-0000-0000-000000000130', 'other')",
            &empty_params(),
        )
        .unwrap();
    assert_eq!(
        other_fires.load(Ordering::SeqCst),
        1,
        "second trigger positive control must fire before host ring eviction checks"
    );

    let cap = audit_db.trigger_audit_ring_capacity();
    assert!(
        cap > 0 && cap <= 4096,
        "trigger audit ring capacity must be bounded and non-zero; got {cap}"
    );
    for n in 0..=cap {
        audit_db
            .execute(
                host_insert_sql(),
                &insert_host_params(uuid(0xA000 + n as u128), "ring"),
            )
            .unwrap();
    }
    assert_eq!(
        ring_host_fires.load(Ordering::SeqCst),
        cap + 1,
        "host trigger must fire for every ring-capacity eviction input"
    );
    let ring = audit_db.trigger_audit_log();
    assert_eq!(
        ring.len(),
        cap,
        "in-memory audit ring must retain exactly its bounded capacity"
    );
    let fired_history = audit_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        fired_history.len(),
        cap + 1,
        "durable audit history must not be truncated by in-memory ring eviction"
    );
    let mut fired_lsns = fired_history
        .iter()
        .map(|entry| entry.firing_lsn)
        .collect::<Vec<_>>();
    fired_lsns.sort_unstable();
    let oldest_fired_lsn = *fired_lsns.first().unwrap();
    let newest_fired_lsn = *fired_lsns.last().unwrap();
    assert!(
        !ring
            .iter()
            .any(|entry| entry.firing_lsn == oldest_fired_lsn),
        "ring must evict the oldest fired audit when capacity is exceeded; ring={ring:?}, history={fired_history:?}"
    );
    assert!(
        ring.iter()
            .any(|entry| entry.firing_lsn == newest_fired_lsn),
        "ring must retain the newest fired audit; ring={ring:?}, history={fired_history:?}"
    );
    let newest_ring_entry = ring
        .iter()
        .find(|entry| entry.firing_lsn == newest_fired_lsn)
        .expect("newest fired audit must be retained in the in-memory ring");
    assert_eq!(
        newest_ring_entry.trigger_name, "host_write_trigger",
        "ring entry must retain the fired trigger name"
    );
    assert_eq!(
        newest_ring_entry.status,
        TriggerAuditStatus::Fired,
        "ring entry for a successful fire must expose Fired status"
    );
    assert_eq!(
        newest_ring_entry.depth, 1,
        "ring entry for a direct successful fire must expose callback depth"
    );
    assert_eq!(
        newest_ring_entry.cascade_row_count, 3,
        "ring entry for host trigger must count relational, graph, and vector cascade effects"
    );
    assert_eq!(
        newest_ring_entry.firing_tx,
        TxId(audit_db.snapshot_at(newest_ring_entry.firing_lsn).0),
        "ring entry firing_tx must resolve from its firing LSN"
    );
    let other_history = audit_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("other_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        other_history.len(),
        1,
        "trigger-name filters must isolate audit history across multiple trigger declarations"
    );
    assert_eq!(
        other_history[0].cascade_row_count, 1,
        "second trigger audit must record its own cascade cardinality independently"
    );
    let expected_fired_history = fired_history.clone();
    let oldest_fired_entry = expected_fired_history
        .iter()
        .find(|entry| entry.firing_lsn == oldest_fired_lsn)
        .cloned()
        .expect("oldest fired entry must be present in durable history before reopen");
    audit_db.close().unwrap();

    let reopened_audit_db = Database::open(&audit_path).unwrap();
    reopened_audit_db
        .register_trigger_callback("host_write_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_audit_db
        .register_trigger_callback("other_trigger", |_, _| Ok(()))
        .unwrap();
    reopened_audit_db.complete_initialization().unwrap();
    let reopened_ring = reopened_audit_db.trigger_audit_log();
    assert_eq!(
        reopened_ring.len(),
        cap,
        "file-backed reopen must restore only the bounded hot ring, not mirror all durable audit history into memory"
    );
    assert!(
        !reopened_ring
            .iter()
            .any(|entry| entry.firing_lsn == oldest_fired_lsn)
            && reopened_ring
                .iter()
                .any(|entry| entry.firing_lsn == newest_fired_lsn),
        "file-backed hot ring must preserve eviction state across reopen; ring={reopened_ring:?}"
    );
    let reopened_fired_history = reopened_audit_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("host_write_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        reopened_fired_history.len(),
        cap + 1,
        "file-backed durable audit history must retain cap+1 fired entries across reopen, independent of the in-memory ring"
    );
    assert!(
        reopened_fired_history.contains(&oldest_fired_entry)
            && expected_fired_history
                .iter()
                .all(|entry| reopened_fired_history.contains(entry)),
        "file-backed durable audit history must retain the oldest evicted ring entry and every fired entry across reopen; oldest={oldest_fired_entry:?}, reopened={reopened_fired_history:?}"
    );
    let reopened_other_history = reopened_audit_db
        .trigger_audit_history(TriggerAuditFilter {
            trigger_name: Some("other_trigger".into()),
            status: Some(TriggerAuditStatusFilter::Fired),
        })
        .unwrap();
    assert_eq!(
        reopened_other_history, other_history,
        "file-backed durable audit history filters must survive reopen for sibling triggers"
    );
    reopened_audit_db.close().unwrap();
}
