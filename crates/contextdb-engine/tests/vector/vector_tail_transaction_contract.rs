use contextdb_core::{
    Error, Incarnation, Lsn, TenantId, Value, VectorIndexRef, VectorPartitionKey,
};
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy, SyncAdoption};
use contextdb_engine::{Database, MaintenancePolicy, database::SyncApplyReceipt};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use tempfile::TempDir;

const DDL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 32, EF_CONSTRUCTION = 96, EF_SEARCH = 17))";

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}
fn partition() -> VectorPartitionRef {
    VectorPartitionRef::new(index(), VectorPartitionKey::unpartitioned())
}
fn params(id: i64) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Int64(id)),
        ("v".to_owned(), Value::Vector(vec![1.0, id as f32 / 100.0])),
    ])
}
fn insert(db: &Database, id: i64) -> contextdb_core::Result<contextdb_engine::QueryResult> {
    db.execute(
        "INSERT INTO items (id, embedding) VALUES ($id, $v)",
        &params(id),
    )
}
fn seed(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(DDL, &HashMap::new()).unwrap();
    insert(db, 1).unwrap();
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
        if db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition())
            .is_some_and(|status| status.base.is_some() || status.dormant_base.is_some())
        {
            break;
        }
    }
    assert_eq!(indexed_ids(db), vec![1]);
}
fn indexed_ids(db: &Database) -> Vec<i64> {
    let result = db
        .execute(
            "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 8",
            &HashMap::from([("q".to_owned(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref value => panic!("unexpected id {value:?}"),
        })
        .collect()
}
fn assert_refusal(db: &Database, error: Error) {
    assert!(
        db.vector_tail_admission_failure_consumed_for_test(),
        "the refusal reaches actual tail admission: {error:?}"
    );
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { operation, .. } if operation == "prepare_hnsw_tail")
    );
    assert_eq!(
        db.execute("SELECT id FROM items ORDER BY id", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        indexed_ids(db),
        vec![1],
        "the previously complete indexed route survives"
    );
}

#[test]
fn local_tail_admission_refuses_the_complete_transaction_before_durability() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("local.db");
    let db = Database::open(&path).unwrap();
    seed(&db);
    let generation = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition())
        .unwrap();
    db.arm_vector_tail_admission_failure_for_test();
    let tx = db.begin_or_panic();
    db.insert_row(
        tx,
        "items",
        HashMap::from([
            ("id".to_owned(), Value::Int64(2)),
            ("embedding".to_owned(), Value::Vector(vec![0.0, 1.0])),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "items",
        HashMap::from([
            ("id".to_owned(), Value::Int64(3)),
            ("embedding".to_owned(), Value::Vector(vec![0.5, 0.5])),
        ]),
    )
    .unwrap();
    assert_refusal(&db, db.commit(tx).unwrap_err());
    assert_eq!(
        db.vector_store_for_test()
            .partition_graph_generation_status(&partition())
            .unwrap()
            .base,
        generation.base
    );
    db.arm_vector_tail_admission_failure_for_test();
    let error = db
        .execute("UPDATE items SET embedding = $v WHERE id = 1", &params(90))
        .unwrap_err();
    assert_refusal(&db, error);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(indexed_ids(&reopened), vec![1]);
    assert_eq!(
        reopened
            .execute("SELECT embedding FROM items WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows[0][0],
        Value::Vector(vec![1.0, 0.01])
    );
}

#[test]
fn sync_tail_admission_refuses_rows_vectors_and_the_authenticated_receipt() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("receiver.db");
    let target = Database::open(&path).unwrap();
    seed(&target);
    let source = Database::open_memory();
    source.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    source.execute(DDL, &HashMap::new()).unwrap();
    let tx = source.begin_or_panic();
    for id in [2, 3] {
        source
            .insert_row(
                tx,
                "items",
                HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    (
                        "embedding".to_owned(),
                        Value::Vector(vec![1.0, id as f32 / 100.0]),
                    ),
                ]),
            )
            .unwrap();
    }
    source.commit(tx).unwrap();
    let changes = source.changes_since(Lsn(1));
    let source_lsn = changes.max_lsn().unwrap();
    let tenant = TenantId::from("tail-admission");
    let incarnation = Incarnation::mint();
    target.arm_vector_tail_admission_failure_for_test();
    let error = target
        .apply_synced_changes_with_receipt(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: "edge".to_owned(),
                incarnation,
                source_lsn,
                dependency_complete: false,
            },
        )
        .unwrap_err();
    assert_refusal(&target, error);
    assert!(
        target
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &tenant,
                "edge",
                incarnation
            )
            .unwrap()
            .is_none()
    );
    target.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(indexed_ids(&reopened), vec![1]);
    assert!(
        reopened
            .persisted_sync_applied_push_watermark_for_node_incarnation(
                &tenant,
                "edge",
                incarnation
            )
            .unwrap()
            .is_none()
    );
    insert(&reopened, 4).unwrap();
    assert_eq!(
        indexed_ids(&reopened),
        vec![1, 4],
        "the next admitted write can still extend the fresh tail"
    );
}

#[test]
fn declared_topology_and_search_breadth_reach_replayed_and_post_restart_tails() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("policy.db");
    let db = Database::open(&path).unwrap();
    seed(&db);
    insert(&db, 2).unwrap();
    assert_eq!(
        db.vector_store_for_test()
            .with_partition_fresh_tail(&partition(), |graph| graph.policy_values()),
        Some((32, 96, 17))
    );
    db.close().unwrap();
    for id in [3, 4] {
        let reopened = Database::open(&path).unwrap();
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(indexed_ids(&reopened).len(), (id - 1) as usize);
        assert_eq!(
            reopened
                .vector_store_for_test()
                .with_partition_fresh_tail(&partition(), |graph| graph.policy_values()),
            Some((32, 96, 17))
        );
        insert(&reopened, id).unwrap();
        assert_eq!(indexed_ids(&reopened).len(), id as usize);
        assert_eq!(
            reopened
                .vector_store_for_test()
                .with_partition_fresh_tail(&partition(), |graph| graph.policy_values()),
            Some((32, 96, 17))
        );
        reopened.close().unwrap();
    }
}

#[derive(Clone, Copy)]
struct FrozenClock;
impl contextdb_core::read_contract::DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _deadline_ms: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn bounded_ids(db: &Database, sql: &str, bound: HashMap<String, Value>) -> Vec<i64> {
    use contextdb_engine::executor::bounded_read_test_support as bounded;
    let request = bounded::BoundedReadRequest::new(
        sql,
        bound,
        contextdb_core::read_contract::ReadLimits {
            work: 10_000_000,
            memory: 64 * 1024 * 1024,
            ..Default::default()
        },
        std::sync::Arc::new(FrozenClock),
    );
    bounded::execute(db, &request)
        .unwrap()
        .result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref value => panic!("unexpected id {value:?}"),
        })
        .collect()
}

#[test]
fn ranked_auto_uses_the_declared_crossover_on_ordinary_and_bounded_reads() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE outcomes (id INTEGER PRIMARY KEY, item_id INTEGER, weight REAL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX outcomes_item ON outcomes(item_id)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(2) AUTO_INDEX_AT 1 HNSW (M = 32, EF_CONSTRUCTION = 96, EF_SEARCH = 17) RANK_POLICY (JOIN outcomes ON item_id, FORMULA 'coalesce({weight}, 0.0)', SORT_KEY priority))", &HashMap::new()).unwrap();
    for id in 1..=3 {
        let vector = match id {
            1 => vec![1.0, 0.0],
            2 => vec![0.8, 0.6],
            _ => vec![-0.8, 0.6],
        };
        db.execute(
            "INSERT INTO items (id, embedding) VALUES ($id, $v)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("v".to_owned(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO outcomes (id, item_id, weight) VALUES ($id, $id, $weight)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                (
                    "weight".to_owned(),
                    Value::Float64(if id == 3 { 1000.0 } else { 0.0 }),
                ),
            ]),
        )
        .unwrap();
    }
    for _ in 0..4 {
        db.run_maintenance_cycle().unwrap();
    }
    let sql =
        "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR AUTO USE RANK priority LIMIT 1";
    let bound = HashMap::from([("q".to_owned(), Value::Vector(vec![1.0, 0.0]))]);
    let indexed = db.execute(sql, &bound).unwrap();
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());
    assert_eq!(
        indexed.rows[0][0],
        Value::Int64(3),
        "declared breadth includes all three vector candidates"
    );
    assert_eq!(bounded_ids(&db, sql, bound.clone()), vec![3]);
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (EF_SEARCH = 1)",
        &HashMap::new(),
    )
    .unwrap();
    assert_ne!(db.execute(sql, &bound).unwrap().rows[0][0], Value::Int64(3));
    assert_ne!(bounded_ids(&db, sql, bound.clone()), vec![3]);
    let mut semantic =
        contextdb_engine::SemanticQuery::new("items", "embedding", vec![1.0, 0.0], 1);
    semantic.sort_key = Some("priority".into());
    assert_ne!(
        db.semantic_search(semantic.clone()).unwrap()[0].values["id"],
        Value::Int64(3)
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (EF_SEARCH = 17)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        db.semantic_search(semantic).unwrap()[0].values["id"],
        Value::Int64(3)
    );

    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET AUTO_INDEX_AT 4",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(db.execute(sql, &bound).unwrap().rows[0][0], Value::Int64(3));
    assert!(!db.__debug_last_query_vector_used_hnsw_for_test());
    assert_eq!(
        bounded_ids(&db, sql, bound),
        vec![3],
        "below the declared crossover, exact ranking sees the distant high-weight row"
    );
    assert!(!db.__debug_last_query_vector_used_hnsw_for_test());
}

#[test]
fn current_filtered_rows_keep_the_same_answers_as_excluded_history_grows() {
    use contextdb_engine::executor::bounded_read_test_support as bounded;
    let root = TempDir::new().unwrap();
    let db = Database::open(root.path().join("history.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, is_current BOOLEAN, embedding VECTOR(2) AUTO_INDEX_AT 1 HNSW (M = 32, EF_CONSTRUCTION = 96, EF_SEARCH = 17))", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX current_items ON items(is_current)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 1..=3 {
        db.execute(
            "INSERT INTO items (id, is_current, embedding) VALUES ($id, true, $v)",
            &params(id),
        )
        .unwrap();
    }
    for _ in 0..8 {
        db.run_maintenance_cycle().unwrap();
    }
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot);
    let sql = "SELECT id FROM items WHERE is_current = true ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let bound = HashMap::from([("q".to_owned(), Value::Vector(vec![1.0, 0.0]))]);
    let expected = db.execute(sql, &bound).unwrap().rows;
    for (start, end) in [(4, 68), (68, 1068)] {
        let tx = db.begin_or_panic();
        for id in start..end {
            db.insert_row(
                tx,
                "items",
                HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("is_current".to_owned(), Value::Bool(false)),
                    ("embedding".to_owned(), Value::Vector(vec![-1.0, 0.0])),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        assert_eq!(db.execute(sql, &bound).unwrap().rows, expected);
        assert_eq!(
            db.execute_at_snapshot(sql, &bound, snapshot).unwrap().rows,
            expected
        );
        let request = bounded::BoundedReadRequest::new(
            sql,
            bound.clone(),
            contextdb_core::read_contract::ReadLimits {
                work: 10_000_000,
                memory: 64 * 1024 * 1024,
                ..Default::default()
            },
            std::sync::Arc::new(FrozenClock),
        );
        let result = bounded::execute(&db, &request).unwrap();
        assert_eq!(result.result.rows, expected);
        assert!(db.__debug_last_query_vector_used_hnsw_for_test());
        assert!(
            result
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::VectorCandidates)
                .copied()
                .unwrap_or(0)
                < 1000,
            "fixed authorized rows must not pay a raw-history pass: {:?}",
            result.telemetry
        );
    }
    drop(pin);
}
