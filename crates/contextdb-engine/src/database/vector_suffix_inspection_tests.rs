// Source-only qualification of the actual canonical writer boundary.
use crate as contextdb_engine;
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
        db.vector_current_identity_admission_failure_consumed_for_test(),
        "the refusal reaches actual canonical identity admission: {error:?}"
    );
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { operation, .. } if operation == "current_identity_delta")
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
fn local_current_identity_admission_refuses_the_complete_transaction_before_durability() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("local.db");
    let db = Database::open(&path).unwrap();
    let writer = db.__accountant_handle_for_test();
    seed(&db);
    let root_before = db.writer_read_root().unwrap().unwrap().storage_identity();
    let generation = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition())
        .unwrap();
    db.arm_vector_current_identity_admission_failure_for_test();
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
        db.writer_read_root().unwrap().unwrap().storage_identity(),
        root_before,
        "row/index/vector/journal/config publication root is unchanged"
    );
    assert_eq!(
        db.vector_store_for_test()
            .partition_graph_generation_status(&partition())
            .unwrap()
            .base,
        generation.base
    );
    db.arm_vector_current_identity_admission_failure_for_test();
    let error = db
        .execute("UPDATE items SET embedding = $v WHERE id = 1", &params(90))
        .unwrap_err();
    assert_refusal(&db, error);
    db.close().unwrap();
    drop(db);
    assert_final_writer_refund(writer);
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
fn sync_current_identity_admission_refuses_rows_vectors_and_the_authenticated_receipt() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("receiver.db");
    let target = Database::open(&path).unwrap();
    let writer = target.__accountant_handle_for_test();
    seed(&target);
    let root_before = target
        .writer_read_root()
        .unwrap()
        .unwrap()
        .storage_identity();
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
    let changes = source
        .changes_since(Lsn(1))
        .expect("read the declared changes");
    let source_lsn = changes.max_lsn().unwrap();
    let tenant = TenantId::from("tail-admission");
    let incarnation = Incarnation::mint();
    target.arm_vector_current_identity_admission_failure_for_test();
    let error = target
        .apply_synced_changes_with_receipt(
            changes.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
            &HashMap::new(),
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: "edge".to_owned(),
                incarnation,
                source_lsn,
                dependency_complete: false,
                hub_contact_ms: None,
            },
        )
        .unwrap_err();
    assert_refusal(&target, error);
    assert_eq!(
        target
            .writer_read_root()
            .unwrap()
            .unwrap()
            .storage_identity(),
        root_before,
        "failed receive installs no row/vector/log/catalog or receipt root"
    );
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
    drop(target);
    assert_final_writer_refund(writer);
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
    for _ in 0..2 {
        reopened
            .apply_synced_changes_with_receipt(
                changes.clone(),
                &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
                &HashMap::new(),
                SyncAdoption::Continuing,
                SyncApplyReceipt {
                    tenant_id: tenant.clone(),
                    node_id: "edge".into(),
                    incarnation,
                    source_lsn,
                    dependency_complete: false,
                    hub_contact_ms: None,
                },
            )
            .unwrap();
        assert_eq!(
            indexed_ids(&reopened),
            vec![1, 2, 3, 4],
            "retry and exact replay preserve complete answers"
        );
        assert_eq!(
            reopened
                .persisted_sync_applied_push_watermark_for_node_incarnation(
                    &tenant,
                    "edge",
                    incarnation
                )
                .unwrap(),
            Some(source_lsn)
        );
    }
    let retry_writer = reopened.__accountant_handle_for_test();
    reopened.close().unwrap();
    drop(reopened);
    assert_final_writer_refund(retry_writer);
}

#[derive(Debug)]
struct ReaderBudget(std::sync::Arc<crate::memory_accounting::MemoryAccountant>);
impl contextdb_core::read_memory::ReadMemoryBudget for ReaderBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.0.try_allocate_for(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}

#[test]
fn captured_suffix_inspection_uses_actual_reader_and_root_without_resident_tail() {
    use crate::memory_accounting::MemoryAccountant;
    use std::sync::Arc;
    let root = TempDir::new().unwrap();
    let path = root.path().join("suffix-inspection.db");
    let db = Database::open(&path).unwrap();
    seed(&db);
    insert(&db, 2).unwrap();
    let snapshot = db.snapshot();
    let store = db.vector_store_for_test();
    assert_eq!(
        store
            .partition_graph_generation_status(&partition())
            .unwrap()
            .fresh_tail_entries,
        0
    );
    assert!(
        store
            .with_partition_fresh_tail(&partition(), |graph| graph.len())
            .is_none()
    );
    let accountant = Arc::new(MemoryAccountant::with_budget(8 * 1024 * 1024));
    let reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> =
        Arc::new(ReaderBudget(accountant.clone()));
    let mut checkpoints = 0usize;
    let old = db
        .inspect_captured_vector_suffix_for_test(
            &partition(),
            snapshot,
            reader.clone(),
            &mut || {
                checkpoints += 1;
                if checkpoints == 2 {
                    insert(&db, 3)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        old.points, 1,
        "the loader keeps the captured suffix after a later commit"
    );
    assert_eq!(old.policy, (32, 96, 17));
    assert_eq!(
        accountant.usage().used,
        0,
        "all actual reader/source/graph owners refund"
    );
    let current = db
        .inspect_captured_vector_suffix_for_test(
            &partition(),
            db.snapshot(),
            reader.clone(),
            &mut || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(current.points, 2);
    assert!(current.replayed_through_lsn > old.replayed_through_lsn);
    assert_eq!(current.policy, (32, 96, 17));
    let repeated = db
        .inspect_captured_vector_suffix_for_test(
            &partition(),
            db.snapshot(),
            reader.clone(),
            &mut || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        current, repeated,
        "topology is the actual deterministic captured replay graph"
    );
    assert_eq!(accountant.usage().used, 0);
    let cancelled = db.inspect_captured_vector_suffix_for_test(
        &partition(),
        db.snapshot(),
        reader.clone(),
        &mut || Err(Error::ReadCancelled),
    );
    assert!(matches!(cancelled, Err(Error::ReadCancelled)));
    assert_eq!(accountant.usage().used, 0);
    let before = indexed_ids(&db);
    let tiny = Arc::new(MemoryAccountant::with_budget(1));
    let tiny_reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> =
        Arc::new(ReaderBudget(tiny.clone()));
    assert!(matches!(
        db.inspect_captured_vector_suffix_for_test(
            &partition(),
            db.snapshot(),
            tiny_reader,
            &mut || Ok(())
        ),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(tiny.usage().used, 0);
    assert_eq!(
        indexed_ids(&db),
        before,
        "inspection refusal changes no durable answer"
    );
    assert_eq!(
        store
            .partition_graph_generation_status(&partition())
            .unwrap()
            .fresh_tail_entries,
        0
    );
    assert!(
        store
            .with_partition_fresh_tail(&partition(), |graph| graph.len())
            .is_none()
    );
    db.close().unwrap();
    drop(store);
    drop(db);
    let file = crate::ReadSession::open_with_options(
        &path,
        crate::ReadSessionOptions {
            limits: contextdb_core::read_contract::ReadLimits {
                memory: 64 * 1024 * 1024,
                ..Default::default()
            },
            memory_limit: Some(64 * 1024 * 1024),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(file.route(), contextdb_core::read_contract::ReadRoute::File);
    let inspected = file
        .__with_resident_history_for_test(|database| {
            database.inspect_captured_vector_suffix_for_test(
                &partition(),
                database.snapshot(),
                database.declared_reader_memory()?,
                &mut || Ok(()),
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        inspected, current,
        "installed file target keeps its own committed root and actual topology"
    );
}

#[test]
fn seeded_suffix_writer_final_owner_refunds_without_injected_failure() {
    let root = TempDir::new().unwrap();
    let db = Database::open(root.path().join("control.db")).unwrap();
    let writer = db.__accountant_handle_for_test();
    seed(&db);
    db.close().unwrap();
    drop(db);
    assert_final_writer_refund(writer);
}

fn assert_final_writer_refund(writer: crate::memory_accounting::AccountantHandle) {
    use crate::memory_accounting::process_aggregate_snapshot;
    // The diagnostic owns the engine-created accountant, limit, and writer
    // counter. Their private seam reports the live identities and typed
    // layouts; it is not an allocator measurement.
    let (native_origin, controls) = writer.native_backing_for_test();
    assert!(
        native_origin,
        "this diagnostic retains an engine-created writer"
    );
    let backing = controls
        .iter()
        .map(|(_, layout)| layout.size())
        .sum::<usize>();
    assert_eq!(
        writer.usage().used,
        backing,
        "all store payloads and controls refund before the diagnostic accountant is freed"
    );
    assert_eq!(writer.underflow_count_for_test(), 0);
    let before = process_aggregate_snapshot().0;
    drop(writer);
    assert_eq!(
        process_aggregate_snapshot().0,
        before - backing,
        "final counter destruction refunds the executing process aggregate exactly"
    );
}
