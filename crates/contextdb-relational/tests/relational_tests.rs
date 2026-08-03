use contextdb_core::{
    ColName, Error, RelationalExecutor, RowId, StateMachineConstraint, TableMeta, UpsertResult,
    Value,
};
use contextdb_relational::{MemRelationalExecutor, RelationalStore};
use contextdb_tx::{TransactionManager, WriteSet, WriteSetApplicator};
use std::collections::HashMap;
use std::sync::Arc;

struct TestStore {
    relational: Arc<RelationalStore>,
}

impl WriteSetApplicator for TestStore {
    fn apply(&self, ws: &WriteSet) -> contextdb_core::Result<()> {
        if ws.relational_deletes.is_empty() || ws.relational_inserts.is_empty() {
            self.relational.apply_deletes_ref(&ws.relational_deletes);
            self.relational.apply_inserts_ref(&ws.relational_inserts);
        } else {
            self.relational
                .apply_replacements_ref(&ws.relational_deletes, &ws.relational_inserts);
        }
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}

fn map(pairs: Vec<(&str, Value)>) -> HashMap<ColName, Value> {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<_, _>>()
}

fn setup() -> (
    Arc<TransactionManager<TestStore>>,
    MemRelationalExecutor<TestStore>,
) {
    let relational = Arc::new(RelationalStore::new());
    relational.create_table("entities", TableMeta::default());
    relational.create_table(
        "observations",
        TableMeta {
            immutable: true,
            ..TableMeta::default()
        },
    );
    relational.create_table(
        "invalidations",
        TableMeta {
            state_machine: Some(StateMachineConstraint {
                column: "status".to_string(),
                transitions: HashMap::from([
                    (
                        "pending".to_string(),
                        vec!["acknowledged".to_string(), "dismissed".to_string()],
                    ),
                    (
                        "acknowledged".to_string(),
                        vec!["resolved".to_string(), "dismissed".to_string()],
                    ),
                ]),
            }),
            ..TableMeta::default()
        },
    );
    let tx_mgr = Arc::new(TransactionManager::new(TestStore {
        relational: relational.clone(),
    }));
    let exec = MemRelationalExecutor::new(relational, tx_mgr.clone());
    (tx_mgr, exec)
}

#[test]
fn scan_respects_snapshot_visibility() {
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();
    exec.insert(
        tx,
        "entities",
        map(vec![("name", Value::Text("e1".into()))]),
    )
    .unwrap();

    let s0 = tx_mgr.snapshot();
    assert_eq!(exec.scan("entities", s0).unwrap().len(), 0);

    tx_mgr.commit(tx).unwrap();
    let s1 = tx_mgr.snapshot();
    assert_eq!(exec.scan("entities", s1).unwrap().len(), 1);
}

#[test]
fn rollback_hides_inserted_rows() {
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();
    exec.insert(
        tx,
        "entities",
        map(vec![("name", Value::Text("e1".into()))]),
    )
    .unwrap();
    tx_mgr.rollback(tx).unwrap();

    let snapshot = tx_mgr.snapshot();
    assert!(exec.scan("entities", snapshot).unwrap().is_empty());
}

#[test]
fn mvcc_snapshot_isolation() {
    let (tx_mgr, exec) = setup();

    let tx1 = tx_mgr.begin();
    exec.insert(
        tx1,
        "entities",
        map(vec![("name", Value::Text("v1".into()))]),
    )
    .unwrap();
    tx_mgr.commit(tx1).unwrap();

    let snap1 = tx_mgr.snapshot();

    let tx2 = tx_mgr.begin();
    exec.insert(
        tx2,
        "entities",
        map(vec![("name", Value::Text("v2".into()))]),
    )
    .unwrap();
    tx_mgr.commit(tx2).unwrap();

    assert_eq!(exec.scan("entities", snap1).unwrap().len(), 1);
    assert_eq!(exec.scan("entities", tx_mgr.snapshot()).unwrap().len(), 2);
}

#[test]
fn upsert_insert_update_noop() {
    let (tx_mgr, exec) = setup();

    let id = uuid::Uuid::new_v4();

    let tx1 = tx_mgr.begin();
    let r1 = exec
        .upsert(
            tx1,
            "entities",
            "id",
            map(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("a".into())),
            ]),
            tx_mgr.snapshot(),
        )
        .unwrap();
    tx_mgr.commit(tx1).unwrap();
    assert_eq!(r1, UpsertResult::Inserted);
    let created_snapshot = tx_mgr.snapshot();
    let created_row_id = exec
        .point_lookup("entities", "id", &Value::Uuid(id), created_snapshot)
        .unwrap()
        .expect("upserted row exists")
        .row_id;

    let tx2 = tx_mgr.begin();
    let r2 = exec
        .upsert(
            tx2,
            "entities",
            "id",
            map(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("b".into())),
            ]),
            tx_mgr.snapshot(),
        )
        .unwrap();
    tx_mgr.commit(tx2).unwrap();
    assert_eq!(r2, UpsertResult::Updated);
    let created_version = exec
        .point_lookup("entities", "id", &Value::Uuid(id), created_snapshot)
        .unwrap()
        .expect("the pre-update snapshot still resolves the created version");
    assert_eq!(created_version.row_id, created_row_id);
    assert_eq!(
        created_version.values.get("name"),
        Some(&Value::Text("a".into()))
    );
    let updated_version = exec
        .point_lookup("entities", "id", &Value::Uuid(id), tx_mgr.snapshot())
        .unwrap()
        .expect("updated row exists");
    assert_eq!(
        updated_version.row_id, created_row_id,
        "an upsert update preserves the row's internal identity"
    );
    assert_eq!(
        updated_version.values.get("name"),
        Some(&Value::Text("b".into()))
    );

    let tx3 = tx_mgr.begin();
    let r3 = exec
        .upsert(
            tx3,
            "entities",
            "id",
            map(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("b".into())),
            ]),
            tx_mgr.snapshot(),
        )
        .unwrap();
    tx_mgr.commit(tx3).unwrap();
    assert_eq!(r3, UpsertResult::NoOp);
}

#[test]
fn observations_are_immutable_for_delete_and_upsert() {
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();

    let del_err = exec.delete(tx, "observations", RowId(1)).unwrap_err();
    assert!(matches!(del_err, Error::ImmutableTable(ref t) if t == "observations"));

    let upsert_err = exec
        .upsert(
            tx,
            "observations",
            "id",
            map(vec![("id", Value::Uuid(uuid::Uuid::new_v4()))]),
            tx_mgr.snapshot(),
        )
        .unwrap_err();
    assert!(matches!(upsert_err, Error::ImmutableTable(ref t) if t == "observations"));
}

#[test]
fn invalidation_state_machine_enforced() {
    let (tx_mgr, exec) = setup();
    let id = uuid::Uuid::new_v4();

    let tx1 = tx_mgr.begin();
    exec.insert(
        tx1,
        "invalidations",
        map(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("pending".into())),
        ]),
    )
    .unwrap();
    tx_mgr.commit(tx1).unwrap();

    let tx2 = tx_mgr.begin();
    exec.upsert(
        tx2,
        "invalidations",
        "id",
        map(vec![
            ("id", Value::Uuid(id)),
            ("status", Value::Text("acknowledged".into())),
        ]),
        tx_mgr.snapshot(),
    )
    .unwrap();
    tx_mgr.commit(tx2).unwrap();

    let tx3 = tx_mgr.begin();
    let err = exec
        .upsert(
            tx3,
            "invalidations",
            "id",
            map(vec![
                ("id", Value::Uuid(id)),
                ("status", Value::Text("pending".into())),
            ]),
            tx_mgr.snapshot(),
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidStateTransition(_)));
}
