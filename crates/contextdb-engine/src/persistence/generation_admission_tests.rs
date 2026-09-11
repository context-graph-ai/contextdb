use super::*;
use crate::{Database, MaintenancePolicy};

#[test]
fn an_older_catalog_charge_is_recomputed_before_graph_decode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("older-charge.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 24, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for id in 0..32 {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.run_maintenance_cycle().unwrap();
    let before = db.vector_memory_ownership_receipt_for_test().base_graph;
    db.close().unwrap();
    drop(db);
    {
        let file = redb::Database::open(&path).unwrap();
        let write = file.begin_write().unwrap();
        {
            let mut table = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .unwrap();
            let (key_guard, value) = table.iter().unwrap().next().unwrap().unwrap();
            let key = key_guard.value().to_vec();
            drop(key_guard);
            let mut catalog: VectorPartitionGenerationCatalogRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                )
                .unwrap();
            drop(value);
            catalog.base.resident_bytes = 1;
            let bytes = RedbPersistence::encode_vector_partition_record(
                VectorPartitionRecordKind::GenerationCatalog,
                &catalog,
            )
            .unwrap();
            table.insert(key.as_slice(), bytes.as_slice()).unwrap();
        }
        write.commit().unwrap();
    }
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let used = db.accountant().usage().used;
    db.set_memory_limit(Some(used + 4096)).unwrap();
    let sql = "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3";
    assert!(matches!(
        db.execute(sql, &HashMap::new()),
        Err(Error::MemoryBudgetExceeded { .. })
    ));
    assert_eq!(db.vector_memory_ownership_receipt_for_test().base_graph, 0);
    db.set_memory_limit(None).unwrap();
    assert_eq!(db.execute(sql, &HashMap::new()).unwrap().rows.len(), 3);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test().base_graph,
        before
    );
    eprintln!(
        "catalog_declared_bytes=1 resolved_topology_charge={before} refused_before_decode=true retry=ok"
    );
}
