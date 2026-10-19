//! A passive `EXPLAIN` of an `AUTO` vector search predicts the route the
//! search actually takes. Both price the statement with the one AUTO cost
//! function, from the same per-partition inputs: allowed and live rows, the
//! serving degree, the saved layers still to load, and whether comparing a row
//! reads its stored body or a resident copy. A residual filter that makes a
//! walk dearer than comparing must be predicted as comparison, and a store
//! whose partitions are partly resident and partly streamed must be predicted
//! as executed.

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, VectorPartitionHnswDisclosure, VectorSearchDisclosure,
    VectorSearchRoute, VectorSearchRouteEvidence,
};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const PARTITIONS: i64 = 3;
/// Rows per partition; every second one matches the residual filter, so a
/// filtered statement allows 625 of each partition's 1,250 live rows.
const ROWS_PER_PARTITION: usize = 1_250;
const DIMENSION: usize = 8;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn declare(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE route_docs (id UUID PRIMARY KEY, partition_id INTEGER NOT NULL, \
             kind TEXT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (partition_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO)"
        ),
        &empty(),
    )
    .expect("declare the partitioned AUTO table");
    // The residual predicate is indexed so the filtered graph route is
    // eligible and AUTO's choice is a cost decision.
    db.execute(
        "CREATE INDEX route_docs_kind_idx ON route_docs(kind)",
        &empty(),
    )
    .expect("index the residual predicate");
    for partition in 0..PARTITIONS {
        for ordinal in 0..ROWS_PER_PARTITION {
            let seed = partition as usize * ROWS_PER_PARTITION + ordinal;
            let embedding = (0..DIMENSION)
                .map(|axis| {
                    let mixed = (seed as u64 + 7)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add((axis as u64 + 3).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                    ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 - 0.5
                })
                .collect::<Vec<_>>();
            db.execute(
                "INSERT INTO route_docs (id, partition_id, kind, embedding) \
                 VALUES ($id, $partition, $kind, $embedding)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Uuid(Uuid::from_u128(seed as u128 + 1)),
                    ),
                    ("partition".to_owned(), Value::Int64(partition)),
                    (
                        "kind".to_owned(),
                        Value::Text(
                            if ordinal % 2 == 0 {
                                "eligible"
                            } else {
                                "other"
                            }
                            .into(),
                        ),
                    ),
                    ("embedding".to_owned(), Value::Vector(embedding)),
                ]),
            )
            .expect("insert one deterministic row");
        }
    }
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS as usize && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish every partition graph");
}

/// Each partition's route, with its reason where the route is exact. A walk's
/// reason names its evidence (`predicted_indexed_route` before execution,
/// `maintained_graph_ready` after), so only an exact route's reason, the
/// statement-wide one, is the same on both sides.
fn routes(disclosure: &VectorSearchDisclosure) -> Vec<(Option<VectorSearchRoute>, Option<String>)> {
    let mut routes = disclosure
        .partition_hnsw
        .iter()
        .map(|partition: &VectorPartitionHnswDisclosure| {
            let reason = (partition.route == Some(VectorSearchRoute::Exact))
                .then(|| partition.reason.clone())
                .flatten();
            (partition.route, reason)
        })
        .collect::<Vec<_>>();
    routes.sort_by_key(|(route, reason)| (format!("{route:?}"), reason.clone()));
    routes
}

/// Explain the statement passively, execute it, and require the same route
/// for the statement and for every partition. Returns the executed route.
fn assert_passive_predicts_executed(db: &Database, filter: Option<&str>) -> VectorSearchRoute {
    let where_clause = filter.map_or(String::new(), |kind| format!("WHERE kind = '{kind}' "));
    let query = [0.1_f32; DIMENSION]
        .iter()
        .map(f32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT id FROM route_docs {where_clause}ORDER BY embedding <=> [{query}] LIMIT 10"
    );
    let passive = db
        .explain_output(&sql)
        .expect("passive explain resolves the AUTO route")
        .vector_search
        .expect("passive explain discloses the vector route");
    assert_eq!(
        passive.route_evidence,
        VectorSearchRouteEvidence::PassivePrediction
    );
    let executed = db
        .execute(&sql, &empty())
        .expect("the AUTO search executes")
        .trace
        .vector_search
        .expect("the executed search discloses its route");
    assert_eq!(executed.route_evidence, VectorSearchRouteEvidence::Executed);
    assert_eq!(
        passive.route, executed.route,
        "the passive route is the executed route ({filter:?}): passive {passive} executed {executed}"
    );
    assert_eq!(
        routes(&passive),
        routes(&executed),
        "every partition's passive route (and an exact route's reason) is its executed one ({filter:?})"
    );
    executed
        .route
        .expect("an executed search resolves its route")
}

#[test]
fn passive_explain_predicts_an_unfiltered_resident_auto_route() {
    let db = Database::open_memory();
    declare(&db);
    // Every live row allowed: the walk is cheaper than comparing 3,750 rows,
    // and both say so.
    assert!(matches!(
        assert_passive_predicts_executed(&db, None),
        VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed
    ));
}

#[test]
fn passive_explain_predicts_a_filtered_resident_auto_route() {
    let db = Database::open_memory();
    declare(&db);
    // A 50% residual filter: the walk would navigate the other half too, so
    // comparing the 1,875 allowed rows is cheaper and execution compares
    // them. The prediction must say the same, which needs the passive count
    // of allowed rows to apply the filter as execution does.
    assert_eq!(
        assert_passive_predicts_executed(&db, Some("eligible")),
        VectorSearchRoute::Exact
    );
}

#[test]
fn passive_explain_predicts_a_mixed_stored_and_resident_auto_route() {
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("routes.redb");
    {
        let db = Database::open(&path).expect("open the durable fixture");
        declare(&db);
        db.close().expect("close the maintained fixture");
    }
    // Reopened, every partition streams its bodies from the store; partition
    // 1 is then loaded whole, so the scope is mixed.
    let db = Database::open(&path).expect("reopen without hydrating raw bodies");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let index = VectorIndexRef::new("route_docs", "embedding");
    let streamed = VectorPartitionKey::from_values(&[Value::Int64(0)])
        .expect("an integer is a partition identity");
    let resident = VectorPartitionKey::from_values(&[Value::Int64(1)])
        .expect("an integer is a partition identity");
    let mut load_owner = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition load reader"),
    );
    db.vector_store_for_test()
        .ensure_raw_partition_loaded(&index, &resident, &mut load_owner)
        .expect("load partition 1's bodies whole");
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &streamed),
        "partition 0 streams its bodies from the store"
    );
    assert!(
        db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &resident),
        "partition 1 holds its bodies resident"
    );
    assert_passive_predicts_executed(&db, None);
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &streamed),
        "the prediction and the search leave the streamed partition streamed"
    );
}
