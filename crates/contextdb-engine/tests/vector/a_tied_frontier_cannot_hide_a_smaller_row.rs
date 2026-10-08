//! Equal-score neighbours behind a larger-id frontier still participate in
//! the canonical top-k, on either committed reader route.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;

fn ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("expected integer row id, got {other:?}"),
        })
        .collect()
}

#[cfg(unix)]
#[test]
fn a_tied_frontier_cannot_hide_a_smaller_row_on_either_reader_route() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("task-scoped tied graph fixture");
    let path = root.path().join("tied-frontier.db");
    let runtime = root.path().join("runtime");
    std::fs::create_dir(&runtime).expect("owner runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("private owner runtime directory");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open fixture owner");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for (table, reverse) in [("ties_forward", false), ("ties_reverse", true)] {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, bucket INT NOT NULL, \
                 embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 \
                 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
                 HNSW (M = 4, EF_CONSTRUCTION = 64, EF_SEARCH = 32))"
            ),
            &HashMap::new(),
        )
        .expect("declare two tied graph partitions");
        let tx = db.begin_or_panic();
        // Every vector has distance one from the query. The permuted third
        // coordinate gives the graph a connected neighbour path whose
        // distance ties do not order its canonical row IDs. Reversing the
        // buckets changes which partition first offers the smallest IDs.
        for id in 1..=120_i64 {
            let bucket = if reverse { 1 - id % 2 } else { id % 2 };
            db.execute_in_tx(
                tx,
                &format!("INSERT INTO {table} VALUES ($id, $bucket, $embedding)"),
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(bucket)),
                    (
                        "embedding".into(),
                        Value::Vector(vec![0.0, 1.0, ((id * 11) % 121) as f32 / 120.0]),
                    ),
                ]),
            )
            .expect("insert one tied vector");
        }
        db.commit(tx).expect("commit tied vectors");
    }
    let ready = (0..512).any(|_| {
        db.run_maintenance_cycle()
            .expect("advance graph maintenance");
        ["ties_forward", "ties_reverse"].into_iter().all(|table| {
            let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
                db.declared_reader_memory()
                    .expect("admit the partition observation reader"),
            );

            db.vector_store_for_test()
                .partition_infos(
                    &VectorIndexRef::new(table, "embedding"),
                    &mut partition_observation_owner_1,
                )
                .expect("inspect graph readiness")
                .iter()
                .filter(|info| info.graph_available)
                .count()
                == 2
        })
    });
    assert!(ready, "both partition orders publish two saved graphs");

    let limits = ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    };
    let options = ReadSessionOptions {
        limits,
        memory_limit: Some(limits.memory as usize),
        ..ReadSessionOptions::default()
    };
    let owner =
        ReadSession::open_owner_only_in_runtime_dir(&path, options.clone(), Some(runtime.clone()))
            .expect("open the owner reader route");
    assert_eq!(owner.route(), ReadRoute::Owner);
    let mut failures = Vec::new();
    for table in ["ties_forward", "ties_reverse"] {
        let sql = format!(
            "SELECT id FROM {table} WHERE bucket IN (0, 1) \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
        );
        let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let exact = db
            .execute(&sql.replace("INDEXED", "EXACT"), &params)
            .expect("authoritative exhaustive tie oracle");
        assert_eq!(ids(&exact), [1, 2, 3]);
        match owner.execute(&sql, &params) {
            Ok(indexed) if ids(&indexed) == ids(&exact) => {}
            Ok(indexed) => failures.push(format!(
                "owner {table}: expected {:?}, got {:?}; trace {:?}",
                ids(&exact),
                ids(&indexed),
                indexed.trace.vector_search,
            )),
            Err(error) => failures.push(format!("owner {table}: {error}")),
        }
    }
    drop(owner);
    db.close().expect("publish and release owner");
    let file = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(&path, options)
    })
    .expect("open committed file reader route");
    assert_eq!(file.route(), ReadRoute::File);
    for table in ["ties_forward", "ties_reverse"] {
        let sql = format!(
            "SELECT id FROM {table} WHERE bucket IN (0, 1) \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
        );
        let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        match file.execute(&sql, &params) {
            Ok(indexed) if ids(&indexed) == [1, 2, 3] => {}
            Ok(indexed) => failures.push(format!(
                "file {table}: got {:?}; trace {:?}",
                ids(&indexed),
                indexed.trace.vector_search,
            )),
            Err(error) => failures.push(format!("file {table}: {error}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
