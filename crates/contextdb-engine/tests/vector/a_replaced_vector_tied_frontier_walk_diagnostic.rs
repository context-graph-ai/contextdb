//! Diagnostic sibling of `a_replaced_vector_cannot_end_a_tied_frontier`: the
//! same fixture, reporting how each partition walk of each reader route ended
//! instead of asserting it. Run explicitly with `--ignored --nocapture`.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use contextdb_vector::hnsw::{with_union_search_receipts, with_union_walk_traces};
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
#[ignore = "diagnostic: run explicitly with --ignored"]
fn a_replaced_vector_tied_frontier_walk_diagnostic() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().expect("task-scoped graph fixture");
    let path = root.path().join("replaced-tie.db");
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
    for (table, quantization) in [
        ("stale_f32", "F32"),
        ("stale_sq8", "SQ8"),
        ("nearer_f32", "F32"),
        ("nearer_sq8", "SQ8"),
    ] {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, bucket INT NOT NULL, \
                 embedding VECTOR(3) WITH (quantization = '{quantization}') \
                 PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
                 HNSW (M = 8, EF_CONSTRUCTION = 64, EF_SEARCH = 32))"
            ),
            &HashMap::new(),
        )
        .expect("declare two graph partitions");
        let tx = db.begin_or_panic();
        // The third coordinate connects a graph path without ordering tied
        // scores by row id. Row 120 is initially nearer than every tied row.
        for id in 1..=122_i64 {
            let embedding = match id {
                120 => vec![1.0, 0.01, 0.0],
                121 => vec![1.0, 0.25, 0.0],
                122 => vec![1.0, 0.5, 0.0],
                _ => vec![0.0, 1.0, ((id * 11) % 121) as f32 / 120.0],
            };
            db.execute_in_tx(
                tx,
                &format!("INSERT INTO {table} VALUES ($id, $bucket, $embedding)"),
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(id % 2)),
                    ("embedding".into(), Value::Vector(embedding)),
                ]),
            )
            .expect("insert one graph vector");
        }
        db.commit(tx).expect("commit graph vectors");
    }
    let ready = (0..512).any(|_| {
        db.run_maintenance_cycle()
            .expect("advance graph maintenance");
        ["stale_f32", "stale_sq8", "nearer_f32", "nearer_sq8"]
            .into_iter()
            .all(|table| {
                let mut partition_observation_owner_1 =
                    contextdb_engine::vector_read_memory::admission(
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
    assert!(ready, "all tables publish saved graphs before replacement");
    for table in ["stale_f32", "stale_sq8", "nearer_f32", "nearer_sq8"] {
        if table.starts_with("nearer_") {
            // Its saved base point is far from the query. The replacement is
            // current only in the tail until a later maintenance publication.
            db.execute(
                &format!("UPDATE {table} SET embedding = $embedding WHERE id = 119"),
                &HashMap::from([("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("replace a farther graph point with the nearest current vector");
        } else {
            for id in [1_i64, 2, 120] {
                db.execute(
                    &format!("UPDATE {table} SET embedding = $embedding WHERE id = $id"),
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("embedding".into(), Value::Vector(vec![-1.0, 0.0, 0.0])),
                    ]),
                )
                .expect("replace a stored vector after graph publication");
            }
        }
    }

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
    let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let owner =
        ReadSession::open_owner_only_in_runtime_dir(&path, options.clone(), Some(runtime.clone()))
            .expect("open owner route");
    assert_eq!(owner.route(), ReadRoute::Owner);
    let mut failures = Vec::new();
    for table in ["stale_f32", "stale_sq8", "nearer_f32", "nearer_sq8"] {
        let sql = format!(
            "SELECT id FROM {table} WHERE bucket IN (0, 1) \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
        );
        let exact = db
            .execute(&sql.replace("INDEXED", "EXACT"), &params)
            .expect("authoritative current-vector oracle");
        let expected = if table.starts_with("nearer_") {
            [119, 120, 121]
        } else {
            [121, 122, 3]
        };
        assert_eq!(ids(&exact), expected, "{table} fixture oracle");
        let ((owner_read, owner_receipts), traces) = with_union_walk_traces(0, || {
            with_union_search_receipts(|| owner.execute(&sql, &params))
        });
        report("owner", table, &owner_receipts, &traces);
        match owner_read {
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
    .expect("open committed file route");
    assert_eq!(file.route(), ReadRoute::File);
    for (table, expected) in [
        ("stale_f32", [121, 122, 3]),
        ("nearer_f32", [119, 120, 121]),
    ] {
        let sql = format!(
            "SELECT id FROM {table} WHERE bucket IN (0, 1) \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
        );
        let ((read, receipts), traces) = with_union_walk_traces(0, || {
            with_union_search_receipts(|| file.execute(&sql, &params))
        });
        report("file", table, &receipts, &traces);
        assert_eq!(receipts.len(), 1, "{table} searches the saved layers once");
        let search = receipts[0];
        assert_eq!(search.terminal, "answer_complete", "{table}");

        assert!(
            search.admission_evaluations + search.expansion_evaluations <= search.budget,
            "{table} stays within the existing graph search allowance"
        );
        if table == "nearer_f32" {
            assert!(
                search.layers_admitted >= 3,
                "the replacement tail and older graph layers serve one search"
            );
        }
        match read {
            Ok(indexed) if ids(&indexed) == expected => {}
            Ok(indexed) => failures.push(format!(
                "file {table}: got {:?}; trace {:?}",
                ids(&indexed),
                indexed.trace.vector_search,
            )),
            Err(error) => failures.push(format!("file {table}: {error}")),
        }
    }
    eprintln!("failures: {failures:#?}");
}

fn report(
    route: &str,
    table: &str,
    receipts: &[contextdb_vector::hnsw::UnionSearchReceipt],
    traces: &[contextdb_vector::hnsw::UnionWalkTrace],
) {
    for receipt in receipts {
        eprintln!(
            "{route} {table}: terminal={} frontier_exhausted={} admission={} expansion={} budget={} layers={} partitions={}",
            receipt.terminal,
            receipt.frontier_exhausted,
            receipt.admission_evaluations,
            receipt.expansion_evaluations,
            receipt.budget,
            receipt.layers_admitted,
            receipt.partitions,
        );
    }
    for trace in traces {
        let mut nearest: Vec<_> = trace
            .evaluated
            .iter()
            .map(|entry| (entry.distance, entry.row_id))
            .collect();
        nearest.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        nearest.truncate(5);
        eprintln!(
            "  {route} {table} partition {}: expansions={} evaluated={} stop={:?} nearest={nearest:?}",
            trace.partition,
            trace.expansions,
            trace.evaluated.len(),
            trace.stop,
        );
    }
}
