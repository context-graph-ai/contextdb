//! Two-sided public score diagnostic. Identical vectors/query on both engines;
//! only the index declaration differs, supplied explicitly by the runner.
#![cfg(feature = "test-seams")]
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy, SemanticQuery};
use std::collections::HashMap;
fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
#[test]
fn public_score_observations_for_every_search_route() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        &root.path().join("score.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    let native =
        std::env::var("INDEX_PORTS_SCORE_ENGINE").unwrap_or_else(|_| "native".into()) == "native";
    let declaration = if native {
        "SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 8.00)"
    } else {
        "HNSW (M = 16, EF_CONSTRUCTION = 200, EF_SEARCH = 64)"
    };
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(96) SEARCH_MODE INDEXED {declaration})"),&empty()).unwrap();
    let vectors = (1..=5)
        .map(|id| {
            (
                id,
                (0..96)
                    .map(|c| (((c as u64 * 31 + id * 17) % 1021) as f32 / 1021.) - 0.5)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    for (id, v) in &vectors {
        db.execute(
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(*id as i64)),
                ("v".into(), Value::Vector(v.clone())),
            ]),
        )
        .unwrap();
    }
    loop {
        let report = db.run_maintenance_cycle().unwrap();
        if report.vector.first_failure.is_some() {
            panic!("maintenance refused: {:?}", report.vector);
        }
        if report.vector.remaining_partitions == 0 && report.vector.ready_partitions > 0 {
            break;
        }
    }
    let q = (0..96)
        .map(|c| (((c * 23 + 5) % 1013) as f32 / 1013.) - 0.5)
        .collect::<Vec<_>>();
    let params = HashMap::from([("q".into(), Value::Vector(q.clone()))]);
    for (id, v) in &vectors {
        let dot = v
            .iter()
            .zip(&q)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>();
        let aa = v.iter().map(|a| f64::from(*a).powi(2)).sum::<f64>();
        let bb = q.iter().map(|b| f64::from(*b).powi(2)).sum::<f64>();
        let exact = dot / (aa.sqrt() * bb.sqrt());
        println!(
            "BODY id={id} cosine_f32={:.17} cosine_f32_bits={} cosine_f64={exact:.17} distance_f64={:.17}",
            contextdb_vector::cosine_similarity(v, &q),
            contextdb_vector::cosine_similarity(v, &q).to_bits(),
            1. - exact
        );
    }
    for (name, mode) in [
        ("INDEXED", VectorSearchMode::Indexed),
        ("AUTO", VectorSearchMode::Auto),
        ("EXACT", VectorSearchMode::Exact),
    ] {
        let sql = format!(
            "SELECT id, score FROM items ORDER BY embedding <=> $q USE VECTOR {name} LIMIT 5"
        );
        let result = db.execute(&sql, &params).unwrap();
        println!(
            "PUBLIC_SQL engine={} mode={name} columns={:?} rows={:?}",
            if native { "native" } else { "hnsw" },
            result.columns(),
            result.rows()
        );
        let mut query = SemanticQuery::new("items", "embedding", q.clone(), vectors.len());
        query.search_mode = Some(mode);
        match db.semantic_search(query) {
            Ok(rust) => {
                for r in rust {
                    println!(
                        "PUBLIC_RUST engine={} mode={name} values={:?} score={:.17}",
                        if native { "native" } else { "hnsw" },
                        r.values.get("id"),
                        r.vector_score
                    );
                }
            }
            Err(error) => println!(
                "PUBLIC_RUST_REFUSAL engine={} mode={name} error={error:?}",
                if native { "native" } else { "hnsw" }
            ),
        }
    }
    println!(
        "PUBLIC_ZERO_DIMENSION engine={} result={:?}",
        if native { "native" } else { "hnsw" },
        db.execute(
            "CREATE TABLE zero_dimension_items (id INT PRIMARY KEY, v VECTOR(0))",
            &empty()
        )
    );
    let zero = HashMap::from([("v".into(), Value::Vector(Vec::new()))]);
    println!(
        "PUBLIC_ZERO_INSERT engine={} result={:?}",
        if native { "native" } else { "hnsw" },
        db.execute("INSERT INTO zero_dimension_items VALUES (1,$v)", &zero)
    );
    let zero_result = db.execute(
        "SELECT id, score FROM zero_dimension_items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
        &zero,
    );
    match zero_result {
        Ok(r) => println!(
            "PUBLIC_ZERO_EXACT engine={} rows={:?}",
            if native { "native" } else { "hnsw" },
            r.rows()
        ),
        Err(e) => println!(
            "PUBLIC_ZERO_EXACT engine={} error={e:?}",
            if native { "native" } else { "hnsw" }
        ),
    };
    db.close().unwrap();
}
