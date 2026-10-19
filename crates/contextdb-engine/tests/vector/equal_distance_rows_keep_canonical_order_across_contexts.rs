//! Equal-distance vectors retain the smallest row ids across Contexts even
//! when the graph reaches their partitions in another order.

use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{Database, MaintenancePolicy, VectorSearchRoute};
use std::collections::HashMap;
use uuid::Uuid;

const LIMIT: usize = 3;
const PARTITIONS: usize = 3;

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xE4AD_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xE4AD_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn embedding(id: usize) -> Vec<f32> {
    match id % 4 {
        0 => vec![0.0, 1.0, 0.0],
        1 => vec![0.0, -1.0, 0.0],
        2 => vec![0.0, 0.0, 1.0],
        _ => vec![0.0, 0.0, -1.0],
    }
}

fn ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

#[test]
fn equal_distance_rows_keep_canonical_order_across_contexts() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for (table, reverse) in [("ties_reverse", true), ("ties_forward", false)] {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, context_id UUID NOT NULL, \
                 generation_id UUID NOT NULL, embedding VECTOR(3) \
                 PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 3 \
                 SEARCH_MODE AUTO AUTO_INDEX_AT 1)"
            ),
            &HashMap::new(),
        )
        .expect("declare the partitioned tie fixture");
        // Insert in row-id order. Only the assignment to Contexts varies:
        // reverse puts the smallest ids in the last graph visited.
        for id in 1..=30usize {
            let group = (id - 1) / 10;
            let partition = if reverse {
                PARTITIONS - 1 - group
            } else {
                group
            };
            db.execute(
                &format!("INSERT INTO {table} VALUES ($id, $context, $generation, $embedding)"),
                &HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    ("embedding".into(), Value::Vector(embedding(id))),
                ]),
            )
            .expect("seed one equal-distance row");
        }
    }
    let mut ready = false;
    for _ in 0..512 {
        db.run_maintenance_cycle()
            .expect("advance deterministic graph maintenance");
        if ["ties_reverse", "ties_forward"].into_iter().all(|table| {
            let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
                db.declared_reader_memory()
                    .expect("admit the partition observation reader"),
            );

            let ready = db
                .vector_store_for_test()
                .partition_infos(
                    &VectorIndexRef::new(table, "embedding"),
                    &mut partition_observation_owner_1,
                )
                .expect("inspect graph readiness");
            ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available)
        }) {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "all Contexts publish a graph before the routes are compared"
    );

    let mut params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let mut contexts = Vec::new();
    let mut generations = Vec::new();
    for partition in 0..PARTITIONS {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    for table in ["ties_reverse", "ties_forward"] {
        let sql = format!(
            "SELECT id FROM {table} WHERE context_id IN ({}) AND generation_id IN ({}) \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        );
        let indexed = db
            .execute(&sql, &params)
            .expect("the indexed route answers the complete scope");
        let exact = db
            .execute(&sql.replace("INDEXED", "EXACT"), &params)
            .expect("the exact route supplies the canonical tie oracle");
        assert_eq!(ids(&exact), vec![1, 2, 3]);
        assert!(matches!(
            indexed
                .trace
                .vector_search
                .as_ref()
                .and_then(|search| search.route),
            Some(VectorSearchRoute::Indexed | VectorSearchRoute::FilteredIndexed)
        ));
        assert_eq!(
            ids(&indexed),
            ids(&exact),
            "the {table} graph must retain the three smallest row ids among ties"
        );
    }
}
