//! A find through the owner's own Rust doors is one search, like the same find
//! in SQL.
//!
//! `Database::semantic_search` and `Database::query_vector` answer the same
//! question a SQL find asks: the k nearest rows over every partition the call
//! admits. A partition's snapshot-compatible graph is up to three layers -- the
//! sealed base, the sealed change beside it and the live tail committed since
//! -- and the question is answered by ONE search: one candidate budget spent
//! best-first over one frontier and one result heap across every non-empty
//! layer of every admitted partition. A door that walks each partition and each
//! layer on its own spends a whole breadth per layer per partition, and answers
//! a different page than the SQL find over the same snapshot.
//!
//! Every cell here asks both owner doors and the SQL find the same question
//! over four Contexts that each carry a saved base, a sealed change and a live
//! tail, and requires one union search per door call, every non-empty layer
//! opened, and the SQL find's page.

use contextdb_core::types::VectorPartitionKey;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use contextdb_vector::VectorPartitionRef;
use contextdb_vector::hnsw::{UnionSearchReceipt, with_union_search_receipts};
use std::collections::HashMap;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
const CONTEXTS: usize = 4;
const BASE_ROWS: usize = 300;
/// Past the tenth of the base the table declares as the point a change is
/// sealed, so every Context seals these into a change graph.
const CHANGE_ROWS: usize = 60;
/// Committed after the change is sealed and below the point the next one
/// would be: a live tail no maintenance consumes.
const TAIL_ROWS: usize = 12;
const LIMIT: usize = 10;
const ID_SLOT: usize = 1_000;
const LAYERS_PER_CONTEXT: usize = 3;

/// The row nearest the change query, only in the second Context's change.
const CHANGE_NEAREST: (usize, usize) = (2, BASE_ROWS);
/// The row nearest the tail query, only in the first Context's live tail.
const TAIL_NEAREST: (usize, usize) = (1, BASE_ROWS + CHANGE_ROWS);
/// Rows nearer each query than anything current, but not current.
const NOT_CURRENT_CHANGE_ROW: (usize, usize) = (3, BASE_ROWS + 1);
const NOT_CURRENT_TAIL_ROW: (usize, usize) = (3, BASE_ROWS + CHANGE_ROWS + 1);

fn context_id(context: usize) -> Uuid {
    Uuid::from_u128(0xD00C_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}

fn row_id((context, local): (usize, usize)) -> i64 {
    (context * ID_SLOT + local + 1) as i64
}

fn noise(ordinal: usize, axis: usize) -> f32 {
    let seed = (ordinal
        .wrapping_mul(0x9e37_79b9)
        .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
        % 10_007;
    (seed as f32 / 10_007.0) - 0.5
}

fn around(direction: usize, spread: f32, ordinal: usize) -> Vec<f32> {
    let mut vector = (0..DIMENSIONS)
        .map(|axis| spread * noise(ordinal, axis))
        .collect::<Vec<_>>();
    vector[direction] += 1.0;
    vector
}

fn scattered(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS).map(|axis| noise(ordinal, axis)).collect()
}

fn axis(direction: usize) -> Vec<f32> {
    around(direction, 0.0, 0)
}

fn row(context: usize, local: usize) -> (bool, Vec<f32>) {
    let ordinal = context * ID_SLOT + local;
    let at = (context, local);
    if at == CHANGE_NEAREST {
        return (true, around(0, 0.02, ordinal));
    }
    if at == TAIL_NEAREST {
        return (true, around(1, 0.02, ordinal));
    }
    if at == NOT_CURRENT_CHANGE_ROW {
        return (false, around(0, 0.004, ordinal));
    }
    if at == NOT_CURRENT_TAIL_ROW {
        return (false, around(1, 0.004, ordinal));
    }
    if local < BASE_ROWS {
        return (true, around(local % 2, 0.35, ordinal));
    }
    (true, scattered(ordinal))
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("door_rows", "embedding")
}

fn partition(context: usize) -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[Value::Uuid(context_id(context))])
            .expect("a context is a partition key"),
    )
}

fn insert(db: &Database, context: usize, locals: std::ops::Range<usize>) {
    for local in locals {
        let (current, vector) = row(context, local);
        db.execute(
            "INSERT INTO door_rows VALUES ($id, $context, $current, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(row_id((context, local)))),
                ("context".into(), Value::Uuid(context_id(context))),
                ("current".into(), Value::Bool(current)),
                ("embedding".into(), Value::Vector(vector)),
            ]),
        )
        .expect("seed one layered row");
    }
}

fn drive_maintenance(db: &Database, until: impl Fn(&Database) -> bool, what: &str) {
    let reached = (0..4096).any(|_| {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        until(db)
    });
    assert!(reached, "{what}");
}

/// Every Context saved as a base, a sealed change on top of it, and a live
/// tail committed after the change. The column declares INDEXED, so the door
/// that cannot name a mode asks for the graph as well.
fn seed(db: &Database) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE door_rows (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             is_current BOOL NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) \
             MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
             CONSOLIDATION (CHANGE_PERCENT = 10))"
        ),
        &HashMap::new(),
    )
    .expect("declare the layered fixture");
    db.execute(
        "CREATE INDEX door_rows_current_idx ON door_rows (is_current)",
        &HashMap::new(),
    )
    .expect("declare the filter index");
    for context in 0..CONTEXTS {
        insert(db, context, 0..BASE_ROWS);
    }
    drive_maintenance(
        db,
        |db| {
            let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
                db.declared_reader_memory()
                    .expect("admit the partition observation reader"),
            );

            let ready = db
                .vector_store_for_test()
                .partition_infos(&index(), &mut partition_observation_owner_1)
                .expect("inspect partition readiness");
            ready.len() == CONTEXTS && ready.iter().all(|info| info.graph_available)
        },
        "every Context saves its base graph",
    );
    for context in 0..CONTEXTS {
        insert(db, context, BASE_ROWS..BASE_ROWS + CHANGE_ROWS);
    }
    drive_maintenance(
        db,
        |db| {
            let store = db.vector_store_for_test();
            (0..CONTEXTS).all(|context| {
                store
                    .partition_graph_generation_status(&partition(context))
                    .is_some_and(|status| {
                        (status.base.is_some() || status.dormant_base.is_some())
                            && (status.change.is_some() || status.dormant_change.is_some())
                            && status.fresh_tail_entries == 0
                    })
            })
        },
        "every Context seals its later rows into a change graph on top of its base",
    );
    let tail = BASE_ROWS + CHANGE_ROWS;
    for context in 0..CONTEXTS {
        insert(db, context, tail..tail + TAIL_ROWS);
    }
    let store = db.vector_store_for_test();
    for context in 0..CONTEXTS {
        let status = store
            .partition_graph_generation_status(&partition(context))
            .expect("every seeded Context has a partition");
        let layers = usize::from(status.base.is_some() || status.dormant_base.is_some())
            + usize::from(status.change.is_some() || status.dormant_change.is_some())
            + 1;
        let suffix = db
            .inspect_captured_vector_suffix_for_test(
                &partition(context),
                db.snapshot(),
                db.declared_reader_memory()
                    .expect("admit the suffix reader"),
                &mut || Ok(()),
            )
            .expect("inspect the captured suffix")
            .expect("each seeded Context has a committed suffix");
        assert!(suffix.points >= TAIL_ROWS);
        assert_eq!(status.fresh_tail_entries, 0);
        assert!(
            store
                .with_partition_fresh_tail(&partition(context), |graph| graph.len())
                .is_none()
        );
        assert_eq!(
            layers, LAYERS_PER_CONTEXT,
            "the fixture gives Context {context} a base, a change and a live tail"
        );
    }
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("an id column holds an integer, not {other:?}"),
        })
        .collect()
}

/// One union search per call, admitting every Context and opening every
/// non-empty layer, inside its one budget.
fn check_receipts(
    failures: &mut Vec<String>,
    door: &str,
    cell: &str,
    receipts: &[UnionSearchReceipt],
) {
    let expected_layers = CONTEXTS * LAYERS_PER_CONTEXT;
    if receipts.len() != 1 {
        failures.push(format!(
            "{door} {cell}: one union search answers the call, but {} ran -- the Contexts \
             were walked one partition and one layer at a time",
            receipts.len()
        ));
        return;
    }
    let receipt = receipts[0];
    if receipt.partitions != CONTEXTS || receipt.layers_admitted != expected_layers {
        failures.push(format!(
            "{door} {cell}: the one search admits all {CONTEXTS} Contexts and opens every \
             non-empty layer -- {expected_layers} of base, change and tail: {receipt:?}"
        ));
    }
    if receipt.expansion_evaluations > receipt.budget {
        failures.push(format!(
            "{door} {cell}: the walk stays inside its one budget: {receipt:?}"
        ));
    }
}

#[test]
fn the_owner_doors_run_one_search_over_every_layer_and_answer_what_sql_answers() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let db = Database::open(directory.path().join("owner-doors.db")).expect("open the fixture");
    seed(&db);
    let mut failures = Vec::<String>::new();

    let mut answers = Vec::<(String, Vec<i64>, Vec<i64>)>::new();

    for (cell, query, filtered) in [
        ("an open query", scattered(999_983), false),
        ("nearest in a change", axis(0), true),
        ("nearest in a tail", axis(1), true),
    ] {
        let predicate = if filtered {
            "WHERE is_current = TRUE "
        } else {
            ""
        };
        let sql = format!(
            "SELECT id FROM door_rows {predicate}ORDER BY embedding <=> $query \
             USE VECTOR INDEXED LIMIT {LIMIT}"
        );
        let params = HashMap::from([("query".to_string(), Value::Vector(query.clone()))]);
        let (sql_answer, sql_receipts) = with_union_search_receipts(|| db.execute(&sql, &params));
        let sql_ids = ids(&sql_answer.expect("the SQL find answers"));
        check_receipts(&mut failures, "SQL", cell, &sql_receipts);

        let mut semantic = SemanticQuery::new("door_rows", "embedding", query.clone(), LIMIT);
        if filtered {
            semantic.where_clause = Some("is_current = TRUE".to_string());
        }
        let (semantic_answer, semantic_receipts) =
            with_union_search_receipts(|| db.semantic_search(semantic.clone()));
        let semantic_answer = semantic_answer.expect("semantic search answers");
        let semantic_ids = semantic_answer
            .iter()
            .map(|result| result.values["id"].as_i64().expect("an integer id"))
            .collect::<Vec<_>>();
        println!(
            "{cell}: sql={sql_ids:?} sql_receipts={sql_receipts:?} semantic={semantic_ids:?} \
             semantic_receipts={semantic_receipts:?}"
        );
        check_receipts(&mut failures, "semantic_search", cell, &semantic_receipts);
        answers.push((
            format!("semantic_search {cell}"),
            semantic_ids,
            sql_ids.clone(),
        ));

        if !filtered {
            // The door that takes no filter and no mode: every partition, the
            // column's declared INDEXED route.
            let (vector_answer, vector_receipts) = with_union_search_receipts(|| {
                db.query_vector(index(), &query, LIMIT, None, db.snapshot())
            });
            let vector_rows = vector_answer.expect("query_vector answers");
            println!("{cell}: query_vector={vector_rows:?} receipts={vector_receipts:?}");
            check_receipts(&mut failures, "query_vector", cell, &vector_receipts);
            let semantic_rows = semantic_answer
                .iter()
                .map(|result| (result.row_id, result.vector_score))
                .collect::<Vec<_>>();
            if vector_rows != semantic_rows {
                failures.push(format!(
                    "query_vector {cell}: answers the same rows and scores as the other owner \
                     door and the SQL find: {vector_rows:?} against {semantic_rows:?}"
                ));
            }
        }
    }
    for (door, answered, sql) in answers {
        if answered != sql {
            failures.push(format!(
                "{door}: answers what the SQL find over the same snapshot answers: \
                 {answered:?} against {sql:?}"
            ));
        }
        for forbidden in [NOT_CURRENT_CHANGE_ROW, NOT_CURRENT_TAIL_ROW] {
            if door.contains("nearest") && answered.contains(&row_id(forbidden)) {
                failures.push(format!(
                    "{door}: row {} is not current and never enters the answer: {answered:?}",
                    row_id(forbidden)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
