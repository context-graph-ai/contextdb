//! The automatic mode must never cost more than the route a caller could have
//! asked for outright.
//!
//! `AUTO` exists so a consumer does not have to know which route is cheaper on
//! its own data. It decides per partition, and the decision it has always made
//! — compare the admitted rows exhaustively when there are few enough of them —
//! stops short of the case that costs the most: a filter that keeps so little
//! of a partition that the rows it admits no longer reach each other through
//! the graph. There a walk has to navigate the excluded rows between them,
//! which costs several times what comparing the admitted rows costs and answers
//! no better.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, VectorSearchRoute};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const PARTITIONS: usize = 4;
const ROWS_PER_PARTITION: usize = 500;
const DIMENSIONS: usize = 32;
const BUCKETS: i64 = 100;
const K: usize = 10;
const HELD_OUT: usize = 100;

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC04A_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC04A_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

/// Well separated on purpose: two rows at the same distance from a query are a
/// tie, and a tie that the two routes break differently reads as a recall miss
/// while both answers are equally right.
fn spread(role: u64, ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (ordinal as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64).wrapping_mul(0x85eb_ca6b_ba21_1f3d))
                .wrapping_add(role.wrapping_mul(0xc2b2_ae35_9d5e_e1b1))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn embedding(ordinal: usize) -> Vec<f32> {
    spread(1, ordinal)
}

fn query_vector(probe: usize) -> Vec<f32> {
    spread(2, probe)
}

fn fixture() -> Database {
    partitioned_fixture(PARTITIONS, ROWS_PER_PARTITION, "bucketed_vectors")
}

fn partitioned_fixture(partitions: usize, rows_per_partition: usize, table: &str) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             generation_id UUID NOT NULL, bucket INT NOT NULL, \
             embedding VECTOR(32) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 64 SEARCH_MODE AUTO)"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    db.execute(
        &format!("CREATE INDEX {table}_bucket_idx ON {table} (context_id, generation_id, bucket)"),
        &HashMap::new(),
    )
    .expect("declare the index the bucket filter is served from");
    for partition in 0..partitions {
        for local in 0..rows_per_partition {
            let ordinal = partition * rows_per_partition + local;
            db.execute(
                &format!(
                    "INSERT INTO {table} VALUES ($id, $context, $generation, $bucket, $embedding)"
                ),
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    ("bucket".into(), Value::Int64(local as i64 % BUCKETS)),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one bucketed vector");
        }
    }
    for _ in 0..2048 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
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
            .expect("inspect partition readiness");
        if ready.len() == partitions && ready.iter().all(|partition| partition.graph_available) {
            return db;
        }
    }
    panic!("every partition must publish a maintained route before the routes are compared");
}

struct Answered {
    rows: Vec<i64>,
    work_units: u64,
    route: Option<VectorSearchRoute>,
}

fn run(db: &Database, buckets: i64, mode: Option<&str>, probe: usize) -> Answered {
    run_over(db, "bucketed_vectors", PARTITIONS, buckets, mode, probe)
}

fn run_over(
    db: &Database,
    table: &str,
    partitions: usize,
    buckets: i64,
    mode: Option<&str>,
    probe: usize,
) -> Answered {
    let mut params = HashMap::from([("query".into(), Value::Vector(query_vector(probe)))]);
    let mut contexts = Vec::with_capacity(partitions);
    let mut generations = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    let mut kept = Vec::with_capacity(buckets as usize);
    for bucket in 0..buckets {
        let name = format!("bucket_{bucket}");
        params.insert(name.clone(), Value::Int64(bucket));
        kept.push(format!("${name}"));
    }
    let clause = match mode {
        Some(mode) => format!(" USE VECTOR {mode}"),
        None => String::new(),
    };
    let sql = format!(
        "SELECT id FROM {table} WHERE context_id IN ({}) AND generation_id IN ({}) \
         AND bucket IN ({}) ORDER BY embedding <=> $query{clause} LIMIT {K}",
        contexts.join(", "),
        generations.join(", "),
        kept.join(", "),
    );
    let request = bounded::BoundedReadRequest::new(sql, params, limits(), Arc::new(FrozenClock));
    let executed = bounded::execute(db, &request).expect("the bounded route arm succeeds");
    let route = executed
        .result
        .trace
        .vector_search
        .as_ref()
        .and_then(|search| search.route);
    Answered {
        rows: executed
            .result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("the projection returns one integer id, got {other:?}"),
            })
            .collect(),
        work_units: executed.telemetry.work_units,
        route,
    }
}

#[test]
fn in_memory_auto_accounts_for_comparison_and_admission_costs() {
    let db = fixture();
    let auto = run(&db, BUCKETS, None, 0);
    let indexed = run(&db, BUCKETS, Some("INDEXED"), 0);
    assert_eq!(
        auto.route, indexed.route,
        "four graphs cost less to walk than comparing all their resident rows"
    );
    assert_eq!(auto.rows, indexed.rows);

    let split = partitioned_fixture(16, 64, "split_cost");
    let auto = run_over(&split, "split_cost", 16, BUCKETS, None, 0);
    let exact = run_over(&split, "split_cost", 16, BUCKETS, Some("EXACT"), 0);
    assert_eq!(exact.route, Some(VectorSearchRoute::Exact));
    assert_eq!(
        auto.route,
        Some(VectorSearchRoute::Exact),
        "opening sixteen small graphs costs more than comparing the same resident rows"
    );
    assert_eq!(auto.rows, exact.rows);
}

#[test]
fn auto_never_costs_more_than_the_route_a_caller_could_have_asked_for() {
    let db = fixture();
    let mut failures = Vec::new();
    // One bucket in a hundred, one in ten, half, and all of them.
    for buckets in [1_i64, 10, 50, BUCKETS] {
        let admitted_per_partition = (ROWS_PER_PARTITION as i64 * buckets / BUCKETS) as usize;
        let mut matched = 0usize;
        let mut auto_work = 0u64;
        let mut exact_work = 0u64;
        let mut indexed_work = 0u64;
        for probe in 0..HELD_OUT {
            let auto = run(&db, buckets, None, probe);
            let exact = run(&db, buckets, Some("EXACT"), probe);
            let indexed = run(&db, buckets, Some("INDEXED"), probe);
            matched += auto
                .rows
                .iter()
                .filter(|row| exact.rows.contains(row))
                .count();
            auto_work += auto.work_units;
            exact_work += exact.work_units;
            indexed_work += indexed.work_units;
            assert_eq!(
                exact.rows.len(),
                K.min(admitted_per_partition * PARTITIONS),
                "the exhaustive route answers a full page at every selectivity"
            );
        }
        let cheapest = exact_work.min(indexed_work);
        let possible = HELD_OUT * K.min(admitted_per_partition * PARTITIONS);
        println!(
            "auto_route buckets={buckets} admitted_per_partition={admitted_per_partition} \
             auto_work={auto_work} exact_work={exact_work} indexed_work={indexed_work} \
             recall={matched}/{possible}"
        );
        if auto_work * 10 > cheapest * 11 {
            failures.push(format!(
                "with {admitted_per_partition} of {ROWS_PER_PARTITION} rows admitted per \
                 partition the automatic route charged {auto_work} work against {cheapest} for \
                 the cheaper route a caller could have asked for"
            ));
        }
        if matched * 100 < possible * 95 {
            failures.push(format!(
                "with {admitted_per_partition} of {ROWS_PER_PARTITION} rows admitted per \
                 partition the automatic route answers at recall@{K} {matched}/{possible}, \
                 under the required .95"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "the automatic route costs what the cheaper explicit route costs:\n{}",
        failures.join("\n")
    );
}

/// The same rows cost the same to search however many partitions they are split
/// across: a statement asks for ten rows once, not ten per partition.
#[test]
fn the_automatic_route_costs_the_same_however_the_scope_is_split() {
    const TOTAL: usize = 2_048;
    let mut failures = Vec::new();
    let mut costs = Vec::new();
    for partitions in [1_usize, 8, 64] {
        let table = format!("split_{partitions}");
        let db = partitioned_fixture(partitions, TOTAL / partitions, &table);
        let mut matched = 0usize;
        let mut indexed_matched = 0usize;
        let mut auto_work = 0u64;
        let mut exact_work = 0u64;
        let mut indexed_work = 0u64;
        for probe in 0..HELD_OUT {
            let auto = run_over(&db, &table, partitions, BUCKETS, None, probe);
            let exact = run_over(&db, &table, partitions, BUCKETS, Some("EXACT"), probe);
            let indexed = run_over(&db, &table, partitions, BUCKETS, Some("INDEXED"), probe);
            matched += auto
                .rows
                .iter()
                .filter(|row| exact.rows.contains(row))
                .count();
            indexed_matched += indexed
                .rows
                .iter()
                .filter(|row| exact.rows.contains(row))
                .count();
            auto_work += auto.work_units;
            exact_work += exact.work_units;
            indexed_work += indexed.work_units;
        }
        let cheapest = exact_work.min(indexed_work);
        let possible = HELD_OUT * K;
        println!(
            "auto_split partitions={partitions} rows_per_partition={} auto_work={auto_work} \
             exact_work={exact_work} indexed_work={indexed_work} auto_recall={matched}/{possible} \
             indexed_recall={indexed_matched}/{possible}",
            TOTAL / partitions,
        );
        if auto_work * 10 > cheapest * 11 {
            failures.push(format!(
                "{TOTAL} rows split {partitions} ways cost the automatic route {auto_work} work \
                 against {cheapest} for the cheaper route a caller could have asked for"
            ));
        }
        if matched * 100 < possible * 95 {
            failures.push(format!(
                "{TOTAL} rows split {partitions} ways answer at recall@{K} {matched}/{possible}, \
                 under the required .95"
            ));
        }
        if indexed_matched * 100 < possible * 95 {
            failures.push(format!(
                "{TOTAL} rows split {partitions} ways answer through the graph at \
                 recall@{K} {indexed_matched}/{possible}, under the required .95"
            ));
        }
        costs.push((partitions, auto_work));
    }
    let cheapest = costs.iter().map(|(_, work)| *work).min().unwrap_or(0);
    for (partitions, work) in &costs {
        if *work > cheapest.saturating_mul(2) {
            failures.push(format!(
                "{TOTAL} rows split {partitions} ways cost {work} work against {cheapest} for the \
                 cheapest arrangement of the same rows"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "one scope costs the same however it is split:\n{}",
        failures.join("\n")
    );
}
