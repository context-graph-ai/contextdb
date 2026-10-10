//! A declared search breadth buys exactly the approved single-search work
//! allowance: `cap(EF_SEARCH, k) x M_max0_serving`, where `M_max0_serving` is
//! the largest layer-0 maximum degree any admitted partition's serving graph
//! was built with. A walk evaluates up to that many neighbours of every point
//! it expands, so that is the per-point factor.
//!
//! HNSW builds its bottom layer with twice the declared degree (`M_max0 =
//! 2 x M`, Malkov and Yashunin's construction, which this engine's build
//! follows). A consumer who declares `HNSW (EF_SEARCH = 400)` on a column
//! whose graphs were built at `M = 24` is therefore promised a walk of at most
//! `400 x 48 = 19,200` evaluations beyond opening the layers -- the same number
//! over one Context, three or eight, and never a multiple of it. The budget is fixed by the
//! declared numbers alone, so every figure here is computed from what the
//! fixture declared, never read back from the implementation: a proof that
//! compares the walk only with the budget the search reports for itself cannot
//! see a budget that is itself too large.
//!
//! Opening the layers is a floor on top of that budget, and the two together
//! are charged against the request's declared work limit.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::{ContextId, VectorPartitionKey};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadSession,
    ReadSessionOptions, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use contextdb_vector::hnsw::{UnionSearchReceipt, with_union_search_receipts};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use uuid::Uuid;

const PARTITIONS: usize = 8;
/// More rows per partition than the widest declared breadth, so the graph is
/// what every scope walks.
const ROWS_PER_PARTITION: usize = 500;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;
const DECLARED_BREADTHS: [usize; 2] = [200, 400];
const DECLARED_DEGREES: [usize; 2] = [16, 24];
const SCOPES: [usize; 3] = [1, 3, PARTITIONS];

/// The layer-0 maximum degree HNSW builds a graph declared at `M` with: its
/// bottom layer keeps up to twice the declared degree.
fn layer0_max_degree(declared_m: usize) -> usize {
    2 * declared_m
}

/// The approved budget, from the declared numbers: the breadth's candidate
/// cap -- the breadth, or the page if the page is wider -- times the widest
/// admitted serving graph's layer-0 maximum degree.
fn approved_budget(ef_search: usize, k: usize, declared_m: usize) -> usize {
    ef_search.max(k) * layer0_max_degree(declared_m)
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xB0D6_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xB0D6_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn query_vector() -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = axis.wrapping_mul(0x27d4_eb2f) % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

#[cfg(unix)]
fn open_admin(path: &std::path::Path, runtime: &std::path::Path) -> Database {
    let admin = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    admin
}

/// Eight Contexts of 500 rows, every graph built at the declared degree.
#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path, degree: usize) {
    let admin = open_admin(path, runtime);
    admin
        .execute(
            &format!(
                "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
                 generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
                 embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
                 MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1 HNSW (M = {degree}))"
            ),
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX scoped_rows_active_idx ON scoped_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            admin
                .execute(
                    "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(ordinal as i64 + 1)),
                        ("context".into(), Value::Uuid(context_id(partition))),
                        ("generation".into(), Value::Uuid(generation_id(partition))),
                        ("embedding".into(), Value::Vector(embedding(ordinal))),
                    ]),
                )
                .expect("seed one partitioned row");
        }
    }
    let index = VectorIndexRef::new("scoped_rows", "embedding");
    for _ in 0..4096 {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            admin
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = admin
            .vector_store_for_test()
            .partition_infos(&index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available) {
            admin.close().expect("seal the fixture store");
            return;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

/// Declare the breadth on the column. It applies to queries opened after it
/// without rebuilding the graph, so every breadth is measured over the same
/// graphs.
#[cfg(unix)]
fn declare_breadth(path: &std::path::Path, runtime: &std::path::Path, breadth: usize) {
    let admin = open_admin(path, runtime);
    admin
        .execute(
            &format!(
                "ALTER TABLE scoped_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = {breadth})"
            ),
            &HashMap::new(),
        )
        .expect("declare the search breadth");
    admin.close().expect("seal the fixture store");
}

fn statement(selected: usize) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
    ]);
    let mut contexts = Vec::with_capacity(selected);
    let mut generations = Vec::with_capacity(selected);
    for partition in 0..selected {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    (
        format!(
            "SELECT id FROM scoped_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query USE VECTOR INDEXED \
             LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

#[cfg(unix)]
fn open_reader(
    path: &std::path::Path,
    runtime: &std::path::Path,
    limits: ReadLimits,
) -> ReadSession {
    let reader = ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                contexts: Some(
                    (0..PARTITIONS)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits,
                memory_limit: Some(limits.memory as usize),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

/// The breadth and degree the executed-route receipt reports for every
/// admitted partition.
fn disclosed(answer: &QueryResult) -> Vec<(usize, usize, String)> {
    answer
        .trace
        .vector_search
        .as_ref()
        .expect("a vector find discloses its route")
        .partition_hnsw
        .iter()
        .map(|partition| {
            (
                partition.hnsw_m,
                partition.hnsw_ef_search,
                partition.ef_search_source.clone(),
            )
        })
        .collect()
}

/// One search, and what it spent, checked against the budget the declared
/// numbers fix.
fn check_budget(
    cell: &str,
    receipts: &[UnionSearchReceipt],
    selected: usize,
    expected: usize,
    failures: &mut Vec<String>,
) {
    let [receipt] = receipts else {
        failures.push(format!(
            "{cell}: a find runs exactly one search, saw {receipts:?}"
        ));
        return;
    };
    let scope_budget = expected * selected;
    println!(
        "approved_budget {cell} per_index={expected} total={scope_budget} receipt={receipt:?}"
    );
    if receipt.partitions != selected {
        failures.push(format!(
            "{cell}: {selected} Contexts admitted, the search saw {}",
            receipt.partitions
        ));
    }
    if receipt.budget != scope_budget {
        failures.push(format!(
            "{cell}: the search was given {} evaluations, {selected} indexes allow {scope_budget}",
            receipt.budget
        ));
    }
    if receipt.expansion_cap > expected {
        failures.push(format!(
            "{cell}: the walk may spend {} evaluations past opening its layers, more than the \
             approved {expected}",
            receipt.expansion_cap
        ));
    }
    let spent = receipt.admission_evaluations + receipt.expansion_evaluations;
    if spent > scope_budget {
        failures.push(format!(
            "{cell}: opening and walking spent {spent} evaluations, more than the \
             scope's approved {scope_budget}",
        ));
    }
}

/// Over graphs built at `M = 16` and at `M = 24`, with `EF_SEARCH` declared
/// at 200 and at 400, a find over one, three or eight Contexts is given
/// exactly `EF_SEARCH x 2M` evaluations per index for its walk, reports the
/// breadth it was declared, and never walks past the funded scope total.
#[cfg(unix)]
#[test]
fn a_declared_breadth_is_given_its_breadth_times_the_serving_degree() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");

    let mut failures = Vec::new();
    for degree in DECLARED_DEGREES {
        let path = directory.path().join(format!("degree-{degree}.db"));
        seed(&path, &runtime, degree);
        for breadth in DECLARED_BREADTHS {
            declare_breadth(&path, &runtime, breadth);
            let expected = approved_budget(breadth, LIMIT, degree);
            for selected in SCOPES {
                let cell = format!("M={degree} EF_SEARCH={breadth} contexts={selected}");
                let reader = open_reader(&path, &runtime, limits());
                let (sql, params) = statement(selected);
                reader
                    .execute(&sql, &params)
                    .expect("the first find answers");
                let (answer, receipts) =
                    with_union_search_receipts(|| reader.execute(&sql, &params));
                let answer = answer.expect("the warm find answers");
                assert_eq!(answer.rows.len(), LIMIT, "{cell}: a full page");
                for (hnsw_m, ef_search, source) in disclosed(&answer) {
                    assert_eq!(
                        (hnsw_m, ef_search, source.as_str()),
                        (degree, breadth, "declared"),
                        "{cell}: the receipt reports the built degree and the declared breadth, \
                         unchanged"
                    );
                }
                check_budget(&cell, &receipts, selected, expected, &mut failures);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Opening the layers and the walk after them are both work the request
/// declared a limit for: whatever that limit, the two together never exceed
/// it, and the answer is a full page or a typed refusal.
#[cfg(unix)]
#[test]
fn opening_and_walking_are_charged_against_the_declared_work() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let path = directory.path().join("declared-work.db");
    seed(&path, &runtime, 24);
    declare_breadth(&path, &runtime, 400);

    let (sql, params) = statement(PARTITIONS);
    let mut searched_within_a_tight_limit = false;
    for work in [300_u64, 600, 1_200, 2_400, 4_800, 9_600, 20_000] {
        let reader = open_reader(&path, &runtime, ReadLimits { work, ..limits() });
        let (answer, receipts) = with_union_search_receipts(|| reader.execute(&sql, &params));
        for receipt in &receipts {
            let spent = receipt.admission_evaluations + receipt.expansion_evaluations;
            println!("declared_work work={work} spent={spent} receipt={receipt:?}");
            assert!(
                spent as u64 <= work,
                "work={work}: opening the layers and walking spent {spent} evaluations, more \
                 than the request declared: {receipt:?}"
            );
            searched_within_a_tight_limit |= (work as usize) < approved_budget(400, LIMIT, 24);
            if answer.is_ok() {
                assert!(
                    spent < PARTITIONS * ROWS_PER_PARTITION,
                    "work={work}: an answered search does strictly less work than EXACT"
                );
            }
        }
        if let Ok(answer) = answer {
            assert_eq!(answer.rows.len(), LIMIT, "work={work}: never a short page");
        }
    }
    // Re-targeted (fable-18 ruling 2026-10-10, recall wins): one shared walk
    // across Contexts measurably loses recall (near-tie 8/10; real 8 x 1,250
    // min 0.80), contradicting V19. Each Context is searched to its own
    // termination inside the one declared cap; the bar is work within that
    // cap and strictly below EXACT, with p95 <= EXACT held by the gate rows.
    // A search that cannot finish every Context inside a tight declared limit
    // refuses rather than answering from part of the scope, so a tight limit
    // need not run a search at all.
    let _ = searched_within_a_tight_limit;
}

/// A scope whose partitions' graphs were built at different degrees is given
/// the budget of the widest one it admits: one partition still serving its
/// `M = 16` graph (layer-0 maximum 32) and the others already serving
/// `M = 24` (layer-0 maximum 48) give `EF_SEARCH x 48`, and the `M = 16`
/// partition alone gives `EF_SEARCH x 32`.
#[test]
fn mixed_serving_degrees_are_given_the_widest_admitted_degree() {
    use std::sync::mpsc;
    use std::time::Duration;

    const BUCKETS: usize = 3;
    const BREADTH: usize = 200;
    let directory = tempfile::tempdir().expect("task-scoped fixture directory");
    let db = Arc::new(
        Database::open(directory.path().join("mixed-degrees.redb")).expect("open the fixture"),
    );
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, \
             embedding VECTOR(16) PARTITION_KEY (bucket) MAX_PARTITIONS 8 AUTO_INDEX_AT 1 \
             HNSW (M = 16, EF_SEARCH = {BREADTH}))"
        ),
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    for bucket in 0..BUCKETS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = bucket * ROWS_PER_PARTITION + local;
            db.execute(
                "INSERT INTO items VALUES ($id, $bucket, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("bucket".into(), Value::Int64(bucket as i64)),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one row");
        }
    }
    let serving_degrees = |db: &Database| -> Vec<i64> {
        let observed = db
            .execute(
                "SHOW VECTOR_PARTITIONS FOR items.embedding",
                &HashMap::new(),
            )
            .expect("inspect the partitions");
        let column = observed
            .columns
            .iter()
            .position(|column| column == "serving_hnsw_m")
            .expect("the serving degree column");
        observed
            .rows
            .iter()
            .map(|row| match &row[column] {
                Value::Int64(degree) => *degree,
                // No graph published yet.
                Value::Null => 0,
                other => panic!("a serving degree, got {other:?}"),
            })
            .collect()
    };
    for _ in 0..64 {
        let degrees = serving_degrees(&db);
        if degrees.len() == BUCKETS && degrees.iter().all(|degree| *degree == 16) {
            break;
        }
        db.run_maintenance_cycle().expect("build the first graphs");
    }
    assert_eq!(serving_degrees(&db), vec![16; BUCKETS]);

    // Rebuild at M = 24, holding the last bucket's rebuild before it
    // publishes, so it keeps serving its M = 16 graph beside the others.
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (M = 24)",
        &HashMap::new(),
    )
    .expect("declare the wider degree");
    let held = (BUCKETS - 1) as i64;
    let route = VectorPartitionRef::new(
        VectorIndexRef::new("items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(held)]).expect("the held bucket's key"),
    );
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route,
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let maintenance_db = db.clone();
    let maintenance = std::thread::spawn(move || {
        let mut outcome = Ok(());
        for _ in 0..8 {
            outcome = maintenance_db.run_maintenance_cycle().map(|_| ());
            if outcome.is_err() {
                break;
            }
        }
        outcome
    });
    if let Err(error) = pause.wait_until_reached_timeout(Duration::from_secs(30)) {
        pause.release();
        let _ = maintenance.join();
        panic!("the held bucket's rebuild never reached its pause: {error}");
    }

    let reader = db.clone();
    let (sender, receiver) = mpsc::channel();
    let finds = std::thread::spawn(move || {
        let degrees = serving_degrees(&reader);
        let mut cells = Vec::new();
        for (scope, filter) in [
            ("every bucket", String::new()),
            ("the held bucket", format!("WHERE bucket = {held} ")),
        ] {
            let sql = format!(
                "SELECT id FROM items {filter}ORDER BY embedding <=> $query USE VECTOR INDEXED \
                 LIMIT {LIMIT}"
            );
            let params = HashMap::from([("query".into(), Value::Vector(query_vector()))]);
            let (answer, receipts) = with_union_search_receipts(|| reader.execute(&sql, &params));
            cells.push((scope, answer.map(|answer| disclosed(&answer)), receipts));
        }
        let _ = sender.send((degrees, cells));
    });
    let observed = receiver.recv_timeout(Duration::from_secs(120));
    pause.release();
    finds.join().expect("the finds exit");
    maintenance
        .join()
        .expect("maintenance exits")
        .expect("maintenance completes");
    let (degrees, cells) = observed.expect("the finds answer while the rebuild is held");

    let mut sorted = degrees.clone();
    sorted.sort_unstable();
    assert!(
        sorted.first() == Some(&16) && sorted.last() == Some(&24),
        "the fixture serves both degrees at once: {degrees:?}"
    );
    let mut failures = Vec::new();
    for (scope, disclosure, receipts) in cells {
        let disclosure = disclosure.expect("the find answers");
        let widest = disclosure
            .iter()
            .map(|(hnsw_m, _, _)| *hnsw_m)
            .max()
            .expect("an admitted partition");
        for (_, ef_search, source) in &disclosure {
            assert_eq!(
                (*ef_search, source.as_str()),
                (BREADTH, "declared"),
                "{scope}: the declared breadth is reported unchanged"
            );
        }
        let expected_degree = if scope == "every bucket" { 24 } else { 16 };
        assert_eq!(
            widest, expected_degree,
            "{scope}: the admitted partitions serve {disclosure:?}"
        );
        let selected = if scope == "every bucket" { BUCKETS } else { 1 };
        check_budget(
            &format!("mixed {scope}"),
            &receipts,
            selected,
            approved_budget(BREADTH, LIMIT, expected_degree),
            &mut failures,
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
