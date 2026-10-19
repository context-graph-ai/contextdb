//! What the approved single-search budget does to a find over one large
//! Context, measured per query rather than assumed.
//!
//! One Context of 12,500 vectors of 768 dimensions, the undeclared breadth
//! profile, `LIMIT 10`, read through a real `ReadSession` on the file route.
//! For every held-out query it records the budget the search was given, what
//! opening the layers and the walk spent, how the search ended, whether the
//! find was refused, and recall@10 against the exhaustive answer on the same
//! stored representation -- on the maintained route asked for explicitly and
//! on AUTO.
//!
//! Run with `--run-ignored ignored-only --no-capture`. The fixture is seeded
//! once at a stable path under this build's scratch directory and reused.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use contextdb_vector::hnsw::with_union_search_receipts;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Instant;
use uuid::Uuid;

const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;

fn rows() -> usize {
    std::env::var("CONTEXTDB_F11_BUDGET_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12_500)
}

fn queries() -> usize {
    std::env::var("CONTEXTDB_F11_BUDGET_QUERIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(25)
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC04B_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04B_1000_0000_0000_0000_0000_0000_0000)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSIONS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

/// A query no stored row is: the corpus occupies ordinals `1..=rows`.
fn held_out_query(ordinal: usize) -> Vec<f32> {
    embedding(10_000_000 + ordinal)
}

fn find_sql(mode: &str) -> String {
    format!(
        "SELECT id FROM passages WHERE context_id IN ($context) AND \
         generation_id IN ($generation) AND is_current = $is_current \
         ORDER BY embedding <=> $query {mode} LIMIT {LIMIT}"
    )
}

fn params(query: &[f32]) -> HashMap<String, Value> {
    HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query.to_vec())),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ])
}

fn answer_ids(answer: &contextdb_engine::QueryResult) -> Vec<i64> {
    answer
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path) {
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
        .execute(
            "CREATE TABLE passages (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR(768) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX passages_active_idx ON passages (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for ordinal in 0..rows() {
        admin
            .execute(
                "INSERT INTO passages VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id())),
                    ("generation".into(), Value::Uuid(generation_id())),
                    ("embedding".into(), Value::Vector(embedding(ordinal + 1))),
                ]),
            )
            .expect("seed one passage");
    }
    let index = VectorIndexRef::new("passages", "embedding");
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
        if !ready.is_empty() && ready.iter().all(|partition| partition.graph_available) {
            admin.close().expect("seal the fixture store");
            return;
        }
    }
    panic!("the partition must publish a maintained route before the read is measured");
}

/// The breadth the current process declares on the column before measuring:
/// `CONTEXTDB_F11_BUDGET_EF=<n>` declares `EF_SEARCH = n`, `default` returns
/// the column to the undeclared profile, and leaving it unset keeps whatever
/// the fixture already declares.
fn declared_breadth() -> Option<String> {
    std::env::var("CONTEXTDB_F11_BUDGET_EF").ok()
}

#[cfg(unix)]
fn declare_breadth(path: &std::path::Path, runtime: &std::path::Path, breadth: &str) {
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
    .expect("open the fixture to declare its breadth");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let value = if breadth.eq_ignore_ascii_case("default") {
        "DEFAULT".to_owned()
    } else {
        breadth
            .parse::<usize>()
            .expect("CONTEXTDB_F11_BUDGET_EF is a breadth or `default`")
            .to_string()
    };
    admin
        .execute(
            &format!("ALTER TABLE passages ALTER COLUMN embedding SET HNSW (EF_SEARCH = {value})"),
            &HashMap::new(),
        )
        .expect("declare the search breadth");
    admin.close().expect("seal the fixture");
}

/// Nearest-rank percentile of an ascending slice.
fn percentile<T: Copy + Default>(sorted: &[T], fraction: f64) -> T {
    if sorted.is_empty() {
        return T::default();
    }
    let position = (fraction * (sorted.len() - 1) as f64).round() as usize;
    sorted[position.min(sorted.len() - 1)]
}

fn spread(values: &mut [usize]) -> String {
    values.sort_unstable();
    format!(
        "{}/{}/{}",
        percentile(values, 0.50),
        percentile(values, 0.95),
        values.last().copied().unwrap_or(0)
    )
}

fn p95_ms(values: &mut [f64]) -> f64 {
    values.sort_by(|left, right| left.partial_cmp(right).expect("finite timings"));
    percentile(values, 0.95)
}

#[cfg(unix)]
#[test]
#[ignore = "diagnostic measurement: it seeds a 12,500 x 768 store (about \
            fifteen minutes in a debug build) and prints one row per held-out query. Every \
            number it prints is an observation; the budget itself is proven by \
            a_declared_breadth_gets_exactly_its_approved_budget."]
fn what_the_approved_budget_does_to_one_large_context() {
    use std::os::unix::fs::PermissionsExt;

    let root = std::path::PathBuf::from(format!(
        "{}/approved-budget/rows-{}",
        env!("CARGO_TARGET_TMPDIR"),
        rows()
    ));
    let path = root.join("one-partition.db");
    let runtime = root.join("runtime");
    if !path.exists() {
        std::fs::create_dir_all(&runtime).expect("the reader runtime directory");
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
            .expect("the runtime directory is private to its owner");
        let seeding = Instant::now();
        seed(&path, &runtime);
        println!(
            "fixture seeded rows={} dims={DIMENSIONS} seed_ms={:.0}",
            rows(),
            seeding.elapsed().as_secs_f64() * 1e3
        );
    }
    if let Some(breadth) = declared_breadth() {
        declare_breadth(&path, &runtime, &breadth);
    }
    let open_reader = |limits: ReadLimits| {
        let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
            ReadSession::open_with_options(
                &path,
                ReadSessionOptions {
                    contexts: Some(BTreeSet::from([ContextId::new(context_id())])),
                    limits,
                    ..ReadSessionOptions::default()
                },
            )
        })
        .expect("open the committed file image the consumer reads through");
        assert_eq!(reader.route(), ReadRoute::File);
        reader
    };

    let held_out: Vec<Vec<f32>> = (0..queries()).map(held_out_query).collect();
    let exact_sql = find_sql("USE VECTOR EXACT");
    // The ground truth is not what is measured, so it is not held to the
    // shipped time limit: a slow build must still be able to produce it.
    let truth: Vec<HashSet<i64>> = {
        let oracle = open_reader(ReadLimits {
            active_ms: 600_000,
            ..ReadLimits::default()
        });
        held_out
            .iter()
            .map(|query| {
                let answer = oracle
                    .execute(&exact_sql, &params(query))
                    .expect("the exhaustive route answers");
                answer_ids(&answer).into_iter().collect()
            })
            .collect()
    };
    // What is measured runs under the shipped limits, and every route is
    // timed on a second pass: the first pass over the same queries is a
    // discarded warm-up.
    let reader = open_reader(ReadLimits::default());

    let exact_p95 = {
        for query in &held_out {
            let _ = reader.execute(&exact_sql, &params(query));
        }
        let mut exact_ms = Vec::with_capacity(held_out.len());
        let mut exact_refusals = 0usize;
        for query in &held_out {
            let started = Instant::now();
            if reader.execute(&exact_sql, &params(query)).is_err() {
                exact_refusals += 1;
            }
            exact_ms.push(started.elapsed().as_secs_f64() * 1e3);
        }
        let p95 = p95_ms(&mut exact_ms);
        println!(
            "summary mode=EXACT rows={} queries={} refusals={exact_refusals} warm_p95_ms={p95:.1}",
            rows(),
            held_out.len()
        );
        p95
    };

    println!(
        "row mode query B admission expansion expansion_cap terminal refused recall_at_10 \
         route class fallback ms"
    );
    for (mode, clause) in [
        ("INDEXED", "USE VECTOR INDEXED"),
        ("AUTO", "USE VECTOR AUTO"),
    ] {
        let sql = find_sql(clause);
        for query in &held_out {
            let _ = reader.execute(&sql, &params(query));
        }
        let mut refusals = 0usize;
        let mut hits = 0usize;
        let mut answered = 0usize;
        let mut admissions = Vec::new();
        let mut expansions = Vec::new();
        let mut warm_ms = Vec::with_capacity(held_out.len());
        let mut walks = 0usize;
        let mut exact_fallbacks = 0usize;
        let mut exact_chosen = 0usize;
        let mut profile = String::from("unknown");
        let mut budget = 0usize;
        for (position, query) in held_out.iter().enumerate() {
            let started = Instant::now();
            let (answer, receipts) =
                with_union_search_receipts(|| reader.execute(&sql, &params(query)));
            let ms = started.elapsed().as_secs_f64() * 1e3;
            warm_ms.push(ms);
            let receipt = receipts.last().copied();
            if let Some(receipt) = receipt {
                admissions.push(receipt.admission_evaluations);
                expansions.push(receipt.expansion_evaluations);
                budget = receipt.budget;
            }
            let walked = receipt.is_some_and(|receipt| receipt.terminal != "refused");
            let (refused, recall, route, class, fallback) = match &answer {
                Ok(answer) => {
                    let found = answer_ids(answer)
                        .into_iter()
                        .filter(|id| truth[position].contains(id))
                        .count();
                    hits += found;
                    answered += 1;
                    let disclosure = answer.trace.vector_search.as_ref();
                    if let Some(partition) =
                        disclosure.and_then(|disclosure| disclosure.partition_hnsw.first())
                    {
                        profile = format!(
                            "hnsw_m={} ef_search={} ef_search_source={}",
                            partition.hnsw_m, partition.hnsw_ef_search, partition.ef_search_source
                        );
                    }
                    let route = disclosure
                        .and_then(|disclosure| disclosure.route)
                        .map_or("none".to_owned(), |route| route.as_str().to_owned());
                    let class = if !route.contains("exact") && walked {
                        walks += 1;
                        "walk"
                    } else if receipt.is_some() {
                        exact_fallbacks += 1;
                        "exact_fallback"
                    } else {
                        exact_chosen += 1;
                        "exact_chosen"
                    };
                    (
                        false,
                        found as f64 / LIMIT as f64,
                        route,
                        class,
                        disclosure
                            .and_then(|disclosure| disclosure.fallback.clone())
                            .unwrap_or_else(|| "none".to_owned()),
                    )
                }
                Err(error) => {
                    refusals += 1;
                    (
                        true,
                        0.0,
                        "refused".to_owned(),
                        "refused",
                        format!("{error}").chars().take(80).collect::<String>(),
                    )
                }
            };
            println!(
                "row {mode} {position} {} {} {} {} {} {refused} {recall:.2} {route} {class} \
                 {fallback:?} {ms:.1}",
                receipt.map_or(0, |receipt| receipt.budget),
                receipt.map_or(0, |receipt| receipt.admission_evaluations),
                receipt.map_or(0, |receipt| receipt.expansion_evaluations),
                receipt.map_or(0, |receipt| receipt.expansion_cap),
                receipt.map_or("no_union_search", |receipt| receipt.terminal),
            );
        }
        println!(
            "summary mode={mode} rows={} {profile} B={budget} queries={} refusals={refusals} \
             recall_all={:.3} recall_answered={:.3} admission_med_p95_max={} \
             expansion_med_p95_max={} warm_p95_ms={:.1} walk={walks} \
             exact_fallback={exact_fallbacks} exact_chosen={exact_chosen} \
             exact_warm_p95_ms={exact_p95:.1}",
            rows(),
            held_out.len(),
            hits as f64 / (held_out.len() * LIMIT) as f64,
            hits as f64 / (answered.max(1) * LIMIT) as f64,
            spread(&mut admissions),
            spread(&mut expansions),
            p95_ms(&mut warm_ms),
        );
    }
}
