//! A find names the (Context, generation) pairs it searches as a row-value
//! list, and pays for those pairs only.
//!
//! `(context_id, generation_id) IN (($c1, $g1), ($c2, $g2))` is the standard
//! SQL row-value predicate for a list of pairs. Two independent `IN` lists mean
//! their cross product, which is correct SQL the engine keeps honouring, but it
//! is not what a caller naming pairs means, and planning it costs one index run
//! per combination -- the square of the Context count, nearly all of them empty.
//! These proofs hold the engine to three things: the row-value list selects
//! exactly the listed rows under SQL's three-valued comparison; a filtered find
//! that has to prove its candidates builds one authenticated window per named
//! pair that holds postings, and none for a pair with no rows; and a find whose
//! filter admits every live vector of the pairs it names enumerates nothing.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::os::unix::fs::PermissionsExt;

const CONTEXTS: usize = 4;
const GENERATIONS: usize = 2;
const ROWS_PER_PAIR: usize = 60;
const LANES: usize = 3;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;
/// A non-current row planted in pair (0, 0) whose embedding IS the query, so
/// any route that forgets `is_current` publishes it first.
const PLANTED_ROW_ID: i64 = 1_000_000;

fn context_id(context: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xF700_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}

/// Generation identities are shared across Contexts on purpose: then every
/// combination of a named Context with a named generation holds rows, and a
/// predicate read as a cross product answers differently from one read as the
/// listed pairs.
fn generation_id(generation: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xF700_1000_0000_0000_0000_0000_0000_0000 + generation as u128)
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
        .map(|axis| ((axis.wrapping_mul(0x27d4_eb2f) % 10_009) as f32 / 10_009.0) - 0.5)
        .collect()
}

struct Row {
    id: i64,
    context: usize,
    generation: usize,
    lane: i64,
    current: bool,
    vector: Vec<f32>,
}

fn rows() -> Vec<Row> {
    let mut rows = Vec::new();
    let mut ordinal = 0usize;
    for context in 0..CONTEXTS {
        for generation in 0..GENERATIONS {
            for local in 0..ROWS_PER_PAIR {
                ordinal += 1;
                rows.push(Row {
                    id: ordinal as i64,
                    context,
                    generation,
                    lane: (local % LANES) as i64,
                    current: true,
                    vector: embedding(ordinal),
                });
            }
        }
    }
    rows
}

fn planted() -> Row {
    Row {
        id: PLANTED_ROW_ID,
        context: 0,
        generation: 0,
        lane: 0,
        current: false,
        vector: query_vector(),
    }
}

fn seed(db: &Database, plant: bool) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE lane_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, lane INT NOT NULL, \
         embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 16 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    db.execute(
        "CREATE INDEX lane_rows_active_idx ON lane_rows (context_id, generation_id, is_current, lane)",
        &HashMap::new(),
    )
    .expect("declare the index the filter is served from");
    let mut all = rows();
    if plant {
        all.push(planted());
    }
    for row in all {
        db.execute(
            "INSERT INTO lane_rows VALUES ($id, $context, $generation, $current, $lane, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(row.id)),
                ("context".into(), Value::Uuid(context_id(row.context))),
                ("generation".into(), Value::Uuid(generation_id(row.generation))),
                ("current".into(), Value::Bool(row.current)),
                ("lane".into(), Value::Int64(row.lane)),
                ("embedding".into(), Value::Vector(row.vector)),
            ]),
        )
        .expect("seed one row");
    }
    for _ in 0..512 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = db
            .vector_store_for_test()
            .partition_infos(
                &VectorIndexRef::new("lane_rows", "embedding"),
                &mut partition_observation_owner_1,
            )
            .expect("inspect partition readiness");
        if ready.len() == CONTEXTS * GENERATIONS
            && ready.iter().all(|partition| partition.graph_available)
        {
            return;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

struct Fixture {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    runtime: std::path::PathBuf,
}

fn sealed_fixture(plant: bool) -> Fixture {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("pairs.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    seed(&admin, plant);
    admin.close().expect("seal the fixture store");
    Fixture {
        _directory: directory,
        path,
        runtime,
    }
}

/// A fresh reader over the committed file image, authorized for every
/// Context: the route a consumer reads through, and the only one that proves
/// its candidates through authenticated windows.
fn reader(fixture: &Fixture) -> ReadSession {
    let reader = ReadSession::with_runtime_directory_for_test(&fixture.runtime, || {
        ReadSession::open_with_options(
            &fixture.path,
            ReadSessionOptions {
                contexts: Some(
                    (0..CONTEXTS)
                        .map(|context| ContextId::new(context_id(context)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: ReadLimits {
                    work: 5_000_000,
                    memory: 512 * 1024 * 1024,
                    ..ReadLimits::default()
                },
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    reader
}

/// The pair list as SQL and its bindings.
fn pair_list(pairs: &[(usize, usize)], params: &mut HashMap<String, Value>) -> String {
    let listed = pairs
        .iter()
        .enumerate()
        .map(|(position, (context, generation))| {
            params.insert(format!("c{position}"), Value::Uuid(context_id(*context)));
            params.insert(
                format!("g{position}"),
                Value::Uuid(generation_id(*generation)),
            );
            format!("($c{position}, $g{position})")
        })
        .collect::<Vec<_>>();
    format!("(context_id, generation_id) IN ({})", listed.join(", "))
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f32 {
    let dot: f32 = left.iter().zip(right).map(|(a, b)| a * b).sum();
    let left_norm: f32 = left.iter().map(|a| a * a).sum::<f32>().sqrt();
    let right_norm: f32 = right.iter().map(|a| a * a).sum::<f32>().sqrt();
    1.0 - dot / (left_norm * right_norm)
}

/// The ten nearest rows among those the statement admits, computed from the
/// inserted vectors alone.
fn oracle(admitted: impl Fn(&Row) -> bool, plant: bool) -> Vec<i64> {
    let query = query_vector();
    let mut all = rows();
    if plant {
        all.push(planted());
    }
    let mut scored = all
        .into_iter()
        .filter(|row| admitted(row))
        .map(|row| (cosine_distance(&query, &row.vector), row.id))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    scored.into_iter().take(LIMIT).map(|(_, id)| id).collect()
}

struct Measured {
    ids: Vec<i64>,
    runs_built: u64,
    postings_walked: u64,
    reads_opened: u64,
    prefix_probes: u64,
    proof_nodes_read: u64,
    proof_nodes_hashed: u64,
    ordered_proof_nodes_read: u64,
}

fn find(reader: &ReadSession, sql: &str, params: &HashMap<String, Value>) -> Measured {
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = reader
        .execute(sql, params)
        .unwrap_or_else(|error| panic!("the read session answers `{sql}`: {error:?}"));
    let counters = work.counters();
    drop(work);
    Measured {
        ids: answer
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("the projection returns one integer id, got {other:?}"),
            })
            .collect(),
        runs_built: counters.verified_runs_built,
        postings_walked: counters.verified_postings_walked,
        reads_opened: counters.verified_reads_opened,
        prefix_probes: counters.verified_prefix_probes,
        proof_nodes_read: counters.proof_nodes_read,
        proof_nodes_hashed: counters.proof_nodes_hashed,
        ordered_proof_nodes_read: counters.ordered_proof_nodes_read,
    }
}

fn sorted(mut ids: Vec<i64>) -> Vec<i64> {
    ids.sort_unstable();
    ids
}

/// SQL's row comparison, on both executors: a listed row matches only as a
/// whole, a NULL element makes the comparison unknown rather than false, and
/// the column order of the row value is the caller's, not the index's.
#[test]
fn a_row_value_list_selects_exactly_the_listed_pairs() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("row-values.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
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
    .expect("open the fixture store");
    for (table, indexed) in [("pairs_indexed", true), ("pairs_plain", false)] {
        db.execute(
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, a INT, b INT, flag BOOL NOT NULL)"),
            &HashMap::new(),
        )
        .expect("declare the pair table");
        if indexed {
            db.execute(
                &format!("CREATE INDEX {table}_idx ON {table} (a, b, flag)"),
                &HashMap::new(),
            )
            .expect("declare the pair index");
        }
        let mut id = 0i64;
        for a in 0..3i64 {
            for b in [Some(0i64), Some(1), Some(2), None] {
                id += 1;
                db.execute(
                    &format!("INSERT INTO {table} VALUES ($id, $a, $b, TRUE)"),
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("a".into(), Value::Int64(a)),
                        ("b".into(), b.map_or(Value::Null, Value::Int64)),
                    ]),
                )
                .expect("seed one pair");
            }
        }
    }
    // Row ids: a * 4 + position of b in [0, 1, 2, NULL] + 1.
    let id = |a: i64, b: Option<i64>| a * 4 + b.unwrap_or(3) + 1;
    let every = (0..3)
        .flat_map(|a| [Some(0), Some(1), Some(2), None].map(|b| id(a, b)))
        .collect::<Vec<_>>();
    let params = HashMap::from([
        ("a0".into(), Value::Int64(0)),
        ("b0".into(), Value::Int64(1)),
        ("a1".into(), Value::Int64(2)),
        ("b1".into(), Value::Int64(2)),
    ]);
    let cases: Vec<(&str, Vec<i64>)> = vec![
        (
            "(a, b) IN ((0, 1), (2, 2))",
            vec![id(0, Some(1)), id(2, Some(2))],
        ),
        (
            "(a, b) IN (($a0, $b0), ($a1, $b1))",
            vec![id(0, Some(1)), id(2, Some(2))],
        ),
        ("(b, a) IN ((1, 0))", vec![id(0, Some(1))]),
        (
            "(a, b) IN ((0, 1), (2, 2)) AND flag = TRUE",
            vec![id(0, Some(1)), id(2, Some(2))],
        ),
        // (0, NULL) against (0, 1) is unknown, so NOT IN leaves it out;
        // (1, NULL) differs on `a`, which is known, so NOT IN keeps it.
        (
            "(a, b) NOT IN ((0, 1))",
            every
                .iter()
                .copied()
                .filter(|row| *row != id(0, Some(1)) && *row != id(0, None))
                .collect(),
        ),
        ("(a, b) IN ((NULL, 1))", Vec::new()),
    ];
    for table in ["pairs_indexed", "pairs_plain"] {
        for (predicate, expected) in &cases {
            let sql = format!("SELECT id FROM {table} WHERE {predicate}");
            let answer = db
                .execute(&sql, &params)
                .unwrap_or_else(|error| panic!("`{sql}` answers: {error:?}"));
            let ids = answer
                .rows
                .iter()
                .map(|row| match row.first() {
                    Some(Value::Int64(id)) => *id,
                    other => panic!("one integer id, got {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                sorted(ids),
                sorted(expected.clone()),
                "{sql} on the writable handle"
            );
        }
    }
    db.close().expect("seal the fixture store");
    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(&path, ReadSessionOptions::default())
    })
    .expect("open the committed file image");
    for table in ["pairs_indexed", "pairs_plain"] {
        for (predicate, expected) in &cases {
            let sql = format!("SELECT id FROM {table} WHERE {predicate}");
            let answer = reader
                .execute(&sql, &params)
                .unwrap_or_else(|error| panic!("`{sql}` answers on the reader: {error:?}"));
            let ids = answer
                .rows
                .iter()
                .map(|row| match row.first() {
                    Some(Value::Int64(id)) => *id,
                    other => panic!("one integer id, got {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                sorted(ids),
                sorted(expected.clone()),
                "{sql} on the read session"
            );
        }
    }
}

/// A filtered find that must prove its candidates builds one authenticated
/// window per named pair that holds postings: never one per combination of the
/// named Contexts and generations, and none for a named pair with no rows. The
/// postings it walks are the listed pairs' own, and its answer is theirs.
#[test]
fn a_filtered_find_builds_one_window_per_named_pair() {
    let fixture = sealed_fixture(false);
    // (0, 0) and (1, 1) hold rows; (0, 1) and (1, 0) hold rows too but are not
    // named; Context 3's generation 5 does not exist at all.
    let named = [(0usize, 0usize), (1, 1), (3, 5)];
    let mut params = HashMap::from([
        ("query".into(), Value::Vector(query_vector())),
        ("is_current".into(), Value::Bool(true)),
        ("lane".into(), Value::Int64(1)),
    ]);
    let pairs = pair_list(&named, &mut params);
    let sql = format!(
        "SELECT id FROM lane_rows WHERE {pairs} AND is_current = $is_current AND lane = $lane \
         ORDER BY embedding <=> $query LIMIT {LIMIT}"
    );
    let measured = find(&reader(&fixture), &sql, &params);
    let expected = oracle(
        |row| row.current && row.lane == 1 && named.contains(&(row.context, row.generation)),
        false,
    );
    assert_eq!(
        measured.ids, expected,
        "the find answers from the named pairs only"
    );
    let lane_postings = (ROWS_PER_PAIR / LANES) as u64;
    assert_eq!(
        (measured.runs_built, measured.postings_walked),
        (2, 2 * lane_postings),
        "a find naming three pairs, one of them empty, builds a window for each pair that holds \
         postings and walks only their postings"
    );
    assert_eq!(
        measured.reads_opened, 1,
        "every window of the statement reconciles through one read and one opened index"
    );
}

/// Several windows of two distinct pairs reuse their authenticated upper
/// paths under one pinned statement, even when window payloads are evicted.
#[test]
fn authenticated_windows_share_one_bounded_proof_batch() {
    use contextdb_engine::persistence::read_persistence_test_scaffold::with_verified_ordered_window_bytes_for_test;

    let fixture = sealed_fixture(false);
    let named = [(0usize, 0usize), (1, 1)];
    let mut params = HashMap::from([
        ("query".into(), Value::Vector(query_vector())),
        ("is_current".into(), Value::Bool(true)),
        ("lane".into(), Value::Int64(1)),
    ]);
    let pairs = pair_list(&named, &mut params);
    let sql = format!(
        "SELECT id FROM lane_rows WHERE {pairs} AND is_current = $is_current AND lane = $lane \
         ORDER BY embedding <=> $query LIMIT {LIMIT}"
    );
    let measured =
        with_verified_ordered_window_bytes_for_test(256, || find(&reader(&fixture), &sql, &params));
    let expected = oracle(
        |row| row.current && row.lane == 1 && named.contains(&(row.context, row.generation)),
        false,
    );
    assert_eq!(measured.ids, expected);
    assert!(
        measured.runs_built >= 4,
        "the test exercises several windows: {}",
        measured.runs_built
    );
    assert_eq!(measured.postings_walked, 40);
    assert_eq!(measured.reads_opened, 1);
    assert!(
        measured.ordered_proof_nodes_read <= 12,
        "the shared upper paths are decoded and hashed once: windows={} ordered_nodes_read={} all_nodes_read={} all_nodes_hashed={}",
        measured.runs_built,
        measured.ordered_proof_nodes_read,
        measured.proof_nodes_read,
        measured.proof_nodes_hashed,
    );
}

/// A find whose filter admits every live vector of the pairs it names proves
/// that without walking a single posting: its answer is the whole partitions'
/// nearest rows, and it builds no window at any scope.
#[test]
fn a_find_whose_filter_admits_whole_pairs_enumerates_nothing() {
    let fixture = sealed_fixture(false);
    for scope in [1usize, 2, 4, CONTEXTS * GENERATIONS] {
        let named = (0..scope)
            .map(|position| (position / GENERATIONS, position % GENERATIONS))
            .collect::<Vec<_>>();
        let mut params = HashMap::from([
            ("query".into(), Value::Vector(query_vector())),
            ("is_current".into(), Value::Bool(true)),
        ]);
        let pairs = pair_list(&named, &mut params);
        let sql = format!(
            "SELECT id FROM lane_rows WHERE {pairs} AND is_current = $is_current \
             ORDER BY embedding <=> $query LIMIT {LIMIT}"
        );
        let measured = find(&reader(&fixture), &sql, &params);
        let expected = oracle(
            |row| row.current && named.contains(&(row.context, row.generation)),
            false,
        );
        assert_eq!(
            measured.ids, expected,
            "{scope} named pairs answer their nearest rows"
        );
        assert_eq!(
            (
                measured.runs_built,
                measured.postings_walked,
                measured.reads_opened
            ),
            (0, 0, 0),
            "a find over {scope} whole pairs enumerates nothing"
        );
        // One proof per named pair that `is_current` rejects nothing there.
        assert_eq!(
            measured.prefix_probes, scope as u64,
            "the whole-pair proof costs one authenticated descent per named pair"
        );
    }
}

/// The shortcut is taken only when it is proved. A non-current row inside a
/// named pair means the filter does NOT admit the whole pair, so the find
/// proves its candidates instead -- and the planted row, nearer than anything
/// else, never reaches the page, which stays full.
#[test]
fn a_pair_holding_a_row_the_filter_rejects_is_proved_not_assumed() {
    let fixture = sealed_fixture(true);
    let named = [(0usize, 0usize), (2, 1)];
    let mut params = HashMap::from([
        ("query".into(), Value::Vector(query_vector())),
        ("is_current".into(), Value::Bool(true)),
    ]);
    let pairs = pair_list(&named, &mut params);
    let sql = format!(
        "SELECT id FROM lane_rows WHERE {pairs} AND is_current = $is_current \
         ORDER BY embedding <=> $query LIMIT {LIMIT}"
    );
    let measured = find(&reader(&fixture), &sql, &params);
    let expected = oracle(
        |row| row.current && named.contains(&(row.context, row.generation)),
        true,
    );
    assert!(!measured.ids.contains(&PLANTED_ROW_ID));
    assert_eq!(measured.ids, expected, "a full page of current rows");
    assert!(
        measured.runs_built > 0,
        "a pair with a rejected row is proved through its windows"
    );
}
