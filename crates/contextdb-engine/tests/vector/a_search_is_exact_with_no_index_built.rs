//! This release ships exact vector search only: the clustered index stays in
//! the tree, switched off. Every passage is searchable the moment its import
//! commits, with exact results, and nothing builds, writes, reads or inspects
//! an index. INDEXED is refused by name at every door. An explicit EXACT on a
//! writable store still loads whole partitions, which the exact scan replaces,
//! so the memory proof here is on AUTO.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey, VectorSearchMode};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadSession,
    ReadSessionOptions, SemanticQuery, VectorSearchRoute,
};
use contextdb_vector::{VectorBuildObserver, with_vector_build_observer};
use std::collections::{BTreeSet, HashMap};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const REFUSAL: &str = "no vector index ships in this release; use EXACT or AUTO";
const AUTO_FALLBACK: &str =
    "no vector index ships in this release; AUTO_INDEX_AT and CALIBRATION_SAMPLES have no effect";
const READER_DEFAULT: usize = 16 * 1024 * 1024;
const DIMENSIONS: usize = 768;
const LARGE_PARTITION_ROWS: usize = 6_000;
const SMALL_PARTITION_ROWS: usize = 50;
const LIMIT: usize = 10;

fn none() -> HashMap<String, Value> {
    HashMap::new()
}

fn context(partition: usize) -> Uuid {
    Uuid::from_u128(0xE7AC_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation(partition: usize) -> Uuid {
    Uuid::from_u128(0xE7AC_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
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

/// (first ordinal, row count) of one partition.
fn partition_rows(partition: usize) -> (usize, usize) {
    match partition {
        0 => (0, LARGE_PARTITION_ROWS),
        n => (
            LARGE_PARTITION_ROWS + (n - 1) * SMALL_PARTITION_ROWS,
            SMALL_PARTITION_ROWS,
        ),
    }
}

fn is_current(ordinal: usize) -> bool {
    ordinal % 7 != 0
}

/// The independent oracle: the inserted vectors themselves, compared in full.
fn oracle(partitions: &[usize]) -> Vec<i64> {
    let query = query_vector();
    let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
    let mut scored = Vec::new();
    for partition in partitions {
        let (first, rows) = partition_rows(*partition);
        for ordinal in (first..first + rows).filter(|ordinal| is_current(*ordinal)) {
            let vector = embedding(ordinal);
            let dot = vector.iter().zip(&query).map(|(a, b)| a * b).sum::<f32>();
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            scored.push((ordinal as i64 + 1, dot / (norm * query_norm)));
        }
    }
    scored.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    scored.truncate(LIMIT);
    scored.into_iter().map(|(id, _)| id).collect()
}

fn secure_runtime(parent: &Path) -> PathBuf {
    let runtime = parent.join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    runtime
}

fn open(path: &Path, runtime: &Path) -> Database {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn seed_passages(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE passages (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1)"
        ),
        &none(),
    )
    .expect("declare the passages");
    db.execute(
        "CREATE INDEX passages_scope ON passages (context_id, generation_id, is_current)",
        &none(),
    )
    .expect("declare the scope index");
    for partition in 0..3 {
        let (first, rows) = partition_rows(partition);
        let ordinals = (first..first + rows).collect::<Vec<_>>();
        for batch in ordinals.chunks(200) {
            let tx = db.begin().expect("begin one import batch");
            for ordinal in batch {
                db.execute_in_tx(
                    tx,
                    "INSERT INTO passages VALUES ($id, $context, $generation, $current, $embedding)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(*ordinal as i64 + 1)),
                        ("context".into(), Value::Uuid(context(partition))),
                        ("generation".into(), Value::Uuid(generation(partition))),
                        ("current".into(), Value::Bool(is_current(*ordinal))),
                        ("embedding".into(), Value::Vector(embedding(*ordinal))),
                    ]),
                )
                .expect("stage one passage");
            }
            db.commit(tx).expect("commit one import batch");
        }
    }
}

fn find(partitions: &[usize]) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("current".to_owned(), Value::Bool(true)),
        ("query".to_owned(), Value::Vector(query_vector())),
    ]);
    let mut contexts = Vec::new();
    let mut generations = Vec::new();
    for partition in partitions {
        params.insert(format!("c{partition}"), Value::Uuid(context(*partition)));
        params.insert(
            format!("g{partition}"),
            Value::Uuid(generation(*partition)),
        );
        contexts.push(format!("$c{partition}"));
        generations.push(format!("$g{partition}"));
    }
    (
        format!(
            "SELECT id FROM passages WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $current ORDER BY embedding <=> $query LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", ")
        ),
        params,
    )
}

fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the find returns ids, got {other:?}"),
        })
        .collect()
}

fn assert_exact_disclosed(answer: &QueryResult, label: &str) {
    let search = answer
        .trace()
        .vector_search
        .as_ref()
        .unwrap_or_else(|| panic!("{label}: the find discloses its route"));
    assert_eq!(search.requested_mode, VectorSearchMode::Auto, "{label}");
    assert_eq!(search.resolved_mode, VectorSearchMode::Exact, "{label}");
    assert_eq!(search.route, Some(VectorSearchRoute::Exact), "{label}");
    assert_eq!(search.fallback.as_deref(), Some(AUTO_FALLBACK), "{label}");
}

const SCOPES: [&[usize]; 3] = [&[0], &[0, 1], &[0, 1, 2]];

#[test]
fn an_auto_find_answers_exactly_after_commit_with_no_maintenance() {
    const { assert!(LARGE_PARTITION_ROWS * DIMENSIONS * size_of::<f32>() > READER_DEFAULT) };
    let directory = tempfile::TempDir::new().expect("fixture directory");
    let path = directory.path().join("passages.db");
    let runtime = secure_runtime(directory.path());

    // Searchable the moment the import commits: no cycle has run.
    let db = open(&path, &runtime);
    seed_passages(&db);
    for scope in SCOPES {
        let (sql, params) = find(scope);
        let answer = db.execute(&sql, &params).expect("a live AUTO find answers");
        assert_eq!(ids(answer.rows()), oracle(scope), "live scope {scope:?}");
        assert_exact_disclosed(&answer, "live");
    }
    db.close().expect("close the writer");

    // Cold, on a writable handle.
    let db = open(&path, &runtime);
    for scope in SCOPES {
        let (sql, params) = find(scope);
        let answer = db.execute(&sql, &params).expect("a cold AUTO find answers");
        assert_eq!(ids(answer.rows()), oracle(scope), "cold scope {scope:?}");
        assert_exact_disclosed(&answer, "cold");
    }
    db.close().expect("close the writer");

    // Cold, on the file route, inside a reader that cannot hold partition 0.
    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(
                    (0..3)
                        .map(|partition| ContextId::new(context(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: ReadLimits {
                    work: 50_000_000,
                    active_ms: 600_000,
                    memory: READER_DEFAULT as u64,
                    ..ReadLimits::default()
                },
                memory_limit: Some(READER_DEFAULT),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image");
    assert_eq!(reader.route(), ReadRoute::File);
    for scope in SCOPES {
        let before = reader
            .file_vector_passive_activity_for_test()
            .expect("vector activity");
        let (sql, params) = find(scope);
        let answer = reader.execute(&sql, &params).expect("a file AUTO find answers");
        let after = reader
            .file_vector_passive_activity_for_test()
            .expect("vector activity");
        assert_eq!(ids(answer.rows()), oracle(scope), "file scope {scope:?}");
        assert_eq!(
            after.raw_partition_loads, before.raw_partition_loads,
            "AUTO never loads a partition's vectors whole, scope {scope:?}"
        );
    }
}

fn assert_refused<T>(result: contextdb_core::Result<T>, door: &str) {
    match result {
        Err(error @ Error::VectorIndexedRouteUnavailable { .. }) => {
            assert_eq!(error.to_string(), REFUSAL, "{door}");
        }
        Err(error) => panic!("{door}: refused with the wrong error: {error:?}"),
        Ok(_) => panic!("{door}: INDEXED answered"),
    }
}

#[test]
fn indexed_is_refused_at_every_door() {
    let directory = tempfile::TempDir::new().expect("fixture directory");
    let path = directory.path().join("doors.db");
    let runtime = secure_runtime(directory.path());
    let db = open(&path, &runtime);
    for ddl in [
        "CREATE TABLE items (id INT PRIMARY KEY, body TEXT, embedding VECTOR(4))",
        "CREATE TABLE declared (id INT PRIMARY KEY, body TEXT, embedding VECTOR(4) SEARCH_MODE INDEXED)",
        "CREATE TABLE empty_items (id INT PRIMARY KEY, body TEXT, embedding VECTOR(4))",
    ] {
        db.execute(ddl, &none()).expect("the grammar stays");
    }
    let q = vec![1.0, 0.0, 0.0, 0.0];
    let params = HashMap::from([("q".to_owned(), Value::Vector(q.clone()))]);
    for table in ["items", "declared"] {
        for id in 1..=3i64 {
            db.execute(
                &format!("INSERT INTO {table} VALUES ($id, 'x', $q)"),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("q".to_owned(), Value::Vector(q.clone())),
                ]),
            )
            .expect("insert");
        }
    }

    let unfiltered = "SELECT id FROM items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 3";
    let filtered =
        "SELECT id FROM items WHERE body = 'x' ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 3";
    let empty = "SELECT id FROM empty_items ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 3";
    let declared = "SELECT id FROM declared ORDER BY embedding <=> $q LIMIT 3";
    for (door, sql) in [
        ("unfiltered", unfiltered),
        ("filtered", filtered),
        ("empty table", empty),
        ("declared column", declared),
    ] {
        assert_refused(db.execute(sql, &params), door);
        let tx = db.begin().expect("begin");
        assert_refused(db.execute_in_tx(tx, sql, &params), door);
        db.rollback(tx).expect("rollback");
    }
    let mut semantic = SemanticQuery::new("items", "embedding", q.clone(), 3);
    semantic.search_mode = Some(VectorSearchMode::Indexed);
    assert_refused(db.semantic_search(semantic), "semantic search override");
    assert_refused(
        db.semantic_search(SemanticQuery::new("declared", "embedding", q.clone(), 3)),
        "semantic search on the declared column",
    );
    assert_refused(
        db.query_vector(
            VectorIndexRef::new("declared", "embedding"),
            &q,
            3,
            None,
            db.snapshot(),
        ),
        "query_vector on the declared column",
    );

    // EXPLAIN says what execution does, ahead of any route-dependent advice.
    for (door, sql) in [
        ("unfiltered", unfiltered),
        ("filtered", filtered),
        ("empty table", empty),
        ("declared column", declared),
    ] {
        let explained = db.explain_output(sql).expect("EXPLAIN does not execute");
        let search = explained.vector_search.expect("EXPLAIN discloses the route");
        assert_eq!(
            search.refusal.as_deref(),
            Some("indexed_route_unavailable"),
            "{door}"
        );
        assert_eq!(search.recovery.as_deref(), Some(REFUSAL), "{door}");
    }

    // SHOW gives the same advice, and an override still answers.
    let show = db
        .execute("SHOW VECTOR_PARTITIONS FOR declared.embedding", &none())
        .expect("SHOW");
    assert_eq!(cell(&show, "query_state"), Value::Text("unavailable".into()));
    assert_eq!(
        cell(&show, "availability_reason"),
        Value::Text("no_index_in_this_release".into())
    );
    assert_eq!(
        cell(&show, "recovery_action"),
        Value::Text("use_exact_or_auto".into())
    );
    for mode in ["AUTO", "EXACT"] {
        let answer = db
            .execute(
                &format!("SELECT id FROM declared ORDER BY embedding <=> $q USE VECTOR {mode} LIMIT 3"),
                &params,
            )
            .expect("an override on the declared column answers");
        assert_eq!(answer.rows().len(), 3, "{mode}");
    }
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET SEARCH_MODE INDEXED",
        &none(),
    )
    .expect("the grammar stays");
    assert_refused(
        db.execute("SELECT id FROM items ORDER BY embedding <=> $q LIMIT 3", &params),
        "altered column",
    );
    db.close().expect("close the writer");

    // The read route has its own door.
    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || ReadSession::open(&path))
        .expect("open the file image");
    assert_eq!(reader.route(), ReadRoute::File);
    assert_refused(reader.execute(unfiltered, &params), "file route");
    assert_refused(reader.execute(declared, &params), "file route, declared");
}

fn cell(result: &QueryResult, name: &str) -> Value {
    let at = result
        .columns()
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("inspection has {name}: {result:?}"));
    result.rows()[0][at].clone()
}

#[derive(Default)]
struct Builds(AtomicU64);

impl VectorBuildObserver for Builds {
    fn before_vector_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn partition_keys(db: &Database, index: &VectorIndexRef) -> Vec<VectorPartitionKey> {
    let mut owner = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory().expect("reader memory"),
    );
    db.vector_store_for_test()
        .partition_infos(index, &mut owner)
        .expect("partition infos")
        .into_iter()
        .map(|partition| partition.partition.partition_key)
        .collect()
}

/// Read straight from the native tables, past the switched-off inspection.
fn assert_no_native_scope(db: &Database, index: &VectorIndexRef, keys: &[VectorPartitionKey], at: &str) {
    assert!(!keys.is_empty(), "{at}: the proof needs a partition");
    for key in keys {
        let error = db
            .with_native_ivf_read_for_test(index, key, |_| Ok(()))
            .expect_err(at);
        assert!(
            error.to_string().contains("no native clustered scope"),
            "{at}: {error:?}"
        );
    }
}

fn assert_nothing_built(report: &contextdb_engine::MaintenanceReport, at: &str) {
    let vector = &report.vector;
    assert_eq!(
        (
            vector.nonempty_indexes,
            vector.built_indexes,
            vector.remaining_indexes,
            vector.nonempty_partitions,
            vector.built_partitions,
            vector.remaining_partitions,
        ),
        (0, 0, 0, 0, 0, 0),
        "{at}"
    );
    assert!(vector.first_failure.is_none(), "{at}");
}

fn small_vector(id: i64) -> Vec<f32> {
    vec![1.0, id as f32 * 0.01, 0.0, 0.0]
}

#[test]
fn no_cycle_or_commit_builds_or_writes_an_index() {
    let directory = tempfile::TempDir::new().expect("fixture directory");
    let path = directory.path().join("nothing.db");
    let runtime = secure_runtime(directory.path());
    let db = open(&path, &runtime);
    let tail_before_any_vector = {
        db.execute(
            "CREATE TABLE kept (id INT PRIMARY KEY, scope INTEGER NOT NULL, \
             embedding VECTOR(4) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1) \
             HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
            &none(),
        )
        .expect("declare the kept table");
        db.persist_import_vector_tails().expect("coverage ask")
    };
    assert!(tail_before_any_vector, "an empty store has nothing uncovered");
    db.execute(
        "CREATE TABLE current_rows (id INT PRIMARY KEY, scope INTEGER NOT NULL, \
         embedding VECTOR(4) PARTITION_KEY (scope) MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1) \
         HISTORY CURRENT ONLY SYNC OFF",
        &none(),
    )
    .expect("declare the current-only table");
    let kept = VectorIndexRef::new("kept", "embedding");
    let current = VectorIndexRef::new("current_rows", "embedding");

    let builds = Arc::new(Builds::default());
    with_vector_build_observer(builds.clone(), || {
        for table in ["kept", "current_rows"] {
            for id in 1..=40i64 {
                db.execute(
                    &format!("INSERT INTO {table} VALUES ($id, $scope, $v)"),
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        ("scope".to_owned(), Value::Int64(id % 2)),
                        ("v".to_owned(), Value::Vector(small_vector(id))),
                    ]),
                )
                .expect("insert");
            }
        }
        let keys_kept = partition_keys(&db, &kept);
        let keys_current = partition_keys(&db, &current);
        assert_no_native_scope(&db, &kept, &keys_kept, "after commits");

        let report = db.run_maintenance_cycle().expect("an explicit cycle");
        assert_nothing_built(&report, "explicit cycle");
        assert_no_native_scope(&db, &kept, &keys_kept, "after an explicit cycle");

        // SHOW reports membership and no index, without reading index pages.
        let show = db
            .execute("SHOW VECTOR_PARTITIONS FOR kept.embedding", &none())
            .expect("SHOW");
        assert_eq!(cell(&show, "query_state"), Value::Text("exact".into()));
        assert_eq!(cell(&show, "indexed_available"), Value::Bool(false));
        assert_eq!(cell(&show, "maintenance_state"), Value::Text("idle".into()));
        assert!(matches!(cell(&show, "live_rows"), Value::Int64(20)));
        assert!(
            !db.persist_import_vector_tails().expect("coverage ask"),
            "no index covers a populated partition"
        );

        // Local erasure, authoritative purge, then retention over the erased
        // table: the data goes, and no index control is written along the way.
        db.execute("DISCARD FROM kept WHERE id = 1", &none())
            .expect("local erasure");
        db.execute("PURGE FROM kept WHERE id = 2", &none())
            .expect("authoritative purge");
        assert_no_native_scope(&db, &kept, &keys_kept, "after erasure and purge");
        db.execute("UPDATE current_rows SET embedding = $v WHERE id = 3", &HashMap::from([
            ("v".to_owned(), Value::Vector(small_vector(1_000))),
        ]))
        .expect("replace a vector");
        db.execute("DELETE FROM current_rows WHERE id = 4", &none())
            .expect("delete a vector");
        db.execute("PURGE FROM current_rows WHERE id = 5", &none())
            .expect("authoritative purge of the current-only table");
        let report = db.run_maintenance_cycle().expect("retention after erasure");
        assert_nothing_built(&report, "retention cycle");
        assert_no_native_scope(&db, &current, &keys_current, "after retention");
        let ks = db
            .execute(
                "SELECT id FROM kept ORDER BY embedding <=> $q LIMIT 40",
                &HashMap::from([("q".to_owned(), Value::Vector(small_vector(40)))]),
            )
            .expect("find after erasure");
        let found = ids(ks.rows());
        assert_eq!(found.len(), 38);
        assert!(!found.contains(&1) && !found.contains(&2), "erased rows stay erased");

        // The engine-owned worker reaches the same off switch.
        db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
        let clock = db
            .__hold_engine_owned_vector_maintenance_clock_for_test()
            .expect("the engine-owned worker runs");
        clock.wake_once().expect("one worker wake");
        drop(clock);
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_no_native_scope(&db, &kept, &keys_kept, "after a worker wake");

        // Nothing for compaction to move; a reopen answers the same.
        db.compact_now().expect("compaction");
        let params = HashMap::from([("q".to_owned(), Value::Vector(small_vector(40)))]);
        let sql = "SELECT id FROM kept ORDER BY embedding <=> $q LIMIT 10";
        let before = ids(db.execute(sql, &params).expect("find").rows());
        db.close().expect("close");
        let db = open(&path, &runtime);
        assert_eq!(ids(db.execute(sql, &params).expect("find").rows()), before);
        assert_no_native_scope(&db, &kept, &keys_kept, "after compaction and reopen");
        db.close().expect("close");
    });
    assert_eq!(builds.0.load(Ordering::SeqCst), 0, "nothing built on this thread");
}

#[test]
fn declared_index_policy_is_recorded_and_has_no_effect() {
    let directory = tempfile::TempDir::new().expect("fixture directory");
    let path = directory.path().join("policy.db");
    let runtime = secure_runtime(directory.path());
    let db = open(&path, &runtime);
    db.execute(
        "CREATE TABLE declared_policy (id INT PRIMARY KEY, embedding VECTOR(4) SEARCH_MODE AUTO \
         AUTO_INDEX_AT 2 WITH (calibration_samples = 4))",
        &none(),
    )
    .expect("the declaration parses");
    for id in 1..=6i64 {
        db.execute(
            "INSERT INTO declared_policy VALUES ($id, $v)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("v".to_owned(), Value::Vector(small_vector(id))),
            ]),
        )
        .expect("insert");
    }
    let schema = render_table_meta(
        "declared_policy",
        &db.table_meta("declared_policy").expect("metadata"),
    );
    assert!(schema.contains("AUTO_INDEX_AT 2"), "{schema}");
    assert!(schema.to_ascii_lowercase().contains("calibration_samples = 4"), "{schema}");

    let params = HashMap::from([("q".to_owned(), Value::Vector(small_vector(6)))]);
    let sql = "SELECT id FROM declared_policy ORDER BY embedding <=> $q LIMIT 6";
    let answer = db.execute(sql, &params).expect("find");
    assert_eq!(answer.rows().len(), 6);
    assert_exact_disclosed(&answer, "executed, above the declared threshold");
    let explained = db.explain_output(sql).expect("EXPLAIN does not execute");
    let search = explained.vector_search.expect("EXPLAIN discloses the route");
    assert_eq!(search.resolved_mode, VectorSearchMode::Exact);
    assert_eq!(search.route, Some(VectorSearchRoute::Exact));
    assert_eq!(search.fallback.as_deref(), Some(AUTO_FALLBACK));
    db.close().expect("close");
}
