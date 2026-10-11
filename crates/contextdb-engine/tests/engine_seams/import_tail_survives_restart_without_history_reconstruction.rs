#![cfg(feature = "test-seams")]
use contextdb_core::{Value, Wallclock};
use contextdb_engine::vector_observations::{VectorJournalReplayWork, observe_journal_replay};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn options() -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    }
}
fn append(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector)),
        ]),
    )
    .unwrap();
}
fn run(history: usize) -> usize {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-tail.db");
    let db = Database::open_with_options(&path, options()).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..16 + history {
        append(&db, id as i64, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    assert!(db.persist_import_vector_tails().unwrap());
    drop(db);
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let _observer = observe_journal_replay(Arc::new(move |event| {
        if matches!(event.work, VectorJournalReplayWork::Record { .. }) {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    for wave in 0..3 {
        let db = Database::open_with_options(&path, options()).unwrap();
        let id = 10_000 + wave;
        append(&db, id, vec![1.0, 0.0, 0.0]);
        // A raw commit advances the operation root without changing saved
        // native pages. Both layers must still read that selected root.
        let before = db
            .execute(
                "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q LIMIT 1",
                &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .unwrap()
            .rows;
        assert_eq!(before, vec![vec![Value::Int64(10_000)]]);
        assert!(db.persist_import_vector_tails().unwrap());
        let rows = db
            .execute(
                "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q LIMIT 1",
                &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .unwrap()
            .rows;
        assert_eq!(rows, vec![vec![Value::Int64(10_000)]]);
    }
    count.load(Ordering::SeqCst)
}
#[test]
fn cold_import_tail_reconstruction_does_not_examine_n_or_four_n_history() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let work = [64, 256].map(run);
    eprintln!("native_tail history=64/256 examined={work:?}");
    assert_eq!(
        work[0], work[1],
        "every import examines only its newly committed suffix"
    );
    assert!(work[0] <= 12, "no saved native tail may be reconstructed");
}

#[test]
fn completed_native_import_stays_searchable_while_replacement_is_queued() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("queued-native-tail.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            memory_limit: Some(64 * 1024 * 1024),
            ..options()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512))", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..2064 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    append(&db, 10_000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    let pending = db.run_maintenance_cycle().unwrap().vector;
    assert!(
        pending.first_failure.is_none(),
        "{:?}",
        pending.first_failure
    );
    let queued = partition_status(&db);
    assert_eq!(queued["query_state"], Value::Text("ready".into()));
    assert_eq!(
        queued["maintenance_state"],
        Value::Text("compacting".into())
    );
    assert!(
        matches!(queued["maintenance_vectors_remaining"], Value::Int64(remaining) if remaining > 0)
    );
    assert_required_recall(&db);
    drop(db);
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            memory_limit: Some(64 * 1024 * 1024),
            ..options()
        },
    )
    .unwrap();
    let resumed = partition_status(&db);
    assert_eq!(resumed["query_state"], Value::Text("ready".into()));
    assert_eq!(
        resumed["maintenance_state"],
        Value::Text("compacting".into())
    );
    append(&db, 10_001, vec![0.0, 0.0, 1.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_required_recall(&db);
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q LIMIT 1";
    let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    assert_eq!(
        db.execute(query, &params).unwrap().rows,
        vec![vec![Value::Int64(10_000)]]
    );
    let mut finished = false;
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        let status = partition_status(&db);
        assert_eq!(status["query_state"], Value::Text("ready".into()));
        if status["maintenance_state"] == Value::Text("idle".into()) {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "the durable replacement must finish in finite cycles"
    );
    assert_required_recall(&db);
    assert_eq!(
        db.execute(query, &params).unwrap().rows,
        vec![vec![Value::Int64(10_000)]]
    );
}

fn partition_status(db: &Database) -> HashMap<String, Value> {
    let result = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR points.embedding",
            &HashMap::new(),
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    result
        .columns
        .into_iter()
        .zip(result.rows.into_iter().next().unwrap())
        .collect()
}

fn assert_required_recall(db: &Database) {
    let params = HashMap::from([("q".into(), Value::Vector(vec![0.0, 1.0, 0.0]))]);
    let exact = db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 32", &params).unwrap().rows;
    let indexed = db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 32", &params).unwrap().rows;
    assert_eq!(
        indexed.len(),
        exact.len(),
        "queued and landed coverage returns the complete top-k"
    );
    let hits = exact.iter().filter(|row| indexed.contains(row)).count();
    assert!(
        hits * 100 >= exact.len() * 95,
        "required recall holds: {hits}/{}",
        exact.len()
    );
}

#[test]
fn native_tail_publication_answers_exactly_before_the_serving_cache_installs() {
    use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe};
    use std::sync::{Mutex, mpsc};
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-publication-gap.db");
    let db = Arc::new(Database::open_with_options(&path, options()).unwrap());
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..80 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    assert!(db.persist_import_vector_tails().unwrap());
    append(&db, 10_000, vec![1.0, 0.0, 0.0]);
    let (published_tx, published_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let writer = Arc::clone(&db);
    let worker = std::thread::spawn(move || {
        let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
        observe(
            Arc::new(move |boundary| {
                if boundary == CommitBoundary::GenerationAfterRedb {
                    published_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
            }),
            || writer.persist_import_vector_tails(),
        )
    });
    published_rx.recv().unwrap();
    let (query, work) = contextdb_engine::writer_query_test_seams::observe(|| {
        db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1", &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]))
    });
    release_tx.send(()).unwrap();
    assert!(worker.join().unwrap().unwrap());
    assert_eq!(query.unwrap().rows, vec![vec![Value::Int64(10_000)]]);
    let roots: Vec<_> = work
        .physical_sources
        .into_iter()
        .flatten()
        .map(|(_, root)| root)
        .collect();
    assert!(
        !roots.is_empty(),
        "the indexed operation opens physical sources"
    );
    assert!(
        roots.iter().all(|root| root == &roots[0]),
        "raw, graph, and controls use one captured physical root: {roots:?}"
    );
}

fn native_fixture(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(path, options()).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    append(&db, 100, vec![0.0, 0.0, 1.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    db
}

#[test]
fn native_tail_crash_recovers_a_complete_catalog_and_graph_at_both_commit_boundaries() {
    use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe};
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Ok(path) = std::env::var("CG_GROWTH_NATIVE_CRASH_PATH") {
        let boundary = if std::env::var("CG_GROWTH_NATIVE_CRASH_CUT").unwrap() == "before" {
            CommitBoundary::GenerationBeforeRedb
        } else {
            CommitBoundary::GenerationAfterRedb
        };
        let db = Database::open_with_options(path, options()).unwrap();
        observe(
            Arc::new(move |at| {
                if at == boundary {
                    std::process::exit(77);
                }
            }),
            || db.persist_import_vector_tails(),
        )
        .unwrap();
        panic!("the requested durability cut must be reached");
    }
    for cut in ["before", "after"] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("native-crash.db");
        let db = native_fixture(&path);
        append(&db, 10_000, vec![1.0, 0.0, 0.0]);
        db.close().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("import_tail_survives_restart_without_history_reconstruction::native_tail_crash_recovers_a_complete_catalog_and_graph_at_both_commit_boundaries")
            .args(["--exact", "--nocapture"])
            .env("CG_GROWTH_NATIVE_CRASH_PATH", &path)
            .env("CG_GROWTH_NATIVE_CRASH_CUT", cut).status().unwrap();
        assert_eq!(child.code(), Some(77), "actual {cut} cut reached");
        let db = Database::open_with_options(&path, options()).unwrap();
        assert_eq!(db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1", &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))])).unwrap().rows, vec![vec![Value::Int64(10_000)]]);
        assert!(db.persist_import_vector_tails().unwrap());
        assert_required_recall(&db);
    }
}

#[test]
fn native_tail_purge_erases_selected_points_and_preserves_survivor_search_after_restart() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-purge.db");
    let db = native_fixture(&path);
    append(&db, 10_000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    db.execute("PURGE FROM points WHERE id = 10000", &HashMap::new())
        .unwrap();
    assert!(
        db.execute("SELECT id FROM points WHERE id = 10000", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    db.close().unwrap();
    let db = Database::open_with_options(&path, options()).unwrap();
    assert_required_recall(&db);
    append(&db, 10_001, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1", &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))])).unwrap().rows, vec![vec![Value::Int64(10_001)]]);
}

#[test]
fn native_tail_reopens_and_appends_inside_one_fixed_writer_budget() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let ownership = [64, 256].map(|history| {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("native-budget.db");
        let db = native_fixture(&path);
        for id in 1000..1000 + history { append(&db, id, vec![0.0, 1.0, id as f32]); }
        assert!(db.persist_import_vector_tails().unwrap());
        db.close().unwrap();
        let db = Database::open_with_options(&path, DatabaseOpenOptions { memory_limit: Some(64 * 1024 * 1024), ..options() }).unwrap();
        append(&db, 10000, vec![1.0, 0.0, 0.0]);
        assert!(db.persist_import_vector_tails().unwrap());
        assert_eq!(db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1", &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))])).unwrap().rows, vec![vec![Value::Int64(10000)]]);
        let receipt = db.vector_memory_ownership_receipt_for_test();
        eprintln!("native fixed-budget history={history} high_water={} receipt={receipt:?}", db.write_memory_high_water_for_test());
        assert!(db.write_memory_high_water_for_test() <= 64 * 1024 * 1024);
        assert_eq!(receipt.raw_vector_bodies, 0, "saved history stays on disk");
        // Descriptor and empty-tail controls remain; raw history stays on disk.
        assert!(receipt.base_graph + receipt.change_graph + receipt.mutable_tail <= 16 * 1024, "bounded native serving controls: {receipt:?}");
        assert_eq!(receipt.temporary_workspace, 0, "import workspace releases at completion");
        (receipt.source_metadata, receipt.base_graph + receipt.change_graph + receipt.mutable_tail)
    });
    assert!(
        ownership[1].0 <= ownership[0].0 + 64 * 1024 && ownership[1].1 <= ownership[0].1 + 4096,
        "retained source controls do not grow with history: {ownership:?}"
    );
}

#[test]
fn native_tail_publications_preserve_an_existing_transaction_snapshot() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-snapshot.db");
    let db = native_fixture(&path);
    let snapshot = db.begin().unwrap();
    let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let before = db.execute_in_tx(snapshot, query, &params).unwrap().rows;
    append(&db, 10000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(
        db.execute_in_tx(snapshot, query, &params).unwrap().rows,
        before
    );
    assert_eq!(
        db.execute(query, &params).unwrap().rows,
        vec![vec![Value::Int64(10000)]]
    );
    db.rollback(snapshot).unwrap();
}

fn saved_change_fixture(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(path, options()).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION (CHANGE_PERCENT = 1))", &HashMap::new()).unwrap();
    for id in 0..16 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..80 {
        append(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    db
}

#[test]
fn an_existing_saved_change_is_adopted_once_before_native_import_suffixes() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Ok(path) = std::env::var("CG_GROWTH_ADOPTION_CRASH_PATH") {
        use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe};
        let boundary = if std::env::var("CG_GROWTH_ADOPTION_CRASH_CUT").unwrap() == "before" {
            CommitBoundary::GenerationBeforeRedb
        } else {
            CommitBoundary::GenerationAfterRedb
        };
        let db = Database::open_with_options(path, options()).unwrap();
        observe(
            Arc::new(move |at| {
                if at == boundary {
                    std::process::exit(77);
                }
            }),
            || db.persist_import_vector_tails(),
        )
        .unwrap();
        panic!("actual saved-change backing commit cut must be reached");
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native-existing-change.db");
    let db = saved_change_fixture(&path);
    let index = contextdb_core::VectorIndexRef::new("points", "embedding");
    let key = contextdb_core::VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap();
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(index, key))
        .unwrap();
    assert!(
        status.change.or(status.dormant_change).is_some(),
        "fixture has an existing saved change: {status:?}"
    );
    let before = status.change.or(status.dormant_change).unwrap();
    let snapshot = db.begin().unwrap();
    let params = HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let held = db.execute_in_tx(snapshot, query, &params).unwrap().rows;
    // Pure adoption preserves the complete logical identity before any suffix.
    assert!(db.persist_import_vector_tails().unwrap());
    let index = contextdb_core::VectorIndexRef::new("points", "embedding");
    let key = contextdb_core::VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap();
    let partition = contextdb_vector::VectorPartitionRef::new(index, key);
    let after = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert_eq!(after.change.or(after.dormant_change), Some(before));
    assert_eq!(
        db.execute_in_tx(snapshot, query, &params).unwrap().rows,
        held
    );
    db.rollback(snapshot).unwrap();
    db.close().unwrap();
    let db = Database::open_with_options(&path, options()).unwrap();
    let reopened = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert_eq!(reopened.change.or(reopened.dormant_change), Some(before));
    let snapshot = db.begin().unwrap();
    let held = db.execute_in_tx(snapshot, query, &params).unwrap().rows;
    append(&db, 10000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    let extended = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert_eq!(
        extended
            .change
            .or(extended.dormant_change)
            .unwrap()
            .generation_id,
        before.generation_id
    );
    assert_eq!(
        db.execute_in_tx(snapshot, query, &params).unwrap().rows,
        held
    );
    db.rollback(snapshot).unwrap();
    assert_required_recall(&db);
    db.close().unwrap();
    let db = Database::open_with_options(&path, options()).unwrap();
    append(&db, 10001, vec![0.0, 0.0, 1.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(db.execute("SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1", &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))])).unwrap().rows, vec![vec![Value::Int64(10000)]]);
    let mut landed = false;
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        if partition_status(&db)["maintenance_state"] == Value::Text("idle".into()) {
            landed = true;
            break;
        }
    }
    assert!(landed, "adopted change replacement lands in finite cycles");
    assert_required_recall(&db);
    let purge_path = root.path().join("adopted-before-suffix-purge.db");
    let db = saved_change_fixture(&purge_path);
    assert!(db.persist_import_vector_tails().unwrap());
    db.execute("PURGE FROM points WHERE id = 77", &HashMap::new())
        .unwrap();
    db.close().unwrap();
    let db = Database::open_with_options(&purge_path, options()).unwrap();
    assert!(
        db.execute("SELECT id FROM points WHERE id = 77", &HashMap::new())
            .unwrap()
            .rows
            .is_empty()
    );
    assert_required_recall(&db);
    append(&db, 10000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(
        db.execute(query, &params).unwrap().rows,
        vec![vec![Value::Int64(10000)]]
    );
    for cut in ["before", "after"] {
        let path = root.path().join(format!("adoption-crash-{cut}.db"));
        let db = saved_change_fixture(&path);
        let expected = db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition)
            .unwrap();
        let expected = expected.change.or(expected.dormant_change).unwrap();
        db.close().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("import_tail_survives_restart_without_history_reconstruction::an_existing_saved_change_is_adopted_once_before_native_import_suffixes")
            .args(["--exact", "--nocapture"])
            .env("CG_GROWTH_ADOPTION_CRASH_PATH", &path)
            .env("CG_GROWTH_ADOPTION_CRASH_CUT", cut).status().unwrap();
        assert_eq!(child.code(), Some(77), "actual adoption {cut} cut reached");
        let db = Database::open_with_options(&path, options()).unwrap();
        let actual = db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition)
            .unwrap();
        assert_eq!(actual.change.or(actual.dormant_change), Some(expected));
        assert_required_recall(&db);
        assert!(db.persist_import_vector_tails().unwrap());
        append(&db, 10000, vec![1.0, 0.0, 0.0]);
        assert!(db.persist_import_vector_tails().unwrap());
        assert_eq!(
            db.execute(query, &params).unwrap().rows,
            vec![vec![Value::Int64(10000)]]
        );
        eprintln!(
            "saved-change adoption {cut} crash: exact identity, indexed reopen and suffix preserved"
        );
    }
}
