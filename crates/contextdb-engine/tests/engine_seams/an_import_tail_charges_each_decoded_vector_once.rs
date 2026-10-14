#![cfg(feature = "test-seams")]
//! Making an import's new passages searchable decodes every new vector of a
//! partition. Those decoded bodies are admitted once, in the import's writer
//! workspace, before they are read. The decode itself must then be charged to
//! the declared writer limit for exactly as long as one stored value is being
//! decoded -- never a second time for the kept vector the workspace already
//! paid for, never accumulated across stored values that are already freed -- and
//! an import whose new vectors exceed the writer budget must still complete,
//! in committed cohorts, without the budget being raised.
use contextdb_core::{Lsn, TxId, Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_observations::{
    ImportTailLoadBoundaryForTest, ImportTailLoadForTest, VectorJournalReplayWork,
    observe_import_tail_loads_for_test, observe_journal_replay,
};
use contextdb_engine::vector_read_memory::{
    reader_parents_created_for_test, unowned_decodes_for_test,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

const DIMENSION: usize = 1536;
const BASE_ROWS: i64 = 16;
const ROOMY_LIMIT: usize = 512 * 1024 * 1024;
const TARGET_ID: i64 = 1_000_000;
const SITE: &str = "persist_import_vector_tail";
const CANDIDATE_SITE: &str = "catch_up_native_import_candidate";
const DECODE_OWNER: (&str, &str) = ("writer_source", "decode_stored_value");
/// The writer's live decode charge while one stored value decodes, above
/// what the load found charged at its start: the same for every value and
/// every suffix size. The kept 1536-dimension vector is decoded into exactly
/// its length and adopts the workspace's prepaid credit, so it is NOT part of
/// this charge; what remains is the value's record, names and decode control
/// (measured at 256 and 512 vectors: identical).
const DECODE_PEAK_PER_VALUE: usize = 1_726;
/// One kept vector's decoded body, prepaid in the import workspace.
const KEPT_VECTOR_BYTES: usize = DIMENSION * 4;

fn vector_for(id: i64) -> Vec<f32> {
    (0..DIMENSION)
        .map(|lane| (((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.5) + 0.5)
        .collect()
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

/// A built 16-row graph, then `suffix_rows` single-row commits and the target.
fn fixture(path: &std::path::Path, limit: usize, suffix_rows: usize) -> Database {
    // Readers are declared large enough to search a pending suffix; the
    // writer limit is what these tests hold fixed.
    let mut options = options(limit);
    options.owner_reads.limits.limits.memory = 1 << 30;
    let db = Database::open_with_options(path, options).unwrap();
    db.execute(
        &format!(
            "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)"
        ),
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..BASE_ROWS {
        append(&db, id, vector_for(id));
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in BASE_ROWS..BASE_ROWS + suffix_rows as i64 {
        append(&db, id, vector_for(id));
    }
    let mut target = vec![0.0; DIMENSION];
    target[0] = 1.0;
    append(&db, TARGET_ID, target);
    db
}

fn assert_searchable(db: &Database, suffix_rows: usize) {
    assert_eq!(
        db.execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new()
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(BASE_ROWS + suffix_rows as i64 + 1)]]
    );
    let mut target = vec![0.0; DIMENSION];
    target[0] = 1.0;
    assert_eq!(
        db.execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(target))]),
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(TARGET_ID)]]
    );
}

/// One observed load: its boundaries and the writer ledger between them.
#[derive(Debug)]
struct Load {
    enter: ImportTailLoadForTest,
    exit: ImportTailLoadForTest,
    ledger: Vec<BudgetLedgerEntry>,
}

impl Load {
    /// Highest live writer charge during the load, above its start.
    fn peak_above_enter(&self) -> usize {
        self.ledger
            .iter()
            .map(|entry| entry.used_after)
            .max()
            .unwrap_or(self.enter.writer_used)
            .saturating_sub(self.enter.writer_used)
    }
    fn decode_admissions(&self) -> usize {
        self.ledger
            .iter()
            .filter(|entry| entry.what == "writer_source/decode_stored_value")
            .count()
    }
}

/// Install a load observer that records every boundary, isolates the writer
/// ledger of each load, and installs the limit `tighten` returns at a load's
/// start, restoring `restore` when that load ends.
fn record_loads(
    tighten: impl Fn(&ImportTailLoadForTest) -> Option<usize> + Send + Sync + 'static,
    restore: usize,
) -> (
    Arc<parking_lot::Mutex<Vec<Load>>>,
    contextdb_engine::vector_observations::ImportTailLoadObservationForTest,
) {
    let loads = Arc::new(parking_lot::Mutex::new(Vec::<Load>::new()));
    let open = Arc::new(parking_lot::Mutex::new(None::<ImportTailLoadForTest>));
    let sink = loads.clone();
    let observation =
        observe_import_tail_loads_for_test(Arc::new(move |event| match event.boundary {
            ImportTailLoadBoundaryForTest::Enter => {
                *open.lock() = Some(event.clone());
                take_budget_ledger_for_test();
                watch_budget_ledger_for_test();
                tighten(event).map(Some)
            }
            _ => {
                let ledger = take_budget_ledger_for_test();
                let enter = open.lock().take().expect("a load ends after it starts");
                sink.lock().push(Load {
                    enter,
                    exit: event.clone(),
                    ledger,
                });
                Some(Some(restore))
            }
        }));
    (loads, observation)
}

fn assert_owned_by_the_writer(load: &Load) {
    assert_eq!(load.enter.decode_owner, DECODE_OWNER, "{load:?}");
    assert!(load.enter.decode_owner_funded_by_writer, "{:?}", load.enter);
    assert!(load.enter.decode_owner_per_call, "{:?}", load.enter);
    assert_eq!(load.enter.decode_held, 0, "the owner starts empty");
    assert_eq!(
        load.exit.decode_held, 0,
        "no decode owner of the load survives it, holding nothing: {:?}",
        load.exit
    );
    assert_eq!(
        load.exit.writer_used, load.enter.writer_used,
        "every decode charge of the load is returned when it ends; the decoded \
         vectors stay paid for only by the workspace admitted before it: {:?}",
        load.exit
    );
}

/// Absorb `suffix_rows` with a roomy writer and return the one load.
fn absorb_roomy(suffix_rows: usize) -> Load {
    let root = tempfile::tempdir().unwrap();
    let db = fixture(
        &root.path().join("charged-once.db"),
        ROOMY_LIMIT,
        suffix_rows,
    );
    let (loads, observation) = record_loads(|_| None, ROOMY_LIMIT);
    let parents = reader_parents_created_for_test();
    let unowned = unowned_decodes_for_test();
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(
        reader_parents_created_for_test(),
        parents,
        "no private reader"
    );
    assert_eq!(
        unowned_decodes_for_test(),
        unowned,
        "no import-tail decode falls back to the private default accountant"
    );
    drop(observation);
    assert_searchable(&db, suffix_rows);
    let mut loads = std::mem::take(&mut *loads.lock());
    assert_eq!(loads.len(), 1, "one cohort under a roomy writer: {loads:?}");
    let load = loads.remove(0);
    assert_eq!(load.enter.site, SITE);
    assert_eq!(load.enter.cohort_vectors, suffix_rows + 1);
    assert_eq!(
        load.exit.boundary,
        ImportTailLoadBoundaryForTest::Loaded {
            vectors: suffix_rows + 1,
            adopted_output: (suffix_rows + 1) * KEPT_VECTOR_BYTES,
        },
        "every kept vector adopts its prepaid workspace credit, once"
    );
    assert_owned_by_the_writer(&load);
    load
}

#[test]
fn an_import_tail_decode_is_charged_once_per_stored_value_and_returned_at_both_load_boundaries() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    // A raised private reader fallback must not be what lets this pass.
    assert_eq!(ReadLimits::SHIPPED_MEMORY, 16 * 1024 * 1024);
    let loads = [255, 511].map(absorb_roomy);
    for load in &loads {
        eprintln!(
            "import tail load: vectors={} workspace={} writer_used={} peak_above_enter={} decode_admissions={}",
            load.enter.cohort_vectors,
            load.enter.workspace_bytes,
            load.enter.writer_used,
            load.peak_above_enter(),
            load.decode_admissions()
        );
        assert!(
            load.peak_above_enter() > 0,
            "each stored value's decode is charged to the writer while it decodes"
        );
        assert!(
            load.peak_above_enter() < KEPT_VECTOR_BYTES,
            "the kept vector is prepaid by the workspace; charging it again in the \
             decode owner is the double charge this rejects: {}",
            load.peak_above_enter()
        );
        assert_eq!(
            load.peak_above_enter(),
            DECODE_PEAK_PER_VALUE,
            "the live decode charge is one stored value's, whatever the suffix size"
        );
    }
}

/// The tight limit the load must fit: its start plus one value's decode.
fn tight_absorb(
    headroom: usize,
    suffix_rows: usize,
) -> (Database, tempfile::TempDir, Vec<Load>, Result<bool, String>) {
    let root = tempfile::tempdir().unwrap();
    let db = fixture(&root.path().join("tight.db"), ROOMY_LIMIT, suffix_rows);
    let (loads, observation) =
        record_loads(move |enter| Some(enter.writer_used + headroom), ROOMY_LIMIT);
    let completed = db
        .persist_import_vector_tails()
        .map_err(|error| error.to_string());
    drop(observation);
    let loads = std::mem::take(&mut *loads.lock());
    (db, root, loads, completed)
}

#[test]
fn a_tight_writer_limit_refuses_a_missing_and_admits_no_duplicate_decode_charge() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let suffix_rows = 255;
    // Exactly one value's decode above the load's start: a decode charged
    // more than once, or accumulated across freed bodies, is refused.
    let (db, _root, loads, completed) = tight_absorb(DECODE_PEAK_PER_VALUE, suffix_rows);
    assert_eq!(completed, Ok(true), "{loads:?}");
    assert_eq!(loads.len(), 1, "{loads:?}");
    assert_owned_by_the_writer(&loads[0]);
    assert_eq!(
        loads[0].exit.boundary,
        ImportTailLoadBoundaryForTest::Loaded {
            vectors: suffix_rows + 1,
            adopted_output: (suffix_rows + 1) * KEPT_VECTOR_BYTES,
        }
    );
    assert_searchable(&db, suffix_rows);
    drop(db);

    // One byte less: a decode that was not charged would still fit.
    let (db, _root, loads, completed) = tight_absorb(DECODE_PEAK_PER_VALUE - 1, suffix_rows);
    assert_eq!(loads.len(), 1, "{loads:?}");
    let refusal = match &loads[0].exit.boundary {
        ImportTailLoadBoundaryForTest::Refused { error } => error.clone(),
        other => panic!("the load must refuse at its decode, saw {other:?}"),
    };
    eprintln!("tight writer decode refusal: {refusal}");
    assert!(
        refusal.starts_with("memory budget exceeded: writer_source/decode_stored_value requested "),
        "{refusal}"
    );
    assert!(
        refusal
            .ends_with("Hint: Raise MEMORY_LIMIT to decode this writer operation's stored values."),
        "{refusal}"
    );
    assert_eq!(
        completed,
        Err(refusal),
        "the refusal reaches the caller unchanged"
    );
    assert_eq!(loads[0].exit.decode_held, 0, "{:?}", loads[0].exit);
    assert_eq!(
        loads[0].exit.writer_used, loads[0].enter.writer_used,
        "a refused load returns every decode charge it made"
    );
    // Committed rows are untouched and a roomy retry absorbs them exactly.
    assert!(db.persist_import_vector_tails().unwrap());
    assert_searchable(&db, suffix_rows);
}

#[test]
fn an_import_larger_than_the_writer_limit_is_absorbed_in_committed_cohorts_without_raising_it() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    const LIMIT: usize = 16 * 1024 * 1024;
    let suffix_rows = 3000;
    let root = tempfile::tempdir().unwrap();
    let db = fixture(&root.path().join("cohorts.db"), LIMIT, suffix_rows);
    let entry_bytes = DIMENSION * 4 + 256;
    assert!(
        (suffix_rows + 1) * entry_bytes > LIMIT,
        "the import's decoded vectors alone exceed the whole writer limit"
    );
    let replayed = Arc::new(AtomicUsize::new(0));
    let observed = replayed.clone();
    let replay = observe_journal_replay(Arc::new(move |event| {
        if matches!(event.work, VectorJournalReplayWork::Vector { .. }) {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let (loads, observation) = record_loads(|_| None, LIMIT);
    let completed = db.persist_import_vector_tails();
    drop(observation);
    drop(replay);
    let loads = std::mem::take(&mut *loads.lock());
    assert!(
        completed.as_ref().is_ok_and(|complete| *complete),
        "the import is absorbed, not refused: {completed:?}"
    );
    assert_eq!(db.memory_limit(), Some(LIMIT), "the limit is never raised");
    let cohorts: Vec<usize> = loads.iter().map(|load| load.enter.cohort_vectors).collect();
    eprintln!("cohorts under a {LIMIT}-byte writer: {cohorts:?}");
    assert!(cohorts.len() > 1, "{cohorts:?}");
    assert_eq!(cohorts.iter().sum::<usize>(), suffix_rows + 1);
    assert_eq!(
        replayed.load(Ordering::SeqCst),
        suffix_rows + 1,
        "every new vector is read exactly once"
    );
    for load in &loads {
        assert_eq!(load.enter.site, SITE);
        assert_eq!(load.enter.writer_limit, Some(LIMIT));
        assert!(
            load.enter.workspace_bytes - 128 * 1024 <= LIMIT / 16,
            "a cohort's decoded vectors take at most a sixteenth of the limit: {:?}",
            load.enter
        );
        assert_eq!(
            load.exit.boundary,
            ImportTailLoadBoundaryForTest::Loaded {
                vectors: load.enter.cohort_vectors,
                adopted_output: load.enter.cohort_vectors * KEPT_VECTOR_BYTES,
            }
        );
        assert_owned_by_the_writer(load);
    }
    assert_searchable(&db, suffix_rows);
}

#[test]
fn a_completed_replacement_catches_up_its_import_cohort_under_the_writer() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("candidate.db");
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(64 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512))", &HashMap::new()).unwrap();
    let small = |db: &Database, id: i64, vector: Vec<f32>| {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
    };
    for id in 0..16 {
        small(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..2064 {
        small(&db, id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    small(&db, 10_000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    // The replacement is queued; a later import commits while it builds.
    let pending = db.run_maintenance_cycle().unwrap().vector;
    assert!(
        pending.first_failure.is_none(),
        "{:?}",
        pending.first_failure
    );
    small(&db, 10_001, vec![0.0, 0.0, 1.0]);
    let (loads, observation) = record_loads(|_| None, 64 * 1024 * 1024);
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{:?}", report.first_failure);
        if loads
            .lock()
            .iter()
            .any(|load| load.enter.site == CANDIDATE_SITE)
        {
            break;
        }
    }
    drop(observation);
    let loads = std::mem::take(&mut *loads.lock());
    let candidate: Vec<&Load> = loads
        .iter()
        .filter(|load| load.enter.site == CANDIDATE_SITE)
        .collect();
    eprintln!("candidate catch-up loads: {candidate:?}");
    assert_eq!(candidate.len(), 1, "{loads:?}");
    assert_eq!(candidate[0].enter.cohort_vectors, 1);
    assert_eq!(
        candidate[0].exit.boundary,
        ImportTailLoadBoundaryForTest::Loaded {
            vectors: 1,
            adopted_output: 3 * 4,
        }
    );
    assert_owned_by_the_writer(candidate[0]);
}

fn options(limit: usize) -> DatabaseOpenOptions {
    DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(limit),
        ..DatabaseOpenOptions::default()
    }
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

/// A built 16-row graph of 3-dimension vectors, 2,048 more rows absorbed as
/// a native tail, a replacement queued, and one later commit.
fn queued_replacement_with_a_later_commit(path: &std::path::Path) -> Database {
    let db = Database::open_with_options(path, options(64 * 1024 * 1024)).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512))", &HashMap::new()).unwrap();
    let small = |id: i64, vector: Vec<f32>| {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
    };
    for id in 0..16 {
        small(id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    for id in 16..2064 {
        small(id, vec![0.0, 1.0, id as f32 + 1.0]);
    }
    small(10_000, vec![1.0, 0.0, 0.0]);
    assert!(db.persist_import_vector_tails().unwrap());
    let pending = db.run_maintenance_cycle().unwrap().vector;
    assert!(
        pending.first_failure.is_none(),
        "{:?}",
        pending.first_failure
    );
    small(10_001, vec![0.0, 0.0, 1.0]);
    db
}

#[test]
fn a_refused_candidate_catch_up_returns_every_decode_charge_and_later_succeeds() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("candidate-refusal.db");
    let db = queued_replacement_with_a_later_commit(&path);
    // The first catch-up load gets no headroom at all: its first decode
    // admission must refuse, and the refusal must leave nothing charged.
    let tightened = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let once = tightened.clone();
    let (loads, observation) = record_loads(
        move |enter| {
            (enter.site == CANDIDATE_SITE && !once.swap(true, Ordering::SeqCst))
                .then_some(enter.writer_used)
        },
        64 * 1024 * 1024,
    );
    let mut failures = 0;
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        if report.first_failure.is_some() {
            failures += 1;
        }
        if loads
            .lock()
            .iter()
            .any(|load| load.enter.site == CANDIDATE_SITE)
        {
            break;
        }
    }
    drop(observation);
    let loads = std::mem::take(&mut *loads.lock());
    let candidate: Vec<&Load> = loads
        .iter()
        .filter(|load| load.enter.site == CANDIDATE_SITE)
        .collect();
    eprintln!("candidate refusal then publication: {candidate:?}");
    assert_eq!(candidate.len(), 1, "{loads:?}");
    let refusal = match &candidate[0].exit.boundary {
        ImportTailLoadBoundaryForTest::Refused { error } => error.clone(),
        other => panic!("the tightened catch-up must refuse at its decode, saw {other:?}"),
    };
    assert!(
        refusal.starts_with("memory budget exceeded: writer_source/decode_stored_value requested "),
        "{refusal}"
    );
    assert_eq!(candidate[0].exit.decode_held, 0, "{:?}", candidate[0].exit);
    assert_eq!(
        candidate[0].exit.writer_used, candidate[0].enter.writer_used,
        "a refused catch-up returns every decode charge it made"
    );
    // The refused catch-up publishes the complete candidate at its own
    // frontier in the same cycle: nothing is reported failed, no build is
    // left, and exactly the later commit stays pending.
    assert_eq!(
        failures, 0,
        "the refused catch-up publishes instead of failing"
    );
    assert!(
        db.vector_build_progress_for_test("points", "embedding")
            .unwrap()
            .is_empty(),
        "the candidate is published"
    );
    assert_eq!(pending_inserts(&db), 1, "only the later commit is pending");
    db.close().unwrap();
    drop(db);
    let db = Database::open_with_options(&path, options(64 * 1024 * 1024)).unwrap();
    assert!(
        db.vector_build_progress_for_test("points", "embedding")
            .unwrap()
            .is_empty(),
        "reopen sees the candidate published"
    );
    assert_eq!(pending_inserts(&db), 1);
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let nearest = |db: &Database, q: Vec<f32>| {
        db.execute(query, &HashMap::from([("q".into(), Value::Vector(q))]))
            .unwrap()
            .rows
    };
    assert_eq!(
        nearest(&db, vec![1.0, 0.0, 0.0]),
        vec![vec![Value::Int64(10_000)]]
    );
    assert_eq!(
        nearest(&db, vec![0.0, 0.0, 1.0]),
        vec![vec![Value::Int64(10_001)]]
    );
    // The later commit is covered by the next bounded pass.
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(pending_inserts(&db), 0);
    assert_eq!(
        nearest(&db, vec![0.0, 0.0, 1.0]),
        vec![vec![Value::Int64(10_001)]]
    );
}

const CRASH_LIMIT: usize = 16 * 1024 * 1024;
const CRASH_SUFFIX_ROWS: usize = 3000;

/// The writer stays at 16 MiB; the reader is declared large enough to search
/// a pending suffix of 2,840 1536-dimension vectors while it is pending.
fn crash_options() -> DatabaseOpenOptions {
    let mut options = options(CRASH_LIMIT);
    options.owner_reads.limits.limits.memory = 1 << 30;
    options
}

fn nearest(db: &Database, query: Vec<f32>) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(query))]),
    )
    .unwrap()
    .rows
}

/// The one route's serving coverage: its LSN, its transaction, the commit
/// transaction the commit index records at that LSN, and the inserts its
/// maintained counters still hold pending there. (SHOW VECTOR_PARTITIONS
/// reports a native tail's counters at its base, for consolidation; readers
/// are admitted from these.)
fn serving(db: &Database) -> (Lsn, TxId, Option<TxId>, usize) {
    let routes = db
        .vector_serving_pending_inserts_for_test("points", "embedding")
        .unwrap();
    assert_eq!(routes.len(), 1, "{routes:?}");
    routes[0]
}

fn pending_inserts(db: &Database) -> usize {
    serving(db).3
}

/// Resume the import tail and prove it decodes exactly the pending vectors:
/// each one once, every one committed after `covered`, none the durable
/// coverage already holds. Then the route is finished and searchable.
fn resume_reads_exactly_the_pending(db: &Database, covered: Lsn, pending: usize) {
    let replayed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let observed = replayed.clone();
    let replay = observe_journal_replay(Arc::new(move |event| {
        if let VectorJournalReplayWork::Vector {
            row_id,
            created_tx,
            lsn,
        } = event.work
        {
            observed.lock().push((row_id, created_tx, lsn));
        }
    }));
    let (loads, observation) = record_loads(|_| None, CRASH_LIMIT);
    assert!(db.persist_import_vector_tails().unwrap());
    drop(observation);
    drop(replay);
    let loads = std::mem::take(&mut *loads.lock());
    let replayed = std::mem::take(&mut *replayed.lock());
    let distinct: std::collections::BTreeSet<_> = replayed.iter().copied().collect();
    assert_eq!(replayed.len(), pending, "each pending vector is read once");
    assert_eq!(distinct.len(), pending, "no vector is read twice");
    assert!(
        replayed.iter().all(|(_, _, lsn)| *lsn > covered),
        "no vector of the durable coverage (through LSN {}) is read again",
        covered.0
    );
    assert_eq!(
        loads
            .iter()
            .map(|load| load.enter.cohort_vectors)
            .sum::<usize>(),
        pending
    );
    for load in &loads {
        assert_owned_by_the_writer(load);
    }
    assert_eq!(pending_inserts(db), 0);
    assert_searchable(db, CRASH_SUFFIX_ROWS);
}

#[test]
fn an_import_interrupted_after_one_cohort_publishes_only_that_cohort_and_resumes_without_re_reading_it()
 {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cohort-interrupt.db");
    let db = fixture(&path, CRASH_LIMIT, CRASH_SUFFIX_ROWS);
    assert_eq!(pending_inserts(&db), CRASH_SUFFIX_ROWS + 1);
    // The second cohort's load gets no headroom: the first cohort is
    // published, the second refuses at its first decode.
    let started = Arc::new(AtomicUsize::new(0));
    let count = started.clone();
    let (loads, observation) = record_loads(
        move |enter| (count.fetch_add(1, Ordering::SeqCst) == 1).then_some(enter.writer_used),
        CRASH_LIMIT,
    );
    let interrupted = db.persist_import_vector_tails();
    drop(observation);
    let loads = std::mem::take(&mut *loads.lock());
    assert!(interrupted.is_err(), "{interrupted:?}");
    assert_eq!(loads.len(), 2, "{loads:?}");
    let first = loads[0].enter.cohort_vectors;
    let through = loads[0]
        .enter
        .cohort_through
        .expect("the first cohort is a committed prefix of the suffix");
    assert!(matches!(
        loads[1].exit.boundary,
        ImportTailLoadBoundaryForTest::Refused { .. }
    ));
    // Coverage ends at the first cohort's last commit, named by its own
    // transaction; every later insert stays pending, exactly.
    let (lsn, tx, commit_tx, pending) = serving(&db);
    assert_eq!(lsn, through);
    assert_eq!(commit_tx, Some(tx), "coverage names that LSN's commit");
    assert_eq!(pending, CRASH_SUFFIX_ROWS + 1 - first);
    // The target is in a later cohort: answered from the pending suffix.
    let mut target = vec![0.0; DIMENSION];
    target[0] = 1.0;
    assert_eq!(nearest(&db, target), vec![vec![Value::Int64(TARGET_ID)]]);
    db.close().unwrap();
    let db = Database::open_with_options(&path, crash_options()).unwrap();
    assert_eq!(
        serving(&db),
        (lsn, tx, commit_tx, pending),
        "the partial coverage and its counters survive a restart"
    );
    resume_reads_exactly_the_pending(&db, through, pending);
}

/// Run `persist_import_vector_tails` in a child process that dies at the
/// first cohort's publication `cut`, then prove what reopen and resume see.
fn crash_at_the_first_cohort_publication(test: &str, cut: &str) {
    use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe};
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let (Ok(path), Ok(out)) = (
        std::env::var("S7_IMPORT_COHORT_CRASH_PATH"),
        std::env::var("S7_IMPORT_COHORT_CRASH_OUT"),
    ) {
        let boundary = if cut == "before" {
            CommitBoundary::GenerationBeforeRedb
        } else {
            CommitBoundary::GenerationAfterRedb
        };
        let db = Database::open_with_options(path, crash_options()).unwrap();
        let first = Arc::new(parking_lot::Mutex::new(None::<(u64, usize)>));
        let loaded = first.clone();
        let _loads = observe_import_tail_loads_for_test(Arc::new(move |event| {
            if let ImportTailLoadBoundaryForTest::Loaded { vectors, .. } = event.boundary {
                let mut first = loaded.lock();
                if first.is_none() {
                    *first = Some((event.cohort_through.expect("a committed prefix").0, vectors));
                }
            }
            None
        }));
        observe(
            Arc::new(move |at| {
                if at == boundary
                    && let Some((lsn, vectors)) = *first.lock()
                {
                    std::fs::write(&out, format!("{lsn} {vectors}")).unwrap();
                    std::process::exit(77);
                }
            }),
            || db.persist_import_vector_tails(),
        )
        .unwrap();
        panic!("the first cohort's publication boundary must be reached");
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cohort-crash.db");
    let out = root.path().join("first-cohort");
    let db = fixture(&path, CRASH_LIMIT, CRASH_SUFFIX_ROWS);
    let before = serving(&db);
    assert_eq!(before.3, CRASH_SUFFIX_ROWS + 1);
    db.close().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg(test)
        .args(["--exact", "--nocapture"])
        .env("S7_IMPORT_COHORT_CRASH_PATH", &path)
        .env("S7_IMPORT_COHORT_CRASH_OUT", &out)
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(77), "the {cut} cut is reached");
    let reported = std::fs::read_to_string(&out).unwrap();
    let mut fields = reported.split(' ');
    let through = Lsn(fields.next().unwrap().parse().unwrap());
    let first: usize = fields.next().unwrap().parse().unwrap();
    let db = Database::open_with_options(&path, crash_options()).unwrap();
    let after = serving(&db);
    eprintln!(
        "{cut} the first cohort's publication: before={before:?} after={after:?} through={through:?} first={first}"
    );
    let covered = if cut == "before" {
        // Nothing of the first cohort is durable: exactly the old coverage.
        assert_eq!(after, before);
        before.0
    } else {
        // The first cohort is durable, named by the commit it ends at, and only it.
        assert_eq!(after.0, through);
        assert_eq!(after.2, Some(after.1), "coverage names that LSN's commit");
        assert_eq!(after.3, CRASH_SUFFIX_ROWS + 1 - first);
        through
    };
    let mut target = vec![0.0; DIMENSION];
    target[0] = 1.0;
    assert_eq!(nearest(&db, target), vec![vec![Value::Int64(TARGET_ID)]]);
    resume_reads_exactly_the_pending(&db, covered, after.3);
}

#[test]
fn a_crash_between_import_cohorts_keeps_later_cohorts_pending_and_resumes_without_reading_them_twice()
 {
    crash_at_the_first_cohort_publication(
        "an_import_tail_charges_each_decoded_vector_once::a_crash_between_import_cohorts_keeps_later_cohorts_pending_and_resumes_without_reading_them_twice",
        "after",
    );
}

#[test]
fn a_crash_before_the_first_cohort_is_durable_keeps_the_old_coverage_and_resumes_from_it() {
    crash_at_the_first_cohort_publication(
        "an_import_tail_charges_each_decoded_vector_once::a_crash_before_the_first_cohort_is_durable_keeps_the_old_coverage_and_resumes_from_it",
        "before",
    );
}

fn absorb_quantized(quantization: &str) {
    const QUANTIZED_DIMENSION: usize = 64;
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join(format!("{quantization}.db")),
        options(ROOMY_LIMIT),
    )
    .unwrap();
    db.execute(
        &format!(
            "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({QUANTIZED_DIMENSION}) WITH (quantization = '{quantization}') PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)"
        ),
        &HashMap::new(),
    )
    .unwrap();
    let vector = |id: i64| -> Vec<f32> {
        (0..QUANTIZED_DIMENSION)
            .map(|lane| (((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.5) + 0.5)
            .collect()
    };
    let insert = |id: i64, value: Vec<f32>| {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(value)),
            ]),
        )
        .unwrap();
    };
    for id in 0..BASE_ROWS {
        insert(id, vector(id));
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    let suffix = 200usize;
    for id in BASE_ROWS..BASE_ROWS + suffix as i64 {
        insert(id, vector(id));
    }
    let mut target = vec![0.0; QUANTIZED_DIMENSION];
    target[0] = 1.0;
    insert(TARGET_ID, target.clone());
    let (loads, observation) = record_loads(|_| None, ROOMY_LIMIT);
    assert!(db.persist_import_vector_tails().unwrap());
    drop(observation);
    let loads = std::mem::take(&mut *loads.lock());
    assert_eq!(loads.len(), 1, "{loads:?}");
    // The quantized payload is decode scratch; its expansion is the kept
    // vector, adopted from the prepaid workspace once per vector.
    assert_eq!(
        loads[0].exit.boundary,
        ImportTailLoadBoundaryForTest::Loaded {
            vectors: suffix + 1,
            adopted_output: (suffix + 1) * QUANTIZED_DIMENSION * 4,
        },
        "{quantization}"
    );
    assert_owned_by_the_writer(&loads[0]);
    assert_eq!(
        db.execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new()
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(BASE_ROWS + suffix as i64 + 1)]]
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(target))]),
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(TARGET_ID)]],
        "{quantization}"
    );
}

#[test]
fn quantized_import_vectors_decode_exactly_with_their_expansion_prepaid() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    absorb_quantized("SQ8");
    absorb_quantized("SQ4");
}

/// One INSERT of `rows` vectors is one committed LSN. Returns whether it
/// committed under `limit`, and if so whether its import tail was absorbed.
fn one_commit_of(rows: usize, limit: usize) -> (bool, Result<bool, String>) {
    let root = tempfile::tempdir().unwrap();
    let db = fixture(&root.path().join("one-commit.db"), limit, 0);
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{row}, 1, $v{row})"));
        let id = 2_000_000 + row as i64;
        params.insert(format!("i{row}"), Value::Int64(id));
        params.insert(format!("v{row}"), Value::Vector(vector_for(id)));
    }
    if db.execute(&sql, &params).is_err() {
        return (false, Ok(false));
    }
    (
        true,
        db.persist_import_vector_tails()
            .map_err(|error| error.to_string()),
    )
}

#[test]
fn every_commit_the_writer_accepted_is_absorbed_under_the_same_limit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    // One committed LSN is the import tail's smallest unit of coverage. A
    // commit holds all its vectors under the writer's declared limit; its
    // import tail must then fit under that same limit, whatever its size.
    let mut largest_committed = 0;
    for rows in [4096usize, 2048, 1024, 512, 256, 128, 64] {
        let (committed, absorbed) = one_commit_of(rows, 16 * 1024 * 1024);
        eprintln!(
            "one commit of {rows} vectors under 16 MiB: committed={committed} absorbed={absorbed:?}"
        );
        if committed {
            largest_committed = largest_committed.max(rows);
            assert_eq!(absorbed, Ok(true), "{rows} vectors in one commit");
        }
    }
    assert!(
        largest_committed > 0,
        "at least one single-commit size is exercised"
    );
}

#[test]
fn a_committed_lsn_larger_than_a_lowered_limit_is_absorbed_by_a_bounded_rebuild() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lowered.db");
    let db = fixture(&path, ROOMY_LIMIT, 0);
    let rows = 4096usize;
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{row}, 1, $v{row})"));
        let id = 2_000_000 + row as i64;
        params.insert(format!("i{row}"), Value::Int64(id));
        params.insert(format!("v{row}"), Value::Vector(vector_for(id)));
    }
    db.execute(&sql, &params).unwrap();
    db.close().unwrap();
    // The operator reopens with a limit below what that one commit's
    // decoded vectors need (4,096 x 6,144 B = 24 MiB > 16 MiB). Coverage is
    // per committed LSN, so the commit is absorbed by a bounded rebuild under
    // the unchanged limit instead of being refused.
    let db = Database::open_with_options(&path, options(16 * 1024 * 1024)).unwrap();
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(db.memory_limit(), Some(16 * 1024 * 1024));
    assert_eq!(pending_inserts(&db), 0);
    assert_eq!(
        db.execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new()
        )
        .unwrap()
        .rows,
        vec![vec![Value::Int64(BASE_ROWS + rows as i64 + 1)]]
    );
}
