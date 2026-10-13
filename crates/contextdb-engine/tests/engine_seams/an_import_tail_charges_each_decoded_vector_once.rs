#![cfg(feature = "test-seams")]
//! Making an import's new passages searchable decodes every new vector of a
//! partition. Those decoded bodies are admitted once, in the import's writer
//! workspace, before they are read. The decode itself must then be charged to
//! the declared writer limit for exactly as long as one stored value is being
//! decoded -- never a second time for the copy the workspace already paid
//! for, never accumulated across stored bodies that are already freed -- and
//! an import whose new vectors exceed the writer budget must still complete,
//! in committed cohorts, without the budget being raised.
use contextdb_core::{Value, Wallclock, read_contract::ReadLimits};
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use contextdb_engine::vector_observations::{
    ImportTailLoadBoundaryForTest, ImportTailLoadForTest, VectorJournalReplayWork,
    observe_import_tail_loads_for_test, observe_journal_replay,
};
use contextdb_engine::vector_read_memory::reader_parents_created_for_test;
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
/// The writer's live decode charge while one stored 1536-dimension vector
/// decodes, above what the load found charged at its start. It is the same
/// for every value and every suffix size: the stored body's codec growth
/// allowance (48 + 16 * 1535 = 24,608), the index name it carries ("points"
/// and "embedding", 15) and its owned copy (1536 * 4 = 6,144).
const DECODE_PEAK_PER_VALUE: usize = 30_767;

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
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(limit),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
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
    assert!(db.persist_import_vector_tails().unwrap());
    assert_eq!(
        reader_parents_created_for_test(),
        parents,
        "no private reader"
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
            vectors: suffix_rows + 1
        }
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
            load.peak_above_enter() >= DIMENSION * 4,
            "each stored body is charged to the writer while it decodes"
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
            vectors: suffix_rows + 1
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
        ImportTailLoadBoundaryForTest::Loaded { vectors: 1 }
    );
    assert_owned_by_the_writer(candidate[0]);
}
