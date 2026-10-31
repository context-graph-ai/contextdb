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
    fixture_in_commits(path, limit, suffix_rows, 1)
}

/// The same suffix written `per_commit` rows to a commit: a cohort is still
/// whole commits, and a large suffix costs a fraction of the commits.
fn fixture_in_commits(
    path: &std::path::Path,
    limit: usize,
    suffix_rows: usize,
    per_commit: usize,
) -> Database {
    // Readers are declared large enough to search a pending suffix; the
    // writer limit is what these tests hold fixed.
    let mut options = options(limit);
    options.owner_reads.limits.limits.memory = 1 << 30;
    let db = Database::open_with_options(path, options).unwrap();
    db.execute(
        &format!(
 "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 IVF (PROBES = 512) CONSOLIDATION NONE)"
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
    let suffix = BASE_ROWS..BASE_ROWS + suffix_rows as i64;
    if per_commit <= 1 {
        for id in suffix {
            append(&db, id, vector_for(id));
        }
    } else {
        for first in suffix.clone().step_by(per_commit) {
            let mut sql = String::from("INSERT INTO points VALUES ");
            let mut params = HashMap::new();
            for id in first..(first + per_commit as i64).min(suffix.end) {
                if id > first {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("($i{id}, 1, $v{id})"));
                params.insert(format!("i{id}"), Value::Int64(id));
                params.insert(format!("v{id}"), Value::Vector(vector_for(id)));
            }
            db.execute(&sql, &params).unwrap();
        }
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
        .rows(),
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
        .rows(),
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

// Stress tier: the property needs a suffix whose decoded vectors exceed the
// whole 16 MiB writer (3,000 x 6 KiB). The default tier keeps the cohort
// bound's witness in the interrupted-import test.
#[cfg(feature = "nightly-tests")]
#[test]
fn an_import_larger_than_the_writer_limit_is_absorbed_in_committed_cohorts_without_raising_it() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    const LIMIT: usize = 16 * 1024 * 1024;
    let suffix_rows = 3000;
    let root = tempfile::tempdir().unwrap();
    let db = fixture_in_commits(&root.path().join("cohorts.db"), LIMIT, suffix_rows, 16);
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
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 IVF (PROBES = 512))", &HashMap::new()).unwrap();
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

const CRASH_LIMIT: usize = 16 * 1024 * 1024;
const CRASH_SUFFIX_ROWS: usize = 480;

fn nearest(db: &Database, query: Vec<f32>) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(query))]),
    )
    .unwrap()
    .rows().to_vec()
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
 "CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({QUANTIZED_DIMENSION}) WITH (quantization = '{quantization}') PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 IVF (PROBES = 512) CONSOLIDATION NONE)"
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
        .rows(),
        vec![vec![Value::Int64(BASE_ROWS + suffix as i64 + 1)]]
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(target))]),
        )
        .unwrap()
        .rows(),
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
    for rows in [512usize, 256, 128, 64] {
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

// Stress tier: one committed LSN whose decoded vectors exceed the reopened
// writer's whole 16 MiB limit (4,096 x 6 KiB). The default tier keeps
// absorption above the cohort share in the commit sweep and the
// one-import-commit suite.
