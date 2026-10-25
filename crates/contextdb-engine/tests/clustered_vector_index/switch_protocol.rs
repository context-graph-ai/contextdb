//! While a published replacement remaps the legacy runs sealed under the old
//! codebook, every committed vector is answered exactly once: the set R of
//! legacy runs is fixed at the publication commit, each remap batch moves
//! whole runs, and at every batch boundary a full-depth search returns each
//! committed row once, with no duplicate and no omission. Writers keep
//! sealing under the new codebook, so R never grows. The old codebook stays
//! retained, with its whole-object charge, until the cursor exhausts R, and
//! is released then. A crash inside a remap batch recovers the same state and
//! finishes with the same property.

use super::support::{
    IndexTestOptions, MAX_MAINTENANCE_CYCLES, PublicationCut, SwitchPhase, SwitchState,
    build_progress, delta_runs, drive_until_serving, empty, int_ids, ivf, observe_publication_cuts,
    open_with_index_test_options, params, serving, switch_state, unit_vector,
};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::collections::BTreeSet;
use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

const DIMENSIONS: usize = 64;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(4 * 1024),
    build_batch_rows: Some(16),
};
const BUILT: std::ops::Range<i64> = 0..120;
const GROWTH: std::ops::Range<i64> = 1_000..1_240;
/// Single-row commits sealed under the old codebook while the replacement
/// trains: the legacy runs.
const LEGACY: std::ops::Range<i64> = 2_000..2_096;
/// Single-row commits made after the switch, sealed under the new codebook.
const AFTER_SWITCH: std::ops::Range<i64> = 3_000..3_064;

fn vector(id: i64) -> Vec<f32> {
    unit_vector(id as u64, DIMENSIONS)
}

fn create(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE switch_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
}

fn insert_one(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO switch_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector(id))),
        ]),
    )
    .expect("commit one vector");
}

fn insert_all(db: &Database, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO switch_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(vector(id))),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

/// A full-depth search in each mode returns every committed row exactly once.
fn assert_each_row_exactly_once(db: &Database, committed: &BTreeSet<i64>, stage: &str) {
    for mode in ["INDEXED", "EXACT"] {
        let ids = int_ids(
            &db.execute(
                &format!(
                    "SELECT id FROM switch_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT {}",
                    committed.len() + 8
                ),
                &params([("query", Value::Vector(vector(424_242)))]),
            )
            .expect("full-depth search"),
        );
        let distinct = ids.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(
            ids.len(),
            distinct.len(),
            "{stage}, {mode}: no committed row is answered twice"
        );
        assert_eq!(
            &distinct, committed,
            "{stage}, {mode}: every committed row is answered, and nothing else"
        );
    }
}

fn state(db: &Database) -> SwitchState {
    switch_state(db, "switch_items", "embedding", &[])
}

/// Serves the first generation, starts its replacement, seals the legacy
/// runs under the old codebook, and publishes the replacement. Returns the
/// committed rows, the old codebook and R as recorded at publication.
fn reach_state_b(db: &Database) -> (BTreeSet<i64>, u64, u64) {
    create(db);
    insert_all(db, BUILT);
    drive_until_serving(db);
    let old = serving(db, "switch_items", "embedding", &[]).expect("the first generation serves");
    insert_all(db, GROWTH);
    let mut training = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if build_progress(db, "switch_items", "embedding", &[]).in_flight {
            training = true;
            break;
        }
    }
    assert!(training, "the replacement started training");
    for id in LEGACY {
        insert_one(db, id);
    }
    let committed = BUILT.chain(GROWTH).chain(LEGACY).collect::<BTreeSet<_>>();
    let mut published = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if matches!(serving(db, "switch_items", "embedding", &[]), Some(now) if now.generation != old.generation)
        {
            published = true;
            break;
        }
    }
    assert!(published, "the replacement published");
    let at_switch = state(db);
    assert_eq!(
        at_switch.phase,
        SwitchPhase::Remapping,
        "the switch enters the remap state"
    );
    assert!(
        at_switch.legacy_runs >= 1,
        "R holds the runs sealed under the old codebook: {at_switch:?}"
    );
    assert!(
        at_switch.legacy_runs_remapped < at_switch.legacy_runs,
        "the publication commit records R before any of it is remapped: {at_switch:?}"
    );
    assert_eq!(
        at_switch.retained_codebook,
        Some(old.codebook),
        "the old codebook is retained"
    );
    assert!(
        at_switch.retained_codebook_charge_bytes > 0,
        "the retained codebook is charged whole"
    );
    (committed, old.codebook, at_switch.legacy_runs)
}

/// Drives remap cycles to completion, committing rows under the new codebook
/// between them, and checks every batch boundary.
fn remap_to_completion(
    db: &Database,
    committed: &mut BTreeSet<i64>,
    old_codebook: u64,
    legacy_runs: u64,
    stage: &str,
) {
    let mut after_switch = AFTER_SWITCH;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        let now = state(db);
        if now.phase == SwitchPhase::Serving {
            break;
        }
        assert_eq!(
            now.legacy_runs, legacy_runs,
            "{stage}: R stays fixed at the publication commit"
        );
        assert_eq!(
            (
                now.retained_codebook,
                now.retained_codebook_charge_bytes > 0
            ),
            (Some(old_codebook), true),
            "{stage}: the old codebook stays retained and charged until R is exhausted: {now:?}"
        );
        assert_each_row_exactly_once(db, committed, &format!("{stage}, remap boundary {now:?}"));
        for id in after_switch.by_ref().take(16) {
            insert_one(db, id);
            committed.insert(id);
        }
        let new_codebook = serving(db, "switch_items", "embedding", &[])
            .unwrap()
            .codebook;
        let tagged_old = delta_runs(db, "switch_items", "embedding", &[])
            .into_iter()
            .filter(|run| run.codebook == Some(old_codebook))
            .count() as u64;
        assert!(
            tagged_old <= legacy_runs - now.legacy_runs_remapped,
            "{stage}: new writes seal under the new codebook {new_codebook}, never into R"
        );
        assert_eq!(
            state(db).legacy_runs,
            legacy_runs,
            "{stage}: writes do not grow R"
        );
        db.run_maintenance_cycle().expect("one remap cycle");
    }
    let done = state(db);
    assert_eq!(
        done.phase,
        SwitchPhase::Serving,
        "{stage}: the cursor exhausts R"
    );
    assert_eq!(
        (done.retained_codebook, done.retained_codebook_charge_bytes),
        (None, 0),
        "{stage}: the old codebook and its charge are released once R is exhausted"
    );
    assert_each_row_exactly_once(db, committed, &format!("{stage}, after the remap"));
}

#[test]
fn every_committed_vector_is_answered_exactly_once_while_legacy_runs_are_remapped() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_index_test_options(&root.path().join("switch.db"), OPTIONS);
    let (mut committed, old_codebook, legacy_runs) = reach_state_b(&db);
    remap_to_completion(&db, &mut committed, old_codebook, legacy_runs, "remap");
    db.close().expect("close the store");
}

const CHILD_PATH: &str = "CONTEXTDB_REMAP_CRASH_CHILD_PATH";
const MARKER: &str = "REMAP_CRASH_CUT";

fn child_filter() -> String {
    let module = module_path!()
        .split_once("::")
        .map(|(_, rest)| rest)
        .unwrap_or(module_path!());
    format!(
        "{module}::a_crash_inside_a_remap_batch_recovers_and_finishes_with_each_row_exactly_once"
    )
}

fn run_child(path: &Path) {
    let db = open_with_index_test_options(path, OPTIONS);
    observe_publication_cuts(
        Arc::new(|cut| {
            if cut == PublicationCut::DuringRemapBatch {
                println!("{MARKER}");
                std::io::stdout().flush().unwrap();
                let mut byte = [0];
                std::io::stdin()
                    .read_exact(&mut byte)
                    .expect("the parent kills the paused child before releasing stdin");
            }
        }),
        || {
            for _ in 0..MAX_MAINTENANCE_CYCLES {
                db.run_maintenance_cycle().unwrap();
            }
        },
    );
}

#[test]
fn a_crash_inside_a_remap_batch_recovers_and_finishes_with_each_row_exactly_once() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        run_child(Path::new(&path));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("remap-crash.db");
    let (mut committed, old_codebook, legacy_runs, before) = {
        let db = open_with_index_test_options(&path, OPTIONS);
        let (committed, old_codebook, legacy_runs) = reach_state_b(&db);
        let before = state(&db);
        db.close().expect("close in the remap state");
        (committed, old_codebook, legacy_runs, before)
    };

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &child_filter(),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_PATH, &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut transcript = String::new();
    let mut reached = false;
    loop {
        let mut line = String::new();
        if stdout.read_line(&mut line).unwrap() == 0 {
            break;
        }
        transcript.push_str(&line);
        if line.trim() == MARKER {
            reached = true;
            break;
        }
    }
    assert!(
        reached,
        "the child reached the inside of a remap batch: {transcript}"
    );
    child.kill().unwrap();
    child.wait().unwrap();

    let db = open_with_index_test_options(&path, OPTIONS);
    assert_eq!(
        state(&db),
        before,
        "the uncommitted remap batch left the remap state exactly as it was"
    );
    assert_each_row_exactly_once(&db, &committed, "after the crash inside a remap batch");
    remap_to_completion(
        &db,
        &mut committed,
        old_codebook,
        legacy_runs,
        "after recovery",
    );
    db.close().unwrap();
}
