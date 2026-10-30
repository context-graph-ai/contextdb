//! Writes stay searchable across a Context's codebook lifecycle. Before the
//! first codebook exists, sealed runs are unclustered and searched exactly,
//! and the first publication assigns them. While a replacement trains a new
//! codebook, writers keep sealing runs under the serving one; after the
//! switch those runs are searched through their old assignment until a
//! bounded remap, resumable from a persisted cursor, moves them to the new
//! codebook. Readers on older snapshots keep their captured roots. A crash
//! immediately before or immediately after the generation switch leaves one
//! complete generation with every committed row served, and recovery
//! finishes the switch and the remap.

use super::support::{
    IndexTestOptions, PublicationCut, build_progress, cosine_distance, count, delta_runs,
    drive_until_serving, empty, int_ids, ivf, maintenance_cycles, observe_publication_cuts,
    observe_search, open_with_index_test_options, params, remap_progress, retained_generations,
    serving, unit_vector, write_buffer,
};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

const DIMENSIONS: usize = 64;
const CAP_BYTES: u64 = 4 * 1024;
const BATCH: u64 = 16;
const OPTIONS: IndexTestOptions = IndexTestOptions {
    write_buffer_cap_bytes: Some(CAP_BYTES),
    build_batch_rows: Some(BATCH),
};
const BUILT: std::ops::Range<i64> = 0..120;
const GROWTH: std::ops::Range<i64> = 1_000..1_240;
/// Single-row commits whose vectors alone are six buffer caps.
const SEALED: std::ops::Range<i64> = 2_000..2_096;

fn create(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE life_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
}

fn vector(id: i64) -> Vec<f32> {
    unit_vector(id as u64, DIMENSIONS)
}

fn insert_one(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO life_items (id, embedding) VALUES ($id, $embedding)",
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
            "INSERT INTO life_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("embedding", Value::Vector(vector(id))),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

fn self_search(db: &Database, id: i64, mode: &str) -> (Vec<i64>, bool) {
    let (result, work) = observe_search(db, || {
        db.execute(
            &format!(
                "SELECT id FROM life_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 1"
            ),
            &params([("query", Value::Vector(vector(id)))]),
        )
        .expect("self search")
    });
    (int_ids(&result), work.clustered_route)
}

/// Every id is found first by an indexed search for its own vector, on the
/// clustered route.
fn assert_served(db: &Database, ids: impl IntoIterator<Item = i64>, stage: &str) {
    for id in ids {
        let (found, clustered) = self_search(db, id, "INDEXED");
        assert!(
            clustered,
            "{stage}: row {id} was searched on the clustered index"
        );
        assert_eq!(found, vec![id], "{stage}: row {id} is served");
    }
}

fn all_ids() -> Vec<i64> {
    BUILT.chain(GROWTH).chain(SEALED).collect()
}

/// A sample that always includes every row sealed during training.
fn sample_ids() -> Vec<i64> {
    BUILT
        .step_by(16)
        .chain(GROWTH.step_by(16))
        .chain(SEALED)
        .collect()
}

fn drive_remap_to_completion(db: &Database, stage: &str) {
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + SEALED.end - SEALED.start) as usize,
        BATCH as usize,
        10,
    ) {
        if remap_progress(db, "life_items", "embedding", &[]).entries_remaining == 0 {
            return;
        }
        db.run_maintenance_cycle().expect("one maintenance cycle");
    }
    panic!(
        "{stage}: the remap did not finish in {} cycles",
        maintenance_cycles(
            (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + SEALED.end - SEALED.start)
                as usize,
            BATCH as usize,
            10
        )
    );
}

fn assert_every_run_on_the_serving_codebook(db: &Database, stage: &str) {
    let codebook = serving(db, "life_items", "embedding", &[])
        .expect("a generation is serving")
        .codebook;
    for run in delta_runs(db, "life_items", "embedding", &[]) {
        assert_eq!(
            run.codebook,
            Some(codebook),
            "{stage}: every sealed run is assigned to the serving codebook: {run:?}"
        );
    }
}

#[test]
fn writes_before_the_first_codebook_stay_exactly_searchable_and_the_first_publication_assigns_them()
{
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_index_test_options(&root.path().join("first.db"), OPTIONS);
    create(&db);
    let ids = SEALED.collect::<Vec<_>>();
    for id in &ids {
        insert_one(&db, *id);
        for mode in ["EXACT", "AUTO"] {
            assert_eq!(
                self_search(&db, *id, mode).0,
                vec![*id],
                "row {id} is found by {mode} as soon as its commit returns, before any codebook"
            );
        }
    }
    assert_eq!(
        serving(&db, "life_items", "embedding", &[]),
        None,
        "no generation is published without maintenance"
    );
    let buffer = write_buffer(&db, "life_items", "embedding");
    assert!(
        buffer.sealed_runs >= 2 && buffer.high_water_bytes <= buffer.cap_bytes,
        "the writes passed the buffer cap and sealed runs instead of growing the buffer: {buffer:?}"
    );
    let runs = delta_runs(&db, "life_items", "embedding", &[]);
    assert!(!runs.is_empty(), "the sealed writes are in delta runs");
    for run in &runs {
        assert_eq!(
            run.codebook, None,
            "a run sealed before any codebook is unclustered: {run:?}"
        );
    }
    let query = vector(77_777);
    let mut expected = ids
        .iter()
        .map(|id| (cosine_distance(&query, &vector(*id)), *id))
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| left.partial_cmp(right).unwrap());
    assert_eq!(
        int_ids(
            &db.execute(
                "SELECT id FROM life_items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10",
                &params([("query", Value::Vector(query))]),
            )
            .unwrap()
        ),
        expected[..10].iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        "the unclustered runs are searched exactly"
    );

    drive_until_serving(&db, ids.len(), BATCH as usize, 10);
    assert!(serving(&db, "life_items", "embedding", &[]).is_some());
    drive_remap_to_completion(&db, "first publication");
    assert_every_run_on_the_serving_codebook(&db, "first publication");
    assert_eq!(count(&db, "life_items"), ids.len() as i64);
    assert_served(&db, ids, "after the first publication");
}

/// Builds the first generation, makes a replacement due and starts it, then
/// commits single rows that seal runs under the first codebook while the
/// replacement trains. Returns (first generation, first codebook).
fn seal_runs_while_a_replacement_trains(db: &Database) -> (u64, u64) {
    create(db);
    insert_all(db, BUILT);
    drive_until_serving(db, (BUILT.end - BUILT.start) as usize, BATCH as usize, 10);
    let first = serving(db, "life_items", "embedding", &[]).expect("the first generation serves");
    insert_all(db, GROWTH);
    let mut training = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + SEALED.end - SEALED.start) as usize,
        BATCH as usize,
        10,
    ) {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if build_progress(db, "life_items", "embedding", &[]).in_flight {
            training = true;
            break;
        }
    }
    assert!(training, "the replacement started training");
    let seals_before = write_buffer(db, "life_items", "embedding").sealed_runs;
    for id in SEALED {
        insert_one(db, id);
        let (found, clustered) = self_search(db, id, "INDEXED");
        assert!(
            clustered && found == vec![id],
            "row {id} is served while the replacement trains"
        );
    }
    assert!(
        build_progress(db, "life_items", "embedding", &[]).in_flight,
        "the runs were sealed while the replacement was still training"
    );
    assert!(
        write_buffer(db, "life_items", "embedding").sealed_runs >= seals_before + 2,
        "several runs were sealed during training"
    );
    let sealed_under_first = delta_runs(db, "life_items", "embedding", &[])
        .into_iter()
        .filter(|run| run.codebook == Some(first.codebook))
        .map(|run| run.entries)
        .sum::<u64>();
    assert!(
        sealed_under_first > 2 * BATCH,
        "the runs sealed during training are under the serving codebook and need several remap batches"
    );
    (first.generation, first.codebook)
}

#[test]
fn runs_sealed_while_a_replacement_trains_stay_served_and_are_remapped_in_bounded_resumable_batches()
 {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("remap.db");
    let mut db = open_with_index_test_options(&path, OPTIONS);
    let (first, first_codebook) = seal_runs_while_a_replacement_trains(&db);
    let sql = "SELECT id FROM life_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    let query = params([("query", Value::Vector(vector(2_005)))]);

    {
        let old = db.snapshot();
        let _pin = db.pin_snapshot(old).expect("pin an old reader");
        let old_answer = int_ids(&db.execute_at_snapshot(sql, &query, old).unwrap());
        let mut published = false;
        for _ in 0..maintenance_cycles(
            (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + SEALED.end - SEALED.start)
                as usize,
            BATCH as usize,
            10,
        ) {
            db.run_maintenance_cycle().expect("one maintenance cycle");
            assert_eq!(count(&db, "life_items"), all_ids().len() as i64);
            assert_served(
                &db,
                sample_ids(),
                "while the replacement trains and publishes",
            );
            assert_eq!(
                int_ids(&db.execute_at_snapshot(sql, &query, old).unwrap()),
                old_answer,
                "the old reader keeps its captured roots"
            );
            if matches!(serving(&db, "life_items", "embedding", &[]), Some(now) if now.generation != first)
            {
                published = true;
                break;
            }
        }
        assert!(published, "the replacement published");
        let replacement = serving(&db, "life_items", "embedding", &[]).unwrap();
        assert_ne!(
            replacement.codebook, first_codebook,
            "the replacement trained a new codebook"
        );
        assert!(
            retained_generations(&db, "life_items", "embedding", &[]).contains(&first),
            "the pinned reader keeps the old generation"
        );

        // One bounded remap step under the old reader.
        let before = remap_progress(&db, "life_items", "embedding", &[]);
        assert!(
            before.entries_remaining > BATCH && before.cursor.is_some(),
            "runs sealed under the old codebook await a remap with a cursor: {before:?}"
        );
        db.run_maintenance_cycle().expect("one maintenance cycle");
        let after = remap_progress(&db, "life_items", "embedding", &[]);
        assert!(
            after.entries_remaining < before.entries_remaining
                && before.entries_remaining - after.entries_remaining <= BATCH,
            "one cycle remaps one bounded batch: {before:?} then {after:?}"
        );
        assert_served(&db, sample_ids(), "mid-remap");
        assert_eq!(
            int_ids(&db.execute_at_snapshot(sql, &query, old).unwrap()),
            old_answer,
            "the old reader keeps its captured roots through the remap"
        );
    }

    // The cursor is persisted: a reopen resumes exactly where the remap was.
    let persisted = remap_progress(&db, "life_items", "embedding", &[]);
    db.close().expect("close mid-remap");
    drop(db);
    db = open_with_index_test_options(&path, OPTIONS);
    assert_eq!(
        remap_progress(&db, "life_items", "embedding", &[]),
        persisted,
        "the remap resumes from its persisted cursor"
    );
    let mut batches = 0;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + SEALED.end - SEALED.start) as usize,
        BATCH as usize,
        10,
    ) {
        let before = remap_progress(&db, "life_items", "embedding", &[]);
        if before.entries_remaining == 0 {
            break;
        }
        db.run_maintenance_cycle().expect("one maintenance cycle");
        let after = remap_progress(&db, "life_items", "embedding", &[]);
        assert!(
            before.entries_remaining - after.entries_remaining <= BATCH,
            "each cycle remaps at most one bounded batch: {before:?} then {after:?}"
        );
        if after.entries_remaining < before.entries_remaining {
            batches += 1;
        }
        assert_served(&db, sample_ids(), "during the resumed remap");
    }
    assert!(
        batches >= 1,
        "the resumed remap made progress in bounded batches"
    );
    assert_eq!(
        remap_progress(&db, "life_items", "embedding", &[]).entries_remaining,
        0
    );
    assert_every_run_on_the_serving_codebook(&db, "after the remap");
    assert_served(&db, all_ids(), "after the remap");
    db.close().expect("close the store");
}

const CHILD_PATH: &str = "CONTEXTDB_CODEBOOK_SWITCH_CHILD_PATH";
const CHILD_CUT: &str = "CONTEXTDB_CODEBOOK_SWITCH_CHILD_CUT";
const MARKER: &str = "CODEBOOK_SWITCH_CUT";
const FINISHED: &str = "CODEBOOK_SWITCH_FINISHED";

fn child_filter() -> String {
    let module = module_path!()
        .split_once("::")
        .map(|(_, rest)| rest)
        .unwrap_or(module_path!());
    format!(
        "{module}::a_crash_immediately_before_or_after_the_codebook_switch_leaves_every_row_served"
    )
}

fn run_child(path: &Path, selected: &str) {
    let db = open_with_index_test_options(path, OPTIONS);
    let selected = selected.to_owned();
    observe_publication_cuts(
        Arc::new(move |cut| {
            if format!("{cut:?}") == selected {
                println!("\n{MARKER} {cut:?}");
                std::io::stdout().flush().unwrap();
                let mut byte = [0];
                std::io::stdin()
                    .read_exact(&mut byte)
                    .expect("the parent kills the paused child before releasing stdin");
            }
        }),
        || drive_until_serving(&db, 456, 16, 10),
    );
    db.close().unwrap();
    println!("{FINISHED}");
}

#[test]
fn a_crash_immediately_before_or_after_the_codebook_switch_leaves_every_row_served() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        run_child(Path::new(&path), &std::env::var(CHILD_CUT).unwrap());
        return;
    }
    for selected in [
        PublicationCut::BeforeGenerationPublication,
        PublicationCut::AfterGenerationPublication,
    ]
    .map(|cut| format!("{cut:?}"))
    .into_iter()
    .chain(["FinishedControl".to_owned()])
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("switch.db");
        let (first, first_codebook) = {
            let db = open_with_index_test_options(&path, OPTIONS);
            let state = seal_runs_while_a_replacement_trains(&db);
            db.close().expect("close the prepared store");
            state
        };

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &child_filter(),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_CUT, &selected)
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
            if line.trim() == format!("{MARKER} {selected}") {
                reached = true;
                break;
            }
        }
        if selected == "FinishedControl" {
            let status = child.wait().unwrap();
            assert!(
                status.success() && transcript.contains(FINISHED),
                "the uninterrupted child finishes the switch: {transcript}"
            );
        } else {
            assert!(
                reached,
                "the switch reached the {selected} boundary: {transcript}"
            );
            child.kill().unwrap();
            child.wait().unwrap();
        }

        let db = open_with_index_test_options(&path, OPTIONS);
        let now = serving(&db, "life_items", "embedding", &[])
            .expect("a complete generation serves after reopen");
        match selected.as_str() {
            "BeforeGenerationPublication" => {
                assert_eq!(
                    now.generation, first,
                    "before the switch the old generation serves"
                );
                assert_eq!(now.codebook, first_codebook);
                for run in delta_runs(&db, "life_items", "embedding", &[]) {
                    assert_eq!(
                        run.codebook,
                        Some(first_codebook),
                        "before the switch every run is under the old codebook: {run:?}"
                    );
                }
            }
            "AfterGenerationPublication" => {
                assert_ne!(
                    now.generation, first,
                    "after the switch the new generation serves"
                );
                assert_ne!(now.codebook, first_codebook);
                let pending = remap_progress(&db, "life_items", "embedding", &[]);
                assert!(
                    pending.entries_remaining > 0 && pending.cursor.is_some(),
                    "immediately after the switch the old-codebook runs await their persisted remap: {pending:?}"
                );
            }
            _ => assert_ne!(
                now.generation, first,
                "the finished switch serves the new generation"
            ),
        }
        assert_eq!(count(&db, "life_items"), all_ids().len() as i64);
        assert_served(&db, all_ids(), &format!("{selected}, on reopen"));

        drive_until_serving(&db, 456, 16, 10);
        drive_remap_to_completion(&db, &selected);
        assert_ne!(
            serving(&db, "life_items", "embedding", &[])
                .unwrap()
                .generation,
            first,
            "{selected}: recovery finishes the switch"
        );
        assert_every_run_on_the_serving_codebook(&db, &selected);
        assert_served(&db, all_ids(), &format!("{selected}, after recovery"));
        db.close().unwrap();
    }
}
