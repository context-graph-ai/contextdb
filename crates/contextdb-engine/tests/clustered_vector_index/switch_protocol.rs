//! While a published replacement remaps the legacy entries sealed under the
//! old codebook, every committed vector is answered exactly once. The legacy
//! set is fixed at the publication commit F1 as the delta entries with commit
//! LSN in (F0, F1], whose membership is exactly the rows committed in that
//! window; every maintenance call, including the publication call and the
//! call that completes the remap, prepares at most one bounded batch of
//! legacy entries, and the switch prepares each legacy entry exactly once;
//! and at every call boundary a full-depth search returns each committed row
//! once, with no duplicate and no omission. Writers keep sealing under the new codebook, so
//! the legacy set never changes. The old codebook stays retained, with a
//! positive charge, until the remap exhausts the legacy set, and is released
//! then. A crash inside a remap batch recovers the same state and finishes
//! with the same property.

use super::support::{
    IndexTestOptions, LegacyEntries, PublicationCut, ReplacementTrigger, SwitchPhase, SwitchState,
    build_progress, delta_entries, drive_until_serving, empty, int_ids, ivf,
    legacy_entries_prepared_total, maintenance_cycles, observe_publication_cuts,
    open_with_index_test_options, params, serving, switch_state, unit_vector, write_buffer,
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
/// trains: the legacy entries.
const LEGACY: std::ops::Range<i64> = 2_000..2_096;
/// Ids of single-row commits made after the switch start here.
const AFTER_SWITCH_FIRST: i64 = 3_000;
/// Commits per remap boundary after the switch: two write-buffer caps of
/// vectors, so every boundary seals.
const AFTER_SWITCH_PER_BOUNDARY: i64 = 32;
const BATCH: u64 = 16;

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

fn prepared(db: &Database) -> u64 {
    legacy_entries_prepared_total(db, "switch_items", "embedding", &[])
}

/// Runs one maintenance call and checks it prepared at most one bounded
/// batch of legacy entries, whatever phase it started or ended in.
fn bounded_cycle(db: &Database, stage: &str) {
    let before = prepared(db);
    db.run_maintenance_cycle().expect("one maintenance cycle");
    let after = prepared(db);
    assert!(
        after >= before && after - before <= BATCH,
        "{stage}: one maintenance call prepares at most one batch of {BATCH} legacy entries: \
         {before} then {after}"
    );
}

/// The legacy members recomputed from the actual delta entries: the
/// (primary key, commit LSN) of every entry with commit LSN in (F0, F1].
/// Before deduplicating, it checks that the raw selected entries, and their
/// distinct (key, version) pairs, are each exactly one per row the test
/// committed while the replacement trained, and that their keys are exactly
/// those rows.
fn legacy_members(db: &Database, legacy: &LegacyEntries, stage: &str) -> BTreeSet<(i64, u64)> {
    let selected = delta_entries(db, "switch_items", "embedding", &[])
        .into_iter()
        .filter(|entry| {
            entry.commit_lsn > legacy.after_lsn && entry.commit_lsn <= legacy.through_lsn
        })
        .map(|entry| match entry.primary_key {
            Value::Int64(id) => (id, entry.commit_lsn),
            other => panic!("the primary key is an integer, got {other:?}"),
        })
        .collect::<Vec<_>>();
    let members = selected.iter().copied().collect::<BTreeSet<_>>();
    let rows = LEGACY.count();
    assert_eq!(
        selected.len(),
        rows,
        "{stage}: the raw entries in (F0, F1] are exactly one per legacy row version: {selected:?}"
    );
    assert_eq!(
        members.len(),
        rows,
        "{stage}: the entries in (F0, F1] are distinct (key, version) pairs, one per legacy row: \
         {selected:?}"
    );
    assert_eq!(
        members.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>(),
        LEGACY.collect::<BTreeSet<_>>(),
        "{stage}: the entries in (F0, F1] hold exactly the rows committed while the replacement trained"
    );
    members
}

struct SwitchFixture {
    committed: BTreeSet<i64>,
    old_codebook: u64,
    legacy: LegacyEntries,
    /// The legacy membership captured at the publication commit.
    members: BTreeSet<(i64, u64)>,
    /// The cumulative preparation count before the publication call.
    prepared_before_publication: u64,
}

/// Serves the first generation, starts its replacement, seals the legacy
/// entries under the old codebook, and publishes the replacement.
fn reach_state_b(db: &Database) -> SwitchFixture {
    create(db);
    insert_all(db, BUILT);
    drive_until_serving(db, (BUILT.end - BUILT.start) as usize, BATCH as usize, 10);
    let old = serving(db, "switch_items", "embedding", &[]).expect("the first generation serves");
    insert_all(db, GROWTH);
    let mut training = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start + 300)
            as usize,
        BATCH as usize,
        10,
    ) {
        bounded_cycle(db, "while the replacement starts");
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
    let prepared_before_publication = prepared(db);
    let mut published = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start + 300)
            as usize,
        BATCH as usize,
        10,
    ) {
        bounded_cycle(db, "up to and including the publication call");
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
    let legacy = at_switch
        .legacy
        .unwrap_or_else(|| panic!("the publication commit records the legacy set: {at_switch:?}"));
    assert!(
        legacy.after_lsn < legacy.through_lsn,
        "the legacy frontier is a non-empty LSN window: {legacy:?}"
    );
    let members = legacy_members(db, &legacy, "at the publication commit");
    assert_eq!(
        members.len() as u64,
        legacy.entries,
        "the recorded size matches the membership"
    );
    assert!(
        at_switch.legacy_entries_remapped < legacy.entries,
        "legacy remap work is pending after the publication commit: {at_switch:?}"
    );
    assert_eq!(
        at_switch.retained_codebook,
        Some(old.codebook),
        "the old codebook is retained"
    );
    assert!(
        at_switch.retained_codebook_charge_bytes > 0,
        "the retained codebook holds a positive charge"
    );
    SwitchFixture {
        committed,
        old_codebook: old.codebook,
        legacy,
        members,
        prepared_before_publication,
    }
}

/// The legacy membership recomputed from the actual delta entries equals the
/// membership captured at the publication commit.
fn assert_legacy_membership(db: &Database, fixture: &SwitchFixture, stage: &str) {
    assert_eq!(
        legacy_members(db, &fixture.legacy, stage),
        fixture.members,
        "{stage}: the entries in (F0, F1] are exactly the legacy members captured at F1"
    );
}

/// Drives remap cycles to completion, sealing new commits under the new
/// codebook between them, and checks every call boundary.
fn remap_to_completion(db: &Database, fixture: &mut SwitchFixture, stage: &str) {
    let new_codebook = serving(db, "switch_items", "embedding", &[])
        .expect("the replacement serves")
        .codebook;
    let mut next_id = AFTER_SWITCH_FIRST;
    let mut remapped = state(db).legacy_entries_remapped;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start + 300)
            as usize,
        BATCH as usize,
        10,
    ) {
        let now = state(db);
        if now.phase == SwitchPhase::Serving {
            break;
        }
        assert_eq!(
            now.legacy,
            Some(fixture.legacy),
            "{stage}: the recorded legacy frontier, size and digest stay as at F1"
        );
        assert!(
            now.legacy_entries_remapped >= remapped,
            "{stage}: remap progress never goes back: {remapped} then {now:?}"
        );
        remapped = now.legacy_entries_remapped;
        assert_eq!(
            (
                now.retained_codebook,
                now.retained_codebook_charge_bytes > 0
            ),
            (Some(fixture.old_codebook), true),
            "{stage}: the old codebook stays retained and charged until the legacy set is \
             exhausted: {now:?}"
        );
        assert_legacy_membership(db, fixture, stage);
        assert_each_row_exactly_once(
            db,
            &fixture.committed,
            &format!("{stage}, remap boundary {now:?}"),
        );

        let seals_before = write_buffer(db, "switch_items", "embedding").sealed_runs;
        for _ in 0..AFTER_SWITCH_PER_BOUNDARY {
            insert_one(db, next_id);
            fixture.committed.insert(next_id);
            next_id += 1;
        }
        assert!(
            write_buffer(db, "switch_items", "embedding").sealed_runs > seals_before,
            "{stage}: the commits after the switch sealed"
        );
        for entry in delta_entries(db, "switch_items", "embedding", &[]) {
            if entry.commit_lsn > fixture.legacy.through_lsn && entry.sealed {
                assert_eq!(
                    entry.codebook,
                    Some(new_codebook),
                    "{stage}: an entry sealed after F1 carries the new codebook: {entry:?}"
                );
            }
        }
        assert_legacy_membership(db, fixture, &format!("{stage}, after new seals"));
        assert_each_row_exactly_once(db, &fixture.committed, &format!("{stage}, after new seals"));
        bounded_cycle(db, &format!("{stage}, remap call"));
    }
    let done = state(db);
    assert_eq!(
        done.phase,
        SwitchPhase::Serving,
        "{stage}: the remap exhausts the legacy set"
    );
    assert_eq!(
        (done.retained_codebook, done.retained_codebook_charge_bytes),
        (None, 0),
        "{stage}: the old codebook and its charge are released once the legacy set is exhausted"
    );
    assert_eq!(
        prepared(db) - fixture.prepared_before_publication,
        fixture.legacy.entries,
        "{stage}: the switch prepared each legacy entry exactly once"
    );
    assert_legacy_membership(db, fixture, &format!("{stage}, after the remap"));
    for entry in delta_entries(db, "switch_items", "embedding", &[]) {
        if entry.sealed {
            assert_eq!(
                entry.codebook,
                Some(new_codebook),
                "{stage}: after the remap every sealed entry is on the new codebook: {entry:?}"
            );
        }
    }
    assert_each_row_exactly_once(db, &fixture.committed, &format!("{stage}, after the remap"));
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn every_committed_vector_is_answered_exactly_once_while_legacy_entries_are_remapped() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_index_test_options(&root.path().join("switch.db"), OPTIONS);
    let mut fixture = reach_state_b(&db);
    remap_to_completion(&db, &mut fixture, "remap");
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
                println!("\n{MARKER}");
                std::io::stdout().flush().unwrap();
                let mut byte = [0];
                std::io::stdin()
                    .read_exact(&mut byte)
                    .expect("the parent kills the paused child before releasing stdin");
            }
        }),
        || {
            for _ in 0..maintenance_cycles(
                (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start
                    + 300) as usize,
                BATCH as usize,
                10,
            ) {
                db.run_maintenance_cycle().unwrap();
            }
        },
    );
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn a_crash_inside_a_remap_batch_recovers_and_finishes_with_each_row_exactly_once() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        run_child(Path::new(&path));
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("remap-crash.db");
    let (mut fixture, before, prepared_at_close) = {
        let db = open_with_index_test_options(&path, OPTIONS);
        let fixture = reach_state_b(&db);
        let before = state(&db);
        let prepared_at_close = prepared(&db);
        db.close().expect("close in the remap state");
        (fixture, before, prepared_at_close)
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
    assert_eq!(
        prepared(&db),
        prepared_at_close,
        "the uncommitted remap batch counts no preparation"
    );
    assert_legacy_membership(&db, &fixture, "after the crash inside a remap batch");
    assert_each_row_exactly_once(
        &db,
        &fixture.committed,
        "after the crash inside a remap batch",
    );
    remap_to_completion(&db, &mut fixture, "after recovery");
    db.close().unwrap();
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn a_replacement_due_while_legacy_entries_are_remapped_waits_for_the_remap_to_finish() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_with_index_test_options(&root.path().join("deferred.db"), OPTIONS);
    let mut fixture = reach_state_b(&db);
    let remapping_generation = serving(&db, "switch_items", "embedding", &[])
        .expect("the replacement serves")
        .generation;
    // Far more than the declared change percent: another replacement is due.
    insert_all(&db, 4_000..4_300);
    fixture.committed.extend(4_000..4_300);

    let mut deferred = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start + 300)
            as usize,
        BATCH as usize,
        10,
    ) {
        bounded_cycle(&db, "with a replacement due during the remap");
        if state(&db).phase == SwitchPhase::Serving {
            break;
        }
        let progress = build_progress(&db, "switch_items", "embedding", &[]);
        assert!(
            !progress.in_flight,
            "no second replacement starts while legacy entries are remapped: {progress:?}"
        );
        assert_eq!(
            (progress.replacement_pending, progress.pending_trigger),
            (true, Some(ReplacementTrigger::ChangePercent)),
            "inspection shows the deferred replacement and its trigger: {progress:?}"
        );
        assert_eq!(
            serving(&db, "switch_items", "embedding", &[])
                .unwrap()
                .generation,
            remapping_generation,
            "the remapping generation keeps serving"
        );
        assert_legacy_membership(&db, &fixture, "with a replacement pending");
        assert_each_row_exactly_once(&db, &fixture.committed, "with a replacement pending");
        deferred = true;
    }
    assert!(
        deferred,
        "the replacement fell due while the remap was still running"
    );
    assert_eq!(state(&db).phase, SwitchPhase::Serving, "the remap finished");
    let waiting = build_progress(&db, "switch_items", "embedding", &[]);
    assert!(
        !waiting.in_flight && waiting.replacement_pending,
        "at the end of the remap the replacement is still pending, not yet started: {waiting:?}"
    );

    bounded_cycle(&db, "the first call after the remap");
    let started = build_progress(&db, "switch_items", "embedding", &[]);
    assert!(
        started.in_flight && !started.replacement_pending,
        "the deferred replacement starts at the next maintenance call: {started:?}"
    );

    let mut finished = false;
    for _ in 0..maintenance_cycles(
        (BUILT.end - BUILT.start + GROWTH.end - GROWTH.start + LEGACY.end - LEGACY.start + 300)
            as usize,
        BATCH as usize,
        10,
    ) * 2
    {
        bounded_cycle(&db, "the deferred replacement");
        let now = state(&db);
        if now.phase == SwitchPhase::Serving
            && serving(&db, "switch_items", "embedding", &[])
                .unwrap()
                .generation
                != remapping_generation
        {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "the deferred replacement publishes and its own remap finishes"
    );
    assert_each_row_exactly_once(&db, &fixture.committed, "after the deferred replacement");
    db.close().expect("close the store");
}
