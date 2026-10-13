//! Proof rows for the bounded maintenance reclaim step (`Database::reclaim_deferred_step`).
//!
//! Every database here is built with the engine's ordinary-commit budget of 64 extents and 64
//! entries, so ordinary commits leave a deferred-free backlog behind exactly as they do in a
//! long-lived writer. The reclaim step is the only thing in these tests that drains it quickly.
//!
//! Safety is judged by what a held reader can still read after pages were reused, never by counts
//! alone: a step that freed a page the reader still needed would let a later write overwrite it.

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, ReclaimBoundary, ReclaimStep,
    TableDefinition,
};
use std::path::Path;

#[cfg(unix)]
struct ReclaimChild {
    child: Option<std::process::Child>,
}

#[cfg(unix)]
impl ReclaimChild {
    fn new(child: std::process::Child) -> Self {
        Self { child: Some(child) }
    }

    fn status(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.as_mut().unwrap().try_wait()
    }

    fn kill_and_reap(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.child.take().unwrap();
        child.kill()?;
        child.wait()
    }
}

#[cfg(unix)]
impl Drop for ReclaimChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("rows");
const ROWS: u64 = 1_500;

fn payload(generation: u64, key: u64) -> Vec<u8> {
    let mut bytes = format!("g{generation}-k{key}-").into_bytes();
    bytes.resize(96, b'.');
    bytes
}

fn open(path: &Path) -> Database {
    open_with(path, false)
}

fn open_with(path: &Path, keep_pace: bool) -> Database {
    open_budget(path, keep_pace, 64)
}

fn open_budget(path: &Path, keep_pace: bool, ordinary_budget: usize) -> Database {
    Database::builder()
        .set_deferred_free_budget(ordinary_budget, ordinary_budget)
        .set_keep_pace_reclaim(keep_pace)
        .create(path)
        .expect("create database with the ordinary-commit reclaim budget")
}

fn seed(db: &Database) {
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(TABLE).unwrap();
        for key in 0..ROWS {
            table.insert(key, payload(0, key).as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
}

/// One durable commit that rewrites every third row, freeing scattered leaf pages.
fn churn(db: &Database, generation: u64) {
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(TABLE).unwrap();
        for key in (0..ROWS).step_by(3) {
            table
                .insert(key, payload(generation, key).as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
}

fn backlog(db: &Database) -> [(usize, usize); 2] {
    db.deferred_free_backlog_for_test().unwrap()
}

fn total(counts: [(usize, usize); 2]) -> (usize, usize) {
    (counts[0].0 + counts[1].0, counts[0].1 + counts[1].1)
}

/// Rows `key % 3 == 0` carry `generation`, the rest are still generation 0.
fn assert_snapshot(read: &redb::ReadTransaction, generation: u64) {
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), ROWS);
    for (key, value) in table.iter().unwrap().map(|row| {
        let row = row.unwrap();
        (row.0.value(), row.1.value().to_vec())
    }) {
        let expected = if key % 3 == 0 { generation } else { 0 };
        assert_eq!(
            value,
            payload(expected, key),
            "key {key} at generation {generation}"
        );
    }
}

/// Call the step until it stops progressing. Returns the final non-progress verdict, the boundary
/// and how many steps progressed.
fn drive(
    db: &Database,
    mut boundary: Option<ReclaimBoundary>,
    limit: usize,
) -> (ReclaimStep, Option<ReclaimBoundary>, usize) {
    let mut progressed = 0;
    for _ in 0..limit {
        match db.reclaim_deferred_step(boundary, 4096, 4096).unwrap() {
            ReclaimStep::Progressed(next) => {
                boundary = Some(next);
                progressed += 1;
            }
            other => return (other, boundary, progressed),
        }
    }
    panic!("reclaim did not settle within {limit} steps");
}

#[test]
fn an_idle_tail_drains_through_steps_alone() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("idle.redb"));
    seed(&db);
    // A reader held across the churn parks the whole history; releasing it and making one last
    // small write leaves a backlog that only maintenance steps (no further writes) can drain.
    let reader = db.begin_read().unwrap();
    for generation in 1..=40 {
        churn(&db, generation);
    }
    drop(reader);
    churn(&db, 41);
    let before = total(backlog(&db));
    assert!(
        before.1 > 64 * 4,
        "the final write must leave a material backlog: {before:?}"
    );
    let (verdict, boundary, progressed) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert!(boundary.is_some() && progressed >= 1);
    // The last write's own frees are inside the sampled prefix, so nothing of the pre-step history
    // is left. Only the steps' own bookkeeping may remain, never more than the steps took.
    let after = total(backlog(&db));
    assert!(
        after.0 <= progressed + 1,
        "{before:?} -> {after:?} after {progressed} steps"
    );
    assert!(after.1 < before.1 / 8, "{before:?} -> {after:?}");
    assert_snapshot(&db.begin_read().unwrap(), 41);
}

#[test]
fn one_oversized_pending_entry_drains_across_several_steps() {
    const BIG: TableDefinition<u64, &[u8]> = TableDefinition::new("big");
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("oversized.redb"));
    let value = vec![7u8; 1_000];
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(BIG).unwrap();
        for key in 0..30_000u64 {
            table.insert(key, value.as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let reader = db.begin_read().unwrap();
    redb::commit_work::reset_commit_pages_for_test();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(BIG).unwrap();
        for key in 0..30_000u64 {
            table.insert(key, vec![8u8; 1_000].as_slice()).unwrap();
        }
    }
    tx.commit().unwrap();
    let work = redb::commit_work::take_commit_pages_for_test();
    assert!(
        work.freed_pages > 2 * 4096,
        "one commit must free more than two steps' extents: {work:?}"
    );
    drop(reader);
    let before = backlog(&db);
    let (verdict, _, progressed) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert!(
        progressed >= 3,
        "an oversized entry needs several steps: {progressed} from {before:?}"
    );
    assert!(total(backlog(&db)).1 < 64, "{:?}", backlog(&db));
}

#[test]
fn a_held_reader_yields_reader_held_and_loses_nothing_it_needs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("held.redb"));
    seed(&db);
    let reader = db.begin_read().unwrap();
    for generation in 1..=30 {
        churn(&db, generation);
    }
    let before = backlog(&db);
    let (verdict, _, progressed) = drive(&db, None, 50);
    assert_eq!(
        verdict,
        ReclaimStep::ReaderHeld,
        "everything is held by the reader"
    );
    assert_eq!(progressed, 0);
    assert_eq!(backlog(&db), before, "a held step must change nothing");
    // Reuse pages as hard as possible, then check the reader still sees its snapshot.
    for generation in 31..=40 {
        churn(&db, generation);
    }
    assert_snapshot(&reader, 0);
    drop(reader);
    let (verdict, _, progressed) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert!(progressed >= 1, "released work must drain");
    assert!(total(backlog(&db)).0 <= progressed + 1);
    assert_snapshot(&db.begin_read().unwrap(), 40);
}

#[test]
fn releasing_older_readers_one_at_a_time_never_frees_what_the_newer_one_reads() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("partial.redb"));
    seed(&db);
    let mut readers = Vec::new();
    for generation in 1..=4u64 {
        readers.push((generation - 1, db.begin_read().unwrap()));
        for round in 0..8 {
            churn(&db, generation * 10 + round);
        }
    }
    // Reader i saw the state after churn round of generation i-th block; track expected content.
    let expected: Vec<u64> = vec![0, 17, 27, 37];
    let mut previous = total(backlog(&db)).0;
    let mut released_with_progress = 0;
    while !readers.is_empty() {
        readers.remove(0);
        let (_, _, progressed) = drive(&db, None, 200);
        if progressed > 0 {
            released_with_progress += 1;
        }
        for generation in 100..104 {
            churn(&db, generation);
        }
        for (index, (_, reader)) in readers.iter().enumerate() {
            let remaining_index = expected.len() - readers.len() + index;
            assert_snapshot(reader, expected[remaining_index]);
        }
        let now = total(backlog(&db)).0;
        assert!(now > 0 || readers.is_empty());
        previous = now;
    }
    let _ = previous;
    assert!(
        released_with_progress >= 3,
        "each release must let steps progress"
    );
}

#[test]
fn frees_added_after_the_boundary_are_never_reclaimed_by_that_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_budget(&dir.path().join("boundary.redb"), false, 1);
    seed(&db);
    for generation in 1..=20 {
        churn(&db, generation);
    }
    // A deliberately small first step leaves pre-boundary work pending while the writer adds more.
    let first = db.reclaim_deferred_step(None, 8, 8).unwrap();
    let ReclaimStep::Progressed(boundary) = first else {
        panic!("a plain backlog must progress: {first:?}");
    };
    // A foreground writer keeps adding frees in both trees while the cycle continues.
    let added = 60;
    for generation in 21..21 + added {
        churn(&db, generation);
    }
    let entries_before = total(backlog(&db)).0;
    let (verdict, _, _) = drive(&db, Some(boundary), 200);
    assert_eq!(
        verdict,
        ReclaimStep::Drained,
        "nothing below the boundary remains"
    );
    let entries_after = total(backlog(&db)).0;
    assert!(
        entries_after >= added as usize,
        "{added} post-boundary commits' frees must survive the cycle: {entries_before} -> {entries_after}"
    );
    // A fresh cycle samples a new boundary and reclaims them once more.
    let (verdict, _, progressed) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert!(progressed >= 1);
    assert!(total(backlog(&db)).0 < entries_after);
    assert_snapshot(&db.begin_read().unwrap(), 20 + added);
}

#[test]
fn a_savepoint_blocks_every_step() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("savepoint.redb"));
    seed(&db);
    let tx = db.begin_write().unwrap();
    let savepoint = tx.ephemeral_savepoint().unwrap();
    tx.commit().unwrap();
    for generation in 1..=10 {
        churn(&db, generation);
    }
    let before = backlog(&db);
    assert_eq!(
        db.reclaim_deferred_step(None, 4096, 4096).unwrap(),
        ReclaimStep::Blocked
    );
    assert_eq!(backlog(&db), before);
    drop(savepoint);
    let (verdict, _, progressed) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert!(progressed >= 1);
}

#[test]
fn one_step_is_bounded_by_its_extent_and_entry_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(&dir.path().join("bounded.redb"));
    seed(&db);
    for generation in 1..=60 {
        churn(&db, generation);
    }
    let before = total(backlog(&db));
    redb::commit_work::reset_commit_pages_for_test();
    let step = db.reclaim_deferred_step(None, 8, 8).unwrap();
    let work = redb::commit_work::take_commit_pages_for_test();
    assert!(matches!(step, ReclaimStep::Progressed(_)));
    assert_eq!(work.commits, 1);
    assert!(
        work.reclaimed_pages <= 8 && work.reclaimed_pages >= 1,
        "{work:?}"
    );
    assert!(work.reclaimed_entries <= 8, "{work:?} from {before:?}");
}

/// A child process commits a few steps and aborts between them; the parent reopens the durable
/// remainder, finds a valid store and the same snapshot content, and finishes the drain.
#[test]
fn a_crash_between_steps_leaves_the_remainder_and_recovers() {
    const ROLE: &str = "WRITECOST_REDB_CRASH_CHILD";
    const PATH: &str = "WRITECOST_REDB_CRASH_PATH";
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.redb");
    {
        let db = open(&path);
        seed(&db);
        for generation in 1..=40 {
            churn(&db, generation);
        }
    }
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(exe)
        .args([
            "--exact",
            "crash_child_steps_then_aborts",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ROLE, "1")
        .env(PATH, &path)
        .status()
        .unwrap();
    assert!(!status.success(), "the child must die by abort");
    let mut db = open(&path);
    assert!(
        db.check_integrity().unwrap(),
        "store must verify after the crash"
    );
    let remainder = total(backlog(&db));
    assert!(
        remainder.0 > 0,
        "a crash between steps leaves the durable remainder"
    );
    assert_snapshot(&db.begin_read().unwrap(), 40);
    let (verdict, _, _) = drive(&db, None, 200);
    assert_eq!(verdict, ReclaimStep::Drained);
    assert_snapshot(&db.begin_read().unwrap(), 40);
}

#[test]
fn crash_child_steps_then_aborts() {
    let (Ok(_), Ok(path)) = (
        std::env::var("WRITECOST_REDB_CRASH_CHILD"),
        std::env::var("WRITECOST_REDB_CRASH_PATH"),
    ) else {
        return;
    };
    let db = open(Path::new(&path));
    let mut boundary = None;
    for _ in 0..2 {
        match db.reclaim_deferred_step(boundary, 16, 16).unwrap() {
            ReclaimStep::Progressed(next) => boundary = Some(next),
            other => panic!("the child needs progressing steps: {other:?}"),
        }
    }
    std::process::abort();
}

/// One commit that rewrites `rows` scattered rows so it frees many page extents at once.
fn big_commit(db: &Database, generation: u64, rows: u64) -> redb::commit_work::CommitPagesForTest {
    redb::commit_work::reset_commit_pages_for_test();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(TABLE).unwrap();
        for key in (0..rows).step_by(1) {
            table
                .insert(key, payload(generation, key).as_slice())
                .unwrap();
        }
    }
    tx.commit().unwrap();
    redb::commit_work::take_commit_pages_for_test()
}

/// Keep-pace reclaim: one large commit freeing K > 64 extents, with at least K eligible older
/// extents pending, reclaims at least K and at most max(64, K). Without keep-pace the same commit
/// reclaims 64. Small commits stay at 64.
#[test]
fn a_large_commit_reclaims_at_least_what_it_frees_and_never_past_the_floor() {
    let mut measured = Vec::new();
    for keep_pace in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_with(&dir.path().join("keep-pace.redb"), keep_pace);
        seed(&db);
        for generation in 1..=12 {
            big_commit(&db, generation, ROWS);
        }
        // Everything above is older than the next commit and no reader holds it.
        let freed_probe = big_commit(&db, 13, ROWS);
        let freed = freed_probe.freed_pages;
        assert!(
            freed > 64,
            "the commit must free more than the ordinary budget: {freed_probe:?}"
        );
        let work = big_commit(&db, 14, ROWS);
        let own = work.freed_pages;
        if keep_pace {
            assert!(
                work.reclaimed_pages >= own.min(1_000_000) || backlog(&db) == [(0, 0); 2],
                "keep-pace must reclaim at least what the commit freed: {work:?}"
            );
            assert!(
                work.reclaimed_pages <= own.max(64),
                "never more than max(64, freed): {work:?}"
            );
            assert!(work.reclaimed_pages > 64, "{work:?}");
        } else {
            assert!(work.reclaimed_pages <= 64, "baseline budget: {work:?}");
        }
        measured.push(work.reclaimed_pages);
        // Content intact.
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), ROWS);
    }
    assert!(
        measured[1] > measured[0],
        "keep-pace reclaims more: {measured:?}"
    );
}

#[test]
fn keep_pace_never_frees_pages_a_held_reader_still_needs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with(&dir.path().join("keep-pace-held.redb"), true);
    seed(&db);
    let reader = db.begin_read().unwrap();
    for generation in 1..=12 {
        big_commit(&db, generation, ROWS);
    }
    let work = big_commit(&db, 13, ROWS);
    assert_eq!(
        work.reclaimed_pages, 0,
        "everything is held by the reader: {work:?}"
    );
    for generation in 14..=20 {
        big_commit(&db, generation, ROWS);
    }
    let table = reader.open_table(TABLE).unwrap();
    for (key, value) in table.iter().unwrap().map(|row| {
        let row = row.unwrap();
        (row.0.value(), row.1.value().to_vec())
    }) {
        assert_eq!(
            value,
            payload(0, key),
            "the held snapshot must stay intact at key {key}"
        );
    }
}

#[test]
fn small_commits_stay_within_the_ordinary_budget_under_keep_pace() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_with(&dir.path().join("keep-pace-small.redb"), true);
    seed(&db);
    for generation in 1..=30 {
        big_commit(&db, generation, ROWS);
    }
    redb::commit_work::reset_commit_pages_for_test();
    let tx = db.begin_write().unwrap();
    {
        let mut table = tx.open_table(TABLE).unwrap();
        table.insert(7, payload(99, 7).as_slice()).unwrap();
    }
    tx.commit().unwrap();
    let work = redb::commit_work::take_commit_pages_for_test();
    assert!(
        work.freed_pages <= 64,
        "a one-row commit frees few extents: {work:?}"
    );
    assert!(work.reclaimed_pages <= 64, "{work:?}");
    assert!(work.reclaimed_entries <= 64, "{work:?}");
}

/// A crash inside a reclaim step: after its reclaim transaction has staged its mutations but
/// before durable publication, the child signals the parent and blocks. The parent kills that
/// exact transaction. The reopened store verifies, still holds the committed data, keeps the
/// remainder, and finishes draining through the same step.
#[cfg(unix)]
#[test]
fn a_kill_at_any_moment_of_a_step_loop_recovers_through_ordinary_storage_recovery() {
    const ROLE: &str = "WRITECOST_REDB_KILL_CHILD";
    const PAUSE_SOCKET: &str = "WRITECOST_REDB_PAUSE_BEFORE_DURABLE_COMMIT_SOCKET";
    const MARKER: &[u8] = b"RECLAIM_MUTATIONS_STAGED";
    if let (Ok(path), Ok(_)) = (std::env::var(ROLE), std::env::var(PAUSE_SOCKET)) {
        let db = open(Path::new(&path));
        let step = db.reclaim_deferred_step(None, 4, 4);
        panic!("the parent must kill the child at the staged reclaim commit: {step:?}");
    }
    use std::os::unix::net::UnixDatagram;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kill.redb");
    {
        let db = open(&path);
        seed(&db);
        for generation in 1..=40 {
            churn(&db, generation);
        }
    }
    let socket = dir.path().join("reclaim-commit.sock");
    let ready = UnixDatagram::bind(&socket).unwrap();
    ready
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    let mut child = ReclaimChild::new(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_kill_at_any_moment_of_a_step_loop_recovers_through_ordinary_storage_recovery",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROLE, &path)
            .env(PAUSE_SOCKET, &socket)
            .spawn()
            .unwrap(),
    );
    let mut marker = [0; MARKER.len()];
    let received = ready.recv(&mut marker);
    assert_eq!(
        received.unwrap_or_else(|_| panic!(
            "child must signal before the rendezvous deadline: {:?}",
            child.status().unwrap()
        )),
        MARKER.len()
    );
    assert_eq!(
        marker, MARKER,
        "child must stop in the staged reclaim transaction"
    );
    child.kill_and_reap().unwrap();
    drop(ready);
    std::fs::remove_file(&socket).unwrap();
    let mut db = open(&path);
    assert!(
        db.check_integrity().unwrap(),
        "store must verify after the staged reclaim kill"
    );
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), ROWS);
    for key in 0..ROWS {
        let value = table.get(key).unwrap().unwrap().value().to_vec();
        let known = (0..=40).any(|generation| {
            let expected = if key % 3 == 0 { generation } else { 0 };
            value == payload(expected, key)
        });
        assert!(
            known,
            "key {key} holds a value no committed generation wrote"
        );
    }
    drop(table);
    drop(read);
    let (verdict, _, _) = drive(&db, None, 2_000);
    assert_eq!(verdict, ReclaimStep::Drained);
}
