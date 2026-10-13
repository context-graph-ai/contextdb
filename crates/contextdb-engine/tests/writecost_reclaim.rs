//! Proof rows for the deferred-free reclaim fix, driven through the public engine surface.
//!
//! Setup: a file-backed store with a `bulk` table big enough that a cursor over it keeps a live
//! snapshot (`first_page().has_more`) and a one-row `churn` table whose 8 KiB updates park one
//! deferred-free entry per commit while that snapshot is held. Every number asserted is read back
//! from the store's own deferred-free trees (`__deferred_free_backlog_for_test`) or from the
//! storage layer's per-commit work counters, never from the fix's own bookkeeping.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, MaintenancePolicy, ReadCursor, ReadSession};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

const BULK_ROWS: i64 = 400;

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
    pub db: Database,
}

fn seed_bulk(db: &Database, first: i64, rows: i64) {
    let mut id = first;
    let end = first + rows;
    while id < end {
        let mut sql = String::from("INSERT INTO bulk (id, payload) VALUES ");
        for i in 0..200.min(end - id) {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({}, '{}')", id + i, "x".repeat(200)));
        }
        db.execute(&sql, &p()).unwrap();
        id += 200;
    }
}

pub fn fixture(policy: MaintenancePolicy) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writecost.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(policy);
    db.execute(
        "CREATE TABLE churn (id INTEGER PRIMARY KEY, payload TEXT)",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT)",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO churn (id, payload) VALUES (0, 'seed')", &p())
        .unwrap();
    seed_bulk(&db, 0, BULK_ROWS);
    Fixture { dir, path, db }
}

pub fn churn(db: &Database, commits: usize) {
    for generation in 0..commits {
        db.execute(
            &format!(
                "UPDATE churn SET payload = 'g{generation}{}' WHERE id = 0",
                "y".repeat(8192)
            ),
            &p(),
        )
        .unwrap();
    }
}

pub fn hold(db: &Database) -> (ReadSession, ReadCursor) {
    let session = db.read_session(ReadLimits::default()).unwrap();
    let cursor = session
        .open_cursor("SELECT id FROM bulk ORDER BY id", &p())
        .unwrap();
    assert!(
        cursor.first_page().has_more,
        "the cursor must keep a live snapshot"
    );
    (session, cursor)
}

pub fn backlog(db: &Database) -> (usize, usize) {
    let counts = db.__deferred_free_backlog_for_test().unwrap();
    (counts[0].0 + counts[1].0, counts[0].1 + counts[1].1)
}

fn rows_in(mut cursor: ReadCursor) -> usize {
    let mut rows = cursor.first_page().rows.len();
    let mut more = cursor.first_page().has_more;
    while more {
        let page = cursor.fetch(None).unwrap();
        rows += page.rows.len();
        more = page.has_more;
    }
    rows
}

fn count_rows(db: &Database, table: &str) -> i64 {
    let result = db
        .execute(&format!("SELECT COUNT(*) FROM {table}"), &p())
        .unwrap();
    match &result.rows[0][0] {
        Value::Int64(n) => *n,
        other => panic!("unexpected count value {other:?}"),
    }
}

/// Run cycles until the backlog is at most a tail of `max_pages`; returns the cycles used.
fn cycles_until_drained(db: &Database, max_pages: usize, limit: usize) -> usize {
    for used in 0..=limit {
        if backlog(db).1 <= max_pages {
            return used;
        }
        db.run_maintenance_cycle().unwrap();
    }
    panic!(
        "{limit} cycles did not drain the backlog: {:?}",
        backlog(db)
    );
}

/// Row 1. A final small write whose snapshot-held history is the only backlog drains through
/// maintenance alone: no further writes, no schema change.
#[test]
fn an_idle_tail_drains_through_maintenance_cycles_alone() {
    let f = fixture(MaintenancePolicy::CallerDriven);
    let (session, cursor) = hold(&f.db);
    churn(&f.db, 96);
    let held = backlog(&f.db);
    assert!(
        held.1 > 3_000,
        "the held snapshot must park a large backlog: {held:?}"
    );
    drop(cursor);
    drop(session);
    churn(&f.db, 1); // the final write; nothing after it
    let before = backlog(&f.db);
    assert!(
        before.1 > 3_000,
        "release does not by itself reclaim: {before:?}"
    );
    // At most four cycles; the fix needs one.
    let used = cycles_until_drained(&f.db, 128, 4);
    assert!(used >= 1, "{before:?}");
    assert!(
        backlog(&f.db).0 <= 3,
        "only the cycle's own bookkeeping tail may remain: {:?}",
        backlog(&f.db)
    );
    assert_eq!(count_rows(&f.db, "bulk"), BULK_ROWS);
    assert_eq!(count_rows(&f.db, "churn"), 1);
}

/// Row 4. A held reader keeps its backlog and its whole snapshot through repeated cycles; after
/// release the retried cycles drain it. No deadline is claimed: the test drives as many bounded
/// cycles as needed.
#[test]
fn a_held_reader_keeps_its_snapshot_and_release_lets_retried_cycles_drain() {
    let f = fixture(MaintenancePolicy::CallerDriven);
    let (session, cursor) = hold(&f.db);
    churn(&f.db, 96);
    let before = backlog(&f.db);
    for _ in 0..3 {
        f.db.run_maintenance_cycle().unwrap();
    }
    let held = backlog(&f.db);
    assert!(
        held.1 >= before.1 && held.0 >= before.0,
        "nothing the reader's snapshot protects may be freed: {before:?} -> {held:?}"
    );
    // Reuse freed pages hard, then read the whole held snapshot.
    churn(&f.db, 24);
    assert_eq!(
        rows_in(cursor),
        BULK_ROWS as usize,
        "the held snapshot must stay intact"
    );
    drop(session);
    // No deadline is claimed: every cycle must make progress, and the backlog must eventually
    // drain. (While a storage-compaction sweep owns reclamation the stage stands down and the
    // backlog drains at the ordinary per-commit rate, so this can take many cycles.)
    let mut previous = backlog(&f.db).1;
    let mut cycles = 0;
    while backlog(&f.db).1 > 128 {
        cycles += 1;
        assert!(
            cycles <= 80,
            "the backlog must eventually drain: {:?} after {cycles} cycles",
            backlog(&f.db)
        );
        f.db.run_maintenance_cycle().unwrap();
        let now = backlog(&f.db).1;
        assert!(
            now < previous,
            "cycle {cycles} must make progress: {previous} -> {now}"
        );
        previous = now;
    }
    assert!(cycles >= 1);
    assert_eq!(count_rows(&f.db, "bulk"), BULK_ROWS);
}

/// Row 9. Releasing older snapshots one at a time frees only what the remaining ones no longer
/// need: the newer snapshot stays whole and its backlog stays parked.
#[test]
fn releasing_older_snapshots_never_frees_what_a_newer_one_reads() {
    let f = fixture(MaintenancePolicy::CallerDriven);
    let (older_session, older) = hold(&f.db);
    churn(&f.db, 40);
    let (newer_session, newer) = hold(&f.db);
    churn(&f.db, 40);
    let both = backlog(&f.db);
    drop(older);
    drop(older_session);
    f.db.run_maintenance_cycle().unwrap();
    let after_older = backlog(&f.db);
    assert!(
        after_older.1 < both.1,
        "releasing the older snapshot must free its share: {both:?} -> {after_older:?}"
    );
    assert!(
        after_older.1 > 0,
        "the newer snapshot's share stays parked: {after_older:?}"
    );
    for _ in 0..3 {
        f.db.run_maintenance_cycle().unwrap();
    }
    churn(&f.db, 16);
    assert_eq!(rows_in(newer), BULK_ROWS as usize);
    drop(newer_session);
    cycles_until_drained(&f.db, 128, 12);
}

/// Rows 6, 7, 8. A declaration-free EngineOwned store spawns no worker, and its commits keep pace:
/// a large commit reclaims at least what it frees (and at most max(64, freed)) without ever
/// passing the reader floor; small commits stay within the ordinary 64; close drains the rest.
#[test]
fn a_declaration_free_store_keeps_pace_on_its_own_commits_and_spawns_no_thread() {
    let f = fixture(MaintenancePolicy::EngineOwned);
    assert!(!f.db.maintenance_status().running);
    let (session, cursor) = hold(&f.db);
    churn(&f.db, 96);
    drop(cursor);
    drop(session);
    let parked = backlog(&f.db);
    assert!(parked.1 > 3_000, "{parked:?}");

    // A small commit stays within the ordinary budget.
    redb::commit_work::reset_commit_pages_for_test();
    churn(&f.db, 1);
    let small = redb::commit_work::take_commit_pages_for_test();
    assert!(
        small.reclaimed_pages <= 64.max(small.freed_pages),
        "{small:?}"
    );

    // One large commit: delete every bulk row (hundreds of extents) with older work pending.
    redb::commit_work::reset_commit_pages_for_test();
    f.db.execute("DELETE FROM bulk WHERE id >= 0", &p())
        .unwrap();
    let large = redb::commit_work::take_commit_pages_for_test();
    assert!(
        large.freed_pages > 64,
        "the delete must free more than the ordinary budget: {large:?}"
    );
    assert!(
        large.reclaimed_pages >= large.freed_pages,
        "a large commit must reclaim at least what it frees: {large:?}"
    );
    assert!(
        large.reclaimed_pages <= large.freed_pages.max(64),
        "never more than max(64, freed): {large:?}"
    );

    // The backlog does not grow across a sequence of large commits.
    let mut previous = backlog(&f.db).1;
    for round in 0..3 {
        seed_bulk(&f.db, 1_000 * (round + 1), 200);
        f.db.execute(
            &format!("DELETE FROM bulk WHERE id >= {}", 1_000 * (round + 1)),
            &p(),
        )
        .unwrap();
        let now = backlog(&f.db).1;
        assert!(
            now <= previous + 64,
            "round {round}: backlog grew {previous} -> {now}"
        );
        previous = now;
    }
    assert!(
        !f.db.maintenance_status().running,
        "no thread is ever spawned for an undeclared store"
    );
    assert!(!f.db.__maintenance_thread_running());

    // Close drains what is left; a reopen sees an empty (or one-entry) tail.
    let path: &Path = &f.path;
    f.db.close().unwrap();
    let reopened = Database::open(path).unwrap();
    assert!(backlog(&reopened).1 <= 128, "{:?}", backlog(&reopened));
}
