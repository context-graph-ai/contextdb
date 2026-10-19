//! A writer that declares no memory limit is bounded by the inherited process
//! ceiling: its admissions are refused, typed, when they would take the
//! process past it, and its page cache is sized within it. Each case runs in
//! a child process, because the test ceiling is process-wide.
use crate::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_core::{Error, Value, Wallclock};
use std::collections::HashMap;

const CHILD: &str = "S7_UNDECLARED_WRITER_CHILD";

fn undeclared() -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    options
}

fn vector_for(id: i64) -> Vec<f32> {
    (0..384)
        .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin())
        .collect()
}

fn insert(db: &Database, first: i64, rows: i64) -> contextdb_core::Result<()> {
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for id in first..first + rows {
        if id > first {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{id}, $v{id})"));
        params.insert(format!("i{id}"), Value::Int64(id));
        params.insert(format!("v{id}"), Value::Vector(vector_for(id)));
    }
    db.execute(&sql, &params).map(|_| ())
}

/// Run `case` in a child of this test binary with the process ceiling set.
fn in_child(test: &str, case: &str, ceiling: usize, body: impl FnOnce(&std::path::Path)) {
    if let Ok(path) = std::env::var(CHILD) {
        crate::memory_accounting::set_process_ceiling_for_test(Some(ceiling));
        body(std::path::Path::new(&path));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(format!("{case}.db"));
    let name = format!(
        "{}::{test}",
        module_path!().trim_start_matches("contextdb_engine::")
    );
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, &path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{case} child failed:\n{}\n{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
}

#[test]
fn an_undeclared_writer_is_refused_typed_at_the_process_ceiling() {
    const CEILING: usize = 96 * 1024 * 1024;
    in_child(
        "an_undeclared_writer_is_refused_typed_at_the_process_ceiling",
        "admission",
        CEILING,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let db = Database::open_with_options(path, undeclared()).unwrap();
            assert_eq!(db.memory_limit(), None, "no writer limit is declared");
            db.execute(
                "CREATE TABLE points (id INT PRIMARY KEY, v VECTOR(384))",
                &HashMap::new(),
            )
            .unwrap();
            // One commit of 65,536 x 1,536 B = 96 MiB of vector bodies alone.
            let refused = insert(&db, 0, 65_536);
            let high_water = db.write_memory_high_water_for_test();
            eprintln!("undeclared writer: commit {refused:?}, high water {high_water}");
            assert!(
                matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
                "a commit past the process ceiling is refused, typed: {refused:?}"
            );
            assert!(
                high_water <= CEILING,
                "the writer never holds more than the process ceiling: {high_water}"
            );
            insert(&db, 0, 16).expect("a commit within the ceiling still succeeds");
            db.close().unwrap();
        },
    );
}

#[test]
fn an_undeclared_writer_page_cache_fits_the_process_ceiling() {
    const CEILING: usize = 32 * 1024 * 1024;
    in_child(
        "an_undeclared_writer_page_cache_fits_the_process_ceiling",
        "cache",
        CEILING,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let db = Database::open_with_options(path, undeclared())
                .expect("an undeclared writer opens within a 32 MiB process ceiling");
            db.execute(
                "CREATE TABLE points (id INT PRIMARY KEY, v VECTOR(384))",
                &HashMap::new(),
            )
            .unwrap();
            // 48 MiB of vector bodies in commits of about 1 MiB, then every
            // row read back: the page cache sees more than the ceiling.
            for commit in 0..48 {
                insert(&db, commit * 680, 680).unwrap();
            }
            let rows = db
                .execute("SELECT id, v FROM points", &HashMap::new())
                .unwrap()
                .rows
                .len();
            assert_eq!(rows, 48 * 680);
            let high_water = db.write_memory_high_water_for_test();
            let (configured, writer, process, ..) = db.writer_cache_for_test();
            eprintln!(
                "undeclared writer under a 32 MiB ceiling: cache {configured} B, writer {writer} B, process residue {process} B, high water {high_water}"
            );
            assert!(
                configured <= CEILING / 16,
                "the page cache is sized from the process ceiling: {configured}"
            );
            // The settled capacity adds the cache's fixed stripe controls.
            assert!(
                writer == 0 && process >= configured && process - configured <= 64 * 1024,
                "the whole cache is held by the process ceiling, none as writer working memory: writer {writer}, process {process}, configured {configured}"
            );
            assert!(
                high_water <= CEILING,
                "the page cache and the writer fit the process ceiling: {high_water}"
            );
            db.close().unwrap();
        },
    );
}

fn table(db: &Database) {
    db.execute(
        "CREATE TABLE points (id INT PRIMARY KEY, v VECTOR(384))",
        &HashMap::new(),
    )
    .unwrap();
}

#[test]
fn a_tagged_original_reservation_is_refused_at_the_process_ceiling_before_any_runtime() {
    const CEILING: usize = 128 * 1024 * 1024;
    in_child(
        "a_tagged_original_reservation_is_refused_at_the_process_ceiling_before_any_runtime",
        "tagged",
        CEILING,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            for native in [false, true] {
                let room = CEILING - crate::memory_accounting::process_aggregate_snapshot().0;
                let (outcome, used_before, used_after, aggregate_before, aggregate_after) =
                    db.writer_original_reservation_for_test(room + (1 << 20), native);
                eprintln!("tagged reservation (native {native}): {outcome:?}");
                let refusal =
                    outcome.expect_err("a reservation past the process ceiling is refused");
                assert!(
                    refusal.contains("MemoryBudgetExceeded") || refusal.contains("Memory"),
                    "the refusal is typed: {refusal}"
                );
                assert_eq!(used_after, used_before, "the writer's counter is unchanged");
                assert_eq!(
                    aggregate_after, aggregate_before,
                    "the process counter is unchanged"
                );
            }
            db.close().unwrap();
        },
    );
}

#[test]
fn an_undeclared_writer_open_is_refused_when_the_process_cannot_hold_its_cache() {
    const CEILING: usize = 256 * 1024 * 1024;
    in_child(
        "an_undeclared_writer_open_is_refused_when_the_process_cannot_hold_its_cache",
        "refused-cache",
        CEILING,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let first = Database::open_with_options(path, undeclared()).unwrap();
            table(&first);
            // Leave 8 MiB of process room: enough for an open's fixed costs,
            // not for a 16 MiB (ceiling / 16) page cache.
            let room = CEILING - crate::memory_accounting::process_aggregate_snapshot().0;
            let (outcome, ..) = first.writer_original_reservation_for_test(room - (8 << 20), false);
            outcome.expect("the competing charge fits");
            let competing = room - (8 << 20);
            let second_path = path.with_extension("second.db");
            let before = crate::memory_accounting::process_aggregate_snapshot().0;
            let refused = Database::open_with_options(&second_path, undeclared())
                .expect_err("the open is refused");
            eprintln!("undeclared open with no room for its cache: {refused:?}");
            assert!(
                matches!(&refused, Error::MemoryBudgetExceeded { operation, .. } if operation == "undeclared_writer_cache"),
                "the cache's own reservation refuses, typed: {refused:?}"
            );
            assert_eq!(
                crate::memory_accounting::process_aggregate_snapshot().0,
                before,
                "the refused open returns every charge it took"
            );
            first.writer_release_for_test(competing);
            first.close().unwrap();
            drop(first);
            let second = Database::open_with_options(&second_path, undeclared())
                .expect("the open succeeds once the competing charge is gone");
            let (configured, writer, process, ..) = second.writer_cache_for_test();
            assert!(
                writer == 0 && process >= configured,
                "{writer} {process} {configured}"
            );
            second.close().unwrap();
        },
    );
}

#[test]
fn a_lowered_process_ceiling_refuses_the_first_admission_after_a_transaction_begins() {
    in_child(
        "a_lowered_process_ceiling_refuses_the_first_admission_after_a_transaction_begins",
        "lowered",
        usize::MAX,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            // The production read, fed by the OS source: the test ceiling
            // override is cleared.
            crate::memory_accounting::set_process_ceiling_for_test(None);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(1 << 40));
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            // Warm: a reservation under the high ceiling is admitted.
            let (warm, ..) = db.writer_original_reservation_for_test(32 << 20, false);
            warm.expect("admitted under the high ceiling");
            db.writer_release_for_test(32 << 20);
            // Lower the source below what the process holds plus 32 MiB. The
            // ceiling is observed at a boundary; after the next transaction
            // begins, the first admission through either door is refused,
            // typed, and leaves both counters where they were.
            let lowered = crate::memory_accounting::process_aggregate_snapshot().0 + (16 << 20);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(lowered));
            let boundary = db.begin().unwrap();
            db.rollback(boundary).unwrap();
            for recorded in [None, Some(1_usize << 40)] {
                // A ceiling recorded at a runtime activation never overrides
                // a smaller current one.
                crate::memory_accounting::record_domain_ceiling_for_test(recorded);
                for native in [false, true] {
                    let (refused, used_before, used_after, aggregate_before, aggregate_after) =
                        db.writer_original_reservation_for_test(32 << 20, native);
                    eprintln!("lowered (recorded {recorded:?}, native {native}): {refused:?}");
                    let refusal = refused.expect_err("the lowered ceiling refuses at once");
                    assert!(refusal.contains("Memory"), "typed: {refusal}");
                    assert_eq!(used_after, used_before, "the writer's counter is unchanged");
                    assert_eq!(
                        aggregate_after, aggregate_before,
                        "the process counter is unchanged"
                    );
                }
            }
            crate::memory_accounting::record_domain_ceiling_for_test(None);
            crate::memory_accounting::set_os_ceiling_source_for_test(None);
            db.close().unwrap();
        },
    );
}

#[test]
fn a_handle_recycle_reinstalls_the_undeclared_writer_cache_capacity() {
    const CEILING: usize = 32 * 1024 * 1024;
    in_child(
        "a_handle_recycle_reinstalls_the_undeclared_writer_cache_capacity",
        "recycle",
        CEILING,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            for batch in 0..8 {
                insert(&db, batch * 64, 64).unwrap();
            }
            db.execute("DELETE FROM points WHERE id < 384", &HashMap::new())
                .unwrap();
            let before = db.writer_cache_for_test();
            let (configured, writer, funded, capacity, reserve) = before;
            eprintln!(
                "before recycle: configured {configured}, writer {writer}, funded {funded}, capacity {capacity}, reserve {reserve}"
            );
            assert_eq!(
                capacity, configured,
                "the open installed the policy capacity"
            );
            assert!(
                reserve > capacity,
                "the reserve includes the cache index on top of the capacity"
            );
            assert!(
                reserve <= writer + funded,
                "the open funded its whole reserve"
            );
            let compacted = db.compact_now().unwrap();
            assert!(
                compacted.ran && compacted.handle_recycled,
                "the handle recycled"
            );
            let after = db.writer_cache_for_test();
            eprintln!("after recycle: {after:?}");
            assert_eq!(
                after.3, capacity,
                "the recycle reinstalls the installed capacity, not the reserve"
            );
            assert!(
                after.4 <= after.1 + after.2,
                "the reopened cache's whole reserve is funded"
            );
            assert_eq!(
                (after.1, after.2),
                (writer, funded),
                "the funding is unchanged"
            );
            // Reads and writes on the recycled handle.
            insert(&db, 10_000, 32).unwrap();
            let rows = db
                .execute("SELECT id FROM points", &HashMap::new())
                .unwrap()
                .rows
                .len();
            assert_eq!(rows, 512 - 384 + 32);
            db.close().unwrap();
        },
    );
}

#[test]
fn a_one_row_commit_reads_the_process_ceiling_once() {
    in_child(
        "a_one_row_commit_reads_the_process_ceiling_once",
        "one-read",
        usize::MAX,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            crate::memory_accounting::set_process_ceiling_for_test(None);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(1 << 40));
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            insert(&db, 0, 1).unwrap();
            let before = crate::memory_accounting::ceiling_observations_for_test();
            insert(&db, 1, 1).unwrap();
            let reads = crate::memory_accounting::ceiling_observations_for_test() - before;
            // Reading the limit files costs system calls; a commit's many
            // admissions must check the value observed when it began.
            assert_eq!(
                reads, 1,
                "one observation per transaction, none per admission"
            );
            crate::memory_accounting::set_os_ceiling_source_for_test(None);
            db.close().unwrap();
        },
    );
}

#[test]
fn a_failed_ceiling_refresh_keeps_the_last_valid_ceiling() {
    in_child(
        "a_failed_ceiling_refresh_keeps_the_last_valid_ceiling",
        "failed-refresh",
        usize::MAX,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            crate::memory_accounting::set_process_ceiling_for_test(None);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(1 << 40));
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            // A lowered limit is observed at a boundary...
            let lowered = crate::memory_accounting::process_aggregate_snapshot().0 + (16 << 20);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(lowered));
            let boundary = db.begin().unwrap();
            db.rollback(boundary).unwrap();
            // ...then discovery fails at the next one. The failure is not
            // "no limit": the lowered limit stays in force.
            crate::memory_accounting::fail_os_ceiling_source_for_test();
            let boundary = db.begin().unwrap();
            db.rollback(boundary).unwrap();
            for native in [false, true] {
                let (refused, used_before, used_after, ..) =
                    db.writer_original_reservation_for_test(32 << 20, native);
                eprintln!("after a failed refresh (native {native}): {refused:?}");
                refused.expect_err("the last valid ceiling still refuses past its room");
                assert_eq!(used_after, used_before);
            }
            let (admitted, ..) = db.writer_original_reservation_for_test(1 << 20, false);
            admitted.expect("room under the last valid ceiling is still admitted");
            db.writer_release_for_test(1 << 20);
            // The failed read is visible where the limits are inspected.
            let shown = db.execute("SHOW MEMORY_LIMIT", &HashMap::new()).unwrap();
            let column = shown
                .columns
                .iter()
                .position(|name| name == "ceiling_refresh_failures")
                .expect("SHOW MEMORY_LIMIT reports failed ceiling reads");
            assert_eq!(shown.rows[0][column], Value::Int64(1));
            crate::memory_accounting::set_os_ceiling_source_for_test(None);
            db.close().unwrap();
        },
    );
}

#[test]
fn a_process_with_no_safe_ceiling_refuses_typed_until_one_is_observed() {
    in_child(
        "a_process_with_no_safe_ceiling_refuses_typed_until_one_is_observed",
        "no-safe-ceiling",
        usize::MAX,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            crate::memory_accounting::set_process_ceiling_for_test(None);
            // Nothing has been observed in this process, and discovery fails.
            crate::memory_accounting::fail_os_ceiling_source_for_test();
            let refused = Database::open_with_options(path, undeclared())
                .expect_err("an undeclared writer cannot open against an unknown ceiling");
            eprintln!("no safe ceiling: {refused:?}");
            assert!(
                matches!(
                    refused,
                    Error::MemoryBudgetExceeded {
                        budget_limit_bytes: 0,
                        ..
                    }
                ),
                "the refusal is typed and names no usable ceiling: {refused:?}"
            );
            // Once discovery succeeds, the next boundary admits again.
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(1 << 40));
            let db = Database::open_with_options(path, undeclared())
                .expect("the open succeeds once a ceiling is observed");
            table(&db);
            crate::memory_accounting::set_os_ceiling_source_for_test(None);
            db.close().unwrap();
        },
    );
}

#[test]
fn a_maintenance_cycle_and_a_pruning_pass_each_observe_a_lowered_ceiling() {
    in_child(
        "a_maintenance_cycle_and_a_pruning_pass_each_observe_a_lowered_ceiling",
        "maintenance-boundary",
        usize::MAX,
        |path| {
            let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
            crate::memory_accounting::set_process_ceiling_for_test(None);
            crate::memory_accounting::set_os_ceiling_source_for_test(Some(1 << 40));
            let db = Database::open_with_options(path, undeclared()).unwrap();
            table(&db);
            db.execute("ALTER TABLE points SET RETAIN 7 DAYS", &HashMap::new())
                .unwrap();
            // No transaction begins between the lowering and each pass: the
            // pass itself must observe the new limit before its FIRST
            // admission, not merely at some transaction it begins later.
            for (lowered, pass) in [((1_usize << 40) - 1, "pruning"), ((1 << 40) - 2, "cycle")] {
                crate::memory_accounting::set_os_ceiling_source_for_test(Some(lowered));
                let before = crate::memory_accounting::ceiling_observations_for_test();
                crate::memory_accounting::arm_first_ceiling_check_for_test();
                match pass {
                    "pruning" => {
                        db.run_pruning_cycle_checked().unwrap();
                    }
                    _ => {
                        db.run_maintenance_cycle().unwrap();
                    }
                }
                let reads = crate::memory_accounting::ceiling_observations_for_test() - before;
                assert!(reads >= 1, "the {pass} pass observes the ceiling");
                assert_eq!(
                    crate::memory_accounting::first_ceiling_check_for_test(),
                    Some(lowered),
                    "the {pass} pass's first admission checks the lowered ceiling"
                );
                assert_eq!(
                    crate::memory_accounting::observed_process_ceiling_for_test(),
                    lowered,
                    "the {pass} pass checks the lowered ceiling"
                );
            }
            crate::memory_accounting::set_os_ceiling_source_for_test(None);
            db.close().unwrap();
        },
    );
}
