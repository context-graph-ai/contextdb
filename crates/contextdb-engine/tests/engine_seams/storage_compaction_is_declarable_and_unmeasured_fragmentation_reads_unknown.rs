//! Storage compaction is a declared store setting, and a compaction receipt
//! never invents a measurement.
//!
//! When the allocator's free-page totals cannot be trusted, or an exact
//! measurement failed, the receipt says the fragmentation is unknown rather
//! than reporting 0 -- including every later batch of a sweep that started
//! unmeasured, across reopen. The start ratio, the minimum interval between
//! automatic attempts, and the explicit compaction stop rules are declared
//! with `SET STORAGE_COMPACTION`: today's values are the silent defaults, a
//! declared value changes when compaction starts and stops, and
//! `SHOW STORAGE_COMPACTION` names each value's source across reopen. The SQL
//! statement and the Rust setter refuse the same out-of-range declarations
//! with the same typed issue; concurrent declarations are serialized, so live
//! state, `SHOW` and reopen agree; and every maintenance receipt names the
//! policy that governed it.
use contextdb_core::{Error, StorageCompactionDeclarationIssue as Issue, Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_parser::{StorageCompactionClauses, StorageCompactionSetting};
use redb::TableDefinition;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

const NOW: u64 = 1_800_000_000_000;
const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;
const PAYLOAD: TableDefinition<u64, &[u8]> = TableDefinition::new("payload");
/// Each row sits alone in a half-empty 8 KiB leaf.
const VALUE_BYTES: usize = 4_100;
const ROWS: u64 = 2_048;

fn open(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// A compacted store of half-empty pages; the rows `keep` rejects are then
/// removed, so exactly their pages are free. The last row is always kept.
fn payload_store(path: &std::path::Path, keep: impl Fn(u64) -> bool) {
    let initial = open(path);
    initial.close().unwrap();
    drop(initial);
    let db = redb::Database::create(path).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(PAYLOAD).unwrap();
        let bytes = vec![0x5c_u8; VALUE_BYTES];
        for key in 0..ROWS {
            table.insert(key, bytes.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    drop(db);
    let compacted = open(path);
    compacted.compact_now().unwrap();
    compacted.close().unwrap();
    drop(compacted);
    let db = redb::Database::create(path).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(PAYLOAD).unwrap();
        for key in (0..ROWS).filter(|key| !keep(*key)) {
            table.remove(key).unwrap();
        }
    }
    write.commit().unwrap();
    db.begin_write().unwrap().commit().unwrap();
    drop(db);
}

fn reclaimable(db: &Database) -> f64 {
    let (maintained, recounted) = db.__allocator_free_page_ratios_for_test().unwrap();
    assert_eq!(maintained, Some(recounted));
    recounted
}

/// `(setting, value, source)` rows of `SHOW STORAGE_COMPACTION`.
fn shown(db: &Database) -> Vec<(String, String, String)> {
    let result = db.execute("SHOW STORAGE_COMPACTION", &p()).unwrap();
    assert_eq!(result.columns, vec!["setting", "value", "source"]);
    result
        .rows
        .iter()
        .map(|row| {
            let text = |value: &Value| match value {
                Value::Text(text) => text.clone(),
                Value::Int64(number) => number.to_string(),
                Value::Float64(number) => number.to_string(),
                other => panic!("unexpected SHOW STORAGE_COMPACTION value {other:?}"),
            };
            (text(&row[0]), text(&row[1]), text(&row[2]))
        })
        .collect()
}

fn setting(name: &str, value: &str, source: &str) -> (String, String, String) {
    (name.to_owned(), value.to_owned(), source.to_owned())
}

fn defaults() -> Vec<(String, String, String)> {
    vec![
        setting("start_ratio", "0.5", "default"),
        setting("min_interval_ms", "3600000", "default"),
        setting("stop_ratio", "0.05", "default"),
        setting("max_sweeps", "8", "default"),
    ]
}

fn refused_with(result: contextdb_core::Result<impl std::fmt::Debug>, issue: Issue, what: &str) {
    match result {
        Err(Error::InvalidStorageCompactionDeclaration { issue: actual }) => {
            assert_eq!(actual, issue, "{what} is refused for the right reason")
        }
        other => panic!("{what} must be refused with {issue:?}, got {other:?}"),
    }
}

#[test]
fn untrusted_free_page_totals_report_unknown_fragmentation_and_take_no_decision() {
    let clock = Arc::new(AtomicU64::new(NOW));
    let reader = clock.clone();
    let _clock = Wallclock::test_clock_guard(move || reader.load(Ordering::SeqCst));
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("unknown.redb"));
    db.execute(
        "CREATE TABLE events (id INT PRIMARY KEY, body TEXT) RETAIN 1 DAYS",
        &p(),
    )
    .unwrap();
    for id in 0..64 {
        db.execute(
            &format!("INSERT INTO events VALUES ({id}, 'an event body that will expire')"),
            &p(),
        )
        .unwrap();
    }
    clock.store(NOW + 2 * DAY_MILLIS, Ordering::SeqCst);
    // As after an interrupted allocator-state save: the totals cannot be
    // trusted until a recount, so nothing was measured.
    db.__distrust_allocator_free_totals_for_test();

    let pruning = db.run_pruning_cycle_checked().unwrap();
    assert!(pruning.pruned_rows > 0, "the expired rows are still pruned");
    assert!(
        format!("{pruning:?}").contains("fragmentation_before: None"),
        "an unmeasured pre-prune ratio reads unknown, never 0: {pruning:?}"
    );
    assert!(
        !pruning.compacted,
        "no compaction decision without a measurement"
    );

    let cycle = db.run_maintenance_cycle().unwrap();
    assert!(
        format!("{:?}", cycle.compaction).contains("fragmentation_before: None"),
        "an automatic check that measured nothing reads unknown, never 0: {:?}",
        cycle.compaction
    );
    assert!(!cycle.compaction.ran);
    db.close().unwrap();
}

#[test]
fn a_declared_start_ratio_changes_when_automatic_compaction_starts() {
    let directory = tempfile::tempdir().unwrap();
    // About one page in ten is free: below the default start ratio of 0.5.
    let default_path = directory.path().join("default-start.redb");
    payload_store(&default_path, |key| key % 10 != 3);
    let db = open(&default_path);
    let free = reclaimable(&db);
    assert!(
        free > 0.05 && free < 0.5,
        "fixture reclaimable ratio {free}"
    );
    assert!(!db.run_maintenance_cycle().unwrap().compaction.ran);
    db.close().unwrap();

    let declared_path = directory.path().join("declared-start.redb");
    payload_store(&declared_path, |key| key % 10 != 3);
    let db = open(&declared_path);
    db.execute("SET STORAGE_COMPACTION START_RATIO 0.05", &p())
        .unwrap();
    let cycle = db.run_maintenance_cycle().unwrap();
    assert!(
        cycle.compaction.ran,
        "a declared start ratio below the store's reclaimable share starts compaction: {:?}",
        cycle.compaction
    );
    db.close().unwrap();
}

#[test]
fn a_declared_stop_rule_changes_how_many_sweeps_compact_now_runs() {
    let directory = tempfile::tempdir().unwrap();
    let sweeps_with = |name: &str, declaration: Option<String>| {
        let path = directory.path().join(format!("{name}.redb"));
        payload_store(&path, |key| key % 8 == 7);
        let db = open(&path);
        if let Some(declaration) = declaration {
            db.execute(&declaration, &p()).unwrap();
        }
        let report = db.compact_now().unwrap();
        assert!(report.ran && report.file_shrank, "{name}: {report:?}");
        let sweeps = Database::__explicit_compaction_sweeps_last_for_test();
        let left = reclaimable(&db);
        db.close().unwrap();
        (sweeps, left)
    };
    let (one, left) = sweeps_with(
        "one-sweep",
        Some("SET STORAGE_COMPACTION MAX_SWEEPS 1".to_owned()),
    );
    assert_eq!(one, 1, "MAX_SWEEPS 1 stops after one sweep");
    assert!(
        left > 0.0,
        "fixture: one sweep leaves interior free pages behind ({left})"
    );
    let (default, _) = sweeps_with("default", None);
    assert_eq!(
        default, 1,
        "by default one sweep leaves this store below the 0.05 stop ratio ({left})"
    );
    let (below, _) = sweeps_with(
        "below-first-sweep",
        Some(format!(
            "SET STORAGE_COMPACTION STOP_RATIO {:.15} MAX_SWEEPS 3",
            left / 2.0
        )),
    );
    assert!(
        (2..=3).contains(&below),
        "a stop ratio below what one sweep leaves keeps sweeping, within MAX_SWEEPS 3: {below}"
    );
    let (whole, _) = sweeps_with(
        "stop-at-one",
        Some("SET STORAGE_COMPACTION STOP_RATIO 1 MAX_SWEEPS 3".to_owned()),
    );
    assert_eq!(whole, 1, "STOP_RATIO 1 is met after any sweep");
}

#[test]
fn a_declared_minimum_interval_changes_when_a_negative_check_repeats() {
    let directory = tempfile::tempdir().unwrap();
    let mut checks = Vec::new();
    for (name, declaration) in [
        ("default", None),
        (
            "ten-minutes",
            Some("SET STORAGE_COMPACTION MIN_INTERVAL 10 MINUTES"),
        ),
    ] {
        let path = directory.path().join(format!("{name}.redb"));
        payload_store(&path, |key| key % 10 != 3);
        let db = open(&path);
        if let Some(declaration) = declaration {
            db.execute(declaration, &p()).unwrap();
        }
        let before = Database::__automatic_compaction_checks_for_test();
        assert!(!db.run_maintenance_cycle().unwrap().compaction.ran);
        let first = Database::__automatic_compaction_checks_for_test() - before;
        // Before the declared deadline: the negative check is not repeated.
        Database::__advance_compaction_clock_for_test(Duration::from_secs(5 * 60));
        assert!(!db.run_maintenance_cycle().unwrap().compaction.ran);
        let before_deadline = Database::__automatic_compaction_checks_for_test() - before;
        // Past the declared deadline, still inside the default hour.
        Database::__advance_compaction_clock_for_test(Duration::from_secs(6 * 60));
        assert!(!db.run_maintenance_cycle().unwrap().compaction.ran);
        let after_deadline = Database::__automatic_compaction_checks_for_test() - before;
        checks.push((name, first, before_deadline, after_deadline));
        db.close().unwrap();
    }
    assert_eq!(
        checks,
        vec![("default", 1, 1, 1), ("ten-minutes", 1, 1, 2)],
        "a negative check waits the declared interval before it repeats"
    );
}

#[test]
fn storage_compaction_declarations_show_their_source_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("declared.redb");
    let db = open(&path);
    let defaults = vec![
        (
            "start_ratio".to_owned(),
            "0.5".to_owned(),
            "default".to_owned(),
        ),
        (
            "min_interval_ms".to_owned(),
            "3600000".to_owned(),
            "default".to_owned(),
        ),
        (
            "stop_ratio".to_owned(),
            "0.05".to_owned(),
            "default".to_owned(),
        ),
        (
            "max_sweeps".to_owned(),
            "8".to_owned(),
            "default".to_owned(),
        ),
    ];
    assert_eq!(
        shown(&db),
        defaults,
        "undeclared settings are today's defaults"
    );
    db.execute(
        "SET STORAGE_COMPACTION START_RATIO 0.4 MIN_INTERVAL 30 MINUTES",
        &p(),
    )
    .unwrap();
    db.execute("SET STORAGE_COMPACTION MAX_SWEEPS 4", &p())
        .unwrap();
    let declared = vec![
        (
            "start_ratio".to_owned(),
            "0.4".to_owned(),
            "declared".to_owned(),
        ),
        (
            "min_interval_ms".to_owned(),
            "1800000".to_owned(),
            "declared".to_owned(),
        ),
        (
            "stop_ratio".to_owned(),
            "0.05".to_owned(),
            "default".to_owned(),
        ),
        (
            "max_sweeps".to_owned(),
            "4".to_owned(),
            "declared".to_owned(),
        ),
    ];
    assert_eq!(
        shown(&db),
        declared,
        "each clause is declared independently"
    );
    db.close().unwrap();
    drop(db);

    let db = open(&path);
    assert_eq!(shown(&db), declared, "declarations persist across reopen");
    for refused in [
        "SET STORAGE_COMPACTION START_RATIO 0",
        "SET STORAGE_COMPACTION START_RATIO 1.5",
        "SET STORAGE_COMPACTION STOP_RATIO 0",
        "SET STORAGE_COMPACTION MAX_SWEEPS 0",
        "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS",
        "SET STORAGE_COMPACTION",
    ] {
        assert!(db.execute(refused, &p()).is_err(), "{refused} is refused");
    }
    assert_eq!(
        shown(&db),
        declared,
        "a refused declaration changes nothing"
    );
    db.execute("SET STORAGE_COMPACTION DEFAULT", &p()).unwrap();
    assert_eq!(
        shown(&db),
        defaults,
        "DEFAULT returns to the silent defaults"
    );
    db.close().unwrap();
    drop(db);
    let db = open(&path);
    assert_eq!(shown(&db), defaults);
    db.close().unwrap();
}

#[test]
fn an_unknown_measurement_stays_unknown_when_an_interrupted_sweep_resumes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("resumed.redb");
    payload_store(&path, |key| key % 8 == 7);
    let db = open(&path);
    // The explicit compaction cannot measure, publishes one batch of its
    // sweep, and is then refused: the sweep stays active and unmeasured.
    Database::__fail_next_exact_fragmentation_measurement_for_test();
    Database::__refuse_storage_compaction_batch_after_for_test(1);
    assert!(db.compact_now().is_err(), "the second batch is refused");
    assert!(
        db.__storage_compaction_progress_for_test().0,
        "fixture: the interrupted sweep is still active"
    );
    let resumed = db.run_maintenance_cycle().unwrap().compaction;
    assert!(resumed.ran, "the active sweep resumes: {resumed:?}");
    assert_eq!(
        resumed.fragmentation_before, None,
        "a sweep that started unmeasured stays unknown when it resumes, never 0"
    );
    assert!(
        db.__storage_compaction_progress_for_test().0,
        "fixture: the sweep is still active at close"
    );
    db.close().unwrap();
    drop(db);

    let db = open(&path);
    assert!(
        db.__storage_compaction_progress_for_test().0,
        "reopen restores the active sweep from its checkpoint"
    );
    let mut batches = 0;
    while db.__storage_compaction_progress_for_test().0 {
        let resumed = db.run_maintenance_cycle().unwrap().compaction;
        assert!(resumed.ran, "the restored sweep resumes: {resumed:?}");
        assert_eq!(
            resumed.fragmentation_before, None,
            "the checkpoint keeps the measurement unknown across reopen"
        );
        batches += 1;
        assert!(batches < 10_000, "the restored sweep completes");
    }
    assert!(batches > 0);
    db.close().unwrap();
}

fn setting_of(sql: &str) -> StorageCompactionSetting {
    match contextdb_parser::parse(sql).unwrap() {
        contextdb_parser::Statement::SetStorageCompaction(setting) => setting,
        other => panic!("{sql} is not a storage compaction declaration: {other:?}"),
    }
}

/// Two storage compaction declarations run through the setter on two
/// threads while the first is paused after its declaration is durable and
/// before it is live.
fn race_declarations(
    first: &'static str,
    second: &'static str,
    before: Option<&str>,
) -> (Vec<(String, String, String)>, Vec<(String, String, String)>) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("race.redb");
    let db = Arc::new(open(&path));
    if let Some(before) = before {
        db.execute(before, &p()).unwrap();
    }
    let live_before = shown(&db);
    let entered = db.__storage_compaction_update_entries_for_test();
    let pause = db.__pause_after_storage_compaction_persist_for_test();
    let first_db = db.clone();
    let first = thread::spawn(move || first_db.set_storage_compaction(&setting_of(first)));
    assert!(
        pause.wait_until_reached_blocking(),
        "the first declaration reaches the point after it is durable"
    );
    assert!(
        db.__storage_compaction_update_lock_is_held_for_test(),
        "a declaration holds the shared setting lock from its read through its live publication"
    );
    let second_db = db.clone();
    let second = thread::spawn(move || second_db.set_storage_compaction(&setting_of(second)));
    db.__wait_for_storage_compaction_update_entry_for_test(entered + 2);
    assert_eq!(
        shown(&db),
        live_before,
        "the paused declaration is not live before its serialized publication"
    );
    pause.release();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    let live = shown(&db);
    let db = Arc::into_inner(db).expect("both setters have finished");
    db.close().unwrap();
    drop(db);
    let reopened = open(&path);
    let durable = shown(&reopened);
    reopened.close().unwrap();
    (live, durable)
}

#[test]
fn concurrent_declarations_of_different_settings_both_survive_in_live_show_and_reopen() {
    let (live, durable) = race_declarations(
        "SET STORAGE_COMPACTION START_RATIO 0.4",
        "SET STORAGE_COMPACTION MAX_SWEEPS 4",
        None,
    );
    let both = vec![
        setting("start_ratio", "0.4", "declared"),
        setting("min_interval_ms", "3600000", "default"),
        setting("stop_ratio", "0.05", "default"),
        setting("max_sweeps", "4", "declared"),
    ];
    assert_eq!(live, both, "neither declaration drops the other's clause");
    assert_eq!(durable, live, "the durable declaration is the live one");
}

#[test]
fn a_default_racing_a_declaration_leaves_live_show_and_reopen_agreeing() {
    let (live, durable) = race_declarations(
        "SET STORAGE_COMPACTION START_RATIO 0.4",
        "SET STORAGE_COMPACTION DEFAULT",
        Some("SET STORAGE_COMPACTION MAX_SWEEPS 4"),
    );
    assert_eq!(
        live,
        defaults(),
        "DEFAULT, serialized after the declaration, leaves the defaults"
    );
    assert_eq!(durable, live, "the durable declaration is the live one");
}

#[test]
fn the_rust_setter_refuses_what_sql_refuses_and_changes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("validated.redb");
    let db = open(&path);
    db.execute("SET STORAGE_COMPACTION START_RATIO 0.4", &p())
        .unwrap();
    let baseline = shown(&db);
    let declare = |clauses: StorageCompactionClauses| {
        db.set_storage_compaction(&StorageCompactionSetting::Declare(clauses))
    };
    let start = |ratio: f64| StorageCompactionClauses {
        start_ratio: Some(ratio),
        ..Default::default()
    };
    let stop = |ratio: f64| StorageCompactionClauses {
        stop_ratio: Some(ratio),
        ..Default::default()
    };
    for ratio in [0.0, -0.1, 1.5, f64::NAN, f64::INFINITY] {
        refused_with(
            declare(start(ratio)),
            Issue::StartRatioOutOfRange,
            &format!("START_RATIO {ratio}"),
        );
        refused_with(
            declare(stop(ratio)),
            Issue::StopRatioOutOfRange,
            &format!("STOP_RATIO {ratio}"),
        );
    }
    refused_with(
        declare(StorageCompactionClauses {
            max_sweeps: Some(0),
            ..Default::default()
        }),
        Issue::MaxSweepsNotPositive,
        "MAX_SWEEPS 0",
    );
    refused_with(
        declare(StorageCompactionClauses {
            min_interval_ms: Some(0),
            ..Default::default()
        }),
        Issue::MinIntervalNotPositive,
        "MIN_INTERVAL 0",
    );
    refused_with(
        declare(StorageCompactionClauses {
            start_ratio: Some(0.3),
            min_interval_ms: Some(i64::MAX as u64 + 1),
            ..Default::default()
        }),
        Issue::MinIntervalOutOfRange,
        "a valid START_RATIO beside an unrepresentable MIN_INTERVAL",
    );
    for (sql, issue) in [
        (
            "SET STORAGE_COMPACTION START_RATIO 0",
            Issue::StartRatioOutOfRange,
        ),
        (
            "SET STORAGE_COMPACTION START_RATIO 1.5",
            Issue::StartRatioOutOfRange,
        ),
        (
            "SET STORAGE_COMPACTION STOP_RATIO 0",
            Issue::StopRatioOutOfRange,
        ),
        (
            "SET STORAGE_COMPACTION MAX_SWEEPS 0",
            Issue::MaxSweepsNotPositive,
        ),
        (
            "SET STORAGE_COMPACTION MAX_SWEEPS 4294967296",
            Issue::MaxSweepsOutOfRange,
        ),
        (
            "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS",
            Issue::MinIntervalNotPositive,
        ),
        (
            "SET STORAGE_COMPACTION START_RATIO 0.3 START_RATIO 0.6",
            Issue::DuplicateClause,
        ),
    ] {
        refused_with(db.execute(sql, &p()), issue, sql);
    }
    assert_eq!(
        shown(&db),
        baseline,
        "a refused declaration changes nothing live"
    );
    db.close().unwrap();
    drop(db);
    let db = open(&path);
    assert_eq!(
        shown(&db),
        baseline,
        "a refused declaration changes nothing durable"
    );
    // The upper bounds are inclusive.
    db.execute("SET STORAGE_COMPACTION START_RATIO 1 STOP_RATIO 1", &p())
        .unwrap();
    assert_eq!(shown(&db)[0], setting("start_ratio", "1", "declared"));
    assert_eq!(shown(&db)[2], setting("stop_ratio", "1", "declared"));
    db.close().unwrap();
}

#[test]
fn the_longest_interval_shows_exactly_and_a_longer_one_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("interval.redb");
    let db = open(&path);
    db.execute(
        "SET STORAGE_COMPACTION MIN_INTERVAL 9223372036854775 SECONDS",
        &p(),
    )
    .unwrap();
    assert_eq!(
        shown(&db)[1],
        setting("min_interval_ms", "9223372036854775000", "declared"),
        "the largest whole-second interval shows exactly"
    );
    for sql in [
        "SET STORAGE_COMPACTION MIN_INTERVAL 9223372036854776 SECONDS",
        "SET STORAGE_COMPACTION MIN_INTERVAL 9223372036854775808 MILLISECONDS",
        "SET STORAGE_COMPACTION MIN_INTERVAL 18446744073709551616 MILLISECONDS",
    ] {
        refused_with(db.execute(sql, &p()), Issue::MinIntervalOutOfRange, sql);
    }
    db.set_storage_compaction(&StorageCompactionSetting::Declare(
        StorageCompactionClauses {
            min_interval_ms: Some(i64::MAX as u64),
            ..Default::default()
        },
    ))
    .unwrap();
    refused_with(
        db.set_storage_compaction(&StorageCompactionSetting::Declare(
            StorageCompactionClauses {
                min_interval_ms: Some(i64::MAX as u64 + 1),
                ..Default::default()
            },
        )),
        Issue::MinIntervalOutOfRange,
        "one millisecond past the inspectable range",
    );
    let longest = setting("min_interval_ms", "9223372036854775807", "declared");
    assert_eq!(shown(&db)[1], longest);
    db.close().unwrap();
    drop(db);
    let db = open(&path);
    assert_eq!(
        shown(&db)[1],
        longest,
        "the longest interval survives reopen exactly"
    );
    db.close().unwrap();
}

#[test]
fn every_maintenance_receipt_names_the_policy_that_governed_it() {
    let undeclared = "policy: StorageCompactionPolicy { start_ratio: None, min_interval_ms: None, \
                      stop_ratio: None, max_sweeps: None }";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("receipts.redb");
    payload_store(&path, |key| key % 10 != 3);
    let db = open(&path);
    let free = reclaimable(&db);

    // A negative check measured the store and took a decision on it.
    let checked = db.run_maintenance_cycle().unwrap();
    assert!(!checked.compaction.ran);
    assert!(
        checked
            .compaction
            .fragmentation_before
            .is_some_and(|measured| (measured - free).abs() < 0.01),
        "a negative check reports what it measured ({free}): {:?}",
        checked.compaction
    );
    for receipt in [
        format!("{:?}", checked.compaction),
        format!("{:?}", checked.pruning),
    ] {
        assert!(
            receipt.contains(undeclared),
            "an undeclared store's receipt names the default policy: {receipt}"
        );
    }

    // No check was due: nothing measured, but the governing policy is named.
    let not_due = db.run_maintenance_cycle().unwrap().compaction;
    assert_eq!(not_due.fragmentation_before, None);
    assert!(format!("{not_due:?}").contains(undeclared), "{not_due:?}");

    // A declared start ratio decides the next due check, and its receipt
    // and every resumed batch of that sweep say so.
    db.execute(
        "SET STORAGE_COMPACTION START_RATIO 0.05 MIN_INTERVAL 1 SECONDS",
        &p(),
    )
    .unwrap();
    Database::__advance_compaction_clock_for_test(Duration::from_secs(2));
    let started = db.run_maintenance_cycle().unwrap().compaction;
    assert!(started.ran, "{started:?}");
    let declared = "policy: StorageCompactionPolicy { start_ratio: Some(0.05), \
                    min_interval_ms: Some(1000), stop_ratio: None, max_sweeps: None }";
    assert!(format!("{started:?}").contains(declared), "{started:?}");
    if db.__storage_compaction_progress_for_test().0 {
        let resumed = db.run_maintenance_cycle().unwrap().compaction;
        assert!(resumed.ran);
        assert!(format!("{resumed:?}").contains(declared), "{resumed:?}");
    }

    // An explicit compaction names the stop rules it ran under.
    db.execute("SET STORAGE_COMPACTION MAX_SWEEPS 2", &p())
        .unwrap();
    let explicit = db.compact_now().unwrap();
    assert!(
        format!("{explicit:?}").contains(
            "policy: StorageCompactionPolicy { start_ratio: Some(0.05), \
             min_interval_ms: Some(1000), stop_ratio: None, max_sweeps: Some(2) }"
        ),
        "{explicit:?}"
    );
    db.close().unwrap();
}
