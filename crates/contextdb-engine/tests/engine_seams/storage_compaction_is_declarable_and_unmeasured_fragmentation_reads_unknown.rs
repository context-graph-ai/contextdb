//! Storage compaction is a declared store setting, and a compaction receipt
//! never invents a measurement.
//!
//! When the allocator's free-page totals cannot be trusted, no compaction
//! decision is taken and the receipt says the fragmentation is unknown rather
//! than reporting 0. The start ratio, the minimum interval between automatic
//! attempts, and the explicit compaction stop rules are declared with
//! `SET STORAGE_COMPACTION`: today's values are the silent defaults, a
//! declared value changes when compaction starts and stops, and
//! `SHOW STORAGE_COMPACTION` names each value's source across reopen.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use redb::TableDefinition;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    let mut sweeps = Vec::new();
    for (name, declaration) in [
        ("default", None),
        (
            "one-sweep",
            Some("SET STORAGE_COMPACTION STOP_RATIO 0 MAX_SWEEPS 1"),
        ),
        (
            "three-sweeps",
            Some("SET STORAGE_COMPACTION STOP_RATIO 0 MAX_SWEEPS 3"),
        ),
    ] {
        let path = directory.path().join(format!("{name}.redb"));
        payload_store(&path, |key| key % 8 == 7);
        let db = open(&path);
        if let Some(declaration) = declaration {
            db.execute(declaration, &p()).unwrap();
        }
        let report = db.compact_now().unwrap();
        assert!(report.ran && report.file_shrank, "{name}: {report:?}");
        sweeps.push((name, Database::__explicit_compaction_sweeps_last_for_test()));
        db.close().unwrap();
    }
    println!("sweeps: {sweeps:?}");
    assert_eq!(
        sweeps[0].1, 1,
        "by default one sweep leaves this store tight"
    );
    assert_eq!(sweeps[1].1, 1, "MAX_SWEEPS 1 stops after one sweep");
    assert!(
        sweeps[2].1 >= 2 && sweeps[2].1 <= 3,
        "STOP_RATIO 0 keeps sweeping until a sweep stops shrinking, within MAX_SWEEPS 3: {sweeps:?}"
    );
}

#[test]
fn a_declared_minimum_interval_changes_when_a_negative_check_repeats() {
    let directory = tempfile::tempdir().unwrap();
    let mut checks = Vec::new();
    for (name, declaration) in [
        ("default", None),
        (
            "every-cycle",
            Some("SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS"),
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
        assert!(!db.run_maintenance_cycle().unwrap().compaction.ran);
        checks.push((
            name,
            Database::__automatic_compaction_checks_for_test() - before,
        ));
        db.close().unwrap();
    }
    assert_eq!(
        checks,
        vec![("default", 1), ("every-cycle", 2)],
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
        "SET STORAGE_COMPACTION START_RATIO 1",
        "SET STORAGE_COMPACTION STOP_RATIO 1",
        "SET STORAGE_COMPACTION MAX_SWEEPS 0",
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
