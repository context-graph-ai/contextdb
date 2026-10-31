//! Consolidation declarations preserve populated data and authored history.
use super::*;
use redb::{ReadableDatabase, TableHandle};
use std::cell::Cell;
thread_local! { static PROJECTED_ROWS: Cell<u64> = const { Cell::new(0) }; }
pub(crate) fn note_projection_rows(rows: u64) {
    PROJECTED_ROWS.with(|count| count.set(count.get() + rows));
}
fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
fn index(column: &str) -> VectorIndexRef {
    VectorIndexRef::new("breadth_rows", column)
}
fn run(db: &Database, sql: &str) {
    db.execute(sql, &empty())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
}
fn column(db: &Database) -> contextdb_core::ColumnDef {
    db.table_meta("breadth_rows")
        .unwrap()
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .unwrap()
}
fn canonical<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}
fn record_authored(db: &Database, after: &mut Lsn, units: &mut Vec<ChangeSet>) {
    units.push(db.changes_since(*after).unwrap());
    *after = db.current_lsn();
}
fn seed(db: &Database, rows: usize) -> Vec<ChangeSet> {
    seed_with_saved_graphs(db, rows, true)
}
fn visible(db: &Database) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id, bucket, body, embedding, compressed, marker FROM breadth_rows ORDER BY id",
        &empty(),
    )
    .unwrap()
    .rows()
    .to_vec()
}
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn physical_roots(db: &Database) -> BTreeMap<String, Option<Option<[u8; 32]>>> {
    let Some(persistence) = &db.persistence else {
        return BTreeMap::new();
    };
    persistence
        .with_db(|store| {
            let read = store.begin_read().unwrap();
            let mut roots = BTreeMap::new();
            for table in read.list_tables().unwrap() {
                let name = table.name();
                if name.starts_with("rel_")
                    || name.starts_with("__rel_index_")
                    || name.starts_with("vector_partition_")
                    || name.starts_with("content_size_")
                    || matches!(
                        name,
                        "vector_entries"
                            | "vector_open_directory"
                            | "__authored_row_bodies"
                            | "__historical_index_commitments"
                    )
                {
                    roots.insert(name.to_owned(), read.normal_table_root(name).unwrap());
                }
            }
            Ok(roots)
        })
        .unwrap()
}
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn authored_rows(db: &Database) -> Vec<(u64, u64, u64, serde_json::Value)> {
    let memory = db.declared_reader_memory().unwrap();
    let decode = crate::read_image_memory::ImageMemory::operation_reader(
        db.accountant.clone(),
        memory.clone(),
    )
    .unwrap();
    let _decode = decode.enter();
    db.change_log_since(Lsn(0))
        .unwrap()
        .into_iter()
        .filter_map(|entry| {
            let ChangeLogEntry::RowInsert { table, row_id, lsn } = entry else {
                return None;
            };
            if table != "breadth_rows" {
                return None;
            }
            let row = db
                .row_for_change(&table, row_id, lsn, memory.clone(), &|| Ok(()), None)
                .unwrap()
                .expect("an immutable authored row cannot disappear after a metadata ALTER");
            let scalar = ["id", "bucket", "body"]
                .into_iter()
                .map(|name| {
                    (
                        name.to_owned(),
                        row.values.get(name).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            Some((row_id.0, row.created_tx.0, lsn.0, canonical(&scalar)))
        })
        .collect()
}
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn assert_frontier(db: &Database) {
    if let Some(p) = &db.persistence {
        p.with_db(|store| {
            let read = store.begin_read().unwrap();
            let table = read
                .open_table(redb::TableDefinition::<&str, u64>::new(
                    "committed_read_lsn_bounds",
                ))
                .unwrap();
            assert_eq!(
                table.get("breadth_rows").unwrap().unwrap().value(),
                db.current_lsn().0,
                "metadata and authored occurrence advance the table read frontier together"
            );
            Ok(())
        })
        .unwrap();
        let root = db.writer_read_root().unwrap().unwrap();
        let recovery = RedbPersistence::writer_recovery_floors_in_read(&root).unwrap();
        assert!(recovery.source_lsn_floor >= db.current_lsn().0);
    }
}

#[test]
fn refused_consolidation_keeps_old_declaration_history_and_graph_authoritative() {
    for file in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("refusal.db");
        let db = if file {
            Database::open(&path).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed(&db, 24);
        let schema = canonical(&db.table_meta("breadth_rows").unwrap());
        let rows = visible(&db);
        let authored = authored_rows(&db);
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let serving = graphs(&db);
        let roots = physical_roots(&db);
        let old_root = db.writer_read_root().unwrap();
        let old_limit = db.memory_limit();
        let cap = db.finalization.accountant.usage().used;
        db.set_memory_limit(Some(cap)).unwrap();
        let refusal = db
            .execute(
                "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION (TOMBSTONE_PERCENT = 19)",
                &empty(),
            )
            .unwrap_err();
        db.set_memory_limit(old_limit).unwrap();
        match refusal {
            Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => {
                assert_eq!(
                    budget_limit_bytes, cap,
                    "retain the actual original writer cap"
                );
                assert!(requested_bytes > available_bytes);
                assert!(
                    !subsystem.is_empty() && !operation.is_empty() && !hint.is_empty(),
                    "retain typed phase and remedy fields"
                );
            }
            other => {
                panic!("writer pressure must remain the original typed memory refusal: {other:?}")
            }
        }
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), schema);
        assert_eq!(visible(&db), rows);
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), history);
        assert_eq!(graphs(&db), serving);
        assert_eq!(physical_roots(&db), roots);
        if let Some(root) = &old_root {
            assert_eq!(
                canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()),
                schema
            );
        }
        drop(old_root);
        db.close().unwrap();
        drop(db);
        if file {
            let reopened = Database::open(&path).unwrap();
            assert_eq!(
                canonical(&reopened.table_meta("breadth_rows").unwrap()),
                schema
            );
            assert_eq!(canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()), history);
            assert_eq!(visible(&reopened), rows);
            assert_eq!(authored_rows(&reopened), authored);
            reopened.close().unwrap();
        }
    }
}

#[test]
fn consolidation_occurrences_replay_with_the_same_authored_ddl_and_row_history() {
    let dir = tempfile::tempdir().unwrap();
    let source = Database::open(dir.path().join("source.db")).unwrap();
    let mut units = seed(&source, 24);
    let authored = authored_rows(&source);
    let mut after = source.current_lsn();
    for members in [
        "(CHANGE_PERCENT = 11, TOMBSTONE_PERCENT = 17)",
        "(TOMBSTONE_PERCENT = 19)",
        "(TOMBSTONE_PERCENT = 19)",
    ] {
        run(
            &source,
            &format!("ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION {members}"),
        );
        record_authored(&source, &mut after, &mut units);
    }
    let replay = Database::open_memory();
    replay.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for unit in units {
        replay
            .apply_changes(
                unit,
                &ConflictPolicies::uniform(crate::sync_types::ConflictPolicy::LatestWins),
            )
            .unwrap();
    }
    assert_eq!(
        canonical(&replay.table_meta("breadth_rows").unwrap()),
        canonical(&source.table_meta("breadth_rows").unwrap())
    );
    assert_eq!(
        canonical(&replay.ddl_log_since(Lsn(0)).unwrap()),
        canonical(&source.ddl_log_since(Lsn(0)).unwrap())
    );
    assert_eq!(
        authored_rows(&replay)
            .iter()
            .map(|row| &row.3)
            .collect::<Vec<_>>(),
        authored.iter().map(|row| &row.3).collect::<Vec<_>>()
    );
    assert_eq!(visible(&replay), visible(&source));
    source.close().unwrap();
    replay.close().unwrap();
}

#[test]
fn consolidation_transaction_rollback_does_not_publish_local_autocommit_metadata() {
    for (file, saved_graphs) in [(false, false), (false, true), (true, false), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let db = if file {
            Database::open(dir.path().join("transaction.db")).unwrap()
        } else {
            Database::open_memory()
        };
        let _ = seed_with_saved_graphs(&db, 24, saved_graphs);
        let authored = authored_rows(&db);
        let before = canonical(&db.table_meta("breadth_rows").unwrap());
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        let rows = visible(&db);
        crate::read_probe::reset_thread_maintenance_worker_starts();
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
            &empty(),
        )
        .unwrap();
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), before);
        assert_eq!(
            crate::read_probe::thread_maintenance_worker_starts(),
            0,
            "private staging starts no maintenance worker"
        );
        db.rollback(tx).unwrap();
        assert_eq!(
            crate::read_probe::thread_maintenance_worker_starts(),
            0,
            "rollback starts no maintenance worker"
        );
        assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), before);
        assert_eq!(canonical(&db.ddl_log_since(Lsn(0)).unwrap()), history);
        assert_eq!(visible(&db), rows);
        assert_eq!(authored_rows(&db), authored);
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
            &empty(),
        )
        .unwrap();
        assert_eq!(
            crate::read_probe::thread_maintenance_worker_starts(),
            0,
            "private staging starts no maintenance worker"
        );
        db.commit(tx).unwrap();
        assert_eq!(
            crate::read_probe::thread_maintenance_worker_starts(),
            0,
            "commit starts no maintenance worker"
        );
        assert!(column(&db).consolidation_disabled);
        assert_eq!(visible(&db), rows);
        assert_eq!(authored_rows(&db), authored);
        assert_eq!(
            db.ddl_log_since(Lsn(0)).unwrap().len(),
            history.as_array().unwrap().len() + 1
        );
        db.close().unwrap();
    }
}

#[test]
fn consolidation_failure_before_and_after_durability_keeps_one_authoritative_declaration() {
    for cut in [10, 11] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cut.db");
        let db = Database::open(&path).unwrap();
        let _ = seed(&db, 24);
        let schema = canonical(&db.table_meta("breadth_rows").unwrap());
        let rows = visible(&db);
        let roots = physical_roots(&db);
        let serving = graphs(&db);
        let authored = authored_rows(&db);
        let count = db.ddl_log_since(Lsn(0)).unwrap().len();
        let root = db.writer_read_root().unwrap().unwrap();
        crate::persistence::arm_policy_metadata_fault_for_test(cut, false);
        let error = db
            .execute(
                "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
                &empty(),
            )
            .unwrap_err();
        assert!(
            matches!(error.engine_diagnostic(), Error::Other(message) if message == "injected policy metadata publication failure"),
            "the actual durability cut must refuse: {error:?}"
        );
        let fresh = db.writer_read_root().unwrap().unwrap();
        let fresh_meta = fresh.borrow_captured_table_meta("breadth_rows").unwrap();
        assert_eq!(
            fresh_meta
                .columns
                .iter()
                .find(|column| column.name == "embedding")
                .unwrap()
                .consolidation_disabled,
            cut == 11,
            "a newly captured root must publish the committed declaration even when its caller received an error"
        );
        assert_eq!(column(&db).consolidation_disabled, cut == 11);
        assert_eq!(
            canonical(fresh.borrow_captured_table_meta("breadth_rows").unwrap()),
            canonical(&db.table_meta("breadth_rows").unwrap()),
            "a root captured after the error must expose the authoritative durable declaration"
        );
        assert_frontier(&db);
        drop(fresh);
        assert_eq!(
            db.ddl_log_since(Lsn(0)).unwrap().len(),
            count + usize::from(cut == 11)
        );
        assert_eq!(
            canonical(root.borrow_captured_table_meta("breadth_rows").unwrap()),
            schema
        );
        if cut == 10 {
            assert_eq!(canonical(&db.table_meta("breadth_rows").unwrap()), schema);
        }
        assert_eq!(physical_roots(&db), roots);
        assert_eq!(graphs(&db), serving);
        assert_eq!(visible(&db), rows);
        assert_eq!(authored_rows(&db), authored);
        if cut == 11 {
            run(
                &db,
                "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
            );
            assert_eq!(
                db.ddl_log_since(Lsn(0)).unwrap().len(),
                count + 2,
                "a committed operation that returned an error must not reuse its durable DDL LSN"
            );
        }
        let authoritative = canonical(&db.table_meta("breadth_rows").unwrap());
        let history = canonical(&db.ddl_log_since(Lsn(0)).unwrap());
        drop(root);
        db.close().unwrap();
        drop(db);
        let reopened = Database::open(&path).unwrap();
        assert_eq!(
            canonical(&reopened.table_meta("breadth_rows").unwrap()),
            authoritative
        );
        assert_eq!(canonical(&reopened.ddl_log_since(Lsn(0)).unwrap()), history);
        assert_eq!(visible(&reopened), rows);
        assert_eq!(authored_rows(&reopened), authored);
        reopened.close().unwrap();
    }
}

#[test]
fn consolidation_crash_cuts_recover_complete_metadata_and_ddl_with_unchanged_data() {
    const CHILD: &str = "CONTEXTDB_CONSOLIDATION_CRASH_PATH";
    const CUT: &str = "CONTEXTDB_CONSOLIDATION_CRASH_CUT";
    if let Some(path) = std::env::var_os(CHILD) {
        let cut: u8 = std::env::var(CUT).unwrap().parse().unwrap();
        let db = Database::open_with_options(
            path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        crate::persistence::arm_policy_metadata_fault_for_test(cut, true);
        run(
            &db,
            "ALTER TABLE breadth_rows ALTER COLUMN embedding SET CONSOLIDATION NONE",
        );
        panic!("the armed durable cut must abort the child");
    }
    for cut in [10u8, 11] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crash.db");
        let db = Database::open(&path).unwrap();
        let _ = seed(&db, 24);
        let rows = visible(&db);
        let authored = authored_rows(&db);
        let roots = physical_roots(&db);
        let history = db.ddl_log_since(Lsn(0)).unwrap().len();
        db.close().unwrap();
        drop(db);
        let name = format!(
            "{}::consolidation_crash_cuts_recover_complete_metadata_and_ddl_with_unchanged_data",
            module_path!().split_once("::").unwrap().1
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name, "--nocapture"])
            .env(CHILD, &path)
            .env(CUT, cut.to_string())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("POLICY_METADATA_CRASH={cut}")),
            "the child must reach its selected cut: {:?}",
            output
        );
        let reopened = Database::open(&path).unwrap();
        reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(column(&reopened).consolidation_disabled, cut == 11);
        assert_eq!(
            reopened.ddl_log_since(Lsn(0)).unwrap().len(),
            history + usize::from(cut == 11)
        );
        assert_eq!(visible(&reopened), rows);
        assert_eq!(authored_rows(&reopened), authored);
        assert_eq!(physical_roots(&reopened), roots);
        reopened.close().unwrap();
    }
}

#[test]
fn phase_file_is_visible_before_an_isolated_child_completes() {
    use std::io::{BufRead, Read, Write};
    const CHILD: &str = "CONTEXTDB_PHASE_VISIBILITY_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let db = Database::open_memory();
        run(
            &db,
            "CREATE TABLE phase_visibility (id INTEGER PRIMARY KEY)",
        );
        println!("PHASE_CHILD_READY");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0u8]).unwrap();
        db.close().unwrap();
        return;
    }
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phases.log");
    let name = format!(
        "{}::phase_file_is_visible_before_an_isolated_child_completes",
        module_path!().split_once("::").unwrap().1
    );
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &name, "--nocapture"])
            .env(CHILD, "1")
            .env("CONTEXTDB_MAINTENANCE_PHASES", "1")
            .env("CONTEXTDB_MAINTENANCE_PHASE_FILE", &path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
    let mut ready = false;
    loop {
        let mut line = String::new();
        if output.read_line(&mut line).unwrap() == 0 {
            break;
        }
        if line.contains("PHASE_CHILD_READY") {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "the harmless actual operation must finish before inspecting its live child"
    );
    let records = std::fs::read_to_string(&path).unwrap();
    assert!(records.contains(&format!("maintenance_phase pid={}", child.0.id())));
    assert!(records.contains("operation=execute phase=enter"));
    assert!(records.contains("operation=execute phase=exit"));
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "phase records are visible while the child is still blocked on stdin"
    );
    child.0.stdin.take().unwrap().write_all(&[1]).unwrap();
    assert!(child.0.wait().unwrap().success());
}

// Fixed, nonallocating observation at the two actual stored-row decode doors.
// Parent usage includes legitimate growing proof/index state; this guard only
// compares the decoder's own original-funded row window for identical shapes.
#[derive(Clone, Copy, Default)]
struct ProjectionDecodeWindows {
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    active: bool,
    starts: [usize; 2],
    finishes: [usize; 2],
    dirty_start: bool,
    empty_finish: bool,
    peak: [usize; 2],
}
thread_local! { static DECODE_WINDOWS: Cell<ProjectionDecodeWindows> = const { Cell::new(ProjectionDecodeWindows { active: false, starts: [0;2], finishes: [0;2], dirty_start: false, empty_finish: false, peak: [0;2] }) }; }
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from schema_projection.rs"
    )
)]
pub(crate) fn observe_projection_decode_window(pass: usize, before: bool, held: usize) {
    DECODE_WINDOWS.with(|slot| {
        let mut work = slot.get();
        if !work.active {
            return;
        }
        if before {
            work.starts[pass] += 1;
            work.dirty_start |= held != 0;
        } else {
            work.finishes[pass] += 1;
            work.empty_finish |= held == 0;
            work.peak[pass] = work.peak[pass].max(held);
        }
        slot.set(work);
    });
}
#[test]
fn scalar_schema_projection_uses_one_fresh_original_funded_decoder_per_actual_row() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut previous_peak = None;
    for rows in [16, 32] {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            dir.path().join("row-windows.redb"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        run(
            &db,
            "CREATE TABLE row_windows (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        );
        for id in 0..rows {
            db.execute(
                "INSERT INTO row_windows VALUES ($id,$body)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("body".into(), Value::Text("same-width".repeat(32))),
                ]),
            )
            .unwrap();
        }
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                DECODE_WINDOWS.with(|s| s.set(ProjectionDecodeWindows::default()));
            }
        }
        let reset = Reset;
        DECODE_WINDOWS.with(|slot| {
            slot.set(ProjectionDecodeWindows {
                active: true,
                ..Default::default()
            })
        });
        run(&db, "ALTER TABLE row_windows ADD COLUMN marker INTEGER");
        let work = DECODE_WINDOWS.with(Cell::get);
        drop(reset);
        assert_eq!(
            work.starts, [rows; 2],
            "both actual stored-row passes visit each physical version exactly once"
        );
        assert_eq!(work.finishes, work.starts);
        assert!(
            !work.dirty_start,
            "a new row must not inherit a preceding row's decoder debit"
        );
        assert!(
            !work.empty_finish,
            "the actual row decode must debit its own original-funded decoder"
        );
        if let Some(peak) = previous_peak {
            assert_eq!(
                work.peak, peak,
                "doubling equal-shaped rows must not grow either one-row decoder window"
            );
        }
        previous_peak = Some(work.peak);
        assert_eq!(
            db.execute("SELECT id FROM row_windows ORDER BY id", &empty())
                .unwrap()
                .rows()
                .len(),
            rows
        );
        db.close().unwrap();
    }
}
fn seed_with_saved_graphs(db: &Database, rows: usize, saved_graphs: bool) -> Vec<ChangeSet> {
    let mut units = Vec::new();
    let mut after = Lsn(0);
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    run(
        db,
        "CREATE TABLE breadth_rows (id INTEGER PRIMARY KEY, bucket INTEGER, body TEXT, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 7), compressed VECTOR(3) WITH (quantization = 'SQ8') SEARCH_MODE INDEXED IVF (PROBES = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );
    record_authored(db, &mut after, &mut units);
    run(db, "CREATE INDEX breadth_bucket ON breadth_rows (bucket)");
    record_authored(db, &mut after, &mut units);
    for id in 1..=rows {
        let x = id as f32;
        db.execute("INSERT INTO breadth_rows (id, bucket, body, embedding, compressed) VALUES ($id, $bucket, $body, $vector, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("bucket".into(), Value::Int64((id % 2) as i64)),
                ("body".into(), Value::Text(format!("authored-{id}"))),
                ("vector".into(), Value::Vector(vec![x, rows as f32 + 1.0 - x, 1.0])),
            ])).unwrap();
        record_authored(db, &mut after, &mut units);
    }
    run(
        db,
        "UPDATE breadth_rows SET body = 'replacement' WHERE id = 1",
    );
    record_authored(db, &mut after, &mut units);
    // Populate authored custody BEFORE the narrow ALTER. An empty-family
    // assertion would reject valid stores and miss the move-out regression.
    run(db, "ALTER TABLE breadth_rows ADD COLUMN marker TEXT");
    record_authored(db, &mut after, &mut units);
    if !saved_graphs {
        assert!(
            graphs(db)
                .iter()
                .all(|(generation, _)| generation.is_none()),
            "the pre-query control must not have a published clustered index"
        );
        return units;
    }
    for _ in 0..8 {
        if ["embedding", "compressed"]
            .iter()
            .all(|name| db.__debug_vector_indexed_available_for_test(index(name)))
        {
            return units;
        }
        db.run_maintenance_cycle().unwrap();
    }
    panic!("fixture must have both published clustered indexes before ALTER");
}
fn graphs(db: &Database) -> Vec<(Option<u64>, bool)> {
    ["embedding", "compressed"]
        .iter()
        .map(|name| {
            (
                db.__debug_vector_index_generation_for_test(index(name)),
                db.__debug_vector_indexed_available_for_test(index(name)),
            )
        })
        .collect()
}
