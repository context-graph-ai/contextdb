//! Canonical local erasure-control contract, including controls that cannot be
//! reconstructed from Purged row lives. These rows belong beside persistence:
//! existing history-witness tests cover historical table roots, rather than
//! this configuration-bound summary and its named absence proofs. All rows
//! share production-created stores; decisive mutants and receipts are recorded
//! in the landing evidence, with test-only commits before implementation.
use super::*;
use crate::database::Database;
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadLimits,
};
use contextdb_core::read_memory::{NativeRefusal, ReadMemoryBudget, ReadMemoryOwner};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};

const FRONTIERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("read_erasure_frontiers");
const GLOBAL: &[u8] = &[0xff];

fn seed(path: &Path) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(crate::database::MaintenancePolicy::CallerDriven);
    for table in ["items", "unrelated"] {
        db.execute(&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST"), &HashMap::new()).unwrap();
        db.execute(
            &format!("INSERT INTO {table} VALUES (1, 'one'), (2, 'two')"),
            &HashMap::new(),
        )
        .unwrap();
    }
    db
}

fn frontier(persistence: &RedbPersistence, key: &[u8]) -> Option<Lsn> {
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = match read.open_table(FRONTIERS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            Ok(table
                .get(key)
                .map_err(RedbPersistence::storage_error)?
                .map(|value| Lsn(u64::from_le_bytes(value.value().try_into().unwrap()))))
        })
        .unwrap()
}

#[derive(Clone, Copy)]
enum Damage {
    MissingGlobal,
    MalformedGlobal,
    RemovedNamed,
    ReplacedTable,
}

fn damaged_control_refuses(damage: Damage, attempt_write: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("damage.db");
    let db = seed(&path);
    db.execute("DISCARD FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let memory = Meter::new(usize::MAX);
    let captured = scope(&db, &["items"], memory.clone());
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    persistence
        .validate_erasure_frontiers(&captured, memory.clone())
        .unwrap();
    // Bypass the producer on the already-open handle. This tests the fresh
    // read-side binding itself, rather than an incidental reopen write.
    persistence
        .with_db(|raw| {
            let write = raw.begin_write().map_err(RedbPersistence::storage_error)?;
            if matches!(damage, Damage::ReplacedTable) {
                match write.delete_table(FRONTIERS) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                }
            }
            {
                let mut table = write
                    .open_table(FRONTIERS)
                    .map_err(RedbPersistence::storage_error)?;
                match damage {
                    Damage::MissingGlobal => {
                        table
                            .remove(GLOBAL)
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    Damage::MalformedGlobal => {
                        table
                            .insert(GLOBAL, &[1_u8][..])
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    Damage::RemovedNamed => {
                        table
                            .remove(&b"items"[..])
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    Damage::ReplacedTable => {
                        table
                            .insert(GLOBAL, &0_u64.to_le_bytes()[..])
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let result = if attempt_write {
        drop(persistence);
        match Database::open(&path) {
            Err(error) => Err(error),
            Ok(db) => {
                let result = db
                    .execute(
                        "INSERT INTO unrelated VALUES (3, 'cannot bless damage')",
                        &HashMap::new(),
                    )
                    .map(|_| ());
                db.close().unwrap();
                result
            }
        }
    } else {
        persistence.validate_erasure_frontiers(&captured, memory)
    };
    assert!(
        matches!(result, Err(Error::StoreCorrupted { .. })),
        "damaged canonical control must refuse: {result:?}"
    );
}

#[test]
fn a_missing_global_erasure_control_refuses() {
    damaged_control_refuses(Damage::MissingGlobal, false);
}
#[test]
fn a_malformed_global_erasure_control_refuses() {
    damaged_control_refuses(Damage::MalformedGlobal, false);
}
#[test]
fn a_removed_named_erasure_control_cannot_be_treated_as_authenticated_absence() {
    damaged_control_refuses(Damage::RemovedNamed, false);
}
#[test]
fn a_replaced_erasure_table_cannot_authorize_a_fresh_read() {
    damaged_control_refuses(Damage::ReplacedTable, false);
}

#[test]
fn lineage_projection_repair_preserves_a_discard_only_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("repair.db");
    let db = seed(&path);
    db.execute("DISCARD FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let erased = db.current_lsn();
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(erased));
    persistence
        .with_db(|db| {
            let write = RedbPersistence::begin_history_write(db)?;
            RedbPersistence::rebuild_read_lineage_config_in_write(&write)?;
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &[],
                &mut persistence.proof_admission(),
            )?;
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(erased));
    assert_eq!(frontier(&persistence, GLOBAL), Some(erased));
    drop(persistence);
    let db = Database::open(&path).unwrap();
    assert!(db.current_lsn() >= erased);
    db.execute("INSERT INTO items VALUES (1, 'fresh')", &HashMap::new())
        .unwrap();
    assert_eq!(
        db.execute("SELECT body FROM items WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![contextdb_core::Value::Text("fresh".into())]]
    );
    db.close().unwrap();
}

fn export_preserves_canonical_control(memory_source: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let artifact = directory.path().join("artifact.db");
    let db = if memory_source {
        let db = Database::open_memory();
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST", &HashMap::new()).unwrap();
        db.execute(
            "INSERT INTO items VALUES (1, 'one'), (2, 'two')",
            &HashMap::new(),
        )
        .unwrap();
        db
    } else {
        seed(&path)
    };
    if memory_source {
        db.execute("DELETE FROM items WHERE id = 1", &HashMap::new())
            .unwrap();
        db.publish_memory_erasure_frontier_for_test(
            &BTreeSet::from(["items".to_owned()]),
            db.current_lsn(),
        )
        .unwrap();
    } else {
        db.execute("DISCARD FROM items WHERE id = 1", &HashMap::new())
            .unwrap();
    }
    let erased = db.current_lsn();
    let report = db.export_snapshot(&artifact).unwrap();
    assert!(report.snapshot_lsn >= erased);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&artifact).unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(erased));
    assert_eq!(frontier(&persistence, GLOBAL), Some(erased));
    drop(persistence);
    let restored = Database::open(&artifact).unwrap();
    assert!(restored.current_lsn() >= erased);
    // The memory fixture proves canonical controls and local coordinates;
    // fresh life after durable DISCARD is the file-backed contract.
    if !memory_source {
        restored
            .execute("INSERT INTO items VALUES (1, 'fresh')", &HashMap::new())
            .unwrap();
        assert_eq!(
            restored
                .read_session(ReadLimits::default())
                .unwrap()
                .execute("SELECT body FROM items WHERE id = 1", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![contextdb_core::Value::Text("fresh".into())]]
        );
    }
    restored.close().unwrap();
}
#[test]
fn a_file_export_preserves_discard_controls_and_their_local_lsn_floor() {
    export_preserves_canonical_control(false);
}
#[test]
fn a_memory_export_preserves_canonical_controls_and_their_local_lsn_floor() {
    export_preserves_canonical_control(true);
}

#[derive(Debug)]
struct Meter {
    cap: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
}
impl Meter {
    fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }
}
impl ReadMemoryBudget for Meter {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let prior = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.cap)
            })
            .map_err(|used| Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: self.cap.saturating_sub(used),
                budget_limit_bytes: self.cap,
                hint: hint.into(),
            })?;
        self.peak.fetch_max(prior + bytes, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        assert!(self.used.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.cap)
            })
            .map(|prior| {
                self.peak.fetch_max(prior + bytes, Ordering::SeqCst);
            })
            .map_err(|used| NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: self.cap.saturating_sub(used),
                budget_limit_bytes: self.cap,
                hint,
            })
    }
    fn native_reserve_supported(&self) -> bool {
        true
    }
}
#[derive(Debug)]
struct FrozenClock;
impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(std::future::pending())
    }
}
fn scope(
    db: &Database,
    names: &[&str],
    memory: Arc<Meter>,
) -> crate::executor::ReadPublicationScope {
    let mut scope = crate::executor::ReadPublicationScope::bounded(
        db.current_lsn(),
        names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>(),
        ReadLimits::default(),
        Arc::new(FrozenClock),
        OwnerReadCancellation::new(),
        0,
    );
    scope.read_memory = Some(memory);
    scope
}
fn measured_authorization(db: &Database, names: &[&str]) -> usize {
    let memory = Meter::new(usize::MAX);
    let scope = scope(db, names, memory.clone());
    let (answer, work) = crate::writer_query_test_seams::observe(|| {
        db.authorize_authoritative_read_publication(&scope)
    });
    drop(answer.unwrap());
    assert_eq!(work.history.sidecar_entries.iter().sum::<u64>(), 0);
    assert_eq!(work.history.sidecar_decodes.iter().sum::<u64>(), 0);
    assert_eq!(
        work.erasure_frontier_probes,
        if names.contains(&"*") {
            1
        } else {
            names.len() as u64
        }
    );
    assert_eq!(
        memory.used.load(Ordering::SeqCst),
        0,
        "publication releases its scratch credits"
    );
    memory.peak.load(Ordering::SeqCst)
}

fn history_authorization_charge(lives: usize, reopen: bool) -> [usize; 2] {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.db");
    let db = seed(&path);
    for _ in 0..lives {
        db.execute("PURGE FROM items WHERE id = 1", &HashMap::new())
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'fresh')", &HashMap::new())
            .unwrap();
    }
    let db = if reopen {
        db.close().unwrap();
        drop(db);
        Database::open(&path).unwrap()
    } else {
        db
    };
    let charges = [
        measured_authorization(&db, &["items", "unrelated"]),
        measured_authorization(&db, &["*"]),
    ];
    assert!(
        charges.into_iter().all(|peak| peak > 0),
        "the request reader funds authorization"
    );
    db.close().unwrap();
    charges
}
#[test]
fn named_and_wildcard_authorization_keep_the_same_charge_as_displaced_history_grows() {
    for reopen in [false, true] {
        assert_eq!(
            history_authorization_charge(2, reopen),
            history_authorization_charge(4, reopen)
        );
    }
}
#[test]
fn named_and_wildcard_probe_counts_stay_fixed_as_distinct_erased_names_grow() {
    for names in [16, 64] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("names.db");
        let db = seed(&path);
        for index in 0..names {
            // Long, legal names cross a native leaf boundary at these sizes.
            let table = format!("erased_{index:03}_{}", "x".repeat(96));
            db.execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
                &HashMap::new(),
            )
            .unwrap();
            db.execute(&format!("INSERT INTO {table} VALUES (1)"), &HashMap::new())
                .unwrap();
            db.execute(&format!("PURGE FROM {table} WHERE id = 1"), &HashMap::new())
                .unwrap();
        }
        measured_authorization(&db, &["items", "unrelated"]);
        measured_authorization(&db, &["*"]);
        db.close().unwrap();
        drop(db);
        let db = Database::open(&path).unwrap();
        measured_authorization(&db, &["items", "unrelated"]);
        measured_authorization(&db, &["*"]);
        db.close().unwrap();
    }
}
#[test]
fn authorization_uses_the_request_reader_and_refunds_a_refused_probe() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reader-memory.db");
    let db = seed(&path);
    let memory = Meter::new(0);
    let scope = scope(&db, &["unrelated"], memory.clone());
    let result = db.authorize_authoritative_read_publication(&scope);
    assert!(matches!(
        result,
        Err(Error::MemoryBudgetExceeded {
            budget_limit_bytes: 0,
            ..
        })
    ));
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
    drop(scope);
    measured_authorization(&db, &["unrelated"]);
    db.close().unwrap();
}

#[test]
fn an_aborted_multi_table_discard_keeps_rows_and_all_frontiers_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("abort.db");
    let db = seed(&path);
    arm_authoritative_purge_point_remove_persistence_failure_for_test();
    let failed = db.execute("DISCARD FROM items, unrelated", &HashMap::new());
    assert!(failed.is_err(), "the actual precommit failure is reached");
    assert!(
        !take_authoritative_purge_point_remove_persistence_failure_for_test(),
        "the one-shot precommit fault was consumed"
    );
    db.close().unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    for table in ["items", "unrelated"] {
        assert_eq!(
            db.execute(
                &format!("SELECT id FROM {table} ORDER BY id"),
                &HashMap::new()
            )
            .unwrap()
            .rows,
            vec![
                vec![contextdb_core::Value::Int64(1)],
                vec![contextdb_core::Value::Int64(2)]
            ]
        );
    }
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    assert_eq!(frontier(&persistence, GLOBAL), Some(Lsn(0)));
    assert_eq!(frontier(&persistence, b"items"), None);
    assert_eq!(frontier(&persistence, b"unrelated"), None);
}

// A fresh process is required here because the contract is recovery after an
// actual process exit on either side of Redb's durability boundary.
const CRASH_PATH: &str = "CONTEXTDB_ERASURE_CRASH_PATH";
const CRASH_CUT: &str = "CONTEXTDB_ERASURE_CRASH_CUT";
#[test]
fn erasure_crash_child() {
    let Some(path) = std::env::var_os(CRASH_PATH) else {
        return;
    };
    let cut = std::env::var(CRASH_CUT).unwrap();
    let db = Database::open(PathBuf::from(path)).unwrap();
    db.set_maintenance_policy(crate::database::MaintenancePolicy::CallerDriven);
    crate::writer_commit_test_seams::observe(
        Arc::new(move |boundary| {
            use crate::writer_commit_test_seams::CommitBoundary;
            let selected = if cut == "before" {
                CommitBoundary::BeforeRedb
            } else {
                CommitBoundary::AfterRedb
            };
            if boundary == selected {
                std::process::exit(0);
            }
        }),
        || {
            db.execute("DISCARD FROM items, unrelated", &HashMap::new())
                .unwrap();
            panic!("the requested durable boundary was not observed");
        },
    );
}
fn crashed_discard_is_atomic(committed: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("crash.db");
    let db = seed(&path);
    let before = db.current_lsn();
    db.close().unwrap();
    drop(db);
    let module = module_path!().split_once("::").unwrap().1;
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{module}::erasure_crash_child"),
            "--nocapture",
        ])
        .env(CRASH_PATH, &path)
        .env(CRASH_CUT, if committed { "after" } else { "before" })
        .status()
        .unwrap();
    assert!(status.success());
    let db = Database::open(&path).unwrap();
    let recovered_lsn = db.current_lsn();
    for table in ["items", "unrelated"] {
        let rows = db
            .execute(
                &format!("SELECT id FROM {table} ORDER BY id"),
                &HashMap::new(),
            )
            .unwrap()
            .rows;
        assert_eq!(
            rows,
            if committed {
                Vec::new()
            } else {
                vec![
                    vec![contextdb_core::Value::Int64(1)],
                    vec![contextdb_core::Value::Int64(2)],
                ]
            }
        );
    }
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    let global = frontier(&persistence, GLOBAL).unwrap();
    if committed {
        assert!(global > before && global <= recovered_lsn);
        assert_eq!(frontier(&persistence, b"items"), Some(global));
        assert_eq!(frontier(&persistence, b"unrelated"), Some(global));
    } else {
        assert_eq!(global, Lsn(0));
        assert_eq!(frontier(&persistence, b"items"), None);
        assert_eq!(frontier(&persistence, b"unrelated"), None);
    }
}
#[test]
fn a_process_exit_before_erasure_durability_recovers_rows_and_frontiers_together() {
    crashed_discard_is_atomic(false);
}
#[test]
fn a_process_exit_after_erasure_durability_recovers_rows_and_frontiers_together() {
    crashed_discard_is_atomic(true);
}

#[test]
fn fresh_lives_and_retention_preserve_the_published_purge_frontier() {
    let now = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let clock = now.clone();
    let _clock = contextdb_core::Wallclock::test_clock_guard(move || clock.load(Ordering::SeqCst));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retention.db");
    let db = seed(&path);
    db.execute("PURGE FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let erased = db.current_lsn();
    db.execute("INSERT INTO items VALUES (1, 'fresh')", &HashMap::new())
        .unwrap();
    db.execute("DELETE FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    db.execute(
        "CREATE TABLE expiry (id INTEGER PRIMARY KEY, body TEXT) RETAIN 2 SECONDS",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO expiry VALUES (1, 'expires')", &HashMap::new())
        .unwrap();
    let captured = scope(&db, &["items"], Meter::new(usize::MAX));
    now.fetch_add(3_000, Ordering::SeqCst);
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    drop(
        db.authorize_authoritative_read_publication(&captured)
            .unwrap(),
    );
    drop(captured);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(erased));
    assert_eq!(frontier(&persistence, GLOBAL), Some(erased));
    assert_eq!(frontier(&persistence, b"expiry"), None);
}

#[test]
fn a_later_writer_cannot_bless_a_removed_named_erasure_control() {
    damaged_control_refuses(Damage::RemovedNamed, true);
}

#[test]
fn an_export_carries_a_canonical_frontier_without_reconstructible_lineage() {
    // The native control is authoritative even when no row-life record or
    // DISCARD config marker can reconstruct it. Use the same canonical copy
    // producer used by artifact installation, in a task-created closed store.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("canonical-control.db");
    let artifact = directory.path().join("canonical-artifact.db");
    let db = seed(&path);
    let local = Lsn(db.current_lsn().0.checked_add(1).unwrap());
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    persistence.copy_erasure_frontier("items", local).unwrap();
    drop(persistence);
    let db = Database::open(&path).unwrap();
    assert!(db.current_lsn() >= local);
    db.export_snapshot(&artifact).unwrap();
    db.close().unwrap();
    drop(db);
    let restored = RedbPersistence::open(&artifact).unwrap();
    assert_eq!(frontier(&restored, b"items"), Some(local));
    assert_eq!(frontier(&restored, GLOBAL), Some(local));
}

#[test]
fn replaying_displaced_purged_history_never_lowers_named_or_global_maxima() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("monotone.db");
    let db = seed(&path);
    db.execute("PURGE FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'fresh')", &HashMap::new())
        .unwrap();
    db.execute("PURGE FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let named = db.current_lsn();
    db.execute("PURGE FROM unrelated WHERE id = 1", &HashMap::new())
        .unwrap();
    let global = db.current_lsn();
    assert!(global > named);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open(&path).unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(named));
    assert_eq!(frontier(&persistence, GLOBAL), Some(global));
    let history = persistence
        .load_config_values_raw_with_prefix(Database::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX)
        .unwrap();
    assert_eq!(history.len(), 1);
    let (key, bytes) = &history[0];
    persistence
        .with_db(|native| {
            let write = RedbPersistence::begin_history_write(native)?;
            RedbPersistence::write_read_lineage_config_entry(&write, key, Some(bytes))?;
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &[],
                &mut persistence.proof_admission(),
            )?;
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    assert_eq!(frontier(&persistence, b"items"), Some(named));
    assert_eq!(frontier(&persistence, GLOBAL), Some(global));
}

#[test]
#[serial_test::serial]
fn authorization_funds_each_native_control_before_allocation_and_until_free() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native-funding.db");
    let db = seed(&path);
    let memory = Meter::new(usize::MAX);
    let captured = scope(&db, &["items"], memory.clone());
    db.close().unwrap();
    drop(db);
    // Disable the resident native page cache to isolate operation-owned
    // controls from already-owned shared cache buffers in the oracle window.
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let counter = &memory.used as *const AtomicUsize as usize;
    let (answer, requests, allocations) =
        super::content_identity::proof_allocation_oracle_tests::source_requests_for_test(
            counter,
            || persistence.validate_erasure_frontiers(&captured, memory.clone()),
        );
    answer.unwrap();
    assert!(
        allocations > 0,
        "the mapped native allocator oracle participated"
    );
    assert_eq!(
        super::content_identity::proof_allocation_oracle_tests::source_unfunded_peak_for_test().0,
        0,
        "every native control and overlap stays funded; first allocation(size={}, align={}) had credit={}, allocations={}, worst(size,live,credit,free)={:?}, requests={:?}, credits={:?}",
        requests[0].1,
        requests[0].2,
        super::content_identity::proof_allocation_oracle_tests::source_request_credit_for_test()[0],
        allocations,
        super::content_identity::proof_allocation_oracle_tests::source_unfunded_peak_for_test().1,
        requests.map(|(_, size, align)| (size, align)),
        super::content_identity::proof_allocation_oracle_tests::source_request_credit_for_test()
    );
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
}

// The shared diagnostic producer must retain its original reader invoice
// through the returned reason's physical free, not only its construction.
#[test]
#[serial_test::serial]
fn an_erasure_reason_keeps_its_reader_credit_until_the_reason_is_freed() {
    use super::content_identity::proof_allocation_oracle_tests as oracle;
    let directory = tempfile::tempdir().unwrap();
    let db = seed(&directory.path().join("reason-funding.db"));
    let memory = Meter::new(usize::MAX);
    let captured = scope(&db, &["items"], memory.clone());
    db.execute("DISCARD FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let frontier = db.current_lsn();
    let counter = &memory.used as *const AtomicUsize as usize;
    let (failure, requests, _) = oracle::source_requests_for_test(counter, || {
        captured.check_erasure_frontier("items", frontier)
    });
    let failure = failure.unwrap_err();
    let Error::ReadFailure(ref typed) = failure else {
        panic!("expected authoritative erasure");
    };
    assert_eq!(
        typed.kind(),
        contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure
    );
    let contextdb_core::read_contract::ReadFailureDetail::Reason { reason } = typed.detail() else {
        panic!("the erasure retains its table and snapshot diagnostic");
    };
    let pointer = reason.as_ptr() as usize;
    let request = requests
        .iter()
        .find(|request| request.0 == pointer)
        .copied()
        .expect("the native oracle observed the reason backing");
    oracle::watch_source_free_for_test(std::slice::from_ref(&request), counter);
    drop(failure);
    let (before, _, frees) = oracle::source_free_for_test(1)[0];
    assert_eq!(frees, 1, "the actual diagnostic backing was freed");
    assert!(
        before >= request.1,
        "the erasure reason keeps its invoice until physical free: funded={before}, allocation={}",
        request.1
    );
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
    db.close().unwrap();
}

fn native_read_scope(owner: ReadMemoryOwner) -> native_storage_admission::StorageAdmissionScope {
    native_storage_admission::StorageAdmissionScope::reader_native_owner(owner)
        .expect("the executing request owner admits its native storage scope")
}

fn begin_native_read(
    raw: &redb::Database,
    scope: &native_storage_admission::StorageAdmissionScope,
) -> redb::NativeReadView {
    scope
        .run_native(|| {
            raw.begin_native_read()
                .map_err(RedbPersistence::storage_error)
        })
        .expect("the original request owner funds its retained native view")
}

// Real native views retain the same committed root independently. This is a
// row of the existing lifecycle helper: the decisive mutant refunds the first
// payer when a shared registration grows or another reader becomes last.
fn retained_reader_lifecycle(first_finishes_first: bool, grow: bool) {
    use super::content_identity::proof_allocation_oracle_tests as oracle;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retained-readers.db");
    let db = seed(&path);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    persistence
        .with_db(|raw| {
            let a = Meter::new(usize::MAX);
            let b = Meter::new(usize::MAX);
            let count_a = &a.used as *const AtomicUsize as usize;
            let first_scope = native_read_scope(ReadMemoryOwner::from_legacy(a.clone()));
            let mut retained_first = None;
            let ((), requests, allocations) = oracle::source_requests_for_test(count_a, || {
                retained_first = Some(begin_native_read(raw, &first_scope));
            });
            drop(first_scope);
            let first = retained_first.expect("capture returns the retained view");
            let root = first.storage_snapshot_identity();
            let first_requests = &requests[..allocations];
            assert_eq!(
                allocations, 4,
                "capture retains the registry and guard backings plus one paid receipt cell for each"
            );
            assert_eq!(
                (first_requests[0].1, first_requests[0].2),
                (first_requests[2].1, first_requests[2].2),
                "the registry and guard each retain a same-layout paid receipt before their backing"
            );
            // All observed allocations are proven live by the returned view:
            // capture first retains the qualified registry receipt and backing,
            // then the guard receipt and backing. No temporary allocation is armed.
            // Arm only after the view has escaped the measurement closure.
            oracle::watch_source_free_for_test(first_requests, count_a);
            let second_scope = native_read_scope(ReadMemoryOwner::from_legacy(b.clone()));
            let second = begin_native_read(raw, &second_scope);
            drop(second_scope);
            assert_eq!(second.storage_snapshot_identity(), root);
            assert_eq!(
                raw.native_read_transaction_count_for_test(root.transaction_id),
                2
            );
            assert_eq!(
                raw.oldest_native_read_transaction_for_test(),
                Some(root.transaction_id)
            );
            let mut later = Vec::new();
            if grow {
                // Distinct transaction ids cross the registry's initial node
                // capacity while both references to the original root stay live.
                for _ in 0..16 {
                    raw.begin_write()
                        .map_err(RedbPersistence::storage_error)?
                        .commit()
                        .map_err(RedbPersistence::storage_error)?;
                    let scope = native_read_scope(ReadMemoryOwner::from_legacy(b.clone()));
                    later.push(begin_native_read(raw, &scope));
                    drop(scope);
                }
                assert_eq!(
                    raw.oldest_native_read_transaction_for_test(),
                    Some(root.transaction_id)
                );
            }
            let mut first = Some(first);
            let mut second = Some(second);
            if first_finishes_first {
                drop(first.take());
            } else {
                drop(second.take());
            }
            assert_eq!(
                raw.native_read_transaction_count_for_test(root.transaction_id),
                1
            );
            assert_eq!(
                raw.oldest_native_read_transaction_for_test(),
                Some(root.transaction_id)
            );
            let remaining = first.as_ref().or(second.as_ref()).unwrap();
            assert_eq!(remaining.storage_snapshot_identity(), root);
            assert!(
                remaining
                    .table_root_fingerprint(FRONTIERS)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            );
            drop(first);
            drop(second);
            assert_eq!(
                raw.native_read_transaction_count_for_test(root.transaction_id),
                0
            );
            assert_eq!(
                raw.oldest_native_read_transaction_for_test(),
                later
                    .first()
                    .map(|read| read.storage_snapshot_identity().transaction_id)
            );
            drop(later);
            assert_eq!(raw.oldest_native_read_transaction_for_test(), None);
            let frees = oracle::source_free_for_test(allocations);
            oracle::disarm_source_free_watch();
            assert_eq!(a.used.load(Ordering::SeqCst), 0);
            assert_eq!(b.used.load(Ordering::SeqCst), 0);
            for ((before, _, count), (_, bytes, _)) in frees
                .into_iter()
                .zip(first_requests.iter().copied())
                .take(allocations)
            {
                assert_eq!(count, 1, "each captured backing actually frees");
                assert!(
                    before >= bytes,
                    "the allocation's original payer remains funded through physical free"
                );
            }
            Ok(())
        })
        .unwrap();
}

#[test]
#[serial_test::serial]
fn shared_root_backing_is_freed_when_the_first_reader_finishes_first() {
    retained_reader_lifecycle(true, false);
}
#[test]
#[serial_test::serial]
fn shared_root_backing_is_freed_when_the_first_reader_finishes_last() {
    retained_reader_lifecycle(false, false);
}
#[test]
#[serial_test::serial]
fn registry_growth_preserves_the_floor_when_the_first_reader_finishes_first() {
    retained_reader_lifecycle(true, true);
}
#[test]
#[serial_test::serial]
fn registry_growth_preserves_the_floor_when_the_first_reader_finishes_last() {
    retained_reader_lifecycle(false, true);
}

// A separate function is warranted for the concurrent API acceptance row:
// the mapped lifecycle helper cannot catch a split id/root capture that only
// manifests while commits race capture. This is regression coverage, not a
// deterministic schedule proof; continuous tracker->page locking remains a
// source-review obligation.
#[test]
fn concurrent_native_capture_keeps_each_returned_root_stable_across_commits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("concurrent-native-capture.db");
    let db = seed(&path);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    persistence
        .with_db(|raw| {
            let start = Arc::new(std::sync::Barrier::new(3));
            std::thread::scope(|threads| {
                let writer_start = Arc::clone(&start);
                threads.spawn(move || {
                    writer_start.wait();
                    for _ in 0..64 {
                        raw.begin_write().unwrap().commit().unwrap();
                    }
                });
                let reader_start = Arc::clone(&start);
                threads.spawn(move || {
                    let payer = Meter::new(usize::MAX);
                    reader_start.wait();
                    for _ in 0..64 {
                        let scope = native_read_scope(ReadMemoryOwner::from_legacy(payer.clone()));
                        let view = begin_native_read(raw, &scope);
                        drop(scope);
                        let identity = view.storage_snapshot_identity();
                        let frontier = view
                            .table_root_fingerprint(FRONTIERS)
                            .unwrap()
                            .expect("the captured erasure table exists");
                        std::thread::yield_now();
                        assert_eq!(view.storage_snapshot_identity(), identity);
                        assert_eq!(
                            view.table_root_fingerprint(FRONTIERS).unwrap(),
                            Some(frontier)
                        );
                        drop(view);
                    }
                    assert_eq!(payer.used.load(Ordering::SeqCst), 0);
                });
                start.wait();
            });
            assert_eq!(raw.oldest_native_read_transaction_for_test(), None);
            Ok(())
        })
        .unwrap();
}

fn cloned_erasure_reason_funding(clone_finishes_last: bool, wildcard: bool) {
    use super::content_identity::proof_allocation_oracle_tests as oracle;
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind};
    let directory = tempfile::tempdir().unwrap();
    let db = seed(&directory.path().join("cloned-reason.db"));
    let memory = Meter::new(usize::MAX);
    let table = if wildcard { "*" } else { "items" };
    let captured = scope(&db, &[table], memory.clone());
    let snapshot = db.current_lsn();
    db.execute("DISCARD FROM items WHERE id = 1", &HashMap::new())
        .unwrap();
    let frontier = db.current_lsn();
    let counter = &memory.used as *const AtomicUsize as usize;
    let (failure, requests, _) = oracle::source_requests_for_test(counter, || {
        captured.check_erasure_frontier(table, frontier)
    });
    let Error::ReadFailure(original) = failure.unwrap_err() else {
        panic!("engine must produce a typed erasure refusal");
    };
    let ReadFailureDetail::Reason { reason } = original.detail() else {
        panic!("engine must preserve scoped reason detail");
    };
    let request = requests
        .iter()
        .find(|entry| entry.0 == reason.as_ptr() as usize)
        .copied()
        .expect("observe the actual engine reason backing");
    let expected = format!(
        "authoritative erasure of table '{table}' invalidated this result's snapshot {}",
        snapshot.0
    );
    assert_eq!(reason, &expected);
    let (cloned, clone_requests, clone_allocations) =
        oracle::source_requests_for_test(counter, || original.clone());
    assert_eq!(cloned, original);
    assert_eq!(cloned.kind(), ReadFailureKind::AuthoritativeErasure);
    oracle::watch_source_free_for_test(std::slice::from_ref(&request), counter);
    drop(captured);
    db.close().unwrap();
    drop(db);
    if clone_finishes_last {
        drop(original);
        // Record before final destruction so the oracle is always disarmed,
        // including on the pre-repair tree's failing lifecycle.
        let early_frees = oracle::source_free_for_test(1)[0].2;
        assert_eq!(
            cloned.detail(),
            &ReadFailureDetail::Reason { reason: expected }
        );
        drop(cloned);
        let observed = oracle::source_free_for_test(1)[0];
        oracle::disarm_source_free_watch();
        assert_eq!(
            early_frees, 0,
            "the clone retains the actual original backing after request exit"
        );
        assert_eq!(observed.2, 1);
        assert!(
            observed.0 >= request.1,
            "original reader funds reason until physical free: {observed:?}, allocation={request:?}"
        );
    } else {
        drop(cloned);
        assert_eq!(oracle::source_free_for_test(1)[0].2, 0);
        assert_eq!(
            original.detail(),
            &ReadFailureDetail::Reason { reason: expected }
        );
        drop(original);
        let observed = oracle::source_free_for_test(1)[0];
        oracle::disarm_source_free_watch();
        assert_eq!(observed.2, 1);
        assert!(
            observed.0 >= request.1,
            "original reader funds reason until physical free: {observed:?}, allocation={request:?}"
        );
    }
    assert_eq!(
        clone_allocations, 0,
        "funded clones share reason and control backing: {clone_requests:?}"
    );
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
}

#[test]
#[serial_test::serial]
fn an_engine_erasure_clone_keeps_its_original_backing_after_the_request_exits() {
    cloned_erasure_reason_funding(true, false);
}
#[test]
#[serial_test::serial]
fn an_engine_erasure_original_keeps_its_backing_after_the_clone_exits() {
    cloned_erasure_reason_funding(false, false);
}
#[test]
#[serial_test::serial]
fn an_engine_wildcard_erasure_clone_keeps_its_backing_after_the_request_exits() {
    cloned_erasure_reason_funding(true, true);
}

#[derive(Debug)]
struct RefusingAdmission {
    meter: Arc<Meter>,
    calls: AtomicUsize,
    refuse_at: AtomicUsize,
}
impl ReadMemoryBudget for RefusingAdmission {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == self.refuse_at.load(Ordering::SeqCst) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.meter.cap,
                hint: hint.into(),
            });
        }
        self.meter.try_reserve(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.meter.release(bytes);
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == self.refuse_at.load(Ordering::SeqCst) {
            return Err(NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.meter.cap,
                hint,
            });
        }
        self.meter
            .try_reserve_native(bytes, subsystem, operation, hint)
    }
    fn native_reserve_supported(&self) -> bool {
        true
    }
}

#[test]
fn native_guard_admission_refusal_rolls_back_count_and_floor_without_stranding_funding() {
    use super::content_identity::proof_allocation_oracle_tests as oracle;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("admission-rollback.db");
    let db = seed(&path);
    let memory = Meter::new(usize::MAX);
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let previous = persistence
        .with_db(|raw| raw.begin_read().map_err(RedbPersistence::storage_error))
        .unwrap();
    let id = previous.storage_snapshot_identity().transaction_id;
    assert!(
        previous
            .table_root_fingerprint(FRONTIERS)
            .unwrap()
            .is_some(),
        "the ordinary legacy reader remains readable"
    );
    let control = Arc::new(RefusingAdmission {
        meter: memory.clone(),
        calls: AtomicUsize::new(0),
        refuse_at: AtomicUsize::new(usize::MAX),
    });
    let owner = ReadMemoryOwner::from_legacy(control.clone());
    let admission = native_read_scope(owner);
    let counter = &memory.used as *const AtomicUsize as usize;
    let (native, requests, allocations) = persistence
        .with_db(|raw| {
            let mut retained_native = None;
            let ((), requests, allocations) = oracle::source_requests_for_test(counter, || {
                retained_native = Some(begin_native_read(raw, &admission));
            });
            Ok((
                retained_native.expect("native capture escapes the allocation window"),
                requests,
                allocations,
            ))
        })
        .unwrap();
    assert_eq!(
        allocations, 4,
        "legacy qualification retains the registry and guard backings plus their paid receipt cells"
    );
    oracle::watch_source_free_for_test(&requests[..allocations], counter);
    let admissions = control.calls.load(Ordering::SeqCst);
    assert!(
        admissions > 1,
        "capture has a post-registration guard admission"
    );
    assert_eq!(
        native.storage_snapshot_identity(),
        previous.storage_snapshot_identity()
    );
    drop(native);
    drop(admission);
    let qualified_baseline = memory.used.load(Ordering::SeqCst);
    assert!(
        qualified_baseline > 0,
        "the legacy reader now retains backing qualified by the native request"
    );
    drop(previous);
    persistence
        .with_db(|raw| {
            assert_eq!(raw.native_read_transaction_count_for_test(id), 0);
            assert_eq!(raw.oldest_native_read_transaction_for_test(), None);
            Ok(())
        })
        .unwrap();
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
    let frees = oracle::source_free_for_test(allocations);
    oracle::disarm_source_free_watch();
    for ((before, _, count), (_, bytes, _)) in frees
        .into_iter()
        .zip(requests.into_iter())
        .take(allocations)
    {
        assert_eq!(count, 1, "qualified native backing is physically released");
        assert!(
            before >= bytes,
            "the qualifying request funds each retained allocation through free"
        );
    }
    // This is the dedicated registration rollback row required by the native
    // funding design. The scope's admission is call zero; every later refusal
    // occurs during native capture/guard construction after the legacy entry
    // has established the pre-existing count and floor.
    for refuse_at in 1..admissions {
        let previous = persistence
            .with_db(|raw| raw.begin_read().map_err(RedbPersistence::storage_error))
            .unwrap();
        let id = previous.storage_snapshot_identity().transaction_id;
        let refusal = Arc::new(RefusingAdmission {
            meter: memory.clone(),
            calls: AtomicUsize::new(0),
            refuse_at: AtomicUsize::new(usize::MAX),
        });
        let admission = native_read_scope(ReadMemoryOwner::from_legacy(refusal.clone()));
        refusal.refuse_at.store(refuse_at, Ordering::SeqCst);
        let result = persistence.with_db(|raw| {
            Ok(admission.run_native(|| {
                raw.begin_native_read()
                    .map_err(RedbPersistence::storage_error)
            }))
        });
        assert!(
            matches!(
                result,
                Ok(Err(
                    native_storage_admission::NativeStorageFailure::Refused(
                        NativeRefusal::MemoryBudgetExceeded {
                            available_bytes: 0,
                            budget_limit_bytes: usize::MAX,
                            ..
                        }
                    )
                ))
            ),
            "preserve original typed admission refusal at {refuse_at}: {result:?}"
        );
        assert_eq!(refusal.calls.load(Ordering::SeqCst), refuse_at + 1);
        drop(admission);
        persistence
            .with_db(|raw| {
                assert_eq!(raw.native_read_transaction_count_for_test(id), 1);
                assert_eq!(raw.oldest_native_read_transaction_for_test(), Some(id));
                Ok(())
            })
            .unwrap();
        drop(previous);
        persistence
            .with_db(|raw| {
                assert_eq!(raw.native_read_transaction_count_for_test(id), 0);
                assert_eq!(raw.oldest_native_read_transaction_for_test(), None);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            memory.used.load(Ordering::SeqCst),
            0,
            "refusal and the surviving legacy reader settle without a leaked backing"
        );
    }
}

#[test]
fn configuration_u8_records_and_summary_fingerprints_authorize_the_same_committed_root() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("configuration-key.db");
    let db = seed(&path);
    let memory = Meter::new(usize::MAX);
    let captured = scope(&db, &["items", "unrelated"], memory.clone());
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    persistence
        .with_db(|raw| {
            let read = raw.begin_read().map_err(RedbPersistence::storage_error)?;
            let roots = read
                .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            assert!(
                roots
                    .get(content_identity::LogicalFamily::Configuration as u8)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            );
            assert!(
                roots
                    .get(u8::MAX)
                    .map_err(RedbPersistence::storage_error)?
                    .is_none()
            );
            assert!(
                read.table_root_fingerprint(CONTENT_IDENTITY_ROOTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            );
            assert!(
                read.table_root_fingerprint(FRONTIERS)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            );
            assert_eq!(
                erasure_frontiers::binding_in_read(&read)?,
                content_identity::content_size_root_in_read(&read)?
            );
            Ok(())
        })
        .unwrap();
    let (result, work) = crate::writer_query_test_seams::observe(|| {
        persistence.validate_erasure_frontiers(&captured, memory.clone())
    });
    result.unwrap();
    assert_eq!(work.erasure_frontier_probes, 2);
    assert_eq!(work.history.sidecar_entries.iter().sum::<u64>(), 0);
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
}

#[test]
fn missing_and_malformed_configuration_bindings_refuse_before_scope_probes() {
    for damage in 0..2 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("configuration-damage.db");
        let db = seed(&path);
        let memory = Meter::new(usize::MAX);
        let captured = scope(&db, &["items"], memory.clone());
        db.close().unwrap();
        drop(db);
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        persistence
            .with_db(|raw| {
                let write = raw.begin_write().map_err(RedbPersistence::storage_error)?;
                {
                    let mut roots = write
                        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let key = content_identity::LogicalFamily::Configuration as u8;
                    if damage == 0 {
                        roots.remove(key).map_err(RedbPersistence::storage_error)?;
                    } else {
                        let mut bytes = roots
                            .get(key)
                            .map_err(RedbPersistence::storage_error)?
                            .unwrap()
                            .value()
                            .to_vec();
                        bytes.truncate(1);
                        roots
                            .insert(key, bytes.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                write.commit().map_err(RedbPersistence::storage_error)
            })
            .unwrap();
        let (result, work) = crate::writer_query_test_seams::observe(|| {
            persistence.validate_erasure_frontiers(&captured, memory.clone())
        });
        assert!(
            matches!(
                result,
                Err(Error::Other(_)) | Err(Error::StoreCorrupted { .. })
            ),
            "damaged Configuration control {damage} refuses: {result:?}"
        );
        assert_eq!(work.erasure_frontier_probes, 0);
        assert_eq!(memory.used.load(Ordering::SeqCst), 0);
    }
}

// Separate row because this is root corruption, not the malformed-record row:
// the decisive mutant reads the lineage-configuration root here and therefore
// accepts a changed content-size root before probing the requested scope.
#[test]
fn a_changed_configuration_content_size_root_is_precise_corruption_before_scope_probes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("configuration-content-root-damage.db");
    let db = seed(&path);
    let memory = Meter::new(usize::MAX);
    let captured = scope(&db, &["items"], memory.clone());
    db.close().unwrap();
    drop(db);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    persistence
        .with_db(|raw| {
            let write = raw.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut roots = write
                    .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                let key = content_identity::LogicalFamily::Configuration as u8;
                let mut content_size_root = roots
                    .get(key)
                    .map_err(RedbPersistence::storage_error)?
                    .expect("the u8 Configuration family has a published content-size root")
                    .value()
                    .to_vec();
                *content_size_root
                    .last_mut()
                    .expect("the published content-size root is nonempty") ^= 1;
                roots
                    .insert(key, content_size_root.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let (result, work) = crate::writer_query_test_seams::observe(|| {
        persistence.validate_erasure_frontiers(&captured, memory.clone())
    });
    assert!(
        matches!(result, Err(Error::StoreCorrupted { .. })),
        "a changed published content-size root is corruption: {result:?}"
    );
    assert_eq!(work.erasure_frontier_probes, 0);
    assert_eq!(memory.used.load(Ordering::SeqCst), 0);
}
