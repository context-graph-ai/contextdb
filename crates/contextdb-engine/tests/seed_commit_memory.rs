//! The full-size vector seed must commit within its unchanged writer working set.
//! A fresh child isolates the existing allocator oracle from other tests. The
//! native heap peak is a necessary lower bound on whole-process memory, paired
//! with the process anonymous-memory diagnostic of the same 100k/768/2GiB cell.

#![cfg(feature = "nightly-tests")]

#[path = "support/heap_oracle.rs"]
#[allow(dead_code)] // The shared oracle also serves read-page funding proofs.
mod heap_oracle;

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::process::Command;
use uuid::Uuid;

#[test]
fn a_full_vector_seed_commits_inside_its_declared_working_memory() {
    if std::env::var_os("CONTEXTDB_SEED_COMMIT_MEMORY_CHILD").is_none() {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_full_vector_seed_commits_inside_its_declared_working_memory",
                "--nocapture",
            ])
            .env("CONTEXTDB_SEED_COMMIT_MEMORY_CHILD", "1")
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "fresh seed process failed: {}\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
        return;
    }

    let limit = 2 * 1024 * 1024 * 1024;
    let (commit, native_peak) = heap_oracle::measure(|| {
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("seed.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.set_memory_limit(Some(limit)).unwrap();
        db.execute(
            "CREATE TABLE seeded (id INT PRIMARY KEY, context_id UUID NOT NULL, \
             embedding VECTOR(768) PARTITION_KEY (context_id) MAX_PARTITIONS 4)",
            &HashMap::new(),
        )
        .unwrap();
        let tx = db.begin().unwrap();
        for id in 0..100_000 {
            db.execute_in_tx(
                tx,
                "INSERT INTO seeded VALUES ($id, $context, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("context".into(), Value::Uuid(Uuid::from_u128(7))),
                    (
                        "embedding".into(),
                        Value::Vector((0..768).map(|lane| ((id + lane) % 31) as f32).collect()),
                    ),
                ]),
            )
            .unwrap();
        }
        db.commit(tx)
    });
    assert!(
        native_peak <= limit,
        "the full seed's native heap peak {native_peak} exceeds its {limit}-byte working set; commit={commit:?}",
    );
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let rss_peak = status
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse::<usize>()
                    .ok()
            })
            .unwrap()
            * 1024;
        assert!(
            rss_peak <= limit,
            "fresh-process RSS peak {rss_peak} exceeds the declared {limit}-byte process ceiling"
        );
    }
    commit.expect("the required full-size seed commits without changing its memory ceiling");
}

#[derive(Debug)]
struct CommitReaderProbe {
    inner: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    vector_copy_bytes: std::sync::atomic::AtomicUsize,
}
impl contextdb_core::read_memory::ReadMemoryBudget for CommitReaderProbe {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        if subsystem == "vector_copy" {
            self.vector_copy_bytes
                .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.try_reserve(bytes, subsystem, operation, hint)
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<(), contextdb_core::read_memory::NativeRefusal> {
        if subsystem == "vector_copy" {
            self.vector_copy_bytes
                .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner
            .try_reserve_native(bytes, subsystem, operation, hint)
    }
    fn native_reserve_supported(&self) -> bool {
        self.inner.native_reserve_supported()
    }
    fn release(&self, bytes: usize) {
        self.inner.release(bytes);
    }
}

#[test]
fn a_seed_projection_names_its_writer_owners_without_debiting_reader_copies() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("owners.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.set_memory_limit(Some(2 * 1024 * 1024 * 1024)).unwrap();
    assert_eq!(
        db.owner_read_config().limits.limits.memory,
        16 * 1024 * 1024
    );
    let empty = HashMap::new();
    db.execute("CREATE TABLE owners (id INT PRIMARY KEY, scope UUID NOT NULL, raw VECTOR(768) PARTITION_KEY (scope), sq8 VECTOR(768) WITH (quantization = 'SQ8') PARTITION_KEY (scope), sq4 VECTOR(768) WITH (quantization = 'SQ4') PARTITION_KEY (scope))", &empty).unwrap();
    db.execute("BEGIN", &empty).unwrap();
    for id in 0..8 {
        let vector = Value::Vector(
            (0..768)
                .map(|lane| ((id + lane) % 31) as f32 / 31.0)
                .collect(),
        );
        db.execute(
            "INSERT INTO owners VALUES ($id, $scope, $raw, $sq8, $sq4)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("scope".into(), Value::Uuid(Uuid::from_u128(7))),
                ("raw".into(), vector.clone()),
                ("sq8".into(), vector.clone()),
                ("sq4".into(), vector),
            ]),
        )
        .unwrap();
    }
    let reader = Arc::new(CommitReaderProbe {
        inner: db.declared_read_memory_for_test(),
        vector_copy_bytes: AtomicUsize::new(0),
    });
    contextdb_engine::memory_accounting::watch_budget_ledger_for_test();
    db.execute_with_read_memory("COMMIT", &empty, reader.clone())
        .unwrap();
    let ledger = contextdb_engine::memory_accounting::take_budget_ledger_for_test();
    assert_eq!(
        reader.vector_copy_bytes.load(Ordering::SeqCst),
        0,
        "commit-copy funding never debits the unchanged statement reader"
    );
    for owner in [
        "writer/prepare_vector_row_directory",
        "writer/prepare_vector_containers",
        "writer/prepare_vector_schema",
        "writer/prepare_vector_binding",
        "writer/prepare_vector_insert_copies",
    ] {
        assert!(
            ledger
                .iter()
                .any(|entry| entry.what == owner && entry.bytes > 0),
            "the seed names and funds {owner} before its storage is constructed; ledger={ledger:?}"
        );
    }
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM owners", &empty)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(8)]]
    );
    db.close().unwrap();
    let reopened = Database::open(directory.path().join("owners.db")).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened
            .execute("SELECT COUNT(*) FROM owners", &empty)
            .unwrap()
            .rows,
        vec![vec![Value::Int64(8)]]
    );
}

#[test]
fn a_seed_commit_has_an_exact_writer_boundary_and_keeps_the_prior_state_on_refusal() {
    let seed = std::collections::hash_map::RandomState::new();
    let fixture = || {
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            directory.path().join("boundary.db"),
            contextdb_engine::DatabaseOpenOptions {
                memory_limit: Some(2 * 1024 * 1024 * 1024),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute("CREATE TABLE boundary (id INT PRIMARY KEY, scope UUID NOT NULL, embedding VECTOR(768) PARTITION_KEY (scope))", &HashMap::with_hasher(seed.clone())).unwrap();
        let tx = db.begin().unwrap();
        for id in 0..8 {
            let mut parameters = HashMap::with_hasher(seed.clone());
            parameters.extend([
                ("id".into(), Value::Int64(id)),
                ("scope".into(), Value::Uuid(Uuid::from_u128(7))),
                ("embedding".into(), Value::Vector(vec![0.25; 768])),
            ]);
            db.execute_in_tx(
                tx,
                "INSERT INTO boundary VALUES ($id, $scope, $embedding)",
                &parameters,
            )
            .unwrap();
        }
        db.accountant().reset_high_water_for_test();
        (directory, db, tx)
    };
    let required = redb::with_random_state_for_test(seed.clone(), || {
        let (directory, db, tx) = fixture();
        let before = db.accountant().usage().used;
        db.commit(tx).unwrap();
        let required = db.accountant().high_water_for_test() - before;
        assert!(required > 0);
        db.close().unwrap();
        drop(db);
        drop(directory);
        required
    });
    for short in [false, true] {
        redb::with_random_state_for_test(seed.clone(), || {
            let (directory, db, tx) = fixture();
            let limit = db.accountant().usage().used + required - usize::from(short);
            db.accountant().set_budget(Some(limit)).unwrap();
            let result = db.commit(tx);
            if short {
                match result {
                    Err(contextdb_core::Error::MemoryBudgetExceeded {
                        requested_bytes,
                        available_bytes,
                        budget_limit_bytes,
                        ..
                    }) => {
                        assert_eq!(budget_limit_bytes, limit);
                        assert_eq!(requested_bytes, available_bytes + 1);
                    }
                    other => {
                        panic!("one byte below the exact writer boundary must refuse typed: {other:?}")
                    }
                }
            } else {
                result.expect("the exact measured writer boundary commits");
            }
            db.accountant()
                .set_budget(Some(2 * 1024 * 1024 * 1024))
                .unwrap();
            let expected = if short { 0 } else { 8 };
            assert_eq!(
                db.execute("SELECT COUNT(*) FROM boundary", &HashMap::with_hasher(seed.clone()))
                    .unwrap()
                    .rows,
                vec![vec![Value::Int64(expected)]]
            );
            db.close().unwrap();
            drop(db);
            let reopened = Database::open(directory.path().join("boundary.db")).unwrap();
            reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            assert_eq!(
                reopened
                    .execute("SELECT COUNT(*) FROM boundary", &HashMap::with_hasher(seed.clone()))
                    .unwrap()
                    .rows,
                vec![vec![Value::Int64(expected)]]
            );
        });
    }
}
