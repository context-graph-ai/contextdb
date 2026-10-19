//! Acceptance: "Rows, searchable vectors and indexes publish atomically and recover completely
//! after crashes." Existing atomic commit/rollback tests cover successful in-process outcomes;
//! these children stop at real durability boundaries before memory publication.

use contextdb_core::{Value, Wallclock};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::writer_commit_test_seams::{CommitBoundary, observe};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy, ReadSession};
use std::{
    collections::HashMap,
    io::{BufRead, Read, Write},
    process::{Command, Stdio},
    sync::Arc,
};

const CHILD_PATH: &str = "CONTEXTDB_ATOMIC_CRASH_CHILD_PATH";
const CHILD_POINT: &str = "CONTEXTDB_ATOMIC_CRASH_CHILD_POINT";

#[test]
fn crash_cutpoints_recover_one_complete_row_index_vector_log_commit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let params = HashMap::new();
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let selected = std::env::var(CHILD_POINT).unwrap();
        let database = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let tx = database.begin_or_panic();
        database
            .execute_in_tx(
                tx,
                "INSERT INTO atomic_items VALUES (2, 42, $vector)",
                &HashMap::from([("vector".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .unwrap();
        observe(
            Arc::new(move |point| {
                if format!("{point:?}") == selected {
                    println!("ATOMIC_COMMIT_CUTPOINT {point:?}");
                    std::io::stdout().flush().unwrap();
                    let mut byte = [0];
                    std::io::stdin()
                        .read_exact(&mut byte)
                        .expect("parent kills the paused child before releasing stdin");
                }
            }),
            || database.commit(tx),
        )
        .unwrap();
        database.close().unwrap();
        println!("ATOMIC_COMMIT_POSITIVE_CONTROL");
        return;
    }
    for selected in [
        "BeforeRedb",
        "DuringRedbSync",
        "AfterRedb",
        "BeforeMemoryPublication",
        "CommittedControl",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("atomic.redb");
        let database = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        database.execute("CREATE TABLE atomic_items (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, embedding VECTOR(3) SEARCH_MODE INDEXED) HISTORY ALL", &params).unwrap();
        database
            .execute(
                "CREATE INDEX atomic_scope ON atomic_items (scope, id)",
                &params,
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO atomic_items VALUES (1, 42, $vector)",
                &HashMap::from([("vector".to_owned(), Value::Vector(vec![0.0, 1.0, 0.0]))]),
            )
            .unwrap();
        let mut ready = false;
        for _ in 0..128 {
            let report = database.run_maintenance_cycle().unwrap();
            if report.vector.ready_partitions == 1 && report.vector.remaining_partitions == 0 {
                ready = true;
                break;
            }
        }
        assert!(ready);
        let old_lsn = database.current_lsn();
        let old_snapshot = database.snapshot();
        database.close().unwrap();
        drop(database);
        let persistence = RedbPersistence::open(&path).unwrap();
        let old_logs = persistence.load_change_log().unwrap().len();
        let old_commits = persistence.load_commit_index().unwrap();
        drop(persistence);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &super::child_test_filter(
                    module_path!(),
                    "crash_cutpoints_recover_one_complete_row_index_vector_log_commit",
                ),
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_POINT, selected)
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
            if line.trim() == format!("ATOMIC_COMMIT_CUTPOINT {selected}") {
                reached = true;
                break;
            }
        }
        if selected == "CommittedControl" {
            assert!(
                child.wait().unwrap().success(),
                "committed control child succeeds: {transcript}"
            );
            assert!(
                transcript.contains("ATOMIC_COMMIT_POSITIVE_CONTROL")
                    && transcript.contains("1 passed")
            );
        } else {
            if reached {
                child.kill().unwrap();
            }
            let status = child.wait().unwrap();
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert!(
                reached,
                "child must reach the actual {selected} cutpoint, not fail during setup: {transcript}\n{stderr}"
            );
            assert!(!status.success(), "the crash child must really be killed");
        }
        let recovered = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let rows = recovered
            .execute(
                "SELECT id FROM atomic_items WHERE scope = 42 ORDER BY id",
                &params,
            )
            .unwrap()
            .rows;
        let old = vec![vec![Value::Int64(1)]];
        let new = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];
        assert!(
            rows == old || rows == new,
            "{selected} recovers exactly one complete row state: {rows:?}"
        );
        let committed = rows == new;
        if selected == "BeforeRedb" {
            assert!(!committed);
        }
        if matches!(
            selected,
            "AfterRedb" | "BeforeMemoryPublication" | "CommittedControl"
        ) {
            assert!(
                committed,
                "durably committed positive cases cannot roll back"
            );
        }
        let expected_lsn = if committed { old_lsn.0 + 1 } else { old_lsn.0 };
        assert_eq!(
            recovered.current_lsn().0,
            expected_lsn,
            "{selected} restores the complete log bound"
        );
        let recovered_snapshot = recovered.snapshot();
        assert_eq!(
            recovered_snapshot > old_snapshot,
            committed,
            "transaction bound agrees with recovered row publication"
        );
        let vector_params =
            HashMap::from([("query".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        let expected_vectors = if committed {
            vec![vec![Value::Int64(2)], vec![Value::Int64(1)]]
        } else {
            old.clone()
        };
        assert_eq!(recovered.execute("SELECT id FROM atomic_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 2", &vector_params).unwrap().rows, expected_vectors, "{selected} searchable vectors agree with recovered rows");
        recovered.close().unwrap();
        drop(recovered);
        let persistence = RedbPersistence::open(&path).unwrap();
        assert_eq!(
            persistence.load_change_log().unwrap().len(),
            old_logs + 2 * usize::from(committed),
            "{selected} recovers the complete change log"
        );
        let commits = persistence.load_commit_index().unwrap();
        assert_eq!(commits.len(), old_commits.len() + usize::from(committed));
        for (lsn, tx) in &old_commits {
            assert_eq!(commits.get(lsn), Some(tx));
        }
        assert_eq!(commits.last_key_value().unwrap().0.0, expected_lsn);
        let durable_rows = persistence.load_relational_table("atomic_items").unwrap();
        assert_eq!(durable_rows.len(), rows.len());
        for row in &durable_rows {
            assert_eq!(
                commits.get(&row.lsn),
                Some(&row.created_tx),
                "row and commit-index versions agree"
            );
        }
        let vectors = persistence.load_vectors().unwrap();
        assert_eq!(vectors.len(), rows.len());
        for vector in vectors {
            assert!(
                durable_rows.iter().any(|row| row.row_id == vector.row_id
                    && row.created_tx == vector.created_tx
                    && row.lsn == vector.lsn),
                "vector and row publication identities agree"
            );
        }
        drop(persistence);
        let file_reader = ReadSession::open(&path).unwrap();
        let indexed = file_reader
            .execute(
                "SELECT id FROM atomic_items WHERE scope = 42 ORDER BY id",
                &params,
            )
            .unwrap();
        assert_eq!(indexed.trace.index_used.as_deref(), Some("atomic_scope"));
        assert_eq!(
            indexed.rows, rows,
            "{selected} physical index postings agree with the complete row state"
        );
        // Keep all cutpoint names tied to the real seam vocabulary.
        let _ = CommitBoundary::BeforeRedb;
    }
}
