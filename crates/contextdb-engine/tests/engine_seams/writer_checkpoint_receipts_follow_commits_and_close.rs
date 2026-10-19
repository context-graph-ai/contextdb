//! Writer checkpoint evidence follows real publication and remains optional.
use crate::resident_history_support::{exec, open};
use contextdb_core::{Value, Wallclock};
use serde_json::Value as Json;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

const CHILD: &str = "CONTEXTDB_CHECKPOINT_RECEIPT_CHILD";
const RECEIPT: &str = "CONTEXTDB_TEST_SEAMS_RECEIPT_PATH";
const NOW: u64 = 1_800_000_000_000;

#[test]
fn checkpoint_receipt_child() {
    let Some(directory) = std::env::var_os(CHILD) else {
        return;
    };
    let directory = Path::new(&directory);
    let _clock = Wallclock::test_clock_guard(|| NOW);
    let db = open(&directory.join("writer.db"), Some(64 * 1024 * 1024));
    exec(
        &db,
        "CREATE TABLE imported (id INT PRIMARY KEY, body TEXT) HISTORY CURRENT ONLY SYNC OFF",
    );
    let mut commits = vec![db.current_lsn().0];
    for batch in 0..3 {
        let tx = db.begin().unwrap();
        for id in batch * 2..batch * 2 + 2 {
            db.execute_in_tx(
                tx,
                &format!("INSERT INTO imported VALUES ({id}, 'batch-{batch}')"),
                &Default::default(),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        commits.push(db.current_lsn().0);
    }
    assert!(commits.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        exec(&db, "SELECT id,body FROM imported ORDER BY id"),
        (0..6)
            .map(|id| vec![Value::Int64(id), Value::Text(format!("batch-{}", id / 2))])
            .collect::<Vec<_>>()
    );
    let final_lsn = db.current_lsn().0;
    let database_id = db.database_identity_for_test();
    db.close().unwrap();
    std::fs::write(
        directory.join("commits.json"),
        serde_json::to_vec(&serde_json::json!({
            "pid": std::process::id(), "database_id": database_id, "commits": commits, "final_lsn": final_lsn
        }))
        .unwrap(),
    )
    .unwrap();
}

fn run_child(directory: &Path, enabled: bool) -> Json {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg(crate::child_test_filter(
            module_path!(),
            "checkpoint_receipt_child",
        ))
        .args(["--exact", "--nocapture"])
        .env(CHILD, directory)
        .env_remove(RECEIPT)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if enabled {
        command.env(RECEIPT, directory.join("receipts.jsonl"));
    }
    let mut child = command.spawn().unwrap();
    fn drain(mut pipe: impl std::io::Read + Send + 'static) -> std::thread::JoinHandle<String> {
        std::thread::spawn(move || {
            let mut text = String::new();
            pipe.read_to_string(&mut text).unwrap();
            text
        })
    }
    let out = drain(child.stdout.take().unwrap());
    let err = drain(child.stderr.take().unwrap());
    let (finished, wait) = std::sync::mpsc::channel();
    let output = std::thread::spawn(move || {
        let text = out.join().unwrap() + &err.join().unwrap();
        let _ = finished.send(());
        text
    });
    let timed_out = wait.recv_timeout(Duration::from_secs(15)).is_err();
    if timed_out {
        let _ = child.kill();
    }
    let status = child.wait().unwrap();
    let output = output.join().unwrap();
    assert!(
        !timed_out && status.success(),
        "receipt child: {status}, timeout={timed_out}\n{output}"
    );
    serde_json::from_slice(&std::fs::read(directory.join("commits.json")).unwrap()).unwrap()
}

fn require_unsigned_fields(object: &Json, fields: &[&str]) {
    for field in fields {
        assert!(
            object[*field].as_u64().is_some(),
            "missing unsigned owner {field}: {object}"
        );
    }
}

fn check_owners(receipt: &Json) {
    require_unsigned_fields(
        receipt,
        &[
            "current_row_versions",
            "historical_row_versions",
            "current_row_values",
            "current_row_side_maps",
            "current_row_index_postings",
            "current_row_f32_projections",
            "historical_row_priced_bytes",
            "staged_row_versions",
            "staged_row_admissions",
            "staged_vector_admissions",
            "staged_vector_owner_controls",
            "staged_index_postings",
            "table_metadata",
            "graph_entries",
            "commit_index_entries",
            "commit_index_charge",
            "persistence_cache",
            "current_membership_charge",
            "captured_membership_charge",
            "ddl_log_entries",
            "ddl_log_estimated_bytes",
            "active_transactions",
            "pending_commit_metadata",
            "pending_schema_stages",
            "named_owner_total",
            "published_root_charge",
            "captured_root_charge",
            "commit_source_reference_charge",
            "storage_maintenance_control_charge",
        ],
    );
    let vector = &receipt["vector"];
    require_unsigned_fields(
        vector,
        &[
            "raw_vector_bodies",
            "raw_reverse_directory",
            "partition_vectors",
            "vector_partition_controls",
            "source_metadata",
            "reader_source_metadata",
            "reader_last_query_trace",
            "reader_child_controls",
            "reader_parent_total",
            "base_graph",
            "change_graph",
            "mutable_tail",
            "typed_partition_keys",
            "retired_pinned_graph_bytes",
            "temporary_workspace",
            "retained_total",
            "non_vector_total",
            "accountant_total",
        ],
    );
    let retained_fields = [
        "raw_vector_bodies",
        "raw_reverse_directory",
        "partition_vectors",
        "vector_partition_controls",
        "base_graph",
        "change_graph",
        "mutable_tail",
        "typed_partition_keys",
        "retired_pinned_graph_bytes",
        "source_metadata",
    ];
    let retained_sum = retained_fields
        .iter()
        .map(|field| vector[*field].as_u64().unwrap())
        .sum::<u64>();
    assert_eq!(retained_sum, vector["retained_total"].as_u64().unwrap());
    let reader_sum = [
        "reader_source_metadata",
        "reader_child_controls",
        "reader_last_query_trace",
    ]
    .iter()
    .map(|field| vector[*field].as_u64().unwrap())
    .sum::<u64>();
    assert_eq!(reader_sum, vector["reader_parent_total"].as_u64().unwrap());
    require_unsigned_fields(
        &receipt["relational_inventory"],
        &[
            "row_versions",
            "row_positions",
            "row_version_positions",
            "sync_provenance",
            "index_trees",
            "index_exact",
            "index_exact_filters",
            "index_membership",
            "index_descriptors",
        ],
    );
    for owner in receipt["current_rows_by_table"]
        .as_object()
        .unwrap()
        .values()
    {
        require_unsigned_fields(
            owner,
            &[
                "versions",
                "values",
                "side_maps",
                "index_postings",
                "f32_projections",
            ],
        );
    }
    for owner in receipt["current_membership_by_table"]
        .as_object()
        .unwrap()
        .values()
    {
        assert!(owner.as_u64().is_some());
    }
    for field in ["change_log_entries", "change_log_charges"] {
        let owners = receipt[field].as_array().unwrap();
        assert_eq!(owners.len(), 6, "all change-log owners must be reported");
        assert!(owners.iter().all(|owner| owner.as_u64().is_some()));
    }
    // Held commit work can produce a real nonzero gap; the receipt must report
    // that difference faithfully rather than invent charges to make it zero.
    assert_eq!(
        i128::from(receipt["reconciliation_gap"].as_i64().unwrap()),
        i128::from(vector["accountant_total"].as_u64().unwrap())
            - i128::from(receipt["named_owner_total"].as_u64().unwrap())
    );
}

#[test]
fn writer_checkpoint_receipts_follow_commits_and_close() {
    let enabled = tempfile::tempdir().unwrap();
    let manifest = run_child(enabled.path(), true);
    let text = std::fs::read_to_string(enabled.path().join("receipts.jsonl")).unwrap();
    assert!(
        text.ends_with('\n'),
        "each checkpoint is a complete JSONL record"
    );
    let lines = text
        .lines()
        .map(|line| serde_json::from_str::<Json>(line).unwrap())
        .collect::<Vec<_>>();
    let receipts = lines
        .iter()
        .filter(|line| line["kind"] == "writer_memory_ownership")
        .collect::<Vec<_>>();
    assert!(!receipts.is_empty());
    let database_id = manifest["database_id"].as_u64().unwrap();
    let mut previous = 0;
    let mut commits = Vec::new();
    let mut closed = false;
    for line in receipts {
        assert_eq!(line["pid"], manifest["pid"]);
        assert_eq!(line["database_id"].as_u64(), Some(database_id));
        assert_eq!(line["wallclock_ms"].as_u64(), Some(NOW));
        #[cfg(target_os = "linux")]
        assert!(line["rss_bytes"].as_u64().is_some_and(|rss| rss > 0));
        let lsn = line["lsn"].as_u64().unwrap();
        assert!(lsn >= previous, "checkpoint LSN moved backwards: {line}");
        previous = lsn;
        match line["checkpoint"].as_str().unwrap() {
            "commit" => {
                assert!(!closed, "commit followed close");
                commits.push(Json::from(lsn));
            }
            "close" => {
                assert!(!closed, "one explicit close checkpoint");
                assert_eq!(line["lsn"], manifest["final_lsn"]);
                closed = true;
            }
            checkpoint => panic!("unexpected checkpoint {checkpoint}"),
        }
        check_owners(&line["receipt"]);
    }
    assert!(closed, "explicit close must emit a receipt");
    assert_eq!(
        Json::Array(commits),
        manifest["commits"],
        "every actual publication, exactly once"
    );
    let absent = tempfile::tempdir().unwrap();
    let control = run_child(absent.path(), false);
    assert!(!absent.path().join("receipts.jsonl").exists());
    assert_eq!(control["commits"], manifest["commits"]);
    assert_eq!(control["final_lsn"], manifest["final_lsn"]);
}
