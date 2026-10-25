//! Every writer memory census sample taken between caller-driven maintenance
//! steps names every byte the writer is charged for, and a maintenance commit
//! that emits its own census line finishes.
//!
//! These are accounting guarantees, kept apart from sample atomicity: each
//! sample here is taken when no maintenance step is running, so a mismatch is
//! a writer byte with no named owner, never a torn read. The census reads the
//! writer accountant's total and the named writer owners; at every such
//! sample they must be equal. Maintenance must really move the accountant
//! between samples, or the guard would only re-read one state.
//!
//! A maintenance commit (retention stamping expired rows) emits its census
//! line while that cycle still holds the maintenance exclusion. The run must
//! finish and every line it emitted must reconcile.
use crate::resident_history_support::{exec, insert_vector, open};
use contextdb_core::Wallclock;
use contextdb_engine::database::WriterMemoryOwnershipReceiptForTest;
use std::path::Path;

const RECEIPT_PATH: &str = "CONTEXTDB_TEST_SEAMS_RECEIPT_PATH";
const ROWS: usize = 1024;
const DIM: usize = 128;
const WORKSPACE: usize = 256 * 1024;
const STEPS: usize = 8;

/// The maintenance journey's partition, with one admitted construction batch
/// of workspace so each maintenance cycle is one finite step.
pub(crate) fn maintained_partition(path: &Path) -> contextdb_engine::Database {
    let db = partition(path);
    let limit = db
        .writer_memory_ownership_receipt_for_test()
        .named_owner_total
        + WORKSPACE;
    db.set_memory_limit(Some(limit)).unwrap();
    db
}

/// The same partition under the store's ordinary 64 MiB limit.
fn partition(path: &Path) -> contextdb_engine::Database {
    let db = open(path, Some(64 * 1024 * 1024));
    exec(
        &db,
        "CREATE TABLE building (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(128) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 32)) HISTORY ALL",
    );
    for id in 0..ROWS {
        insert_vector(&db, "building", id, Some(0), DIM);
    }
    db
}

pub(crate) fn unnamed(sample: &WriterMemoryOwnershipReceiptForTest, at: &str) -> Option<String> {
    let accountant = sample.vector.accountant_total as i64;
    let named = sample.named_owner_total as i64;
    (accountant != named || sample.reconciliation_gap != 0).then(|| {
        format!(
            "{at}: writer accountant total {accountant} != named writer owners {named} \
             ({} unnamed bytes, reported gap {})",
            accountant - named,
            sample.reconciliation_gap
        )
    })
}

#[test]
fn every_census_sample_between_caller_driven_maintenance_steps_names_every_writer_byte() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let db = maintained_partition(&dir.path().join("step-census.db"));
    let mut samples = vec![(
        "before maintenance".to_string(),
        db.writer_memory_ownership_receipt_for_test(),
    )];
    for step in 0..STEPS {
        db.run_maintenance_cycle()
            .unwrap_or_else(|error| panic!("caller-driven maintenance step {step}: {error:?}"));
        samples.push((
            format!("after maintenance step {step}"),
            db.writer_memory_ownership_receipt_for_test(),
        ));
    }
    let totals = samples
        .iter()
        .map(|(_, sample)| sample.vector.accountant_total)
        .collect::<Vec<_>>();
    assert!(
        totals.windows(2).any(|pair| pair[0] != pair[1]),
        "maintenance moves the writer accountant between samples: {totals:?}"
    );
    let failures = samples
        .iter()
        .filter_map(|(at, sample)| unnamed(sample, at))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "census samples between maintenance steps do not name every writer byte:\n{}",
        failures.join("\n")
    );
    db.close().unwrap();
}

pub(crate) const NOW: u64 = 1_700_000_000_000;

/// Child body: caller-driven maintenance whose retention commits emit census
/// lines while the cycle holds its maintenance exclusion. The rows of a
/// retained table have expired when the cycles run, so each cycle stamps
/// them and commits.
fn emitting_maintenance(path: &Path, receipts: &Path) {
    let _clock = Wallclock::test_clock_guard(|| NOW);
    let db = partition(path);
    exec(
        &db,
        "CREATE TABLE expiring (id INT PRIMARY KEY, body TEXT NOT NULL) RETAIN 1 HOURS SYNC OFF",
    );
    for id in 0..64 {
        exec(
            &db,
            &format!("INSERT INTO expiring VALUES ({id}, 'row {id}')"),
        );
    }
    let _expired = Wallclock::test_clock_guard(|| NOW + 2 * 3_600_000);
    let lines = || {
        std::fs::read_to_string(receipts)
            .map(|text| text.lines().count())
            .unwrap_or(0)
    };
    let mut emitted_during_maintenance = 0;
    for step in 0..STEPS {
        let before = lines();
        db.run_maintenance_cycle()
            .unwrap_or_else(|error| panic!("caller-driven maintenance step {step}: {error:?}"));
        emitted_during_maintenance += lines() - before;
    }
    assert!(
        emitted_during_maintenance > 0,
        "maintenance commits emitted their own census lines"
    );
    db.close().unwrap();
}

#[test]
fn a_maintenance_commit_that_emits_its_census_line_finishes_and_names_every_writer_byte() {
    let dir = tempfile::tempdir().unwrap();
    let receipts = dir.path().join("writer.jsonl");
    if let Some(path) = std::env::var_os(RECEIPT_PATH) {
        emitting_maintenance(&dir.path().join("emitting.db"), Path::new(&path));
        return;
    }
    let name =
        "a_maintenance_commit_that_emits_its_census_line_finishes_and_names_every_writer_byte";
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(module_path!(), name),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(RECEIPT_PATH, &receipts)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("launch caller-driven maintenance with census lines enabled");
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let (done, finished) = std::sync::mpsc::channel();
    let readers = std::thread::spawn(move || {
        let read = |mut stream: Box<dyn std::io::Read + Send>| {
            let mut text = String::new();
            stream.read_to_string(&mut text).unwrap();
            text
        };
        let output = (read(Box::new(stdout)), read(Box::new(stderr)));
        let _ = done.send(());
        output
    });
    // A failsafe so a run that never returns fails here instead of holding
    // the harness; it is not a latency bar and does not name a cause.
    if finished
        .recv_timeout(std::time::Duration::from_secs(300))
        .is_err()
    {
        child.kill().unwrap();
        let _ = child.wait();
        panic!(
            "caller-driven maintenance with census emission did not finish within the 300 s failsafe"
        );
    }
    let status = child.wait().unwrap();
    let (stdout, stderr) = readers.join().unwrap();
    assert!(
        stdout.contains("running 1 test"),
        "the exact filter runs the emitting maintenance once: {stdout}"
    );
    assert!(
        status.success(),
        "emitting maintenance failed:\n{stdout}\n{stderr}"
    );
    let text = std::fs::read_to_string(&receipts).expect("census lines were written");
    let mut failures = Vec::new();
    let mut checked = 0;
    for line in text.lines() {
        let line: serde_json::Value = serde_json::from_str(line).expect("one JSON line");
        if line["kind"] != "writer_memory_ownership" {
            continue;
        }
        checked += 1;
        let receipt = &line["receipt"];
        let accountant = receipt["vector"]["accountant_total"].as_i64().unwrap();
        let named = receipt["named_owner_total"].as_i64().unwrap();
        if accountant != named || receipt["reconciliation_gap"].as_i64() != Some(0) {
            failures.push(format!(
                "{} at LSN {}: writer accountant total {accountant} != named writer owners \
                 {named} ({} unnamed bytes)",
                line["checkpoint"],
                line["lsn"],
                accountant - named
            ));
        }
    }
    assert!(checked > 0, "census lines were emitted");
    assert!(
        failures.is_empty(),
        "emitted census lines do not name every writer byte:\n{}",
        failures.join("\n")
    );
}
