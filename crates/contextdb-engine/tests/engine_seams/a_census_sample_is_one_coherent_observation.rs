//! A writer memory census sample is one coherent observation, and taking it
//! from inside maintenance cannot wedge the writer.
//!
//! The census reads the writer's named owners and the writer accountant's
//! total. A maintenance step moves both: it charges construction workspace,
//! commits graph pages and returns what it no longer needs. A sample whose
//! components were read before a maintenance step and whose total was read
//! after it does not reconcile, so the engine offers one atomic sample: one
//! callback that sees the named owners and the total under the same exclusion
//! of commits and maintenance. Every such sample must reconcile exactly, at
//! every caller-driven maintenance step. A maintenance commit that emits its
//! own census line already holds the maintenance exclusion; that emission must
//! complete rather than wait on itself.
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
fn maintained_partition(path: &Path) -> contextdb_engine::Database {
    let db = open(path, Some(64 * 1024 * 1024));
    exec(
        &db,
        "CREATE TABLE building (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(128) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 32)) HISTORY ALL",
    );
    for id in 0..ROWS {
        insert_vector(&db, "building", id, Some(0), DIM);
    }
    let limit = db
        .writer_memory_ownership_receipt_for_test()
        .named_owner_total
        + WORKSPACE;
    db.set_memory_limit(Some(limit)).unwrap();
    db
}

fn atomic_sample(db: &contextdb_engine::Database) -> WriterMemoryOwnershipReceiptForTest {
    db.with_atomic_writer_memory_ownership_receipt_for_test(|receipt| receipt)
}

fn incoherence(sample: &WriterMemoryOwnershipReceiptForTest, at: &str) -> Option<String> {
    let accountant = sample.vector.accountant_total as i64;
    let named = sample.named_owner_total as i64;
    (accountant != named || sample.reconciliation_gap != 0).then(|| {
        format!(
            "{at}: writer accountant total {accountant} != named writer owners {named} \
             ({} bytes, reported gap {})",
            accountant - named,
            sample.reconciliation_gap
        )
    })
}

#[test]
fn an_atomic_census_sample_reconciles_at_every_caller_driven_maintenance_step() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let db = maintained_partition(&dir.path().join("atomic-census.db"));
    let mut failures = Vec::new();
    let mut straddling_gaps = Vec::new();
    let mut before = atomic_sample(&db);
    failures.extend(incoherence(&before, "before maintenance"));
    for step in 0..STEPS {
        db.run_maintenance_cycle()
            .unwrap_or_else(|error| panic!("caller-driven maintenance step {step}: {error:?}"));
        let after = atomic_sample(&db);
        failures.extend(incoherence(
            &after,
            &format!("after maintenance step {step}"),
        ));
        // What a sample straddling this step would have reported: the named
        // owners read before it, the accountant total read after it.
        straddling_gaps
            .push(after.vector.accountant_total as i64 - before.named_owner_total as i64);
        before = after;
    }
    assert!(
        straddling_gaps.iter().any(|gap| *gap != 0),
        "a maintenance step moves the writer accountant, so a sample that reads \
         components and total on either side of it cannot reconcile: {straddling_gaps:?}"
    );
    assert!(
        failures.is_empty(),
        "atomic census samples do not account for every writer byte:\n{}",
        failures.join("\n")
    );
    db.close().unwrap();
}

/// Child body: caller-driven maintenance whose commits emit census lines
/// from inside the maintenance exclusion.
fn emitting_maintenance(path: &Path, receipts: &Path) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = maintained_partition(path);
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
        let sample = atomic_sample(&db);
        if let Some(failure) = incoherence(&sample, &format!("after maintenance step {step}")) {
            panic!("{failure}");
        }
    }
    assert!(
        emitted_during_maintenance > 0,
        "maintenance commits emitted their own census lines"
    );
    db.close().unwrap();
}

#[test]
fn a_maintenance_commit_that_emits_its_census_line_completes_and_reconciles() {
    let dir = tempfile::tempdir().unwrap();
    let receipts = dir.path().join("writer.jsonl");
    if let Some(path) = std::env::var_os(RECEIPT_PATH) {
        emitting_maintenance(&dir.path().join("emitting.db"), Path::new(&path));
        return;
    }
    let name = "a_maintenance_commit_that_emits_its_census_line_completes_and_reconciles";
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
    // A failsafe, not a latency claim: an emission that waits on the
    // maintenance exclusion its own commit already holds never returns.
    if finished
        .recv_timeout(std::time::Duration::from_secs(300))
        .is_err()
    {
        child.kill().unwrap();
        let _ = child.wait();
        panic!("a maintenance commit emitting its census line waited on itself and never finished");
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
                "{} at LSN {}: writer accountant total {accountant} != named writer owners {named}",
                line["checkpoint"], line["lsn"]
            ));
        }
    }
    assert!(checked > 0, "census lines were emitted");
    assert!(
        failures.is_empty(),
        "emitted census lines do not account for every writer byte:\n{}",
        failures.join("\n")
    );
}
