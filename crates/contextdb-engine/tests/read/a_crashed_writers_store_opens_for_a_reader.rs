//! A writer that dies leaves a store a reader on the same machine opens and
//! reads at once -- the commit the next writer's repair keeps, at the fixed
//! cost of a healthy reader's admission.
//!
//! The committed image a killed writer leaves is complete: every commit that
//! returned is there, and the one in flight is either wholly there or not at
//! all. A direct reader is entitled to that image straight away. It must not
//! wait for a writer to walk the whole file and settle it, it must not walk
//! the file itself, and it must not settle anything: reading writes nothing.
//! What it serves is exactly what the next writer's repair keeps.
//!
//! Only a writer that died on this boot, on this exact data file, leaves a
//! file a reader can trust that far; any other crashed store is refused,
//! typed, and left to a writer.
//!
//! The writer runs in a child process and dies by `abort` (a seam on the
//! writer's own store file, never a sleep):
//! - between two commits;
//! - at the sync of a commit whose writes all reached the file;
//! - before any one of a commit's writes, at every position in it;
//! - at the sync of a torn commit, whose header reached the file and whose
//!   pages did not -- a synthetic damaged root, since a process crash cannot
//!   tear a commit; the reader falls back past it exactly as repair does.
//!
//! The reader then opens the file in a process of its own with nobody owning
//! it, and a writer opened afterwards repairs it and reads the same rows.
use contextdb_core::read_contract::{ReadFailureKind, ReadRoute};
use contextdb_core::{Error, Value};
use contextdb_engine::ReadSessionOptions;
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    RawRedbReadOnlyOpenObservation, arm_crashed_capture_byte_allowance_for_test,
    arm_writer_sync_crash_for_test, arm_writer_torn_commit_crash_for_test,
    arm_writer_write_crash_for_test, crashed_capture_bytes_held_for_test,
    install_crashed_capture_hook_for_test, set_reader_sees_another_boot_for_test,
    set_reader_sees_untrusted_filesystem_for_test, try_raw_redb_read_only_open_for_test,
    writer_file_io_for_test, writer_writes_for_test,
};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;

const ISOLATED: &str = "CONTEXTDB_CRASHED_WRITER_ISOLATED";
const WRITER: &str = "CONTEXTDB_CRASHED_WRITER_ROLE";
const STORE: &str = "CONTEXTDB_CRASHED_WRITER_STORE";
const WRITE_AT: &str = "CONTEXTDB_CRASHED_WRITER_WRITE_AT";
const READER: &str = "CONTEXTDB_CRASHED_WRITER_READER";
const COMMITTED_ROWS: i64 = 3;
/// Rows the large store holds before its writer dies.
const LARGE_ROWS: i64 = 20_000;
/// The exit code of a writer child whose armed write never came: the commit
/// it armed finished, so there is no later write position to try.
const COMMIT_FINISHED: i32 = 3;

/// Run `test` again in a child process with a private runtime root, so the
/// writer it spawns and the reader it opens share nothing with other tests.
fn isolated(test: &str) -> bool {
    if std::env::var_os(ISOLATED).is_some() {
        return false;
    }
    let directory = tempfile::tempdir().unwrap();
    let platform = directory.path().join("platform-runtime");
    std::fs::create_dir(&platform).unwrap();
    std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env("XDG_RUNTIME_DIR", &platform)
        .env(ISOLATED, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

fn insert(database: &Database, id: i64) {
    database
        .execute(
            &format!("INSERT INTO crash_rows (id, payload) VALUES ({id}, 'row-{id}')"),
            &HashMap::new(),
        )
        .expect("commit one row");
}

fn declare(database: &Database) {
    database
        .execute(
            "CREATE TABLE crash_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("declare the table");
}

/// The writer child: commit the rows, then die as its role says. Returns
/// false in every process that is not the writer child.
fn writer_child() -> bool {
    let Ok(role) = std::env::var(WRITER) else {
        return false;
    };
    let store = std::path::PathBuf::from(std::env::var(STORE).unwrap());
    let database = Database::open(&store).expect("open the writer");
    declare(&database);
    if role == "large-between-commits" {
        for batch in 0..(LARGE_ROWS / 1_000) {
            let values = (0..1_000)
                .map(|offset| {
                    let id = batch * 1_000 + offset + 1;
                    format!("({id}, 'row-{id}')")
                })
                .collect::<Vec<_>>()
                .join(", ");
            database
                .execute(
                    &format!("INSERT INTO crash_rows (id, payload) VALUES {values}"),
                    &HashMap::new(),
                )
                .expect("commit a batch");
        }
        eprintln!("WRITER_COMMITTED={LARGE_ROWS}");
        std::process::abort();
    }
    for id in 1..=COMMITTED_ROWS {
        insert(&database, id);
    }
    eprintln!("WRITER_COMMITTED={COMMITTED_ROWS}");
    match role.as_str() {
        "between-commits" => std::process::abort(),
        "at-a-whole-commits-sync" => {
            arm_writer_sync_crash_for_test();
            insert(&database, COMMITTED_ROWS + 1);
            panic!("the armed sync never came: the commit was not durable");
        }
        "at-a-torn-commits-sync" => {
            arm_writer_torn_commit_crash_for_test();
            insert(&database, COMMITTED_ROWS + 1);
            panic!("the armed sync never came: the commit was not durable");
        }
        "before-a-write" => {
            let nth: u64 = std::env::var(WRITE_AT).unwrap().parse().unwrap();
            let before = writer_writes_for_test();
            arm_writer_write_crash_for_test(nth);
            insert(&database, COMMITTED_ROWS + 1);
            eprintln!("WRITER_COMMIT_WRITES={}", writer_writes_for_test() - before);
            std::process::exit(COMMIT_FINISHED);
        }
        "measure" => {
            const COMMITS: u64 = 64;
            let (syncs_before, bytes_before) = writer_file_io_for_test();
            for id in 0..COMMITS as i64 {
                insert(&database, 1_000 + id);
            }
            let (syncs_after, bytes_after) = writer_file_io_for_test();
            println!(
                "PER_ONE_ROW_COMMIT syncs={:.2} bytes={}",
                (syncs_after - syncs_before) as f64 / COMMITS as f64,
                (bytes_after - bytes_before) / COMMITS
            );
            database.close().expect("close the measured writer");
            true
        }
        other => panic!("unknown writer role {other}"),
    }
}

fn run_writer(test: &str, role: &str, store: &Path, write_at: Option<u64>) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env(WRITER, role)
        .env(STORE, store);
    if let Some(nth) = write_at {
        command.env(WRITE_AT, nth.to_string());
    }
    command.output().unwrap()
}

/// What one reader process saw on a crashed store.
#[derive(Debug, PartialEq, Eq)]
enum ReaderSaw {
    Rows(Vec<i64>),
    RequiresWriter,
    /// The read stopped while its root was chosen: the error, the reads the
    /// capture had made, and the bytes its allowance still held.
    Stopped {
        error: String,
        reads: Option<u64>,
        held: Option<usize>,
    },
}

/// The reader child: open the store with nobody owning it, under the seams
/// its role names, and print what it saw and what its admission cost.
/// Returns false in every process that is not the reader child.
fn reader_child() -> bool {
    let Ok(role) = std::env::var(READER) else {
        return false;
    };
    let store = std::path::PathBuf::from(std::env::var(STORE).unwrap());
    let mut clock = None;
    match role.as_str() {
        "another-boot" => set_reader_sees_another_boot_for_test(true),
        "untrusted-filesystem" => set_reader_sees_untrusted_filesystem_for_test(true),
        "plain" => {}
        "tight-budget" => arm_crashed_capture_byte_allowance_for_test(0),
        "deadline-at-1" => {
            let manual = std::sync::Arc::new(ManualDeadlineClock::at(0));
            let expire = std::sync::Arc::clone(&manual);
            install_crashed_capture_hook_for_test(move |checkpoint| {
                if checkpoint == 1 {
                    expire.advance_to(u64::MAX / 4);
                }
                false
            });
            clock = Some(manual);
        }
        cancel if cancel.starts_with("cancel-at-") => {
            let at: u32 = cancel["cancel-at-".len()..].parse().unwrap();
            install_crashed_capture_hook_for_test(move |checkpoint| checkpoint == at);
        }
        other => panic!("unknown reader role {other}"),
    }
    let read_before = process_bytes_read();
    let opened = match clock {
        Some(clock) => {
            ReadSession::open_with_clock_for_test(&store, ReadSessionOptions::default(), clock)
        }
        None => ReadSession::open(&store),
    };
    match opened {
        Ok(session) => {
            let read_at_open = process_bytes_read() - read_before;
            assert_eq!(session.route(), ReadRoute::File, "nobody owns the store");
            let receipt = redb::last_admission_read_receipt()
                .expect("the crashed store's root was chosen through the admission budget");
            let rows = session
                // A sample inside a read session's row ceiling: every crashed
                // store here holds its rows 1..=N, so its first rows tell
                // which commits it kept.
                .execute(
                    "SELECT id FROM crash_rows ORDER BY id LIMIT 400",
                    &HashMap::new(),
                )
                .expect("the reader reads the committed rows")
                .rows
                .into_iter()
                .map(|row| match row.as_slice() {
                    [Value::Int64(id)] => id.to_string(),
                    other => panic!("unexpected row {other:?}"),
                })
                .collect::<Vec<_>>();
            println!(
                "READER_ROWS={} READER_ADMISSION_PAGES={} READER_OPEN_BYTES={read_at_open}",
                rows.join(","),
                receipt.charged_pages
            );
        }
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::DirectReadRequiresWriter =>
        {
            println!("READER_REQUIRES_WRITER");
        }
        Err(stopped) => {
            println!(
                "READER_STOPPED reads={:?} held={:?} error={}",
                redb::last_admission_read_receipt().map(|receipt| receipt.requests),
                crashed_capture_bytes_held_for_test(),
                format!("{stopped:?}").replace(' ', "_")
            );
        }
    }
    true
}

/// Bytes this process has read through read system calls.
fn process_bytes_read() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

struct ReaderReport {
    saw: ReaderSaw,
    admission_pages: u64,
    open_bytes: u64,
}

fn run_reader(test: &str, role: &str, store: &Path) -> ReaderReport {
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(crate::child_test_filter(module_path!(), test))
        .arg("--nocapture")
        .env(READER, role)
        .env(STORE, store)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "reader child failed: {stdout} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if stdout
        .lines()
        .any(|line| line.contains("READER_REQUIRES_WRITER"))
    {
        return ReaderReport {
            saw: ReaderSaw::RequiresWriter,
            admission_pages: 0,
            open_bytes: 0,
        };
    }
    if let Some(line) = stdout.lines().find(|line| line.contains("READER_STOPPED")) {
        let field = |name: &str| {
            line.split_whitespace()
                .find_map(|part| part.strip_prefix(name))
                .unwrap()
                .to_owned()
        };
        let number = |text: String| -> Option<u64> {
            text.strip_prefix("Some(")
                .and_then(|rest| rest.strip_suffix(')'))
                .map(|value| value.parse().unwrap())
        };
        return ReaderReport {
            saw: ReaderSaw::Stopped {
                error: field("error="),
                reads: number(field("reads=")),
                held: number(field("held=")).map(|bytes| bytes as usize),
            },
            admission_pages: 0,
            open_bytes: 0,
        };
    }
    let line = stdout
        .lines()
        .find(|line| line.contains("READER_ROWS="))
        .expect("the reader reports what it saw");
    let field = |name: &str| {
        line.split_whitespace()
            .find_map(|part| part.strip_prefix(name))
            .unwrap()
            .to_owned()
    };
    let rows = field("READER_ROWS=");
    ReaderReport {
        saw: ReaderSaw::Rows(if rows.is_empty() {
            Vec::new()
        } else {
            rows.split(',').map(|id| id.parse().unwrap()).collect()
        }),
        admission_pages: field("READER_ADMISSION_PAGES=").parse().unwrap(),
        open_bytes: field("READER_OPEN_BYTES=").parse().unwrap(),
    }
}

/// The rows a writer reads after its own open repaired the store.
fn rows_after_repair(store: &Path) -> Vec<i64> {
    let database = Database::open(store).expect("a writer repairs the crashed store");
    let rows = database
        .execute("SELECT id FROM crash_rows ORDER BY id", &HashMap::new())
        .expect("the repaired store reads")
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    database.close().expect("close the repairing writer");
    rows
}

fn crash(test: &str, role: &str, store: &Path, write_at: Option<u64>) -> std::process::Output {
    let writer = run_writer(test, role, store, write_at);
    let stderr = String::from_utf8_lossy(&writer.stderr);
    assert!(
        stderr.contains("WRITER_COMMITTED="),
        "the writer commits before it dies: {stderr}"
    );
    writer
}

/// Read the crashed store as `role` says, check it changed nothing and that
/// the ordinary read-only open still sees the crash, then repair it with a
/// writer and return both what the reader saw and what repair kept.
fn read_then_repair(test: &str, role: &str, store: &Path) -> (ReaderReport, Vec<i64>) {
    let before = std::fs::read(store).expect("the dead writer's store");
    assert_eq!(
        try_raw_redb_read_only_open_for_test(store),
        RawRedbReadOnlyOpenObservation::RepairAborted,
        "every other read-only open still sees the crash: crash classification \
         keys its settle-then-repair on exactly this refusal"
    );
    let report = run_reader(test, role, store);
    assert!(
        std::fs::read(store).expect("the store after the read") == before,
        "reading repaired nothing: the store file is byte-for-byte what the writer left"
    );
    if let ReaderSaw::Rows(_) = report.saw {
        assert!(
            report.admission_pages <= redb::ADMISSION_PAGE_BUDGET,
            "a crashed store's admission stays inside a healthy reader's budget: {}",
            report.admission_pages
        );
    }
    (report, rows_after_repair(store))
}

fn a_reader_serves_what_repair_keeps(test: &str, role: &str, visible_rows: i64) {
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("crashed.db");
    let writer = crash(test, role, &store, None);
    assert!(!writer.status.success(), "the writer died");
    let (report, repaired) = read_then_repair(test, "plain", &store);
    let expected = (1..=visible_rows).collect::<Vec<_>>();
    assert_eq!(
        report.saw,
        ReaderSaw::Rows(expected.clone()),
        "every whole commit is there, and nothing of a torn one"
    );
    assert_eq!(
        repaired, expected,
        "repair keeps exactly what the reader served"
    );
}

#[test]
fn a_reader_opens_a_store_whose_writer_died_between_commits() {
    const TEST: &str = "a_reader_opens_a_store_whose_writer_died_between_commits";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    a_reader_serves_what_repair_keeps(TEST, "between-commits", COMMITTED_ROWS);
}

#[test]
fn a_reader_sees_a_commit_whose_writes_all_landed_before_its_writer_died() {
    const TEST: &str = "a_reader_sees_a_commit_whose_writes_all_landed_before_its_writer_died";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    a_reader_serves_what_repair_keeps(TEST, "at-a-whole-commits-sync", COMMITTED_ROWS + 1);
}

#[test]
fn a_reader_falls_back_past_a_torn_commit_to_the_one_before_it() {
    const TEST: &str = "a_reader_falls_back_past_a_torn_commit_to_the_one_before_it";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    a_reader_serves_what_repair_keeps(TEST, "at-a-torn-commits-sync", COMMITTED_ROWS);
}

/// A writer killed before any one of its commit's writes -- at every position
/// in that commit -- leaves a store whose reader serves exactly what the next
/// writer's repair keeps. Every page of a commit reaches the file before the
/// header naming it, so a header the reader trusts never names a missing page.
#[test]
fn a_writer_killed_at_any_write_of_a_commit_leaves_what_repair_keeps() {
    const TEST: &str = "a_writer_killed_at_any_write_of_a_commit_leaves_what_repair_keeps";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    let mut nth = 1;
    loop {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("crashed.db");
        let writer = crash(TEST, "before-a-write", &store, Some(nth));
        if writer.status.code() == Some(COMMIT_FINISHED) {
            assert!(nth > 1, "a commit makes at least one write");
            break;
        }
        assert!(
            String::from_utf8_lossy(&writer.stderr).contains("WRITER_WRITE_CRASH="),
            "the writer died at its armed write"
        );
        let (report, repaired) = read_then_repair(TEST, "plain", &store);
        match report.saw {
            ReaderSaw::Rows(rows) => assert_eq!(
                rows, repaired,
                "killed before write {nth}: the reader served what repair discards"
            ),
            ReaderSaw::RequiresWriter => {
                panic!("killed before write {nth}: a same-boot crashed store is refused")
            }
            stopped @ ReaderSaw::Stopped { .. } => {
                panic!("killed before write {nth}: an unstopped read stopped: {stopped:?}")
            }
        }
        nth += 1;
        assert!(
            nth < 10_000,
            "a one-row commit makes a bounded number of writes"
        );
    }
}

/// A crashed store's admission costs a healthy reader's fixed allowance --
/// pages charged and bytes read at open alike -- however large the store.
#[test]
fn a_crashed_stores_admission_does_not_grow_with_the_store() {
    const TEST: &str = "a_crashed_stores_admission_does_not_grow_with_the_store";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    let small_directory = tempfile::tempdir().unwrap();
    let small = small_directory.path().join("small.db");
    crash(TEST, "between-commits", &small, None);
    let large_directory = tempfile::tempdir().unwrap();
    let large = large_directory.path().join("large.db");
    crash(TEST, "large-between-commits", &large, None);
    let large_bytes = std::fs::metadata(&large).unwrap().len();
    let small_bytes = std::fs::metadata(&small).unwrap().len();
    assert!(
        large_bytes > 8 * small_bytes,
        "the large store is many times the small one: {large_bytes} vs {small_bytes}"
    );

    let (small_report, _) = read_then_repair(TEST, "plain", &small);
    let (large_report, large_rows) = read_then_repair(TEST, "plain", &large);
    assert_eq!(large_rows.len() as i64, LARGE_ROWS);
    assert_eq!(
        large_report.admission_pages, small_report.admission_pages,
        "a crashed store's admission charges the same pages at any size"
    );
    assert!(
        large_report.open_bytes < small_report.open_bytes + 256 * 1024,
        "opening a crashed store reads no more of a large one than of a small one: \
         {} vs {}",
        large_report.open_bytes,
        small_report.open_bytes
    );
}

/// A crashed store the reader cannot prove was left on this boot, on this
/// exact file, on a trusted local filesystem is refused, typed, before any
/// page beyond the header is read -- and a writer still repairs it.
#[test]
fn a_crashed_store_the_reader_cannot_vouch_for_is_left_to_a_writer() {
    const TEST: &str = "a_crashed_store_the_reader_cannot_vouch_for_is_left_to_a_writer";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    for reader in ["another-boot", "untrusted-filesystem"] {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("crashed.db");
        crash(TEST, "between-commits", &store, None);
        let (report, repaired) = read_then_repair(TEST, reader, &store);
        assert_eq!(report.saw, ReaderSaw::RequiresWriter, "{reader}: refused");
        assert_eq!(repaired, (1..=COMMITTED_ROWS).collect::<Vec<_>>());
    }

    // The same bytes under the same name, but a different file: the dead
    // writer's published run names the file it opened, not this one.
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("crashed.db");
    crash(TEST, "between-commits", &store, None);
    let substitute = directory.path().join("substitute.db");
    std::fs::copy(&store, &substitute).unwrap();
    std::fs::rename(&substitute, &store).unwrap();
    let (report, repaired) = read_then_repair(TEST, "plain", &store);
    assert_eq!(
        report.saw,
        ReaderSaw::RequiresWriter,
        "a substituted data file is not the one its companion vouches for"
    );
    assert_eq!(repaired, (1..=COMMITTED_ROWS).collect::<Vec<_>>());
}

/// Not a verdict: prints what one one-row commit hands the store file, for
/// the cost line of the change that makes every commit crash-readable.
#[test]
fn what_a_one_row_commit_writes_and_syncs() {
    const TEST: &str = "what_a_one_row_commit_writes_and_syncs";
    if writer_child() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("measured.db");
    let writer = run_writer(TEST, "measure", &store, None);
    let stdout = String::from_utf8_lossy(&writer.stdout);
    assert!(
        writer.status.success(),
        "{stdout} {}",
        String::from_utf8_lossy(&writer.stderr)
    );
    let line = stdout
        .lines()
        .find(|line| line.starts_with("PER_ONE_ROW_COMMIT"))
        .expect("the measured writer reports its per-commit cost");
    eprintln!("{line}");
    assert!(
        line.contains("syncs=1.00 "),
        "a one-row commit syncs the store file exactly once: {line}"
    );
}

/// Choosing a crashed store's root is charged to the reader and stoppable:
/// each header and root read is admitted against the reader's own budget
/// before it is made, and a cancellation or an expired deadline at any of
/// those points stops the read right there -- typed, with no read after it,
/// no other slot tried, every charge given back, and the store untouched for
/// the next writer to repair.
#[test]
fn a_crashed_stores_root_capture_is_charged_and_stoppable() {
    const TEST: &str = "a_crashed_stores_root_capture_is_charged_and_stoppable";
    if writer_child() || reader_child() || isolated(TEST) {
        return;
    }
    // Read requests made before each stop: the opening header (1), the header
    // read again (1), then each root page as a prefix window and the page (2).
    for (reader, expect_error, expect_reads) in [
        ("cancel-at-0", "ReadCancelled", Some(1)),
        ("cancel-at-1", "ReadCancelled", Some(2)),
        ("cancel-at-2", "ReadCancelled", Some(4)),
        ("deadline-at-1", "Active", Some(2)),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("crashed.db");
        crash(TEST, "between-commits", &store, None);
        let (report, repaired) = read_then_repair(TEST, reader, &store);
        let ReaderSaw::Stopped { error, reads, .. } = report.saw else {
            panic!("{reader}: the read stops, not {:?}", report.saw);
        };
        assert!(
            error.contains(expect_error),
            "{reader}: stopped typed, not {error}"
        );
        assert_eq!(
            reads, expect_reads,
            "{reader}: nothing is read after the stop"
        );
        assert_eq!(repaired, (1..=COMMITTED_ROWS).collect::<Vec<_>>());
    }

    let directory = tempfile::tempdir().unwrap();
    let store = directory.path().join("crashed.db");
    crash(TEST, "between-commits", &store, None);
    let (report, repaired) = read_then_repair(TEST, "tight-budget", &store);
    let ReaderSaw::Stopped { error, reads, held } = report.saw else {
        panic!(
            "a root capture the reader cannot fund stops, not {:?}",
            report.saw
        );
    };
    assert!(
        error.contains("MemoryBudgetExceeded"),
        "refused by memory, typed, not {error}"
    );
    assert!(
        reads.is_some_and(|reads| reads <= 3),
        "no other slot is tried after a refused read: {reads:?}"
    );
    assert_eq!(held, Some(0), "every charge the capture took is given back");
    assert_eq!(repaired, (1..=COMMITTED_ROWS).collect::<Vec<_>>());
}
