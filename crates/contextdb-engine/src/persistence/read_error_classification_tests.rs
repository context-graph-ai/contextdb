//! One transient read error stays what it is. It is never reported as a corrupt
//! store with reset advice (CDB-207), and it never brings the process down
//! (CDB-208). Each case fails a few measured read positions, not every read:
//! the positions are counted on a dry run of the same operation. The abort
//! case runs in a child process of this test binary, since an abort cannot be
//! caught in-process.
use crate::Database;
use crate::persistence::read_persistence_test_scaffold::fail_nth_writer_read_for_test;
use contextdb_core::{Error, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const CHILD: &str = "CONTEXTDB_EIO_CHILD";

fn params(id: i64, body: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Int64(id)),
        ("body".to_owned(), Value::Text(body.to_owned())),
    ])
}

/// A small closed store with committed rows, so reads reach real pages.
fn fixture(dir: &Path) -> PathBuf {
    let path = dir.join("base.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let body = "w".repeat(1024);
    for start in (0..200i64).step_by(50) {
        let tx = db.begin().unwrap();
        for id in start..start + 50 {
            db.execute_in_tx(tx, "INSERT INTO t VALUES ($id, $body)", &params(id, &body))
                .unwrap();
        }
        db.commit(tx).unwrap();
    }
    db.close().unwrap();
    path
}

fn copy_store(base: &Path, to: &Path) {
    for entry in std::fs::read_dir(base.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix("base.db") {
            std::fs::copy(entry.path(), format!("{}{rest}", to.display())).unwrap();
        }
    }
}

fn says_corrupt(error: &Error) -> bool {
    matches!(error, Error::StoreCorrupted { .. }) || format!("{error}").contains("reset")
}

/// The store is intact afterwards: it opens, and every seeded row is there.
fn assert_intact(path: &Path, why: &str) {
    let db = Database::open(path).unwrap_or_else(|error| panic!("{why}: reopen: {error:?}"));
    let rows = db
        .execute("SELECT id FROM t WHERE id < 200", &HashMap::new())
        .unwrap_or_else(|error| panic!("{why}: read back: {error:?}"))
        .rows()
        .len();
    assert_eq!(rows, 200, "{why}: committed rows survive");
    db.close().unwrap();
}

/// Open, add one row, close: the shape of a context-graph import's reopen.
fn open_and_commit(path: &Path) -> contextdb_core::Result<()> {
    let db = Database::open(path)?;
    db.execute("INSERT INTO t VALUES ($id, $body)", &params(10_000, "new"))?;
    db.close()
}

#[test]
fn one_read_error_while_opening_and_committing_is_an_io_error_not_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let dry = dir.path().join("dry.db");
    copy_store(&base, &dry);
    let (opened, open_reads) = fail_nth_writer_read_for_test(0, || Database::open(&dry));
    opened.unwrap().close().unwrap();
    // The first reads of open, where the cg import was told to reset, and the
    // first reads after open, inside the commit.
    for nth in [1, 2, 3, open_reads + 1, open_reads + 2] {
        let path = dir.path().join(format!("case{nth}.db"));
        copy_store(&base, &path);
        let (outcome, reads) = fail_nth_writer_read_for_test(nth, || open_and_commit(&path));
        assert!(reads >= nth, "read {nth} was reached ({reads} reads)");
        let error = outcome.expect_err("the failed read is reported");
        assert!(
            !says_corrupt(&error),
            "read {nth} of {reads}: one read error must not say corrupt or advise reset: {error:?}"
        );
        assert_intact(&path, &format!("after a failed read {nth}"));
    }
}

/// A larger commit, then closing the handle: the shape that aborted cg.
fn larger_commit_and_close(path: &Path) -> contextdb_core::Result<()> {
    let db = Database::open(path)?;
    db.execute("DELETE FROM t WHERE id >= 100", &HashMap::new())?;
    let body = "n".repeat(1024);
    let tx = db.begin()?;
    for id in 1_000..1_200i64 {
        db.execute_in_tx(tx, "INSERT INTO t VALUES ($id, $body)", &params(id, &body))?;
    }
    db.commit(tx)?;
    drop(db);
    Ok(())
}

#[test]
fn one_read_error_inside_a_larger_commit_or_its_close_never_brings_the_process_down() {
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "persistence::read_error_classification_tests::one_read_error_inside_a_larger_commit_or_its_close_never_brings_the_process_down",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&output.stderr);
        let tail: Vec<_> = text.lines().rev().take(12).collect();
        assert!(
            output.status.success(),
            "the child did not exit cleanly ({:?}); last output:\n{}",
            output.status,
            tail.join("\n")
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let base = fixture(dir.path());
    let dry = dir.path().join("dry.db");
    copy_store(&base, &dry);
    let (done, total) = fail_nth_writer_read_for_test(0, || larger_commit_and_close(&dry));
    done.unwrap();
    // The last reads of the commit and its close, where the durable commit has
    // happened and only in-memory publication remains, and the middle.
    let mut panics = Vec::new();
    for nth in [total, total - 1, total - 2, total / 2] {
        let path = dir.path().join(format!("case{nth}.db"));
        copy_store(&base, &path);
        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fail_nth_writer_read_for_test(nth, || larger_commit_and_close(&path))
        }));
        if let Err(payload) = run {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                .unwrap_or_default();
            eprintln!("read {nth} of {total}: PANICKED: {message}");
            panics.push((nth, message));
        }
        let reopened = Database::open(&path);
        assert!(
            reopened.is_ok(),
            "read {nth}: the store reopens: {:?}",
            reopened.err()
        );
    }
    assert!(
        panics.is_empty(),
        "{} of the measured read positions panicked instead of returning an error: {panics:?}",
        panics.len()
    );
}
