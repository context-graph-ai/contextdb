//! A reader that has finished removes the registration record it published.
//!
//! A direct reader registers its snapshot in a record, and rewrites that
//! record in place once its exact root is known. When the reader is done, the
//! record goes with it: nothing of a finished read stays registered.
//!
//! The test runs again in a child process whose platform runtime directory is
//! private to it, so it sees only what this read published.
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

const CHILD: &str = "CONTEXTDB_OWN_RECORD_CHILD";

fn records_under(directory: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "snapshot")
            {
                found.push(path);
            }
        }
    }
    found
}

#[test]
fn a_finished_reader_leaves_no_registration_record_of_its_own() {
    const TEST: &str = "a_finished_reader_leaves_no_registration_record_of_its_own";
    if std::env::var_os(CHILD).is_none() {
        let directory = tempfile::tempdir().unwrap();
        let platform = directory.path().join("platform-runtime");
        std::fs::create_dir(&platform).unwrap();
        std::fs::set_permissions(&platform, std::fs::Permissions::from_mode(0o700)).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(crate::child_test_filter(module_path!(), TEST))
            .arg("--nocapture")
            .env("XDG_RUNTIME_DIR", &platform)
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "own-record child failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let platform = PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").unwrap());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("own-record.db");
    let database = Database::open(&path).expect("create the store");
    database
        .execute(
            "CREATE TABLE records (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("declare the table");
    database
        .execute("INSERT INTO records (id) VALUES (1)", &HashMap::new())
        .expect("commit one row");
    database.close().expect("release the store");

    let session = ReadSession::open(&path).expect("a direct reader opens the store");
    assert_eq!(
        session
            .execute("SELECT id FROM records", &HashMap::new())
            .expect("the reader reads")
            .rows
            .len(),
        1
    );
    assert!(
        !records_under(&platform).is_empty(),
        "a reading session is registered while it reads"
    );
    drop(session);
    assert_eq!(
        records_under(&platform),
        Vec::<PathBuf>::new(),
        "a finished reader leaves no registration record of its own"
    );
}
