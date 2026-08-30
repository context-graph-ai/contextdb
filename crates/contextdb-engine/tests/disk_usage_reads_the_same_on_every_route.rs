//! What a store says about its own disk usage does not depend on how it was
//! asked.
//!
//! `SHOW DISK_LIMIT` answers a limit, what is used, what is left, and the
//! startup ceiling. Over a channel it answered all four; read from the file it
//! answered the limit and nothing else -- `used` and `available` were absent,
//! as though a reader holding the very file the number measures could not know
//! how big it is. The route is an implementation detail, so the number is now
//! the same measurement of the same file either way: the length of the store
//! on disk, and the limit minus it.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::Value;
use contextdb_core::read_contract::{OwnerReadLimits, ReadLimits, ReadRoute};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, QueryResult, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const DISK_LIMIT_BYTES: i64 = 1_073_741_824;

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn seed(database: &Database) {
    for statement in [
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, body TEXT)",
        "INSERT INTO documents (id, body) VALUES (1, 'something on disk')",
        "SET DISK_LIMIT '1G'",
    ] {
        database
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("seed `{statement}`: {error}"));
    }
}

fn column(answered: &QueryResult, name: &str) -> Value {
    let position = answered
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("`{name}` is one of {:?}", answered.columns));
    answered
        .rows
        .first()
        .and_then(|row| row.get(position))
        .cloned()
        .unwrap_or_else(|| panic!("`SHOW DISK_LIMIT` answers one row: {:?}", answered.rows))
}

fn number(answered: &QueryResult, name: &str) -> i64 {
    match column(answered, name) {
        Value::Int64(value) => value,
        other => panic!("`{name}` is a number, got {other:?}"),
    }
}

#[test]
fn disk_usage_is_the_same_number_from_the_owner_and_from_the_file() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let runtime_root = secure_runtime_root(directory.path(), "disk-usage-runtime");
    let path = directory.path().join("disk-usage.db");

    let owner = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.clone()),
                limits: OwnerReadLimits {
                    limits: ReadLimits::default(),
                    concurrency: 4,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&owner);

    let over_the_channel = {
        let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
            ReadSession::open_with_options(&path, ReadSessionOptions::default())
        })
        .expect("a live owner is reachable");
        assert_eq!(session.route(), ReadRoute::Owner);
        session
            .execute("SHOW DISK_LIMIT", &HashMap::new())
            .expect("the owner answers what its store is using")
    };

    // The same store, gone idle, read as a file.
    owner.close().expect("the writer closes cleanly");
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, ReadSessionOptions::default())
    })
    .expect("the idle store opens for reading");
    assert_eq!(session.route(), ReadRoute::File);
    let from_the_file = session
        .execute("SHOW DISK_LIMIT", &HashMap::new())
        .expect("the file answers what the same store is using");

    assert_eq!(
        from_the_file.columns, over_the_channel.columns,
        "one question, one shape of answer"
    );
    assert_eq!(
        number(&from_the_file, "limit"),
        DISK_LIMIT_BYTES,
        "the limit the store declared is the limit either route reports"
    );
    assert_eq!(number(&over_the_channel, "limit"), DISK_LIMIT_BYTES);

    let used_from_the_file = number(&from_the_file, "used");
    assert!(
        used_from_the_file > 0,
        "a store with a table and a row in it occupies disk, and a reader holding that file \
         can say how much: {used_from_the_file}"
    );
    // Not merely "a number": THE number. The store is idle by now, so the
    // file cannot move under this comparison.
    let on_disk = i64::try_from(std::fs::metadata(&path).expect("stat the idle store").len())
        .expect("a store size fits an i64");
    assert_eq!(
        used_from_the_file, on_disk,
        "what the file route calls used is the length of the store file itself"
    );
    assert_eq!(
        number(&from_the_file, "available"),
        DISK_LIMIT_BYTES - used_from_the_file,
        "what is left is the limit minus what is used, the same arithmetic on both routes"
    );
    let used_over_the_channel = number(&over_the_channel, "used");
    assert_eq!(
        number(&over_the_channel, "available"),
        DISK_LIMIT_BYTES - used_over_the_channel,
    );

    // The writer closed between the two reads, which can settle the file, so
    // the two measurements are of the same file at two moments rather than the
    // same instant. What must match is that both measured the file at all.
    assert!(
        used_over_the_channel > 0,
        "the owner reports real usage too: {used_over_the_channel}"
    );

    assert_eq!(
        column(&from_the_file, "startup_ceiling"),
        column(&over_the_channel, "startup_ceiling"),
        "and the startup ceiling reads the same either way"
    );
}
