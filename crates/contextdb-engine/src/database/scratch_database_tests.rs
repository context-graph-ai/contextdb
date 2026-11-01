//! `:memory:` is a durable scratch database in a directory of its own.

use super::*;
use crate::scratch::test_support::{captured_warnings, with_ram_backed_probe};
use std::collections::HashMap;

fn scratch_directory_of(db: &Database) -> PathBuf {
    db.persistence
        .as_ref()
        .expect("a scratch database has persistence")
        .path()
        .parent()
        .expect("the database file sits in a directory")
        .to_path_buf()
}

fn write_and_read_back(db: &Database) {
    let params = HashMap::new();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &params,
    )
    .unwrap();
    db.execute("INSERT INTO notes (id, body) VALUES (1, 'kept')", &params)
        .unwrap();
    let result = db.execute("SELECT body FROM notes", &params).unwrap();
    assert_eq!(result.rows().len(), 1);
}

#[test]
fn both_memory_doors_return_a_database_with_persistence() {
    let through_path = Database::open(MEMORY_DATABASE_PATH).unwrap();
    let through_constructor = Database::open_memory();
    for db in [&through_path, &through_constructor] {
        assert!(db.persistence.is_some());
        assert!(scratch_directory_of(db).is_dir());
        write_and_read_back(db);
    }
    assert_ne!(
        scratch_directory_of(&through_path),
        scratch_directory_of(&through_constructor),
        "every scratch database gets a directory of its own"
    );
}

#[test]
fn the_scratch_directory_is_under_the_user_data_directory_and_leaves_with_the_database() {
    let root = crate::scratch::user_scratch_root().unwrap();
    let system_temp = std::env::temp_dir();
    let db = Database::open_memory();
    write_and_read_back(&db);
    let directory = scratch_directory_of(&db);
    assert_eq!(
        directory
            .parent()
            .map(|parent| parent.canonicalize().unwrap()),
        Some(root.canonicalize().unwrap()),
        "{} is not directly under {}",
        directory.display(),
        root.display()
    );
    assert!(
        root.ends_with("contextdb/scratch"),
        "scratch root {} is not <user data dir>/contextdb/scratch",
        root.display()
    );
    assert!(
        !directory.starts_with(&system_temp) || root.starts_with(&system_temp),
        "{} must not be in the system temp dir {}",
        directory.display(),
        system_temp.display()
    );
    assert!(!directory.starts_with("/tmp") && !directory.starts_with("/dev/shm"));
    drop(db);
    assert!(
        !directory.exists(),
        "{} outlived its database",
        directory.display()
    );
}

#[test]
fn a_disk_limit_applies_to_the_scratch_file_instead_of_refusing_the_open() {
    let db = Database::open_with_options(
        MEMORY_DATABASE_PATH,
        DatabaseOpenOptions {
            disk_limit: Some(64 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    write_and_read_back(&db);
}

#[test]
fn a_ram_backed_scratch_directory_warns_once_and_still_opens() {
    fn ram(_: &Path) -> bool {
        true
    }
    fn disk(_: &Path) -> bool {
        false
    }
    let mut opened = Vec::new();
    let warned = with_ram_backed_probe(ram, || {
        captured_warnings(|| {
            let db = Database::open_memory();
            write_and_read_back(&db);
            opened.push(db);
        })
    });
    let scratch_warnings: Vec<_> = warned
        .iter()
        .filter(|line| line.contains("scratch storage"))
        .collect();
    assert_eq!(scratch_warnings.len(), 1, "{warned:?}");
    assert_eq!(opened.len(), 1);

    let quiet = with_ram_backed_probe(disk, || {
        captured_warnings(|| {
            let db = Database::open_memory();
            write_and_read_back(&db);
        })
    });
    assert!(
        quiet.iter().all(|line| !line.contains("scratch storage")),
        "{quiet:?}"
    );
}

#[test]
fn a_failed_scratch_open_leaves_no_directory_behind() {
    let mut seen = None;
    let result = Database::open_in_scratch(|file| {
        assert!(file.parent().unwrap().is_dir());
        seen = Some(file.to_path_buf());
        Err(Error::Other("refused".into()))
    });
    assert!(result.is_err());
    let file = seen.expect("the open closure ran");
    assert!(!file.parent().unwrap().exists());
}

#[test]
fn a_ddl_stage_creates_no_blob_repository_of_its_own() {
    let db = Database::open_memory();
    let before = crate::read_probe::observed().blob_repository_opens;
    for ddl in [
        "CREATE TABLE stage_probe (id INT PRIMARY KEY, name TEXT)",
        "ALTER TABLE stage_probe ADD COLUMN extra TEXT",
        "ALTER TABLE stage_probe DROP COLUMN extra",
    ] {
        db.execute(ddl, &std::collections::HashMap::new()).unwrap();
    }
    assert_eq!(
        crate::read_probe::observed().blob_repository_opens,
        before,
        "a private schema image holds no media repository, so a DDL stage makes no scratch store"
    );
}
