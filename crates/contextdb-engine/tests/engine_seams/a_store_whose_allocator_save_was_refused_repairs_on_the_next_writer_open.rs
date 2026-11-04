//! A store whose writer refused to save an allocator table (its regional
//! summary disagreed with its own words) closes with no allocator table.
//! What the engine sees on such a store:
//! - a direct read is refused with the typed "needs a writer" failure;
//! - an open that cannot take the store's claim classifies it without
//!   repair, which cannot judge it (the classification is `None`), so the
//!   claim's own refusal stands and the store is left byte-for-byte as it
//!   was, still refusing direct reads;
//! - `contextdb diagnose` (`RedbPersistence::is_legacy_format_store`) lets
//!   redb's crash repair run, as it documents for a store left without
//!   allocator state, and answers "current format, nothing to correct"
//!   (`Ok(false)`), after which direct reads open;
//! - a writable open repairs it, keeps every row and keeps its database
//!   identity.
#![cfg(unix)]

use contextdb_core::{Error, Value, read_contract::ReadFailureKind};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const ROWS: i64 = 300;

fn body(id: i64) -> String {
    format!("{id:04}-{}", "x".repeat(2000))
}

/// An engine store with enough rows that region 0's order-0 allocator
/// bitmap has a summary level, then closed and reopened as a raw redb store
/// whose summary gets one planted wrong bit; dropping the raw handle runs
/// its quick-repair commit, which must refuse to save the allocator.
/// Returns the store's database identity.
fn latched_store(path: &Path) -> u64 {
    let database = Database::open(path).expect("create store");
    database
        .execute(
            "CREATE TABLE rows (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("commit schema");
    for id in 0..ROWS {
        database
            .execute(
                &format!("INSERT INTO rows VALUES ({id}, '{}')", body(id)),
                &HashMap::new(),
            )
            .expect("commit row");
    }
    let identity = database.database_identity_for_test();
    database.close().expect("close fixture owner");

    let raw = redb::Database::open(path).expect("open physical store");
    let planted = raw.plant_allocator_summary_mismatch_for_test();
    assert!(
        planted.is_some(),
        "fixture: region 0's order-0 allocator bitmap has no summary level to plant in"
    );
    // Drop runs the quick-repair commit that saves the allocator table. If
    // that commit's own allocations healed the plant, a correct save would
    // publish an exact table, the direct read below would open, and this
    // test would fail: a healed plant can only cause a false failure, never
    // a false pass.
    drop(raw);
    identity
}

fn assert_direct_read_needs_a_writer(path: &Path, context: &str) {
    match ReadSession::open(path) {
        Err(Error::ReadFailure(failure))
            if failure.kind() == ReadFailureKind::DirectReadRequiresWriter => {}
        Err(other) => {
            panic!("{context}: a direct read was refused with the wrong error: {other:?}")
        }
        Ok(_) => panic!(
            "{context}: a direct read opened a store whose allocator save was refused; \
             the refused allocator table was published"
        ),
    }
}

fn assert_rows(database: &Database, context: &str) {
    let result = database
        .execute("SELECT id, body FROM rows ORDER BY id", &HashMap::new())
        .unwrap_or_else(|error| panic!("{context}: rows unreadable: {error:?}"));
    let rows = result.rows();
    let expected: Vec<Vec<Value>> = (0..ROWS)
        .map(|id| vec![Value::Int64(id), Value::Text(body(id))])
        .collect();
    assert!(
        rows == expected,
        "{context}: {} rows read back, expected {ROWS} unchanged rows",
        rows.len()
    );
}

/// Puts permissions back when dropped, so a failing assertion still leaves
/// the scratch directory removable.
struct PermissionRestore(Vec<(PathBuf, fs::Permissions)>);

impl PermissionRestore {
    fn for_paths(paths: &[&Path]) -> Self {
        Self(
            paths
                .iter()
                .map(|path| {
                    let permissions = fs::metadata(path).expect("read permissions").permissions();
                    ((*path).to_path_buf(), permissions)
                })
                .collect(),
        )
    }
}

impl Drop for PermissionRestore {
    fn drop(&mut self) {
        for (path, permissions) in &self.0 {
            let _ = fs::set_permissions(path, permissions.clone());
        }
    }
}

/// The two names a store's claim companion can have.
fn remove_companions(path: &Path) {
    let mut appended = path.as_os_str().to_os_string();
    appended.push(".lock");
    for companion in [PathBuf::from(appended), path.with_extension("lock")] {
        match fs::remove_file(&companion) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove {}: {error}", companion.display()),
        }
    }
}

#[test]
fn a_store_whose_allocator_save_was_refused_repairs_on_the_next_writer_open() {
    assert_ne!(
        nix::unistd::Uid::effective().as_raw(),
        0,
        "the read-only directory step needs a non-root test process"
    );
    let root = tempfile::tempdir().expect("task-scoped store");

    // Inspection that may not repair leaves the store exactly as it was.
    let directory = root.path().join("inspected");
    fs::create_dir(&directory).expect("create store directory");
    let inspected = directory.join("inspected.db");
    latched_store(&inspected);
    assert_direct_read_needs_a_writer(&inspected, "before any inspection");
    remove_companions(&inspected);
    let bytes = fs::read(&inspected).expect("read the store's bytes");
    {
        let _restore = PermissionRestore::for_paths(&[&directory, &inspected]);
        fs::set_permissions(&inspected, fs::Permissions::from_mode(0o444))
            .expect("store is read-only");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o555))
            .expect("store directory is read-only");
        match Database::open(&inspected) {
            Ok(_) => panic!("an open with no claim possible opened the store"),
            Err(Error::LegacyVectorStoreDetected { .. }) => {
                panic!("the classification without repair judged a current store legacy")
            }
            Err(_claim_refusal) => {}
        }
    }
    assert!(
        fs::read(&inspected).expect("read the store's bytes again") == bytes,
        "the classification without repair changed the store"
    );
    assert_direct_read_needs_a_writer(&inspected, "after the classification without repair");

    // `contextdb diagnose` repairs it and answers "current format".
    assert!(
        !RedbPersistence::is_legacy_format_store(&inspected).expect("diagnose repairs and answers"),
        "diagnose judged a current-format store whose allocator save was refused legacy"
    );
    let file =
        ReadSession::open(&inspected).expect("direct read after diagnose repaired the store");
    drop(file);

    // A writable open repairs it, keeps every row and its identity.
    let reopened = root.path().join("reopened.db");
    let identity = latched_store(&reopened);
    assert_direct_read_needs_a_writer(&reopened, "before the writable open");
    let recovered = Database::open(&reopened).expect("writable open repairs the store");
    assert_eq!(
        recovered.database_identity_for_test(),
        identity,
        "the repair changed the store's database identity"
    );
    assert_rows(&recovered, "after the writable open");
    recovered.close().expect("close recovered owner");
    let file = ReadSession::open(&reopened)
        .expect("direct read after the writable open repaired the store");
    drop(file);
}
