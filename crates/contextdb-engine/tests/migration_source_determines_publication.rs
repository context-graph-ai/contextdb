//! A migration publishes the store it was opened on, at that store's own name.
//!
//! The door takes the validated legacy source and nothing else, so what gets
//! published, where it lands, the name the replacement is built under, and the
//! rows a changeset cannot carry are all read out of the source itself. This
//! runs the real migration against a real legacy root and reads back what it
//! actually did.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::database::{MigrationBoundary, migration_boundary_seam};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The repository's genuine schema-level-legacy root. It is kept beside the
/// migration command's own proofs; the engine reads the same bytes rather
/// than carrying a second copy that could drift from it.
fn legacy_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../contextdb-cli/tests/fixtures/legacy-v1.0.0-store.db")
}

/// The engine's naming rule for a migration target: a sibling of the store
/// being replaced, carrying this process and a per-attempt sequence.
fn follows_the_target_naming_rule(source: &Path, candidate: &Path) -> bool {
    let Some(source_name) = source.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(name) = candidate.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let prefix = format!("{source_name}.migrate-");
    let Some(rest) = name.strip_prefix(&prefix) else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(".tmp") else {
        return false;
    };
    let Some((process, sequence)) = rest.split_once('-') else {
        return false;
    };
    process == format!("{:08x}", std::process::id())
        && sequence.len() == 16
        && sequence.chars().all(|c| c.is_ascii_hexdigit())
}

fn siblings_of(path: &Path) -> Vec<PathBuf> {
    let directory = path.parent().expect("the fixture has a parent directory");
    std::fs::read_dir(directory)
        .expect("read the migration directory")
        .map(|entry| entry.expect("read a migration directory entry").path())
        .filter(|entry| entry != path)
        .collect()
}

#[test]
fn the_validated_source_alone_decides_what_and_where_the_migration_publishes() {
    let directory = tempfile::TempDir::new().expect("task-scoped migration directory");
    let path = directory.path().join("published-store.db");
    std::fs::copy(legacy_fixture(), &path).expect("stage a genuine legacy root");
    let canonical = std::fs::canonicalize(&path).expect("canonicalize the staged legacy root");

    let source =
        Database::open_legacy_for_migration(&path).expect("open the validated legacy source");
    let expected_keyless = source
        .keyless_table_rows()
        .expect("read the source's own keyless-table rows");

    // Watch the one boundary at which the replacement exists under its
    // generated name, and record every sibling the door created.
    let observed_targets: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&observed_targets);
    let watched = canonical.clone();
    migration_boundary_seam::install_boundary_observer_for_test(Arc::new(
        move |boundary: MigrationBoundary| {
            if boundary == MigrationBoundary::TemporaryStoreOpened {
                recorder
                    .lock()
                    .expect("migration target observation lock")
                    .extend(siblings_of(&watched));
            }
        },
    ));
    let migrated = source.migrate_in_place();
    migration_boundary_seam::clear_boundary_observer_for_test();
    let receipt = migrated.expect("the door migrates the source it was opened on");

    // Where: the store that was opened is the store that was published.
    assert!(
        canonical.exists(),
        "the source pathname still holds a store"
    );
    let published = Database::open(&canonical).expect("the published store opens current-format");

    // What: the rows the source held came across, and the keyless tables the
    // source reported are exactly the ones the receipt accounts for.
    assert!(
        receipt.applied_rows > 0,
        "the migration replayed the source's rows"
    );
    let expected_keyless_rows: u64 = expected_keyless
        .values()
        .map(|table| u64::try_from(table.rows.len()).expect("keyless row count fits this platform"))
        .sum();
    assert_eq!(receipt.keyless_rows_copied, expected_keyless_rows);
    let mut receipted: Vec<(String, u64)> = receipt.keyless_table_receipts.clone();
    receipted.sort();
    let mut expected: Vec<(String, u64)> = expected_keyless
        .iter()
        .map(|(name, table)| {
            (
                name.clone(),
                u64::try_from(table.rows.len()).expect("keyless row count fits this platform"),
            )
        })
        .collect();
    expected.sort();
    assert_eq!(receipted, expected);
    for (table, rows) in &expected {
        let read_back = published
            .execute(&format!("SELECT * FROM {table}"), &HashMap::new())
            .expect("read a migrated keyless table");
        assert_eq!(
            u64::try_from(read_back.rows.len()).expect("row count fits this platform"),
            *rows
        );
    }
    published.close().expect("close the published store");

    // The name: the replacement was built at a sibling target the engine
    // named, and nothing the door generated was left behind.
    let observed = observed_targets
        .lock()
        .expect("migration target observation lock")
        .clone();
    let targets: Vec<&PathBuf> = observed
        .iter()
        .filter(|candidate| follows_the_target_naming_rule(&canonical, candidate))
        .collect();
    assert_eq!(
        targets.len(),
        1,
        "the door builds its replacement at exactly one engine-named sibling: {observed:?}"
    );
    let residue: Vec<PathBuf> = siblings_of(&canonical)
        .into_iter()
        .filter(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(".migrate-"))
        })
        .collect();
    assert!(
        residue.is_empty(),
        "a published migration leaves no generated artifact behind: {residue:?}"
    );
}

#[test]
fn a_current_format_root_is_not_a_migration_source_at_all() {
    let directory = tempfile::TempDir::new().expect("task-scoped current-format directory");
    let path = directory.path().join("current.db");
    let db = Database::open(&path).expect("create a current-format store");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("seed the current-format store");
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(1))),
            ("body".to_owned(), Value::Text("kept".to_owned())),
        ]),
    )
    .expect("seed a current-format row");
    db.close().expect("close the current-format store");

    assert!(
        Database::open_legacy_for_migration(&path).is_err(),
        "a current-format root is refused before any door exists to call"
    );
}
