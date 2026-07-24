//! `contextdb-cli migrate <path>` migrates a REAL legacy-format store in
//! place (backup first).
//!
//! The fixture (`fixtures/legacy-v1.0.0-store.db`) is a genuine data root
//! built by the real v1.0.0 release binary (git tag `v1.0.0`, commit
//! `e6cf60c`), seeded with rows, a vector column, and a graph edge, then
//! copied out untouched. It is NOT synthetic corruption: opening a pristine
//! copy without migrating first refuses with the dedicated
//! `Error::LegacyVectorStoreDetected` variant, naming `contextdb migrate` as
//! the recovery command (see
//! `a_pristine_legacy_store_copy_fails_to_open_under_the_current_engine_without_migrating`
//! below).
//!
//! The root cause this fixture proved (recorded here so it is not
//! re-derived): `persistence.rs`'s top-level format-version marker has
//! stayed at the literal string `"1.0.0"` since the v1.0.0 release, so that
//! marker comparison alone never trips — the REAL incompatibility is one
//! level down, in the `TableMeta`/`ColumnDef` row-meta encoding itself.
//! bincode's struct-as-tuple encoding carries no field-count marker, so
//! those types' existing trailing-field `unwrap_or_default()` tolerance
//! (meant to read an OLDER, shorter-CURRENT-shaped payload) does not
//! detect "no more fields for me" and stop; it keeps consuming bytes that
//! belong to the NEXT `ColumnDef` (or the next `TableMeta` field), and only
//! surfaced as a raw `bincode decode error: InvalidBooleanValue(N)` once a
//! borrowed byte landed on a `bool` field — proven here even for a table
//! with no vector column at all. The fix is a genuine legacy-format reader:
//! `LegacyColumnDefV1`/`LegacyTableMetaV1` in `persistence.rs`, matching the
//! exact v1.0.0 field layout (fewer trailing fields than today), tried as a
//! fallback when the current layout's decode fails; `Database::open` refuses
//! the whole root when that fallback fires, and the new
//! `Database::open_legacy_for_migration` entry point is the only place that
//! reads past the refusal, for `contextdb migrate` to use.
//!
//! Contract this suite pins: `migrate` writes a `<path>.bak` backup BEFORE
//! mutating anything, then rewrites `<path>` in place such that it opens
//! cleanly under current HEAD with every row/vector/edge preserved; a second
//! `migrate` on the now-current-format path is a safe no-op (never destroys
//! data); running `migrate` on a path that was never legacy also
//! refuses/no-ops without touching the file.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use contextdb_core::Value;
use contextdb_engine::Database;

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-v1.0.0-store.db")
}

/// A genuine legacy-format root (built the same way as `fixture_path()` --
/// the real v1.0.0 release binary, git tag `v1.0.0`, commit `e6cf60c`) that
/// additionally carries a KEYLESS table (`events`, no primary key / natural
/// key / `id` column) with two rows, plus a keyed control table (`items`,
/// `id UUID PRIMARY KEY`) with one row. `events`' rows are omitted from the
/// changeset `migrate` replays (a changeset entry requires a natural key),
/// so this fixture is what exercises the keyless-table current-state copy
/// step through `contextdb migrate`'s real production path end to end.
fn keyless_fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-v1.0.0-keyless-store.db")
}

fn scratch_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("cdb-migrate-{label}-"))
        .tempdir()
        .expect("scratch dir")
}

fn read_bytes(path: &Path) -> Vec<u8> {
    let mut buf = Vec::new();
    std::fs::File::open(path)
        .unwrap_or_else(|err| panic!("open {}: {err}", path.display()))
        .read_to_end(&mut buf)
        .expect("read file");
    buf
}

fn run_migrate(path: &Path) -> (std::process::ExitStatus, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_contextdb"))
        .arg("migrate")
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn contextdb-cli migrate");
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

// ---------------------------------------------------------------------------
// The committed fixture, opened DIRECTLY (no migrate), is unreadable by the
// current engine — pinning the provenance claim made in the file header
// above against the real, committed binary artifact.
// ---------------------------------------------------------------------------

/// Detection is now a clean typed refusal (see the file header for the root
/// cause and the fix): opening a pristine legacy copy without migrating
/// first refuses with `Error::LegacyVectorStoreDetected`, naming `contextdb
/// migrate` as the recovery command — never a raw, unclassified decode
/// error.
#[test]
fn a_pristine_legacy_store_copy_fails_to_open_under_the_current_engine_without_migrating() {
    let dir = scratch_dir("unreadable-pin");
    let db_path = dir.path().join("legacy.db");
    std::fs::copy(fixture_path(), &db_path).expect("stage fixture copy");

    let err = Database::open(&db_path).err().unwrap_or_else(|| {
        panic!(
            "a real legacy-format store must NOT open cleanly under the current engine \
             without migrating first — if this ever succeeds, the fixture's provenance claim \
             (see the file header) no longer holds and this pin must be revisited"
        )
    });

    assert!(
        matches!(err, contextdb_core::Error::LegacyVectorStoreDetected { .. }),
        "a legacy-format root must refuse with the dedicated typed variant, not an unclassified \
         error; got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("contextdb migrate"),
        "the refusal must name the `contextdb migrate` recovery command; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Migrates a real legacy root in place, backup first, data equal.
// ---------------------------------------------------------------------------

#[test]
fn migrate_a_real_legacy_store_backs_up_and_preserves_every_row_vector_and_edge() {
    let dir = scratch_dir("happy");
    let db_path = dir.path().join("legacy.db");
    std::fs::copy(fixture_path(), &db_path).expect("stage fixture copy");
    let original_bytes = read_bytes(&db_path);
    let backup_path = dir.path().join("legacy.db.bak");

    let (status, _stdout, stderr) = run_migrate(&db_path);

    assert!(
        status.success(),
        "migrate must succeed on a real legacy root, got exit {:?}. stderr:\n{stderr}",
        status.code()
    );

    assert!(
        backup_path.exists(),
        "migrate must write a backup at {} BEFORE mutating the original — none found. stderr:\n{stderr}",
        backup_path.display()
    );
    assert_eq!(
        read_bytes(&backup_path),
        original_bytes,
        "the backup must be a byte-identical copy of the PRE-migration store"
    );

    // The migrated path must now open cleanly under CURRENT HEAD, with
    // every row this fixture was seeded with (two entities, two decisions,
    // two BASED_ON edges — see the file header for the fixture's
    // provenance) intact.
    let migrated = Database::open(&db_path).unwrap_or_else(|err| {
        panic!("the migrated store must open under current HEAD, got: {err}")
    });

    let mut entities: Vec<(String, Vec<f32>)> = migrated
        .execute("SELECT name, embedding FROM entities", &Default::default())
        .expect("select entities")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(name), Value::Vector(embedding)) => (name.clone(), embedding.clone()),
            other => panic!("unexpected entities row shape: {other:?}"),
        })
        .collect();
    entities.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        entities,
        vec![
            (
                "gadget-service".to_string(),
                vec![0.50_f32, 0.60, 0.70, 0.80]
            ),
            (
                "widget-service".to_string(),
                vec![0.10_f32, 0.20, 0.30, 0.40]
            ),
        ],
        "every entity row and its vector must survive migration unchanged"
    );

    let mut decisions: Vec<(String, String)> = migrated
        .execute(
            "SELECT statement, status FROM decisions",
            &Default::default(),
        )
        .expect("select decisions")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(statement), Value::Text(status)) => (statement.clone(), status.clone()),
            other => panic!("unexpected decisions row shape: {other:?}"),
        })
        .collect();
    decisions.sort();
    assert_eq!(
        decisions,
        vec![
            ("retire gadget-service".to_string(), "active".to_string()),
            (
                "use widget-service for auth".to_string(),
                "active".to_string()
            ),
        ],
        "every decision row must survive migration unchanged"
    );

    let mut edges: Vec<(String, String, String)> = migrated
        .execute(
            "SELECT source_id, target_id, edge_type FROM edges WHERE edge_type = 'BASED_ON'",
            &Default::default(),
        )
        .expect("select edges")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1], &row[2]) {
            (Value::Uuid(source), Value::Uuid(target), Value::Text(edge_type)) => {
                (source.to_string(), target.to_string(), edge_type.clone())
            }
            other => panic!("unexpected edges row shape: {other:?}"),
        })
        .collect();
    edges.sort();
    assert_eq!(
        edges,
        vec![
            (
                "22222222-2222-2222-2222-222222222221".to_string(),
                "11111111-1111-1111-1111-111111111111".to_string(),
                "BASED_ON".to_string(),
            ),
            (
                "22222222-2222-2222-2222-222222222222".to_string(),
                "11111111-1111-1111-1111-111111111112".to_string(),
                "BASED_ON".to_string(),
            ),
        ],
        "both BASED_ON edges seeded into the legacy fixture must survive migration with their \
         exact source, target, and edge type intact — a matching count alone would miss a \
         swapped endpoint or a mistyped edge"
    );
}

// ---------------------------------------------------------------------------
// A current-format root refuses clearly, and is left untouched.
// ---------------------------------------------------------------------------

#[test]
fn migrate_a_current_format_root_refuses_and_leaves_it_untouched() {
    let dir = scratch_dir("current-format");
    let db_path = dir.path().join("current.db");
    {
        let db = Database::open(&db_path).expect("create current-format store");
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, body TEXT)",
            &Default::default(),
        )
        .expect("create table");
        db.close().expect("close");
    }
    let before = read_bytes(&db_path);

    let (status, _stdout, stderr) = run_migrate(&db_path);

    assert!(
        !status.success(),
        "migrate on an already-current-format root must refuse, not silently succeed. stderr:\n{stderr}"
    );
    assert!(
        !stderr.trim().is_empty(),
        "a refusal must explain itself on stderr"
    );
    assert_eq!(
        read_bytes(&db_path),
        before,
        "a refused migrate must never modify the store it declined to touch"
    );
}

// ---------------------------------------------------------------------------
// Keyless tables must have their current rows preserved by migration.
// ---------------------------------------------------------------------------

/// Drives the REAL `contextdb migrate` production path (the same
/// `Command::new(env!("CARGO_BIN_EXE_contextdb"))` invocation every other
/// test in this file uses) against a genuine legacy-format store that
/// carries a keyless table with two rows plus a keyed control table with
/// one row. A changeset cannot represent a keyless-table row (it requires a
/// natural key), so `events` survives migration only through the dedicated
/// current-state copy step in `ops.rs`'s `run_migrate` -- this test proves
/// that step works when driven exactly as a real user would drive it,
/// rather than a test harness reimplementing the copy logic itself.
#[test]
fn migrate_copies_keyless_table_rows_via_the_production_path_and_receipts_them() {
    let dir = scratch_dir("keyless-production-path");
    let db_path = dir.path().join("legacy.db");
    std::fs::copy(keyless_fixture_path(), &db_path).expect("stage keyless fixture copy");

    let (status, stdout, stderr) = run_migrate(&db_path);

    assert!(
        status.success(),
        "migrate must succeed on a legacy root carrying a keyless table, got exit {:?}. \
         stdout:\n{stdout}\nstderr:\n{stderr}",
        status.code()
    );

    let migrated = Database::open(&db_path)
        .unwrap_or_else(|err| panic!("the migrated store must open under current HEAD: {err}"));

    let mut events: Vec<String> = migrated
        .execute("SELECT payload FROM events", &Default::default())
        .expect("select events")
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.clone(),
            other => panic!("unexpected events row shape: {other:?}"),
        })
        .collect();
    events.sort();
    assert_eq!(
        events,
        vec!["event-1".to_string(), "event-2".to_string()],
        "both keyless-table rows must survive migration through the real copy step -- a \
         changeset alone cannot carry them, since a keyless row has no natural key"
    );

    // The keyed control table proves ordinary changeset replay still works
    // side by side with the keyless copy step.
    let items = migrated
        .execute("SELECT label FROM items", &Default::default())
        .expect("select items");
    assert_eq!(
        items.rows.len(),
        1,
        "the keyed control table must survive migration via the ordinary changeset path"
    );

    assert!(
        stdout.contains("events"),
        "the receipt must name the keyless table by name; got stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("current-state-only"),
        "the receipt must say the keyless-table rows are current-state-only (a changeset \
         cannot carry them); got stdout:\n{stdout}"
    );
    assert!(
        stdout.contains('2'),
        "the receipt must show the keyless table's own row count (2); got stdout:\n{stdout}"
    );
}

// ---------------------------------------------------------------------------
// Idempotent second run — migrating twice never loses or duplicates data.
// ---------------------------------------------------------------------------

/// The full, comparable content of a migrated store: every entity (name +
/// embedding), every decision (statement + status), and every BASED_ON edge
/// (source + target + type) — sorted so row order never matters. Comparing
/// this snapshot before and after a second migrate proves the second run
/// changes NOTHING, not merely that the row counts stayed put (a swap, a
/// dropped vector, or a rewritten edge endpoint would all still leave the
/// counts unchanged).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StoreSnapshot {
    entities: Vec<(String, Vec<u32>)>,
    decisions: Vec<(String, String)>,
    edges: Vec<(String, String, String)>,
}

fn snapshot(db_path: &Path) -> StoreSnapshot {
    let db = Database::open(db_path).unwrap_or_else(|err| panic!("open store for snapshot: {err}"));
    let mut entities: Vec<(String, Vec<u32>)> = db
        .execute("SELECT name, embedding FROM entities", &Default::default())
        .expect("select entities")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(name), Value::Vector(embedding)) => (
                name.clone(),
                embedding.iter().map(|f| f.to_bits()).collect(),
            ),
            other => panic!("unexpected entities row shape: {other:?}"),
        })
        .collect();
    entities.sort();
    let mut decisions: Vec<(String, String)> = db
        .execute(
            "SELECT statement, status FROM decisions",
            &Default::default(),
        )
        .expect("select decisions")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Text(statement), Value::Text(status)) => (statement.clone(), status.clone()),
            other => panic!("unexpected decisions row shape: {other:?}"),
        })
        .collect();
    decisions.sort();
    let mut edges: Vec<(String, String, String)> = db
        .execute(
            "SELECT source_id, target_id, edge_type FROM edges",
            &Default::default(),
        )
        .expect("select edges")
        .rows
        .into_iter()
        .map(|row| match (&row[0], &row[1], &row[2]) {
            (Value::Uuid(source), Value::Uuid(target), Value::Text(edge_type)) => {
                (source.to_string(), target.to_string(), edge_type.clone())
            }
            other => panic!("unexpected edges row shape: {other:?}"),
        })
        .collect();
    edges.sort();
    db.close().expect("close");
    StoreSnapshot {
        entities,
        decisions,
        edges,
    }
}

#[test]
fn migrating_a_legacy_store_twice_never_changes_the_data_on_the_second_run() {
    let dir = scratch_dir("idempotent");
    let db_path = dir.path().join("legacy.db");
    std::fs::copy(fixture_path(), &db_path).expect("stage fixture copy");

    let (first_status, _out, first_err) = run_migrate(&db_path);
    assert!(
        first_status.success(),
        "the first migrate must succeed. stderr:\n{first_err}"
    );

    let after_first = snapshot(&db_path);

    // The second run must not corrupt or duplicate data, whether it
    // reports success-as-no-op or a clear refusal — see the file header for
    // why this suite does not pin one exit-code shape for "already
    // current-format" across both this case and the fresh-current-root
    // case above.
    let (_second_status, _out2, _second_err) = run_migrate(&db_path);

    let after_second = snapshot(&db_path);
    assert_eq!(
        after_second, after_first,
        "a second migrate run must be a data-preserving no-op: every entity, decision, and \
         edge must compare byte-for-byte equal to the pre-second-run snapshot, never a \
         duplicate or a loss"
    );
}
