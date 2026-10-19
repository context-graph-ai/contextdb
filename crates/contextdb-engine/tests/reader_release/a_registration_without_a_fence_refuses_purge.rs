//! A reader registration whose purge-publication fence is missing is an
//! uncertain inventory, and an uncertain inventory refuses PURGE.
//!
//! Promise (contextdb-intent, authoritative erasure): a PURGE is accepted only
//! when the writer can account for every reader that might still serve the
//! erased rows. A registration with no fence cannot be told "this reader will
//! be refused after the purge", so the PURGE is refused with the typed
//! refusal; a registration that vanished entirely with its reader is not
//! uncertain.
//!
//! Test-estate reason (2026-10-15, reader/writer handoff): the bounded-result
//! publication passes results through when no fence exists, so nothing yet
//! guards a live registration whose fence is missing. No existing test removes
//! a live registration's fence. The refusal already holds today; this pins it
//! across the change of acquisition order.

use contextdb_core::Error;
use contextdb_core::Value;
use contextdb_core::read_contract::{ReadFailureKind, ReadRoute};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

fn fences_under(directory: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            fences_under(&path, found);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "fence")
        {
            found.push(path);
        }
    }
}

/// Test-estate reason (2026-10-15): a purge is refused while a live reader's registration has no
/// publication fence, because the inventory is uncertain. Already true today; pinned so the change
/// of acquisition order keeps it.
#[test]
fn a_live_registration_without_its_fence_refuses_purge() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let path = directory.path().join("unfenced.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("secure the runtime directory");
    let seed = Database::open(&path).expect("seed writer");
    seed.execute(
        "CREATE TABLE erased_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL",
        &HashMap::new(),
    )
    .expect("create the table");
    seed.execute(
        "INSERT INTO erased_rows (id, payload) VALUES ($id, 'secret')",
        &HashMap::from([("id".to_owned(), Value::Int64(1))]),
    )
    .expect("seed a row");
    seed.close().expect("close the seed writer");

    let session = ReadSession::open_in_runtime_dir(
        &path,
        ReadSessionOptions::default(),
        Some(runtime.clone()),
    )
    .expect("a direct reader registers");
    assert_eq!(session.route(), ReadRoute::File);

    let mut fences = Vec::new();
    fences_under(&runtime, &mut fences);
    assert!(
        !fences.is_empty(),
        "a registered reader publishes a purge-publication fence under its runtime root \
         (looked for *.fence under {})",
        runtime.display()
    );
    for fence in &fences {
        std::fs::remove_file(fence).expect("remove the registration's fence");
    }

    let writer = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("the writer opens: the reader holds nothing after its load");
    let refusal = writer
        .execute("PURGE FROM erased_rows WHERE id = 1", &HashMap::new())
        .expect_err("a live registration with no fence is an uncertain inventory");
    assert!(
        matches!(
            &refusal,
            Error::ReadFailure(failure)
                if failure.kind() == ReadFailureKind::AuthoritativeErasure
        ) || matches!(&refusal, Error::ReaderCoordinationUnavailable { operation, .. }
                if operation == "authoritative purge"),
        "PURGE is refused as an uncertain inventory (authoritative erasure or a purge \
         coordination refusal), got {refusal:?}"
    );
    drop(session);
    writer.close().expect("close the writer");
}
