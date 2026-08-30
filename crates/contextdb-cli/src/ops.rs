//! The `migrate` / `reset` / `diagnose` / `snapshot` / `inspect` / `purge`
//! store-maintenance subcommands.
//!
//! Dispatch happens BEFORE the existing REPL's `clap` parsing (see
//! `main.rs`), by literal first-argument match, so the existing
//! `contextdb <path> [OPTIONS]` invocation is completely unaffected —
//! only a first argument that names one of those commands
//! is intercepted here.

use crate::{EXIT_ERROR, EXIT_OK, EXIT_USAGE};
use contextdb_engine::Database;
#[cfg(feature = "test-seams")]
use contextdb_engine::database::MigrationBoundary;
use contextdb_engine::database::{MigrationFailureStage, MigrationReceipt, SnapshotInspector};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::work_ledger::BlobHash;
use std::path::{Path, PathBuf};

/// Test-only coordination at the real migration replacement boundaries.
/// The seam observes and pauses the production operation; it never acquires
/// or substitutes for either the source-store guard or a competing writer.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod migration_replacement_test_scaffold {
    use std::collections::VecDeque;
    use std::sync::{Condvar, Mutex, OnceLock};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MigrationReplacementBoundary {
        SourceGuardAcquired,
        TemporaryStoreOpened,
        TemporaryStoreImported,
        SourceStoreSealed,
        TemporaryStoreBuilt,
        TemporaryStoreDurablyPreparedAndOwned,
        BeforeAtomicSwap,
        AfterAtomicSwap,
        TemporaryCompanionCleaned,
        BeforeFinalGuardRelease,
    }

    impl MigrationReplacementBoundary {
        pub const ALL: [Self; 10] = [
            Self::SourceGuardAcquired,
            Self::TemporaryStoreOpened,
            Self::TemporaryStoreImported,
            Self::SourceStoreSealed,
            Self::TemporaryStoreBuilt,
            Self::TemporaryStoreDurablyPreparedAndOwned,
            Self::BeforeAtomicSwap,
            Self::AfterAtomicSwap,
            Self::TemporaryCompanionCleaned,
            Self::BeforeFinalGuardRelease,
        ];
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MigrationReplacementEvent {
        Checkpoint(MigrationReplacementBoundary),
        CompletedAfterGuardRelease,
        FinishedBeforeExpectedCheckpoint {
            expected: MigrationReplacementBoundary,
        },
        UnexpectedCheckpoint {
            expected: MigrationReplacementBoundary,
            actual: MigrationReplacementBoundary,
        },
    }

    #[derive(Debug, Default)]
    struct MigrationReplacementState {
        armed: bool,
        expected: VecDeque<MigrationReplacementBoundary>,
        reached: Option<MigrationReplacementBoundary>,
        released: bool,
        finished: bool,
        unexpected: Option<(MigrationReplacementBoundary, MigrationReplacementBoundary)>,
    }

    static MIGRATION_REPLACEMENT: OnceLock<(Mutex<MigrationReplacementState>, Condvar)> =
        OnceLock::new();

    // The migration's two irreversible filesystem steps -- the atomic swap
    // and the generated companion's removal -- belong to the engine door that
    // performs them, and so do the injectors that make them fail. They are
    // named here so a proof written against this command keeps reaching for
    // them where it always did.
    pub use contextdb_engine::database::migration_fault_seam::{
        FinalRenameFaultGuard, TemporaryCompanionCleanupFaultGuard,
        TemporaryCompanionCleanupFaultObservation, arm_final_rename_failure_for_test,
        arm_temporary_companion_cleanup_failure_for_test,
    };

    fn migration_replacement() -> &'static (Mutex<MigrationReplacementState>, Condvar) {
        MIGRATION_REPLACEMENT.get_or_init(|| {
            (
                Mutex::new(MigrationReplacementState::default()),
                Condvar::new(),
            )
        })
    }

    pub fn arm_migration_replacement_sequence_for_test() {
        let (state, _) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            !state.armed,
            "migration replacement sequence is already armed"
        );
        *state = MigrationReplacementState {
            armed: true,
            expected: MigrationReplacementBoundary::ALL.into_iter().collect(),
            reached: None,
            released: false,
            finished: false,
            unexpected: None,
        };
    }

    pub fn next_migration_replacement_event_for_test() -> MigrationReplacementEvent {
        let (state, changed) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some((expected, actual)) = state.unexpected.take() {
                *state = MigrationReplacementState::default();
                changed.notify_all();
                return MigrationReplacementEvent::UnexpectedCheckpoint { expected, actual };
            }
            if let Some(boundary) = state.reached {
                return MigrationReplacementEvent::Checkpoint(boundary);
            }
            if state.finished {
                let event = match state.expected.front().copied() {
                    Some(expected) => {
                        MigrationReplacementEvent::FinishedBeforeExpectedCheckpoint { expected }
                    }
                    None => MigrationReplacementEvent::CompletedAfterGuardRelease,
                };
                *state = MigrationReplacementState::default();
                changed.notify_all();
                return event;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub fn release_migration_replacement_checkpoint_for_test() {
        let (state, changed) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reached = state
            .reached
            .take()
            .expect("migration replacement checkpoint was not reached");
        assert_eq!(state.expected.pop_front(), Some(reached));
        state.released = true;
        changed.notify_all();
    }

    pub fn finish_migration_replacement_sequence_for_test() {
        let (state, changed) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.armed {
            state.finished = true;
            changed.notify_all();
        }
    }

    pub fn cancel_migration_replacement_sequence_for_test() {
        let (state, changed) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = MigrationReplacementState::default();
        changed.notify_all();
    }

    pub(crate) fn migration_replacement_checkpoint_for_test(
        boundary: MigrationReplacementBoundary,
    ) {
        let (state, changed) = migration_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.armed {
            return;
        }
        let Some(expected) = state.expected.front().copied() else {
            return;
        };
        if expected != boundary {
            state.unexpected = Some((expected, boundary));
            changed.notify_all();
            return;
        }
        state.reached = Some(boundary);
        state.released = false;
        changed.notify_all();
        while state.armed && !state.released {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.released = false;
    }
}

/// If `raw_args[1]` names one of the store-maintenance subcommands, run its
/// handler and return the process exit code to use. `None` means "not one
/// of these" — the caller falls through to the existing REPL entry point
/// unchanged.
pub fn dispatch_if_subcommand(raw_args: &[String]) -> Option<i32> {
    let name = raw_args.get(1)?;
    run_operational(name, &raw_args[2..])
}

/// Run the operational command `name` with its own argument tail, exactly as
/// it was typed. `None` means the word does not name an operational command.
///
/// The command tree in the binary resolves the spelling and hands the tail
/// here, so the two surfaces cannot disagree about which words exist.
pub fn run_operational(name: &str, args: &[String]) -> Option<i32> {
    match name {
        "migrate" => Some(run_migrate(args)),
        "reset" => Some(run_reset(args)),
        "diagnose" => Some(run_diagnose(args)),
        "snapshot" => Some(run_snapshot(args)),
        "inspect" => Some(run_inspect(args)),
        "purge" => Some(run_purge(args)),
        _ => None,
    }
}

/// Inspect durable facts from any readable data root -- a completed snapshot
/// export or a live database file still in use by another process -- without
/// ever opening the caller-supplied path directly. The engine copies it into
/// a private disposable copy first, opens only that copy, and exposes only
/// bounded key/blob DTOs.
fn run_inspect(args: &[String]) -> i32 {
    enum Request {
        Key {
            table: String,
            key: NaturalKey,
            columns: Vec<String>,
        },
        Blob {
            hash: [u8; 32],
        },
        SyncApplyState,
    }

    let Some(kind) = args.first().map(String::as_str) else {
        eprintln!("Error: inspect requires `key`, `blob`, or `sync-apply-state`");
        return EXIT_USAGE;
    };
    let Some(artifact) = args.get(1).filter(|arg| !arg.starts_with("--")) else {
        eprintln!("Error: inspect requires a snapshot artifact or database file path");
        return EXIT_USAGE;
    };
    let request = match kind {
        "key" => {
            let mut table = None;
            let mut key_json = None;
            let mut columns = Vec::new();
            let mut json_seen = false;
            let mut index = 2;
            while index < args.len() {
                let option = args[index].as_str();
                if option == "--json" {
                    if json_seen {
                        eprintln!("Error: inspect key accepts --json only once");
                        return EXIT_USAGE;
                    }
                    json_seen = true;
                    index += 1;
                    continue;
                }
                if !matches!(option, "--table" | "--key-json" | "--column") {
                    eprintln!("Error: unknown inspect key option '{option}'");
                    return EXIT_USAGE;
                }
                let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--"))
                else {
                    eprintln!("Error: inspect key option {option} requires a value");
                    return EXIT_USAGE;
                };
                match option {
                    "--table" if table.replace(value.clone()).is_some() => {
                        eprintln!("Error: inspect key accepts --table only once");
                        return EXIT_USAGE;
                    }
                    "--key-json" if key_json.replace(value.clone()).is_some() => {
                        eprintln!("Error: inspect key accepts --key-json only once");
                        return EXIT_USAGE;
                    }
                    "--column" if columns.contains(value) => {
                        eprintln!("Error: inspect key accepts each --column only once");
                        return EXIT_USAGE;
                    }
                    "--column" => columns.push(value.clone()),
                    _ => {}
                }
                index += 2;
            }
            let Some(table) = table else {
                eprintln!("Error: inspect key requires --table <TABLE>");
                return EXIT_USAGE;
            };
            let Some(key_json) = key_json else {
                eprintln!("Error: inspect key requires --key-json <NATURAL_KEY_JSON>");
                return EXIT_USAGE;
            };
            if columns.len() > 16 || columns.iter().any(|column| column.len() > 128) {
                eprintln!(
                    "Error: inspect key accepts at most 16 --column values of at most 128 bytes each"
                );
                return EXIT_USAGE;
            }
            if key_json.len() > 64 * 1024 {
                eprintln!("Error: --key-json exceeds the 64 KiB inspection input bound");
                return EXIT_USAGE;
            }
            let key = match serde_json::from_str::<NaturalKey>(&key_json) {
                Ok(key) => key,
                Err(err) => {
                    eprintln!("Error: --key-json is not a natural-key document: {err}");
                    return EXIT_USAGE;
                }
            };
            Request::Key {
                table,
                key,
                columns,
            }
        }
        "blob" => {
            let mut hash = None;
            let mut json_seen = false;
            let mut index = 2;
            while index < args.len() {
                let option = args[index].as_str();
                if option == "--json" {
                    if json_seen {
                        eprintln!("Error: inspect blob accepts --json only once");
                        return EXIT_USAGE;
                    }
                    json_seen = true;
                    index += 1;
                    continue;
                }
                if option != "--hash" {
                    eprintln!("Error: unknown inspect blob option '{option}'");
                    return EXIT_USAGE;
                }
                let Some(value) = args.get(index + 1).filter(|value| !value.starts_with("--"))
                else {
                    eprintln!("Error: inspect blob option --hash requires a value");
                    return EXIT_USAGE;
                };
                if hash.replace(value.clone()).is_some() {
                    eprintln!("Error: inspect blob accepts --hash only once");
                    return EXIT_USAGE;
                }
                index += 2;
            }
            let Some(hash) = hash else {
                eprintln!("Error: inspect blob requires --hash <64_HEX_CHARS>");
                return EXIT_USAGE;
            };
            let hash = match BlobHash::from_hex(&hash) {
                Ok(hash) => hash,
                Err(err) => {
                    eprintln!("Error: invalid blob hash: {err}");
                    return EXIT_USAGE;
                }
            };
            Request::Blob {
                hash: hash.as_bytes(),
            }
        }
        "sync-apply-state" => {
            let mut json_seen = false;
            for option in &args[2..] {
                if option == "--json" && !json_seen {
                    json_seen = true;
                } else if option == "--json" {
                    eprintln!("Error: inspect sync-apply-state accepts --json only once");
                    return EXIT_USAGE;
                } else {
                    eprintln!("Error: unknown inspect sync-apply-state option '{option}'");
                    return EXIT_USAGE;
                }
            }
            Request::SyncApplyState
        }
        _ => {
            eprintln!("Error: inspect requires `key`, `blob`, or `sync-apply-state`");
            return EXIT_USAGE;
        }
    };
    let inspector = match SnapshotInspector::open(artifact) {
        Ok(inspector) => inspector,
        Err(err) => {
            eprintln!("Error: snapshot inspection failed: {err}");
            return EXIT_ERROR;
        }
    };
    let result = match request {
        Request::Key {
            table,
            key,
            columns,
        } => inspector
            .inspect_key(&table, &key, &columns)
            .and_then(|report| {
                serde_json::to_value(report).map_err(|err| {
                    contextdb_core::Error::Other(format!("failed to encode key inspection: {err}"))
                })
            }),
        Request::Blob { hash } => inspector.inspect_blob(hash).and_then(|report| {
            serde_json::to_value(report).map_err(|err| {
                contextdb_core::Error::Other(format!("failed to encode blob inspection: {err}"))
            })
        }),
        Request::SyncApplyState => inspector.inspect_sync_apply_state().and_then(|report| {
            serde_json::to_value(report).map_err(|err| {
                contextdb_core::Error::Other(format!(
                    "failed to encode sync-apply-state inspection: {err}"
                ))
            })
        }),
    };
    let close = inspector.close();
    let report = match result {
        Ok(report) => report,
        Err(err) => {
            eprintln!("Error: snapshot inspection failed: {err}");
            return EXIT_ERROR;
        }
    };
    if let Err(err) = close {
        eprintln!("Error: snapshot inspection copy did not close cleanly: {err}");
        return EXIT_ERROR;
    }
    if args.iter().any(|arg| arg == "--json") {
        println!("{report}");
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("inspection report is serializable")
        );
    }
    EXIT_OK
}

/// `contextdb snapshot export <database> <artifact> [--json]` — publish a
/// transactionally consistent, purge-fenced backup through the engine's
/// production snapshot path. The destination must not already exist.
fn run_snapshot(args: &[String]) -> i32 {
    if args.first().map(String::as_str) != Some("export") {
        eprintln!("Error: snapshot requires `export <database> <artifact>`");
        return EXIT_USAGE;
    }
    let json = args.iter().any(|arg| arg == "--json");
    let paths = args[1..]
        .iter()
        .filter(|arg| !arg.starts_with("--"))
        .collect::<Vec<_>>();
    let [source, destination] = paths.as_slice() else {
        eprintln!("Error: snapshot export requires a database path and a new artifact path");
        return EXIT_USAGE;
    };
    let db = match Database::open(source) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Error: failed to open '{}': {err}", source);
            return EXIT_ERROR;
        }
    };
    let report = db.export_snapshot(destination);
    let close = db.close();
    let report = match report {
        Ok(report) => report,
        Err(err) => {
            eprintln!("Error: snapshot export failed: {err}");
            return EXIT_ERROR;
        }
    };
    if let Err(err) = close {
        eprintln!("Error: snapshot source did not close cleanly: {err}");
        return EXIT_ERROR;
    }
    if json {
        println!(
            "{}",
            serde_json::json!({
                "snapshot": {
                    "source": source,
                    "artifact": destination,
                    "snapshot_lsn": report.snapshot_lsn.0,
                    "rows": report.rows,
                    "edges": report.edges,
                    "vectors": report.vectors,
                    "bytes": report.bytes_written,
                }
            })
        );
    } else {
        println!(
            "snapshot exported to '{}' at LSN {} ({} rows, {} edges, {} vectors, {} bytes)",
            destination,
            report.snapshot_lsn.0,
            report.rows,
            report.edges,
            report.vectors,
            report.bytes_written,
        );
    }
    EXIT_OK
}

fn positional_path(args: &[String]) -> Option<&str> {
    args.iter()
        .find(|arg| !arg.starts_with("--"))
        .map(|s| s.as_str())
}

#[cfg(not(unix))]
fn copy_migration_backup(source: &Path, destination: &Path) -> std::io::Result<u64> {
    let source_path_before = std::fs::symlink_metadata(source)?;
    if source_path_before.file_type().is_symlink() || !source_path_before.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "migration source is not a direct regular file",
        ));
    }
    let mut source_options = std::fs::OpenOptions::new();
    source_options.read(true);
    let mut source_file = source_options.open(source)?;
    let source_opened = source_file.metadata()?;
    let source_path_after = std::fs::symlink_metadata(source)?;
    if !source_opened.file_type().is_file()
        || source_path_after.file_type().is_symlink()
        || !source_path_after.file_type().is_file()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "migration source changed while its backup was opened",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if source_path_before.dev() != source_opened.dev()
            || source_path_before.ino() != source_opened.ino()
            || source_opened.dev() != source_path_after.dev()
            || source_opened.ino() != source_path_after.ino()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "migration source inode changed while its backup was opened",
            ));
        }
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut destination_file = options.open(destination)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        destination_file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    let destination_opened = destination_file.metadata()?;
    match std::io::copy(&mut source_file, &mut destination_file)
        .and_then(|copied| {
            let source_descriptor_end = source_file.metadata()?;
            let source_path_end = std::fs::symlink_metadata(source)?;
            if !source_descriptor_end.file_type().is_file()
                || source_opened.len() != source_descriptor_end.len()
                || copied != source_opened.len()
                || source_path_end.file_type().is_symlink()
                || !source_path_end.file_type().is_file()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "migration source size or type changed while its backup was copied",
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                if source_opened.dev() != source_descriptor_end.dev()
                    || source_opened.ino() != source_descriptor_end.ino()
                    || source_opened.dev() != source_path_end.dev()
                    || source_opened.ino() != source_path_end.ino()
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "migration source inode changed while its backup was copied",
                    ));
                }
            }
            Ok(copied)
        })
        .and_then(|copied| {
            destination_file.sync_all()?;
            let destination_path = std::fs::symlink_metadata(destination)?;
            if !destination_opened.file_type().is_file()
                || destination_path.file_type().is_symlink()
                || !destination_path.file_type().is_file()
                || destination_file.metadata()?.len() != copied
                || destination_path.len() != copied
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "migration backup path changed while it was published",
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                let destination_descriptor = destination_file.metadata()?;
                if destination_opened.dev() != destination_descriptor.dev()
                    || destination_opened.ino() != destination_descriptor.ino()
                    || destination_opened.dev() != destination_path.dev()
                    || destination_opened.ino() != destination_path.ino()
                    || destination_descriptor.mode() & 0o7777 != 0o600
                    || destination_path.mode() & 0o7777 != 0o600
                    || destination_descriptor.nlink() != 1
                    || destination_path.nlink() != 1
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "migration backup inode changed while it was published",
                    ));
                }
            }
            Ok(copied)
        })
        .and_then(|copied| {
            let parent = destination
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map(|()| copied)
        }) {
        Ok(copied) => Ok(copied),
        Err(error) => {
            drop(destination_file);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                if std::fs::symlink_metadata(destination).is_ok_and(|current| {
                    !current.file_type().is_symlink()
                        && current.file_type().is_file()
                        && current.dev() == destination_opened.dev()
                        && current.ino() == destination_opened.ino()
                }) {
                    let _ = std::fs::remove_file(destination);
                }
            }
            Err(error)
        }
    }
}

/// `contextdb migrate <path>` — migrate a legacy-format store in place:
/// back up the original untouched, read every row/edge/vector/DDL statement
/// out of it via the engine's own legacy-tolerant loader, write it into a
/// fresh current-format root, then atomically swap it in. Refuses (leaving
/// the path untouched) on a store that is already current-format, and on any
/// failure before the atomic swap.
fn run_migrate(args: &[String]) -> i32 {
    let Some(path_str) = positional_path(args) else {
        eprintln!("Error: migrate requires a database path");
        return EXIT_USAGE;
    };
    // Resolve the source once. Every lock, backup, temporary sibling, swap,
    // publication, and message below uses this exact identity, so a symlink
    // spelling cannot coordinate one pathname and replace another.
    let path = match std::fs::canonicalize(Path::new(path_str)) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("Error: '{}' could not be migrated: {err}", path_str);
            return EXIT_ERROR;
        }
    };

    // Detect: is this already current-format, or genuinely legacy? Uses a
    // read-only handle (`RedbPersistence::is_legacy_format_store`) rather
    // than a normal `Database::open` — opening a redb file read-write
    // performs a housekeeping write on every open regardless of any
    // application transaction, which would violate "a refused migrate must
    // never modify the store it declined to touch" for an already-current
    // root, and would silently perturb the legacy root's bytes before the
    // backup below is even taken.
    match RedbPersistence::is_legacy_format_store(&path) {
        Ok(false) => {
            eprintln!(
                "Error: '{}' is already current-format; there is nothing to migrate.",
                path.display()
            );
            return EXIT_ERROR;
        }
        Ok(true) => {
            // Confirmed legacy — proceed below.
        }
        Err(err) => {
            eprintln!("Error: '{}' could not be migrated: {err}", path.display());
            return EXIT_ERROR;
        }
    }

    // Never overwrite a prior backup. A symlink at this name counts as an
    // existing destination and is never followed or replaced.
    let mut backup_name = path.as_os_str().to_os_string();
    backup_name.push(".bak");
    let backup_path = PathBuf::from(backup_name);
    match std::fs::symlink_metadata(&backup_path) {
        Ok(_) => {
            eprintln!(
                "Error: a backup already exists at '{}'; migrate refuses to overwrite it. Move or \
                 remove it first if it is no longer needed.",
                backup_path.display()
            );
            return EXIT_ERROR;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            eprintln!(
                "Error: backup destination '{}' could not be inspected: {error}",
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    }

    // This migration-only source owns the original companion guard and an
    // exclusive lock on the exact old-store descriptor without opening Redb
    // writable yet. The first source read consumes that same descriptor into
    // Redb after the untouched backup; no pathname-global reentrancy or
    // unlock/reopen gap is involved.
    let legacy_db = match Database::open_legacy_for_migration(&path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Error: '{}' could not be migrated: {err}", path.display());
            return EXIT_ERROR;
        }
    };
    #[cfg(feature = "test-seams")]
    migration_replacement_test_scaffold::migration_replacement_checkpoint_for_test(
        migration_replacement_test_scaffold::MigrationReplacementBoundary::SourceGuardAcquired,
    );

    // Copy only after exclusive migration ownership is established and
    // before writable Redb hydration can perform open-time housekeeping.
    // By this point the source pathname may already have been unlinked or
    // replaced -- the guard, not the name, is what this migration owns. The
    // backup therefore reads through the descriptor locked when the guard
    // was acquired and never resolves the source pathname again.
    #[cfg(unix)]
    let backup_result = legacy_db
        .copy_locked_source_to(&backup_path)
        .map_err(|err| err.to_string());
    #[cfg(not(unix))]
    let backup_result = copy_migration_backup(&path, &backup_path).map_err(|err| err.to_string());
    if let Err(err) = backup_result {
        let _ = legacy_db.close();
        eprintln!(
            "Error: failed to write a backup at '{}' before migrating '{}': {err}",
            backup_path.display(),
            path.display()
        );
        return EXIT_ERROR;
    }
    // Opening the fresh current-format root, importing every row, copying
    // keyless-table current state, durably recording and preparing the exact
    // target, swapping it into place, dropping the generated companion and
    // releasing both guards is the engine's one door, and so is deciding what
    // gets published and where: the door republishes the validated source at
    // its own pathname, names its own target, and reads the source's own
    // keyless rows. This command renders whichever phase the door reports and
    // receives a receipt; it never holds a handle onto the store being built,
    // and never performs the swap that publishes it. A test build watches the
    // door's boundaries through the engine's own observation seam; a
    // production build installs nothing.
    #[cfg(feature = "test-seams")]
    contextdb_engine::database::migration_boundary_seam::install_boundary_observer_for_test(
        std::sync::Arc::new(|boundary: MigrationBoundary| {
            use migration_replacement_test_scaffold::MigrationReplacementBoundary as ScaffoldBoundary;
            let mapped = match boundary {
                MigrationBoundary::TemporaryStoreOpened => ScaffoldBoundary::TemporaryStoreOpened,
                MigrationBoundary::TemporaryStoreImported => {
                    ScaffoldBoundary::TemporaryStoreImported
                }
                MigrationBoundary::SourceStoreSealed => ScaffoldBoundary::SourceStoreSealed,
                MigrationBoundary::TemporaryStoreBuilt => ScaffoldBoundary::TemporaryStoreBuilt,
                MigrationBoundary::TemporaryStoreDurablyPreparedAndOwned => {
                    ScaffoldBoundary::TemporaryStoreDurablyPreparedAndOwned
                }
                MigrationBoundary::BeforeAtomicSwap => ScaffoldBoundary::BeforeAtomicSwap,
                MigrationBoundary::AfterAtomicSwap => ScaffoldBoundary::AfterAtomicSwap,
                MigrationBoundary::TemporaryCompanionCleaned => {
                    ScaffoldBoundary::TemporaryCompanionCleaned
                }
                MigrationBoundary::BeforeFinalGuardRelease => {
                    ScaffoldBoundary::BeforeFinalGuardRelease
                }
            };
            migration_replacement_test_scaffold::migration_replacement_checkpoint_for_test(mapped);
        }),
    );

    let migrated = legacy_db.migrate_in_place();
    #[cfg(feature = "test-seams")]
    contextdb_engine::database::migration_boundary_seam::clear_boundary_observer_for_test();
    let receipt = match migrated {
        Ok(receipt) => receipt,
        Err(err) => {
            match err.stage() {
                MigrationFailureStage::BeforeSwap => eprintln!(
                    "Error: '{}' has a backup at '{}', but {err}",
                    path.display(),
                    backup_path.display()
                ),
                MigrationFailureStage::AtSwap => eprintln!(
                    "Error: migrating '{}' failed at the final swap: {err}. The store is \
                     unchanged and still usable, its pre-migration backup is at '{}', and every \
                     artifact the migration generated has been removed.",
                    path.display(),
                    backup_path.display()
                ),
                MigrationFailureStage::AfterSwap => eprintln!(
                    "Error: '{}' was migrated, but replacement publication or final source close \
                     failed: {err}",
                    path.display()
                ),
            }
            return EXIT_ERROR;
        }
    };
    let MigrationReceipt {
        applied_rows,
        keyless_rows_copied,
        keyless_table_receipts,
    } = receipt;

    let message = if keyless_rows_copied > 0 {
        let mut lines = vec![format!(
            "migrated '{}' in place ({applied_rows} rows from changeset + {keyless_rows_copied} \
             keyless-table rows from current state); the pre-migration store is backed up at '{}'.",
            path.display(),
            backup_path.display()
        )];
        for (table_name, copied) in &keyless_table_receipts {
            if *copied > 0 {
                lines.push(format!(
                    "  - keyless table '{table_name}': {copied} row(s) copied from current \
                     state (current-state-only; a changeset cannot carry a keyless row)."
                ));
            }
        }
        lines.join("\n")
    } else {
        format!(
            "migrated '{}' in place ({applied_rows} rows applied); the pre-migration store is \
             backed up at '{}'.",
            path.display(),
            backup_path.display()
        )
    };
    println!("{}", message);
    EXIT_OK
}

/// `contextdb reset <path> --force` — recreate a wedged/corrupt store
/// from scratch. Refuses without `--force`, leaving the store untouched.
fn run_reset(args: &[String]) -> i32 {
    let Some(path_str) = positional_path(args) else {
        eprintln!("Error: reset requires a database path");
        return EXIT_USAGE;
    };
    let path = Path::new(path_str);
    let force = args.iter().any(|arg| arg == "--force");
    if !force {
        eprintln!(
            "Error: reset destroys the existing store, so it requires the explicit --force \
             flag; rerun as `contextdb reset {} --force` once you've restored any data you \
             need from a backup or a healthy sync peer.",
            path.display()
        );
        return EXIT_USAGE;
    }

    match Database::force_reset(path) {
        Ok(()) => {
            println!(
                "reset '{}': a fresh, empty current-format store was created.",
                path.display()
            );
            EXIT_OK
        }
        Err(err) => {
            eprintln!("Error: reset failed for '{}': {err}", path.display());
            EXIT_ERROR
        }
    }
}

/// `contextdb purge <path> --table <t> --force` — permanently and
/// authoritatively erase every row of `<t>` (no `WHERE` support at the
/// operator door; use the engine's `PURGE FROM <table> WHERE ...` SQL
/// directly if a narrower selection is needed). Refuses without `--force`,
/// exactly like `reset`. Dispatches straight to the engine's existing
/// `PURGE FROM <table>` statement (B_2's authoritative-purge machinery) so
/// this door shares the exact same purge-fence semantics `.maintenance` and
/// snapshot restore already rely on -- including the standalone-edge-only
/// refusal (`Error::PurgeRequiresAuthoritativeHub`) once a sync peer is
/// configured.
fn run_purge(args: &[String]) -> i32 {
    let Some(path_str) = positional_path(args) else {
        eprintln!("Error: purge requires a database path");
        return EXIT_USAGE;
    };
    let path = Path::new(path_str);
    let table = args
        .iter()
        .position(|arg| arg == "--table")
        .and_then(|idx| args.get(idx + 1));
    let Some(table) = table else {
        eprintln!("Error: purge requires --table <name>");
        return EXIT_USAGE;
    };
    let force = args.iter().any(|arg| arg == "--force");
    if !force {
        eprintln!(
            "Error: purge permanently and irreversibly erases rows, so it requires the \
             explicit --force flag; rerun as `contextdb purge {} --table {table} --force` \
             once you're certain.",
            path.display()
        );
        return EXIT_USAGE;
    }

    let db = match Database::open(path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Error: failed to open '{}': {err}", path.display());
            return EXIT_ERROR;
        }
    };
    let result = db.execute(
        &format!("PURGE FROM {table}"),
        &std::collections::HashMap::new(),
    );
    let close_result = db.close();
    match result {
        Ok(outcome) => {
            if let Err(err) = close_result {
                eprintln!(
                    "Error: purge succeeded but failed to close '{}': {err}",
                    path.display()
                );
                return EXIT_ERROR;
            }
            println!(
                "purge '{}': erased {} row(s) from '{table}'.",
                path.display(),
                outcome.rows_affected,
            );
            EXIT_OK
        }
        Err(err) => {
            eprintln!("Error: purge failed for '{}': {err}", path.display());
            EXIT_ERROR
        }
    }
}

/// `contextdb diagnose <path>` — report what is salvageable/diagnosable in
/// a store (which format marker it carries, whether the schema layout is
/// current or legacy), without modifying a healthy one. Uses the same
/// detection `migrate` does (`RedbPersistence::is_legacy_format_store`),
/// which reads through a read-only handle rather than a normal
/// `Database::open` — a plain read-write open performs a housekeeping write
/// on every open regardless of any application transaction, which would
/// break "never modifies the store it inspects" for a store that is
/// actually healthy. A store left dirty by a crashed writer is the one
/// exception: it is unreadable until redb's own crash repair runs, so
/// classifying it lets that repair happen. This is diagnosis, never a
/// schema-level in-place repair — `contextdb reset --force` is the recovery
/// command once you've restored any data you need.
fn run_diagnose(args: &[String]) -> i32 {
    let Some(path_str) = positional_path(args) else {
        eprintln!("Error: diagnose requires a database path");
        return EXIT_USAGE;
    };
    let path = Path::new(path_str);

    match RedbPersistence::is_legacy_format_store(path) {
        Ok(false) => {
            println!(
                "diagnose: '{}' is current-format and its schema layout reads cleanly; \
                 nothing to correct.",
                path.display()
            );
            EXIT_OK
        }
        Ok(true) => {
            println!(
                "diagnose: '{}' is a legacy-format store (its on-disk schema layout predates this \
                 release), not corrupt.\nThis report is diagnosis, not a schema-level \
                 in-place repair; a healthy store is not modified. \
                 Run `contextdb migrate {}` to migrate it in place.",
                path.display(),
                path.display()
            );
            EXIT_ERROR
        }
        Err(err) => {
            println!(
                "diagnose: '{}' — {err}\nThis report is diagnosis, not a schema-level in-place \
                 repair; a healthy store is not modified. \
                 Run `contextdb reset {} --force` to recreate it (after restoring any needed \
                 data from a backup or a healthy sync peer).",
                path.display(),
                path.display()
            );
            EXIT_ERROR
        }
    }
}
