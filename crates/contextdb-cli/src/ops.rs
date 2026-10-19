//! The `reset` / `diagnose` / `snapshot` / `inspect` / `purge`
//! store-maintenance subcommands.
//!
//! Dispatch happens BEFORE the existing REPL's `clap` parsing (see
//! `main.rs`), by literal first-argument match, so the existing
//! `contextdb <path> [OPTIONS]` invocation is completely unaffected —
//! only a first argument that names one of those commands
//! is intercepted here.

use crate::{EXIT_ERROR, EXIT_OK, EXIT_USAGE, ErrorClass, OutputOptions};
use contextdb_engine::Database;
use contextdb_engine::database::SnapshotInspector;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::vector_read_memory::DeclaredReadMemory;
use contextdb_engine::work_ledger::BlobHash;
use std::path::Path;

/// If `raw_args[1]` names one of the store-maintenance subcommands, run its
/// handler and return the process exit code to use. `None` means "not one
/// of these" — the caller falls through to the existing REPL entry point
/// unchanged.
pub fn dispatch_if_subcommand<F>(raw_args: &[String], read_memory_factory: F) -> Option<i32>
where
    F: FnOnce() -> contextdb_core::Result<(
        std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        std::sync::Arc<DeclaredReadMemory>,
    )>,
{
    let name = raw_args.get(1)?;
    run_operational(name, &raw_args[2..], read_memory_factory)
}

/// Run the operational command `name` with its own argument tail, exactly as
/// it was typed. `None` means the word does not name an operational command.
///
/// The command tree in the binary resolves the spelling and hands the tail
/// here, so the two surfaces cannot disagree about which words exist.
pub fn run_operational<F>(name: &str, args: &[String], read_memory_factory: F) -> Option<i32>
where
    F: FnOnce() -> contextdb_core::Result<(
        std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        std::sync::Arc<DeclaredReadMemory>,
    )>,
{
    match name {
        "reset" => Some(run_reset(args)),
        "diagnose" => Some(run_diagnose(args)),
        "snapshot" => Some(run_snapshot(args, read_memory_factory)),
        "inspect" => Some(run_inspect(args, read_memory_factory)),
        "purge" => Some(run_purge(args)),
        _ => None,
    }
}

/// Inspect durable facts from any readable data root -- a completed snapshot
/// export or a live database file still in use by another process -- without
/// ever opening the caller-supplied path directly. The engine copies it into
/// a private disposable copy first, opens only that copy, and exposes only
/// bounded key/blob DTOs.
fn run_inspect<F>(args: &[String], read_memory_factory: F) -> i32
where
    F: FnOnce() -> contextdb_core::Result<(
        std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        std::sync::Arc<DeclaredReadMemory>,
    )>,
{
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
    let output = OutputOptions {
        json: args.iter().any(|arg| arg == "--json"),
    };
    let (read_memory, declared_read_memory) = match read_memory_factory() {
        Ok(read_memory) => read_memory,
        Err(err) => {
            match &err {
                contextdb_core::Error::ReadFailure(failure) => {
                    output.report_read_failure(failure, None);
                }
                _ => output.report_error(ErrorClass::of(&err), &err.to_string()),
            }
            return EXIT_ERROR;
        }
    };
    let inspector =
        match SnapshotInspector::open(artifact, read_memory.clone(), declared_read_memory) {
            Ok(inspector) => inspector,
            Err(err) => {
                match &err {
                    contextdb_core::Error::ReadFailure(failure) => {
                        output.report_read_failure(failure, None);
                    }
                    _ => output.report_error(ErrorClass::of(&err), &err.to_string()),
                }
                return EXIT_ERROR;
            }
        };
    let result = match request {
        Request::Key {
            table,
            key,
            columns,
        } => inspector
            .inspect_key(&table, &key, &columns, std::sync::Arc::clone(&read_memory))
            .and_then(|report| {
                serde_json::to_value(report).map_err(|err| {
                    contextdb_core::Error::Other((format!("failed to encode key inspection: {err}")).into())
                })
            }),
        Request::Blob { hash } => inspector.inspect_blob(hash).and_then(|report| {
            serde_json::to_value(report).map_err(|err| {
                contextdb_core::Error::Other((format!("failed to encode blob inspection: {err}")).into())
            })
        }),
        Request::SyncApplyState => inspector.inspect_sync_apply_state().and_then(|report| {
            serde_json::to_value(report).map_err(|err| {
                contextdb_core::Error::Other((format!(
                    "failed to encode sync-apply-state inspection: {err}"
                )).into())
            })
        }),
    };
    let close = inspector.close();
    let report = match result {
        Ok(report) => report,
        Err(err) => {
            match &err {
                contextdb_core::Error::ReadFailure(failure) => {
                    output.report_read_failure(failure, None);
                }
                _ => eprintln!("Error: snapshot inspection failed: {err}"),
            }
            return EXIT_ERROR;
        }
    };
    if let Err(err) = close {
        match &err {
            contextdb_core::Error::ReadFailure(failure) => {
                output.report_read_failure(failure, None);
            }
            _ => eprintln!("Error: snapshot inspection copy did not close cleanly: {err}"),
        }
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
    drop(report);
    drop(read_memory);
    EXIT_OK
}

/// `contextdb snapshot export <database> <artifact> [--json]` — publish a
/// transactionally consistent, purge-fenced backup through the engine's
/// production snapshot path. The destination must not already exist.
fn run_snapshot<F>(args: &[String], read_memory_factory: F) -> i32
where
    F: FnOnce() -> contextdb_core::Result<(
        std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        std::sync::Arc<DeclaredReadMemory>,
    )>,
{
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
    let (read_memory, _declared_parent) = match read_memory_factory() {
        Ok(owner) => owner,
        Err(err) => {
            OutputOptions { json }.report_error(ErrorClass::of(&err), &err.to_string());
            return EXIT_ERROR;
        }
    };
    let db = match Database::open(source) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("Error: failed to open '{}': {err}", source);
            return EXIT_ERROR;
        }
    };
    let report = db.export_snapshot_with_read_memory(destination, read_memory);
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
            "backup source is not a direct regular file",
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
            "backup source changed while it was opened",
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
                "backup source inode changed while it was opened",
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
                    "backup source size or type changed while it was copied",
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
                        "backup source inode changed while it was copied",
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
                    "backup path changed while it was published",
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
                        "backup inode changed while it was published",
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
    // This invocation owns the read working set through execution, result
    // rendering, and source close, independently of the store's writer funding.
    let read_memory =
        DeclaredReadMemory::from_reader_limit(db.owner_read_config().limits.limits.memory);
    let result = read_memory.child().and_then(|request_memory| {
        db.execute_with_read_memory(
            &format!("PURGE FROM {table}"),
            &std::collections::HashMap::new(),
            request_memory,
        )
    });
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
/// current or incompatible), without modifying a healthy one. Uses
/// `RedbPersistence::is_legacy_format_store`,
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
                "diagnose: '{}' was written by an earlier build. This store is not read. \
                 Move it aside and create the store again.",
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
