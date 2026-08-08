//! The `migrate` / `reset` / `repair` / `snapshot` / `inspect` / `purge`
//! store-maintenance subcommands.
//!
//! Dispatch happens BEFORE the existing REPL's `clap` parsing (see
//! `main.rs`), by literal first-argument match, so the existing
//! `contextdb <path> [OPTIONS]` invocation is completely unaffected —
//! only a first argument that names one of those commands
//! is intercepted here.

use crate::{EXIT_ERROR, EXIT_OK, EXIT_USAGE};
use contextdb_engine::Database;
use contextdb_engine::database::SnapshotInspector;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::work_ledger::BlobHash;
use std::path::{Path, PathBuf};

/// If `raw_args[1]` names one of the store-maintenance subcommands, run its
/// handler and return the process exit code to use. `None` means "not one
/// of these" — the caller falls through to the existing REPL entry point
/// unchanged.
pub fn dispatch_if_subcommand(raw_args: &[String]) -> Option<i32> {
    let name = raw_args.get(1)?;
    match name.as_str() {
        "migrate" => Some(run_migrate(&raw_args[2..])),
        "reset" => Some(run_reset(&raw_args[2..])),
        "repair" => Some(run_repair(&raw_args[2..])),
        "snapshot" => Some(run_snapshot(&raw_args[2..])),
        "inspect" => Some(run_inspect(&raw_args[2..])),
        "purge" => Some(run_purge(&raw_args[2..])),
        _ => None,
    }
}

/// Inspect durable facts from a completed snapshot artifact without opening
/// the supplied file. The engine owns a private disposable copy and exposes
/// only bounded key/blob DTOs.
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
        eprintln!("Error: inspect requires a completed snapshot artifact");
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

/// The sibling `.lock` path `RedbPersistence` uses for `path`.
fn lock_path_for(path: &Path) -> PathBuf {
    path.with_extension("lock")
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
    let path = Path::new(path_str);

    // Detect: is this already current-format, or genuinely legacy? Uses a
    // read-only handle (`RedbPersistence::is_legacy_format_store`) rather
    // than a normal `Database::open` — opening a redb file read-write
    // performs a housekeeping write on every open regardless of any
    // application transaction, which would violate "a refused migrate must
    // never modify the store it declined to touch" for an already-current
    // root, and would silently perturb the legacy root's bytes before the
    // backup below is even taken.
    match RedbPersistence::is_legacy_format_store(path) {
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

    // Backup FIRST, before anything else touches the path. Never overwrite
    // an existing backup — that would silently destroy whatever it was
    // protecting (possibly a DIFFERENT pre-migration state from an earlier,
    // interrupted attempt).
    let backup_path = PathBuf::from(format!("{}.bak", path.display()));
    if backup_path.exists() {
        eprintln!(
            "Error: a backup already exists at '{}'; migrate refuses to overwrite it. Move or \
             remove it first if it is no longer needed.",
            backup_path.display()
        );
        return EXIT_ERROR;
    }
    if let Err(err) = std::fs::copy(path, &backup_path) {
        eprintln!(
            "Error: failed to write a backup at '{}' before migrating '{}': {err}",
            backup_path.display(),
            path.display()
        );
        return EXIT_ERROR;
    }

    // Read every row/edge/vector/DDL statement out of the legacy root.
    let legacy_db = match Database::open_legacy_for_migration(path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!(
                "Error: '{}' has a backup at '{}', but the legacy store could not be read: {err}",
                path.display(),
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    };
    // Before closing the legacy DB, detect keyless tables and collect their
    // current rows so they can be copied after the changeset replay (since
    // changesets omit rows without natural keys). A scan failure here is a
    // hard error (never silently skipped) -- see `collect_keyless_table_rows`.
    let keyless_table_rows = match legacy_db.keyless_table_rows() {
        Ok(rows) => rows,
        Err(err) => {
            let _ = legacy_db.close();
            eprintln!(
                "Error: '{}' has a backup at '{}', but reading its keyless-table rows failed: \
                 {err}",
                path.display(),
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    };

    // Write into a fresh current-format root, in the SAME directory as
    // `path` so the final swap below is an atomic same-filesystem rename.
    let tmp_path = PathBuf::from(format!("{}.migrate-tmp", path.display()));
    let _ = std::fs::remove_file(&tmp_path);
    let _ = std::fs::remove_file(lock_path_for(&tmp_path));
    let tmp_db = match Database::open(&tmp_path) {
        Ok(db) => db,
        Err(err) => {
            let _ = legacy_db.close();
            eprintln!(
                "Error: '{}' has a backup at '{}', but the migration target could not be \
                 created: {err}",
                path.display(),
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    };
    let apply_result = tmp_db.import_legacy_database(&legacy_db);
    let applied_rows = match apply_result {
        Ok(result) => result.applied_rows,
        Err(err) => {
            let _ = legacy_db.close();
            let _ = tmp_db.close();
            let _ = std::fs::remove_file(&tmp_path);
            let _ = std::fs::remove_file(lock_path_for(&tmp_path));
            eprintln!(
                "Error: '{}' has a backup at '{}', but writing the migrated data failed: {err}",
                path.display(),
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    };
    if let Err(err) = legacy_db.close() {
        let _ = tmp_db.close();
        let _ = std::fs::remove_file(&tmp_path);
        let _ = std::fs::remove_file(lock_path_for(&tmp_path));
        eprintln!(
            "Error: '{}' has a backup at '{}', but the legacy store did not close cleanly: {err}",
            path.display(),
            backup_path.display()
        );
        return EXIT_ERROR;
    }

    // Copy current rows of keyless tables into the migrated database.
    // These rows are not representable in a changeset, so we copy the
    // VISIBLE CURRENT state after the changeset replay. `columns` is the
    // ONE ordered list this table's `KeylessTableRows` already carries, used
    // for BOTH the column-name list and the `$name` placeholder list, so
    // the two always name the same column in the same position; `row` is
    // keyed by those same column names, so every `$name` placeholder
    // resolves against the matching value regardless of `HashMap` iteration
    // order.
    let mut keyless_rows_copied = 0u64;
    let mut keyless_table_receipts: Vec<(String, u64)> = Vec::new();
    for (table_name, table_rows) in &keyless_table_rows {
        let columns = &table_rows.columns;
        let rows = &table_rows.rows;
        let column_list = columns.join(", ");
        let placeholder_list = columns
            .iter()
            .map(|c| format!("${c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql =
            format!("INSERT INTO {table_name} ({column_list}) VALUES ({placeholder_list})");
        let mut copied_for_table = 0u64;
        for row in rows {
            match tmp_db.execute(&insert_sql, row) {
                Ok(_) => {
                    keyless_rows_copied += 1;
                    copied_for_table += 1;
                }
                Err(err) => {
                    let _ = tmp_db.close();
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = std::fs::remove_file(lock_path_for(&tmp_path));
                    eprintln!(
                        "Error: '{}' has a backup at '{}', but copying keyless rows failed: {err}",
                        path.display(),
                        backup_path.display()
                    );
                    return EXIT_ERROR;
                }
            }
        }
        keyless_table_receipts.push((table_name.clone(), copied_for_table));
    }
    if let Err(err) = tmp_db.close() {
        // Best-effort cleanup of the not-yet-swapped-in tmp root; the error
        // itself is still surfaced either way.
        let _ = std::fs::remove_file(&tmp_path);
        let _ = std::fs::remove_file(lock_path_for(&tmp_path));
        eprintln!(
            "Error: '{}' has a backup at '{}', but the migrated store did not close cleanly: \
             {err}",
            path.display(),
            backup_path.display()
        );
        return EXIT_ERROR;
    }

    // Atomic swap: the migrated root replaces the legacy one in one rename.
    if let Err(err) = std::fs::rename(&tmp_path, path) {
        eprintln!(
            "Error: '{}' has a backup at '{}' and the migrated data is ready at '{}', but the \
             final swap failed: {err}. Rename '{}' to '{}' by hand to finish.",
            path.display(),
            backup_path.display(),
            tmp_path.display(),
            tmp_path.display(),
            path.display()
        );
        return EXIT_ERROR;
    }
    let _ = std::fs::remove_file(lock_path_for(&tmp_path));

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

/// `contextdb repair <path>` — report what is salvageable/diagnosable in
/// a store (which format marker it carries, whether the schema layout is
/// current or legacy), WITHOUT ever modifying it. Uses the same read-only
/// handle `migrate`'s detection does (`RedbPersistence::is_legacy_format_store`)
/// rather than a normal `Database::open` — a plain read-write open performs a
/// housekeeping write on every open regardless of any application
/// transaction, which would break "never modifies the store it inspects" for
/// a store that is actually healthy. This is read-only diagnosis, never
/// in-place repair — `contextdb reset --force` is the recovery command once
/// you've restored any data you need.
fn run_repair(args: &[String]) -> i32 {
    let Some(path_str) = positional_path(args) else {
        eprintln!("Error: repair requires a database path");
        return EXIT_USAGE;
    };
    let path = Path::new(path_str);

    match RedbPersistence::is_legacy_format_store(path) {
        Ok(false) => {
            println!(
                "repair: '{}' is current-format and its schema layout reads cleanly; nothing to \
                 repair.",
                path.display()
            );
            EXIT_OK
        }
        Ok(true) => {
            println!(
                "repair: '{}' is a legacy-format store (its on-disk schema layout predates this \
                 release), not corrupt.\nThis report is read-only; the store was not modified. \
                 Run `contextdb migrate {}` to migrate it in place.",
                path.display(),
                path.display()
            );
            EXIT_ERROR
        }
        Err(err) => {
            println!(
                "repair: '{}' — {err}\nThis report is read-only; the store was not modified. \
                 Run `contextdb reset {} --force` to recreate it (after restoring any needed \
                 data from a backup or a healthy sync peer).",
                path.display(),
                path.display()
            );
            EXIT_ERROR
        }
    }
}
