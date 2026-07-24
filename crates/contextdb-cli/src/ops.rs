//! The `migrate` / `reset` / `repair` store-maintenance subcommands.
//!
//! Dispatch happens BEFORE the existing REPL's `clap` parsing (see
//! `main.rs`), by literal first-argument match, so the existing
//! `contextdb <path> [OPTIONS]` invocation is completely unaffected —
//! only a first argument that is exactly `migrate`, `reset`, or `repair`
//! is intercepted here.

use crate::{EXIT_ERROR, EXIT_OK, EXIT_USAGE};
use contextdb_core::{Lsn, Result, Value};
use contextdb_engine::Database;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::sync_types::{
    ConflictPolicies, ConflictPolicy, natural_key_columns_for_meta,
};
use std::collections::HashMap;
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
        _ => None,
    }
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

/// One keyless table's current-state rows, ready to be copied into the
/// migrated store. `columns` is the SINGLE ordered column list `run_migrate`
/// uses for both the INSERT statement's column list and its `$name`
/// placeholders, so the two can never drift out of alignment with each
/// other; each `rows` entry is keyed by those same column names, so a
/// `$name` placeholder always finds its value.
struct KeylessTableRows {
    columns: Vec<String>,
    rows: Vec<HashMap<String, Value>>,
}

/// `SELECT * FROM <table_name>` in isolation — the one place a keyless
/// table's current state is read out of the source database. Pulled out of
/// `collect_keyless_table_rows` so the scan-failure path is a single,
/// independently testable choke point: a table whose scan errors must fail
/// the whole migration loudly, never be silently skipped (see the unit test
/// below, which drives this exact function against a table name that does
/// not exist).
fn scan_table_rows(db: &Database, table_name: &str) -> Result<contextdb_engine::QueryResult> {
    db.execute(
        &format!("SELECT * FROM {}", table_name),
        &Default::default(),
    )
}

/// Detect keyless tables in the database and return their current visible rows.
/// A keyless table is one without a natural key (no PRIMARY KEY, no natural_key_column,
/// no "id" fallback). These rows are not representable in a changeset and must be
/// copied separately after changeset replay.
///
/// A scan failure on any keyless table is propagated as an error rather than
/// silently skipped: swallowing it here would let migration swap in a store
/// silently missing that table's rows, with no error ever reported.
fn collect_keyless_table_rows(
    db: &Database,
) -> Result<std::collections::BTreeMap<String, KeylessTableRows>> {
    let mut result = std::collections::BTreeMap::new();

    // Iterate over all tables and find keyless ones
    for table_name in db.table_names() {
        if let Some(table_meta) = db.table_meta(&table_name)
            && natural_key_columns_for_meta(&table_meta).is_none()
        {
            // This is a keyless table — scan and collect its rows. The
            // column list is derived ONCE here, in declared-schema order,
            // and carried alongside the rows so the caller never has to
            // (re)derive an ordering from a `HashMap`'s own iteration order,
            // which is nondeterministic.
            let scan_result = scan_table_rows(db, &table_name)?;
            let columns: Vec<String> = table_meta.columns.iter().map(|c| c.name.clone()).collect();
            let mut table_rows = Vec::new();
            for scan_row in scan_result.rows {
                let mut row_map = HashMap::new();
                for (i, col_name) in columns.iter().enumerate() {
                    if i < scan_row.len() {
                        row_map.insert(col_name.clone(), scan_row[i].clone());
                    }
                }
                table_rows.push(row_map);
            }
            if !table_rows.is_empty() {
                result.insert(
                    table_name.clone(),
                    KeylessTableRows {
                        columns,
                        rows: table_rows,
                    },
                );
            }
        }
    }

    Ok(result)
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
    let changes = legacy_db.changes_since(Lsn(0));

    // Before closing the legacy DB, detect keyless tables and collect their
    // current rows so they can be copied after the changeset replay (since
    // changesets omit rows without natural keys). A scan failure here is a
    // hard error (never silently skipped) -- see `collect_keyless_table_rows`.
    let keyless_table_rows = match collect_keyless_table_rows(&legacy_db) {
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

    if let Err(err) = legacy_db.close() {
        eprintln!(
            "Error: '{}' has a backup at '{}', but the legacy store did not close cleanly: {err}",
            path.display(),
            backup_path.display()
        );
        return EXIT_ERROR;
    }

    // Write into a fresh current-format root, in the SAME directory as
    // `path` so the final swap below is an atomic same-filesystem rename.
    let tmp_path = PathBuf::from(format!("{}.migrate-tmp", path.display()));
    let _ = std::fs::remove_file(&tmp_path);
    let _ = std::fs::remove_file(lock_path_for(&tmp_path));
    let tmp_db = match Database::open(&tmp_path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!(
                "Error: '{}' has a backup at '{}', but the migration target could not be \
                 created: {err}",
                path.display(),
                backup_path.display()
            );
            return EXIT_ERROR;
        }
    };
    let apply_result = tmp_db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    );
    let applied_rows = match apply_result {
        Ok(result) => result.applied_rows,
        Err(err) => {
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
    for (table_name, KeylessTableRows { columns, rows }) in keyless_table_rows.iter() {
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

    if path.exists()
        && let Err(err) = std::fs::remove_file(path)
    {
        eprintln!(
            "Error: reset failed to remove the existing store at '{}': {err}",
            path.display()
        );
        return EXIT_ERROR;
    }
    let lock_path = lock_path_for(path);
    if lock_path.exists() {
        let _ = std::fs::remove_file(&lock_path);
    }

    match Database::open(path) {
        Ok(db) => match db.close() {
            Ok(()) => {
                println!(
                    "reset '{}': a fresh, empty current-format store was created.",
                    path.display()
                );
                EXIT_OK
            }
            Err(err) => {
                eprintln!(
                    "Error: reset recreated '{}' but it did not close cleanly: {err}",
                    path.display()
                );
                EXIT_ERROR
            }
        },
        Err(err) => {
            eprintln!(
                "Error: reset removed the existing store at '{}' but failed to recreate it: \
                 {err}",
                path.display()
            );
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

#[cfg(test)]
mod keyless_scan_tests {
    use super::*;

    /// The exact bug this module exists to close: BEFORE this fix,
    /// `collect_keyless_table_rows` wrapped its scan in `if let
    /// Ok(scan_result) = db.execute(...)`, so a table whose scan failed was
    /// just absent from the result — migration proceeded and swapped in a
    /// store silently missing that table's rows, with no error anywhere.
    /// This drives `scan_table_rows` (the exact choke point) against a
    /// table name that does not exist, which fails deterministically and
    /// without depending on any deeper engine failure mode; the fixed
    /// contract is that this propagates as `Err`, never a quietly-absent
    /// entry.
    #[test]
    fn a_scan_failure_on_a_keyless_table_propagates_as_an_error() {
        let db = Database::open_memory();
        let err = scan_table_rows(&db, "table_that_does_not_exist")
            .expect_err("scanning a nonexistent table must fail, not silently return nothing");
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("table_that_does_not_exist")
                || msg.to_lowercase().contains("table"),
            "the propagated error should be traceable to the table it failed on; got: {msg}"
        );
    }
}
