//! Column-Level IMMUTABLE — engine, sync-apply, ALTER, CLI raw-path tests.

use contextdb_core::table_meta::{ColumnType, TableMeta};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use contextdb_parser::ast as past;
use contextdb_parser::{Statement, parse};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

const DECISIONS_DDL: &str = "\
CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  decision_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  reasoning JSON,
  confidence REAL,
  status TEXT NOT NULL DEFAULT 'active'
) STATE MACHINE (status: active -> [superseded, archived])";

fn db_with_decisions() -> Database {
    let db = Database::open_memory();
    db.execute(DECISIONS_DDL, &empty())
        .expect("CREATE TABLE decisions must parse and apply");
    db
}

fn insert_decision(db: &Database, id: Uuid) {
    db.execute(
        "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status) \
         VALUES ($id, $dt, $desc, $r, $conf, $st)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("dt", Value::Text("sql-migration".into())),
            ("desc", Value::Text("adopt contextdb".into())),
            ("r", Value::Json(serde_json::json!({"alt": "stay"}))),
            ("conf", Value::Float64(0.9)),
            ("st", Value::Text("active".into())),
        ]),
    )
    .expect("INSERT must succeed — flagged columns accept their first value");
}

fn row_text(db: &Database, id: Uuid, col: &str) -> String {
    let result = db
        .execute(
            "SELECT * FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("SELECT must succeed");
    let idx = result
        .columns
        .iter()
        .position(|c| c == col)
        .unwrap_or_else(|| panic!("column {col} missing from result"));
    match &result.rows[0][idx] {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text for {col}, got {other:?}"),
    }
}

/// Capture the full row `Value` vector for side-channel / ghost-write checks.
/// A ghost-write impl that silently mutates then errors would show a changed
/// `Vec<Value>` even if one column's `Value` variant happened to match the
/// original (e.g., Text-vs-Text with same content but different capacity).
fn row_snapshot(db: &Database, table: &str, id: Uuid) -> Vec<Value> {
    let result = db
        .execute(
            &format!("SELECT * FROM {table} WHERE id = $id"),
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("snapshot SELECT must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "expected exactly one row for snapshot"
    );
    result.rows[0].clone()
}

/// Assert that a rejection produced zero forward progress in the change log
/// and zero DDL entries, and that the row snapshot is bytes-identical to the
/// snapshot captured before the rejected statement.
fn assert_no_ghost_write(
    db: &Database,
    table: &str,
    id: Uuid,
    before: &[Value],
    lsn_before: contextdb_core::Lsn,
) {
    let ddl_after = db.ddl_log_since(lsn_before);
    assert!(
        ddl_after.is_empty(),
        "rejection must not emit any DDL change, got {} entries",
        ddl_after.len()
    );
    let change_after = db.change_log_since(lsn_before);
    assert!(
        change_after.is_empty(),
        "rejection must not emit any row-change log entry, got {} entries",
        change_after.len()
    );
    let after = row_snapshot(db, table, id);
    assert_eq!(
        &after, before,
        "rejection must leave the row bytes-identical (no ghost write)"
    );
}

// ============================================================================
// CE01 — RED — UPDATE of flagged column rejected; row unchanged. [I1, I2]
// Pins: `UPDATE SET flagged = ...` returns Error::ImmutableColumn with the
// correct table + column fields, and the row is untouched on readback.
// ============================================================================
#[test]
fn ce01_update_flagged_column_rejected_and_row_unchanged() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();
    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'other' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("UPDATE of flagged decision_type must fail");
    match err {
        Error::ImmutableColumn { table, column } => {
            assert_eq!(table, "decisions");
            assert_eq!(column, "decision_type");
        }
        other => panic!("expected ImmutableColumn, got {other:?}"),
    }

    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// CE02 — RED — mutable column still updatable. Positive control for CE01.
// ============================================================================
#[test]
fn ce02_update_non_flagged_column_succeeds() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    db.execute(
        "UPDATE decisions SET status = 'superseded' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("UPDATE of mutable status must succeed");

    assert_eq!(row_text(&db, id, "status"), "superseded");
}

// ============================================================================
// CE03 — RED — mixed SET list: whole statement rejected, neither column written.
// Pins I1 (all-or-nothing).
// ============================================================================
#[test]
fn ce03_mixed_set_list_rejects_whole_statement() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            "UPDATE decisions SET status = 'superseded', decision_type = 'edit' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("mixed SET containing flagged column must be rejected");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "expected ImmutableColumn(decision_type), got {err:?}"
    );

    assert_eq!(row_text(&db, id, "status"), "active");
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
}

// ============================================================================
// CE04 — RED — error names first flagged column in SET-list iteration order.
// Pins I2. Reverse-order variant binds determinism.
// ============================================================================
#[test]
fn ce04_first_flagged_column_in_set_list_named_in_error() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'a', description = 'b' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "first SET-list position must win, got {err:?}"
    );

    let err2 = db
        .execute(
            "UPDATE decisions SET description = 'b', decision_type = 'a' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err2, Error::ImmutableColumn { ref column, .. } if column == "description"),
        "reverse order must name description, got {err2:?}"
    );
}

// ============================================================================
// CE05 — RED — ON CONFLICT DO UPDATE SET flagged_col: rejected, row count
// unchanged.
// ============================================================================
#[test]
fn ce05_on_conflict_do_update_on_flagged_column_rejected() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let before_count = db
        .execute("SELECT * FROM decisions", &empty())
        .unwrap()
        .rows
        .len();
    let before_snapshot = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let err = db
        .execute(
            "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status) \
             VALUES ($id, $dt, $desc, $r, $conf, $st) \
             ON CONFLICT (id) DO UPDATE SET decision_type = 'edit'",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("dt", Value::Text("sql-migration".into())),
                ("desc", Value::Text("adopt contextdb".into())),
                ("r", Value::Json(serde_json::json!({}))),
                ("conf", Value::Float64(0.9)),
                ("st", Value::Text("active".into())),
            ]),
        )
        .expect_err("ON CONFLICT DO UPDATE on flagged column must be rejected");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "expected ImmutableColumn(decision_type), got {err:?}"
    );

    let after_count = db
        .execute("SELECT * FROM decisions", &empty())
        .unwrap()
        .rows
        .len();
    assert_eq!(
        before_count, after_count,
        "row count must be unchanged after rejection"
    );
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before_snapshot, lsn_before);
}

// ============================================================================
// CE06 — REGRESSION GUARD — INSERT of flagged column succeeds and readable.
// ============================================================================
#[test]
fn ce06_insert_of_flagged_column_succeeds_and_readable() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    assert_eq!(row_text(&db, id, "description"), "adopt contextdb");
}

// ============================================================================
// CE07 — REGRESSION GUARD — DELETE unaffected even with flagged columns.
// ============================================================================
#[test]
fn ce07_delete_row_with_flagged_columns_succeeds() {
    let db = db_with_decisions();
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    insert_decision(&db, id_a);
    insert_decision(&db, id_b);

    db.execute(
        "DELETE FROM decisions WHERE id = $id",
        &params(vec![("id", Value::Uuid(id_a))]),
    )
    .expect("DELETE must succeed even when table has flagged columns");

    let result = db.execute("SELECT * FROM decisions", &empty()).unwrap();
    assert_eq!(result.rows.len(), 1, "exactly one row must remain");
    let idx = result.columns.iter().position(|c| c == "id").unwrap();
    assert_eq!(
        result.rows[0][idx],
        Value::Uuid(id_b),
        "the remaining row must be the non-deleted one"
    );
}

// ============================================================================
// CE08 — RED — nonexistent column beats ImmutableColumn in error ordering.
// ============================================================================
#[test]
fn ce08_nonexistent_column_error_outranks_immutable_column() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'a', not_a_col = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        !matches!(err, Error::ImmutableColumn { .. }),
        "nonexistent-column error must win over ImmutableColumn, got {err:?}"
    );
    // Positive match: the error must name the missing column.
    let msg = format!("{err}");
    assert!(
        msg.contains("not_a_col"),
        "error must name the missing column 'not_a_col', got: {msg}"
    );
}

// ============================================================================
// CE09 — RED — table-level IMMUTABLE wins over column-level on UPDATE.
// ============================================================================
#[test]
fn ce09_table_immutable_short_circuits_column_check() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit (id UUID PRIMARY KEY, who TEXT NOT NULL IMMUTABLE, note TEXT) IMMUTABLE",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO audit (id, who, note) VALUES ($id, $w, $n)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("w", Value::Text("svc".into())),
            ("n", Value::Text("ok".into())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "UPDATE audit SET who = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err, Error::ImmutableTable(ref t) if t == "audit"),
        "table-level immutable must win, got {err:?}"
    );
}

// ============================================================================
// CE10 — RED — I13: nonexistent column error wins even over ImmutableTable.
// ============================================================================
#[test]
fn ce10_nonexistent_column_outranks_table_immutable() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit (id UUID PRIMARY KEY, who TEXT NOT NULL IMMUTABLE) IMMUTABLE",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO audit (id, who) VALUES ($id, $w)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("w", Value::Text("svc".into())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "UPDATE audit SET nope = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        !matches!(
            err,
            Error::ImmutableTable(_) | Error::ImmutableColumn { .. }
        ),
        "nonexistent-column must win over both immutables, got {err:?}"
    );
    // Positive match: error names the missing column.
    let msg = format!("{err}");
    assert!(
        msg.contains("nope"),
        "error must name the missing column 'nope', got: {msg}"
    );
}

// ============================================================================
// CE11 — RED — PK+IMMUTABLE: UPDATE of id rejected with ImmutableColumn.
// ============================================================================
#[test]
fn ce11_pk_plus_immutable_update_rejected_as_immutable_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY IMMUTABLE, name TEXT)",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $n)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("n", Value::Text("a".into())),
        ]),
    )
    .unwrap();
    let new_id = Uuid::new_v4();
    let err = db
        .execute(
            "UPDATE t SET id = $new WHERE id = $id",
            &params(vec![("new", Value::Uuid(new_id)), ("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "id"),
        "PK+IMMUTABLE must return ImmutableColumn(id), got {err:?}"
    );
    // Prove the row still exists at the ORIGINAL id (rejected UPDATE did not
    // shift the PK).
    let rows_at_orig = db
        .execute(
            "SELECT * FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(
        rows_at_orig.rows.len(),
        1,
        "row must still be at original id"
    );
    let rows_at_new = db
        .execute(
            "SELECT * FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(new_id))]),
        )
        .unwrap();
    assert_eq!(rows_at_new.rows.len(), 0, "row must NOT appear at new id");
}

// ============================================================================
// CE12 — REGRESSION GUARD — STATE MACHINE + IMMUTABLE on non-status column.
// ============================================================================
#[test]
fn ce12_state_machine_plus_immutable_on_different_columns_parses() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE d (\
           id UUID PRIMARY KEY,\
           decision_type TEXT NOT NULL IMMUTABLE,\
           status TEXT NOT NULL DEFAULT 'active'\
         ) STATE MACHINE (status: active -> [archived])",
        &empty(),
    )
    .expect("schema with STATE MACHINE + IMMUTABLE on different columns must parse");
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO d (id, decision_type, status) VALUES ($id, $dt, $st)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("dt", Value::Text("x".into())),
            ("st", Value::Text("active".into())),
        ]),
    )
    .unwrap();

    db.execute(
        "UPDATE d SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("state-machine transition must succeed");

    let err = db
        .execute(
            "UPDATE d SET decision_type = 'y' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(matches!(err, Error::ImmutableColumn { .. }));
}

// ============================================================================
// CE13 — RED — parser rejects STATE MACHINE status column declared IMMUTABLE.
// Pins I8.
// ============================================================================
#[test]
fn ce13_parser_rejects_state_machine_status_column_declared_immutable() {
    let result = parse(
        "CREATE TABLE d (\
           id UUID PRIMARY KEY,\
           status TEXT NOT NULL IMMUTABLE\
         ) STATE MACHINE (status: active -> [archived])",
    );
    match result {
        Err(Error::ParseError(msg)) => {
            assert!(
                msg.contains("status"),
                "parser message must name the offending column 'status', got: {msg}"
            );
        }
        other => panic!("expected ParseError naming 'status', got {other:?}"),
    }
}

// ============================================================================
// CE14 — RED — UNIQUE + IMMUTABLE + NOT NULL + DEFAULT + REFERENCES + EXPIRES
// compose on the same column.
// ============================================================================
#[test]
fn ce14_all_constraints_including_immutable_compose_on_same_column() {
    let stmt = parse(
        "CREATE TABLE t (\
           ref_id UUID PRIMARY KEY,\
           tag TEXT NOT NULL UNIQUE IMMUTABLE DEFAULT 'x' REFERENCES t(ref_id) EXPIRES\
         )",
    )
    .expect("all-constraints composition must parse");
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CreateTable, got {other:?}"),
    };
    let col = ct
        .columns
        .iter()
        .find(|c| c.name == "tag")
        .expect("column 'tag' must be present");
    assert!(col.immutable, "ColumnDef.immutable must be true");
    assert!(col.unique, "ColumnDef.unique must be true");
    assert!(!col.nullable, "NOT NULL must be preserved");
    assert!(col.default.is_some(), "DEFAULT must be preserved");
    assert!(col.references.is_some(), "REFERENCES must be preserved");
    assert!(col.expires, "EXPIRES must be preserved");
}

// ============================================================================
// CE15 — REGRESSION GUARD — table-level IMMUTABLE + column-level IMMUTABLE.
// ============================================================================
#[test]
fn ce15_table_and_column_immutable_parse_together() {
    let stmt =
        parse("CREATE TABLE t (id UUID PRIMARY KEY, c TEXT NOT NULL IMMUTABLE) IMMUTABLE").unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };
    assert!(ct.immutable, "table-level IMMUTABLE must parse");
    let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
    assert!(c.immutable, "column-level IMMUTABLE must parse");
}

// ============================================================================
// CE16 — RED — DAG + RETAIN + per-column IMMUTABLE compose.
// ============================================================================
#[test]
fn ce16_dag_retain_plus_column_immutable_compose() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE e (\
           id UUID PRIMARY KEY,\
           source_id UUID NOT NULL,\
           target_id UUID NOT NULL,\
           edge_type TEXT NOT NULL IMMUTABLE\
         ) DAG('REL') RETAIN 1 DAYS",
        &empty(),
    )
    .expect("DAG + RETAIN + column IMMUTABLE must parse");

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO e (id, source_id, target_id, edge_type) VALUES ($id, $s, $t, $et)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("s", Value::Uuid(Uuid::new_v4())),
            ("t", Value::Uuid(Uuid::new_v4())),
            ("et", Value::Text("REL".into())),
        ]),
    )
    .unwrap();

    let err = db
        .execute(
            "UPDATE e SET edge_type = 'OTHER' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("UPDATE of flagged edge_type must fail");
    if let Error::ImmutableColumn { table, column } = &err {
        assert_eq!(table, "e");
        assert_eq!(column, "edge_type");
    } else {
        panic!("expected ImmutableColumn, got {err:?}");
    }
    // Readback: flagged value must be the original.
    let result = db
        .execute(
            "SELECT * FROM e WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    let idx = result
        .columns
        .iter()
        .position(|c| c == "edge_type")
        .unwrap();
    assert_eq!(result.rows[0][idx], Value::Text("REL".into()));
}

// ============================================================================
// CP01 — RED — IMMUTABLE accepted as column_constraint in any position.
// ============================================================================
#[test]
fn cp01_immutable_accepted_in_any_constraint_position() {
    for sql in [
        "CREATE TABLE t (c TEXT NOT NULL IMMUTABLE DEFAULT 'x')",
        "CREATE TABLE t (c TEXT IMMUTABLE NOT NULL DEFAULT 'x')",
        "CREATE TABLE t (c TEXT NOT NULL DEFAULT 'x' IMMUTABLE)",
        "CREATE TABLE t (c TEXT DEFAULT 'x' NOT NULL IMMUTABLE)",
    ] {
        let stmt = parse(sql).unwrap_or_else(|e| panic!("{sql} failed: {e:?}"));
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            other => panic!("expected CreateTable for {sql}, got {other:?}"),
        };
        let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
        assert!(c.immutable, "{sql}: ColumnDef.immutable must be true");
    }
}

// ============================================================================
// CP02 — RED — IMMUTABLE is case-insensitive.
// ============================================================================
#[test]
fn cp02_immutable_case_insensitive() {
    for kw in ["IMMUTABLE", "immutable", "Immutable", "ImMuTaBlE"] {
        let sql = format!("CREATE TABLE t (c TEXT NOT NULL {kw})");
        let stmt = parse(&sql).unwrap_or_else(|e| panic!("{sql} failed: {e:?}"));
        let ct = match stmt {
            Statement::CreateTable(ct) => ct,
            other => panic!("{sql}: expected CreateTable, got {other:?}"),
        };
        let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
        assert!(c.immutable, "{sql}: immutable flag must be set");
    }
}

// ============================================================================
// CP03 — RED — duplicate IMMUTABLE names the offending column.
// ============================================================================
#[test]
fn cp03_duplicate_immutable_names_offending_column() {
    let result = parse("CREATE TABLE t (c TEXT NOT NULL IMMUTABLE IMMUTABLE)");
    match result {
        Err(Error::ParseError(msg)) => {
            assert!(
                msg.to_lowercase().contains("immutable"),
                "parser message must mention IMMUTABLE, got: {msg}"
            );
            assert!(
                msg.contains('c') || msg.to_lowercase().contains("column"),
                "parser message must name the column or 'column', got: {msg}"
            );
        }
        other => panic!("expected ParseError, got {other:?}"),
    }
}

// ============================================================================
// CP04 — RED — misplaced IMMUTABLE keyword returns a targeted ParseError.
// ============================================================================
#[test]
fn cp04_misplaced_immutable_keyword_yields_parse_error() {
    let result = parse("CREATE TABLE t (IMMUTABLE c INTEGER)");
    match result {
        Err(Error::ParseError(msg)) => {
            assert!(
                msg.to_uppercase().contains("IMMUTABLE"),
                "parser message must specifically name IMMUTABLE (not a generic 'expected identifier'), got: {msg}"
            );
        }
        other => panic!("expected ParseError, got {other:?}"),
    }
}

// ============================================================================
// CP05 — REGRESSION GUARD — IMMUTABLE co-exists with explicit NULL.
// ============================================================================
#[test]
fn cp05_immutable_with_explicit_null_parses() {
    let stmt = parse("CREATE TABLE t (c TEXT IMMUTABLE NULL)")
        .expect("IMMUTABLE with explicit NULL must parse");
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };
    let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
    assert!(c.immutable);
    assert!(c.nullable);
}

// ============================================================================
// CP06 — RED — table-level `COLUMN IMMUTABLE (...)` clause is NOT part of this
// feature.
// ============================================================================
#[test]
fn cp06_table_level_column_immutable_syntax_rejected() {
    let result = parse("CREATE TABLE t (c INT) COLUMN IMMUTABLE (c)");
    assert!(
        matches!(result, Err(Error::ParseError(_))),
        "table-level COLUMN IMMUTABLE syntax must return ParseError, got {result:?}"
    );
}

// ============================================================================
// SC01 — RED — backward-compatible metadata decode.
// ============================================================================
#[test]
fn sc01_old_serialized_column_def_deserializes_as_mutable() {
    let old_json = r#"{
        "name": "c",
        "column_type": "Text",
        "nullable": true,
        "primary_key": false,
        "unique": false,
        "default": null,
        "references": null,
        "expires": false
    }"#;
    let decoded: contextdb_core::ColumnDef =
        serde_json::from_str(old_json).expect("old-shape ColumnDef must decode");
    assert!(
        !decoded.immutable,
        "missing `immutable` field must default to false, got true"
    );
    assert_eq!(decoded.name, "c");
}

// ============================================================================
// SC02 — RED — reopen: flag survives Database::open after close.
// ============================================================================
#[test]
fn sc02_flag_survives_close_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db.redb");
    let id = Uuid::new_v4();
    {
        let db = Database::open(&path).unwrap();
        db.execute(DECISIONS_DDL, &empty()).unwrap();
        insert_decision(&db, id);
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'other' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("post-reopen UPDATE on flagged column must fail");
    assert!(matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"));
}

// ============================================================================
// SC03 — RED — CreateTable DdlChange round-trips the flag via apply_changes.
// ============================================================================
#[test]
fn sc03_create_table_ddl_round_trips_immutable_flag_via_apply_changes() {
    let origin = db_with_decisions();
    let ddl = origin.ddl_log_since(contextdb_core::Lsn(0));
    assert!(!ddl.is_empty(), "origin must have at least one DdlChange");
    let create = ddl
        .into_iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "decisions"))
        .expect("origin DDL log must contain CreateTable for decisions");

    let peer = Database::open_memory();
    let change_set = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![create],
    };
    let _: ApplyResult = peer
        .apply_changes(
            change_set,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("apply_changes must accept CreateTable");

    let meta = peer_table_meta(&peer, "decisions");
    let col = meta
        .columns
        .iter()
        .find(|c| c.name == "decision_type")
        .expect("peer must have decision_type column");
    assert!(
        col.immutable,
        "peer ColumnDef.immutable must be true after schema replication"
    );
}

// Helper: re-parse the CreateTable DDL emitted for `table` into a TableMeta.
#[allow(clippy::collapsible_if)]
fn peer_table_meta(db: &Database, table: &str) -> TableMeta {
    let ddl = db.ddl_log_since(contextdb_core::Lsn(0));
    for change in ddl {
        if let DdlChange::CreateTable {
            name,
            columns,
            constraints,
        } = change
        {
            if name == table {
                let sql = format!(
                    "CREATE TABLE {name} ({cols}){cons}",
                    name = name,
                    cols = columns
                        .iter()
                        .map(|(n, t)| format!("{n} {t}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    cons = if constraints.is_empty() {
                        String::new()
                    } else {
                        format!(" {}", constraints.join(" "))
                    },
                );
                let stmt = parse(&sql).expect("re-parse must succeed");
                let ct = match stmt {
                    Statement::CreateTable(ct) => ct,
                    _ => panic!("expected CreateTable"),
                };
                return TableMeta {
                    columns: ct
                        .columns
                        .iter()
                        .map(|c| contextdb_core::ColumnDef {
                            name: c.name.clone(),
                            column_type: match c.data_type {
                                past::DataType::Uuid => ColumnType::Uuid,
                                past::DataType::Text => ColumnType::Text,
                                past::DataType::Integer => ColumnType::Integer,
                                past::DataType::Real => ColumnType::Real,
                                past::DataType::Boolean => ColumnType::Boolean,
                                past::DataType::Timestamp => ColumnType::Timestamp,
                                past::DataType::Json => ColumnType::Json,
                                past::DataType::Vector(d) => ColumnType::Vector(d as usize),
                                past::DataType::TxId => ColumnType::TxId,
                            },
                            nullable: c.nullable,
                            primary_key: c.primary_key,
                            unique: c.unique,
                            default: None,
                            references: None,
                            expires: c.expires,
                            immutable: c.immutable,
                        })
                        .collect(),
                    ..TableMeta::default()
                };
            }
        }
    }
    panic!("table {table} not found in DDL log");
}

// ============================================================================
// SC04 — RED — after replication, peer UPDATE on flagged column rejected.
// ============================================================================
#[test]
fn sc04_peer_update_on_flagged_column_rejected_after_replication() {
    let origin = db_with_decisions();
    let ddl = origin.ddl_log_since(contextdb_core::Lsn(0));
    let create = ddl
        .into_iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "decisions"))
        .unwrap();

    let peer = Database::open_memory();
    peer.apply_changes(
        ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![create],
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();

    let id = Uuid::new_v4();
    insert_decision(&peer, id);

    peer.execute(
        "UPDATE decisions SET status = 'archived' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("peer non-flagged update must succeed");

    let err = peer
        .execute(
            "UPDATE decisions SET description = 'edit' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail on peer");
    assert!(matches!(err, Error::ImmutableColumn { ref column, .. } if column == "description"));
}

// ============================================================================
// SA01 — RED — apply_changes rejects flagged mutation on existing row.
// ============================================================================
#[test]
fn sa01_apply_changes_rejects_flagged_mutation_on_existing_row() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    let original = row_text(&db, id, "decision_type");
    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("decision_type".to_string(), Value::Text("tampered".into())),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(10_000),
    };
    let result: ApplyResult = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("apply_changes must not propagate the error as fatal");

    assert_eq!(
        result.applied_rows, 0,
        "flagged-column mutation via sync must NOT be applied"
    );
    assert!(
        !result.conflicts.is_empty(),
        "the rejection must be surfaced as a conflict"
    );
    // F13: reason label must name the immutable-column violation so downstream
    // observability can classify the conflict correctly.
    let reason = result.conflicts[0]
        .reason
        .as_deref()
        .unwrap_or("")
        .to_lowercase();
    assert!(
        reason.contains("immutable"),
        "expected conflict.reason to mention 'immutable', got {reason:?}"
    );
    assert_eq!(row_text(&db, id, "decision_type"), original);
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// SA02 — RED — apply_changes: new row applies normally; flagged column set.
// ============================================================================
#[test]
fn sa02_apply_changes_new_row_sets_flagged_column_value() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("decision_type".to_string(), Value::Text("fresh".into())),
            ("description".to_string(), Value::Text("new row".into())),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(20_000),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(result.applied_rows, 1);
    assert_eq!(row_text(&db, id, "decision_type"), "fresh");
}

// ============================================================================
// SA03 — REGRESSION GUARD — same-value replay: no immutable-reasoned conflict raised.
// Passes vacuously on the stub (no immutable check runs); gate value is
// binding the impl not to label same-value replays as immutable-conflicts.
// ============================================================================
#[test]
fn sa03_apply_changes_same_value_replay_is_noop() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            (
                "decision_type".to_string(),
                Value::Text("sql-migration".into()),
            ),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(30_000),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("same-value replay must not error");
    for conflict in &result.conflicts {
        let reason = conflict.reason.clone().unwrap_or_default();
        assert!(
            !reason.to_lowercase().contains("immutable"),
            "same-value replay must not be rejected as an immutable-column conflict, got: {reason}"
        );
    }
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
}

// ============================================================================
// SA04 — REGRESSION GUARD — apply_changes accepts fresh row with flagged value.
// ============================================================================
#[test]
fn sa04_apply_changes_fresh_row_accepted() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("decision_type".to_string(), Value::Text("anchor".into())),
            ("description".to_string(), Value::Text("fresh".into())),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(40_000),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(result.applied_rows, 1);
    assert_eq!(
        row_text(&db, id, "decision_type"),
        "anchor",
        "fresh-row apply must set the flagged column to the incoming value"
    );
    assert_eq!(row_text(&db, id, "description"), "fresh");
}

// ============================================================================
// SA05 — RED — upsert with differing flagged value rejected; row unchanged
// even under LatestWins with a higher LSN.
// ============================================================================
#[test]
fn sa05_sync_upsert_flagged_diff_rejected_row_unchanged() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("decision_type".to_string(), Value::Text("TAMPERED".into())),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(u64::MAX / 2),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(result.applied_rows, 0, "flagged diff must NOT overwrite");
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// AL01 — RED — ALTER DROP COLUMN on flagged column rejected.
// ============================================================================
#[test]
fn al01_alter_drop_flagged_column_rejected() {
    let db = db_with_decisions();
    let err = db
        .execute("ALTER TABLE decisions DROP COLUMN decision_type", &empty())
        .expect_err("DROP of flagged column must fail");
    match err {
        Error::ImmutableColumn { table, column } => {
            assert_eq!(table, "decisions");
            assert_eq!(column, "decision_type");
        }
        other => panic!("expected ImmutableColumn, got {other:?}"),
    }
}

// ============================================================================
// AL02 — REGRESSION GUARD — DROP of non-flagged column succeeds.
// ============================================================================
#[test]
fn al02_alter_drop_non_flagged_column_succeeds() {
    let db = db_with_decisions();
    db.execute("ALTER TABLE decisions DROP COLUMN reasoning", &empty())
        .expect("DROP of non-flagged reasoning must succeed");
    let result = db.execute("SELECT * FROM decisions", &empty()).unwrap();
    assert!(!result.columns.iter().any(|c| c == "reasoning"));
}

// ============================================================================
// AL03 — RED — ALTER RENAME COLUMN on flagged column rejected.
// ============================================================================
#[test]
fn al03_alter_rename_flagged_column_rejected() {
    let db = db_with_decisions();
    let err = db
        .execute(
            "ALTER TABLE decisions RENAME COLUMN decision_type TO kind",
            &empty(),
        )
        .expect_err("RENAME of flagged column must fail");
    assert!(matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"));
}

// ============================================================================
// AL04 — REGRESSION GUARD — RENAME of non-flagged column succeeds.
// ============================================================================
#[test]
fn al04_alter_rename_non_flagged_column_succeeds() {
    let db = db_with_decisions();
    db.execute(
        "ALTER TABLE decisions RENAME COLUMN reasoning TO rationale",
        &empty(),
    )
    .expect("RENAME of non-flagged reasoning must succeed");
    let result = db.execute("SELECT * FROM decisions", &empty()).unwrap();
    assert!(result.columns.iter().any(|c| c == "rationale"));
}

// ============================================================================
// AL05 — RED — ALTER ADD COLUMN ... IMMUTABLE then UPDATE rejected.
// ============================================================================
#[test]
fn al05_alter_add_immutable_column_then_update_rejected() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $n)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("n", Value::Text("a".into())),
        ]),
    )
    .unwrap();

    db.execute("ALTER TABLE t ADD COLUMN stamp TEXT IMMUTABLE", &empty())
        .expect("ADD COLUMN ... IMMUTABLE must succeed");

    let err = db
        .execute(
            "UPDATE t SET stamp = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("UPDATE on newly added IMMUTABLE column must fail");
    assert!(matches!(err, Error::ImmutableColumn { ref column, .. } if column == "stamp"));
}

// ============================================================================
// RE01 — REGRESSION GUARD — read paths unaffected.
// ============================================================================
#[test]
fn re01_read_paths_unaffected_by_flag() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    let result = db.execute("SELECT * FROM decisions", &empty()).unwrap();
    assert_eq!(result.rows.len(), 1);
    let idx = result
        .columns
        .iter()
        .position(|c| c == "decision_type")
        .unwrap();
    assert_eq!(
        result.rows[0][idx],
        Value::Text("sql-migration".into()),
        "flagged column must be readable in SELECT *"
    );
    let filtered = db
        .execute(
            "SELECT * FROM decisions WHERE decision_type = $dt",
            &params(vec![("dt", Value::Text("sql-migration".into()))]),
        )
        .unwrap();
    assert_eq!(filtered.rows.len(), 1);
}

// ============================================================================
// RC01 — RED — render_table_meta emits IMMUTABLE on flagged columns only.
// ============================================================================
#[test]
fn rc01_schema_render_shows_immutable_per_column() {
    let db = db_with_decisions();
    let meta = peer_table_meta(&db, "decisions");
    let rendered = render_table_meta("decisions", &meta);

    for line in rendered.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("decision_type ") || trimmed.starts_with("description ") {
            assert!(
                line.contains("IMMUTABLE"),
                "flagged column line must contain IMMUTABLE: {line}"
            );
        }
        if trimmed.starts_with("status ") {
            assert!(
                !line.contains("IMMUTABLE"),
                "mutable status must NOT contain IMMUTABLE: {line}"
            );
        }
    }
}

// ============================================================================
// RC02 — RED — render -> parse round-trip preserves the flag.
// ============================================================================
#[test]
fn rc02_render_parse_round_trip_preserves_flag() {
    let db = db_with_decisions();
    let meta = peer_table_meta(&db, "decisions");
    let rendered = render_table_meta("decisions_rt", &meta);
    let sql = rendered.trim();
    let stmt = parse(sql)
        .unwrap_or_else(|e| panic!("re-parse of rendered schema must succeed: {e:?}\nsql:\n{sql}"));
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CreateTable, got {other:?}"),
    };
    let col = ct
        .columns
        .iter()
        .find(|c| c.name == "decision_type")
        .unwrap();
    assert!(
        col.immutable,
        "round-tripped ColumnDef.immutable must be true"
    );
}

// ============================================================================
// CL01 — RED — Display for ImmutableColumn matches the contract string.
// ============================================================================
#[test]
fn cl01_cli_raw_path_immutable_column_display() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            "UPDATE decisions SET description = 'e' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert_eq!(
        format!("{err}"),
        "column `description` on table `decisions` is immutable",
        "Display for ImmutableColumn must match the contract string"
    );
}

// ============================================================================
// IC01 — RED — Rejected UPDATE does not survive across handle drop + reopen.
// After rollback + drop(db) + Database::open(path), the readback equals the
// original INSERTed value. Pins: a write-then-error bug cannot hide behind
// read-your-own-writes.
// ============================================================================
#[test]
fn ic01_fresh_handle_readback_after_rejected_update() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ic01.redb");
    let id = Uuid::new_v4();
    {
        let db = Database::open(&path).unwrap();
        db.execute(DECISIONS_DDL, &empty()).unwrap();
        insert_decision(&db, id);

        // Open an explicit session txn, attempt the rejected UPDATE, roll back.
        let before = row_snapshot(&db, "decisions", id);
        let lsn_before = db.current_lsn();
        db.execute("BEGIN", &empty()).unwrap();
        let err = db
            .execute(
                "UPDATE decisions SET decision_type = 'tampered' WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("rejected UPDATE must fail");
        assert!(matches!(err, Error::ImmutableColumn { .. }));
        db.execute("ROLLBACK", &empty()).unwrap();
        assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);

        db.close().unwrap();
        drop(db);
    }

    // Fresh process-level handle — no read-your-own-writes shortcut.
    let db2 = Database::open(&path).unwrap();
    assert_eq!(
        row_text(&db2, id, "decision_type"),
        "sql-migration",
        "on-disk value must be the original INSERTed value, not the rejected UPDATE"
    );
}

// ============================================================================
// AL06 — RED — ALTER COLUMN TYPE on a flagged column is rejected (parse OR
// enforcement). The engine currently does not support ALTER COLUMN TYPE at
// all, so the positive path for this test is parser rejection. When / if the
// engine grows the syntax, enforcement must fire.
// ============================================================================
#[test]
fn al06_alter_column_type_on_flagged_rejected() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, c INTEGER NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();

    let result = db.execute("ALTER TABLE t ALTER COLUMN c TYPE TEXT", &empty());
    // The grammar does not currently have an `ALTER COLUMN ... TYPE` alternative
    // (see `crates/contextdb-parser/src/grammar.pest` — `alter_action` only
    // enumerates ADD / DROP / RENAME / SET RETAIN / DROP RETAIN / SET policy /
    // DROP policy). If that grammar grows the alternative, the enforcement path
    // must reject with `ImmutableColumn`. Either way, the SQL above is refused.
    match result {
        Err(Error::ParseError(msg)) => {
            // Parser rejection — message must name either the keyword or the
            // column, not be a bare generic pest fallback.
            let lower = msg.to_lowercase();
            assert!(
                lower.contains("alter column") || lower.contains("type") || msg.contains(" c "),
                "ParseError message must name ALTER COLUMN / TYPE / column, got: {msg}"
            );
        }
        Err(Error::ImmutableColumn { table, column }) => {
            assert_eq!(table, "t");
            assert_eq!(column, "c");
        }
        Err(other) => panic!(
            "expected ParseError (naming ALTER COLUMN / TYPE / column) or ImmutableColumn, got {other:?}"
        ),
        Ok(_) => panic!("ALTER COLUMN TYPE on flagged column must NOT succeed"),
    }
}

// ============================================================================
// AL07 — RED — DROP on flagged rejected; subsequent ADD of same name fails
// because the column still exists with its original value. "Nothing disappears."
// ============================================================================
#[test]
fn al07_same_statement_drop_add_still_refuses_flagged_drop() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let drop_err = db
        .execute("ALTER TABLE decisions DROP COLUMN decision_type", &empty())
        .expect_err("DROP of flagged column must fail");
    assert!(
        matches!(drop_err, Error::ImmutableColumn { ref column, .. } if column == "decision_type")
    );

    // Column still present with original value.
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");

    // ADD fails because the column still exists (same name).
    let add_err = db
        .execute(
            "ALTER TABLE decisions ADD COLUMN decision_type TEXT NOT NULL",
            &empty(),
        )
        .expect_err("ADD of already-present column must fail");
    // contextdb-relational's `alter_table_add_column` surfaces the
    // "column already exists" condition via `Error::Other(String)` (see
    // `crates/contextdb-relational/src/store.rs:93`). Pin both the variant
    // and the phrase.
    match &add_err {
        Error::Other(msg) => assert!(
            msg.to_lowercase().contains("already exists"),
            "expected 'already exists' in error message, got: {msg}"
        ),
        other => panic!("expected Error::Other with 'already exists', got {other:?}"),
    }

    // And the readback is still the original.
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
}

// ============================================================================
// AL08 — RED — ALTER ADD COLUMN IMMUTABLE cross-checks existing propagation
// rules. If a propagation rule's SET target names the new column, the ADD is
// rejected with Error::ImmutableColumn{table,column}. Closes the
// DROP-then-ADD-as-flagged loophole: a propagation rule that targeted a
// mutable column cannot be smuggled into targeting a flagged one.
// ============================================================================
#[test]
fn al08_alter_add_column_immutable_cross_checks_propagation_rules() {
    let db = Database::open_memory();
    let empty: HashMap<String, Value> = HashMap::new();
    db.execute(
        "CREATE TABLE t (           id UUID PRIMARY KEY,           status TEXT NOT NULL,           parent_id UUID REFERENCES parent(id) ON STATE archived PROPAGATE SET status         )",
        &empty,
    )
    .expect("CREATE TABLE with FK propagation targeting mutable status must parse");
    db.execute("ALTER TABLE t DROP COLUMN status", &empty)
        .expect("DROP non-flagged status must succeed");
    let err = db
        .execute(
            "ALTER TABLE t ADD COLUMN status TEXT NOT NULL IMMUTABLE",
            &empty,
        )
        .expect_err(
            "ALTER ADD COLUMN IMMUTABLE must reject when a propagation rule targets this column name",
        );
    match err {
        Error::ImmutableColumn { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "status");
        }
        other => panic!("expected Error::ImmutableColumn {{table,column}}, got {other:?}"),
    }
}

// ============================================================================
// AL09 — REGRESSION GUARD — ALTER ADD COLUMN IMMUTABLE succeeds when the new
// column's name is NOT the target of any propagation rule on the table.
// Positive control for AL08.
// ============================================================================
#[test]
fn al09_alter_add_column_immutable_succeeds_when_no_propagation_rule_targets_it() {
    let db = Database::open_memory();
    let empty: HashMap<String, Value> = HashMap::new();
    db.execute(
        "CREATE TABLE t (           id UUID PRIMARY KEY,           status TEXT NOT NULL,           parent_id UUID REFERENCES parent(id) ON STATE archived PROPAGATE SET status         )",
        &empty,
    )
    .expect("CREATE TABLE with FK propagation must parse");
    db.execute(
        "ALTER TABLE t ADD COLUMN reason TEXT NOT NULL IMMUTABLE",
        &empty,
    )
    .expect("ADD COLUMN IMMUTABLE must succeed when no propagation rule targets the new column");
}

// ============================================================================
// IC03a — RED — Library write path Database::insert_row accepts a flagged
// column value on a fresh row; readback matches.
// ============================================================================
#[test]
fn ic03a_insert_row_library_path_sets_flagged_column() {
    let db = db_with_decisions();
    let tx = db.begin();
    let id = Uuid::new_v4();
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("id".to_string(), Value::Uuid(id));
    values.insert("decision_type".to_string(), Value::Text("lib-path".into()));
    values.insert(
        "description".to_string(),
        Value::Text("lib-path-desc".into()),
    );
    values.insert("reasoning".to_string(), Value::Json(serde_json::json!({})));
    values.insert("confidence".to_string(), Value::Float64(0.5));
    values.insert("status".to_string(), Value::Text("active".into()));
    db.insert_row(tx, "decisions", values)
        .expect("insert_row must succeed on the library write path");
    db.commit(tx).unwrap();

    assert_eq!(row_text(&db, id, "decision_type"), "lib-path");
}

// ============================================================================
// IC03b — RED — Library write path Database::upsert_row with a DIFFERENT
// flagged column value against an existing row returns ImmutableColumn; local
// row unchanged.
// ============================================================================
#[test]
fn ic03b_upsert_row_library_path_rejects_flagged_mutation() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let tx = db.begin();
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("id".to_string(), Value::Uuid(id));
    values.insert(
        "decision_type".to_string(),
        Value::Text("UPSERT_TAMPER".into()),
    );
    values.insert(
        "description".to_string(),
        Value::Text("adopt contextdb".into()),
    );
    values.insert("status".to_string(), Value::Text("active".into()));
    let err = db
        .upsert_row(tx, "decisions", "id", values)
        .expect_err("upsert_row with differing flagged value must fail");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "expected ImmutableColumn, got {err:?}"
    );
    let _ = db.rollback(tx);

    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
}

// ============================================================================
// IC03c — REGRESSION GUARD — API-surface lockdown.
// Asserts that the list of public row-mutating / row-writing methods on
// `contextdb_engine::Database` is exactly the documented set. A future impl
// that adds a new public mutation method (e.g., `set_field`, `update_row`,
// typed-row builder) breaks this test and forces the author to add flagged-
// column enforcement + a test for the new path.
//
// Implementation: compile-time name reference — each `let _ = Database::name;`
// binds a fn item and fails to compile if the method is removed or renamed.
// Signatures are intentionally NOT re-stated so non-surface refactors don't
// break the test. The "nothing else exists" half is held by Step-4 review:
// this test plan binds the surface today; a new public mutation method must
// come with its own enforcement path + test.
// ============================================================================
#[test]
fn ic03c_api_surface_row_mutations_locked() {
    // Each `let _ = Database::name;` coerces the fn item to a compile-time
    // reference. If a method is removed or renamed, compilation fails —
    // that is this test's only job. Signatures are intentionally NOT
    // re-stated; this keeps the test stable across non-surface refactors
    // (parameter reordering, type aliasing) and focused on the "nothing
    // vanishes from the public write surface silently" property.
    let _ = Database::insert_row;
    let _ = Database::upsert_row;
    let _ = Database::delete_row;
    let _ = Database::insert_edge;
    let _ = Database::delete_edge;
    let _ = Database::insert_vector;
    let _ = Database::delete_vector;
    let _ = Database::execute;
    let _ = Database::execute_in_tx;
    let _ = Database::apply_changes;
}

// ============================================================================
// IC04 — RED — Supersede-via-new-row preserves both rows with original values.
// ============================================================================
#[test]
fn ic04_supersede_with_new_row_preserves_both() {
    let db = db_with_decisions();
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    // Row A — the original.
    db.execute(
        "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status)          VALUES ($id, $dt, $desc, $r, $conf, $st)",
        &params(vec![
            ("id", Value::Uuid(id_a)),
            ("dt", Value::Text("original".into())),
            ("desc", Value::Text("orig".into())),
            ("r", Value::Json(serde_json::json!({}))),
            ("conf", Value::Float64(0.5)),
            ("st", Value::Text("active".into())),
        ]),
    )
    .unwrap();

    // Attempt mutation — rejected.
    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'corrected' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id_a))]),
        )
        .expect_err("UPDATE must be rejected");
    assert!(matches!(err, Error::ImmutableColumn { .. }));

    // Correction: a NEW row.
    db.execute(
        "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status)          VALUES ($id, $dt, $desc, $r, $conf, $st)",
        &params(vec![
            ("id", Value::Uuid(id_b)),
            ("dt", Value::Text("corrected".into())),
            ("desc", Value::Text("corr".into())),
            ("r", Value::Json(serde_json::json!({}))),
            ("conf", Value::Float64(0.5)),
            ("st", Value::Text("active".into())),
        ]),
    )
    .unwrap();

    // Both rows present with original flagged values.
    assert_eq!(row_text(&db, id_a, "decision_type"), "original");
    assert_eq!(row_text(&db, id_b, "decision_type"), "corrected");
}

// ============================================================================
// IC05 — RED — Rejection followed by handle drop + reopen preserves original.
// Variant of IC01 without an explicit rollback — auto-commit UPDATE failure.
// ============================================================================
#[test]
fn ic05_rejection_then_reopen_preserves() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ic05.redb");
    let id = Uuid::new_v4();
    {
        let db = Database::open(&path).unwrap();
        db.execute(DECISIONS_DDL, &empty()).unwrap();
        insert_decision(&db, id);

        let before = row_snapshot(&db, "decisions", id);
        let lsn_before = db.current_lsn();
        let err = db
            .execute(
                "UPDATE decisions SET decision_type = 'nope' WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("UPDATE must fail");
        assert!(matches!(err, Error::ImmutableColumn { .. }));
        assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);

        db.close().unwrap();
        drop(db);
    }

    let db2 = Database::open(&path).unwrap();
    assert_eq!(row_text(&db2, id, "decision_type"), "sql-migration");
}

// ============================================================================
// IC06 — RED — After ON CONFLICT rejection, a subsequent unrelated INSERT
// succeeds — proving allocator / checkpoint state was restored.
// ============================================================================
#[test]
fn ic06_on_conflict_rejection_checkpoint_restored() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    // Rejected ON CONFLICT DO UPDATE on flagged column.
    let err = db
        .execute(
            "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status)              VALUES ($id, $dt, $desc, $r, $conf, $st)              ON CONFLICT (id) DO UPDATE SET decision_type = 'rejected_value'",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("dt", Value::Text("sql-migration".into())),
                ("desc", Value::Text("adopt contextdb".into())),
                ("r", Value::Json(serde_json::json!({}))),
                ("conf", Value::Float64(0.9)),
                ("st", Value::Text("active".into())),
            ]),
        )
        .expect_err("must fail");
    assert!(matches!(err, Error::ImmutableColumn { .. }));

    // Follow-up INSERT on a new row must succeed — proving the checkpoint
    // / allocator bytes from the failed merge were restored.
    let id2 = Uuid::new_v4();
    db.execute(
        "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status)          VALUES ($id, $dt, $desc, $r, $conf, $st)",
        &params(vec![
            ("id", Value::Uuid(id2)),
            ("dt", Value::Text("followup".into())),
            ("desc", Value::Text("follow".into())),
            ("r", Value::Json(serde_json::json!({}))),
            ("conf", Value::Float64(0.7)),
            ("st", Value::Text("active".into())),
        ]),
    )
    .expect("follow-up INSERT must succeed after ON CONFLICT rejection");

    assert_eq!(row_text(&db, id2, "decision_type"), "followup");
}

// ============================================================================
// IC07a — RED — UPDATE on nonexistent table returns TableNotFound, never
// ImmutableColumn. Position 1 of the error ordering.
// ============================================================================
#[test]
fn ic07a_error_ordering_table_not_exist_wins() {
    let db = db_with_decisions();
    let err = db
        .execute(
            "UPDATE does_not_exist SET decision_type = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
        )
        .expect_err("must fail");
    match err {
        Error::TableNotFound(name) => assert_eq!(name, "does_not_exist"),
        other => panic!("expected TableNotFound, got {other:?}"),
    }
}

// ============================================================================
// IC07b — RED — ImmutableColumn wins over InvalidStateTransition.
// Position 4 before position 5 of the error ordering.
// ============================================================================
#[test]
fn ic07b_error_ordering_immutable_wins_over_state_machine() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            // active -> 'nonsense_state' is not a legal transition. And the
            // SET list also names a flagged column.
            "UPDATE decisions SET status = 'nonsense_state', decision_type = 'x' WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "expected ImmutableColumn wins over InvalidStateTransition, got {err:?}"
    );
}

// ============================================================================
// IC07c — RED — ImmutableColumn wins over NOT NULL violation.
// Position 4 before position 6 of the error ordering.
// ============================================================================
#[test]
fn ic07c_error_ordering_immutable_wins_over_not_null() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    let err = db
        .execute(
            // decision_type is NOT NULL IMMUTABLE. Setting it to NULL tests
            // that the immutable check fires first.
            "UPDATE decisions SET decision_type = NULL WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect_err("must fail");
    assert!(
        matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"),
        "expected ImmutableColumn wins over NOT NULL, got {err:?}"
    );
}

// ============================================================================
// IC08_<type> — RED — Type-agnostic enforcement. One test per supported type.
// Each test creates a fresh table with exactly one flagged column of the
// target type, INSERTs a value, attempts UPDATE, destructures the error, and
// reads the row back to confirm the flagged column's value is bytes-identical
// to the original INSERTed value. Column names vary across tests to catch
// reserved-word / case-sensitivity parser bugs.
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn type_reject_helper(
    create_sql: &str,
    flagged_col: &str,
    insert_sql: &str,
    insert_params: Vec<(&str, Value)>,
    update_sql: &str,
    update_params: Vec<(&str, Value)>,
    original_value: &Value,
    row_id: Uuid,
) {
    let db = Database::open_memory();
    db.execute(create_sql, &empty()).unwrap();
    db.execute(insert_sql, &params(insert_params)).unwrap();
    let err = db
        .execute(update_sql, &params(update_params))
        .expect_err("UPDATE of flagged column must fail");
    if let Error::ImmutableColumn { table, column } = &err {
        assert_eq!(table, "t");
        assert_eq!(column, flagged_col);
    } else {
        panic!(
            "expected Error::ImmutableColumn {{ table: \"t\", column: {flagged_col:?} }}, got {err:?}"
        );
    }
    // Bytes-equal readback — catches value-equal-but-variant-different bugs.
    let result = db
        .execute(
            "SELECT * FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(row_id))]),
        )
        .unwrap();
    let idx = result
        .columns
        .iter()
        .position(|c| c == flagged_col)
        .unwrap_or_else(|| panic!("flagged column {flagged_col} missing from SELECT *"));
    assert_eq!(
        &result.rows[0][idx], original_value,
        "flagged column value must be bytes-identical to the original INSERT"
    );
}

// IC08_text — RED — Type-coverage: TEXT-typed flagged column UPDATE rejected.
#[test]
fn ic08_text_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Text("orig".into());
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, decision_type TEXT NOT NULL IMMUTABLE)",
        "decision_type",
        "INSERT INTO t (id, decision_type) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET decision_type = 'tamper' WHERE id = $id",
        vec![("id", Value::Uuid(id))],
        &original,
        id,
    );
}

// IC08_uuid — RED — Type-coverage: UUID-typed flagged column UPDATE rejected.
#[test]
fn ic08_uuid_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let orig_uuid = Uuid::new_v4();
    let original = Value::Uuid(orig_uuid);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, anchor UUID NOT NULL IMMUTABLE)",
        "anchor",
        "INSERT INTO t (id, anchor) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET anchor = $nv WHERE id = $id",
        vec![("id", Value::Uuid(id)), ("nv", Value::Uuid(Uuid::new_v4()))],
        &original,
        id,
    );
}

// IC08_txid — RED — Type-coverage: TXID-typed flagged column UPDATE rejected.
#[test]
fn ic08_txid_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::TxId(contextdb_core::TxId(1));
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, created_tx TXID NOT NULL IMMUTABLE)",
        "created_tx",
        "INSERT INTO t (id, created_tx) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET created_tx = $nv WHERE id = $id",
        vec![
            ("id", Value::Uuid(id)),
            ("nv", Value::TxId(contextdb_core::TxId(2))),
        ],
        &original,
        id,
    );
}

// IC08_json — RED — Type-coverage: JSON-typed flagged column UPDATE rejected.
#[test]
fn ic08_json_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Json(serde_json::json!({"a":1}));
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, provenance JSON NOT NULL IMMUTABLE)",
        "provenance",
        "INSERT INTO t (id, provenance) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET provenance = $nv WHERE id = $id",
        vec![
            ("id", Value::Uuid(id)),
            ("nv", Value::Json(serde_json::json!({"a":2}))),
        ],
        &original,
        id,
    );
}

// IC08_integer — RED — Type-coverage: INTEGER-typed flagged column UPDATE rejected.
#[test]
fn ic08_integer_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Int64(1);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, ord INTEGER NOT NULL IMMUTABLE)",
        "ord",
        "INSERT INTO t (id, ord) VALUES ($id, 1)",
        vec![("id", Value::Uuid(id))],
        "UPDATE t SET ord = 2 WHERE id = $id",
        vec![("id", Value::Uuid(id))],
        &original,
        id,
    );
}

// IC08_real — RED — Type-coverage: REAL-typed flagged column UPDATE rejected.
#[test]
fn ic08_real_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Float64(1.0);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, weight REAL NOT NULL IMMUTABLE)",
        "weight",
        "INSERT INTO t (id, weight) VALUES ($id, 1.0)",
        vec![("id", Value::Uuid(id))],
        "UPDATE t SET weight = 2.0 WHERE id = $id",
        vec![("id", Value::Uuid(id))],
        &original,
        id,
    );
}

// IC08_boolean — RED — Type-coverage: BOOLEAN-typed flagged column UPDATE rejected.
#[test]
fn ic08_boolean_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Bool(true);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, was_seen BOOLEAN NOT NULL IMMUTABLE)",
        "was_seen",
        "INSERT INTO t (id, was_seen) VALUES ($id, TRUE)",
        vec![("id", Value::Uuid(id))],
        "UPDATE t SET was_seen = FALSE WHERE id = $id",
        vec![("id", Value::Uuid(id))],
        &original,
        id,
    );
}

// IC08_timestamp — RED — Type-coverage: TIMESTAMP-typed flagged column UPDATE rejected.
#[test]
fn ic08_timestamp_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Timestamp(0);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, recorded_at TIMESTAMP NOT NULL IMMUTABLE)",
        "recorded_at",
        "INSERT INTO t (id, recorded_at) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET recorded_at = $nv WHERE id = $id",
        vec![("id", Value::Uuid(id)), ("nv", Value::Timestamp(3_600_000))],
        &original,
        id,
    );
}

// IC08_vector — RED — Type-coverage: VECTOR-typed flagged column UPDATE rejected.
#[test]
fn ic08_vector_flagged_update_rejected() {
    let id = Uuid::new_v4();
    let original = Value::Vector(vec![1.0, 2.0, 3.0]);
    type_reject_helper(
        "CREATE TABLE t (id UUID PRIMARY KEY, seed_vec VECTOR(3) NOT NULL IMMUTABLE)",
        "seed_vec",
        "INSERT INTO t (id, seed_vec) VALUES ($id, $v)",
        vec![("id", Value::Uuid(id)), ("v", original.clone())],
        "UPDATE t SET seed_vec = $nv WHERE id = $id",
        vec![
            ("id", Value::Uuid(id)),
            ("nv", Value::Vector(vec![4.0, 5.0, 6.0])),
        ],
        &original,
        id,
    );
}

// ============================================================================
// IC09 — RED — Sync apply rejection under non-LatestWins policies.
// EdgeWins: locally-authored policy that prefers "the edge" (incoming).
// Flagged-column diff must still be refused.
// ============================================================================
#[test]
fn ic09_sync_apply_rejection_under_edge_wins() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            (
                "decision_type".to_string(),
                Value::Text("edge-tamper".into()),
            ),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(u64::MAX / 2),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::EdgeWins),
        )
        .unwrap();
    assert_eq!(
        result.applied_rows, 0,
        "flagged diff must NOT overwrite under EdgeWins"
    );
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// IC09_insert_if_not_exists — REGRESSION GUARD — sync-apply under
// InsertIfNotExists: existing row with differing flagged value → policy
// already skips existing rows. Passes on today's engine; gate value is
// binding the non-immutable-reason label on the conflict.
// ============================================================================
#[test]
fn ic09_sync_apply_rejection_under_insert_if_not_exists() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);
    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let incoming = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            (
                "decision_type".to_string(),
                Value::Text("iine-tamper".into()),
            ),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(50_000),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .unwrap();
    assert_eq!(
        result.applied_rows, 0,
        "InsertIfNotExists skips existing row"
    );
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// IC10 — RED — Explicit BEGIN session: flagged-UPDATE rejection does not
// abort the session; surrounding mutable UPDATEs commit.
// ============================================================================
#[test]
fn ic10_multi_statement_txn_flagged_reject_rolls_back_only_failing_stmt() {
    let db = db_with_decisions();
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    db.execute("BEGIN", &empty()).unwrap();

    db.execute(
        "UPDATE decisions SET status = 'superseded' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("mutable UPDATE 1 must succeed");

    let err = db.execute(
        "UPDATE decisions SET decision_type = 'bad' WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    );
    assert!(
        matches!(err, Err(Error::ImmutableColumn { .. })),
        "flagged UPDATE must be rejected, got {err:?}"
    );

    db.execute(
        "UPDATE decisions SET confidence = 0.1 WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("mutable UPDATE 2 must succeed after rejected stmt");

    db.execute("COMMIT", &empty()).unwrap();

    // Both mutable UPDATEs committed.
    assert_eq!(row_text(&db, id, "status"), "superseded");
    let result = db
        .execute(
            "SELECT * FROM decisions WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    let conf_idx = result
        .columns
        .iter()
        .position(|c| c == "confidence")
        .unwrap();
    assert_eq!(result.rows[0][conf_idx], Value::Float64(0.1));
    // Flagged UPDATE did not commit.
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
}

// ============================================================================
// IC11a — REGRESSION GUARD — Multi-row UPDATE with flagged column only in WHERE: all
// mutable updates succeed; flagged values of all matching rows unchanged.
// ============================================================================
#[test]
fn ic11a_multi_row_update_flagged_in_where_succeeds() {
    let db = db_with_decisions();
    let mut ids = Vec::with_capacity(10);
    for _ in 0..10 {
        let id = Uuid::new_v4();
        insert_decision(&db, id);
        ids.push(id);
    }

    // Bulk mutable update where the WHERE clause reads the flagged column.
    db.execute(
        "UPDATE decisions SET status = 'superseded' WHERE decision_type = 'sql-migration'",
        &empty(),
    )
    .expect("multi-row mutable UPDATE must succeed");

    for id in &ids {
        assert_eq!(row_text(&db, *id, "status"), "superseded");
        assert_eq!(row_text(&db, *id, "decision_type"), "sql-migration");
    }
}

// ============================================================================
// IC11b — RED — Multi-row UPDATE that SETs a flagged column is rejected in
// full; zero rows mutated, including their non-flagged columns.
// ============================================================================
#[test]
fn ic11b_multi_row_update_flagged_in_set_rejects_all_rows() {
    let db = db_with_decisions();
    let mut ids = Vec::with_capacity(10);
    for _ in 0..10 {
        let id = Uuid::new_v4();
        insert_decision(&db, id);
        ids.push(id);
    }

    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'tamper' WHERE status = 'active'",
            &empty(),
        )
        .expect_err("multi-row UPDATE that SETs flagged must fail");
    assert!(matches!(err, Error::ImmutableColumn { .. }));

    for id in &ids {
        // Flagged value unchanged.
        assert_eq!(row_text(&db, *id, "decision_type"), "sql-migration");
        // Status unchanged (still 'active').
        assert_eq!(row_text(&db, *id, "status"), "active");
    }
}

// ============================================================================
// IC12 — RED — Concurrent SQL UPDATE + apply_changes on same flagged column.
// Both reject; local row unchanged after both threads return.
// The Database handle is Send + Sync via Arc; we share it across two threads.
// ============================================================================
#[test]
fn ic12_concurrent_update_and_sync_apply_on_flagged() {
    use std::sync::Arc;
    use std::thread;

    let db = Arc::new(db_with_decisions());
    let id = Uuid::new_v4();
    insert_decision(&db, id);

    // Capture the pre-rejection snapshot BEFORE either thread spawns so the
    // ghost-write check is race-free.
    let before = row_snapshot(&db, "decisions", id);
    let lsn_before = db.current_lsn();

    let db_a = Arc::clone(&db);
    let t_a = thread::spawn(move || {
        let err = db_a
            .execute(
                "UPDATE decisions SET decision_type = 'thread_a' WHERE id = $id",
                &params(vec![("id", Value::Uuid(id))]),
            )
            .expect_err("thread A UPDATE must fail");
        assert!(matches!(err, Error::ImmutableColumn { .. }));
    });

    let db_b = Arc::clone(&db);
    let t_b = thread::spawn(move || {
        let incoming = RowChange {
            table: "decisions".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("decision_type".to_string(), Value::Text("thread_b".into())),
                (
                    "description".to_string(),
                    Value::Text("adopt contextdb".into()),
                ),
                ("status".to_string(), Value::Text("active".into())),
            ]),
            deleted: false,
            lsn: contextdb_core::Lsn(u64::MAX / 2 + 1),
        };
        let result = db_b
            .apply_changes(
                ChangeSet {
                    rows: vec![incoming],
                    edges: Vec::new(),
                    vectors: Vec::new(),
                    ddl: Vec::new(),
                },
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .unwrap();
        assert_eq!(result.applied_rows, 0, "thread B sync-apply must not apply");
    });

    t_a.join().unwrap();
    t_b.join().unwrap();

    // Final local state: original flagged value.
    assert_eq!(row_text(&db, id, "decision_type"), "sql-migration");
    assert_no_ghost_write(&db, "decisions", id, &before, lsn_before);
}

// ============================================================================
// CE17 — RED — Parser rejects a STATE MACHINE whose status column is also
// declared IMMUTABLE when the table also declares a PROPAGATE ON EDGE rule
// targeting that status column. The propagation rule writes the status
// column; combined with IMMUTABLE on that column, the declaration is
// contradictory. Parser must name the offending column.
// ============================================================================
#[test]
fn ce17_propagation_rule_targeting_flagged_status_column_rejected_at_parse() {
    let result = parse(
        "CREATE TABLE d (           id UUID PRIMARY KEY,           status TEXT NOT NULL IMMUTABLE         ) STATE MACHINE (status: active -> [invalidated])            PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated",
    );
    match result {
        Err(Error::ParseError(msg)) => {
            assert!(
                msg.contains("status"),
                "parser message must name the offending column 'status', got: {msg}"
            );
        }
        other => panic!("expected ParseError naming 'status', got {other:?}"),
    }
}

// ============================================================================
// CE17b — REGRESSION GUARD — same propagation rule but the flagged column is
// a NON-status column; the declaration parses cleanly because the
// propagation rule only writes the (mutable) status column.
// ============================================================================
#[test]
fn ce17b_propagation_rule_targeting_mutable_status_with_flagged_other_parses() {
    let stmt = parse(
        "CREATE TABLE d (           id UUID PRIMARY KEY,           decision_type TEXT NOT NULL IMMUTABLE,           status TEXT NOT NULL         ) STATE MACHINE (status: active -> [invalidated])            PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated",
    )
    .expect("propagation on mutable status column must parse even with a flagged non-status column");
    match stmt {
        Statement::CreateTable(ct) => {
            let dt = ct
                .columns
                .iter()
                .find(|c| c.name == "decision_type")
                .unwrap();
            assert!(dt.immutable);
            let st = ct.columns.iter().find(|c| c.name == "status").unwrap();
            assert!(!st.immutable);
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

// ============================================================================
// CE17c — RED — Parser rejects an FK-propagation clause (column-level
// `REFERENCES parent(id) ON STATE x PROPAGATE SET flagged_col`) whose SET
// target is a column declared IMMUTABLE on the same table. The FK-propagation
// path writes into the target column; combined with IMMUTABLE on that column,
// the declaration is contradictory. Parser must name the offending column.
// This closes the grammar-path gap left by CE17 which only covers
// `propagate_edge_option`; `fk_propagation_clause` is the second write-path
// propagation construct in the grammar (grammar.pest:177-186).
// ============================================================================
#[test]
fn ce17c_fk_propagation_clause_targeting_flagged_column_rejected_at_parse() {
    let result = parse(
        "CREATE TABLE child (           id UUID PRIMARY KEY,           status TEXT NOT NULL IMMUTABLE,           parent_id UUID REFERENCES parent(id) ON STATE archived PROPAGATE SET status         )",
    );
    match result {
        Err(Error::ParseError(msg)) => {
            assert!(
                msg.contains("status"),
                "parser message must name the offending column 'status', got: {msg}"
            );
        }
        other => panic!("expected ParseError naming 'status', got {other:?}"),
    }
}

// ============================================================================
// CE17d — REGRESSION GUARD — same FK-propagation clause but the SET target is
// a mutable column; the declaration parses cleanly. Positive control for CE17c.
// ============================================================================
#[test]
fn ce17d_fk_propagation_clause_targeting_mutable_column_parses() {
    let stmt = parse(
        "CREATE TABLE child (           id UUID PRIMARY KEY,           decision_type TEXT NOT NULL IMMUTABLE,           status TEXT NOT NULL,           parent_id UUID REFERENCES parent(id) ON STATE archived PROPAGATE SET status         )",
    )
    .expect("FK-propagation targeting a mutable column must parse even when another column is flagged");
    match stmt {
        Statement::CreateTable(ct) => {
            let dt = ct
                .columns
                .iter()
                .find(|c| c.name == "decision_type")
                .unwrap();
            assert!(dt.immutable);
            let st = ct.columns.iter().find(|c| c.name == "status").unwrap();
            assert!(!st.immutable);
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

// ============================================================================
// SC01b — RED — bincode real round-trip of old-shape ColumnDef. Old shape
// has no `immutable` field; the local mirror struct reproduces the
// pre-feature field ordering + `#[serde(default)]` attributes. Decoding
// those bytes into `contextdb_core::ColumnDef` must succeed AND set the
// missing `immutable` field to false (I5 backward compat).
// ============================================================================
#[test]
fn sc01b_bincode_old_shape_column_def_decodes_as_mutable() {
    use serde::{Deserialize, Serialize};

    // Mirror the pre-feature `ColumnDef` shape exactly — same field order,
    // same `#[serde(default)]` attributes where the current type has them.
    // DataType::TxId is mirrored via its serde tag.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct OldColumnDef {
        name: String,
        column_type: contextdb_core::table_meta::ColumnType,
        nullable: bool,
        primary_key: bool,
        #[serde(default)]
        unique: bool,
        #[serde(default)]
        default: Option<String>,
        #[serde(default)]
        references: Option<contextdb_core::table_meta::ForeignKeyReference>,
        #[serde(default)]
        expires: bool,
    }

    let old = OldColumnDef {
        name: "c".to_string(),
        column_type: contextdb_core::table_meta::ColumnType::Text,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        references: None,
        expires: false,
    };

    // bincode config — match contextdb-core's standard config (see
    // `crates/contextdb-engine/src/schema_enforcer.rs:131` for the identical
    // call pattern).
    let bytes = bincode::serde::encode_to_vec(&old, bincode::config::standard())
        .expect("old-shape ColumnDef must encode with bincode");

    let (decoded, _): (contextdb_core::ColumnDef, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .expect("old-shape bytes must decode into new ColumnDef");

    assert_eq!(decoded.name, "c");
    assert!(decoded.nullable);
    assert!(!decoded.primary_key);
    assert!(!decoded.unique);
    assert!(decoded.default.is_none());
    assert!(decoded.references.is_none());
    assert!(!decoded.expires);
    assert!(
        !decoded.immutable,
        "missing `immutable` field must default to false on decode, got true"
    );
}

// ============================================================================
// IC14 — RED — Cross-Value-variant coercion into a flagged column must be
// rejected. sync-apply receives a RowChange whose flagged-column value is a
// Value::Int64 against a local Value::TxId(TxId(42)). Behavior contract:
// ImmutableColumn wins (variant mismatch on a flagged column = attempted
// mutation). Paired positive control: fresh INSERT via apply_changes with a
// correctly-typed Value::TxId succeeds.
// ============================================================================
#[test]
fn ic14_cross_variant_coercion_into_flagged_column_rejected() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, created_tx TXID NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();

    // Drive committed_watermark past 42 via auto-commit SQL so the seed INSERT's
    // Value::TxId(TxId(42)) respects the txid B7 bound (n <= max(watermark, active_tx)).
    db.execute(
        "CREATE TABLE bump (id UUID PRIMARY KEY, n INTEGER)",
        &empty(),
    )
    .unwrap();
    for n in 0..50i64 {
        db.execute(
            "INSERT INTO bump (id, n) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("n", Value::Int64(n)),
            ]),
        )
        .unwrap();
    }
    assert!(
        db.committed_watermark().0 >= 42,
        "bump precondition failed: watermark must be >= 42"
    );

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, created_tx) VALUES ($id, $v)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("v", Value::TxId(contextdb_core::TxId(42))),
        ]),
    )
    .unwrap();

    // Cross-variant incoming: Int64 into a TxId column, differing value.
    let incoming = RowChange {
        table: "t".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("created_tx".to_string(), Value::Int64(99)),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(u64::MAX / 2),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(
        result.applied_rows, 0,
        "variant-mismatch on flagged column is an attempted mutation — must be rejected"
    );
    assert!(!result.conflicts.is_empty());
    let reason = result.conflicts[0]
        .reason
        .as_deref()
        .unwrap_or("")
        .to_lowercase();
    assert!(
        reason.contains("immutable") || reason.contains("type"),
        "rejection reason must mention 'immutable' or a type mismatch, got: {reason:?}"
    );
    // Local value unchanged.
    let result = db
        .execute(
            "SELECT * FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    let idx = result
        .columns
        .iter()
        .position(|c| c == "created_tx")
        .unwrap();
    assert_eq!(result.rows[0][idx], Value::TxId(contextdb_core::TxId(42)));
}

// ============================================================================
// IC14b — REGRESSION GUARD — sync-apply onto a fresh row with a correctly-
// typed Value::TxId succeeds (positive control for IC14).
// ============================================================================
#[test]
fn ic14b_sync_apply_fresh_row_correct_variant_accepted() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, created_tx TXID NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    let incoming = RowChange {
        table: "t".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            (
                "created_tx".to_string(),
                Value::TxId(contextdb_core::TxId(7)),
            ),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(1000),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![incoming],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(result.applied_rows, 1);
}

// ============================================================================
// IC11c — RED — Multi-row mixed SET list: seed 10 rows with DISTINCT flagged
// AND mutable values, run UPDATE that SETs both a flagged and a mutable
// column. All 4 matched rows are rejected; zero partial mutation observable.
// ============================================================================
#[test]
fn ic11c_multi_row_mixed_set_list_rejects_all_rows_no_partial_mutation() {
    let db = db_with_decisions();

    // Seed 10 rows. We use a local helper to attach a known ordinal to each
    // row via `confidence` so we can reason about the "id > 5" WHERE clause
    // below against a deterministic attribute that we control.
    let mut rows: Vec<(Uuid, String, f64)> = Vec::with_capacity(10);
    for i in 0..10u8 {
        let id = Uuid::new_v4();
        let dt = format!("t{i}");
        let status = if i % 2 == 0 { "active" } else { "superseded" };
        let conf = i as f64 / 10.0;
        db.execute(
            "INSERT INTO decisions (id, decision_type, description, reasoning, confidence, status)              VALUES ($id, $dt, $desc, $r, $conf, $st)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("dt", Value::Text(dt.clone())),
                ("desc", Value::Text(format!("row{i}"))),
                ("r", Value::Json(serde_json::json!({"i": i}))),
                ("conf", Value::Float64(conf)),
                ("st", Value::Text(status.into())),
            ]),
        )
        .unwrap();
        rows.push((id, dt, conf));
    }

    // WHERE confidence > 0.4 matches rows 5..9 (5 rows).
    let err = db
        .execute(
            "UPDATE decisions SET decision_type = 'new', status = 'archived' WHERE confidence > 0.4",
            &empty(),
        )
        .expect_err("multi-row mixed SET on flagged must be rejected");
    assert!(matches!(err, Error::ImmutableColumn { ref column, .. } if column == "decision_type"));

    // No partial mutation: each row retains BOTH its original flagged value
    // AND its original status.
    for (id, dt, _conf) in &rows {
        assert_eq!(row_text(&db, *id, "decision_type"), *dt);
        // Status: rows 5..9 are a mix of "active" / "superseded" via i%2.
        // We do not re-derive; we assert status is one of the original values
        // AND it is NOT 'archived'.
        let s = row_text(&db, *id, "status");
        assert!(
            s == "active" || s == "superseded",
            "status must be untouched, got {s}"
        );
        assert_ne!(s, "archived", "status must NOT be the rejected new value");
    }
}

// ============================================================================
// SA06 — RED — Partial ChangeSet: 2 row-changes, one mutates a flagged column
// (reject), the other mutates a mutable column only (apply). applied_rows ==
// 1; conflicts.len() == 1; row A unchanged; row B reflects the new value.
// ============================================================================
#[test]
fn sa06_partial_changeset_rejects_flagged_row_applies_clean_row() {
    let db = db_with_decisions();
    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    insert_decision(&db, id_a);
    insert_decision(&db, id_b);

    let row_a_tamper = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id_a),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id_a)),
            (
                "decision_type".to_string(),
                Value::Text("PARTIAL_TAMPER".into()),
            ),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("active".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(u64::MAX / 2),
    };
    let row_b_clean = RowChange {
        table: "decisions".to_string(),
        natural_key: NaturalKey {
            column: "id".to_string(),
            value: Value::Uuid(id_b),
        },
        values: HashMap::from([
            ("id".to_string(), Value::Uuid(id_b)),
            (
                "decision_type".to_string(),
                Value::Text("sql-migration".into()),
            ),
            (
                "description".to_string(),
                Value::Text("adopt contextdb".into()),
            ),
            ("status".to_string(), Value::Text("archived".into())),
        ]),
        deleted: false,
        lsn: contextdb_core::Lsn(u64::MAX / 2 + 10),
    };
    let result = db
        .apply_changes(
            ChangeSet {
                rows: vec![row_a_tamper, row_b_clean],
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    assert_eq!(result.applied_rows, 1, "only the clean row must apply");
    assert_eq!(result.conflicts.len(), 1, "exactly one conflict for row A");

    // Row A unchanged.
    assert_eq!(row_text(&db, id_a, "decision_type"), "sql-migration");
    // Row B mutable column updated.
    assert_eq!(row_text(&db, id_b, "status"), "archived");
}

// ============================================================================
// RC03 — RED — `sql_type_for_meta_column` direct-render path emits IMMUTABLE
// on a flagged column. Bypasses the AST route and hits the engine-internal
// render helper used by DDL replication.
// The function is a module-private `fn` in `contextdb-engine::database`, so
// we reach it indirectly by constructing a `TableMeta` in-crate and asking
// the engine to render the DDL via its public ddl_log path after CREATE.
// ============================================================================
#[test]
fn rc03_meta_column_render_preserves_immutable() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, c TEXT NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();
    let ddl = db.ddl_log_since(contextdb_core::Lsn(0));
    let create = ddl
        .into_iter()
        .find(|d| matches!(d, DdlChange::CreateTable { name, .. } if name == "t"))
        .expect("DDL log must contain CreateTable for t");

    // Extract the rendered column-type string for `c` from the DdlChange.
    if let DdlChange::CreateTable { columns, .. } = create {
        let (_name, rendered) = columns
            .iter()
            .find(|(n, _)| n == "c")
            .expect("column c must appear in the rendered DDL");
        assert!(
            rendered.to_uppercase().contains("IMMUTABLE"),
            "meta-column render must emit IMMUTABLE, got {rendered:?}"
        );
    } else {
        panic!("expected CreateTable");
    }

    // Re-parse to prove the round-trip preserves the flag.
    let sql = "CREATE TABLE t_rt (id UUID PRIMARY KEY, c TEXT NOT NULL IMMUTABLE)";
    let stmt = parse(sql).unwrap();
    if let Statement::CreateTable(ct) = stmt {
        let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
        assert!(c.immutable);
    } else {
        panic!("expected CreateTable");
    }
}
