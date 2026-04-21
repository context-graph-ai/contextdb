//! Parser-level checks that must hold even without engine-side enforcement.

use contextdb_core::Error;
use contextdb_parser::ast::Statement;
use contextdb_parser::parse;

// ============================================================================
// Parser positive — IMMUTABLE round-trips through parser into ast::ColumnDef.
// ============================================================================
#[test]
fn parser_immutable_flag_lands_on_ast_column_def() {
    let stmt =
        parse("CREATE TABLE t (c TEXT NOT NULL IMMUTABLE)").expect("IMMUTABLE keyword must parse");
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CreateTable, got {other:?}"),
    };
    let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
    assert!(c.immutable, "ast::ColumnDef.immutable must be set");
}

// ============================================================================
// Parser default — absence of IMMUTABLE leaves the flag false.
// ============================================================================
#[test]
fn parser_no_immutable_keyword_leaves_flag_false() {
    let stmt = parse("CREATE TABLE t (c TEXT NOT NULL)").unwrap();
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };
    let c = ct.columns.iter().find(|c| c.name == "c").unwrap();
    assert!(!c.immutable, "default must be false");
}

// ============================================================================
// Parser rejects duplicate IMMUTABLE.
// ============================================================================
#[test]
fn parser_rejects_duplicate_immutable() {
    let result = parse("CREATE TABLE t (c TEXT IMMUTABLE IMMUTABLE)");
    assert!(matches!(result, Err(Error::ParseError(_))));
}
