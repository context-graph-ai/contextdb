//! FIX 5 — the superseded legacy conflict-policy DDL is removed (greenfield).
//!
//! The `SET SYNC_CONFLICT_POLICY '...'` global statement and the
//! `ALTER TABLE t SET/DROP SYNC_CONFLICT_POLICY` forms were a SECOND policy
//! surface that exposed `edge_wins`/`server_wins`, contradicting the
//! declared-clause-only, keep-first / keep-latest contract. Greenfield forbids
//! the legacy spelling, so it must no longer parse. The DECLARED
//! `SYNC CONFLICT KEEP FIRST | LATEST` clause is unaffected.

use contextdb_parser::parse;

#[test]
fn legacy_global_set_sync_conflict_policy_is_a_parse_error() {
    assert!(
        parse("SET SYNC_CONFLICT_POLICY 'latest_wins'").is_err(),
        "the global SET SYNC_CONFLICT_POLICY DDL must no longer parse"
    );
    assert!(
        parse("SET SYNC_CONFLICT_POLICY 'server_wins'").is_err(),
        "the legacy server_wins spelling must not parse"
    );
    assert!(
        parse("SET SYNC_CONFLICT_POLICY 'edge_wins'").is_err(),
        "the legacy edge_wins spelling must not parse"
    );
}

#[test]
fn legacy_alter_table_sync_conflict_policy_is_a_parse_error() {
    assert!(
        parse("ALTER TABLE t SET SYNC_CONFLICT_POLICY 'latest_wins'").is_err(),
        "the ALTER TABLE SET SYNC_CONFLICT_POLICY DDL must no longer parse"
    );
    assert!(
        parse("ALTER TABLE t DROP SYNC_CONFLICT_POLICY").is_err(),
        "the ALTER TABLE DROP SYNC_CONFLICT_POLICY DDL must no longer parse"
    );
}

#[test]
fn declared_sync_conflict_clause_still_parses() {
    assert!(
        parse("CREATE TABLE t (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST").is_ok(),
        "the declared keep-latest clause is unaffected"
    );
    assert!(
        parse("CREATE TABLE t (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP FIRST").is_ok(),
        "the declared keep-first clause is unaffected"
    );
}
