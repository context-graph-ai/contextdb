//! Conflict policy is an APPLICATION declaration on the table, not an
//! engine-hardwired rule.
//!
//! What these pin, in plain language: which value survives when the same key is
//! written on more than one machine, or the same row is re-sent, is a policy the
//! TABLE declares — its own clause, `SYNC CONFLICT KEEP FIRST` (the first value
//! written for a key stays) or `SYNC CONFLICT KEEP LATEST` (the newest write
//! wins). The declaration persists with the table, renders in `.schema`, travels
//! with synced DDL, and is reconstructed on the receiving machine — exactly as
//! the direction clause does. A table that declares no policy gets the engine's
//! non-overwriting default (keep-first): a re-send of an existing key does not
//! silently overwrite it.
//!
//! Before this change, the `SYNC CONFLICT` clause was not in the grammar, so
//! every declaration here was a parse error, and a synced definition arrived
//! with no policy reconstructed.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Every
//! assertion reads persisted state or a rendered declaration.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// The `.schema` render of one table — the operator-visible declaration.
fn render(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table '{table}' must exist"));
    contextdb_engine::cli_render::render_table_meta(table, &meta)
}

/// The two policy words, so a test can say "exactly this one appears".
const POLICY_SPELLINGS: [&str; 2] = ["SYNC CONFLICT KEEP FIRST", "SYNC CONFLICT KEEP LATEST"];

/// The rendered declaration must name EXACTLY the expected policy — and, when no
/// policy is expected, none at all (the non-overwriting default renders nothing,
/// like an undeclared direction).
fn assert_renders_exactly_policy(rendered: &str, expected: Option<&str>) {
    for spelling in POLICY_SPELLINGS {
        let found = rendered.matches(spelling).count();
        let wanted = usize::from(Some(spelling) == expected);
        assert_eq!(
            found, wanted,
            "the declaration must render {expected:?} and nothing else, but \
             '{spelling}' appears {found} time(s), got:\n{rendered}"
        );
    }
}

/// A table definition arriving from another machine, applied through the public
/// sync door exactly as a peer's changeset would deliver it.
fn arrive_create_table(db: &Database, name: &str, columns: &[(&str, &str)], constraints: &[&str]) {
    db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::CreateTable {
                name: name.to_string(),
                columns: columns
                    .iter()
                    .map(|(c, t)| (c.to_string(), t.to_string()))
                    .collect(),
                constraints: constraints.iter().map(|c| c.to_string()).collect(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .unwrap_or_else(|err| panic!("an arriving definition for '{name}' must apply: {err}"));
}

// ---------------------------------------------------------------------------
// The clause parses, persists across a restart, and renders in `.schema`
// ---------------------------------------------------------------------------

/// Both policies are declarable, survive a restart, and are visible to an
/// operator reading the schema — no application re-registers anything on start.
#[test]
fn declared_conflict_policy_persists_across_restart_and_renders_in_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("conflict.db");

    {
        let db = Database::open(&path).expect("open");
        db.execute(
            "CREATE TABLE first_wins (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect("SYNC CONFLICT KEEP FIRST must be declarable");
        db.execute(
            "CREATE TABLE latest_wins (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("SYNC CONFLICT KEEP LATEST must be declarable");
    }

    let db = Database::open(&path).expect("reopen");
    assert_renders_exactly_policy(&render(&db, "first_wins"), Some("SYNC CONFLICT KEEP FIRST"));
    assert_renders_exactly_policy(
        &render(&db, "latest_wins"),
        Some("SYNC CONFLICT KEEP LATEST"),
    );
}

/// A table that declares no policy shows no conflict clause — the non-overwriting
/// default is the engine's, not an operator's, so nothing renders for it to
/// puzzle over (the same treatment an undeclared direction gets).
#[test]
fn an_undeclared_conflict_policy_renders_nothing() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("create");
    assert_renders_exactly_policy(&render(&db, "notes"), None);
}

/// The rendered schema is the declaration: re-executing it elsewhere reproduces
/// the same policy, so `.schema` is a faithful export and not a lossy summary.
/// Alongside the direction and retention clauses, to prove the policy clause
/// composes rather than displacing them.
#[test]
fn the_rendered_conflict_policy_round_trips() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
             RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect("a retained two-way keep-first table must be declarable");
    let rendered = render(&source, "windows");

    let receiver = Database::open_memory();
    receiver
        .execute(&rendered, &p())
        .unwrap_or_else(|err| panic!("rendered DDL must execute: {err}\n{rendered}"));
    assert_eq!(
        render(&receiver, "windows"),
        rendered,
        "the policy, the direction and the retention window must all survive the round trip"
    );
    assert!(
        rendered.contains("SYNC CONFLICT KEEP FIRST")
            && rendered.contains("SYNC TWO WAY")
            && rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "all three declarations render together, got:\n{rendered}"
    );
}

// ---------------------------------------------------------------------------
// The declaration travels with synced DDL and is reconstructed on the receiver
// ---------------------------------------------------------------------------

/// A definition written on another machine lands here with its policy intact.
/// The arrival path reconstructs the declared policy into meta rather than
/// dropping it — a table whose definition syncs must carry the very policy the
/// declaration exists to express.
#[test]
fn an_arriving_definition_reconstructs_its_declared_policy() {
    for clause in POLICY_SPELLINGS {
        let db = Database::open_memory();
        arrive_create_table(
            &db,
            "windows",
            &[("id", "INTEGER PRIMARY KEY"), ("body", "TEXT")],
            &[clause],
        );
        let rendered = render(&db, "windows");
        assert!(
            rendered.contains(clause),
            "the arriving policy '{clause}' must be reconstructed into meta, got:\n{rendered}"
        );
    }
}

/// The whole path end to end, on the REAL emitted changeset: one machine declares
/// the policy, its changeset carries the definition, the other machine applies it
/// and ends up with the same declaration. An implementation that stores the
/// policy but forgets to render it into outgoing DDL leaves every other machine
/// honoring a policy nobody declared.
#[test]
fn a_declared_policy_travels_through_the_real_changeset() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE first_wins (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect("declare keep-first");
    source
        .execute(
            "CREATE TABLE latest_wins (id INTEGER PRIMARY KEY, body TEXT) \
             SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("declare keep-latest");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiving machine must accept both definitions");

    assert_renders_exactly_policy(
        &render(&receiver, "first_wins"),
        Some("SYNC CONFLICT KEEP FIRST"),
    );
    assert_renders_exactly_policy(
        &render(&receiver, "latest_wins"),
        Some("SYNC CONFLICT KEEP LATEST"),
    );
}
