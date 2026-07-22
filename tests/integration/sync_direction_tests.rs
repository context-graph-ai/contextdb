//! Sync-policy decoupling — the declaration doors for retention, sync
//! direction, and their independence.
//!
//! What these pin, in plain language: how long a table keeps its rows and where
//! those rows travel are two separate declarations. `RETAIN` says only when
//! rows expire. `SYNC SAFE` says only "do not expire an outbound row before the
//! destination confirms receipt". Direction is its own clause —
//! `SYNC OFF | SYNC PUSH ONLY | SYNC PULL ONLY | SYNC TWO WAY` — which persists
//! with the table, renders in `.schema`, travels with synced DDL, and applies to
//! retained and non-retained tables alike. A table that declares no direction is
//! two-way, the engine default.
//!
//! Before this change, `RETAIN` force-converted a table to push-only at three
//! doors, `TWO WAY` was refused at four, the direction clause did not exist in
//! the grammar, and a synced definition arrived with its retention and
//! direction thrown away (`rough_sync_table_meta`).
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads. Every
//! assertion reads persisted state or a rendered declaration.

use contextdb_core::{Error, Lsn, Value};
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

/// The direction words, so a test can say "none of these appear".
const DIRECTION_SPELLINGS: [&str; 4] = [
    "SYNC OFF",
    "SYNC PUSH ONLY",
    "SYNC PULL ONLY",
    "SYNC TWO WAY",
];

/// The rendered declaration must name EXACTLY one direction — the expected one.
/// A render that prints two of them, prints a different one, or prints none at
/// all where one was explicitly declared, is wrong in a way that a bare
/// `contains` check waves through.
fn assert_renders_exactly_direction(rendered: &str, expected: &str) {
    for spelling in DIRECTION_SPELLINGS {
        let found = rendered.matches(spelling).count();
        let wanted = usize::from(spelling == expected);
        assert_eq!(
            found, wanted,
            "the declaration must render '{expected}' and nothing else, but \
             '{spelling}' appears {found} time(s), got:\n{rendered}"
        );
    }
}

/// A table definition arriving from another machine, applied through the public
/// sync door exactly as a peer's changeset would deliver it.
fn arrive_create_table(
    db: &Database,
    name: &str,
    columns: &[(&str, &str)],
    constraints: &[&str],
) -> Result<(), Error> {
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
    .map(|_| ())
}

// ---------------------------------------------------------------------------
// C1a — the retention spellings are untouched, and say nothing about direction
// ---------------------------------------------------------------------------

/// `RETAIN 48 HOURS` and `RETAIN 48 HOURS SYNC SAFE` parse exactly as they do
/// today and persist exactly what they always did: a window, and a delivery
/// promise. Nothing else.
#[test]
fn c1a_retention_spellings_parse_and_persist_as_today() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("retention.db");

    {
        let db = Database::open(&path).expect("open");
        db.execute(
            "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS",
            &p(),
        )
        .expect("plain retention must keep parsing");
        db.execute(
            "CREATE TABLE promised (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC SAFE",
            &p(),
        )
        .expect("retention with the delivery promise must keep parsing");
    }

    let db = Database::open(&path).expect("reopen");
    let plain = db.table_meta("plain").expect("plain meta");
    assert_eq!(plain.default_ttl_seconds, Some(48 * 60 * 60));
    assert!(!plain.sync_safe, "plain RETAIN promises no delivery");
    let promised = db.table_meta("promised").expect("promised meta");
    assert_eq!(promised.default_ttl_seconds, Some(48 * 60 * 60));
    assert!(
        promised.sync_safe,
        "SYNC SAFE is still the delivery promise"
    );
}

/// The decisive half of C1a: declaring retention — with or without the delivery
/// promise — changes no direction. Neither table carries a direction word in its
/// declaration, so both are the engine default, two-way. Today the render reads
/// `RETAIN 48 HOURS SYNC SAFE PUSH ONLY` because `RETAIN` converted the table.
#[test]
fn c1a_retention_declares_no_direction() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS",
        &p(),
    )
    .expect("create plain");
    db.execute(
        "CREATE TABLE promised (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC SAFE",
        &p(),
    )
    .expect("create promised");

    for table in ["plain", "promised"] {
        let rendered = render(&db, table);
        for one_way in ["SYNC PUSH ONLY", "SYNC PULL ONLY", "SYNC OFF", "PUSH ONLY"] {
            assert!(
                !rendered.contains(one_way),
                "declaring retention on '{table}' must not attach a direction \
                 ({one_way}); the two are separate declarations. Got:\n{rendered}"
            );
        }
    }
}

/// The same, one door along: adding retention to an existing table by `ALTER`
/// leaves the direction it already had. Today this is the
/// `retained_sync_policy = PushOnly` conversion in the ALTER path.
#[test]
fn c1a_alter_adding_retention_leaves_the_declared_direction_alone() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        &p(),
    )
    .expect("a two-way table");

    db.execute("ALTER TABLE windows SET RETAIN 48 HOURS SYNC SAFE", &p())
        .expect("adding retention to a two-way table must be accepted");

    let rendered = render(&db, "windows");
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
    assert!(
        !rendered.contains("PUSH ONLY"),
        "and retention must not convert the table to one-way, got:\n{rendered}"
    );
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "while the retention declaration itself lands, got:\n{rendered}"
    );
}

// ---------------------------------------------------------------------------
// C1c — the nested direction form is deleted from the grammar
// ---------------------------------------------------------------------------

/// A direction word nested inside the retention clause is a parse error, and
/// the message tells the writer the separate clause to use instead. Never
/// silently accepted, never kept as an alias.
#[test]
fn c1c_nested_push_only_form_is_a_parse_error_naming_the_separate_clause() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE PUSH ONLY",
            &p(),
        )
        .expect_err("the nested direction form must no longer parse");
    let message = err.to_string();
    assert!(
        message.to_uppercase().contains("SYNC PUSH ONLY"),
        "the refusal must name the separate direction clause to write instead, got: {message}"
    );
    assert!(
        db.table_meta("windows").is_none(),
        "a refused declaration leaves no table behind"
    );
}

#[test]
fn c1c_nested_two_way_form_is_a_parse_error_naming_the_separate_clause() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE TWO WAY",
            &p(),
        )
        .expect_err("the nested direction form must no longer parse");
    let message = err.to_string();
    assert!(
        message.to_uppercase().contains("SYNC TWO WAY"),
        "the refusal must name the separate direction clause to write instead — \
         and must NOT be the old 'a retained table is one-way' refusal, got: {message}"
    );
    assert!(
        db.table_meta("windows").is_none(),
        "a refused declaration leaves no table behind"
    );
}

/// The `ALTER` door spells the retention clause too, so the nested form dies
/// there as well.
#[test]
fn c1c_nested_form_is_a_parse_error_on_alter_set_retain() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE windows (id INTEGER PRIMARY KEY)", &p())
        .expect("create");

    let err = db
        .execute(
            "ALTER TABLE windows SET RETAIN 12 HOURS SYNC SAFE PUSH ONLY",
            &p(),
        )
        .expect_err("the nested direction form must no longer parse on ALTER");
    assert!(
        err.to_string().to_uppercase().contains("SYNC PUSH ONLY"),
        "the refusal must name the separate direction clause, got: {err}"
    );
    let meta = db.table_meta("windows").expect("the table still exists");
    assert_eq!(
        meta.default_ttl_seconds, None,
        "and no part of the refused statement is applied: {meta:?}"
    );
}

// ---------------------------------------------------------------------------
// C1d — direction is its own clause: declared, persisted, rendered, portable
// ---------------------------------------------------------------------------

/// All four settings, on retained and non-retained tables alike, survive a
/// restart and are visible to an operator reading the schema. No application
/// re-registers anything on start.
#[test]
fn c1d_every_direction_persists_across_restart_and_renders_in_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("direction.db");

    {
        let db = Database::open(&path).expect("open");
        db.execute(
            "CREATE TABLE off_table (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
            &p(),
        )
        .expect("SYNC OFF must be declarable");
        db.execute(
            "CREATE TABLE push_table (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
            &p(),
        )
        .expect("SYNC PUSH ONLY must be declarable");
        db.execute(
            "CREATE TABLE pull_table (id INTEGER PRIMARY KEY, body TEXT) SYNC PULL ONLY",
            &p(),
        )
        .expect("SYNC PULL ONLY must be declarable");
        db.execute(
            "CREATE TABLE both_table (id INTEGER PRIMARY KEY, body TEXT) \
             RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
            &p(),
        )
        .expect("SYNC TWO WAY must be declarable, retention and all");
    }

    let db = Database::open(&path).expect("reopen");
    for (table, clause) in [
        ("off_table", "SYNC OFF"),
        ("push_table", "SYNC PUSH ONLY"),
        ("pull_table", "SYNC PULL ONLY"),
        ("both_table", "SYNC TWO WAY"),
    ] {
        let rendered = render(&db, table);
        assert_renders_exactly_direction(&rendered, clause);
    }
    let both = render(&db, "both_table");
    assert!(
        both.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the retention declaration rides alongside the direction, got:\n{both}"
    );
}

/// A table that declares no direction is two-way — the engine default — so its
/// schema shows no one-way restriction for an operator to puzzle over.
#[test]
fn c1d_a_table_with_no_direction_clause_is_two_way() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("create");
    let rendered = render(&db, "notes");
    for one_way in ["SYNC OFF", "PUSH ONLY", "PULL ONLY"] {
        assert!(
            !rendered.contains(one_way),
            "an undeclared direction is two-way, not {one_way}, got:\n{rendered}"
        );
    }
}

/// The rendered schema is the declaration: re-executing it elsewhere reproduces
/// the same direction, so `.schema` is a faithful export and not a lossy
/// summary.
#[test]
fn c1d_the_rendered_direction_round_trips() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
             RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
            &p(),
        )
        .expect("create");
    let rendered = render(&source, "windows");

    let receiver = Database::open_memory();
    receiver
        .execute(&rendered, &p())
        .unwrap_or_else(|err| panic!("rendered DDL must execute: {err}\n{rendered}"));
    assert_eq!(
        render(&receiver, "windows"),
        rendered,
        "the direction and the retention window must both survive the round trip"
    );
}

// ---------------------------------------------------------------------------
// C1b / C1f — a retained two-way table is accepted at every door
// ---------------------------------------------------------------------------

/// The local `CREATE` door. This is the exact declaration a consumer writes.
#[test]
fn c1b_a_retained_two_way_table_is_accepted_at_create() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
        &p(),
    )
    .expect("a retained two-way table must be accepted — retention decides no direction");
    let rendered = render(&db, "windows");
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the retention declaration must be visible to an operator, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
}

/// The local `ALTER` door.
#[test]
fn c1b_a_retained_two_way_table_is_accepted_at_alter() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
        &p(),
    )
    .expect("create");
    db.execute("ALTER TABLE windows SET RETAIN 12 HOURS SYNC SAFE", &p())
        .expect("declaring retention on a two-way table must be accepted at ALTER");

    let rendered = render(&db, "windows");
    assert!(
        rendered.contains("RETAIN 12 HOURS SYNC SAFE"),
        "the table keeps its retention declaration, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
}

/// The arrival door, in isolation: a definition written on another machine
/// lands here with its retention window and its direction intact. Today the
/// arrival path throws both away (`rough_sync_table_meta` hardcodes them to
/// `None`), so a table that syncs its definition loses the very policy the
/// declaration exists to carry.
#[test]
fn c1b_an_arriving_retained_two_way_definition_is_accepted_and_reconstructed() {
    let db = Database::open_memory();
    arrive_create_table(
        &db,
        "windows",
        &[("id", "INTEGER PRIMARY KEY"), ("body", "TEXT")],
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC TWO WAY"],
    )
    .expect("a retained two-way definition arriving from another machine must be accepted");

    let rendered = render(&db, "windows");
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the arriving retention window must be reconstructed into meta, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
    let meta = db.table_meta("windows").expect("meta");
    assert_eq!(
        meta.default_ttl_seconds,
        Some(48 * 60 * 60),
        "the window is the declared one, not a default: {meta:?}"
    );
    assert!(meta.sync_safe, "and the delivery promise arrived: {meta:?}");
}

/// The whole path end to end, in the shape sync actually uses it: one machine
/// declares the table, its changeset carries the definition, the other machine
/// applies it and ends up with the same declaration.
#[test]
fn c1f_a_retained_two_way_definition_travels_from_one_machine_to_another() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
             RETAIN 48 HOURS SYNC SAFE SYNC TWO WAY",
            &p(),
        )
        .expect("the writing machine declares the table");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiving machine must accept the definition");

    let rendered = render(&receiver, "windows");
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the receiving machine must hold the retention window the writer declared, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
}

// ---------------------------------------------------------------------------
// C1e — a promise of delivery with nowhere to deliver is refused when written
// ---------------------------------------------------------------------------

/// `SYNC SAFE` says "do not expire this row before the destination confirms
/// receipt". A direction that never delivers the table makes that promise
/// unkeepable, so the contradiction is refused at the moment it is written —
/// not left to strand rows that never expire.
fn assert_delivery_contradiction_refusal(message: &str, direction: &str) {
    let upper = message.to_uppercase();
    assert!(
        upper.contains("SYNC SAFE"),
        "the refusal must name the promise it cannot keep, got: {message}"
    );
    assert!(
        upper.contains(direction),
        "the refusal must name the direction that contradicts it ({direction}), got: {message}"
    );
    assert!(
        message.to_lowercase().contains("deliver"),
        "the refusal must say what the conflict IS — the table would never be \
         delivered — got: {message}"
    );
}

#[test]
fn c1e_sync_safe_with_sync_off_is_refused_at_create() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY) RETAIN 48 HOURS SYNC SAFE SYNC OFF",
            &p(),
        )
        .expect_err("a delivery promise with no delivery must be refused at declaration");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC OFF");
    assert!(
        db.table_meta("windows").is_none(),
        "the whole statement is refused — no half-created table"
    );
}

#[test]
fn c1e_sync_safe_with_sync_pull_only_is_refused_at_create() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY) \
             RETAIN 48 HOURS SYNC SAFE SYNC PULL ONLY",
            &p(),
        )
        .expect_err("a pull-only table never delivers, so the promise is refused");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC PULL ONLY");
    assert!(db.table_meta("windows").is_none());
}

#[test]
fn c1e_sync_safe_with_a_non_delivering_direction_is_refused_at_alter() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC PULL ONLY",
        &p(),
    )
    .expect("a pull-only table is fine until it promises delivery");

    let err = db
        .execute("ALTER TABLE windows SET RETAIN 48 HOURS SYNC SAFE", &p())
        .expect_err("the ALTER door must refuse the same contradiction");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC PULL ONLY");

    let meta = db.table_meta("windows").expect("the table still exists");
    assert!(
        !meta.sync_safe,
        "a refused ALTER leaves no delivery promise behind: {meta:?}"
    );
    assert_eq!(
        meta.default_ttl_seconds, None,
        "and applies no part of the statement, not even the window: {meta:?}"
    );
}

/// The negative control that keeps the refusal narrow. A colocated installation
/// that keeps one copy declares retention with synchronization off and no
/// delivery promise — perfectly legal, and it must stay legal.
#[test]
fn c1e_retention_without_a_delivery_promise_may_declare_any_direction() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE colocated (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC OFF",
        &p(),
    )
    .expect("plain retention with synchronization off is a supported deployment");
    db.execute(
        "CREATE TABLE dashboard (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC PULL ONLY",
        &p(),
    )
    .expect("plain retention on a pull-only table is legal too");

    assert_renders_exactly_direction(&render(&db, "colocated"), "SYNC OFF");
    assert_renders_exactly_direction(&render(&db, "dashboard"), "SYNC PULL ONLY");
}

/// The arrival door refuses the contradiction as well — otherwise a peer could
/// install here the exact declaration every local door rejects, and this engine
/// would delete undelivered rows on its behalf.
#[test]
fn c1e_an_arriving_sync_safe_declaration_with_no_delivery_is_refused() {
    let db = Database::open_memory();
    let err = arrive_create_table(
        &db,
        "windows",
        &[("id", "INTEGER PRIMARY KEY"), ("body", "TEXT")],
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC OFF"],
    )
    .expect_err("arriving DDL must be refused exactly as the local doors are");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC OFF");
    assert!(
        db.table_meta("windows").is_none(),
        "a refused arrival applies no schema side effects"
    );
}

/// Direction is declared on `ALTER` too — an operator moving an installation
/// changes the setting on the table it already has, without recreating it.
#[test]
fn c1d_direction_is_declarable_on_alter() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT)",
        &p(),
    )
    .expect("create");

    db.execute("ALTER TABLE windows SET SYNC PULL ONLY", &p())
        .expect("the direction must be changeable on an existing table");
    assert_renders_exactly_direction(&render(&db, "windows"), "SYNC PULL ONLY");

    db.execute("ALTER TABLE windows SET SYNC TWO WAY", &p())
        .expect("and changeable back");
    assert_renders_exactly_direction(&render(&db, "windows"), "SYNC TWO WAY");
}

/// The contradiction is refused in the other order too: a table that already
/// promises delivery cannot be moved to a direction that never delivers it.
#[test]
fn c1e_altering_a_sync_safe_table_to_a_non_delivering_direction_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC SAFE",
        &p(),
    )
    .expect("create");

    let err = db
        .execute("ALTER TABLE windows SET SYNC OFF", &p())
        .expect_err("a delivery promise cannot be left with nowhere to deliver");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC OFF");
    assert!(
        !render(&db, "windows").contains("SYNC OFF"),
        "a refused ALTER applies no part of itself"
    );
}

/// Belt and braces on the vocabulary: none of the four direction spellings is
/// silently ignored on arrival. A direction that arrives and is dropped is the
/// reconstruction gap this change closes.
#[test]
fn c1d_each_arriving_direction_is_reconstructed_not_dropped() {
    for clause in DIRECTION_SPELLINGS {
        let db = Database::open_memory();
        arrive_create_table(
            &db,
            "windows",
            &[("id", "INTEGER PRIMARY KEY"), ("body", "TEXT")],
            &[clause],
        )
        .unwrap_or_else(|err| panic!("an arriving '{clause}' definition must apply: {err}"));
        let rendered = render(&db, "windows");
        assert!(
            rendered.contains(clause),
            "the arriving direction '{clause}' must be reconstructed into meta, got:\n{rendered}"
        );
    }
}

// ---------------------------------------------------------------------------
// C1b / C1d — the declarations travel on the REAL emitted changeset, not only
// on a fixture a test hand-built with the answer already in it
// ---------------------------------------------------------------------------

/// Every direction is emitted by the writing machine and reconstructed by the
/// receiving one. The fixture-built arrival tests above prove reconstruction;
/// this proves the declaration actually gets ONTO the wire — an implementation
/// that stores the direction but forgets to render it into outgoing DDL leaves
/// every other machine honoring a policy nobody declared.
#[test]
fn c1d_declared_directions_travel_through_the_real_changeset_on_create() {
    let source = Database::open_memory();
    for (table, clause) in [
        ("off_table", "SYNC OFF"),
        ("push_table", "SYNC PUSH ONLY"),
        ("pull_table", "SYNC PULL ONLY"),
        ("both_table", "SYNC TWO WAY"),
    ] {
        source
            .execute(
                &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT) {clause}"),
                &p(),
            )
            .unwrap_or_else(|err| panic!("{clause} must be declarable: {err}"));
    }

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiving machine must accept every definition");

    for (table, clause) in [
        ("off_table", "SYNC OFF"),
        ("push_table", "SYNC PUSH ONLY"),
        ("pull_table", "SYNC PULL ONLY"),
        ("both_table", "SYNC TWO WAY"),
    ] {
        assert_renders_exactly_direction(&render(&receiver, table), clause);
    }
}

/// A direction CHANGED after the table existed travels too — the `ALTER` a
/// operator runs when they move an installation, carried on the emitted
/// `AlterTable` rather than only in local metadata.
#[test]
fn c1d_an_altered_direction_travels_through_the_real_changeset() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &p(),
        )
        .expect("create");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiver takes the original definition");
    assert_renders_exactly_direction(&render(&receiver, "windows"), "SYNC TWO WAY");

    let before_alter = source.current_lsn();
    source
        .execute("ALTER TABLE windows SET SYNC PULL ONLY", &p())
        .expect("the direction changes on the writing machine");
    receiver
        .apply_changes(
            source.changes_since(before_alter),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiver must accept the changed direction");

    assert_renders_exactly_direction(&render(&receiver, "windows"), "SYNC PULL ONLY");
}

/// Declaring retention by `ALTER` on a two-way table travels as an `AlterTable`
/// and arrives still two-way. This is the door where the local projection used
/// to force push-only — and where it used to bail out of projecting at all.
#[test]
fn c1b_an_altered_retention_travels_and_leaves_the_direction_alone() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &p(),
        )
        .expect("create");
    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiver takes the original definition");

    let before_alter = source.current_lsn();
    source
        .execute("ALTER TABLE windows SET RETAIN 48 HOURS SYNC SAFE", &p())
        .expect("retention is declared on the writing machine");
    receiver
        .apply_changes(
            source.changes_since(before_alter),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiver must accept the retention declaration");

    let rendered = render(&receiver, "windows");
    assert!(
        rendered.contains("RETAIN 48 HOURS SYNC SAFE"),
        "the retention window must travel, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");
}

// ---------------------------------------------------------------------------
// C1e — the arrival door refuses every shape of the contradiction, not one
// ---------------------------------------------------------------------------

/// A table definition CHANGE arriving from another machine.
fn arrive_alter_table(
    db: &Database,
    name: &str,
    columns: &[(&str, &str)],
    constraints: &[&str],
    lsn: u64,
) -> Result<(), Error> {
    db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::AlterTable {
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
            ddl_lsn: vec![Lsn(lsn)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .map(|_| ())
}

const WINDOW_COLUMNS: [(&str, &str); 2] = [("id", "INTEGER PRIMARY KEY"), ("body", "TEXT")];

#[test]
fn c1e_an_arriving_sync_safe_declaration_with_pull_only_is_refused() {
    let db = Database::open_memory();
    let err = arrive_create_table(
        &db,
        "windows",
        &WINDOW_COLUMNS,
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC PULL ONLY"],
    )
    .expect_err("a pull-only table never delivers, so the arriving promise is refused");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC PULL ONLY");
    assert!(
        db.table_meta("windows").is_none(),
        "a refused arrival applies no schema side effects"
    );
}

/// The arriving `ALTER`, first order: a peer adds the delivery promise to a
/// table that this machine holds as non-delivering.
#[test]
fn c1e_an_arriving_alter_adding_sync_safe_to_a_non_delivering_table_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC PULL ONLY",
        &p(),
    )
    .expect("a pull-only table is fine until it promises delivery");

    let err = arrive_alter_table(
        &db,
        "windows",
        &WINDOW_COLUMNS,
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC PULL ONLY"],
        2,
    )
    .expect_err("the arriving ALTER must be refused exactly as the local one is");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC PULL ONLY");

    let meta = db.table_meta("windows").expect("the table still exists");
    assert!(
        !meta.sync_safe,
        "a refused arrival leaves no delivery promise behind: {meta:?}"
    );
}

/// The arriving `ALTER`, other order: a peer moves a table that already
/// promises delivery to a direction that never delivers it.
#[test]
fn c1e_an_arriving_alter_moving_a_sync_safe_table_off_delivery_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC SAFE",
        &p(),
    )
    .expect("create");

    let err = arrive_alter_table(
        &db,
        "windows",
        &WINDOW_COLUMNS,
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC OFF"],
        2,
    )
    .expect_err("a delivery promise cannot arrive with nowhere to deliver");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC OFF");
    assert!(
        !render(&db, "windows").contains("SYNC OFF"),
        "a refused arrival applies no part of itself"
    );
}

/// The cross-cases the pair above leaves as a diagonal. First: add the delivery
/// promise by arriving ALTER to a table this machine holds as `SYNC OFF` (the
/// other non-delivering direction, not the PULL ONLY covered above).
#[test]
fn c1e_an_arriving_alter_adding_sync_safe_to_a_sync_off_table_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &p(),
    )
    .expect("a sync-off table is fine until it promises delivery");

    let err = arrive_alter_table(
        &db,
        "windows",
        &WINDOW_COLUMNS,
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC OFF"],
        2,
    )
    .expect_err("the arriving ALTER must be refused exactly as the local one is");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC OFF");

    let meta = db.table_meta("windows").expect("the table still exists");
    assert!(
        !meta.sync_safe,
        "a refused arrival leaves no delivery promise behind: {meta:?}"
    );
}

/// The other cross-case: move an existing `SYNC SAFE` table to `SYNC PULL ONLY`
/// by arriving ALTER (the other non-delivering direction, not the SYNC OFF
/// covered above).
#[test]
fn c1e_an_arriving_alter_moving_a_sync_safe_table_to_pull_only_is_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC SAFE",
        &p(),
    )
    .expect("create");

    let err = arrive_alter_table(
        &db,
        "windows",
        &WINDOW_COLUMNS,
        &["RETAIN 48 HOURS SYNC SAFE", "SYNC PULL ONLY"],
        2,
    )
    .expect_err("a delivery promise cannot arrive moved to a direction that never delivers");
    assert_delivery_contradiction_refusal(&err.to_string(), "SYNC PULL ONLY");
    assert!(
        !render(&db, "windows").contains("SYNC PULL ONLY"),
        "a refused arrival applies no part of itself"
    );
}

// ---------------------------------------------------------------------------
// C1f — the retained two-way table WITHOUT the delivery promise
// ---------------------------------------------------------------------------

/// Every other two-way acceptance fixture carries `SYNC SAFE` as well. This one
/// does not: plain retention plus an explicit two-way direction, which is what a
/// hosted installation writes when it wants recovery but promises nothing about
/// deletion order. It must be accepted locally and on arrival, and the direction
/// must survive both.
#[test]
fn c1f_a_retained_two_way_table_without_the_delivery_promise_is_accepted() {
    let local = Database::open_memory();
    local
        .execute(
            "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) RETAIN 48 HOURS SYNC TWO WAY",
            &p(),
        )
        .expect("plain retention with an explicit two-way direction must be accepted");
    let rendered = render(&local, "windows");
    assert!(
        rendered.contains("RETAIN 48 HOURS") && !rendered.contains("SYNC SAFE"),
        "the window is declared and no delivery promise is invented, got:\n{rendered}"
    );
    assert_renders_exactly_direction(&rendered, "SYNC TWO WAY");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            local.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the same declaration must be accepted when it arrives from another machine");
    assert_renders_exactly_direction(&render(&receiver, "windows"), "SYNC TWO WAY");
}
