//! `HISTORY ALL` / `HISTORY CURRENT ONLY` -- a table's version-history policy,
//! declared alongside `RETAIN` (row lifetime), `SYNC ...` (travel), and
//! `SYNC CONFLICT ...` (arbitration).
//!
//! What these pin, in plain language: `HISTORY CURRENT ONLY` declares that
//! only a row's CURRENT version has consumer value, so superseded versions
//! may be reclaimed. That is only safe when the table cannot hand a stale
//! "first" value to a peer that pulls it -- so the declaration is refused on
//! a table that both DELIVERS rows to a peer (`SYNC PUSH ONLY` / `SYNC TWO
//! WAY`, the default) and arbitrates conflicts non-overwriting (`SYNC
//! CONFLICT KEEP FIRST`, the default when no conflict clause is written).
//! The refusal must fire at every door that can produce the combination:
//! local `CREATE TABLE`, local `ALTER TABLE ... SET HISTORY`, local `ALTER
//! TABLE ... SET SYNC CONFLICT`/`SET SYNC ...` widening the table into the
//! combination after the fact, an arriving `CreateTable` for a brand-new
//! table, and an arriving `CreateTable` that ADOPTS an existing table's
//! policy.
//!
//! The grammar/parser/`TableMeta` plumbing for the clause is real (parsing,
//! storage, the parse-time IMMUTABLE refusal). The refusal itself, the
//! `.schema`/`--json` rendering, the sync-DDL round trip, and the
//! maintenance-eligibility switch are NOT yet wired, so most of these tests
//! currently fail -- each one pins the contract the wiring must satisfy once
//! it lands.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::{ConflictPolicy as DeclaredConflictPolicy, HistoryPolicy, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn render(db: &Database, table: &str) -> String {
    let meta = db
        .table_meta(table)
        .unwrap_or_else(|| panic!("table '{table}' must exist"));
    contextdb_engine::cli_render::render_table_meta(table, &meta)
}

// ---------------------------------------------------------------------------
// The declaration itself: parses, stores, carries through a live session.
// (The renderer/json rendering tests live in
// crates/contextdb-cli/src/json_output.rs's own test module, next to the
// existing conflict-policy rendering tests -- that surface is `pub(crate)`
// there and not reachable from this workspace-level integration test.)
// ---------------------------------------------------------------------------

/// Both spellings parse and are visible on the in-session `TableMeta` --
/// real, wired plumbing, so every other door test below has a real
/// declaration to test refusal against.
#[test]
fn both_history_spellings_are_declarable_and_stored() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE keeps_all (id INTEGER PRIMARY KEY, v TEXT) HISTORY ALL",
        &p(),
    )
    .expect("HISTORY ALL must be declarable");
    db.execute(
        "CREATE TABLE keeps_current (id INTEGER PRIMARY KEY, v TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("HISTORY CURRENT ONLY must be declarable");

    assert_eq!(
        db.table_meta("keeps_all").unwrap().history_policy,
        Some(HistoryPolicy::All)
    );
    assert_eq!(
        db.table_meta("keeps_current").unwrap().history_policy,
        Some(HistoryPolicy::CurrentOnly)
    );
}

/// A table that declares no HISTORY clause carries no policy -- the engine
/// applies the keep-everything default without recording an operator choice
/// that was never made.
#[test]
fn an_undeclared_history_policy_is_none() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY)", &p())
        .expect("create");
    assert_eq!(db.table_meta("notes").unwrap().history_policy, None);
}

/// `IMMUTABLE` and `HISTORY CURRENT ONLY` are refused together at PARSE TIME:
/// an immutable table never supersedes a row, so it has no version history to
/// reclaim. This refusal is real (wired in the same commit as the grammar),
/// unlike the delivery-hazard refusal doors exercised below.
#[test]
fn immutable_and_history_current_only_are_refused_together() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE frozen (id INTEGER PRIMARY KEY) IMMUTABLE HISTORY CURRENT ONLY",
            &p(),
        )
        .expect_err("an immutable table declaring HISTORY CURRENT ONLY must be refused");
    let message = err.to_string();
    assert!(
        message.contains("IMMUTABLE") && message.contains("HISTORY"),
        "the refusal must name both clauses, got: {message}"
    );
}

// ---------------------------------------------------------------------------
// Refusal door: local CREATE TABLE
// ---------------------------------------------------------------------------

/// A table that both DELIVERS (the default direction, `SYNC TWO WAY`) and
/// arbitrates non-overwriting (the default conflict policy, keep-first) must
/// refuse `HISTORY CURRENT ONLY` at CREATE -- reclaiming history on a
/// keep-first table a peer can pull would hand that peer a stale value filed
/// as the FIRST write for the key. The message must say the table is
/// keep-first by default.
#[test]
fn create_table_refuses_history_current_only_on_a_default_keep_first_delivering_table() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
             HISTORY CURRENT ONLY",
            &p(),
        )
        .expect_err(
            "HISTORY CURRENT ONLY on a table that delivers under the default (keep-first) \
             conflict policy must be refused at CREATE",
        );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP FIRST")
            || message.to_ascii_uppercase().contains("KEEP LATEST"),
        "the refusal message must name the conflict-policy fix (declare SYNC CONFLICT KEEP \
         LATEST, or SYNC OFF), got: {message}"
    );
}

/// The same refusal fires when the conflict policy is written explicitly
/// rather than defaulted -- the hazard is a property of the EFFECTIVE
/// policy, not of whether a clause was typed.
#[test]
fn create_table_refuses_history_current_only_with_an_explicit_keep_first_declaration() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect_err("an explicit KEEP FIRST does not exempt the table from the refusal");
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP LATEST")
            || message.to_ascii_uppercase().contains("SYNC OFF"),
        "the refusal message must name the fix (declare SYNC CONFLICT KEEP LATEST, or SYNC \
         OFF), got: {message}"
    );
}

/// The escape hatches both work: `SYNC CONFLICT KEEP LATEST` (current-truth
/// declared) and `SYNC OFF` (never leaves this machine, so no peer can ever
/// receive a stale-first value) both make `HISTORY CURRENT ONLY` legal.
#[test]
fn create_table_accepts_history_current_only_via_either_escape_hatch() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE keep_latest_ok (id INTEGER PRIMARY KEY, v TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("HISTORY CURRENT ONLY + SYNC CONFLICT KEEP LATEST must be legal");
    db.execute(
        "CREATE TABLE sync_off_ok (id INTEGER PRIMARY KEY, v TEXT) \
         HISTORY CURRENT ONLY SYNC OFF",
        &p(),
    )
    .expect("HISTORY CURRENT ONLY + SYNC OFF must be legal (no peer can ever pull it)");
}

/// A `SYNC PULL ONLY` table never sends anything, so it cannot hand a peer a
/// stale value either -- the hazard is about DELIVERY, not about the policy
/// alone.
#[test]
fn create_table_accepts_history_current_only_on_a_pull_only_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE pull_only_ok (id INTEGER PRIMARY KEY, v TEXT) \
         HISTORY CURRENT ONLY SYNC PULL ONLY",
        &p(),
    )
    .expect("a table that never sends rows is not a stale-first-value hazard");
}

// ---------------------------------------------------------------------------
// Refusal door: local ALTER TABLE ... SET HISTORY
// ---------------------------------------------------------------------------

/// The same refusal must fire when `HISTORY CURRENT ONLY` arrives via ALTER
/// on an already-keep-first, already-delivering table -- not only at CREATE.
#[test]
fn alter_set_history_refuses_current_only_on_a_keep_first_delivering_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT)",
        &p(),
    )
    .expect("create a plain keep-first, two-way (delivering) table");

    let err = db
        .execute("ALTER TABLE device_status SET HISTORY CURRENT ONLY", &p())
        .expect_err("ALTER ... SET HISTORY CURRENT ONLY must be refused under keep-first delivery");
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP LATEST")
            || message.to_ascii_uppercase().contains("SYNC OFF"),
        "the refusal message must name the fix (declare SYNC CONFLICT KEEP LATEST, or SYNC \
         OFF), got: {message}"
    );
    assert!(
        !matches!(
            db.table_meta("device_status").unwrap().history_policy,
            Some(HistoryPolicy::CurrentOnly)
        ),
        "a refused ALTER must apply no part of itself: {err}"
    );
}

// ---------------------------------------------------------------------------
// Refusal door: local ALTER TABLE ... SET SYNC CONFLICT (narrowing an
// already-current-only table BACK to keep-first while it still delivers must
// be refused just as surely as declaring the combination directly)
// ---------------------------------------------------------------------------

#[test]
fn alter_set_sync_conflict_refuses_narrowing_to_keep_first_under_history_current_only() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("a keep-latest current-only delivering table is legal");

    let err = db
        .execute(
            "ALTER TABLE device_status SET SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect_err(
            "narrowing to KEEP FIRST while HISTORY CURRENT ONLY is declared under active \
             delivery must be refused -- this is the mirror of the CREATE-time refusal, and its \
             absence is exactly how that refusal gets defeated through the back door",
        );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("HISTORY"),
        "the refusal message must name the declared HISTORY CURRENT ONLY policy this ALTER \
         would leave in an unsafe combination, got: {message}"
    );
    assert_eq!(
        db.table_meta("device_status").unwrap().conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST),
        "a refused ALTER must apply no part of itself: {err}"
    );
}

// ---------------------------------------------------------------------------
// Refusal door: local ALTER TABLE ... SET SYNC <direction> (widening a
// PULL-only/OFF table that already carries HISTORY CURRENT ONLY under
// keep-first into a delivering direction)
// ---------------------------------------------------------------------------

#[test]
fn alter_set_sync_direction_refuses_widening_into_delivery_under_history_current_only() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
         HISTORY CURRENT ONLY SYNC OFF",
        &p(),
    )
    .expect("HISTORY CURRENT ONLY under SYNC OFF (keep-first default) is legal");

    let err = db
        .execute("ALTER TABLE device_status SET SYNC TWO WAY", &p())
        .expect_err(
            "widening a keep-first HISTORY CURRENT ONLY table into a delivering direction must \
             be refused -- SYNC TWO WAY would recreate exactly the CREATE-time hazard",
        );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("HISTORY")
            && (message.to_ascii_uppercase().contains("KEEP LATEST")
                || message.to_ascii_uppercase().contains("HISTORY ALL")),
        "the refusal message must name the declared HISTORY CURRENT ONLY policy and the fix \
         (declare SYNC CONFLICT KEEP LATEST first, or set HISTORY ALL), got: {message}"
    );
    assert_ne!(
        db.table_meta("device_status").unwrap().sync_direction,
        Some(contextdb_core::SyncDirection::Both),
        "a refused ALTER must apply no part of itself: {err}"
    );
}

// ---------------------------------------------------------------------------
// Refusal door: arriving CreateTable (brand-new table)
// ---------------------------------------------------------------------------

/// Matching the shipped precedent for `SYNC SAFE` on a keyless table and for
/// an undeliverable promise: a peer must not be able to install locally, via
/// synced DDL, the exact declaration every local door rejects.
#[test]
fn arriving_create_table_refuses_history_current_only_under_keep_first_delivery() {
    let db = Database::open_memory();
    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::CreateTable {
                    name: "device_status".to_string(),
                    columns: vec![
                        ("device_id".to_string(), "TEXT PRIMARY KEY".to_string()),
                        ("state".to_string(), "TEXT".to_string()),
                    ],
                    constraints: vec!["HISTORY CURRENT ONLY".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(1)],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err(
            "an arriving CreateTable declaring HISTORY CURRENT ONLY under keep-first delivery \
             must be refused at the preflight, exactly as the local CREATE door refuses it",
        );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP LATEST")
            || message.to_ascii_uppercase().contains("SYNC OFF"),
        "the refusal message must name the fix (declare SYNC CONFLICT KEEP LATEST, or SYNC \
         OFF), exactly as the local CREATE door's refusal does, got: {message}"
    );
    assert!(
        db.table_meta("device_status").is_none(),
        "a refused arriving CreateTable must leave no partial table behind"
    );
}

// ---------------------------------------------------------------------------
// Refusal door: arriving CreateTable that ADOPTS an existing table (the back
// door the adopt branch overwrites four policy fields through today)
// ---------------------------------------------------------------------------

#[test]
fn arriving_create_table_refuses_adopting_history_current_only_onto_an_existing_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT)",
        &p(),
    )
    .expect("a local keep-first, two-way table already exists here");

    let err = db
        .apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::CreateTable {
                    name: "device_status".to_string(),
                    columns: vec![
                        ("device_id".to_string(), "TEXT PRIMARY KEY".to_string()),
                        ("state".to_string(), "TEXT".to_string()),
                    ],
                    constraints: vec!["HISTORY CURRENT ONLY".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                }],
                ddl_lsn: vec![Lsn(1)],
                ..Default::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err(
            "the adopt branch must not install a refused combination onto an existing table \
             through the back door",
        );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP LATEST")
            || message.to_ascii_uppercase().contains("SYNC OFF"),
        "the refusal message must name the fix (declare SYNC CONFLICT KEEP LATEST, or SYNC \
         OFF), exactly as the local CREATE door's refusal does, got: {message}"
    );
    assert_ne!(
        db.table_meta("device_status").unwrap().history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "a refused adopt must leave the existing table's declaration untouched"
    );
}

// ---------------------------------------------------------------------------
// Transport round trip (the outbound half: `create_table_constraints_from_
// meta` does not yet emit HISTORY, and the inbound half:
// `rough_sync_table_meta` does not yet reconstruct it)
// ---------------------------------------------------------------------------

#[test]
fn a_declared_history_policy_travels_through_the_real_changeset() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("declare HISTORY CURRENT ONLY on a keep-latest table");

    let receiver = Database::open_memory();
    receiver
        .apply_changes(
            source.changes_since(Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("the receiving machine must accept the definition");

    assert_eq!(
        receiver.table_meta("device_status").unwrap().history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "the declared HISTORY policy must be reconstructed on the receiving machine, exactly \
         as the conflict policy and sync direction already are"
    );
}

/// The rendered `.schema` DDL is the declaration: re-executing it elsewhere
/// must reproduce the same policy. (This exercises the renderer from the
/// engine side; the CLI `--json` rendering is pinned in `contextdb-cli`'s
/// own test module.)
#[test]
fn the_rendered_history_policy_round_trips_through_schema() {
    let source = Database::open_memory();
    source
        .execute(
            "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("declare");
    let rendered = render(&source, "device_status");
    assert!(
        rendered.contains("HISTORY CURRENT ONLY"),
        ".schema must render the declared HISTORY policy, got:\n{rendered}"
    );

    let receiver = Database::open_memory();
    receiver
        .execute(&rendered, &p())
        .unwrap_or_else(|err| panic!("rendered DDL must execute: {err}\n{rendered}"));
    assert_eq!(
        render(&receiver, "device_status"),
        rendered,
        "the HISTORY policy must survive the .schema round trip"
    );
}

// ---------------------------------------------------------------------------
// The maintenance-eligibility switch and the synced-ALTER re-arming gap.
// Both still key off the hardcoded three-table list
// (`VERSION_COMPACTION_TABLES`), so a table that declares `HISTORY CURRENT
// ONLY` through any door is invisible to the maintenance loop today.
// ---------------------------------------------------------------------------

/// A freshly declared `HISTORY CURRENT ONLY` table must make the database
/// eligible for maintenance and start the loop -- eligibility must be
/// declaration-driven, not keyed to the fixed built-in table-name list.
#[test]
fn declaring_history_current_only_makes_the_database_maintenance_eligible() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare");

    let status = db.maintenance_status();
    assert!(
        status.currency_compaction_enabled,
        "a database holding a table that declares HISTORY CURRENT ONLY must report itself \
         maintenance-eligible for the version-cleanup pass, not only when the table's name is \
         one of the three hardcoded built-ins"
    );
    assert!(
        status.running,
        "eligibility must actually start the loop, exactly as a declared RETAIN window does"
    );
}

/// A `HISTORY CURRENT ONLY` declaration ARRIVING as a synced ALTER on an
/// already-existing table must start the maintenance loop without requiring
/// a reopen -- the same shipped defect exists for `RETAIN` today (the
/// arriving-AlterTable apply path never calls
/// `start_maintenance_if_eligible()`).
#[test]
fn history_current_only_arriving_as_an_alter_starts_maintenance_without_reopen() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE device_status (device_id TEXT PRIMARY KEY, state TEXT)",
        &p(),
    )
    .expect("a plain table already exists here");
    assert!(
        !db.maintenance_status().running,
        "a database with nothing declared starts no maintenance loop"
    );

    db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::AlterTable {
                name: "device_status".to_string(),
                // The real emitter (`alter_table_ddl_change`) always carries
                // the table's FULL post-alter column list here, not only
                // newly added columns -- `sync_create_table_sql`'s shape
                // validator builds a standalone `CREATE TABLE` from exactly
                // this list to check well-formedness, so an empty list here
                // (this table adds no columns) produces a `CREATE TABLE t
                // ()`, which is not valid DDL on its own.
                columns: vec![
                    ("device_id".to_string(), "TEXT PRIMARY KEY".to_string()),
                    ("state".to_string(), "TEXT".to_string()),
                ],
                constraints: vec![
                    "HISTORY CURRENT ONLY".to_string(),
                    "SYNC CONFLICT KEEP LATEST".to_string(),
                ],
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .expect("an arriving ALTER declaring a legal HISTORY CURRENT ONLY combination must apply");

    assert!(
        db.maintenance_status().running,
        "the arriving ALTER must start the maintenance loop on THIS machine without a reopen -- \
         waiting for the next process restart is a shipped defect this test closes"
    );
}

// ---------------------------------------------------------------------------
// A refused table's siblings in the SAME arriving batch: one table declaring
// an illegal HISTORY CURRENT ONLY combination must not block an unrelated,
// perfectly legal table riding in the same changeset. Forward-looking: the
// refusal itself does not exist yet (see the file-level doc comment above),
// so `apply_changes` currently succeeds outright here with no refusal at
// all. The refusal
// is wired at the SAME preflight the two adjacent sync-DDL refusals
// (`refuse_undeliverable_promise_sync_ddl`, `refuse_keyless_sync_safe_sync_
// ddl`) already use -- one pass over the WHOLE arriving changeset, before
// any of it applies, exactly like those two. There is no per-table partial-
// application path in the shipped sync-apply architecture (building one
// would be a materially larger, separate change touching all three
// preflight refusals uniformly), so one illegal table necessarily fails the
// whole batch together, matching its neighbors. The batch is safely
// retryable as a whole (nothing applied, no watermark advanced) -- the same
// contract an unconfirmed push already has elsewhere in this engine.
// ---------------------------------------------------------------------------

#[test]
fn a_refused_table_in_an_arriving_batch_fails_the_whole_batch_together() {
    let db = Database::open_memory();
    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "device_status".to_string(),
                    columns: vec![
                        ("device_id".to_string(), "TEXT PRIMARY KEY".to_string()),
                        ("state".to_string(), "TEXT".to_string()),
                    ],
                    // Illegal: HISTORY CURRENT ONLY under keep-first
                    // delivery with no escape hatch declared.
                    constraints: vec!["HISTORY CURRENT ONLY".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTable {
                    name: "sensor_log".to_string(),
                    columns: vec![
                        ("sensor_id".to_string(), "TEXT PRIMARY KEY".to_string()),
                        ("reading".to_string(), "TEXT".to_string()),
                    ],
                    // A perfectly ordinary, unrelated table with no
                    // HISTORY declaration at all.
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![Lsn(1), Lsn(2)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    );

    let err = result.expect_err(
        "one illegal table in an arriving batch must fail the WHOLE batch together -- matching \
         the two adjacent sync-DDL refusals this one sits beside, both of which already reject \
         the full changeset from the same preflight, before any of it applies",
    );
    let message = err.to_string();
    assert!(
        message.to_ascii_uppercase().contains("KEEP LATEST")
            || message.to_ascii_uppercase().contains("SYNC OFF"),
        "the refusal message must still name the fix for the offending table, got: {message}"
    );
    assert!(
        db.table_meta("sensor_log").is_none(),
        "a batch that fails together must apply NONE of it -- the unrelated sibling table must \
         not be partially applied either; the whole batch is safely retryable once the \
         offending table's declaration is fixed"
    );
    assert!(
        db.table_meta("device_status").is_none(),
        "the offending table itself must not be partially applied"
    );
}
