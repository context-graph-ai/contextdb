//! `SHOW SYNC_CONFLICT_POLICY` must reflect the policy a table DECLARED,
//! which is what the sync apply resolves against.
//!
//! The conflict policy a `CREATE ... SYNC CONFLICT ...` declares is stored on the
//! table meta; `sync_conflict_policy_for_table` resolves against that meta. But
//! `SHOW SYNC_CONFLICT_POLICY` read a separate in-memory runtime layer that no
//! product surface writes (its only setters were dead code), so a declared policy
//! never appeared. SHOW must read the declared meta so what it reports is what the
//! sync path actually uses.

use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::peer_directory::install_peer_directory_schema;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;

fn policy_rows(db: &Database) -> Vec<String> {
    let result = db
        .execute("SHOW SYNC_CONFLICT_POLICY", &HashMap::new())
        .expect("show");
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(t) => t.clone(),
            other => panic!("policy column must be text, got {other:?}"),
        })
        .collect()
}

#[test]
fn show_reflects_a_create_declared_conflict_policy() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &empty,
    )
    .expect("declare a table with a conflict policy");

    let rows = policy_rows(&db);
    assert!(
        rows.iter().any(|row| row == "notes=keep_latest"),
        "SHOW SYNC_CONFLICT_POLICY must reflect the CREATE-declared policy the \
         sync path resolves against; got {rows:?}"
    );

    // A table with no declared policy contributes no per-table row.
    db.execute(
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
        &empty,
    )
    .expect("declare a plain table");
    let rows = policy_rows(&db);
    assert!(
        !rows.iter().any(|row| row.starts_with("plain=")),
        "an undeclared table must not appear as a per-table policy row; got {rows:?}"
    );
    assert!(
        rows.iter().any(|row| row == "notes=keep_latest"),
        "the declared table still appears; got {rows:?}"
    );
}

/// `work_claims` / `work_results` / `work_cancellations` are hub-refereed:
/// they carry no `SYNC CONFLICT` clause in their own `CREATE TABLE` text at
/// all (see `work_ledger::CREATE_WORK_CLAIMS` and siblings), and instead ride
/// an engine-private `ServerWins` override merged in at every sync
/// chokepoint (`work_ledger::apply_work_ledger_policy_overrides_inner`) --
/// the hub's first-arrived row stands, a losing edge sees an attributed
/// conflict, behaviorally the same "keep the first accepted value" contract
/// `keep_first` already names for a declared table. Today `SHOW
/// SYNC_CONFLICT_POLICY` reads only the declared `TableMeta` layer, so these
/// three tables contribute NO row at all -- an operator has no way to see
/// that hub-refereed arbitration governs them. They must appear, in the same
/// policy vocabulary the declared rows use, marked as engine-owned so they
/// read distinctly from a table an operator actually declared.
#[test]
fn show_renders_engine_owned_arbitration_for_the_hub_refereed_work_ledger_tables() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let rows = policy_rows(&db);
    for table in ["work_claims", "work_results", "work_cancellations"] {
        let expected = format!("{table}=keep_first (engine-owned)");
        assert!(
            rows.iter().any(|row| row == &expected),
            "SHOW SYNC_CONFLICT_POLICY must render {table}'s engine-private hub-refereed \
             arbitration in policy vocabulary, visible and marked engine-owned; \
             expected a row {expected:?}, got {rows:?}"
        );
    }
}

/// The flip side of visibility: an engine-owned row must be visible-but-
/// UNSETTABLE. Nothing about rendering `work_claims` in `SHOW
/// SYNC_CONFLICT_POLICY` may make it look like ordinary operator-declared
/// policy an `ALTER TABLE ... SYNC CONFLICT ...` can move. Today none of
/// these three tables are on the engine-owned reserved-name list
/// (`executor::ENGINE_OWNED_LEDGER_TABLES` only names `work_inputs`,
/// `work_capabilities`, `peer_directory`, `work_node_contacts`), so this
/// declaration attempt is not refused at all -- it silently writes a
/// `TableMeta.conflict_policy` that the real sync path never consults (the
/// hardcoded `ServerWins` override always wins at the sync chokepoint),
/// leaving a declaration that lies about what governs the table.
#[test]
fn engine_owned_work_ledger_arbitration_refuses_a_local_declaration_attempt() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    let empty = HashMap::new();

    for table in ["work_claims", "work_results", "work_cancellations"] {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &empty,
            )
            .expect_err(&format!(
                "declaring a SYNC CONFLICT policy on the engine-owned, hub-refereed \
                 table {table} must refuse -- its real arbitration is never settable \
                 by a local declaration"
            ));
        match err {
            Error::SchemaInvalid { reason } => {
                assert!(
                    reason.contains(table) && reason.contains("engine-owned"),
                    "the refusal must name {table} as engine-owned infrastructure: {reason}"
                );
            }
            other => panic!(
                "expected Error::SchemaInvalid refusing the declaration attempt on {table}, \
                 got {other:?}"
            ),
        }
    }
}

/// The full set of engine-governed work-ledger tables, derived from
/// `work_ledger::work_ledger_conflict_policy_entries_inner` (every table
/// `apply_work_ledger_policy_overrides_inner` unconditionally overrides at
/// every sync chokepoint, so none of the seven has a locally-settable
/// arbitration), paired with a `SET SYNC CONFLICT` word that MISMATCHES its
/// true effective arbitration -- the same word the mirror guard
/// (`refuse_engine_owned_policy_mutation`) already tolerates as a verbatim
/// restate must still be refused when it actually changes the declared
/// value. `work_jobs` / `work_claims` / `work_results` / `work_failures` /
/// `work_cancellations` all arbitrate `keep_first` (three via the
/// engine-private `ServerWins` override with no declared clause at all, two
/// via a declared `SYNC CONFLICT KEEP FIRST` in their own `CREATE TABLE`
/// text) so `KEEP LATEST` mismatches all five; `work_inputs` also
/// arbitrates `keep_first` so `KEEP LATEST` mismatches it too;
/// `work_capabilities` arbitrates `keep_latest` so `KEEP FIRST` is the
/// mismatching word there.
const ENGINE_GOVERNED_WORK_LEDGER_TABLES_WITH_MISMATCHING_ALTER: [(&str, &str); 7] = [
    ("work_jobs", "KEEP LATEST"),
    ("work_claims", "KEEP LATEST"),
    ("work_results", "KEEP LATEST"),
    ("work_failures", "KEEP LATEST"),
    ("work_cancellations", "KEEP LATEST"),
    ("work_inputs", "KEEP LATEST"),
    ("work_capabilities", "KEEP FIRST"),
];

/// The true effective arbitration word for each engine-governed work-ledger
/// table, i.e. what `SHOW SYNC_CONFLICT_POLICY` must render regardless of
/// what a (refused) local declaration attempt asked for.
const ENGINE_GOVERNED_WORK_LEDGER_TABLES_TRUE_ARBITRATION: [(&str, &str); 7] = [
    ("work_jobs", "keep_first"),
    ("work_claims", "keep_first"),
    ("work_results", "keep_first"),
    ("work_failures", "keep_first"),
    ("work_cancellations", "keep_first"),
    ("work_inputs", "keep_first"),
    ("work_capabilities", "keep_latest"),
];

/// Blocker: the cold review proved `work_jobs` (and, by the same gap,
/// `work_failures`) are engine-governed -- their real arbitration is the
/// hardcoded override in `work_ledger_conflict_policy_entries_inner`, merged
/// in unconditionally at every sync chokepoint -- but neither is on the
/// hub-refereed refusal list (`work_ledger::hub_refereed_engine_owned_conflict_policy_display`
/// only names the three `ServerWins` tables) NOR on the reserved-shape list
/// (`executor::ENGINE_OWNED_LEDGER_TABLES` only names `work_inputs`,
/// `work_capabilities`, `peer_directory`, `work_node_contacts`). So today
/// `ALTER TABLE work_jobs SET SYNC CONFLICT KEEP LATEST` is silently
/// ACCEPTED: it rewrites the declared `TableMeta.conflict_policy` that
/// `SHOW SYNC_CONFLICT_POLICY`'s first (undeclared-table) loop reads, while
/// the actual sync path keeps arbitrating `keep_first` regardless -- a
/// declaration that lies about what governs the table. Every one of the
/// seven engine-governed work-ledger tables must refuse a mismatching local
/// declaration the same way the three hub-refereed ones already do.
#[test]
fn every_engine_governed_work_ledger_table_refuses_a_mismatching_alter() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    let empty = HashMap::new();

    for (table, word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_WITH_MISMATCHING_ALTER {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT {word}"),
                &empty,
            )
            .expect_err(&format!(
                "declaring a SYNC CONFLICT policy on the engine-governed work-ledger \
                 table {table} must refuse -- its real arbitration is never settable by \
                 a local declaration"
            ));
        match err {
            Error::SchemaInvalid { reason } => {
                assert!(
                    reason.contains(table) && reason.contains("engine-owned"),
                    "the refusal must name {table} as engine-owned infrastructure: {reason}"
                );
            }
            other => panic!(
                "expected Error::SchemaInvalid refusing the declaration attempt on {table}, \
                 got {other:?}"
            ),
        }
    }
}

/// Blocker, visibility half: `SHOW SYNC_CONFLICT_POLICY` must mark every one
/// of the seven engine-governed work-ledger tables `(engine-owned)` with its
/// TRUE effective arbitration word, not just the three `ServerWins` tables
/// `hub_refereed_engine_owned_conflict_policy_display` names today.
/// `work_jobs` / `work_failures` / `work_inputs` currently render only via
/// the generic declared-`TableMeta` loop (because their own `CREATE TABLE`
/// text carries a `SYNC CONFLICT` clause) -- UNMARKED, indistinguishable
/// from a table an operator declared and can still move. Before the fix,
/// this must fail because those four rows (`work_jobs`, `work_failures`,
/// `work_inputs`, `work_capabilities`) are missing the `(engine-owned)`
/// suffix.
#[test]
fn show_marks_every_engine_governed_work_ledger_table_engine_owned_with_its_true_arbitration() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let rows = policy_rows(&db);
    for (table, word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_TRUE_ARBITRATION {
        let expected = format!("{table}={word} (engine-owned)");
        assert!(
            rows.iter().any(|row| row == &expected),
            "SHOW SYNC_CONFLICT_POLICY must render {table}'s true engine-governed \
             arbitration, marked engine-owned; expected a row {expected:?}, got {rows:?}"
        );
        let unmarked = format!("{table}={word}");
        assert!(
            !rows.iter().any(|row| row == &unmarked),
            "{table} must never render unmarked -- an unmarked row reads as \
             operator-declared and settable; got {rows:?}"
        );
    }
}

/// Blocker, no-lie-survives half: a REFUSED `ALTER ... SET SYNC CONFLICT`
/// attempt against an engine-governed work-ledger table must leave `SHOW
/// SYNC_CONFLICT_POLICY` reporting the unchanged, true arbitration -- never
/// the value the refused declaration asked for. Before the fix, `work_jobs`
/// and `work_failures` are not refused at all, so the ALTER silently
/// succeeds and `SHOW` renders the LIE (`work_jobs=keep_latest`, unmarked)
/// while the sync path keeps arbitrating `keep_first`.
#[test]
fn show_marker_and_true_arbitration_survive_an_attempted_alter_on_every_engine_governed_table() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    let empty = HashMap::new();

    for (table, word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_WITH_MISMATCHING_ALTER {
        // The attempt is expected to fail (covered by the refusal test
        // above); ignore the outcome here and check only that SHOW never
        // reflects it.
        let _ = db.execute(
            &format!("ALTER TABLE {table} SET SYNC CONFLICT {word}"),
            &empty,
        );
    }

    let rows = policy_rows(&db);
    for (table, true_word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_TRUE_ARBITRATION {
        let expected = format!("{table}={true_word} (engine-owned)");
        assert!(
            rows.iter().any(|row| row == &expected),
            "after a refused ALTER, SHOW SYNC_CONFLICT_POLICY must still report {table}'s \
             true engine-owned arbitration {expected:?}; got {rows:?}"
        );
    }
    for (table, mismatching_word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_WITH_MISMATCHING_ALTER {
        let lie_unmarked = format!(
            "{table}={}",
            mismatching_word.to_lowercase().replace(' ', "_")
        );
        assert!(
            !rows.iter().any(|row| row == &lie_unmarked),
            "SHOW SYNC_CONFLICT_POLICY must never render the value a refused ALTER asked \
             for; found the lie {lie_unmarked:?} for {table} in {rows:?}"
        );
    }
}

/// The contrast case (resolves the "unmarked" ambiguity): a table an
/// operator actually creates and declares a `SYNC CONFLICT` policy on stays
/// unmarked in `SHOW SYNC_CONFLICT_POLICY` and stays settable by a later
/// `ALTER ... SET SYNC CONFLICT ...` -- "unmarked" means "operator-governed
/// and this instance's declaration is authoritative," which only holds if
/// no engine-governed table ever renders unmarked (the two tests above).
#[test]
fn user_declared_table_stays_unmarked_and_alterable() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        &empty,
    )
    .expect("declare an operator table with a conflict policy");

    let rows = policy_rows(&db);
    assert!(
        rows.iter().any(|row| row == "notes=keep_first"),
        "an operator-declared table must render unmarked (no '(engine-owned)' suffix); \
         got {rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| row == "notes=keep_first (engine-owned)"),
        "an operator-declared table must never be marked engine-owned; got {rows:?}"
    );

    db.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST", &empty)
        .expect("an operator-declared table's conflict policy must stay settable");
    let rows = policy_rows(&db);
    assert!(
        rows.iter().any(|row| row == "notes=keep_latest"),
        "the operator's ALTER must actually move the declared policy; got {rows:?}"
    );
}

/// Blocker (fix round 2, item 1 -- ghost rows): `install_work_ledger_schema`
/// is OPT-IN, called only by fabric/server paths that actually use the work
/// ledger (`work_ledger.rs`'s own doc comment: "Create the seven ledger
/// tables if absent"). A plain `Database::open_memory()` with no install
/// call has none of the seven tables -- `SELECT * FROM work_jobs` fails
/// "table not found". But `SHOW SYNC_CONFLICT_POLICY`'s engine-owned loop
/// (`executor.rs`) renders `engine_owned_work_ledger_conflict_policy_display()`
/// unconditionally, with no existence check against `db.table_names()`, so
/// today it lists all seven "(engine-owned)" rows anyway -- ghost rows for
/// tables that do not exist in this store. `SHOW` must report what actually
/// governs THIS store, not what would govern it if the ledger were
/// installed.
#[test]
fn show_never_renders_work_ledger_rows_when_the_ledger_is_not_installed() {
    let db = Database::open_memory();

    // Confirm the premise: the ledger really is absent from this store.
    let err = db
        .execute("SELECT * FROM work_jobs", &HashMap::new())
        .expect_err("a plain open must not have the work ledger installed");
    match err {
        Error::TableNotFound(table) => assert_eq!(
            table, "work_jobs",
            "expected the not-found error to name work_jobs, got {table:?}"
        ),
        other => panic!("expected Error::TableNotFound(\"work_jobs\"), got {other:?}"),
    }

    let rows = policy_rows(&db);
    for (table, _) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_TRUE_ARBITRATION {
        assert!(
            !rows.iter().any(|row| row.starts_with(&format!("{table}="))),
            "a table this store never installed must not appear in SHOW \
             SYNC_CONFLICT_POLICY at all (ghost row); table {table:?} appeared in {rows:?}"
        );
    }
}

/// The installed-store contrast half of the ghost-row fix: once
/// `install_work_ledger_schema` actually runs, all seven tables exist and
/// SHOW must render every one, marked engine-owned, exactly as the round-1
/// tests above already pin.
#[test]
fn show_renders_work_ledger_rows_once_the_ledger_is_installed() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let rows = policy_rows(&db);
    for (table, word) in ENGINE_GOVERNED_WORK_LEDGER_TABLES_TRUE_ARBITRATION {
        let expected = format!("{table}={word} (engine-owned)");
        assert!(
            rows.iter().any(|row| row == &expected),
            "once installed, {table} must render {expected:?}; got {rows:?}"
        );
    }
}

/// Blocker (fix round 2, item 2 -- refusal text honesty): the engine-owned
/// ALTER refusal (`engine_owned_policy_refusal`) is shared wording for two
/// structurally different situations. For `work_inputs` / `work_capabilities`
/// (and the other `ENGINE_OWNED_LEDGER_TABLES`) the claim "its SYNC CONFLICT
/// declaration lives once, in the engine's own CREATE TABLE text for
/// {table}" is TRUE -- `CREATE_WORK_INPUTS` / `CREATE_WORK_CAPABILITIES`
/// really do carry a `SYNC CONFLICT` clause. For `work_claims` / `work_results`
/// / `work_cancellations` it is FALSE: their own `CREATE TABLE` text
/// (`CREATE_WORK_CLAIMS` etc, `work_ledger.rs`) carries no `SYNC CONFLICT`
/// clause at all -- their real arbitration is a purely engine-INTERNAL
/// `ServerWins` override (`apply_work_ledger_policy_overrides_inner`) that
/// no DDL text anywhere declares. Today's refusal message points a
/// dissatisfied operator at DDL text that does not exist. This does not pin
/// exact prose -- only that the "lives... in the engine's own CREATE TABLE
/// text" claim is ABSENT for these three, while the table name and
/// "engine-owned" framing are still present.
#[test]
fn hub_refereed_refusal_never_claims_a_create_table_text_that_does_not_exist() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    let empty = HashMap::new();

    for table in ["work_claims", "work_results", "work_cancellations"] {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &empty,
            )
            .expect_err(&format!("{table}'s declaration attempt must still refuse"));
        match err {
            Error::SchemaInvalid { reason } => {
                assert!(
                    reason.contains(table) && reason.contains("engine-owned"),
                    "the refusal must still name {table} as engine-owned infrastructure: {reason}"
                );
                assert!(
                    !reason.contains("lives once, in the engine's own CREATE TABLE text"),
                    "{table} carries NO SYNC CONFLICT clause in its own CREATE TABLE text \
                     at all (see CREATE_WORK_CLAIMS/CREATE_WORK_RESULTS/CREATE_WORK_CANCELLATIONS \
                     in work_ledger.rs) -- the refusal must not point at DDL text that does \
                     not exist: {reason}"
                );
            }
            other => panic!("expected Error::SchemaInvalid for {table}, got {other:?}"),
        }
    }
}

/// Blocker (fix round 2, item 4 -- existence first): for the five
/// work-ledger tables covered only by
/// `refuse_hub_refereed_ledger_sync_conflict_declaration` (`work_jobs`,
/// `work_claims`, `work_results`, `work_failures`, `work_cancellations` --
/// the two `ENGINE_OWNED_LEDGER_TABLES` members `work_inputs`/
/// `work_capabilities` take a different door and already get this right),
/// that door is a static name check with no existence lookup at all, called
/// BEFORE `db.table_meta(&p.table)` in the `AlterAction::SetSyncConflict`
/// arm (`executor.rs`). So on a store where the ledger was never installed,
/// `ALTER TABLE work_claims SET SYNC CONFLICT KEEP LATEST` returns the
/// engine-owned refusal -- implying a real, governed table -- instead of
/// "table not found", which is what every OTHER unknown-table ALTER
/// reports and what a caller must see to tell "this table doesn't exist
/// yet" apart from "this table exists and is engine-governed".
#[test]
fn alter_on_a_hub_refereed_table_reports_not_found_before_engine_owned_when_ledger_absent() {
    let db = Database::open_memory();
    let empty = HashMap::new();

    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
    ] {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &empty,
            )
            .expect_err(&format!(
                "{table} does not exist in this store, so its ALTER must fail somehow"
            ));
        match err {
            Error::Other(reason) => {
                assert!(
                    reason.contains("not found"),
                    "expected a table-not-found error for absent {table}, got: {reason}"
                );
            }
            Error::SchemaInvalid { reason } => panic!(
                "{table} does not exist in this store yet -- ALTER must report \
                 'table not found', not the engine-owned refusal (existence must be \
                 checked before ownership): {reason}"
            ),
            other => {
                panic!("expected Error::Other('table not found') for absent {table}, got {other:?}")
            }
        }
    }
}

/// The installed-store contrast half of item 4: once the ledger exists,
/// the SAME `ALTER` on the SAME five tables must produce the engine-owned
/// refusal (already pinned by
/// `every_engine_governed_work_ledger_table_refuses_a_mismatching_alter`
/// above) -- existence-first ordering must not change what a genuinely
/// installed table reports.
#[test]
fn alter_on_a_hub_refereed_table_reports_engine_owned_when_ledger_installed() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    let empty = HashMap::new();

    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
    ] {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &empty,
            )
            .expect_err(&format!("{table}'s declaration attempt must refuse"));
        match err {
            Error::SchemaInvalid { reason } => {
                assert!(
                    reason.contains(table) && reason.contains("engine-owned"),
                    "the refusal must name {table} as engine-owned infrastructure: {reason}"
                );
            }
            other => panic!(
                "expected Error::SchemaInvalid refusing the declaration attempt on {table}, \
                 got {other:?}"
            ),
        }
    }
}

// Item 5 (EdgeWins arm in `conflict_policy_display_word`) is NOT covered
// here: `conflict_policy_display_word` is a private `fn` in `work_ledger.rs`
// with no `#[cfg(feature = "test-seams")]` export, called from exactly one
// site (`engine_owned_work_ledger_conflict_policy_display`) whose only input
// is the fixed 7-entry `work_ledger_conflict_policy_entries_inner()` array --
// which never contains `ConflictPolicy::EdgeWins` (only `ServerWins`,
// `InsertIfNotExists`, `LatestWins` appear there). No product path can drive
// `EdgeWins` into this function from outside `work_ledger.rs`, so the
// `LatestWins | EdgeWins => "keep_latest"` arm is unreachable from any test
// surface available to this suite. The implementer should close it
// structurally (e.g. drop `EdgeWins` from the match and let the exhaustive
// match fail to compile if a future caller ever feeds it a wider policy
// set, or add a `debug_assert!`/explicit `unreachable!()` for it) rather
// than relying on a test pin here.

// ---------------------------------------------------------------------
// Fix round 5, item 3 (blocker -- MARK ALL ENGINE-GOVERNED ROWS): round
// 1's ruling was "unmarked = operator-governed and settable, so no
// engine-governed table may ever render unmarked" -- but `SHOW
// SYNC_CONFLICT_POLICY`'s engine-owned second loop (`executor.rs`) only
// iterates `engine_owned_work_ledger_conflict_policy_display()`, the SEVEN
// work-ledger tables. `peer_directory` is engine-owned too (one of the
// nine reserved names; ALTER on it is already refused via
// `refuse_engine_owned_policy_axes` / `ENGINE_OWNED_LEDGER_TABLES`, proven
// by `set_history_all_on_peer_directory_refuses` in the sibling suite) and
// declares its own canonical `SYNC CONFLICT KEEP LATEST`
// (`peer_directory::CREATE_PEER_DIRECTORY`), so it falls through to the
// FIRST (generic declared-`TableMeta`) loop and renders UNMARKED --
// indistinguishable from a table an operator declared and can still move.
// ---------------------------------------------------------------------

/// RED: with `peer_directory` installed, `SHOW SYNC_CONFLICT_POLICY` must
/// render it with its true arbitration word AND the `(engine-owned)`
/// marker, never the unmarked form -- the same contract already proven for
/// the seven work-ledger tables above.
#[test]
fn show_marks_peer_directory_engine_owned_with_its_true_arbitration() {
    let db = Database::open_memory();
    install_peer_directory_schema(&db).expect("install peer_directory schema");

    let rows = policy_rows(&db);
    let expected = "peer_directory=keep_latest (engine-owned)";
    assert!(
        rows.iter().any(|row| row == expected),
        "peer_directory is engine-owned (ALTER on it is already refused) and must render \
         marked, exactly like the seven work-ledger tables; expected a row {expected:?}, \
         got {rows:?}"
    );
    let unmarked = "peer_directory=keep_latest";
    assert!(
        !rows.iter().any(|row| row == unmarked),
        "peer_directory must never render unmarked -- an unmarked row reads as \
         operator-governed and settable, which is false for it; got {rows:?}"
    );
}

/// GREEN pin, scope note: `work_node_contacts` (the ninth reserved name)
/// declares NO canonical `SYNC CONFLICT` value at all
/// (`engine_owned_ledger_policy("work_node_contacts").conflict == None`,
/// `executor.rs`), and `refuse_engine_owned_policy_axes` refuses ANY
/// explicit conflict-policy declaration on it (since any explicit value
/// differs from `None`) -- so it can never carry a `conflict_policy` on its
/// `TableMeta` at all, marked or unmarked, exactly like a plain table that
/// never declared `SYNC CONFLICT`
/// (`show_reflects_a_create_declared_conflict_policy`'s `plain` case
/// above). It is therefore structurally out of scope for the "renders
/// unmarked" defect the other pins above guard against -- there is no row
/// to mismark. This pin documents that boundary rather than asserting a
/// marked row that can never exist.
#[test]
fn work_node_contacts_never_appears_in_show_since_it_declares_no_conflict_policy() {
    let db = Database::open_memory();
    // work_node_contacts is contextdb-server's table; its shape is
    // reproduced here without a contextdb-server dependency, matching the
    // sibling suite's own pattern (`set_history_all_on_work_node_contacts_refuses`).
    db.execute(
        "CREATE TABLE work_node_contacts (\
            node_id TEXT PRIMARY KEY, \
            last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("create work_node_contacts");

    let rows = policy_rows(&db);
    assert!(
        !rows
            .iter()
            .any(|row| row.starts_with("work_node_contacts=")),
        "work_node_contacts declares no SYNC CONFLICT value at all, so it must not appear \
         in SHOW SYNC_CONFLICT_POLICY in any form; got {rows:?}"
    );
}
