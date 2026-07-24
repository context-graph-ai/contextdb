//! The four built-in ledger tables (`work_inputs`, `work_capabilities`,
//! `peer_directory`, `work_node_contacts`) declare their own RETAIN /
//! HISTORY / SYNC CONFLICT / SYNC .../ SYNC SAFE policy exactly once, in
//! their own `CREATE TABLE` text -- and that declaration is an ENGINE
//! INVARIANT, not operator policy: the engine's own bookkeeping (retention
//! pruning, version-history reclaim) depends on each one staying at its
//! declared shape, so it is never something a command-typed `ALTER TABLE`,
//! an arriving sync `AlterTable`, or an arriving sync `CreateTable` adopting
//! an already-existing table may change.
//!
//! Four doors enforce it, all judging the same four canonical shapes
//! (`engine_owned_ledger_policy`):
//! - `refuse_engine_owned_policy_mutation` (`executor.rs`) -- the LOCAL
//!   ALTER door; refuses an operator's own `ALTER TABLE` outright, before
//!   the write lock is taken. Covers every clause that can move a table's
//!   policy: `RETAIN` / `HISTORY` / `SYNC CONFLICT` / `SYNC ...`
//!   (`AlterAction::SetSyncDirection`) alike.
//! - `refuse_engine_owned_reserved_name_shape` +
//!   `refuse_engine_owned_policy_axes` (`executor.rs`, both called from the
//!   local `PhysicalPlan::CreateTable` arm) -- the LOCAL fresh-`CREATE TABLE`
//!   door; a consumer typing one of the four reserved names directly is
//!   refused if the declared COLUMNS don't structurally match the owning
//!   installer's own `CREATE TABLE` text, or if an EXPLICIT policy clause
//!   differs from canonical -- naming the installer as the table's owner
//!   either way.
//! - `refuse_engine_owned_policy_sync_ddl` (`database.rs`) -- the SYNC-APPLY
//!   preflight; refuses an EXPLICIT differing value arriving over sync,
//!   whether spelled as an `AlterTable` or as a `CreateTable` adopting an
//!   already-existing table, atomically for the whole batch before any of it
//!   is written.
//! - The sync-apply merge (`engine_owned_merged_policy`) -- once a batch
//!   clears the door above, an axis the arriving shape is SILENT on
//!   PRESERVES the table's current declared value instead of clearing it.
//!   Every `AlterTable` / `CreateTable`-adopt emitter bakes a table's FULL
//!   current shape into the wire, whether or not a given axis actually
//!   changed, so a half-healed peer's own in-progress multi-step reconcile
//!   (or a dropped clause from an unguarded/older binary) must interoperate
//!   rather than being read as an implicit clear; an axis the arriving shape
//!   DOES restate is, by the time this merge runs, guaranteed to already
//!   equal canonical, and adopting it verbatim is what heals a legacy
//!   pre-declaration root through a peer's push exactly like a local
//!   reconcile `ALTER` heals it.
//!
//! A plain operator table is entirely unaffected: the identical clause on a
//! non-built-in table applies exactly as it always has, and creating a table
//! under any name outside the four reserved ones is unrestricted.
//!
//! Before the local door existed, the installers' own reconcile arms
//! (`install_work_ledger_schema`, `install_peer_directory_schema`) were the
//! ONLY thing standing between an operator's `DROP RETAIN` / `SET HISTORY
//! ALL` / `SET SYNC CONFLICT KEEP FIRST` and that operator's own rows being
//! silently deleted the next time a fabric-consumer process opened the
//! database -- the reconcile arm cannot tell "an operator just opted out" apart
//! from "this root predates the declaration," and silently re-applied the
//! engine's declaration either way. Shutting that door is what makes that
//! silent reversion structurally impossible for a LOCAL `ALTER`. The sync
//! side had its own version of the same gap until the preflight+merge pair
//! above closed it too: an arriving DDL silent on a policy axis used to be
//! taken as an implicit clear, so a peer (or a hand-crafted changeset) could
//! drop `work_inputs`' `RETAIN` window, or adopt an off-canonical
//! `SYNC CONFLICT` / `SYNC` direction wholesale through the `CreateTable`
//! adopt back door, without ever going through the refused local `ALTER`
//! shape. See the tests below for both.

use contextdb_core::{Error, HistoryPolicy, Lsn, SyncDirection, Value};
use contextdb_engine::Database;
use contextdb_engine::peer_directory::install_peer_directory_schema;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;

/// `work_capabilities`' full declared column list, reused by every probe
/// below that needs to hand-craft an arriving `AlterTable` / `CreateTable`
/// for it (a structural mismatch on the columns would raise a DIFFERENT
/// error than the policy-axis refusal these tests are pinning).
fn work_capabilities_columns() -> Vec<(String, String)> {
    vec![
        ("capability_key".to_string(), "TEXT PRIMARY KEY".to_string()),
        ("node_id".to_string(), "TEXT NOT NULL".to_string()),
        ("capability_id".to_string(), "TEXT NOT NULL".to_string()),
        ("tags".to_string(), "JSON NOT NULL".to_string()),
        ("detail".to_string(), "JSON".to_string()),
        (
            "advertised_at".to_string(),
            "TIMESTAMP NOT NULL".to_string(),
        ),
    ]
}

/// `work_inputs`' full declared column list, for the same reason.
fn work_inputs_columns() -> Vec<(String, String)> {
    vec![
        ("input_key".to_string(), "TEXT PRIMARY KEY".to_string()),
        ("job_id".to_string(), "TEXT NOT NULL".to_string()),
        ("seq".to_string(), "INTEGER NOT NULL".to_string()),
        ("payload".to_string(), "TEXT NOT NULL".to_string()),
    ]
}

fn apply_single_ddl(db: &Database, ddl: DdlChange) -> contextdb_core::Result<()> {
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1)],
    };
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .map(|_| ())
}

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn schema_invalid_reason(err: Error) -> String {
    match err {
        Error::SchemaInvalid { reason } => reason,
        other => panic!("expected Error::SchemaInvalid, got {other:?}"),
    }
}

#[test]
fn drop_retain_on_work_inputs_refuses_with_engine_owned_message() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_inputs DROP RETAIN", &p())
        .expect_err("DROP RETAIN on the engine-owned work_inputs table must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(
        reason.contains("RETAIN"),
        "must name the RETAIN clause: {reason}"
    );

    // Refused before the write lock is taken -- the declared window survives.
    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert_eq!(
        meta.default_ttl_seconds,
        Some(7 * 24 * 60 * 60),
        "a refused DROP RETAIN must apply no part of itself"
    );
}

#[test]
fn set_retain_to_a_different_window_on_work_inputs_also_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_inputs SET RETAIN 30 DAYS", &p())
        .expect_err("widening work_inputs' RETAIN window is engine policy too, not just DROP");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("RETAIN"),
        "{reason}"
    );

    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert_eq!(meta.default_ttl_seconds, Some(7 * 24 * 60 * 60));
}

#[test]
fn set_sync_conflict_keep_first_on_work_capabilities_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute(
            "ALTER TABLE work_capabilities SET SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect_err("SET SYNC CONFLICT KEEP FIRST on work_capabilities must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(
        reason.contains("SYNC CONFLICT"),
        "must name the clause: {reason}"
    );

    let meta = db.table_meta("work_capabilities").expect("exists");
    assert_eq!(meta.conflict_policy, Some(ConflictPolicy::LatestWins));
}

#[test]
fn set_history_all_on_peer_directory_refuses() {
    let db = Database::open_memory();
    install_peer_directory_schema(&db).expect("install peer_directory");

    let err = db
        .execute("ALTER TABLE peer_directory SET HISTORY ALL", &p())
        .expect_err("SET HISTORY ALL on peer_directory must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(reason.contains("HISTORY"), "must name the clause: {reason}");

    let meta = db.table_meta("peer_directory").expect("exists");
    assert_eq!(meta.history_policy, Some(HistoryPolicy::CurrentOnly));
}

#[test]
fn set_history_all_on_work_node_contacts_refuses() {
    // work_node_contacts is contextdb-server's table (`SYNC OFF`), but the
    // door is table-name-scoped in contextdb-engine, so its shape can be
    // reproduced here without a contextdb-server dependency.
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_node_contacts (\
            node_id TEXT PRIMARY KEY, \
            last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &p(),
    )
    .expect("create work_node_contacts");

    let err = db
        .execute("ALTER TABLE work_node_contacts SET HISTORY ALL", &p())
        .expect_err("SET HISTORY ALL on work_node_contacts must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_node_contacts") && reason.contains("engine-owned"),
        "{reason}"
    );

    let meta = db.table_meta("work_node_contacts").expect("exists");
    assert_eq!(meta.history_policy, Some(HistoryPolicy::CurrentOnly));
}

/// Green guard: the identical clauses on a plain operator table are
/// entirely unaffected -- the door is table-name-scoped to the four
/// built-ins, not a blanket refusal of RETAIN / HISTORY / SYNC CONFLICT
/// mutations everywhere.
#[test]
fn equivalent_alter_on_a_user_table_still_works() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("create notes");

    db.execute("ALTER TABLE notes SET RETAIN 3 DAYS", &p())
        .expect("SET RETAIN on a user table must still work");
    // KEEP LATEST before HISTORY CURRENT ONLY, matching the order the
    // reconcile arms use -- the table must never be observed keep-first
    // while HISTORY CURRENT ONLY is declared
    // (`refuse_reclaimed_history_under_keep_first` refuses that combination
    // regardless of which table it is on, unrelated to this door).
    db.execute("ALTER TABLE notes SET SYNC CONFLICT KEEP LATEST", &p())
        .expect("SET SYNC CONFLICT on a user table must still work");
    db.execute("ALTER TABLE notes SET HISTORY CURRENT ONLY", &p())
        .expect("SET HISTORY on a user table must still work");
    db.execute("ALTER TABLE notes DROP RETAIN", &p())
        .expect("DROP RETAIN on a user table must still work");

    let meta = db.table_meta("notes").expect("notes exists");
    assert_eq!(
        meta.default_ttl_seconds, None,
        "the DROP RETAIN above must have taken effect"
    );
    assert_eq!(meta.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(meta.conflict_policy, Some(ConflictPolicy::LatestWins));
}

/// The reconcile arms stay exactly as they were written -- their own exact
/// healing calls (`SET RETAIN 7 DAYS`, `SET SYNC CONFLICT KEEP LATEST`,
/// `SET HISTORY CURRENT ONLY`) restate the engine's own declared shape
/// verbatim, so this door must let them through. `builtin_schema_reconcile_
/// tests.rs` already proves the reconcile end-to-end on a real reopened
/// root; this pins the door's own contract directly: restating the
/// currently-declared value is not a mutation.
#[test]
fn restating_the_declared_value_verbatim_is_not_refused() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    db.execute("ALTER TABLE work_inputs SET RETAIN 7 DAYS", &p())
        .expect("restating work_inputs' own declared RETAIN window must not refuse");
    db.execute(
        "ALTER TABLE work_capabilities SET SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("restating work_capabilities' own declared SYNC CONFLICT must not refuse");
    db.execute(
        "ALTER TABLE work_capabilities SET HISTORY CURRENT ONLY",
        &p(),
    )
    .expect("restating work_capabilities' own declared HISTORY must not refuse");
}

/// The arriving-sync-DDL door refuses the identical mutation, built from a
/// hand-crafted incoming changeset that bypasses local execution entirely --
/// exactly the shape a stale/unguarded peer or a malicious changeset would
/// produce (the local door above makes this unreachable through this
/// engine's own `execute`).
///
/// DDL preflight validates a whole changeset atomically before any of it is
/// written (`preflight_sync_ddl_mixed_apply` walks every DDL item across
/// every LSN group before `apply_changes_single_lsn_group` ever runs), so a
/// changeset carrying BOTH the offending built-in mutation and an unrelated
/// sibling table's ALTER is refused as a whole -- proven below by neither
/// table's declared policy changing. "The batch's unrelated sibling tables
/// still apply" is proven the way an atomic preflight makes true: the same
/// sibling mutation, resent on its own (as a real sync session would after
/// dropping the offending item), applies normally -- the door is scoped to
/// the four named built-in tables, not a blanket refusal of this clause.
#[test]
fn arriving_sync_ddl_refuses_engine_owned_mutation_sibling_table_still_applies() {
    let hub = Database::open_memory();
    install_work_ledger_schema(&hub).expect("install ledger schema on hub");
    hub.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("create sibling user table on hub");

    let offending_alter = DdlChange::AlterTable {
        name: "work_capabilities".to_string(),
        columns: vec![
            ("capability_key".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("node_id".to_string(), "TEXT NOT NULL".to_string()),
            ("capability_id".to_string(), "TEXT NOT NULL".to_string()),
            ("tags".to_string(), "JSON NOT NULL".to_string()),
            ("detail".to_string(), "JSON".to_string()),
            (
                "advertised_at".to_string(),
                "TIMESTAMP NOT NULL".to_string(),
            ),
        ],
        constraints: vec!["SYNC CONFLICT KEEP FIRST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let sibling_alter = DdlChange::AlterTable {
        name: "notes".to_string(),
        columns: vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("body".to_string(), "TEXT".to_string()),
        ],
        constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };

    let combined = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![sibling_alter.clone(), offending_alter],
        ddl_lsn: vec![Lsn(hub.current_lsn().0 + 1), Lsn(hub.current_lsn().0 + 2)],
    };
    let err = hub
        .apply_changes(
            combined,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("a changeset carrying the engine-owned mutation must be refused as a whole");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities"),
        "must name the offending table: {reason}"
    );
    assert!(
        !reason.contains("'notes'"),
        "must not name the unrelated sibling table: {reason}"
    );

    // Nothing from the refused batch was written -- not even the sibling's
    // half of it -- because DDL preflight validates the whole changeset
    // before any of it is applied.
    let capabilities_meta = hub.table_meta("work_capabilities").expect("exists");
    assert_eq!(
        capabilities_meta.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "the built-in's declared policy must be untouched by the refused batch"
    );
    let notes_meta = hub.table_meta("notes").expect("exists");
    assert_eq!(
        notes_meta.conflict_policy, None,
        "the sibling's ALTER inside the refused batch must not have landed either -- \
         DDL preflight is whole-batch, not partial"
    );

    // The sibling's IDENTICAL mutation, sent on its own, applies normally --
    // the door is scoped to the four named built-in tables.
    let sibling_only = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![sibling_alter],
        ddl_lsn: vec![Lsn(hub.current_lsn().0 + 1)],
    };
    hub.apply_changes(
        sibling_only,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .expect("the sibling table's identical mutation must still apply on its own");
    let notes_meta = hub.table_meta("notes").expect("exists");
    assert_eq!(notes_meta.conflict_policy, Some(ConflictPolicy::LatestWins));
}

// ---------------------------------------------------------------------
// Sync-apply preserve-or-refuse (Findings 1 & 2, cold review #4).
// ---------------------------------------------------------------------

/// FINDING 1 (cold review #4), the reviewer's own probe shape: an arriving
/// `AlterTable` for `work_inputs` that carries the FULL post-alter shape with
/// NO `RETAIN` clause at all -- exactly the wire shape `DROP RETAIN` produces
/// on an unguarded/older peer, or a hand-crafted changeset. The interop
/// contract holds -- apply succeeds, this is not refused -- but before the
/// fix the silent axis was taken as an implicit clear and
/// `default_ttl_seconds` landed `None`. It must now PRESERVE the engine's own
/// declared window instead.
#[test]
fn arriving_alter_silent_on_retain_preserves_work_inputs_window() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let silent_on_retain = DdlChange::AlterTable {
        name: "work_inputs".to_string(),
        columns: work_inputs_columns(),
        constraints: Vec::new(), // no RETAIN clause -- the post-DROP-RETAIN shape
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, silent_on_retain)
        .expect("an arriving AlterTable silent on RETAIN must still apply -- interop holds");

    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert_eq!(
        meta.default_ttl_seconds,
        Some(7 * 24 * 60 * 60),
        "silence on RETAIN must PRESERVE the engine's declared window, never clear it"
    );
}

/// Sibling-axis variant of the above: an arriving `AlterTable` for
/// `work_capabilities` silent on BOTH `HISTORY` and `SYNC CONFLICT` (the
/// shape a half-healed peer's own in-progress multi-step reconcile can
/// produce, or a peer that only altered an unrelated column) must PRESERVE
/// both declared axes rather than clearing them.
#[test]
fn arriving_alter_silent_on_history_and_conflict_preserves_work_capabilities_policy() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let silent_on_policy = DdlChange::AlterTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: Vec::new(), // no HISTORY / SYNC CONFLICT clause at all
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, silent_on_policy).expect(
        "an arriving AlterTable silent on HISTORY/SYNC CONFLICT must still apply -- interop holds",
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(
        meta.history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "silence on HISTORY must PRESERVE the declared policy, never clear it"
    );
    assert_eq!(
        meta.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "silence on SYNC CONFLICT must PRESERVE the declared policy, never clear it"
    );
}

/// Sibling-axis variant on `SYNC` direction: `work_node_contacts` declares
/// `SYNC OFF`. An arriving `AlterTable` silent on EVERY policy axis (no
/// `HISTORY` / `SYNC` clause at all -- the shape an unrelated column-only
/// ALTER from a peer would carry) must PRESERVE the declared `SYNC OFF`, not
/// clear it back to the undeclared default (`SyncDirection::Both`) -- the
/// same clearing defect as Finding 1, on the direction axis instead of
/// RETAIN. (Silent on `HISTORY` too, deliberately: an incoming shape
/// explicit on `HISTORY CURRENT ONLY` but silent on direction alone is not a
/// shape this table's own reconcile order ever produces -- see
/// `install_node_contacts_schema`, which sets direction BEFORE history -- and
/// separately trips the pre-existing `refuse_reclaimed_history_under_
/// keep_first` hazard check, which is unrelated to this test's contract and
/// out of this round's scope.)
#[test]
fn arriving_alter_silent_on_direction_preserves_work_node_contacts_sync_off() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_node_contacts (\
            node_id TEXT PRIMARY KEY, \
            last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("create work_node_contacts");

    let silent_on_everything = DdlChange::AlterTable {
        name: "work_node_contacts".to_string(),
        columns: vec![
            ("node_id".to_string(), "TEXT PRIMARY KEY".to_string()),
            (
                "last_contact_ms".to_string(),
                "TIMESTAMP NOT NULL".to_string(),
            ),
        ],
        constraints: Vec::new(), // silent on HISTORY and SYNC direction alike
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, silent_on_everything).expect(
        "an arriving AlterTable silent on SYNC direction must still apply -- interop holds",
    );

    let meta = db
        .table_meta("work_node_contacts")
        .expect("work_node_contacts exists");
    assert_eq!(
        meta.sync_direction,
        Some(SyncDirection::None),
        "silence on SYNC direction must PRESERVE the declared SYNC OFF, never clear it"
    );
    assert_eq!(
        meta.history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "silence on HISTORY must also PRESERVE the declared policy"
    );
}

/// FINDING 2 (cold review #4), the reviewer's first probe shape: a
/// hand-crafted arriving `CreateTable` for the ALREADY-EXISTING
/// `work_capabilities`, explicitly declaring `SYNC CONFLICT KEEP FIRST` --
/// the byte-identical mutation `arriving_sync_ddl_refuses_engine_owned_
/// mutation_sibling_table_still_applies` proves refused when spelled as an
/// `AlterTable`. Before the fix the CreateTable-adopt branch admitted this
/// wholesale; it must now refuse identically.
#[test]
fn arriving_create_table_adopt_keep_first_on_work_capabilities_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let offending_create = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: vec!["SYNC CONFLICT KEEP FIRST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let err = apply_single_ddl(&db, offending_create).expect_err(
        "an arriving CreateTable adopting work_capabilities with SYNC CONFLICT KEEP FIRST \
         must refuse exactly like the AlterTable mirror",
    );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(
        reason.contains("SYNC CONFLICT"),
        "must name the clause: {reason}"
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(
        meta.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "a refused CreateTable-adopt must apply no part of itself"
    );
}

/// FINDING 2 (cold review #4), the reviewer's second probe shape (the
/// aggravator): a hand-crafted arriving `CreateTable` for the
/// ALREADY-EXISTING `work_capabilities`, explicitly declaring `SYNC OFF`.
/// `work_capabilities` has no reconcile heal on its `sync_direction` axis
/// (only `install_node_contacts_schema` heals direction, for
/// `work_node_contacts`), so before the fix this landed SILENTLY and
/// PERMANENTLY stopped capability advertisements from syncing -- the same
/// engine-owned mutation the local ALTER door refuses, reachable through the
/// adopt back door on a different axis than Finding 2's first probe.
#[test]
fn arriving_create_table_adopt_sync_off_on_work_capabilities_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let offending_create = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: vec!["SYNC OFF".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let err = apply_single_ddl(&db, offending_create).expect_err(
        "an arriving CreateTable adopting work_capabilities with SYNC OFF must refuse -- \
         the aggravator: direction has no reconcile heal on this table",
    );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(
        meta.sync_direction, None,
        "a refused CreateTable-adopt must apply no part of itself -- work_capabilities stays \
         undeclared (default direction), not SYNC OFF"
    );
}

/// Green guard: a `SYNC SAFE` addition is refused too (the aggravator's
/// sibling axis) -- none of the four engine-owned tables declare `SYNC
/// SAFE`, so an arriving CreateTable-adopt that adds it is an explicit
/// non-canonical value, not silence.
#[test]
fn arriving_create_table_adopt_sync_safe_on_work_inputs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let offending_create = DdlChange::CreateTable {
        name: "work_inputs".to_string(),
        columns: work_inputs_columns(),
        constraints: vec!["RETAIN 7 DAYS SYNC SAFE".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let err = apply_single_ddl(&db, offending_create).expect_err(
        "an arriving CreateTable adopting work_inputs with SYNC SAFE must refuse -- \
         work_inputs declares plain RETAIN with no delivery promise",
    );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("engine-owned"),
        "{reason}"
    );

    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert!(
        !meta.sync_safe,
        "a refused CreateTable-adopt must apply no part of itself"
    );
}

/// Green guard: a `CreateTable` that restates `work_capabilities`' OWN
/// canonical shape verbatim -- a healed peer's real push -- still applies.
/// The restatement exception is not merely an ALTER-door concept; the
/// CreateTable-adopt mirror must let it through identically.
#[test]
fn arriving_create_table_adopt_verbatim_canonical_shape_still_applies() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let healed_peer_create = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: vec![
            "HISTORY CURRENT ONLY".to_string(),
            "SYNC CONFLICT KEEP LATEST".to_string(),
        ],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, healed_peer_create).expect(
        "a CreateTable-adopt restating work_capabilities' own canonical shape verbatim \
         must not refuse",
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(meta.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(meta.conflict_policy, Some(ConflictPolicy::LatestWins));
}

/// Green guard: a CreateTable-adopt against a LEGACY pre-declaration
/// `work_capabilities` (created before the four tables declared their own
/// policy, so it carries no HISTORY / SYNC CONFLICT at all) that restates the
/// canonical shape HEALS it, exactly as a local reconcile ALTER would --
/// proving the merge's "adopt the canonical value" branch is a real heal,
/// not only a no-op on an already-healed table.
#[test]
fn arriving_create_table_adopt_verbatim_canonical_shape_heals_a_legacy_root() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_capabilities (\
            capability_key TEXT PRIMARY KEY, \
            node_id TEXT NOT NULL, \
            capability_id TEXT NOT NULL, \
            tags JSON NOT NULL, \
            detail JSON, \
            advertised_at TIMESTAMP NOT NULL)",
        &HashMap::new(),
    )
    .expect("create a legacy pre-declaration work_capabilities");
    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(
        meta.history_policy, None,
        "premise: legacy root is undeclared"
    );
    assert_eq!(
        meta.conflict_policy, None,
        "premise: legacy root is undeclared"
    );

    let healed_peer_create = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: vec![
            "HISTORY CURRENT ONLY".to_string(),
            "SYNC CONFLICT KEEP LATEST".to_string(),
        ],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, healed_peer_create)
        .expect("a healed peer's canonical CreateTable-adopt must heal a legacy root");

    let healed = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(healed.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(healed.conflict_policy, Some(ConflictPolicy::LatestWins));
}

/// The innocent case named in cold review #4's Finding 2: an older binary
/// that does not know `ALTER TABLE ADD COLUMN` (or any column change) so it
/// emits `DropTable` + `CreateTable` instead of an `AlterTable`, and its
/// `CreateTable` restates the shape it has always known -- the OLD,
/// undeclared one (no HISTORY / SYNC CONFLICT clause), because that binary
/// predates the policy declaration entirely.
///
/// Stated explicitly, this is what the fixed semantics actually produce:
/// `CreateTable` is refused by neither door here, because a `DropTable`
/// immediately ahead of it in the SAME batch removes the table from the
/// preflight's projection before the `CreateTable` is judged -- so it is
/// judged as a FRESH create, not an adopt (fresh creation of one of the four
/// names stays the owning installer's domain, per this round's explicit
/// scope, not this door's -- a separately tracked, pre-existing gap). The
/// batch applies, and the recreated table lands undeclared -- exactly the
/// legacy pre-declaration shape a fresh install of that older binary would
/// have produced -- until the next `install_work_ledger_schema` call
/// reconciles it, exactly like any other legacy root.
#[test]
fn drop_then_recreate_from_older_binary_lands_undeclared_pending_next_reconcile() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let drop = DdlChange::DropTable {
        name: "work_capabilities".to_string(),
    };
    let recreate_undeclared = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: Vec::new(), // the older binary's own shape: no HISTORY / SYNC CONFLICT
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![drop, recreate_undeclared],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1), Lsn(db.current_lsn().0 + 2)],
    };
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .expect(
        "an older binary's innocent DROP+CREATE with canonical columns and silent policy must apply",
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists after recreate");
    assert_eq!(
        meta.history_policy, None,
        "the recreated table lands undeclared, exactly like a legacy pre-declaration root, \
         until the next install_work_ledger_schema call reconciles it"
    );
    assert_eq!(
        meta.conflict_policy, None,
        "the recreated table lands undeclared, exactly like a legacy pre-declaration root, \
         until the next install_work_ledger_schema call reconciles it"
    );

    // The next installer call reconciles it, exactly like any other legacy
    // root -- proving the recreated table is not stuck undeclared forever.
    install_work_ledger_schema(&db).expect("reconcile the recreated table");
    let reconciled = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists");
    assert_eq!(reconciled.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(reconciled.conflict_policy, Some(ConflictPolicy::LatestWins));
}

// ---------------------------------------------------------------------
// Door: LOCAL `ALTER TABLE ... SET SYNC ...` (a pre-existing gap --
// `SetSyncDirection` was never a guarded axis anywhere, even though
// `architecture.md` already presented `SYNC OFF` as part of
// `work_node_contacts`' engine-owned shape).
// ---------------------------------------------------------------------

/// RED shape: `SET SYNC OFF` on `work_capabilities` must refuse exactly like
/// `SET RETAIN` / `SET HISTORY` / `SET SYNC CONFLICT` already do --
/// `work_capabilities` declares no explicit direction at all (the default
/// governs), so ANY explicit `SET SYNC` on it differs from that canonical
/// silence.
#[test]
fn set_sync_off_on_work_capabilities_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_capabilities SET SYNC OFF", &p())
        .expect_err("SET SYNC OFF on work_capabilities must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(reason.contains("SYNC"), "must name the clause: {reason}");

    let meta = db.table_meta("work_capabilities").expect("exists");
    assert_eq!(
        meta.sync_direction, None,
        "a refused SET SYNC must apply no part of itself -- work_capabilities \
         stays undeclared (default direction), not SYNC OFF"
    );
}

/// Green guard: the identical `SET SYNC OFF` on a plain operator table is
/// entirely unaffected.
#[test]
fn set_sync_off_on_a_user_table_still_works() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("create notes");

    db.execute("ALTER TABLE notes SET SYNC OFF", &p())
        .expect("SET SYNC on a user table must still work");

    let meta = db.table_meta("notes").expect("notes exists");
    assert_eq!(meta.sync_direction, Some(SyncDirection::None));
}

/// Green guard: `work_node_contacts` declares `SYNC OFF` itself, so restating
/// it verbatim via a local `ALTER TABLE` -- exactly what
/// `install_node_contacts_schema`'s own reconcile issues on a legacy root --
/// must not refuse.
#[test]
fn restating_sync_off_on_work_node_contacts_is_not_refused() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_node_contacts (\
            node_id TEXT PRIMARY KEY, \
            last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &p(),
    )
    .expect("create work_node_contacts");

    db.execute("ALTER TABLE work_node_contacts SET SYNC OFF", &p())
        .expect("restating work_node_contacts' own declared SYNC OFF must not refuse");
}

// ---------------------------------------------------------------------
// Door: LOCAL `CREATE TABLE` of a reserved name (the pre-existing,
// separately-tracked gap this file's own
// `drop_then_recreate_from_older_binary_lands_undeclared_pending_next_reconcile`
// docstring named -- fresh LOCAL creation of one of the four names was
// entirely unguarded, so a consumer could hand any column shape at all to a
// reserved name). Two independent halves: the COLUMN shape
// (`refuse_engine_owned_reserved_name_shape`) and the POLICY axes
// (`refuse_engine_owned_policy_axes`, reused from the sync doors) -- both
// must pass for a local CREATE of one of the four names to succeed.
// ---------------------------------------------------------------------

/// RED shape: a consumer locally creating `work_capabilities` with an
/// unrelated column shape must refuse, naming the installer as the table's
/// owner.
#[test]
fn create_table_work_capabilities_with_wrong_columns_refuses_locally() {
    let db = Database::open_memory();

    let err = db
        .execute("CREATE TABLE work_capabilities (x TEXT)", &p())
        .expect_err("creating work_capabilities with the wrong column shape must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(
        reason.contains("install_work_ledger_schema"),
        "must name the owning installer: {reason}"
    );
    assert!(
        db.table_meta("work_capabilities").is_none(),
        "a refused CREATE TABLE must not leave a half-made table behind"
    );
}

/// Green guard: the door is scoped to the four reserved names -- any other
/// table name may declare any column shape at all, exactly as before.
#[test]
fn create_table_of_a_non_reserved_name_with_arbitrary_columns_still_works() {
    let db = Database::open_memory();

    db.execute("CREATE TABLE work_capabilities_v2 (x TEXT)", &p())
        .expect("a non-reserved name with any column shape must still create");
}

/// Green guard: the installers' own canonical `CREATE TABLE` text -- correct
/// columns AND the declared policy together -- is exactly what
/// `install_work_ledger_schema` / `install_peer_directory_schema` /
/// `install_node_contacts_schema` execute on a fresh root, and must keep
/// working under this door. Proven directly through the real installers
/// (rather than a hand-typed restatement) so this test fails first if the
/// door and an installer's own text ever drift apart.
#[test]
fn installer_fresh_create_still_works_under_the_reserved_name_door() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install_work_ledger_schema must still create fresh");
    install_peer_directory_schema(&db)
        .expect("install_peer_directory_schema must still create fresh");

    let capabilities = db.table_meta("work_capabilities").expect("exists");
    assert_eq!(
        capabilities.history_policy,
        Some(HistoryPolicy::CurrentOnly)
    );
    assert_eq!(
        capabilities.conflict_policy,
        Some(ConflictPolicy::LatestWins)
    );
    let inputs = db.table_meta("work_inputs").expect("exists");
    assert_eq!(inputs.default_ttl_seconds, Some(7 * 24 * 60 * 60));
    let peers = db.table_meta("peer_directory").expect("exists");
    assert_eq!(peers.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(peers.conflict_policy, Some(ConflictPolicy::LatestWins));
}

/// Green guard: the column-shape half tolerates silence on policy -- a local
/// CREATE restating `work_capabilities`' exact canonical columns but no
/// HISTORY / SYNC CONFLICT clause at all is exactly the shape a
/// pre-declaration legacy root has (see
/// `arriving_create_table_adopt_verbatim_canonical_shape_heals_a_legacy_root`
/// and `builtin_schema_reconcile_tests.rs`, which construct this same shape
/// through this same local path), and must keep working.
#[test]
fn create_table_work_capabilities_with_canonical_columns_silent_on_policy_still_works() {
    let db = Database::open_memory();

    db.execute(
        "CREATE TABLE work_capabilities (\
            capability_key TEXT PRIMARY KEY, \
            node_id TEXT NOT NULL, \
            capability_id TEXT NOT NULL, \
            tags JSON NOT NULL, \
            detail JSON, \
            advertised_at TIMESTAMP NOT NULL)",
        &p(),
    )
    .expect(
        "canonical columns with no policy clause at all must still create -- \
         the pre-declaration legacy shape",
    );

    let meta = db.table_meta("work_capabilities").expect("exists");
    assert_eq!(meta.history_policy, None);
    assert_eq!(meta.conflict_policy, None);
}

/// The policy half of the door: canonical columns, but an EXPLICIT
/// non-canonical `SYNC CONFLICT` clause, must refuse -- the same question
/// `refuse_engine_owned_policy_axes` already asks for the arriving-sync
/// doors, now asked here too.
#[test]
fn create_table_work_capabilities_with_canonical_columns_and_wrong_explicit_conflict_refuses() {
    let db = Database::open_memory();

    let err = db
        .execute(
            "CREATE TABLE work_capabilities (\
                capability_key TEXT PRIMARY KEY, \
                node_id TEXT NOT NULL, \
                capability_id TEXT NOT NULL, \
                tags JSON NOT NULL, \
                detail JSON, \
                advertised_at TIMESTAMP NOT NULL) SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect_err(
            "canonical columns with an explicit non-canonical SYNC CONFLICT must still refuse",
        );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "{reason}"
    );
    assert!(
        reason.contains("SYNC CONFLICT"),
        "must name the clause: {reason}"
    );
    assert!(
        db.table_meta("work_capabilities").is_none(),
        "a refused CREATE TABLE must not leave a half-made table behind"
    );
}

/// The local `SET RETAIN` arm must also refuse the `sync_safe` flag on
/// engine-owned tables, exactly as the arriving-sync door does
/// (`arriving_create_table_adopt_sync_safe_on_work_inputs_refuses`). Before
/// the fix, the `SetRetain` arm passed only the retention window to the
/// refusal gate, omitting `sync_safe`, so `ALTER TABLE work_inputs SET RETAIN
/// 7 DAYS SYNC SAFE` silently restated the canonical window (and so passed the
/// gate) but then applied `table_meta.sync_safe = true` on the engine-owned
/// table.
#[test]
fn set_retain_verbatim_window_with_sync_safe_on_work_inputs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_inputs SET RETAIN 7 DAYS SYNC SAFE", &p())
        .expect_err("SET RETAIN with SYNC SAFE on the engine-owned work_inputs table must refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("engine-owned"),
        "must name the table as engine-owned infrastructure: {reason}"
    );
    assert!(
        reason.contains("SYNC SAFE"),
        "must name the SYNC SAFE clause: {reason}"
    );

    // Refused before the write lock is taken -- no part of the ALTER applies,
    // not the window, not the SYNC SAFE flag.
    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert_eq!(
        meta.default_ttl_seconds,
        Some(7 * 24 * 60 * 60),
        "the declared window survives a refused ALTER"
    );
    assert!(
        !meta.sync_safe,
        "a refused ALTER must apply no part of itself -- sync_safe stays false"
    );
}

/// Green guard: `ALTER TABLE work_inputs SET RETAIN 7 DAYS` (no SYNC SAFE)
/// must still succeed -- the installer's own heal restatement path must keep
/// working.
#[test]
fn set_retain_verbatim_window_without_sync_safe_on_work_inputs_succeeds() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    db.execute("ALTER TABLE work_inputs SET RETAIN 7 DAYS", &p())
        .expect("SET RETAIN without SYNC SAFE on work_inputs must still succeed");

    let meta = db.table_meta("work_inputs").expect("work_inputs exists");
    assert_eq!(meta.default_ttl_seconds, Some(7 * 24 * 60 * 60));
    assert!(!meta.sync_safe);
}

/// Green guard: `SET RETAIN ... SYNC SAFE` on an operator-created table must
/// still succeed. A user table with the needed prerequisites (PRIMARY KEY, a
/// covering index, and a sync direction) can use SYNC SAFE.
#[test]
fn set_retain_with_sync_safe_on_user_table_succeeds() {
    let db = Database::open_memory();
    // Create a user table with the prerequisites for SYNC SAFE: a PRIMARY KEY,
    // a covering index, and a sync direction. Mirror the pattern from
    // per_edge_receipt_confirmation_tests.rs.
    db.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 1 HOURS SYNC SAFE",
        &p(),
    )
    .expect("create windows table with SYNC SAFE");

    db.execute("ALTER TABLE windows SET RETAIN 2 HOURS SYNC SAFE", &p())
        .expect("SET RETAIN with SYNC SAFE on a user table must still work");

    let meta = db.table_meta("windows").expect("windows exists");
    assert_eq!(meta.default_ttl_seconds, Some(2 * 60 * 60));
    assert!(meta.sync_safe);
}

// ---------------------------------------------------------------------
// Door: SYNC PREFLIGHT on fresh `CREATE TABLE` of a reserved name
// (the gap where DROP + CREATE could circumvent the guards on a
// CreateTable for a table not yet in `projected`, allowing a peer to
// hand any column shape or policy to one of the four reserved names).
// ---------------------------------------------------------------------

/// RED shape: an arriving sync batch with DROP work_inputs + CREATE
/// work_inputs with wrong columns must refuse atomically, with the table
/// still present and unmodified.
#[test]
fn drop_then_recreate_work_inputs_with_wrong_columns_over_sync_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let drop = DdlChange::DropTable {
        name: "work_inputs".to_string(),
    };
    let recreate_wrong_columns = DdlChange::CreateTable {
        name: "work_inputs".to_string(),
        columns: vec![("x".to_string(), "TEXT PRIMARY KEY".to_string())],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![drop, recreate_wrong_columns],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1), Lsn(db.current_lsn().0 + 2)],
    };

    let err = db
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("DROP + CREATE work_inputs with wrong columns must refuse");

    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("engine-owned"),
        "refusal must name work_inputs as engine-owned: {reason}"
    );

    let meta = db
        .table_meta("work_inputs")
        .expect("work_inputs must still exist after refused batch");
    // Verify the table was not modified by checking it still has the
    // canonical columns
    assert_eq!(
        meta.columns.len(),
        4,
        "work_inputs must retain its original column count after refused batch"
    );
}

/// RED shape: an arriving sync batch with DROP work_capabilities +
/// CREATE work_capabilities with canonical columns but an explicit
/// non-canonical SYNC CONFLICT clause must refuse atomically.
#[test]
fn drop_then_recreate_work_capabilities_with_wrong_explicit_policy_over_sync_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let drop = DdlChange::DropTable {
        name: "work_capabilities".to_string(),
    };
    // Declare the canonical HISTORY but the wrong SYNC CONFLICT: the canonical
    // is KEEP LATEST, but we declare KEEP FIRST
    let recreate_wrong_policy = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: vec![
            "HISTORY CURRENT ONLY".to_string(),
            "SYNC CONFLICT KEEP FIRST".to_string(),
        ],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![drop, recreate_wrong_policy],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1), Lsn(db.current_lsn().0 + 2)],
    };

    let err = db
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("DROP + CREATE work_capabilities with wrong SYNC CONFLICT must refuse");

    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_capabilities") && reason.contains("engine-owned"),
        "refusal must name work_capabilities as engine-owned: {reason}"
    );
    assert!(
        reason.contains("SYNC CONFLICT"),
        "refusal must name the conflicting policy axis: {reason}"
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities must still exist");
    assert_eq!(
        meta.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "work_capabilities must retain its original SYNC CONFLICT after refused batch"
    );
}

/// Green guard: an arriving sync batch with DROP work_capabilities +
/// CREATE work_capabilities with canonical columns and silent policy
/// must still succeed, landing undeclared pending the next reconcile.
/// This is the innocent older-binary path that must keep working.
#[test]
fn drop_then_recreate_work_capabilities_with_canonical_columns_silent_policy_still_succeeds_over_sync()
 {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let drop = DdlChange::DropTable {
        name: "work_capabilities".to_string(),
    };
    let recreate_undeclared = DdlChange::CreateTable {
        name: "work_capabilities".to_string(),
        columns: work_capabilities_columns(),
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![drop, recreate_undeclared],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1), Lsn(db.current_lsn().0 + 2)],
    };

    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .expect(
        "DROP + CREATE work_capabilities with canonical columns and silent policy must succeed",
    );

    let meta = db
        .table_meta("work_capabilities")
        .expect("work_capabilities exists after recreate");
    assert_eq!(
        meta.history_policy, None,
        "recreated table lands undeclared, exactly like a legacy pre-declaration root"
    );
    assert_eq!(
        meta.conflict_policy, None,
        "recreated table lands undeclared, exactly like a legacy pre-declaration root"
    );
}
