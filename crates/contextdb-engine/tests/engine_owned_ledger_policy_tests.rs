//! The four ORIGINAL built-in ledger tables (`work_inputs`, `work_capabilities`,
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
//!   door; a consumer typing one of these four reserved names directly is
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
//! FIVE MORE reserved names join the shape door above: the hub-refereed
//! work-ledger tables `work_jobs` / `work_claims` / `work_results` /
//! `work_failures` / `work_cancellations` (NINE reserved names total --
//! `engine_owned_reserved_table_columns` covers all nine -- so
//! `refuse_engine_owned_reserved_name_shape` refuses a structurally
//! mismatched CREATE on any of the nine, not just the original four). These
//! five carry a NARROWER policy contract than the four above, though: only
//! their `SYNC CONFLICT` axis is engine-governed
//! (`refuse_hub_refereed_ledger_sync_conflict_mismatch` at CREATE and over
//! sync, its ALTER-time counterpart at the local `ALTER` door) --
//! mismatch-refusing and honest-verbatim-restate-tolerant, never a
//! categorical "no ALTER at all" the way the four above are. `RETAIN` /
//! `HISTORY` / `SYNC` direction on these five are UNGUARDED today (a filed,
//! not-yet-executed gap, C1-1), because none of the five declares a
//! canonical value on those axes the way `engine_owned_ledger_policy` does
//! for the original four.
//!
//! A plain operator table is entirely unaffected: the identical clause on a
//! non-built-in table applies exactly as it always has, and creating a table
//! under any name outside the NINE reserved names above is unrestricted.
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

use contextdb_core::{
    ConflictPolicy as DeclaredConflictPolicy, Error, HistoryPolicy, Lsn, SingleColumnForeignKey,
    SyncDirection, Value,
};
use contextdb_engine::Database;
use contextdb_engine::peer_directory::{
    install_peer_directory_schema, lookup_peer_ticket, register_peer_ticket,
};
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
    assert_eq!(
        meta.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
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
    assert_eq!(
        meta.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
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
        Some(DeclaredConflictPolicy::KEEP_LATEST),
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
    assert_eq!(
        notes_meta.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
}

// ---------------------------------------------------------------------
// Sync-apply preserve-or-refuse for engine-owned table policy.
// ---------------------------------------------------------------------

/// An arriving
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
        Some(DeclaredConflictPolicy::KEEP_LATEST),
        "silence on SYNC CONFLICT must PRESERVE the declared policy, never clear it"
    );
}

/// Sibling-axis variant on `SYNC` direction: `work_node_contacts` declares
/// `SYNC OFF`. An arriving `AlterTable` silent on EVERY policy axis (no
/// `HISTORY` / `SYNC` clause at all -- the shape an unrelated column-only
/// ALTER from a peer would carry) must PRESERVE the declared `SYNC OFF`, not
/// clear it back to the undeclared default (`SyncDirection::Both`) -- the
/// same clearing defect on the direction axis instead of
/// RETAIN. (Silent on `HISTORY` too, deliberately: an incoming shape
/// explicit on `HISTORY CURRENT ONLY` but silent on direction alone is not a
/// shape this table's own reconcile order ever produces -- see
/// `install_node_contacts_schema`, which sets direction BEFORE history -- and
/// separately trips the pre-existing `refuse_reclaimed_history_under_
/// keep_first` hazard check, which is unrelated to this test's contract and
/// outside this test's contract.)
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

/// A hand-crafted arriving
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
        Some(DeclaredConflictPolicy::KEEP_LATEST),
        "a refused CreateTable-adopt must apply no part of itself"
    );
}

/// A second hand-crafted arriving `CreateTable` probe covers the
/// ALREADY-EXISTING `work_capabilities`, explicitly declaring `SYNC OFF`.
/// `work_capabilities` has no reconcile heal on its `sync_direction` axis
/// (only `install_node_contacts_schema` heals direction, for
/// `work_node_contacts`), so before the fix this landed SILENTLY and
/// PERMANENTLY stopped capability advertisements from syncing -- the same
/// engine-owned mutation the local ALTER door refuses, reachable through the
/// adopt back door on a different policy axis.
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
        meta.sync_direction,
        Some(SyncDirection::Both),
        "a refused CreateTable-adopt must apply no part of itself -- work_capabilities stays \
         explicitly SYNC TWO WAY, not SYNC OFF"
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
         work_inputs declares SYNC TWO WAY but no SYNC SAFE",
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
            "SYNC TWO WAY".to_string(),
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
    assert_eq!(
        meta.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
    assert_eq!(meta.sync_direction, Some(SyncDirection::Both));
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
            "SYNC TWO WAY".to_string(),
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
    assert_eq!(
        healed.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
    assert_eq!(healed.sync_direction, Some(SyncDirection::Both));
}

/// An older binary
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
/// names stays the owning installer's domain, not this door's. The
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
    assert_eq!(
        reconciled.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
}

// ---------------------------------------------------------------------
// Door: LOCAL `ALTER TABLE ... SET SYNC ...` (a pre-existing gap --
// `SetSyncDirection` was never a guarded axis anywhere, even though
// `architecture.md` already presented `SYNC OFF` as part of
// `work_node_contacts`' engine-owned shape).
// ---------------------------------------------------------------------

/// RED shape: `SET SYNC OFF` on `work_capabilities` must refuse exactly like
/// `SET RETAIN` / `SET HISTORY` / `SET SYNC CONFLICT` already do --
/// `work_capabilities` declares `SYNC TWO WAY`, so any other explicit `SET
/// SYNC` direction differs from that canonical declaration.
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
        meta.sync_direction,
        Some(SyncDirection::Both),
        "a refused SET SYNC must apply no part of itself -- work_capabilities \
         stays explicitly SYNC TWO WAY, not SYNC OFF"
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

/// Blocker (fix round 2, item 3 -- reserved names): the reserved-name door
/// (`refuse_engine_owned_reserved_name_shape`) only recognizes
/// [`ENGINE_OWNED_LEDGER_TABLES`]'s four names (`work_inputs`,
/// `work_capabilities`, `peer_directory`, `work_node_contacts`) --
/// `engine_owned_reserved_table_columns` returns `None` for any other name,
/// so the door is a silent no-op for it. But `work_jobs` / `work_claims` /
/// `work_results` / `work_failures` / `work_cancellations` are equally
/// engine-owned work-ledger infrastructure (all seven are governed by
/// `work_ledger::WORK_LEDGER_TABLES` / `work_ledger_conflict_policy_entries_inner`)
/// -- a consumer typing `CREATE TABLE work_claims (x TEXT)` today succeeds,
/// silently shadowing the name `install_work_ledger_schema` needs later and
/// leaving that later install call to collide with (or skip past, since
/// installation is idempotent-by-name-presence) a table with an unrelated
/// shape. The same refusal pinned above for `work_capabilities` must hold,
/// uniformly, for the other six reserved work-ledger names too.
#[test]
fn create_table_of_each_other_work_ledger_reserved_name_with_wrong_columns_refuses_locally() {
    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
    ] {
        let db = Database::open_memory();
        let result = db.execute(&format!("CREATE TABLE {table} (x TEXT)"), &p());
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "CREATE TABLE {table} (x TEXT) must refuse -- {table} is reserved \
                 work-ledger infrastructure, not an operator table name, exactly like \
                 work_capabilities above; it was silently accepted with the wrong shape"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must name {table} as engine-owned infrastructure: {reason}"
        );
        assert!(
            db.table_meta(table).is_none(),
            "a refused CREATE TABLE must not leave a half-made {table} table behind"
        );
    }
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
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
    assert_eq!(capabilities.sync_direction, Some(SyncDirection::Both));
    let inputs = db.table_meta("work_inputs").expect("exists");
    assert_eq!(inputs.default_ttl_seconds, Some(7 * 24 * 60 * 60));
    assert_eq!(
        inputs.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_FIRST)
    );
    assert_eq!(inputs.sync_direction, Some(SyncDirection::Both));
    let peers = db.table_meta("peer_directory").expect("exists");
    assert_eq!(peers.history_policy, Some(HistoryPolicy::CurrentOnly));
    assert_eq!(
        peers.conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST)
    );
    assert_eq!(peers.sync_direction, Some(SyncDirection::Both));
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
        Some(DeclaredConflictPolicy::KEEP_LATEST),
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

// ---------------------------------------------------------------------
// Fix round 3, item 1 (blocker -- POLICY-CLAUSE DOOR): the round-2 fix
// extended the RESERVED-NAME / column-shape door
// (`refuse_engine_owned_reserved_name_shape`) to all nine names, but the
// POLICY-CLAUSE door (`refuse_engine_owned_policy_axes`, gated on the
// still-four-member `ENGINE_OWNED_LEDGER_TABLES`) was not. So today
// `CREATE TABLE work_jobs (<canonical 12 columns>) SYNC CONFLICT KEEP
// LATEST` is ACCEPTED: canonical columns pass the shape door, and no door
// at all judges the explicit `SYNC CONFLICT` clause for `work_jobs` /
// `work_claims` / `work_results` / `work_failures` / `work_cancellations`
// (`refuse_hub_refereed_ledger_sync_conflict_declaration` -- the door that
// DOES cover these five -- is wired only into the local ALTER arm and the
// three wire-DDL preflights named in the round-3 brief; the local CREATE
// arm calls only `refuse_engine_owned_policy_axes`). `db.table_meta`
// then carries the lying `KEEP_LATEST`, while `SHOW SYNC_CONFLICT_POLICY`
// (round-2 fix) still renders the true `keep_first (engine-owned)` --
// `.schema`/SHOW disagreement, the same shape round 1's `work_jobs` bug
// took.
// ---------------------------------------------------------------------

/// All five hub-refereed table names arbitrate `keep_first` (three via the
/// engine-private `ServerWins` override with no declared clause at all,
/// two -- `work_jobs` / `work_failures` -- via their own declared `SYNC
/// CONFLICT KEEP FIRST`), so `KEEP LATEST` mismatches every one of them.
const HUB_REFEREED_TABLES: [&str; 5] = [
    "work_jobs",
    "work_claims",
    "work_results",
    "work_failures",
    "work_cancellations",
];

/// The canonical `CREATE TABLE ...` prefix (columns only, no trailing
/// policy clause) for one of the five hub-refereed tables, mirrored by hand
/// from `work_ledger.rs`'s `CREATE_WORK_JOBS` / `CREATE_WORK_CLAIMS` /
/// `CREATE_WORK_RESULTS` / `CREATE_WORK_FAILURES` / `CREATE_WORK_CANCELLATIONS`
/// constants, with their own trailing `SYNC TWO WAY [SYNC CONFLICT KEEP
/// FIRST]` stripped so a test can append whatever clause it is probing.
fn hub_refereed_canonical_create_prefix(table: &str) -> &'static str {
    match table {
        "work_jobs" => {
            "CREATE TABLE work_jobs (\
             job_id TEXT PRIMARY KEY, \
             work_class TEXT NOT NULL, \
             mode TEXT NOT NULL, \
             requirement_tags JSON NOT NULL, \
             input_refs JSON NOT NULL, \
             output_schema TEXT, \
             priority INTEGER NOT NULL, \
             deadline TIMESTAMP, \
             max_attempts INTEGER NOT NULL, \
             submitter_node_id TEXT NOT NULL, \
             provenance JSON, \
             submitted_at TIMESTAMP NOT NULL)"
        }
        "work_claims" => {
            "CREATE TABLE work_claims (\
             claim_key TEXT PRIMARY KEY, \
             job_id TEXT NOT NULL, \
             attempt INTEGER NOT NULL, \
             node_id TEXT NOT NULL, \
             lease_deadline TIMESTAMP NOT NULL, \
             claimed_at TIMESTAMP NOT NULL)"
        }
        "work_results" => {
            "CREATE TABLE work_results (\
             job_id TEXT PRIMARY KEY, \
             attempt INTEGER NOT NULL, \
             executor_node_id TEXT NOT NULL, \
             output TEXT NOT NULL, \
             receipt JSON NOT NULL, \
             completed_at TIMESTAMP NOT NULL)"
        }
        "work_failures" => {
            "CREATE TABLE work_failures (\
             failure_key TEXT PRIMARY KEY, \
             job_id TEXT NOT NULL, \
             attempt INTEGER NOT NULL, \
             node_id TEXT NOT NULL, \
             error TEXT NOT NULL, \
             failed_at TIMESTAMP NOT NULL)"
        }
        "work_cancellations" => {
            "CREATE TABLE work_cancellations (\
             job_id TEXT PRIMARY KEY, \
             requested_by TEXT NOT NULL, \
             reason TEXT, \
             cancelled_at TIMESTAMP NOT NULL)"
        }
        other => panic!("no canonical prefix known for {other}"),
    }
}

/// The wire-format `(name, type)` column list for one of the five
/// hub-refereed tables, mirroring [`hub_refereed_canonical_create_prefix`]
/// for the `DdlChange` wire-path tests below (same source of truth as
/// `engine_owned_reserved_table_columns` in `executor.rs`).
fn hub_refereed_wire_columns(table: &str) -> Vec<(String, String)> {
    let cols: &[(&str, &str)] = match table {
        "work_jobs" => &[
            ("job_id", "TEXT PRIMARY KEY"),
            ("work_class", "TEXT NOT NULL"),
            ("mode", "TEXT NOT NULL"),
            ("requirement_tags", "JSON NOT NULL"),
            ("input_refs", "JSON NOT NULL"),
            ("output_schema", "TEXT"),
            ("priority", "INTEGER NOT NULL"),
            ("deadline", "TIMESTAMP"),
            ("max_attempts", "INTEGER NOT NULL"),
            ("submitter_node_id", "TEXT NOT NULL"),
            ("provenance", "JSON"),
            ("submitted_at", "TIMESTAMP NOT NULL"),
        ],
        "work_claims" => &[
            ("claim_key", "TEXT PRIMARY KEY"),
            ("job_id", "TEXT NOT NULL"),
            ("attempt", "INTEGER NOT NULL"),
            ("node_id", "TEXT NOT NULL"),
            ("lease_deadline", "TIMESTAMP NOT NULL"),
            ("claimed_at", "TIMESTAMP NOT NULL"),
        ],
        "work_results" => &[
            ("job_id", "TEXT PRIMARY KEY"),
            ("attempt", "INTEGER NOT NULL"),
            ("executor_node_id", "TEXT NOT NULL"),
            ("output", "TEXT NOT NULL"),
            ("receipt", "JSON NOT NULL"),
            ("completed_at", "TIMESTAMP NOT NULL"),
        ],
        "work_failures" => &[
            ("failure_key", "TEXT PRIMARY KEY"),
            ("job_id", "TEXT NOT NULL"),
            ("attempt", "INTEGER NOT NULL"),
            ("node_id", "TEXT NOT NULL"),
            ("error", "TEXT NOT NULL"),
            ("failed_at", "TIMESTAMP NOT NULL"),
        ],
        "work_cancellations" => &[
            ("job_id", "TEXT PRIMARY KEY"),
            ("requested_by", "TEXT NOT NULL"),
            ("reason", "TEXT"),
            ("cancelled_at", "TIMESTAMP NOT NULL"),
        ],
        other => panic!("no wire columns known for {other}"),
    };
    cols.iter()
        .map(|(name, ty)| (name.to_string(), ty.to_string()))
        .collect()
}

/// RED: local `CREATE TABLE {table} (<canonical columns>) SYNC CONFLICT
/// KEEP LATEST` must refuse for every hub-refereed name, exactly like the
/// already-shipped `work_capabilities` pin
/// (`create_table_work_capabilities_with_canonical_columns_and_wrong_explicit_conflict_refuses`)
/// above -- naming the table as engine-owned, naming the `SYNC CONFLICT`
/// clause, and leaving no half-made table behind.
#[test]
fn local_create_of_each_hub_refereed_table_with_wrong_explicit_conflict_refuses() {
    for table in HUB_REFEREED_TABLES {
        let db = Database::open_memory();
        let ddl = format!(
            "{} SYNC CONFLICT KEEP LATEST",
            hub_refereed_canonical_create_prefix(table)
        );
        let result = db.execute(&ddl, &p());
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "CREATE TABLE {table} (<canonical columns>) SYNC CONFLICT KEEP LATEST must \
                 refuse -- {table}'s real arbitration is keep_first, not the declared \
                 keep_latest; it was silently accepted with the lying clause"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must name {table} as engine-owned infrastructure: {reason}"
        );
        assert!(
            reason.contains("SYNC CONFLICT"),
            "must name the clause: {reason}"
        );
        assert!(
            db.table_meta(table).is_none(),
            "a refused CREATE TABLE must not leave a half-made {table} table behind"
        );
    }
}

/// GREEN contrast pin: canonical columns with NO policy clause at all must
/// still create -- the pre-declaration legacy shape, mirroring
/// `create_table_work_capabilities_with_canonical_columns_silent_on_policy_still_works`
/// for all five hub-refereed names.
#[test]
fn local_create_of_each_hub_refereed_table_with_canonical_columns_and_no_clause_still_works() {
    for table in HUB_REFEREED_TABLES {
        let db = Database::open_memory();
        let ddl = hub_refereed_canonical_create_prefix(table).to_string();
        db.execute(&ddl, &p()).unwrap_or_else(|err| {
            panic!(
                "canonical columns with no policy clause at all must still create for \
                 {table}: {err:?}"
            )
        });
    }
}

/// GREEN contrast pin: canonical columns with the HONEST clause (the
/// table's own true arbitration, `keep_first`) must still create --
/// mirroring `work_inputs`' declared-clause behavior. This must stay legal
/// once the policy-clause door closes the mismatching case above.
#[test]
fn local_create_of_each_hub_refereed_table_with_the_honest_clause_still_works() {
    for table in HUB_REFEREED_TABLES {
        let db = Database::open_memory();
        let ddl = format!(
            "{} SYNC CONFLICT KEEP FIRST",
            hub_refereed_canonical_create_prefix(table)
        );
        db.execute(&ddl, &p()).unwrap_or_else(|err| {
            panic!(
                "canonical columns with the honest clause must still create for {table}: {err:?}"
            )
        });
    }
}

/// GREEN contrast pin (already-shipped four-name behavior must be
/// unaffected by closing the five-name gap): `work_inputs` with canonical
/// columns and an explicit mismatching `SYNC CONFLICT` clause already
/// refuses today via `refuse_engine_owned_policy_axes` /
/// `ENGINE_OWNED_LEDGER_TABLES`, exactly like the existing
/// `work_capabilities` pin above.
#[test]
fn local_create_of_work_inputs_with_wrong_explicit_conflict_still_refuses() {
    let db = Database::open_memory();
    let err = db
        .execute(
            "CREATE TABLE work_inputs (\
                input_key TEXT PRIMARY KEY, \
                job_id TEXT NOT NULL, \
                seq INTEGER NOT NULL, \
                payload TEXT NOT NULL) SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect_err(
            "canonical columns with an explicit non-canonical SYNC CONFLICT must still refuse",
        );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_inputs") && reason.contains("engine-owned"),
        "{reason}"
    );
    assert!(
        reason.contains("SYNC CONFLICT"),
        "must name the clause: {reason}"
    );
    assert!(
        db.table_meta("work_inputs").is_none(),
        "a refused CREATE TABLE must not leave a half-made table behind"
    );
}

/// RED, wire path: an arriving `CreateTable` DDL adopting an
/// ALREADY-INSTALLED hub-refereed table with an explicit mismatching `SYNC
/// CONFLICT` clause must refuse, exactly like the local CREATE probe above
/// and like the existing `work_capabilities` wire pin
/// (`arriving_create_table_adopt_keep_first_on_work_capabilities_refuses`).
/// The three wire-DDL preflights that should gate this
/// (`database.rs:39933/39950/39971`) currently call only
/// `refuse_engine_owned_policy_axes`, a no-op for these five names.
#[test]
fn arriving_create_table_adopt_wrong_explicit_conflict_on_a_hub_refereed_table_refuses() {
    for table in HUB_REFEREED_TABLES {
        let db = Database::open_memory();
        install_work_ledger_schema(&db).expect("install ledger schema");
        let before = db
            .table_meta(table)
            .unwrap_or_else(|| panic!("{table} exists after install"))
            .conflict_policy;

        let offending_create = DdlChange::CreateTable {
            name: table.to_string(),
            columns: hub_refereed_wire_columns(table),
            constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        };
        let result = apply_single_ddl(&db, offending_create);
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "an arriving CreateTable adopting {table} with SYNC CONFLICT KEEP LATEST must \
                 refuse exactly like the local CREATE and the AlterTable mirror"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must name {table} as engine-owned infrastructure: {reason}"
        );

        let after = db
            .table_meta(table)
            .unwrap_or_else(|| panic!("{table} still exists"))
            .conflict_policy;
        assert_eq!(
            before, after,
            "a refused CreateTable-adopt must apply no part of itself to {table}"
        );
    }
}

/// RED, wire path (the AlterTable mirror of the test above): an arriving
/// `AlterTable` DDL restating an installed hub-refereed table's full
/// current shape but with an explicit mismatching `SYNC CONFLICT` clause
/// must refuse identically.
#[test]
fn arriving_alter_table_wrong_explicit_conflict_on_a_hub_refereed_table_refuses() {
    for table in HUB_REFEREED_TABLES {
        let db = Database::open_memory();
        install_work_ledger_schema(&db).expect("install ledger schema");
        let before = db
            .table_meta(table)
            .unwrap_or_else(|| panic!("{table} exists after install"))
            .conflict_policy;

        let offending_alter = DdlChange::AlterTable {
            name: table.to_string(),
            columns: hub_refereed_wire_columns(table),
            constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        };
        let result = apply_single_ddl(&db, offending_alter);
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "an arriving AlterTable restating {table} with SYNC CONFLICT KEEP LATEST must \
                 refuse exactly like the local ALTER door"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must name {table} as engine-owned infrastructure: {reason}"
        );

        let after = db
            .table_meta(table)
            .unwrap_or_else(|| panic!("{table} still exists"))
            .conflict_policy;
        assert_eq!(
            before, after,
            "a refused AlterTable must apply no part of itself to {table}"
        );
    }
}

/// The "no lie survives" half: after EVERY refused local-CREATE, wire-
/// CreateTable-adopt, and wire-AlterTable attempt above, `SHOW
/// SYNC_CONFLICT_POLICY` must still report each hub-refereed table's true
/// arbitration -- never the mismatching value a refused attempt asked for.
/// This is the `.schema`/SHOW-never-disagree half of item 1.
#[test]
fn schema_and_show_never_disagree_after_a_refused_alter_or_wire_attempt_on_a_hub_refereed_table() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    for table in HUB_REFEREED_TABLES {
        // Local ALTER (already refused since round 2 -- re-attempted here
        // only to build the post-attempt SHOW state this test checks).
        let _ = db.execute(
            &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
            &p(),
        );
        // Wire CreateTable-adopt.
        let _ = apply_single_ddl(
            &db,
            DdlChange::CreateTable {
                name: table.to_string(),
                columns: hub_refereed_wire_columns(table),
                constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        );
        // Wire AlterTable.
        let _ = apply_single_ddl(
            &db,
            DdlChange::AlterTable {
                name: table.to_string(),
                columns: hub_refereed_wire_columns(table),
                constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        );
    }

    let result = db.execute("SHOW SYNC_CONFLICT_POLICY", &p()).expect("show");
    let rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(t) => t.clone(),
            other => panic!("policy column must be text, got {other:?}"),
        })
        .collect();
    for table in HUB_REFEREED_TABLES {
        let expected = format!("{table}=keep_first (engine-owned)");
        assert!(
            rows.iter().any(|row| row == &expected),
            "after every refused attempt above, SHOW SYNC_CONFLICT_POLICY must still report \
             {table}'s true arbitration {expected:?}; got {rows:?}"
        );
        let lie = format!("{table}=keep_latest");
        assert!(
            !rows
                .iter()
                .any(|row| row == &lie || row == &format!("{lie} (engine-owned)")),
            "SHOW SYNC_CONFLICT_POLICY must never render the value a refused attempt asked \
             for; found the lie for {table} in {rows:?}"
        );
    }
}

// ---------------------------------------------------------------------
// Fix round 3, item 2 (blocker -- REFUSAL TEXT SCOPE): the SYNC-CONFLICT
// refusal for a hub-refereed table (`engine_owned_policy_refusal`, both its
// `work_claims`/`work_results`/`work_cancellations` branch and its generic
// fallback used by `work_jobs`/`work_failures`) says "{table} is
// (re)declared only by the installer that owns it, never by an ALTER an
// operator types" -- a TABLE-WIDE claim. But for these five tables, SYNC
// CONFLICT is the ONLY axis any door actually judges: no door named in this
// suite gates `ALTER TABLE work_jobs SET RETAIN ...` or `... SET SYNC OFF`
// at all (that wider gap is filed separately, NOT this lane's scope -- see
// the round-3 brief). The refusal text must speak only for the axis it
// actually enforces.
// ---------------------------------------------------------------------

/// RED: the hub-refereed SYNC CONFLICT refusal must name the SYNC CONFLICT
/// axis specifically and must NOT claim the table is categorically
/// unelterable ("never by an ALTER an operator types") -- a claim this
/// suite's own `local_create_of_...`/wire-path tests above show is false
/// for RETAIN/HISTORY/SYNC on these five tables today.
#[test]
fn hub_refereed_sync_conflict_refusal_names_only_the_conflict_axis() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    for table in HUB_REFEREED_TABLES {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &p(),
            )
            .expect_err(&format!("{table}'s declaration attempt must still refuse"));
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must still name {table} as engine-owned infrastructure: {reason}"
        );
        assert!(
            reason.contains("SYNC CONFLICT"),
            "the refusal must name the specific axis it is refusing: {reason}"
        );
        assert!(
            !reason.contains("never by an ALTER an operator types"),
            "the refusal must not claim {table} is categorically unalterable -- only its \
             SYNC CONFLICT axis is engine-governed today, RETAIN/HISTORY/SYNC on {table} are \
             unguarded (filed separately): {reason}"
        );
    }
}

// ---------------------------------------------------------------------
// Fix round 4 (blocker, small polish): the run's ruled decision makes a
// consumer-typed `CREATE TABLE work_jobs (<canonical columns>) SYNC
// CONFLICT KEEP FIRST` (the HONEST clause -- true arbitration verbatim)
// LEGAL, proven by the GREEN pin
// `local_create_of_each_hub_refereed_table_with_the_honest_clause_still_works`
// above. But the SYNC-CONFLICT refusal message
// (`engine_owned_policy_refusal`'s hub-refereed branch, round 3's fix)
// still ends "{table}'s SYNC CONFLICT is (re)declared only by the
// installer that owns it" -- installer-EXCLUSIVITY, which is now false: a
// consumer CAN legally (re)declare it, verbatim, in their own CREATE
// TABLE. Only a MISMATCHING declaration is refused; the axis is not
// installer-exclusive, only mismatch-refusing.
// ---------------------------------------------------------------------

/// RED: the hub-refereed SYNC CONFLICT refusal must not claim
/// installer-exclusive declaration rights over the axis, since the honest
/// (canonical-matching) clause is legal for a consumer to type directly --
/// see `local_create_of_each_hub_refereed_table_with_the_honest_clause_still_works`
/// above for the paired proof that the claim is false.
#[test]
fn hub_refereed_sync_conflict_refusal_does_not_claim_installer_exclusivity() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    for table in HUB_REFEREED_TABLES {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
                &p(),
            )
            .expect_err(&format!("{table}'s declaration attempt must still refuse"));
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must still name {table} as engine-owned infrastructure: {reason}"
        );
        assert!(
            !reason.contains("(re)declared only by the installer"),
            "the refusal must not claim {table}'s SYNC CONFLICT is (re)declared only by the \
             installer -- a consumer CAN legally declare it verbatim (the honest clause), so \
             only a MISMATCHING declaration is refused, not installer-exclusivity: {reason}"
        );
    }
}

// ---------------------------------------------------------------------
// Fix round 5, item 1 (blocker -- HONEST RESTATE AT ALTER): the refusal
// message (round 4's fix, `engine_owned_policy_refusal`'s hub-refereed
// branch) now says "A SYNC CONFLICT clause that restates this arbitration
// verbatim, or no SYNC CONFLICT clause at all, is accepted here; a value
// that mismatches it is refused" -- and the CREATE door
// (`refuse_hub_refereed_ledger_sync_conflict_mismatch`) genuinely lives up
// to that: `Ok` for a verbatim restate, `Err` only for a mismatch. But the
// local ALTER arm (`AlterAction::SetSyncConflict`, `executor.rs`) still
// calls `refuse_hub_refereed_ledger_sync_conflict_declaration`, which is a
// pure table-membership check with no comparison to the incoming value at
// all -- it refuses EVERY declaration attempt on these five tables,
// honest or not. So `ALTER TABLE work_jobs SET SYNC CONFLICT KEEP FIRST`
// (work_jobs' own true arbitration, restated verbatim) is refused today,
// while the message it's refused WITH claims it should be accepted. The
// four older `ENGINE_OWNED_LEDGER_TABLES` names already tolerate this at
// ALTER (`restating_the_declared_value_verbatim_is_not_refused` above) --
// the five hub-refereed names must match that precedent.
// ---------------------------------------------------------------------

/// RED: `ALTER TABLE {table} SET SYNC CONFLICT KEEP FIRST` -- the honest,
/// canonical-matching restate -- must succeed on every hub-refereed table,
/// mirroring the CREATE door's already-correct tolerance
/// (`local_create_of_each_hub_refereed_table_with_the_honest_clause_still_works`)
/// and the four older engine-owned tables' own ALTER-time precedent
/// (`restating_the_declared_value_verbatim_is_not_refused`).
#[test]
fn alter_honest_verbatim_restate_on_a_hub_refereed_table_succeeds() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    for table in HUB_REFEREED_TABLES {
        db.execute(
            &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP FIRST"),
            &p(),
        )
        .unwrap_or_else(|err| {
            panic!(
                "ALTER TABLE {table} SET SYNC CONFLICT KEEP FIRST restates {table}'s own \
                 true arbitration (keep_first) verbatim -- the CREATE door and the four \
                 older engine-owned tables' ALTER door both tolerate this restate; the \
                 hub-refereed ALTER door must too, exactly as its own refusal message \
                 (for a genuine mismatch) already claims it does: {err:?}"
            )
        });
    }
}

// ---------------------------------------------------------------------
// Fix round 5, item 2 (blocker -- SHAPE-REFUSAL MESSAGE): round 4 fixed
// the installer-exclusivity overclaim on the SYNC-CONFLICT branch of
// `engine_owned_policy_refusal` only. The SEPARATE shape-refusal message
// (`refuse_engine_owned_reserved_name_shape` and its wire mirror
// `refuse_engine_owned_reserved_name_shape_wire`) still says "{table} is
// (re)declared only by the installer that owns it ({installer}), never by
// a CREATE TABLE a consumer types directly -- choose a different name" --
// which was already false for the original four `ENGINE_OWNED_LEDGER_TABLES`
// names (a canonical-shape consumer CREATE has always succeeded for them,
// e.g. `create_table_work_capabilities_with_canonical_columns_silent_on_policy_still_works`
// above) and is newly false for the five hub-refereed names too, since
// round 3 extended the shape door to them
// (`local_create_of_each_hub_refereed_table_with_canonical_columns_and_no_clause_still_works`).
// The message should state the REAL rule: columns must structurally match
// the canonical shape, not that only the installer may ever type the name.
// ---------------------------------------------------------------------

/// RED: the shape-refusal message (triggered by a wrong-shape local CREATE
/// of any of the nine reserved names) must not claim installer-exclusive
/// naming rights -- a consumer's CANONICAL-shape CREATE already succeeds
/// for every one of them.
#[test]
fn shape_refusal_does_not_claim_installer_exclusivity() {
    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
        "work_inputs",
        "work_capabilities",
        "peer_directory",
        "work_node_contacts",
    ] {
        let db = Database::open_memory();
        let err = db
            .execute(&format!("CREATE TABLE {table} (x TEXT)"), &p())
            .expect_err(&format!("{table} with the wrong column shape must refuse"));
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains(table) && reason.contains("engine-owned"),
            "must name {table} as engine-owned infrastructure: {reason}"
        );
        assert!(
            !reason.contains("never by a CREATE TABLE a consumer types directly"),
            "the shape refusal must not claim {table} is installer-exclusive -- a consumer's \
             CANONICAL-shape CREATE already succeeds for {table}; only a structural mismatch \
             is refused: {reason}"
        );
        assert!(
            reason.to_lowercase().contains("column") || reason.to_lowercase().contains("shape"),
            "the refusal should state the real rule -- columns must structurally match the \
             canonical shape -- not installer-exclusivity: {reason}"
        );
    }
}

// ---------------------------------------------------------------------
// Fix round 6 (blocker, ship-gate finding with a LIVE repro): the shape
// door (`refuse_engine_owned_reserved_name_shape` locally,
// `refuse_engine_owned_reserved_name_shape_wire` over the wire) compares
// only column COUNT / NAME / DATA TYPE / PRIMARY KEY -- never nullability,
// `UNIQUE`, `DEFAULT`, or `REFERENCES`, all of which
// `contextdb_parser::ast::ColumnDef` already carries (`nullable: bool`,
// `unique: bool`, `default: Option<Expr>`, `references: Option<ForeignKey>`
// -- no parser plumbing needed, the AST has everything). So `CREATE TABLE
// work_jobs (job_id TEXT PRIMARY KEY, work_class TEXT NOT NULL UNIQUE,
// ...canonical rest...)` is ACCEPTED today -- `work_class` is canonically
// just `TEXT NOT NULL` (`work_ledger.rs`'s `CREATE_WORK_JOBS`), no
// `UNIQUE` -- and `install_work_ledger_schema` later adopts the table
// as-is (idempotent-by-name-presence, no structural repair,
// `work_ledger.rs:715`-area). LIVE-PROVEN by the ship-gate: two valid jobs
// sharing a `work_class` insert as `{"rows_affected":1}` then
// `{"rows_affected":0}` -- the second job silently vanishes (a UNIQUE
// constraint turns the canonically-append-only `work_jobs` INSERT into a
// silent no-op), with nothing in the response to show it happened.
// ---------------------------------------------------------------------

/// One canonical `work_jobs` CREATE prefix (`hub_refereed_canonical_create_prefix`)
/// with exactly one column's DECLARED TEXT swapped for a hidden,
/// behavior-changing variant -- the column name/type/primary-key triple
/// the current door checks is UNCHANGED, only an attribute the door never
/// looks at differs. Each entry is `(label, mismatched full CREATE TABLE
/// DDL)`.
fn work_jobs_hidden_attribute_mismatches() -> Vec<(&'static str, String)> {
    let canonical = hub_refereed_canonical_create_prefix("work_jobs");
    vec![
        (
            // The exact live repro: an added UNIQUE constraint turns a
            // canonically append-only history table into one where a
            // second job sharing `work_class` silently fails to insert.
            "work_class gains a hidden UNIQUE constraint",
            canonical.replace(
                "work_class TEXT NOT NULL,",
                "work_class TEXT NOT NULL UNIQUE,",
            ),
        ),
        (
            // `output_schema` is canonically nullable (no `NOT NULL` in
            // `CREATE_WORK_JOBS`); flipping it to NOT NULL silently starts
            // rejecting every job whose caller omits an output schema --
            // a class of the same "the door didn't look, so nothing
            // stopped it" failure as the UNIQUE case.
            "output_schema loses its canonical nullability",
            canonical.replace("output_schema TEXT,", "output_schema TEXT NOT NULL,"),
        ),
        (
            // `priority` canonically carries no DEFAULT; a hidden DEFAULT
            // silently changes what an omitted `priority` on INSERT means.
            "priority gains a hidden DEFAULT",
            canonical.replace(
                "priority INTEGER NOT NULL,",
                "priority INTEGER NOT NULL DEFAULT 0,",
            ),
        ),
        (
            // Fix round 9: this suite's own comment above (naming
            // `references: Option<ForeignKey>` alongside `unique`/`default`
            // as an axis the door checks) had never actually been exercised
            // by a matrix row -- `work_class` canonically carries no
            // `REFERENCES` at all. Self-referencing (work_class -> this
            // same table's own PRIMARY KEY) so this row needs no extra
            // setup, matching every other row in this fixture.
            "work_class gains a hidden REFERENCES clause",
            canonical.replace(
                "work_class TEXT NOT NULL,",
                "work_class TEXT NOT NULL REFERENCES work_jobs(job_id),",
            ),
        ),
    ]
}

/// RED: local `CREATE TABLE work_jobs (...)` with canonical column
/// name/type/primary-key but a hidden attribute mismatch on one column
/// (UNIQUE / NOT NULL / DEFAULT) must refuse, exactly like a
/// name/type/primary-key mismatch already does
/// (`local_create_of_each_hub_refereed_table_with_wrong_explicit_conflict_refuses`'s
/// sibling shape tests above). Confirmed live before authoring: today each
/// of these three DDLs is silently ACCEPTED (this test's failure IS that
/// confirmation, captured as a real assertion rather than a throwaway
/// probe).
#[test]
fn local_create_of_work_jobs_with_a_hidden_attribute_mismatch_refuses() {
    for (label, ddl) in work_jobs_hidden_attribute_mismatches() {
        let db = Database::open_memory();
        let result = db.execute(&ddl, &p());
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "CREATE TABLE work_jobs (...) with a hidden attribute mismatch ({label}) must \
                 refuse -- the shape door checks only column count/name/type/primary-key, so \
                 this DDL was silently ACCEPTED even though it declares a structurally \
                 different work_jobs than work_ledger.rs's CREATE_WORK_JOBS. DDL: {ddl}"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains("work_jobs") && reason.contains("engine-owned"),
            "must name work_jobs as engine-owned infrastructure ({label}): {reason}"
        );
        assert!(
            db.table_meta("work_jobs").is_none(),
            "a refused CREATE TABLE must not leave a half-made work_jobs table behind \
             ({label})"
        );
    }
}

/// The same defect through the shared door on a DIFFERENT one of the nine
/// reserved names (`peer_directory`, one of the original four
/// `ENGINE_OWNED_LEDGER_TABLES`) -- proving this is the shared shape door's
/// bug, not a `work_jobs`-only quirk. `peer_directory`'s `ticket` column is
/// canonically `TEXT NOT NULL` with no `UNIQUE`
/// (`peer_directory::CREATE_PEER_DIRECTORY`).
#[test]
fn local_create_of_peer_directory_with_a_hidden_unique_constraint_refuses() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE peer_directory (\
            node_id TEXT PRIMARY KEY, \
            ticket TEXT NOT NULL UNIQUE, \
            enrolled_at TIMESTAMP NOT NULL)",
        &p(),
    );
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE peer_directory (...) with a hidden UNIQUE on ticket must refuse -- \
             the shape door is shared with work_jobs and has the identical gap: it checks only \
             column count/name/type/primary-key, never UNIQUE/NOT NULL/DEFAULT/REFERENCES"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
}

/// RED, wire path: a FRESH arriving `CreateTable` DDL (the table does not
/// yet exist locally, so `refuse_engine_owned_policy_sync_ddl`'s
/// fresh-create arm runs `refuse_engine_owned_reserved_name_shape_wire`)
/// naming `work_jobs` with canonical name/type/primary-key but the SAME
/// three hidden attribute mismatches must refuse identically to the local
/// door above -- the wire door has the same narrow comparison
/// (`executor.rs`'s `refuse_engine_owned_reserved_name_shape_wire`, which
/// only inspects a `starts_with` type prefix and a `"PRIMARY KEY"`
/// substring on the wire type string).
/// One column-list mutator, named so the mismatch table below reads as data
/// instead of a clippy-flagged inline function-pointer type.
type ColumnMismatchMutator = fn(&mut Vec<(String, String)>);

#[test]
fn arriving_fresh_create_table_of_work_jobs_with_a_hidden_attribute_mismatch_refuses() {
    let mismatches: [(&str, ColumnMismatchMutator); 3] = [
        ("work_class gains a hidden UNIQUE constraint", |cols| {
            for (name, ty) in cols.iter_mut() {
                if name == "work_class" {
                    *ty = "TEXT NOT NULL UNIQUE".to_string();
                }
            }
        }),
        ("output_schema loses its canonical nullability", |cols| {
            for (name, ty) in cols.iter_mut() {
                if name == "output_schema" {
                    *ty = "TEXT NOT NULL".to_string();
                }
            }
        }),
        ("priority gains a hidden DEFAULT", |cols| {
            for (name, ty) in cols.iter_mut() {
                if name == "priority" {
                    *ty = "INTEGER NOT NULL DEFAULT 0".to_string();
                }
            }
        }),
    ];

    for (label, mutate) in mismatches {
        let db = Database::open_memory();
        let mut columns = hub_refereed_wire_columns("work_jobs");
        mutate(&mut columns);

        let offending_create = DdlChange::CreateTable {
            name: "work_jobs".to_string(),
            columns,
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        };
        let result = apply_single_ddl(&db, offending_create);
        let err = match result {
            Err(err) => err,
            Ok(_) => panic!(
                "an arriving fresh CreateTable of work_jobs with a hidden attribute mismatch \
                 ({label}) must refuse, exactly like the local CREATE door above"
            ),
        };
        let reason = schema_invalid_reason(err);
        assert!(
            reason.contains("work_jobs") && reason.contains("engine-owned"),
            "must name work_jobs as engine-owned infrastructure ({label}): {reason}"
        );
        assert!(
            db.table_meta("work_jobs").is_none(),
            "a refused arriving CreateTable must not leave a half-made work_jobs table \
             behind ({label})"
        );
    }
}

/// GREEN contrast pin: the EXACT canonical `work_jobs` DDL, verbatim from
/// `work_ledger.rs`'s `CREATE_WORK_JOBS` (word-for-word, not via the
/// shared prefix helper, so this pin fails first if that constant and this
/// literal ever drift apart), still creates successfully -- the fix for
/// the hidden-attribute gap above must not start refusing the real
/// installer's own text.
#[test]
fn local_create_of_work_jobs_with_the_verbatim_canonical_ddl_still_succeeds() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_jobs (\
             job_id TEXT PRIMARY KEY, \
             work_class TEXT NOT NULL, \
             mode TEXT NOT NULL, \
             requirement_tags JSON NOT NULL, \
             input_refs JSON NOT NULL, \
             output_schema TEXT, \
             priority INTEGER NOT NULL, \
             deadline TIMESTAMP, \
             max_attempts INTEGER NOT NULL, \
             submitter_node_id TEXT NOT NULL, \
             provenance JSON, \
             submitted_at TIMESTAMP NOT NULL) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &p(),
    )
    .expect("the verbatim canonical work_jobs DDL must still create successfully");
}

/// GREEN contrast pin, live-repro half: two jobs with the SAME `work_class`
/// insert cleanly (both `rows_affected == 1`) against the canonical,
/// unmodified `work_jobs` shape -- `work_class` is NOT unique canonically.
/// This is the other side of the live repro: once the shape door refuses
/// the hidden-UNIQUE DDL above, the only way to reach this table is through
/// the real installer's canonical shape, where two same-`work_class` jobs
/// are both ordinary, expected inserts -- neither silently vanishes.
#[test]
fn two_jobs_sharing_a_work_class_both_insert_on_the_canonical_shape() {
    // Mirrors `work_ledger::submit_job_in_tx`'s own INSERT text and param
    // binding exactly (every column bound as a typed `Value`, not a raw SQL
    // literal), so this test cannot fail on unrelated literal-syntax
    // grounds -- only on the live-repro question of whether a second job
    // sharing `work_class` actually inserts.
    fn insert_job_params(job_id: &str, work_class: &str) -> HashMap<String, Value> {
        let mut params = HashMap::new();
        params.insert("job_id".to_string(), Value::Text(job_id.to_string()));
        params.insert(
            "work_class".to_string(),
            Value::Text(work_class.to_string()),
        );
        params.insert("mode".to_string(), Value::Text("batch".to_string()));
        params.insert(
            "requirement_tags".to_string(),
            Value::Json(serde_json::json!([])),
        );
        params.insert("input_refs".to_string(), Value::Json(serde_json::json!([])));
        params.insert("output_schema".to_string(), Value::Null);
        params.insert("priority".to_string(), Value::Int64(0));
        params.insert("deadline".to_string(), Value::Null);
        params.insert("max_attempts".to_string(), Value::Int64(2));
        params.insert(
            "submitter_node_id".to_string(),
            Value::Text("node-a".to_string()),
        );
        params.insert("provenance".to_string(), Value::Null);
        params.insert(
            "submitted_at".to_string(),
            Value::Timestamp(1_700_000_000_000),
        );
        params
    }
    const INSERT_WORK_JOB_SQL: &str = "INSERT INTO work_jobs (job_id, work_class, mode, requirement_tags, input_refs, \
         output_schema, priority, deadline, max_attempts, submitter_node_id, provenance, \
         submitted_at) VALUES ($job_id, $work_class, $mode, $requirement_tags, $input_refs, \
         $output_schema, $priority, $deadline, $max_attempts, $submitter_node_id, \
         $provenance, $submitted_at)";

    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let insert1 = db
        .execute(INSERT_WORK_JOB_SQL, &insert_job_params("j1", "shared"))
        .expect("first job must insert");
    assert_eq!(
        insert1.rows_affected, 1,
        "first job with work_class=shared must insert"
    );

    let insert2 = db
        .execute(INSERT_WORK_JOB_SQL, &insert_job_params("j2", "shared"))
        .expect("second job must insert");
    assert_eq!(
        insert2.rows_affected, 1,
        "second job sharing the SAME work_class as the first must ALSO insert -- work_class \
         is not unique on the canonical work_jobs shape; a silent rows_affected:0 here is the \
         live-repro failure a hidden UNIQUE constraint on work_class causes"
    );
}

// ---------------------------------------------------------------------
// Round 6 addendum (blocker): `engine_owned_policy_refusal` has THREE
// message branches. Rounds 4 and 5 fixed the two branches reachable only
// for the five hub-refereed work-ledger tables (the `work_claims` /
// `work_results` / `work_cancellations` no-declared-clause branch, and the
// `work_jobs` / `work_failures` declared-clause branch) to stop claiming
// installer-exclusivity / categorical ALTER-immunity. The THIRD branch --
// the generic fall-through `Error::SchemaInvalid` at the bottom of the
// function -- was never touched by those fixes, and it is what the four
// ORIGINAL `ENGINE_OWNED_LEDGER_TABLES` (`work_inputs` / `work_capabilities`
// / `peer_directory` / `work_node_contacts`) still get for a mismatching
// SYNC CONFLICT declaration (`is_hub_refereed_sync_conflict_table` is
// false for all four, so `clause == "SYNC CONFLICT" &&
// is_hub_refereed_sync_conflict_table(table)` never matches them). It
// still says "{table} is (re)declared only by the installer that owns it,
// never by an ALTER an operator types" -- the SAME falsehood rounds 4/5
// fixed elsewhere, surviving in the one branch those fixes edited around.
// Binary-disproven exactly like before: `restating_the_declared_value_verbatim_is_not_refused`
// above already proves an honest verbatim ALTER restate succeeds on these
// four tables; this test pins the message text alongside that proof.
// ---------------------------------------------------------------------

/// Shared assertion for a SYNC-CONFLICT refusal message, reused across all
/// three of `engine_owned_policy_refusal`'s branches: it must name the
/// table as engine-owned, but must NOT claim installer-exclusive
/// declaration rights or categorical ALTER-immunity -- an honest,
/// canonical-matching value can legally be declared via ALTER on every one
/// of the nine reserved names, hub-refereed or original four alike.
fn assert_sync_conflict_refusal_tolerates_the_honest_restate(reason: &str, table: &str) {
    assert!(
        reason.contains(table) && reason.contains("engine-owned"),
        "must name {table} as engine-owned infrastructure: {reason}"
    );
    assert!(
        !reason.contains("(re)declared only by the installer"),
        "the refusal must not claim {table}'s SYNC CONFLICT is (re)declared only by the \
         installer -- the honest, canonical-matching value CAN legally be declared via \
         ALTER: {reason}"
    );
    assert!(
        !reason.contains("never by an ALTER an operator types"),
        "the refusal must not claim {table} is categorically immune to ALTER -- an honest \
         verbatim restate via ALTER already succeeds on {table}: {reason}"
    );
}

/// RED: the generic fall-through branch of `engine_owned_policy_refusal`
/// (reached by `work_capabilities` and `peer_directory` -- two of the four
/// original `ENGINE_OWNED_LEDGER_TABLES`, both canonically `keep_latest` --
/// on a mismatching `SYNC CONFLICT KEEP FIRST`) must not claim
/// installer-exclusivity or ALTER-immunity, and the paired fact that makes
/// the claim false (the honest restate succeeding via ALTER) is asserted
/// immediately after each refusal, not just cited.
#[test]
fn fallback_sync_conflict_refusal_does_not_claim_installer_exclusivity_or_alter_immunity() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    install_peer_directory_schema(&db).expect("install peer_directory schema");

    for table in ["work_capabilities", "peer_directory"] {
        let err = db
            .execute(
                &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP FIRST"),
                &p(),
            )
            .expect_err(&format!(
                "a mismatching SYNC CONFLICT (KEEP FIRST vs {table}'s true keep_latest) must \
                 refuse"
            ));
        assert_sync_conflict_refusal_tolerates_the_honest_restate(
            &schema_invalid_reason(err),
            table,
        );

        db.execute(
            &format!("ALTER TABLE {table} SET SYNC CONFLICT KEEP LATEST"),
            &p(),
        )
        .unwrap_or_else(|err| {
            panic!(
                "the honest verbatim restate (KEEP LATEST, {table}'s true word) must succeed \
                 via ALTER -- the refusal just asserted above claims this is impossible: \
                 {err:?}"
            )
        });
    }
}

// ---------------------------------------------------------------------
// Fix round 7 (blocker, two independent ship reviews at tip 8c69eea, both
// with live binary repros): the reserved-name shape door
// (`refuse_engine_owned_reserved_name_shape` / its wire mirror) still
// compares only column count/name/type/primary-key/nullable and the
// UNIQUE/DEFAULT/REFERENCES absence round 6 added -- it never looks at
// `IMMUTABLE` or `EXPIRES`, both of which `ColumnDef` already carries
// (`immutable: bool`, `expires: bool` -- no parser plumbing needed, exactly
// the round-6 finding's shape). And no door anywhere judges a LOCAL `ALTER
// TABLE ... ADD/DROP/RENAME COLUMN` on a reserved name at all, nor does the
// arriving-wire `AlterTable` preflight arm judge column shape (it calls only
// the policy-axis doors) -- so `merge_sync_alter_existing_column`
// (`database.rs:39640`) silently LATCHES an incoming IMMUTABLE / UNIQUE
// column attribute onto an already-installed reserved table with no gate in
// front of it at all.
// ---------------------------------------------------------------------

/// RED, live repro (Codex): a hidden `IMMUTABLE` on `peer_directory`'s
/// `ticket` column must refuse locally -- `CREATE_PEER_DIRECTORY` declares
/// `ticket` as plain `TEXT NOT NULL`, never `IMMUTABLE`. Accepted today: an
/// `IMMUTABLE` ticket column arms `register_peer_ticket`'s own re-enrollment
/// upsert (`INSERT ... ON CONFLICT (node_id) DO UPDATE SET ticket = ...`) to
/// fail with `Error::ImmutableColumn` the next time a node actually rotates
/// its ticket, leaving a stale, undialable ticket in `peer_directory`
/// permanently.
#[test]
fn local_create_of_peer_directory_with_a_hidden_immutable_ticket_refuses() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE peer_directory (\
            node_id TEXT PRIMARY KEY, \
            ticket TEXT NOT NULL IMMUTABLE, \
            enrolled_at TIMESTAMP NOT NULL)",
        &p(),
    );
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE peer_directory (...) with a hidden IMMUTABLE on ticket must refuse -- \
             the shape door checks only column count/name/type/primary-key/nullable and the \
             UNIQUE/DEFAULT/REFERENCES absence, never IMMUTABLE/EXPIRES, so this DDL is \
             silently ACCEPTED even though it declares a structurally different peer_directory \
             than peer_directory.rs's CREATE_PEER_DIRECTORY"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("peer_directory").is_none(),
        "a refused CREATE TABLE must not leave a half-made peer_directory table behind"
    );
}

/// GREEN companion to the pin above: on the real canonical `peer_directory`
/// shape (the only shape reachable once the IMMUTABLE door above closes),
/// ticket rotation -- a node re-enrolling with a new ticket -- actually
/// updates the stored ticket, so the failure mode the RED above names (a
/// permanently stale, undialable ticket) cannot occur through the real
/// installer's own shape.
#[test]
fn peer_directory_ticket_rotation_updates_the_ticket_on_canonical_shape() {
    let db = Database::open_memory();
    install_peer_directory_schema(&db).expect("install peer_directory");

    register_peer_ticket(&db, "node-a", "ticket-1", 1_700_000_000_000)
        .expect("first enrollment must insert");
    assert_eq!(
        lookup_peer_ticket(&db, "node-a").expect("lookup after first enrollment"),
        Some("ticket-1".to_string())
    );

    register_peer_ticket(&db, "node-a", "ticket-2", 1_700_000_100_000)
        .expect("re-enrollment (ticket rotation) must update, not refuse or duplicate");
    assert_eq!(
        lookup_peer_ticket(&db, "node-a").expect("lookup after rotation"),
        Some("ticket-2".to_string()),
        "ticket rotation must make the NEW ticket win -- a stale undialable ticket is exactly \
         the failure an IMMUTABLE ticket column would cause"
    );
}

/// RED, live repro: a hidden `EXPIRES` on `work_jobs`' `deadline` column must
/// refuse locally -- `CREATE_WORK_JOBS` declares `deadline` as plain
/// nullable `TIMESTAMP`, never `EXPIRES`. Accepted today: an `EXPIRES`
/// deadline column arms row expiry on the engine's own append-only job
/// ledger, silently pruning jobs `work_ledger.rs`'s own bookkeeping still
/// expects to find.
#[test]
fn local_create_of_work_jobs_with_a_hidden_expires_deadline_refuses() {
    let db = Database::open_memory();
    let ddl = hub_refereed_canonical_create_prefix("work_jobs")
        .replace("deadline TIMESTAMP, ", "deadline TIMESTAMP EXPIRES, ");
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_jobs (...) with a hidden EXPIRES on deadline must refuse -- the \
             shape door never looks at EXPIRES, so this DDL is silently ACCEPTED. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_jobs table behind"
    );
}

/// RED, wire path: the fresh-`CreateTable` mirror of the IMMUTABLE pin above
/// -- `refuse_engine_owned_reserved_name_shape_wire`'s `wire_has_extra_
/// constraint` token check names only `UNIQUE` / `DEFAULT` / `REFERENCES`,
/// never `IMMUTABLE`, so an arriving fresh CreateTable naming work_jobs with
/// an IMMUTABLE work_class is silently accepted exactly like the local door.
#[test]
fn arriving_fresh_create_table_of_work_jobs_with_a_hidden_immutable_work_class_refuses() {
    let db = Database::open_memory();
    let mut columns = hub_refereed_wire_columns("work_jobs");
    for (name, ty) in columns.iter_mut() {
        if name == "work_class" {
            *ty = "TEXT NOT NULL IMMUTABLE".to_string();
        }
    }
    let offending_create = DdlChange::CreateTable {
        name: "work_jobs".to_string(),
        columns,
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_create);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving fresh CreateTable of work_jobs with a hidden IMMUTABLE on work_class \
             must refuse, exactly like the local CREATE door above -- the wire shape door's \
             extra-constraint token check names only UNIQUE/DEFAULT/REFERENCES, never \
             IMMUTABLE"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused arriving CreateTable must not leave a half-made work_jobs table behind"
    );
}

/// RED, wire path: the fresh-`CreateTable` mirror of the EXPIRES pin above.
#[test]
fn arriving_fresh_create_table_of_work_jobs_with_a_hidden_expires_deadline_refuses() {
    let db = Database::open_memory();
    let mut columns = hub_refereed_wire_columns("work_jobs");
    for (name, ty) in columns.iter_mut() {
        if name == "deadline" {
            *ty = "TIMESTAMP EXPIRES".to_string();
        }
    }
    let offending_create = DdlChange::CreateTable {
        name: "work_jobs".to_string(),
        columns,
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_create);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving fresh CreateTable of work_jobs with a hidden EXPIRES on deadline must \
             refuse, exactly like the local CREATE door above -- the wire shape door's \
             extra-constraint token check names only UNIQUE/DEFAULT/REFERENCES, never EXPIRES"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused arriving CreateTable must not leave a half-made work_jobs table behind"
    );
}

// ---------------------------------------------------------------------
// Fix round 7: the LOCAL ALTER shape door does not exist at all -- no code
// anywhere refuses `ALTER TABLE <reserved-name> ADD/DROP/RENAME COLUMN`, so
// an operator can freely reshape one of the nine reserved names' columns
// after `install_work_ledger_schema` has already created it.
// ---------------------------------------------------------------------

/// RED: dropping a column the engine's own bookkeeping reads from its own
/// job table must refuse.
#[test]
fn alter_drop_column_on_work_jobs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let result = db.execute("ALTER TABLE work_jobs DROP COLUMN work_class", &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "ALTER TABLE work_jobs DROP COLUMN work_class must refuse -- no door judges a \
             LOCAL ALTER's column-shape changes on any of the nine reserved names today"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    let meta = db.table_meta("work_jobs").expect("work_jobs exists");
    assert!(
        meta.columns.iter().any(|c| c.name == "work_class"),
        "a refused ALTER must not have dropped work_class"
    );
}

/// RED: adding an arbitrary column (with an arbitrary UNIQUE constraint) to
/// the engine's own job ledger must refuse.
#[test]
fn alter_add_column_with_unique_on_work_jobs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let result = db.execute("ALTER TABLE work_jobs ADD COLUMN k TEXT UNIQUE", &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "ALTER TABLE work_jobs ADD COLUMN k TEXT UNIQUE must refuse -- an operator can \
             freely reshape the engine's own job ledger after install_work_ledger_schema has \
             already created it"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    let meta = db.table_meta("work_jobs").expect("work_jobs exists");
    assert!(
        !meta.columns.iter().any(|c| c.name == "k"),
        "a refused ALTER must not have added column k"
    );
}

/// RED: renaming a column the engine's own bookkeeping reads by name must
/// refuse.
#[test]
fn alter_rename_column_on_work_jobs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let result = db.execute(
        "ALTER TABLE work_jobs RENAME COLUMN work_class TO wat",
        &p(),
    );
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "ALTER TABLE work_jobs RENAME COLUMN work_class TO wat must refuse -- renaming a \
             column the engine's own bookkeeping (work_ledger.rs) reads by name silently \
             breaks it"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    let meta = db.table_meta("work_jobs").expect("work_jobs exists");
    assert!(
        meta.columns.iter().any(|c| c.name == "work_class"),
        "a refused ALTER must not have renamed work_class"
    );
}

/// Green guard: the identical three column-shape ALTERs on a plain operator
/// table are entirely unaffected -- the door (once it exists) must be
/// table-name-scoped to the nine reserved names, not a blanket refusal of
/// ADD/DROP/RENAME COLUMN everywhere.
#[test]
fn column_shape_alters_on_a_user_table_still_work() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, extra TEXT)",
        &p(),
    )
    .expect("create notes");

    db.execute("ALTER TABLE notes DROP COLUMN extra", &p())
        .expect("DROP COLUMN on a user table must still work");
    db.execute("ALTER TABLE notes ADD COLUMN k TEXT UNIQUE", &p())
        .expect("ADD COLUMN ... UNIQUE on a user table must still work");
    db.execute("ALTER TABLE notes RENAME COLUMN body TO content", &p())
        .expect("RENAME COLUMN on a user table must still work");

    let meta = db.table_meta("notes").expect("notes exists");
    assert!(!meta.columns.iter().any(|c| c.name == "extra"));
    assert!(meta.columns.iter().any(|c| c.name == "k"));
    assert!(meta.columns.iter().any(|c| c.name == "content"));
}

// ---------------------------------------------------------------------
// Fix round 7: the arriving-wire `AlterTable` preflight arm of
// `refuse_engine_owned_policy_sync_ddl` (`database.rs:39930`) calls only
// `refuse_engine_owned_policy_axes` and
// `refuse_hub_refereed_ledger_sync_conflict_mismatch` -- neither judges
// column shape at all. `merge_sync_alter_existing_column`
// (`database.rs:39640`) then LATCHES an incoming column's `unique` /
// `immutable` flag onto the current table's column unconditionally, with no
// door in front of it for any of the nine reserved names.
// ---------------------------------------------------------------------

/// RED, wire path: an arriving `AlterTable` restating an already-installed
/// `work_jobs` with a hidden UNIQUE on `work_class` must refuse -- today it
/// silently latches `unique=true` onto the engine's own job table, arming
/// the identical silent-INSERT-no-op live repro round 6 fixed for the local
/// CREATE door, reachable here through ALTER instead.
#[test]
fn arriving_alter_table_with_a_hidden_unique_on_work_jobs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let mut columns = hub_refereed_wire_columns("work_jobs");
    for (name, ty) in columns.iter_mut() {
        if name == "work_class" {
            *ty = "TEXT NOT NULL UNIQUE".to_string();
        }
    }
    let offending_alter = DdlChange::AlterTable {
        name: "work_jobs".to_string(),
        columns,
        constraints: vec!["SYNC CONFLICT KEEP FIRST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_alter);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving AlterTable restating work_jobs with a hidden UNIQUE on work_class must \
             refuse -- neither door in refuse_engine_owned_policy_sync_ddl's AlterTable arm \
             judges column shape, so merge_sync_alter_existing_column silently latches \
             unique=true onto the engine's own job table"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    let meta = db.table_meta("work_jobs").expect("work_jobs exists");
    assert!(
        !meta
            .columns
            .iter()
            .any(|c| c.name == "work_class" && c.unique),
        "a refused AlterTable must not have latched UNIQUE onto work_class"
    );
}

/// RED, wire path, sibling table class: an arriving `AlterTable` restating
/// an already-installed `peer_directory` with a hidden IMMUTABLE on `ticket`
/// must refuse -- proving this is the shared AlterTable preflight's gap, not
/// a `work_jobs`-only or hub-refereed-only quirk. Today it silently latches
/// `immutable=true` onto `ticket`, arming the same ticket-rotation failure
/// the local CREATE door's IMMUTABLE pin catches, reachable here through
/// ALTER instead.
#[test]
fn arriving_alter_table_with_a_hidden_immutable_on_peer_directory_refuses() {
    let db = Database::open_memory();
    install_peer_directory_schema(&db).expect("install peer_directory");

    let offending_alter = DdlChange::AlterTable {
        name: "peer_directory".to_string(),
        columns: vec![
            ("node_id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("ticket".to_string(), "TEXT NOT NULL IMMUTABLE".to_string()),
            ("enrolled_at".to_string(), "TIMESTAMP NOT NULL".to_string()),
        ],
        constraints: vec![
            "HISTORY CURRENT ONLY".to_string(),
            "SYNC TWO WAY".to_string(),
            "SYNC CONFLICT KEEP LATEST".to_string(),
        ],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_alter);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving AlterTable restating peer_directory with a hidden IMMUTABLE on ticket \
             must refuse -- neither door in refuse_engine_owned_policy_sync_ddl's AlterTable \
             arm judges column shape, so merge_sync_alter_existing_column silently latches \
             immutable=true onto the engine's own ticket column"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
    let meta = db
        .table_meta("peer_directory")
        .expect("peer_directory exists");
    assert!(
        !meta
            .columns
            .iter()
            .any(|c| c.name == "ticket" && c.immutable),
        "a refused AlterTable must not have latched IMMUTABLE onto ticket"
    );
}

// ---------------------------------------------------------------------
// Fix round 7 (blocker -- REFUSAL TEXT TRUTH, work_node_contacts): the
// generic SYNC-CONFLICT fallback branch of `engine_owned_policy_refusal`
// (`executor.rs`) says "its SYNC CONFLICT declaration lives once, in the
// engine's own CREATE TABLE text for {table}" and "a SYNC CONFLICT clause
// that restates this arbitration verbatim ... is accepted here" -- both
// FALSE for `work_node_contacts`: its own CREATE TABLE text
// (`CREATE_WORK_NODE_CONTACTS`, contextdb-server) declares NO SYNC CONFLICT
// clause at all (`engine_owned_ledger_policy`'s own canonical entry for it
// is `conflict: None`), so there is no declaration to point at and no
// verbatim value a caller could restate to be accepted -- EVERY explicit
// SYNC CONFLICT value mismatches `None`.
// ---------------------------------------------------------------------

/// RED: the refusal for an explicit SYNC CONFLICT on `work_node_contacts`
/// must not cite a nonexistent declaration or promise a verbatim restate
/// that cannot exist.
#[test]
fn set_sync_conflict_on_work_node_contacts_does_not_lie_about_a_declaration() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE work_node_contacts (\
            node_id TEXT PRIMARY KEY, \
            last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF",
        &p(),
    )
    .expect("create work_node_contacts");

    let err = db
        .execute(
            "ALTER TABLE work_node_contacts SET SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect_err("an explicit SYNC CONFLICT on work_node_contacts must still refuse");
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_node_contacts") && reason.contains("engine-owned"),
        "must name work_node_contacts as engine-owned infrastructure: {reason}"
    );
    assert!(
        !reason.contains("lives once, in the engine's own CREATE TABLE text"),
        "work_node_contacts' own CREATE TABLE text (CREATE_WORK_NODE_CONTACTS) declares no \
         SYNC CONFLICT clause at all -- the refusal must not point at DDL text that does not \
         exist: {reason}"
    );
    assert!(
        !reason.contains("restates this arbitration verbatim"),
        "there is no canonical SYNC CONFLICT value for work_node_contacts to restate -- \
         engine_owned_ledger_policy's own canonical entry for it is conflict: None, so EVERY \
         explicit SYNC CONFLICT value mismatches, not just a wrong one -- the refusal must not \
         promise a verbatim restate is ever accepted: {reason}"
    );

    let meta = db
        .table_meta("work_node_contacts")
        .expect("work_node_contacts exists");
    assert_eq!(
        meta.conflict_policy, None,
        "a refused SET SYNC CONFLICT must apply no part of itself"
    );
}

// ---------------------------------------------------------------------
// Fix round 7 (blocker -- SHAPE-REFUSAL MESSAGE TRUTH): the shape-refusal
// message (`engine_owned_reserved_shape_refusal`) says a CREATE is accepted
// "only when its columns match that shape exactly, in name, type, and
// primary key" -- but every one of round 6's own hidden-attribute-mismatch
// fixtures (`work_jobs_hidden_attribute_mismatches`) satisfies name, type,
// AND primary key exactly; only an unrelated attribute (UNIQUE / nullability
// / DEFAULT, and now IMMUTABLE / EXPIRES) differs. Claiming the refusal is
// about name/type/primary-key gives a dissatisfied operator nothing to
// diagnose the ACTUAL mismatch with.
// ---------------------------------------------------------------------

/// RED: the shape-refusal message must not claim the comparison is limited
/// to name/type/primary-key, and should name the offending column so an
/// operator can actually diagnose the mismatch.
#[test]
fn shape_refusal_names_the_offending_column_not_just_name_type_primary_key() {
    for (label, ddl) in work_jobs_hidden_attribute_mismatches() {
        let db = Database::open_memory();
        let err = db
            .execute(&ddl, &p())
            .expect_err(&format!("{label} must still refuse"));
        let reason = schema_invalid_reason(err);
        assert!(
            !reason.contains("exactly, in name, type, and primary key"),
            "every one of these three DDLs satisfies name/type/primary-key exactly -- only an \
             UNRELATED attribute differs -- so claiming the refusal is about name/type/\
             primary-key gives an operator nothing to diagnose ({label}): {reason}"
        );
        assert!(
            reason.contains("work_class")
                || reason.contains("output_schema")
                || reason.contains("priority"),
            "the refusal should name the offending column so an operator can diagnose which \
             attribute is wrong ({label}): {reason}"
        );
    }
}

// ---------------------------------------------------------------------
// Fix round 8 (two independent verdicts at 1f13013, both confirming the
// round-7 column-level fix works): the reserved-name shape door still
// compares only COLUMNS and table-level key constraints (composite UNIQUE /
// PRIMARY KEY / FOREIGN KEY) -- it never compares the plan's table-level
// OPTIONS (`CreateTablePlan.immutable` / `.state_machine` / `.dag_edge_types`
// / `.propagation_rules`). None of the nine reserved tables' own CREATE
// TABLE text declares a table-level IMMUTABLE, so a consumer typing one in
// is a structural mismatch exactly like a hidden per-column UNIQUE was
// before round 7 -- but nothing today judges it.
// ---------------------------------------------------------------------

/// RED, live repro (Codex): a table-level `IMMUTABLE` option on `peer_directory`
/// -- structurally different from `CREATE_PEER_DIRECTORY`, which declares no
/// table-level IMMUTABLE at all -- must refuse locally. Accepted today:
/// `refuse_engine_owned_reserved_name_shape` reads `p.columns` /
/// `p.unique_constraints` / `p.primary_key_columns` / `p.composite_foreign_keys`
/// but never `p.immutable`, and `executor.rs`'s `CreateTable` arm installs
/// `p.immutable` into `TableMeta.immutable` uncompared. Once installed, a
/// table-level-immutable `peer_directory` arms `register_peer_ticket`'s own
/// re-enrollment upsert to refuse with `Error::ImmutableTable` the next time
/// a node rotates its ticket -- the ticket-rotation break round 7's
/// column-level IMMUTABLE pin caught, reachable here through the
/// TABLE-level option instead of a column-level one.
///
/// No `HISTORY CURRENT ONLY` clause here (unlike this suite's other
/// `peer_directory` DDL fixtures) -- the parser refuses `IMMUTABLE` and
/// `HISTORY CURRENT ONLY` together outright as mutually exclusive (an
/// immutable table never supersedes a row, so it has no version history to
/// reclaim), a genuine, unrelated semantic rule that fires before this
/// door's own question is ever asked. `refuse_engine_owned_policy_axes`
/// already tolerates silence on `HISTORY` (a legacy pre-declaration shape),
/// so omitting it here isolates the table-level-IMMUTABLE question this RED
/// pins -- matching the exact DDL the live repro used.
#[test]
fn local_create_of_peer_directory_with_a_table_level_immutable_option_refuses() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE peer_directory (\
            node_id TEXT PRIMARY KEY, \
            ticket TEXT NOT NULL, \
            enrolled_at TIMESTAMP NOT NULL) IMMUTABLE SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    );
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE peer_directory (...) IMMUTABLE must refuse -- CREATE_PEER_DIRECTORY \
             declares no table-level IMMUTABLE option; the reserved-name shape door never reads \
             CreateTablePlan.immutable, so this DDL is silently ACCEPTED and arms \
             register_peer_ticket's ticket-rotation upsert to fail with Error::ImmutableTable"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("peer_directory").is_none(),
        "a refused CREATE TABLE must not leave a half-made peer_directory table behind"
    );
}

/// RED, sibling table class: the identical table-level `IMMUTABLE` mismatch
/// on `work_jobs` (a hub-refereed table, not one of the original
/// `ENGINE_OWNED_LEDGER_TABLES` four) must also refuse -- proving the gap is
/// the shared shape door's, not `peer_directory`-only. `IMMUTABLE` is the
/// only table-level option exercised here: none of the nine reserved
/// tables' canonical shapes name a designated status column, so
/// `STATE_MACHINE (...)` / `DAG (...)` have no valid column to reference on
/// any of them -- IMMUTABLE is the one table-level option genuinely
/// expressible against a canonical column list without inventing an
/// unrelated column.
#[test]
fn local_create_of_work_jobs_with_a_table_level_immutable_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} IMMUTABLE SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        hub_refereed_canonical_create_prefix("work_jobs")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_jobs (...) IMMUTABLE must refuse -- CREATE_WORK_JOBS declares no \
             table-level IMMUTABLE option, and the shape door never reads \
             CreateTablePlan.immutable. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_jobs table behind"
    );
}

/// RED, wire path: the fresh-`CreateTable` mirror of the table-level
/// IMMUTABLE pin above -- `refuse_engine_owned_reserved_name_shape_wire`'s
/// signature never receives the arriving `constraints: Vec<String>` list at
/// all (only columns / composite_unique / composite_foreign_keys), so a
/// table-level `IMMUTABLE` entry in `constraints` reaches
/// `rough_sync_table_meta` untouched by any shape door and lands as
/// `TableMeta.immutable = true` on a fresh reserved-name create.
///
/// No `HISTORY CURRENT ONLY` constraint here, for the same reason the local
/// CREATE pin above omits it: `IMMUTABLE` and `HISTORY CURRENT ONLY` are
/// refused together as mutually exclusive by the reconstructed-SQL
/// pre-validation this path also runs, before this RED's own question is
/// ever asked; silence on `HISTORY` is already tolerated
/// (`refuse_engine_owned_policy_axes`).
#[test]
fn arriving_fresh_create_table_of_peer_directory_with_a_table_level_immutable_option_refuses() {
    let db = Database::open_memory();
    let offending_create = DdlChange::CreateTable {
        name: "peer_directory".to_string(),
        columns: vec![
            ("node_id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("ticket".to_string(), "TEXT NOT NULL".to_string()),
            ("enrolled_at".to_string(), "TIMESTAMP NOT NULL".to_string()),
        ],
        constraints: vec![
            "SYNC TWO WAY".to_string(),
            "SYNC CONFLICT KEEP LATEST".to_string(),
            "IMMUTABLE".to_string(),
        ],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_create);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving fresh CreateTable of peer_directory with a table-level IMMUTABLE \
             constraint must refuse, exactly like the local CREATE door above -- \
             refuse_engine_owned_reserved_name_shape_wire's signature never receives the \
             arriving table-level constraints list at all"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("peer_directory").is_none(),
        "a refused arriving CreateTable must not leave a half-made peer_directory table behind"
    );
}

// ---------------------------------------------------------------------
// Fix round 8 addendum (cold reviewer widened the table-level-options
// finding with live probes): the gap is not IMMUTABLE-only, and not
// peer_directory-only -- STATE_MACHINE and DAG are the SAME uncompared
// `CreateTablePlan` fields (`state_machine` / `dag_edge_types`), confirmed
// accepted today on other reserved names. Table choice matters here: some
// reserved names are shielded from an IMMUTABLE probe by an UNRELATED
// parser rule (peer_directory / work_capabilities / work_node_contacts
// refuse `IMMUTABLE` + their own declared `HISTORY CURRENT ONLY` as
// mutually exclusive; `work_inputs` refuses `IMMUTABLE` + its declared
// `RETAIN` the same way) -- so the IMMUTABLE axis is pinned on `work_jobs`
// / `work_claims` (genuinely exposed, no such shield), while DAG -- which
// carries no such conflict with HISTORY -- is pinned on `peer_directory`
// (proving the shield is specific to IMMUTABLE, not a defense of the door).
// ---------------------------------------------------------------------

/// RED, sibling table: the identical table-level `IMMUTABLE` mismatch on
/// `work_claims` (SYNC-CONFLICT-only hub-refereed policy, no HISTORY
/// declared to shield it) must refuse.
#[test]
fn local_create_of_work_claims_with_a_table_level_immutable_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} IMMUTABLE",
        hub_refereed_canonical_create_prefix("work_claims")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_claims (...) IMMUTABLE must refuse -- CREATE_WORK_CLAIMS \
             declares no table-level IMMUTABLE option, and the shape door never reads \
             CreateTablePlan.immutable. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_claims") && reason.contains("engine-owned"),
        "must name work_claims as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_claims").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_claims table behind"
    );
}

/// GREEN companion, the strongest consequence pin (owner-requested): on the
/// real canonical `work_claims` shape -- the only shape reachable once the
/// table-level-IMMUTABLE door above closes -- lease renewal (an UPDATE
/// extending an already-claimed job's `lease_deadline`) actually applies.
/// Today a smuggled-in table-level-immutable `work_claims` is silently
/// accepted (the RED above) and then refuses every such UPDATE with
/// `Error::ImmutableTable` ("work_claims are immutable") -- lease renewal
/// dead fabric-wide. This proves the failure mode cannot occur through the
/// real installer's own (non-immutable) shape.
#[test]
fn work_claims_lease_renewal_update_succeeds_on_canonical_shape() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let mut insert_params = HashMap::new();
    insert_params.insert("claim_key".to_string(), Value::Text("c1".to_string()));
    insert_params.insert("job_id".to_string(), Value::Text("j1".to_string()));
    insert_params.insert("attempt".to_string(), Value::Int64(1));
    insert_params.insert("node_id".to_string(), Value::Text("node-a".to_string()));
    insert_params.insert(
        "lease_deadline".to_string(),
        Value::Timestamp(1_700_000_000_000),
    );
    insert_params.insert(
        "claimed_at".to_string(),
        Value::Timestamp(1_700_000_000_000),
    );
    db.execute(
        "INSERT INTO work_claims (claim_key, job_id, attempt, node_id, lease_deadline, \
         claimed_at) VALUES ($claim_key, $job_id, $attempt, $node_id, $lease_deadline, \
         $claimed_at)",
        &insert_params,
    )
    .expect("initial claim insert must succeed");

    let mut update_params = HashMap::new();
    update_params.insert("claim_key".to_string(), Value::Text("c1".to_string()));
    update_params.insert(
        "lease_deadline".to_string(),
        Value::Timestamp(1_700_000_100_000),
    );
    db.execute(
        "UPDATE work_claims SET lease_deadline = $lease_deadline WHERE claim_key = $claim_key",
        &update_params,
    )
    .expect(
        "lease renewal (extending lease_deadline on an already-claimed job) must succeed on \
         the canonical, non-immutable work_claims shape -- a table-level-immutable work_claims \
         would refuse this with Error::ImmutableTable, killing lease renewal fabric-wide",
    );
}

/// RED, PROPAGATE axis: a table-level `PROPAGATE ON STATE node_id EXCLUDE
/// VECTOR` option on `work_claims` must refuse -- `CREATE_WORK_CLAIMS`
/// declares no propagation rule at all. Confirmed live: silently accepted
/// today (`CreateTablePlan.propagation_rules` is the fourth uncompared
/// table-level field alongside `immutable` / `state_machine` /
/// `dag_edge_types`).
#[test]
fn local_create_of_work_claims_with_a_table_level_propagate_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} PROPAGATE ON STATE node_id EXCLUDE VECTOR",
        hub_refereed_canonical_create_prefix("work_claims")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_claims (...) PROPAGATE ON STATE node_id EXCLUDE VECTOR must \
             refuse -- CREATE_WORK_CLAIMS declares no table-level PROPAGATE option, and the \
             shape door never reads CreateTablePlan.propagation_rules. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_claims") && reason.contains("engine-owned"),
        "must name work_claims as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_claims").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_claims table behind"
    );
}

/// RED, STATE_MACHINE axis: a table-level `STATE_MACHINE (node_id: a -> b)`
/// option on `work_claims` -- structurally different from `CREATE_WORK_CLAIMS`,
/// which declares no state machine at all -- must refuse. Confirmed live:
/// accepted today for the same reason the IMMUTABLE probes above are --
/// `refuse_engine_owned_reserved_name_shape` receives only 5 of
/// `CreateTablePlan`'s 13 fields (name/columns/unique_constraints/
/// primary_key_columns/composite_foreign_keys); `state_machine` is never
/// passed in.
#[test]
fn local_create_of_work_claims_with_a_table_level_state_machine_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} STATE_MACHINE (node_id: a -> b)",
        hub_refereed_canonical_create_prefix("work_claims")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_claims (...) STATE_MACHINE (node_id: a -> b) must refuse -- \
             CREATE_WORK_CLAIMS declares no table-level STATE_MACHINE option, and the shape \
             door never reads CreateTablePlan.state_machine. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_claims") && reason.contains("engine-owned"),
        "must name work_claims as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_claims").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_claims table behind"
    );
}

/// RED, STATE_MACHINE axis, sibling table: the identical mismatch on
/// `work_jobs` (`STATE_MACHINE (mode: a -> b)`), proving the gap is the
/// shared door's, not `work_claims`-only.
#[test]
fn local_create_of_work_jobs_with_a_table_level_state_machine_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} STATE_MACHINE (mode: a -> b)",
        hub_refereed_canonical_create_prefix("work_jobs")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_jobs (...) STATE_MACHINE (mode: a -> b) must refuse -- \
             CREATE_WORK_JOBS declares no table-level STATE_MACHINE option, and the shape door \
             never reads CreateTablePlan.state_machine. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_jobs table behind"
    );
}

/// RED, DAG axis: a table-level `DAG ('e')` option on `work_claims` must
/// refuse -- `CREATE_WORK_CLAIMS` declares no `dag_edge_types` at all.
#[test]
fn local_create_of_work_claims_with_a_table_level_dag_option_refuses() {
    let db = Database::open_memory();
    let ddl = format!(
        "{} DAG ('e')",
        hub_refereed_canonical_create_prefix("work_claims")
    );
    let result = db.execute(&ddl, &p());
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE work_claims (...) DAG ('e') must refuse -- CREATE_WORK_CLAIMS \
             declares no table-level DAG option, and the shape door never reads \
             CreateTablePlan.dag_edge_types. DDL: {ddl}"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_claims") && reason.contains("engine-owned"),
        "must name work_claims as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_claims").is_none(),
        "a refused CREATE TABLE must not leave a half-made work_claims table behind"
    );
}

/// RED, DAG axis, the table the IMMUTABLE probe cannot reach: `peer_directory`
/// declares `HISTORY CURRENT ONLY`, which shields it from an `IMMUTABLE`
/// probe via the unrelated mutual-exclusion rule -- but `DAG` carries no
/// such conflict, so it sails straight past that shield and exposes the
/// identical missing-comparison gap on this table too.
#[test]
fn local_create_of_peer_directory_with_a_table_level_dag_option_refuses() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE peer_directory (\
            node_id TEXT PRIMARY KEY, \
            ticket TEXT NOT NULL, \
            enrolled_at TIMESTAMP NOT NULL) DAG ('x')",
        &p(),
    );
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "CREATE TABLE peer_directory (...) DAG ('x') must refuse -- CREATE_PEER_DIRECTORY \
             declares no table-level DAG option, and the shape door never reads \
             CreateTablePlan.dag_edge_types -- unlike a table-level IMMUTABLE probe on this \
             same table, DAG carries no unrelated mutual-exclusion conflict with \
             peer_directory's declared HISTORY CURRENT ONLY to hide behind"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("peer_directory") && reason.contains("engine-owned"),
        "must name peer_directory as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("peer_directory").is_none(),
        "a refused CREATE TABLE must not leave a half-made peer_directory table behind"
    );
}

// ---------------------------------------------------------------------
// Fix round 8 (review must-fix, reverses a recorded owner ruling --
// existence-first): round 7's LOCAL ALTER shape door
// (`refuse_engine_owned_reserved_name_column_alter`) runs BEFORE any
// `db.table_meta` existence lookup in the `AddColumn` / `DropColumn` /
// `RenameColumn` arms, so on a store where a reserved table was NEVER
// installed, a column-shape ALTER on that name reports the engine-owned
// refusal -- implying a real, governed table -- instead of "table not
// found", which is what every OTHER unknown-table ALTER (including this
// same table's own `SET SYNC CONFLICT` / `SET RETAIN` / `SET HISTORY` arms)
// already reports and what a caller must see to tell "this table doesn't
// exist yet" apart from "this table exists and is engine-governed" (the
// same existence-first ruling the round-2 fix already applied to
// `AlterAction::SetSyncConflict`, pinned by
// `alter_on_a_hub_refereed_table_reports_not_found_before_engine_owned_when_ledger_absent`
// in `show_sync_conflict_policy_tests.rs`).
// ---------------------------------------------------------------------

/// RED: on a fresh store with no ledger installed at all, a column-shape
/// ALTER on any of the nine reserved names must report "table not found",
/// not the engine-owned refusal.
#[test]
fn alter_column_shape_on_a_reserved_name_reports_not_found_before_engine_owned_when_ledger_absent()
{
    let db = Database::open_memory();

    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
        "work_inputs",
        "work_capabilities",
        "peer_directory",
        "work_node_contacts",
    ] {
        let err = db
            .execute(&format!("ALTER TABLE {table} ADD COLUMN x TEXT"), &p())
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
                "{table} does not exist in this store yet -- ALTER must report 'table not \
                 found', not the engine-owned refusal (existence must be checked before \
                 ownership, matching the round-2 ruling already applied to SET SYNC CONFLICT): \
                 {reason}"
            ),
            other => {
                panic!("expected Error::Other('table not found') for absent {table}, got {other:?}")
            }
        }
    }
}

// ---------------------------------------------------------------------
// Fix round 8 (review low finding): the arriving-wire door reparses column
// type strings through the real parser, but the SEPARATE code path that
// actually APPLIES an arriving AlterTable's column shape
// (`rough_sync_column_def`, `database.rs`) re-reads the SAME raw string
// with naive `.contains("UNIQUE")` / `.contains("IMMUTABLE")` substring
// checks, and `normalize_schema_type` only collapses whitespace -- it never
// strips comments, even though the grammar's own `WHITESPACE` rule treats a
// block comment as whitespace (`/* ... */` is silently skippable, exactly
// like a space). So an arriving column type string like `TEXT /*UNIQUE*/`
// is syntactically just `TEXT` to the REAL parser (used only for the
// preflight's SQL-validity check, `validate_sync_alter_table_shape_ddl`),
// but the naive string that is ACTUALLY merged into the applied column
// still sees the substring inside the comment and sets `unique = true`.
// This applies to ANY table, reserved or not -- it is not a reserved-name
// door gap at all, but a defect in the shared column-merge machinery every
// arriving AlterTable goes through.
// ---------------------------------------------------------------------

/// RED: an arriving `AlterTable` on a plain OPERATOR table (`notes`, outside
/// the nine reserved names entirely) restating a column's type with a
/// block-comment-smuggled `UNIQUE` token must NOT apply `unique = true` --
/// the real parser sees no `UNIQUE` token once the comment is stripped as
/// whitespace, so the applied column must not either.
#[test]
fn arriving_alter_table_comment_smuggled_unique_on_an_operator_table_does_not_apply() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("create notes");

    let smuggled_alter = DdlChange::AlterTable {
        name: "notes".to_string(),
        columns: vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("body".to_string(), "TEXT /*UNIQUE*/".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, smuggled_alter).expect(
        "an arriving AlterTable restating notes with a block comment in body's type must still \
         apply -- the DDL is syntactically valid (a block comment is WHITESPACE to the real \
         parser, contextdb-parser/src/grammar.pest's WHITESPACE rule)",
    );

    let meta = db.table_meta("notes").expect("notes exists");
    let body = meta
        .columns
        .iter()
        .find(|c| c.name == "body")
        .expect("body column exists");
    assert!(
        !body.unique,
        "body's arriving type text 'TEXT /*UNIQUE*/' carries no real UNIQUE token once \
         comments are stripped by the real grammar -- the applied column must reflect that \
         honest parse, not rough_sync_column_def's naive `.contains(\"UNIQUE\")` substring \
         match against the RAW, un-stripped string, which sees the token inside the comment \
         and silently sets unique=true"
    );
}

/// RED, sibling axis: the identical comment-smuggle for `IMMUTABLE` on a
/// plain operator table -- `IMMUTABLE` has a real behavioral consequence
/// (`Error::ImmutableTable` on UPSERT, `Error::ImmutableColumn` on
/// ALTER/UPDATE), so a silently-smuggled-in `immutable = true` is not just
/// an inert flag.
#[test]
fn arriving_alter_table_comment_smuggled_immutable_on_an_operator_table_does_not_apply() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("create notes");

    let smuggled_alter = DdlChange::AlterTable {
        name: "notes".to_string(),
        columns: vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("body".to_string(), "TEXT /*IMMUTABLE*/".to_string()),
        ],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, smuggled_alter).expect(
        "an arriving AlterTable restating notes with a block comment in body's type must still \
         apply -- the DDL is syntactically valid",
    );

    let meta = db.table_meta("notes").expect("notes exists");
    let body = meta
        .columns
        .iter()
        .find(|c| c.name == "body")
        .expect("body column exists");
    assert!(
        !body.immutable,
        "body's arriving type text 'TEXT /*IMMUTABLE*/' carries no real IMMUTABLE token once \
         comments are stripped by the real grammar -- the applied column must reflect that \
         honest parse, not a naive substring match against the raw string"
    );
}

// ---------------------------------------------------------------------
// Fix round 9, item 1 (Codex blocker, live-reproduced): the sync AlterTable
// preflight (`refuse_engine_owned_policy_sync_ddl`'s `AlterTable` arm,
// `database.rs`) calls `refuse_engine_owned_reserved_name_shape_wire`
// WITHOUT the arriving `foreign_keys` field, even though it is available
// right there in the match arm -- and the wire shape door's own signature
// (`executor.rs`) has no `foreign_keys` parameter to receive it AT ALL: it
// reconstructs candidate `CREATE TABLE` text via
// `sync_create_table_sql(table, columns, constraints, &[], &[], &[])`, a
// hardcoded empty foreign-key list. Meanwhile `foreign_keys` DOES reach
// `rough_sync_table_meta` (used to build the `incoming` merged straight into
// the applied column via `merge_sync_alter_existing_column`), so a hidden
// `REFERENCES` smuggled in through the wire's separate `foreign_keys` field
// is invisible to every shape check yet still lands on the installed
// column. Same gap on the fresh-CreateTable arm, which calls the identical
// under-parameterized door.
// ---------------------------------------------------------------------

/// RED, live repro: an arriving `AlterTable` restating an already-installed
/// `work_jobs`'s full canonical columns and honest policy, but carrying a
/// `foreign_keys` entry pointing `work_class` at an unrelated operator
/// table, must refuse.
#[test]
fn arriving_alter_table_with_a_foreign_key_on_work_jobs_refuses() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    db.execute("CREATE TABLE operators (id UUID PRIMARY KEY)", &p())
        .expect("create an operator table to reference");

    let offending_alter = DdlChange::AlterTable {
        name: "work_jobs".to_string(),
        columns: hub_refereed_wire_columns("work_jobs"),
        constraints: vec!["SYNC CONFLICT KEEP FIRST".to_string()],
        foreign_keys: vec![SingleColumnForeignKey {
            child_column: "work_class".to_string(),
            parent_table: "operators".to_string(),
            parent_column: "id".to_string(),
        }],
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let result = apply_single_ddl(&db, offending_alter);
    let err = match result {
        Err(err) => err,
        Ok(_) => panic!(
            "an arriving AlterTable restating work_jobs with a foreign_keys entry on work_class \
             must refuse -- refuse_engine_owned_reserved_name_shape_wire has no foreign_keys \
             parameter at all (it reconstructs candidate DDL with a hardcoded empty \
             foreign-key list), so a hidden REFERENCES clause carried on the wire's separate \
             foreign_keys field is invisible to the shape door, while \
             merge_sync_alter_existing_column still latches it onto the installed column"
        ),
    };
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    let meta = db.table_meta("work_jobs").expect("work_jobs exists");
    assert!(
        meta.columns
            .iter()
            .find(|c| c.name == "work_class")
            .expect("work_class column exists")
            .references
            .is_none(),
        "a refused AlterTable must not have latched a REFERENCES onto work_class"
    );
}

/// Green guard, wire path, the fresh-`CreateTable` mirror of the ALTER
/// finding above -- confirms the gap is specific to `AlterTable`, not the
/// fresh-create arm. Unlike `refuse_engine_owned_policy_sync_ddl`'s
/// `AlterTable` arm, its fresh-create arm's downstream general validation
/// (`validate_sync_table_shape_ddl`) reconstructs the FULL candidate SQL
/// WITH the real `foreign_keys` (`sync_create_table_sql` renders a matching
/// entry as a per-column `REFERENCES` clause), so a fresh reserved-name
/// create carrying a hidden `foreign_keys` entry already refuses today --
/// this closes the matrix's own claimed-but-untested REFERENCES axis for
/// the wire fresh-create path by proving it, not by pinning a new bug (the
/// local-matrix sibling row above, `work_jobs_hidden_attribute_mismatches`,
/// closes the same axis for the local AST path the same way -- already
/// green, not a new RED).
#[test]
fn arriving_fresh_create_table_of_work_jobs_with_a_foreign_key_reference_already_refuses() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE operators (id UUID PRIMARY KEY)", &p())
        .expect("create an operator table to reference");

    let offending_create = DdlChange::CreateTable {
        name: "work_jobs".to_string(),
        columns: hub_refereed_wire_columns("work_jobs"),
        constraints: Vec::new(),
        foreign_keys: vec![SingleColumnForeignKey {
            child_column: "work_class".to_string(),
            parent_table: "operators".to_string(),
            parent_column: "id".to_string(),
        }],
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let err = apply_single_ddl(&db, offending_create).expect_err(
        "an arriving fresh CreateTable of work_jobs with a foreign_keys entry on work_class \
         must refuse -- and it already does, via validate_sync_table_shape_ddl's \
         real-foreign-key SQL reconstruction, unlike the AlterTable arm pinned above",
    );
    let reason = schema_invalid_reason(err);
    assert!(
        reason.contains("work_jobs") && reason.contains("engine-owned"),
        "must name work_jobs as engine-owned infrastructure: {reason}"
    );
    assert!(
        db.table_meta("work_jobs").is_none(),
        "a refused arriving CreateTable must not leave a half-made work_jobs table behind"
    );
}

/// Green guard: an arriving `AlterTable` restating `work_jobs`'s own
/// canonical shape with an EMPTY `foreign_keys` list -- the honest,
/// no-op-on-this-axis case -- must still apply. The fix for the REDs above
/// must not start refusing every arriving AlterTable on a reserved name
/// merely for carrying a `foreign_keys` field at all.
#[test]
fn arriving_alter_table_restating_work_jobs_with_empty_foreign_keys_still_applies() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let honest_alter = DdlChange::AlterTable {
        name: "work_jobs".to_string(),
        columns: hub_refereed_wire_columns("work_jobs"),
        constraints: vec!["SYNC CONFLICT KEEP FIRST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, honest_alter).expect(
        "an arriving AlterTable restating work_jobs' own canonical shape with an empty \
         foreign_keys list must still apply",
    );
}

// ---------------------------------------------------------------------
// Fix round 9, item 2 (Opus must-fix, fourth recurrence of the same
// falsehood): `engine_owned_policy_refusal`'s final fall-through arm --
// reached for the RETAIN / HISTORY / SYNC-direction axes on the four
// ORIGINAL `ENGINE_OWNED_LEDGER_TABLES` -- still says "{table} is
// (re)declared only by the installer that owns it, never by an ALTER an
// operator types". That is false on every one of those three axes exactly
// the way it was already fixed THREE times before on the SYNC CONFLICT
// axis (rounds 4/5/6's `hub_refereed_sync_conflict_refusal_does_not_claim_
// installer_exclusivity` / `alter_honest_verbatim_restate_on_a_hub_refereed_
// table_succeeds` / `fallback_sync_conflict_refusal_does_not_claim_
// installer_exclusivity_or_alter_immunity` above): an honest, canonical-
// matching value CAN legally be declared via ALTER --
// `restating_the_declared_value_verbatim_is_not_refused` above already
// proves this for RETAIN / HISTORY / SYNC CONFLICT together. This suite
// already owns the exact assertion helper for this claim
// (`assert_sync_conflict_refusal_tolerates_the_honest_restate`, despite its
// SYNC-CONFLICT-flavored name its body names no clause at all -- it is
// already clause-generic) but it has only ever been APPLIED to the SYNC
// CONFLICT axis. Applying it to RETAIN / HISTORY / SYNC direction is what
// this item pins.
// ---------------------------------------------------------------------

/// RED: the fall-through refusal for a mismatching RETAIN window on
/// `work_inputs` must not claim installer-exclusivity or categorical
/// ALTER-immunity -- the paired fact that makes the claim false (the honest
/// restate succeeding via ALTER) is asserted immediately after, exactly
/// like this suite's own SYNC CONFLICT precedent.
#[test]
fn fallback_retain_refusal_does_not_claim_installer_exclusivity_or_alter_immunity() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_inputs SET RETAIN 1 DAYS", &p())
        .expect_err("a mismatching RETAIN window (1 DAY vs work_inputs' true 7 DAYS) must refuse");
    assert_sync_conflict_refusal_tolerates_the_honest_restate(
        &schema_invalid_reason(err),
        "work_inputs",
    );

    db.execute("ALTER TABLE work_inputs SET RETAIN 7 DAYS", &p())
        .unwrap_or_else(|err| {
            panic!(
                "the honest verbatim restate (7 DAYS, work_inputs' true window) must succeed \
                 via ALTER -- the refusal just asserted above claims this is impossible: \
                 {err:?}"
            )
        });
}

/// RED: the fall-through refusal for a mismatching HISTORY declaration on
/// `work_capabilities` / `peer_directory` (both canonically `HISTORY
/// CURRENT ONLY`) must not claim installer-exclusivity or categorical
/// ALTER-immunity either.
#[test]
fn fallback_history_refusal_does_not_claim_installer_exclusivity_or_alter_immunity() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");
    install_peer_directory_schema(&db).expect("install peer_directory schema");

    for table in ["work_capabilities", "peer_directory"] {
        let err = db
            .execute(&format!("ALTER TABLE {table} SET HISTORY ALL"), &p())
            .expect_err(&format!(
                "a mismatching HISTORY (ALL vs {table}'s true CURRENT ONLY) must refuse"
            ));
        assert_sync_conflict_refusal_tolerates_the_honest_restate(
            &schema_invalid_reason(err),
            table,
        );

        db.execute(
            &format!("ALTER TABLE {table} SET HISTORY CURRENT ONLY"),
            &p(),
        )
        .unwrap_or_else(|err| {
            panic!(
                "the honest verbatim restate (CURRENT ONLY, {table}'s true value) must \
                     succeed via ALTER -- the refusal just asserted above claims this is \
                     impossible: {err:?}"
            )
        });
    }
}

/// RED: the fall-through refusal for a mismatching SYNC direction on
/// `work_capabilities` (canonically `SYNC TWO WAY`) must not claim
/// installer-exclusivity or categorical ALTER-immunity either.
#[test]
fn fallback_sync_direction_refusal_does_not_claim_installer_exclusivity_or_alter_immunity() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install ledger schema");

    let err = db
        .execute("ALTER TABLE work_capabilities SET SYNC OFF", &p())
        .expect_err(
            "a mismatching SYNC direction (OFF vs work_capabilities' true TWO WAY) must refuse",
        );
    assert_sync_conflict_refusal_tolerates_the_honest_restate(
        &schema_invalid_reason(err),
        "work_capabilities",
    );

    db.execute("ALTER TABLE work_capabilities SET SYNC TWO WAY", &p())
        .unwrap_or_else(|err| {
            panic!(
                "the honest verbatim restate (TWO WAY, work_capabilities' true direction) must \
                 succeed via ALTER -- the refusal just asserted above claims this is \
                 impossible: {err:?}"
            )
        });
}

// ---------------------------------------------------------------------
// Fix round 10 (cold review must-fix): existence-first ordering was never
// door-wide -- `alter_column_shape_on_a_reserved_name_reports_not_found_
// before_engine_owned_when_ledger_absent` above only exercises the three
// column-shape `AlterAction` variants. Of the other five, `SetHistory` and
// `SetSyncConflict` already look up `db.table_meta(&p.table)` before
// calling `refuse_engine_owned_policy_mutation`, but `SetRetain`,
// `DropRetain`, and `SetSyncDirection` (`executor.rs`) all call
// `refuse_engine_owned_policy_mutation` FIRST -- so on a store where a
// reserved table was never installed, those three report the engine-owned
// refusal (implying a real, governed table) instead of "table not found".
// ---------------------------------------------------------------------

/// RED: on a fresh store with no ledger installed at all, EVERY
/// `AlterAction` variant on any of the nine reserved names must report
/// "table not found", not the engine-owned refusal -- the existence-before-
/// ownership ordering the door claims to follow uniformly. Covers all
/// eight variants (`AddColumn` / `DropColumn` / `RenameColumn` /
/// `SetRetain` / `DropRetain` / `SetHistory` / `SetSyncConflict` /
/// `SetSyncDirection`), not just the column-shape three the sibling test
/// above already pins.
#[test]
fn every_alter_action_on_a_reserved_name_reports_not_found_before_engine_owned_when_ledger_absent()
{
    let db = Database::open_memory();

    let alter_actions: [(&str, &str); 8] = [
        ("AddColumn", "ADD COLUMN x TEXT"),
        ("DropColumn", "DROP COLUMN x"),
        ("RenameColumn", "RENAME COLUMN x TO y"),
        ("SetRetain", "SET RETAIN 1 DAYS"),
        ("DropRetain", "DROP RETAIN"),
        ("SetHistory", "SET HISTORY ALL"),
        ("SetSyncConflict", "SET SYNC CONFLICT KEEP LATEST"),
        ("SetSyncDirection", "SET SYNC PUSH ONLY"),
    ];

    for table in [
        "work_jobs",
        "work_claims",
        "work_results",
        "work_failures",
        "work_cancellations",
        "work_inputs",
        "work_capabilities",
        "peer_directory",
        "work_node_contacts",
    ] {
        for (label, clause) in alter_actions {
            let err = db
                .execute(&format!("ALTER TABLE {table} {clause}"), &p())
                .expect_err(&format!(
                    "{table} does not exist in this store, so ALTER TABLE {table} {clause} \
                     ({label}) must fail somehow"
                ));
            match err {
                Error::Other(reason) => {
                    assert!(
                        reason.contains("not found"),
                        "expected a table-not-found error for absent {table} ({label}), got: \
                         {reason}"
                    );
                }
                Error::SchemaInvalid { reason } => panic!(
                    "{table} does not exist in this store yet -- ALTER TABLE {table} {clause} \
                     ({label}) must report 'table not found', not the engine-owned refusal \
                     (existence must be checked before ownership on every AlterAction variant, \
                     not just the column-shape ones and SET SYNC CONFLICT / SET HISTORY): \
                     {reason}"
                ),
                other => panic!(
                    "expected Error::Other('table not found') for absent {table} ({label}), \
                     got {other:?}"
                ),
            }
        }
    }
}
