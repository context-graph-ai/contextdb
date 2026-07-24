//! The built-in tables that carry their own HISTORY / SYNC / RETAIN
//! declaration in-DDL (`work_capabilities`, `peer_directory`,
//! `work_node_contacts`, `work_inputs`) must gain that declaration on a
//! root that predates it, not just on a freshly created root.
//!
//! Each installer (`install_work_ledger_schema`, `install_peer_directory_
//! schema`, `install_node_contacts_schema`) is idempotent-by-absence: it
//! only ever `CREATE TABLE`s a table that does not exist yet, so an already-
//! present table is never recreated with the newer DDL. The reconcile half
//! (an `ALTER TABLE` run when the table exists but its `TableMeta` doesn't
//! yet carry the declaration) is what closes that gap, and until this test
//! it had no coverage proving it end to end on a real reopened root.
//! `work_inputs`' `RETAIN 7 DAYS` reconcile is the same gap on a different
//! axis: a root created before that declaration landed would otherwise keep
//! every input copy forever.
//!
//! HONEST CONSTRUCTION NOTE: there is no old-shape binary fixture for this —
//! all four declarations landed in the same work as the tables themselves,
//! so no released build ever shipped them undeclared. The pre-declaration
//! root below is built directly through the store's own DDL surface: the
//! identical column list each installer's own `CREATE TABLE` constant
//! carries, with the trailing `HISTORY` / `SYNC CONFLICT` / `SYNC OFF` /
//! `RETAIN` clause stripped off. That is exactly the on-disk shape an older
//! installer revision (one that had not yet learned to declare the table)
//! would have produced — not a synthetic stand-in for it.
//!
//! NOW-TRUE INVARIANT: a gap on one of these four tables' declared policy
//! can ONLY mean this — a root that predates the declaration, never "an
//! operator (or a peer) just opted out." `refuse_engine_owned_policy_
//! mutation` (`contextdb-engine/src/executor.rs`) refuses every LOCAL
//! `ALTER TABLE` that would move one of these tables away from its declared
//! shape, before any part of it is written; its arriving-sync-DDL mirror,
//! `refuse_engine_owned_policy_sync_ddl` (`database.rs`), refuses the same
//! mutation atomically for a whole sync batch, whether it arrives spelled as
//! an `AlterTable` or as a `CreateTable` adopting an already-existing table;
//! and the sync-apply merge (`engine_owned_merged_policy`) makes sure a
//! silent axis on an arriving DDL PRESERVES the table's current declared
//! value rather than clearing it, so an in-progress multi-step reconcile (or
//! a dropped clause from an unguarded/older binary) never reads as an
//! implicit opt-out either. Before these doors existed,
//! `default_ttl_seconds.is_none()` / a missing `history_policy` /
//! `conflict_policy` was ambiguous between "this legacy root" and "an
//! operator (or a peer) just opted out" — and the reconcile arms below could
//! not tell the two apart, so they silently re-declared the engine's policy
//! over an operator's own explicit choice. See
//! `crates/contextdb-engine/tests/engine_owned_ledger_policy_tests.rs` for
//! that door's own coverage; this file stays scoped to proving the
//! reconcile heals a genuine legacy root end to end.

use contextdb_core::{ConflictPolicy, HistoryPolicy, SyncDirection, Value};
use contextdb_engine::Database;
use contextdb_engine::peer_directory::install_peer_directory_schema;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use contextdb_server::work_ledger::install_node_contacts_schema;
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

const BUILTINS: [&str; 3] = ["work_capabilities", "peer_directory", "work_node_contacts"];

#[test]
fn pre_declaration_root_gains_all_three_builtin_declarations_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pre-declaration.db");

    // Build the pre-declaration root: all three built-ins present, in their
    // OLD undeclared shape -- plain CREATE TABLE, no HISTORY / SYNC clause
    // at all, the way a pre-declaration installer would have left them.
    {
        let db = Database::open(&path).expect("create pre-declaration root");
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
        .expect("declare pre-declaration work_capabilities");
        db.execute(
            "CREATE TABLE peer_directory (\
                node_id TEXT PRIMARY KEY, \
                ticket TEXT NOT NULL, \
                enrolled_at TIMESTAMP NOT NULL)",
            &p(),
        )
        .expect("declare pre-declaration peer_directory");
        db.execute(
            "CREATE TABLE work_node_contacts (\
                node_id TEXT PRIMARY KEY, \
                last_contact_ms TIMESTAMP NOT NULL)",
            &p(),
        )
        .expect("declare pre-declaration work_node_contacts");
        db.execute(
            "CREATE TABLE work_inputs (\
                input_key TEXT PRIMARY KEY, \
                job_id TEXT NOT NULL, \
                seq INTEGER NOT NULL, \
                payload TEXT NOT NULL)",
            &p(),
        )
        .expect("declare pre-declaration work_inputs");

        // Confirm the constructed root really is undeclared before it is
        // dropped -- the premise the rest of this test depends on.
        for table in BUILTINS {
            let meta = db
                .table_meta(table)
                .unwrap_or_else(|| panic!("{table} must exist on the pre-declaration root"));
            assert_eq!(
                meta.history_policy, None,
                "{table} must start with no HISTORY declaration"
            );
            assert_eq!(
                meta.conflict_policy, None,
                "{table} must start with no SYNC CONFLICT declaration"
            );
            assert_eq!(
                meta.sync_direction, None,
                "{table} must start with no SYNC direction declaration"
            );
        }
        let work_inputs_meta = db
            .table_meta("work_inputs")
            .expect("work_inputs must exist on the pre-declaration root");
        assert_eq!(
            work_inputs_meta.default_ttl_seconds, None,
            "work_inputs must start with no RETAIN declaration"
        );
        assert!(
            !db.maintenance_status().running,
            "an undeclared root must not have armed the maintenance loop yet"
        );
        db.close().expect("close pre-declaration root");
    }

    // Reopen -- the same path a real upgraded root takes -- and run each
    // installer exactly as its owning consumer does at startup. Every
    // installer sees its table already present and must reconcile it via
    // ALTER TABLE rather than skip it outright.
    let reopened = Database::open(&path).expect("reopen the pre-declaration root");
    install_work_ledger_schema(&reopened)
        .expect("reconcile work_capabilities (plus the six sibling ledger tables)");
    install_peer_directory_schema(&reopened).expect("reconcile peer_directory");
    install_node_contacts_schema(&reopened).expect("reconcile work_node_contacts");

    let capabilities = reopened
        .table_meta("work_capabilities")
        .expect("work_capabilities survives reopen");
    assert_eq!(
        capabilities.history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "work_capabilities must gain HISTORY CURRENT ONLY on reconcile"
    );
    assert_eq!(
        capabilities.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "work_capabilities must gain SYNC CONFLICT KEEP LATEST on reconcile"
    );

    let peers = reopened
        .table_meta("peer_directory")
        .expect("peer_directory survives reopen");
    assert_eq!(
        peers.history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "peer_directory must gain HISTORY CURRENT ONLY on reconcile"
    );
    assert_eq!(
        peers.conflict_policy,
        Some(ConflictPolicy::LatestWins),
        "peer_directory must gain SYNC CONFLICT KEEP LATEST on reconcile"
    );

    let contacts = reopened
        .table_meta("work_node_contacts")
        .expect("work_node_contacts survives reopen");
    assert_eq!(
        contacts.history_policy,
        Some(HistoryPolicy::CurrentOnly),
        "work_node_contacts must gain HISTORY CURRENT ONLY on reconcile"
    );
    assert_eq!(
        contacts.sync_direction,
        Some(SyncDirection::None),
        "work_node_contacts must gain SYNC OFF on reconcile"
    );

    let inputs = reopened
        .table_meta("work_inputs")
        .expect("work_inputs survives reopen");
    assert_eq!(
        inputs.default_ttl_seconds,
        Some(7 * 24 * 60 * 60),
        "work_inputs must gain RETAIN 7 DAYS on reconcile, an upgraded root must self-bound \
         its input copies exactly like a freshly created root does"
    );

    // The reconcile's whole point: a root that gains its first HISTORY
    // CURRENT ONLY declaration this way must self-bound, exactly like a
    // freshly created root does -- not stay silently unmaintained until
    // some other table happens to arm the loop.
    let status = reopened.maintenance_status();
    assert!(
        status.running,
        "reconciled HISTORY CURRENT ONLY tables must arm the maintenance loop: {status:?}"
    );
    assert!(
        status.currency_compaction_enabled,
        "reconcile must make the root compaction-eligible: {status:?}"
    );
}
