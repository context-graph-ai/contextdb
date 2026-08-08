//! `work_node_contacts` is `contextdb-server`'s table
//! (`work_ledger::CREATE_WORK_NODE_CONTACTS`), but `contextdb-engine`'s
//! reserved-name shape door (`engine_owned_reserved_table_create_ddl` in
//! `contextdb-engine/src/executor.rs`) cannot import that constant --
//! `contextdb-engine` sits BELOW `contextdb-server` in the dependency graph,
//! so depending on it here would be a layering inversion. The door instead
//! carries a hand-duplicated copy
//! (`contextdb_engine::executor::ENGINE_OWNED_WORK_NODE_CONTACTS_CREATE_DDL`),
//! with a doc comment asking a future editor to keep the two in agreement by
//! hand -- a promise nothing enforced until this test.
//!
//! This pins the promise mechanically: if either DDL string ever changes
//! without the other, this test fails loudly instead of the shape door
//! silently drifting from the table it actually governs (a mismatch here
//! would reintroduce exactly the "which shape is canonical?" confusion the
//! shape-door work across several earlier revisions exists to close).

#[test]
fn engine_owned_shape_door_copy_matches_the_real_installer_ddl() {
    assert_eq!(
        contextdb_engine::executor::ENGINE_OWNED_WORK_NODE_CONTACTS_CREATE_DDL,
        contextdb_server::work_ledger::CREATE_WORK_NODE_CONTACTS,
        "contextdb-engine's hand-duplicated copy of work_node_contacts' CREATE TABLE text \
         (executor::ENGINE_OWNED_WORK_NODE_CONTACTS_CREATE_DDL) has drifted from the real \
         installer DDL (contextdb_server::work_ledger::CREATE_WORK_NODE_CONTACTS) -- the \
         reserved-name shape door is now judging every work_node_contacts CREATE/ALTER against \
         a shape that table's own installer no longer creates. Update the engine-side copy to \
         match."
    );
}
