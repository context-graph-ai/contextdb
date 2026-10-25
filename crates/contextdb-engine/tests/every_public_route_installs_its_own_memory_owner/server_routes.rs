//! Server work routes that run against a store.

use super::fixture::Store;
use super::{Row, call};
use contextdb_server::work_ledger::*;

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_server::work_ledger::install_node_contacts_schema",
            drive: || {
                let store = Store::open();
                call(|| install_node_contacts_schema(&store.db))
            },
        },
        Row {
            route: "contextdb_server::work_ledger::install_node_contacts_schema_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    install_node_contacts_schema_with_read_memory(
                        &store.db,
                        store
                            .db
                            .declared_reader_memory()
                            .map_err(|error| format!("{error:?}"))
                            .expect("fixture reader"),
                    )
                })
            },
        },
        Row {
            route: "contextdb_server::work_ledger::record_node_contact",
            drive: || {
                let store = Store::open();
                install_node_contacts_schema(&store.db).expect("fixture contacts schema");
                call(|| record_node_contact(&store.db, "node-a", 2_000))
            },
        },
        Row {
            route: "contextdb_server::work_ledger::record_node_contact_with_read_memory",
            drive: || {
                let store = Store::open();
                install_node_contacts_schema(&store.db).expect("fixture contacts schema");
                let memory = store.db.declared_reader_memory().expect("fixture reader");
                call(|| record_node_contact_with_read_memory(&store.db, memory, "node-a", 2_000))
            },
        },
    ]
}
