//! Engine-private hub bookkeeping used by authenticated sync orchestration.

use crate::Database;
use contextdb_core::{Error, Value};
use std::collections::HashMap;

/// The hub-local liveness table never leaves its authoritative hub.
pub(crate) const WORK_NODE_CONTACTS_TABLE: &str = "work_node_contacts";

const CREATE_WORK_NODE_CONTACTS: &str = "CREATE TABLE work_node_contacts (\
     node_id TEXT PRIMARY KEY, \
     last_contact_ms TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC OFF";

/// Preserve the server's established idempotent installation/reconciliation
/// path before recording the first authenticated contact. This table is
/// ContextDB-owned hub bookkeeping, so its declaration is authored here and
/// never comes from an application policy map.
pub(crate) fn ensure_node_contacts_schema(
    db: &Database,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(), Error> {
    if !db
        .table_names()
        .iter()
        .any(|name| name == WORK_NODE_CONTACTS_TABLE)
    {
        db.execute_with_read_memory(
            CREATE_WORK_NODE_CONTACTS,
            &HashMap::new(),
            read_memory.clone(),
        )?;
        return Ok(());
    }
    let Some(meta) = db.table_meta(WORK_NODE_CONTACTS_TABLE) else {
        return Ok(());
    };
    if meta.sync_direction != Some(contextdb_core::SyncDirection::None) {
        db.execute_with_read_memory(
            "ALTER TABLE work_node_contacts SET SYNC OFF",
            &HashMap::new(),
            read_memory.clone(),
        )?;
    }
    if meta.history_policy != Some(contextdb_core::HistoryPolicy::CurrentOnly) {
        db.execute_with_read_memory(
            "ALTER TABLE work_node_contacts SET HISTORY CURRENT ONLY",
            &HashMap::new(),
            read_memory.clone(),
        )?;
    }
    Ok(())
}

/// Record one authenticated exchange. Schema installation remains the
/// server-side work-ledger concern; a missing table is reported to the serve
/// loop and never turns an unauthenticated request into state.
pub(crate) fn record_node_contact(
    db: &Database,
    node_id: &str,
    now_ms: i64,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(), Error> {
    ensure_node_contacts_schema(db, read_memory.clone())?;
    db.execute_with_read_memory(
        "INSERT INTO work_node_contacts (node_id, last_contact_ms) VALUES ($node_id, $last_contact_ms) \
         ON CONFLICT (node_id) DO UPDATE SET last_contact_ms = $last_contact_ms",
        &HashMap::from([
            ("node_id".to_string(), Value::Text(node_id.to_string())),
            ("last_contact_ms".to_string(), Value::Timestamp(now_ms)),
        ]),
     read_memory.clone())?;
    Ok(())
}
