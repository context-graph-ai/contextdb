//! The fabric peer-directory: a synced `node_id -> dialable-ticket` map,
//! populated at enrollment, so a production consumer holding only a job's
//! `submitter_node_id` can discover and dial the blob's holder.

use crate::Database;
use crate::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_core::{Result, Value};
use std::collections::HashMap;

// Node-keyed, latest-wins by its own documented contract (see
// `register_peer_ticket`'s upsert and `peer_directory_conflict_policy_entries`
// below) — a current-truth registry, so it declares its own version-cleanup
// eligibility in its own DDL rather than riding a hardcoded table-name list.
pub(crate) const CREATE_PEER_DIRECTORY: &str = "CREATE TABLE peer_directory (\
     node_id TEXT PRIMARY KEY, \
     ticket TEXT NOT NULL, \
     enrolled_at TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

/// Create the `peer_directory` table if absent. A root created before this
/// table declared its own HISTORY / SYNC CONFLICT clauses is reconciled via
/// the same `ALTER TABLE` an operator would type (KEEP LATEST before HISTORY
/// CURRENT ONLY, so the table is never observed keep-first while the
/// declaration is in force) — matching `install_work_ledger_schema`'s
/// reconciliation of `work_capabilities`.
pub fn install_peer_directory_schema(db: &Database) -> Result<()> {
    let read_memory = db.declared_reader_memory()?;
    install_peer_directory_schema_with_read_memory(db, read_memory)
}

/// Execute using the caller's existing operation read owner.
pub fn install_peer_directory_schema_with_read_memory(
    db: &Database,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    if !db.table_names().iter().any(|name| name == "peer_directory") {
        db.execute_with_read_memory(CREATE_PEER_DIRECTORY, &HashMap::new(), read_memory.clone())?;
        return Ok(());
    }
    let Some(meta) = db.table_meta("peer_directory") else {
        return Ok(());
    };
    if meta.conflict_policy != Some(contextdb_core::ConflictPolicy::KEEP_LATEST) {
        db.execute_with_read_memory(
            "ALTER TABLE peer_directory SET SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
            read_memory.clone(),
        )?;
    }
    if meta.history_policy != Some(contextdb_core::HistoryPolicy::CurrentOnly) {
        db.execute_with_read_memory(
            "ALTER TABLE peer_directory SET HISTORY CURRENT ONLY",
            &HashMap::new(),
            read_memory.clone(),
        )?;
    }
    Ok(())
}

/// The `peer_directory` table's conflict contract: node-id-keyed,
/// latest-wins — a node re-enrolling with a rotated ticket overwrites its own
/// row fabric-wide, mirroring the row-level upsert this module does locally.
pub(crate) fn peer_directory_conflict_policy_entries() -> [(&'static str, ConflictPolicy); 1] {
    [("peer_directory", ConflictPolicy::LatestWins)]
}

/// `peer_directory`'s entry above, in `SHOW SYNC_CONFLICT_POLICY` display
/// vocabulary -- reuses
/// [`crate::work_ledger::conflict_policy_display_word`] (the same
/// word-mapping the seven work-ledger tables' engine-owned rows use), so
/// `keep_first`/`keep_latest` are spelled identically everywhere `SHOW`
/// renders an engine-owned row, from one source of truth.
pub(crate) fn peer_directory_conflict_policy_display() -> Vec<(&'static str, &'static str)> {
    peer_directory_conflict_policy_entries()
        .into_iter()
        .map(|(table, policy)| {
            (
                table,
                crate::work_ledger::conflict_policy_display_word(policy),
            )
        })
        .collect()
}

/// Merge `peer_directory`'s policy over `policies`. Sync chokepoints call
/// this alongside [`crate::work_ledger::apply_work_ledger_policy_overrides`]
/// so the table syncs across the fabric like `work_capabilities`.
pub(crate) fn apply_peer_directory_policy_overrides(policies: &mut ConflictPolicies) {
    for (table, policy) in peer_directory_conflict_policy_entries() {
        policies.per_table.insert(table.to_string(), policy);
    }
}

/// Upsert `node_id -> ticket`: a node's row is keyed on its own `node_id`, so
/// a re-enrolling node (a rotated ticket) overwrites its prior row rather
/// than accumulating history.
pub fn register_peer_ticket(db: &Database, node_id: &str, ticket: &str, now_ms: i64) -> Result<()> {
    let read_memory = db.declared_reader_memory()?;
    register_peer_ticket_with_read_memory(db, node_id, ticket, now_ms, read_memory)
}

/// Execute using the caller's existing operation read owner.
pub fn register_peer_ticket_with_read_memory(
    db: &Database,
    node_id: &str,
    ticket: &str,
    now_ms: i64,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let mut params = HashMap::new();
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
    params.insert("ticket".to_string(), Value::Text(ticket.to_string()));
    params.insert("enrolled_at".to_string(), Value::Timestamp(now_ms));
    db.execute_with_read_memory(
        "INSERT INTO peer_directory (node_id, ticket, enrolled_at) \
         VALUES ($node_id, $ticket, $enrolled_at) \
         ON CONFLICT (node_id) DO UPDATE SET ticket = $ticket, enrolled_at = $enrolled_at",
        &params,
        read_memory.clone(),
    )?;
    Ok(())
}

/// Resolve `node_id` to its current dialable ticket, if enrolled.
pub fn lookup_peer_ticket(db: &Database, node_id: &str) -> Result<Option<String>> {
    let read_memory = db.declared_reader_memory()?;
    lookup_peer_ticket_with_read_memory(db, node_id, read_memory)
}

/// Execute using the caller's existing operation read owner.
pub fn lookup_peer_ticket_with_read_memory(
    db: &Database,
    node_id: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<String>> {
    let mut params = HashMap::new();
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
    let result = db.execute_with_read_memory(
        "SELECT ticket FROM peer_directory WHERE node_id = $node_id",
        &params,
        read_memory.clone(),
    )?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let ticket_idx = result
        .columns
        .iter()
        .position(|c| c == "ticket")
        .ok_or_else(|| {
            contextdb_core::Error::Other("peer_directory: missing column ticket".to_string())
        })?;
    match &row[ticket_idx] {
        Value::Text(s) => Ok(Some(s.clone())),
        other => Err(contextdb_core::Error::Other(format!(
            "peer_directory: ticket must be TEXT, found {other:?}"
        ))),
    }
}
