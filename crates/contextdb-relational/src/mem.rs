use crate::store::RelationalStore;
use contextdb_core::{schema, *};
use contextdb_tx::{TxManager, WriteSetApplicator};
use std::collections::HashMap;
use std::sync::Arc;

pub struct MemRelationalExecutor<S: WriteSetApplicator> {
    store: Arc<RelationalStore>,
    tx_mgr: Arc<TxManager<S>>,
}

impl<S: WriteSetApplicator> MemRelationalExecutor<S> {
    pub fn new(store: Arc<RelationalStore>, tx_mgr: Arc<TxManager<S>>) -> Self {
        Self { store, tx_mgr }
    }

    pub fn scan_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        let tables = self.store.tables.read();
        let rows = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        let mut result: Vec<VersionedRow> = rows
            .iter()
            .filter(|r| r.visible_at(snapshot))
            .cloned()
            .collect();

        if let Some(tx_id) = tx {
            let _ = self.tx_mgr.with_write_set(tx_id, |ws| {
                for (t, row) in &ws.relational_inserts {
                    if t == table {
                        result.push(row.clone());
                    }
                }
            });
        }

        Ok(result)
    }

    pub fn scan_filter_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>> {
        let all = self.scan_with_tx(tx, table, snapshot)?;
        Ok(all.into_iter().filter(|r| predicate(r)).collect())
    }

    pub fn point_lookup_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        let all = self.scan_with_tx(tx, table, snapshot)?;
        Ok(all.into_iter().find(|r| r.values.get(col) == Some(value)))
    }

    fn validate_state_transition(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if table != "invalidations" {
            return Ok(());
        }

        let new_status = match values.get("status") {
            Some(Value::Text(s)) => s.as_str(),
            _ => return Ok(()),
        };

        let id = match values.get("id") {
            Some(Value::Uuid(id)) => *id,
            _ => return Ok(()),
        };

        if let Some(existing) =
            self.point_lookup_with_tx(Some(tx), "invalidations", "id", &Value::Uuid(id), snapshot)?
        {
            let old_status = existing
                .values
                .get("status")
                .and_then(Value::as_text)
                .unwrap_or("");
            if !schema::is_valid_transition(old_status, new_status) {
                return Err(Error::InvalidStateTransition(format!(
                    "{} -> {}",
                    old_status, new_status
                )));
            }
        }

        Ok(())
    }

    pub fn insert_with_tx(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<RowId> {
        self.validate_state_transition(tx, table, &values, snapshot)?;

        let row_id = self.store.new_row_id();
        let row = VersionedRow {
            row_id,
            values,
            created_tx: tx,
            deleted_tx: None,
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts.push((table.to_string(), row));
        })?;

        Ok(row_id)
    }

    pub fn upsert_with_tx(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<UpsertResult> {
        if schema::is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        self.validate_state_transition(tx, table, &values, snapshot)?;

        let conflict_val = values
            .get(conflict_col)
            .ok_or_else(|| Error::Other("conflict column not in values".to_string()))?
            .clone();

        let existing =
            self.point_lookup_with_tx(Some(tx), table, conflict_col, &conflict_val, snapshot)?;

        match existing {
            None => {
                self.insert_with_tx(tx, table, values, snapshot)?;
                Ok(UpsertResult::Inserted)
            }
            Some(existing_row) => {
                let changed = values
                    .iter()
                    .any(|(k, v)| existing_row.values.get(k) != Some(v));
                if !changed {
                    return Ok(UpsertResult::NoOp);
                }

                self.delete(tx, table, existing_row.row_id)?;
                self.insert_with_tx(tx, table, values, snapshot)?;
                Ok(UpsertResult::Updated)
            }
        }
    }
}

impl<S: WriteSetApplicator> RelationalExecutor for MemRelationalExecutor<S> {
    fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>> {
        self.scan_with_tx(None, table, snapshot)
    }

    fn scan_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>> {
        self.scan_filter_with_tx(None, table, snapshot, predicate)
    }

    fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        self.point_lookup_with_tx(None, table, col, value, snapshot)
    }

    fn insert(&self, tx: TxId, table: &str, values: HashMap<ColName, Value>) -> Result<RowId> {
        let snapshot = self.tx_mgr.snapshot();
        self.insert_with_tx(tx, table, values, snapshot)
    }

    fn upsert(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<UpsertResult> {
        self.upsert_with_tx(tx, table, conflict_col, values, snapshot)
    }

    fn delete(&self, tx: TxId, table: &str, row_id: RowId) -> Result<()> {
        if schema::is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_deletes.push((table.to_string(), row_id, tx));
        })?;

        Ok(())
    }
}
