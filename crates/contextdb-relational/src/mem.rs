use crate::store::{RelationalStore, index_key_from_values};
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator, row_matches_delete_predicates};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct MemRelationalExecutor<S: WriteSetApplicator> {
    store: Arc<RelationalStore>,
    tx_mgr: Arc<TransactionManager<S>>,
}

impl<S: WriteSetApplicator> MemRelationalExecutor<S> {
    pub fn new(store: Arc<RelationalStore>, tx_mgr: Arc<TransactionManager<S>>) -> Self {
        Self { store, tx_mgr }
    }

    fn ensure_table_exists(&self, table: &str) -> Result<()> {
        if self.store.table_meta.read().contains_key(table) {
            Ok(())
        } else {
            Err(Error::TableNotFound(table.to_string()))
        }
    }

    pub fn scan_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        let mut result: Vec<VersionedRow> = {
            let tables = self.store.tables.read();
            let rows = tables
                .get(table)
                .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
            self.store.bump_scan_rows_touched(rows.len() as u64);
            rows.iter()
                .filter(|r| r.visible_at(snapshot))
                .cloned()
                .collect()
        };

        if let Some(tx_id) = tx {
            let _ = self.tx_mgr.with_write_set(tx_id, |ws| {
                let committed_row_ids: std::collections::HashSet<RowId> =
                    result.iter().map(|row| row.row_id).collect();
                let deleted_row_ids: std::collections::HashSet<RowId> = ws
                    .relational_deletes
                    .iter()
                    .filter(|(t, _, _)| t == table)
                    .map(|(_, row_id, _)| *row_id)
                    .collect();
                result.retain(|row| {
                    !deleted_row_ids.contains(&row.row_id)
                        && !row_matches_delete_predicates(
                            &ws.relational_delete_predicates,
                            table,
                            row,
                        )
                });
                let mut seen_inserts = std::collections::HashSet::new();
                let mut inserts = ws
                    .relational_inserts
                    .iter()
                    .rev()
                    .filter(|(t, row)| {
                        t == table
                            && seen_inserts.insert(row.row_id)
                            && (!deleted_row_ids.contains(&row.row_id)
                                || committed_row_ids.contains(&row.row_id))
                    })
                    .map(|(_, row)| row.clone())
                    .collect::<Vec<_>>();
                inserts.reverse();
                for row in inserts {
                    result.push(row);
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
        self.ensure_table_exists(table)?;

        let (deleted_row_ids, delete_predicates) = match tx {
            Some(tx_id) => self.tx_mgr.with_write_set(tx_id, |ws| {
                let deleted_row_ids = ws
                    .relational_deletes
                    .iter()
                    .filter(|(t, _, _)| t == table)
                    .map(|(_, row_id, _)| *row_id)
                    .collect::<HashSet<_>>();
                let delete_predicates = ws
                    .relational_delete_predicates
                    .iter()
                    .filter(|predicate| predicate.table == table)
                    .cloned()
                    .collect::<Vec<_>>();
                (deleted_row_ids, delete_predicates)
            })?,
            None => (HashSet::new(), Vec::new()),
        };

        let committed_index_checked = match self.indexed_committed_point_lookup(
            table,
            col,
            value,
            snapshot,
            &deleted_row_ids,
        ) {
            Some(Some(indexed)) => {
                if !row_matches_delete_predicates(&delete_predicates, table, &indexed) {
                    return Ok(Some(indexed));
                }
                true
            }
            Some(None) => true,
            None => false,
        };

        if let Some(tx_id) = tx {
            let committed_deleted_row_ids = deleted_row_ids
                .iter()
                .copied()
                .filter(|row_id| self.store.row_by_id(table, *row_id, snapshot).is_some())
                .collect::<HashSet<_>>();
            let staged = self.tx_mgr.with_write_set(tx_id, |ws| {
                let mut seen_inserts = HashSet::new();
                ws.relational_inserts
                    .iter()
                    .rev()
                    .filter(|(insert_table, row)| {
                        insert_table == table
                            && seen_inserts.insert(row.row_id)
                            && (!deleted_row_ids.contains(&row.row_id)
                                || committed_deleted_row_ids.contains(&row.row_id))
                    })
                    .find(|(_, row)| row.values.get(col) == Some(value))
                    .map(|(_, row)| row.clone())
            })?;
            if staged.is_some() {
                return Ok(staged);
            }
        }
        if committed_index_checked {
            return Ok(None);
        }

        let all = self.scan_with_tx(tx, table, snapshot)?;
        Ok(all.into_iter().find(|r| r.values.get(col) == Some(value)))
    }

    fn indexed_committed_point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
        deleted_row_ids: &HashSet<RowId>,
    ) -> Option<Option<VersionedRow>> {
        let row_id = {
            let indexes = self.store.indexes.read();
            let storage = indexes.get(table).and_then(|table_indexes| {
                let pk_key = format!("__pk_{col}");
                let unique_key = format!("__unique_{col}");
                table_indexes
                    .get(&pk_key)
                    .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == col)
                    .or_else(|| {
                        table_indexes
                            .get(&unique_key)
                            .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == col)
                    })
                    .or_else(|| {
                        table_indexes
                            .values()
                            .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == col)
                    })
            })?;
            let key = index_key_from_values(&storage.columns[..1], std::slice::from_ref(value));
            storage.exact_postings(&key).and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| {
                        !deleted_row_ids.contains(&entry.row_id) && entry.visible_at(snapshot)
                    })
                    .map(|entry| entry.row_id)
            })
        };
        Some(row_id.and_then(|row_id| self.store.row_by_id(table, row_id, snapshot)))
    }

    fn validate_state_transition(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        let meta = self.store.table_meta.read();
        let Some(sm) = meta.get(table).and_then(|m| m.state_machine.as_ref()) else {
            return Ok(());
        };
        let col = &sm.column;

        let new_status = match values.get(col) {
            Some(Value::Text(s)) => s.as_str(),
            _ => return Ok(()),
        };

        let id = match values.get("id") {
            Some(v @ Value::Uuid(_)) => v.clone(),
            _ => return Ok(()),
        };

        if let Some(existing) = self.point_lookup_with_tx(Some(tx), table, "id", &id, snapshot)? {
            let old_status = existing
                .values
                .get(col)
                .and_then(Value::as_text)
                .unwrap_or("");
            if !self
                .store
                .validate_state_transition(table, col, old_status, new_status)
            {
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
        self.ensure_table_exists(table)?;
        self.validate_state_transition(tx, table, &values, snapshot)?;

        let row_id = self.store.new_row_id();
        let row = VersionedRow {
            row_id,
            values,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
            created_at: Some(contextdb_core::Wallclock::now()),
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts.push((table.to_string(), row));
        })?;

        Ok(row_id)
    }

    pub fn insert_with_row_id(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<RowId> {
        self.ensure_table_exists(table)?;
        self.validate_state_transition(tx, table, &values, snapshot)?;
        self.stage_insert_with_row_id(tx, table, row_id, values)
    }

    /// As [`Self::insert_with_row_id`], but stamping the row with a birth time
    /// the caller carries rather than reading the clock. The sync-apply path
    /// uses it so a replicated row keeps the age it was written with.
    pub fn insert_with_row_id_at(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        created_at: Wallclock,
    ) -> Result<RowId> {
        self.ensure_table_exists(table)?;
        self.validate_state_transition(tx, table, &values, snapshot)?;
        self.stage_insert_with_row_id_at(tx, table, row_id, values, created_at)
    }

    pub fn insert_with_row_id_assume_no_state_machine(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
    ) -> Result<RowId> {
        self.stage_insert_with_row_id(tx, table, row_id, values)
    }

    pub fn insert_with_row_id_assume_no_state_machine_at(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        created_at: Wallclock,
    ) -> Result<RowId> {
        self.stage_insert_with_row_id_at(tx, table, row_id, values, created_at)
    }

    fn stage_insert_with_row_id(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
    ) -> Result<RowId> {
        self.stage_insert_with_row_id_at(tx, table, row_id, values, Wallclock::now())
    }

    fn stage_insert_with_row_id_at(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        created_at: Wallclock,
    ) -> Result<RowId> {
        let row = VersionedRow {
            row_id,
            values,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
            created_at: Some(created_at),
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
        self.ensure_table_exists(table)?;
        if self.store.is_immutable(table) {
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
        self.ensure_table_exists(table)?;
        if self.store.is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        let committed_row_exists = self
            .store
            .row_by_id(table, row_id, SnapshotId::from_raw_wire(u64::MAX))
            .is_some();

        self.tx_mgr.with_write_set(tx, |ws| {
            let table_name = table.to_string();
            ws.relational_inserts
                .retain(|(t, row)| !(t == table && row.row_id == row_id));

            if committed_row_exists
                && !ws
                    .relational_deletes
                    .iter()
                    .any(|(t, deleted_row_id, _)| t == table && *deleted_row_id == row_id)
            {
                ws.relational_deletes.push((table_name, row_id, tx));
            }
        })?;

        Ok(())
    }
}
