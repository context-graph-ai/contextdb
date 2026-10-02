//! Maintenance borrows one committed frontier without collecting row payloads.
use super::*;
use crate::persistence::writer_adapters::WriterRelationalSource;
use contextdb_relational::source::RelationalReadSource;
use parking_lot::RwLockReadGuard;
use std::ops::Bound;

pub(super) struct MaintenanceFrontier<'a> {
    memory: Option<RwLockReadGuard<'a, HashMap<String, Vec<VersionedRow>>>>,
    source: Option<WriterRelationalSource>,
    store: &'a RelationalStore,
}

impl<'a> MaintenanceFrontier<'a> {
    /// Mutating maintenance holds the existing commit guard to keep this
    /// canonical frontier and its metadata coherent. The read-only pending
    /// probe may retain the immutable published frontier without that guard.
    pub(super) fn capture(
        store: &'a RelationalStore,
        manager: &TransactionManager<DynStore>,
    ) -> Result<Self> {
        let root = writer_read::durable_root(manager.capture_published_read().handle)?;
        let source = root.map(WriterRelationalSource::new).transpose()?;
        if store.uses_disk_backed_committed_bodies() && source.is_none() {
            return Err(Error::Other(
                "maintenance has no committed durable root".into(),
            ));
        }
        let memory = source.is_none().then(|| store.tables.read());
        Ok(Self {
            memory,
            source,
            store,
        })
    }

    pub(super) fn cleanup_rotation(&self) -> Result<Option<[u8; 32]>> {
        self.source
            .as_ref()
            .map_or(Ok(None), WriterRelationalSource::cleanup_rotation)
    }

    pub(super) fn is_durable(&self) -> bool {
        self.source.is_some()
    }

    pub(super) fn has_table(&self, table: &str) -> Result<bool> {
        if let Some(source) = &self.source {
            return match source.validate_table(table) {
                Ok(()) => Ok(true),
                Err(Error::TableNotFound(_)) => Ok(false),
                Err(error) => Err(error),
            };
        }
        Ok(self
            .memory
            .as_ref()
            .is_some_and(|tables| tables.contains_key(table)))
    }

    pub(super) fn visit(
        &self,
        table: &str,
        mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        if let Some(source) = &self.source {
            let mut cursor = source.row_cursor(table, Bound::Unbounded, Bound::Unbounded, false)?;
            while let Some(window) = cursor.next_window()? {
                if !visit(window.value())? {
                    break;
                }
            }
        } else if let Some(rows) = self.memory.as_ref().and_then(|tables| tables.get(table)) {
            for row in rows {
                if !visit(row)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(super) fn visit_row(
        &self,
        table: &str,
        row_id: RowId,
        mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        if let Some(source) = &self.source {
            let mut cursor = source.row_id_cursor(table, row_id, source.snapshot())?;
            while let Some(window) = cursor.next_window()? {
                if !visit(window.value())? {
                    break;
                }
            }
        } else if let Some(rows) = self.memory.as_ref().and_then(|tables| tables.get(table)) {
            for row in rows.iter().filter(|row| row.row_id == row_id) {
                if !visit(row)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(super) fn sync_source_kind(
        &self,
        table: &str,
        row_id: RowId,
    ) -> Result<Option<contextdb_relational::store::SyncSourceKind>> {
        match &self.source {
            Some(source) => Ok(source.sync_source(table, row_id)?.map(|(_, kind)| kind)),
            None => Ok(self.store.sync_source_kind(table, row_id)),
        }
    }

    /// Each physical keeper/protection record spends one unit of the shared
    /// cycle allowance. Positions become durable beside the selected removals.
    pub(super) fn select_durable_cleanup(
        &self,
        table: &str,
        meta: &TableMeta,
        remaining: &mut usize,
        removal_allowance: &mut usize,
        mut select: impl FnMut(&VersionedRow, Lsn) -> Result<bool>,
    ) -> Result<crate::persistence::writer_cleanup_progress::CurrencyCleanupProgress> {
        use crate::persistence::writer_cleanup_progress::CurrencyCleanupGroup;
        let source = self.source.as_ref().expect("durable cleanup source");
        let declaration = RedbPersistence::currency_cleanup_declaration(meta)?;
        let mut state = source.cleanup_progress(table, declaration)?;
        let mut refreshed_row = None;
        while *remaining > 0 && *removal_allowance > 0 {
            if state.group.is_none() {
                let lower = state.after_group.map(|row| {
                    let mut key = [255; 24];
                    key[..8].copy_from_slice(&row.0.to_be_bytes());
                    key
                });
                let mut cursor = source.row_cursor(
                    table,
                    lower
                        .as_ref()
                        .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice())),
                    Bound::Unbounded,
                    false,
                )?;
                let Some(window) = cursor.next_window()? else {
                    state.after_group = None;
                    state.through = source.lsn();
                    break;
                };
                *remaining -= 1;
                #[cfg(feature = "test-seams")]
                resident_history_test_seams::note(|work| work.cleanup_versions_examined += 1);
                state.group = Some(CurrencyCleanupGroup {
                    row: window.value().row_id,
                    keeper: None,
                    prune_after: None,
                });
                continue;
            }
            let group = state.group.as_mut().expect("cleanup group selected");
            if refreshed_row != Some(group.row) {
                if *remaining < 2 {
                    break;
                }
                // One authenticated head record and one physical-body existence
                // probe replace an unbounded keeper discovery scan. Rebinding
                // on resume protects the latest version after intervening writes.
                group.keeper = Some(source.row_head(table, group.row)?);
                refreshed_row = Some(group.row);
                *remaining -= 2;
                #[cfg(feature = "test-seams")]
                resident_history_test_seams::note(|work| work.cleanup_versions_examined += 2);
            }
            if *remaining == 0 {
                break;
            }
            let mut first = [0; 24];
            first[..8].copy_from_slice(&group.row.0.to_be_bytes());
            let mut last = [255; 24];
            last[..8].copy_from_slice(&group.row.0.to_be_bytes());
            let mut cursor = source.row_cursor(
                table,
                group
                    .prune_after
                    .as_ref()
                    .map_or(Bound::Included(first.as_slice()), |key| {
                        Bound::Excluded(key.as_slice())
                    }),
                Bound::Included(last.as_slice()),
                false,
            )?;
            let Some(window) = cursor.next_window()? else {
                state.after_group = Some(group.row);
                state.group = None;
                continue;
            };
            *remaining -= 1;
            #[cfg(feature = "test-seams")]
            resident_history_test_seams::note(|work| work.cleanup_versions_examined += 1);
            let row = window.value();
            let key: [u8; 24] =
                window
                    .position_key()
                    .try_into()
                    .map_err(|_| Error::StoreCorrupted {
                        path: table.into(),
                        reason: "cleanup row position has the wrong length".into(),
                    })?;
            if row.lsn <= state.through && select(row, group.keeper.expect("cleanup keeper").0)? {
                *removal_allowance -= 1;
            }
            group.prune_after = Some(key);
        }
        Ok(state)
    }
}
