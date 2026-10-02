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

    /// Durable row keys group identities, so the keeper scan needs only one
    /// identity/LSN pair and never a directory or retained payload collection.
    pub(super) fn visit_durable_groups(
        &self,
        table: &str,
        mut visit: impl FnMut(RowId, Lsn) -> Result<()>,
    ) -> Result<()> {
        let mut group = None::<(RowId, Lsn)>;
        self.visit(table, |row| {
            #[cfg(feature = "test-seams")]
            resident_history_test_seams::note(|work| work.cleanup_versions_examined += 1);
            match group {
                Some((row_id, lsn)) if row_id == row.row_id => {
                    group = Some((row_id, lsn.max(row.lsn)));
                }
                Some((row_id, lsn)) => {
                    visit(row_id, lsn)?;
                    group = Some((row.row_id, row.lsn));
                }
                None => group = Some((row.row_id, row.lsn)),
            }
            Ok(true)
        })?;
        if let Some((row_id, lsn)) = group {
            visit(row_id, lsn)?;
        }
        Ok(())
    }
}
