//! Maintenance borrows one committed frontier without collecting row payloads.
use super::*;
use crate::persistence::writer_adapters::WriterRelationalSource;
use contextdb_relational::source::RelationalReadSource;
use parking_lot::RwLockReadGuard;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

pub(super) struct MaintenanceFrontier<'a> {
    memory: Option<RwLockReadGuard<'a, HashMap<String, Vec<VersionedRow>>>>,
    source: Option<WriterRelationalSource>,
    store: &'a RelationalStore,
    root: Option<Arc<crate::persistence::WriterSnapshot>>,
    retention_windows: HashMap<String, RetentionWindow>,
    retention_progress: Vec<(String, Vec<u8>)>,
    retention_scope: bool,
    /// Queued identities of an expiring cohort that spans windows: rows the
    /// previous pass found protected only by an equally expired row outside
    /// its window. They join this pass's selection so the joint fixed point
    /// sees the whole cohort at once.
    retention_cohort: HashMap<String, Vec<[u8; 24]>>,
    retention_cohort_phase: Option<String>,
    cohort_discovered: parking_lot::Mutex<Vec<(String, [u8; 24])>>,
    cohort_clock: parking_lot::Mutex<Option<(Wallclock, Lsn)>>,
}

/// Rows of one cross-window expiry cohort that a pass carries forward. The
/// queue is durable beside the window positions; a cohort larger than this
/// keeps its members protected (never wrongly removed) until it shrinks.
const RETENTION_COHORT_CAPACITY: usize = 128;

pub(super) fn retention_physical_key(row: &VersionedRow) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..8].copy_from_slice(&row.row_id.0.to_be_bytes());
    key[8..16].copy_from_slice(&row.created_tx.0.to_be_bytes());
    key[16..].copy_from_slice(&row.lsn.0.to_be_bytes());
    key
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
        let source = root.clone().map(WriterRelationalSource::new).transpose()?;
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
            root,
            retention_windows: HashMap::new(),
            retention_progress: Vec::new(),
            retention_scope: false,
            retention_cohort: HashMap::new(),
            retention_cohort_phase: None,
            cohort_discovered: parking_lot::Mutex::new(Vec::new()),
            cohort_clock: parking_lot::Mutex::new(None),
        })
    }

    /// The repeated candidate, protection and mutation walks share one finite
    /// physical window. Only its key bounds survive between walks; row bodies
    /// are released with each admitted cursor window.
    pub(super) fn retention_batch(
        mut self,
        metas: &HashMap<String, TableMeta>,
        phase: &str,
        workspace: &mut crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<Self> {
        self.retention_scope = true;
        let (Some(root), Some(source)) = (&self.root, &self.source) else {
            return Ok(self);
        };
        // Dependent cohorts (a retained child referencing a retained parent)
        // use the same finite windows. Safety does not depend on the window:
        // FK protection probes the complete visible child frontier
        // (`visit_references`), so a parent whose live child lies outside this
        // window stays protected and is reconsidered when its own window comes
        // round again, after the child has expired. Within one window the
        // joint fixed point still lets a parent and its child expire together.
        workspace.try_grow_for(
            1024usize.saturating_add(
                metas
                    .iter()
                    .filter(|(_, meta)| meta.default_ttl_seconds.is_some())
                    .map(|(name, _)| 512 + name.len() * 4)
                    .sum::<usize>(),
            ),
            "retention",
            "selection_continuation",
            "Raise MEMORY_LIMIT to admit retention selection controls.",
        )?;
        let cohort_key = format!("retention_cohort/{phase}");
        if let Some(queued) = root.config_point::<Vec<(String, [u8; 24])>>(&cohort_key)? {
            workspace.try_grow_for(
                queued
                    .value
                    .iter()
                    .map(|(table, _)| 64 + table.len())
                    .sum::<usize>(),
                "retention",
                "selection_continuation",
                "Raise MEMORY_LIMIT to admit retention selection controls.",
            )?;
            for (table, key) in &queued.value {
                if metas
                    .get(table)
                    .is_some_and(|meta| meta.default_ttl_seconds.is_some())
                {
                    self.retention_cohort
                        .entry(table.clone())
                        .or_default()
                        .push(*key);
                }
            }
        }
        self.retention_cohort_phase = Some(cohort_key);
        let mut tables = metas
            .iter()
            .filter(|(_, meta)| meta.default_ttl_seconds.is_some())
            .collect::<Vec<_>>();
        tables.sort_unstable_by_key(|(name, _)| *name);
        let rotation_key = format!("retention_selection/{phase}/");
        if let Some(after) = root.config_point::<String>(&rotation_key)? {
            let start = tables.partition_point(|(name, _)| name.as_str() <= after.value.as_str());
            if start < tables.len() {
                tables.rotate_left(start);
            }
        }
        let mut rotation = None;
        // Expiry only stamps visibility and can select a wider independent
        // window than physical reclamation. Referenced parents retain the
        // smaller probe budget for their FK checks.
        let has_referenced_retained_table = metas.values().any(|child| {
            child
                .columns
                .iter()
                .filter_map(|column| column.references.as_ref())
                .any(|reference| {
                    metas
                        .get(&reference.table)
                        .is_some_and(|parent| parent.default_ttl_seconds.is_some())
                })
                || child.composite_foreign_keys.iter().any(|reference| {
                    metas
                        .get(&reference.parent_table)
                        .is_some_and(|parent| parent.default_ttl_seconds.is_some())
                })
        });
        let mut remaining = if phase == "expiry" && !has_referenced_retained_table {
            128usize
        } else {
            32usize
        };
        for (table, meta) in tables {
            let key = format!("retention_selection/{phase}/{table}");
            let declaration = RedbPersistence::currency_cleanup_declaration(meta)?;
            let saved = root.config_point::<RetentionPosition>(&key)?;
            let saved = saved
                .as_ref()
                .filter(|saved| saved.value.declaration == declaration);
            let after = saved.and_then(|saved| saved.value.after);
            let mut window = RetentionWindow {
                after,
                through: None,
            };
            if remaining > 0 {
                rotation = Some(table.clone());
                // Freeze this sweep's last physical identity. New appended
                // identities join the next sweep instead of extending this
                // one indefinitely. Read only one admitted reverse window.
                let end = if let Some(saved) = saved.filter(|saved| saved.value.after.is_some()) {
                    saved.value.end
                } else {
                    let mut tail =
                        source.row_cursor(table, Bound::Unbounded, Bound::Unbounded, true)?;
                    let tail = tail.next_window()?;
                    #[cfg(feature = "test-seams")]
                    if tail.is_some() {
                        resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                    }
                    tail.map(|row| {
                        row.position_key()
                            .try_into()
                            .map_err(|_| Error::StoreCorrupted {
                                path: table.clone(),
                                reason: "retention end position has the wrong length".into(),
                            })
                    })
                    .transpose()?
                };
                let mut cursor = source.row_cursor(
                    table,
                    after
                        .as_ref()
                        .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice())),
                    end.as_ref().map_or(Bound::Unbounded, |key: &[u8; 24]| {
                        Bound::Included(key.as_slice())
                    }),
                    false,
                )?;
                let mut next = after;
                while end.is_some() && remaining > 0 {
                    let Some(row) = cursor.next_window()? else {
                        next = None;
                        break;
                    };
                    let physical: [u8; 24] =
                        row.position_key()
                            .try_into()
                            .map_err(|_| Error::StoreCorrupted {
                                path: table.clone(),
                                reason: "retention position has the wrong length".into(),
                            })?;
                    window.through = Some(physical);
                    next = Some(physical);
                    remaining -= 1;
                    #[cfg(feature = "test-seams")]
                    resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                }
                self.retention_progress.push((
                    key,
                    RedbPersistence::encode_config_value(&RetentionPosition {
                        declaration,
                        after: next,
                        end: next.and(end),
                    })?,
                ));
            }
            self.retention_windows.insert(table.clone(), window);
        }
        if let Some(rotation) = rotation {
            self.retention_progress.push((
                rotation_key,
                RedbPersistence::encode_config_value(&rotation)?,
            ));
        }
        Ok(self)
    }

    pub(super) fn retention_progress(&self) -> &[(String, Vec<u8>)] {
        &self.retention_progress
    }

    /// The clock the candidate selection judged expiry with; a protecting row
    /// outside the window is queued only when it is expired by the same rule.
    pub(super) fn set_cohort_clock(&self, now: Wallclock, sync_watermark: Lsn) {
        *self.cohort_clock.lock() = Some((now, sync_watermark));
    }

    /// Called when `parent` (selected) is held back by a live referencing
    /// `child` that this pass did not select. When the child is itself
    /// expired, both are queued so a later pass judges them together.
    pub(super) fn note_cohort_blocker(
        &self,
        metas: &HashMap<String, TableMeta>,
        parent_table: &str,
        parent: &VersionedRow,
        child_table: &str,
        child: &VersionedRow,
    ) -> Result<()> {
        if self.retention_cohort_phase.is_none() || self.source.is_none() {
            return Ok(());
        }
        let Some((now, watermark)) = *self.cohort_clock.lock() else {
            return Ok(());
        };
        let Some(meta) = metas
            .get(child_table)
            .filter(|meta| meta.default_ttl_seconds.is_some())
        else {
            return Ok(());
        };
        let arrived_by_sync = matches!(
            self.sync_source_kind(child_table, child.row_id)?,
            Some(contextdb_relational::store::SyncSourceKind::Pulled)
        );
        if !row_is_prunable(child, meta, now, watermark, arrived_by_sync) {
            return Ok(());
        }
        let mut discovered = self.cohort_discovered.lock();
        for (table, key) in [
            (parent_table, retention_physical_key(parent)),
            (child_table, retention_physical_key(child)),
        ] {
            if discovered.len() >= RETENTION_COHORT_CAPACITY {
                break;
            }
            if !discovered
                .iter()
                .any(|(known, known_key)| known == table && *known_key == key)
            {
                discovered.push((table.to_owned(), key));
            }
        }
        Ok(())
    }

    /// The durable cohort queue for the next pass: queued members this pass
    /// still had to protect, then newly discovered cross-window blockers.
    /// Members that expired, were pruned, or stopped being candidates leave.
    pub(super) fn cohort_progress(
        &self,
        protected: impl Fn(&str, &[u8; 24]) -> bool,
    ) -> Result<Option<(String, Vec<u8>)>> {
        let Some(key) = &self.retention_cohort_phase else {
            return Ok(None);
        };
        if self.source.is_none() {
            return Ok(None);
        }
        let mut next: Vec<(String, [u8; 24])> = Vec::new();
        let mut tables = self.retention_cohort.iter().collect::<Vec<_>>();
        tables.sort_unstable_by_key(|(table, _)| *table);
        for (table, keys) in tables {
            for queued in keys {
                if next.len() < RETENTION_COHORT_CAPACITY && protected(table, queued) {
                    next.push((table.clone(), *queued));
                }
            }
        }
        for (table, discovered) in self.cohort_discovered.lock().iter() {
            if next.len() >= RETENTION_COHORT_CAPACITY {
                break;
            }
            if !next
                .iter()
                .any(|(known, known_key)| known == table && known_key == discovered)
            {
                next.push((table.clone(), *discovered));
            }
        }
        if next.is_empty() && self.retention_cohort.is_empty() {
            return Ok(None);
        }
        Ok(Some((
            key.clone(),
            RedbPersistence::encode_config_value(&next)?,
        )))
    }

    pub(super) fn no_future_stamps(&self, table: &str, horizon: u64) -> Result<bool> {
        match &self.root {
            Some(root) => Ok(RedbPersistence::retention_clock_upper_in_read(root, table)?
                .is_some_and(|stamp| stamp.0 <= horizon)),
            None => Ok(false),
        }
    }

    pub(super) fn visit_all(
        &self,
        table: &str,
        visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        self.visit_selected(table, false, visit)
    }

    pub(super) fn cleanup_rotation(&self) -> Result<Option<[u8; 32]>> {
        self.source
            .as_ref()
            .map_or(Ok(None), WriterRelationalSource::cleanup_rotation)
    }

    /// Once a finite cleanup has started, its remaining windows must run even
    /// when earlier windows took the superseded population below the start gate.
    pub(super) fn cleanup_in_progress(&self, table: &str, meta: &TableMeta) -> Result<bool> {
        let Some(source) = &self.source else {
            return Ok(false);
        };
        let declaration = RedbPersistence::currency_cleanup_declaration(meta)?;
        let progress = source.cleanup_progress(table, declaration)?;
        Ok(progress.group.is_some() || progress.after_group.is_some())
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
        visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        self.visit_selected(table, true, visit)
    }

    /// FK protection probes the complete captured child frontier, independently
    /// of the candidate window. Current-only postings avoid retained history.
    pub(super) fn visit_references(
        &self,
        table: &str,
        columns: &[String],
        values: &[&Value],
        mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        if let Some(source) = &self.source {
            let metas = self.store.table_meta.read();
            let meta = metas
                .get(table)
                .ok_or_else(|| Error::TableNotFound(table.into()))?;
            if let Some(index) = meta.indexes.iter().find(|index| {
                index.columns.len() == columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(columns)
                        .all(|((name, _), column)| name == column)
            }) {
                let _key_memory = RedbPersistence::admit_retention_probe_key(
                    self.root.as_ref().expect("durable reference root"),
                    128usize.saturating_add(
                        values
                            .iter()
                            .map(|value| held::value(value).saturating_mul(2))
                            .sum::<usize>(),
                    ),
                )?;
                let key = index
                    .columns
                    .iter()
                    .zip(values)
                    .map(|((_, direction), value)| match direction {
                        SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc((*value).clone())),
                        SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc((*value).clone())),
                    })
                    .collect::<Vec<_>>();
                let mut cursor = source.exact_index_cursor(table, &index.name, true, &key)?;
                while let Some(posting) = cursor.next_window()? {
                    #[cfg(feature = "test-seams")]
                    resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                    if let Some(row) = contextdb_relational::source::visible_row(
                        source,
                        table,
                        posting.value().entry.row_id,
                        source.snapshot(),
                    )? {
                        #[cfg(feature = "test-seams")]
                        resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                        if !visit(row.value())? {
                            break;
                        }
                    }
                }
                return Ok(());
            }
        }
        self.visit_selected(table, false, |row| {
            #[cfg(feature = "test-seams")]
            if self.is_durable() {
                resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
            }
            visit(row)
        })
    }

    fn visit_selected(
        &self,
        table: &str,
        selected_only: bool,
        mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
    ) -> Result<()> {
        if let Some(source) = &self.source {
            let selected = if selected_only {
                self.retention_windows.get(table)
            } else {
                None
            };
            let in_window = |key: &[u8; 24]| {
                selected.is_some_and(|window| {
                    window.through.is_some_and(|through| {
                        *key <= through && window.after.is_none_or(|after| *key > after)
                    })
                })
            };
            if selected_only && let Some(queued) = self.retention_cohort.get(table) {
                for key in queued.iter().filter(|key| !in_window(key)) {
                    if let Some(window) = source.exact_row(table, key)?
                        && !visit(window.value())?
                    {
                        return Ok(());
                    }
                }
            }
            if selected.is_some_and(|window| window.through.is_none()) {
                return Ok(());
            }
            let mut cursor = source.row_cursor(
                table,
                selected
                    .and_then(|window| window.after.as_ref())
                    .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice())),
                selected
                    .and_then(|window| window.through.as_ref())
                    .map_or(Bound::Unbounded, |key| Bound::Included(key.as_slice())),
                false,
            )?;
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
                #[cfg(feature = "test-seams")]
                if self.retention_scope {
                    resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                }
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

#[derive(Serialize, Deserialize)]
struct RetentionPosition {
    declaration: [u8; 32],
    after: Option<[u8; 24]>,
    end: Option<[u8; 24]>,
}

struct RetentionWindow {
    after: Option<[u8; 24]>,
    through: Option<[u8; 24]>,
}
