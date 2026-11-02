//! What a PURGE of a parent row also erases: every retained life of every
//! row whose declared following foreign key names it, transitively.
//!
//! Two routes find them, and neither scans a table:
//! - the child's covering index, read through its history table, which keeps
//!   the postings of deleted and replaced lives as well as current ones;
//! - the parent-keyed archive postings, for lives an earlier DROP or a body
//!   change archived. These are found from the parent alone, whatever the
//!   child table is declared as now, or whether it still exists.
use super::purge_followers::purge_followers_of;
use super::writer_read::next_window_with_work;
use super::*;

impl Database {
    /// The follower lives of `selections` not already in it. Each is charged
    /// to `credit` before it is held.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_followers(
        &self,
        selections: &[AuthoritativePurgeSelection],
        credit: &mut contextdb_core::read_memory::ReadCredit,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<AuthoritativePurgeSelection>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _scope = self.writer_read_scope(read_memory.clone())?;
        let Some(root) = self.writer_read_root()? else {
            return Ok(Vec::new());
        };
        let catalog = self
            .table_names()
            .into_iter()
            .filter_map(|name| self.table_meta(&name).map(|meta| (name, meta)))
            .collect::<HashMap<_, _>>();
        let mut known = HashSet::<(String, RowId)>::new();
        let mut queue = std::collections::VecDeque::<(String, NaturalKey)>::new();
        for selection in selections {
            credit.try_grow_for(
                selection
                    .table
                    .len()
                    .saturating_mul(2)
                    .saturating_add(selection.natural_key.held_bytes())
                    .saturating_add(128),
                "writer_source",
                "purge_follower_traversal",
                "Raise MEMORY_LIMIT to find what follows this purge.",
            )?;
            known.insert((selection.table.clone(), selection.local_row_id));
            queue.push_back((selection.table.clone(), selection.natural_key.clone()));
        }
        let mut found = Vec::new();
        while let Some((parent_table, parent_key)) = queue.pop_front() {
            let values = parent_key.key_values();
            let mut lives = Vec::<(String, u64, RowId, Option<NaturalKey>)>::new();
            for follower in purge_followers_of(
                catalog.iter().map(|(name, meta)| (name.as_str(), meta)),
                &parent_table,
            ) {
                let generation = self
                    .durable_lineage_table_generation(&follower.child_table, read_memory.clone())?;
                for row_id in self.follower_rows_in_index_history(
                    &root,
                    &follower.child_table,
                    &follower.child_columns,
                    &values,
                    credit,
                    read_memory.clone(),
                )? {
                    lives.push((follower.child_table.clone(), generation, row_id, None));
                }
            }
            for archived in root.archived_purge_followers(&parent_table, &values, credit)? {
                lives.push((
                    archived.child_table,
                    archived.table_generation,
                    archived.row_id,
                    Some(archived.natural_key),
                ));
            }
            for (table, generation, row_id, archived_key) in lives {
                if !known.insert((table.clone(), row_id)) {
                    continue;
                }
                let Some(selection) = self.pin_purge_follower_life(
                    &root,
                    &catalog,
                    &table,
                    generation,
                    row_id,
                    archived_key,
                    read_memory.clone(),
                )?
                else {
                    continue;
                };
                credit.try_grow_for(
                    std::mem::size_of::<AuthoritativePurgeSelection>()
                        .saturating_add(selection.table.len().saturating_mul(3))
                        .saturating_add(selection.natural_key.held_bytes().saturating_mul(2))
                        .saturating_add(selection.lineage_root.len())
                        .saturating_add(128),
                    "writer_source",
                    "purge_follower_selection",
                    "Raise MEMORY_LIMIT to erase what follows this purge.",
                )?;
                queue.push_back((selection.table.clone(), selection.natural_key.clone()));
                found.push(selection);
            }
        }
        Ok(found)
    }

    /// Every row id a covering index of `table` has ever held for these
    /// leading values: current, deleted and replaced lives alike.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn follower_rows_in_index_history(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        table: &str,
        columns: &[String],
        values: &[Value],
        credit: &mut contextdb_core::read_memory::ReadCredit,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Vec<RowId>> {
        use contextdb_relational::source::{RelationalIndexRange, RelationalReadSource};
        use std::ops::Bound;
        let meta = root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let Some(index) = meta.indexes.iter().find(|index| {
            index.columns.len() >= columns.len()
                && index
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|((have, _), want)| have == want)
        }) else {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "{table}({}) follows a purge but has no index leading with those columns",
                    columns.join(", ")
                ),
            });
        };
        let source = self.statement_relational_source(root.clone(), read_memory.clone())?;
        credit.try_grow_for(
            values
                .iter()
                .map(Value::estimated_bytes)
                .sum::<usize>()
                .saturating_mul(8)
                .saturating_add(256),
            "writer_source",
            "purge_follower_keys",
            "Raise MEMORY_LIMIT to find what follows this purge.",
        )?;
        let key = index_key_from_values(&index.columns[..columns.len()], values);
        let lower = source.index_position_key(false, &key, RowId(0), TxId(0))?;
        let mut cursor = source.index_cursor(
            table,
            &index.name,
            RelationalIndexRange {
                current: false,
                lower: Bound::Included(lower.as_bytes()),
                upper: Bound::Unbounded,
                reverse: false,
            },
        )?;
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        while let Some(posting) = next_window_with_work(&mut cursor, &mut || Ok::<(), Error>(()))? {
            let position = posting.position_key().to_vec();
            let posting = posting.value();
            if !posting.key.starts_with(&key) {
                break;
            }
            source.check_historical_posting(
                table,
                &index.name,
                &posting.key,
                &posting.entry,
                &position,
                &mut || Ok(()),
            )?;
            if seen.insert(posting.entry.row_id) {
                credit.try_grow_for(
                    std::mem::size_of::<RowId>().saturating_mul(4),
                    "writer_source",
                    "purge_follower_identities",
                    "Raise MEMORY_LIMIT to find what follows this purge.",
                )?;
                rows.push(posting.entry.row_id);
            }
        }
        Ok(rows)
    }

    /// Pin one follower life by its row id. A current visible life is
    /// resolved as any purge selection is. Any other retained life is pinned
    /// by the one durable witness that names its row. A life already purged is
    /// skipped; a retained life no witness names refuses the whole purge,
    /// because its erasure could not be certified.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "Each argument names one part of the life being pinned."
    )]
    fn pin_purge_follower_life(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        catalog: &HashMap<String, TableMeta>,
        table: &str,
        table_generation: u64,
        row_id: RowId,
        archived_key: Option<NaturalKey>,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Option<AuthoritativePurgeSelection>> {
        let natural_key = match archived_key {
            Some(key) => key,
            None => {
                let meta = catalog
                    .get(table)
                    .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
                let owner =
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone());
                let mut cursor = root.row_cursor(owner, table, row_id)?;
                let Some(window) = cursor.next_row()? else {
                    return Err(Error::SyncError(
                        (format!("purge follower {table} row has no retained version")).into(),
                    ));
                };
                natural_key_from_row_values(meta, &window.value.values)
                    .ok_or_else(|| Error::NotSyncEligible(table.to_owned()))?
            }
        };
        let current_generation = self.table_meta(table).is_some()
            && self.durable_lineage_table_generation(table, read_memory.clone())?
                == table_generation;
        if current_generation
            && self
                .visible_row_by_natural_key(
                    table,
                    &natural_key,
                    self.snapshot_for_read(),
                    &HashSet::new(),
                    read_memory.clone(),
                )?
                .is_some_and(|row| row.row_id == row_id)
        {
            return self
                .resolve_authoritative_purge_selection(table, &natural_key, read_memory)
                .map(Some);
        }
        let state = self.lineage_state_lock.lock();
        let lifecycle = self
            .load_lineage_record(
                &state,
                &Self::durable_lineage_config_key(table, &natural_key, table_generation),
            )?
            .filter(|record| record.local_row_id == Some(row_id));
        if lifecycle
            .as_ref()
            .is_some_and(|record| record.delete_obligation == DurableDeleteObligation::Purged)
        {
            return Ok(None);
        }
        let lineage_root = if let Some(sidecar) = self
            .load_row_lineage_sidecar(
                &state,
                &Self::durable_row_lineage_config_key(table, &natural_key, table_generation),
            )?
            .filter(|sidecar| {
                sidecar.table_generation == table_generation && sidecar.local_row_id == Some(row_id)
            }) {
            sidecar.lineage_root
        } else if let Some(creation_lsn) = self
            .load_unbound_creation_lineage(
                &state,
                &Self::durable_unbound_creation_config_key(
                    table,
                    &natural_key,
                    table_generation,
                    row_id,
                ),
            )?
            .filter(|evidence| {
                evidence.table == table
                    && evidence.natural_key == natural_key
                    && evidence.table_generation == table_generation
                    && evidence.local_row_id == row_id
            })
            .and_then(|evidence| evidence.creation_lsn)
        {
            format!(
                "unbound:{}:{}",
                Self::hex_component(&sync_identity_key(table, &natural_key)),
                creation_lsn.0
            )
        } else if let Some(record) = lifecycle {
            record.lineage_root
        } else {
            return Err(Error::SyncError(
                (format!(
                    "cannot erase a retained {table} life that follows this purge: no durable \
                     lineage evidence names it"
                ))
                .into(),
            ));
        };
        Ok(Some(AuthoritativePurgeSelection {
            table: table.to_string(),
            table_generation,
            natural_key,
            lineage_root,
            local_row_id: row_id,
            retained: true,
        }))
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Database {
    /// The retained lives a purge of `parent_key` in `parent_table` would also
    /// erase right now, transitively, as `(table, row id, retained)`. Found by
    /// the same discovery the purge runs; nothing is erased.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn purge_follower_lives_for_test(
        &self,
        parent_table: &str,
        parent_key: &NaturalKey,
    ) -> Result<Vec<(String, RowId, bool)>> {
        let read_memory = self.declared_reader_memory()?;
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let probe = AuthoritativePurgeSelection {
            table: parent_table.to_string(),
            table_generation: 0,
            natural_key: parent_key.clone(),
            lineage_root: String::new(),
            local_row_id: RowId(u64::MAX),
            retained: false,
        };
        let mut lives = self
            .authoritative_purge_followers(std::slice::from_ref(&probe), &mut credit, read_memory)?
            .into_iter()
            .map(|selection| (selection.table, selection.local_row_id, selection.retained))
            .collect::<Vec<_>>();
        lives.sort();
        Ok(lives)
    }
}
