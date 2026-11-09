//! What a PURGE of a parent row also erases: every retained life of every
//! row whose declared following foreign key names it, transitively.
//!
//! Two routes find them, and neither scans a table:
//! - the child's covering index, read through its history table, which keeps
//!   the postings of deleted and replaced lives as well as current ones;
//! - the parent-keyed archive postings, for lives an earlier DROP or a body
//!   change archived. These are found from the parent alone, whatever the
//!   child table is declared as now, or whether it still exists.
use super::purge_followers::{
    PurgeFollower, declared_purge_follower_bytes, declared_purge_follower_count,
    declared_purge_followers_into,
};
use super::writer_read::next_window_with_work;
use super::*;
use contextdb_core::read_memory::ReadCredit;

const HINT: &str = "Raise MEMORY_LIMIT to find what follows this purge.";

/// Admit room for one more element before `vec` grows to hold it. The grown
/// buffer exists beside the old one while elements move, so both are counted.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admit_vec_slot<T>(
    vec: &mut Vec<T>,
    credit: &mut ReadCredit,
    operation: &'static str,
) -> Result<()> {
    if vec.len() == vec.capacity() {
        let grow = vec.capacity().max(4);
        credit.try_grow_for(
            vec.capacity()
                .saturating_add(grow)
                .saturating_mul(std::mem::size_of::<T>()),
            "writer_source",
            operation,
            HINT,
        )?;
        vec.reserve_exact(grow);
    }
    Ok(())
}

/// What a hash table holding `capacity` entries of `entry` bytes can own:
/// its buckets, control bytes, load-factor slack and the rehash overlap.
fn hash_table_bytes(capacity: usize, entry: usize) -> usize {
    capacity
        .saturating_mul(entry.saturating_add(1))
        .saturating_mul(4)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn admit_set_slot<T: Eq + std::hash::Hash>(
    set: &mut HashSet<T>,
    credit: &mut ReadCredit,
) -> Result<()> {
    if set.len() == set.capacity() {
        let grow = set.capacity().max(4);
        credit.try_grow_for(
            hash_table_bytes(
                set.capacity().saturating_add(grow),
                std::mem::size_of::<T>(),
            ),
            "writer_source",
            "purge_follower_dedup",
            HINT,
        )?;
        set.reserve(grow);
    }
    Ok(())
}

/// Record `(table, row_id)` as found, admitting every byte it adds first.
/// False when it was already found.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn admit_known(
    known: &mut HashMap<String, HashSet<RowId>>,
    table: &str,
    row_id: RowId,
    credit: &mut ReadCredit,
) -> Result<bool> {
    if let Some(ids) = known.get_mut(table) {
        if ids.contains(&row_id) {
            return Ok(false);
        }
        admit_set_slot(ids, credit)?;
        ids.insert(row_id);
        return Ok(true);
    }
    if known.len() == known.capacity() {
        let grow = known.capacity().max(4);
        credit.try_grow_for(
            hash_table_bytes(
                known.capacity().saturating_add(grow),
                std::mem::size_of::<(String, HashSet<RowId>)>(),
            ),
            "writer_source",
            "purge_follower_dedup",
            HINT,
        )?;
        known.reserve(grow);
    }
    credit.try_grow_for(table.len(), "writer_source", "purge_follower_dedup", HINT)?;
    let mut ids = HashSet::new();
    admit_set_slot(&mut ids, credit)?;
    ids.insert(row_id);
    known.insert(table.to_owned(), ids);
    Ok(true)
}

/// Queue a table and key whose followers are still to be found, admitting
/// the queue slot and both copies first.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn admit_queued(
    queue: &mut std::collections::VecDeque<(String, NaturalKey)>,
    table: &str,
    key: &NaturalKey,
    credit: &mut ReadCredit,
) -> Result<()> {
    if queue.len() == queue.capacity() {
        let grow = queue.capacity().max(4);
        credit.try_grow_for(
            queue
                .capacity()
                .saturating_add(grow)
                .saturating_mul(std::mem::size_of::<(String, NaturalKey)>()),
            "writer_source",
            "purge_follower_traversal",
            HINT,
        )?;
        queue.reserve_exact(grow);
    }
    credit.try_grow_for(
        table.len().saturating_add(key.held_bytes()),
        "writer_source",
        "purge_follower_traversal",
        HINT,
    )?;
    queue.push_back((table.to_owned(), key.clone()));
    Ok(())
}

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
        published_root: bool,
    ) -> Result<Vec<AuthoritativePurgeSelection>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Under the commit mutex the statement's captured root predates any
        // child committed since it was taken; that pass reads the root
        // published now, so such a child is found.
        let (_scope, _decode) = if published_root {
            let (scope, decode) = self.published_root_read_scope(read_memory.clone())?;
            (scope, Some(decode))
        } else {
            (self.writer_read_scope(read_memory.clone())?, None)
        };
        let Some(root) = self.writer_read_root()? else {
            return Ok(Vec::new());
        };
        let followers = self.admitted_purge_followers(credit)?;
        let mut known = HashMap::<String, HashSet<RowId>>::new();
        let mut queue = std::collections::VecDeque::<(String, NaturalKey)>::new();
        for selection in selections {
            admit_known(&mut known, &selection.table, selection.local_row_id, credit)?;
            admit_queued(&mut queue, &selection.table, &selection.natural_key, credit)?;
        }
        let mut found = Vec::new();
        while let Some((parent_table, parent_key)) = queue.pop_front() {
            credit.try_grow_for(
                parent_key.held_bytes().saturating_mul(2),
                "writer_source",
                "purge_follower_keys",
                HINT,
            )?;
            let values = parent_key.key_values();
            for follower in followers
                .iter()
                .filter(|follower| follower.parent_table == parent_table)
            {
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
                    if admit_known(&mut known, &follower.child_table, row_id, credit)? {
                        self.admit_follower_life(
                            &root,
                            &follower.child_table,
                            generation,
                            row_id,
                            None,
                            credit,
                            &mut queue,
                            &mut found,
                            read_memory.clone(),
                        )?;
                    }
                }
            }
            for archived in root.archived_purge_followers(&parent_table, &values, credit)? {
                if admit_known(&mut known, &archived.child_table, archived.row_id, credit)? {
                    self.admit_follower_life(
                        &root,
                        &archived.child_table,
                        archived.table_generation,
                        archived.row_id,
                        Some(archived.natural_key),
                        credit,
                        &mut queue,
                        &mut found,
                        read_memory.clone(),
                    )?;
                }
            }
        }
        Ok(found)
    }

    /// Every declared following foreign key in the catalog, admitted before it
    /// is copied and ordered by child table and ordinal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admitted_purge_followers(
        &self,
        credit: &mut ReadCredit,
    ) -> Result<Vec<PurgeFollower>> {
        let catalog = self.relational_store.table_meta.read();
        let (count, bytes) =
            catalog
                .iter()
                .fold((0_usize, 0_usize), |(count, bytes), (table, meta)| {
                    (
                        count.saturating_add(declared_purge_follower_count(meta)),
                        bytes.saturating_add(declared_purge_follower_bytes(table, meta)),
                    )
                });
        credit.try_grow_for(
            count
                .saturating_mul(std::mem::size_of::<PurgeFollower>())
                .saturating_add(bytes),
            "writer_source",
            "purge_follower_declarations",
            HINT,
        )?;
        // Built straight into the admitted exact-capacity list.
        let mut followers = Vec::with_capacity(count);
        for (table, meta) in catalog.iter() {
            declared_purge_followers_into(table, meta, &mut followers);
        }
        drop(catalog);
        followers.sort_unstable_by(|left, right| {
            (&left.child_table, left.ordinal).cmp(&(&right.child_table, right.ordinal))
        });
        Ok(followers)
    }

    /// Pin one found life and queue it for its own followers; every byte it
    /// adds is admitted before it exists.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "Each argument names one part of the life being admitted."
    )]
    fn admit_follower_life(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        table: &str,
        table_generation: u64,
        row_id: RowId,
        archived_key: Option<NaturalKey>,
        credit: &mut ReadCredit,
        queue: &mut std::collections::VecDeque<(String, NaturalKey)>,
        found: &mut Vec<AuthoritativePurgeSelection>,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        {
            use std::sync::atomic::Ordering;
            let seam = &super::authoritative_purge_kernel::PURGE_FOLLOWERS_BEFORE_REFUSAL;
            match seam.load(Ordering::SeqCst) {
                0 => {
                    seam.store(-1, Ordering::SeqCst);
                    return Err(Error::Other(
                        "purge follower admission refused by the test seam"
                            .to_string()
                            .into(),
                    ));
                }
                remaining if remaining > 0 => seam.store(remaining - 1, Ordering::SeqCst),
                _ => {}
            }
        }
        let Some(selection) = self.pin_purge_follower_life(
            root,
            table,
            table_generation,
            row_id,
            archived_key,
            credit,
            read_memory,
        )?
        else {
            return Ok(());
        };
        admit_queued(queue, &selection.table, &selection.natural_key, credit)?;
        admit_vec_slot(found, credit, "purge_follower_selection")?;
        found.push(selection);
        Ok(())
    }

    /// Read at the root published now, not the root the statement captured:
    /// under the commit mutex that is every row committed before this purge.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn published_root_read_scope(
        &self,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<(
        super::writer_read::WriterReadScope,
        crate::read_image_memory::ImageMemoryScope,
    )> {
        let image = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory.clone(),
        )?;
        let published =
            super::writer_read::durable_root(self.tx_mgr.capture_published_read().handle)?;
        Ok((
            self.enter_writer_read(published, read_memory),
            image.enter(),
        ))
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
            let copied = posting.position_key().len();
            credit.try_grow_for(copied, "writer_source", "purge_follower_keys", HINT)?;
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
            if !seen.contains(&posting.entry.row_id) {
                admit_set_slot(&mut seen, credit)?;
                seen.insert(posting.entry.row_id);
                admit_vec_slot(&mut rows, credit, "purge_follower_identities")?;
                rows.push(posting.entry.row_id);
            }
            drop(position);
            credit.try_shrink(copied)?;
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
        table: &str,
        table_generation: u64,
        row_id: RowId,
        archived_key: Option<NaturalKey>,
        credit: &mut ReadCredit,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Option<AuthoritativePurgeSelection>> {
        // The selection, its table name and its lineage keys.
        credit.try_grow_for(
            std::mem::size_of::<AuthoritativePurgeSelection>()
                .saturating_add(table.len().saturating_mul(4))
                .saturating_add(256),
            "writer_source",
            "purge_follower_selection",
            HINT,
        )?;
        let natural_key = match archived_key {
            Some(key) => key,
            None => {
                let meta = root
                    .borrow_captured_table_meta(table)
                    .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
                let owner =
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone());
                let mut cursor = root.row_cursor(owner, table, row_id)?;
                let Some(window) = cursor.next_row()? else {
                    return Err(Error::SyncError(
                        (format!("purge follower {table} row has no retained version")).into(),
                    ));
                };
                credit.try_grow_for(
                    window.value.estimated_bytes(),
                    "writer_source",
                    "purge_follower_selection",
                    HINT,
                )?;
                natural_key_from_row_values(meta, &window.value.values)
                    .ok_or_else(|| Error::NotSyncEligible(table.to_owned()))?
            }
        };
        // The lineage keys and root this life's witness is read under.
        credit.try_grow_for(
            natural_key
                .held_bytes()
                .saturating_mul(4)
                .saturating_add(table.len().saturating_mul(4))
                .saturating_add(256),
            "writer_source",
            "purge_follower_selection",
            HINT,
        )?;
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
            .authoritative_purge_followers(
                std::slice::from_ref(&probe),
                &mut credit,
                read_memory,
                false,
            )?
            .into_iter()
            .map(|selection| (selection.table, selection.local_row_id, selection.retained))
            .collect::<Vec<_>>();
        lives.sort();
        Ok(lives)
    }
}
