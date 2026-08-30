use crate::store::{RelationalStore, index_key_from_values};
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator, row_matches_delete_predicates};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

pub struct MemRelationalExecutor<S: WriteSetApplicator> {
    store: Arc<RelationalStore>,
    tx_mgr: Arc<TransactionManager<S>>,
}

/// Position in an ordered B-tree posting walk. The cursor stores only owned
/// keys plus the last consumed row identity, so it can be retained by a
/// higher-level cursor without retaining a relational-store lock across
/// fetches. Postings are row_id-ordered (I18), so resuming strictly after the
/// stored identity is immune to concurrent middle inserts: a posting committed
/// after the walk began lands at its identity-ordered position and never
/// shifts an already-consumed identity back into range.
#[derive(Debug, Clone, Default)]
pub struct BoundedIndexCursor {
    key: Option<IndexKey>,
    last_row_id: Option<RowId>,
    retained_key_bytes: usize,
    key_generation: u64,
    exhausted: bool,
}

impl BoundedIndexCursor {
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn retained_key_bytes(&self) -> usize {
        self.retained_key_bytes
    }

    pub fn key_generation(&self) -> u64 {
        self.key_generation
    }
}

/// Owned position in a real ordered posting tree. The continuation never
/// retains a store lock across pulls and never reconstructs ordering values
/// from table rows. It addresses its resume point by the last consumed row
/// identity, never by raw index: posting vectors are row_id-ordered (I18) and
/// concurrent commits middle-insert into them, so an integer position would
/// re-emit or skip rows the pinned snapshot is entitled to see exactly once.
#[derive(Debug, Clone)]
pub struct BoundedOrderedRowCursor {
    index: Option<String>,
    key: Option<IndexKey>,
    last_row_id: Option<RowId>,
    reverse: bool,
    /// Where in tree order the run this cursor was opened for begins and
    /// ends, stated on the index's leading component. A cursor opened with no
    /// declared run walks the whole index, which is what an ordering with no
    /// predicate on the leading column asks for.
    run_start: Bound<DirectedValue>,
    run_end: Bound<DirectedValue>,
    /// The runs still to walk once the current one is finished, held in the
    /// order the predicate named them and taken from the front. A predicate
    /// naming a LIST of values names one run per value: reading from the
    /// least listed value to the greatest instead reads every key in between
    /// and rejects it afterwards. A range names a single run and leaves this
    /// empty.
    pending_runs: std::collections::VecDeque<(Bound<DirectedValue>, Bound<DirectedValue>)>,
    retained_key_bytes: usize,
    key_generation: u64,
    exhausted: bool,
}

impl Default for BoundedOrderedRowCursor {
    fn default() -> Self {
        Self {
            index: None,
            key: None,
            last_row_id: None,
            reverse: false,
            run_start: Bound::Unbounded,
            run_end: Bound::Unbounded,
            pending_runs: std::collections::VecDeque::new(),
            retained_key_bytes: 0,
            key_generation: 0,
            exhausted: false,
        }
    }
}

impl BoundedOrderedRowCursor {
    pub fn for_index(index: String, reverse: bool) -> Self {
        Self {
            index: Some(index),
            key: None,
            last_row_id: None,
            reverse,
            run_start: Bound::Unbounded,
            run_end: Bound::Unbounded,
            pending_runs: std::collections::VecDeque::new(),
            retained_key_bytes: 0,
            key_generation: 0,
            exhausted: false,
        }
    }

    /// Declare the run of index keys this cursor answers for, in tree order.
    /// The source then starts at the first key inside the run and stops at
    /// the first key past it, instead of starting at the first key of the
    /// index and rejecting everything the predicate excludes.
    pub fn seeking_run(mut self, start: Bound<DirectedValue>, end: Bound<DirectedValue>) -> Self {
        self.run_start = start;
        self.run_end = end;
        self
    }

    /// Declare SEVERAL runs, walked one after another in the order given. A
    /// predicate naming a list of values names one run per value, and the
    /// walk answers each of them in turn: it seeks to the first key of a run,
    /// stops at the first key past it, and then seeks to the next run's first
    /// key rather than reading the keys that lie between two listed values.
    /// An empty list leaves the cursor walking the whole index, which is what
    /// a cursor with no declared run already does.
    pub fn seeking_runs(
        mut self,
        runs: impl IntoIterator<Item = (Bound<DirectedValue>, Bound<DirectedValue>)>,
    ) -> Self {
        let mut runs = runs.into_iter();
        let Some((start, end)) = runs.next() else {
            return self;
        };
        self.run_start = start;
        self.run_end = end;
        self.pending_runs = runs.collect();
        self
    }

    /// What the runs still to walk are holding. A predicate naming a list of
    /// values names one run per value, and every run past the first stays in
    /// this cursor for as long as the read does -- so a continuation that
    /// leaves them out reports a read holding less memory than it holds.
    pub fn pending_run_bytes(&self) -> usize {
        let mut bytes = self.pending_runs.len().saturating_mul(std::mem::size_of::<(
            Bound<DirectedValue>,
            Bound<DirectedValue>,
        )>());
        for (start, end) in &self.pending_runs {
            bytes = bytes
                .saturating_add(bounded_run_bound_bytes(start))
                .saturating_add(bounded_run_bound_bytes(end));
        }
        bytes
    }

    /// Move to the next declared run, if the predicate named one. The walk
    /// re-seeks from that run's own start, so the resume anchor of the run
    /// just finished is dropped; the key it retained is still charged and is
    /// reconciled by the caller on the next key change, exactly as a key
    /// change inside one run is.
    fn advance_to_next_run(&mut self) -> bool {
        let Some((start, end)) = self.pending_runs.pop_front() else {
            return false;
        };
        self.run_start = start;
        self.run_end = end;
        self.key = None;
        self.last_row_id = None;
        true
    }

    /// Whether this key still sits inside the declared run in the direction
    /// the cursor walks. A key past the far end ends the walk; the near end
    /// is where the walk began, so it never ends it.
    fn inside_declared_run(&self, key: &IndexKey) -> bool {
        let Some(leading) = key.first() else {
            return true;
        };
        let far_end = if self.reverse {
            &self.run_start
        } else {
            &self.run_end
        };
        match far_end {
            Bound::Unbounded => true,
            Bound::Included(edge) => {
                if self.reverse {
                    leading >= edge
                } else {
                    leading <= edge
                }
            }
            Bound::Excluded(edge) => {
                if self.reverse {
                    leading > edge
                } else {
                    leading < edge
                }
            }
        }
    }

    /// The tree-order bound the walk starts from. A composite index orders by
    /// the whole key, so an inclusive edge on the leading component alone
    /// cannot express "this value followed by anything" on the far side; the
    /// walk therefore seeks only from the side it starts on and stops at the
    /// other by inspecting one key past the run.
    fn seek_from(&self, components: usize) -> Bound<IndexKey> {
        let near_end = if self.reverse {
            if components != 1 {
                return Bound::Unbounded;
            }
            &self.run_end
        } else {
            &self.run_start
        };
        match near_end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(edge) => Bound::Included(vec![edge.clone()]),
            Bound::Excluded(edge) => Bound::Excluded(vec![edge.clone()]),
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn index_name(&self) -> Option<&str> {
        self.index.as_deref()
    }

    pub fn retained_key_bytes(&self) -> usize {
        self.retained_key_bytes
    }

    pub fn key_generation(&self) -> u64 {
        self.key_generation
    }
}

/// Owned position in a physical table version vector. The continuation
/// carries the identities of the last inspected and the last emitted rows so
/// a pull can re-anchor after maintenance physically compacts the vector:
/// `retain`-style removal preserves relative order, so the nearest surviving
/// anchor at or below the stale position locates the exact resume point
/// without trusting a raw index across pulls.
#[derive(Debug, Clone, Default)]
pub struct BoundedPhysicalCursor {
    position: usize,
    end: usize,
    last_inspected: Option<(RowId, TxId)>,
    last_emitted: Option<(RowId, TxId)>,
    exhausted: bool,
}

impl BoundedPhysicalCursor {
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Record that the most recently pulled row was emitted downstream. The
    /// emitted anchor names a row visible at the caller's registered
    /// snapshot, which maintenance never physically removes, so relocation
    /// can always resume strictly after the last row the caller consumed.
    pub fn note_emitted(&mut self) {
        self.last_emitted = self.last_inspected;
    }
}

/// The next not-yet-consumed run of same-row_id postings in traversal order.
/// Postings are row_id-ascending (I18); a replacement that keeps this index's
/// key tombstones the old posting and inserts a new one under the SAME
/// row_id, so one row identity can own several adjacent postings with
/// disjoint visibility windows. The run is therefore the consumption unit:
/// resuming strictly after the run's row identity can never skip the one
/// visible twin, and a concurrent middle insert (always a post-snapshot,
/// invisible posting) never shifts a consumed identity back into range.
fn next_posting_run(
    entries: &[crate::store::IndexEntry],
    last_row_id: Option<RowId>,
    reverse: bool,
) -> Option<(usize, usize)> {
    if reverse {
        let end = match last_row_id {
            Some(last) => entries.partition_point(|entry| entry.row_id < last),
            None => entries.len(),
        };
        let run_row_id = entries.get(end.checked_sub(1)?)?.row_id;
        let start = entries.partition_point(|entry| entry.row_id < run_row_id);
        Some((start, end))
    } else {
        let start = match last_row_id {
            Some(last) => entries.partition_point(|entry| entry.row_id <= last),
            None => 0,
        };
        let run_row_id = entries.get(start)?.row_id;
        let end = entries.partition_point(|entry| entry.row_id <= run_row_id);
        Some((start, end))
    }
}

/// The keys an exact-lookup index is being asked, and the postings of the one
/// key currently being read.
///
/// Only one key's postings are held at a time: a probe over many keys is
/// bounded by its widest key, not by the sum of them.
pub struct BoundedExactIndexCursor {
    keys: std::collections::VecDeque<IndexKey>,
    postings: Vec<crate::store::IndexEntry>,
    position: usize,
    retained_bytes: usize,
    exhausted: bool,
}

impl BoundedExactIndexCursor {
    pub fn for_keys(keys: impl IntoIterator<Item = IndexKey>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
            postings: Vec::new(),
            position: 0,
            retained_bytes: 0,
            exhausted: false,
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// What this cursor is holding right now, for the caller that charged it.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn take_retained_bytes(&mut self) -> usize {
        self.postings = Vec::new();
        self.position = 0;
        std::mem::take(&mut self.retained_bytes)
    }
}

/// What one edge of a declared run holds beyond its own enum slot.
fn bounded_run_bound_bytes(bound: &Bound<DirectedValue>) -> usize {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => match value {
            DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => {
                value.estimated_bytes()
            }
        },
        Bound::Unbounded => 0,
    }
}

fn bounded_index_key_heap_bytes(key: &IndexKey) -> Result<usize> {
    let mut bytes = key
        .len()
        .checked_mul(std::mem::size_of::<DirectedValue>())
        .ok_or_else(|| Error::Other("bounded index key size overflow".to_string()))?;
    for value in key {
        let value = match value {
            DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => {
                value
            }
        };
        bytes = bytes
            .checked_add(value.estimated_bytes())
            .ok_or_else(|| Error::Other("bounded index key size overflow".to_string()))?;
    }
    Ok(bytes)
}

fn bounded_index_key_owned_bytes(key: &IndexKey) -> Result<usize> {
    std::mem::size_of::<IndexKey>()
        .checked_add(bounded_index_key_heap_bytes(key)?)
        .ok_or_else(|| Error::Other("bounded owned index key size overflow".to_string()))
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

    /// Number of physical versions currently addressable by a pull scan.
    /// Capturing this once gives a cursor a stable end position; later commits
    /// are outside its snapshot and do not extend its source.
    pub fn bounded_table_len(&self, table: &str) -> Result<usize> {
        let tables = self.store.tables.read();
        tables
            .get(table)
            .map(Vec::len)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))
    }

    /// Open a physical-scan continuation over the versions addressable right
    /// now. Later commits append past the captured end and are outside the
    /// caller's snapshot; maintenance compaction only shrinks the vector, and
    /// the pull path re-anchors by identity when it does.
    pub fn bounded_physical_cursor(&self, table: &str) -> Result<BoundedPhysicalCursor> {
        Ok(BoundedPhysicalCursor {
            position: 0,
            end: self.bounded_table_len(table)?,
            last_inspected: None,
            last_emitted: None,
            exhausted: false,
        })
    }

    /// Read the next physical table item after the caller has admitted its
    /// work and memory charge. `before_touch` runs once per row this call
    /// inspects — including the identity comparisons of a relocation after
    /// maintenance compacted the vector — and `before_clone` runs while the
    /// selected row is still borrowed from the store, before it is cloned.
    pub fn bounded_physical_row_next<E>(
        &self,
        table: &str,
        cursor: &mut BoundedPhysicalCursor,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_clone: impl FnOnce(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        if cursor.exhausted {
            return Ok(None);
        }
        let tables = self.store.tables.read();
        let rows = tables
            .get(table)
            .ok_or_else(|| E::from(Error::TableNotFound(table.to_string())))?;
        if let Some(anchor_position) = cursor.position.checked_sub(1) {
            let anchored = rows
                .get(anchor_position)
                .is_some_and(|row| Some((row.row_id, row.created_tx)) == cursor.last_inspected);
            if !anchored {
                // Maintenance compacted the vector under the continuation.
                // Removal preserves relative order, so the last inspected
                // identity (or, failing that, the last emitted identity,
                // which sits at or before it and is protected by the
                // caller's registered snapshot) is the nearest surviving
                // anchor at or below the stale position. Each candidate is
                // charged before its identity is read.
                let mut found = None;
                if !rows.is_empty() {
                    let mut candidate = anchor_position.min(rows.len() - 1);
                    loop {
                        before_touch()?;
                        let row = &rows[candidate];
                        let identity = Some((row.row_id, row.created_tx));
                        if identity == cursor.last_inspected {
                            found = Some((candidate, true));
                            break;
                        }
                        if identity == cursor.last_emitted {
                            found = Some((candidate, false));
                            break;
                        }
                        let Some(next) = candidate.checked_sub(1) else {
                            break;
                        };
                        candidate = next;
                    }
                }
                match found {
                    Some((found_position, exact)) => {
                        if exact {
                            // Every removed slot below the anchor tightens the
                            // captured end by the same amount; the end can
                            // only over-approximate (post-snapshot appends are
                            // invisible and merely cost charged inspections),
                            // never under-approximate.
                            cursor.end =
                                cursor.end.saturating_sub(anchor_position - found_position);
                        } else {
                            // Resuming from the emitted anchor re-inspects the
                            // rows between it and the lost inspected anchor;
                            // all of them were invisible or filtered, so the
                            // re-inspection is deterministic re-filtering,
                            // never a duplicate emission.
                            cursor.last_inspected = cursor.last_emitted;
                        }
                        cursor.position = found_position + 1;
                    }
                    None => {
                        if cursor.last_emitted.is_some() {
                            return Err(E::from(Error::Other(
                                "bounded physical scan lost its emitted continuation row"
                                    .to_string(),
                            )));
                        }
                        cursor.position = 0;
                        cursor.last_inspected = None;
                    }
                }
            }
        }
        if cursor.position >= cursor.end || cursor.position >= rows.len() {
            cursor.exhausted = true;
            return Ok(None);
        }
        before_touch()?;
        let row = &rows[cursor.position];
        before_clone(row.estimated_bytes())?;
        let row = row.clone();
        cursor.last_inspected = Some((row.row_id, row.created_tx));
        cursor.position += 1;
        self.store.bump_scan_rows_touched(1);
        Ok(Some(row))
    }

    /// Pull the next real posting from an ordered user index, one row
    /// identity per call: the run's live posting when one exists, otherwise
    /// its first posting (a replacement tombstones the posting it supersedes,
    /// so a run holds at most one live entry). Locating the owned
    /// continuation key and the resume point reads only ordering metadata;
    /// `before_touch` runs before each posting this call inspects, and
    /// `before_clone` immediately before the selected posting is cloned.
    pub fn bounded_index_next<E>(
        &self,
        table: &str,
        index: &str,
        cursor: &mut BoundedIndexCursor,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_clone: impl FnOnce(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<(IndexKey, crate::store::IndexEntry)>, E>
    where
        E: From<Error>,
    {
        if cursor.exhausted {
            return Ok(None);
        }
        let indexes = self.store.indexes.read();
        let storage = indexes
            .get(table)
            .and_then(|table_indexes| table_indexes.get(index))
            .ok_or_else(|| {
                E::from(Error::IndexNotFound {
                    table: table.to_string(),
                    index: index.to_string(),
                })
            })?;

        let selected = if let Some(key) = cursor.key.as_ref() {
            let same_key = storage.tree.get(key).and_then(|entries| {
                next_posting_run(entries, cursor.last_row_id, false).map(|run| (key, run, entries))
            });
            same_key.or_else(|| {
                storage
                    .tree
                    .range::<[DirectedValue], _>((
                        Bound::Excluded(key.as_slice()),
                        Bound::Unbounded,
                    ))
                    .find_map(|(next_key, entries)| {
                        next_posting_run(entries, None, false).map(|run| (next_key, run, entries))
                    })
            })
        } else {
            storage.tree.iter().find_map(|(key, entries)| {
                next_posting_run(entries, None, false).map(|run| (key, run, entries))
            })
        };

        let Some((key, (run_start, run_end), entries)) = selected else {
            cursor.exhausted = true;
            return Ok(None);
        };
        let mut live_entry = None;
        for entry in &entries[run_start..run_end] {
            before_touch()?;
            self.store.bump_index_entries_touched(1);
            if entry.deleted_tx.is_none() {
                live_entry = Some(entry);
                break;
            }
        }
        let entry = match live_entry {
            Some(entry) => entry,
            None => &entries[run_start],
        };
        let run_row_id = entry.row_id;
        let key_changed = cursor
            .key
            .as_ref()
            .is_none_or(|current| current.as_slice() != key.as_slice());
        let next_generation = if key_changed {
            Some(cursor.key_generation.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded index key generation overflow".to_string(),
                ))
            })?)
        } else {
            None
        };
        let output_key_bytes = bounded_index_key_owned_bytes(key).map_err(E::from)?;
        let new_cursor_key_bytes = if key_changed {
            bounded_index_key_heap_bytes(key).map_err(E::from)?
        } else {
            0
        };
        let clone_bytes = std::mem::size_of::<crate::store::IndexEntry>()
            .checked_add(output_key_bytes)
            .and_then(|bytes| bytes.checked_add(new_cursor_key_bytes))
            .ok_or_else(|| {
                E::from(Error::Other(
                    "bounded index clone size overflow".to_string(),
                ))
            })?;
        before_clone(clone_bytes)?;
        let replacement_key = if key_changed { Some(key.clone()) } else { None };
        let output_key = key.clone();
        let entry = entry.clone();
        if let (Some(generation), Some(replacement_key)) = (next_generation, replacement_key) {
            cursor.key = Some(replacement_key);
            cursor.retained_key_bytes = new_cursor_key_bytes;
            cursor.key_generation = generation;
        }
        cursor.last_row_id = Some(run_row_id);
        cursor.exhausted = run_end >= entries.len()
            && storage
                .tree
                .range::<[DirectedValue], _>((
                    Bound::Excluded(output_key.as_slice()),
                    Bound::Unbounded,
                ))
                .next()
                .is_none();
        Ok(Some((output_key, entry)))
    }

    /// Pull one candidate from a real ordered index, consuming one row
    /// identity's posting run per call. The source callback runs before each
    /// posting this call inspects, and the reservation callback runs before
    /// the continuation key and row are cloned. Exact-only auto indexes
    /// deliberately do not enter this path: their hash buckets cannot
    /// provide SQL ordering without a separate ordered store seam.
    /// One row at a time from the exact keys a predicate names.
    ///
    /// An index built for exact lookup keeps its postings in a hash map and no
    /// ordered tree, so it cannot be walked or ranged -- but it can be asked a
    /// whole key, which is exactly what an equality or an in-list names. This
    /// answers those, one visible row per call, charging for each posting it
    /// inspects so a lookup costs what it actually reads.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_exact_index_row_next<E>(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut BoundedExactIndexCursor,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
        mut before_clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        if cursor.exhausted {
            return Ok(None);
        }
        let indexes = self.store.indexes.read();
        let table_indexes = indexes
            .get(table)
            .ok_or_else(|| E::from(Error::TableNotFound(table.to_string())))?;
        let storage = table_indexes.get(index).ok_or_else(|| {
            E::from(Error::IndexNotFound {
                table: table.to_string(),
                index: index.to_string(),
            })
        })?;

        loop {
            while cursor.position < cursor.postings.len() {
                let entry = cursor.postings[cursor.position].clone();
                cursor.position += 1;
                before_touch()?;
                self.store.bump_index_entries_touched(1);
                if !entry.visible_at(snapshot) {
                    continue;
                }
                // A posting names a ROW, and the row is read at the
                // snapshot. It cannot name a row VERSION: an update that
                // leaves every indexed column alone deliberately does not
                // touch the index -- the key has not changed, so the posting
                // that was there stays there and goes on carrying the
                // creating transaction of the version it was written for
                // (`RelationalStore::apply_replacements_ref`, via
                // `same_index_key_replacements`). Demanding that the version
                // the snapshot shows be the version the posting was written
                // for therefore refused every lookup of a row that had been
                // updated in place, primary keys most of all -- a primary key
                // is exactly the key an upsert never changes.
                //
                // Nothing is yielded twice by reading it this way. When a key
                // DOES change, the old posting is tombstoned and a new one
                // inserted, so one posting is visible per snapshot; the
                // untouched-index case leaves a single posting to begin with.
                let row =
                    self.store
                        .row_by_id_before_clone(table, entry.row_id, snapshot, |row| {
                            before_clone(row.estimated_bytes())
                        })?;
                let Some(row) = row else {
                    // The posting outlived every version of its row that this
                    // snapshot can see. There is nothing here for this reader,
                    // and the next posting may still have something.
                    continue;
                };
                return Ok(Some(row));
            }

            // This key is spent; release what its postings held before the
            // next one is charged for, so a many-key probe holds one key's
            // worth of postings rather than every key's.
            let released = cursor.take_retained_bytes();
            if released > 0 {
                release_retained(released)?;
            }
            let Some(key) = cursor.keys.pop_front() else {
                cursor.exhausted = true;
                return Ok(None);
            };
            let postings = storage
                .exact_postings(&key)
                .map(|postings| postings.to_vec())
                .unwrap_or_default();
            let retained = postings
                .len()
                .checked_mul(std::mem::size_of::<crate::store::IndexEntry>())
                .ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded exact posting size overflow".to_string(),
                    ))
                })?;
            before_retain(retained)?;
            cursor.postings = postings;
            cursor.position = 0;
            cursor.retained_bytes = retained;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bounded_ordered_row_next<E>(
        &self,
        table: &str,
        column: &str,
        direction: SortDirection,
        snapshot: SnapshotId,
        cursor: &mut BoundedOrderedRowCursor,
        mut before_touch: impl FnMut() -> std::result::Result<(), E>,
        before_clone: impl FnOnce(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        if cursor.exhausted {
            return Ok(None);
        }

        let indexes = self.store.indexes.read();
        let table_indexes = indexes
            .get(table)
            .ok_or_else(|| E::from(Error::TableNotFound(table.to_string())))?;
        let index_name = cursor.index.as_deref().ok_or_else(|| {
            E::from(Error::Other(
                "bounded ordered cursor requires a selected index".to_string(),
            ))
        })?;
        let storage = table_indexes.get(index_name).ok_or_else(|| {
            E::from(Error::IndexNotFound {
                table: table.to_string(),
                index: index_name.to_string(),
            })
        })?;
        if storage.exact_only()
            || storage
                .columns
                .first()
                .is_none_or(|(candidate, _)| candidate != column)
        {
            return Err(E::from(Error::Other(format!(
                "index {index_name} cannot order {table}.{column}"
            ))));
        }
        let expected_reverse = storage.columns[0].1 != direction;
        if cursor.reverse != expected_reverse {
            return Err(E::from(Error::Other(format!(
                "ordered cursor direction does not match index {index_name}"
            ))));
        }

        // The predicate names where the answer starts and where it ends, so
        // the walk seeks to the first key inside the declared run and stops at
        // the first key past it rather than reading the index end to end.
        let seek_from = cursor.seek_from(storage.columns.len());
        let seek_from = match &seek_from {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(edge) => Bound::Included(edge.as_slice()),
            Bound::Excluded(edge) => Bound::Excluded(edge.as_slice()),
        };
        let selected = if let Some(key) = cursor.key.as_ref() {
            let same_key = storage.tree.get(key).and_then(|entries| {
                next_posting_run(entries, cursor.last_row_id, cursor.reverse)
                    .map(|run| (key, run, entries))
            });
            same_key.or_else(|| {
                if cursor.reverse {
                    storage
                        .tree
                        .range::<[DirectedValue], _>((
                            Bound::Unbounded,
                            Bound::Excluded(key.as_slice()),
                        ))
                        .rev()
                        .take_while(|(next_key, _)| cursor.inside_declared_run(next_key))
                        .find_map(|(next_key, entries)| {
                            next_posting_run(entries, None, true)
                                .map(|run| (next_key, run, entries))
                        })
                } else {
                    storage
                        .tree
                        .range::<[DirectedValue], _>((
                            Bound::Excluded(key.as_slice()),
                            Bound::Unbounded,
                        ))
                        .take_while(|(next_key, _)| cursor.inside_declared_run(next_key))
                        .find_map(|(next_key, entries)| {
                            next_posting_run(entries, None, false)
                                .map(|run| (next_key, run, entries))
                        })
                }
            })
        } else if cursor.reverse {
            storage
                .tree
                .range::<[DirectedValue], _>((Bound::Unbounded, seek_from))
                .rev()
                .take_while(|(key, _)| cursor.inside_declared_run(key))
                .find_map(|(key, entries)| {
                    next_posting_run(entries, None, true).map(|run| (key, run, entries))
                })
        } else {
            storage
                .tree
                .range::<[DirectedValue], _>((seek_from, Bound::Unbounded))
                .take_while(|(key, _)| cursor.inside_declared_run(key))
                .find_map(|(key, entries)| {
                    next_posting_run(entries, None, false).map(|run| (key, run, entries))
                })
        };

        let Some((key, (run_start, run_end), entries)) = selected else {
            // Nothing is left in the run being walked. A predicate that named
            // a LIST of values named one run per value, so the walk moves to
            // the next one and seeks from its start rather than ending here --
            // reading from the least listed value to the greatest would read
            // every key in between and reject it afterwards.
            if cursor.advance_to_next_run() {
                return Ok(None);
            }
            cursor.exhausted = true;
            return Ok(None);
        };
        let run_row_id = entries[run_start].row_id;
        // The run is this ROW's postings, and which VERSION of it the reader
        // sees is the snapshot's answer, not the postings'. A posting whose
        // key never changed is never rewritten -- an update that leaves every
        // indexed column alone deliberately does not touch the index -- so it
        // goes on carrying the creating transaction of the version it was
        // written for. Letting the postings' own version windows decide
        // whether this row is offered at all dropped a row the reader could
        // plainly see: the walk skipped it and moved on, and the caller got a
        // short answer with nothing said about it.
        //
        // So the run decides only WHICH ROW is next. Every posting in it is
        // still charged, because reading it is work the request is paying for,
        // and the row is resolved once below -- one row per run, so nothing is
        // handed out twice.
        for _ in &entries[run_start..run_end] {
            before_touch()?;
            self.store.bump_index_entries_touched(1);
        }
        let visible_entry = entries.get(run_start);
        let key_changed = cursor
            .key
            .as_ref()
            .is_none_or(|current| current.as_slice() != key.as_slice());
        let next_generation = if key_changed {
            Some(cursor.key_generation.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded ordered key generation overflow".to_string(),
                ))
            })?)
        } else {
            None
        };
        let new_cursor_key_bytes = if key_changed {
            bounded_index_key_heap_bytes(key).map_err(E::from)?
        } else {
            0
        };
        let another_same_key = if cursor.reverse {
            run_start > 0
        } else {
            run_end < entries.len()
        };
        let another_key = if cursor.reverse {
            storage
                .tree
                .range::<[DirectedValue], _>((Bound::Unbounded, Bound::Excluded(key.as_slice())))
                .next_back()
                .is_some_and(|(next_key, _)| cursor.inside_declared_run(next_key))
        } else {
            storage
                .tree
                .range::<[DirectedValue], _>((Bound::Excluded(key.as_slice()), Bound::Unbounded))
                .next()
                .is_some_and(|(next_key, _)| cursor.inside_declared_run(next_key))
        };
        // Charged exactly once, on whichever of the paths below is taken.
        let mut before_clone = Some(before_clone);
        let row = if let Some(entry) = visible_entry {
            let row = self
                .store
                // Same rule as the exact probe above, for the same reason:
                // the posting names a row, the snapshot names the version.
                .row_by_id_before_clone(table, entry.row_id, snapshot, |row| {
                    let clone_bytes = row
                        .estimated_bytes()
                        .checked_add(new_cursor_key_bytes)
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded ordered clone size overflow".to_string(),
                            ))
                        })?;
                    let before_clone = before_clone.take().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded ordered row charged twice".to_string(),
                        ))
                    })?;
                    before_clone(clone_bytes)
                })?;
            match row {
                Some(row) => Some(row),
                // The posting names a row; the SNAPSHOT names the version. No
                // version this snapshot can see is the ordinary answer for a
                // row deleted after the posting was written, one inserted
                // after this snapshot began, or a version a writer has just
                // replaced -- so the walk steps over it, exactly as the exact
                // probe below does, rather than calling the index corrupt and
                // failing a statement the store can answer.
                None => {
                    if let Some(before_clone) = before_clone.take() {
                        before_clone(new_cursor_key_bytes)?;
                    }
                    None
                }
            }
        } else {
            if let Some(before_clone) = before_clone.take() {
                before_clone(new_cursor_key_bytes)?;
            }
            None
        };
        let replacement_key = if key_changed { Some(key.clone()) } else { None };
        if let (Some(generation), Some(replacement_key)) = (next_generation, replacement_key) {
            cursor.key = Some(replacement_key);
            cursor.retained_key_bytes = new_cursor_key_bytes;
            cursor.key_generation = generation;
        }
        cursor.last_row_id = Some(run_row_id);
        cursor.exhausted = !another_same_key && !another_key && cursor.pending_runs.is_empty();
        Ok(row)
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
                self.insert_with_row_id(tx, table, existing_row.row_id, values, snapshot)?;
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
