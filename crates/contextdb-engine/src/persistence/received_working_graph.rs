//! Graph versions in the same unpublished write as the received row image.
//! Initialization preserves authenticated physical bytes and never gathers an
//! adjacency list; read windows free every table guard before their consumer.
use super::*;
use super::received_working::ReceivedWorkingWrite;
use std::ops::Bound;

pub(super) const WORKING_GRAPH_FWD: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_fwd");
pub(super) const WORKING_GRAPH_REV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_rev");

pub(super) fn graph_key_matches(key: &[u8], edge: &AdjEntry, reverse: bool) -> bool {
    let Some(length) = edge.edge_type.len().checked_add(49) else { return false; };
    if key.len() != length { return false; }
    let (first, second) = if reverse { (edge.target, edge.source) } else { (edge.source, edge.target) };
    &key[..16] == first.as_bytes() && &key[16..32] == second.as_bytes()
        && &key[32..length-17] == edge.edge_type.as_bytes() && key[length-17] == 0
        && key[length-16..length-8] == edge.created_tx.0.to_be_bytes()
        && key[length-8..] == edge.lsn.0.to_be_bytes()
}

pub(crate) struct WorkingEdge {
    edge: AdjEntry,
    key: Vec<u8>,
    encoded: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl WorkingEdge {
    pub(crate) fn value(&self) -> &AdjEntry { &self.edge }
    pub(crate) fn position(&self) -> &[u8] { &self.key }
    pub(super) fn encoded(&self) -> &[u8] { &self.encoded }
}
impl Drop for WorkingEdge { fn drop(&mut self) {} }

impl ReceivedWorkingWrite {
    pub(crate) fn graph_prepared(&self) -> Result<bool> {
        if !self.graph_is_ready() { return Ok(false); }
        self.with_write(|write| Ok(write.table_exists(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?))
    }

    pub(crate) fn prepare_graph(
        &self,
        store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        root: WriterSnapshotHandle,
    ) -> Result<()> {
        self.require_original(original)?;
        self.require_store(store)?;
        root.require_working_store(store)?;
        if self.graph_prepared()? { return Ok(()); }
        self.prepare_graph_state(original)?;
        let read_memory = crate::memory_accounting::persistence_original_projection(original)?.read_budget()?;
        for (public, destination) in [(GRAPH_FWD_TABLE, WORKING_GRAPH_FWD), (GRAPH_REV_TABLE, WORKING_GRAPH_REV)] {
            let frame = OriginalPersistenceFrame::new(original)?;
            frame.reserve(destination.name().len())?;
            let name = crate::memory_accounting::preparation_copy_string(destination.name())?;
            self.keep_scratch_name(name, frame.into_reservation())?;
            self.with_write(|write| {
                match write.delete_table(destination) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                }
                write.open_table(destination).map(|_| ()).map_err(RedbPersistence::storage_error)
            })?;
            let mut cursor = root.cursor(writer_source::SourceWindowOwner::Read(read_memory.clone()),
                public.name(), writer_source::WriterCursorFamily::Bytes,
                Bound::Unbounded, Bound::Unbounded, false)?;
            while let Some(window) = cursor.next_edge_for_working()? {
                // Both directions use the existing graph proof/counterpart
                // verifier. The exact authenticated bytes, not a normalized
                // serialization, become the private physical version.
                self.with_write(|write| {
                    let mut target = write.open_table(destination).map_err(RedbPersistence::storage_error)?;
                    target.insert(window.position(), window.value.1.as_slice()).map_err(RedbPersistence::storage_error)?;
                    Ok(())
                })?;
            }
        }
        self.graph_became_ready();
        Ok(())
    }

    pub(crate) fn next_graph_edge(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        reverse: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Option<WorkingEdge>> {
        self.require_original(original)?;
        self.with_write(|write| next_graph_in_write(original, write, reverse, lower, upper))
    }
}

struct GraphPosition {
    key: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
fn original_graph_key(frame: &OriginalPersistenceFrame<'_>, edge: &AdjEntry, reverse: bool) -> Result<Vec<u8>> {
    let length = edge.edge_type.len().checked_add(49).ok_or_else(|| Error::Other("working graph key overflow".into()))?;
    frame.reserve(length)?;
    let mut key = Vec::with_capacity(length);
    let (first, second) = if reverse { (edge.target, edge.source) } else { (edge.source, edge.target) };
    key.extend_from_slice(first.as_bytes()); key.extend_from_slice(second.as_bytes());
    key.extend_from_slice(edge.edge_type.as_bytes()); key.push(0);
    key.extend_from_slice(&edge.created_tx.0.to_be_bytes()); key.extend_from_slice(&edge.lsn.0.to_be_bytes());
    if key.len() != length || key.capacity() != length { return Err(Error::Other("working graph key capacity differs from admission".into())); }
    Ok(key)
}
impl ReceivedWorkingWrite {
    fn put_working_graph_edge(&self, original: &crate::memory_accounting::OwnedMemoryReservation, edge: &AdjEntry) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let encoded = frame.encode(edge)?;
        let forward = original_graph_key(&frame, edge, false)?;
        let reverse = original_graph_key(&frame, edge, true)?;
        // All original admission and encoding precede mutable native guards.
        self.with_write(|write| {
            let mut fwd = write.open_table(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?;
            let mut rev = write.open_table(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?;
            fwd.insert(forward.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            rev.insert(reverse.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            Ok(())
        })
    }

    /// Remove only versions of the exact pre-DROP row identity. The bounded
    /// prefix seek avoids rewriting or gathering unrelated graph history.
    pub(crate) fn remove_graph_identity(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        source: NodeId, kind: &str, target: NodeId,
    ) -> Result<()> {
        self.require_original(original)?;
        if !self.graph_prepared()? {
            return Err(Error::Other("private graph DROP has no completed private image".into()));
        }
        self.select_graph_type(original, kind)?;
        self.retire_graph_occurrences(original, source, kind, target)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let length = kind.len().checked_add(33)
            .ok_or_else(|| Error::Other("working graph identity overflow".into()))?;
        frame.reserve(length.checked_mul(2)
            .ok_or_else(|| Error::Other("working graph identity overflow".into()))?)?;
        let mut prefix = Vec::with_capacity(length);
        prefix.extend_from_slice(source.as_bytes());
        prefix.extend_from_slice(target.as_bytes());
        prefix.extend_from_slice(kind.as_bytes());
        prefix.push(0);
        if prefix.capacity() != length {
            return Err(Error::Other("working graph identity capacity differs from admission".into()));
        }
        let upper = writer_source::prefix_end(&prefix);
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Included(prefix.as_slice()),
                |position| Bound::Excluded(position.key.as_slice()));
            let upper = upper.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
            let Some(window) = self.next_graph_edge(original, false, lower, upper)? else { break; };
            let next = OriginalPersistenceFrame::new(original)?;
            let key = next.copy_bytes(window.position())?;
            after = Some(GraphPosition { key, _owner: next.into_reservation() });
            // An edge type containing a NUL can share this byte prefix. The
            // decoded identity, not prefix inclusion, authorizes its removal.
            if window.edge.source != source || window.edge.target != target || window.edge.edge_type != kind {
                continue;
            }
            let removal = OriginalPersistenceFrame::new(original)?;
            let reverse = original_graph_key(&removal, &window.edge, true)?;
            self.with_write(|write| {
                write.open_table(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?
                    .remove(window.position()).map_err(RedbPersistence::storage_error)?;
                write.open_table(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?
                    .remove(reverse.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        Ok(())
    }

    pub(crate) fn apply_graph_replacements(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        deletes: &[(NodeId, String, NodeId, TxId)],
        inserts: &[AdjEntry],
    ) -> Result<()> {
        self.require_original(original)?;
        if deletes.is_empty() && inserts.is_empty() { return Ok(()); }
        if !self.graph_prepared()? { return Err(Error::Other("received graph source has no completed private image".into())); }
        for (source, kind, target, deleted_tx) in deletes {
            self.select_graph_type(original, kind)?;
            let frame = OriginalPersistenceFrame::new(original)?;
            let length = kind.len().checked_add(33).ok_or_else(|| Error::Other("working graph identity overflow".into()))?;
            frame.reserve(length.checked_mul(2).ok_or_else(|| Error::Other("working graph identity overflow".into()))?)?;
            let mut prefix = Vec::with_capacity(length);
            prefix.extend_from_slice(source.as_bytes()); prefix.extend_from_slice(target.as_bytes());
            prefix.extend_from_slice(kind.as_bytes()); prefix.push(0);
            if prefix.capacity() != length { return Err(Error::Other("working graph identity capacity differs from admission".into())); }
            let upper = writer_source::prefix_end(&prefix);
            let mut after = None::<GraphPosition>;
            loop {
                let lower = after.as_ref().map_or(Bound::Included(prefix.as_slice()), |position| Bound::Excluded(position.key.as_slice()));
                let upper = upper.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
                let Some(mut window) = self.next_graph_edge(original, false, lower, upper)? else { break; };
                let next = OriginalPersistenceFrame::new(original)?;
                let next_key = next.copy_bytes(window.position())?;
                after = Some(GraphPosition { key: next_key, _owner: next.into_reservation() });
                if window.edge.source == *source && window.edge.target == *target
                    && window.edge.edge_type == *kind && window.edge.deleted_tx.is_none() {
                    window.edge.deleted_tx = Some(*deleted_tx);
                    self.put_working_graph_edge(original, &window.edge)?;
                }
                // Only a continuation key survives this iteration, not the
                // previous decoded body alongside the next window.
            }
        }
        for edge in inserts {
            self.select_graph_type(original, &edge.edge_type)?;
            self.put_working_graph_edge(original, edge)?;
        }
        Ok(())
    }
}

pub(super) fn next_graph_in_write(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction,
    reverse: bool,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<Option<WorkingEdge>> {
    next_graph_in_table(original, write, if reverse { WORKING_GRAPH_REV } else { WORKING_GRAPH_FWD }, reverse, lower, upper)
}

fn next_graph_in_table(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
    reverse: bool,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<Option<WorkingEdge>> {
    let frame = OriginalPersistenceFrame::new(original)?;
    let (edge, key, encoded) = {
        let table = write.open_table(definition)
            .map_err(RedbPersistence::storage_error)?;
        let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
        let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
        let edge = frame.decode::<AdjEntry>(body.value())?;
        if !graph_key_matches(key.value(), &edge, reverse) {
            return Err(Error::StoreCorrupted { path: "received working graph".into(), reason: "private edge key and body disagree".into() });
        }
        let key = frame.copy_bytes(key.value())?;
        let encoded = frame.copy_bytes(body.value())?;
        (edge, key, encoded)
    };
    Ok(Some(WorkingEdge { edge, key, encoded, _owner: frame.into_reservation() }))
}

/// Consumer callbacks run after both native table/range guards close. Each
/// original window retains decoded data, exact source bytes and continuation.
pub(super) fn visit_graph_in_write(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction,
    selected_types: Option<&HashSet<String>>,
    mut visit: impl FnMut(&AdjEntry, &[u8], &[u8]) -> Result<()>,
) -> Result<()> {
    let mut after = None::<GraphPosition>;
    loop {
        let lower = after.as_ref().map_or(Bound::Unbounded, |position| Bound::Excluded(position.key.as_slice()));
        let Some(window) = next_graph_in_write(original, write, false, lower, Bound::Unbounded)? else { break; };
        let next = OriginalPersistenceFrame::new(original)?;
        let key = next.copy_bytes(window.position())?;
        after = Some(GraphPosition { key, _owner: next.into_reservation() });
        if selected_types.is_none_or(|types| types.contains(&window.value().edge_type)) {
            visit(window.value(), window.position(), window.encoded())?;
        }
    }
    Ok(())
}

impl ReceivedWorkingWrite {
    pub(crate) fn visit_graph_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        selected: Option<&HashSet<String>>,
        visit: impl FnMut(&AdjEntry, &[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.require_original(original)?;
        if !self.graph_prepared()? { return Err(Error::Other("received graph source has no completed private image".into())); }
        self.with_write(|write| visit_graph_in_write(original, write, selected, visit))
    }
}

/// A separate destination prevents a rewritten key from being visited again,
/// and preserves source ordering when receiver coordinates reorder versions.
const WORKING_GRAPH_REWRITE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_rewrite");

pub(crate) enum GraphRewrite { Keep, Replace, Remove }

impl ReceivedWorkingWrite {
    pub(crate) fn rewrite_graph(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        mut transform: impl FnMut(&mut AdjEntry) -> Result<GraphRewrite>,
    ) -> Result<()> {
        self.require_original(original)?;
        if !self.graph_prepared()? { return Err(Error::Other("received graph rewrite has no completed private image".into())); }
        self.require_graph_mutable()?;
        let rewrite_generation = self.graph_state.lock().generation;
        let name_frame = OriginalPersistenceFrame::new(original)?;
        name_frame.reserve(WORKING_GRAPH_REWRITE.name().len())?;
        let name = crate::memory_accounting::preparation_copy_string(WORKING_GRAPH_REWRITE.name())?;
        self.keep_scratch_name(name, name_frame.into_reservation())?;
        self.with_write(|write| {
            match write.delete_table(WORKING_GRAPH_REWRITE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            }
            write.open_table(WORKING_GRAPH_REWRITE).map(|_| ()).map_err(RedbPersistence::storage_error)
        })?;
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |position| Bound::Excluded(position.key.as_slice()));
            let Some(mut window) = self.next_graph_edge(original, false, lower, Bound::Unbounded)? else { break; };
            let next = OriginalPersistenceFrame::new(original)?;
            let key = next.copy_bytes(window.position())?;
            after = Some(GraphPosition { key, _owner: next.into_reservation() });
            let frame = OriginalPersistenceFrame::new(original)?;
            let rewritten;
            let encoded;
            let action = transform(&mut window.edge)?;
            // Reentry may freeze/reset the source or catch a latched failure.
            // Check before ANY post-callback write, including rewrite scratch.
            super::received_working_source::graph_witness_loan(self, original)?;
            self.require_graph_mutable()?;
            if self.graph_state.lock().generation != rewrite_generation {
                return Err(Error::Other("working graph reset during rewrite".into()));
            }
            let (key, body) = match action {
                GraphRewrite::Keep => (window.key.as_slice(), window.encoded.as_slice()),
                GraphRewrite::Remove => continue,
                GraphRewrite::Replace => {
                    rewritten = original_graph_key(&frame, &window.edge, false)?;
                    encoded = frame.encode(&window.edge)?;
                    (rewritten.as_slice(), encoded.as_slice())
                }
            };
            self.with_write(|write| {
                write.open_table(WORKING_GRAPH_REWRITE).map_err(RedbPersistence::storage_error)?
                    .insert(key, body).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        // The complete replacement exists before either old direction goes.
        self.require_graph_mutable()?;
        self.graph_became_unready();
        self.with_write(|write| {
            write.delete_table(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?;
            write.delete_table(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?;
            write.open_table(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?;
            write.open_table(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?;
            Ok(())
        })?;
        after = None;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |position| Bound::Excluded(position.key.as_slice()));
            let Some(window) = self.with_write(|write| next_graph_in_table(original, write,
                WORKING_GRAPH_REWRITE, false, lower, Bound::Unbounded))? else { break; };
            let next = OriginalPersistenceFrame::new(original)?;
            let key = next.copy_bytes(window.position())?;
            after = Some(GraphPosition { key, _owner: next.into_reservation() });
            let frame = OriginalPersistenceFrame::new(original)?;
            let reverse = original_graph_key(&frame, &window.edge, true)?;
            self.with_write(|write| {
                write.open_table(WORKING_GRAPH_FWD).map_err(RedbPersistence::storage_error)?
                    .insert(window.position(), window.encoded()).map_err(RedbPersistence::storage_error)?;
                write.open_table(WORKING_GRAPH_REV).map_err(RedbPersistence::storage_error)?
                    .insert(reverse.as_slice(), window.encoded()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        self.graph_became_ready();
        Ok(())
    }
}

// Internal, transaction-local bookkeeping. No table is a published graph owner.
const WORKING_GRAPH_TYPES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_types");
const WORKING_GRAPH_CREATIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_creations");
const WORKING_GRAPH_DELETIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_deletions");

pub(super) struct WorkingGraphState {
    prepared: bool,
    generation: u64,
    next_occurrence: u64,
    dirty: bool,
    frozen: bool,
    undo: u64,
    checkpoints: u64,
    capturing: bool,
    declarations_owner: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
impl WorkingGraphState {
    pub(super) fn new() -> Self {
        Self { prepared: false, generation: 0, next_occurrence: 0, dirty: false, frozen: false, undo: 0, checkpoints: 0, capturing: false, declarations_owner: None }
    }
    pub(super) fn reset(&mut self) -> Result<()> {
        // Do not reuse an old capture even if newly empty table roots match.
        let generation = self.generation.checked_add(1)
            .ok_or_else(|| Error::Other("working graph generation overflow".into()))?;
        *self = Self { generation, ..Self::new() };
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum WorkingGraphOrigin {
    CommittedCarry,
    Received { source_lsn: Lsn },
    Callback { phase_position: Lsn, authored: bool },
}
/// Allocate before a pending creation can be coalesced by logical identity.
/// This scalar witness never substitutes for retaining the occurrence's body.
#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkingGraphOccurrence {
    generation: u64,
    write_identity: u64,
    loan_identity: Option<u64>,
    ordinal: u64,
    origin: WorkingGraphOrigin,
}
impl WorkingGraphOccurrence {
    pub(crate) fn ordinal(&self) -> u64 { self.ordinal }
    pub(crate) fn origin(&self) -> WorkingGraphOrigin { self.origin }
}
#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkingGraphCoordinate {
    pub(crate) occurrence: WorkingGraphOccurrence,
    pub(crate) destination: Option<(TxId, Lsn)>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkingGraphCapture {
    original_address: usize,
    write_identity: u64,
    loan_identity: Option<u64>,
    roots: [Option<Option<[u8; 32]>>; 11],
    generation: u64,
    next_occurrence: u64,
    dirty: bool,
}
/// The scratch relation owns persistent occurrence facts. Returned witnesses
/// acquire source authority only after these facts validate against this write.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredGraphCoordinate {
    write_identity: u64,
    generation: u64,
    ordinal: u64,
    origin: WorkingGraphOrigin,
    destination: Option<(TxId, Lsn)>,
}
impl WorkingGraphCoordinate {
    fn stored(&self) -> StoredGraphCoordinate {
        StoredGraphCoordinate {
            write_identity: self.occurrence.write_identity,
            generation: self.occurrence.generation,
            ordinal: self.occurrence.ordinal,
            origin: self.occurrence.origin,
            destination: self.destination,
        }
    }
}
struct WorkingGraphRecord {
    key: Vec<u8>,
    body: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl ReceivedWorkingWrite {
    fn prepare_graph_state(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        self.require_original(original)?;
        if self.graph_state.lock().prepared { return Ok(()); }
        for definition in [WORKING_GRAPH_TYPES, WORKING_GRAPH_CREATIONS, WORKING_GRAPH_DELETIONS, WORKING_GRAPH_BODIES, WORKING_GRAPH_PENDING, WORKING_GRAPH_UNDO, WORKING_GRAPH_KEY_MAP, WORKING_GRAPH_INSTALLED, WORKING_GRAPH_EVENTS, WORKING_GRAPH_LAST] {
            let frame = OriginalPersistenceFrame::new(original)?;
            let name = frame.copy_text(definition.name())?;
            // Track before opening: even an interrupted preparation is discarded.
            self.keep_scratch_name(name, frame.into_reservation())?;
            self.with_write(|write| write.open_table(definition).map(|_| ()).map_err(RedbPersistence::storage_error))?;
        }
        self.graph_state.lock().prepared = true;
        Ok(())
    }
    fn require_graph_mutable(&self) -> Result<()> {
        let state = self.graph_state.lock();
        if !state.prepared || state.frozen {
            return Err(Error::Other("working graph bookkeeping is unavailable for mutation".into()));
        }
        Ok(())
    }
    pub(crate) fn select_graph_type(&self, original: &crate::memory_accounting::OwnedMemoryReservation, kind: &str) -> Result<()> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = frame.copy_bytes(kind.as_bytes())?;
        self.graph_record_put(original, 0, &key, Some(&[]))?;
        self.graph_state.lock().dirty = true;
        Ok(())
    }
    pub(crate) fn graph_type_contains_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation, kind: &str) -> Result<bool> {
        self.require_original(original)?;
        self.with_write(|write| {
            let table = write.open_table(WORKING_GRAPH_TYPES).map_err(RedbPersistence::storage_error)?;
            let found = table.get(kind.as_bytes()).map_err(RedbPersistence::storage_error)?.is_some();
            Ok(found)
        })
    }
    fn next_graph_record(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        definition: TableDefinition<&[u8], &[u8]>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<Option<WorkingGraphRecord>> {
        self.require_original(original)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let record = self.with_write(|write| {
            let table = write.open_table(definition).map_err(RedbPersistence::storage_error)?;
            let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
            let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
            Ok(Some((frame.copy_bytes(key.value())?, frame.copy_bytes(body.value())?)))
        })?;
        Ok(record.map(|(key, body)| WorkingGraphRecord { key, body, _owner: frame.into_reservation() }))
    }
    pub(crate) fn visit_graph_types_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        self.require_original(original)?;
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(record) = self.next_graph_record(original, WORKING_GRAPH_TYPES, lower, Bound::Unbounded)? else { break; };
            let kind = std::str::from_utf8(&record.key).map_err(|_| Error::StoreCorrupted {
                path: "received working graph selection".into(), reason: "invalid exact type bytes".into() })?;
            if !record.body.is_empty() { return Err(Error::StoreCorrupted {
                path: "received working graph selection".into(), reason: "invalid selection marker".into() }); }
            visit(kind)?; // All native guards closed; only this name is resident.
            let next = OriginalPersistenceFrame::new(original)?;
            after = Some(GraphPosition { key: next.copy_bytes(&record.key)?, _owner: next.into_reservation() });
        }
        Ok(())
    }
    pub(crate) fn allocate_graph_occurrence(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        origin: WorkingGraphOrigin) -> Result<WorkingGraphOccurrence> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        let mut state = self.graph_state.lock();
        let ordinal = state.next_occurrence;
        state.next_occurrence = ordinal.checked_add(1).ok_or_else(|| Error::Other("working graph occurrence overflow".into()))?;
        Ok(WorkingGraphOccurrence { generation: state.generation,
            write_identity: self.control_identity,
            loan_identity: super::received_working_source::graph_witness_loan(self, original)?, ordinal, origin })
    }
    /// Creation uses its exact forward physical key. Deletion uses the affected
    /// predecessor's exact key, never the newest edge with the same identity.
    pub(crate) fn record_graph_coordinate(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        predecessor_or_creation: &AdjEntry, deletion: bool, coordinate: WorkingGraphCoordinate) -> Result<()> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        {
            let state = self.graph_state.lock();
            if coordinate.occurrence.generation != state.generation
                || coordinate.occurrence.write_identity != self.control_identity
                || coordinate.occurrence.loan_identity != super::received_working_source::graph_witness_loan(self, original)?
                || coordinate.occurrence.ordinal >= state.next_occurrence {
                return Err(Error::Other("working graph coordinate has no current occurrence".into()));
            }
        }
        self.select_graph_type(original, &predecessor_or_creation.edge_type)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = original_graph_key(&frame, predecessor_or_creation, false)?;
        let encoded = frame.encode(&coordinate.stored())?;
        self.with_write(|write| {
            let mut table = write.open_table(if deletion { WORKING_GRAPH_DELETIONS } else { WORKING_GRAPH_CREATIONS })
                .map_err(RedbPersistence::storage_error)?;
            {
                let existing = table.get(key.as_slice()).map_err(RedbPersistence::storage_error)?;
                if let Some(existing) = existing {
                    if existing.value() != encoded.as_slice() {
                        return Err(Error::Other("conflicting coordinates for one physical graph version".into()));
                    }
                    return Ok(());
                }
            }
            Ok(())
        })?;
        self.graph_record_put(original, if deletion { 2 } else { 1 }, &key, Some(&encoded))
    }
    pub(crate) fn graph_coordinate_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        edge: &AdjEntry, deletion: bool) -> Result<Option<WorkingGraphCoordinate>> {
        self.require_original(original)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = original_graph_key(&frame, edge, false)?;
        let record = self.next_graph_record(original, if deletion { WORKING_GRAPH_DELETIONS } else { WORKING_GRAPH_CREATIONS },
            Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))?;
        record.map(|record| self.decode_graph_coordinate(original, &frame, &record.body)).transpose()
    }
    fn decode_graph_coordinate(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        frame: &OriginalPersistenceFrame<'_>, body: &[u8]) -> Result<WorkingGraphCoordinate> {
        self.require_original(original)?;
        let stored: StoredGraphCoordinate = frame.decode(body)?;
        let state = self.graph_state.lock();
        if !state.prepared || stored.write_identity != self.control_identity
            || stored.generation != state.generation || stored.ordinal >= state.next_occurrence {
            return Err(Error::StoreCorrupted { path: "received working graph coordinates".into(),
                reason: "coordinate belongs to another occurrence scope".into() });
        }
        Ok(WorkingGraphCoordinate {
            occurrence: WorkingGraphOccurrence { write_identity: self.control_identity,
                loan_identity: super::received_working_source::graph_witness_loan(self, original)?,
                generation: stored.generation, ordinal: stored.ordinal, origin: stored.origin },
            destination: stored.destination,
        })
    }
    /// A root witness for subsequent current-prefix validation and terminal
    /// selection. It does not activate either consumer by itself.
    pub(crate) fn capture_graph(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<WorkingGraphCapture> {
        self.require_original(original)?;
        if !self.graph_prepared()? || !self.graph_state.lock().prepared {
            return Err(Error::Other("working graph capture has no completed image".into()));
        }
        let roots = self.with_write(|write| {
            let mut roots = [None; 11];
            for (slot, definition) in roots.iter_mut().zip([WORKING_GRAPH_TYPES, WORKING_GRAPH_CREATIONS,
                WORKING_GRAPH_DELETIONS, WORKING_GRAPH_FWD, WORKING_GRAPH_REV, WORKING_GRAPH_BODIES, WORKING_GRAPH_PENDING, WORKING_GRAPH_KEY_MAP, WORKING_GRAPH_INSTALLED, WORKING_GRAPH_EVENTS, WORKING_GRAPH_LAST]) {
                *slot = write.normal_table_root(definition.name()).map_err(RedbPersistence::storage_error)?;
                if slot.is_none() { return Err(Error::Other("working graph capture lost a scratch table".into())); }
            }
            Ok(roots)
        })?;
        let state = self.graph_state.lock();
        Ok(WorkingGraphCapture { original_address: original as *const _ as usize,
            write_identity: self.control_identity,
            loan_identity: super::received_working_source::graph_witness_loan(self, original)?, roots, generation: state.generation, next_occurrence: state.next_occurrence, dirty: state.dirty })
    }
    pub(crate) fn check_graph_capture(&self, original: &crate::memory_accounting::OwnedMemoryReservation, captured: &WorkingGraphCapture) -> Result<()> {
        if self.capture_graph(original)? != *captured {
            return Err(Error::Other("working graph changed after capture".into()));
        }
        Ok(())
    }
    pub(crate) fn freeze_graph_selection(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<WorkingGraphCapture> {
        if self.graph_state.lock().checkpoints != 0 { return Err(Error::Other("graph freeze retained a statement checkpoint".into())); }
        let capture = self.capture_graph(original)?;
        self.graph_state.lock().frozen = true;
        Ok(capture)
    }
}

impl ReceivedWorkingWrite {
    /// Terminal controls retain the write and authentic Original, not a loan
    /// token whose synchronous callback scope has already ended.
    pub(crate) fn selected_graph_type_original(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        selection: &WorkingGraphCapture, kind: &str) -> Result<bool> {
        self.check_graph_capture(original, selection)?;
        let selected = self.graph_type_contains_original(original, kind)?;
        self.check_graph_capture(original, selection)?;
        Ok(selected)
    }
    pub(crate) fn visit_selected_graph_original(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        selection: &WorkingGraphCapture,
        mut visit: impl FnMut(&AdjEntry, &[u8], &[u8]) -> Result<()>) -> Result<()> {
        self.check_graph_capture(original, selection)?;
        self.visit_graph_original(original, None, |edge, key, encoded| {
            if self.selected_graph_type_original(original, selection, &edge.edge_type)? {
                visit(edge, key, encoded)?;
            }
            self.check_graph_capture(original, selection)
        })?;
        self.check_graph_capture(original, selection)
    }
    pub(crate) fn graph_selection_dirty(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<bool> {
        self.require_original(original)?;
        Ok(self.graph_state.lock().dirty)
    }
}

impl ReceivedWorkingWrite {
    pub(crate) fn visit_graph_coordinates_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        deletion: bool, mut visit: impl FnMut(&[u8], WorkingGraphCoordinate) -> Result<()>) -> Result<()> {
        self.require_original(original)?;
        let captured = self.capture_graph(original)?;
        let definition = if deletion { WORKING_GRAPH_DELETIONS } else { WORKING_GRAPH_CREATIONS };
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(record) = self.next_graph_record(original, definition, lower, Bound::Unbounded)? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let coordinate = self.decode_graph_coordinate(original, &frame, &record.body)?;
            visit(&record.key, coordinate)?;
            self.check_graph_capture(original, &captured)?;
            let next = OriginalPersistenceFrame::new(original)?;
            after = Some(GraphPosition { key: next.copy_bytes(&record.key)?, _owner: next.into_reservation() });
        }
        Ok(())
    }
}

// Bodies of accepted occurrences live in native scratch, before the pending
// logical-identity overlay can replace or delete them. They are never a second
// resident history image.
const WORKING_GRAPH_BODIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_bodies");
const WORKING_GRAPH_PENDING: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_pending");
const WORKING_GRAPH_UNDO: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_undo");
#[derive(Clone, Copy)]
pub(crate) struct WorkingGraphCheckpoint {
    generation: u64,
    undo: u64,
    occurrence: u64,
    dirty: bool,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct GraphUndo { table: u8, key: Vec<u8>, body: Option<Vec<u8>> }
fn bookkeeping_table(index: u8) -> Result<TableDefinition<'static, &'static [u8], &'static [u8]>> {
    match index {
        0 => Ok(WORKING_GRAPH_TYPES), 1 => Ok(WORKING_GRAPH_CREATIONS),
        2 => Ok(WORKING_GRAPH_DELETIONS), 3 => Ok(WORKING_GRAPH_BODIES),
        4 => Ok(WORKING_GRAPH_PENDING), 5 => Ok(WORKING_GRAPH_LAST),
        _ => Err(Error::Other("invalid working graph undo table".into())),
    }
}
impl ReceivedWorkingWrite {
    fn graph_record_put(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        index: u8, key: &[u8], body: Option<&[u8]>) -> Result<()> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        let definition = bookkeeping_table(index)?;
        let undo = {
            let mut state = self.graph_state.lock();
            if state.checkpoints == 0 { None } else {
                let ordinal = state.undo;
                state.undo = ordinal.checked_add(1).ok_or_else(|| Error::Other("working graph undo overflow".into()))?;
                Some(ordinal)
            }
        };
        let frame = OriginalPersistenceFrame::new(original)?;
        if let Some(ordinal) = undo {
            let key = frame.copy_bytes(key)?;
            let before = self.with_write(|write| {
                let table = write.open_table(definition).map_err(RedbPersistence::storage_error)?;
                table.get(key.as_slice()).map_err(RedbPersistence::storage_error)?
                    .map(|v| frame.copy_bytes(v.value())).transpose()
            })?;
            let before = GraphUndo { table: index, key, body: before };
            let encoded = frame.encode(&before)?;
            self.with_write(|write| {
                write.open_table(WORKING_GRAPH_UNDO).map_err(RedbPersistence::storage_error)?
                    .insert(ordinal.to_be_bytes().as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        self.with_write(|write| {
            let mut table = write.open_table(definition).map_err(RedbPersistence::storage_error)?;
            if let Some(body) = body { table.insert(key, body).map_err(RedbPersistence::storage_error)?; }
            else { table.remove(key).map_err(RedbPersistence::storage_error)?; }
            Ok(())
        })
    }
    pub(crate) fn checkpoint_graph(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<WorkingGraphCheckpoint> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        let mut state = self.graph_state.lock();
        state.checkpoints = state.checkpoints.checked_add(1).ok_or_else(|| Error::Other("working graph checkpoint overflow".into()))?;
        Ok(WorkingGraphCheckpoint { generation: state.generation, undo: state.undo,
            occurrence: state.next_occurrence, dirty: state.dirty })
    }
    pub(crate) fn settle_graph_checkpoint(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        checkpoint: WorkingGraphCheckpoint, restore: bool) -> Result<()> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        {
            let state = self.graph_state.lock();
            if state.generation != checkpoint.generation || state.checkpoints == 0 || state.undo < checkpoint.undo {
                return Err(Error::Other("working graph checkpoint is stale".into()));
            }
        }
        if restore {
            loop {
                let ordinal = self.graph_state.lock().undo;
                if ordinal == checkpoint.undo { break; }
                let ordinal = ordinal - 1;
                let frame = OriginalPersistenceFrame::new(original)?;
                let record = self.next_graph_record(original, WORKING_GRAPH_UNDO,
                    Bound::Included(ordinal.to_be_bytes().as_slice()), Bound::Included(ordinal.to_be_bytes().as_slice()))?
                    .ok_or_else(|| Error::Other("working graph undo lost its original body".into()))?;
                let undo: GraphUndo = frame.decode(&record.body)?;
                self.with_write(|write| {
                    let mut table = write.open_table(bookkeeping_table(undo.table)?).map_err(RedbPersistence::storage_error)?;
                    if let Some(body) = &undo.body { table.insert(undo.key.as_slice(), body.as_slice()).map_err(RedbPersistence::storage_error)?; }
                    else { table.remove(undo.key.as_slice()).map_err(RedbPersistence::storage_error)?; }
                    drop(table);
                    write.open_table(WORKING_GRAPH_UNDO).map_err(RedbPersistence::storage_error)?
                        .remove(ordinal.to_be_bytes().as_slice()).map_err(RedbPersistence::storage_error)?;
                    Ok(())
                })?;
                self.graph_state.lock().undo = ordinal;
            }
        }
        let mut state = self.graph_state.lock();
        state.checkpoints -= 1;
        if restore { state.next_occurrence = checkpoint.occurrence; state.dirty = checkpoint.dirty; }
        Ok(())
    }
    fn pending_graph_key(frame: &OriginalPersistenceFrame<'_>, source: NodeId, kind: &str, target: NodeId) -> Result<Vec<u8>> {
        frame.encode(&(source, kind, target))
    }
    pub(crate) fn pending_graph_coordinate(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        edge: &AdjEntry) -> Result<Option<WorkingGraphCoordinate>> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = Self::pending_graph_key(&frame, edge.source, &edge.edge_type, edge.target)?;
        let Some(pending) = self.next_graph_record(original, WORKING_GRAPH_PENDING,
            Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))? else { return Ok(None); };
        let Some(body) = self.next_graph_record(original, WORKING_GRAPH_BODIES,
            Bound::Included(pending.body.as_slice()), Bound::Included(pending.body.as_slice()))? else {
                return Err(Error::Other("pending graph occurrence lost its body".into()));
            };
        let body: AdjEntry = frame.decode(&body.body)?;
        self.graph_coordinate_original(original, &body, false)
    }
    pub(crate) fn retain_graph_mutation(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        origin: WorkingGraphOrigin, source: NodeId, kind: &str, target: NodeId,
        after: Option<&AdjEntry>, deleted_tx: TxId) -> Result<()> {
        self.require_original(original)?;
        let occurrence = self.allocate_graph_occurrence(original, origin)?;
        let coordinate = WorkingGraphCoordinate { occurrence, destination: None };
        self.select_graph_type(original, kind)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let identity = Self::pending_graph_key(&frame, source, kind, target)?;
        let encoded_coordinate = frame.encode(&coordinate.stored())?;
        self.graph_record_put(original, 5, &identity, Some(&encoded_coordinate))?;
        if let Some(pending) = self.next_graph_record(original, WORKING_GRAPH_PENDING,
            Bound::Included(identity.as_slice()), Bound::Included(identity.as_slice()))? {
            let body = self.next_graph_record(original, WORKING_GRAPH_BODIES,
                Bound::Included(pending.body.as_slice()), Bound::Included(pending.body.as_slice()))?
                .ok_or_else(|| Error::Other("graph predecessor occurrence body is missing".into()))?;
            let mut edge: AdjEntry = frame.decode(&body.body)?;
            self.record_graph_coordinate(original, &edge, true, coordinate)?;
            edge.deleted_tx = Some(deleted_tx);
            let encoded = frame.encode(&edge)?;
            self.graph_record_put(original, 3, &body.key, Some(&encoded))?;
            self.graph_record_put(original, 4, &identity, None)?;
        } else {
            // Select exact physical predecessors; a shared logical identity is
            // never used later to guess which retained lifetime was deleted.
            let length = kind.len().checked_add(33).ok_or_else(|| Error::Other("graph predecessor prefix overflow".into()))?;
            frame.reserve(length.checked_mul(2).ok_or_else(|| Error::Other("graph predecessor prefix overflow".into()))?)?;
            let mut prefix = Vec::with_capacity(length);
            prefix.extend_from_slice(source.as_bytes()); prefix.extend_from_slice(target.as_bytes());
            prefix.extend_from_slice(kind.as_bytes()); prefix.push(0);
            let upper = writer_source::prefix_end(&prefix);
            let mut position = None::<GraphPosition>;
            loop {
                let lower = position.as_ref().map_or(Bound::Included(prefix.as_slice()), |p| Bound::Excluded(p.key.as_slice()));
                let upper = upper.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.as_slice()));
                let Some(edge) = self.next_graph_edge(original, false, lower, upper)? else { break; };
                let next = OriginalPersistenceFrame::new(original)?;
                position = Some(GraphPosition { key: next.copy_bytes(edge.position())?, _owner: next.into_reservation() });
                if edge.edge.source == source && edge.edge.target == target && edge.edge.edge_type == kind
                    && edge.edge.deleted_tx.is_none() && self.graph_coordinate_original(original, &edge.edge, true)?.is_none() {
                    self.record_graph_coordinate(original, &edge.edge, true, coordinate)?;
                }
            }
        }
        if let Some(after) = after {
            let encoded = frame.encode(after)?;
            let mut edge: AdjEntry = frame.decode(&encoded)?;
            // Private-only coordinates are confined to scratch until restamp.
            edge.created_tx = TxId(u64::MAX);
            edge.lsn = Lsn(occurrence.ordinal.checked_add(1).ok_or_else(|| Error::Other("graph occurrence overflow".into()))?);
            edge.deleted_tx = None;
            let key = original_graph_key(&frame, &edge, false)?;
            self.record_graph_coordinate(original, &edge, false, coordinate)?;
            let encoded = frame.encode(&edge)?;
            self.graph_record_put(original, 3, &key, Some(&encoded))?;
            self.graph_record_put(original, 4, &identity, Some(&key))?;
        }
        Ok(())
    }
    pub(crate) fn restamp_graph_occurrences(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        receiver: TxId, mut position: impl FnMut(WorkingGraphOrigin) -> Result<Lsn>) -> Result<()> {
        self.rewrite_graph(original, |edge| {
            if let Some(mut deletion) = self.graph_coordinate_original(original, edge, true)? {
                let frame = OriginalPersistenceFrame::new(original)?;
                let key = original_graph_key(&frame, edge, false)?;
                deletion.destination = Some((receiver, position(deletion.occurrence.origin)?));
                let encoded = frame.encode(&deletion.stored())?;
                self.graph_record_put(original, 2, &key, Some(&encoded))?;
                edge.deleted_tx = Some(receiver);
                Ok(GraphRewrite::Replace)
            } else { Ok(GraphRewrite::Keep) }
        })?;
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(record) = self.next_graph_record(original, WORKING_GRAPH_BODIES, lower, Bound::Unbounded)? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let mut edge: AdjEntry = frame.decode(&record.body)?;
            let mut creation = self.graph_coordinate_original(original, &edge, false)?
                .ok_or_else(|| Error::Other("graph body has no exact creation occurrence".into()))?;
            let mut deletion = self.graph_coordinate_original(original, &edge, true)?;
            edge.created_tx = receiver;
            edge.lsn = position(creation.occurrence.origin)?;
            creation.destination = Some((receiver, edge.lsn));
            let encoded = frame.encode(&creation.stored())?;
            self.graph_record_put(original, 1, &record.key, Some(&encoded))?;
            if let Some(deletion) = deletion.as_mut() {
                deletion.destination = Some((receiver, position(deletion.occurrence.origin)?));
                let encoded = frame.encode(&deletion.stored())?;
                self.graph_record_put(original, 2, &record.key, Some(&encoded))?;
            }
            edge.deleted_tx = deletion.map(|_| receiver);
            let key = original_graph_key(&frame, &edge, false)?;
            let installed = self.next_graph_record(original, WORKING_GRAPH_INSTALLED,
                Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))?;
            if installed.is_none() && self.next_graph_edge(original, false,
                Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))?.is_some() {
                return Err(Error::Other("graph occurrence collides with a committed physical version".into()));
            }
            if let Some(installed) = installed {
                let prior: u64 = frame.decode(&installed.body)?;
                if prior >= creation.occurrence.ordinal { return Err(Error::Other("graph occurrence destination order reversed".into())); }
            }
            // Same source position and identity is one authored occurrence;
            // helper mutations within it coalesce in accepted operation order.
            self.put_working_graph_edge(original, &edge)?;
            self.with_write(|write| {
                write.open_table(WORKING_GRAPH_KEY_MAP).map_err(RedbPersistence::storage_error)?
                    .insert(record.key.as_slice(), key.as_slice()).map_err(RedbPersistence::storage_error)?;
                let ordinal = frame.encode(&creation.occurrence.ordinal)?;
                write.open_table(WORKING_GRAPH_INSTALLED).map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), ordinal.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
            after = Some(GraphPosition { key: frame.copy_bytes(&record.key)?, _owner: frame.into_reservation() });
        }
        self.prepare_graph_events(original, receiver)?;
        Ok(())
    }
}

impl ReceivedWorkingWrite {
    pub(crate) fn begin_graph_capture(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        self.graph_state.lock().capturing = true;
        Ok(())
    }
    pub(crate) fn graph_capturing(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<bool> {
        self.require_original(original)?;
        Ok(self.graph_state.lock().capturing)
    }
}

const WORKING_GRAPH_KEY_MAP: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_key_map");
const WORKING_GRAPH_INSTALLED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_installed");
const WORKING_GRAPH_EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_events");
#[derive(serde::Serialize, serde::Deserialize)]
struct WorkingGraphEvent {
    entry: ChangeLogEntry,
    predecessor: Option<Vec<u8>>,
}
impl ReceivedWorkingWrite {
    pub(crate) fn retire_graph_occurrences(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        source: NodeId, kind: &str, target: NodeId) -> Result<()> {
        self.select_graph_type(original, kind)?;
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(record) = self.next_graph_record(original, WORKING_GRAPH_BODIES, lower, Bound::Unbounded)? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let edge: AdjEntry = frame.decode(&record.body)?;
            if edge.source == source && edge.target == target && edge.edge_type == kind {
                for index in [1, 2, 3] { self.graph_record_put(original, index, &record.key, None)?; }
            }
            after = Some(GraphPosition { key: frame.copy_bytes(&record.key)?, _owner: frame.into_reservation() });
        }
        let frame = OriginalPersistenceFrame::new(original)?;
        let identity = Self::pending_graph_key(&frame, source, kind, target)?;
        self.graph_record_put(original, 4, &identity, None)
    }
    fn put_graph_event(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        edge: &AdjEntry, lsn: Lsn, predecessor: Option<&[u8]>) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let name = frame.copy_text(&edge.edge_type)?;
        let entry = if predecessor.is_some() {
            ChangeLogEntry::EdgeDelete { source: edge.source, target: edge.target, edge_type: name, lsn }
        } else { ChangeLogEntry::EdgeInsert { source: edge.source, target: edge.target, edge_type: name, lsn } };
        let identity = Self::pending_graph_key(&frame, edge.source, &edge.edge_type, edge.target)?;
        let size = identity.len().checked_add(9).ok_or_else(|| Error::Other("graph event key overflow".into()))?;
        frame.reserve(size)?;
        let mut key = Vec::with_capacity(size);
        key.extend_from_slice(&lsn.0.to_be_bytes()); key.extend_from_slice(&identity); key.push(u8::from(predecessor.is_some()));
        let event = WorkingGraphEvent { entry, predecessor: predecessor.map(|key| frame.copy_bytes(key)).transpose()? };
        let body = frame.encode(&event)?;
        self.with_write(|write| {
            write.open_table(WORKING_GRAPH_EVENTS).map_err(RedbPersistence::storage_error)?
                .insert(key.as_slice(), body.as_slice()).map_err(RedbPersistence::storage_error)?;
            Ok(())
        })
    }
    fn prepare_graph_events(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        receiver: TxId) -> Result<()> {
        // Creation keys are already canonicalized. Stream occurrences and use
        // the exact source-to-destination map; no latest-identity body lookup.
        for deletion in [false, true] {
            let definition = if deletion { WORKING_GRAPH_DELETIONS } else { WORKING_GRAPH_CREATIONS };
            let mut after = None::<GraphPosition>;
            loop {
                let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
                let Some(record) = self.next_graph_record(original, definition, lower, Bound::Unbounded)? else { break; };
                let frame = OriginalPersistenceFrame::new(original)?;
                after = Some(GraphPosition { key: frame.copy_bytes(&record.key)?, _owner: frame.into_reservation() });
                let frame = OriginalPersistenceFrame::new(original)?;
                let coordinate = self.decode_graph_coordinate(original, &frame, &record.body)?;
                let key = record.key.as_slice();
                if matches!(coordinate.occurrence.origin, WorkingGraphOrigin::Callback { authored: false, .. }) { continue; }
                let mapped = self.next_graph_record(original, WORKING_GRAPH_KEY_MAP,
                    Bound::Included(key), Bound::Included(key))?;
                let physical = mapped.as_ref().map_or(key, |record| record.body.as_slice());
                let Some(edge) = self.next_graph_edge(original, false,
                    Bound::Included(physical), Bound::Included(physical))? else { continue; };
                if deletion && edge.edge.deleted_tx != Some(receiver) { continue; }
                let (tx, lsn) = coordinate.destination.ok_or_else(|| Error::Other("graph event lost its exact restamped coordinate".into()))?;
                if tx != receiver { return Err(Error::Other("graph event belongs to another receiver".into())); }
                self.put_graph_event(original, &edge.edge, lsn, deletion.then_some(physical))?;
            }
        }
        Ok(())
    }
    pub(crate) fn visit_graph_events_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        selection: &WorkingGraphCapture,
        mut visit: impl FnMut(&ChangeLogEntry, Option<&[u8]>) -> Result<()>) -> Result<()> {
        self.check_graph_capture(original, selection)?;
        let mut after = None::<GraphPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(record) = self.next_graph_record(original, WORKING_GRAPH_EVENTS, lower, Bound::Unbounded)? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let event: WorkingGraphEvent = frame.decode(&record.body)?;
            let kind = match &event.entry {
                ChangeLogEntry::EdgeInsert { edge_type, .. } | ChangeLogEntry::EdgeDelete { edge_type, .. } => edge_type,
                _ => return Err(Error::Other("non-graph entry in graph occurrence spool".into())),
            };
            if !self.selected_graph_type_original(original, selection, kind)? {
                return Err(Error::Other("graph event escaped its frozen selected type".into()));
            }
            visit(&event.entry, event.predecessor.as_deref())?;
            self.check_graph_capture(original, selection)?;
            after = Some(GraphPosition { key: frame.copy_bytes(&record.key)?, _owner: frame.into_reservation() });
        }
        Ok(())
    }
}

const WORKING_GRAPH_LAST: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_graph_last");
impl ReceivedWorkingWrite {
    pub(crate) fn last_graph_coordinate_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        source: NodeId, kind: &str, target: NodeId) -> Result<Option<WorkingGraphCoordinate>> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let identity = Self::pending_graph_key(&frame, source, kind, target)?;
        let record = self.next_graph_record(original, WORKING_GRAPH_LAST,
            Bound::Included(identity.as_slice()), Bound::Included(identity.as_slice()))?;
        record.map(|record| self.decode_graph_coordinate(original, &frame, &record.body)).transpose()
    }
}

impl ReceivedWorkingWrite {
    pub(crate) fn replace_graph_declarations_original(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        owner: &mut Option<crate::memory_accounting::OwnedMemoryReservation>)
        -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        self.require_original(original)?;
        self.require_graph_mutable()?;
        if owner.is_none() { return Err(Error::Other("private DAG declaration has no original invoice".into())); }
        // No fallible operation follows the transfer. The caller holds the old
        // invoice until the old executor declaration body has been destroyed.
        Ok(self.graph_state.lock().declarations_owner.replace(owner.take().expect("validated declaration original")))
    }
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
