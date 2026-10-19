//! Graph reads share one captured-root dispatch for writers and committed images.
//! Image cursors retain keys and one admitted edge window, never adjacency.
use super::*;
use contextdb_core::Direction;
use contextdb_core::read_memory::{OwnedReadBox, ReadCredit, ReadMemoryBudget};
use contextdb_graph::source::{
    GraphCursor, GraphCursorOwner, GraphEdgeWindow, GraphQuery, GraphReadSource, GraphSnapshot,
};
use std::ops::Bound;

const HINT: &str = "Raise the declared reader memory limit to read graph pages.";

#[derive(Clone)]
pub(crate) enum CapturedGraphReadRoot<R> {
    Writer(R),
    Image(Arc<RetainedReadSource>),
}
pub(crate) type GraphReadRoot = CapturedGraphReadRoot<Arc<WriterSnapshot>>;
pub(crate) type EngineGraphReadRoot = CapturedGraphReadRoot<WriterSnapshotHandle>;
impl<R> CapturedGraphReadRoot<R>
where R: Clone + Into<WriterSnapshotHandle> + std::ops::Deref<Target = WriterSnapshot> {

    pub(crate) fn frontier(&self) -> SnapshotId {
        match self {
            Self::Writer(root) => root.snapshot,
            Self::Image(source) => source.logical_frontier,
        }
    }
    pub(crate) fn snapshot(
        &self,
        snapshot: SnapshotId,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Arc<dyn GraphSnapshot>> {
        match self {
            Self::Writer(root) => {
                let root: WriterSnapshotHandle = root.clone().into();
                if snapshot.0 < root.snapshot.0 {
                    with_pages(memory.clone(), || {
                        let _credit = ReadCredit::try_new(
                            memory.clone(),
                            130,
                            "reader",
                            "graph_root_binding",
                            HINT,
                        )?;
                        RedbPersistence::read_authenticated_bounds_in_read(
                            root.owner_read()?.as_ref(),
                        )
                        .map(|_| ())
                    })?;
                }
                if snapshot.0 > root.snapshot.0 {
                    return Err(Error::Other(
                        "graph snapshot exceeds the captured source frontier".into()));
                }
                Ok(Arc::new(
                    writer_adapters::WriterGraphSnapshot::new_for_read(root.clone(), memory)?,
                ))
            }
            Self::Image(source) => RetainedGraphSource(source.clone()).capture(snapshot, memory),
        }
    }
    pub(crate) fn cursor(
        &self,
        reverse: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<GraphReadCursor> {
        self.cursor_lane(reverse, false, lower, upper, memory)
    }
    pub(crate) fn cursor_lane(
        &self,
        reverse: bool,
        row_owned: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<GraphReadCursor> {
        with_pages(memory.clone(), || {
            let _credit =
                ReadCredit::try_new(memory.clone(), 130, "reader", "graph_root_binding", HINT)?;
            match self {
                Self::Writer(root) => {
                let root: WriterSnapshotHandle = root.clone().into();
                    RedbPersistence::read_authenticated_bounds_in_read(root.owner_read()?.as_ref())
                        .map(|_| ())
                }
                Self::Image(source) => source.with_read(|read| {
                    RedbPersistence::read_authenticated_bounds_in_read(read).map(|_| ())
                }),
            }
        })?;
        match self {
            Self::Writer(root) => {
                let root: WriterSnapshotHandle = root.clone().into();
                // The concrete Box is admitted to the executing child before
                // allocation, and freed before this separate control credit.
                let control = credit(
                    memory.clone(),
                    std::mem::size_of::<writer_source::WriterReadCursor>(),
                )?;
                let cursor = root.cursor(
                    writer_source::SourceWindowOwner::Read(memory),
                    if row_owned {
                        if reverse {
                            "row_graph_rev"
                        } else {
                            "row_graph_fwd"
                        }
                    } else if reverse {
                        "graph_rev"
                    } else {
                        "graph_fwd"
                    },
                    writer_source::WriterCursorFamily::Bytes,
                    lower,
                    upper,
                    false,
                )?;
                Ok(GraphReadCursor::Writer(WriterGraphReadCursor {
                    cursor: Box::new(cursor),
                    row_owned,
                    reverse,
                    _control: control,
                }))
            }
            Self::Image(source) => Ok(GraphReadCursor::Image(RetainedGraphCursor::new_lane(
                source.clone(),
                reverse,
                row_owned,
                lower,
                upper,
                memory,
            )?)),
        }
    }
}

pub(crate) struct WriterGraphReadCursor {
    row_owned: bool,
    reverse: bool,
    cursor: Box<writer_source::WriterReadCursor>,
    _control: ReadCredit,
}

pub(crate) enum GraphReadCursor {
    Writer(WriterGraphReadCursor),
    Image(RetainedGraphCursor),
}
impl GraphReadCursor {
    pub(crate) fn next_edge_with(
        &mut self,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<GraphReadWindow>> {
        checkpoint()?;
        match self {
            Self::Writer(cursor) => {
                if cursor.row_owned {
                    cursor
                        .cursor
                        .next_row_edge_handoff_with(cursor.reverse, &mut || checkpoint(), Ok)
                        .map(|edge| edge.map(GraphReadWindow::WriterRow))
                } else {
                    cursor
                        .cursor
                        .next_edge_with(checkpoint)
                        .map(|edge| edge.map(GraphReadWindow::Writer))
                }
            }
            Self::Image(cursor) => cursor
                .next_window_with(checkpoint)
                .map(|edge| edge.map(GraphReadWindow::Image)),
        }
    }
}
pub(crate) enum GraphReadWindow {
    Writer(writer_source::WriterWindow<AdjEntry>),
    WriterRow(writer_source::WriterWindow<contextdb_tx::OwnedAdjEntry>),
    Image(RetainedGraphWindow),
}
impl GraphReadWindow {
    pub(crate) fn edge(&self) -> &AdjEntry {
        match self {
            Self::Writer(window) => &window.value,
            Self::WriterRow(window) => &window.value.entry,
            Self::Image(window) => &window.edge,
        }
    }
    pub(crate) fn row_owner(&self) -> Option<&contextdb_tx::RowEdgeOwner> {
        match self {
            Self::Writer(_) => None,
            Self::WriterRow(window) => Some(&window.value.owner),
            Self::Image(window) => window.row_owner.as_ref(),
        }
    }
    pub(crate) fn position(&self) -> &[u8] {
        match self {
            Self::Writer(window) => window.position(),
            Self::WriterRow(window) => window.position(),
            Self::Image(window) => &window.position,
        }
    }
}

pub(crate) struct RetainedGraphSource(pub(crate) Arc<RetainedReadSource>);
impl GraphReadSource for RetainedGraphSource {
    fn capture(
        &self,
        snapshot: SnapshotId,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Arc<dyn GraphSnapshot>> {
        if snapshot.0 > self.0.logical_frontier.0 {
            return Err(Error::Other(
                "graph snapshot exceeds the captured source frontier".into()));
        }
        if snapshot.0 < self.0.logical_frontier.0 {
            with_pages(memory.clone(), || {
                let _credit = credit(memory.clone(), 130)?;
                self.0.with_read(|read| {
                    RedbPersistence::read_authenticated_bounds_in_read(read).map(|_| ())
                })
            })?;
        }
        let control = credit(memory.clone(), std::mem::size_of::<RetainedGraphSnapshot>())?;
        Ok(Arc::new(RetainedGraphSnapshot {
            source: self.0.clone(),
            memory,
            _control: control,
        }))
    }
}
struct RetainedGraphSnapshot {
    source: Arc<RetainedReadSource>,
    memory: Arc<dyn ReadMemoryBudget>,
    _control: ReadCredit,
}
impl GraphSnapshot for RetainedGraphSnapshot {
    fn row_cursor(
        &self,
        query: GraphQuery<'_>,
        owner: &contextdb_tx::RowEdgeOwner,
    ) -> Result<GraphCursorOwner> {
        let reverse = match query.direction {
            Direction::Outgoing => false,
            Direction::Incoming => true,
            Direction::Both => {
                return Err(Error::Other(
                    "owned adjacency requires one direction".into()));
            }
        };
        let (other, kind) = query
            .identity
            .ok_or_else(|| Error::Other("owned adjacency requires an exact triad".into()))?;
        let funding = writer_source::SourceWindowOwner::Read(self.memory.clone());
        let table_len = u32::try_from(owner.table.len())
            .map_err(|_| Error::Other("owned table name overflow".into()))?
            .to_be_bytes();
        let key = funding.endpoint(
            &[
                query.node.as_bytes(),
                other.as_bytes(),
                kind.as_bytes(),
                &[0],
                &table_len,
                owner.table.as_bytes(),
                &owner.table_generation.to_be_bytes(),
                &owner.row_id.0.to_be_bytes(),
                &owner.created_tx.0.to_be_bytes(),
                &owner.lsn.0.to_be_bytes(),
            ],
            "row_graph_identity",
        )?;
        let control = credit(
            self.memory.clone(),
            std::mem::size_of::<RetainedGraphCursor>(),
        )?;
        let cursor = RetainedGraphCursor::new_lane(
            self.source.clone(),
            reverse,
            true,
            Bound::Included(&key.key),
            Bound::Included(&key.key),
            self.memory.clone(),
        )?;
        Ok(GraphCursorOwner::new(Box::new(cursor), control))
    }

    fn cursor(&self, query: GraphQuery<'_>) -> Result<GraphCursorOwner> {
        let reverse = match query.direction {
            Direction::Outgoing => false,
            Direction::Incoming => true,
            Direction::Both => {
                return Err(Error::Other(
                    "graph source requires a directional cursor".into()));
            }
        };
        let owner = writer_source::SourceWindowOwner::Read(self.memory.clone());
        let prefix = if let Some((other, kind)) = query.identity {
            owner.endpoint(
                &[
                    query.node.as_bytes(),
                    other.as_bytes(),
                    kind.as_bytes(),
                    &[0],
                ],
                "graph_cursor",
            )?
        } else {
            owner.endpoint(&[query.node.as_bytes()], "graph_cursor")?
        };
        let end = owner.prefix_end(&prefix.key, "graph_cursor")?;
        let cursor = RetainedGraphCursor::new(
            self.source.clone(),
            reverse,
            Bound::Included(&prefix.key),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            self.memory.clone(),
        )?;
        let row_cursor = RetainedGraphCursor::new_lane(
            self.source.clone(),
            reverse,
            true,
            Bound::Included(&prefix.key),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            self.memory.clone(),
        )?;
        let control = credit(
            self.memory.clone(),
            std::mem::size_of::<MergedImageGraphCursor>(),
        )?;
        Ok(GraphCursorOwner::new(
            Box::new(MergedImageGraphCursor {
                raw: cursor,
                row: row_cursor,
                raw_head: None,
                row_head: None,
                raw_done: false,
                row_done: false,
                reverse,
            }),
            control,
        ))
    }
}

struct MergedImageGraphCursor {
    raw: RetainedGraphCursor,
    row: RetainedGraphCursor,
    raw_head: Option<OwnedReadBox<dyn GraphEdgeWindow>>,
    row_head: Option<OwnedReadBox<dyn GraphEdgeWindow>>,
    raw_done: bool,
    row_done: bool,
    reverse: bool,
}
impl GraphCursor for MergedImageGraphCursor {
    fn next_source_window_bound(&self) -> usize {
        usize::from(!self.raw_done && self.raw_head.is_none())
            + usize::from(!self.row_done && self.row_head.is_none())
    }
    fn next_edge(&mut self) -> Result<Option<OwnedReadBox<dyn GraphEdgeWindow>>> {
        if !self.raw_done && self.raw_head.is_none() {
            self.raw_head = self.raw.next_edge()?;
            self.raw_done = self.raw_head.is_none();
        }
        if !self.row_done && self.row_head.is_none() {
            self.row_head = self.row.next_edge()?;
            self.row_done = self.row_head.is_none();
        }
        let take_raw = match (&self.raw_head, &self.row_head) {
            (Some(a), Some(b)) => {
                let a = a.edge();
                let b = b.edge();
                if self.reverse {
                    (a.target, a.source, &a.edge_type) <= (b.target, b.source, &b.edge_type)
                } else {
                    (a.source, a.target, &a.edge_type) <= (b.source, b.target, &b.edge_type)
                }
            }
            (Some(_), None) => true,
            _ => false,
        };
        Ok(if take_raw {
            self.raw_head.take()
        } else {
            self.row_head.take()
        })
    }
}

struct Key {
    bytes: Box<[u8]>,
    _credit: ReadCredit,
}
impl Key {
    fn new(bytes: &[u8], memory: Arc<dyn ReadMemoryBudget>) -> Result<Self> {
        let credit = credit(memory, bytes.len())?;
        Ok(Self {
            bytes: Box::from(bytes),
            _credit: credit,
        })
    }
}
fn credit(memory: Arc<dyn ReadMemoryBudget>, bytes: usize) -> Result<ReadCredit> {
    ReadCredit::try_new(memory, bytes, "graph_source", "read_window", HINT)
}
/// Admission for any stored page a persistence read faults in, graph or not.
/// It names the budget that funds the read, not a graph window.
fn page_credit(memory: Arc<dyn ReadMemoryBudget>, bytes: usize) -> Result<ReadCredit> {
    ReadCredit::try_new(
        memory,
        bytes,
        "storage_pages",
        "read_page",
        "Raise the memory limit that funds this read to read its stored pages.",
    )
}
fn owned_bound(bound: Bound<&[u8]>, memory: Arc<dyn ReadMemoryBudget>) -> Result<Bound<Key>> {
    Ok(match bound {
        Bound::Included(key) => Bound::Included(Key::new(key, memory)?),
        Bound::Excluded(key) => Bound::Excluded(Key::new(key, memory)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}
fn borrowed_bound(bound: &Bound<Key>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(key) => Bound::Included(&key.bytes),
        Bound::Excluded(key) => Bound::Excluded(&key.bytes),
        Bound::Unbounded => Bound::Unbounded,
    }
}
pub(crate) struct RetainedGraphCursor {
    row_owned: bool,
    source: Arc<RetainedReadSource>,
    reverse: bool,
    lower: Bound<Key>,
    upper: Bound<Key>,
    after: Option<Key>,
    finished: bool,
    memory: Arc<dyn ReadMemoryBudget>,
    _control: ReadCredit,
}
impl RetainedGraphCursor {
    fn new(
        source: Arc<RetainedReadSource>,
        reverse: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new_lane(source, reverse, false, lower, upper, memory)
    }
    fn new_lane(
        source: Arc<RetainedReadSource>,
        reverse: bool,
        row_owned: bool,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Self> {
        with_pages(memory.clone(), || {
            let _binding = credit(memory.clone(), 130)?;
            source.with_read(|read| {
                RedbPersistence::read_authenticated_bounds_in_read(read)?;
                if row_owned {
                    RedbPersistence::verify_history_root(read, "row_graph_fwd")?;
                    RedbPersistence::verify_history_root(read, "row_graph_rev")?;
                }
                Ok(())
            })
        })?;
        let control = credit(memory.clone(), std::mem::size_of::<Self>())?;
        let lower = owned_bound(lower, memory.clone())?;
        let upper = owned_bound(upper, memory.clone())?;
        Ok(Self {
            source,
            row_owned,
            reverse,
            lower,
            upper,
            after: None,
            finished: false,
            memory,
            _control: control,
        })
    }
    fn next_window(&mut self) -> Result<Option<RetainedGraphWindow>> {
        self.next_window_with(&|| Ok(()))
    }
    fn next_window_with(
        &mut self,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<RetainedGraphWindow>> {
        self.source
            .check_history_directory(self.memory.clone(), checkpoint)?;
        if self.finished {
            return Ok(None);
        }
        let lower = self.after.as_ref().map_or_else(
            || borrowed_bound(&self.lower),
            |key| Bound::Excluded(key.bytes.as_ref()),
        );
        let upper = borrowed_bound(&self.upper);
        checkpoint()?;
        let result = with_pages(self.memory.clone(), || {
            self.source.with_read(|read| {
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
                let definition = if self.row_owned {
                    if self.reverse {
                        super::row_graph::REVERSE
                    } else {
                        super::row_graph::FORWARD
                    }
                } else if self.reverse {
                    GRAPH_REV_TABLE
                } else {
                    GRAPH_FWD_TABLE
                };
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Err(self
                            .source
                            .content_identity_corruption("graph projection table missing"));
                    }
                    Err(error) => {
                        return Err(diagnosis(self.source.as_ref(), read_table_error(error)));
                    }
                };
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    redb::TableHandle::name(&definition),
                    "range",
                    &[],
                    None,
                    None,
                );
                let mut range = table
                    .range::<&[u8]>((lower, upper))
                    .map_err(|error| diagnosis(self.source.as_ref(), read_storage_error(error)))?;
                let Some((key, value)) = range
                    .next()
                    .transpose()
                    .map_err(|error| diagnosis(self.source.as_ref(), read_storage_error(error)))?
                else {
                    return Ok(None);
                };
                #[cfg(any(test, feature = "test-seams"))]
                {
                    crate::read_work_probe::note_graph(
                        redb::TableHandle::name(&definition),
                        "iteration_step",
                        key.value(),
                        None,
                        None,
                    );
                    crate::read_work_probe::note_graph(
                        redb::TableHandle::name(&definition),
                        "supplied_primary",
                        key.value(),
                        None,
                        None,
                    );
                }
                let memory = self.source.graph_window_memory(self.memory.clone());
                let (edge, row_owner) = memory.scope(|| -> Result<_> {
                    if self.row_owned {
                        RedbPersistence::verify_history_root(read, "row_graph_fwd")?;
                        RedbPersistence::verify_history_root(read, "row_graph_rev")?;
                        let _decode = crate::read_image_memory::ImageMemory::enter_decode_call()?;
                        let (owned, used): (contextdb_tx::OwnedAdjEntry, usize) =
                            crate::read_image_memory::decode(value.value())?;
                        super::row_graph::validate(&owned)?;
                        if used != value.value().len()
                            || RedbPersistence::row_graph_key(&owned, self.reverse)? != key.value()
                        {
                            return Err(self.source.content_identity_corruption(
                                "invalid row-derived adjacency key or value",
                            ));
                        }
                        let peer_key = RedbPersistence::row_graph_key(&owned, !self.reverse)?;
                        let peer = read
                            .open_table(if self.reverse {
                                super::row_graph::FORWARD
                            } else {
                                super::row_graph::REVERSE
                            })
                            .map_err(read_table_error)
                            .map_err(|e| diagnosis(self.source.as_ref(), e))?;
                        let matching = peer
                            .get(peer_key.as_slice())
                            .map_err(read_storage_error)
                            .map_err(|e| diagnosis(self.source.as_ref(), e))?
                            .is_some_and(|v| v.value() == value.value());
                        if !matching {
                            return Err(self.source.content_identity_corruption(
                                "row-derived reciprocal adjacency disagrees",
                            ));
                        }
                        Ok((owned.entry, Some(owned.owner)))
                    } else {
                        let edge = decode_edge(
                            self.source.as_ref(),
                            key.value(),
                            value.value(),
                            self.reverse,
                        )?;
                        self.source.verify_graph_edge_proof(
                            read,
                            key.value(),
                            value.value(),
                            &edge,
                            self.reverse,
                            self.memory.clone(),
                        )?;
                        Ok((edge, None))
                    }
                })?;
                let after = Key::new(key.value(), self.memory.clone())?;
                let position = Key::new(key.value(), self.memory.clone())?;

                Ok(Some((
                    after,
                    RetainedGraphWindow {
                        edge,
                        row_owner,
                        position: position.bytes,
                        _position_credit: position._credit,
                        _memory: memory,
                    },
                )))
            })
        })?;
        match result {
            Some((after, window)) => {
                self.after = Some(after);
                Ok(Some(window))
            }
            None => {
                self.finished = true;
                self.after = None;
                Ok(None)
            }
        }
    }
}
impl GraphCursor for RetainedGraphCursor {
    fn next_edge(&mut self) -> Result<Option<OwnedReadBox<dyn GraphEdgeWindow>>> {
        let Some(window) = self.next_window()? else {
            return Ok(None);
        };
        let control = credit(
            self.memory.clone(),
            std::mem::size_of::<RetainedGraphWindow>(),
        )?;
        Ok(Some(OwnedReadBox::new(
            Box::new(window) as Box<dyn GraphEdgeWindow>,
            control,
        )))
    }
}
pub(crate) struct RetainedGraphWindow {
    row_owner: Option<contextdb_tx::RowEdgeOwner>,
    edge: AdjEntry,
    position: Box<[u8]>,
    _position_credit: ReadCredit,
    _memory: Arc<crate::read_image_memory::ImageMemory>,
}
impl GraphEdgeWindow for RetainedGraphWindow {
    fn row_owner(&self) -> Option<&contextdb_tx::RowEdgeOwner> {
        self.row_owner.as_ref()
    }
    fn edge(&self) -> &AdjEntry {
        &self.edge
    }
}

trait GraphDiagnosis {
    fn corruption(&self, reason: String) -> Error;
    fn diagnose(&self, error: LoadReadImageError) -> Error;
}
impl GraphDiagnosis for RetainedReadSource {
    fn corruption(&self, reason: String) -> Error {
        self.content_identity_corruption(reason)
    }
    fn diagnose(&self, error: LoadReadImageError) -> Error {
        image_diagnosis(self, error)
    }
}
impl GraphDiagnosis for GraphProofContext<'_> {
    fn corruption(&self, reason: String) -> Error {
        self.content_identity_corruption(reason)
    }
    fn diagnose(&self, error: LoadReadImageError) -> Error {
        match self.image {
            Some(source) => image_diagnosis(source, error),
            None => self.content_identity_corruption(format!("{error:?}")),
        }
    }
}
fn diagnosis(source: &(impl GraphDiagnosis + ?Sized), error: LoadReadImageError) -> Error {
    source.diagnose(error)
}
pub(super) fn image_diagnosis(source: &RetainedReadSource, error: LoadReadImageError) -> Error {
    crate::database::ImageHistoryError::Store {
        path: source.database_path.display().to_string(),
        diagnosis: Box::new(error),
    }
    .into_engine_error()
}
fn decode_edge(
    source: &(impl GraphDiagnosis + ?Sized),
    key: &[u8],
    bytes: &[u8],
    reverse: bool,
) -> Result<AdjEntry> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    #[cfg(any(test, feature = "test-seams"))]
    crate::read_work_probe::note_graph("graph_payload", "payload_decode", key, None, None);
    let (edge, consumed): (AdjEntry, usize) =
        crate::read_image_memory::decode(bytes).map_err(|error| match error {
            Error::Other(reason) => source.corruption(reason.into_owned()),
            other => other,
        })?;
    if consumed != bytes.len() {
        return Err(Error::StoreCorrupted {
            path: "graph adjacency".into(),
            reason: "edge record has trailing bytes".into(),
        });
    }
    let expected_len = 49usize
        .checked_add(edge.edge_type.len())
        .ok_or_else(|| source.corruption("graph key length overflow".into()))?;
    let (first, second) = if reverse {
        (edge.target, edge.source)
    } else {
        (edge.source, edge.target)
    };
    if key.len() != expected_len
        || &key[..16] != first.as_bytes()
        || &key[16..32] != second.as_bytes()
        || &key[32..32 + edge.edge_type.len()] != edge.edge_type.as_bytes()
        || key[32 + edge.edge_type.len()] != 0
        || key[expected_len - 16..expected_len - 8] != edge.created_tx.0.to_be_bytes()
        || key[expected_len - 8..] != edge.lsn.0.to_be_bytes()
    {
        return Err(Error::StoreCorrupted {
            path: "graph adjacency".into(),
            reason: "edge key does not match its payload".into(),
        });
    }
    Ok(edge)
}
// Match the old canonical bincode comparison, including float bit identity.
// Property map iteration order is irrelevant and no comparison scratch is allocated.
fn same_value(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Float64(left), Value::Float64(right)) => left.to_bits() == right.to_bits(),
        (Value::Vector(left), Value::Vector(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| left.to_bits() == right.to_bits())
        }
        (Value::Json(left), Value::Json(right)) => same_json(left, right),
        _ => left == right,
    }
}
fn same_json(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value as Json;
    match (left, right) {
        (Json::Number(left), Json::Number(right)) if left.is_f64() || right.is_f64() => {
            left.is_f64()
                && right.is_f64()
                && left.as_f64().map(f64::to_bits) == right.as_f64().map(f64::to_bits)
        }
        (Json::Array(left), Json::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| same_json(left, right))
        }
        (Json::Object(left), Json::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, left)| right.get(key).is_some_and(|right| same_json(left, right)))
        }
        _ => left == right,
    }
}
fn same_edge(left: &AdjEntry, right: &AdjEntry) -> bool {
    left.source == right.source
        && left.target == right.target
        && left.edge_type == right.edge_type
        && left.created_tx == right.created_tx
        && left.deleted_tx == right.deleted_tx
        && left.lsn == right.lsn
        && left.properties.len() == right.properties.len()
        && left.properties.iter().all(|(name, value)| {
            right
                .properties
                .get(name)
                .is_some_and(|right| same_value(value, right))
        })
}
impl RetainedReadSource {
    fn graph_window_memory(
        &self,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Arc<crate::read_image_memory::ImageMemory> {
        crate::read_image_memory::ImageMemory::charging_reader_for(
            self._source_memory.memory.accountant.clone(),
            memory,
            "graph_source",
            "edge_decode",
            HINT,
        )
    }
    fn verify_graph_edge_proof(
        &self,
        read: &redb::ReadTransaction,
        key: &[u8],
        raw: &[u8],
        edge: &AdjEntry,
        reverse: bool,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        GraphProofContext::image(self, memory.clone())
            .verify_graph_edge_proof(read, key, raw, edge, reverse, memory)
    }
    pub(crate) fn verify_stored_graph_history(
        &self,
        key: &str,
        bytes: &[u8],
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        self.with_read(|read| self.verify_stored_graph_history_in_read(read, key, bytes, memory))
    }
    pub(super) fn verify_stored_graph_history_in_read(
        &self,
        read: &redb::ReadTransaction,
        key: &str,
        bytes: &[u8],
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let scratch = self.graph_window_memory(memory.clone());
        with_pages(memory.clone(), || {
            scratch.scope(|| {
                GraphProofContext::image(self, memory.clone())
                    .verify_stored_graph_history(read, key, bytes, memory)
            })
        })
    }
}

pub(super) struct GraphProofContext<'a> {
    image: Option<&'a RetainedReadSource>,
    proof_memory: Arc<dyn ReadMemoryBudget>,
    creation_reuse_eligible: bool,
    verifier: Option<&'a content_identity::ContentIdentityReadVerifier>,
    scratch: Option<Arc<crate::read_image_memory::ImageMemory>>,
    logical_frontier: SnapshotId,
    graph_lsn_frontier: Lsn,
    graph_retirement_frontier: u64,
    graph_occurrence_id_floor: u64,
}
// Borrowed only by the immediately enclosing edge proof. The canonical key
// keeps its existing reader charge; the fixed witness has its own reader charge.
struct VerifiedGraphCreation<'a> {
    read: &'a redb::ReadTransaction,
    key: &'a [u8],
    digest: [u8; 32],
    _credit: ReadCredit,
}
impl<'a> GraphProofContext<'a> {
    fn image(source: &'a RetainedReadSource, memory: Arc<dyn ReadMemoryBudget>) -> Self {
        Self {
            image: Some(source),
            proof_memory: memory,
            // Retained images are supplied by the existing Read-owned source route.
            creation_reuse_eligible: true,
            verifier: None,
            scratch: None,
            logical_frontier: source.logical_frontier,
            graph_lsn_frontier: source.graph_lsn_frontier,
            graph_retirement_frontier: source.graph_retirement_frontier,
            graph_occurrence_id_floor: source.graph_occurrence_id_floor,
        }
    }
    fn content_identity_corruption(&self, reason: impl std::fmt::Display) -> Error {
        match self.image {
            Some(source) => source.content_identity_corruption(reason),
            None => Error::StoreCorrupted {
                path: "graph history".into(),
                reason: reason.to_string(),
            },
        }
    }
    fn authenticate_mutation(
        &self,
        mutation: &content_identity::ContentIdentityMutation,
    ) -> Result<()> {
        match self.image {
            Some(source) => source
                .content_identity
                .verify_with_read_memory(mutation, self.proof_memory.clone()),
            None => self
                .verifier
                .expect("writer proof verifier")
                .verify_with_read_memory(mutation, self.proof_memory.clone()),
        }
    }
    fn graph_window_memory(
        &self,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Arc<crate::read_image_memory::ImageMemory> {
        match self.image {
            Some(source) => source.graph_window_memory(memory),
            None => self
                .scratch
                .as_ref()
                .expect("graph proof scratch owner")
                .clone(),
        }
    }
    fn verify_graph_counterpart(
        &self,
        read: &redb::ReadTransaction,
        edge: &AdjEntry,
        reverse: bool,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Option<[u8; 32]>> {
        let mismatch = || {
            diagnosis(
                self,
                direct_read_requires_writer(
                    "forward and reverse graph projections need writable reconciliation",
                ),
            )
        };
        let scratch = self.graph_window_memory(memory.clone());
        scratch.scope(|| {
            let owner = writer_source::SourceWindowOwner::Read(memory.clone());
            let (first, second) = if reverse {
                (edge.source, edge.target)
            } else {
                (edge.target, edge.source)
            };
            let key = owner.endpoint(
                &[
                    first.as_bytes(),
                    second.as_bytes(),
                    edge.edge_type.as_bytes(),
                    &[0],
                    &edge.created_tx.0.to_be_bytes(),
                    &edge.lsn.0.to_be_bytes(),
                ],
                "counterpart_key",
            )?;
            let table = match read.open_table(if reverse {
                GRAPH_FWD_TABLE
            } else {
                GRAPH_REV_TABLE
            }) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Err(mismatch()),
                Err(error) => return Err(diagnosis(self, read_table_error(error))),
            };
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_graph(
                if reverse { "graph_fwd" } else { "graph_rev" },
                "get",
                &key.key,
                None,
                None,
            );
            let value = table
                .get(key.key.as_ref())
                .map_err(|error| diagnosis(self, read_storage_error(error)))?
                .ok_or_else(mismatch)?;
            let counterpart = decode_edge(self, &key.key, value.value(), !reverse)?;
            if !same_edge(edge, &counterpart) {
                return Err(mismatch());
            }
            if reverse {
                Ok(Some(graph_history_record::contribution_digest(
                    true,
                    &key.key,
                    value.value(),
                )?))
            } else {
                Ok(None)
            }
        })
    }
    /// Shared-LSN siblings may be below the maximum. The committed metadata
    /// bound, not a scan or a fabricated per-edge equality, proves admission.
    fn verify_graph_creation(&self, edge: &AdjEntry) -> Result<()> {
        if edge.lsn > self.graph_lsn_frontier
            || edge.created_tx.0 > self.logical_frontier.0
            || edge
                .deleted_tx
                .is_some_and(|tx| tx.0 > self.logical_frontier.0)
        {
            return Err(self.content_identity_corruption(
                "edge version exceeds the authenticated committed group bounds",
            ));
        }
        Ok(())
    }
    fn graph_bytes<T>(
        &self,
        read: &redb::ReadTransaction,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        key: &[u8],
        inspect: impl FnOnce(Option<&[u8]>) -> Result<T>,
    ) -> Result<T> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
        let table = read
            .open_table(definition)
            .map_err(|e| diagnosis(self, read_table_error(e)))?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            redb::TableHandle::name(&definition),
            "get",
            key,
            None,
            None,
        );
        let value = table
            .get(key)
            .map_err(|e| diagnosis(self, read_storage_error(e)))?;
        inspect(value.as_ref().map(|v| v.value()))
    }

    fn connected_graph_end(&self, read: &redb::ReadTransaction, position: Lsn) -> Result<Option<Lsn>> {
        let config = match read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(diagnosis(self,read_table_error(error))),
        };
        let member = |at: Lsn| -> Result<Option<(u64,u64)>> {
            let key = graph_history_record::connected_graph_membership_key(at);
            let key = std::str::from_utf8(&key).expect("ASCII membership key");
            let Some(value) = config.get(key).map_err(|error| diagnosis(self,read_storage_error(error)))?
                else { return Ok(None); };
            let _record = credit(self.proof_memory.clone(),key.len().saturating_add(value.value().len())
                .saturating_mul(4).saturating_add(1024))?;
            let mutations = content_identity::config_mutations(key,Some(value.value()));
            for mutation in &mutations { self.authenticate_mutation(mutation)?; }
            graph_history_record::connected_graph_interval(value.value()).map(Some)
                .ok_or_else(|| self.content_identity_corruption("invalid graph acceptance membership"))
        };
        let Some((start,end)) = member(position)? else { return Ok(None); };
        if start == 0 || start > position.0 || end < position.0 || end > self.graph_lsn_frontier.0 {
            return Err(self.content_identity_corruption("graph acceptance membership exceeds captured interval"));
        }
        if member(Lsn(end))? != Some((start,end)) {
            return Err(self.content_identity_corruption("graph acceptance END membership disagrees"));
        }
        Ok(Some(Lsn(end)))
    }

    fn graph_commit(
        &self,
        read: &redb::ReadTransaction,
        j: Lsn,
        q: TxId,
        bootstrap_creation: bool,
        _role: &str,
    ) -> Result<()> {
        if let Some(source) = self.image {
            let _controls = credit(
                self.proof_memory.clone(),
                content_identity::history_open_control::DIRECTORY_CHECK_SCRATCH_BYTES,
            )?;
            source.history_open_control.check_directory(read)?;
        }
        if q.0 > self.logical_frontier.0 || j > self.graph_lsn_frontier {
            return Err(
                self.content_identity_corruption("graph contribution exceeds captured bounds")
            );
        }
        if bootstrap_creation && j == Lsn(0) {
            return Ok(());
        }
        let missing = || {
            diagnosis(
                self,
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::MissingCommitIndex,
                    format!("commit index is missing graph history LSN {}", j.0),
                ),
            )
        };
        let table = match read.open_table(COMMIT_INDEX_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Err(missing()),
            Err(e) => return Err(diagnosis(self, read_table_error(e))),
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "commit_index",
            "get",
            &j.0.to_be_bytes(),
            Some(contextdb_core::Lsn(j.0)),
            Some(_role),
        );
        let direct = table.get(j.0).map_err(|e| diagnosis(self,read_storage_error(e)))?;
        let (committed_position,value) = if let Some(value) = direct { (j,value) } else {
            let end = self.connected_graph_end(read,j)?.ok_or_else(missing)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_graph("commit_index","get",&end.0.to_be_bytes(),Some(end),Some(_role));
            let value = table.get(end.0).map_err(|e| diagnosis(self,read_storage_error(e)))?
                .ok_or_else(missing)?;
            (end,value)
        };
        self.authenticate_mutation(&content_identity::commit_index_mutation(
            committed_position,
            TxId(value.value()),
        ))?;
        if value.value() < q.0 {
            return Err(self.content_identity_corruption(
                "graph contribution exceeds its required public commit",
            ));
        }
        Ok(())
    }

    fn graph_digest_value(&self, value: Option<&[u8]>, expected: [u8; 32]) -> Result<()> {
        match value {
            Some(v) if v.len() == 33 && v[0] == 1 && v[1..] == expected => Ok(()),
            _ => Err(self
                .content_identity_corruption("graph contribution digest is missing or mismatched")),
        }
    }

    // A single backward seek resolves its concrete source and that source's J.
    // No lifecycle or recursive maximum check occurs in this source window.
    fn graph_maximum(
        &self,
        read: &redb::ReadTransaction,
        a: Lsn,
        memory: Arc<dyn ReadMemoryBudget>,
        verified_creation: Option<&VerifiedGraphCreation<'_>>,
    ) -> Result<TxId> {
        let owner = writer_source::SourceWindowOwner::Read(memory.clone());
        let prefix = owner.endpoint(&[&[1], &a.0.to_be_bytes()], "graph_maximum")?;
        let end = owner.prefix_end(&prefix.key, "graph_maximum")?;
        let table = read
            .open_table(graph_history_record::GRAPH_CONTRIBUTIONS)
            .map_err(|e| diagnosis(self, read_table_error(e)))?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "graph_contribution",
            "maximum_seek",
            &prefix.key,
            Some(contextdb_core::Lsn(a.0)),
            None,
        );
        let mut range = table
            .range::<&[u8]>((
                Bound::Included(prefix.key.as_ref()),
                end.as_ref()
                    .map_or(Bound::Unbounded, |e| Bound::Excluded(e.key.as_ref())),
            ))
            .map_err(|e| diagnosis(self, read_storage_error(e)))?;
        let (key, value) = range
            .next_back()
            .transpose()
            .map_err(|e| diagnosis(self, read_storage_error(e)))?
            .ok_or_else(|| self.content_identity_corruption("physical graph maximum is missing"))?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::graph_storage_key(
            "graph_contribution",
            "iteration_step",
            key.value(),
        );
        let k = key.value();
        if k.len() < 18 || !k.starts_with(prefix.key.as_ref()) {
            return Err(self.content_identity_corruption("malformed graph maximum key"));
        }
        let q = TxId(u64::from_be_bytes(k[9..17].try_into().unwrap()));
        let (j, creation) = match k[17] {
            0 => {
                if k.len() < 22 {
                    return Err(self.content_identity_corruption("short creation contribution"));
                }
                let len = u32::from_be_bytes(k[18..22].try_into().unwrap()) as usize;
                if k.len().checked_sub(22) != Some(len) {
                    return Err(
                        self.content_identity_corruption("creation contribution length mismatch")
                    );
                }
                let p = &k[22..];
                let (_, _, _, tx, lsn) = graph_history_record::parse_physical_predecessor(p)?;
                if tx != q || lsn != a {
                    return Err(
                        self.content_identity_corruption("creation maximum coordinate mismatch")
                    );
                }
                // A matching witness was authenticated together with its counterpart
                // in this edge proof and this exact captured read transaction. A
                // missing or mismatched witness keeps the full source proof below.
                if let Some(verified) = verified_creation.filter(|verified| {
                    std::ptr::eq(verified.read, read)
                        && verified.key == p
                        && value.value().len() == 33
                        && value.value()[0] == 1
                        && value.value()[1..] == verified.digest
                }) {
                    self.graph_digest_value(Some(value.value()), verified.digest)?;
                } else {
                    self.graph_bytes(read, GRAPH_FWD_TABLE, p, |raw| {
                        let raw = raw.ok_or_else(|| {
                            self.content_identity_corruption("creation maximum payload missing")
                        })?;
                        let edge = decode_edge(self, p, raw, false)?;
                        self.verify_graph_creation(&edge)?;
                        self.verify_graph_counterpart(read, &edge, false, memory.clone())?;
                        self.graph_digest_value(
                            Some(value.value()),
                            graph_history_record::contribution_digest(true, p, raw)?,
                        )
                    })?;
                }
                (a, true)
            }
            1 => {
                if k.len() != 26 {
                    return Err(
                        self.content_identity_corruption("deletion contribution length mismatch")
                    );
                }
                let id = u64::from_be_bytes(k[18..26].try_into().unwrap());
                self.verify_graph_occurrence_id(id)?;
                let (stored_lsn, ordinal) = self.graph_location(read, id)?;
                if stored_lsn != a {
                    return Err(self.content_identity_corruption(
                        "maximum location has different physical LSN",
                    ));
                }
                let text_len = 41usize;
                let _text_charge = credit(memory.clone(), text_len)?;
                let key = format!("{:020}:{:06}", stored_lsn.0, ordinal);
                let history = read
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(|e| diagnosis(self, read_table_error(e)))?;
                let raw = history
                    .get(observed_graph_key!("history", "get", key.as_str()))
                    .map_err(|e| diagnosis(self, read_storage_error(e)))?
                    .ok_or_else(|| {
                        self.content_identity_corruption("maximum history source missing")
                    })?;
                self.authenticate_mutation(&RedbPersistence::stored_change_log_mutation(
                    &key,
                    raw.value(),
                )?)?;
                let record = RedbPersistence::decode_stored_change_log(raw.value())?;
                let witness = record.witness.as_ref().ok_or_else(|| {
                    self.content_identity_corruption("maximum source has no witness")
                })?;
                if witness.id != id || witness.deletion_tx != q {
                    return Err(self.content_identity_corruption(
                        "maximum source disagrees with contribution",
                    ));
                }
                self.graph_digest_value(
                    Some(value.value()),
                    graph_history_record::contribution_digest(
                        false,
                        &id.to_be_bytes(),
                        raw.value(),
                    )?,
                )?;
                (record.entry.lsn(), false)
            }
            _ => return Err(self.content_identity_corruption("unknown graph contribution kind")),
        };
        self.graph_commit(read, j, q, creation, "maximum_source")?;
        Ok(q)
    }

    fn verify_graph_occurrence_id(&self, id: u64) -> Result<()> {
        if id == 0 || id > self.graph_occurrence_id_floor {
            return Err(self.content_identity_corruption(
                "graph occurrence id is outside captured allocation bounds",
            ));
        }
        Ok(())
    }

    fn graph_location(&self, read: &redb::ReadTransaction, id: u64) -> Result<(Lsn, u64)> {
        self.graph_bytes(
            read,
            graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
            &graph_history_record::location_key(id),
            |raw| {
                let v = raw.ok_or_else(|| {
                    self.content_identity_corruption("graph occurrence location missing")
                })?;
                if v.len() != 17 || v[0] != 1 {
                    return Err(
                        self.content_identity_corruption("malformed graph occurrence location")
                    );
                }
                Ok((
                    Lsn(u64::from_be_bytes(v[1..9].try_into().unwrap())),
                    u64::from_be_bytes(v[9..17].try_into().unwrap()),
                ))
            },
        )
    }

    fn verify_graph_edge_proof(
        &self,
        read: &redb::ReadTransaction,
        key: &[u8],
        raw: &[u8],
        edge: &AdjEntry,
        reverse: bool,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        self.verify_graph_creation(edge)?;
        let counterpart_digest =
            self.verify_graph_counterpart(read, edge, reverse, memory.clone())?;
        let owner = writer_source::SourceWindowOwner::Read(memory.clone());
        let p = if reverse {
            owner.endpoint(&[&key[16..32], &key[..16], &key[32..]], "graph_forward_key")?
        } else {
            owner.endpoint(&[key], "graph_forward_key")?
        };
        let verified_creation = if self.creation_reuse_eligible {
            let witness_credit = credit(
                memory.clone(),
                std::mem::size_of::<VerifiedGraphCreation<'_>>(),
            )?;
            Some(VerifiedGraphCreation {
                read,
                key: &p.key,
                digest: match counterpart_digest {
                    Some(digest) => digest,
                    None => graph_history_record::contribution_digest(true, &p.key, raw)?,
                },
                _credit: witness_credit,
            })
        } else {
            None
        };
        let own = graph_history_record::creation_contribution_key(&p.key)?;
        self.graph_bytes(read, graph_history_record::GRAPH_CONTRIBUTIONS, &own, |v| {
            self.graph_digest_value(
                v,
                match verified_creation.as_ref() {
                    Some(verified) => verified.digest,
                    None => match counterpart_digest {
                        Some(digest) => digest,
                        None => graph_history_record::contribution_digest(true, &p.key, raw)?,
                    },
                },
            )
        })?;
        let maximum = self.graph_maximum(read, edge.lsn, memory, verified_creation.as_ref())?;
        if edge.created_tx > maximum {
            return Err(self.content_identity_corruption("edge exceeds physical graph maximum"));
        }
        self.graph_commit(read, edge.lsn, edge.created_tx, true, "requested_own")
    }

    /// Validate only the supplied history window at its actual physical key.
    /// Present and Retired take at most 12 and 10 additional records.
    pub(super) fn verify_stored_graph_history(
        &self,
        read: &redb::ReadTransaction,
        key: &str,
        bytes: &[u8],
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
            crate::read_work_probe::note_graph(
                "history",
                "supplied_primary",
                key.as_bytes(),
                None,
                None,
            );
        }
        if let Some(source) = self.image {
            let _controls = credit(
                memory.clone(),
                content_identity::history_open_control::DIRECTORY_CHECK_SCRATCH_BYTES,
            )?;
            source.history_open_control.check_directory(read)?;
        }
        self.authenticate_mutation(&RedbPersistence::stored_change_log_mutation(key, bytes)?)?;
        let record = RedbPersistence::decode_stored_change_log(bytes)?;
        let Some(witness) = record.witness.as_ref() else {
            return Ok(());
        };
        self.verify_graph_occurrence_id(witness.id)?;
        let (stored_lsn, ordinal) = self.graph_location(read, witness.id)?;
        let _key_charge = credit(memory.clone(), 41)?;
        if key != format!("{:020}:{:06}", stored_lsn.0, ordinal) {
            return Err(self.content_identity_corruption(
                "history location does not name supplied physical key",
            ));
        }
        crate::read_image_memory::reserve(26)?;
        let own = graph_history_record::deletion_contribution_key(stored_lsn, witness);
        self.graph_bytes(read, graph_history_record::GRAPH_CONTRIBUTIONS, &own, |v| {
            self.graph_digest_value(
                v,
                graph_history_record::contribution_digest(false, &witness.id.to_be_bytes(), bytes)?,
            )
        })?;
        let p = &witness.predecessor;
        let (_, _, _, created, creation_lsn) = graph_history_record::parse_physical_predecessor(p)?;
        let d = witness.deletion_tx;
        if created >= d || creation_lsn >= record.entry.lsn() || d.0 > self.logical_frontier.0 {
            return Err(self.content_identity_corruption(
                "graph predecessor deletion ordering exceeds captured bounds",
            ));
        }
        self.graph_commit(read, record.entry.lsn(), d, false, "requested_own")?;
        let receipt = graph_history_record::lifecycle_key(p)?;
        let state = self.graph_bytes(
            read,
            graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
            &receipt,
            |v| {
                let v = v.ok_or_else(|| {
                    self.content_identity_corruption("exact graph lifecycle receipt missing")
                })?;
                if v.len() != 18
                    || v[0] != 1
                    || u64::from_be_bytes(v[1..9].try_into().unwrap()) != d.0
                    || u64::from_be_bytes(v[9..17].try_into().unwrap()) == 0
                    || v[17] > 1
                {
                    return Err(self.content_identity_corruption(
                        "graph lifecycle receipt malformed or contradictory",
                    ));
                }
                Ok(v[17])
            },
        )?;
        let owner = writer_source::SourceWindowOwner::Read(memory.clone());
        let reverse = owner.endpoint(&[&p[16..32], &p[..16], &p[32..]], "history_reverse_key")?;
        self.graph_bytes(read, GRAPH_FWD_TABLE, p, |fwd| {
            self.graph_bytes(read, GRAPH_REV_TABLE, &reverse.key, |rev| {
                if state == 1 {
                    if fwd.is_some()
                        || rev.is_some()
                        || creation_lsn.0 > self.graph_retirement_frontier
                    {
                        return Err(self.content_identity_corruption(
                            "retired predecessor lacks exact absence/frontier proof",
                        ));
                    }
                    return Ok(());
                }
                let fwd = fwd.ok_or_else(|| {
                    self.content_identity_corruption(
                        "present predecessor forward projection missing",
                    )
                })?;
                let rev = rev.ok_or_else(|| {
                    self.content_identity_corruption(
                        "present predecessor reverse projection missing",
                    )
                })?;
                let edge = decode_edge(self, p, fwd, false)?;
                let counterpart = decode_edge(self, &reverse.key, rev, true)?;
                if !same_edge(&edge, &counterpart) || edge.deleted_tx != Some(d) {
                    return Err(self.content_identity_corruption(
                        "present predecessor projections disagree with receipt",
                    ));
                }
                self.verify_graph_creation(&edge)?;
                let creation = graph_history_record::creation_contribution_key(p)?;
                self.graph_bytes(
                    read,
                    graph_history_record::GRAPH_CONTRIBUTIONS,
                    &creation,
                    |v| {
                        self.graph_digest_value(
                            v,
                            graph_history_record::contribution_digest(true, p, fwd)?,
                        )
                    },
                )?;
                self.graph_commit(read, creation_lsn, created, true, "predecessor")
            })
        })?;
        let maximum = self.graph_maximum(read, stored_lsn, memory.clone(), None)?;
        if d > maximum {
            return Err(self.content_identity_corruption("history exceeds physical graph maximum"));
        }
        Ok(())
    }
}

pub(super) fn with_pages<T>(
    memory: Arc<dyn ReadMemoryBudget>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_pages_result(memory, read, |error| error)
}

pub(super) fn with_pages_result<T, E>(
    memory: Arc<dyn ReadMemoryBudget>,
    read: impl FnOnce() -> std::result::Result<T, E>,
    refusal_error: impl FnOnce(Error) -> E,
) -> std::result::Result<T, E> {
    // These shared controls belong to this executing reader, including the
    // closure's exact Arc allocation. Keep their credit outside both Arcs so
    // the allocations are freed before their funding returns.
    let mut controls = ReadCredit::new(memory.clone());
    if let Err(error) = controls.try_grow_for(
        crate::memory_accounting::writer_adapter_type_layout::<Mutex<Option<Error>>>().size(),
        "graph_source",
        "read_window",
        HINT,
    ) {
        return Err(refusal_error(error));
    }
    let refusal = Arc::new(Mutex::new(None));
    let captured = refusal.clone();
    let admission = move |bytes| match page_credit(memory.clone(), bytes) {
        Ok(charge) => {
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_worker_page_admitted(
                bytes,
                crate::read_work_probe::funding_owner_name(&memory),
            );
            Ok(Box::new(charge) as redb::ReadPageCharge)
        }
        Err(error) => {
            *captured.lock().unwrap_or_else(|error| error.into_inner()) = Some(error);
            Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory))
        }
    };
    fn closure_arc_bytes<T>(_: &T) -> usize {
        crate::memory_accounting::writer_adapter_type_layout::<T>().size()
    }
    if let Err(error) = controls.try_grow_for(
        closure_arc_bytes(&admission),
        "graph_source",
        "read_window",
        HINT,
    ) {
        return Err(refusal_error(error));
    }
    let admission = Arc::new(admission)
        as Arc<dyn Fn(usize) -> std::io::Result<redb::ReadPageCharge> + Send + Sync>;
    let result = redb::with_read_page_charge(admission, read);
    if let Some(error) = refusal
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take()
    {
        return Err(refusal_error(error));
    }
    result
}

impl WriterSnapshot {
    fn with_graph_proof_at_root<T>(
        &self,
        owner: &writer_source::SourceWindowOwner,
        verify: impl FnOnce(
            &GraphProofContext<'_>,
            &redb::ReadTransaction,
            Arc<dyn ReadMemoryBudget>,
        ) -> Result<T>,
    ) -> Result<T> {
        let read = self.owner_read()?;
        with_graph_proof_in_read(
            &read,
            Some(self.snapshot),
            owner,
            owner.image(self, "graph_proof"),
            verify,
        )
    }
    pub(crate) fn verify_stored_graph_history_at_root(
        &self,
        key: &str,
        bytes: &[u8],
        owner: &writer_source::SourceWindowOwner,
    ) -> Result<()> {
        self.with_graph_proof_at_root(owner, |context, read, memory| {
            context.verify_stored_graph_history(read, key, bytes, memory)
        })
    }
    pub(crate) fn verify_graph_edge_at_root(
        &self,
        key: &[u8],
        bytes: &[u8],
        edge: &AdjEntry,
        reverse: bool,
        owner: &writer_source::SourceWindowOwner,
    ) -> Result<()> {
        self.with_graph_proof_at_root(owner, |context, read, memory| {
            context.verify_graph_edge_proof(read, key, bytes, edge, reverse, memory)
        })
    }
}

/// Point proofs over the caller's already captured transaction. Both writer
/// snapshots and the public full-history load use this exact verifier kernel.
pub(super) fn with_graph_proof_in_read<T>(
    read: &redb::ReadTransaction,
    logical_frontier: Option<SnapshotId>,
    owner: &writer_source::SourceWindowOwner,
    scratch: Arc<crate::read_image_memory::ImageMemory>,
    verify: impl FnOnce(
        &GraphProofContext<'_>,
        &redb::ReadTransaction,
        Arc<dyn ReadMemoryBudget>,
    ) -> Result<T>,
) -> Result<T> {
    #[cfg(any(test, feature = "test-seams"))]
    crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
    let memory: Arc<dyn ReadMemoryBudget> = match owner {
        writer_source::SourceWindowOwner::Read(memory) => memory.clone(),
        writer_source::SourceWindowOwner::Write(memory, original_use) => {
            memory.read_budget_for_original(original_use.clone())?
        }
    };
    with_pages(memory.clone(), || {
        scratch.scope(|| {
            let bounds = RedbPersistence::read_authenticated_bounds_in_read(read)?;
            let _control = owner.reserve(
                std::mem::size_of::<crate::memory_accounting::MemoryAccountant>()
                    .saturating_add(std::mem::size_of::<
                        content_identity::ContentIdentityReadVerifier,
                    >())
                    .saturating_add(128),
                "graph_proof_control",
            )?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                read,
                Path::new("graph history"),
                None,
            )?;
            let context = GraphProofContext {
                image: None,
                proof_memory: memory.clone(),
                creation_reuse_eligible: matches!(owner, writer_source::SourceWindowOwner::Read(_)),
                verifier: Some(&verifier),
                scratch: Some(scratch.clone()),
                logical_frontier: logical_frontier.unwrap_or(SnapshotId(bounds.identity_tx_floor)),
                graph_lsn_frontier: Lsn(bounds.source_lsn_floor),
                graph_retirement_frontier: bounds.graph_retirement_frontier,
                graph_occurrence_id_floor: bounds.graph_occurrence_id_floor,
            };
            verify(&context, read, memory)
        })
    })
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
