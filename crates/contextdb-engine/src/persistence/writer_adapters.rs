//! Storage-neutral store adapters retain the original root and window owners.
use super::writer_source::*;
use super::*;
use contextdb_core::Direction;
use contextdb_core::read_memory::OwnedReadBox;
use contextdb_relational::source::{
    RelationalCursor, RelationalCursorOwner, RelationalIndexRange, RelationalPositionKey,
    RelationalPosting, RelationalReadSource, RelationalWindow,
};
use contextdb_relational::store::{IndexEntry, SyncSourceKind};
use std::ops::Bound;

pub(crate) struct WriterRelationalSource {
    root: Arc<WriterSnapshot>,
    owner: SourceWindowOwner,
    _control: SourceWindowCredit,
}
impl WriterRelationalSource {
    pub(crate) fn row_head(&self, table: &str, row: RowId) -> Result<(Lsn, [u8; 24])> {
        let key = RedbPersistence::writer_row_head(&self.root, table, row)?;
        Ok((Lsn(u64::from_be_bytes(key[16..].try_into().unwrap())), key))
    }
    pub(crate) fn cleanup_rotation(&self) -> Result<Option<[u8; 32]>> {
        RedbPersistence::currency_cleanup_rotation(&self.root)
    }
    pub(crate) fn cleanup_progress(
        &self,
        table: &str,
        declaration: [u8; 32],
    ) -> Result<super::writer_cleanup_progress::CurrencyCleanupProgress> {
        RedbPersistence::currency_cleanup_progress(&self.root, table, declaration)
    }
    pub(crate) fn new_for_read(
        root: Arc<WriterSnapshot>,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Read(read))
    }
    pub(crate) fn new_for_write(
        root: Arc<WriterSnapshot>,
        writer: Arc<crate::memory_accounting::MemoryAccountant>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Write(writer))
    }
    fn new_owned(root: Arc<WriterSnapshot>, owner: SourceWindowOwner) -> Result<Self> {
        // Payload admission only. Standard Arc control allocation is unresolved.
        let control = root.adapter_control(&owner, std::alloc::Layout::new::<Self>().size())?;
        Ok(Self {
            root,
            owner,
            _control: control,
        })
    }
}
struct RowCursor {
    cursor: WriterReadCursor,
}
struct PostingCursor {
    cursor: WriterReadCursor,
    current: bool,
}

impl<T: Send> RelationalWindow<T> for WriterWindow<T> {
    fn value(&self) -> &T {
        &self.value
    }
    fn position_key(&self) -> &[u8] {
        &self.position
    }
}
impl RelationalPositionKey for WriterWindow<Vec<u8>> {
    fn as_bytes(&self) -> &[u8] {
        &self.value
    }
}
impl RelationalCursor<VersionedRow> for RowCursor {
    fn next_window(&mut self) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.next_window_before(&mut || Ok(()))
    }
    fn next_window_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.cursor.next_row_before_handoff(before, |window| {
            window.boxed(|boxed| boxed as Box<dyn RelationalWindow<VersionedRow>>)
        })
    }
    fn examined_entries(&self) -> u64 {
        self.cursor.examined_entries()
    }
}
impl RelationalCursor<RelationalPosting> for PostingCursor {
    fn next_window(
        &mut self,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<RelationalPosting>>>> {
        self.next_window_before(&mut || Ok(()))
    }
    fn next_window_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<RelationalPosting>>>> {
        self.cursor
            .next_index_before_handoff(self.current, before, |window| {
                let window = window.map(|value| RelationalPosting {
                    key: value.tuple,
                    entry: IndexEntry {
                        row_id: value.row_id,
                        created_tx: value.created_tx,
                        deleted_tx: value.deleted_tx,
                    },
                });
                window.boxed(|boxed| boxed as Box<dyn RelationalWindow<RelationalPosting>>)
            })
    }
    fn examined_entries(&self) -> u64 {
        self.cursor.examined_entries()
    }
}
impl RelationalReadSource for WriterRelationalSource {
    fn validate_table(&self, table: &str) -> Result<()> {
        self.root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))
            .map(|_| ())
    }
    fn snapshot(&self) -> SnapshotId {
        self.root.snapshot
    }
    fn lsn(&self) -> Lsn {
        self.root.lsn
    }
    fn row_cursor(
        &self,
        table: &str,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<RelationalCursorOwner<VersionedRow>> {
        self.validate_table(table)?;
        let control = self
            .owner
            .reserve_box_credit(std::alloc::Layout::new::<RowCursor>().size())?;
        let physical = self
            .owner
            .endpoint(&[b"rel_", table.as_bytes()], "cursor_keys")?;
        let name = std::str::from_utf8(&physical.key).expect("UTF-8 table name with ASCII prefix");
        let cursor = self.root.cursor(
            self.owner.clone(),
            name,
            WriterCursorFamily::Bytes,
            lower,
            upper,
            reverse,
        )?;
        let payload: Box<dyn RelationalCursor<VersionedRow>> = Box::new(RowCursor { cursor });
        Ok(RelationalCursorOwner::new(payload, control))
    }
    fn row_id_cursor(
        &self,
        table: &str,
        row: RowId,
        snapshot: SnapshotId,
    ) -> Result<RelationalCursorOwner<VersionedRow>> {
        let mut lower = [0u8; 24];
        lower[..8].copy_from_slice(&row.0.to_be_bytes());
        let mut upper = [255u8; 24];
        upper[..8].copy_from_slice(&row.0.to_be_bytes());
        upper[8..16].copy_from_slice(&snapshot.0.to_be_bytes());
        self.row_cursor(
            table,
            Bound::Included(&lower),
            Bound::Included(&upper),
            true,
        )
    }
    fn exact_row(
        &self,
        table: &str,
        position: &[u8; 24],
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.validate_table(table)?;
        // This cursor is stack-owned; only its returned window is boxed.
        self.root
            .exact_row(self.owner.clone(), table, position)?
            .next_row_before_handoff(&mut || Ok(()), |window| {
                window.boxed(|boxed| boxed as Box<dyn RelationalWindow<VersionedRow>>)
            })
    }
    fn index_cursor(
        &self,
        table: &str,
        index: &str,
        range: RelationalIndexRange<'_>,
    ) -> Result<RelationalCursorOwner<RelationalPosting>> {
        self.validate_table(table)?;
        let control = self
            .owner
            .reserve_box_credit(std::alloc::Layout::new::<PostingCursor>().size())?;
        let payload: Box<dyn RelationalCursor<RelationalPosting>> = Box::new(PostingCursor {
            cursor: self.root.index_cursor(
                self.owner.clone(),
                table,
                index,
                range.current,
                range.lower,
                range.upper,
                range.reverse,
            )?,
            current: range.current,
        });
        Ok(RelationalCursorOwner::new(payload, control))
    }
    fn exact_index_cursor(
        &self,
        table: &str,
        index: &str,
        current: bool,
        key: &IndexKey,
    ) -> Result<RelationalCursorOwner<RelationalPosting>> {
        self.validate_table(table)?;
        let control = self
            .owner
            .reserve_box_credit(std::alloc::Layout::new::<PostingCursor>().size())?;
        let payload: Box<dyn RelationalCursor<RelationalPosting>> = Box::new(PostingCursor {
            cursor: self
                .root
                .exact_index_cursor(self.owner.clone(), table, index, current, key)?,
            current,
        });
        Ok(RelationalCursorOwner::new(payload, control))
    }
    fn index_position_key(
        &self,
        current: bool,
        key: &IndexKey,
        row: RowId,
        created: TxId,
    ) -> Result<OwnedReadBox<dyn RelationalPositionKey>> {
        Ok(self
            .root
            .index_position_key(
                self.owner.clone(),
                key,
                row,
                if current { TxId(0) } else { created },
            )?
            .boxed(|boxed| boxed as Box<dyn RelationalPositionKey>)?)
    }
    fn sync_source(&self, table: &str, row: RowId) -> Result<Option<(Lsn, SyncSourceKind)>> {
        self.validate_table(table)?;
        self.root
            .sync_source(self.owner.clone(), table, row)?
            .map(|(lsn, kind)| match kind {
                0 => Ok((lsn, SyncSourceKind::Pulled)),
                1 => Ok((lsn, SyncSourceKind::AcceptedLocal)),
                2 => Ok((lsn, SyncSourceKind::AcceptedLocalPending)),
                _ => Err(Error::StoreCorrupted {
                    path: "sync provenance".into(),
                    reason: "unknown source kind".into(),
                }),
            })
            .transpose()
    }
}

pub(crate) struct WriterGraphSnapshot {
    root: Arc<WriterSnapshot>,
    owner: SourceWindowOwner,
    _control: SourceWindowCredit,
}
impl WriterGraphSnapshot {
    pub(crate) fn new_for_read(
        root: Arc<WriterSnapshot>,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Read(read))
    }
    pub(crate) fn new_for_write(
        root: Arc<WriterSnapshot>,
        writer: Arc<crate::memory_accounting::MemoryAccountant>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Write(writer))
    }
    fn new_owned(root: Arc<WriterSnapshot>, owner: SourceWindowOwner) -> Result<Self> {
        // Payload admission only. No guessed standard Arc control layout.
        let control = root.adapter_control(&owner, std::alloc::Layout::new::<Self>().size())?;
        Ok(Self {
            root,
            owner,
            _control: control,
        })
    }
}
struct EdgeCursor {
    cursor: WriterReadCursor,
}
impl contextdb_graph::source::GraphEdgeWindow for WriterWindow<AdjEntry> {
    fn edge(&self) -> &AdjEntry {
        &self.value
    }
}
impl contextdb_graph::source::GraphCursor for EdgeCursor {
    fn next_edge(
        &mut self,
    ) -> Result<Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>> {
        self.cursor.next_edge_handoff(|window| {
            window.boxed(|boxed| boxed as Box<dyn contextdb_graph::source::GraphEdgeWindow>)
        })
    }
}
impl contextdb_graph::source::GraphSnapshot for WriterGraphSnapshot {
    fn cursor(
        &self,
        query: contextdb_graph::source::GraphQuery<'_>,
    ) -> Result<contextdb_graph::source::GraphCursorOwner> {
        let reverse = match query.direction {
            Direction::Outgoing => false,
            Direction::Incoming => true,
            Direction::Both => {
                return Err(Error::Other(
                    "graph source requires a directional cursor".into(),
                ));
            }
        };
        let cursor = if let Some((other, edge_type)) = query.identity {
            let prefix = self.owner.endpoint(
                &[
                    query.node.as_bytes(),
                    other.as_bytes(),
                    edge_type.as_bytes(),
                    &[0],
                ],
                "store_adapter",
            )?;
            let end = self.owner.prefix_end(&prefix.key, "store_adapter")?;
            self.root.cursor(
                self.owner.clone(),
                if reverse { "graph_rev" } else { "graph_fwd" },
                WriterCursorFamily::Bytes,
                Bound::Included(&prefix.key),
                end.as_ref()
                    .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
                false,
            )?
        } else {
            self.root
                .graph_cursor(self.owner.clone(), query.node, reverse)?
        };
        let control = self
            .owner
            .reserve_box_credit(std::alloc::Layout::new::<EdgeCursor>().size())?;
        let payload: Box<dyn contextdb_graph::source::GraphCursor> =
            Box::new(EdgeCursor { cursor });
        Ok(contextdb_graph::source::GraphCursorOwner::new(
            payload, control,
        ))
    }
}
