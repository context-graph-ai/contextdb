//! Storage-neutral store adapters retain the original root and window owners.
use super::writer_source::*;
use super::*;
use contextdb_core::Direction;
use contextdb_relational::source::{
    RelationalCursor, RelationalIndexRange, RelationalPositionKey, RelationalPosting,
    RelationalReadSource, RelationalWindow,
};
use contextdb_relational::store::{IndexEntry, SyncSourceKind};
use std::ops::Bound;

pub(crate) struct WriterRelationalSource {
    root: Arc<WriterSnapshot>,
    _control: crate::memory_accounting::OwnedMemoryReservation,
}
impl WriterRelationalSource {
    pub(crate) fn new(root: Arc<WriterSnapshot>) -> Result<Self> {
        let control = root.adapter_control(std::mem::size_of::<Self>())?;
        Ok(Self {
            root,
            _control: control,
        })
    }
}
struct RowCursor(WriterReadCursor);
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
    fn next_window(&mut self) -> Result<Option<Box<dyn RelationalWindow<VersionedRow>>>> {
        self.next_window_before(&mut || Ok(()))
    }
    fn next_window_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<Box<dyn RelationalWindow<VersionedRow>>>> {
        self.0
            .next_row_before(before)?
            .map(|window| {
                window
                    .boxed()
                    .map(|boxed| boxed as Box<dyn RelationalWindow<VersionedRow>>)
            })
            .transpose()
    }
    fn examined_entries(&self) -> u64 {
        self.0.examined_entries()
    }
}
impl RelationalCursor<RelationalPosting> for PostingCursor {
    fn next_window(&mut self) -> Result<Option<Box<dyn RelationalWindow<RelationalPosting>>>> {
        self.next_window_before(&mut || Ok(()))
    }
    fn next_window_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<Box<dyn RelationalWindow<RelationalPosting>>>> {
        self.cursor
            .next_index_before(self.current, before)?
            .map(|window| {
                let window = window.map(|value| RelationalPosting {
                    key: value.tuple,
                    entry: IndexEntry {
                        row_id: value.row_id,
                        created_tx: value.created_tx,
                        deleted_tx: value.deleted_tx,
                    },
                });
                window
                    .boxed()
                    .map(|boxed| boxed as Box<dyn RelationalWindow<RelationalPosting>>)
            })
            .transpose()
    }
    fn examined_entries(&self) -> u64 {
        self.cursor.examined_entries()
    }
}
impl RelationalReadSource for WriterRelationalSource {
    fn validate_table(&self, table: &str) -> Result<()> {
        self.root
            .captured_table_meta(table)
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
    ) -> Result<Box<dyn RelationalCursor<VersionedRow>>> {
        self.validate_table(table)?;
        Ok(Box::new(RowCursor(self.root.cursor(
            &RedbPersistence::rel_table_name(table),
            WriterCursorFamily::Bytes,
            lower,
            upper,
            reverse,
        )?)))
    }
    fn row_id_cursor(
        &self,
        table: &str,
        row: RowId,
        snapshot: SnapshotId,
    ) -> Result<Box<dyn RelationalCursor<VersionedRow>>> {
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
    ) -> Result<Option<Box<dyn RelationalWindow<VersionedRow>>>> {
        self.validate_table(table)?;
        RowCursor(self.root.exact_row(table, position)?).next_window()
    }
    fn index_cursor(
        &self,
        table: &str,
        index: &str,
        range: RelationalIndexRange<'_>,
    ) -> Result<Box<dyn RelationalCursor<RelationalPosting>>> {
        self.validate_table(table)?;
        Ok(Box::new(PostingCursor {
            cursor: self.root.index_cursor(
                table,
                index,
                range.current,
                range.lower,
                range.upper,
                range.reverse,
            )?,
            current: range.current,
        }))
    }
    fn exact_index_cursor(
        &self,
        table: &str,
        index: &str,
        current: bool,
        key: &IndexKey,
    ) -> Result<Box<dyn RelationalCursor<RelationalPosting>>> {
        self.validate_table(table)?;
        Ok(Box::new(PostingCursor {
            cursor: self.root.exact_index_cursor(table, index, current, key)?,
            current,
        }))
    }
    fn index_position_key(
        &self,
        current: bool,
        key: &IndexKey,
        row: RowId,
        created: TxId,
    ) -> Result<Box<dyn RelationalPositionKey>> {
        Ok(Box::new(self.root.index_position_key(
            key,
            row,
            if current { TxId(0) } else { created },
        )?))
    }
    fn sync_source(&self, table: &str, row: RowId) -> Result<Option<(Lsn, SyncSourceKind)>> {
        self.validate_table(table)?;
        self.root
            .sync_source(table, row)?
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
    _control: crate::memory_accounting::OwnedMemoryReservation,
}
impl WriterGraphSnapshot {
    pub(crate) fn new(root: Arc<WriterSnapshot>) -> Result<Self> {
        let control =
            root.adapter_control(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())?;
        Ok(Self {
            root,
            _control: control,
        })
    }
}
struct EdgeCursor(WriterReadCursor);
impl contextdb_graph::source::GraphEdgeWindow for WriterWindow<AdjEntry> {
    fn edge(&self) -> &AdjEntry {
        &self.value
    }
}
impl contextdb_graph::source::GraphCursor for EdgeCursor {
    fn next_edge(&mut self) -> Result<Option<Box<dyn contextdb_graph::source::GraphEdgeWindow>>> {
        self.0
            .next_edge()?
            .map(|window| {
                window
                    .boxed()
                    .map(|window| window as Box<dyn contextdb_graph::source::GraphEdgeWindow>)
            })
            .transpose()
    }
}
impl contextdb_graph::source::GraphSnapshot for WriterGraphSnapshot {
    fn cursor(
        &self,
        query: contextdb_graph::source::GraphQuery<'_>,
    ) -> Result<Box<dyn contextdb_graph::source::GraphCursor>> {
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
            let _scratch = self
                .root
                .adapter_control((33 + edge_type.len()).saturating_mul(2))?;
            let mut prefix = Vec::with_capacity(33 + edge_type.len());
            prefix.extend_from_slice(query.node.as_bytes());
            prefix.extend_from_slice(other.as_bytes());
            prefix.extend_from_slice(edge_type.as_bytes());
            prefix.push(0);
            let end = prefix_end(&prefix);
            self.root.cursor(
                if reverse { "graph_rev" } else { "graph_fwd" },
                WriterCursorFamily::Bytes,
                Bound::Included(&prefix),
                end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
                false,
            )?
        } else {
            self.root.graph_cursor(query.node, reverse)?
        };
        Ok(Box::new(EdgeCursor(cursor)))
    }
}
