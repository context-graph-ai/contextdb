//! Root-scoped relational reads. Implementations retain the engine's admitted
//! source window, never a second decoded cache or a fresh root per lookup.
use crate::store::{IndexEntry, SyncSourceKind};
use contextdb_core::{IndexKey, Lsn, Result, RowId, SnapshotId, TxId, VersionedRow};
use contextdb_tx::{OrderedStagedRows, StagedPosition};
use std::ops::Bound;

/// One admitted decoded record. The owner must remain alive until the payload
/// is released. Copying a payload requires separate caller admission.
pub trait RelationalWindow<T>: Send {
    fn value(&self) -> &T;
    fn position_key(&self) -> &[u8];
}

pub trait RelationalCursor<T>: Send {
    fn next_window(&mut self) -> Result<Option<Box<dyn RelationalWindow<T>>>>;
    /// Admit work after a record exists and before decoding its payload.
    fn next_window_before(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<Box<dyn RelationalWindow<T>>>> {
        before()?;
        self.next_window()
    }
    /// Physical source steps, including invisible versions.
    fn examined_entries(&self) -> u64;
}

pub struct RelationalPosting {
    pub key: IndexKey,
    pub entry: IndexEntry,
}

pub struct RelationalIndexRange<'a> {
    pub current: bool,
    pub lower: Bound<&'a [u8]>,
    pub upper: Bound<&'a [u8]>,
    pub reverse: bool,
}

/// Scratch admission follows an encoded endpoint until its final release.
pub trait RelationalPositionKey: Send {
    fn as_bytes(&self) -> &[u8];
}

/// An already captured committed root. Engine adapters implement this over
/// WriterSnapshot; this interface has no publication or root-capture door.
pub trait RelationalReadSource: Send + Sync {
    fn snapshot(&self) -> SnapshotId;
    fn lsn(&self) -> Lsn;
    fn row_cursor(
        &self,
        table: &str,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Box<dyn RelationalCursor<VersionedRow>>>;
    /// Reverse version order within one row identity.
    fn row_id_cursor(
        &self,
        table: &str,
        row: RowId,
        snapshot: SnapshotId,
    ) -> Result<Box<dyn RelationalCursor<VersionedRow>>>;
    fn exact_row(
        &self,
        table: &str,
        position: &[u8; 24],
    ) -> Result<Option<Box<dyn RelationalWindow<VersionedRow>>>>;
    fn index_cursor(
        &self,
        table: &str,
        index: &str,
        range: RelationalIndexRange<'_>,
    ) -> Result<Box<dyn RelationalCursor<RelationalPosting>>>;
    fn exact_index_cursor(
        &self,
        table: &str,
        index: &str,
        current: bool,
        key: &IndexKey,
    ) -> Result<Box<dyn RelationalCursor<RelationalPosting>>>;
    /// Construct full physical endpoints under source scratch admission.
    fn index_position_key(
        &self,
        current: bool,
        key: &IndexKey,
        row: RowId,
        created: TxId,
    ) -> Result<Box<dyn RelationalPositionKey>>;
    fn sync_source(&self, table: &str, row: RowId) -> Result<Option<(Lsn, SyncSourceKind)>>;
}

/// Retains only the currently inspected version while reverse seeking.
pub fn visible_row(
    source: &dyn RelationalReadSource,
    table: &str,
    row: RowId,
    snapshot: SnapshotId,
) -> Result<Option<Box<dyn RelationalWindow<VersionedRow>>>> {
    ensure_snapshot(source, snapshot)?;
    let mut cursor = source.row_id_cursor(table, row, snapshot)?;
    while let Some(window) = cursor.next_window()? {
        if window.value().created_tx.0 <= snapshot.0 {
            return Ok(window.value().visible_at(snapshot).then_some(window));
        }
    }
    Ok(None)
}

fn ensure_snapshot(source: &dyn RelationalReadSource, snapshot: SnapshotId) -> Result<()> {
    if snapshot.0 > source.snapshot().0 {
        return Err(contextdb_core::Error::Other(
            "relational snapshot exceeds the captured source frontier".into(),
        ));
    }
    Ok(())
}

/// Borrowed transaction state, frozen by the existing write-set owner. Exact
/// bounds seek the maintained staged postings instead of scanning inserts.
pub struct StagedExactProbe<'a> {
    pub ordered: &'a OrderedStagedRows,
    pub inserts: &'a [(String, VersionedRow)],
    /// The identity being validated is excluded from both staged and committed
    /// matches; other rows sharing the exact key must still be inspected.
    pub skip_row_id: Option<RowId>,
    /// Caller-admitted complete tuple/id endpoints for this exact key.
    pub lower: &'a (IndexKey, RowId),
    pub upper: &'a (IndexKey, RowId),
}

pub enum RelationalProbeHit<'a> {
    Committed(Box<dyn RelationalWindow<VersionedRow>>),
    Staged(&'a VersionedRow),
}

impl RelationalProbeHit<'_> {
    pub fn row(&self) -> &VersionedRow {
        match self {
            Self::Committed(window) => window.value(),
            Self::Staged(row) => row,
        }
    }
}

/// Shared PK/UNIQUE/FK/composite probe primitive. `hidden` is an incremental
/// identity lookup supplied by the engine, including staged deletes and
/// replaced committed identities. It must not rebuild or scan the write set.
/// Null/coercion/constraint policy stays with the engine's existing validators.
pub fn exact_probe<'a>(
    source: &dyn RelationalReadSource,
    table: &str,
    index: &str,
    key: &IndexKey,
    snapshot: SnapshotId,
    staged: Option<StagedExactProbe<'a>>,
    mut hidden: impl FnMut(RowId) -> bool,
) -> Result<Option<RelationalProbeHit<'a>>> {
    ensure_snapshot(source, snapshot)?;
    let skip_row_id = staged.as_ref().and_then(|probe| probe.skip_row_id);
    if let Some(staged) = staged {
        if staged.lower.0 != *key
            || staged.upper.0 != *key
            || staged.lower.1 != RowId(0)
            || staged.upper.1 != RowId(u64::MAX)
        {
            return Err(contextdb_core::Error::Other(
                "staged exact probe requires complete tuple bounds".into(),
            ));
        }
        if let Some(cursor) = staged.ordered.cursor(
            table,
            index,
            Bound::Included(staged.lower),
            Bound::Included(staged.upper),
            false,
        ) {
            for position in cursor {
                if Some(position.row_id) == skip_row_id {
                    continue;
                }
                let row = staged_row(staged.inserts, table, position)?;
                // The maintained overlay has already removed deleted staged
                // inserts; hidden suppresses committed predecessors only.
                return Ok(Some(RelationalProbeHit::Staged(row)));
            }
        }
    }
    let current = snapshot == source.snapshot();
    let mut cursor = source.exact_index_cursor(table, index, current, key)?;
    while let Some(posting) = cursor.next_window()? {
        let entry = &posting.value().entry;
        if Some(entry.row_id) == skip_row_id || hidden(entry.row_id) || !entry.visible_at(snapshot)
        {
            continue;
        }
        if let Some(row) = visible_row(source, table, entry.row_id, snapshot)? {
            return Ok(Some(RelationalProbeHit::Committed(row)));
        }
    }
    Ok(None)
}

pub fn staged_row<'a>(
    inserts: &'a [(String, VersionedRow)],
    table: &str,
    position: StagedPosition,
) -> Result<&'a VersionedRow> {
    let (row_table, row) = inserts.get(position.position).ok_or_else(|| {
        contextdb_core::Error::Other("staged relational posting has no row position".into())
    })?;
    if row_table != table || row.row_id != position.row_id || row.created_tx != position.created_tx
    {
        return Err(contextdb_core::Error::Other(
            "staged relational posting identity differs from its row".into(),
        ));
    }
    Ok(row)
}

/// The engine projects this key under scratch admission from a borrowed
/// OrderedStagedRows position. The charge follows the owned key, while the
/// write-set lock/frozen owner keeps the borrowed row stable.
pub struct StagedRelationalPosting<'a> {
    pub key: IndexKey,
    pub row: &'a VersionedRow,
    pub charge: std::sync::Arc<dyn contextdb_tx::StagedPostingCharge>,
}

pub enum OrderedRelationalHit<'a> {
    Committed {
        posting: Box<dyn RelationalWindow<RelationalPosting>>,
        row: Box<dyn RelationalWindow<VersionedRow>>,
    },
    Staged(StagedRelationalPosting<'a>),
}

impl OrderedRelationalHit<'_> {
    pub fn row(&self) -> &VersionedRow {
        match self {
            Self::Committed { row, .. } => row.value(),
            Self::Staged(posting) => posting.row,
        }
    }

    pub fn key(&self) -> &IndexKey {
        match self {
            Self::Committed { posting, .. } => &posting.value().key,
            Self::Staged(posting) => &posting.key,
        }
    }
}

/// Merge finite admitted windows in complete tuple/id order. Bounds and staged
/// projection use the same normalized ordering supplied by the engine. No
/// committed or staged prefix is collected or sorted; dropping this owner
/// releases its root, cursor and pending window even before exhaustion.
pub struct OrderedRelationalMerge<'a> {
    source: Option<std::sync::Arc<dyn RelationalReadSource>>,
    committed: Option<Box<dyn RelationalCursor<RelationalPosting>>>,
    staged: Option<Box<dyn Iterator<Item = Result<StagedRelationalPosting<'a>>> + 'a>>,
    committed_head: Option<Box<dyn RelationalWindow<RelationalPosting>>>,
    staged_head: Option<StagedRelationalPosting<'a>>,
    snapshot: SnapshotId,
    table: &'a str,
    reverse: bool,
    committed_done: bool,
    staged_done: bool,
    examined: u64,
}

impl<'a> OrderedRelationalMerge<'a> {
    pub fn new(
        source: std::sync::Arc<dyn RelationalReadSource>,
        staged: Box<dyn Iterator<Item = Result<StagedRelationalPosting<'a>>> + 'a>,
        table: &'a str,
        index: &str,
        snapshot: SnapshotId,
        range: RelationalIndexRange<'_>,
    ) -> Result<Self> {
        ensure_snapshot(source.as_ref(), snapshot)?;
        if range.current != (snapshot == source.snapshot()) {
            return Err(contextdb_core::Error::Other(
                "relational posting family differs from the requested snapshot".into(),
            ));
        }
        let reverse = range.reverse;
        let committed = source.index_cursor(table, index, range)?;
        Ok(Self {
            source: Some(source),
            committed: Some(committed),
            staged: Some(staged),
            committed_head: None,
            staged_head: None,
            snapshot,
            table,
            reverse,
            committed_done: false,
            staged_done: false,
            examined: 0,
        })
    }

    pub fn examined_entries(&self) -> u64 {
        self.committed
            .as_ref()
            .map_or(self.examined, |cursor| cursor.examined_entries())
    }

    /// Release LIMIT, cancellation and completion holds while the cursor
    /// object itself may survive in a higher-level continuation.
    pub fn cancel(&mut self) {
        self.examined = self.examined_entries();
        self.committed_head = None;
        self.staged_head = None;
        self.committed = None;
        self.staged = None;
        self.source = None;
        self.committed_done = true;
        self.staged_done = true;
    }

    pub fn next(
        &mut self,
        hidden_committed: impl FnMut(RowId) -> bool,
    ) -> Result<Option<OrderedRelationalHit<'a>>> {
        let result = self.next_inner(hidden_committed);
        if result.is_err() {
            self.cancel();
        }
        result
    }

    fn next_inner(
        &mut self,
        mut hidden_committed: impl FnMut(RowId) -> bool,
    ) -> Result<Option<OrderedRelationalHit<'a>>> {
        loop {
            while self.committed_head.is_none() && !self.committed_done {
                let Some(window) = self
                    .committed
                    .as_mut()
                    .expect("active source cursor")
                    .next_window()?
                else {
                    self.committed_done = true;
                    break;
                };
                let entry = &window.value().entry;
                if !hidden_committed(entry.row_id) && entry.visible_at(self.snapshot) {
                    self.committed_head = Some(window);
                }
            }
            if self.staged_head.is_none() && !self.staged_done {
                self.staged_head = self
                    .staged
                    .as_mut()
                    .expect("active staged cursor")
                    .next()
                    .transpose()?;
                self.staged_done = self.staged_head.is_none();
            }
            let staged_first = match (&self.committed_head, &self.staged_head) {
                (Some(committed), Some(staged)) => {
                    let posting = committed.value();
                    let order = posting
                        .key
                        .cmp(&staged.key)
                        .then_with(|| posting.entry.row_id.cmp(&staged.row.row_id));
                    // Equal identities are emitted from the staged owner only.
                    if order.is_eq() {
                        self.committed_head = None;
                        true
                    } else if self.reverse {
                        order.is_lt()
                    } else {
                        order.is_gt()
                    }
                }
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => {
                    self.cancel();
                    return Ok(None);
                }
            };
            if staged_first {
                return Ok(self.staged_head.take().map(OrderedRelationalHit::Staged));
            }
            let posting = self.committed_head.take().expect("committed head selected");
            if let Some(row) = visible_row(
                self.source.as_ref().expect("active source root").as_ref(),
                self.table,
                posting.value().entry.row_id,
                self.snapshot,
            )? {
                return Ok(Some(OrderedRelationalHit::Committed { posting, row }));
            }
        }
    }
}
