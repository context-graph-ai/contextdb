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
    requested_snapshot: SnapshotId,
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
    pub(crate) fn new_for_read_at_snapshot(
        root: Arc<WriterSnapshot>,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        snapshot: SnapshotId,
    ) -> Result<Self> {
        let mut source = Self::new_for_read(root, read)?;
        if snapshot.0 > source.root.snapshot.0 {
            return Err(Error::Other(
                "row snapshot exceeds captured frontier".into(),
            ));
        }
        source.requested_snapshot = snapshot;
        Ok(source)
    }

    pub(crate) fn verify_historical_versions(&self, table: &str) -> Result<()> {
        self.verify_historical_versions_with_checkpoint(table, &mut || Ok(()))
    }

    pub(crate) fn verify_historical_versions_with_checkpoint(
        &self,
        table: &str,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical row read requires its reader child".into(),
            ));
        };
        self.root.visit_historical_rows(
            table,
            None,
            self.root.snapshot,
            memory.clone(),
            checkpoint,
            &mut |_| Ok(()),
        )
    }

    pub(crate) fn exact_historical_row(
        &self,
        table: &str,
        position: &[u8; 24],
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.exact_historical_row_with_checkpoint(table, position, &mut || Ok(()))
    }

    pub(crate) fn exact_historical_row_with_checkpoint(
        &self,
        table: &str,
        position: &[u8; 24],
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "exact historical row requires its reader child".into(),
            ));
        };
        let row = RowId(u64::from_be_bytes(
            position[..8].try_into().expect("row identity"),
        ));
        self.root.visit_historical_rows(
            table,
            Some(row),
            self.root.snapshot,
            memory.clone(),
            checkpoint,
            &mut |_| Ok(()),
        )?;
        checkpoint()?;
        self.exact_row(table, position)
    }

    pub(crate) fn new_for_write(
        root: Arc<WriterSnapshot>,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::writer(writer))
    }
    fn new_owned(root: Arc<WriterSnapshot>, owner: SourceWindowOwner) -> Result<Self> {
        // This adapter is returned on the stack; its existing root Arc is cloned.
        let control = root.adapter_control(&owner, std::alloc::Layout::new::<Self>().size())?;
        let requested_snapshot = root.snapshot;
        Ok(Self {
            root,
            owner,
            requested_snapshot,
            _control: control,
        })
    }
    fn row_cursor_owned(
        &self,
        table: &str,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
        historical: Option<(SnapshotId, Option<RowId>)>,
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
        let historical = historical
            .map(|(snapshot, row_id)| {
                let credit = self.owner.reserve(table.len(), "historical_cursor_table")?;
                Ok::<_, Error>((
                    self.root.clone(),
                    Box::<str>::from(table),
                    snapshot,
                    row_id,
                    credit,
                ))
            })
            .transpose()?;
        let payload: Box<dyn RelationalCursor<VersionedRow>> =
            Box::new(RowCursor { cursor, historical });
        Ok(RelationalCursorOwner::new(payload, control))
    }

    /// Complete reciprocal candidate reconciliation precedes every historical
    /// index cursor success, including a stopped or zero-quota continuation.
    pub(crate) fn historical_index_preflight(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        self.historical_index_preflight_with_intent(table, index, snapshot, false, before)
    }

    /// An indexed point lookup consumes producer-maintained postings from this
    /// captured root. Authenticate their root and the row root before seeking;
    /// unrelated retained versions are not part of the lookup's working set.
    pub(crate) fn indexed_point_preflight(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if snapshot.0 > self.root.snapshot.0 {
            return Err(Error::Other(
                "index snapshot exceeds captured frontier".into(),
            ));
        }
        let names = self
            .owner
            .endpoint(&[b"rel_", table.as_bytes()], "index_witness_names")?;
        let row_name = std::str::from_utf8(&names.key).expect("UTF-8 table name");
        before()?;
        let read = self.root.owner_read()?;
        RedbPersistence::verify_history_root(&read, row_name)?;
        let _index_name = self.owner.reserve(
            table.len().saturating_add(index.len()).saturating_add(96),
            "index_witness_names",
        )?;
        let index_name = RedbPersistence::relational_index_table_name(table, index);
        before()?;
        RedbPersistence::verify_history_root(&read, &index_name)?;
        before()
    }

    pub(crate) fn indexed_point_row(
        &self,
        table: &str,
        row: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        let mut lower = [0u8; 24];
        lower[..8].copy_from_slice(&row.0.to_be_bytes());
        let mut upper = [255u8; 24];
        upper[..8].copy_from_slice(&row.0.to_be_bytes());
        upper[8..16].copy_from_slice(&snapshot.0.to_be_bytes());
        // The point preflight authenticated both producer roots. Seek the
        // selected row's latest version at the requested frontier directly.
        let mut cursor = self.row_cursor_owned(
            table,
            Bound::Included(&lower),
            Bound::Included(&upper),
            true,
            None,
        )?;
        let selected = cursor.next_window()?;
        Ok(selected.filter(|row| row.value().visible_at(snapshot)))
    }

    pub(crate) fn historical_index_versions_preflight(
        &self,
        table: &str,
        index: &str,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        // Explicit retained-version traversal is historical at every numeric
        // frontier. Validate every retained version's snapshot before the
        // original all-version index cursor or any callback can run.
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical versions require their reader child".into(),
            ));
        };
        self.root.visit_historical_rows(
            table,
            None,
            self.root.snapshot,
            memory.clone(),
            before,
            &mut |_| Ok(()),
        )?;
        self.historical_index_preflight_with_intent(
            table,
            index,
            self.root.snapshot,
            true,
            before,
        )?;
        let mut versions =
            self.row_cursor_owned(table, Bound::Unbounded, Bound::Unbounded, false, None)?;
        while let Some(row) = versions.next_window_before(before)? {
            self.historical_index_preflight_with_intent(
                table,
                index,
                SnapshotId(row.value().created_tx.0),
                true,
                before,
            )?;
        }
        before()
    }

    pub(crate) fn index_position_key_before<E: From<Error>>(
        &self,
        current: bool,
        key: &[DirectedValue],
        row: RowId,
        created: TxId,
        before: &mut dyn FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<OwnedReadBox<dyn RelationalPositionKey>, E> {
        let mut refusal = None;
        let result = self.root.index_position_key_with_checkpoint(
            self.owner.clone(),
            key,
            row,
            if current { TxId(0) } else { created },
            &mut || {
                before().map_err(|error| {
                    refusal = Some(error);
                    Error::ReadCancelled
                })
            },
        );
        if let Some(error) = refusal {
            return Err(error);
        }
        result
            .and_then(|window| window.boxed(|boxed| boxed as Box<dyn RelationalPositionKey>))
            .map_err(E::from)
    }

    fn historical_position_key(
        &self,
        key: &IndexKey,
        row: RowId,
        created: TxId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<OwnedReadBox<dyn RelationalPositionKey>> {
        self.root
            .index_position_key_with_checkpoint(self.owner.clone(), key, row, created, before)?
            .boxed(|boxed| boxed as Box<dyn RelationalPositionKey>)
    }

    fn historical_row_position_key(
        &self,
        declaration: &IndexDecl,
        row: &VersionedRow,
        created: TxId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<OwnedReadBox<dyn RelationalPositionKey>> {
        self.root
            .row_index_position_key_with_checkpoint(
                self.owner.clone(),
                declaration,
                row,
                created,
                before,
            )?
            .boxed(|boxed| boxed as Box<dyn RelationalPositionKey>)
    }

    fn historical_index_preflight_with_intent(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        versions: bool,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if !versions && snapshot == self.root.snapshot {
            return Ok(());
        }
        if snapshot.0 > self.root.snapshot.0 {
            return Err(Error::Other(
                "index snapshot exceeds captured frontier".into(),
            ));
        }
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical index read requires its reader child".into(),
            ));
        };
        let meta = self
            .root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let declaration = meta
            .indexes
            .iter()
            .find(|decl| decl.name == index)
            .ok_or_else(|| Error::IndexNotFound {
                table: table.into(),
                index: index.into(),
            })?;
        let damage = |reason: &str| Error::StoreCorrupted {
            path: table.into(),
            reason: reason.into(),
        };
        let _state = self.owner.reserve(
            std::mem::size_of::<(
                u64,
                u64,
                Option<SourceResume>,
                Option<WriterWindow<VersionedRow>>,
                std::cell::RefCell<&mut dyn FnMut() -> Result<()>>,
            )>(),
            "index_preflight_controls",
        )?;
        let touch = std::cell::RefCell::new(before);
        let mut expected = 0u64;
        self.root.visit_historical_rows(
            table,
            None,
            snapshot,
            memory.clone(),
            &mut || (touch.borrow_mut())(),
            &mut |row| {
                (touch.borrow_mut())()?;
                (touch.borrow_mut())()?;
                (touch.borrow_mut())()?;
                let low = self.historical_row_position_key(
                    declaration,
                    &row.value,
                    TxId(0),
                    &mut || (touch.borrow_mut())(),
                )?;
                (touch.borrow_mut())()?;
                let high = self.historical_row_position_key(
                    declaration,
                    &row.value,
                    TxId(u64::MAX),
                    &mut || (touch.borrow_mut())(),
                )?;
                let mut postings = self.index_cursor(
                    table,
                    index,
                    RelationalIndexRange {
                        current: false,
                        lower: Bound::Included(low.as_bytes()),
                        upper: Bound::Included(high.as_bytes()),
                        reverse: false,
                    },
                )?;
                let mut visible = 0u64;
                while let Some(posting) =
                    postings.next_window_before(&mut || (touch.borrow_mut())())?
                {
                    RedbPersistence::validated_index_key(declaration, meta, &posting.value().key)
                        .map_err(|reason| damage(&reason))?;
                    (touch.borrow_mut())()?;
                    let raw = self.historical_position_key(
                        &posting.value().key,
                        posting.value().entry.row_id,
                        posting.value().entry.created_tx,
                        &mut || (touch.borrow_mut())(),
                    )?;
                    if !content_identity::version_manifest::equal_with_checkpoint(
                        raw.as_bytes(),
                        posting.position_key(),
                        &mut || (touch.borrow_mut())(),
                    )? {
                        return Err(damage("historical posting raw tie bytes are noncanonical"));
                    }
                    drop(raw);
                    if posting.value().entry.created_tx.0 > self.root.snapshot.0
                        || posting.value().entry.deleted_tx.is_some_and(|tx| {
                            tx <= posting.value().entry.created_tx || tx.0 > self.root.snapshot.0
                        })
                    {
                        return Err(damage("historical posting lifecycle exceeds its capture"));
                    }
                    if posting.value().entry.visible_at(snapshot) {
                        let canonical = self.historical_row_position_key(
                            declaration,
                            &row.value,
                            posting.value().entry.created_tx,
                            &mut || (touch.borrow_mut())(),
                        )?;
                        if posting.value().entry.row_id != row.value.row_id
                            || !content_identity::version_manifest::equal_with_checkpoint(
                                canonical.as_bytes(),
                                posting.position_key(),
                                &mut || (touch.borrow_mut())(),
                            )?
                        {
                            return Err(damage(
                                "historical posting differs from authenticated row key",
                            ));
                        }
                        visible = visible
                            .checked_add(1)
                            .ok_or_else(|| damage("posting count overflow"))?;
                    }
                }
                if visible != 1 {
                    return Err(damage(
                        "authenticated visible row lacks exactly one historical posting",
                    ));
                }
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| damage("visible row count overflow"))?;
                Ok(())
            },
        )?;
        let mut postings = self.index_cursor(
            table,
            index,
            RelationalIndexRange {
                current: false,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                reverse: false,
            },
        )?;
        let mut previous = None::<SourceResume>;
        let mut actual = 0u64;
        while let Some(posting) = postings.next_window_before(&mut || (touch.borrow_mut())())? {
            RedbPersistence::validated_index_key(declaration, meta, &posting.value().key)
                .map_err(|reason| damage(&reason))?;
            (touch.borrow_mut())()?;
            let raw = self.historical_position_key(
                &posting.value().key,
                posting.value().entry.row_id,
                posting.value().entry.created_tx,
                &mut || (touch.borrow_mut())(),
            )?;
            if !content_identity::version_manifest::equal_with_checkpoint(
                raw.as_bytes(),
                posting.position_key(),
                &mut || (touch.borrow_mut())(),
            )? {
                return Err(damage("historical posting raw tie bytes are noncanonical"));
            }
            drop(raw);
            if posting.value().entry.created_tx.0 > self.root.snapshot.0
                || posting.value().entry.deleted_tx.is_some_and(|tx| {
                    tx <= posting.value().entry.created_tx || tx.0 > self.root.snapshot.0
                })
            {
                return Err(damage("historical posting lifecycle exceeds its capture"));
            }
            if let Some(previous) = &previous
                && content_identity::validated_preflight_posting_order(
                    &previous.key,
                    posting.position_key(),
                    &mut || (touch.borrow_mut())(),
                )? != std::cmp::Ordering::Less
            {
                return Err(damage("historical postings are duplicated or reordered"));
            }
            previous = Some(
                self.owner
                    .endpoint(&[posting.position_key()], "index_preflight_previous")?,
            );
            let mut selected = None;
            self.root.visit_historical_rows(
                table,
                Some(posting.value().entry.row_id),
                snapshot,
                memory.clone(),
                &mut || (touch.borrow_mut())(),
                &mut |row| {
                    selected = Some(row);
                    Ok(())
                },
            )?;
            if posting.value().entry.visible_at(snapshot) {
                let row = selected.ok_or_else(|| {
                    damage("visible historical posting has no authenticated visible row")
                })?;
                (touch.borrow_mut())()?;
                let canonical = self.historical_row_position_key(
                    declaration,
                    &row.value,
                    posting.value().entry.created_tx,
                    &mut || (touch.borrow_mut())(),
                )?;
                if !content_identity::version_manifest::equal_with_checkpoint(
                    canonical.as_bytes(),
                    posting.position_key(),
                    &mut || (touch.borrow_mut())(),
                )? {
                    return Err(damage(
                        "historical posting is an extra or moved visible candidate",
                    ));
                }
                actual = actual
                    .checked_add(1)
                    .ok_or_else(|| damage("visible posting count overflow"))?;
            }
        }
        if actual != expected {
            return Err(damage("historical index candidate counts differ"));
        }
        (touch.borrow_mut())()?;
        Ok(())
    }
}
struct RowCursor {
    cursor: WriterReadCursor,
    historical: Option<(
        Arc<WriterSnapshot>,
        Box<str>,
        SnapshotId,
        Option<RowId>,
        SourceWindowCredit,
    )>,
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
        if let Some((_, _, snapshot, _, _)) = &self.historical {
            return self
                .cursor
                .next_historical_row_before_handoff(*snapshot, before, |window| {
                    window.boxed(|boxed| boxed as Box<dyn RelationalWindow<VersionedRow>>)
                });
        }
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
        self.row_cursor_owned(
            table,
            lower,
            upper,
            reverse,
            (self.requested_snapshot.0 < self.root.snapshot.0)
                .then_some((self.requested_snapshot, None)),
        )
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
        self.row_cursor_owned(
            table,
            Bound::Included(&lower),
            Bound::Included(&upper),
            true,
            (snapshot.0 < self.root.snapshot.0).then_some((snapshot, Some(row))),
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
        self.root
            .index_position_key(
                self.owner.clone(),
                key,
                row,
                if current { TxId(0) } else { created },
            )?
            .boxed(|boxed| boxed as Box<dyn RelationalPositionKey>)
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
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::writer(writer))
    }
    fn new_owned(root: Arc<WriterSnapshot>, owner: SourceWindowOwner) -> Result<Self> {
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = match &owner {
            SourceWindowOwner::Read(memory) => memory.clone(),
            SourceWindowOwner::Write(memory, original_use) => {
                memory.read_budget_for_original(original_use.clone())?
            }
        };
        super::graph_read::with_pages(memory, || {
            let _binding = owner.reserve(130, "graph_root_binding")?;
            RedbPersistence::read_authenticated_bounds_in_read(root.owner_read()?.as_ref())
                .map(|_| ())
        })?;
        // Payload admission only. No guessed standard Arc control layout.
        let control = root.adapter_control(&owner, std::alloc::Layout::new::<Self>().size())?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            let (funding_owner, owner_identity, control_bytes) = match (&owner, &control) {
                (SourceWindowOwner::Read(owner), SourceWindowCredit::Read(credit)) => (
                    crate::read_work_probe::funding_owner_name(owner),
                    crate::read_work_probe::funding_owner_id(owner),
                    credit.bytes(),
                ),
                (SourceWindowOwner::Write(owner, _), SourceWindowCredit::Write(credit)) => (
                    format!(
                        "{}@{:p}",
                        std::any::type_name::<crate::memory_accounting::MemoryAccountant>(),
                        owner.as_ptr()
                    ),
                    owner.as_ptr() as usize,
                    credit.bytes(),
                ),
                _ => unreachable!("control credit follows its funding owner"),
            };
            crate::read_work_probe::note_graph_snapshot_capture(
                root.storage_identity(),
                funding_owner,
                owner_identity,
                control_bytes,
            );
        }
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
