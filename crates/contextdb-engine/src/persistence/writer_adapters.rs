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
    working: Option<super::received_working_source::WorkingReadSource>,
    root: WriterSnapshotHandle,
    owner: SourceWindowOwner,
    requested_snapshot: SnapshotId,
    _control: SourceWindowCredit,
}
impl WriterRelationalSource {
    pub(crate) fn for_working(mut self,working:super::received_working_source::WorkingReadSource)->Self {
        self.requested_snapshot=working.snapshot();
        self.working=Some(working);
        self
    }

    pub(crate) fn row_head(&self, table: &str, row: RowId) -> Result<(Lsn, [u8; 24])> {
        if let Some(source)=&self.working {return source.row_head(table,row);}
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
        if let Some(source)=&self.working {
            source.preflight(table,None,source.snapshot(),&mut ||Ok(()))?;
            return Ok(super::writer_cleanup_progress::CurrencyCleanupProgress {
                declaration,through:source.lsn(),after_group:None,group:None,
            });
        }
        RedbPersistence::currency_cleanup_progress(&self.root, table, declaration)
    }
    pub(crate) fn new_for_read(
        root: WriterSnapshotHandle,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Read(read))
    }
    pub(crate) fn new_for_read_at_snapshot(
        root: WriterSnapshotHandle,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        snapshot: SnapshotId,
    ) -> Result<Self> {
        let mut source = Self::new_for_read(root, read)?;
        if snapshot.0 > source.root.snapshot.0 {
            return Err(Error::Other(
                "row snapshot exceeds captured frontier".into()));
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
        if let Some(source)=&self.working {return source.preflight(table,None,source.snapshot(),checkpoint);}
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical row read requires its reader child".into()));
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
        if let Some(source)=&self.working {
            source.preflight(table,None,source.snapshot(),checkpoint)?;
            return source.exact_row(table,position);
        }
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "exact historical row requires its reader child".into()));
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
        root: WriterSnapshotHandle,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::writer(writer))
    }
    fn new_owned(root: WriterSnapshotHandle, owner: SourceWindowOwner) -> Result<Self> {
        // This adapter is returned on the stack; its existing root Arc is cloned.
        let control = root.adapter_control(&owner, std::alloc::Layout::new::<Self>().size())?;
        let requested_snapshot = root.snapshot;
        Ok(Self {
            working: None,
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
            .map(|_| self.owner.reserve(table.len(), "historical_cursor_table"))
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
        if let Some(source)=&self.working {return source.preflight(table,Some(index),snapshot,before);}
        if snapshot.0 > self.root.snapshot.0 {
            return Err(Error::Other(
                "index snapshot exceeds captured frontier".into()));
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

    /// The row-only half of [`Self::indexed_point_preflight`]: authenticate
    /// the row table's history root once at this captured root before exact
    /// version seeks. No index participates in a direct version read.
    pub(crate) fn row_point_preflight(
        &self,
        table: &str,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if let Some(source)=&self.working {return source.preflight(table,None,snapshot,before);}
        if snapshot.0 > self.root.snapshot.0 {
            return Err(Error::Other(
                "row snapshot exceeds captured frontier".into(),
            ));
        }
        let names = self
            .owner
            .endpoint(&[b"rel_", table.as_bytes()], "index_witness_names")?;
        let row_name = std::str::from_utf8(&names.key).expect("UTF-8 table name");
        before()?;
        let read = self.root.owner_read()?;
        RedbPersistence::verify_history_root(&read, row_name)?;
        before()
    }

    pub(crate) fn indexed_point_row(
        &self,
        table: &str,
        row: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        if let Some(source)=&self.working {
            return contextdb_relational::source::visible_row(source,table,row,snapshot);
        }
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
        #[cfg(any(test, feature = "test-seams"))]
        let selected =
            super::writer_source::with_exact_row_fault_for_test(|| cursor.next_window())?;
        #[cfg(not(any(test, feature = "test-seams")))]
        let selected = cursor.next_window()?;
        Ok(selected.filter(|row| row.value().visible_at(snapshot)))
    }

    pub(crate) fn historical_index_versions_preflight(
        &self,
        table: &str,
        index: &str,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if let Some(source)=&self.working {return source.preflight(table,Some(index),source.snapshot(),before);}
        // Explicit retained-version traversal is historical at every numeric
        // frontier. Validate every retained version's snapshot before the
        // original all-version index cursor or any callback can run.
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical versions require their reader child".into()));
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

    /// An ordinary historical index read authenticates the row and index
    /// roots it reads under, once, at a cost independent of the table: each
    /// posting and version it then consumes is bounded and compared as it is
    /// read (`check_historical_posting`, `check_posting_matches_version`).
    /// Only an explicit all-version traversal still reconciles every row
    /// with every posting, because visiting every version is that
    /// operation.
    fn historical_index_roots_preflight(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if let Some(source)=&self.working {return source.preflight(table,Some(index),snapshot,before);}
        self.indexed_point_preflight(table, index, snapshot, before)?;
        // Completeness without a walk: the writer's published commitment
        // says these postings cover exactly what the row versions cover.
        let meta = self
            .root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let declaration = RedbPersistence::relational_index_declarations(meta)
            .into_iter()
            .find(|declaration| declaration.name == index)
            .ok_or_else(|| Error::IndexNotFound {
                table: table.into(),
                index: index.into(),
            })?;
        before()?;
        let read = self.root.owner_read()?;
        let damage = |reason: &str| Error::StoreCorrupted {
            path: RedbPersistence::relational_index_table_name(table, index),
            reason: reason.into(),
        };
        match historical_index_commitment::published_in_read(&read, table, &declaration)? {
            Some((entry, equal)) => {
                before()?;
                content_identity::ContentIdentityReadVerifier::open(&read, Path::new(table), None)?
                    .verify(&entry)?;
                if !equal {
                    return Err(damage(
                        "historical index postings do not cover what its row versions cover",
                    ));
                }
            }
            None => {
                let name = RedbPersistence::relational_index_table_name(table, index);
                let empty = match read.open_table(
                    TableDefinition::<DurableRelationalIndexKey, &[u8]>::new(name.as_str()),
                ) {
                    Ok(postings) => postings
                        .is_empty()
                        .map_err(RedbPersistence::storage_error)?,
                    Err(redb::TableError::TableDoesNotExist(_)) => true,
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                if !empty {
                    return Err(damage("historical index postings carry no commitment"));
                }
            }
        }
        before()
    }

    /// Bound one consumed historical posting before anything decides its
    /// visibility: a valid key, canonical tie bytes, and a lifecycle inside
    /// the capture. Malformed metadata on a dead group refuses; it never
    /// reads as an empty answer.
    pub(crate) fn check_historical_posting(
        &self,
        table: &str,
        index: &str,
        key: &IndexKey,
        entry: &IndexEntry,
        position: &[u8],
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let working_meta=self.working.as_ref().map(|source|source.metadata(table)).transpose()?;
        let meta=if let Some(meta)=&working_meta {meta.value()}else{
            self.root.borrow_captured_table_meta(table).ok_or_else(||Error::TableNotFound(table.into()))?
        };
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
        RedbPersistence::validated_index_key(declaration, meta, key)
            .map_err(|reason| damage(&reason))?;
        before()?;
        let raw = self.historical_position_key(key, entry.row_id, entry.created_tx, before)?;
        if !content_identity::version_manifest::equal_with_checkpoint(
            raw.as_bytes(),
            position,
            before,
        )? {
            return Err(damage("historical posting raw tie bytes are noncanonical"));
        }
        if entry.created_tx.0 > self.snapshot().0
            || entry
                .deleted_tx
                .is_some_and(|tx| tx <= entry.created_tx || tx.0 > self.snapshot().0)
        {
            return Err(damage("historical posting lifecycle exceeds its capture"));
        }
        Ok(())
    }

    /// A historical posting covers one interval of a row's life during which
    /// its indexed key did not change; retention may split it. The visible
    /// version it selects must be the same row, lie inside that interval,
    /// and carry that key: the posting is at the version's canonical
    /// position for the interval's start.
    pub(crate) fn check_posting_matches_version(
        &self,
        table: &str,
        index: &str,
        entry: &IndexEntry,
        position: &[u8],
        row: &VersionedRow,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let working_meta=self.working.as_ref().map(|source|source.metadata(table)).transpose()?;
        let meta=if let Some(meta)=&working_meta {meta.value()}else{
            self.root.borrow_captured_table_meta(table).ok_or_else(||Error::TableNotFound(table.into()))?
        };
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
        let inside = entry.created_tx <= row.created_tx
            && match (entry.deleted_tx, row.deleted_tx) {
                (None, _) => true,
                (Some(end), Some(deleted)) => deleted <= end,
                (Some(_), None) => false,
            };
        if entry.row_id != row.row_id || !inside {
            return Err(damage(
                "historical posting interval does not contain its selected version",
            ));
        }
        before()?;
        let canonical =
            self.historical_row_position_key(declaration, row, entry.created_tx, before)?;
        if !content_identity::version_manifest::equal_with_checkpoint(
            canonical.as_bytes(),
            position,
            before,
        )? {
            return Err(damage(
                "historical posting differs from authenticated row key",
            ));
        }
        Ok(())
    }

    fn historical_index_preflight_with_intent(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        versions: bool,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if let Some(source)=&self.working {return source.preflight(table,Some(index),snapshot,before);}
        if !versions && snapshot == self.root.snapshot {
            return Ok(());
        }
        if !versions {
            return self.historical_index_roots_preflight(table, index, snapshot, before);
        }
        if snapshot.0 > self.root.snapshot.0 {
            return Err(Error::Other(
                "index snapshot exceeds captured frontier".into()));
        }
        let SourceWindowOwner::Read(memory) = &self.owner else {
            return Err(Error::Other(
                "historical index read requires its reader child".into()));
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
    /// Present for a historical cursor; holds that mode's table-name credit.
    historical: Option<SourceWindowCredit>,
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
        if self.historical.is_some() {
            return self
                .cursor
                .next_historical_row_before_handoff(before, |window| {
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
        #[cfg(any(test, feature = "test-seams"))]
        let historical = !self.current;
        self.cursor
            .next_index_before_handoff(self.current, before, |window| {
                #[cfg(any(test, feature = "test-seams"))]
                if historical {
                    crate::read_work_probe::note_historical_posting_window();
                }
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
        if let Some(source)=&self.working {return source.validate_table(table);}
        self.root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))
            .map(|_| ())
    }
    fn snapshot(&self) -> SnapshotId {
        if let Some(source)=&self.working {return source.snapshot();}
        self.root.snapshot
    }
    fn lsn(&self) -> Lsn {
        if let Some(source)=&self.working {return source.lsn();}
        self.root.lsn
    }
    fn row_cursor(
        &self,
        table: &str,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        reverse: bool,
    ) -> Result<RelationalCursorOwner<VersionedRow>> {
        if let Some(source)=&self.working {return source.row_cursor(table,lower,upper,reverse);}
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
        if let Some(source)=&self.working {return source.row_id_cursor(table,row,snapshot);}
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
        if let Some(source)=&self.working {return source.exact_row(table,position);}
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
        if let Some(source)=&self.working {return source.index_cursor(table,index,range);}
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
        if let Some(source)=&self.working {return source.exact_index_cursor(table,index,current,key);}
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
        if let Some(source)=&self.working {return source.index_position_key(current,key,row,created);}
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
        if let Some(source)=&self.working {return source.sync_source(table,row);}
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
    working: Option<super::received_working_source::WorkingReadSource>,
    root: WriterSnapshotHandle,
    owner: SourceWindowOwner,
    _control: SourceWindowCredit,
}
impl WriterGraphSnapshot {
    pub(crate) fn for_working(mut self, source: super::received_working_source::WorkingReadSource) -> Self {
        self.working = Some(source);
        self
    }
    pub(crate) fn new_for_read(
        root: WriterSnapshotHandle,
        read: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::Read(read))
    }
    pub(crate) fn new_for_write(
        root: WriterSnapshotHandle,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        Self::new_owned(root, SourceWindowOwner::writer(writer))
    }
    fn new_owned(root: WriterSnapshotHandle, owner: SourceWindowOwner) -> Result<Self> {
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
            working: None,
            root,
            owner,
            _control: control,
        })
    }
}
struct EdgeCursor {
    cursor: WriterReadCursor,
    row_cursor: WriterReadCursor,
    reverse: bool,
    raw_head: Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>,
    row_head: Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>,
    raw_done: bool,
    row_done: bool,
}
struct ExactRowEdgeCursor {
    cursor: WriterReadCursor,
    reverse: bool,
}
impl contextdb_graph::source::GraphEdgeWindow for WriterWindow<contextdb_tx::OwnedAdjEntry> {
    fn edge(&self) -> &AdjEntry {
        &self.value.entry
    }
    fn row_owner(&self) -> Option<&contextdb_tx::RowEdgeOwner> {
        Some(&self.value.owner)
    }
}
impl contextdb_graph::source::GraphCursor for ExactRowEdgeCursor {
    fn next_edge(
        &mut self,
    ) -> Result<Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>> {
        self.cursor.next_row_edge_handoff(self.reverse, |window| {
            window.boxed(|boxed| boxed as Box<dyn contextdb_graph::source::GraphEdgeWindow>)
        })
    }
}
impl contextdb_graph::source::GraphEdgeWindow for WriterWindow<AdjEntry> {
    fn edge(&self) -> &AdjEntry {
        &self.value
    }
}
impl contextdb_graph::source::GraphCursor for EdgeCursor {
    fn next_source_window_bound(&self) -> usize {
        usize::from(!self.raw_done && self.raw_head.is_none())
            + usize::from(!self.row_done && self.row_head.is_none())
    }
    fn next_edge(
        &mut self,
    ) -> Result<Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>> {
        if !self.raw_done && self.raw_head.is_none() {
            self.raw_head = self.cursor.next_edge_handoff(|window| {
                window.boxed(|boxed| boxed as Box<dyn contextdb_graph::source::GraphEdgeWindow>)
            })?;
            self.raw_done = self.raw_head.is_none();
        }
        if !self.row_done && self.row_head.is_none() {
            self.row_head = self
                .row_cursor
                .next_row_edge_handoff(self.reverse, |window| {
                    window.boxed(|boxed| boxed as Box<dyn contextdb_graph::source::GraphEdgeWindow>)
                })?;
            self.row_done = self.row_head.is_none();
        }
        let take_raw = match (&self.raw_head, &self.row_head) {
            (Some(raw), Some(row)) => {
                let a = raw.edge();
                let b = row.edge();
                let (a_first, a_second, b_first, b_second) = if self.reverse {
                    (a.target, a.source, b.target, b.source)
                } else {
                    (a.source, a.target, b.source, b.target)
                };
                (a_first, a_second, &a.edge_type) <= (b_first, b_second, &b.edge_type)
            }
            (Some(_), None) => true,
            (None, _) => false,
        };
        Ok(if take_raw {
            self.raw_head.take()
        } else {
            self.row_head.take()
        })
    }
}
impl contextdb_graph::source::GraphSnapshot for WriterGraphSnapshot {
    fn row_cursor(
        &self,
        query: contextdb_graph::source::GraphQuery<'_>,
        owner: &contextdb_tx::RowEdgeOwner,
    ) -> Result<contextdb_graph::source::GraphCursorOwner> {
        let reverse = match query.direction {
            Direction::Outgoing => false,
            Direction::Incoming => true,
            Direction::Both => {
                return Err(Error::Other("owned adjacency needs one direction".into()));
            }
        };
        let (other, edge_type) = query
            .identity
            .ok_or_else(|| Error::Other("owned adjacency needs its exact triad".into()))?;
        let table_len = u32::try_from(owner.table.len())
            .map_err(|_| Error::Other("owned table name overflow".into()))?
            .to_be_bytes();
        let key = self.owner.endpoint(
            &[
                query.node.as_bytes(),
                other.as_bytes(),
                edge_type.as_bytes(),
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
        let cursor = self.root.cursor(
            self.owner.clone(),
            if reverse {
                super::row_graph::REVERSE.name()
            } else {
                super::row_graph::FORWARD.name()
            },
            WriterCursorFamily::Bytes,
            Bound::Included(&key.key),
            Bound::Included(&key.key),
            false,
        )?;
        let control = self
            .owner
            .reserve_box_credit(std::alloc::Layout::new::<ExactRowEdgeCursor>().size())?;
        let payload: Box<dyn contextdb_graph::source::GraphCursor> =
            Box::new(ExactRowEdgeCursor { cursor, reverse });
        Ok(contextdb_graph::source::GraphCursorOwner::new(
            payload, control,
        ))
    }

    fn cursor(
        &self,
        query: contextdb_graph::source::GraphQuery<'_>,
    ) -> Result<contextdb_graph::source::GraphCursorOwner> {
        if let Some(source) = &self.working { return source.graph_cursor(query); }
        let reverse = match query.direction {
            Direction::Outgoing => false,
            Direction::Incoming => true,
            Direction::Both => {
                return Err(Error::Other(
                    "graph source requires a directional cursor".into()));
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
        let prefix = if let Some((other, edge_type)) = query.identity {
            self.owner.endpoint(
                &[
                    query.node.as_bytes(),
                    other.as_bytes(),
                    edge_type.as_bytes(),
                    &[0],
                ],
                "row_graph_prefix",
            )?
        } else {
            self.owner
                .endpoint(&[query.node.as_bytes()], "row_graph_prefix")?
        };
        let end = self.owner.prefix_end(&prefix.key, "row_graph_prefix")?;
        let row_cursor = self.root.cursor(
            self.owner.clone(),
            if reverse {
                super::row_graph::REVERSE.name()
            } else {
                super::row_graph::FORWARD.name()
            },
            WriterCursorFamily::Bytes,
            Bound::Included(&prefix.key),
            end.as_ref()
                .map_or(Bound::Unbounded, |end| Bound::Excluded(end.key.as_ref())),
            false,
        )?;
        let payload: Box<dyn contextdb_graph::source::GraphCursor> = Box::new(EdgeCursor {
            cursor,
            row_cursor,
            reverse,
            raw_head: None,
            row_head: None,
            raw_done: false,
            row_done: false,
        });
        Ok(contextdb_graph::source::GraphCursorOwner::new(
            payload, control,
        ))
    }
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
