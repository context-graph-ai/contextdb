//! Durable vector membership sources for a writable store.
//!
//! Every operation captures the writer's current published root once and
//! reads only that root, so one count, probe or cursor never mixes two
//! frontiers. Row commits publish their memberships and the maintained
//! summaries in the same transaction as the row, so a later operation sees
//! them without the vector store holding any decoded directory. Each record
//! read is authenticated (checksum, key and route) before it is used.
use super::*;
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};
use contextdb_vector::store::{
    DormantRawVectorLoader, DormantVectorLoadRequest, RawVectorInspectionSnapshot,
    RawVectorMembershipCursor, RawVectorMembershipSource, RawVectorMembershipSummary,
    RawVectorReverseMembershipCursor, RawVectorReverseMembershipSource,
};
use std::ops::Bound;

const HINT: &str = "Raise MEMORY_LIMIT to read this vector membership window.";

#[cfg(feature = "test-seams")]
thread_local! {
    static PENDING_SUMMARY_FAILURE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn with_pending_summary_failure_for_test<R>(
    persistence: Option<usize>,
    action: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PENDING_SUMMARY_FAILURE.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(PENDING_SUMMARY_FAILURE.with(|slot| slot.replace(persistence)));
    action()
}

fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "vector_partition_memberships".into(),
        reason: reason.into(),
    }
}

/// One operation's root: the writer's published root once the writer is
/// installed, otherwise (only while the store is still opening) a plain read
/// of the committed file.
struct MembershipRoot {
    read: Arc<redb::ReadTransaction>,
    writer: Option<Arc<WriterSnapshot>>,
}

impl MembershipRoot {
    fn capture(persistence: &RedbPersistence) -> Result<Self> {
        if let Ok(capture) = persistence.paged_root_capture() {
            let (read, handle) =
                capture().map_err(vector_membership_pages::typed_metadata_error)?;
            let writer = handle
                .downcast::<WriterSnapshot>()
                .map_err(|_| damage("writer root capture returned another owner"))?;
            return Ok(Self {
                read,
                writer: Some(writer),
            });
        }
        let read = persistence.with_db(|db| {
            db.begin_read()
                .map_err(vector_membership_pages::metadata_error)
        })?;
        Ok(Self {
            read: Arc::new(read),
            writer: None,
        })
    }

    fn check_scope(&self, table: &str) -> Result<()> {
        match &self.writer {
            Some(writer) => writer.check_table_scope(table),
            None => Ok(()),
        }
    }

    /// The newest snapshot this root holds, when it is the writer's root.
    fn snapshot(&self) -> Option<SnapshotId> {
        self.writer.as_ref().map(|writer| writer.snapshot)
    }
}

fn read_summary(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
) -> Result<RawVectorMembershipSummary> {
    let summary = vector_membership_pages::read_partition_summary(read, partition)?;
    summary_counts(&summary)
}

fn summary_counts(
    summary: &vector_membership_pages::MembershipSummary,
) -> Result<RawVectorMembershipSummary> {
    Ok(RawVectorMembershipSummary {
        entries: usize::try_from(summary.entries)
            .map_err(|_| damage("membership count overflow"))?,
        live_entries: usize::try_from(summary.live_entries)
            .map_err(|_| damage("live membership count overflow"))?,
        max_tx: summary.max_tx,
    })
}

/// The body identity a raw-vector loader resolves.
fn body_identity(record: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    RawVectorDirectoryEntry {
        row_id: record.row_id,
        created_tx: record.vector_created_tx,
        deleted_tx: record.deleted_tx,
        lsn: record.vector_lsn,
    }
}

/// The visibility interval a cursor reports: the owning row version's
/// interval, which is when this placement is visible.
fn visibility_interval(record: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    RawVectorDirectoryEntry {
        row_id: record.row_id,
        created_tx: record.visible_from,
        deleted_tx: record.deleted_tx,
        lsn: record.vector_lsn,
    }
}

fn visible(record: &VectorPartitionMembershipRecord, snapshot: SnapshotId) -> bool {
    record.visible_from.0 <= snapshot.0 && record.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
}

fn decode_forward(
    partition: &VectorPartitionRef,
    key: &[u8],
    value: &[u8],
) -> Result<VectorPartitionMembershipRecord> {
    #[cfg(feature = "test-seams")]
    crate::database::resident_history_test_seams::note(|work| work.membership_keys_examined += 1);
    let record: VectorPartitionMembershipRecord =
        decode_vector_partition_record_payload(VectorPartitionRecordKind::Membership, value)
            .map_err(|reason| damage(&reason))?;
    if RedbPersistence::vector_partition_membership_key(&record).as_slice() != key
        || record.index != partition.index
        || record.partition_key != partition.partition_key
        || record.visible_from != record.row_created_tx.max(record.vector_created_tx)
        || record
            .deleted_tx
            .is_some_and(|tx| tx <= record.visible_from)
    {
        return Err(damage(
            "membership key, route or lifecycle differs from its authenticated value",
        ));
    }
    Ok(record)
}

pub(crate) type PublishedMembershipRoot =
    Arc<dyn Fn() -> Result<Option<Arc<WriterSnapshot>>> + Send + Sync>;

/// One partition's membership directory, read from the forward membership
/// table at each operation's captured root.
pub(crate) struct DurableMembershipSource {
    published_root: PublishedMembershipRoot,
    persistence: Arc<RedbPersistence>,
    partition: VectorPartitionRef,
    summary: RawVectorMembershipSummary,
    accountant: Arc<MemoryAccountant>,
}

impl DurableMembershipSource {
    pub(crate) fn new(
        persistence: Arc<RedbPersistence>,
        partition: VectorPartitionRef,
        summary: RawVectorMembershipSummary,
        accountant: Arc<MemoryAccountant>,
        published_root: PublishedMembershipRoot,
    ) -> Arc<Self> {
        Arc::new(Self {
            published_root,
            persistence,
            partition,
            summary,
            accountant,
        })
    }

    // Compare physical user-tree headers, independent of logical TxId and
    // of the Arc wrapper used to materialize a retained root.
    fn captured_catalog_required(&self, root: &MembershipRoot) -> Result<bool> {
        let Some(captured) = &root.writer else {
            return Ok(false);
        };
        let published = (self.published_root)()?
            .ok_or_else(|| damage("writer publication has no durable root"))?;
        Ok(captured.storage_identity().user_root != published.storage_identity().user_root)
    }

    fn catalog_frontier(&self, root: &MembershipRoot) -> Result<Option<TxId>> {
        let generations = self.catalog_generations(root)?;
        Ok(generations.map(|(base, change)| change.unwrap_or(base).covered_tx))
    }

    fn catalog_generations(
        &self,
        root: &MembershipRoot,
    ) -> Result<
        Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
    > {
        match self
            .persistence
            .vector_partition_generation_descriptors_in_read(
                &root.read,
                &self.partition.index,
                &self.partition.partition_key,
            )
            .map_err(vector_membership_pages::typed_metadata_error)
        {
            Ok(generations) => Ok(generations),
            // Catalog damage belongs to its route. The caller has already
            // authenticated the membership summary at this captured root.
            Err(Error::StoreCorrupted { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn window_bytes(&self) -> usize {
        self.partition
            .index
            .table
            .len()
            .saturating_add(self.partition.index.column.len())
            .saturating_add(self.partition.partition_key.estimated_bytes())
            .saturating_mul(32)
            .saturating_add(4096)
    }

    fn read_pending_metadata(
        &self,
        root: &MembershipRoot,
    ) -> Result<vector_membership_pages::MembershipSummary> {
        root.check_scope(&self.partition.index.table)?;
        #[cfg(feature = "test-seams")]
        crate::database::resident_history_test_seams::note(|work| work.pending_summary_reads += 1);
        #[cfg(feature = "test-seams")]
        if PENDING_SUMMARY_FAILURE.with(|slot| slot.get())
            == Some(Arc::as_ptr(&self.persistence) as usize)
        {
            return Err(damage("injected pending summary metadata read failure"));
        }
        let summary = vector_membership_pages::read_partition_summary(&root.read, &self.partition)?;
        Ok(summary)
    }

    fn pending_metadata_window(&self) -> Result<OwnedMemoryReservation> {
        OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            self.partition
                .index
                .table
                .len()
                .saturating_add(self.partition.index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes())
                .saturating_add(24)
                .saturating_mul(2)
                .saturating_add(512),
            "vector",
            "pending_metadata",
            HINT,
        )
    }

    fn pending_counts_for(
        summary: &vector_membership_pages::MembershipSummary,
        covered: Option<TxId>,
    ) -> Result<(usize, usize)> {
        // The requested frontier must be represented at this exact root.
        let counts = match covered {
            None => &summary.pending,
            Some(frontier) => std::iter::once(&summary.pending)
                .chain(summary.base_pending.iter())
                .chain(summary.candidate_pending.iter())
                .find(|counts| counts.covered_tx == frontier)
                .ok_or_else(|| damage("pending summary disagrees with serving frontier"))?,
        };
        Ok((
            usize::try_from(counts.pending_inserts)
                .map_err(|_| damage("pending insert count overflow"))?,
            usize::try_from(counts.tombstones)
                .map_err(|_| damage("pending tombstone count overflow"))?,
        ))
    }

    fn read_pending_counts(
        &self,
        covered: Option<TxId>,
        capture: impl FnOnce() -> Result<MembershipRoot>,
    ) -> Result<Option<(usize, usize)>> {
        let _window = self.pending_metadata_window()?;
        let root = capture()?;
        let summary = self.read_pending_metadata(&root)?;
        let covered = if self.captured_catalog_required(&root)? {
            let frontier = self
                .catalog_frontier(&root)?
                .unwrap_or(summary.pending.covered_tx);
            if summary.pending.covered_tx != frontier {
                return Err(damage(
                    "pending summary disagrees with captured catalog frontier",
                ));
            }
            Some(frontier)
        } else {
            covered
        };
        Self::pending_counts_for(&summary, covered).map(Some)
    }

    fn cursor(&self, row: Option<RowId>) -> Result<MembershipCursor> {
        let root = MembershipRoot::capture(&self.persistence)?;
        root.check_scope(&self.partition.index.table)?;
        let prefix = match row {
            Some(row) => RedbPersistence::vector_partition_membership_row_prefix(
                &self.partition.index,
                &self.partition.partition_key,
                row,
            ),
            None => RedbPersistence::vector_partition_prefix(
                &self.partition.index,
                &self.partition.partition_key,
            ),
        };
        let expected = match row {
            Some(_) => None,
            None => Some(read_summary(&root.read, &self.partition)?.entries),
        };
        Ok(MembershipCursor {
            root,
            partition: self.partition.clone(),
            prefix,
            range: None,
            done: false,
            max_bytes: self.window_bytes(),
            seen: 0,
            expected,
        })
    }

    /// Visit this partition's journal changes after `after_lsn`, in LSN
    /// order, at one captured root; `visit` returns false to stop.
    fn journal_suffix(
        &self,
        after_lsn: Lsn,
        visit: &mut dyn FnMut(VectorPartitionJournalChange) -> bool,
    ) -> Result<()> {
        let root = MembershipRoot::capture(&self.persistence)?;
        root.check_scope(&self.partition.index.table)?;
        let table = match root.read.open_table(VECTOR_PARTITION_JOURNAL_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(vector_membership_pages::metadata_error(error)),
        };
        let prefix = RedbPersistence::vector_partition_prefix(
            &self.partition.index,
            &self.partition.partition_key,
        );
        let mut lower = prefix.clone();
        lower.extend_from_slice(&after_lsn.0.saturating_add(1).to_be_bytes());
        let end = writer_source::prefix_end(&prefix);
        let upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        for item in table
            .range::<&[u8]>((Bound::Included(lower.as_slice()), upper))
            .map_err(vector_membership_pages::metadata_error)?
        {
            let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
            let record: VectorPartitionJournalRecord =
                self.persistence.decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                    &self.partition.index,
                    &self.partition.partition_key,
                )?;
            if RedbPersistence::vector_partition_journal_key(&record).as_slice() != key.value()
                || record.lsn <= after_lsn
            {
                return Err(damage("journal key or LSN disagrees with its suffix range"));
            }
            if !visit(record.change) {
                break;
            }
        }
        Ok(())
    }

    /// Walk one row prefix (or the whole partition) under a single admitted
    /// window, stopping at the first record `select` accepts.
    fn find<T>(
        &self,
        row: Option<RowId>,
        before: &mut dyn FnMut() -> bool,
        mut select: impl FnMut(&VectorPartitionMembershipRecord) -> Option<T>,
    ) -> Result<Option<T>> {
        let _window = OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            self.window_bytes(),
            "vector",
            "membership_window",
            HINT,
        )?;
        let mut cursor = self.cursor(row)?;
        while let Some(record) = cursor.next_record(before)? {
            if let Some(value) = select(&record) {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

struct MembershipCursor {
    root: MembershipRoot,
    partition: VectorPartitionRef,
    prefix: Vec<u8>,
    range: Option<redb::Range<'static, &'static [u8], &'static [u8]>>,
    done: bool,
    max_bytes: usize,
    seen: usize,
    expected: Option<usize>,
}

impl MembershipCursor {
    fn next_record(
        &mut self,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        if self.done {
            return Ok(None);
        }
        if !before() {
            return Err(Error::ReadCancelled);
        }
        self.root.check_scope(&self.partition.index.table)?;
        // A whole-partition stream reads each page once; it must not fill
        // the shared read cache. Row probes keep ordinary cached reads.
        let _scan = self.expected.is_some().then(redb::scan_reads);
        if self.range.is_none() {
            let table = match self.root.read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    if self.expected.is_some_and(|expected| expected != 0) {
                        return Err(damage(
                            "membership table is missing beside its maintained summary",
                        ));
                    }
                    self.done = true;
                    return Ok(None);
                }
                Err(error) => return Err(vector_membership_pages::metadata_error(error)),
            };
            let end = writer_source::prefix_end(&self.prefix);
            let upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
            // The range is held for the stream's lifetime: one seek, then
            // each record advances within its leaf.
            self.range = Some(
                table
                    .range::<&[u8]>((Bound::Included(self.prefix.as_slice()), upper))
                    .map_err(vector_membership_pages::metadata_error)?,
            );
        }
        let item = self
            .range
            .as_mut()
            .expect("membership range is open")
            .next()
            .transpose()
            .map_err(vector_membership_pages::metadata_error)?;
        let Some((key, value)) = item else {
            if self.expected.is_some_and(|expected| expected != self.seen) {
                return Err(damage(
                    "membership stream cardinality differs from its maintained summary",
                ));
            }
            self.done = true;
            return Ok(None);
        };
        if key
            .value()
            .len()
            .saturating_add(value.value().len())
            .saturating_mul(3)
            > self.max_bytes
        {
            return Err(damage(
                "membership record exceeds its admitted metadata window",
            ));
        }
        let record = decode_forward(&self.partition, key.value(), value.value())?;
        self.seen += 1;
        Ok(Some(record))
    }
}

impl RawVectorMembershipCursor for MembershipCursor {
    fn next(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.next_record(&mut || request(DormantVectorLoadRequest::Checkpoint))
            .map(|record| record.as_ref().map(visibility_interval))
    }
}

impl RawVectorMembershipSource for DurableMembershipSource {
    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
    ) -> Option<Arc<dyn RawVectorMembershipSource>> {
        Some(Self::new(
            self.persistence.clone(),
            VectorPartitionRef::new(index.clone(), self.partition.partition_key.clone()),
            self.summary,
            self.accountant.clone(),
            self.published_root.clone(),
        ))
    }

    fn published_summary(&self) -> RawVectorMembershipSummary {
        self.summary
    }

    fn current_summary(&self) -> Result<RawVectorMembershipSummary> {
        // Called after a removal the store did not publish itself (cleanup,
        // retention, purge), which commits before the writer republishes its
        // root: read the latest committed summary.
        let read = self.persistence.with_db(|db| {
            db.begin_read()
                .map_err(vector_membership_pages::metadata_error)
        })?;
        read_summary(&read, &self.partition)
    }

    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<usize> {
        let root = MembershipRoot::capture(&self.persistence)?;
        root.check_scope(&self.partition.index.table)?;
        if root.snapshot().is_some_and(|newest| snapshot.0 >= newest.0) {
            // Every membership in this root is committed at or before the
            // statement's snapshot, so the visible ones are exactly the
            // maintained live count.
            return Ok(read_summary(&root.read, &self.partition)?.live_entries);
        }
        drop(root);
        let mut count = 0;
        self.find(None, before, |record| {
            count += usize::from(visible(record, snapshot));
            None::<()>
        })?;
        Ok(count)
    }

    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.find(Some(row), before, |record| {
            visible(record, snapshot).then(|| body_identity(record))
        })
    }

    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<bool> {
        Ok(self.identity_by_row_lsn(row, lsn, before)?.is_some())
    }

    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.find(Some(row), before, |record| {
            (record.vector_lsn == lsn).then(|| body_identity(record))
        })
    }

    fn visit_suffix(
        &self,
        after_lsn: Lsn,
        visit: &mut dyn FnMut(contextdb_vector::store::RawVectorSuffixChange) -> Result<()>,
    ) -> Result<bool> {
        use contextdb_vector::store::RawVectorSuffixChange;
        let mut failed = None;
        self.journal_suffix(after_lsn, &mut |change| {
            let result = match change {
                VectorPartitionJournalChange::Upsert(membership) => {
                    let opened = visit(RawVectorSuffixChange::Opened {
                        identity: RawVectorDirectoryEntry {
                            deleted_tx: None,
                            ..body_identity(&membership)
                        },
                        visible_from: membership.visible_from,
                    });
                    match (opened, membership.deleted_tx) {
                        (Ok(()), Some(deleted_tx)) => visit(RawVectorSuffixChange::Closed {
                            row_id: membership.row_id,
                            created_tx: membership.vector_created_tx,
                            lsn: membership.vector_lsn,
                            deleted_tx,
                        }),
                        (result, _) => result,
                    }
                }
                VectorPartitionJournalChange::Tombstone(tombstone) => {
                    visit(RawVectorSuffixChange::Closed {
                        row_id: tombstone.membership.row_id,
                        created_tx: tombstone.membership.vector_created_tx,
                        lsn: tombstone.membership.vector_lsn,
                        deleted_tx: tombstone.deleted_tx,
                    })
                }
            };
            if let Err(error) = result {
                failed = Some(error);
                return false;
            }
            true
        })?;
        failed.map_or(Ok(true), Err)
    }

    fn has_durable_pending_counts(&self) -> bool {
        true
    }

    fn pending_counts(&self, covered: Option<TxId>) -> Result<Option<(usize, usize)>> {
        self.read_pending_counts(covered, || MembershipRoot::capture(&self.persistence))
    }

    fn inspection_snapshot(
        &self,
        serving_covered: Option<TxId>,
    ) -> Result<Option<RawVectorInspectionSnapshot>> {
        let _window = self.pending_metadata_window()?;
        let root = MembershipRoot::capture(&self.persistence)?;
        let summary = self.read_pending_metadata(&root)?;
        let generations = self.catalog_generations(&root)?;
        let (base, change) = generations.map_or((None, None), |(base, change)| {
            let identity = |generation: VectorPartitionGenerationDescriptor| {
                contextdb_vector::store::VectorGraphGeneration {
                    generation_id: generation.generation_id,
                    covered_tx: generation.covered_tx,
                    covered_lsn: generation.covered_lsn,
                    durable_bytes: generation.durable_bytes,
                    policy_revision: generation.policy_revision,
                    hnsw_m: generation.hnsw_m,
                    hnsw_ef_construction: generation.hnsw_ef_construction,
                }
            };
            (Some(identity(base)), change.map(identity))
        });
        let covered = change
            .or(base)
            .map_or(summary.pending.covered_tx, |generation| {
                generation.covered_tx
            });
        // Serving metadata and counters are committed together. A candidate
        // matching some other frontier cannot authenticate this catalog.
        if summary.pending.covered_tx != covered {
            return Err(damage(
                "pending summary disagrees with captured catalog frontier",
            ));
        }
        let captured_catalog = self.captured_catalog_required(&root)?;
        let frontier = if captured_catalog || generations.is_none() {
            covered
        } else {
            serving_covered.unwrap_or(covered)
        };
        let (pending_inserts, tombstones) = Self::pending_counts_for(&summary, Some(frontier))?;
        Ok(Some(RawVectorInspectionSnapshot {
            captured_catalog,
            summary: summary_counts(&summary)?,
            base,
            change,
            pending_inserts,
            tombstones,
            #[cfg(feature = "test-seams")]
            base_pending: {
                let frontier = base.map_or(TxId(0), |generation| generation.covered_tx);
                [
                    &summary.pending,
                    summary.base_pending.as_ref().unwrap_or(&summary.pending),
                ]
                .into_iter()
                .find(|counts| counts.covered_tx == frontier)
                .map(|counts| (counts.pending_inserts as usize, counts.tombstones as usize))
            },
        }))
    }

    fn visit_identities(
        &self,
        before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
    ) -> Result<()> {
        let mut failed = None;
        self.find(None, before, |record| {
            if let Err(error) = visit(body_identity(record)) {
                failed = Some(error);
                return Some(());
            }
            None
        })?;
        failed.map_or(Ok(()), Err)
    }

    fn visit_row_versions(
        &self,
        row: RowId,
        before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
    ) -> Result<()> {
        let mut failed = None;
        self.find(Some(row), before, |record| {
            if let Err(error) = visit(body_identity(record)) {
                failed = Some(error);
                return Some(());
            }
            None
        })?;
        failed.map_or(Ok(()), Err)
    }

    fn open(
        &self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Box<dyn RawVectorMembershipCursor>> {
        if !request(DormantVectorLoadRequest::Reserve(self.window_bytes())) {
            return Err(Error::ReadCancelled);
        }
        Ok(Box::new(self.cursor(None)?))
    }
}

/// A column's row-to-partition directory, read from the reverse companion
/// at each operation's captured root.
pub(crate) struct DurableReverseMembershipSource {
    persistence: Arc<RedbPersistence>,
    index: VectorIndexRef,
    accountant: Arc<MemoryAccountant>,
}

impl DurableReverseMembershipSource {
    pub(crate) fn new(
        persistence: Arc<RedbPersistence>,
        index: VectorIndexRef,
        accountant: Arc<MemoryAccountant>,
    ) -> Arc<Self> {
        Arc::new(Self {
            persistence,
            index,
            accountant,
        })
    }

    fn cursor(&self, row: RowId) -> Result<ReverseCursor> {
        let memory = OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            (self.index.table.len() + self.index.column.len()).saturating_mul(32) + 4096,
            "vector",
            "membership_reverse_window",
            "Raise MEMORY_LIMIT to read a reverse membership window.",
        )?;
        let root = MembershipRoot::capture(&self.persistence)?;
        root.check_scope(&self.index.table)?;
        Ok(ReverseCursor {
            root,
            index: self.index.clone(),
            row,
            prefix: vector_membership_pages::reverse_row_prefix(&self.index, row),
            before_key: None,
            done: false,
            memory,
        })
    }
}

struct ReverseCursor {
    root: MembershipRoot,
    index: VectorIndexRef,
    row: RowId,
    prefix: Vec<u8>,
    before_key: Option<Vec<u8>>,
    done: bool,
    memory: OwnedMemoryReservation,
}

impl ReverseCursor {
    /// Newest row version first: reverse keys order a row's placements by
    /// its row version, so the current placement is read before history.
    fn next_record(
        &mut self,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionMembershipRecord>> {
        if self.done {
            return Ok(None);
        }
        if !before() {
            return Err(Error::ReadCancelled);
        }
        self.root.check_scope(&self.index.table)?;
        let table = match self.root.read.open_table(vector_membership_pages::REVERSE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                self.done = true;
                return Ok(None);
            }
            Err(error) => return Err(vector_membership_pages::metadata_error(error)),
        };
        let lower = Bound::Included(self.prefix.as_slice());
        let end = writer_source::prefix_end(&self.prefix);
        let upper = self
            .before_key
            .as_deref()
            .or(end.as_deref())
            .map_or(Bound::Unbounded, Bound::Excluded);
        let item = table
            .range::<&[u8]>((lower, upper))
            .map_err(vector_membership_pages::metadata_error)?
            .next_back()
            .transpose()
            .map_err(vector_membership_pages::metadata_error)?;
        let Some((key, value)) = item else {
            self.done = true;
            return Ok(None);
        };
        let needed = (key.value().len() + value.value().len())
            .saturating_mul(3)
            .saturating_add(std::mem::size_of::<VectorPartitionMembershipRecord>());
        if needed > self.memory.bytes() {
            self.memory.try_grow_for(
                needed - self.memory.bytes(),
                "vector",
                "membership_reverse_window",
                "Raise MEMORY_LIMIT to decode the selected reverse membership.",
            )?;
        }
        #[cfg(feature = "test-seams")]
        crate::database::resident_history_test_seams::note(|work| {
            work.membership_keys_examined += 1
        });
        let record: VectorPartitionMembershipRecord = decode_vector_partition_record_payload(
            VectorPartitionRecordKind::Membership,
            value.value(),
        )
        .map_err(|reason| damage(&reason))?;
        if vector_membership_pages::reverse_key(&record).as_slice() != key.value()
            || record.index != self.index
            || record.row_id != self.row
        {
            return Err(damage(
                "reverse membership differs from its authenticated row identity",
            ));
        }
        self.before_key = Some(key.value().to_vec());
        Ok(Some(record))
    }
}

impl RawVectorReverseMembershipCursor for ReverseCursor {
    fn next(&mut self, before: &mut dyn FnMut() -> bool) -> Result<Option<VectorPartitionKey>> {
        self.next_record(before)
            .map(|record| record.map(|record| record.partition_key))
    }
}

impl RawVectorReverseMembershipSource for DurableReverseMembershipSource {
    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
    ) -> Option<Arc<dyn RawVectorReverseMembershipSource>> {
        Some(Self::new(
            self.persistence.clone(),
            index.clone(),
            self.accountant.clone(),
        ))
    }

    fn visible_partition(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionKey>> {
        let mut cursor = self.cursor(row)?;
        while let Some(record) = cursor.next_record(before)? {
            if visible(&record, snapshot) {
                return Ok(Some(record.partition_key));
            }
        }
        Ok(None)
    }

    fn open_row(
        &self,
        row: RowId,
        before: &mut dyn FnMut() -> bool,
    ) -> Result<Box<dyn RawVectorReverseMembershipCursor>> {
        if !before() {
            return Err(Error::ReadCancelled);
        }
        Ok(Box::new(self.cursor(row)?))
    }
}

/// Creates the durable source pair for a partition first written after open.
pub(crate) struct DurablePartitionSourceFactory {
    pub(crate) published_root: PublishedMembershipRoot,
    pub(crate) persistence: Arc<RedbPersistence>,
    pub(crate) accountant: Arc<MemoryAccountant>,
    pub(crate) loader:
        Arc<dyn Fn(&VectorPartitionRef) -> Arc<dyn DormantRawVectorLoader> + Send + Sync>,
}

impl contextdb_vector::store::DurableRawPartitionFactory for DurablePartitionSourceFactory {
    fn create(
        &self,
        partition: &VectorPartitionRef,
    ) -> (
        Arc<dyn RawVectorMembershipSource>,
        Arc<dyn DormantRawVectorLoader>,
    ) {
        (
            DurableMembershipSource::new(
                self.persistence.clone(),
                partition.clone(),
                RawVectorMembershipSummary::default(),
                self.accountant.clone(),
                self.published_root.clone(),
            ),
            (self.loader)(partition),
        )
    }
}

/// Builds a raw-body loader for one partition of a declared layout.
pub(crate) type DurableLoaderBuilder = Arc<
    dyn Fn(
            &VectorPartitionRef,
            &contextdb_vector::VectorIndexLayout,
        ) -> Arc<dyn DormantRawVectorLoader>
        + Send
        + Sync,
>;

/// Creates the durable sources of a vector column declared after open.
pub(crate) struct DurableColumnSourceFactory {
    pub(crate) published_root: PublishedMembershipRoot,
    pub(crate) persistence: Arc<RedbPersistence>,
    pub(crate) accountant: Arc<MemoryAccountant>,
    pub(crate) loader: DurableLoaderBuilder,
}

impl DurableColumnSourceFactory {
    pub(crate) fn column_sources(
        &self,
        index: &VectorIndexRef,
        layout: &contextdb_vector::VectorIndexLayout,
    ) -> (
        Arc<dyn RawVectorReverseMembershipSource>,
        Arc<dyn contextdb_vector::store::DurableRawPartitionFactory>,
    ) {
        let loader = self.loader.clone();
        let layout = layout.clone();
        (
            DurableReverseMembershipSource::new(
                self.persistence.clone(),
                index.clone(),
                self.accountant.clone(),
            ),
            Arc::new(DurablePartitionSourceFactory {
                published_root: self.published_root.clone(),
                persistence: self.persistence.clone(),
                accountant: self.accountant.clone(),
                loader: Arc::new(move |partition: &VectorPartitionRef| loader(partition, &layout)),
            }),
        )
    }
}

impl contextdb_vector::store::DurableRawColumnFactory for DurableColumnSourceFactory {
    fn create(
        &self,
        index: &VectorIndexRef,
        layout: &contextdb_vector::VectorIndexLayout,
    ) -> (
        Arc<dyn RawVectorReverseMembershipSource>,
        Arc<dyn contextdb_vector::store::DurableRawPartitionFactory>,
    ) {
        self.column_sources(index, layout)
    }
}

impl RedbPersistence {
    /// Every partition with a maintained membership summary, with its counts
    /// at the committed root. Reads one summary record per partition.
    pub(crate) fn durable_membership_partitions(
        &self,
    ) -> Result<Vec<(VectorPartitionRef, RawVectorMembershipSummary)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            vector_membership_pages::validate_in_read(&read)?;
            let summaries = match read.open_table(vector_membership_pages::SUMMARY) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut partitions = Vec::new();
            for item in summaries.iter().map_err(Self::storage_error)? {
                let (key, value) = item.map_err(Self::storage_error)?;
                if key.value().is_empty() {
                    continue;
                }
                let summary = vector_membership_pages::decode_summary(key.value(), value.value())?;
                if summary.entries == 0 {
                    continue;
                }
                partitions.push((
                    vector_membership_pages::partition_from_prefix(key.value())?,
                    summary_counts(&summary)?,
                ));
            }
            Ok(partitions)
        })
    }
}
