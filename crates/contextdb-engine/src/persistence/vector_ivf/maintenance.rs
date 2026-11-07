//! Finite native construction over authoritative membership, then native pages.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::control::{SourceCursor, SourceEnd};
use super::*;
use contextdb_core::{ColumnDef, Lsn, SnapshotId};
use contextdb_vector::ivf::{
    RowIdentity,
    storage::{BuildPhase, PageStore, ReadPages, WritePages},
};
use redb::ReadableTable;
use std::ops::Bound;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Progress {
    pub source_snapshot: SnapshotId,
    pub in_flight: bool,
    pub published: bool,
    pub ready: bool,
    pub calibration: bool,
}

/// Two exact-capacity range keys and the complete input holder share one
/// original-owner invoice. Reused across every source key in the batch.
struct SourceBounds {
    lower: Vec<u8>,
    upper: Vec<u8>,
    _invoice: memory::Invoice,
}
impl SourceBounds {
    fn reserve(owner: &contextdb_vector::MemoryBudgetOwner, route: &[u8]) -> Result<Self> {
        let capacity = route.len().checked_add(40).ok_or_else(corrupt)?;
        let fixed = std::alloc::Layout::new::<(Self, Input<'_>)>();
        let payload = std::alloc::Layout::array::<u8>(capacity.checked_mul(2).ok_or_else(corrupt)?)
            .map_err(|_| corrupt())?;
        let layout = fixed
            .extend(payload)
            .map_err(|_| corrupt())?
            .0
            .pad_to_align();
        let invoice = memory::Invoice::reserve(owner, layout.size(), "clustered_source_range")?;
        Ok(Self {
            lower: Vec::with_capacity(capacity),
            upper: Vec::with_capacity(capacity),
            _invoice: invoice,
        })
    }
    fn capture_end(
        &mut self,
        write: &redb::WriteTransaction,
        route: &[u8],
    ) -> Result<Option<SourceEnd>> {
        self.upper.clear();
        self.upper.extend_from_slice(route);
        self.upper.extend_from_slice(&[255; 40]);
        let source = write
            .open_table(super::super::VECTOR_PARTITION_MEMBERSHIP_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let mut cursor = source
            .range::<&[u8]>((
                Bound::Included(route),
                Bound::Included(self.upper.as_slice()),
            ))
            .map_err(RedbPersistence::storage_error)?;
        let Some(next) = cursor.next_back() else {
            return Ok(None);
        };
        let (key, _) = next.map_err(RedbPersistence::storage_error)?;
        let suffix = key.value().strip_prefix(route).ok_or_else(corrupt)?;
        if suffix.len() != 40 {
            return Err(corrupt());
        }
        Ok(Some(SourceEnd {
            first: suffix[..32].try_into().map_err(|_| corrupt())?,
            last: suffix[32..].try_into().map_err(|_| corrupt())?,
        }))
    }
    fn set_end(&mut self, route: &[u8], end: Option<SourceEnd>) {
        self.upper.clear();
        self.upper.extend_from_slice(route);
        if let Some(end) = end {
            self.upper.extend_from_slice(&end.first);
            self.upper.extend_from_slice(&end.last);
        }
    }
}

/// One source body and one fixed cursor. The producer consumes at most its
/// declared ingest count; rejected historical keys consume the same allowance.
struct Input<'a> {
    persistence: &'a RedbPersistence,
    write: &'a redb::WriteTransaction,
    original: &'a crate::memory_accounting::OwnedMemoryReservation,
    index: &'a VectorIndexRef,
    tuple: &'a VectorPartitionKey,
    route: &'a [u8],
    snapshot: SnapshotId,
    frontier: Lsn,
    after: Option<SourceCursor>,
    bounds: &'a mut SourceBounds,
    no_source: bool,
    examined: usize,
    limit: usize,
    exhausted: bool,
    error: Option<Error>,
    checkpoint: &'a dyn Fn() -> Result<()>,
}
impl Input<'_> {
    fn take_next(&mut self) -> Result<Option<(RowIdentity, Vec<f32>)>> {
        if self.no_source {
            self.exhausted = true;
            return Ok(None);
        }
        use super::super::vector_membership_pages::{SOURCE, source_key};
        while self.examined < self.limit {
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::work_point(
                crate::vector_ivf_observations::NativeIvfWorkPoint::BuildBatch,
            );
            (self.checkpoint)()?;
            self.bounds.lower.clear();
            self.bounds.lower.extend_from_slice(self.route);
            let lower = &mut self.bounds.lower;
            let bound = if let Some(after) = self.after {
                lower.extend_from_slice(&after.canonical[24..32]);
                lower.extend_from_slice(
                    &(!u64::from_be_bytes(
                        after.membership[..8].try_into().map_err(|_| corrupt())?,
                    ))
                    .to_be_bytes(),
                );
                lower.extend_from_slice(&after.canonical[..8]);
                lower.extend_from_slice(
                    &(!u64::from_be_bytes(
                        after.membership[8..16].try_into().map_err(|_| corrupt())?,
                    ))
                    .to_be_bytes(),
                );
                lower.extend_from_slice(
                    &(!u64::from_be_bytes(
                        after.membership[16..24].try_into().map_err(|_| corrupt())?,
                    ))
                    .to_be_bytes(),
                );
                Bound::Excluded(lower.as_slice())
            } else {
                Bound::Included(lower.as_slice())
            };
            let source = self
                .write
                .open_table(super::super::VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut cursor = source
                .range::<&[u8]>((bound, Bound::Included(self.bounds.upper.as_slice())))
                .map_err(RedbPersistence::storage_error)?;
            let Some(next) = cursor.next() else {
                self.exhausted = true;
                return Ok(None);
            };
            let (key, value) = next.map_err(RedbPersistence::storage_error)?;
            if !key.value().starts_with(self.route) {
                self.exhausted = true;
                return Ok(None);
            }
            if key.value().len() != self.route.len() + 40 {
                return Err(corrupt());
            }
            let record: super::super::VectorPartitionMembershipRecord =
                self.persistence.decode_vector_partition_record(
                    super::super::VectorPartitionRecordKind::Membership,
                    value.value(),
                )?;
            if RedbPersistence::vector_partition_membership_key(&record) != key.value() {
                return Err(corrupt());
            }
            let source_key = source_key(&record);
            let companions = self
                .write
                .open_table(SOURCE)
                .map_err(RedbPersistence::storage_error)?;
            let companion = companions
                .get(source_key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(corrupt)?;
            if companion.value() != value.value() {
                return Err(corrupt());
            }
            let suffix = &source_key[self.route.len()..];
            if suffix.len() != 56 {
                return Err(corrupt());
            }
            if record.index != *self.index || record.partition_key != *self.tuple {
                return Err(corrupt());
            }
            self.examined += 1;
            self.after = Some(SourceCursor {
                canonical: suffix[..32].try_into().map_err(|_| corrupt())?,
                membership: suffix[32..].try_into().map_err(|_| corrupt())?,
            });
            if record.row_lsn > self.frontier
                || record.visible_from.0 > self.snapshot.0
                || record
                    .deleted_tx
                    .is_some_and(|deleted| deleted.0 <= self.snapshot.0)
            {
                continue;
            }
            let body_key = RedbPersistence::vector_identity_key(
                &record.index,
                record.row_id,
                record.vector_created_tx,
                record.vector_lsn,
            );
            let vectors = self
                .write
                .open_table(super::super::VECTORS_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let value = vectors
                .get(body_key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(corrupt)?;
            let frame = super::super::OriginalPersistenceFrame::new(self.original)?;
            let entry = RedbPersistence::decode_vector_entry_original(&frame, value.value())?;
            if entry.index != record.index
                || entry.row_id != record.row_id
                || entry.created_tx != record.vector_created_tx
                || entry.lsn != record.vector_lsn
            {
                return Err(corrupt());
            }
            let _ = self.persistence;
            return Ok(Some((
                RowIdentity {
                    row: entry.row_id.0,
                    commit_lsn: entry.lsn.0,
                },
                entry.vector,
            )));
        }
        Ok(None)
    }
}
impl Iterator for Input<'_> {
    type Item = (RowIdentity, Vec<f32>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() {
            return None;
        }
        match self.take_next() {
            Ok(next) => next,
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }
}

impl RedbPersistence {
    pub(crate) fn advance_native_ivf_partition(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        column: &ColumnDef,
        can_reclaim: bool,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Progress> {
        #[cfg(feature = "test-seams")]
        let test_column = self.native_ivf_test_column(column)?;
        #[cfg(feature = "test-seams")]
        let column = &test_column;
        #[cfg(feature = "test-seams")]
        let disk_owner = self.native_ivf_physical_disk_owner_for_test()?;
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            checkpoint()?;
            let root = (self.paged_root_capture()?)()?;
            let (_, snapshot, frontier) = root.authenticate()?;
            let calibration_pending=qualification::pending(&root,projection.read_budget()?,index,tuple,column,checkpoint)?;
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                let native_owner = projection.vector_budget()?;
                let mut session = write::WriteSession::borrow(
                    &write,
                    &admission,
                    native_owner.clone(),
                    snapshot,
                    frontier,
                    frontier,
                )?;
                #[cfg(feature = "test-seams")]
                { session.disk_owner=Some(disk_owner.clone()); }
                let mut retained_change=None;
                let mut published_generation=None;
                let progress =
                    session.with_scope(index, tuple, checkpoint, &|_| Ok(()), |scope| {
                        let route = Self::vector_partition_prefix(index, tuple);
                        let summary = write::source_summary(&write, &route)?;
                        scope.prepare_existing_roots()?;
                        if let Some(build)=scope.build_state(0)?.filter(|build|build.phase!=BuildPhase::Ready) {scope.prepare_build_roots(build.manifest.generation,build.phase)?;}
                        let restore=scope.build_state(0)?.is_some_and(|build|
                            build.phase==BuildPhase::Ready&&build.serving_at_start.is_some());
                        if restore
                            && let Some(manifest)=contextdb_vector::ivf::retained_codebook_manifest(scope,0)? {
                                let route=retained::route_identity(index,tuple);
                                self.native_ivf_cache.ensure_retained(&native_owner,scope,route,scope.key,manifest)?;
                            }
                        let usage = original.accountant().usage();
                        let source_bytes = match column.column_type {
                            contextdb_core::ColumnType::Vector(d) => {
                                d.checked_mul(8).ok_or_else(corrupt)?
                            }
                            _ => return Err(corrupt()),
                        };
                        let controls = std::mem::size_of::<write::WriteOperation<'_, '_, '_>>();

                        let mut resolved = crate::database::vector_ivf::resolved::resolve_with_native_receipts(
                            index,
                            column,
                            summary.live_entries,
                            usage.limit.map_or(u64::MAX, |n| n as u64),
                            usage
                                .available
                                .unwrap_or(usize::MAX)
                                .saturating_sub(controls)
                                .saturating_sub(source_bytes),
                            training_identity(tuple, summary.source_identity()),
                            scope.native_entry_backing_quote()?,
                            None,
                        )?;
                        let mut state = scope.build_state(0)?;
                        let serving = scope.manifest(0)?;
                        let mut published = false;
                        let mut calibration=false;
                        let mut ready = serving.is_some_and(|m| m.active_lists != 0);
                        let mut family_snapshot = snapshot;
                        let catalog = scope.with_clustered_writer(
                            resolved.policy,
                            resolved.training,
                            |w| w.inspect_catalog(0),
                        )?;
                        let remapping = catalog.is_some_and(|c| c.retained_codebook.is_some());
                        // Declarations and consolidation thresholds belong to this
                        // tuple. A pending replacement waits until all old runs
                        // have completed their bounded remap.
                        let replacement_due = replacement_trigger(
                            column,&scope.record.borrow(),serving,summary.live_entries,
                            catalog.is_some_and(|catalog|catalog.unserviceable.is_some()),
                        ).is_some()||calibration_pending;
                        let new_build = state.is_none()
                            || (state.is_some_and(|b| b.phase == BuildPhase::Ready)
                                && (!ready || replacement_due));
                        let mut source_bounds = if (new_build && !remapping)
                            || state.is_some_and(|b| b.phase == BuildPhase::Ingest)
                        {
                            Some(SourceBounds::reserve(&native_owner, &route)?)
                        } else {
                            None
                        };
                        if new_build&&!remapping {
                            scope.prepare_tail_roots(catalog.is_none_or(|catalog|catalog.unserviceable.is_none()))?;
                            let generation=serving.map_or(Some(1),|manifest|manifest.generation.checked_add(1)).ok_or_else(corrupt)?;
                            scope.prepare_build_roots(generation,BuildPhase::Ingest)?;
                        }
                        if source_bounds.is_some() {
                            let usage = original.accountant().usage();
                            resolved = crate::database::vector_ivf::resolved::resolve_with_native_receipts(
                                index,
                                column,
                                summary.live_entries,
                                usage.limit.map_or(u64::MAX, |n| n as u64),
                                usage
                                    .available
                                    .unwrap_or(usize::MAX)
                                    .saturating_sub(controls)
                                    .saturating_sub(source_bytes),
                                training_identity(tuple, summary.source_identity()),
                                scope.native_entry_backing_quote()?,
                                None,
                            )?;
                        }
                        if new_build && !remapping {
                            let end = source_bounds
                                .as_mut()
                                .ok_or_else(corrupt)?
                                .capture_end(&write, &route)?;
                            {
                                let mut record = scope.record.borrow_mut();
                                record.policy_revision = column.vector_policy_revision;
                                record.build_budget_bytes = resolved.build_budget_bytes;
                                record.source_identity = summary.source_identity();
                                record.source_entries = summary.entries;
                                record.ingested_vectors = 0;
                                record.source_cursor = None;
                                record.source_end = end;
                                // The serving generation and its committed deltas
                                // continue to cover the source during replacement.
                                if !ready {
                                    record.canonical_complete = false;
                                }
                                record.build_changes = Default::default();
                                record.training_evaluations = 0;
                                record.assignment_evaluations = 0;
                            }
                            state = Some(scope.with_clustered_writer(
                                resolved.policy,
                                resolved.training,
                                |w| w.begin_build(0),
                            )?);
                        }
                        if let Some(mut build) = state.filter(|b| b.phase != BuildPhase::Ready) {
                            let family_frontier = {
                                let record = scope.record.borrow();
                                let family = record
                                    .family(build.manifest.generation)
                                    .ok_or_else(corrupt)?;
                                family_snapshot = SnapshotId(family.source_snapshot);
                                Lsn(family.source_frontier)
                            };
                            if build.phase == BuildPhase::Ingest {
                                let end = scope.record.borrow().source_end;
                                let bounds = source_bounds.as_mut().ok_or_else(corrupt)?;
                                bounds.set_end(&route, end);
                                let mut input = Input {
                                    persistence: self,
                                    write: &write,
                                    original: &original,
                                    index,
                                    tuple,
                                    route: &route,
                                    snapshot: family_snapshot,
                                    frontier: family_frontier,
                                    after: scope.record.borrow().source_cursor,
                                    bounds,
                                    no_source: end.is_none(),
                                    examined: 0,
                                    limit: build.training.ingest_batch_keys,
                                    exhausted: false,
                                    error: None,
                                    checkpoint,
                                };
                                // Record each identity only as the real iterator delivers it
                                // to the production ingest operation in this outer write.
                                let ingested = scope.with_clustered_writer(
                                    resolved.policy,
                                    resolved.training,
                                    |w| w.ingest_versioned_batch(&mut build, &mut input),
                                )?;
                                if let Some(error) = input.error {
                                    return Err(error);
                                }
                                {
                                    let mut record = scope.record.borrow_mut();
                                    record.source_cursor = input.after;
                                    record.ingested_vectors = record
                                        .ingested_vectors
                                        .checked_add(ingested as u64)
                                        .ok_or_else(corrupt)?;
                                }
                                // The cursor and payload become durable in the same outer
                                // transaction even when this batch stopped on history keys.
                                scope.begin()?.commit()?;
                                if input.exhausted {
                                    {
                                        let record = scope.record.borrow();
                                        let family = record
                                            .family(build.manifest.generation)
                                            .ok_or_else(corrupt)?;
                                        if record.ingested_vectors != family.basis_live_entries {
                                            return Err(corrupt());
                                        }
                                    }
                                    scope.with_clustered_writer(
                                        resolved.policy,
                                        resolved.training,
                                        |w| w.finish_ingestion(&mut build),
                                    )?;
                                }
                            } else {
                                scope.with_clustered_writer(
                                    resolved.policy,resolved.training,
                                    |w|w.advance_build_preparation(&mut build),
                                )?;
                                if build.phase==BuildPhase::Publish {
                                    let active=self.native_ivf_cache.calibration.lock().as_ref().filter(|active|active.route==retained::route_identity(index,tuple)&&active.manifest==build.manifest&&!active.cancelled.load(std::sync::atomic::Ordering::Acquire)).cloned();
                                    let complete=if let Some(active)=active.as_ref().filter(|active|active.finished.load(std::sync::atomic::Ordering::Acquire)) {
                                        matches!(scope.read_qualification(build.manifest.generation,0,0)?,Some(qualification::Record::Header(header)) if header.complete&&header.epoch==active.epoch&&header.generation==build.manifest.generation)
                                    } else {false};
                                    if build.manifest.active_lists==0||complete {
                                        published=scope.with_clustered_writer(resolved.policy,resolved.training,|w|w.advance_build(&mut build))?;
                                    } else if active.is_some() {calibration=true;}
                                    else {
                                        // An unpublished interrupted receipt is cleared in
                                        // bounded batches before a new root is captured.
                                        let cleared=scope.clear_calibration_records(build.manifest.generation,build.training.phase_batch_entries)?;
                                        if cleared {
                                            calibration=scope.with_clustered_writer(resolved.policy,resolved.training,|w|w.prepare_publication_view(&build))?.is_some();
                                        }
                                    }
                                }
                                if published {
                                    published_generation=Some(build.manifest.generation);
                                    let mut record = scope.record.borrow_mut();
                                    let family = record
                                        .family(build.manifest.generation)
                                        .ok_or_else(corrupt)?;
                                    let mut expected = record.source_identity;
                                    for (value, delta) in
                                        expected.iter_mut().zip(record.build_changes.digest)
                                    {
                                        *value ^= delta;
                                    }
                                    let expected_entries = record
                                        .source_entries
                                        .checked_add(record.build_changes.inserted)
                                        .and_then(|n| n.checked_sub(record.build_changes.removed))
                                        .ok_or_else(corrupt)?;
                                    let expected_live = family
                                        .basis_live_entries
                                        .checked_add(record.build_changes.live_inserted)
                                        .and_then(|n| {
                                            n.checked_sub(record.build_changes.live_removed)
                                        })
                                        .ok_or_else(corrupt)?;
                                    if expected != summary.source_identity()
                                        || expected_entries != summary.entries
                                        || expected_live != summary.live_entries
                                    {
                                        return Err(corrupt());
                                    }
                                    record.current_membership_digest = expected;
                                    record.current_membership_entries = summary.entries;
                                    record.current_membership_live_entries = summary.live_entries;
                                    record.committed_through_lsn = frontier.0;
                                    record.canonical_complete = true;
                                    drop(record);
                                    scope.begin()?.commit()?;
                                    ready = true;
                                }
                            }
                            state = Some(build);
                        } else if ready || remapping {
                            scope.with_clustered_writer(
                                resolved.policy,
                                resolved.training,
                                |w| {
                                    w.advance_coalescing(
                                        0,
                                        column
                                            .vector_engine_policy
                                            .coalesce_pages_per_step
                                            .unwrap_or(
                                                super::super::STORAGE_MAINTENANCE_PAGE_BATCH as u64,
                                            ) as usize,
                                    )?;
                                    w.advance_remap(
                                        0,
                                        column
                                            .vector_engine_policy
                                            .remap_batch_entries
                                            .unwrap_or(resolved.training.phase_batch_entries as u64)
                                            as usize,
                                    )?;
                                    Ok(())
                                },
                            )?;
                        }
                        let remaining = scope
                            .with_clustered_writer(resolved.policy, resolved.training, |w| {
                                w.inspect_catalog(0)
                            })?
                            .is_some_and(|catalog| {
                                catalog.retained_codebook.is_some()
                                    || catalog.coalescing
                                    || catalog.runs_over_cap != 0
                            });
                        let reclaiming = if can_reclaim
                            && ready
                            && !remaining
                            && state.is_none_or(|b| b.phase == BuildPhase::Ready)
                        {
                            scope.reclaim_inactive(resolved.training.phase_batch_entries)?
                        } else {
                            false
                        };
                        let possible=scope.build_state(0)?.is_some_and(|build|
                            build.phase==BuildPhase::Ready&&build.serving_at_start.is_some());
                        let route=retained::route_identity(index,tuple);
                        if possible||self.native_ivf_cache.retained_entry(route,scope.key).is_some() {
                            let desired=contextdb_vector::ivf::retained_codebook_manifest(scope,0)?;
                            retained_change=self.native_ivf_cache.prepare_retained(&native_owner,scope,route,scope.key,desired)?;
                        }
                        Ok(Progress {
                            source_snapshot: family_snapshot,
                            // Retiring the last old run of a generation with no
                            // lists leaves the first real build for the next cycle.
                            in_flight: state.is_some_and(|b| b.phase != BuildPhase::Ready)
                                || remaining
                                || reclaiming
                                || (remapping && !ready),
                            published,
                            ready,
                            calibration,
                        })
                    })?;
                {
                    let route = Self::vector_partition_prefix(index, tuple);
                    let mut floors = write
                        .open_table(super::super::vector_paged::FLOORS)
                        .map_err(Self::storage_error)?;
                    if progress.in_flight {
                        floors
                            .insert(route.as_slice(), progress.source_snapshot.0)
                            .map_err(Self::storage_error)?;
                    } else {
                        floors
                            .remove(route.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
                session.bind_final()?;
                drop(session);
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                checkpoint()?;
                #[cfg(feature = "test-seams")]
                {
                    use crate::vector_ivf_observations::*;
                    Producer::current().boundary(NativeIvfFaultBoundary::BeforeOuterCommit)?;
                }
                write.commit().map_err(Self::storage_error)?;
                if let Some(change)=retained_change {change.commit();}
                if let Some(generation)=published_generation {
                    let route=retained::route_identity(index,tuple);
                    self.native_ivf_cache.cancel_calibration_where(|active|active.route==route&&active.manifest.generation==generation);
                }
                #[cfg(feature = "test-seams")]
                {
                    use crate::vector_ivf_observations::*;
                    Producer::current().emit(NativeIvfSealEvent::OuterCommitted {
                        actual_commit_lsn: frontier,
                    });
                }
                Ok(progress)
            })
        })
    }
}

/// Constructor witnesses consume the same authoritative iterator and kernel
/// construction as maintenance, inside the existing abort-only write factory.
#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(crate) fn prepare_native_ivf_constructor_for_test<R>(
        &self,
        scope: &mut write::WriteScope<'_, '_>,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        column: &ColumnDef,
        complete: bool,
        run: impl FnOnce(
            &mut write::WriteScope<'_, '_>,
            contextdb_vector::ivf::Policy,
            contextdb_vector::ivf::TrainingPolicy,
            contextdb_vector::ivf::storage::BuildState,
            contextdb_vector::MemoryBudgetOwner,
        ) -> Result<R>,
    ) -> Result<R> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let (write, snapshot, owner) = scope.constructor_context_for_test();
        let route = Self::vector_partition_prefix(index, tuple);
        let summary = write::source_summary(write, &route)?;
        let mut bounds = SourceBounds::reserve(&owner, &route)?;
        let end = bounds.capture_end(write, &route)?;
        bounds.set_end(&route, end);
        scope.prepare_existing_roots()?;
        scope.prepare_tail_roots(true)?;
        let generation = scope
            .manifest(0)?
            .map_or(Some(1), |manifest| manifest.generation.checked_add(1))
            .ok_or_else(corrupt)?;
        scope.prepare_build_roots(generation, BuildPhase::Ingest)?;
        scope.prepare_build_roots(generation, BuildPhase::Seed)?;
        let usage = original.accountant().usage();
        let source_bytes = match column.column_type {
            contextdb_core::ColumnType::Vector(d) => d.checked_mul(8).ok_or_else(corrupt)?,
            _ => return Err(corrupt()),
        };
        let available = usage
            .available
            .unwrap_or(usize::MAX)
            .saturating_sub(std::mem::size_of::<write::WriteOperation<'_, '_, '_>>())
            .saturating_sub(source_bytes);
        // The witness stages its entire finite build inside one outer abort.
        let resolved = crate::database::vector_ivf::resolved::resolve_with_native_receipts(
            index,
            column,
            summary.live_entries,
            usage.limit.map_or(u64::MAX, |n| n as u64),
            available,
            training_identity(tuple, summary.source_identity()),
            scope.native_entry_backing_quote()?,
            Some(usize::try_from(summary.live_entries).map_err(|_| corrupt())?),
        )?;
        let build = scope.with_clustered_writer(resolved.policy, resolved.training, |writer| {
            let mut build = writer.begin_build(0)?;
            let mut input = Input {
                persistence: self,
                write,
                original: &original,
                index,
                tuple,
                route: &route,
                snapshot,
                frontier: Lsn(build.source_lsn),
                after: None,
                bounds: &mut bounds,
                no_source: end.is_none(),
                examined: 0,
                limit: build.training.ingest_batch_keys,
                exhausted: false,
                error: None,
                checkpoint: &|| Ok(()),
            };
            loop {
                input.examined = 0;
                writer.ingest_versioned_batch(&mut build, &mut input)?;
                if let Some(error) = input.error.take() {
                    return Err(error);
                }
                if input.exhausted {
                    break;
                }
            }
            writer.finish_ingestion(&mut build)?;
            if complete {
                // The counts-free constructor reads real seeded codebook
                // pages. Its allocation witness needs no training, assignment
                // or publication of an otherwise unrelated complete build.
                while build.phase == BuildPhase::Seed {
                    writer.advance_build(&mut build)?;
                }
            }
            Ok(build)
        })?;
        drop(bounds);
        run(scope, resolved.policy, resolved.training, build, owner)
    }
}

/// The same declared-policy decision drives maintenance and inspection.
pub(super) fn replacement_trigger(
    column: &contextdb_core::ColumnDef,
    record: &control::ScopeRecord,
    serving: Option<contextdb_vector::ivf::storage::Manifest>,
    live: u64,
    damaged: bool,
) -> Option<&'static str> {
    let manifest = serving?;
    if record.policy_revision != column.vector_policy_revision {
        return Some("policy_revision");
    }
    if damaged {
        return Some("requalification");
    }
    if column.consolidation_disabled {
        return None;
    }
    let family = record.family(manifest.generation)?;
    let basis = family.basis_live_entries.max(1);
    let changed = manifest.slots.saturating_sub(family.basis_live_entries);
    let tombstones = manifest.slots.saturating_sub(live);
    if u128::from(changed) * 100
        >= u128::from(basis) * u128::from(column.effective_consolidation_change_percent())
    {
        return Some("change_percent");
    }
    if u128::from(tombstones) * 100
        >= u128::from(basis) * u128::from(column.effective_consolidation_tombstone_percent())
    {
        return Some("tombstone_percent");
    }
    None
}
