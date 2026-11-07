//! Short metadata publications keep vectors on the original immutable capture.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::{
    super::*,
    capture::{Capture, Owner},
};
use contextdb_core::ColumnDef;
use contextdb_vector::ivf::storage::{BuildPhase, PageStore, ReadPages, WritePages};
use std::sync::atomic::Ordering;

impl RedbPersistence {
    /// The caller holds the existing publication boundary after complete().
    /// No search or population pass occurs while that boundary is held.
    pub(crate) fn capture_native_ivf_calibration(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        column: &ColumnDef,
    ) -> Result<bool> {
        let route = retained::route_identity(index, tuple);
        if let Some(active) = self.native_ivf_cache.calibration.lock().as_ref() {
            return Ok(active.route == route);
        }
        let epoch = self
            .native_ivf_cache
            .calibration_epoch
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| corrupt())?;
        let _decode = self.writer_decode_scope()?;
        let original = self.require_decode_original()?;
        let root = (self.paged_root_capture()?)()?;
        let capture = Capture::new(root, original, index, tuple, column, epoch)?;
        let mut active = self.native_ivf_cache.calibration.lock();
        if active.is_none() {
            *active = Some(capture);
            Ok(true)
        } else {
            Ok(active.as_ref().is_some_and(|active| active.route == route))
        }
    }
    pub(crate) fn native_ivf_calibration_is_selected(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> bool {
        let route = retained::route_identity(index, tuple);
        self.native_ivf_cache
            .calibration
            .lock()
            .as_ref()
            .is_some_and(|capture| capture.route == route)
    }
    pub(crate) fn native_ivf_calibration_route(&self) -> Option<[u8; 32]> {
        self.native_ivf_cache
            .calibration
            .lock()
            .as_ref()
            .map(|capture| capture.route)
    }
    pub(in crate::persistence::vector_ivf) fn selected_calibration(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Option<Owner> {
        let route = retained::route_identity(index, tuple);
        self.native_ivf_cache
            .calibration
            .lock()
            .as_ref()
            .filter(|capture| capture.route == route && !capture.cancelled.load(Ordering::Acquire))
            .cloned()
    }
    pub(in crate::persistence::vector_ivf) fn write_calibration<R>(
        &self,
        capture: &Capture,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        checkpoint: &dyn Fn() -> Result<()>,
        write_records: impl FnOnce(&mut write::WriteScope<'_, '_>) -> Result<R>,
    ) -> Result<R> {
        super::sample::check(capture, index, checkpoint)?;
        let original = capture.original.preparation_original()?;
        let admission =
            super::super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            // Validate active identity while the caller's commit boundary is held.
            let current = self
                .native_ivf_cache
                .calibration
                .lock()
                .as_ref()
                .is_some_and(|active| {
                    active.epoch == capture.epoch
                        && active.scope == capture.scope
                        && !active.cancelled.load(Ordering::Acquire)
                });
            if !current {
                return Err(contextdb_core::Error::ReadCancelled);
            }
            let root = (self.paged_root_capture()?)()?;
            let (_, snapshot, frontier) = root.authenticate()?;
            self.with_db(|database| {
                let transaction = Self::begin_history_write(database)?;
                let mut session = write::WriteSession::borrow(
                    &transaction,
                    &admission,
                    capture.owner.clone(),
                    snapshot,
                    frontier,
                    frontier,
                )?;
                let result =
                    session.with_scope(index, tuple, checkpoint, &|_| Ok(()), |scope| {
                        if scope.key != capture.scope {
                            return Err(corrupt());
                        }
                        scope.prepare_existing_roots()?;
                        let build = scope.build_state(0)?.ok_or_else(corrupt)?;
                        if build.phase != BuildPhase::Publish || build.manifest != capture.manifest
                        {
                            return Err(Error::ReadCancelled);
                        }
                        if let Some(record) =
                            scope.read_qualification(capture.manifest.generation, 0, 0)?
                        {
                            match record {
                                qualification::Record::Header(header)
                                    if header.epoch == capture.epoch
                                        && header.generation == capture.manifest.generation => {}
                                _ => return Err(Error::ReadCancelled),
                            }
                        }
                        let result = write_records(scope)?;
                        scope.begin()?.commit()?;
                        Ok(result)
                    })?;
                session.bind_final()?;
                drop(session);
                Self::write_writer_recovery_bounds_in_write(&transaction, None)?;
                checkpoint()?;
                transaction.commit().map_err(Self::storage_error)?;
                Ok(result)
            })
        })
    }
    pub(in crate::persistence::vector_ivf) fn read_calibration<R>(
        &self,
        capture: &Capture,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        checkpoint: &dyn Fn() -> Result<()>,
        read_records: impl FnOnce(&read::PageSource<'_>) -> Result<R>,
    ) -> Result<R> {
        super::sample::check(capture, index, checkpoint)?;
        // Only receipt metadata uses this newer authenticated root. Search and
        // query-vector reads always use capture.root.
        let root = (self.paged_root_capture()?)()?;
        let authority = read::NativeAuthority::open_uncached(&root, capture.memory.clone())?;
        let check = || super::sample::check(capture, index, checkpoint);
        let source = read::PageSource::open(&authority, index, tuple, &check, &|_| Ok(()))?
            .ok_or_else(corrupt)?;
        match source.calibration_record(capture.manifest.generation, 0, 0)? {
            Some(qualification::Record::Header(header))
                if header.epoch == capture.epoch
                    && header.generation == capture.manifest.generation =>
            {
                read_records(&source)
            }
            _ => Err(Error::ReadCancelled),
        }
    }
}
