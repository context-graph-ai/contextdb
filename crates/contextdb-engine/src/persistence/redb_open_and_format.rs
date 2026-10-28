use super::*;

impl RedbPersistence {
    /// Actual collection backing capacity retained by staged paged publications.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn paged_publication_owners_for_test(&self) -> usize {
        let publications = self.paged_publications.lock();
        let pages_capacity = |pages: &PreencodedVectorPartitionGraphPages| {
            pages.root_key.capacity()
                + pages.manifest_bytes.capacity()
                + pages.graph_bytes.capacity()
        };
        publications.capacity()
            * (std::mem::size_of::<(Vec<u8>, vector_paged::PagedPublication)>() + 1)
            + publications
                .iter()
                .map(|(key, publication)| {
                    let prepared = &publication.prepared;
                    key.capacity()
                        + prepared.base_key.capacity()
                        + prepared.base_bytes.capacity()
                        + pages_capacity(&prepared.base_pages)
                        + prepared
                            .change
                            .as_ref()
                            .map_or(0, |(key, bytes)| key.capacity() + bytes.capacity())
                        + prepared.change_pages.as_ref().map_or(0, pages_capacity)
                        + prepared.catalog_key.capacity()
                        + prepared.catalog_bytes.capacity()
                        + prepared.catalog_route.index.table.capacity()
                        + prepared.catalog_route.index.column.capacity()
                        // This accessor counts component Vec and TEXT capacities
                        // plus the inline key, already included in the map slot.
                        + (prepared.catalog_route.partition_key.estimated_bytes()
                            - std::mem::size_of::<VectorPartitionKey>())
                        + prepared.attributable_stale_catalog_keys.capacity()
                            * std::mem::size_of::<Vec<u8>>()
                        + prepared
                            .attributable_stale_catalog_keys
                            .iter()
                            .map(Vec::capacity)
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    /// Publish a completed checkpoint as a fresh physical database
    /// incarnation. The checkpoint bytes are a snapshot of the source, but
    /// its adjacent companion and authoritative-erasure fence belong only to
    /// this destination inode.
    pub(crate) fn publish_export_artifact(temp_path: &Path, dest: &Path) -> std::io::Result<()> {
        let companion = CompanionGuard::acquire(dest, CompanionAdmission::NewStore)
            .map_err(std::io::Error::other)?;
        if let Err(error) = std::fs::hard_link(temp_path, dest) {
            companion.discard_created(dest);
            return Err(error);
        }
        if let Err(error) = companion.publish_writer_run(dest, true, None) {
            // The just-created destination is still covered by this
            // companion's exclusive claim, so an unsuccessful publication
            // leaves neither half of an artifact behind.
            let _ = std::fs::remove_file(dest);
            companion.discard_created(dest);
            return Err(std::io::Error::other(error));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn lock_companion_slot(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, Option<CompanionGuard>>> {
        self.lock_file.write().map_err(|_| {
            crate::funded_error::corruption_text(
                self.require_decode_original()
                    .and_then(|original| original.read_budget()),
                self.path.display(),
                "in-process companion ownership state was poisoned".into(),
            )
        })
    }

    /// Publish this writer's REAL serving state beside the store, so a later
    /// reader is told why inspection is unavailable instead of meeting this
    /// writer's lock on the committed file and reporting whatever the storage
    /// layer said.
    /// End this writer's claim window now that its inspection channel is up
    /// and can answer for itself. See [`CompanionGuard::close_claim_window`].
    pub(crate) fn close_claim_window(&self) {
        let Ok(guard) = self.lock_companion_slot() else {
            return;
        };
        if let Some(companion) = guard.as_ref() {
            companion.close_claim_window();
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_owner_read_status(&self, status: OwnerReadStatus) -> Result<()> {
        let guard = self.lock_companion_slot()?;
        let Some(companion) = guard.as_ref() else {
            return Ok(());
        };
        let path = self.path.clone();
        companion.record_owner_read_status(&path, status)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn lock_database(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, Option<redb::Database>>> {
        self.db.write().map_err(|_| {
            Error::Other(
                (format!(
                    "in-process Redb handle state was poisoned for {}",
                    self.path.display()
                ))
                .into(),
            )
        })
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn with_owner_cursor_handle_locked_for_test<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> T {
        let _handle = self
            .db
            .write()
            .expect("hold persistence handle for admission proof");
        operation()
    }

    #[cfg(all(test, unix, feature = "test-seams"))]
    #[doc(hidden)]
    pub(crate) fn with_owner_cursor_registry_locked_for_test<T>(
        &self,
        operation: impl FnOnce() -> T,
    ) -> T {
        let mut registry = ReaderSnapshotRegistryLock::create(
            self.runtime_dir.as_deref().expect("task-scoped registry"),
            self.physical_identity().unwrap(),
        )
        .unwrap();
        registry.lock_exclusive().unwrap();
        operation()
    }

    #[cfg(all(test, unix, feature = "test-seams"))]
    #[doc(hidden)]
    pub(crate) fn owner_cursor_registry_paths_for_test(&self) -> Vec<PathBuf> {
        reader_publication_directories(self.runtime_dir.as_deref())
            .iter()
            .map(|runtime| {
                reader_snapshot_registry_lock_path(
                    runtime.path(),
                    self.physical_identity().unwrap(),
                )
            })
            .collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn owner_cursor_pin(&self) -> Result<OwnerCursorStoragePin> {
        self.owner_cursor_pin_for_admission()
            .map_err(OwnerCursorPinAdmissionError::into_error)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn owner_cursor_pin_for_admission(
        &self,
    ) -> std::result::Result<OwnerCursorStoragePin, OwnerCursorPinAdmissionError> {
        self.owner_cursor_pin_from_read(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn owner_cursor_pin_for_root(
        &self,
        root: &WriterSnapshot,
    ) -> std::result::Result<OwnerCursorStoragePin, OwnerCursorPinAdmissionError> {
        self.owner_cursor_pin_from_read(Some(root.owner_read()?))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn owner_cursor_pin_from_read(
        &self,
        captured: Option<Arc<redb::ReadTransaction>>,
    ) -> std::result::Result<OwnerCursorStoragePin, OwnerCursorPinAdmissionError> {
        let _coordination = OwnerCursorCoordinationScope::enter();
        let coordination_refusal = |reason| Error::ReaderCoordinationUnavailable {
            operation: "owner cursor pin".to_owned(),
            path: self.path.display().to_string(),
            reason,
        };
        // Cursor capture runs under the engine commit mutex. Close and recycle
        // can hold these guards while refreshing the provider, so admission
        // must release the commit boundary before retrying an occupied guard.
        let companion_guard = self.lock_file.try_read().map_err(|error| {
            let refusal =
                coordination_refusal(format!("companion ownership state is unavailable: {error}"));
            match error {
                std::sync::TryLockError::WouldBlock => OwnerCursorPinAdmissionError::Busy(refusal),
                std::sync::TryLockError::Poisoned(_) => {
                    OwnerCursorPinAdmissionError::Refused(refusal)
                }
            }
        })?;
        if companion_guard.is_none() {
            return Err(
                coordination_refusal("database companion ownership is closed".to_owned()).into(),
            );
        }
        let database_guard = self.db.try_read().map_err(|error| {
            let refusal =
                coordination_refusal(format!("Redb handle state is unavailable: {error}"));
            match error {
                std::sync::TryLockError::WouldBlock => OwnerCursorPinAdmissionError::Busy(refusal),
                std::sync::TryLockError::Poisoned(_) => {
                    OwnerCursorPinAdmissionError::Refused(refusal)
                }
            }
        })?;
        let db = database_guard
            .as_ref()
            .ok_or_else(|| coordination_refusal("database persistence is closed".to_owned()))?;
        let read = match captured {
            Some(read) => read,
            None => Arc::new(db.begin_read().map_err(Self::storage_error)?),
        };
        let companion = inspect_companion_record(&self.path).map_err(|error| {
            Error::ReaderCoordinationUnavailable {
                operation: "owner cursor pin".to_owned(),
                path: self.path.display().to_string(),
                reason: format!(
                    "cannot inspect published owner cursor companion identity: {error}"
                ),
            }
        })?;
        let identity = companion.fields.database_identity;
        let writer_run = companion.fields.writer_run_number;
        let owner = self
            .writer_file
            .try_read()
            .map_err(|error| {
                let refusal =
                    coordination_refusal(format!("writer file state is unavailable: {error}"));
                match error {
                    std::sync::TryLockError::WouldBlock => {
                        OwnerCursorPinAdmissionError::Busy(refusal)
                    }
                    std::sync::TryLockError::Poisoned(_) => {
                        OwnerCursorPinAdmissionError::Refused(refusal)
                    }
                }
            })?
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::ReaderCoordinationUnavailable {
                operation: "owner cursor pin".to_owned(),
                path: self.path.display().to_string(),
                reason: "database physical ownership is closed".to_owned(),
            })?;
        #[cfg(unix)]
        let physical_identity = owner.identity();
        #[cfg(not(unix))]
        let physical_identity = (0, 0);
        owner
            .admit_owner_cursor_identity(identity)
            .map_err(|error| Error::ReaderCoordinationUnavailable {
                operation: "owner cursor pin".to_owned(),
                path: self.path.display().to_string(),
                reason: format!("owner cursor identity cannot be admitted: {error}"),
            })?;
        let registration = ReadSnapshotRegistrationGuard::from_owner(
            &self.path,
            identity,
            writer_run,
            physical_identity,
            read.storage_snapshot_identity(),
            self.runtime_dir.as_deref(),
        )
        .map_err(|error| {
            let path = match &error {
                LoadReadImageError::IoAtPath { path, .. }
                | LoadReadImageError::RegistryContention { path, .. } => path,
                _ => &self.path,
            };
            let refusal = Error::ReaderCoordinationUnavailable {
                operation: "owner cursor pin".to_owned(),
                path: path.display().to_string(),
                reason: format!("reader snapshot registration failed: {error}"),
            };
            if matches!(error, LoadReadImageError::RegistryContention { .. }) {
                OwnerCursorPinAdmissionError::Busy(refusal)
            } else {
                OwnerCursorPinAdmissionError::Refused(refusal)
            }
        })?;
        // Admit the published root while its local transaction still protects
        // every page. The remote reader replaces this process registration
        // before it accepts its first page.
        #[cfg(all(test, target_os = "linux"))]
        owner_cursor_admission_tests::before_provider_admission_for_test();
        drop(
            db.begin_external_read_nonblocking(read.storage_snapshot_identity())
                .map_err(|error| {
                    let cause = error.to_string();
                    let busy = redb::Database::is_external_snapshot_refresh_busy(&error);
                    let coordination_failed = busy
                        || matches!(
                            &error,
                            redb::TransactionError::Storage(redb::StorageError::LockPoisoned(_))
                        );
                    let refusal = match Self::storage_error(error) {
                        Error::ReaderCoordinationUnavailable { path, reason, .. } => {
                            Error::ReaderCoordinationUnavailable {
                                operation: "owner cursor pin".to_owned(),
                                path,
                                reason: format!(
                                    "external snapshot admission failed: {cause}; {reason}"
                                ),
                            }
                        }
                        _ if coordination_failed => coordination_refusal(format!(
                            "external snapshot admission failed: {cause}"
                        )),
                        other => other,
                    };
                    if busy {
                        OwnerCursorPinAdmissionError::Busy(refusal)
                    } else {
                        OwnerCursorPinAdmissionError::Refused(refusal)
                    }
                })?,
        );
        Ok(OwnerCursorStoragePin {
            read,
            _registration: registration,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn physical_identity(&self) -> Result<(u64, u64)> {
        let owner = self.writer_file.read().map_err(Self::storage_error)?;
        #[cfg(unix)]
        {
            owner.as_ref().map(|owner| owner.identity()).ok_or_else(|| {
                Error::Other(("database physical ownership is closed".to_owned()).into())
            })
        }
        #[cfg(not(unix))]
        {
            let _ = owner;
            Ok((0, 0))
        }
    }

    /// Cap each growth of the writer's file at `ratio` of its size (never
    /// below twice the allocation that needs it). The database derives the
    /// ratio from the declared `STORAGE_COMPACTION STOP_RATIO`: the slack the
    /// store accepts after compaction is the most one growth may add, so a
    /// compacted file does not double back on its next write. A declared
    /// `DISK_LIMIT` still refuses growth past it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn set_growth_step_ratio(&self, ratio: f64) -> Result<()> {
        // The fork keeps the declared ratio exactly and floors the byte step,
        // so the cap never exceeds the declaration at any store size.
        let database = self.db.read().map_err(Self::storage_error)?;
        if let Some(db) = database.as_ref() {
            db.set_growth_step_ratio(ratio);
        }
        Ok(())
    }

    /// Production-dead: the growth step the writer's handle applies for a
    /// request of `required` bytes at `usable` bytes.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn growth_step_for_test(&self, usable: u64, required: u64) -> Result<Option<u64>> {
        let database = self.db.read().map_err(Self::storage_error)?;
        Ok(database
            .as_ref()
            .and_then(|db| db.growth_step_for_test(usable, required)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn set_disk_limit(&self, limit: Option<u64>) -> Result<()> {
        let owner = self.writer_file.read().map_err(Self::storage_error)?;
        owner
            .as_ref()
            .ok_or_else(|| {
                Error::Other(("database physical ownership is closed".to_owned()).into())
            })?
            .set_disk_limit(limit)
            .map_err(Self::storage_error)
    }

    /// Called only after the Database operation barrier has frozen public
    /// writes and every background writer has joined. The companion hold
    /// remains exclusive through the drain and final resource release.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn make_writer_quiescent(&self) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if self.handoff_prepare_io_fault.swap(false, Ordering::SeqCst) {
            return Err(Self::storage_error(std::io::Error::other(
                "injected handoff preparation I/O failure",
            )));
        }
        {
            let mut database = self.lock_database()?;
            // A failed handle recycle has already closed its Redb handle.
            // That clean close is the handoff preparation; the outer writer
            // descriptor and companion hold remain owned until close releases
            // them below.
            if let Some(database) = database.as_mut() {
                database
                    .prepare_read_only_handoff()
                    .map_err(Self::storage_error)?;
            }
        }
        let owner = self.writer_file.read().map_err(Self::storage_error)?;
        owner
            .as_ref()
            .ok_or_else(|| {
                Error::Other(("database physical ownership is closed".to_owned()).into())
            })?
            .make_quiescent()
            .map_err(Self::storage_error)
    }

    /// The quiescent owner's close, run only after both a successful
    /// quiescence and a successful drain of accepted reads, while this owner
    /// still holds its data file (shared since quiescence) and its companion
    /// (which excludes another writer). For every runtime root that already
    /// exists -- none is created here -- it takes that root's registry lock by
    /// try and removes the registry when no registration of any process
    /// remains. A contended root is left exactly as it is.
    pub(crate) fn remove_idle_reader_registries(&self) {
        #[cfg(unix)]
        {
            let Ok(owner) = self.writer_file.read() else {
                return;
            };
            let Some(owner) = owner.as_ref() else {
                return;
            };
            let identity = owner.identity();
            for runtime in existing_reader_registry_roots(self.runtime_dir.as_deref()) {
                // By try only: a writer never waits on a runtime root's
                // serializer, and a join or leave in progress keeps its root.
                let Ok(serializer) = hold_runtime_root(runtime.root(), true) else {
                    continue;
                };
                let Ok(mut registry_lock) =
                    ReaderSnapshotRegistryLock::open(runtime.path(), identity)
                else {
                    continue;
                };
                if registry_lock.try_lock_exclusive().is_err() {
                    continue;
                }
                let supplied = self
                    .runtime_dir
                    .as_deref()
                    .is_some_and(|path| path == runtime.path());
                let created_root = (!supplied && runtime.root() != runtime.path())
                    .then(|| runtime.path().to_path_buf());
                remove_registry_if_last(
                    &reader_snapshot_directory(runtime.path(), identity),
                    &registry_lock.path,
                    true,
                    None,
                );
                let _ = registry_lock.unlock();
                drop(serializer);
                if let Some(root) = created_root.as_deref()
                    && let Ok(_exclusive) = serialize_runtime_root(runtime.root(), true)
                {
                    let _ = std::fs::remove_dir(root);
                }
            }
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn fail_next_handoff_for_test(&self) {
        self.handoff_prepare_io_fault.store(true, Ordering::SeqCst);
    }

    /// One bounded maintenance reclaim step over this owner's storage: at most
    /// `RECLAIM_EXTENTS_PER_STEP` extents and `RECLAIM_ENTRIES_PER_STEP` entries freed before both the
    /// reader floor and the cycle's boundary, in one ordinary durable commit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reclaim_deferred_step(
        &self,
        boundary: Option<redb::ReclaimBoundary>,
    ) -> Result<redb::ReclaimStep> {
        let companion = self.lock_file.read().map_err(Self::storage_error)?;
        if companion.is_none() {
            return Err(Error::Other(
                ("database persistence is closed".to_string()).into(),
            ));
        }
        let database = self.db.read().map_err(Self::storage_error)?;
        match database.as_ref() {
            Some(database) => database
                .reclaim_deferred_step(boundary, RECLAIM_EXTENTS_PER_STEP, RECLAIM_ENTRIES_PER_STEP)
                .map_err(Self::storage_error),
            None => Ok(redb::ReclaimStep::Drained),
        }
    }

    /// Whether a foreground writer is waiting for the write slot.
    pub(crate) fn has_waiting_writer(&self) -> bool {
        self.db
            .read()
            .ok()
            .and_then(|database| {
                database
                    .as_ref()
                    .map(redb::Database::has_waiting_write_transactions)
            })
            .unwrap_or(false)
    }

    /// Drain eligible deferred frees while this owner can still commit. A
    /// retained read or savepoint leaves protected entries for a later owner.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn drain_deferred_frees_before_handoff(&self) -> Result<()> {
        if let Some(error) = self.saved_export_abandonment_error.lock().as_ref() {
            return Err(Error::Other(
                (format!("saved generation abandonment failed: {error}")).into(),
            ));
        }
        let companion = self.lock_file.read().map_err(Self::storage_error)?;
        if companion.is_none() {
            return Err(Error::Other(
                ("database persistence is closed".to_string()).into(),
            ));
        }
        let database = self.db.read().map_err(Self::storage_error)?;
        // A failed recycle cleanly dropped the old backend before its reopen
        // failed. It still owns the companion and physical descriptor, but has
        // no backend through which cleanup could commit. Closing that owner
        // releases its resources; a later owner can drain any durable backlog.
        // An actual cleanup failure on a live backend must still be reported.
        if let Some(database) = database.as_ref() {
            database
                .drain_deferred_frees()
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn fail_next_close_drain_for_test(&self) -> Result<()> {
        self.with_db(|db| {
            db.fail_next_deferred_free_commit_for_test();
            Ok(())
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn projected_disk_growth_bytes_for_test(&self) -> Result<u64> {
        let owner = self.writer_file.read().map_err(Self::storage_error)?;
        Ok(owner
            .as_ref()
            .ok_or_else(|| {
                Error::Other(("database physical ownership is closed".to_owned()).into())
            })?
            .projected_growth_bytes())
    }

    /// What to tell a caller whose claim on a companion-less path could not be
    /// created at all.
    ///
    /// The claim is what would have let this open judge the file, so without
    /// it the remaining option is read-only classification: an unsupported
    /// layout is refused with the store creation remedy, a root that cannot
    /// be decoded is named with its next step, and a file that
    /// really is a current store leaves the claim's own refusal standing,
    /// because then the directory is the only thing wrong.
    pub(super) fn refusal_without_a_claim(
        path: &Path,
        claim_error: Error,
        absent: AbsentStoreAnswer,
    ) -> Error {
        // Strictly read-only. Nothing is claimed on this road, and redb's
        // crash repair REWRITES the store it repairs: running it here would
        // rewrite a store this process was just refused the right to hold, and
        // two openers meeting the same crash-dirty root would rewrite it at
        // the same time. So a root that cannot be read without repairing it is
        // simply not judged from here, and the claim's own refusal stands --
        // which is the answer that road gave before the claim moved earlier,
        // with the file left byte-for-byte as it was found.
        match Self::classify_format(path, false, absent, REDB_CACHE_BYTES, None) {
            Ok(Some(true)) => Error::LegacyVectorStoreDetected {
                found_format_marker: "unsupported or missing durable layout".to_owned(),
                expected_release: CURRENT_FORMAT_VERSION.to_owned(),
            },
            Ok(Some(false)) | Ok(None) => claim_error,
            Err(classification) => classification,
        }
    }

    /// Create a store whose very first artifact writes - the format marker and
    /// the initial content identity - are already admitted through the owning
    /// database's accountant. Attaching afterwards leaves that prefix, which is
    /// a whole empty proof, maintained outside `MEMORY_LIMIT`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn create_with_accountant(
        path: &Path,
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        cache_bytes: usize,
    ) -> Result<Self> {
        Self::create_with_accountant_in_runtime_dir(path, accountant, cache_bytes, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn create_with_accountant_in_runtime_dir(
        path: &Path,
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_writer_open();
        let lock_file = CompanionGuard::acquire(path, CompanionAdmission::NewStore)?;
        Self::create_with_lock(path, lock_file, accountant, cache_bytes, runtime_dir)
    }

    /// Recreate a store while holding its existing coordination lock. The lock
    /// file is never unlinked: its OS lock is the ownership boundary that
    /// prevents another process from opening the path between removal and the
    /// fresh format marker being written.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn recreate(path: &Path) -> Result<Self> {
        let writer = match crate::read_session::DatabaseOpenOptions::default().memory_limit {
            Some(bytes) => crate::memory_accounting::MemoryAccountant::writer_budget(bytes)?,
            None => crate::memory_accounting::MemoryAccountant::writer_no_limit()?,
        };
        Self::recreate_with_owner(path, writer.accountant())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn recreate_with_owner(
        path: &Path,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        let _operation =
            crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone())?;
        let companion_preexists = companion_exists(path)?;
        let (lock_file, old_db) = if companion_preexists {
            let lock_file = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
            let old_db = Self::open_outgoing_store(path)?;
            (lock_file, old_db)
        } else {
            // An existing companion-less root is the one lock-order
            // exception: prove Redb exclusivity first so a reader refusal
            // cannot leave a newly-created `.lock` behind.
            let old_db = Self::open_outgoing_store(path)?;
            let lock_file = match CompanionGuard::acquire(path, CompanionAdmission::CreateIfMissing)
            {
                Ok(lock_file) => lock_file,
                Err(error) => {
                    drop(old_db);
                    return Err(error);
                }
            };
            (lock_file, old_db)
        };
        if old_db.1.is_none() {
            return Self::create_with_lock(
                path,
                lock_file,
                Some(accountant),
                REDB_CACHE_BYTES,
                None,
            );
        }
        lock_file.publish_writer_run(path, false, None)?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::GuardAcquired,
        );

        let temp_path = replacement_temp_path(path);
        let temp_file = create_replacement_placeholder(&temp_path)?;
        let replacement_owner = match LockedWriterFile::lock(temp_file, None) {
            Ok(owner) => owner,
            Err(error) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(Self::storage_open_error(&temp_path, error));
            }
        };
        let created = Self::open_hook_suppressed(|| {
            redb_builder().create_with_native_backend_and_external_snapshots(
                replacement_owner.view(),
                replacement_owner.external_snapshot_provider(),
            )
        });
        let replacement_db = match created {
            Ok(Ok(database)) => database,
            Ok(Err(error)) => {
                drop(replacement_owner);
                let _ = std::fs::remove_file(&temp_path);
                return Err(Self::storage_open_error(&temp_path, error));
            }
            Err(_) => {
                drop(replacement_owner);
                let _ = std::fs::remove_file(&temp_path);
                return Err(crate::funded_error::corruption_text(
                    writer_open_diagnostic_budget(),
                    temp_path.display(),
                    "replacement Redb creation panicked".into(),
                ));
            }
        };
        if let Err(error) = Self::write_format_marker(
            &replacement_db,
            &mut content_identity::ProofAdmission::new(Some(accountant.clone()))
                .with_scratch_owner(Some(Arc::downgrade(&replacement_owner))),
        ) {
            drop(replacement_db);
            drop(replacement_owner);
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        if let Err(error) = lock_file.prepare_replacement_database(path, &temp_path) {
            drop(replacement_db);
            drop(replacement_owner);
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::BeforeAtomicReplacement,
        );
        if let Err(error) = std::fs::rename(&temp_path, path) {
            drop(replacement_db);
            drop(replacement_owner);
            let _ = std::fs::remove_file(&temp_path);
            return Err(Self::storage_error(error));
        }
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::AfterAtomicReplacement,
        );
        sync_database_parent(path)?;
        lock_file.publish_replacement_database(path)?;
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::ReplacementPublishedBeforeGuardRelease,
        );
        // Both exclusive Redb handles and the original companion guard have
        // survived every replacement and publication boundary. Only now may
        // the unlinked old-store inode be released.
        drop(old_db);
        Ok({
            let mut storage = std::mem::MaybeUninit::<Self>::uninit();
            let mut construction = PersistenceConstruction {
                source: storage.as_mut_ptr(),
                initialized: [false; 19],
            };
            // SAFETY: each field is written once and marked only after its
            // initializer succeeds. The stack guard owns exactly marked fields.
            unsafe {
                {
                    std::ptr::addr_of_mut!((*construction.source).path).write(path.to_path_buf());
                    construction.initialized[0] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).runtime_dir).write(None);
                    construction.initialized[1] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).lock_file)
                        .write(Arc::new(std::sync::RwLock::new(Some(lock_file))));
                    construction.initialized[4] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).db)
                        .write(Arc::new(std::sync::RwLock::new(Some(replacement_db))));
                    construction.initialized[2] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).writer_file)
                        .write(std::sync::RwLock::new(Some(replacement_owner)));
                    construction.initialized[3] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).storage_compaction)
                        .write(Mutex::new(Default::default()));
                    construction.initialized[5] = true;
                }

                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).handoff_prepare_io_fault)
                        .write(AtomicBool::new(false));
                    construction.initialized[6] = true;
                }

                {
                    let proof_accountant = std::sync::OnceLock::new();
                    let _ = proof_accountant.set(ProofAccountantSlot::attached(accountant.clone()));
                    std::ptr::addr_of_mut!((*construction.source).proof_accountant)
                        .write(proof_accountant);
                    construction.initialized[15] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).writer_snapshots)
                        .write(Arc::new(parking_lot::Mutex::new(Vec::new())));
                    construction.initialized[7] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).writer_schema)
                        .write(parking_lot::Mutex::new(None));
                    construction.initialized[8] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).cache_charge_bytes)
                        .write(std::sync::OnceLock::new());
                    std::ptr::addr_of_mut!((*construction.source).cache_installation)
                        .write(CacheInstallation::default());
                    construction.initialized[9] = true;
                    std::ptr::addr_of_mut!((*construction.source).cache_stripe_residue)
                        .write(std::sync::OnceLock::new());
                    construction.initialized[18] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).used_legacy_table_meta_layout)
                        .write(AtomicBool::new(false));
                    construction.initialized[10] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).paged_publications)
                        .write(Default::default());
                    construction.initialized[11] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).paged_root_capture)
                        .write(std::sync::OnceLock::new());
                    construction.initialized[12] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).scoped_vector_root_capture)
                        .write(std::sync::OnceLock::new());
                    construction.initialized[13] = true;
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_repair_audit)
                        .write(parking_lot::Mutex::new(None));
                    construction.initialized[14] = true;
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).saved_export_recovery_cutoff)
                        .write(std::sync::OnceLock::new());
                    construction.initialized[16] = true;
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).saved_export_abandonment_error)
                        .write(Arc::new(parking_lot::Mutex::new(None)));
                    construction.initialized[17] = true;
                }
            }
            construction.initialized.fill(false);
            unsafe { storage.assume_init() }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn create_with_lock(
        path: &Path,
        lock_file: CompanionGuard,
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Self> {
        // The caller has already established companion-first ownership for a
        // genuinely new database. Reserve the store itself with create-new
        // and no-follow semantics, then hand that exact descriptor to Redb;
        // a path race can therefore never redirect creation through a symlink.
        let store_file = match create_replacement_placeholder(path) {
            Ok(file) => file,
            Err(error) => {
                drop(lock_file);
                return Err(error);
            }
        };
        let owner = LockedWriterFile::lock(store_file, runtime_dir)
            .map_err(|error| Self::storage_open_error(path, error))?;
        let created = Self::open_hook_suppressed(|| {
            redb_builder_with_cache(cache_bytes).create_with_native_backend_and_external_snapshots(
                owner.view(),
                owner.external_snapshot_provider(),
            )
        });
        let create_result = match created {
            Ok(inner) => inner.map_err(|err| Self::storage_open_error(path, err)),
            Err(_) => Err(crate::funded_error::corruption_display(
                writer_open_diagnostic_budget(),
                path.display(),
                format_args!(
                    "metadata/format read panicked; store may be truncated or corrupt — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            )),
        };
        match create_result {
            Ok(db) => match Self::write_format_marker(
                &db,
                &mut content_identity::ProofAdmission::new(accountant.clone())
                    .with_scratch_owner(Some(Arc::downgrade(&owner))),
            )
            .and_then(|()| lock_file.publish_writer_run(path, true, owner.boot_binding()))
            {
                Ok(()) => {
                    let proof_accountant = std::sync::OnceLock::new();

                    if let Some(accountant) = accountant {
                        let _ = proof_accountant.set(ProofAccountantSlot::attached(accountant));
                    }
                    Ok({
                        let mut storage = std::mem::MaybeUninit::<Self>::uninit();
                        let mut construction = PersistenceConstruction {
                            source: storage.as_mut_ptr(),
                            initialized: [false; 19],
                        };
                        // SAFETY: each field is written once and marked only after its
                        // initializer succeeds. The stack guard owns exactly marked fields.
                        unsafe {
                            {
                                std::ptr::addr_of_mut!((*construction.source).path)
                                    .write(path.to_path_buf());
                                construction.initialized[0] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).runtime_dir)
                                    .write(runtime_dir.map(Path::to_path_buf));
                                construction.initialized[1] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).lock_file)
                                    .write(Arc::new(std::sync::RwLock::new(Some(lock_file))));
                                construction.initialized[4] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).db)
                                    .write(Arc::new(std::sync::RwLock::new(Some(db))));
                                construction.initialized[2] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).writer_file)
                                    .write(std::sync::RwLock::new(Some(owner)));
                                construction.initialized[3] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).storage_compaction)
                                    .write(Mutex::new(Default::default()));
                                construction.initialized[5] = true;
                            }

                            #[cfg(any(test, feature = "test-seams"))]
                            {
                                std::ptr::addr_of_mut!(
                                    (*construction.source).handoff_prepare_io_fault
                                )
                                .write(AtomicBool::new(false));
                                construction.initialized[6] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).proof_accountant)
                                    .write(proof_accountant);
                                construction.initialized[15] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).writer_snapshots)
                                    .write(Arc::new(parking_lot::Mutex::new(Vec::new())));
                                construction.initialized[7] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).writer_schema)
                                    .write(parking_lot::Mutex::new(None));
                                construction.initialized[8] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).cache_charge_bytes)
                                    .write(std::sync::OnceLock::new());
                                std::ptr::addr_of_mut!((*construction.source).cache_installation)
                                    .write(CacheInstallation::default());
                                construction.initialized[9] = true;
                                std::ptr::addr_of_mut!((*construction.source).cache_stripe_residue)
                                    .write(std::sync::OnceLock::new());
                                construction.initialized[18] = true;
                            }

                            {
                                std::ptr::addr_of_mut!(
                                    (*construction.source).used_legacy_table_meta_layout
                                )
                                .write(AtomicBool::new(false));
                                construction.initialized[10] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).paged_publications)
                                    .write(Default::default());
                                construction.initialized[11] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).paged_root_capture)
                                    .write(std::sync::OnceLock::new());
                                construction.initialized[12] = true;
                            }

                            {
                                std::ptr::addr_of_mut!(
                                    (*construction.source).scoped_vector_root_capture
                                )
                                .write(std::sync::OnceLock::new());
                                construction.initialized[13] = true;
                            }

                            {
                                std::ptr::addr_of_mut!((*construction.source).vector_repair_audit)
                                    .write(parking_lot::Mutex::new(None));
                                construction.initialized[14] = true;
                            }
                            {
                                std::ptr::addr_of_mut!(
                                    (*construction.source).saved_export_recovery_cutoff
                                )
                                .write(std::sync::OnceLock::new());
                                construction.initialized[16] = true;
                            }
                            {
                                std::ptr::addr_of_mut!(
                                    (*construction.source).saved_export_abandonment_error
                                )
                                .write(Arc::new(parking_lot::Mutex::new(None)));
                                construction.initialized[17] = true;
                            }
                        }
                        construction.initialized.fill(false);
                        unsafe { storage.assume_init() }
                    })
                }
                Err(err) => {
                    drop(db);
                    drop(owner);
                    drop(lock_file);
                    Err(err)
                }
            },
            Err(err) => {
                drop(owner);
                drop(lock_file);
                Err(err)
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(crate) fn read_cache_observation(&self) -> Result<Option<redb::ReadCacheObservation>> {
        let database = self.db.read().map_err(Self::storage_error)?;
        // Retain actual memory before releasing the handle lock. Statements
        // read this lock too, so no guard crosses statement execution.
        Ok(database
            .as_ref()
            .map(redb::Database::read_cache_observation))
    }

    /// Settle the native capacity after the effective persisted limit is known,
    /// before writable startup publishes any source or reader root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn settle_writer_cache_budget(&self, bytes: usize) -> Result<usize> {
        if self.cache_installation.owner.lock().is_none() {
            // Standalone persistence has no fixed original cache funder. Keep
            // it honestly unqualified, without a transient counterfeit receipt.
            return self.settle_writer_cache_budget_prepared(bytes);
        }
        let accountant = self.require_decode_accountant()?;
        cache_reserve::with_preparation(&accountant, true, || {
            self.settle_writer_cache_budget_prepared(bytes)
        })
    }

    /// Initial settlement holds preparation's actual new-index/window invoice
    /// until the fixed reserve has taken custody. It never refunds in the gap
    /// between returning the new size and installing its genuine payer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn settle_and_retain_writer_cache_budget(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
        capacity: usize,
    ) -> Result<()> {
        cache_reserve::with_preparation(accountant, true, || {
            struct StartupCleanup<'a> {
                persistence: &'a RedbPersistence,
                armed: bool,
            }
            impl Drop for StartupCleanup<'_> {
                fn drop(&mut self) {
                    if self.armed {
                        self.persistence.close();
                    }
                }
            }
            // A failed admission or unwind must physically free the prepared
            // cache before the preparation scope can refund its original grant.
            let mut cleanup = StartupCleanup {
                persistence: self,
                armed: true,
            };
            let reserve = self.settle_writer_cache_budget_prepared(capacity)?;
            self.retain_writer_cache_charge(
                accountant,
                reserve,
                accountant.usage().limit.is_none(),
            )?;
            cleanup.armed = false;
            Ok(())
        })
    }

    /// Cache retirement performs no I/O or allocation and needs no new payer.
    /// Closing the store already frees this same backing under exclusive access.
    pub(crate) fn retire_read_cache_request(&self, request: u64) {
        let database = self.db.read().unwrap_or_else(|error| error.into_inner());
        if let Some(db) = database.as_ref() {
            db.retire_read_cache_request(request);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn settle_writer_cache_budget_prepared(&self, bytes: usize) -> Result<usize> {
        let reserve = self.with_db(|db| {
            db.settle_startup_cache_budget(bytes)
                .map_err(Self::storage_error)
        })?;
        self.cache_installation.record(bytes, reserve);
        Ok(reserve)
    }

    /// Record what the owning database charged for this store's page cache.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn cache_index_bytes_for_test(&self) -> Result<usize> {
        self.with_db(|db| Ok(db.cache_index_bytes()))
    }

    /// Install one original invoice and share its actual occupancy controls with
    /// every page and later handle generation. Metadata below is never a refund.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retain_writer_cache_charge(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
        bytes: usize,
        process_residue: bool,
    ) -> Result<()> {
        let reserve = cache_reserve::original(accountant, bytes, process_residue)?;
        let owner = self.with_db(|db| Ok(db.retain_cache_reserve(reserve)))?;
        let mut installed = self.cache_installation.owner.lock();
        assert!(
            installed.is_none(),
            "writer cache funding is installed once"
        );
        *installed = Some(owner);
        let _ = self
            .cache_charge_bytes
            .set(if process_residue { 0 } else { bytes });
        let _ = self
            .cache_stripe_residue
            .set(if process_residue { bytes } else { 0 });
        Ok(())
    }

    pub(crate) fn cache_stripe_residue_bytes(&self) -> usize {
        self.cache_stripe_residue.get().copied().unwrap_or(0)
    }

    /// The capacity a handle recycle reinstalls: exactly the capacity the
    /// open settled, whoever funds it. The settled reserve includes the
    /// cache index on top of that capacity, so it is never a capacity itself.
    pub(crate) fn recycle_cache_capacity(&self) -> usize {
        self.cache_installation
            .capacity()
            .unwrap_or(REDB_CACHE_BYTES)
    }

    /// The capacity the native cache was last settled to, if any.
    pub(crate) fn installed_cache_capacity(&self) -> Option<usize> {
        self.cache_installation.capacity()
    }

    /// What this store's page cache is funded with: the writer's charge plus
    /// the process residue held for the rest. `None` when no owner charged it.
    pub(crate) fn cache_funded_bytes(&self) -> Option<usize> {
        self.cache_charge_bytes
            .get()
            .map(|writer| writer + self.cache_stripe_residue_bytes())
    }

    /// `(capacity, reserve)` of the native cache as last installed.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from maintenance_and_limits.rs"
        )
    )]
    pub(crate) fn cache_installation_for_test(&self) -> (usize, usize) {
        (
            self.cache_installation.capacity().unwrap_or(0),
            self.cache_installation.reserve(),
        )
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn reader_registry_charge_for_test(&self, accountant: usize) -> usize {
        self.db
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map_or(0, |db| db.reader_registry_charge_for_test(accountant))
    }

    /// Writer cache charge metadata; physical owners alone release its invoice.
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from independent_cache_reserve_lifetime_tests.rs"
        )
    )]
    pub(crate) fn cache_charge(&self) -> usize {
        self.cache_charge_bytes.get().copied().unwrap_or(0)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from relational_posting_prune.rs"
        )
    )]
    pub(crate) fn open_with_cache(path: &Path, cache_bytes: usize) -> Result<Self> {
        Self::open_with_cache_in_runtime_dir(path, cache_bytes, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_with_cache_in_runtime_dir(
        path: &Path,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Self> {
        // This constructor declares the same default as the raw open doors;
        // an enclosing operation keeps its existing payer.
        let _decode_scope = match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.enter(),
            Err(_) => crate::read_image_memory::ImageMemory::enter_writer_operation(
                crate::memory_accounting::AccountantHandle::standalone(
                    crate::read_session::DatabaseOpenOptions::default().memory_limit,
                    true,
                )?,
            )?,
        };
        Self::open_inner(path, AbsentStoreAnswer::Taxonomy, cache_bytes, runtime_dir)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from persistence.rs"
        )
    )]
    pub(crate) fn open_existing_only_with_cache(path: &Path, cache_bytes: usize) -> Result<Self> {
        Self::open_existing_only_with_cache_in_runtime_dir(path, cache_bytes, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_existing_only_with_cache_in_runtime_dir(
        path: &Path,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Self> {
        // This constructor declares the same default as the raw open doors;
        // an enclosing operation keeps its existing payer.
        let _decode_scope = match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.enter(),
            Err(_) => crate::read_image_memory::ImageMemory::enter_writer_operation(
                crate::memory_accounting::AccountantHandle::standalone(
                    crate::read_session::DatabaseOpenOptions::default().memory_limit,
                    true,
                )?,
            )?,
        };
        Self::open_inner(path, AbsentStoreAnswer::Typed, cache_bytes, runtime_dir)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_inner(
        path: &Path,
        absent: AbsentStoreAnswer,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_writer_open();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_writer_open_attempt();
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_writer_open_attempt_for_test();
        let companion_preexists = companion_exists(path)?;
        let (lock_file, db) = if companion_preexists {
            let lock_file = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
            match Self::open_db_checked(path, absent, cache_bytes, runtime_dir) {
                Ok(db) => (lock_file, db),
                Err(error) => {
                    drop(lock_file);
                    return Err(error);
                }
            }
        } else {
            // This road is where ownership and publication are furthest apart.
            // The writer proves the root readable and current through an
            // exclusive writable open, and that open IS the moment it owns the
            // store -- everything a caller could dial comes long afterwards.
            // So the claim is taken FIRST and spans the whole stretch, carrying
            // NO record: the companion codec is untouched and nothing is said
            // about serving. A caller arriving mid-open is told the store is
            // owned and not answering yet, never that nobody owns it.
            //
            // Refusing an unsupported or corrupt root must still leave the directory
            // exactly as it was found, and that promise is kept the other way
            // round now: every refusal below takes the companion away again.
            let lock_file = match CompanionGuard::acquire(path, CompanionAdmission::CreateIfMissing)
            {
                Ok(lock_file) => lock_file,
                Err(error) => {
                    // Nothing was claimed. A directory that will not take a
                    // new file is the ordinary cause, and it says nothing
                    // about the FILE the caller pointed at -- which is the
                    // thing they are owed an answer about, with its recovery
                    // instruction, rather than a complaint about companion
                    // creation. Classifying read-only opens nothing writable
                    // and creates nothing, and with no claim in existence an
                    // owner-only caller is still told, truly, that nobody owns
                    // this path. A companion that DOES exist means the claim
                    // was refused by a live holder, and that answer stands.
                    if companion_exists(path).unwrap_or(true) {
                        return Err(error);
                    }
                    return Err(Self::refusal_without_a_claim(path, error, absent));
                }
            };
            // Own the physical store first -- the one wait any writer makes
            // for readers already inside -- then classify through the real
            // read-only decoder on that owned descriptor before any writable
            // open, so Redb's housekeeping never alters a file that is about
            // to be refused, and the check itself never shares the data lock
            // with anyone. Every refusal gives the store back before the
            // claim.
            let owner = match Self::open_writer_file_in_runtime_dir(path, absent, runtime_dir) {
                Ok(owner) => owner,
                Err(error) => {
                    lock_file.discard_created(path);
                    return Err(error);
                }
            };
            match Self::classify_owned(path, &owner, &lock_file, absent, cache_bytes, runtime_dir) {
                Ok(false) => {}
                Ok(true) => {
                    drop(owner);
                    lock_file.discard_created(path);
                    return Err(Error::LegacyVectorStoreDetected {
                        found_format_marker: "unsupported or missing durable layout".to_owned(),
                        expected_release: CURRENT_FORMAT_VERSION.to_owned(),
                    });
                }
                Err(error) => {
                    drop(owner);
                    lock_file.discard_created(path);
                    return Err(error);
                }
            }
            let database = match Self::open_writer_view_checked_in_runtime_dir(
                path,
                absent,
                &owner,
                cache_bytes,
                runtime_dir,
            ) {
                Ok(database) => database,
                Err(error) => {
                    drop(owner);
                    lock_file.discard_created(path);
                    return Err(error);
                }
            };
            let db = OpenedWriterDatabase { database, owner };
            if let Err(error) = Self::validate_format_marker(&db.database, path) {
                drop(db);
                lock_file.discard_created(path);
                return Err(error);
            }
            (lock_file, db)
        };
        // The store is open and any repair is done: only now may this run
        // say which boot and which file it holds.
        let writer_boot = db.owner.boot_binding();
        let publish = if companion_preexists {
            Self::validate_format_marker(&db.database, path)
                .and_then(|()| lock_file.publish_writer_run(path, false, writer_boot))
        } else {
            lock_file.publish_writer_run(path, false, writer_boot)
        };
        match publish {
            Ok(()) => {
                let persistence = {
                    let mut storage = std::mem::MaybeUninit::<Self>::uninit();
                    let mut construction = PersistenceConstruction {
                        source: storage.as_mut_ptr(),
                        initialized: [false; 19],
                    };
                    // SAFETY: each field is written once and marked only after its
                    // initializer succeeds. The stack guard owns exactly marked fields.
                    unsafe {
                        {
                            std::ptr::addr_of_mut!((*construction.source).path)
                                .write(path.to_path_buf());
                            construction.initialized[0] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).runtime_dir)
                                .write(runtime_dir.map(Path::to_path_buf));
                            construction.initialized[1] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).lock_file)
                                .write(Arc::new(std::sync::RwLock::new(Some(lock_file))));
                            construction.initialized[4] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).db)
                                .write(Arc::new(std::sync::RwLock::new(Some(db.database))));
                            construction.initialized[2] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).writer_file)
                                .write(std::sync::RwLock::new(Some(db.owner)));
                            construction.initialized[3] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).storage_compaction)
                                .write(Mutex::new(Default::default()));
                            construction.initialized[5] = true;
                        }

                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::addr_of_mut!((*construction.source).handoff_prepare_io_fault)
                                .write(AtomicBool::new(false));
                            construction.initialized[6] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).proof_accountant)
                                .write(std::sync::OnceLock::new());
                            construction.initialized[15] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).writer_snapshots)
                                .write(Arc::new(parking_lot::Mutex::new(Vec::new())));
                            construction.initialized[7] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).writer_schema)
                                .write(parking_lot::Mutex::new(None));
                            construction.initialized[8] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).cache_charge_bytes)
                                .write(std::sync::OnceLock::new());
                            std::ptr::addr_of_mut!((*construction.source).cache_installation)
                                .write(CacheInstallation::default());
                            construction.initialized[9] = true;
                            std::ptr::addr_of_mut!((*construction.source).cache_stripe_residue)
                                .write(std::sync::OnceLock::new());
                            construction.initialized[18] = true;
                        }

                        {
                            std::ptr::addr_of_mut!(
                                (*construction.source).used_legacy_table_meta_layout
                            )
                            .write(AtomicBool::new(false));
                            construction.initialized[10] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).paged_publications)
                                .write(Default::default());
                            construction.initialized[11] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).paged_root_capture)
                                .write(std::sync::OnceLock::new());
                            construction.initialized[12] = true;
                        }

                        {
                            std::ptr::addr_of_mut!(
                                (*construction.source).scoped_vector_root_capture
                            )
                            .write(std::sync::OnceLock::new());
                            construction.initialized[13] = true;
                        }

                        {
                            std::ptr::addr_of_mut!((*construction.source).vector_repair_audit)
                                .write(parking_lot::Mutex::new(None));
                            construction.initialized[14] = true;
                        }
                        {
                            std::ptr::addr_of_mut!(
                                (*construction.source).saved_export_recovery_cutoff
                            )
                            .write(std::sync::OnceLock::new());
                            construction.initialized[16] = true;
                        }
                        {
                            std::ptr::addr_of_mut!(
                                (*construction.source).saved_export_abandonment_error
                            )
                            .write(Arc::new(parking_lot::Mutex::new(None)));
                            construction.initialized[17] = true;
                        }
                    }
                    construction.initialized.fill(false);
                    unsafe { storage.assume_init() }
                };
                persistence.recover_authoritative_purge_publication()?;
                persistence.truncate_uncommitted_commit_index_tail()?;
                Ok(persistence)
            }
            Err(err) => {
                drop(db);
                if companion_preexists {
                    drop(lock_file);
                } else {
                    // The companion on that road exists only because this open
                    // created it, so an open that ends here leaves nothing
                    // beside the store either.
                    lock_file.discard_created(path);
                }
                Err(err)
            }
        }
    }

    pub fn close(&self) {
        let schema = self.writer_schema.lock().take();
        drop(schema);
        let mut db = self
            .db
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(db.take());
        // Physical aliases retain the counters and the genuine parent after
        // close. Drop only persistence's preparation owner here.
        let cache_owner = self.cache_installation.owner.lock().take();
        drop(cache_owner);
        let owner = self
            .writer_file
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(owner);
        // Release this guard before taking the companion: replacement publication
        // uses companion-before-database ordering. Concurrent close cannot reach
        // companion release until the cache flush and physical release finish.
        drop(db);
        let lock_file = self
            .lock_file
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(lock_file);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether any table's meta on this root decoded only via the fallback
    /// layout (see [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// Set the first time [`Self::load_all_table_meta`] hits that fallback;
    /// never cleared. `Database::open` consults this to refuse a
    /// incompatible schema layout instead of silently loading it.
    pub fn used_legacy_table_meta_layout(&self) -> bool {
        self.used_legacy_table_meta_layout.load(Ordering::Relaxed)
    }

    /// Bind the companion to the generation that is actually present at
    /// `path` before redb's crash repair rewrites it. A crashed replacement
    /// leaves a recorded intent naming its target by the target's exact
    /// bytes; that record is what decides whether the surviving store is the
    /// prior generation or the replacement, and therefore which database
    /// identity the reopened store carries. Repairing first would change
    /// those bytes and leave the record matching neither generation, so the
    /// store would come back carrying the other generation's identity.
    ///
    /// A root with no companion, or a companion with nothing recorded, is
    /// left byte-for-byte untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn settle_pending_replacement_before_repair(path: &Path) -> Result<()> {
        if !companion_exists(path)? {
            return Ok(());
        }
        let companion = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
        companion.settle_pending_replacement(path)
    }

    /// Whether the store at `path` has an incompatible format: either its top-level
    /// format-version marker does not match [`CURRENT_FORMAT_VERSION`], or
    /// the marker matches but the underlying `TableMeta`/`ColumnDef` schema
    /// layout still only decodes via [`Self::decode_table_meta_versioned`]'s
    /// fallback.
    ///
    /// Decided through a `redb::ReadOnlyDatabase` handle SPECIFICALLY so
    /// this check itself never mutates the file: a plain read-write
    /// `redb::Database::open` performs a housekeeping write on every open —
    /// independent of any application-level transaction — which would
    /// mutate a healthy store during diagnosis if this detection used one.
    ///
    /// The one exception is a store redb itself refuses to open read-only
    /// because its last writer died mid-run (`RepairAborted`): that root is
    /// unreadable until redb's own crash repair runs, which needs a
    /// read-write handle, so it is re-opened read-write and classified. That
    /// is the crash-recovery path; a cleanly shut store is never touched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn is_legacy_format_store(path: &Path) -> Result<bool> {
        let writer = match crate::read_session::DatabaseOpenOptions::default().memory_limit {
            Some(bytes) => crate::memory_accounting::MemoryAccountant::writer_budget(bytes)?,
            None => crate::memory_accounting::MemoryAccountant::writer_no_limit()?,
        };
        let _operation =
            crate::read_image_memory::ImageMemory::enter_writer_operation(writer.accountant())?;
        Self::legacy_format_verdict(path, AbsentStoreAnswer::Taxonomy, REDB_CACHE_BYTES, None)
    }

    /// The same verdict, told how to answer when the root it went to classify
    /// turns out not to be there at all.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn legacy_format_verdict(
        path: &Path,
        absent: AbsentStoreAnswer,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<bool> {
        // The ordinary caller of this holds the store, so repairing a
        // crash-dirty root is its business to do.
        Ok(Self::classify_format(path, true, absent, cache_bytes, runtime_dir)?.unwrap_or(false))
    }

    /// The same classification, told whether it may let redb repair a
    /// crash-dirty root in order to read it.
    ///
    /// `Some` is a verdict; `None` means the root cannot be judged without a
    /// repair the caller is not allowed to run. Only the claim-less road
    /// forbids it, and it forbids it for a reason: redb's crash repair
    /// REWRITES the store, and doing that with no claim held would let two
    /// openers meeting the same crash-dirty root rewrite it at the same time,
    /// and would rewrite a store this process was refused the right to hold.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_format(
        path: &Path,
        may_repair: bool,
        absent: AbsentStoreAnswer,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<Option<bool>> {
        // Suppressed + `catch_unwind`-guarded exactly like `open_db_checked`'s
        // read-write open: a truncated/corrupt file can trip redb's internal
        // page_manager assertion (a panic, not a clean `Err`) on the
        // READ-ONLY path too, and this call must never let that raw panic
        // (or its backtrace) escape to the caller.
        let opened = Self::open_hook_suppressed(|| {
            let capture = Self::capture_for_classification(path, cache_bytes, runtime_dir)?;
            Ok(Self::classify_capture(&capture))
        });
        match opened {
            Ok(Ok(verdict)) => verdict.map(Some),
            // A store whose last writer died mid-run carries no persisted
            // allocator state (or a header still marked recovery-required),
            // and redb refuses to repair either through a read-only handle:
            // `DatabaseError::RepairAborted`. Classifying exactly that root
            // is the FIRST thing crash recovery has to do -- a store left
            // dirty after a crashed writer is unreadable until redb's own
            // crash repair runs, which needs a read-write handle. So this
            // ONE refusal, and only it, re-opens read-write. A cleanly shut
            // store never reaches this arm, so the untouched-detection
            // contract documented above still holds for every healthy root.
            Ok(Err(ClassificationOpenError::Refused(redb::DatabaseError::RepairAborted)))
                if !may_repair =>
            {
                Ok(None)
            }
            Ok(Err(ClassificationOpenError::Refused(redb::DatabaseError::RepairAborted))) => {
                // redb's crash repair rewrites the store it repairs, and a
                // recorded replacement intent names its generation by that
                // generation's exact bytes. So the companion is settled
                // against the store as the crash left it -- before the
                // repair below changes those bytes -- and the reopened root
                // therefore keeps the database identity bound to the
                // generation the fingerprint selected, instead of the other
                // generation's.
                Self::settle_pending_replacement_before_repair(path)?;
                let repaired = Self::open_db_checked(path, absent, cache_bytes, runtime_dir)?;
                let read_txn = repaired
                    .database
                    .begin_read()
                    .map_err(Self::storage_error)?;
                Self::classify_committed(&read_txn).map(Some)
            }
            Ok(Err(ClassificationOpenError::Refused(err))) => Err(
                Self::refused_open_error_in_runtime_dir(path, err, absent, runtime_dir),
            ),
            Ok(Err(ClassificationOpenError::Coordination(error))) => Err(error),
            Err(_) => Err(Self::classification_panicked(path)),
        }
    }

    /// Classify a store this caller already owns: it holds the companion claim
    /// and the physical store through `owner`, so the classification takes no
    /// lock of its own, registers nothing, and nobody else can write while it
    /// reads. A crash-dirty root is settled against its pending replacement
    /// through the caller's own claim, then repaired through the same owned
    /// descriptor.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_owned(
        path: &Path,
        owner: &Arc<LockedWriterFile>,
        claim: &CompanionGuard,
        absent: AbsentStoreAnswer,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<bool> {
        let opened = Self::open_hook_suppressed(|| {
            redb_builder_with_cache(cache_bytes).open_read_only_with_backend(owner.view())
        });
        match opened {
            Ok(Ok(database)) => {
                let read_txn = database.begin_read().map_err(Self::storage_error)?;
                Self::classify_committed(&read_txn)
            }
            Ok(Err(redb::DatabaseError::RepairAborted)) => {
                // See `classify_format`: settle before the repair rewrites
                // the bytes a pending replacement is recorded against.
                claim.settle_pending_replacement(path)?;
                let repaired = Self::open_writer_view_checked_in_runtime_dir(
                    path,
                    absent,
                    owner,
                    cache_bytes,
                    runtime_dir,
                )?;
                let read_txn = repaired.begin_read().map_err(Self::storage_error)?;
                Self::classify_committed(&read_txn)
            }
            Ok(Err(err)) => Err(Self::refused_open_error_in_runtime_dir(
                path,
                err,
                absent,
                runtime_dir,
            )),
            Err(_) => Err(Self::classification_panicked(path)),
        }
    }

    pub(super) fn classification_panicked(path: &Path) -> Error {
        crate::funded_error::corruption_display(
            writer_open_diagnostic_budget(),
            path.display(),
            format_args!(
                "metadata/format read panicked; store may be truncated or corrupt — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        )
    }

    /// The verdict, read at one committed root: `true` when the durable
    /// layout is unsupported or missing.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_committed(read_txn: &redb::ReadTransaction) -> Result<bool> {
        let format_table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(true),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let Some(marker_bytes) = format_table
            .get(FORMAT_VERSION_KEY)
            .map_err(Self::storage_error)?
        else {
            return Ok(true);
        };
        let marker: String = Self::decode(marker_bytes.value())?;
        if marker != CURRENT_FORMAT_VERSION {
            return Ok(true);
        }

        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(err) => return Err(Self::storage_error(err)),
        };
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::pause_classification_scan();
        for entry in meta_table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if key.value().strip_prefix("table:").is_some() {
                let (_, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                if via_legacy {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_capture(capture: &ClassificationCapture) -> Result<bool> {
        #[cfg(unix)]
        let read_txn = capture.database.begin_external_read(capture.snapshot);
        #[cfg(not(unix))]
        let read_txn = capture.database.begin_read();
        Self::classify_committed(&read_txn.map_err(Self::storage_error)?)
    }

    /// Read a store this caller holds no claim on, the way a direct reader
    /// does: through the admission gate, with only a fixed handful of page
    /// reads under the data lock, and at a root its registration protects
    /// once that lock is given back. Whatever the classification then reads
    /// -- however many tables the store has -- happens after a waiting writer
    /// has been let through.
    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn capture_for_classification(
        path: &Path,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> std::result::Result<ClassificationCapture, ClassificationOpenError> {
        // The registration opens its own descriptor and reports only kinds of
        // failure; this open reports the file's own answer exactly as a
        // writable open would, before anything is registered.
        let mut options = OpenOptions::new();
        options.read(true);
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_CLOEXEC);
        }
        drop(options.open(path).map_err(|error| {
            ClassificationOpenError::Refused(redb::DatabaseError::Storage(redb::StorageError::Io(
                error,
            )))
        })?);
        let coordination = |error: LoadReadImageError| match error {
            LoadReadImageError::Contention { .. } => {
                ClassificationOpenError::Refused(redb::DatabaseError::DatabaseAlreadyOpen)
            }
            LoadReadImageError::StoreMissing { reason } => ClassificationOpenError::Refused(
                redb::DatabaseError::Storage(redb::StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    reason,
                ))),
            ),
            other => ClassificationOpenError::Coordination(Error::Other(
                (format!(
                    "could not register the format check's read of {}: {other}",
                    path.display()
                ))
                .into(),
            )),
        };
        let mut registration = ReadSnapshotRegistrationGuard::open_provisional(path, runtime_dir)
            .map_err(coordination)?;
        let view = registration.view().map_err(coordination)?;
        let mut builder = redb_builder();
        builder.set_cache_size(cache_bytes);
        let database = builder
            .open_read_only_external_with_backend(view)
            .map_err(ClassificationOpenError::Refused)?;
        let captured = if database.writer_crashed() {
            Err(ClassificationOpenError::Refused(
                redb::DatabaseError::RepairAborted,
            ))
        } else {
            database
                .committed_snapshot_for_registration()
                .map_err(ClassificationOpenError::Refused)
                .and_then(|snapshot| {
                    registration
                        .upgrade_in_place(path, snapshot)
                        .map_err(coordination)
                        .map(|()| snapshot)
                })
        };
        // The admission locks go back first, on success and failure alike.
        let released = registration.release_load_lock().map_err(coordination);
        match (captured, released) {
            (Ok(snapshot), Ok(())) => Ok(ClassificationCapture {
                database,
                snapshot,
                _registration: registration,
            }),
            (captured, released) => {
                // Nothing can be served from this handle once it is gone, so
                // only then may the record say it serves nothing.
                drop(database);
                if registration.record_write_started {
                    registration.mark_failed();
                }
                Err(captured.err().or(released.err()).expect("one side failed"))
            }
        }
    }

    #[cfg(not(unix))]
    pub(super) fn capture_for_classification(
        path: &Path,
        cache_bytes: usize,
        _runtime_dir: Option<&Path>,
    ) -> std::result::Result<ClassificationCapture, ClassificationOpenError> {
        let descriptor = OpenOptions::new().read(true).open(path).map_err(|error| {
            ClassificationOpenError::Refused(redb::DatabaseError::Storage(redb::StorageError::Io(
                error,
            )))
        })?;
        match fs2::FileExt::try_lock_shared(&descriptor) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(ClassificationOpenError::Refused(
                    redb::DatabaseError::DatabaseAlreadyOpen,
                ));
            }
            Err(error) => {
                return Err(ClassificationOpenError::Refused(
                    redb::DatabaseError::Storage(redb::StorageError::Io(error)),
                ));
            }
        }
        let view = ReadSnapshotFileView {
            file: descriptor.try_clone().map_err(|error| {
                ClassificationOpenError::Refused(redb::DatabaseError::Storage(
                    redb::StorageError::Io(error),
                ))
            })?,
        };
        let mut builder = redb_builder();
        builder.set_cache_size(cache_bytes);
        let database = builder
            .open_read_only_with_backend(view)
            .map_err(ClassificationOpenError::Refused)?;
        Ok(ClassificationCapture {
            database,
            _descriptor: descriptor,
        })
    }

    /// One-line actionable recovery step appended to a corrupt-store error so
    /// the user is never left with a dead end: run `contextdb diagnose` to see
    /// what is salvageable (never modifies the store), or `contextdb reset
    /// --force` to recreate it once you've restored from a backup or a
    /// healthy sync peer if you need the data first.
    pub(crate) const CORRUPT_STORE_NEXT_STEP: &'static str = "run `contextdb diagnose <path>` to see what is \
        salvageable (it never modifies the store), or `contextdb reset <path> --force` to \
        recreate it — restore from a backup or a healthy sync peer first if you need the \
        existing data";

    /// Run a redb store-open closure with the default panic hook suppressed.
    ///
    /// redb can panic (an internal assertion) when a truncated/corrupt file has
    /// a valid-looking header but an inconsistent page layout. `catch_unwind`
    /// catches the unwind, but it does NOT stop the default panic hook from
    /// printing the raw crate location (`redb-x/.../page_manager.rs:NN`) to
    /// stderr first — an implementation leak into the user's terminal. Silence
    /// the hook for the minimal window around the open, then restore it.
    ///
    /// Only this thread's report is silenced, and only while `f` runs: the
    /// process's hook is wrapped once and asks a per-thread counter, so
    /// concurrent opens never wait for one another and a panic on any other
    /// thread is still reported.
    pub(super) fn open_hook_suppressed<T>(f: impl FnOnce() -> T) -> std::thread::Result<T> {
        PANIC_HOOK_WRAPPED.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                if PANIC_REPORTS_SUPPRESSED.with(std::cell::Cell::get) == 0 {
                    previous(info);
                }
            }));
        });
        struct Suppressed;
        impl Drop for Suppressed {
            fn drop(&mut self) {
                PANIC_REPORTS_SUPPRESSED.with(|count| count.set(count.get() - 1));
            }
        }
        PANIC_REPORTS_SUPPRESSED.with(|count| count.set(count.get() + 1));
        let _suppressed = Suppressed;
        catch_unwind(AssertUnwindSafe(f))
    }

    /// The store a recreate is about to replace, opened only so its exclusivity
    /// is proven and its inode stays held until the replacement is published.
    /// A store whose own bytes cannot be read is exactly what recreate exists
    /// to replace, so that unreadability yields no handle instead of aborting;
    /// every other refusal -- a live holder, a permission wall -- still fails
    /// closed and stops the recreate before anything is changed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_outgoing_store(
        path: &Path,
    ) -> Result<(Option<redb::Database>, Option<Arc<LockedWriterFile>>)> {
        let owner = match Self::open_writer_file(path, AbsentStoreAnswer::Taxonomy) {
            Ok(owner) => owner,
            Err(Error::StoreCorrupted { .. }) => return Ok((None, None)),
            Err(error) => return Err(error),
        };
        match Self::open_writer_view_checked(
            path,
            AbsentStoreAnswer::Taxonomy,
            &owner,
            REDB_CACHE_BYTES,
        ) {
            Ok(database) => Ok((Some(database), Some(owner))),
            Err(Error::StoreCorrupted { .. }) => Ok((None, Some(owner))),
            Err(error) => Err(error),
        }
    }

    /// Say what a refused store open means, and what to do about it.
    ///
    /// A person who points a door at a file that cannot be a store is stuck
    /// until they are told what to go and do, so every refusal that is about
    /// the file's contents ends with the one recovery sentence this build
    /// publishes. Contention and permissions are different problems with
    /// different answers, and keep their own wording.
    pub(super) fn refused_open_error_in_runtime_dir(
        path: &Path,
        err: redb::DatabaseError,
        absent: AbsentStoreAnswer,
        runtime_dir: Option<&Path>,
    ) -> Error {
        if matches!(err, redb::DatabaseError::DatabaseAlreadyOpen) {
            return Self::held_store_error_in_runtime_dir(path, runtime_dir);
        }
        // Nothing at the path is only a corruption verdict if something was
        // supposed to be there. For a door that promised to create nothing it
        // is the answer the caller asked for, and telling them to run a repair
        // on a file that does not exist would be worse than useless.
        if absent == AbsentStoreAnswer::Typed && open_found_no_store(&err) {
            return Error::StoreMissing {
                path: path.display().to_string().into(),
            };
        }
        if open_was_refused_by_permissions(&err) {
            return Self::permission_refused_open_error_in_runtime_dir(path, err, runtime_dir);
        }
        if matches!(
            &err,
            redb::DatabaseError::Storage(redb::StorageError::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && error
                        .to_string()
                        .contains("authenticated reader snapshot registry is indeterminate")
        ) {
            // The trusted registration scan could not establish a complete
            // set, so refusing before redb can reclaim is required. The
            // uncertainty is outside the database image and is not evidence
            // that the image itself is corrupt.
            return Self::storage_error(err);
        }
        Error::StoreCorrupted {
            path: path.display().to_string().into(),
            reason: format!(
                "metadata/format could not be read: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            )
            .into(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_file(
        path: &Path,
        absent: AbsentStoreAnswer,
    ) -> Result<Arc<LockedWriterFile>> {
        Self::open_writer_file_in_runtime_dir(path, absent, None)
    }

    #[cfg(unix)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_open_diagnostic(
        path: &Path,
    ) -> Result<wait_diagnostic::PreparedOpenDiagnostic> {
        let context = current_writer_wait();
        let original = match context.payer.as_ref() {
            Some(original) => original.wait_memory_owner().map_err(|error| {
                native_storage_admission::original_storage_error(
                    native_storage_admission::NativeStorageFailure::Refused(error),
                )
            })?,
            None => crate::read_image_memory::active_owner()?.diagnostic_budget()?,
        };
        wait_diagnostic::PreparedOpenDiagnostic::new(original, path)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_file_in_runtime_dir(
        path: &Path,
        absent: AbsentStoreAnswer,
        runtime_dir: Option<&Path>,
    ) -> Result<Arc<LockedWriterFile>> {
        #[cfg(unix)]
        let mut diagnostic = Some(Self::prepare_open_diagnostic(path)?);
        Self::open_writer_file_prepared(
            path,
            absent,
            runtime_dir,
            #[cfg(unix)]
            &mut diagnostic,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_file_prepared(
        path: &Path,
        absent: AbsentStoreAnswer,
        runtime_dir: Option<&Path>,
        #[cfg(unix)] diagnostic: &mut Option<wait_diagnostic::PreparedOpenDiagnostic>,
    ) -> Result<Arc<LockedWriterFile>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_CLOEXEC);
        }
        let file = options.open(path).map_err(|error| {
            #[cfg(unix)]
            {
                diagnostic
                    .take()
                    .expect("prepared descriptor diagnostic")
                    .into_database_error(error.into(), path, absent, runtime_dir)
            }
            #[cfg(not(unix))]
            {
                Self::refused_open_error_in_runtime_dir(path, error.into(), absent, runtime_dir)
            }
        })?;
        let owner = Self::acquire_writer_file(
            path,
            file,
            absent,
            runtime_dir,
            #[cfg(unix)]
            diagnostic,
        )?;
        // Exclusive ownership can be acquired only after every older direct
        // reader has either completed registration or abandoned admission.
        // Capture that stable pre-writer set before redb opens a view and can
        // perform repair/reclamation work. Session registrations do not hold
        // the writer lock and therefore never become writer admission.
        let database_identity = inspect_companion_record(path)
            .ok()
            .map(|record| record.fields.database_identity);
        owner
            .retain_reader_snapshots(database_identity)
            .map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(registry) = owner.reader_snapshots() {
            read_persistence_test_scaffold::note_writer_reader_snapshots_for_test(registry);
        }
        Ok(owner)
    }

    /// Take the physical store for this writer on the descriptor it keeps,
    /// waiting once for readers already admitted, within the caller's bounds.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire_writer_file(
        path: &Path,
        file: File,
        absent: AbsentStoreAnswer,
        runtime_dir: Option<&Path>,
        #[cfg(unix)] diagnostic: &mut Option<wait_diagnostic::PreparedOpenDiagnostic>,
    ) -> Result<Arc<LockedWriterFile>> {
        #[cfg(unix)]
        {
            use physical_admission::{WriterAcquireRefusal, WriterWait, acquire_for_writer};
            let context = current_writer_wait();
            let wait = WriterWait {
                payer: context.payer.as_ref(),
                stop: context.stop.as_ref(),
                deadline: context
                    .deadline
                    .as_ref()
                    .map(|deadline| (Arc::clone(&deadline.clock), deadline.deadline_ms)),
            };
            #[cfg(all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ))]
            let identity = {
                use std::os::unix::fs::MetadataExt as _;
                let metadata = match file.metadata() {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        return Err(diagnostic
                            .take()
                            .expect("prepared writer acquisition diagnostic")
                            .into_storage_error(physical_admission::WaitIo::Io(error)));
                    }
                };
                (metadata.dev(), metadata.ino())
            };
            #[cfg(all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ))]
            if writer_wait_helper::still_quarantined(identity) {
                return Err(Error::ReaderWaitQuarantined {
                    path: diagnostic.take().expect("prepared quarantined path").path,
                });
            }
            match acquire_for_writer(&file, &wait, path) {
                Ok(()) => LockedWriterFile::from_locked(file, runtime_dir).map_err(|error| {
                    diagnostic
                        .take()
                        .expect("prepared writer acquisition diagnostic")
                        .into_storage_error(physical_admission::WaitIo::Io(error))
                }),
                Err(WriterAcquireRefusal::HeldByWriter) => {
                    Err(Self::held_by_writer_error_from_record(path))
                }
                Err(WriterAcquireRefusal::DeadlineExpired) => {
                    Err(Self::held_store_error_in_runtime_dir(path, runtime_dir))
                }
                Err(WriterAcquireRefusal::Cancelled) => Err(Error::ReadCancelled),
                Err(WriterAcquireRefusal::CancellableWaitUnsupported) => {
                    Err(Error::ReaderWaitUnsupported)
                }
                Err(WriterAcquireRefusal::HelperFailed(reason)) => {
                    Err(Error::ReaderWaitHelperFailed(reason.0))
                }
                #[cfg(all(
                    target_os = "linux",
                    any(target_arch = "x86_64", target_arch = "aarch64")
                ))]
                Err(WriterAcquireRefusal::Quarantined(helper)) => {
                    writer_wait_helper::enter_quarantine(identity, file, helper);
                    Err(Error::ReaderWaitQuarantined {
                        path: diagnostic.take().expect("prepared quarantined path").path,
                    })
                }

                Err(WriterAcquireRefusal::Memory(error)) => Err(error),
                Err(WriterAcquireRefusal::NativeIo(error)) => Err(diagnostic
                    .take()
                    .expect("prepared writer acquisition diagnostic")
                    .into_error(error, path, absent, runtime_dir)),
                Err(WriterAcquireRefusal::Io(error)) => Err(diagnostic
                    .take()
                    .expect("prepared writer acquisition diagnostic")
                    .into_error(
                        physical_admission::WaitIo::Io(error),
                        path,
                        absent,
                        runtime_dir,
                    )),
            }
        }
        #[cfg(not(unix))]
        LockedWriterFile::lock(file, runtime_dir).map_err(|error| {
            Self::refused_open_error_in_runtime_dir(path, error, absent, runtime_dir)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_db_checked(
        path: &Path,
        absent: AbsentStoreAnswer,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<OpenedWriterDatabase> {
        #[cfg(unix)]
        let mut diagnostic = Some(Self::prepare_open_diagnostic(path)?);
        let owner = Self::open_writer_file_prepared(
            path,
            absent,
            runtime_dir,
            #[cfg(unix)]
            &mut diagnostic,
        )?;
        let database = Self::open_writer_view_prepared(
            path,
            absent,
            &owner,
            cache_bytes,
            runtime_dir,
            #[cfg(unix)]
            &mut diagnostic,
        )?;
        Ok(OpenedWriterDatabase { database, owner })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_view_checked(
        path: &Path,
        absent: AbsentStoreAnswer,
        owner: &Arc<LockedWriterFile>,
        cache_bytes: usize,
    ) -> Result<redb::Database> {
        Self::open_writer_view_checked_in_runtime_dir(path, absent, owner, cache_bytes, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_view_checked_in_runtime_dir(
        path: &Path,
        absent: AbsentStoreAnswer,
        owner: &Arc<LockedWriterFile>,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
    ) -> Result<redb::Database> {
        #[cfg(unix)]
        let mut diagnostic = Some(Self::prepare_open_diagnostic(path)?);
        Self::open_writer_view_prepared(
            path,
            absent,
            owner,
            cache_bytes,
            runtime_dir,
            #[cfg(unix)]
            &mut diagnostic,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_writer_view_prepared(
        path: &Path,
        absent: AbsentStoreAnswer,
        owner: &Arc<LockedWriterFile>,
        cache_bytes: usize,
        runtime_dir: Option<&Path>,
        #[cfg(unix)] diagnostic: &mut Option<wait_diagnostic::PreparedOpenDiagnostic>,
    ) -> Result<redb::Database> {
        let provider = owner.external_snapshot_provider();
        let opened = Self::open_hook_suppressed(|| {
            redb_builder_with_cache(cache_bytes)
                .open_with_native_backend_and_external_snapshots(owner.view(), provider)
        });
        match opened {
            Ok(Ok(db)) => Ok(db),
            Ok(Err(err)) => {
                #[cfg(unix)]
                {
                    Err(diagnostic
                        .take()
                        .expect("prepared backend open diagnostic")
                        .into_database_error(err, path, absent, runtime_dir))
                }
                #[cfg(not(unix))]
                {
                    Err(Self::refused_open_error_in_runtime_dir(
                        path,
                        err,
                        absent,
                        runtime_dir,
                    ))
                }
            }
            Err(_) => {
                #[cfg(unix)]
                {
                    Err(diagnostic
                        .take()
                        .expect("prepared backend panic diagnostic")
                        .into_panic_error())
                }
                #[cfg(not(unix))]
                {
                    Err(Error::StoreCorrupted {
                        path: path.display().to_string().into(),
                        reason: format!(
                            "metadata/format read panicked; store may be truncated or corrupt — {}",
                            Self::CORRUPT_STORE_NEXT_STEP
                        )
                        .into(),
                    })
                }
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn write_format_marker(
        db: &redb::Database,
        admission: &mut content_identity::ProofAdmission,
    ) -> Result<()> {
        let write_txn = db.begin_write().map_err(Self::storage_error)?;
        custody_groups::initialize_empty(&write_txn)?;
        Self::initialize_history_witnesses(&write_txn)?;
        let write_txn = write_txn.with_table_root_hook(Self::historical_root_event);
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
        {
            let mut table = write_txn
                .open_table(FORMAT_METADATA_TABLE)
                .map_err(Self::storage_error)?;
            let encoded = Self::encode(&CURRENT_FORMAT_VERSION.to_string())?;
            table
                .insert(FORMAT_VERSION_KEY, encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        Self::initialize_graph_proof_tables_in_write(&write_txn)?;
        Self::initialize_row_graph_in_write(&write_txn)?;
        erasure_frontiers::initialize(&write_txn)?;
        Self::initialize_content_identity_in_write_admitted(&write_txn, admission)?;
        write_txn
            .open_table(VECTOR_OPEN_DIRECTORY_TABLE)
            .map_err(Self::storage_error)?
            .insert(VECTOR_OPEN_DIRECTORY_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            &write_txn,
            VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        write_txn
            .open_table(READ_OPEN_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            &write_txn,
            READ_OPEN_CONFIG_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        write_txn
            .open_table(READ_CUSTODY_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            &write_txn,
            READ_CUSTODY_CONFIG_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        write_txn
            .open_table(READ_LINEAGE_CONFIG_TABLE)
            .map_err(Self::storage_error)?
            .insert(READ_OPEN_CONFIG_ROOT_KEY, &[0_u8; 40][..])
            .map_err(Self::storage_error)?;
        Self::write_open_index_commitment(
            &write_txn,
            READ_LINEAGE_CONFIG_COMMITMENT_KEY,
            &[0_u8; 40],
        )?;
        write_txn
            .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
            .map_err(Self::storage_error)?;
        Self::write_vector_body_root_fingerprint(&write_txn)?;
        Self::apply_content_identity_mutations_in_write(&write_txn, &[], admission)?;
        Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
        {
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write_txn.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
                graph_observation_checkpoint(11)?;
            }
            graph_commit_result
        }
        .map_err(Self::storage_error)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_format_marker(db: &redb::Database, path: &Path) -> Result<()> {
        let read_txn = db.begin_read().map_err(|err| {
            crate::funded_error::corruption_display(
                writer_open_diagnostic_budget(),
                path.display(),
                format_args!(
                    "metadata read failed: {err} — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            )
        })?;
        let table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(Error::LegacyVectorStoreDetected {
                    found_format_marker: String::new(),
                    expected_release: CURRENT_FORMAT_VERSION.to_string(),
                });
            }
            Err(err) => {
                return Err(crate::funded_error::corruption_display(
                    writer_open_diagnostic_budget(),
                    path.display(),
                    format_args!(
                        "metadata table could not be read: {err} — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                ));
            }
        };
        let value = table
            .get(FORMAT_VERSION_KEY)
            .map_err(|err| {
                crate::funded_error::corruption_display(
                    writer_open_diagnostic_budget(),
                    path.display(),
                    format_args!(
                        "metadata format_version could not be read: {err} — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                )
            })?
            .ok_or_else(|| {
                crate::funded_error::corruption_display(
                    writer_open_diagnostic_budget(),
                    path.display(),
                    format_args!(
                        "metadata table is missing format_version — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                )
            })?;
        let marker: String = Self::decode(value.value()).map_err(|err| {
            crate::funded_error::corruption_display(
                writer_open_diagnostic_budget(),
                path.display(),
                format_args!(
                    "metadata format_version is corrupt: {err} — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            )
        })?;
        if marker == CURRENT_FORMAT_VERSION {
            for definition in [
                GRAPH_FWD_TABLE,
                GRAPH_REV_TABLE,
                graph_history_record::GRAPH_CONTRIBUTIONS,
                graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
                graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
            ] {
                read_txn.open_table(definition).map_err(|error| {
                    crate::funded_error::corruption_display(
                        writer_open_diagnostic_budget(),
                        path.display(),
                        format_args!("required graph proof table is unavailable: {error}"),
                    )
                })?;
            }
            Self::check_history_witness_schema(&read_txn)
        } else {
            Err(Error::LegacyVectorStoreDetected {
                found_format_marker: marker,
                expected_release: CURRENT_FORMAT_VERSION.to_string(),
            })
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_complete_vector_partition_image(
        &self,
        stage: &ReceivedSchemaPersistenceProjection,
        publication_lsn: Lsn,
    ) -> Result<CompleteVectorPartitionImage> {
        let paged_rows = if stage.row_records.is_some() {
            Some(stage.materialize_rows_compatibility()?)
        } else {
            None
        };
        let resident_rows = stage.rows_image();
        let source_rows = paged_rows.as_ref().unwrap_or(&*resident_rows);
        let mut decoded_bodies = Vec::new();
        let mut decoded_memberships = Vec::new();
        if stage.vector_records.is_some() {
            stage.visit_vector_bodies(|body| {
                stage.vector_image_memory.reserve(
                    content_identity::original_record_work_bytes(body)?
                        .saturating_add(std::mem::size_of::<VectorEntry>()),
                )?;
                decoded_bodies.reserve_exact(1);
                decoded_bodies.push(body.clone());
                Ok(())
            })?;
            stage.visit_vector_memberships(|member| {
                stage.vector_image_memory.reserve(
                    content_identity::original_record_work_bytes(member)?
                        .saturating_add(std::mem::size_of::<VectorPartitionMembershipRecord>()),
                )?;
                decoded_memberships.reserve_exact(1);
                decoded_memberships.push(member.clone());
                Ok(())
            })?;
        }
        let decoded_bodies = if stage.vector_records.is_some() {
            &decoded_bodies
        } else {
            &stage.vectors
        };
        let decoded_memberships = if stage.vector_records.is_some() {
            &decoded_memberships
        } else {
            &stage.vector_memberships
        };
        // Admission precedes indexes and route copies. Each placement names one
        // exact row version and one canonical body, never a row/body cross product.
        let row_count = source_rows.values().map(Vec::len).sum::<usize>();
        let record_slots = std::mem::size_of::<VectorPartitionMembershipRecord>()
            .saturating_mul(4)
            .saturating_add(std::mem::size_of::<VectorPartitionJournalRecord>().saturating_mul(8))
            .saturating_add(std::mem::size_of::<VectorPartitionTombstoneRecord>().saturating_mul(4))
            .saturating_add(1024);
        let mut bytes =
            row_count
                .saturating_mul(std::mem::size_of::<(&str, &VersionedRow)>())
                .saturating_add(decoded_bodies.len().saturating_mul(
                    std::mem::size_of::<&VectorEntry>() + std::mem::size_of::<bool>(),
                ));
        for membership in decoded_memberships {
            let route_bytes = membership
                .index
                .table
                .len()
                .saturating_add(membership.index.column.len())
                .saturating_add(membership.partition_key.estimated_bytes())
                .saturating_add(128);
            bytes = bytes
                .saturating_add(record_slots)
                .saturating_add(route_bytes.saturating_mul(16));
        }
        if !decoded_memberships.is_empty() {
            bytes = bytes.saturating_add(record_slots.saturating_mul(4));
        }
        stage.vector_image_memory.reserve(bytes)?;
        let mut rows = Vec::with_capacity(row_count);
        for (table, versions) in source_rows {
            rows.extend(versions.iter().map(|row| (table.as_str(), row)));
        }
        rows.sort_unstable_by_key(|(table, row)| (*table, row.row_id, row.created_tx, row.lsn));
        let mut bodies = decoded_bodies.iter().collect::<Vec<_>>();
        bodies.sort_unstable_by_key(|body| {
            (
                body.index.table.as_str(),
                body.index.column.as_str(),
                body.row_id,
                body.created_tx,
                body.lsn,
            )
        });
        for pair in bodies.windows(2) {
            let same_identity = pair[0].index == pair[1].index
                && pair[0].row_id == pair[1].row_id
                && pair[0].created_tx == pair[1].created_tx
                && pair[0].lsn == pair[1].lsn;
            if same_identity
                && (pair[0].deleted_tx != pair[1].deleted_tx
                    || pair[0].vector.len() != pair[1].vector.len()
                    || pair[0]
                        .vector
                        .iter()
                        .zip(&pair[1].vector)
                        .any(|(a, b)| a.to_bits() != b.to_bits()))
            {
                return Err(self.vector_partition_corruption(
                    "received placements disagree on their canonical body",
                ));
            }
        }
        bodies.dedup_by_key(|body| {
            (
                body.index.table.as_str(),
                body.index.column.as_str(),
                body.row_id,
                body.created_tx,
                body.lsn,
            )
        });
        let mut body_has_placement = vec![false; bodies.len()];
        let mut memberships_by_key = BTreeMap::<Vec<u8>, VectorPartitionMembershipRecord>::new();
        let mut occupied = HashMap::<VectorIndexRef, HashSet<VectorPartitionKey>>::new();
        let mut caps = HashMap::<VectorIndexRef, Option<u32>>::new();
        for membership in decoded_memberships {
            let (meta, declaration) = stage
                .table_meta
                .get(&membership.index.table)
                .and_then(|meta| {
                    meta.columns
                        .iter()
                        .find(|column| column.name == membership.index.column)
                        .map(|column| (meta, column))
                })
                .filter(|(_, column)| matches!(&column.column_type, ColumnType::Vector(_)))
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: membership.index.clone(),
                })?;
            let row_position = rows
                .binary_search_by_key(
                    &(
                        membership.index.table.as_str(),
                        membership.row_id,
                        membership.row_created_tx,
                        membership.row_lsn,
                    ),
                    |(table, row)| (*table, row.row_id, row.created_tx, row.lsn),
                )
                .map_err(|_| {
                    self.vector_partition_corruption(
                        "received placement has no exact accepted row owner",
                    )
                })?;
            let row = rows[row_position].1;
            let body_position = bodies
                .binary_search_by_key(
                    &(
                        membership.index.table.as_str(),
                        membership.index.column.as_str(),
                        membership.row_id,
                        membership.vector_created_tx,
                        membership.vector_lsn,
                    ),
                    |body| {
                        (
                            body.index.table.as_str(),
                            body.index.column.as_str(),
                            body.row_id,
                            body.created_tx,
                            body.lsn,
                        )
                    },
                )
                .map_err(|_| {
                    self.vector_partition_corruption(
                        "received placement has no exact canonical body",
                    )
                })?;
            let body = bodies[body_position];
            body_has_placement[body_position] = true;
            if membership.visible_from < TxId(row.created_tx.0.max(body.created_tx.0))
                || membership
                    .deleted_tx
                    .is_some_and(|deleted| deleted <= membership.visible_from)
                || [row.deleted_tx, body.deleted_tx]
                    .into_iter()
                    .flatten()
                    .any(|deleted| {
                        membership
                            .deleted_tx
                            .is_none_or(|placement_end| placement_end > deleted)
                    })
            {
                return Err(self.vector_partition_corruption(
                    "received placement exceeds its exact row or body lifecycle",
                ));
            }
            let partition_key =
                vector_partition_key_for_row(&membership.index, meta, declaration, row)?;
            if partition_key != membership.partition_key {
                return Err(self.vector_partition_corruption(
                    "received placement partition disagrees with its exact row owner",
                ));
            }
            caps.entry(membership.index.clone())
                .or_insert_with(|| declaration.effective_max_partitions());
            let key = Self::vector_partition_membership_key(membership);
            if let Some(existing) = memberships_by_key.insert(key, membership.clone())
                && existing != *membership
            {
                return Err(self.vector_partition_corruption(
                    "complete received image contains conflicting vector memberships",
                ));
            }
            // The cap counts placements that are still live, as the vector
            // store's own cap and SHOW VECTOR_INDEXES do; an ended placement
            // occupies no partition.
            if membership.deleted_tx.is_none() {
                occupied
                    .entry(membership.index.clone())
                    .or_default()
                    .insert(partition_key);
            }
        }
        if body_has_placement
            .iter()
            .any(|has_placement| !has_placement)
        {
            return Err(self.vector_partition_corruption(
                "received canonical body has no exact accepted placement",
            ));
        }

        for (index, partitions) in occupied {
            if let Some(max_partitions) = caps.get(&index).copied().flatten()
                && partitions.len() > max_partitions as usize
            {
                return Err(Error::VectorPartitionLimitExceeded {
                    index,
                    max_partitions,
                });
            }
        }

        let memberships = memberships_by_key.into_values().collect::<Vec<_>>();
        let mut journal = Vec::new();
        let mut tombstones = Vec::new();
        let mut ordinal = 0_u64;
        for membership in &memberships {
            journal.push(VectorPartitionJournalRecord {
                index: membership.index.clone(),
                partition_key: membership.partition_key.clone(),
                lsn: publication_lsn,
                ordinal,
                change: VectorPartitionJournalChange::Upsert(membership.clone()),
            });
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                Error::Other(
                    ("complete vector partition journal ordinal overflow".to_string()).into(),
                )
            })?;
            if let Some(deleted_tx) = membership.deleted_tx {
                let tombstone = VectorPartitionTombstoneRecord {
                    membership: membership.clone(),
                    deleted_tx,
                    lsn: publication_lsn,
                    cause: VectorPartitionTombstoneCause::Delete,
                };
                journal.push(VectorPartitionJournalRecord {
                    index: membership.index.clone(),
                    partition_key: membership.partition_key.clone(),
                    lsn: publication_lsn,
                    ordinal,
                    change: VectorPartitionJournalChange::Tombstone(tombstone.clone()),
                });
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    Error::Other(
                        ("complete vector partition journal ordinal overflow".to_string()).into(),
                    )
                })?;
                tombstones.push(tombstone);
            }
        }
        Ok(CompleteVectorPartitionImage {
            paged_memberships: false,
            memberships,
            journal,
            tombstones,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_vector_partition_image_replacement_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        affected_tables: &HashSet<String>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        let matches = |index: &VectorIndexRef| affected_tables.contains(&index.table);
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionMembershipRecord>(
            write_txn,
            VECTOR_PARTITION_MEMBERSHIP_TABLE,
            VectorPartitionRecordKind::Membership,
            &matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionJournalRecord>(
            write_txn,
            VECTOR_PARTITION_JOURNAL_TABLE,
            VectorPartitionRecordKind::Journal,
            &matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionTombstoneRecord>(
            write_txn,
            VECTOR_PARTITION_TOMBSTONE_TABLE,
            VectorPartitionRecordKind::Tombstone,
            &matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionBaseGenerationRecord>(
            write_txn,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            VectorPartitionRecordKind::BaseGeneration,
            &matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionChangeGenerationRecord>(
            write_txn,
            VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
            VectorPartitionRecordKind::ChangeGeneration,
            &matches,
        )?;
        self.remove_vector_partition_records_matching_in_write::<VectorPartitionGenerationCatalogRecord>(
            write_txn,
            VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
            VectorPartitionRecordKind::GenerationCatalog,
            &matches,
        )?;
        self.remove_vector_partition_graph_pages_matching_in_write(
            write_txn,
            Some(accountant.clone()),
            &matches,
        )?;
        for table in affected_tables {
            vector_membership_pages::remove_routes_matching(
                write_txn,
                table,
                None,
                self.proof_accountant(),
            )?;
        }
        // Seed surviving fixed frontiers before replaying membership transitions,
        // so the rebuilt candidate counts include exactly post-frontier changes.
        self.seed_paged_image_frontiers_in_write(write_txn, affected_tables, accountant)?;
        Ok(())
    }
}
