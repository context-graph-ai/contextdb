use super::*;

/// The one door every physical posting comes through, on the point probe, both
/// range directions and the write path's index rebuild. A key or visibility
/// record that is not exactly and canonically what this store writes is typed
/// store corruption here, before the entry advances anything.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_relational_index_entry(
    database_path: &Path,
    entry: Option<(
        redb::AccessGuard<'_, DurableRelationalIndexKey>,
        redb::AccessGuard<'_, &[u8]>,
    )>,
) -> Result<Option<(Vec<u8>, RelationalIndexPosition, Option<TxId>)>> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    let Some((key, value)) = entry else {
        return Ok(None);
    };
    let corrupt = |reason: contextdb_core::error::DiagnosticText| {
        crate::funded_error::corruption_text(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            database_path.display(),
            reason,
        )
    };
    let key = key.value();
    let position = key.decode_canonical().map_err(|error| match error {
        Error::Other(reason) => corrupt(reason),
        other => other,
    })?;
    let deleted_tx = RedbPersistence::decode_exact::<Option<TxId>>(value.value()).map_err(
        |error| match error {
            Error::Other(reason) => crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                database_path.display(),
                format_args!("durable index visibility record is damaged: {reason}"),
            ),
            other => other,
        },
    )?;
    Ok(Some((key.0, position, deleted_tx)))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_relational_index_entry_original(
    frame: &OriginalPersistenceFrame<'_>,
    database_path: &Path,
    entry: Option<(
        redb::AccessGuard<'_, DurableRelationalIndexKey>,
        redb::AccessGuard<'_, &[u8]>,
    )>,
) -> Result<Option<(Vec<u8>, RelationalIndexPosition, Option<TxId>)>> {
    let Some((key, value)) = entry else {
        return Ok(None);
    };
    let corrupt = |reason: contextdb_core::error::DiagnosticText| {
        crate::funded_error::corruption_text(
            crate::funded_error::original_budget(frame.original),
            database_path.display(),
            reason,
        )
    };
    // Admit the native owned from_bytes representation before requesting it.
    frame.reserve(key.retained_page_bytes())?;
    let key = key.value();
    let encoded: EncodedRelationalIndexPosition =
        frame.decode(&key.0).map_err(|error| match error {
            Error::Other(reason) => crate::funded_error::corruption_display(
                crate::funded_error::original_budget(frame.original),
                database_path.display(),
                format_args!("durable index position is not an exactly decodable key: {reason}"),
            ),
            other => other,
        })?;
    let _json_scratch = frame.json_scratch(encoded.key.iter().map(|(_, value)| value))?;
    let canonical = frame.encode(&encoded)?;
    if canonical != key.0 {
        return Err(corrupt(
            "durable index position is not canonically encoded".into(),
        ));
    }
    frame.array::<DirectedValue>(encoded.key.len())?;
    let position = encoded.into();
    let deleted_tx = frame
        .decode::<Option<TxId>>(value.value())
        .map_err(|error| match error {
            Error::Other(reason) => crate::funded_error::corruption_display(
                crate::funded_error::original_budget(frame.original),
                database_path.display(),
                format_args!("durable index visibility record is damaged: {reason}"),
            ),
            other => other,
        })?;
    Ok(Some((key.0, position, deleted_tx)))
}

/// Choose a crashed store's root inside the reader's own admission: every
/// header and root page it reads is charged to the reader's declared memory
/// before it is read, and the read's cancellation and deadline are checked
/// before each of those reads. A refusal or a stop is returned as it is --
/// typed, with nothing read after it and no other slot tried.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn capture_crashed_root(
    database: &redb::ReadOnlyDatabase,
    open_context: Option<&crate::executor::bounded::ReadOpenContext>,
) -> std::result::Result<redb::StorageSnapshotIdentity, LoadReadImageError> {
    let Some(context) = open_context else {
        return database
            .crashed_snapshot_for_registration(&mut || Ok(()))
            .map_err(read_database_open_error);
    };
    let stopped = std::cell::RefCell::new(None);
    #[cfg(any(test, feature = "test-seams"))]
    let read_memory =
        read_persistence_test_scaffold::crashed_capture_memory_for_test(context.read_memory());
    #[cfg(not(any(test, feature = "test-seams")))]
    let read_memory = context.read_memory();
    let captured = {
        let mut reads = 0u32;
        let mut checkpoint = || {
            #[cfg(any(test, feature = "test-seams"))]
            if read_persistence_test_scaffold::crashed_capture_checkpoint_for_test(reads) {
                context.cancel_for_test();
            }
            reads = reads.saturating_add(1);
            context.checkpoint().map_err(|error| {
                *stopped.borrow_mut() = Some(error);
                std::io::Error::other("the read stopped while its crashed store's root was chosen")
            })
        };
        graph_read::with_pages_result(
            read_memory,
            || {
                database
                    .crashed_snapshot_for_registration(&mut checkpoint)
                    .map_err(read_database_open_error)
            },
            LoadReadImageError::Memory,
        )
    };
    if let Some(error) = stopped.into_inner() {
        return Err(LoadReadImageError::Memory(error));
    }
    captured
}

impl ReadImageSource {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn start(
        database_path: &Path,
        cache_bytes: usize,
        index_page_capacity: usize,
        source_memory: SourceShared<SourceMemoryReservation>,
        runtime_dir: Option<&Path>,
        registration: Option<ReadSnapshotRegistrationGuard>,
        open_context: Option<&crate::executor::bounded::ReadOpenContext>,
    ) -> std::result::Result<Self, LoadReadImageError> {
        // The hold comes FIRST, before the committed file is opened at all, so
        // there is no instant in which this reader has the store and nothing
        // says so. It is the definitive fact; the breadcrumb below is the
        // best-effort note that lets a refusal name a process.
        let hold = ReaderStoreHold::take(database_path);
        // This shared data-file lock closes the gap for stores whose companion
        // is absent or unavailable. The same descriptor becomes the retained
        // physical capability after registration publication.
        let snapshot_registration = match registration {
            Some(mut registration) => {
                if let Some(transaction) =
                    open_context.and_then(|context| context.registration_transaction())
                    && registration
                        .opening_transaction
                        .as_ref()
                        .is_none_or(|existing| existing.identity() != transaction.identity())
                {
                    registration.opening_transaction = Some(transaction.clone());
                }
                registration
            }
            #[cfg(unix)]
            None => match open_context {
                Some(context) => ReadSnapshotRegistrationGuard::open_provisional_for_read(
                    database_path,
                    runtime_dir,
                    context,
                )?,
                None => ReadSnapshotRegistrationGuard::open_provisional_with_transaction(
                    database_path,
                    runtime_dir,
                    None,
                )?,
            },
            #[cfg(not(unix))]
            None => ReadSnapshotRegistrationGuard::open(database_path)?,
        };
        let breadcrumb = current_reader_breadcrumb().ok();
        // The breadcrumb uses the preferred coordination directory.
        // Publication is best effort; an unverifiable process remains anonymous.
        let breadcrumb_guard = match reader_runtime_directory(runtime_dir) {
            Some(runtime) => match open_context {
                Some(context) => ReaderBreadcrumbGuard::create_for_open(
                    database_path,
                    &runtime,
                    breadcrumb.as_ref(),
                    context,
                )?,
                None => {
                    ReaderBreadcrumbGuard::create_in(database_path, &runtime, breadcrumb.as_ref())
                        .ok()
                }
            },
            None => None,
        };
        let snapshot_view = snapshot_registration.view()?;
        #[cfg(test)]
        let native_observation = direct_source_contract_rows::begin(1, &source_memory.memory);
        let preparation = cache_reserve::direct_preparation(&source_memory.memory)
            .map_err(LoadReadImageError::Memory)?;
        let opened = redb::with_retained_cache_preparation_admission(preparation, || {
            RedbPersistence::open_hook_suppressed(|| {
                let mut builder = redb_builder();
                // Prepay the index for every selectable share; later mutation
                // reuses these physical slots rather than retaining old arenas.
                builder.set_cache_size(index_page_capacity);
                // A reader always reads at a registered root, so its handle never
                // builds or loads allocator state. Where registration exists, the
                // native open's clean-state verdict is asked for separately below.
                if cfg!(unix) || snapshot_registration.registration.is_some() {
                    builder.open_read_only_external_with_backend(snapshot_view)
                } else {
                    // A reader never allocates; a store whose writer died is
                    // refused here and left to the next writer.
                    builder.open_read_only_allocator_free_with_backend(snapshot_view)
                }
            })
        });
        #[cfg(test)]
        drop(native_observation);
        let database = match opened {
            Ok(Ok(database)) => database,
            Ok(Err(error)) => return Err(read_database_open_error(error)),
            Err(_) => {
                return Err(read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    "read-only Redb open panicked while reading the store",
                ));
            }
        };
        // Carry the SAME source reservation into native cache occupancy owners.
        // Detached pages/transactions may outlive this Rust database wrapper;
        // their last physical free must precede the original reserve's refund.
        database
            .set_read_cache_capacity(cache_bytes)
            .map_err(read_storage_error)?;
        let cache_owner = database.retain_cache_reserve(source_memory.clone().into_native_credit());
        unsafe fn settle_source_cache(
            raw: std::ptr::NonNull<()>,
            held: &std::sync::atomic::AtomicUsize,
        ) {
            unsafe { raw.cast::<SourceMemoryReservation>().as_ref() }.settle_cache_free(held);
        }
        // SAFETY: the SAME SourceShared strong owner is inside both reserve
        // counters. The stable payload stays alive through the final callback;
        // settlement only refunds already-freed excess and never accesses cache.
        unsafe {
            cache_owner.set_read_settlement(
                std::ptr::NonNull::from(&*source_memory).cast(),
                settle_source_cache,
            );
        }
        drop(cache_owner);
        // A store whose writer died is read at the commit the next writer's
        // repair keeps, chosen at the same fixed cost as a healthy capture
        // below -- but only when that writer died on this boot, on this exact
        // file, on a local filesystem: then every page of whatever commit the
        // header names is in the file. Otherwise only a writer may settle it.
        #[cfg(unix)]
        let crashed_commit = snapshot_registration.load_lock_held && database.writer_crashed();
        #[cfg(unix)]
        if crashed_commit {
            snapshot_registration.crashed_store_is_readable(database_path)?;
        }
        // Native preparation admitted each actual index/control Layout before
        // allocation. Its genuine receipt now follows the native backings;
        // the page-cache/source reserve above remains independent. No late debit.
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_read_only_open();
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_read_image_source_start_for_test(
            database_path,
            breadcrumb_guard.as_ref().map(|guard| guard.path()),
        );
        #[cfg(any(test, feature = "test-seams"))]
        {
            let checkpoint_breadcrumb = breadcrumb_guard.as_ref().and(breadcrumb.as_ref());
            let checkpoint_path = checkpoint_breadcrumb
                .and_then(|_| breadcrumb_guard.as_ref().map(|guard| guard.path()));
            read_persistence_test_scaffold::read_image_hydration_checkpoint_for_test(
                database_path,
                checkpoint_path,
                checkpoint_breadcrumb,
            );
        }
        // Admission ends here. The root this reader reads is recorded exactly
        // while it still holds the data lock, and only then are the data lock
        // and the admission reader hold given back -- BEFORE the image is
        // decoded. A writer waiting for this reader waits for this much and no
        // more; every later read of this session is at the recorded root.
        #[cfg(unix)]
        let snapshot_registration = {
            let mut snapshot_registration = snapshot_registration;
            if snapshot_registration.load_lock_held {
                let captured = (|| {
                    #[cfg(any(test, feature = "test-seams"))]
                    if read_persistence_test_scaffold::registration_fault_for_test(database_path)
                        == Some(read_persistence_test_scaffold::RegistrationFault::CaptureRefuses)
                    {
                        return Err(read_database_open_error(redb::DatabaseError::RepairAborted));
                    }
                    let storage_snapshot = if crashed_commit {
                        capture_crashed_root(&database, open_context)?
                    } else {
                        database
                            .committed_snapshot_for_registration()
                            .map_err(read_database_open_error)?
                    };
                    snapshot_registration.upgrade_in_place(database_path, storage_snapshot)
                })();
                // Give the admission locks back FIRST, on success and on
                // failure alike; the registration guard -- whose teardown
                // takes registry locks -- is dropped only after this.
                let released = snapshot_registration.release_load_lock();
                // A failure once its record was being written: the data lock
                // and reader hold are given back above, so the record can now
                // say -- provably -- that this reader serves nothing.
                if captured.is_err() && snapshot_registration.record_write_started {
                    snapshot_registration.mark_failed();
                }
                #[cfg(any(test, feature = "test-seams"))]
                read_persistence_test_scaffold::pause_reader_at_point(
                    database_path,
                    read_persistence_test_scaffold::ReaderPausePoint::AfterDRelease,
                );
                captured?;
                released?;
            }
            snapshot_registration
        };
        let store_file_bytes = snapshot_registration.file_len();
        Ok(Self {
            database_path: database_path.to_path_buf(),
            runtime_dir: runtime_dir.map(Path::to_path_buf),
            database: Some(database),
            snapshot_registration: Some(snapshot_registration),
            hold,
            breadcrumb: breadcrumb_guard,
            source_accesses: 0,
            // A stat of the file already open: it reads nothing, changes
            // nothing, and creates nothing, which is what a direct read is
            // allowed to do.
            store_file_bytes,
            source_memory: Some(source_memory),
            #[cfg(any(test, feature = "test-seams"))]
            source_handles_dropped_for_test: false,
            #[cfg(any(test, feature = "test-seams"))]
            source_released_for_test: false,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_read_transaction<T>(
        &mut self,
        read: impl FnOnce(&redb::ReadTransaction) -> std::result::Result<T, LoadReadImageError>,
    ) -> std::result::Result<T, LoadReadImageError> {
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| invalid_read_image_state("read-only database is absent"))?;
        let registration = self
            .snapshot_registration
            .as_ref()
            .ok_or_else(|| invalid_read_image_state("reader snapshot registration is absent"))?;
        let transaction = if registration.registration.is_some() {
            database.begin_external_read(registration.snapshot.storage_snapshot)
        } else {
            database.begin_read()
        }
        .map_err(read_transaction_error)?;
        self.source_accesses = self.source_accesses.checked_add(1).ok_or_else(|| {
            invalid_read_image_state("read-only persistence source-access count overflowed")
        })?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_source_access();
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_read_image_source_access_for_test(&self.database_path);
        let result = read(&transaction);
        if result.is_ok()
            && self
                .snapshot_registration
                .as_ref()
                .is_some_and(|r| r.registration.is_none())
        {
            let storage_snapshot = transaction.storage_snapshot_identity();
            self.snapshot_registration
                .as_mut()
                .ok_or_else(|| invalid_read_image_state("reader snapshot registration is absent"))?
                .publish(
                    &self.database_path,
                    storage_snapshot,
                    self.runtime_dir.as_deref(),
                )?;
        }
        drop(transaction);
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn finish(
        mut self,
        image: ReadPersistenceImage,
    ) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
        let image = ReadPersistenceImage {
            source_path: Some(self.database_path.clone()),
            store_file_bytes: self.store_file_bytes,
            ..image
        };
        if self.source_accesses == 0 {
            return Err(LoadReadImageError::Release(
                "no persistence source access was observed".to_owned(),
            ));
        }
        let database = self.database.take().ok_or_else(|| {
            invalid_read_image_state("read-only database disappeared before retained handoff")
        })?;
        let mut registration = self.snapshot_registration.take().ok_or_else(|| {
            invalid_read_image_state("reader snapshot registration disappeared before handoff")
        })?;
        // Registration is durable and its exact descriptor remains open.
        // Release only the load-time shared lock so an admitted writer may
        // progress while the session record continues to name this snapshot.
        registration.release_load_lock()?;
        let snapshot = registration.snapshot.storage_snapshot;
        let registration = Arc::new(registration);
        let identity_read = database
            .begin_external_read(snapshot)
            .map_err(read_transaction_error)?;
        if identity_read.storage_snapshot_identity() != snapshot {
            return Err(invalid_read_image_state(
                "content-identity verifier escaped the registered snapshot",
            ));
        }
        let cache_memory = self
            .source_memory
            .as_ref()
            .ok_or_else(|| {
                invalid_read_image_state("reader source memory reservation disappeared")
            })?
            .memory
            .clone();
        // The verifier's own walks admit what they allocate through the same
        // accountant this session already reserves its image memory from.
        let mut content_identity = content_identity::ContentIdentityReadVerifier::open(
            &identity_read,
            &self.database_path,
            Some(cache_memory.accountant.clone()),
        )
        .map_err(read_image_error)?;
        // This source took `READ_VERIFIER_MEMORY_BYTES` for the verifier's own
        // retained state before Redb was opened, and holds it for the handle's
        // lifetime. What the record witnesses do not spend of it is the FLOOR
        // of the chunks this reader keeps resident; past that the set grows
        // into this session's own declared read memory and gives those bytes
        // back before any proof admission is refused.
        content_identity.retain_proved_chunks_within(content_identity::RESIDENT_PROOF_CHUNK_BYTES);
        drop(identity_read);
        let logical_frontier = SnapshotId(
            image
                .commit_index
                .values()
                .copied()
                .max()
                .unwrap_or(TxId(0))
                .0
                .max(
                    image
                        .history_on_request
                        .as_ref()
                        .and_then(|history| history.commit_index_last)
                        .map_or(0, |(_, tx)| tx.0),
                ),
        );
        let logical_frontier =
            logical_frontier.max(SnapshotId(image.read_bounds.identity_tx_floor));
        let history_open_control = image
            .history_on_request
            .as_ref()
            .and_then(|history| history.open_control)
            .ok_or_else(|| {
                invalid_read_image_state("validated history control disappeared before handoff")
            })?;
        history_open_control
            .check_snapshot(snapshot)
            .map_err(read_image_error)?;
        let own_control = self
            .source_memory
            .as_mut()
            .ok_or_else(|| {
                invalid_read_image_state("reader source memory reservation disappeared")
            })?
            .detach_control(RetainedReadSource::layout().size())
            .map_err(read_image_error)?;
        let source = RetainedReadSource::try_new_prepaid(
            RetainedReadSourceData {
                database_path: self.database_path.clone(),
                database,
                snapshot,
                logical_frontier,
                graph_lsn_frontier: Lsn(image.read_bounds.source_lsn_floor),
                graph_retirement_frontier: image.read_bounds.graph_retirement_frontier,
                graph_occurrence_id_floor: image.read_bounds.graph_occurrence_id_floor,
                registration,
                content_identity,
                history_open_control,
                read_memory: (image.open_context.read_memory()),
                content_proof: Mutex::new(content_identity::ReadAuthenticationNodes::new(
                    RETAINED_CONTENT_PROOF_BYTES,
                    Some(cache_memory.accountant.clone()),
                )),
                physical_rows: Mutex::new(None),
                image_config_cache: Mutex::new(HashMap::new()),
                native_ivf_cache: Default::default(),
                verified_ordered_runs: VerifiedOrderedRunCache::new(cache_memory),
                _source_memory: self
                    .source_memory
                    .as_ref()
                    .ok_or_else(|| {
                        invalid_read_image_state("reader source memory reservation disappeared")
                    })?
                    .clone(),
            },
            own_control,
        )
        .map_err(LoadReadImageError::Memory)?;
        source
            .authenticate_open_config(&image.config_values)
            .map_err(read_image_error)?;
        source.content_identity.clear_startup_proofs();
        source.clear_statement_proofs();
        // Hydration no longer holds writer admission and a caller waiting to
        // take it is woken here. The session keeps only the unlocked exact
        // descriptor/database/root and its authenticated registration.
        drop(self.hold.take());
        let breadcrumb = self.breadcrumb.take();
        #[cfg(any(test, feature = "test-seams"))]
        let mut note_handles_released = || {
            read_persistence_test_scaffold::note_read_image_source_handles_dropped_for_test(
                &self.database_path,
            );
            self.source_handles_dropped_for_test = true;
        };
        #[cfg(not(any(test, feature = "test-seams")))]
        let note_handles_released = || {};
        // All authentication and proof reads are complete before removal
        // begins. On refusal the staged release consumes and drops the only
        // retained source; no source alias has been published to a caller.
        let (breadcrumb_path, source) = match breadcrumb {
            Some(breadcrumb) => {
                let (path, source) = breadcrumb
                    .release(source, note_handles_released)
                    .map_err(|error| LoadReadImageError::Release(error.to_string()))?;
                (Some(path), source)
            }
            None => {
                note_handles_released();
                (None, source)
            }
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_persistence_release_receipt();
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_read_image_source_release_for_test(
            &self.database_path,
            breadcrumb_path.as_deref(),
        );
        #[cfg(any(test, feature = "test-seams"))]
        {
            self.source_released_for_test = true;
        }
        Ok(ReadPersistenceLoad {
            image,
            release_receipt: ReadPersistenceReleaseReceipt {
                breadcrumb_path,
                source_accesses: self.source_accesses,
                _seal: ReadPersistenceReleaseSeal,
            },
            source,
        })
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for ReadImageSource {
    fn drop(&mut self) {
        if self.source_released_for_test {
            return;
        }
        // Match the native field-drop order on every failed hydration. The
        // events witness physical cleanup; they do not create a success receipt.
        drop(self.database.take());
        drop(self.snapshot_registration.take());
        drop(self.hold.take());
        if !self.source_handles_dropped_for_test {
            read_persistence_test_scaffold::note_read_image_source_handles_dropped_for_test(
                &self.database_path,
            );
        }
        let breadcrumb_path = self
            .breadcrumb
            .as_ref()
            .map(|guard| guard.path().to_path_buf());
        drop(self.breadcrumb.take());
        drop(self.source_memory.take());
        read_persistence_test_scaffold::note_read_image_source_release_for_test(
            &self.database_path,
            breadcrumb_path.as_deref(),
        );
    }
}

pub(super) fn read_image_corruption(
    cause: ReadImageCorruptionCause,
    reason: impl std::fmt::Display,
) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Corrupt { cause, reason }
}

/// A memory refusal or cancellation met while decoding a stored body is the
/// caller's budget or stop, never damage to the store: report it typed.
pub(super) fn preserves_typed_read_refusal(error: &Error) -> bool {
    crate::read_image_memory::is_memory_refusal(error) || matches!(error, Error::ReadCancelled)
}

pub(super) fn read_image_error(error: Error) -> LoadReadImageError {
    if let Error::Other(message) = &error
        && super::is_storage_io(&error)
    {
        return match message
            .as_text()
            .and_then(|message| message.strip_prefix(STORAGE_IO_ERROR_PREFIX))
        {
            Some(reason) => read_image_io("reading Redb storage", reason),
            None => read_image_io("reading Redb storage", message),
        };
    }
    if matches!(
        error,
        Error::MemoryBudgetExceeded { .. } | Error::ReadFailure(_) | Error::ReadCancelled
    ) {
        LoadReadImageError::Memory(error)
    } else if crate::read_image_memory::is_missing_decode_owner(&error) {
        // The existing public direct-file vocabulary cannot yet preserve
        // the additive typed configuration answer. Never call it corruption.
        invalid_read_image_state(error)
    } else {
        read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error)
    }
}

pub(super) fn read_storage_error(error: redb::StorageError) -> LoadReadImageError {
    match error {
        redb::StorageError::AdmissionRefused(error) => match error.downcast::<Error>() {
            Ok(error) => read_image_error(*error),
            Err(error) => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error),
        },
        // A short read or rejected bytes are damage; any other kind is I/O.
        redb::StorageError::Io(error) if super::io_kind_is_damage(error.kind()) => {
            read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error)
        }
        redb::StorageError::Io(error) => read_image_io("reading Redb storage", error),
        redb::StorageError::PreviousIo => read_image_io(
            "reading Redb storage",
            "a prior storage I/O operation failed",
        ),
        redb::StorageError::Corrupted(reason) => {
            read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason)
        }
        redb::StorageError::DatabaseClosed => {
            invalid_read_image_state("read-only Redb database closed during hydration")
        }
        // A refusal with no owner carries no figures to report, and it says
        // nothing about the bytes in the store.
        redb::StorageError::Native(error @ redb::NativeColdReadError::AdmissionRefused) => {
            read_image_io("reading Redb storage", error)
        }
        redb::StorageError::Native(redb::NativeColdReadError::AdmissionRefusedAt(owner)) => {
            let refusal = owner.inspect_failure(|cause| {
                match cause.and_then(|cause| {
                    cause.downcast_ref::<native_storage_admission::NativeStorageFailure>()
                }) {
                    Some(native_storage_admission::NativeStorageFailure::Refused(refusal)) => {
                        Some(*refusal)
                    }
                    _ => None,
                }
            });
            match refusal {
                Some(refusal) => {
                    LoadReadImageError::Memory(native_storage_admission::original_storage_error(
                        native_storage_admission::NativeStorageFailure::Refused(refusal),
                    ))
                }
                // Preserve the existing foreign/legacy diagnosis. This fallback
                // keeps its inherited allocating rendering, without claiming
                // that an opaque foreign cause has the closed source protocol.
                None => read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    redb::StorageError::Native(redb::NativeColdReadError::AdmissionRefusedAt(
                        owner,
                    )),
                ),
            }
        }
        redb::StorageError::Native(redb::NativeColdReadError::AllocatorFailed { site, layout }) => {
            LoadReadImageError::Memory(Error::NativeEngineOther(
                contextdb_core::read_memory::NativeRefusal::AllocatorFailed { site, layout },
            ))
        }
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

pub(super) fn read_table_error(error: redb::TableError) -> LoadReadImageError {
    match error {
        redb::TableError::Storage(error) => read_storage_error(error),
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

pub(super) fn read_transaction_error(error: redb::TransactionError) -> LoadReadImageError {
    match error {
        redb::TransactionError::Storage(error) => read_storage_error(error),
        other => invalid_read_image_state(other),
    }
}

pub(super) fn direct_read_requires_writer(reason: impl std::fmt::Display) -> LoadReadImageError {
    direct_read_requires_writer_for(
        ReadImageRequiresWriterCause::WritableSanitizerWouldChange,
        reason,
    )
}

pub(super) fn direct_read_requires_writer_for(
    cause: ReadImageRequiresWriterCause,
    reason: impl std::fmt::Display,
) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::RequiresWriter { cause, reason }
}

pub(super) fn legacy_read_layout(reason: impl std::fmt::Display) -> LoadReadImageError {
    read_image_corruption(ReadImageCorruptionCause::LegacyLayout, reason)
}

pub(super) fn invalid_read_image_state(reason: impl std::fmt::Display) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::InvalidState { reason }
}

pub(super) fn read_image_contention(reason: impl std::fmt::Display) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Contention { reason }
}

pub(super) fn read_image_io(
    operation: &'static str,
    reason: impl std::fmt::Display,
) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Io { operation, reason }
}

pub(super) fn read_descriptor_open_error(path: &Path, error: std::io::Error) -> LoadReadImageError {
    match error.kind() {
        std::io::ErrorKind::NotFound => LoadReadImageError::StoreMissing {
            reason: format!("{}: {error}", path.display()),
        },
        std::io::ErrorKind::WouldBlock => read_image_contention(error),
        _ => read_image_io("opening the exact read descriptor", error),
    }
}

pub(super) fn read_database_open_error(error: redb::DatabaseError) -> LoadReadImageError {
    match error {
        redb::DatabaseError::DatabaseAlreadyOpen => {
            read_image_contention("the store has an exclusive writer")
        }
        redb::DatabaseError::RepairAborted => direct_read_requires_writer(
            "Redb requires writable recovery before this committed image can be read",
        ),
        redb::DatabaseError::UpgradeRequired(version) => {
            legacy_read_layout(format!("unsupported Redb file format version {version}"))
        }
        redb::DatabaseError::TransactionInProgress => invalid_read_image_state(
            "Redb reported a transaction in progress during read-only open",
        ),
        // What the caller must do next depends entirely on which of these it
        // is: a store that is not there is a dead end, a store somebody else
        // holds means asking that holder instead.
        redb::DatabaseError::Storage(error @ redb::StorageError::Native(_)) => {
            read_storage_error(error)
        }
        redb::DatabaseError::Storage(redb::StorageError::Io(error)) => match error.kind() {
            std::io::ErrorKind::NotFound => LoadReadImageError::StoreMissing {
                reason: error.to_string(),
            },
            std::io::ErrorKind::WouldBlock => read_image_contention(error),
            // The file is there and the machine handed its bytes over
            // perfectly well; the bytes are simply not a store. That is the
            // same finding a writable open makes about the same file, so it
            // is reported as what it is -- a store that cannot be read --
            // rather than as an unbuilt capability, which is what an operator
            // used to be told and could do nothing with.
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof => {
                read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error)
            }
            _ => read_image_io("opening the read-only Redb database", error),
        },
        redb::DatabaseError::Storage(redb::StorageError::Corrupted(reason)) => {
            read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason)
        }
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_table_meta(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<String, TableMeta>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let meta_table = match read_txn.open_table(META_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut table_meta = HashMap::new();
    for entry in meta_table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(name) = key.value().strip_prefix("table:") else {
            continue;
        };
        let (meta, legacy) = RedbPersistence::decode_table_meta_versioned(value.value())
            .map_err(read_image_error)?;
        if legacy {
            return Err(legacy_read_layout(format!(
                "table {name} uses an unsupported metadata layout"
            )));
        }
        table_meta.insert(name.to_owned(), meta);
    }
    Ok(table_meta)
}

/// Both hydration and paged reads use the schema maximum from the pinned,
/// authenticated bounds root; no population scan or retained bound map.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_read_version_bounds(
    read: &redb::ReadTransaction,
    table: &str,
    lsn: Lsn,
    created: TxId,
    deleted: Option<TxId>,
    tx_frontier: TxId,
    lsn_frontier: Lsn,
) -> std::result::Result<(), LoadReadImageError> {
    let table_max = RedbPersistence::table_read_lsn_in_read(read, table)?;
    if lsn > table_max
        || lsn > lsn_frontier
        || created > tx_frontier
        || deleted.is_some_and(|tx| tx > tx_frontier)
    {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            format!("{table} version exceeds its authenticated committed bounds"),
        ));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-seams"))]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_relational_table(
    read_txn: &redb::ReadTransaction,
    name: &str,
    meta: &TableMeta,
) -> std::result::Result<Vec<VersionedRow>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table_name = RedbPersistence::rel_table_name(name);
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name.as_str());
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(redb::TableError::TableTypeMismatch { .. }) => {
            return Err(legacy_read_layout(format!(
                "table {name} uses an unsupported relational layout"
            )));
        }
        Err(error) => return Err(read_table_error(error)),
    };
    let bounds =
        RedbPersistence::read_authenticated_bounds_in_read(read_txn).map_err(read_image_error)?;
    let mut rows = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        // Loading a store is most of what opening one costs, and a caller
        // waiting on a large store is told how far it has got. What arrives is
        // counted where it arrives: the stored bytes of one row.
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_decoded_row();
        contextdb_core::find_attribution::count("decoded_rows", 1);
        let row = RedbPersistence::decode_versioned_row(value.value(), Some(meta))
            .map_err(read_image_error)?;
        validate_read_version_bounds(
            read_txn,
            name,
            row.lsn,
            row.created_tx,
            row.deleted_tx,
            TxId(bounds.identity_tx_floor),
            Lsn(bounds.source_lsn_floor),
        )?;
        validate_read_row(name, &row, meta)?;
        rows.push(row);
    }
    Ok(rows)
}
#[expect(
    clippy::result_large_err,
    reason = "Read-image corruption diagnostics retain their original inline form."
)]
pub(super) fn validate_read_row_id(
    table: &str,
    row: RowId,
) -> std::result::Result<(), LoadReadImageError> {
    if i64::try_from(row.0).is_err() {
        return Err(invalid_read_image_state(format!(
            "row {} in table {table} is outside the relational identifier range",
            row.0
        )));
    }
    Ok(())
}
#[expect(
    clippy::result_large_err,
    reason = "Read-image corruption diagnostics retain their original inline form."
)]
pub(super) fn validate_read_cell_shape(
    table: &str,
    row: RowId,
    column: &str,
    shape: ReadCellShape,
    meta: &TableMeta,
) -> std::result::Result<(), LoadReadImageError> {
    let Some(definition) = meta
        .columns
        .iter()
        .find(|definition| definition.name == column)
    else {
        return Err(direct_read_requires_writer(format!(
            "row {} in table {table} carries unknown column {column}",
            row.0
        )));
    };
    if let ColumnType::Vector(dimension) = definition.column_type {
        let absent_embedding_is_legal =
            definition.nullable || !matches!(definition.quantization, VectorQuantization::F32);
        match shape {
            ReadCellShape::Vector(length) if length == dimension => {}
            ReadCellShape::Null if absent_embedding_is_legal => {}
            _ => {
                return Err(direct_read_requires_writer(format!(
                    "row {} in table {table} carries an invalid vector value for {column}",
                    row.0
                )));
            }
        }
    }
    Ok(())
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn read_row_shape_error(table: &str, error: LoadReadImageError) -> Error {
    crate::funded_error::corruption_display(
        crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
        table,
        error,
    )
}
#[expect(
    clippy::result_large_err,
    reason = "Read-image corruption diagnostics retain their original inline form."
)]
pub(super) fn validate_read_row(
    table: &str,
    row: &VersionedRow,
    meta: &TableMeta,
) -> std::result::Result<(), LoadReadImageError> {
    validate_read_row_id(table, row.row_id)?;
    for (column, value) in &row.values {
        let shape = match value {
            Value::Null => ReadCellShape::Null,
            Value::Vector(values) => ReadCellShape::Vector(values.len()),
            _ => ReadCellShape::Other,
        };
        validate_read_cell_shape(table, row.row_id, column, shape, meta)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn remove_quantized_placeholders(row: &mut VersionedRow, meta: &TableMeta) {
    for column in &meta.columns {
        if !matches!(column.column_type, ColumnType::Vector(_)) {
            continue;
        }
        if !matches!(column.quantization, VectorQuantization::F32)
            && matches!(row.values.get(&column.name), Some(Value::Null))
        {
            row.values.remove(&column.name);
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_vectors(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<VectorEntry>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = match read_txn.open_table(VECTORS_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let bounds =
        RedbPersistence::read_authenticated_bounds_in_read(read_txn).map_err(read_image_error)?;
    let mut vectors = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_decoded_vector();
        contextdb_core::find_attribution::count("decoded_vectors", 1);
        let vector =
            RedbPersistence::decode_vector_entry(value.value()).map_err(read_image_error)?;
        validate_read_version_bounds(
            read_txn,
            &vector.index.table,
            vector.lsn,
            vector.created_tx,
            vector.deleted_tx,
            TxId(bounds.identity_tx_floor),
            Lsn(bounds.source_lsn_floor),
        )?;
        vectors.push(vector);
    }
    Ok(vectors)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_vector_partition_record_payload<T: serde::de::DeserializeOwned>(
    expected_kind: VectorPartitionRecordKind,
    encoded: &[u8],
) -> Result<T> {
    decode_vector_partition_record_payload_checked(expected_kind, encoded).map_err(|failure| {
        match failure {
            RecordDecodeFailure::Refusal(error) => error,
            RecordDecodeFailure::Damage(reason) => Error::Other(reason),
        }
    })
}

impl RecordDecodeFailure {
    pub(super) fn into_error(
        self,
        damage: impl FnOnce(contextdb_core::error::DiagnosticText) -> Error,
    ) -> Error {
        match self {
            Self::Refusal(error) => error,
            Self::Damage(reason) => damage(reason),
        }
    }
}
impl std::fmt::Display for PayloadDecodeDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "payload decode failed: {}", self.0)
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_vector_partition_record_payload_checked<T: serde::de::DeserializeOwned>(
    expected_kind: VectorPartitionRecordKind,
    encoded: &[u8],
) -> std::result::Result<T, RecordDecodeFailure> {
    // This decoder already requires the source's existing operation owner.
    // Prepare the receiver before invoking any failing decode producer.
    let owner = crate::read_image_memory::active_owner().map_err(RecordDecodeFailure::Refusal)?;
    let budget = owner
        .diagnostic_budget()
        .map_err(RecordDecodeFailure::Refusal)?;
    let mut prepared = Some(
        contextdb_core::error::DiagnosticText::try_prepare_shared::<PayloadDecodeDiagnostic>(
            budget,
            "diagnostic",
            "decode_payload",
            "Raise the declared memory limit to decode this record.",
        )
        .map_err(RecordDecodeFailure::Refusal)?,
    );
    let mut classify = |error: Error| {
        if preserves_typed_read_refusal(&error) {
            RecordDecodeFailure::Refusal(error)
        } else {
            RecordDecodeFailure::Damage(contextdb_core::error::DiagnosticText::from_shared(
                prepared
                    .take()
                    .expect("first decode failure owns its prepared receiver")
                    .publish(PayloadDecodeDiagnostic(error)),
            ))
        }
    };
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(&mut classify)?;
    let payload = vector_partition_record_payload(expected_kind, encoded)
        .map_err(RecordDecodeFailure::Damage)?;
    RedbPersistence::decode_exact(payload).map_err(classify)
}

pub(super) fn vector_partition_record_payload(
    expected_kind: VectorPartitionRecordKind,
    encoded: &[u8],
) -> std::result::Result<&[u8], contextdb_core::error::DiagnosticText> {
    let header_len = VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1 + 8;
    let minimum_len = header_len + 32;
    if encoded.len() < minimum_len {
        return Err(contextdb_core::error::DiagnosticText::from(
            "record is truncated",
        ));
    }
    let checksum_offset = encoded.len() - 32;
    let expected_checksum = blake3::hash(&encoded[..checksum_offset]);
    #[cfg(any(test, feature = "test-seams"))]
    if expected_kind == VectorPartitionRecordKind::Membership {
        crate::database::resident_history_test_seams::note(|work| {
            work.membership_envelopes_verified += 1
        });
    }
    if expected_checksum.as_bytes()[..] != encoded[checksum_offset..] {
        return Err(contextdb_core::error::DiagnosticText::from(
            "checksum mismatch",
        ));
    }
    if &encoded[..VECTOR_PARTITION_RECORD_MAGIC.len()] != VECTOR_PARTITION_RECORD_MAGIC {
        return Err(contextdb_core::error::DiagnosticText::from(
            "magic mismatch",
        ));
    }
    let mut cursor = VECTOR_PARTITION_RECORD_MAGIC.len();
    let version = u16::from_be_bytes(
        encoded[cursor..cursor + 2]
            .try_into()
            .map_err(|_| contextdb_core::error::DiagnosticText::from("version is truncated"))?,
    );
    cursor += 2;
    if version != VECTOR_PARTITION_RECORD_VERSION {
        return Err(contextdb_core::error::DiagnosticText::from_unsigned(
            "unsupported record version ",
            u64::from(version),
            "",
        ));
    }
    let kind = encoded[cursor];
    cursor += 1;
    if kind != expected_kind as u8 {
        return Err(contextdb_core::error::DiagnosticText::from(
            "record kind mismatch",
        ));
    }
    let payload_len = usize::try_from(u64::from_be_bytes(
        encoded[cursor..cursor + 8]
            .try_into()
            .map_err(|_| contextdb_core::error::DiagnosticText::from("length is truncated"))?,
    ))
    .map_err(|_| {
        contextdb_core::error::DiagnosticText::from("payload length does not fit usize")
    })?;
    cursor += 8;
    let payload_end = cursor
        .checked_add(payload_len)
        .ok_or_else(|| contextdb_core::error::DiagnosticText::from("payload length overflow"))?;
    if payload_end != checksum_offset {
        return Err(contextdb_core::error::DiagnosticText::from(
            "payload length mismatch",
        ));
    }
    Ok(&encoded[cursor..payload_end])
}

impl<'de> Deserialize<'de> for InspectedVectorPartitionComponents {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct ComponentsVisitor;
        impl<'de> serde::de::Visitor<'de> for ComponentsVisitor {
            type Value = InspectedVectorPartitionComponents;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("vector partition components")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut count = 0usize;
                let mut text_bytes = 0usize;
                while let Some(component) =
                    sequence.next_element::<InspectedVectorPartitionComponent<'de>>()?
                {
                    count = count.checked_add(1).ok_or_else(|| {
                        serde::de::Error::custom("partition component count overflow")
                    })?;
                    if let InspectedVectorPartitionComponent::Text(text) = component {
                        text_bytes = text_bytes.checked_add(text.len()).ok_or_else(|| {
                            serde::de::Error::custom("partition component text overflow")
                        })?;
                    }
                }
                Ok(InspectedVectorPartitionComponents { count, text_bytes })
            }
        }
        deserializer.deserialize_seq(ComponentsVisitor)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for InspectedSequence<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct SequenceVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for SequenceVisitor<T> {
            type Value = InspectedSequence<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a complete persisted sequence")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                while sequence.next_element::<T>()?.is_some() {}
                Ok(InspectedSequence {
                    marker: std::marker::PhantomData,
                })
            }
        }
        deserializer.deserialize_seq(SequenceVisitor(std::marker::PhantomData))
    }
}

pub(super) fn inspect_vector_partition_metadata_route(
    payload: &[u8],
) -> std::result::Result<usize, contextdb_core::error::DiagnosticText> {
    // Struct/tuple field boundaries have no framing in the existing bincode
    // format: index.table, index.column, then the partition's component Vec.
    type Route<'a> = (&'a str, &'a str, InspectedVectorPartitionComponents);
    bincode::serde::borrow_decode_from_slice::<Route<'_>, _>(payload, bincode::config::standard())
        .map(|(_, consumed)| consumed)
        .map_err(|_| {
            contextdb_core::error::DiagnosticText::from("metadata route is truncated or invalid")
        })
}

/// Inspect native route capacity using borrowed fields before its owning
/// serde decoder or any route/key clones can allocate. Vec growth is admitted
/// through the next power-of-two capacity, including short scalar components.
pub(super) fn vector_partition_route_native_capacity(
    payload: &[u8],
) -> std::result::Result<usize, contextdb_core::error::DiagnosticText> {
    type Route<'a> = (&'a str, &'a str, InspectedVectorPartitionComponents);
    let ((table, column, components), _) =
        bincode::serde::borrow_decode_from_slice::<Route<'_>, _>(
            payload,
            bincode::config::standard(),
        )
        .map_err(|_| {
            contextdb_core::error::DiagnosticText::from("metadata route is truncated or invalid")
        })?;
    let capacity = if components.count == 0 {
        0
    } else {
        components
            .count
            .checked_next_power_of_two()
            .ok_or_else(|| {
                contextdb_core::error::DiagnosticText::from("partition native capacity overflow")
            })?
    };
    capacity
        .checked_mul(std::mem::size_of::<contextdb_core::VectorPartitionComponent>())
        .and_then(|n| n.checked_add(components.text_bytes))
        .and_then(|n| n.checked_add(table.len()))
        .and_then(|n| n.checked_add(column.len()))
        .ok_or_else(|| {
            contextdb_core::error::DiagnosticText::from("partition native route bytes overflow")
        })
}

pub(super) fn prevalidate_vector_partition_metadata_routes(
    kind: VectorPartitionRecordKind,
    payload: &[u8],
) -> std::result::Result<(), contextdb_core::error::DiagnosticText> {
    if !matches!(
        kind,
        VectorPartitionRecordKind::Membership
            | VectorPartitionRecordKind::Journal
            | VectorPartitionRecordKind::Tombstone
            | VectorPartitionRecordKind::BaseGeneration
    ) {
        return Ok(());
    }
    let route_end = inspect_vector_partition_metadata_route(payload)?;
    if matches!(kind, VectorPartitionRecordKind::BaseGeneration) {
        type Graph<'a> = (&'a [u8], [u8; 32]);
        type BaseTail<'a> = (
            u64,
            TxId,
            Lsn,
            TxId,
            Lsn,
            u32,
            VectorQuantization,
            u16,
            u16,
            Graph<'a>,
            [u8; 32],
        );
        type ChangeTail<'a> = (
            u64,
            u64,
            TxId,
            Lsn,
            TxId,
            Lsn,
            u32,
            VectorQuantization,
            u16,
            u16,
            Graph<'a>,
        );
        let tail = &payload[route_end..];
        let consumed = if kind == VectorPartitionRecordKind::BaseGeneration {
            bincode::serde::borrow_decode_from_slice::<BaseTail<'_>, _>(
                tail,
                bincode::config::standard(),
            )
            .map(|(_, consumed)| consumed)
        } else {
            bincode::serde::borrow_decode_from_slice::<ChangeTail<'_>, _>(
                tail,
                bincode::config::standard(),
            )
            .map(|(_, consumed)| consumed)
        }
        .map_err(|_| {
            contextdb_core::error::DiagnosticText::from(
                "generation graph payload is truncated or invalid",
            )
        })?;
        if consumed != tail.len() {
            return Err(contextdb_core::error::DiagnosticText::from(
                "generation graph payload has trailing bytes",
            ));
        }
    }
    if kind == VectorPartitionRecordKind::Journal {
        let ((_, _, variant), fixed_bytes): ((Lsn, u64, u32), usize) =
            bincode::serde::decode_from_slice(&payload[route_end..], bincode::config::standard())
                .map_err(|_| {
                contextdb_core::error::DiagnosticText::from(
                    "journal fixed prefix is truncated or invalid",
                )
            })?;
        if variant > 1 {
            return Err(contextdb_core::error::DiagnosticText::from(
                "journal change variant is invalid",
            ));
        }
        inspect_vector_partition_metadata_route(&payload[route_end + fixed_bytes..])?;
    }
    Ok(())
}

/// Compare the existing canonical serializer directly against authenticated
/// bytes. This owns no route buffer and never asks serde to allocate from a
/// persisted string/sequence length hint.
pub(super) fn vector_partition_expected_route_prefix(
    payload: &[u8],
    index: &VectorIndexRef,
    partition_key: &VectorPartitionKey,
) -> std::result::Result<usize, contextdb_core::error::DiagnosticText> {
    struct CompareWriter<'a> {
        remaining: &'a [u8],
    }
    impl std::io::Write for CompareWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if !self.remaining.starts_with(bytes) {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            self.remaining = &self.remaining[bytes.len()..];
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    bincode::serde::encode_into_std_write(
        (index, partition_key),
        &mut CompareWriter { remaining: payload },
        bincode::config::standard(),
    )
    .map_err(|_| {
        contextdb_core::error::DiagnosticText::from(
            "record route prefix does not match its expected partition",
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_raw_vector_entry_encoded_width(
    bytes: &[u8],
    index: &VectorIndexRef,
    dimension: usize,
    corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
) -> Result<()> {
    // Identical envelope bound for writable and retained raw-body readers.
    const FRAMING_BYTES: usize = 2 * 9 + 9 + 5 + 9 + 2 * 4 + 5 + 9 + 1 + 9 + 9;
    let bound = dimension
        .checked_mul(std::mem::size_of::<f32>())
        .and_then(|bytes| bytes.checked_add(index.table.len()))
        .and_then(|bytes| bytes.checked_add(index.column.len()))
        .and_then(|bytes| bytes.checked_add(FRAMING_BYTES))
        .ok_or_else(|| corruption("raw-vector encoded width bound overflow".into()))?;
    if bytes.len() > bound {
        return Err(crate::funded_error::map_diagnostic(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            format_args!(
                "raw-vector encoded body width {} exceeds declared route/dimension bound {bound}",
                bytes.len()
            ),
            &corruption,
        ));
    }
    Ok(())
}

/// Retained images use the same persisted codec but must check identities and
/// predicted expansion before allocating the decoded F32 representation.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_retained_raw_vector_entry(
    bytes: &[u8],
    membership: &VectorPartitionMembershipRecord,
    expected_dimension: usize,
    corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
) -> Result<VectorEntry> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    validate_raw_vector_entry_encoded_width(
        bytes,
        &membership.index,
        expected_dimension,
        &corruption,
    )?;
    let persisted: PersistedVectorEntry = RedbPersistence::decode(bytes).map_err(|error| {
        if preserves_typed_read_refusal(&error) {
            return error;
        }
        crate::funded_error::map_diagnostic(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            format_args!("a raw-vector directory body cannot be decoded: {error}"),
            &corruption,
        )
    })?;
    if persisted.index != membership.index
        || persisted.row_id != membership.row_id
        || persisted.created_tx != membership.vector_created_tx
        || persisted.lsn != membership.vector_lsn
    {
        return Err(corruption(
            "a raw-vector body disagrees with its membership identity".into(),
        ));
    }
    let decoded_bytes = persisted.vector.decoded_bytes();
    let decoded_dimension = decoded_bytes / std::mem::size_of::<f32>();
    if decoded_dimension != expected_dimension {
        return Err(crate::funded_error::map_diagnostic(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            format_args!(
                "raw-vector body dimension {decoded_dimension} does not match declared dimension {expected_dimension}"
            ),
            &corruption,
        ));
    }
    crate::read_image_memory::reserve(decoded_bytes).map_err(|error| {
        if preserves_typed_read_refusal(&error) {
            return error;
        }
        crate::funded_error::map_diagnostic(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            format_args!("a raw-vector directory body cannot be decoded: {error}"),
            &corruption,
        )
    })?;
    Ok(VectorEntry {
        index: persisted.index,
        row_id: persisted.row_id,
        vector: persisted.vector.to_f32(),
        created_tx: persisted.created_tx,
        deleted_tx: persisted.deleted_tx,
        lsn: persisted.lsn,
    })
}

/// Count first in the same pinned root, then admit compact identity owners
/// before decoding. Only one route-bearing membership is owned at a time.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn stream_raw_partition_memberships<T>(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
    prepare: impl FnOnce(usize) -> Result<T>,
    mut visit: impl FnMut(&mut T, VectorPartitionMembershipRecord) -> Result<()>,
    corruption: impl Fn(contextdb_core::error::DiagnosticText) -> Error,
) -> Result<T> {
    vector_membership_source::operation_checkpoint()?;
    let prefix =
        RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
    let table = match read.open_byte_table_bounded(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return prepare(0),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut count = 0usize;
    for item in table
        .prefix_bounded(prefix.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        vector_membership_source::operation_checkpoint()?;
        let (key, _) = item.map_err(RedbPersistence::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::resident_history_test_seams::note(|work| {
            work.membership_keys_examined += 1
        });
        if !key.value().starts_with(&prefix) {
            break;
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| corruption("raw membership count overflow".into()))?;
    }
    let mut state = prepare(count)?;
    for item in table
        .prefix_bounded(prefix.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        vector_membership_source::operation_checkpoint()?;
        let (key, value) = item.map_err(RedbPersistence::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::resident_history_test_seams::note(|work| {
            work.membership_keys_examined += 1
        });
        if !key.value().starts_with(&prefix) {
            break;
        }
        let payload = vector_partition_expected_metadata_payload(
            VectorPartitionRecordKind::Membership,
            value.value(),
            &partition.index,
            &partition.partition_key,
        )
        .map_err(&corruption)?;
        // Each callback consumes this one metadata window. Its decoded route
        // and temporary body decodes must release before the next membership,
        // rather than accumulating in the enclosing operation's image owner.
        let window = crate::read_image_memory::active_owner()?.row_owner()?;
        window.scope(|| -> Result<()> {
            let membership: VectorPartitionMembershipRecord =
                RedbPersistence::decode_exact(payload).map_err(|error| {
                    if preserves_typed_read_refusal(&error) {
                        error
                    } else {
                        crate::funded_error::map_diagnostic(
                            window.diagnostic_budget(),
                            format_args!("raw membership payload decode failed: {error}"),
                            &corruption,
                        )
                    }
                })?;
            if RedbPersistence::vector_partition_membership_key(&membership).as_slice()
                != key.value()
            {
                return Err(corruption(
                    "raw-vector membership key disagrees with its checksummed payload".into(),
                ));
            }
            vector_membership_source::operation_checkpoint()?;
            visit(&mut state, membership)
        })?;
    }
    vector_membership_source::operation_checkpoint()?;
    Ok(state)
}

pub(super) fn vector_partition_expected_metadata_payload<'a>(
    kind: VectorPartitionRecordKind,
    encoded: &'a [u8],
    index: &VectorIndexRef,
    partition_key: &VectorPartitionKey,
) -> std::result::Result<&'a [u8], contextdb_core::error::DiagnosticText> {
    let payload = vector_partition_record_payload(kind, encoded)?;
    let route_end = vector_partition_expected_route_prefix(payload, index, partition_key)?;
    if kind == VectorPartitionRecordKind::Journal {
        // Both variants place membership first, hence repeat the same route.
        // These fixed scalar fields are decoded by bincode without heap owners.
        let ((_, _, variant), fixed_bytes): ((Lsn, u64, u32), usize) =
            bincode::serde::decode_from_slice(&payload[route_end..], bincode::config::standard())
                .map_err(|_| {
                contextdb_core::error::DiagnosticText::from(
                    "journal fixed prefix is truncated or invalid",
                )
            })?;
        if variant > 1 {
            return Err(contextdb_core::error::DiagnosticText::from(
                "journal change variant is invalid",
            ));
        }
        vector_partition_expected_route_prefix(
            &payload[route_end + fixed_bytes..],
            index,
            partition_key,
        )?;
    }
    Ok(payload)
}

/// Stale catalog attribution needs only an exact route and an authenticated,
/// completely decodable fixed suffix. Descriptors and publication IDs contain
/// no strings or sequences, so unrelated catalogs never allocate route owners.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn verify_open_index_commitment(
    read_txn: &redb::ReadTransaction,
    key: &str,
    root: &[u8],
) -> std::result::Result<(), LoadReadImageError> {
    let metadata = read_txn
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(read_table_error)?;
    let commitment = metadata
        .get(key)
        .map_err(read_storage_error)?
        .ok_or_else(|| {
            read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                format!("{key} commitment is absent"),
            )
        })?;
    if commitment.value() != blake3::hash(root).as_bytes() {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            format!("{key} commitment disagrees with the open index"),
        ));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_scan_body_root_matches(read: &redb::ReadTransaction) -> Result<bool> {
    let actual = read
        .table_root_fingerprint(VECTORS_TABLE)
        .map_err(vector_membership_pages::metadata_error)?
        .unwrap_or([0; 32]);
    let metadata = match read.open_table(CONTENT_IDENTITY_METADATA_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(error) => return Err(vector_membership_pages::metadata_error(error)),
    };
    let expected = metadata
        .get(VECTOR_BODY_ROOT_FINGERPRINT_KEY)
        .map_err(vector_membership_pages::metadata_error)?;
    Ok(expected.is_some_and(|expected| expected.value() == actual))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn vector_body_root_matches(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<bool, LoadReadImageError> {
    let actual = read_txn
        .table_root_fingerprint(VECTORS_TABLE)
        .map_err(read_table_error)?
        .unwrap_or([0; 32]);
    let metadata = read_txn
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(read_table_error)?;
    let expected = metadata
        .get(VECTOR_BODY_ROOT_FINGERPRINT_KEY)
        .map_err(read_storage_error)?
        .ok_or_else(|| {
            read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "vector body root fingerprint is absent",
            )
        })?;
    Ok(expected.value() == actual)
}

/// Validate the committed vector directory and report whether it is ready.
/// Vector bodies and journal pages deliberately remain untouched during hydration.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_raw_vector_directory(
    read_txn: &redb::ReadTransaction,
    memory: &crate::read_image_memory::ImageMemoryHandle,
) -> std::result::Result<ReadRawVectorDirectory, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    vector_membership_pages::validate_in_read(read_txn).map_err(read_image_error)?;
    // Schema metadata is store-wide: damage there refuses the whole read here.
    vector_paged::validate_schema_meta_root(read_txn).map_err(read_image_error)?;
    let mut raw_ready = true;
    let memberships = match read_txn.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
        Ok(table) => Some(table),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(read_table_error(error)),
    };
    let directory = match read_txn.open_table(VECTOR_OPEN_DIRECTORY_TABLE) {
        Ok(table) => Some(table),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(read_table_error(error)),
    };
    if memberships.is_some() && directory.is_none() {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            "compact vector directory table is absent",
        ));
    }
    let directory_root = directory
        .as_ref()
        .map(|table| {
            table
                .get(VECTOR_OPEN_DIRECTORY_ROOT_KEY)
                .map_err(read_storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    read_image_corruption(
                        ReadImageCorruptionCause::MalformedRecord,
                        "compact vector directory root is absent",
                    )
                })
        })
        .transpose()?;
    if let Some(root) = &directory_root {
        if root.len() != 40 {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "compact vector directory root has invalid size",
            ));
        }
        verify_open_index_commitment(read_txn, VECTOR_OPEN_DIRECTORY_COMMITMENT_KEY, root)?;
    }
    let memberships_present = memberships.is_some();
    let membership_count = match memberships.as_ref() {
        Some(table) => table.len().map_err(read_storage_error)?,
        None => 0,
    };
    drop(memberships);
    drop(directory);
    if !memberships_present {
        let indexed_count = directory_root
            .as_ref()
            .map(|root| u64::from_be_bytes(root[..8].try_into().unwrap()))
            .unwrap_or(0);
        let body_count = match read_txn.open_table(VECTORS_TABLE) {
            Ok(table) => table.len().map_err(read_storage_error)?,
            Err(redb::TableError::TableDoesNotExist(_)) => 0,
            Err(error) => return Err(read_table_error(error)),
        };
        // An empty vector store needs no directory. A store with an indexed
        // vector or a body but no memberships is refused by the open.
        raw_ready = indexed_count == 0 && body_count == 0;
    }
    if memberships_present && let Some(root) = directory_root {
        // The membership and open-directory pages are bound to the committed
        // root by their table-root fingerprints (validated above, O(1)); the
        // per-record envelopes are checked when a route reads them. Only the
        // O(1) cardinalities are compared here.
        if root[..8] != membership_count.to_be_bytes() {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "compact vector directory count disagrees with memberships",
            ));
        }
        let vector_count = match read_txn.open_table(VECTORS_TABLE) {
            Ok(table) => table.len().map_err(read_storage_error)?,
            Err(redb::TableError::TableDoesNotExist(_)) => 0,
            Err(error) => return Err(read_table_error(error)),
        };
        // A body may own several disjoint historical placements. The writer
        // maintains the number of distinct referenced body identities and
        // binds it with the membership set on the committed vector roots.
        let referenced_body_count =
            vector_membership_pages::referenced_body_count_in_read(read_txn)
                .map_err(read_image_error)?;
        if vector_count != referenced_body_count {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "compact vector directory distinct body count disagrees with vector bodies",
            ));
        }

        if !vector_body_root_matches(read_txn)? {
            // A raw write changed the vector tree without changing the
            // writer-owned directory. This rare damage path may inspect body
            // pages: only a healthy cold open has the dimension-independent
            // I/O promise. Payload-only damage remains a selected-body proof;
            // a changed physical key is structural damage at open.
            let vectors = read_txn
                .open_table(VECTORS_TABLE)
                .map_err(read_table_error)?;
            let memberships = read_txn
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(read_table_error)?;
            for item in memberships.iter().map_err(read_storage_error)? {
                let (_, value) = item.map_err(read_storage_error)?;
                let member: VectorPartitionMembershipRecord =
                    decode_vector_partition_record_payload(
                        VectorPartitionRecordKind::Membership,
                        value.value(),
                    )
                    .map_err(|reason| {
                        read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason)
                    })?;
                let key = RedbPersistence::vector_identity_key(
                    &member.index,
                    member.row_id,
                    member.vector_created_tx,
                    member.vector_lsn,
                );
                if vectors
                    .get_bounded(key.as_slice())
                    .map_err(read_storage_error)?
                    .is_none()
                {
                    return Err(direct_read_requires_writer_for(
                        ReadImageRequiresWriterCause::InvalidVectorReference,
                        "the committed directory names a missing vector body key",
                    ));
                }
            }
            let reverse = read_txn
                .open_table(vector_membership_pages::REVERSE)
                .map_err(read_table_error)?;
            vector_identity_range::visit_table(
                &vectors,
                None,
                memory.accountant.clone(),
                &mut || Ok(()),
                &mut |identity| {
                    let prefix = vector_membership_pages::reverse_row_prefix(
                        &identity.index,
                        identity.row_id,
                    );
                    let mut found = false;
                    for item in reverse
                        .prefix_bounded(&prefix)
                        .map_err(RedbPersistence::storage_error)?
                    {
                        let (_, value) = item.map_err(RedbPersistence::storage_error)?;
                        let member: VectorPartitionMembershipRecord =
                            decode_vector_partition_record_payload(
                                VectorPartitionRecordKind::Membership,
                                value.value(),
                            )?;
                        if member.index == identity.index
                            && member.row_id == identity.row_id
                            && member.vector_created_tx == identity.created_tx
                            && member.vector_lsn == identity.lsn
                        {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Err(Error::StoreCorrupted {
                            path: "vector_entries".into(),
                            reason: "a vector body key is absent from the committed directory"
                                .into(),
                        });
                    }
                    Ok(())
                },
            )
            .map_err(|error| match error {
                Error::StoreCorrupted { .. } => direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    "a vector body key is absent from the committed directory",
                ),
                other => read_image_error(other),
            })?;
        }
    }

    // Vector bodies are authenticated when selected through the retained
    // source. Reading their B-tree at open faults payload pages proportional
    // to the declared vector dimension, even though no vector is hydrated.

    Ok(ReadRawVectorDirectory { ready: raw_ready })
}

#[cfg(any(test, feature = "test-seams"))]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_and_collect_current_vectors(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &mut HashMap<String, Vec<VersionedRow>>,
    vector_entries: &[VectorEntry],
    ddl_log: &[(Lsn, DdlChange)],
) -> std::result::Result<HashMap<(VectorIndexRef, RowId), Vec<f32>>, LoadReadImageError> {
    let vector_specs = read_vector_specs_from_meta(table_meta);
    let renames = read_vector_renames_from_ddl_log(ddl_log);
    let mut current = HashMap::new();
    let mut current_order = HashMap::<(VectorIndexRef, RowId), (Lsn, TxId)>::new();
    for entry in vector_entries {
        if i64::try_from(entry.row_id.0).is_err() {
            return Err(invalid_read_image_state(format!(
                "vector {}.{} has out-of-range row id {}",
                entry.index.table, entry.index.column, entry.row_id.0
            )));
        }
        let Some(resolved_index) =
            resolve_read_vector_index(&entry.index, entry.lsn, &vector_specs, &renames)
        else {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} cannot be reconciled with current metadata",
                    entry.index.table, entry.index.column
                ),
            ));
        };
        if resolved_index != entry.index {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} requires writable rename reconciliation to {}.{}",
                    entry.index.table,
                    entry.index.column,
                    resolved_index.table,
                    resolved_index.column
                ),
            ));
        }
        let Some(&dimension) = vector_specs.get(&entry.index) else {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} has no current metadata",
                    entry.index.table, entry.index.column
                ),
            ));
        };
        if entry.vector.len() != dimension {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector {}.{} row {} has dimension {} instead of {dimension}",
                    entry.index.table,
                    entry.index.column,
                    entry.row_id.0,
                    entry.vector.len()
                ),
            ));
        }
        let rows = relational_tables.get(&entry.index.table).ok_or_else(|| {
            direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index references unavailable table {}",
                    entry.index.table
                ),
            )
        })?;
        let matching_row = rows.iter().find(|row| {
            row.row_id == entry.row_id && row.created_tx == entry.created_tx && row.lsn == entry.lsn
        });
        if !rows.iter().any(|row| row.row_id == entry.row_id) {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector {}.{} row {} has no relational owner",
                    entry.index.table, entry.index.column, entry.row_id.0
                ),
            ));
        }
        let column = table_meta
            .get(&entry.index.table)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable table {}",
                        entry.index.table
                    ),
                )
            })?
            .columns
            .iter()
            .find(|column| column.name == entry.index.column)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable column {}.{}",
                        entry.index.table, entry.index.column
                    ),
                )
            })?;
        // Compaction can legitimately retire the exact relational version
        // that first owned a still-live vector. Compare exact occurrences,
        // or a live body-only successor for a live vector; a historical
        // deleted vector without its compacted row version has no row value
        // left to compare.
        let comparison_row = matching_row.or_else(|| {
            if entry.deleted_tx.is_none() {
                rows.iter()
                    .filter(|row| row.row_id == entry.row_id && row.deleted_tx.is_none())
                    .max_by_key(|row| (row.lsn, row.created_tx))
            } else {
                None
            }
        });
        if let Some(row) = comparison_row {
            let row_value = row.values.get(&entry.index.column);
            let row_matches_vector = match column.quantization {
                // An inline value must be the stored one. An empty nullable slot
                // keeps no copy; a required NULL stays damage.
                VectorQuantization::F32 => {
                    matches!(row_value, Some(Value::Vector(row_vector)) if row_vector == &entry.vector)
                        || (column.nullable && matches!(row_value, None | Some(Value::Null)))
                }
                // A quantized column keeps no copy in the row: the store's
                // entry -- the one being checked here -- IS the value. So an
                // empty slot and an absent slot are both what a healthy row
                // looks like, and only a row carrying a DIFFERENT vector has
                // diverged from the store.
                VectorQuantization::SQ4 | VectorQuantization::SQ8 => {
                    !matches!(row_value, Some(Value::Vector(row_vector)) if row_vector != &entry.vector)
                }
            };
            if !row_matches_vector {
                return Err(direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::RowVectorDivergence,
                    format!(
                        "row and vector state diverge for {}.{} row {}",
                        entry.index.table, entry.index.column, entry.row_id.0
                    ),
                ));
            }
        }
        if entry.deleted_tx.is_some()
            || !rows
                .iter()
                .any(|row| row.row_id == entry.row_id && row.deleted_tx.is_none())
        {
            continue;
        }
        let key = (entry.index.clone(), entry.row_id);
        let order = (entry.lsn, entry.created_tx);
        if current_order
            .get(&key)
            .is_none_or(|existing| order > *existing)
        {
            current_order.insert(key.clone(), order);
            current.insert(key, entry.vector.clone());
        }
    }

    ensure_no_vector_supplement_needed(table_meta, relational_tables, vector_entries)?;
    ensure_required_quantized_vectors_are_present(table_meta, relational_tables, &current)?;

    // A quantized column's value is NOT reconstructed into the row. The row
    // keeps an empty slot, here as on the writer's own reopen, and a reader
    // that names the column takes the value from the vector entries this
    // image carries. Writing it in would give the image a full-precision copy
    // per row that no read asked for. The loop still runs, because it is also
    // what proves every entry names a column this image really has.
    for entry in vector_entries {
        let column = table_meta
            .get(&entry.index.table)
            .and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| column.name == entry.index.column)
            })
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable column {}.{}",
                        entry.index.table, entry.index.column
                    ),
                )
            })?;
        if matches!(column.quantization, VectorQuantization::F32) {
            continue;
        }
        let rows = relational_tables
            .get_mut(&entry.index.table)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable table {}",
                        entry.index.table
                    ),
                )
            })?;
        let _ = rows;
    }
    for (table, meta) in table_meta {
        if let Some(rows) = relational_tables.get_mut(table) {
            for row in rows {
                remove_quantized_placeholders(row, meta);
            }
        }
    }
    Ok(current)
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn read_vector_specs_from_meta(
    table_meta: &HashMap<String, TableMeta>,
) -> HashMap<VectorIndexRef, usize> {
    let mut specs = HashMap::new();
    for (table, meta) in table_meta {
        for column in &meta.columns {
            if let ColumnType::Vector(dimension) = column.column_type {
                specs.insert(
                    VectorIndexRef::new(table.clone(), column.name.clone()),
                    dimension,
                );
            }
        }
    }
    specs
}

pub(super) fn read_vector_rename_constraint(constraints: &[String]) -> Option<(String, String)> {
    const PREFIX: &str = "VECTOR_RENAME(";
    for constraint in constraints {
        let trimmed = constraint.trim();
        if !trimmed.to_ascii_uppercase().starts_with(PREFIX) || !trimmed.ends_with(')') {
            continue;
        }
        let body = &trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)];
        let (from, to) = body.split_once(',')?;
        let from = from
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_owned();
        let to = to
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_owned();
        if !from.is_empty() && !to.is_empty() {
            return Some((from, to));
        }
    }
    None
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn read_vector_renames_from_ddl_log(
    ddl_log: &[(Lsn, DdlChange)],
) -> Vec<ReadVectorRename> {
    let mut renames = Vec::new();
    for (lsn, change) in ddl_log {
        let DdlChange::AlterTable {
            name, constraints, ..
        } = change
        else {
            continue;
        };
        if let Some((from, to)) = read_vector_rename_constraint(constraints) {
            renames.push(ReadVectorRename {
                lsn: *lsn,
                from: VectorIndexRef::new(name, from),
                to: VectorIndexRef::new(name, to),
            });
        }
    }
    renames.sort_by(|left, right| {
        left.lsn
            .cmp(&right.lsn)
            .then(left.from.table.cmp(&right.from.table))
            .then(left.from.column.cmp(&right.from.column))
            .then(left.to.table.cmp(&right.to.table))
            .then(left.to.column.cmp(&right.to.column))
    });
    renames
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn resolve_read_vector_index(
    index: &VectorIndexRef,
    entry_lsn: Lsn,
    vector_specs: &HashMap<VectorIndexRef, usize>,
    renames: &[ReadVectorRename],
) -> Option<VectorIndexRef> {
    let mut current = index.clone();
    let mut seen = HashSet::new();
    for _ in 0..=renames.len() {
        if !seen.insert(current.clone()) {
            return None;
        }
        if let Some(rename) = renames
            .iter()
            .filter(|rename| rename.from == current && entry_lsn <= rename.lsn)
            .min_by_key(|rename| rename.lsn)
        {
            current = rename.to.clone();
            continue;
        }
        return vector_specs.contains_key(&current).then_some(current);
    }
    None
}

#[cfg(any(test, feature = "test-seams"))]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn ensure_no_vector_supplement_needed(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    vector_entries: &[VectorEntry],
) -> std::result::Result<(), LoadReadImageError> {
    let seen_occurrences = vector_entries
        .iter()
        .map(|entry| {
            (
                entry.index.clone(),
                entry.row_id,
                entry.created_tx,
                entry.lsn,
            )
        })
        .collect::<HashSet<_>>();
    let known_owners = vector_entries
        .iter()
        .map(|entry| (entry.index.clone(), entry.row_id))
        .collect::<HashSet<_>>();
    for (table, meta) in table_meta {
        let Some(rows) = relational_tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                let Some(Value::Vector(vector)) = row.values.get(&column.name) else {
                    continue;
                };
                if vector.len() != dimension {
                    continue;
                }
                if row.deleted_tx.is_none() && known_owners.contains(&(index.clone(), row.row_id)) {
                    continue;
                }
                if !seen_occurrences.contains(&(index.clone(), row.row_id, row.created_tx, row.lsn))
                {
                    return Err(read_image_corruption(
                        ReadImageCorruptionCause::MalformedRecord,
                        format!(
                            "row {} in {}.{} has no authoritative vector body",
                            row.row_id.0, table, column.name
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-seams"))]
/// A quantized vector column keeps its embedding outside the relational row:
/// the cell holds an empty placeholder and the reader serves the value from
/// the separately durable vector entry the row owns. Where such a column is
/// declared `NOT NULL`, the schema promises every live row an embedding, so
/// the placeholder is only legal while that entry is still there. Proving it
/// here is what keeps `NOT NULL` meaning the same thing on a quantized column
/// as on a full-precision one: a file that has lost the entry can no longer
/// keep the promise it records, and an operator reading it directly is told a
/// writer is the next step instead of being handed a row whose required column
/// has quietly vanished.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn ensure_required_quantized_vectors_are_present(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    current_vectors: &HashMap<(VectorIndexRef, RowId), Vec<f32>>,
) -> std::result::Result<(), LoadReadImageError> {
    for (table, meta) in table_meta {
        let Some(rows) = relational_tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            if !matches!(column.column_type, ColumnType::Vector(_))
                || column.nullable
                || matches!(column.quantization, VectorQuantization::F32)
            {
                continue;
            }
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                // A deleted row version is history; only what a read would
                // serve has to keep the declaration.
                if row.deleted_tx.is_some() {
                    continue;
                }
                if !matches!(row.values.get(&column.name), Some(Value::Null)) {
                    continue;
                }
                if current_vectors.contains_key(&(index.clone(), row.row_id)) {
                    continue;
                }
                return Err(read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    format!(
                        "row {} in {}.{} has no authoritative vector body for a required column",
                        row.row_id.0, table, column.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_sync_source_lsns(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<(String, RowId), Lsn>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = match read_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut values = HashMap::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(key) = RedbPersistence::decode_sync_row_source_lsn_key(key.value()) else {
            return Err(invalid_read_image_state(
                "sync provenance has an invalid row key",
            ));
        };
        values.insert(key, Lsn(value.value()));
    }
    Ok(values)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_sync_source_kinds(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<(String, RowId), u8>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = match read_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut values = HashMap::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(key) = RedbPersistence::decode_sync_row_source_lsn_key(key.value()) else {
            return Err(invalid_read_image_state(
                "sync provenance kind has an invalid row key",
            ));
        };
        values.insert(key, value.value());
    }
    Ok(values)
}

/// Open a table that may not exist yet in this snapshot.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn open_optional_read_table<K: redb::Key + 'static, V: redb::Value + 'static>(
    read: &redb::ReadTransaction,
    definition: TableDefinition<K, V>,
) -> Result<Option<redb::ReadOnlyTable<K, V>>> {
    match read.open_table(definition) {
        Ok(table) => Ok(Some(table)),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(error) => Err(RedbPersistence::storage_error(error)),
    }
}

pub(super) fn note_open_history(class: OpenHistoryClass, bytes: usize) {
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::note_open_history(class, bytes);
    #[cfg(not(any(test, feature = "test-seams")))]
    let _ = (class, bytes);
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn diagnose_commit_index_past_committed_frontier(
    read_txn: &redb::ReadTransaction,
    committed_last: Option<(Lsn, TxId)>,
    context: &contextdb_tx::ReadCommitContext<'_>,
) -> std::result::Result<(), LoadReadImageError> {
    context.checkpoint().map_err(read_image_error)?;
    let _controls = contextdb_core::read_memory::ReadCredit::try_new(
        context.read_memory.clone(),
        std::mem::size_of::<redb::ReadOnlyTable<u64, u64>>()
            + std::mem::size_of::<redb::Range<'_, u64, u64>>(),
        "reader",
        "commit_repair_probe",
        "Raise the declared reader memory limit.",
    )
    .map_err(LoadReadImageError::Memory)?;
    let table = match read_txn.open_table(COMMIT_INDEX_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(read_table_error(error)),
    };
    let start = match committed_last {
        Some((lsn, _)) => match lsn.0.checked_add(1) {
            Some(start) => start,
            None => return Ok(()),
        },
        None => 0,
    };
    let mut previous = committed_last.map_or(TxId(0), |(_, tx)| tx);
    let mut uncommitted = 0u64;
    let mut lower = std::ops::Bound::Included(start);
    loop {
        context.checkpoint().map_err(read_image_error)?;
        let pair = {
            let mut range = table
                .range((lower, std::ops::Bound::Unbounded))
                .map_err(read_storage_error)?;
            range
                .next()
                .transpose()
                .map_err(read_storage_error)?
                .map(|(key, tx)| (key.value(), tx.value()))
        };
        let Some((lsn, tx)) = pair else {
            break;
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "commit_index",
            "iteration_step",
            &lsn.to_be_bytes(),
            Some(Lsn(lsn)),
            None,
        );
        note_open_history(OpenHistoryClass::CommitIndexScanned, 0);
        uncommitted = uncommitted.checked_add(1).ok_or_else(|| {
            read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "uncommitted commit-index count overflow",
            )
        })?;
        let tx = TxId(tx);
        if tx < previous {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::NonMonotonicCommitIndex,
                "commit-index transaction order needs writable repair",
            ));
        }
        previous = tx;
        lower = std::ops::Bound::Excluded(lsn);
    }
    if uncommitted > 0 {
        return Err(direct_read_requires_writer_for(
            ReadImageRequiresWriterCause::WritableSanitizerWouldChange,
            format!(
                "writable recovery must truncate {uncommitted} uncommitted commit-index \
                 entries past {committed_last:?}"
            ),
        ));
    }
    Ok(())
}

/// Read the last committed commit-index entry and the last committed
/// schema-change position from authenticated fixed controls, without reading
/// history proof bodies. Require the physical endpoint facts used by open to
/// agree; ordinary operations separately check directory agreement and paths.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn read_authenticated_history_frontiers(
    read_txn: &redb::ReadTransaction,
    _path: &Path,
    _memory: &crate::read_image_memory::ImageMemoryHandle,
    context: &contextdb_tx::ReadCommitContext<'_>,
) -> std::result::Result<AuthenticatedHistoryFrontiers, LoadReadImageError> {
    read_authenticated_history_frontiers_with_bounds(read_txn, context, None)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn read_authenticated_history_frontiers_with_bounds(
    read_txn: &redb::ReadTransaction,
    context: &contextdb_tx::ReadCommitContext<'_>,
    authenticated: Option<&writer_recovery::AuthenticatedReadBounds<'_>>,
) -> std::result::Result<AuthenticatedHistoryFrontiers, LoadReadImageError> {
    context.checkpoint().map_err(read_image_error)?;
    let _credit = contextdb_core::read_memory::ReadCredit::try_new(
        context.read_memory.clone(),
        content_identity::history_open_control::OPEN_CONTROL_SCRATCH_BYTES,
        "reader",
        "history_open_controls",
        "Raise the declared reader memory limit.",
    )
    .map_err(LoadReadImageError::Memory)?;
    let standalone;
    let authenticated = match authenticated {
        Some(proof) if std::ptr::eq(proof.read(), read_txn) => proof,
        _ => {
            standalone =
                RedbPersistence::authenticate_read_bounds(read_txn).map_err(read_image_error)?;
            &standalone
        }
    };
    let control = content_identity::history_open_control::acquire_authenticated(
        authenticated,
        authenticated.bounds().commit_count,
    )
    .map_err(read_image_error)?;
    let fixed = control.control();
    let corrupt =
        |reason: &str| read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason);
    let commits = read_txn
        .open_table(COMMIT_INDEX_TABLE)
        .map_err(read_table_error)?;
    let first = commits
        .first()
        .map_err(read_storage_error)?
        .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
    let last = match fixed.last {
        None => None,
        Some((lsn, expected)) => {
            let tx = commits
                .get(lsn.0)
                .map_err(read_storage_error)?
                .map(|tx| TxId(tx.value()))
                .ok_or_else(|| corrupt("the last committed commit-index entry is missing"))?;
            if content_identity::commit_index_value_hash(tx) != expected {
                return Err(corrupt(
                    "the last commit-index entry names another transaction",
                ));
            }
            Some((lsn, tx))
        }
    };
    // With no committed frontier every physical entry is a repair suffix.
    // Leave its refusal to suffix diagnosis before directory disagreement.
    if fixed.first.is_some() && first.map(|pair| pair.0) != fixed.first.map(|pair| pair.0) {
        return Err(corrupt(
            "physical first commit coordinate differs from control",
        ));
    }
    let ddl = read_txn
        .open_table(DDL_LOG_TABLE)
        .map_err(read_table_error)?;
    match (fixed.ddl, ddl.last().map_err(read_storage_error)?) {
        (None, None) => {}
        (Some((len, lsn, expected, _)), Some((key, _))) => {
            let key = key.value();
            if u64::try_from(key.len()).ok() != Some(len)
                || content_identity::history_open_control::ddl_coordinate_digest(
                    key.as_bytes(),
                    Some(&|| context.checkpoint()),
                )
                .map_err(read_image_error)?
                    != expected
                || RedbPersistence::ddl_lsn_from_key(key).map_err(read_image_error)? != lsn
            {
                return Err(corrupt(
                    "stored schema history final coordinate differs from control",
                ));
            }
        }
        _ => {
            return Err(corrupt(
                "stored schema history final coordinate differs from control",
            ));
        }
    }
    context.checkpoint().map_err(read_image_error)?;
    Ok(AuthenticatedHistoryFrontiers {
        control,
        commit_last: last,
        ddl_last_lsn: fixed.ddl.map(|(_, lsn, _, _)| lsn),
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_raw_string_table(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<&str, &[u8]>,
    history: Option<OpenHistoryClass>,
) -> std::result::Result<Vec<(String, Vec<u8>)>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        // Every copy the image keeps is admitted before it is made.
        let copied = key.value().len() + value.value().len();
        if let Some(class) = history {
            note_open_history(class, copied);
        }
        crate::read_image_memory::reserve(copied).map_err(read_image_error)?;
        entries.push((key.value().to_owned(), value.value().to_vec()));
    }
    Ok(entries)
}

/// Refuse the next lineage-record authentication-copy admission, exactly as
/// an exhausted reader would, and admit every other reservation normally.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn refuse_next_lineage_scratch_admission_for_test() {
    LINEAGE_SCRATCH_REFUSAL.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// Per-row lineage keys: one per row life that ever carried sync lineage.
pub(crate) fn read_image_lineage_config_key(key: &str) -> bool {
    key.starts_with(super::row_graph::RETIRED_PREFIX)
        || key.starts_with(crate::database::Database::DURABLE_LINEAGE_CONFIG_PREFIX)
        || key.starts_with(crate::database::Database::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX)
        || key.starts_with("sync_lineage.v1.pending.")
}

/// These keys have already been point-read from the pinned image, including
/// their absence. A later startup lookup need not revisit the config B-tree.
pub(crate) fn read_image_open_config_key(key: &str) -> bool {
    READ_IMAGE_OPEN_CONFIG_KEYS.contains(&key)
        || READ_IMAGE_OPEN_CONFIG_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_open_config(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<(String, Vec<u8>)>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = read_txn
        .open_table(READ_OPEN_CONFIG_TABLE)
        .map_err(read_table_error)?;
    let root = table
        .get(READ_OPEN_CONFIG_ROOT_KEY)
        .map_err(read_storage_error)?
        .ok_or_else(|| {
            read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "read-open config index root is absent",
            )
        })?;
    let root = root.value().to_vec();
    if root.len() != 40 {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            "read-open config index root has invalid size",
        ));
    }
    verify_open_index_commitment(read_txn, READ_OPEN_CONFIG_COMMITMENT_KEY, &root)?;
    let mut entries = Vec::new();
    let mut count = 0_u64;
    let mut xor = [0_u8; 32];
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        if key.value() == READ_OPEN_CONFIG_ROOT_KEY {
            continue;
        }
        if !read_image_open_config_key(key.value()) {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "read-open config index holds an unrelated key",
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(key.value().as_bytes());
        hasher.update(value.value());
        for (dst, src) in xor.iter_mut().zip(hasher.finalize().as_bytes()) {
            *dst ^= src;
        }
        count = count.saturating_add(1);
        crate::read_image_memory::reserve(key.value().len() + value.value().len())
            .map_err(read_image_error)?;
        entries.push((key.value().to_owned(), value.value().to_vec()));
    }
    if root[..8] != count.to_be_bytes() || root[8..] != xor {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            "read-open config index aggregate disagrees with its values",
        ));
    }
    Ok(entries)
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from reader_registration.rs"
    )
)]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn decode_read_trigger_audit_stamps(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<(String, u64)>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let table = match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        note_open_history(
            OpenHistoryClass::Audit,
            key.value().len() + std::mem::size_of::<u64>(),
        );
        entries.push((key.value().to_owned(), value.value()));
    }
    Ok(entries)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn read_sink_queue_names(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<String>, LoadReadImageError> {
    let mut names = read_txn
        .list_tables()
        .map_err(read_storage_error)?
        .filter_map(|handle| {
            handle
                .name()
                .strip_prefix("__sink_queue_")
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
}

/// Each durable sink queue's depth and newest entry id, read from the
/// table's root and its last key. No payload is read or copied.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_read_sink_queue_extents(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<BTreeMap<String, SinkQueueExtent>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let mut extents = BTreeMap::new();
    for name in read_sink_queue_names(read_txn)? {
        let table_name = RedbPersistence::sink_queue_table_name(&name);
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
        let table = read_txn.open_table(definition).map_err(read_table_error)?;
        let depth = table.len().map_err(read_storage_error)?;
        let newest_id = table
            .last()
            .map_err(read_storage_error)?
            .map(|(key, _)| key.value())
            .unwrap_or(0);
        extents.insert(name, SinkQueueExtent { depth, newest_id });
    }
    Ok(extents)
}

#[cfg(any(test, feature = "test-seams"))]
#[allow(clippy::type_complexity)]
#[cfg_attr(
    not(feature = "test-seams"),
    allow(
        dead_code,
        reason = "KEEP-SEAM: live only under the test-seams feature; used from reader_registration.rs"
    )
)]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn decode_read_sink_queues(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<BTreeMap<String, Vec<(u64, Vec<u8>)>>, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    let mut queues = BTreeMap::new();
    for name in read_sink_queue_names(read_txn)? {
        let table_name = RedbPersistence::sink_queue_table_name(&name);
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
        let table = read_txn.open_table(definition).map_err(read_table_error)?;
        let mut entries = Vec::new();
        for entry in table.iter().map_err(read_storage_error)? {
            let (key, value) = entry.map_err(read_storage_error)?;
            note_open_history(
                OpenHistoryClass::Queued,
                std::mem::size_of::<u64>() + value.value().len(),
            );
            entries.push((key.value(), value.value().to_vec()));
        }
        queues.insert(name, entries);
    }
    Ok(queues)
}

/// The integer key a caller names a row by: the value in its declared
/// single-column integer primary key. The internal `RowId` is an allocator
/// counter, so projecting it would answer a lookup for row `42` with whatever
/// row happened to be inserted forty-second.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn read_row_relational_identifier(
    table: &str,
    row_id: RowId,
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
) -> Option<i64> {
    let meta = table_meta.get(table)?;
    if meta.primary_key_columns.len() > 1 {
        return None;
    }
    let key_column = meta.primary_key_columns.first().cloned().or_else(|| {
        let mut declared = meta.columns.iter().filter(|column| column.primary_key);
        let column = declared.next()?;
        declared.next().is_none().then(|| column.name.clone())
    })?;
    let row = relational_tables
        .get(table)?
        .iter()
        .filter(|row| row.row_id == row_id && row.deleted_tx.is_none())
        .max_by_key(|row| (row.lsn, row.created_tx))?;
    match row.values.get(&key_column) {
        Some(Value::Int64(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
pub(super) fn compatibility_read_vector_projection(
    current_vectors: &HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
) -> std::result::Result<BTreeMap<(String, i64), Vec<f32>>, LoadReadImageError> {
    let mut entries = current_vectors.iter().collect::<Vec<_>>();
    entries.sort_by(
        |((left_index, left_row), _), ((right_index, right_row), _)| {
            (&left_index.table, &left_index.column, left_row).cmp(&(
                &right_index.table,
                &right_index.column,
                right_row,
            ))
        },
    );
    let mut vectors = BTreeMap::new();
    for ((index, row_id), vector) in entries {
        let identifier = match read_row_relational_identifier(
            &index.table,
            *row_id,
            table_meta,
            relational_tables,
        ) {
            Some(identifier) => identifier,
            None => i64::try_from(row_id.0).map_err(|_| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector row id {} for {}.{} is outside the relational identifier range",
                        row_id.0, index.table, index.column
                    ),
                )
            })?,
        };
        vectors.insert((index.table.clone(), identifier), vector.clone());
    }
    Ok(vectors)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_complete_read_image(
    read_txn: &redb::ReadTransaction,
    memory: crate::read_image_memory::ImageMemoryHandle,
    open_context: crate::executor::bounded::ReadOpenContext,
) -> std::result::Result<ReadPersistenceImage, LoadReadImageError> {
    let _decode_scope =
        crate::read_image_memory::ImageMemory::enter_decode_call().map_err(read_image_error)?;
    // What each step of the open holds, for the attribution sink only.
    let open_started = std::time::Instant::now();
    let note_open = |step: &str| {
        if !contextdb_core::find_attribution::enabled() {
            return;
        }
        use contextdb_core::find_attribution::Field;
        let usage = memory.accountant.usage();
        contextdb_core::find_attribution::note_event(
            "reader_open_step",
            vec![
                ("step".to_string(), Field::Text(step.to_string())),
                (
                    "accountant_used_bytes".to_string(),
                    Field::Count(usage.used as u64),
                ),
                (
                    "image_held_bytes".to_string(),
                    Field::Count(memory.held() as u64),
                ),
                (
                    "ms".to_string(),
                    Field::Millis(open_started.elapsed().as_secs_f64() * 1e3),
                ),
            ],
        );
    };
    note_open("start");
    let format_table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => {
            return Err(legacy_read_layout("format metadata is absent"));
        }
        Err(error) => return Err(read_table_error(error)),
    };
    let format = format_table
        .get(FORMAT_VERSION_KEY)
        .map_err(read_storage_error)?
        .ok_or_else(|| legacy_read_layout("format marker is absent"))?;
    let format: String = RedbPersistence::decode(format.value()).map_err(read_image_error)?;
    if format != CURRENT_FORMAT_VERSION {
        return Err(legacy_read_layout(format!(
            "format marker {format:?} is not current"
        )));
    }
    drop(format_table);
    RedbPersistence::check_history_witness_schema(read_txn).map_err(read_image_error)?;

    match read_txn.open_table(CONTENT_IDENTITY_METADATA_TABLE) {
        Ok(table) => drop(table),
        Err(redb::TableError::TableDoesNotExist(_)) => {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                "logical content identity metadata is absent",
            ));
        }
        Err(error) => return Err(read_table_error(error)),
    }
    let content_identity =
        RedbPersistence::load_content_identity_from_read(read_txn).map_err(read_image_error)?;
    note_open("content_identity");

    memory
        .reserve(std::mem::size_of::<WriterRecoveryBounds>())
        .map_err(read_image_error)?;
    // The bounds remain charged as retained image state. The borrowed capture
    // pointer is fixed open scratch and releases on every success/error exit.
    let _bounds_capture_credit = contextdb_core::read_memory::ReadCredit::try_new(
        open_context.read_memory(),
        std::mem::size_of::<writer_recovery::AuthenticatedReadBounds<'_>>()
            - std::mem::size_of::<WriterRecoveryBounds>(),
        "reader",
        "authenticated_bounds_capture",
        "Raise the declared reader memory limit.",
    )
    .map_err(LoadReadImageError::Memory)?;
    let authenticated_bounds =
        RedbPersistence::authenticate_read_bounds(read_txn).map_err(read_image_error)?;
    let read_bounds = authenticated_bounds.bounds();
    let table_meta = decode_read_table_meta(read_txn)?;
    // Both image loading paths consume the authenticated schema facts at
    // open. Bodies are compared with these same values when admitted below
    // or on demand; no schema-bound map is retained beside the source.
    for table in table_meta.keys() {
        let maximum = RedbPersistence::table_read_lsn_in_read(read_txn, table)?;
        if maximum.0 > read_bounds.source_lsn_floor {
            return Err(read_image_corruption(
                ReadImageCorruptionCause::MalformedRecord,
                format!("{table} maximum exceeds the committed cross-subsystem bound"),
            ));
        }
    }
    note_open("table_meta");
    let raw_vector_directory = decode_read_raw_vector_directory(read_txn, &memory)?;
    note_open("vector_catalog_and_raw_vector_directory");
    // Every store this build wrote declares relational readiness and, unless
    // its vector store is empty, a membership directory. Malformed readiness
    // is corruption and wins over absence; absence is refused here, before
    // any row or vector is decoded.
    RedbPersistence::relational_indexes_ready_in_read(read_txn, &table_meta)?;
    if !raw_vector_directory.ready {
        return Err(direct_read_requires_writer_for(
            ReadImageRequiresWriterCause::ReadinessAbsent,
            "the vector store holds an indexed vector or a vector body but has no membership directory",
        ));
    }
    note_open("readiness");
    let persisted_sync_lsns = decode_read_sync_source_lsns(read_txn)?;
    let persisted_sync_kinds = decode_read_sync_source_kinds(read_txn)?;
    note_open("sync_sources");
    let mut sync_source_kinds = persisted_sync_kinds
        .into_iter()
        .filter(|(key, _)| persisted_sync_lsns.contains_key(key))
        .map(|(key, kind)| {
            let kind = match kind {
                1 => SyncSourceKind::AcceptedLocal,
                2 => SyncSourceKind::AcceptedLocalPending,
                _ => SyncSourceKind::Pulled,
            };
            (key, kind)
        })
        .collect::<HashMap<_, _>>();
    for key in persisted_sync_lsns.keys() {
        sync_source_kinds
            .entry(key.clone())
            .or_insert(SyncSourceKind::Pulled);
    }
    let sync_source_lsns = persisted_sync_lsns;
    // The history frontiers capture the scalar directory descriptor.
    // A detected repair suffix is diagnosed before root/count disagreement.
    let open_checkpoint = || open_context.checkpoint();
    let directory_context =
        contextdb_tx::ReadCommitContext::new(open_context.read_memory(), &open_checkpoint);
    let frontiers = read_authenticated_history_frontiers_with_bounds(
        read_txn,
        &directory_context,
        Some(&authenticated_bounds),
    )?;
    diagnose_commit_index_past_committed_frontier(
        read_txn,
        frontiers.commit_last,
        &directory_context,
    )?;
    let fixed = frontiers.control.control();
    let count = usize::try_from(fixed.commit_count).map_err(|_| {
        read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            "commit directory cardinality exceeds usize",
        )
    })?;
    let commits = read_txn
        .open_table(COMMIT_INDEX_TABLE)
        .map_err(read_table_error)?;
    let first = commits
        .first()
        .map_err(read_storage_error)?
        .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
    let last = commits
        .last()
        .map_err(read_storage_error)?
        .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
    if last != frontiers.commit_last {
        return Err(read_image_corruption(
            ReadImageCorruptionCause::MalformedRecord,
            "commit directory endpoint differs from authenticated history",
        ));
    }
    note_open("commit_index");
    let commit_index = BTreeMap::new();
    let history_on_request = Some(ReadHistoryOnRequest {
        open_control: Some(frontiers.control),
        commit_index_last: last,
        commit_index_first: first,
        commit_index_count: count,
        ddl_last_lsn: frontiers.ddl_last_lsn,
    });
    let config_values = decode_read_open_config(read_txn)?;
    note_open("config");
    // Audit records and queued payloads are history no query asks for at
    // open. Sink audit and trigger-audit stamps have no reader at all, and
    // the trigger-audit ring a reader reports lives in the configuration;
    // the full trigger-audit table is only its fallback when a store carries
    // no ring. Queues contribute their depth and newest id, never payloads.
    let trigger_audit_ring_present = [
        TRIGGER_AUDIT_RING_CONFIG_KEY,
        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
    ]
    .iter()
    .all(|key| config_values.iter().any(|(stored, _)| stored == key));
    let trigger_audit = if trigger_audit_ring_present {
        Vec::new()
    } else {
        decode_read_raw_string_table(read_txn, TRIGGER_AUDIT_TABLE, Some(OpenHistoryClass::Audit))?
    };
    note_open("audits");
    let sink_queue_extents = decode_read_sink_queue_extents(read_txn)?;
    note_open("sink_queues");
    Ok(ReadPersistenceImage {
        open_context,
        content_identity,
        memory,
        table_meta,
        #[cfg(any(test, feature = "test-seams"))]
        relational_tables: HashMap::new(),
        read_bounds: authenticated_bounds.into_bounds(),
        vector_entries: Vec::new(),
        raw_vector_memberships: Vec::new(),
        current_vectors: HashMap::new(),
        sync_source_lsns,
        sync_source_kinds,
        change_log: Vec::new(),
        ddl_log: Vec::new(),
        commit_index,
        history_on_request,
        config_values,
        sink_audit: Vec::new(),
        trigger_audit,
        trigger_audit_stamps: Vec::new(),
        sink_queues: BTreeMap::new(),
        sink_queue_extents,
        source_path: None,
        // Filled in by the source that has the file open; the decoder itself
        // is handed a read transaction and never sees a path.
        store_file_bytes: None,
    })
}

/// The path of a store's companion lock file.
///
/// Delegates to `contextdb_core::store_companion_path`, the one source of
/// truth for this name: vigil links contextdb-core unconditionally but
/// contextdb-engine only behind its `fabric` feature, so the naming rule
/// must live where every consumer can reach it. This function stays public
/// here purely so existing engine callers keep working; see
/// `contextdb_core::store_companion_path` for the append contract and the
/// "Stable companion lock" authority reference.
pub fn store_companion_path(store_path: &Path) -> PathBuf {
    contextdb_core::store_companion_path(store_path)
}

pub(super) fn companion_path(path: &Path) -> PathBuf {
    store_companion_path(path)
}

/// A writer refusal that names the store it is about and, when the holder has
/// published its companion record, the exact process holding it. The record is
/// read without the lock the holder owns, which is safe because every
/// generation carries its own checksum: an unpublished or torn read leaves the
/// refusal naming only the store rather than inventing a process number.
pub(super) fn held_by_writer_error(database_path: &Path, companion: &mut File) -> Error {
    let process_id = read_companion_state(companion)
        .ok()
        .and_then(|state| state.active_record)
        .map(|record| u64::from(record.fields.process_id));
    Error::ReadFailure(
        ReadFailure::new(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                process_id,
                store_path: database_path.display().to_string(),
            }),
        )
        .expect("held-by-writer carries the writer-contention detail"),
    )
}

/// Whether this pathname still names the exact companion file the caller is
/// holding open. A removal is bound to the inode it was promised, never to a
/// name somebody else may have since put something else at.
#[cfg(unix)]
pub(super) fn companion_still_names(companion: &Path, held: &File) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    let Ok(open) = held.metadata() else {
        return false;
    };
    let Ok(named) = std::fs::symlink_metadata(companion) else {
        return false;
    };
    named.file_type().is_file() && open.dev() == named.dev() && open.ino() == named.ino()
}

#[cfg(not(unix))]
pub(super) fn companion_still_names(companion: &Path, _held: &File) -> bool {
    companion.is_file()
}

pub(super) fn companion_slot_offset(slot: usize) -> std::result::Result<usize, String> {
    if slot >= 2 {
        return Err(format!("companion slot {slot} is out of range"));
    }
    Ok(COMPANION_FIRST_SLOT_OFFSET + slot * COMPANION_SLOT_BYTES)
}

pub(super) fn companion_selector_bytes(
    active_slot: usize,
    generation: u64,
) -> std::result::Result<[u8; COMPANION_SELECTOR_BYTES], String> {
    if active_slot >= 2 {
        return Err(format!(
            "companion active slot {active_slot} is out of range"
        ));
    }
    let mut selector = [0_u8; COMPANION_SELECTOR_BYTES];
    selector[..COMPANION_SELECTOR_MAGIC.len()].copy_from_slice(COMPANION_SELECTOR_MAGIC);
    selector[8] = active_slot as u8;
    selector[16..24].copy_from_slice(&generation.to_le_bytes());
    let checksum = blake3::hash(&selector[..COMPANION_SELECTOR_BODY_BYTES]);
    selector[COMPANION_SELECTOR_BODY_BYTES..].copy_from_slice(checksum.as_bytes());
    Ok(selector)
}

pub(super) fn initialize_companion_inode(
    file: &mut File,
) -> std::result::Result<CompanionState, String> {
    let selector = companion_selector_bytes(0, 0)?;
    // Every intermediate is recognizable as an incomplete generation-zero
    // initialization. No write can turn a prior complete record into this
    // state because this function is called only when no valid slot exists.
    file.set_len(0)
        .map_err(|error| format!("clear companion inode: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync cleared companion inode: {error}"))?;
    file.set_len(COMPANION_FILE_BYTES as u64)
        .map_err(|error| format!("size companion inode: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync sized companion inode: {error}"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("seek companion inode: {error}"))?;
    file.write_all(COMPANION_FILE_MAGIC)
        .map_err(|error| format!("write companion magic: {error}"))?;
    file.sync_data()
        .map_err(|error| format!("sync companion magic: {error}"))?;
    file.seek(SeekFrom::Start(COMPANION_SELECTOR_OFFSET as u64))
        .map_err(|error| format!("seek companion selector: {error}"))?;
    file.write_all(&selector)
        .map_err(|error| format!("write initial companion selector: {error}"))?;
    file.sync_data()
        .map_err(|error| format!("sync initial companion selector: {error}"))?;
    Ok(CompanionState {
        needs_initialization: false,
        active_slot: None,
        active_record: None,
        pending_slot: None,
        pending_replacement: None,
        pending_matches_store: None,
    })
}

pub(super) fn encode_pending_replacement(
    store_fingerprint: [u8; 32],
    intended_record: EncodedCompanionRecord,
) -> std::result::Result<PendingReplacement, String> {
    let intended_length = u32::try_from(intended_record.checksummed_payload.len())
        .map_err(|_| "pending replacement record is too large".to_owned())?;
    let mut payload = Vec::with_capacity(
        COMPANION_PENDING_MAGIC.len() + 32 + 4 + intended_record.checksummed_payload.len() + 32,
    );
    payload.extend_from_slice(COMPANION_PENDING_MAGIC);
    payload.extend_from_slice(&store_fingerprint);
    payload.extend_from_slice(&intended_length.to_le_bytes());
    payload.extend_from_slice(&intended_record.checksummed_payload);
    payload.extend_from_slice(&intended_record.stored_checksum);
    if payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err("pending replacement exceeds its fixed companion slot".to_owned());
    }
    let stored_checksum = *blake3::hash(&payload).as_bytes();
    Ok(PendingReplacement {
        store_fingerprint,
        intended_record,
        checksummed_payload: payload,
        stored_checksum,
    })
}

pub(super) fn decode_pending_replacement(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<PendingReplacement, String> {
    if *blake3::hash(payload).as_bytes() != stored_checksum {
        return Err("pending replacement checksum mismatch".to_owned());
    }
    let mut cursor = CompanionPayloadCursor { payload, offset: 0 };
    if cursor.take(COMPANION_PENDING_MAGIC.len())? != COMPANION_PENDING_MAGIC {
        return Err("pending replacement magic mismatch".to_owned());
    }
    let store_fingerprint = cursor.array::<32>()?;
    let intended_length = usize::try_from(cursor.u32()?)
        .map_err(|_| "pending replacement length does not fit usize".to_owned())?;
    let intended_payload = cursor.take(intended_length)?.to_vec();
    let intended_checksum = cursor.array::<32>()?;
    cursor.finish()?;
    let intended_record = decode_companion_record(&intended_payload, intended_checksum)?;
    Ok(PendingReplacement {
        store_fingerprint,
        intended_record,
        checksummed_payload: payload.to_vec(),
        stored_checksum,
    })
}

pub(super) fn parse_companion_slot(
    bytes: &[u8],
) -> std::result::Result<ParsedCompanionSlot, String> {
    if bytes.len() != COMPANION_SLOT_BYTES {
        return Err(format!(
            "companion slot has {} bytes, expected {COMPANION_SLOT_BYTES}",
            bytes.len()
        ));
    }
    let length = u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| "companion slot length is truncated".to_owned())?,
    ) as usize;
    let payload = &bytes[4..4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    let checksum: [u8; 32] = bytes[4 + COMPANION_SLOT_PAYLOAD_CAPACITY..]
        .try_into()
        .map_err(|_| "companion slot checksum is truncated".to_owned())?;
    if length == 0 {
        if payload.iter().any(|byte| *byte != 0) || checksum != [0; 32] {
            return Err("vacant companion slot contains record bytes".to_owned());
        }
        return Ok(ParsedCompanionSlot::Vacant);
    }
    if length > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(format!(
            "companion slot payload length {length} exceeds its capacity"
        ));
    }
    if payload[length..].iter().any(|byte| *byte != 0) {
        return Err("companion slot has nonzero padding".to_owned());
    }
    let payload = &payload[..length];
    if payload.starts_with(COMPANION_PENDING_MAGIC) {
        decode_pending_replacement(payload, checksum).map(ParsedCompanionSlot::Pending)
    } else {
        decode_companion_record(payload, checksum).map(ParsedCompanionSlot::Record)
    }
}

pub(super) fn recoverable_generation_zero_initialization(bytes: &[u8]) -> bool {
    if bytes.len() > COMPANION_FILE_BYTES {
        return false;
    }
    let Ok(selector) = companion_selector_bytes(0, 0) else {
        return false;
    };
    let mut intended = vec![0_u8; COMPANION_FILE_BYTES];
    intended[..COMPANION_FILE_MAGIC.len()].copy_from_slice(COMPANION_FILE_MAGIC);
    intended[COMPANION_SELECTOR_OFFSET..].copy_from_slice(&selector);
    bytes
        .iter()
        .zip(intended)
        .all(|(observed, intended)| *observed == 0 || *observed == intended)
}

pub(super) fn parse_companion_selector(bytes: &[u8]) -> Option<(usize, u64)> {
    let selector = bytes.get(COMPANION_SELECTOR_OFFSET..COMPANION_FILE_BYTES)?;
    let body = selector.get(..COMPANION_SELECTOR_BODY_BYTES)?;
    if body.get(..COMPANION_SELECTOR_MAGIC.len())? != COMPANION_SELECTOR_MAGIC {
        return None;
    }
    let active_slot = usize::from(*body.get(8)?);
    if active_slot >= 2 || body.get(9..16)?.iter().any(|byte| *byte != 0) {
        return None;
    }
    let generation = u64::from_le_bytes(body.get(16..24)?.try_into().ok()?);
    let checksum: [u8; 32] = selector
        .get(COMPANION_SELECTOR_BODY_BYTES..COMPANION_SELECTOR_BYTES)?
        .try_into()
        .ok()?;
    (*blake3::hash(body).as_bytes() == checksum).then_some((active_slot, generation))
}

/// The companion's state, or why it could not be read. An I/O failure keeps its
/// kind, so a device that failed to answer is not reported as a lost companion.
fn read_companion_state_classified(
    file: &mut File,
) -> std::result::Result<CompanionState, (Option<std::io::ErrorKind>, String)> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| (Some(error.kind()), format!("seek companion: {error}")))?;
    let mut bytes = vec![0_u8; COMPANION_FILE_BYTES];
    let mut length = 0;
    while length < COMPANION_FILE_BYTES {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(error) = super::read_persistence_test_scaffold::companion_read_fault_for_test()
        {
            return Err((Some(error.kind()), format!("read companion: {error}")));
        }
        let read = match file.read(&mut bytes[length..]) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err((Some(error.kind()), format!("read companion: {error}"))),
        };
        if read == 0 {
            break;
        }
        length += read;
    }
    bytes.truncate(length);
    if length == COMPANION_FILE_BYTES {
        let mut oversize = [0_u8; 1];
        if file.read(&mut oversize).map_err(|error| {
            (
                Some(error.kind()),
                format!("probe companion length: {error}"),
            )
        })? != 0
        {
            return Err((
                None,
                format!("companion inode exceeds its fixed {COMPANION_FILE_BYTES}-byte layout"),
            ));
        }
    }
    if recoverable_generation_zero_initialization(&bytes)
        && (bytes.len() != COMPANION_FILE_BYTES || !bytes.starts_with(COMPANION_FILE_MAGIC))
    {
        return Ok(CompanionState {
            needs_initialization: true,
            active_slot: None,
            active_record: None,
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        });
    }
    if bytes.len() != COMPANION_FILE_BYTES || !bytes.starts_with(COMPANION_FILE_MAGIC) {
        return Err((None, "companion inode has an unsupported layout".to_owned()));
    }
    let slots: [std::result::Result<ParsedCompanionSlot, String>; 2] =
        std::array::from_fn(|slot| {
            let offset = companion_slot_offset(slot).expect("fixed companion slot is in range");
            parse_companion_slot(&bytes[offset..offset + COMPANION_SLOT_BYTES])
        });
    let selected =
        parse_companion_selector(&bytes).and_then(|(slot, generation)| match &slots[slot] {
            Ok(ParsedCompanionSlot::Record(record)) if record.fields.generation == generation => {
                Some((slot, record.clone()))
            }
            _ => None,
        });
    // A malformed/torn selector is not a single point of failure. Choose the
    // highest checksummed normal slot; either the old or the new synced
    // generation is therefore recoverable after every selector boundary.
    let fallback = slots
        .iter()
        .enumerate()
        .filter_map(|(slot, value)| match value {
            Ok(ParsedCompanionSlot::Record(record)) => Some((slot, record.clone())),
            _ => None,
        })
        .max_by_key(|(slot, record)| (record.fields.generation, *slot));
    let active = selected.or(fallback);
    let pending = slots
        .iter()
        .enumerate()
        .filter_map(|(slot, value)| match value {
            Ok(ParsedCompanionSlot::Pending(pending)) => Some((slot, pending.clone())),
            _ => None,
        })
        .max_by_key(|(slot, pending)| (pending.intended_record.fields.generation, *slot));
    let all_vacant = slots
        .iter()
        .all(|slot| matches!(slot, Ok(ParsedCompanionSlot::Vacant)));
    // Generation zero deliberately has no published identity yet. If its
    // selector is still intact, an interrupted first-slot payload/checksum
    // write is just another incomplete initialization: discard no complete
    // record, reinitialize in place, and let the next writer publish afresh.
    if active.is_none() && pending.is_none() && parse_companion_selector(&bytes) == Some((0, 0)) {
        return Ok(CompanionState {
            needs_initialization: true,
            active_slot: None,
            active_record: None,
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        });
    }
    if active.is_none() && pending.is_none() && !all_vacant {
        let damage = slots
            .iter()
            .filter_map(|slot| slot.as_ref().err())
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        return Err((None, format!("companion has no recoverable slot: {damage}")));
    }
    if active.is_none() && pending.is_none() {
        if recoverable_generation_zero_initialization(&bytes) {
            return Ok(CompanionState {
                needs_initialization: true,
                active_slot: None,
                active_record: None,
                pending_slot: None,
                pending_replacement: None,
                pending_matches_store: None,
            });
        }
        return Err((
            None,
            "empty companion has no valid generation-zero selector".to_owned(),
        ));
    }
    let (active_slot, active_record) = active
        .map(|(slot, record)| (Some(slot), Some(record)))
        .unwrap_or((None, None));
    let (pending_slot, pending_replacement) = pending
        .map(|(slot, pending)| (Some(slot), Some(pending)))
        .unwrap_or((None, None));
    Ok(CompanionState {
        needs_initialization: false,
        active_slot,
        active_record,
        pending_slot,
        pending_replacement,
        pending_matches_store: None,
    })
}

pub(super) fn read_companion_state(file: &mut File) -> std::result::Result<CompanionState, String> {
    read_companion_state_classified(file).map_err(|(_, reason)| reason)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn companion_writer_record(
    path: &Path,
    previous: Option<&EncodedCompanionRecord>,
    new_database_identity: bool,
    writer_boot: Option<WriterBootBinding>,
) -> Result<EncodedCompanionRecord> {
    let generation = match previous {
        Some(record) => record
            .fields
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Other(("companion generation overflow".to_owned()).into()))?,
        None => 1,
    };
    let database_identity = if new_database_identity {
        DatabaseIdentity(uuid::Uuid::new_v4().into_bytes())
    } else {
        previous
            .map(|record| record.fields.database_identity)
            .unwrap_or_else(|| DatabaseIdentity(uuid::Uuid::new_v4().into_bytes()))
    };
    // A writer that has just CLAIMED the store has not yet decided anything
    // about serving inspection -- its channel has not been tried. So the run
    // it publishes says nothing about serving rather than inventing a verdict
    // a later reader would be told: a reader arriving inside that window sees
    // a store whose owner has not answered the question yet, and asks again,
    // instead of being handed a startup failure that never happened. The real
    // decision is re-recorded the moment it exists.
    let owner_read_status = bounded_companion_status(OwnerReadStatus {
        state: OwnerServingState::NotApplicable,
        reason: None,
    });
    let record = CompanionRecord {
        format_version: 1,
        generation,
        database_identity,
        writer_run_number: WriterRunNumber(uuid::Uuid::new_v4().into_bytes()),
        owner_user: companion_effective_user_identity(),
        channel_address: derive_channel_address(path).map_err(|error| {
            Error::Other((format!("derive companion channel address: {error}")).into())
        })?,
        process_id: std::process::id(),
        owner_read_status,
        writer_boot,
    };
    prepare_companion_record_for_publication(&record)
        .map_err(|error| Error::Other((format!("encode companion record: {error}")).into()))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn companion_exists(path: &Path) -> Result<bool> {
    let companion = companion_path(path);
    match std::fs::symlink_metadata(&companion) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(crate::funded_error::corruption_text(
                writer_open_diagnostic_budget(),
                companion.display(),
                "companion path is a symbolic link".into(),
            ))
        }
        Ok(metadata) if !metadata.file_type().is_file() => {
            Err(crate::funded_error::corruption_text(
                writer_open_diagnostic_budget(),
                companion.display(),
                "companion path is not a regular file".into(),
            ))
        }
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RedbPersistence::storage_error(error)),
    }
}

#[cfg(unix)]
impl CompanionUse {
    pub(super) fn permits(self, mode: u32) -> bool {
        match self {
            Self::Writing => mode & 0o7777 == 0o600,
            // Owner-readable and nothing whatever for group or other. A
            // world-readable companion is still refused: privacy is the part
            // of the mode that matters to a reader.
            Self::Reading => matches!(mode & 0o7777, 0o600 | 0o400),
        }
    }

    pub(super) fn required_mode(self) -> &'static str {
        match self {
            Self::Writing => "0600",
            Self::Reading => "0600 or 0400",
        }
    }
}

#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_companion_metadata(
    path: &Path,
    file: &File,
    before_lock: &std::fs::Metadata,
    intent: CompanionUse,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let after_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    let same_inode = |left: &std::fs::Metadata, right: &std::fs::Metadata| {
        left.dev() == right.dev() && left.ino() == right.ino()
    };
    let expected_user = nix::unistd::Uid::effective().as_raw();
    if !before_lock.file_type().is_file()
        || !after_lock.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
        || before_lock.uid() != expected_user
        || after_lock.uid() != expected_user
        || path_metadata.uid() != expected_user
        || !intent.permits(before_lock.mode())
        || !intent.permits(after_lock.mode())
        || !intent.permits(path_metadata.mode())
        || before_lock.nlink() != 1
        || after_lock.nlink() != 1
        || path_metadata.nlink() != 1
        || !same_inode(before_lock, &after_lock)
        || !same_inode(&after_lock, &path_metadata)
    {
        // Name the permission, because a mode is the one thing on this list an
        // operator can put right, and "corrupt" sends them to a backup instead.
        return Err(crate::funded_error::corruption_display(
            writer_open_diagnostic_budget(),
            path.display(),
            format_args!(
                "companion must remain one current-user-owned regular inode with mode {} across \
                 lock acquisition; this one is mode {:04o}",
                intent.required_mode(),
                path_metadata.mode() & 0o7777,
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn validate_companion_metadata(
    path: &Path,
    file: &File,
    before_lock: &std::fs::Metadata,
    _intent: CompanionUse,
) -> Result<()> {
    let after_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if !before_lock.file_type().is_file()
        || !after_lock.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
    {
        return Err(crate::funded_error::corruption_display(
            writer_open_diagnostic_budget(),
            path.display(),
            "companion must be a direct regular file",
        ));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn store_file_fingerprint(path: &Path) -> Result<[u8; 32]> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(RedbPersistence::storage_error)?;
    let metadata = file.metadata().map_err(RedbPersistence::storage_error)?;
    if !metadata.file_type().is_file() {
        return Err(Error::Other(
            (format!(
                "replacement database is not a regular file: {}",
                path.display()
            ))
            .into(),
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-store-fingerprint-v1");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        hasher.update(&metadata.dev().to_le_bytes());
        hasher.update(&metadata.ino().to_le_bytes());
    }
    hasher.update(&metadata.len().to_le_bytes());
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(RedbPersistence::storage_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if !after.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
        || after.len() != metadata.len()
        || path_metadata.len() != metadata.len()
    {
        return Err(Error::Other(
            (format!(
                "replacement database path changed while it was fingerprinted: {}",
                path.display()
            ))
            .into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // More than one name for this inode means the pathname is not proof
        // of which file was measured: another name can be renamed over it, or
        // this one unlinked, with the inode surviving. A fingerprint taken
        // here cannot identify a generation, so anything destructive decided
        // on it is refused rather than run on the pathname's word.
        if metadata.nlink() != 1 || after.nlink() != 1 || path_metadata.nlink() != 1 {
            return Err(Error::StoreIdentityUnprovable {
                path: path.display().to_string(),
            });
        }
        if metadata.dev() != after.dev()
            || metadata.ino() != after.ino()
            || metadata.dev() != path_metadata.dev()
            || metadata.ino() != path_metadata.ino()
        {
            return Err(Error::Other((format!(
                "the inode at {} changed while it was fingerprinted, so the fingerprint identifies no generation",
                path.display()
            )).into()));
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Hash an already-open store descriptor exactly as [`store_file_fingerprint`]
/// hashes a pathname. The bytes are identical; the difference is that the
/// inode is named by a descriptor this call holds, so nothing swapped into
/// the pathname afterwards can change what was measured.
#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn store_descriptor_fingerprint(
    file: &mut File,
    metadata: &std::fs::Metadata,
) -> Result<[u8; 32]> {
    use std::os::unix::fs::MetadataExt as _;

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-store-fingerprint-v1");
    hasher.update(&metadata.dev().to_le_bytes());
    hasher.update(&metadata.ino().to_le_bytes());
    hasher.update(&metadata.len().to_le_bytes());
    file.seek(SeekFrom::Start(0))
        .map_err(RedbPersistence::storage_error)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(RedbPersistence::storage_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().map_err(RedbPersistence::storage_error)?;
    if after.dev() != metadata.dev()
        || after.ino() != metadata.ino()
        || after.len() != metadata.len()
    {
        return Err(Error::Other(
            ("replacement candidate changed while it was fingerprinted".to_owned()).into(),
        ));
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
pub(super) fn open_directory_entry(directory: &File, name: &Path) -> std::io::Result<File> {
    let owned = nix::fcntl::openat(
        directory,
        name,
        nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_NOFOLLOW | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    Ok(File::from(owned))
}

/// Unlink `name` under `directory` only while that name still resolves to
/// `identity`.
///
/// The re-check and the removal go through the SAME directory descriptor and
/// the identity comes from a descriptor this call opens, so an entry swapped
/// in the parent between the two cannot redirect the unlink onto a file this
/// store never recorded. Anything that cannot be proven to be `identity` is
/// LEFT IN PLACE and reported as not removed -- never deleted on a guess.
#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn unlink_verified_entry(
    directory: &File,
    name: &Path,
    identity: (u64, u64),
) -> Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    let candidate = match open_directory_entry(directory, name) {
        Ok(candidate) => candidate,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let metadata = candidate
        .metadata()
        .map_err(RedbPersistence::storage_error)?;
    if !metadata.file_type().is_file() || (metadata.dev(), metadata.ino()) != identity {
        return Ok(false);
    }
    nix::unistd::unlinkat(directory, name, nix::unistd::UnlinkatFlags::NoRemoveDir)
        .map_err(|error| RedbPersistence::storage_error(std::io::Error::from(error)))?;
    drop(candidate);
    Ok(true)
}

/// Remove the exact store a durable replacement intent recorded, together
/// with the companion beside it.
///
/// The intent carries a checksummed store fingerprint, never a pathname, so
/// the target is IDENTIFIED by fingerprinting each store-adjacent candidate
/// through a descriptor and removing only the one whose fingerprint is the
/// recorded value. A pathname pattern bounds the scan; it never decides a
/// removal, so a complete store that merely looks generated is untouched.
///
/// Every open and the unlink go through one held directory descriptor, and
/// the inode is re-proven on that descriptor immediately before the unlink.
/// A candidate whose identity cannot be proven is left alone; failing to
/// remove the RECORDED target is reported typed rather than passed over,
/// because that is the operator artifact this call exists to clear.
#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn remove_abandoned_replacement_target(
    path: &Path,
    fingerprint: &[u8; 32],
) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let (Some(directory_path), Some(store_name)) = (path.parent(), path.file_name()) else {
        return Ok(());
    };
    let mut prefix = store_name.to_os_string();
    prefix.push(".");
    let companion_name = companion_path(path)
        .file_name()
        .map(std::ffi::OsStr::to_os_string);
    let directory = match File::open(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let entries = match std::fs::read_dir(directory_path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let name = entry.map_err(RedbPersistence::storage_error)?.file_name();
        if name == *store_name || companion_name.as_deref() == Some(name.as_os_str()) {
            continue;
        }
        if !name
            .as_encoded_bytes()
            .starts_with(prefix.as_encoded_bytes())
        {
            continue;
        }
        names.push(PathBuf::from(name));
    }
    names.sort();
    let mut removed = false;
    for name in names {
        // A candidate that cannot be opened and measured through this
        // directory descriptor -- a directory, a symlink, a file another
        // owner holds -- is by definition not the inode this store recorded,
        // so it is passed over rather than guessed about.
        let Ok(mut candidate) = open_directory_entry(&directory, &name) else {
            continue;
        };
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        if !metadata.file_type().is_file() {
            continue;
        }
        let identity = (metadata.dev(), metadata.ino());
        let Ok(observed) = store_descriptor_fingerprint(&mut candidate, &metadata) else {
            continue;
        };
        if observed != *fingerprint {
            continue;
        }
        drop(candidate);
        if !unlink_verified_entry(&directory, &name, identity)? {
            return Err(crate::funded_error::corruption_text(writer_open_diagnostic_budget(), directory_path.join(&name).display(), "the recorded replacement target could not be re-proven at its own name, so it was left in place".into()));
        }
        let sidecar = contextdb_core::store_companion_path(&name);
        if let Ok(companion) = open_directory_entry(&directory, &sidecar)
            && let Ok(companion_metadata) = companion.metadata()
            && companion_metadata.file_type().is_file()
        {
            let companion_identity = (companion_metadata.dev(), companion_metadata.ino());
            drop(companion);
            unlink_verified_entry(&directory, &sidecar, companion_identity)?;
        }
        removed = true;
    }
    if removed {
        sync_database_parent(path)?;
    }
    Ok(())
}

/// Without `unlinkat`, an inode proven by a descriptor cannot be removed by
/// that identity, only by a name another process could have re-pointed. The
/// abandoned target is therefore LEFT in place rather than removed on a
/// pathname's word.
#[cfg(not(unix))]
pub(super) fn remove_abandoned_replacement_target(
    _path: &Path,
    _fingerprint: &[u8; 32],
) -> Result<()> {
    Ok(())
}

/// Remove the companion the installed replacement was generated with, at the
/// pathname it was generated under.
///
/// A replacement is installed by renaming its store onto this pathname, which
/// leaves the companion it was built with orphaned at the name the store just
/// left. The intent records a fingerprint, never a pathname, so that orphan is
/// IDENTIFIED the same way the abandoned target's sidecar is -- by the
/// companion naming rule -- and then removed only when TWO independent proofs
/// hold on the candidate itself: its own durable record decodes and names
/// exactly the store pathname it sits beside, and no store remains at that
/// pathname. A file that does not decode as a companion generation, and a
/// companion whose store is still there, are therefore never touched, so a
/// generated-shaped decoy survives recovery whole.
///
/// Every open and the unlink go through one held directory descriptor, and the
/// inode is re-proven on that descriptor immediately before the unlink.
#[cfg(unix)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn remove_installed_replacement_companion(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let (Some(directory_path), Some(store_name)) = (path.parent(), path.file_name()) else {
        return Ok(());
    };
    let mut prefix = store_name.to_os_string();
    prefix.push(".");
    let own_companion = companion_path(path)
        .file_name()
        .map(std::ffi::OsStr::to_os_string);
    // The address a companion records is derived from its store's resolved
    // pathname, and the store this one named is already gone, so the
    // directory is resolved once here and the vanished name joined onto it.
    let canonical_directory = match std::fs::canonicalize(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let directory = match File::open(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let entries = match std::fs::read_dir(directory_path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let name = entry.map_err(RedbPersistence::storage_error)?.file_name();
        if name == *store_name || own_companion.as_deref() == Some(name.as_os_str()) {
            continue;
        }
        if !name
            .as_encoded_bytes()
            .starts_with(prefix.as_encoded_bytes())
        {
            continue;
        }
        names.push(PathBuf::from(name));
    }
    names.sort();
    let mut removed = false;
    for name in names {
        // The companion naming rule is the store's own pathname with `.lock`
        // appended (`companion_path`), so the stem below is that store name.
        if name.extension().and_then(std::ffi::OsStr::to_str) != Some("lock") {
            continue;
        }
        // The store pathname this candidate is the companion OF -- the name
        // the installed replacement occupied before it was renamed here.
        let Some(generated_name) = name.file_stem().map(std::ffi::OsStr::to_os_string) else {
            continue;
        };
        let generated_name = PathBuf::from(generated_name);
        // A companion whose store is still at its own name belongs to that
        // store, not to the generation installed here.
        if open_directory_entry(&directory, &generated_name).is_ok() {
            continue;
        }
        let recorded_address = ChannelAddress(
            *blake3::hash(
                canonical_directory
                    .join(&generated_name)
                    .as_os_str()
                    .as_encoded_bytes(),
            )
            .as_bytes(),
        );
        let Ok(mut candidate) = open_directory_entry(&directory, &name) else {
            continue;
        };
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        if !metadata.file_type().is_file() {
            continue;
        }
        let identity = (metadata.dev(), metadata.ino());
        let Ok(state) = read_companion_state(&mut candidate) else {
            continue;
        };
        let Some(record) = state.active_record else {
            continue;
        };
        if record.fields.channel_address != recorded_address {
            continue;
        }
        drop(candidate);
        if unlink_verified_entry(&directory, &name, identity)? {
            removed = true;
        }
    }
    if removed {
        sync_database_parent(path)?;
    }
    Ok(())
}

/// Without `unlinkat`, a companion proven by a descriptor cannot be removed by
/// that identity, only by a name another process could have re-pointed. The
/// orphaned companion is therefore LEFT in place rather than removed on a
/// pathname's word.
#[cfg(not(unix))]
pub(super) fn remove_installed_replacement_companion(_path: &Path) -> Result<()> {
    Ok(())
}

impl CompanionAdmission {
    pub(super) const fn may_create(self) -> bool {
        !matches!(self, Self::Existing)
    }

    pub(super) const fn reinitializes_residue(self) -> bool {
        matches!(self, Self::NewStore)
    }
}

impl CompanionGuard {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire(path: &Path, admission: CompanionAdmission) -> Result<Self> {
        Self::acquire_inner(path, admission, true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire_inner(
        path: &Path,
        admission: CompanionAdmission,
        may_create: bool,
    ) -> Result<Self> {
        let companion_path = companion_path(path);
        let exists = companion_exists(path)?;
        let create = !exists && may_create && admission.may_create();
        if !exists && !create {
            return Err(Error::Other(
                (format!(
                    "store companion does not exist: {}",
                    companion_path.display()
                ))
                .into(),
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        if create {
            options.create_new(true);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = match options.open(&companion_path) {
            Ok(file) => file,
            Err(error) if create && error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Self::acquire_inner(path, admission, false);
            }
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        // A created companion is owner-only from its first byte (the `mode`
        // above), never by a chmod that would leave a readable window. An
        // inode that already existed is repaired to the same invariant --
        // only when it is one regular file this user owns, so nothing another
        // user could have planted is ever adopted -- because refusing it
        // would make an ordinary pathname permanently unopenable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            use std::os::unix::fs::PermissionsExt as _;
            let opened = file.metadata().map_err(RedbPersistence::storage_error)?;
            if opened.file_type().is_file()
                && opened.uid() == nix::unistd::Uid::effective().as_raw()
                && opened.nlink() == 1
                && opened.mode() & 0o7777 != 0o600
            {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))
                    .map_err(RedbPersistence::storage_error)?;
            }
            if create {
                file.sync_all().map_err(RedbPersistence::storage_error)?;
            }
        }
        let before_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
        validate_companion_metadata(&companion_path, &file, &before_lock, CompanionUse::Writing)?;
        // The claim-window byte first: only writers ever lock it, so a
        // reader asking about it can never make this request fail, and from
        // this instant every newcomer sees an unsettled claim.
        #[cfg(unix)]
        match take_companion_range_lock_exclusive(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET) {
            Ok(RangeLockOutcome::Taken) => {}
            Ok(RangeLockOutcome::HeldByAnother) => {
                return Err(held_by_writer_error(path, &mut file));
            }
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
        if !try_lock_exclusive(&file)? {
            release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            return Err(held_by_writer_error(path, &mut file));
        }
        if let Err(error) =
            validate_companion_metadata(&companion_path, &file, &before_lock, CompanionUse::Writing)
        {
            release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            unlock_file(&file);
            return Err(error);
        }
        #[cfg(unix)]
        if let Err(error) = take_companion_range_lock_exclusive(&file, COMPANION_OWNER_LOCK_OFFSET)
        {
            release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            unlock_file(&file);
            return Err(RedbPersistence::storage_error(error));
        }
        // This IS the moment the store becomes owned. Everything a reader can
        // dial is published later, so between here and that publication a
        // process genuinely holds the store and nothing published says so --
        // and a caller told "nobody owns this" there goes off to create and
        // take a store somebody else is already holding. Holding the
        // claim-window byte from here is what makes that stretch answerable.
        // It was taken above, before the claim itself.
        crate::read_session::note_writer_open_event(
            crate::read_session::ReadSessionEvent::CompanionClaimTaken,
        );
        if create && let Err(error) = sync_database_parent(&companion_path) {
            unlock_file(&file);
            return Err(error);
        }
        let companion = match read_companion_state_classified(&mut file) {
            Ok(companion) => companion,
            // The store is being created here, so an unreadable companion is
            // residue rather than this store's lost generation.
            // `publish_writer_run` rewrites the whole fixed layout.
            Err(_) if admission.reinitializes_residue() => CompanionState {
                needs_initialization: true,
                active_slot: None,
                active_record: None,
                pending_slot: None,
                pending_replacement: None,
                pending_matches_store: None,
            },
            // A device that failed to answer the companion read is I/O, not a
            // lost companion: no corruption verdict and no reset advice.
            Err((Some(kind), error)) if !super::io_kind_is_damage(kind) => {
                unlock_file(&file);
                return Err(RedbPersistence::storage_message_error(error));
            }
            Err((_, error)) => {
                unlock_file(&file);
                return Err(crate::funded_error::corruption_display(
                    writer_open_diagnostic_budget(),
                    companion_path.display(),
                    format_args!("companion cannot be recovered: {error}"),
                ));
            }
        };
        let pending_matches_store = companion
            .pending_replacement
            .as_ref()
            .map(|pending| {
                store_file_fingerprint(path)
                    .map(|fingerprint| fingerprint == pending.store_fingerprint)
            })
            .transpose()?;
        let mut companion = companion;
        companion.pending_matches_store = pending_matches_store;
        Ok(Self {
            path: companion_path,
            state: Mutex::new(CompanionGuardState { file, companion }),
        })
    }

    /// Take back a companion this open created for itself, because the open
    /// is being refused.
    ///
    /// On the road where a store has no companion yet, the claim is taken
    /// before the root has been proven readable and current -- it has to be,
    /// because the writer already owns the store from that moment and a caller
    /// arriving there must not be told the store is free. The other half of
    /// that promise is here: an open that is then refused, because the file is
    /// an unsupported layout or a corrupt root or not a store at all, leaves the
    /// directory exactly as it found it. An operator who points a writer at
    /// the wrong file is told so and is not left with a companion sitting
    /// beside it.
    ///
    /// The removal happens while the companion is still held exclusively, so
    /// no other opener can adopt the inode in the gap, and only while the
    /// pathname still names the file this guard is holding.
    ///
    /// A process that dies part-way through such an open leaves the companion
    /// behind, exactly as a process that dies part-way through creating a new
    /// store does today. That residue is what the next opener's
    /// residue-reinitializing admission is for; it is not a new condition.
    pub(super) fn discard_created(self, database_path: &Path) {
        let companion = companion_path(database_path);
        let Ok(state) = self.lock_state() else {
            return;
        };
        if companion_still_names(&companion, &state.file) {
            let _ = std::fs::remove_file(&companion);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CompanionGuardState>> {
        self.state.lock().map_err(|_| {
            crate::funded_error::corruption_text(
                writer_open_diagnostic_budget(),
                self.path.display(),
                "companion guard state was poisoned by an interrupted operation".into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn finish_pending_replacement(
        state: &mut CompanionGuardState,
    ) -> Result<EncodedCompanionRecord> {
        let pending_slot = state.companion.pending_slot.ok_or_else(|| {
            Error::Other(("pending replacement has no companion slot".to_owned()).into())
        })?;
        let pending = state.companion.pending_replacement.clone().ok_or_else(|| {
            Error::Other(("pending replacement record is absent".to_owned()).into())
        })?;
        let record_slot = 1 - pending_slot;
        write_companion_record(&mut state.file, record_slot, &pending.intended_record)
            .map_err(RedbPersistence::storage_error)?;
        write_companion_record_copy(&mut state.file, pending_slot, &pending.intended_record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion = CompanionState {
            needs_initialization: false,
            active_slot: Some(record_slot),
            active_record: Some(pending.intended_record.clone()),
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        };
        Ok(pending.intended_record)
    }

    /// Resolve a recorded intent while establishing ownership of the store.
    /// A recorded target the surviving generation did not adopt is residue,
    /// and this is the moment it is removed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn resolve_pending_replacement(
        path: &Path,
        state: &mut CompanionGuardState,
    ) -> Result<()> {
        Self::resolve_pending_replacement_inner(path, state, AbandonedTarget::Remove)
    }

    /// Resolve a recorded intent while a replacement is being re-recorded.
    /// The target named by the standing record is the one about to be named
    /// again, so removing it here would delete the live replacement.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_pending_replacement(
        path: &Path,
        state: &mut CompanionGuardState,
    ) -> Result<()> {
        Self::resolve_pending_replacement_inner(path, state, AbandonedTarget::Retain)
    }

    /// Resolve a recorded intent against the store exactly as it stands,
    /// without publishing a writer run. The recorded intent names its
    /// generation by a fingerprint of that generation's bytes, so anything
    /// that rewrites the store -- redb's own crash repair above all -- must
    /// let this resolution complete FIRST, while the fingerprint still
    /// selects a generation.
    /// A companion with nothing recorded is left byte-for-byte untouched.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn settle_pending_replacement(&self, path: &Path) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.companion.pending_replacement.is_none() {
            return Ok(());
        }
        Self::resolve_pending_replacement(path, &mut state)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn resolve_pending_replacement_inner(
        path: &Path,
        state: &mut CompanionGuardState,
        abandoned: AbandonedTarget,
    ) -> Result<()> {
        if state.companion.pending_replacement.is_none() {
            return Ok(());
        }
        match state.companion.pending_matches_store {
            Some(true) => {
                Self::finish_pending_replacement(state)?;
                // The recorded generation is the store standing here, so the
                // companion it was generated with is orphaned at the name it
                // was renamed away from. Removing it while this guard owns
                // the store is what keeps an interrupted replacement from
                // leaving an operator artifact, exactly as the abandoned
                // branch below removes the target it recorded.
                if abandoned == AbandonedTarget::Remove {
                    remove_installed_replacement_companion(path)?;
                }
                Ok(())
            }
            Some(false) if state.companion.active_record.is_some() => {
                // The prior generation is the surviving store, so the target
                // this companion recorded is abandoned. Removing it here --
                // while this guard still owns the store -- is what keeps a
                // crashed replacement from leaving an operator artifact.
                let recorded = state
                    .companion
                    .pending_replacement
                    .as_ref()
                    .map(|pending| pending.store_fingerprint);
                if let (AbandonedTarget::Remove, Some(fingerprint)) = (abandoned, recorded) {
                    remove_abandoned_replacement_target(path, &fingerprint)?;
                }
                Ok(())
            }
            Some(false) => Err(crate::funded_error::corruption_text(writer_open_diagnostic_budget(), path.display(), "pending replacement does not identify this store and no prior companion generation survives".into())),
            None => Err(crate::funded_error::corruption_text(writer_open_diagnostic_budget(), path.display(), "pending replacement could not be matched to the store".into())),
        }
    }

    /// Publish this writer's run. `writer_boot` is given only once the
    /// store's open -- and any repair it made -- has finished, which is what
    /// lets a later reader of this store, should this writer die, trust the
    /// committed file without repairing it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_writer_run(
        &self,
        path: &Path,
        new_database_identity: bool,
        writer_boot: Option<WriterBootBinding>,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.companion.needs_initialization {
            state.companion = initialize_companion_inode(&mut state.file)
                .map_err(RedbPersistence::storage_message_error)?;
        }
        Self::resolve_pending_replacement(path, &mut state)?;
        let record = companion_writer_record(
            path,
            state.companion.active_record.as_ref(),
            new_database_identity,
            writer_boot,
        )?;
        let active_slot = state.companion.active_slot.unwrap_or(1);
        let inactive_slot = 1 - active_slot;
        write_companion_record(&mut state.file, inactive_slot, &record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion = CompanionState {
            needs_initialization: false,
            active_slot: Some(inactive_slot),
            active_record: Some(record),
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        };
        // The claim-window byte is already held: this writer took it when it
        // first claimed the companion, which is when it began to own the
        // store. The run published here still says nothing about serving, so
        // the window stays open until the decision exists.
        Ok(())
    }

    /// Re-record what the writer holding this store decided about serving
    /// inspection, keeping every other published fact exactly as it is.
    ///
    /// The record a writer publishes at open is written BEFORE it knows
    /// whether its channel came up, so the state has to be re-recorded once
    /// the answer exists and again whenever it changes. It is the one thing a
    /// later reader has to go on, so a reader that cannot reach a channel is
    /// told the writer's own word rather than the storage layer's complaint.
    /// The writer run number and channel address are untouched: this is the
    /// same run saying more about itself, never a new one.
    /// End this writer's claim window because what it decided can now be seen
    /// by a caller asking about the store.
    ///
    /// A writer whose channel is up has already answered the only question the
    /// window exists for: the caller dials it and is told, by the writer
    /// itself, that it is serving. Waiting for the record to be written before
    /// letting that caller go would keep it asleep past the moment the answer
    /// existed. A writer that is NOT serving has no channel to be asked, so its
    /// window closes where its decision becomes durable instead.
    pub(super) fn close_claim_window(&self) {
        let Ok(state) = self.lock_state() else {
            return;
        };
        release_companion_range_lock(&state.file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn record_owner_read_status(
        &self,
        path: &Path,
        status: OwnerReadStatus,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let recorded = Self::publish_decided_owner_read_status(path, &mut state, status);
        // This writer has decided, and whatever it decided is now beside the
        // store, so its claim window is over. Releasing only here is what makes
        // the wake honest: a caller sleeping on this byte wakes to a record
        // that already says what this writer decided, never to the window it
        // was waiting out.
        release_companion_range_lock(&state.file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
        recorded
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_decided_owner_read_status(
        path: &Path,
        state: &mut CompanionGuardState,
        status: OwnerReadStatus,
    ) -> Result<()> {
        let Some(previous) = state.companion.active_record.clone() else {
            return Ok(());
        };
        let status = bounded_companion_status(status);
        if previous.fields.owner_read_status == status {
            return Ok(());
        }
        Self::resolve_pending_replacement(path, state)?;
        let Some(previous) = state.companion.active_record.clone() else {
            return Ok(());
        };
        let mut fields = previous.fields.clone();
        fields.owner_read_status = status;
        fields.generation = fields
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Other(("companion generation overflow".to_owned()).into()))?;
        let record = prepare_companion_record_for_publication(&fields)
            .map_err(|error| Error::Other((format!("encode companion record: {error}")).into()))?;
        let active_slot = state.companion.active_slot.unwrap_or(1);
        let inactive_slot = 1 - active_slot;
        write_companion_record(&mut state.file, inactive_slot, &record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion.active_slot = Some(inactive_slot);
        state.companion.active_record = Some(record);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_replacement_database(
        &self,
        path: &Path,
        replacement_path: &Path,
    ) -> Result<()> {
        let fingerprint = store_file_fingerprint(replacement_path)?;
        let mut state = self.lock_state()?;
        if state.companion.needs_initialization {
            state.companion = initialize_companion_inode(&mut state.file)
                .map_err(RedbPersistence::storage_message_error)?;
        }
        Self::retain_pending_replacement(path, &mut state)?;
        let record =
            companion_writer_record(path, state.companion.active_record.as_ref(), true, None)?;
        let pending = encode_pending_replacement(fingerprint, record)
            .map_err(RedbPersistence::storage_message_error)?;
        let active_slot = state.companion.active_slot.ok_or_else(|| {
            crate::funded_error::corruption_text(
                writer_open_diagnostic_budget(),
                companion_path(path).display(),
                "replacement requires a published prior companion generation".into(),
            )
        })?;
        let pending_slot = 1 - active_slot;
        write_companion_pending_slot(&mut state.file, pending_slot, &pending)
            .map_err(RedbPersistence::storage_error)?;
        state.companion.pending_slot = Some(pending_slot);
        state.companion.pending_replacement = Some(pending);
        state.companion.pending_matches_store = Some(false);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_replacement_database(&self, path: &Path) -> Result<()> {
        let mut state = self.lock_state()?;
        let pending = state
            .companion
            .pending_replacement
            .as_ref()
            .ok_or_else(|| Error::Other(("replacement was not prepared".to_owned()).into()))?;
        let fingerprint = store_file_fingerprint(path)?;
        if fingerprint != pending.store_fingerprint {
            return Err(crate::funded_error::corruption_text(
                writer_open_diagnostic_budget(),
                path.display(),
                "replacement path does not match the checksummed pending database identity".into(),
            ));
        }
        state.companion.pending_matches_store = Some(true);
        Self::finish_pending_replacement(&mut state)?;
        Ok(())
    }
}

#[cfg(test)]
mod bare_admission_refusal_tests {
    use super::*;

    /// A refusal that names no owner says nothing about the store's bytes,
    /// so neither read door may give it the corruption answer and its advice.
    #[test]
    fn a_bare_admission_refusal_is_not_reported_as_corruption() {
        let bare = || redb::StorageError::Native(redb::NativeColdReadError::AdmissionRefused);
        assert!(matches!(
            read_storage_error(bare()),
            LoadReadImageError::Io { .. }
        ));
        assert!(matches!(
            read_database_open_error(redb::DatabaseError::Storage(bare())),
            LoadReadImageError::Io { .. }
        ));
    }
}
