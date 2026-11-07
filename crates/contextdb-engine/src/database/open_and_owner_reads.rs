#![cfg_attr(
    not(feature = "iroh"),
    expect(
        dead_code,
        reason = "Only the Iroh sync transport calls this; builds without that feature keep it for the shared wire, custody and blob types."
    )
)]
use super::*;

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_builtin_with_options(
        path: impl AsRef<Path>,
        options: DatabaseOpenOptions,
    ) -> Result<Self> {
        let path = path.as_ref();
        #[cfg(any(test, feature = "test-seams"))]
        {
            let interceptor = COMMON_OPTIONS_OPEN_INTERCEPTOR.with(|slot| slot.borrow().clone());
            if let Some(interceptor) = interceptor {
                return interceptor.intercept(path, &options);
            }
        }
        let (config, _) = OpenConfiguration::take_options(options);
        Self::open_constructed(path, config, PluginBinding::builtin())
    }

    /// Open the scratch database that `:memory:` names: an ordinary durable
    /// database in a fresh directory under the user data directory, owned by
    /// the database it holds and removed when that database releases its file.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_in_scratch(open: impl FnOnce(&Path) -> Result<Self>) -> Result<Self> {
        let scratch_error = |error: std::io::Error| {
            Error::Other((format!("create scratch database directory: {error}")).into())
        };
        let root = crate::scratch::user_scratch_root().map_err(scratch_error)?;
        let directory =
            crate::scratch::ScratchDirectory::create_in(&root).map_err(scratch_error)?;
        crate::scratch::warn_if_ram_backed(directory.path());
        let file = directory.path().join(SCRATCH_DATABASE_FILE);
        let key = canonical_database_path(&file)?;
        crate::scratch::adopt(key.clone(), directory);
        match open(&file) {
            Ok(db) => Ok(db),
            Err(error) => {
                crate::scratch::release(&key);
                Err(error)
            }
        }
    }

    /// Open a database through the common writable-open configuration path.
    ///
    /// Every convenience opener builds its options and arrives here, so the
    /// configuration a database was opened with is the configuration it can
    /// be asked about afterwards.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_with_options(path: impl AsRef<Path>, options: DatabaseOpenOptions) -> Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        {
            let interceptor = COMMON_OPTIONS_OPEN_INTERCEPTOR.with(|slot| slot.borrow().clone());
            if let Some(interceptor) = interceptor {
                return interceptor.intercept(path.as_ref(), &options);
            }
        }
        let (config, binding) = OpenConfiguration::take_options(options);
        Self::open_constructed(path.as_ref(), config, binding)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn reset_content_identity_work_for_test(&self) -> Result<()> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other("content identity requires a file-backed store".into()))?
            .reset_content_identity_work_for_test();
        Ok(())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn take_content_identity_work_for_test(&self) -> Result<(u64, u64, u64)> {
        Ok(self
            .persistence
            .as_ref()
            .ok_or_else(|| Error::Other("content identity requires a file-backed store".into()))?
            .take_content_identity_work_for_test())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn take_content_identity_work_bytes_for_test(&self) -> Result<(u64, u64)> {
        Ok(self
            .persistence
            .as_ref()
            .ok_or_else(|| Error::Other("content identity requires a file-backed store".into()))?
            .take_content_identity_work_bytes_for_test())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn damage_checkpoint_f32_projection_for_test(&self) -> Result<()> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Error::Other("checkpoint damage requires a file-backed store".into()))?
            .damage_checkpoint_f32_projection_for_test()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_constructed(
        path: &Path,
        config: OpenConfiguration,
        binding: PluginBinding,
    ) -> Result<Self> {
        let OpenConfiguration {
            owner_reads,
            open_disposition,
            memory_limit,
            disk_limit,
            maintenance_policy,
            maintenance_poll_interval,
            contexts,
            scope_labels,
            principal,
            reader_wait_stop,
            reader_wait_deadline,
            #[cfg(any(test, feature = "test-seams"))]
            test_observer,
            #[cfg(any(test, feature = "test-seams"))]
            test_kernel_observer,
        } = config;
        Self::open_configured(
            path,
            DatabaseOpenOptions {
                owner_reads,
                open_disposition,
                plugin: binding.erased(),
                memory_limit,
                disk_limit,
                maintenance_policy,
                maintenance_poll_interval,
                contexts,
                scope_labels,
                principal,
                reader_wait_stop,
                reader_wait_deadline,
                #[cfg(any(test, feature = "test-seams"))]
                test_observer,
                #[cfg(any(test, feature = "test-seams"))]
                test_kernel_observer,
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_configured(path: &Path, options: DatabaseOpenOptions) -> Result<Self> {
        Self::open_configured_with_host(path, options, DatabaseHostConstruction::Legacy)
    }

    /// Unit modules use the real open assembly, including file reopen.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn open_owned_for_test(path: &Path, options: DatabaseOpenOptions) -> Result<Self> {
        Self::open_configured_with_host(path, options, DatabaseHostConstruction::Owned)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_configured_with_host(
        path: &Path,
        options: DatabaseOpenOptions,
        host_construction: DatabaseHostConstruction,
    ) -> Result<Self> {
        if path.as_os_str() == MEMORY_DATABASE_PATH {
            // `:memory:` names a scratch database, not the absence of a file:
            // an ordinary durable database in a scratch directory of its own.
            let mut options = options;
            options.open_disposition = OpenDisposition::CreateIfMissing;
            return Self::open_in_scratch(|file| {
                Self::open_configured_with_host(file, options, host_construction)
            });
        }
        let DatabaseOpenOptions {
            owner_reads,
            open_disposition,
            plugin,
            memory_limit,
            disk_limit,
            maintenance_policy,
            maintenance_poll_interval,
            contexts,
            scope_labels,
            principal,
            reader_wait_stop,
            reader_wait_deadline,
            #[cfg(any(test, feature = "test-seams"))]
            test_observer,
            #[cfg(any(test, feature = "test-seams"))]
            test_kernel_observer,
        } = options;
        let accountant = match memory_limit {
            Some(bytes) => MemoryAccountant::writer_budget(bytes)?,
            None => MemoryAccountant::writer_no_limit()?,
        };
        // Keep the caller's exact cancellation and absolute deadline together
        // with this open's original writer payer before any store acquisition.
        // Absent bounds, the open still waits until the store is handed over.
        let _reader_wait = crate::persistence::WriterWaitScope::enter(
            reader_wait_stop,
            reader_wait_deadline,
            accountant.writer_wait_payer(),
        );
        let declared_read_memory =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                owner_reads.limits.limits.memory,
            )?;
        let mut db = {
            // The observer this open was given stays in force for the whole
            // of it, so the milestones a writer reaches below the options
            // layer -- its first claim on the store, before anything about it
            // is published -- still reach the caller that asked to see them.
            #[cfg(any(test, feature = "test-seams"))]
            let db =
                crate::read_session::with_writer_open_observer(test_observer.as_ref(), || {
                    Self::open_loaded(
                        path,
                        Arc::clone(&plugin),
                        accountant,
                        disk_limit,
                        open_disposition,
                        owner_reads.runtime_dir.as_deref(),
                        declared_read_memory.clone(),
                        declared_read_memory.child()?,
                        host_construction,
                    )
                })?;
            #[cfg(not(any(test, feature = "test-seams")))]
            let db = Self::open_loaded(
                path,
                Arc::clone(&plugin),
                accountant,
                disk_limit,
                open_disposition,
                owner_reads.runtime_dir.as_deref(),
                declared_read_memory.clone(),
                declared_read_memory.child()?,
                host_construction,
            )?;
            observe_open_event!(test_observer, OpenRegistryAcquired);
            observe_open_event!(test_observer, PersistenceOpened);
            observe_open_event!(test_observer, BlobRepositoryOpened);
            db
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_plugin_start();
        db.plugin.on_open()?;
        observe_open_event!(test_observer, PluginOpened);
        db.declared_read_memory = declared_read_memory;
        db.owner_read_config = Arc::new(owner_reads);
        db.maintenance_caller_driven.store(
            maintenance_policy == MaintenancePolicy::CallerDriven,
            Ordering::SeqCst,
        );
        if let Some(interval) = maintenance_poll_interval {
            db.apply_maintenance_poll_interval(interval)?;
        }
        db.access = AccessConstraints {
            contexts,
            scope_labels,
            principal,
        };
        #[cfg(any(test, feature = "test-seams"))]
        {
            db.route_observer = test_observer.clone();
            db.kernel_observer = test_kernel_observer;
        }
        {
            db.start_cron_tickler_if_schedules_present()?;
            db.load_retention_sync_peer();
            db.reconcile_maintenance_thread();
            match db.start_owner_read_service(path) {
                Ok(service) => {
                    db.owner_read_service = service;
                    #[cfg(feature = "test-seams")]
                    if let Some(service) = &db.owner_read_service {
                        service.observe_last_handle_release_for_test(Arc::clone(
                            &db.finalization.release_completion_for_test,
                        ));
                    }
                    if db.owner_read_service.is_some() {
                        observe_open_event!(test_observer, OwnerServiceStarted);
                        // A caller that arrived while this writer was still
                        // claiming the store can be told the answer now: the
                        // channel is listening and says for itself that it is
                        // serving. Keeping such a caller asleep until the
                        // record is written would hold it past the moment its
                        // answer existed.
                        db.close_claim_window();
                    }
                }
                Err(reason) => db.owner_read_startup_failure = Some(reason),
            }
            db.record_owner_read_status();
        }
        // Startup proof/schema reads must not warm the published writer cache.
        // Evict at the final unpublished boundary, then restore exactly the
        // existing effective declared capacity and its already-held reserve.
        // That is the capacity the open funded, not one recomputed from a
        // ceiling that may have moved since.
        if let Some(persistence) = db.persistence.as_ref() {
            let capacity = persistence.installed_cache_capacity().unwrap_or_else(|| {
                crate::persistence::redb_writer_cache_bytes_for_limit(db.accountant.usage().limit)
            });
            persistence.settle_writer_cache_budget(0)?;
            persistence.settle_writer_cache_budget(capacity)?;
        }
        Ok(db)
    }

    /// Publish this writer's REAL serving state into the companion beside the
    /// store.
    ///
    /// The record written when the store was claimed is necessarily earlier
    /// than the answer -- a reader needs the channel address before the
    /// channel exists -- so the decision is re-recorded once it is known and
    /// again whenever it changes. It is the only thing a reader who cannot
    /// reach a channel has to go on, which is why it must say what this writer
    /// actually decided rather than a fixed placeholder.
    ///
    /// Failing to re-record never fails the operation that prompted it: the
    /// writer works either way, and the worst case is the answer a reader
    /// would have had before this record existed at all.
    /// End this writer's claim window because its channel is up and can be
    /// asked directly. A writer that is not serving has no channel to ask, so
    /// its window closes when its decision reaches the companion instead.
    pub(super) fn close_claim_window(&self) {
        if let Some(persistence) = &self.persistence {
            persistence.close_claim_window();
        }
    }

    pub(super) fn record_owner_read_status(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let status = self.owner_read_status();
        if let Err(error) = persistence.record_owner_read_status(status) {
            tracing::warn!(
                error = %error,
                "the store's recorded owner-serving state could not be updated"
            );
        }
    }

    /// Bring up the owner inspection channel beside a file-backed open.
    ///
    /// Failing to serve never fails the database open: the caller still gets
    /// a working writer, and `owner_read_status` says why inspection is
    /// unavailable instead of the caller discovering it as an empty result.
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn start_owner_read_service(
        &self,
        path: &Path,
    ) -> std::result::Result<Option<Arc<crate::owner_read::OwnerReadService>>, String> {
        if !self.owner_read_config.enabled {
            return Ok(None);
        }
        #[cfg(unix)]
        {
            self.start_owner_read_channel(path).map(Some)
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(None)
        }
    }

    #[cfg(unix)]
    pub(super) fn start_owner_read_channel(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<crate::owner_read::OwnerReadService>, String> {
        use crate::local_transport::{
            ChannelPathFacts, LocalConfigurationSource, LocalHandshake, OWNER_ONLY_MODE,
            StaleChannelAction, UnixLocalCarrier, channel_socket_path,
        };
        #[cfg(not(target_os = "linux"))]
        use crate::local_transport::{StaleChannelProbe, reconcile_stale_channel};
        use crate::owner_read::{OwnerReadService, OwnerServiceSpec, ValidatedOwnerListener};
        use contextdb_core::read_contract::{DeadlineClock, OwnerReadStatus, OwnerServingState};

        let canonical = canonical_database_path(path).map_err(|error| error.to_string())?;
        let (database_identity, writer_run, owner_user, channel_address) =
            crate::persistence::published_writer_identity(&canonical)
                .ok_or_else(|| "companion record has no published writer identity".to_owned())?;
        // A directory an operator supplied IS the directory the channel goes
        // in -- a packaged service or add-on has its service manager create
        // and own exactly that path. Only the platform base is a container
        // whose owner-only `contextdb` child this owner creates for itself.
        let runtime_directory = crate::local_transport::runtime_directory_for_store(
            self.owner_read_config.runtime_dir.as_deref(),
            owner_user,
        )
        .map_err(|error| error.to_string())?
        .path()
        .to_path_buf();
        let channel_path = channel_socket_path(&runtime_directory, channel_address)
            .map_err(|error| error.to_string())?;
        let handshake = LocalHandshake::current(database_identity, writer_run, owner_user);
        let clock: Arc<dyn DeadlineClock> = self.owner_read_clock();
        // A pathname is not proof that a channel is stale. Ask it once inside
        // the shipped connection budget, bind that evidence to the exact
        // socket inode observed before the ask, and remove only that inode
        // when nobody answers. A live, foreign, malformed, or concurrently
        // replaced responder is preserved and this writer simply reports
        // that its inspection service could not start.
        match std::fs::symlink_metadata(&channel_path) {
            Ok(_) => {
                let deadline_ms = clock
                    .now_ms()
                    .checked_add(ReadClientTimeouts::default().connect_ms)
                    .ok_or_else(|| "owner-channel startup deadline overflow".to_owned())?;
                #[cfg(target_os = "linux")]
                let action = {
                    let evidence = UnixLocalCarrier
                        .probe_stale_funded(
                            &channel_path,
                            &handshake,
                            &clock,
                            deadline_ms,
                            self.finalization
                                .accountant
                                .read_budget()
                                .map_err(|error| error.to_string())?,
                        )
                        .map_err(|error| error.to_string())?;
                    UnixLocalCarrier.remove_after_proven_stale(&channel_path, evidence)
                };
                #[cfg(not(target_os = "linux"))]
                let action = reconcile_stale_channel(
                    &UnixLocalCarrier,
                    &StaleChannelProbe {
                        path: channel_path.clone(),
                        expected_owner: handshake.clone(),
                    },
                    clock.as_ref(),
                    deadline_ms,
                );
                match action.map_err(|error| error.to_string())? {
                    StaleChannelAction::RemoveAndRetry => {}
                    StaleChannelAction::Preserve => {
                        return Err(
                            "the existing owner channel could not be proven stale".to_owned()
                        );
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
        let listener = ValidatedOwnerListener::new(ChannelPathFacts {
            // These are the facts the channel is about to be created with;
            // binding verifies the created socket against them.
            path: channel_path,
            runtime_directory,
            is_socket: true,
            owner: owner_user,
            mode: OWNER_ONLY_MODE,
        });
        let spec = OwnerServiceSpec::new(
            Arc::new(self.scoped_with_constraints(None, None, None)),
            listener,
            handshake,
            OwnerReadStatus {
                state: OwnerServingState::Serving,
                reason: None,
            },
            (*self.owner_read_config).clone(),
            if self.owner_read_config.limits == OwnerReadLimits::default()
                && self.owner_read_config.timeouts == OwnerServiceTimeouts::default()
            {
                LocalConfigurationSource::Default
            } else {
                LocalConfigurationSource::Override
            },
            clock,
        );
        OwnerReadService::start(spec).map_err(|error| error.to_string())
    }
    #[cfg(unix)]
    pub(super) fn owner_read_clock(&self) -> Arc<dyn contextdb_core::read_contract::DeadlineClock> {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(hooks) = &self.owner_read_config.test_hooks {
            return Arc::clone(&hooks.clock);
        }
        Arc::new(crate::local_transport::MonotonicDeadlineClock::new())
    }

    /// Create an in-process bounded read view over this live database.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn read_session(&self, limits: ReadLimits) -> Result<ReadSession> {
        let _operation = self.open_operation()?;
        Ok(ReadSession::from_live_database(
            self.reader_handle(),
            limits,
        ))
    }

    /// Create an in-process bounded read view that reports what its reads are
    /// doing while they run.
    ///
    /// A caller holding an open database gets the same running commentary a
    /// caller opening the store by path gets: rows and bytes assembled, items
    /// examined, and milliseconds spent are told to the observer from inside
    /// each read, so a long read is distinguishable from a wedged one. There
    /// is no hydration phase to report — this database is already open — so
    /// the reports describe execution only.
    ///
    /// The reports carry the same bounds this session reads under: every
    /// figure is progress against the ceiling of the same name in `limits`,
    /// and reporting adds no work of its own beyond the observer's, which
    /// runs inline on the reading thread and so is part of that read's cost.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn read_session_with_progress(
        &self,
        limits: ReadLimits,
        progress: Arc<dyn crate::read_progress::ReadProgressObserver>,
    ) -> Result<ReadSession> {
        let _operation = self.open_operation()?;
        Ok(ReadSession::from_live_database_with_progress(
            self.reader_handle(),
            limits,
            progress,
        ))
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn read_session_with_observer_for_test(
        &self,
        limits: ReadLimits,
        observer: Arc<dyn ReadSessionTestObserver>,
    ) -> Result<ReadSession> {
        let _operation = self.open_operation()?;
        Ok(ReadSession::from_live_database_with_observer(
            self.reader_handle(),
            limits,
            observer,
        ))
    }

    /// Open a bounded reading session whose reads deadline against a clock the
    /// caller drives. Live-database sessions otherwise hardcode the monotonic
    /// clock, which leaves their deadline behaviour unreachable from a test;
    /// the owner route already honours a supplied clock.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn read_session_with_clock_for_test(
        &self,
        limits: ReadLimits,
        clock: Arc<dyn contextdb_core::read_contract::DeadlineClock>,
    ) -> Result<ReadSession> {
        let _operation = self.open_operation()?;
        Ok(ReadSession::from_live_database_with_clock(
            self.reader_handle(),
            limits,
            clock,
        ))
    }

    /// Attach an observer that can be reached only through the bounded
    /// executor probe used by this live-state session.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn read_session_with_kernel_observer_for_test(
        &self,
        limits: ReadLimits,
        observer: Arc<dyn ReadKernelTestObserver>,
    ) -> Result<ReadSession> {
        let _operation = self.open_operation()?;
        Ok(ReadSession::from_live_database_with_kernel_observer(
            self.reader_handle(),
            limits,
            observer,
        ))
    }

    /// What this open is holding, read without selecting a route.
    ///
    /// A live database IS the owner, so it can answer the same question a
    /// reader's selected route answers, without dialling its own channel: the
    /// channel identity it is reachable at, the owner-side counts, and whether
    /// each resource an open acquires is still held. Reading it never counts
    /// as a channel operation or a backend open, which is what lets a proof
    /// tell an in-process dispatch from one that went through the channel.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn read_route_resources_for_test(&self) -> Result<ReadRouteResourceSnapshot> {
        let observed = crate::read_probe::observed();
        let owner = self
            .owner_read_service
            .as_ref()
            .map(|service| service.resources());
        Ok(ReadRouteResourceSnapshot {
            channel_identity: self.channel_identity_for_test(),
            // What THIS open started, not what the process has started. A
            // count of every owner service anywhere in the process answers a
            // different question than the one a caller asking about this
            // database is asking, and drifts as soon as anything else opens.
            owner_services_started: u64::from(self.owner_read_service.is_some()),
            // This handle reaches its own stores directly. It has selected no
            // route, so it has spoken over no channel and opened no direct
            // backend, and asking it what it holds must not change that.
            local_channel_operations: 0,
            direct_backend_opens: 0,
            bounded_source_items_completed: observed.bounded_source_touches,
            active_owner_slots: owner.as_ref().map(|owner| owner.active_slots).unwrap_or(0),
            active_cursors: owner.as_ref().map(|owner| owner.cursor_count).unwrap_or(0),
            open_registry_owned: self.open_registry_path.lock().is_some(),
            persistence_owned: self.persistence.is_some(),
            blob_repository_owned: self.resource_owner,
            plugin_open: self.resource_owner,
            snapshot_registry_owned: self.resource_owner,
            memory_accountant_owned: self.resource_owner,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn live_owner_read_service_count_for_test() -> usize {
        crate::owner_read::OwnerReadService::live_service_count_for_test()
    }

    /// Whether this process still holds the store at `path`.
    ///
    /// The open registry is the one place that answers this: an entry stands
    /// for as long as some handle — or the finalizer still winding one down —
    /// owns the store, and disappears when the last of them lets go.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn store_is_retained_by_this_process_for_test(path: &Path) -> bool {
        let Ok(canonical) = canonical_database_path(path) else {
            return false;
        };
        // Never wait for the registry: a finalizer that is mid-transition
        // holds this lock while it works, and a caller asking what is still
        // held must not be able to block behind the very release it is
        // watching. A registry nobody can read at this instant is one somebody
        // is still inside, so the store is still this process's.
        match open_file_registry().entries.try_lock() {
            Some(entries) => entries.contains_key(&canonical),
            None => true,
        }
    }

    /// The fixed-length key readers address this STORE by, whether or not this
    /// open is serving it yet. The owner publishes what its kernel does for
    /// its channel under this key, and a reader that dialled the same store
    /// reads it there.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn store_channel_address_for_test(
        &self,
    ) -> Option<contextdb_core::read_contract::ChannelAddress> {
        let persistence = self.persistence.as_ref()?;
        crate::local_transport::derive_channel_address(persistence.path()).ok()
    }

    /// The fixed-length key this store's local channel is reachable at, when
    /// this open is the one serving it.
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn channel_identity_for_test(
        &self,
    ) -> Option<contextdb_core::read_contract::ChannelAddress> {
        let persistence = self.persistence.as_ref()?;
        self.owner_read_service.as_ref()?;
        crate::local_transport::derive_channel_address(persistence.path()).ok()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_common_open_interceptor_for_test<T>(
        interceptor: Arc<dyn DatabaseOpenInterceptor>,
        operation: impl FnOnce() -> T,
    ) -> T {
        let previous = COMMON_OPTIONS_OPEN_INTERCEPTOR.with(|slot| slot.replace(Some(interceptor)));
        let _guard = CommonOptionsOpenInterceptorGuard { previous };
        operation()
    }

    /// Observe the ordinary public execution path without replacing it. The
    /// supplied closure must call the real `Database::execute` door.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn with_read_execution_convergence_observer_for_test<T>(
        &self,
        observer: Arc<dyn ReadExecutionConvergenceObserver>,
        operation: impl FnOnce() -> T,
    ) -> T {
        let previous =
            READ_EXECUTION_CONVERGENCE_OBSERVER.with(|slot| slot.replace(Some(observer)));
        let _guard = ReadExecutionConvergenceObserverGuard { previous };
        operation()
    }

    /// Called only by the shared pull kernel after it consumes the plan chosen
    /// by the active public execution. Kept crate-private so tests cannot mint
    /// a passing pull trace around a separate eager executor.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn observe_pull_kernel_entered_for_test() {
        observe_read_execution_convergence(ReadExecutionConvergenceEvent::PullKernelEntered);
    }

    /// Capture the active public-execute observer as the probe passed into the
    /// shared bounded kernel. Source-touch events can then originate only at
    /// that kernel's production source loops, even if execution moves threads.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn read_execution_kernel_probe_for_test() -> Option<Arc<dyn ExecutionProbe>> {
        READ_EXECUTION_CONVERGENCE_OBSERVER.with(|slot| {
            slot.borrow().as_ref().map(|observer| {
                Arc::new(ReadExecutionKernelProbe {
                    observer: Arc::clone(observer),
                }) as Arc<dyn ExecutionProbe>
            })
        })
    }

    /// Called only after the shared pull kernel reaches its one terminal drain.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn observe_pull_kernel_drained_for_test() {
        observe_read_execution_convergence(ReadExecutionConvergenceEvent::PullKernelDrained);
    }

    /// Stop admitting owner reads and wait for the work already admitted,
    /// through the configured drain deadline. The writer has already frozen
    /// and published quiescence; crossing the deadline keeps its resources
    /// owned so a later close or final drop can finish the drain.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn drain_owner_reads(&self) -> Result<()> {
        let Some(service) = &self.owner_read_service else {
            return Ok(());
        };
        if !self.resource_owner {
            return Ok(());
        }
        service.shutdown_and_drain().map_err(|error| match error {
            crate::owner_read::OwnerReadScaffoldError::Database(error) => error,
            other => Error::Other((other.to_string()).into()),
        })
    }

    /// The writer has crossed its last operation barrier and stopped its
    /// background writers. The still-held companion lock authenticates this
    /// promise until the final owner resource is released.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_quiescent_owner(&self) -> Result<()> {
        if !self.resource_owner {
            return Ok(());
        }
        if let Some(persistence) = &self.persistence {
            persistence.make_writer_quiescent()?;
            persistence.record_owner_read_status(OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::ShutdownDraining),
            })?;
        }
        Ok(())
    }

    /// Settle eligible storage frees before handoff disables all writes.
    /// The operation barrier and joined background writers keep this the
    /// owner's last sequence of durable cleanup commits.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn drain_storage_before_handoff(&self) -> Result<()> {
        if self.resource_owner
            && let Some(persistence) = &self.persistence
        {
            // New owner requests must not capture this publication after its
            // release. An accepted request may still need to capture it, so
            // retain the root until that request drains. Suspended cursors and
            // external readers keep their own independent shared roots.
            let reads_drained = self.owner_read_service.as_ref().is_none_or(|service| {
                service
                    .pause_admission_for_handoff()
                    .is_ok_and(|in_flight| !in_flight)
            });
            if reads_drained {
                self.tx_mgr.release_publication_handle();
            }
            persistence.drain_deferred_frees_before_handoff()?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn observe_quiescent_stop_for_test(
        &self,
        phase: crate::read_session::QuiescentStopPhase,
    ) {
        if let Some(hooks) = &self.owner_read_config.test_hooks
            && let Some(observer) = &hooks.quiescent_stop
        {
            observer(phase);
        }
    }

    /// Ask this database's own owner handler a question from inside the
    /// process that holds it.
    ///
    /// The embedded caller is the owner, so it reaches the handler directly:
    /// no local channel, no authentication, and no admission slot, which is
    /// why a saturated owner still answers its own process. A store with no
    /// running owner service, or one configured without a handler, is told so
    /// plainly rather than being made to wait.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn request_owner_in_process(
        &self,
        namespace: &str,
        request: &[u8],
    ) -> Result<Vec<u8>> {
        let Some(service) = &self.owner_read_service else {
            return Err(Error::ReadFailure(
                contextdb_core::read_contract::ReadFailure::new(
                    contextdb_core::read_contract::ReadFailureKind::OwnerNotRunning,
                    contextdb_core::read_contract::ReadFailureDetail::None,
                )
                .expect("an owner-not-running refusal carries no further detail"),
            ));
        };
        service
            .request_in_process(namespace, request)
            .map_err(|error| match error {
                crate::owner_read::OwnerReadScaffoldError::Database(error) => error,
                crate::owner_read::OwnerReadScaffoldError::Refused(failure) => {
                    Error::ReadFailure(failure)
                }
                other => Error::Other((other.to_string()).into()),
            })
    }

    /// Stop serving owner reads, without waiting for what is already running.
    ///
    /// Answers whether the resources this open is holding may be released now:
    /// work still in flight is still using them.
    pub(super) fn stop_owner_reads_without_waiting(&self) -> bool {
        let Some(service) = &self.owner_read_service else {
            return true;
        };
        if !self.resource_owner {
            return true;
        }
        match service.begin_graceful_shutdown_without_wait() {
            Ok(still_in_flight) => !still_in_flight,
            Err(_) => false,
        }
    }

    /// How many request frames this handle's owner service has taken off a
    /// connection since it started, or `None` when this handle is not serving
    /// one. Counted where the frame is read and before anything is decided
    /// about it, so it answers whether a statement reached the owner at all --
    /// which no other meter can, because a request the owner refuses and a
    /// request that was never sent leave the same trace everywhere else.
    /// Every accepted frame counts, an owner-status probe and an interrupt
    /// included, so a caller compares a reading taken immediately before with
    /// one taken immediately after.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn owner_received_request_count_for_test(&self) -> Option<u64> {
        self.owner_read_service
            .as_ref()
            .map(|service| service.received_request_count())
    }

    /// The kernel observer this STORE carries, if one was attached when it was
    /// opened. The owner service asks for it because the work a channel reader
    /// requests runs on the owner's own thread, where a reader's thread-local
    /// observer cannot reach.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn kernel_observer_for_test(
        &self,
    ) -> Option<Arc<dyn crate::read_session::ReadKernelTestObserver>> {
        self.kernel_observer.clone()
    }

    /// Report the current owner-read lifecycle state without opening a route.
    pub fn owner_read_status(&self) -> OwnerReadStatus {
        if self.persistence.is_none() {
            return OwnerReadStatus {
                state: OwnerServingState::NotApplicable,
                reason: None,
            };
        }
        if let Some(service) = &self.owner_read_service {
            return service.status();
        }
        if !self.owner_read_config.enabled {
            return OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            };
        }
        if let Some(reason) = &self.owner_read_startup_failure {
            return OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::StartupFailure(reason.clone())),
            };
        }
        owner_read_not_implemented_status()
    }

    /// Force-recreate a file-backed store while owning its normal open lock
    /// for the complete destructive operation. A live owner is reported as
    /// `DatabaseLocked` before either the database or its coordination lock is
    /// changed; after the owner exits the same operation creates a fresh,
    /// current-format root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn force_reset(path: impl AsRef<Path>) -> Result<()> {
        let writer = match DatabaseOpenOptions::default().memory_limit {
            Some(bytes) => MemoryAccountant::writer_budget(bytes)?,
            None => MemoryAccountant::writer_no_limit()?,
        };
        let _operation =
            crate::read_image_memory::ImageMemory::enter_writer_operation(writer.accountant())?;
        let canonical_path = canonical_database_path(path.as_ref())?;
        let registry_reservation = OpenRegistryReservation::acquire(canonical_path.clone())?;
        let persistence =
            RedbPersistence::recreate_with_owner(&canonical_path, writer.accountant())?;
        persistence.close();
        drop(registry_reservation);
        Ok(())
    }

    /// The durable fabric identity kept beside this database, when this
    /// instance owns a file-backed store. This stays crate-private so callers
    /// cannot use the database as a general persistence-path discovery API.
    pub(crate) fn sync_identity_path(&self) -> Option<PathBuf> {
        let persistence = self.persistence.as_ref()?;
        let mut file_name = persistence.path().file_name()?.to_os_string();
        file_name.push(".fabric-identity.key");
        Some(persistence.path().with_file_name(file_name))
    }

    /// Internal media adapter attachment. The returned repository is the
    /// same engine-owned instance shared by scoped database handles.
    #[doc(hidden)]
    pub(crate) fn blob_repository(&self) -> Arc<BlobRepository> {
        let _operation = self.assert_open_operation();
        self.blob_repository.clone()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn wait_until_authoritative_purge_waits_for_blob_hash_for_test(
        &self,
        hash: &[u8; 32],
        timeout: Duration,
    ) -> bool {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .wait_until_authoritative_purge_waits_for_hash_for_test(hash, timeout)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_with_contexts<P: AsRef<Path>>(
        path: P,
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Result<Self> {
        Self::open_with_constraints(path, Some(contexts), None, None)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn open_memory_with_contexts(
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Self {
        Self::open_memory_with_constraints(Some(contexts), None, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_with_scope_labels<P: AsRef<Path>>(
        path: P,
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Result<Self> {
        Self::open_with_constraints(path, None, Some(labels), None)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn open_memory_with_scope_labels(
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Self {
        Self::open_memory_with_constraints(None, Some(labels), None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_as_principal<P: AsRef<Path>>(
        path: P,
        principal: contextdb_core::types::Principal,
    ) -> Result<Self> {
        Self::open_with_constraints(path, None, None, Some(principal))
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn open_memory_as_principal(principal: contextdb_core::types::Principal) -> Self {
        Self::open_memory_with_constraints(None, None, Some(principal))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn open_with_constraints<P: AsRef<Path>>(
        path: P,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Result<Self> {
        Self::open_builtin_with_options(
            path,
            DatabaseOpenOptions {
                contexts,
                scope_labels,
                principal,
                ..DatabaseOpenOptions::default()
            },
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn open_memory_with_constraints(
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Self {
        Self::open_with_constraints(MEMORY_DATABASE_PATH, contexts, scope_labels, principal)
            .expect("failed to open in-memory database with access constraints")
    }

    pub fn scoped_with_contexts(
        &self,
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Self {
        self.scoped_with_constraints(Some(contexts), None, None)
    }

    /// The handle one read session's DECLARATION reads through, or the reason
    /// this writer will not serve that declaration at all.
    ///
    /// Contexts and scope labels are sets, so a declaration over them is an
    /// INTERSECTION with what this writer may itself see and can only take
    /// rows away. Identities are not sets: `Agent("service")` and
    /// `Agent("tenant-a")` hold whatever grants each was given and neither is
    /// inside the other, so there is no intersection to serve. So the
    /// principal axis has its own rule -- a writer that named no identity
    /// HONORS the declared one, a writer that named the same one serves it
    /// unchanged, and a writer that named a DIFFERENT one refuses, because
    /// keeping its own would answer the reader every row its grants open up,
    /// which is strictly more than the reader declared.
    pub(crate) fn scoped_for_read_declaration(
        &self,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> std::result::Result<Self, DeclaredReadRefusal> {
        if let (Some(declared), Some(serving)) =
            (principal.as_ref(), self.access.principal.as_ref())
            && declared != serving
        {
            return Err(DeclaredReadRefusal::PrincipalConflict {
                declared: declared.clone(),
                serving: serving.clone(),
            });
        }
        Ok(self.scoped_with_constraints(contexts, scope_labels, principal))
    }

    /// A handle for a caller who is going to READ this store.
    ///
    /// The store stays alive for as long as one of these does: a reader that
    /// is mid-request, or holding a suspended cursor, is still looking at the
    /// store after the handle that acquired it has gone away.
    pub(super) fn reader_handle(&self) -> Self {
        let mut handle = self.scoped_with_constraints(None, None, None);
        handle.handle_role = HandleRole::Reader;
        // The reader has no transaction control of its own and gains none
        // here; it only watches the opener's slot, so a read this session
        // serves sees the work the session that opened it has staged, and
        // stops seeing it the moment that session commits or rolls back.
        handle.origin_session_tx = Some(Arc::clone(&self.session_tx));
        self.finalization.note_new_holder();
        handle
    }

    /// Keep the store's deferred release behind one owner-side request or
    /// cursor without manufacturing another database interface for it.
    pub(crate) fn hold_for_owner_read(&self) -> OwnerReadStoreHold {
        self.finalization.note_new_holder();
        OwnerReadStoreHold {
            finalization: Arc::clone(&self.finalization),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_loaded(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        writer_budget: WriterBudget,
        startup_disk_limit: Option<u64>,
        open_disposition: OpenDisposition,
        runtime_dir: Option<&Path>,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        host_construction: DatabaseHostConstruction,
    ) -> Result<Self> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
            writer_budget.accountant(),
            read_memory.clone(),
        )?
        .enter();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_writable_database_open();
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let canonical_path = canonical_database_path(path.as_ref())?;
        // Redb's page cache is process-resident memory this store holds for
        // as long as it is open, and it used to take a flat 8 MiB whatever
        // the operator declared -- most of a small budget, all of a tiny one,
        // and never once consulted against the limit. It is the same figure
        // the read path has always used, so both sides of the store answer
        // one rule, and it is charged here so a declared limit covers it.
        // Redb's page cache is sized here because the size has to be handed
        // to Redb as it opens. It is CHARGED further in, once the store's own
        // persisted `MEMORY_LIMIT` has settled the same writer record, so
        // startup and retained ownership keep their domain association.
        let cache_bytes = crate::persistence::redb_writer_cache_bytes_for_limit(
            writer_budget.accountant().usage().limit,
        );
        let (registry_reservation, persistence) = acquire_registry_and_persistence(
            &canonical_path,
            open_disposition,
            cache_bytes,
            runtime_dir,
            writer_budget.accountant(),
        )?;
        Self::open_loaded_from_persistence(
            registry_reservation,
            persistence,
            plugin,
            writer_budget,
            startup_disk_limit,
            &mut vector_copy_owner,
            read_memory,
            declared_read_memory,
            host_construction,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_loaded_from_persistence(
        registry_reservation: OpenRegistryReservation,
        persistence: Arc<RedbPersistence>,
        plugin: Arc<dyn DatabasePlugin>,
        writer_budget: WriterBudget,
        startup_disk_limit: Option<u64>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
        host_construction: DatabaseHostConstruction,
    ) -> Result<Self> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
            writer_budget.accountant(),
            read_memory.clone(),
        )?
        .enter();
        let accountant = writer_budget.accountant();
        let vector_accountant = writer_budget.vector_accountant();
        persistence.require_current_content_identity()?;
        let persisted_storage_compaction = persistence
            .load_config_value::<StorageCompactionPolicy>(STORAGE_COMPACTION_CONFIG_KEY)?;
        let persisted_maintenance_poll_interval_ms = persistence
            .load_config_value::<u64>(MAINTENANCE_POLL_INTERVAL_CONFIG_KEY)?
            .filter(|milliseconds| *milliseconds != 0)
            .unwrap_or(MAINTENANCE_TICK.as_millis() as u64);
        let persisted_vector_serving_cache_bytes = persistence
            .load_config_value::<u64>(VECTOR_SERVING_CACHE_CONFIG_KEY)?
            .unwrap_or(0);
        if let Some(persisted) = persistence.load_config_value::<usize>("memory_limit")? {
            let startup = accountant.usage();
            if let Some(ceiling) = startup.startup_ceiling {
                accountant.set_budget(Some(persisted.min(ceiling)))?;
            } else if startup.limit.is_none() {
                accountant.set_budget(Some(persisted))?;
            }
        }
        // Proof maintenance is database memory like any other: it admits what
        // it allocates through this database's own accountant, so a write whose
        // proof cannot fit the configured budget receives the existing typed
        // refusal instead of allocating anyway. Attached after the persisted
        // memory limit has settled, so the accountant here is the one the
        // composite store and every other subsystem share.
        persistence.attach_memory_accountant(accountant.clone());
        persistence.restore_storage_compaction()?;
        // The persisted effective limit sizes the actual native cache before
        // source roots are published. Charge that same reserve, including any
        // page whose final holder has not released it during startup eviction.
        // An open is a boundary: the cache is sized from the process ceiling as
        // it stands now.
        crate::memory_accounting::observe_inherited_process_ceiling();
        // Preserve the existing startup retained-charge rule for declared
        // writers and process admission for undeclared cache residue. Genuine
        // temporary invoices bridge old/new index and allocator-window backing
        // until this same original fixed reserve owns the settled cache.
        persistence.settle_and_retain_writer_cache_budget(
            &accountant,
            crate::persistence::redb_writer_cache_bytes_for_limit(accountant.usage().limit),
        )?;

        let persisted_disk_limit = persistence.load_config_value::<u64>("disk_limit")?;
        let startup_disk_ceiling = startup_disk_limit;
        let effective_disk_limit = match (persisted_disk_limit, startup_disk_limit) {
            (Some(persisted), Some(ceiling)) => Some(persisted.min(ceiling)),
            (Some(persisted), None) => Some(persisted),
            (None, Some(ceiling)) => Some(ceiling),
            (None, None) => None,
        };
        persistence.set_disk_limit(effective_disk_limit)?;

        let all_meta = persistence.load_all_table_meta()?;
        // A `TableMeta`/`ColumnDef` payload with fewer fields than the required
        // schema layout refuses the whole root. No opener reads past this
        // refusal or rewrites the root to make it readable.
        if persistence.used_legacy_table_meta_layout() {
            return Err(Error::LegacyVectorStoreDetected {
                found_format_marker: format!(
                    "{} (unsupported table/column schema layout)",
                    crate::persistence::CURRENT_FORMAT_VERSION
                ),
                expected_release: crate::persistence::CURRENT_FORMAT_VERSION.to_string(),
            });
        }

        let relational = Arc::new(RelationalStore::new());
        relational.enable_disk_backed_committed_bodies();
        for (name, meta) in &all_meta {
            let mut runtime_meta = meta.clone();
            let user_indexes = runtime_meta
                .indexes
                .iter()
                .filter(|index| index.kind == IndexKind::UserDeclared)
                .cloned()
                .collect::<Vec<_>>();
            runtime_meta.indexes = crate::executor::auto_indexes_for_table_meta(&runtime_meta);
            runtime_meta.indexes.extend(user_indexes);
            relational.create_table(name, runtime_meta.clone());
            // Register EVERY index declared in TableMeta.indexes — this
            // includes auto-indexes (kind=Auto) synthesized at CREATE TABLE
            // time AND user-declared indexes (kind=UserDeclared).
            for decl in &runtime_meta.indexes {
                if decl.kind == IndexKind::Auto {
                    relational.create_exact_index_storage(name, &decl.name, decl.columns.clone());
                } else {
                    relational.create_index_storage_with_budget(
                        name,
                        &decl.name,
                        decl.columns.clone(),
                        accountant.read_budget()?,
                    );
                }
            }
        }
        // Committed rows and provenance are read from the published root.
        // Opening installs only schema and index descriptors.

        let graph_source = Arc::new(writer_read::ScopedGraphSource::new());
        let graph = Arc::new(GraphStore::with_source(graph_source.clone()));

        let vector = Arc::new(VectorStore::new());
        register_vector_layouts(
            &vector,
            &all_meta,
            vector_accountant.clone(),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let loaded_ddl_log = Vec::new();
        // The loaders registered here and the handle built below share ONE cell,
        // so a partition faulted in after `SET VECTOR_SERVING_CACHE` sizes from
        // the value in force then rather than the one read at open.
        let vector_serving_cache_cell = Arc::new(AtomicU64::new(
            persistence
                .load_config_value::<u64>(VECTOR_SERVING_CACHE_CONFIG_KEY)?
                .unwrap_or(0),
        ));

        let loaded_change_log = Vec::new();
        // Recovery uses durable end keys and maintained identity floors. It
        // must not depend on the resident projections or enumerate retained
        // versions to decide which identities a new writer may allocate.
        let open_bounds = persistence.writer_open_bounds()?;
        let max_tx = open_bounds.max_tx;
        // Every durable mutation door maintains the recovery identity floors.
        let max_row_id = open_bounds.max_row_id;
        let max_lsn = open_bounds.max_lsn;
        relational.set_next_row_id(RowId(max_row_id.0.saturating_add(1)));

        let (initial_table_index, initial_lsn_refcounts) =
            build_change_log_aux_indexes(&loaded_change_log);
        // A reopen re-charges what it rehydrates: the log and the commit
        // index the store loads are memory it holds from this moment, exactly
        // as the entries it appends later are.
        accountant.charge_retained(crate::composite_store::change_log_retained_bytes(
            &loaded_change_log,
        ));
        let change_log = Arc::new(RwLock::new(loaded_change_log));
        let change_log_table_index = Arc::new(RwLock::new(initial_table_index));
        let change_log_lsn_refcounts = Arc::new(RwLock::new(initial_lsn_refcounts));
        let ddl_log = Arc::new(RwLock::new(loaded_ddl_log));
        let received_schema_stages = schema_stage_registry::ReceivedRegistry::new_arc(&accountant)?;
        let pending_local_schema_stages =
            schema_stage_registry::PendingRegistry::new_arc(&accountant)?;
        let local_schema_stages = schema_stage_registry::LocalRegistry::new_arc(&accountant)?;
        let snapshot_registry = Arc::new(SnapshotFloorRegistry::default());
        let vector_partition_snapshot_stages = Arc::new(Mutex::new(HashMap::new()));
        let apply_phase_pause = Arc::new(ApplyPhasePause::new());
        let event_bus = Arc::new(EventBusState::new());
        // Prepare from the actual writer before finalization consumes it.
        let owned_assembly = host_construction.prepare(event_bus.clone(), &writer_budget)?;
        let finalization = StoreFinalization::prepare(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            DatabaseAssemblyBudget::Writer(writer_budget),
        )?;
        finalization.defer_loaded_state_owner();
        let mut composite = CompositeStore::new_with_apply_phase_pause(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            change_log_table_index.clone(),
            change_log_lsn_refcounts.clone(),
            ddl_log.clone(),
            vector_partition_snapshot_stages.clone(),
            accountant.clone(),
            apply_phase_pause.clone(),
        );
        composite.memory_row_retirements = false;
        let retention_deferred_edge_nodes = composite.row_retirements.clone();
        let local_erasure_stages = composite.local_erasure_stages.clone();
        let received_arriving_table_meta = composite.received_arriving_table_meta.clone();
        let trigger = Arc::new(TriggerState::new());
        let initial_root = match owned_assembly.as_ref() {
            Some(assembly) => assembly
                .events
                .with_owned_persistence_original(|original| {
                    let control = persistence.reserve_owned_writer_snapshot(original)?;
                    persistence.prepare_native_writer_snapshot(
                        control,
                        SnapshotId::from_tx(max_tx),
                        max_lsn,
                    )
                })?,
            None => persistence
                .initialize_writer_snapshot(SnapshotId::from_tx(max_tx), max_lsn)?
                .into(),
        };
        let persistent = PersistentCompositeStore::new(
            composite,
            persistence.clone(),
            initial_root.clone(),
            Some(event_bus.clone()),
            Some(trigger.clone()),
            received_schema_stages.clone(),
            local_schema_stages.clone(),
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        let commit_source_reference_charge = persistent.source_control_bytes_for_test();
        let tx_mgr = match owned_assembly {
            Some(assembly) => DatabaseTransactionHost::prepare_owned_persistent(
                persistent,
                assembly,
                persistence.clone(),
                initial_root,
                TxId(max_tx.0.saturating_add(1)),
                Lsn(max_lsn.0.saturating_add(1)),
                max_tx,
            )?,
            None => {
                let store: DynStore = AdmittedDynStore::prepare(persistent, &accountant)?;
                DatabaseTransactionHost::legacy(Arc::new(TransactionManager::new_with_directory(
                    store,
                    TxId(max_tx.0.saturating_add(1)),
                    Lsn(max_lsn.0.saturating_add(1)),
                    max_tx,
                    persistence.clone(),
                    Some(initial_root.into_legacy()),
                )))
            }
        };
        graph_source.bind(&tx_mgr);
        register_dormant_raw_vector_partitions(
            &vector,
            &tx_mgr,
            &persistence,
            &accountant,
            vector_copy_owner,
        )?;
        let blob_repository = BlobRepository::shared(persistence.clone());

        let db = Self::build_db(
            tx_mgr,
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
            ddl_log,
            received_schema_stages,
            local_erasure_stages,
            received_arriving_table_meta,
            pending_local_schema_stages,
            local_schema_stages,
            snapshot_registry,
            vector_partition_snapshot_stages,
            retention_deferred_edge_nodes,
            Some(persistence),
            blob_repository,
            Some(registry_reservation.disarm()),
            apply_phase_pause,
            plugin,
            finalization,
            vector_serving_cache_cell.clone(),
            effective_disk_limit,
            startup_disk_ceiling,
            event_bus,
            trigger,
            declared_read_memory,
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        let db = {
            let mut db = db;
            db.commit_source_reference_charge = commit_source_reference_charge;
            db
        };
        db.maintenance_poll_interval_ms
            .store(persisted_maintenance_poll_interval_ms, Ordering::SeqCst);
        db.vector_serving_cache_bytes
            .store(persisted_vector_serving_cache_bytes, Ordering::SeqCst);
        if let Some(declared) = persisted_storage_compaction {
            // A persisted declaration is held to the same ranges a new one is.
            declared.validate()?;
            db.storage_compaction_policy.lock().declared = declared;
        }
        if let Some(persistence) = db.persistence.as_ref() {
            persistence.set_growth_step_ratio(db.storage_compaction_policy().stop_ratio())?;
        }

        for meta in all_meta.values() {
            if !meta.dag_edge_types.is_empty() {
                db.graph.register_dag_edge_types(&meta.dag_edge_types);
            }
        }
        db.rebuild_rank_formula_cache_from_meta(&all_meta)?;

        db.account_loaded_state()?;
        // In-flight native builds restore their construction floors before
        // any maintenance or cleanup can run against this handle.
        db.restore_native_construction_floors()?;
        db.load_cron_state_from_persistence()?;
        db.load_event_bus_state_from_persistence()?;
        db.load_trigger_state_from_persistence()?;

        Ok(db)
    }

    /// Declare who this handle reads as and which part of the store it is
    /// looking at, after the handle exists.
    ///
    /// A writable open says the same thing in its options and this is the same
    /// value it lands in, for the one caller that cannot: a committed image is
    /// hydrated and projected before a read session's declaration can be
    /// applied to it, so the declaration arrives here instead. Everything
    /// downstream is identical -- the row gate reads this and nothing else.
    pub(crate) fn declare_read_access(
        &mut self,
        contexts: Option<BTreeSet<ContextId>>,
        scope_labels: Option<BTreeSet<ScopeLabel>>,
        principal: Option<Principal>,
    ) {
        self.access = AccessConstraints {
            contexts,
            scope_labels,
            principal,
        };
    }

    /// A handle over a committed image that was already decoded and whose
    /// source file is closed.
    ///
    /// This is the reading counterpart of `open_loaded_from_persistence`: the
    /// same runtime stores, the same counters, and therefore the same planner
    /// and the same physical decisions -- assembled from values instead of
    /// from a mutable source. There is no writable store behind the composite
    /// and no media repository because the image carries no media. A validated
    /// dormant vector descriptor retains the registered descriptor/root source
    /// for its immutable graph; the graph body is loaded only if a later
    /// indexed query selects that route.
    /// Every repair the writable path performs while loading is absent by
    /// construction: a store that would need one was refused before the image
    /// was ever built.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_committed_image(
        parts: crate::persistence::ReadPersistenceImageParts,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: crate::memory_accounting::AccountantHandle,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
    ) -> Result<Self> {
        parts.open_context.checkpoint()?;
        let read_memory = parts.open_context.read_memory();
        let decode_memory = crate::read_image_memory::ImageMemory::operation_reader(
            accountant.clone(),
            read_memory.clone(),
        )?;
        let _decode_scope = decode_memory.enter();
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        #[cfg(any(test, feature = "test-seams"))]
        let vector_adapter_before = accountant.usage().used;
        let vector_accountant: contextdb_vector::MemoryBudgetOwner = accountant.vector_budget()?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_commit_directory_vector_adapter_admission(
            &accountant,
            vector_adapter_before,
        );
        let crate::persistence::ReadPersistenceImageParts {
            open_context,
            content_identity: _,
            memory: _image_memory,
            table_meta: all_meta,
            read_bounds,
            vector_entries: _,
            raw_vector_memberships: _,
            current_vectors: _,
            sync_source_lsns,
            sync_source_kinds,
            change_log: loaded_change_log,
            ddl_log: loaded_ddl_log,
            commit_index,
            history_on_request,
            config_values,
            sink_audit: _,
            trigger_audit,
            trigger_audit_stamps: _,
            sink_queues,
            sink_queue_extents,
            retained_source,
            source_path: _,
            store_file_bytes,
        } = parts;
        // The decoded configuration moves into the startup state, which holds
        // the one copy the image keeps; its bytes were admitted as they were
        // decoded and stay charged to the image for as long as it lives.
        let startup = Arc::new(CommittedImageStartupState {
            config_values: config_values.into_iter().collect(),
            config_source: retained_source.clone(),
            sink_queues,
            sink_queue_extents,
            trigger_audit,
        });
        let config_values = &startup.config_values;

        let config_value =
            |key: &str| -> Option<&[u8]> { config_values.get(key).map(|bytes| bytes.as_slice()) };
        if let Some(bytes) = config_value("memory_limit") {
            let persisted =
                crate::persistence::RedbPersistence::decode_config_value::<usize>(bytes)?;
            let startup = accountant.usage();
            if let Some(ceiling) = startup.startup_ceiling {
                accountant.set_budget(Some(persisted.min(ceiling)))?;
            } else if startup.limit.is_none() {
                // Restore the live scalar setting on the original controls;
                // do not replace their payer, counter or runtime-only ceiling.
                accountant.set_budget(Some(persisted))?;
            }
        }
        let disk_limit = match config_value("disk_limit") {
            Some(bytes) => Some(crate::persistence::RedbPersistence::decode_config_value::<
                u64,
            >(bytes)?),
            None => None,
        };
        let persisted_storage_compaction = config_value(STORAGE_COMPACTION_CONFIG_KEY)
            .map(
                crate::persistence::RedbPersistence::decode_config_value::<StorageCompactionPolicy>,
            )
            .transpose()?;
        let persisted_maintenance_poll_interval_ms =
            config_value(MAINTENANCE_POLL_INTERVAL_CONFIG_KEY)
                .map(crate::persistence::RedbPersistence::decode_config_value::<u64>)
                .transpose()?
                .filter(|milliseconds| *milliseconds != 0)
                .unwrap_or(MAINTENANCE_TICK.as_millis() as u64);
        let vector_serving_cache_cell = Arc::new(AtomicU64::new(0));
        let persisted_vector_serving_cache_bytes = config_value(VECTOR_SERVING_CACHE_CONFIG_KEY)
            .map(crate::persistence::RedbPersistence::decode_config_value::<u64>)
            .transpose()?
            .unwrap_or(0);

        let relational = Arc::new(RelationalStore::new());
        // The reader holds no postings of its own, so it is priced by the
        // filters it physically holds, not by every declared index.
        relational.price_exact_filters_by_presence();
        for (name, meta) in &all_meta {
            let mut runtime_meta = meta.clone();
            let user_indexes = runtime_meta
                .indexes
                .iter()
                .filter(|index| index.kind == IndexKind::UserDeclared)
                .cloned()
                .collect::<Vec<_>>();
            runtime_meta.indexes = crate::executor::auto_indexes_for_table_meta(&runtime_meta);
            runtime_meta.indexes.extend(user_indexes);
            relational.create_table(name, runtime_meta.clone());
            for decl in &runtime_meta.indexes {
                if decl.kind == IndexKind::Auto {
                    relational.create_exact_index_storage(name, &decl.name, decl.columns.clone());
                } else {
                    relational.create_index_storage_with_budget(
                        name,
                        &decl.name,
                        decl.columns.clone(),
                        accountant.read_budget()?,
                    );
                }
            }
        }
        relational.replace_sync_source_lsns(sync_source_lsns);
        relational.replace_sync_source_kinds(sync_source_kinds);

        #[cfg(any(test, feature = "test-seams"))]
        let graph_source_funding_before =
            (_image_memory.held(), _image_memory.accountant.usage().used);
        _image_memory.reserve(std::mem::size_of::<
            crate::persistence::graph_read::RetainedGraphSource,
        >())?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_commit_directory_source_registration_funding(
            &_image_memory,
            graph_source_funding_before,
        );
        let graph = Arc::new(GraphStore::with_source(Arc::new(
            crate::persistence::graph_read::RetainedGraphSource(retained_source.clone()),
        )));

        let vector = Arc::new(VectorStore::new());
        register_vector_layouts(
            &vector,
            &all_meta,
            vector_accountant.clone(),
            &mut vector_copy_owner,
            read_memory.clone(),
        )?;
        register_retained_raw_vector_partitions(
            &vector,
            &retained_source,
            &accountant,
            &mut vector_copy_owner,
        )?;

        let max_row_id = relational.max_row_id();
        // History left in the store contributes its stored maxima; the
        // commit index is transaction-monotone, so its last entry carries both.
        let stored_commit_last = history_on_request
            .as_ref()
            .and_then(|history| history.commit_index_last);
        let max_tx = max_tx_across_all(&relational, &graph)
            .max(commit_index.values().copied().max().unwrap_or(TxId(0)))
            .max(stored_commit_last.map_or(TxId(0), |(_, tx)| tx))
            .max(TxId(read_bounds.identity_tx_floor));
        let commit_index_max_lsn = commit_index
            .keys()
            .next_back()
            .copied()
            .unwrap_or(Lsn(0))
            .max(stored_commit_last.map_or(Lsn(0), |(lsn, _)| lsn));
        let ddl_max_lsn = loaded_ddl_log
            .iter()
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(Lsn(0))
            .max(
                history_on_request
                    .as_ref()
                    .and_then(|history| history.ddl_last_lsn)
                    .unwrap_or(Lsn(0)),
            );
        // Every lineage write raises the persisted row-id and LSN floors in
        // its own transaction (`raise_config_identity_floors_in_write`), the
        // same floors a writable open trusts; no lineage record is decoded.
        let max_row_id = max_row_id.max(RowId(read_bounds.external_row_id_floor));
        let max_lsn = max_lsn_across_all(&relational, &graph)
            .max(commit_index_max_lsn)
            .max(ddl_max_lsn)
            .max(Lsn(read_bounds.source_lsn_floor));
        relational.set_next_row_id(RowId(max_row_id.0.saturating_add(1)));

        let (initial_table_index, initial_lsn_refcounts) =
            build_change_log_aux_indexes(&loaded_change_log);
        // A committed READ image pays nothing at the writer's retention door.
        // The charge the two writable opens take there is for a log that
        // GROWS with an import and for a commit index that gains an entry per
        // commit; an image appends to neither. What it does hold of both was
        // already admitted against this very accountant when it was decoded
        // (`read_image_memory::decode`), so charging it again here taxed a
        // read session's declared ceiling twice for the same bytes -- about
        // 1.28 MB of a 24 MiB ceiling on the reader fixtures, which then
        // refused the open at `load_vector_directories`.
        // `durable_writer_retained_bytes` answers 0 for this handle, so the
        // close and the drop hand back exactly what this open took: nothing.
        let change_log = Arc::new(RwLock::new(loaded_change_log));
        let change_log_table_index = Arc::new(RwLock::new(initial_table_index));
        let change_log_lsn_refcounts = Arc::new(RwLock::new(initial_lsn_refcounts));
        let ddl_log = Arc::new(RwLock::new(loaded_ddl_log));
        let received_schema_stages = schema_stage_registry::ReceivedRegistry::new_arc(&accountant)?;
        let pending_local_schema_stages =
            schema_stage_registry::PendingRegistry::new_arc(&accountant)?;
        let local_schema_stages = schema_stage_registry::LocalRegistry::new_arc(&accountant)?;
        let snapshot_registry = Arc::new(SnapshotFloorRegistry::default());
        let vector_partition_snapshot_stages = Arc::new(Mutex::new(HashMap::new()));
        let apply_phase_pause = Arc::new(ApplyPhasePause::new());
        let finalization = StoreFinalization::prepare(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            DatabaseAssemblyBudget::ReadImage(accountant.clone(), vector_accountant),
        )?;
        finalization.defer_loaded_state_owner();
        let composite = CompositeStore::new_with_apply_phase_pause(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            change_log_table_index.clone(),
            change_log_lsn_refcounts.clone(),
            ddl_log.clone(),
            vector_partition_snapshot_stages.clone(),
            accountant.clone(),
            apply_phase_pause.clone(),
        );
        let retention_deferred_edge_nodes = composite.row_retirements.clone();
        let local_erasure_stages = composite.local_erasure_stages.clone();
        let received_arriving_table_meta = composite.received_arriving_table_meta.clone();
        let store: DynStore = AdmittedDynStore::prepare(composite, &accountant)?;
        let directory_bounds = history_on_request.as_ref().ok_or_else(|| {
            crate::funded_error::corruption_text(
                Ok(read_memory.clone()),
                retained_source.database_path().display(),
                "captured image has no authenticated commit directory bounds".into(),
            )
        })?;
        let directory = crate::persistence::read_commit_directory::RetainedCommitDirectory::new(
            retained_source.clone(),
            directory_bounds,
            &contextdb_tx::ReadCommitContext::new(read_memory.clone(), &|| {
                open_context.checkpoint()
            }),
        )?;
        let tx_mgr = Arc::new(TransactionManager::new_with_captured_directory(
            store,
            TxId(max_tx.0.saturating_add(1)),
            Lsn(max_lsn.0.saturating_add(1)),
            max_tx,
            max_lsn,
            directory,
            read_memory.clone(),
            None,
        ));
        let event_bus = Arc::new(EventBusState::new());
        let trigger = Arc::new(TriggerState::new());

        let mut db = Self::build_db(
            DatabaseTransactionHost::legacy(tx_mgr),
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
            ddl_log,
            received_schema_stages,
            local_erasure_stages,
            received_arriving_table_meta,
            pending_local_schema_stages,
            local_schema_stages,
            snapshot_registry,
            vector_partition_snapshot_stages,
            retention_deferred_edge_nodes,
            None,
            BlobRepository::absent(),
            None,
            apply_phase_pause,
            plugin,
            finalization,
            vector_serving_cache_cell.clone(),
            disk_limit,
            None,
            event_bus,
            trigger,
            declared_read_memory,
        )?;
        db.maintenance_poll_interval_ms
            .store(persisted_maintenance_poll_interval_ms, Ordering::SeqCst);
        db.vector_serving_cache_bytes
            .store(persisted_vector_serving_cache_bytes, Ordering::SeqCst);
        if let Some(declared) = persisted_storage_compaction {
            // A persisted declaration is held to the same ranges a new one is.
            declared.validate()?;
            db.storage_compaction_policy.lock().declared = declared;
        }
        if let Some(persistence) = db.persistence.as_ref() {
            persistence.set_growth_step_ratio(db.storage_compaction_policy().stop_ratio())?;
        }
        // The sealed read image retains its durable destination,
        // so bindings and scoped custody credit agree with the live database.
        *db.retention_sync_peer.lock() = startup
            .config_values
            .get(RETENTION_SYNC_PEER_CONFIG_KEY)
            .map(|bytes| RedbPersistence::decode_config_value_exact::<String>(bytes))
            .transpose()?;
        db.committed_image_startup = Some(startup);
        db.image_history = Some(Arc::new(ImageHistory {
            source: retained_source.clone(),
            load_history: history_on_request.is_some(),
            loaded: Mutex::new(None),
        }));
        *db.finalization.loaded_history.lock() = Some(LoadedHistoryBuffers {
            history: db
                .image_history
                .as_ref()
                .expect("constructed image history")
                .clone(),
            ddl_log: db.ddl_log.clone(),
            change_log: db.change_log.clone(),
            table_index: db.change_log_table_index.clone(),
            lsn_refcounts: db.change_log_lsn_refcounts.clone(),
            _image_memory: _image_memory.clone(),
        });
        db.retained_graph_source = Some(retained_source.clone());
        db.retained_relational_source = Some(retained_source);
        db.read_image_memory = Some(_image_memory);
        // This handle has no persistence to stat, so the length the reader
        // measured of the file it read this image out of travels with the
        // image and answers for it.
        db.image_store_file_bytes = store_file_bytes;

        for meta in all_meta.values() {
            if !meta.dag_edge_types.is_empty() {
                db.graph.register_dag_edge_types(&meta.dag_edge_types);
            }
        }
        db.rebuild_rank_formula_cache_from_meta(&all_meta)?;
        db.account_loaded_state()?;
        db.load_cron_state_from_persistence()?;
        db.load_event_bus_state_from_persistence()?;
        db.load_trigger_state_from_persistence()?;
        open_context.checkpoint()?;

        Ok(db)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_private_working_image(
        plugin: Arc<dyn DatabasePlugin>,
        writer_budget: WriterBudget,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
        transaction_snapshot: Option<(TxId, TxId)>,
        working_graph: Option<FundedWorkingGraphSource>,
        working_vectors: Option<
            crate::persistence::received_working_vector::WorkingVectorCatalogOwner,
        >,
    ) -> Result<Self> {
        Self::open_private_working_image_with_host(
            plugin,
            writer_budget,
            declared_read_memory,
            transaction_snapshot,
            working_graph,
            working_vectors,
            DatabaseHostConstruction::Legacy,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_private_working_image_with_host(
        plugin: Arc<dyn DatabasePlugin>,
        writer_budget: WriterBudget,
        declared_read_memory: crate::vector_read_memory::DeclaredReadRoot,
        transaction_snapshot: Option<(TxId, TxId)>,
        working_graph: Option<FundedWorkingGraphSource>,
        working_vectors: Option<
            crate::persistence::received_working_vector::WorkingVectorCatalogOwner,
        >,
        host_construction: DatabaseHostConstruction,
    ) -> Result<Self> {
        let accountant = writer_budget.accountant();
        let vector_accountant = writer_budget.vector_accountant();
        let relational = Arc::new(RelationalStore::new());
        let graph = Arc::new(match &working_graph {
            Some(owner) => {
                let owned = owner.clone();
                GraphStore::with_funded_source(owned.source, owned.control)
            }
            None => GraphStore::new(),
        });
        let vector = Arc::new(VectorStore::new());
        vector.account_loaded_state(vector_accountant.clone())?;
        let change_log = Arc::new(RwLock::new(Vec::new()));
        let change_log_table_index = Arc::new(RwLock::new(ChangeLogTableIndex::new()));
        let change_log_lsn_refcounts = Arc::new(RwLock::new(ChangeLogLsnRefcounts::new()));
        let ddl_log = Arc::new(RwLock::new(Vec::new()));
        let received_schema_stages = schema_stage_registry::ReceivedRegistry::new_arc(&accountant)?;
        let pending_local_schema_stages =
            schema_stage_registry::PendingRegistry::new_arc(&accountant)?;
        let local_schema_stages = schema_stage_registry::LocalRegistry::new_arc(&accountant)?;
        let snapshot_registry = Arc::new(SnapshotFloorRegistry::default());
        let vector_partition_snapshot_stages = Arc::new(Mutex::new(HashMap::new()));
        let apply_phase_pause = Arc::new(ApplyPhasePause::new());
        let trigger = Arc::new(TriggerState::new());
        let event_bus = Arc::new(EventBusState::new());
        // The companion and its funder come from this store's original writer.
        let owned_assembly = host_construction.prepare(event_bus.clone(), &writer_budget)?;
        let finalization = StoreFinalization::prepare(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            DatabaseAssemblyBudget::Writer(writer_budget),
        )?;
        let composite = CompositeStore::new_with_apply_phase_pause(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            change_log_table_index.clone(),
            change_log_lsn_refcounts.clone(),
            ddl_log.clone(),
            vector_partition_snapshot_stages.clone(),
            accountant.clone(),
            apply_phase_pause.clone(),
        );
        let composite = if working_graph.is_some() {
            composite.with_private_working_graph()
        } else {
            composite
        };
        let retention_deferred_edge_nodes = composite.row_retirements.clone();
        let local_erasure_stages = composite.local_erasure_stages.clone();
        let received_arriving_table_meta = composite.received_arriving_table_meta.clone();
        let tx_mgr = match owned_assembly {
            Some(assembly) => {
                // Captured working-image counters belong to the borrowed host.
                if transaction_snapshot.is_some() {
                    return Err(Error::Other(
                        "owning memory assembly requires fresh counters".into(),
                    ));
                }
                DatabaseTransactionHost::prepare_owned_memory(composite, assembly)?
            }
            None => {
                let store: DynStore = AdmittedDynStore::prepare(composite, &accountant)?;
                DatabaseTransactionHost::legacy(Arc::new(match transaction_snapshot {
                    Some((next_tx, committed)) => {
                        TransactionManager::new_with_counters(store, next_tx, Lsn(1), committed)
                    }
                    None => TransactionManager::new(store),
                }))
            }
        };
        if let Some(source) = &working_graph {
            source.source.bind(&tx_mgr);
        }
        if let Some(catalog) = &working_vectors {
            catalog.bind(tx_mgr.identity())?;
        }
        let blob_repository = BlobRepository::absent();

        let db = Self::build_db(
            tx_mgr,
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
            ddl_log,
            received_schema_stages,
            local_erasure_stages,
            received_arriving_table_meta,
            pending_local_schema_stages,
            local_schema_stages,
            snapshot_registry,
            vector_partition_snapshot_stages,
            retention_deferred_edge_nodes,
            None,
            blob_repository,
            None,
            apply_phase_pause,
            plugin,
            finalization,
            Arc::new(AtomicU64::new(0)),
            None,
            None,
            event_bus,
            trigger,
            declared_read_memory,
        )?;
        Ok(db)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn begin(&self) -> Result<TxId> {
        let _operation = self.open_operation_after_public_tx_control_wait("begin")?;
        self.tx_mgr.try_begin()
    }

    pub(super) fn callback_active_process_wide() -> bool {
        global_callback_active_count().load(Ordering::SeqCst) > 0
    }

    /// The deadline this handle waits for a same-database trigger callback
    /// to go idle. The shipped default governs unless this handle was told
    /// otherwise through the private test seam; nothing in the environment
    /// is consulted.
    pub(super) fn trigger_deadlock_timeout(&self) -> Duration {
        self.trigger_deadlock_timeout_override
            .lock()
            .unwrap_or(DEFAULT_TRIGGER_DEADLOCK_TIMEOUT)
    }

    /// States the deadline THIS handle waits for a same-database trigger
    /// callback to go idle before the typed cross-thread error is due.
    ///
    /// A deadlock-guard journey needs a deadline far shorter than the shipped
    /// one, and the deadline is behavior, so it can only be stated privately,
    /// per handle -- never exported into the process, where it would become a
    /// behavior surface every deployment could reach.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn set_trigger_deadlock_timeout_for_test(&self, timeout: std::time::Duration) {
        *self.trigger_deadlock_timeout_override.lock() = Some(timeout);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_operation_after_public_tx_control_wait(
        &self,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        self.open_operation_after_public_tx_control_wait_for_tx(surface, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_operation_after_public_tx_control_wait_for_tx(
        &self,
        surface: &'static str,
        tx: Option<TxId>,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_reentry_allowed()?;
        if let Some(tx) = tx {
            self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
        }
        self.assert_public_tx_control_cross_thread_allowed_for_surface(surface)?;
        self.open_operation()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_callback_reentry_allowed(&self) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_public_tx_control_cross_thread_allowed_for_surface(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn wait_for_same_db_trigger_callback_idle(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        let timeout = self.trigger_deadlock_timeout();
        let started = Instant::now();
        let mut observed_wait = false;
        let mut guard = self.trigger.wait_lock.lock();
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(closed_database_error());
            }
            self.assert_callback_reentry_allowed()?;
            match self
                .trigger
                .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
            {
                TriggerContention::None => {
                    if surface != "close"
                        && self.trigger.close_waiter_count.load(Ordering::SeqCst) > 0
                    {
                        let _ = self
                            .trigger
                            .waiters
                            .wait_for(&mut guard, Duration::from_millis(1));
                        continue;
                    }
                    return Ok(());
                }
                TriggerContention::SameDb => {
                    if !observed_wait {
                        self.trigger
                            .wait_observed_count
                            .fetch_add(1, Ordering::SeqCst);
                        #[cfg(any(test, feature = "test-seams"))]
                        self.trigger.wait_observed_changed.notify_all();
                        observed_wait = true;
                    }
                }
            }

            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return Err(self.trigger_same_db_deadlock_timeout_error(surface, elapsed));
            }
            let remaining = timeout.saturating_sub(elapsed);
            self.trigger.waiters.wait_for(&mut guard, remaining);
        }
    }

    pub(super) fn trigger_same_db_deadlock_timeout_error(
        &self,
        surface: &'static str,
        waited: Duration,
    ) -> Error {
        self.trigger
            .deadlock_guard_timeout_observed_count
            .fetch_add(1, Ordering::SeqCst);
        self.trigger
            .typed_err_observed_same_db_count
            .fetch_add(1, Ordering::SeqCst);
        let trigger_name = self.trigger.active_trigger_name_for_wait();
        let waited_ms = waited.as_millis() as u64;
        tracing::warn!(
            trigger = %trigger_name,
            trigger_name = %trigger_name,
            waited_ms = waited_ms,
            surface = surface,
            "trigger callback wait exceeded deadlock guard"
        );
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger,
        }
    }

    // Internal engine-managed writes still need callback-active gates, and
    // trigger contention stays scoped to this database so independent embedded
    // handles do not poison each other through process-wide state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn begin_for_internal_write(&self) -> Result<TxId> {
        let _operation = self.open_operation()?;
        self.assert_internal_write_callbacks_allowed()?;
        self.tx_mgr.try_begin()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn begin_for_public_autocommit_write(&self) -> Result<TxId> {
        let _operation = self.open_operation_after_internal_write_wait("execute")?;
        self.tx_mgr.try_begin()
    }

    pub(super) fn rollback_preopened_autocommit_tx(&self, tx: Option<TxId>) {
        if let Some(tx) = tx {
            let _ = self.tx_mgr.rollback_empty_without_commit_lock(tx);
        }
    }

    pub(super) fn enter_sql_write_control_bypass(&self, tx: TxId) -> SqlWriteControlBypassGuard {
        let db_id = self.id;
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| {
            stack.borrow_mut().push((db_id, tx));
        });
        SqlWriteControlBypassGuard { db_id, tx }
    }

    pub(super) fn sql_write_control_bypass_active(&self, tx: TxId) -> bool {
        let db_id = self.id;
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| stack.borrow().contains(&(db_id, tx)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_internal_write_callbacks_allowed(&self) -> Result<()> {
        if !Self::callback_active_process_wide() {
            return Ok(());
        }
        self.assert_callback_reentry_allowed()?;
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_operation_after_internal_write_wait(
        &self,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_reentry_allowed()?;
        self.assert_internal_write_cross_thread_allowed(surface)?;
        self.open_operation()
    }

    pub(super) fn cron_callback_tx_bound_matches(&self, tx: TxId) -> bool {
        let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
        let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
        cron_tx == Some(tx) && cron_db == Some(self.id)
    }

    pub(crate) fn trigger_callback_tx_bound_matches(&self, tx: TxId) -> bool {
        let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
        let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
        trigger_tx == Some(tx) && trigger_db == Some(self.id)
    }

    pub(crate) fn trigger_callback_wallclock(&self) -> Wallclock {
        TRIGGER_CALLBACK_WALLCLOCK
            .with(|slot| slot.get())
            .unwrap_or_else(current_wallclock)
    }

    pub(super) fn callback_tx_bound_matches(&self, tx: TxId) -> bool {
        self.cron_callback_tx_bound_matches(tx) || self.trigger_callback_tx_bound_matches(tx)
    }

    /// This handle's identity. Two handles never share one, including a handle
    /// constructed at an address a dropped handle used to occupy -- which is
    /// the whole reason the identity is not the address.
    pub(crate) fn identity(&self) -> DatabaseId {
        self.id
    }

    /// The same identity as a plain number, for a pin that has to compare many
    /// handles' identities without reaching into the type.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn database_identity_for_test(&self) -> u64 {
        self.id.0
    }

    pub(crate) fn vector_schema_gate_id(&self) -> usize {
        Arc::as_ptr(&self.vector_schema_gates) as usize
    }

    pub(super) fn callback_active_on_current_thread_for_this_db(&self) -> bool {
        let gate_id = self.vector_schema_gate_id();
        (CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && CRON_CALLBACK_VECTOR_SCHEMA_GATE.with(|slot| slot.get()) == Some(gate_id))
            || (TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
                && TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE.with(|slot| slot.get()) == Some(gate_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_callback_tx_bound_write_allowed(&self, tx: TxId) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && !self.cron_callback_tx_bound_matches(tx)
        {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
            && !self.trigger_callback_tx_bound_matches(tx)
        {
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_trigger_callback_tx_not_captured_cross_thread(
        &self,
        tx: TxId,
    ) -> Result<()> {
        if self.trigger.callback_tx_active(tx) && !self.trigger_callback_tx_bound_matches(tx) {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_operation_after_write_control_wait(
        &self,
        tx: TxId,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if self.sql_write_control_bypass_active(tx)
            || !Self::callback_active_process_wide()
            || self.callback_tx_bound_matches(tx)
        {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_tx_bound_write_allowed(tx)?;
        self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
        self.assert_write_control_cross_thread_allowed_for_surface(surface)?;
        self.open_operation()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn open_operation_after_statement_callback_wait(
        &self,
        stmt: &Statement,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_statement_allowed_for_callbacks_for_surface(stmt, surface)?;
        self.open_operation()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_write_control_cross_thread_allowed_for_surface(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn assert_internal_write_cross_thread_allowed(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    /// Test/example helper. Production code MUST use `?` propagation against
    /// the typed callback-active errors. Unwraps the engine's
    /// `CallbackActiveCrossThread` / `CallbackReentry` variants as panics; not
    /// appropriate for code that may run alongside trigger or cron callbacks.
    pub fn begin_or_panic(&self) -> TxId {
        self.begin().unwrap_or_else(|err| panic!("{err}"))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn commit(&self, tx: TxId) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.commit_with_read_memory(tx, &mut vector_copy_owner, read_memory)
    }

    /// Commit and return the finalized transaction id, without a post-commit read.
    /// A transaction that makes no durable change returns `None`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn commit_returning_txid(&self, tx: TxId) -> Result<Option<TxId>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.commit_returning_txid_with_read_memory(tx, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_with_read_memory(
        &self,
        tx: TxId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_returning_txid_with_read_memory(tx, vector_copy_owner, read_memory)
            .map(|_| ())
    }
}
