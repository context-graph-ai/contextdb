use super::super::*;
use crate::vector_ivf_observations::*;
impl Database {
    /// Exercise actual PageStore/WritePages under the ordinary commit boundary
    /// and original writer admission, then abort the enclosing native write.
    /// This cannot prove successful native SQL or source-coverage publication.
    pub fn with_aborting_native_ivf_write_for_test<R>(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        callbacks: NativeIvfDriveCallbacks<'_>,
        run: impl FnOnce(&mut NativeIvfWriteForTest<'_, '_>) -> Result<R>,
    ) -> Result<R> {
        let _operation = self.assert_open_operation();
        self.with_commit_lock(|| {
            let persistence = self.persistence.as_ref().ok_or_else(|| {
                Error::Other("native vector storage drive requires writable persistence".into())
            })?;
            let (snapshot, frontier) = self.tx_mgr.published_read_snapshot();
            persistence.with_aborting_native_ivf_write(
                index,
                tuple,
                snapshot,
                frontier,
                callbacks.checkpoint,
                callbacks.before_distance,
                run,
            )
        })
    }
}

impl Database {
    /// Capture through the established writer snapshot factory.
    pub fn with_native_ivf_read_for_test<R>(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        run: impl FnOnce(&NativeIvfReadForTest<'_>) -> Result<R>,
    ) -> Result<R> {
        let _operation = self.assert_open_operation();
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other("native read drive requires a writer snapshot supplier".into())
        })?;
        let memory = self.declared_reader_memory()?;
        let _owner = self.operation_read_owner(memory.clone())?;
        let root = self
            .native_ivf_root()?
            .ok_or_else(|| Error::Other("native read drive has no captured root".into()))?;
        let maintenance = || self.finalization.accountant.vector_budget();
        let boundary = |run: &mut dyn FnMut() -> Result<()>| {
            self.with_native_retained_selection(&root, &|| Ok(()), run)
        };
        crate::persistence::vector_ivf::with_captured_read(
            &root,
            memory,
            index,
            tuple,
            NativeIvfDriveCallbacks {
                checkpoint: &|| Ok(()),
                before_distance: &|_| Ok(()),
            },
            Some(crate::persistence::vector_ivf::retained::Selection {
                cache: &persistence.native_ivf_cache,
                owner: &maintenance,
                boundary: &boundary,
            }),
            run,
        )
    }
    pub fn native_ivf_serving_descriptor_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<Option<NativeIvfServingDescriptorForTest>> {
        self.with_native_ivf_read_for_test(index, tuple, |scope| scope.serving_descriptor())
    }
}

impl Database {
    pub fn native_ivf_resolved_policy_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<NativeIvfResolvedPolicyForTest> {
        let _operation = self.assert_open_operation();
        let resolved = self.resolve_native_ivf_policy(index, tuple)?;
        Ok(NativeIvfResolvedPolicyForTest {
            policy: resolved.policy,
            training: resolved.training,
            active_lists: resolved.active_lists,
            population: resolved.population,
            expected_vectors: resolved.expected_vectors,
            build_budget_bytes: resolved.build_budget_bytes,
            codebook_budget_bytes: resolved.codebook_budget_bytes,
            workspace_budget_bytes: resolved.workspace_budget_bytes,
            codebook: resolved.codebook,
            workspace: resolved.workspace,
        })
    }
}

impl Database {
    pub fn native_ivf_build_progress_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<NativeIvfBuildProgressForTest> {
        let metadata = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let column = metadata
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::Other("vector inspection column is absent".into()))?;
        self.with_native_ivf_read_for_test(index, tuple, |scope| {
            let mut progress = scope.build_progress()?;
            progress.replacement_trigger = scope.replacement_trigger_for_test(column)?;
            Ok(progress)
        })
    }
    pub fn native_ivf_journal_identity_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        row: RowId,
    ) -> Result<NativeIvfJournalIdentityForTest> {
        self.with_native_ivf_read_for_test(index, tuple, |scope| scope.journal_identity(row))
    }
    pub fn native_ivf_original_owner_for_test(&self) -> Result<usize> {
        let _operation = self.assert_open_operation();
        Ok(self
            .finalization
            .accountant
            .vector_budget()?
            .as_ref()
            .stable_funder_identity())
    }
    pub fn native_ivf_source_identity_for_test(
        &self,
        index: &VectorIndexRef,
        primary_key: i64,
    ) -> Result<contextdb_vector::ivf::RowIdentity> {
        let _operation = self.assert_open_operation();
        let metadata = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let key = metadata
            .columns
            .iter()
            .find(|c| c.primary_key)
            .ok_or_else(|| {
                Error::Other("source selection requires the declared primary key".into())
            })?;
        let rows = self.scan_filter(&index.table, self.snapshot(), &|row| {
            row.values.get(&key.name) == Some(&Value::Int64(primary_key))
        })?;
        if rows.len() != 1 {
            return Err(Error::Other(
                "source selection did not identify exactly one live row".into(),
            ));
        }
        let memory = self.declared_reader_memory()?;
        let mut copies = crate::vector_read_memory::admission(memory.clone());
        let entry = self
            .find_vector_by_index_and_row(index, rows[0].row_id, &mut copies, memory)?
            .ok_or_else(|| Error::Other("selected row has no live vector".into()))?;
        Ok(contextdb_vector::ivf::RowIdentity {
            row: entry.row_id.0,
            commit_lsn: entry.lsn.0,
        })
    }
}

impl Database {
    pub fn native_ivf_set_test_options(&self, options: NativeIvfIndexTestOptions) -> Result<()> {
        let _operation = self.assert_open_operation();
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| Error::Other("native test options require a file store".into()))?;
        self.publish_direct_metadata(|| {
            persistence.flush_config_value("test.native_ivf_options", &options)
        })
    }
}

impl Database {
    pub fn native_ivf_scope_roots_at_snapshot_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        snapshot: SnapshotId,
    ) -> Result<NativeIvfScopeRootsForTest> {
        let memory = self.declared_reader_memory()?;
        let _intent = self.vector_historical_read_intent(snapshot, memory);
        self.with_snapshot_override(snapshot, || {
            self.with_native_ivf_read_for_test(index, tuple, |scope| scope.scope_roots())
        })
    }
    pub fn visit_native_ivf_selected_source_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        snapshot: SnapshotId,
        visit: &mut dyn FnMut(NativeIvfSourceRecordForTest<'_>),
    ) -> Result<()> {
        let memory = self.declared_reader_memory()?;
        let _intent = self.vector_historical_read_intent(snapshot, memory);
        self.with_snapshot_override(snapshot, || {
            self.with_native_ivf_read_for_test(index, tuple, |scope| {
                scope.visit_authoritative_source(visit)
            })
        })
    }
}

impl Database {
    pub fn native_ivf_maintenance_state_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<NativeIvfMaintenanceStateForTest> {
        self.with_native_ivf_read_for_test(index, tuple, |scope| scope.maintenance_state())
    }
    pub fn native_ivf_catalog_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<Option<contextdb_vector::ivf::CatalogState>> {
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let column = meta
            .columns
            .iter()
            .find(|c| c.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        self.with_native_ivf_read_for_test(index, tuple, |scope| {
            scope.catalog(column.vector_engine_policy.max_delta_runs.unwrap_or(2))
        })
    }
    pub fn visit_native_ivf_physical_pages_for_test(
        &self,
        index: &VectorIndexRef,
        visit: &mut dyn FnMut(NativeIvfPhysicalPageForTest<'_>),
    ) -> Result<()> {
        let memory = self.declared_reader_memory()?;
        let mut copies = crate::vector_read_memory::admission(memory);
        let tuples = self.vector_store.partition_keys(index, &mut copies)?;
        for tuple in tuples {
            self.with_native_ivf_read_for_test(index, &tuple, |scope| {
                scope.visit_physical_pages(visit)
            })?;
        }
        Ok(())
    }
    pub fn native_ivf_quantized_record_for_test(
        &self,
        index: &VectorIndexRef,
        vector: &[f32],
    ) -> Result<Vec<u8>> {
        self.with_native_ivf_read_for_test(index, &VectorPartitionKey::unpartitioned(), |scope| {
            scope.quantized_record(vector)
        })
    }
}

impl Database {
    pub fn corrupt_native_ivf_serving_descriptor_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        offset: usize,
        mask: u8,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other("descriptor corruption requires native persistence".into())
        })?;
        self.publish_direct_metadata(|| {
            persistence.corrupt_native_ivf_descriptor(index, tuple, offset, mask)
        })
    }
}

impl Database {
    pub fn corrupt_native_ivf_page_for_test(
        &self,
        storage: u64,
        key: contextdb_vector::ivf::storage::PageKey,
        payload: bool,
        offset: usize,
        mask: u8,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| Error::Other("page corruption requires native persistence".into()))?;
        self.publish_direct_metadata(|| {
            persistence.corrupt_native_ivf_page(storage, key, payload, offset, mask)
        })
    }
}

impl Database {
    /// The observer narrows this existing original accountant only at the
    /// selected real invoice, then restores it after admission or refusal.
    fn with_native_constructor_allowance_for_test<R>(
        &self,
        target: NativeIvfConstructorTargetForTest,
        available: usize,
        observer: std::sync::Arc<dyn contextdb_vector::ivf::IvfConstructorObserverForTest>,
        run: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        use contextdb_vector::ivf::{
            IvfConstructorAllocationForTest as Allocation,
            IvfConstructorObserverForTest as Observer,
        };
        struct Restore {
            accountant: crate::memory_accounting::AccountantHandle,
            limit: Option<usize>,
        }
        impl Drop for Restore {
            fn drop(&mut self) {
                self.accountant
                    .set_budget(self.limit)
                    .expect("restore the original valid constructor allowance");
            }
        }
        struct Probe {
            accountant: crate::memory_accounting::AccountantHandle,
            limit: Option<usize>,
            available: usize,
            target: NativeIvfConstructorTargetForTest,
            armed: std::sync::atomic::AtomicBool,
            observer: std::sync::Arc<dyn Observer>,
        }
        impl Observer for Probe {
            fn before_reserve(&self, owner: usize, bytes: usize, operation: &'static str) {
                let selected = matches!(
                    (self.target, operation),
                    (
                        NativeIvfConstructorTargetForTest::WriterWorkspace,
                        "clustered_writer_workspace"
                    ) | (
                        NativeIvfConstructorTargetForTest::Codebook { .. },
                        "clustered_codebook"
                    )
                );
                if selected && !self.armed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    assert_eq!(
                        owner,
                        self.accountant.as_ptr() as usize,
                        "the real original writer pays this constructor"
                    );
                    let wanted = self
                        .accountant
                        .usage()
                        .used
                        .checked_add(self.available)
                        .expect("representable probe allowance");
                    if self.limit.is_none_or(|limit| wanted <= limit) {
                        self.accountant
                            .set_budget(Some(wanted))
                            .expect("narrow this existing original allowance");
                    }
                }
                self.observer.before_reserve(owner, bytes, operation);
            }
            fn admitted(&self, allocation: Allocation) {
                let selected = matches!(
                    (self.target, allocation),
                    (
                        NativeIvfConstructorTargetForTest::WriterWorkspace,
                        Allocation::WriterWorkspace(_)
                    ) | (
                        NativeIvfConstructorTargetForTest::Codebook { .. },
                        Allocation::Codebook(_)
                    )
                );
                if selected {
                    self.accountant.set_budget(self.limit).expect(
                        "restore original source and I/O allowance after the actual target debit",
                    );
                }
                self.observer.admitted(allocation);
            }
            fn constructed(&self, allocation: Allocation) {
                self.observer.constructed(allocation);
            }
            fn debited(&self, id: u64, owner: usize, bytes: usize, operation: &'static str) {
                self.observer.debited(id, owner, bytes, operation);
            }
            fn buffers_freed(&self, id: u64, owner: usize, bytes: usize, operation: &'static str) {
                self.observer.buffers_freed(id, owner, bytes, operation);
            }
            fn released(&self, id: u64, owner: usize, bytes: usize, operation: &'static str) {
                self.observer.released(id, owner, bytes, operation);
            }
        }
        let accountant =
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant);
        let limit = accountant.usage().limit;
        let _restore = Restore {
            accountant: accountant.clone(),
            limit,
        };
        contextdb_vector::ivf::with_ivf_constructor_observer_for_test(
            std::sync::Arc::new(Probe {
                accountant,
                limit,
                available,
                target,
                armed: std::sync::atomic::AtomicBool::new(false),
                observer,
            }),
            run,
        )
    }
    pub fn native_ivf_probe_constructor_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        target: NativeIvfConstructorTargetForTest,
        available: usize,
        observer: std::sync::Arc<dyn contextdb_vector::ivf::IvfConstructorObserverForTest>,
    ) -> Result<()> {
        let resolved = self.resolve_native_ivf_policy(index, tuple)?;
        let _operation = self.assert_open_operation();
        self.with_commit_lock(|| {
            let persistence = self.persistence.as_ref().ok_or_else(|| {
                Error::Other("native constructor probe requires writable persistence".into())
            })?;
            let (snapshot, frontier) = self.tx_mgr.published_read_snapshot();
            persistence.with_aborting_native_ivf_write(
                index,
                tuple,
                snapshot,
                frontier,
                &|| Ok(()),
                &|_| Ok(()),
                |scope| {
                    if matches!(target, NativeIvfConstructorTargetForTest::WriterWorkspace) {
                        return self.with_native_constructor_allowance_for_test(
                            target,
                            available,
                            observer,
                            || {
                                scope.with_clustered_writer(
                                    resolved.policy,
                                    resolved.training,
                                    |_| Ok(()),
                                )
                            },
                        );
                    }
                    let metadata = self.relational_store().table_meta.read();
                    let column = metadata
                        .get(&index.table)
                        .and_then(|meta| {
                            meta.columns
                                .iter()
                                .find(|column| column.name == index.column)
                        })
                        .ok_or_else(|| {
                            Error::Other("constructor probe names no declared column".into())
                        })?;
                    let complete = matches!(
                        target,
                        NativeIvfConstructorTargetForTest::Codebook {
                            training_counts: false
                        }
                    );
                    persistence.prepare_native_ivf_constructor_for_test(
                        scope,
                        index,
                        tuple,
                        column,
                        complete,
                        |scope, policy, training, mut build, owner| {
                            self.with_native_constructor_allowance_for_test(
                                target,
                                available,
                                observer,
                                || {
                                    if complete {
                                        contextdb_vector::ivf::load_codebook_values_for_test(
                                            &owner,
                                            scope,
                                            build.manifest,
                                        )
                                    } else {
                                        scope.with_clustered_writer(policy, training, |writer| {
                                            writer.advance_build(&mut build).map(|_| ())
                                        })
                                    }
                                },
                            )
                        },
                    )
                },
            )
        })
    }
}

impl Database {
    pub fn native_ivf_physical_disk_owner_for_test(
        &self,
    ) -> Result<crate::vector_ivf_observations::NativeIvfPhysicalDiskOwnerForTest> {
        self.persistence
            .as_ref()
            .ok_or_else(|| {
                Error::Other("native disk observation requires writable persistence".into())
            })?
            .native_ivf_physical_disk_owner_for_test()
    }
}

impl Database {
    pub fn native_ivf_retained_codebook_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<NativeIvfRetainedCodebookForTest> {
        self.with_native_ivf_read_for_test(index, tuple, |source| {
            Ok(source.retained_codebook_for_test())
        })
    }
}

impl Database {
    /// Resolve an observed native row through the existing current row reader.
    pub fn native_ivf_primary_key_for_row_for_test(
        &self,
        table: &str,
        row: u64,
        commit_lsn: Option<u64>,
    ) -> Result<Value> {
        let metadata = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let key = metadata
            .columns
            .iter()
            .find(|column| column.primary_key)
            .ok_or_else(|| Error::Other("native row inspection requires a primary key".into()))?;
        let snapshot = commit_lsn.map_or_else(
            || self.snapshot(),
            |lsn| self.tx_mgr.snapshot_at_lsn(Lsn(lsn)),
        );
        let row =
            self.find_row_by_id_at(table, RowId(row), snapshot, self.declared_reader_memory()?)?;
        row.values
            .get(&key.name)
            .cloned()
            .ok_or_else(|| Error::Other("observed row has no primary key".into()))
    }
}

impl Database {
    pub fn native_ivf_verification_window_for_test(&self) -> Result<u64> {
        let root = self
            .native_ivf_root()?
            .ok_or_else(|| Error::Other("no native verification window".into()))?;
        Ok(root.native_ivf_cache().observation_identity_for_test())
    }
    pub fn remove_native_ivf_page_for_test(
        &self,
        storage: u64,
        key: contextdb_vector::ivf::storage::PageKey,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| Error::Other("native page fault requires a writable store".into()))?;
        self.publish_direct_metadata(|| persistence.remove_native_ivf_page_for_test(storage, key))
    }
}

impl Database {
    pub fn plant_native_ivf_qualification_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        depths: &[u32],
        indexed_mean_work: u64,
        exact_mean_work: u64,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let column = meta
            .columns
            .iter()
            .find(|c| c.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other("qualification fixture requires the native writer".into())
        })?;
        self.publish_direct_metadata(|| {
            persistence.plant_native_ivf_qualification_for_test(
                index,
                tuple,
                column,
                depths,
                indexed_mean_work,
                exact_mean_work,
            )
        })
    }
}
