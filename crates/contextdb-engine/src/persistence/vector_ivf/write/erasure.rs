//! Authoritative erasure stays inside the caller's already borrowed row transaction.
use super::*;
use contextdb_vector::ivf::storage::BuildPhase;
use redb::ReadableTable;

impl WriteSession<'_> {
    pub(crate) fn erase_rows(
        &mut self,
        persistence: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        identities: &[super::super::super::VectorPartitionVersionIdentity],
    ) -> Result<()> {
        for (at, identity) in identities.iter().enumerate() {
            if identities[..at].iter().any(|old| {
                old.index == identity.index && old.partition_key == identity.partition_key
            }) {
                continue;
            }
            let index = &identity.index;
            let tuple = &identity.partition_key;
            self.with_scope(index, tuple, &|| Ok(()), &|_| Ok(()), |scope| {
                let route = RedbPersistence::vector_partition_prefix(index, tuple);
                let summary = source_summary(scope.session.write, &route)?;
                let meta = {
                    let table = scope
                        .session
                        .write
                        .open_table(super::super::super::META_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let key = RedbPersistence::meta_key(&index.table);
                    let value = table
                        .get(key.as_str())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(corrupt)?;
                    RedbPersistence::decode_table_meta_versioned(value.value())?.0
                };
                let column = meta
                    .columns
                    .iter()
                    .find(|column| column.name == index.column)
                    .ok_or_else(corrupt)?;
                #[cfg(not(feature = "test-seams"))]
                let _ = persistence;
                #[cfg(feature = "test-seams")]
                let test_column = persistence.native_ivf_test_column(column)?;
                #[cfg(feature = "test-seams")]
                let column = &test_column;
                let usage = original.accountant().usage();
                let resolved = crate::database::vector_ivf::resolved::resolve(
                    index,
                    column,
                    summary.live_entries,
                    usage.limit.map_or(u64::MAX, |n| n as u64),
                    usage
                        .available
                        .unwrap_or(usize::MAX)
                        .saturating_sub(std::mem::size_of::<WriteOperation<'_, '_, '_>>())
                        .saturating_sub(PAGE_BYTES),
                    training_identity(tuple, summary.source_identity()),
                )?;
                // Generation zero is the pre-build canonical journal, not a serving index.
                let serving = scope
                    .manifest(0)?
                    .filter(|manifest| manifest.active_lists != 0);
                let abandoned = scope
                    .record
                    .borrow()
                    .build_state()?
                    .is_some_and(|state| state.phase != BuildPhase::Ready);
                if abandoned {
                    let mut record = scope.record.borrow_mut();
                    record.aborted_builds =
                        record.aborted_builds.checked_add(1).ok_or_else(corrupt)?;
                    record.build = serving.map(|manifest| {
                        BuildState {
                            manifest,
                            phase: BuildPhase::Ready,
                            cursor: 0,
                            iteration: resolved.training.iterations,
                            training: resolved.training,
                            serving_at_start: None,
                            source_lsn: scope.session.snapshot_lsn.0,
                            publication_coalesce: None,
                        }
                        .into()
                    });
                    record.source_cursor = None;
                    record.source_end = None;
                    record.ingested_vectors = 0;
                    record.build_changes = Default::default();
                }
                if serving.is_some() {
                    let _input = Invoice::reserve(
                        &scope.session.owner,
                        PAGE_BYTES,
                        "clustered_erasure_slot_input",
                    )?;
                    let mut page = [0; PAGE_BYTES];
                    scope.with_row_writer(resolved.policy, resolved.training, |writer| {
                        while writer
                            .inspect_catalog(0)?
                            .is_some_and(|catalog| catalog.retained_codebook.is_some())
                        {
                            writer.advance_remap(0, resolved.training.phase_batch_entries)?;
                            writer.advance_coalescing(
                                0,
                                column
                                    .vector_engine_policy
                                    .coalesce_pages_per_step
                                    .unwrap_or(
                                        crate::persistence::STORAGE_MAINTENANCE_PAGE_BATCH as u64,
                                    ) as usize,
                            )?;
                        }
                        let manifest = writer.storage().manifest(0)?.ok_or_else(corrupt)?;
                        let stride = (manifest.dimensions as u64)
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(8))
                            .ok_or_else(corrupt)?;
                        for slot in 0..manifest.slots {
                            scope.check_cancel()?;
                            let offset = slot.checked_mul(stride).ok_or_else(corrupt)?;
                            let mut row = [0; 8];
                            let mut copied = 0;
                            while copied < 8 {
                                let position =
                                    offset.checked_add(copied as u64).ok_or_else(corrupt)?;
                                if !writer.storage().read_admitted_page(
                                    manifest.key(
                                        PageKind::Canonical,
                                        0,
                                        position / PAGE_BYTES as u64,
                                    ),
                                    &mut page,
                                )? {
                                    return Err(corrupt());
                                }
                                let start = (position % PAGE_BYTES as u64) as usize;
                                let take = (8 - copied).min(PAGE_BYTES - start);
                                row[copied..copied + take]
                                    .copy_from_slice(&page[start..start + take]);
                                copied += take;
                            }
                            let row = u64::from_le_bytes(row);
                            if identities.iter().any(|identity| {
                                identity.index == *index
                                    && identity.partition_key == *tuple
                                    && identity.row_id.0 == row
                            }) {
                                writer.erase_row(0, slot)?;
                            }
                        }
                        Ok(())
                    })?;
                    while scope.reclaim_inactive(resolved.training.phase_batch_entries)? {}
                } else {
                    scope.erase_unserved_families()?;
                    let mut record = scope.record.borrow_mut();
                    record.serving = None;
                    record.build = None;
                    record.canonical_complete = false;
                }
                {
                    let mut record = scope.record.borrow_mut();
                    record.current_membership_digest = summary.source_identity();
                    record.current_membership_entries = summary.entries;
                    record.current_membership_live_entries = summary.live_entries;
                    record.committed_through_lsn = scope.session.row_commit_lsn.0;
                }
                scope.persist_erasure_controls()
            })?;
        }
        Ok(())
    }
}
impl WriteScope<'_, '_> {
    fn erase_family_tables(&self, family: Family) -> Result<()> {
        if family.qualification_root.is_some() {
            let name = super::super::qualification::TableName::new(family.storage)?;
            self.session
                .write
                .delete_table(redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?))
                .map_err(RedbPersistence::storage_error)?;
        }
        for tag in 1..=16 {
            self.check_cancel()?;
            let kind = header::kind_from_tag(tag)?;
            let name = KindTableName::new(family.storage, kind)?;
            self.session
                .write
                .delete_table(redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?))
                .map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
    fn erase_unserved_families(&mut self) -> Result<()> {
        for at in 0..control::ROOTS {
            let family = self.record.borrow().families[at];
            if let Some(family) = family {
                self.erase_family_tables(family)?;
                self.record.borrow_mut().families[at] = None;
            }
        }
        let mut prefix = [0; 65];
        prefix[..64].copy_from_slice(&self.key);
        prefix[64] = b'R';
        loop {
            self.check_cancel()?;
            let next = {
                let table = self
                    .session
                    .write
                    .open_table(SCOPES)
                    .map_err(RedbPersistence::storage_error)?;
                let mut cursor = table
                    .range(prefix.as_slice()..)
                    .map_err(RedbPersistence::storage_error)?;
                match cursor.next() {
                    None => None,
                    Some(entry) => {
                        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                        if key.value().starts_with(&prefix) {
                            Some((
                                <[u8; 73]>::try_from(key.value()).map_err(|_| corrupt())?,
                                control::decode_retired_family(key.value(), value.value())?,
                            ))
                        } else {
                            None
                        }
                    }
                }
            };
            let Some((key, family)) = next else {
                break;
            };
            self.erase_family_tables(family)?;
            self.session
                .write
                .open_table(SCOPES)
                .map_err(RedbPersistence::storage_error)?
                .remove(key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
    fn persist_erasure_controls(&self) -> Result<()> {
        struct Buffers {
            scope: [u8; PAGE_BYTES],
            descriptor: [u8; super::super::descriptor::BYTES],
        }
        let _invoice = Invoice::reserve(
            &self.session.owner,
            std::mem::size_of::<Buffers>(),
            "clustered_erasure_controls",
        )?;
        let mut buffers = Buffers {
            scope: [0; PAGE_BYTES],
            descriptor: [0; super::super::descriptor::BYTES],
        };
        self.check_cancel()?;
        {
            let record = self.record.borrow();
            record.validate(self.index, record.schema, record.tuple)?;
            record.encode(&self.key, &mut buffers.scope)?;
        }
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .boundary(NativeIvfFaultBoundary::BeforeScopeWrite {
                scope: self.observed_scope(),
                operation: None,
            })?;
        let native = NativeIo::Borrowed(self.session.write);
        native.put(SCOPES.name(), &self.key, &buffers.scope)?;
        if self.record.borrow().serving.is_some() {
            super::super::descriptor::encode(
                &self.key,
                &self.record.borrow(),
                &mut buffers.descriptor,
            )?;
            native.put(
                SCOPES.name(),
                &super::super::descriptor::key(&self.key),
                &buffers.descriptor,
            )?;
        }
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::ScopeWritten {
                scope: self.observed_scope(),
                operation: None,
                stored: self.record.borrow().observed(&self.key)?,
            });
        Ok(())
    }
}
