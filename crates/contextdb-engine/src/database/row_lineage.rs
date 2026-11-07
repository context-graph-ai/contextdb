use super::*;

#[cfg_attr(not(feature = "sync-orchestration"), allow(dead_code))]
impl Database {
    /// Stamping config records at commit is write working memory. Each record
    /// decodes under its own writer-funded scope that ends with the record,
    /// so the commit's decoder charge stays one record, never one per staged
    /// row retained on the committing caller's reader.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_record_scope(&self) -> Result<crate::read_image_memory::ImageMemoryScope> {
        crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stamp_unbound_creation_lineages_at_commit(
        &self,
        ws: &mut contextdb_tx::WriteSet,
    ) -> Result<()> {
        let Some(lsn) = self
            .private_acceptance_end_for_write_set(ws)
            .or(ws.commit_lsn)
        else {
            return Ok(());
        };
        self.stamp_durable_delete_positions_at_commit(ws)?;
        for (key, encoded) in &mut ws.config_writes {
            if key.starts_with(SYNC_PUSH_OUTCOME_PREFIX) {
                let _record_scope = self.commit_record_scope()?;
                let mut result: DurableSyncPushOutcome =
                    RedbPersistence::decode_config_value(encoded)?;
                if result.result.new_lsn == Lsn(0) {
                    result.result.new_lsn = lsn;
                }
                *encoded = RedbPersistence::encode_config_value(&result)?;
            }
        }
        let mut creation_positions = HashMap::new();
        for (key, encoded) in &mut ws.config_writes {
            if !key.starts_with("sync_creation_lineage.v1.") {
                continue;
            }
            let _record_scope = self.commit_record_scope()?;
            let mut record: DurableUnboundCreationLineage =
                RedbPersistence::decode_config_value(encoded)?;
            record.creation_lsn = Some(lsn);
            creation_positions.insert(
                (
                    sync_identity_key(&record.table, &record.natural_key),
                    record.table_generation,
                    record.local_row_id,
                ),
                lsn,
            );
            *encoded = RedbPersistence::encode_config_value(&record)?;
        }
        for (key, encoded) in &mut ws.config_writes {
            if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                continue;
            }
            let _record_scope = self.commit_record_scope()?;
            let mut record: DurableLineageRecord = RedbPersistence::decode_config_value(encoded)?;
            if record.delete_lsn == 0 {
                record.delete_lsn = lsn.0;
            }
            if record.author_node_id.is_none()
                && let Some(creation_lsn) = creation_positions.get(&(
                    sync_identity_key(&record.table, &record.natural_key),
                    record.table_generation,
                    record.local_row_id.unwrap_or(RowId(0)),
                ))
            {
                record.author_local_mutation_position = creation_lsn.0;
                record.lineage_root = format!(
                    "unbound:{}:{}",
                    Self::hex_component(&sync_identity_key(&record.table, &record.natural_key)),
                    creation_lsn.0
                );
            }
            *encoded = RedbPersistence::encode_config_value(&record)?;
        }
        // A later life's delete may replace an earlier life's purged record.
        // The commit mutex is held, so the durable record read here is the
        // one this write replaces; its tombstone commits in the same write.
        // These records stay decoded until the end, under one writer scope.
        let _lifecycle_scope = self.commit_record_scope()?;
        let lifecycle_writes = ws
            .config_writes
            .iter()
            .filter(|(key, _)| key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX))
            .map(|(key, encoded)| {
                RedbPersistence::decode_config_value::<DurableLineageRecord>(encoded)
                    .map(|record| (key.clone(), record))
            })
            .collect::<Result<Vec<_>>>()?;
        for (key, record) in self.purged_lives_displaced_by(&lifecycle_writes)? {
            ws.config_writes
                .push((key, RedbPersistence::encode_config_value(&record)?));
        }
        Ok(())
    }

    /// Native terminal stamping operates on one original-funded record at a
    /// time. It never lends a mutable WriteSet or relabels a decoder credit as
    /// the independently copied target's original invoice.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stamp_unbound_creation_lineages_in_prepared_write_set(
        &self,
        host: &EngineDatabaseTransactions,
        original: &OwnedMemoryReservation,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
    ) -> Result<()> {
        crate::memory_accounting::validate_owned_original(
            original,
            &self.finalization.accountant,
            &host.funder,
        )?;
        let lsn = held.with_legacy(|ws| {
            self.private_acceptance_end_for_write_set(ws)
                .or(ws.commit_lsn)
        });
        let Some(lsn) = lsn else {
            return Ok(());
        };
        let count = held.read().config_writes.len();
        let published = self.tx_mgr.capture_published_read();
        let root = writer_read::durable_root(published.handle)?;
        held.with_tx_checkpoint(|held| {
            // Preserve the legacy distinction: delete coordinates use the
            // actual terminal LSN, while creation/outcome evidence uses the
            // received acceptance end when the private marker is present.
            if let Some(delete_lsn) = held.with_legacy(|ws| ws.commit_lsn) {
                for position in 0..count {
                    let replacement = {
                        let (key, bytes) = &held.read().config_writes[position];
                        if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) { continue; }
                        let mut record = RedbPersistence::decode_owned_config_value::<DurableLineageRecord>(original, bytes)?;
                        if record.value.delete_lsn != 0 { continue; }
                        record.value.delete_lsn = delete_lsn.0;
                        Self::prepare_original_terminal_config(held, original, key, &record.value)?
                    };
                    let prepared = held.prepare_aux_splice(position, &[position], replacement)?;
                    held.publish_tx(prepared);
                }
            }
            for position in 0..count {
                let replacement = {
                    let (key, bytes) = &held.read().config_writes[position];
                    if key.starts_with(SYNC_PUSH_OUTCOME_PREFIX) {
                        let mut record = RedbPersistence::decode_owned_config_value::<DurableSyncPushOutcome>(original, bytes)?;
                        if record.value.result.new_lsn == Lsn(0) { record.value.result.new_lsn = lsn; }
                        Some(Self::prepare_original_terminal_config(held, original, key, &record.value)?)
                    } else if key.starts_with("sync_creation_lineage.v1.") {
                        let mut record = RedbPersistence::decode_owned_config_value::<DurableUnboundCreationLineage>(original, bytes)?;
                        record.value.creation_lsn = Some(lsn);
                        Some(Self::prepare_original_terminal_config(held, original, key, &record.value)?)
                    } else { None }
                };
                if let Some(replacement) = replacement {
                    let prepared = held.prepare_aux_splice(position, &[position], replacement)?;
                    held.publish_tx(prepared);
                }
            }
            for position in 0..count {
                let replacement = {
                    let (key, bytes) = &held.read().config_writes[position];
                    if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) { continue; }
                    let mut record = RedbPersistence::decode_owned_config_value::<DurableLineageRecord>(original, bytes)?;
                    if record.value.delete_lsn == 0 { record.value.delete_lsn = lsn.0; }
                    if record.value.author_node_id.is_none() {
                        // Probe the exact canonical identity bytes, including
                        // numeric key representation, rather than comparing a
                        // display string or a potentially lossy logical key.
                        let creation_key = Self::original_terminal_identity_key(original,
                            &record.value.table, &record.value.natural_key,
                            format_args!("sync_creation_lineage.v1.{:016x}.{:016x}.",
                                record.value.table_generation, record.value.local_row_id.unwrap_or(RowId(0)).0),
                            format_args!(""))?;
                        let mut has_creation = false;
                        for (candidate_key, bytes) in &held.read().config_writes[..count] {
                            if !candidate_key.starts_with("sync_creation_lineage.v1.") { continue; }
                            let candidate = RedbPersistence::decode_owned_config_value::<DurableUnboundCreationLineage>(original, bytes)?;
                            let identity = Self::original_terminal_identity_key(original,
                                &candidate.value.table, &candidate.value.natural_key,
                                format_args!("sync_creation_lineage.v1.{:016x}.{:016x}.",
                                    candidate.value.table_generation, candidate.value.local_row_id.0),
                                format_args!(""))?;
                            if identity.value == creation_key.value { has_creation = true; break; }
                        }
                        if has_creation {
                            let mut lineage_root = Self::original_terminal_identity_key(original,
                                &record.value.table, &record.value.natural_key,
                                format_args!("unbound:"), format_args!(":{}", lsn.0))?;
                            crate::memory_accounting::merge_preparation_original(
                                &mut record.allocation, &mut lineage_root.allocation)?;
                            record.value.lineage_root = std::mem::take(&mut lineage_root.value);
                            record.value.author_local_mutation_position = lsn.0;
                        }
                    }
                    Self::prepare_original_terminal_config(held, original, key, &record.value)?
                };
                let prepared = held.prepare_aux_splice(position, &[position], replacement)?;
                held.publish_tx(prepared);
            }
            // Preserve every displaced purged life in source order. Only one
            // predecessor is decoded at a time; an earlier same-key write is
            // authoritative over the captured published base.
            for position in 0..count {
                let history = {
                    let (key, bytes) = &held.read().config_writes[position];
                    if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) { continue; }
                    let incoming = RedbPersistence::decode_owned_config_value::<DurableLineageRecord>(original, bytes)?;
                    let previous = held.read().config_writes[..position].iter().rev()
                        .find(|(previous, _)| previous == key);
                    let earlier;
                    let durable;
                    let volatile;
                    let predecessor = if let Some((_, bytes)) = previous {
                        earlier = RedbPersistence::decode_owned_config_value::<DurableLineageRecord>(original, bytes)?;
                        Some(&earlier.value)
                    } else if let Some(root) = &root {
                        durable = root.config_point_original::<DurableLineageRecord>(original, key)?;
                        durable.as_ref().map(|window| &window.value)
                    } else {
                        let state = self.lineage_state_lock.lock();
                        volatile = match state.records.get(key) {
                            Some(record) => {
                                let bytes = RedbPersistence::encode_owned_event(original, record)?;
                                Some(RedbPersistence::decode_owned_config_value::<DurableLineageRecord>(original, &bytes.bytes)?)
                            }
                            None => None,
                        };
                        volatile.as_ref().map(|record| &record.value)
                    };
                    match predecessor {
                        Some(record) if record.delete_obligation == DurableDeleteObligation::Purged
                            && record.lineage_root != incoming.value.lineage_root => {
                            let digest = blake3::hash(record.lineage_root.as_bytes()).to_hex();
                            let key = Self::original_terminal_identity_key(original,
                                &record.table, &record.natural_key,
                                format_args!("{}{:016x}.", Self::DURABLE_PURGED_LINEAGE_HISTORY_PREFIX, record.table_generation),
                                format_args!(".{digest}"))?;
                            Some(Self::prepare_original_terminal_config(held, original, &key.value, record)?)
                        }
                        _ => None,
                    }
                };
                if let Some(history) = history {
                    let at = held.read().config_writes.len();
                    let prepared = held.prepare_aux_splice(at, &[], history)?;
                    held.publish_tx(prepared);
                }
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_original_terminal_config<T: serde::Serialize>(
        held: &contextdb_tx::manager::EngineGuard<'_>,
        original: &OwnedMemoryReservation,
        key: &str,
        value: &T,
    ) -> Result<contextdb_tx::staged_source::AdmittedAuxBatch> {
        let mut encoded = RedbPersistence::encode_owned_event(original, value)?;
        let _key_memory = crate::memory_accounting::reserve_preparation_from_original(
            original,
            key.len(),
            "persistence",
            "terminal_config_key",
            "Reduce staged records or raise MEMORY_LIMIT.",
        )?;
        // This temporary body is declared after both original payers. The
        // lower copy admits its own independent target before allocating it.
        let body = (
            crate::memory_accounting::preparation_copy_string(key)?,
            std::mem::take(&mut encoded.bytes),
        );
        held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(
            std::slice::from_ref(&body),
        ))
    }

    /// Stream the established MessagePack identity bytes as hex into an
    /// exactly admitted native key, without an intermediate identity buffer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn original_terminal_identity_key(
        original: &OwnedMemoryReservation,
        table: &str,
        natural_key: &NaturalKey,
        prefix: std::fmt::Arguments<'_>,
        suffix: std::fmt::Arguments<'_>,
    ) -> Result<crate::persistence::OriginalDecoded<String>> {
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        struct KeyWriter<'a>(&'a mut String);
        impl std::fmt::Write for KeyWriter<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                if text.len() > self.0.capacity() - self.0.len() {
                    return Err(std::fmt::Error);
                }
                self.0.push_str(text);
                Ok(())
            }
        }
        impl std::io::Write for KeyWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let needed = bytes
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
                if needed > self.0.capacity() - self.0.len() {
                    return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
                }
                for byte in bytes {
                    self.0.push(HEX[(byte >> 4) as usize] as char);
                    self.0.push(HEX[(byte & 15) as usize] as char);
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(0);
        std::fmt::write(&mut count, prefix).map_err(|_| EngineMetadataJournal::identity_error())?;
        std::fmt::write(&mut count, suffix).map_err(|_| EngineMetadataJournal::identity_error())?;
        let mut identity = Count(
            8usize
                .checked_add(table.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?,
        );
        serde::Serialize::serialize(natural_key, &mut rmp_serde::Serializer::new(&mut identity))
            .map_err(|error| {
                Error::Other((format!("sync natural key encode error: {error}")).into())
            })?;
        let bytes = identity
            .0
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(count.0))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        let allocation = crate::memory_accounting::reserve_preparation_from_original(
            original,
            bytes,
            "persistence",
            "terminal_identity_key",
            "Reduce staged records or raise MEMORY_LIMIT.",
        )?;
        let mut key = crate::persistence::OriginalDecoded {
            value: String::with_capacity(bytes),
            allocation,
        };
        if key.value.capacity() != bytes {
            return Err(EngineMetadataJournal::identity_error());
        }
        let mut writer = KeyWriter(&mut key.value);
        std::fmt::write(&mut writer, prefix)
            .map_err(|_| EngineMetadataJournal::identity_error())?;
        std::io::Write::write_all(&mut writer, &(table.len() as u64).to_be_bytes())
            .map_err(|_| EngineMetadataJournal::identity_error())?;
        std::io::Write::write_all(&mut writer, table.as_bytes())
            .map_err(|_| EngineMetadataJournal::identity_error())?;
        serde::Serialize::serialize(natural_key, &mut rmp_serde::Serializer::new(&mut writer))
            .map_err(|error| {
                Error::Other((format!("sync natural key encode error: {error}")).into())
            })?;
        std::fmt::write(&mut writer, suffix)
            .map_err(|_| EngineMetadataJournal::identity_error())?;
        if key.value.len() != bytes {
            return Err(EngineMetadataJournal::identity_error());
        }
        Ok(key)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stamp_durable_delete_positions_at_commit(
        &self,
        ws: &mut contextdb_tx::WriteSet,
    ) -> Result<()> {
        let Some(lsn) = ws.commit_lsn else {
            return Ok(());
        };
        for (key, encoded) in &mut ws.config_writes {
            if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                continue;
            }
            let _record_scope = self.commit_record_scope()?;
            let mut record: DurableLineageRecord = RedbPersistence::decode_config_value(encoded)?;
            if record.delete_lsn == 0 {
                record.delete_lsn = lsn.0;
                *encoded = RedbPersistence::encode_config_value(&record)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_delivery_row_lineages(
        &self,
        ws: &mut dyn crate::custody::preparation::CommitWriteSource,
        changes: &ChangeSet,
        tenant: &TenantId,
        node: &str,
        incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<PreparedDeliveryLineages> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut result = HashMap::new();
        let state = self.lineage_state_lock.lock();
        for change in &changes.rows {
            let meta = self
                .table_meta(&change.table)
                .ok_or_else(crate::custody::canonical::invalid)?;
            let row = ws
                .relational_inserts()
                .iter()
                .find_map(|(table, row)| {
                    (*table == change.table
                        && natural_key_from_row_values(&meta, &row.values).as_ref()
                            == Some(&change.natural_key))
                    .then_some(row.clone())
                })
                .ok_or_else(crate::custody::canonical::invalid)?;
            let generation =
                self.durable_lineage_table_generation(&change.table, read_memory.clone())?;
            let sidecar_key = Self::durable_row_lineage_config_key(
                &change.table,
                &change.natural_key,
                generation,
            );
            let creation_key = Self::durable_unbound_creation_config_key(
                &change.table,
                &change.natural_key,
                generation,
                row.row_id,
            );
            let creation: Option<DurableUnboundCreationLineage> = ws
                .config_writes()
                .iter()
                .rev()
                .find(|(k, _)| k == &creation_key)
                .map(|(_, bytes)| RedbPersistence::decode_config_value(bytes))
                .transpose()?;
            // Offline roots still get their real transaction's creator evidence.
            let creation = creation.or_else(|| {
                (meta.sync_direction == Some(contextdb_core::SyncDirection::None)).then(|| {
                    DurableUnboundCreationLineage {
                        table: change.table.clone(),
                        natural_key: change.natural_key.clone(),
                        table_generation: generation,
                        local_row_id: row.row_id,
                        creation_lsn: ws.commit_lsn(),
                    }
                })
            });
            let sidecar = if let Some(creation) = creation {
                if creation.creation_lsn != ws.commit_lsn()
                    || creation.local_row_id != row.row_id
                    || creation.table_generation != generation
                {
                    return Err(crate::custody::canonical::invalid());
                }
                let lsn = creation
                    .creation_lsn
                    .ok_or_else(crate::custody::canonical::invalid)?;
                let mut sidecar = DurableRowLineageSidecar {
                    author_node_id: node.into(),
                    author_database_incarnation: incarnation,
                    author_local_mutation_position: lsn,
                    table_generation: generation,
                    lineage_root: format!("author:{}:{}:{}", node, incarnation.to_hex(), lsn.0),
                    lineage_attestation: Vec::new(),
                    local_row_id: Some(row.row_id),
                    locally_created: true,
                };
                sidecar.lineage_attestation = signer(&Self::lineage_attestation_bytes(
                    tenant,
                    &change.table,
                    &change.natural_key,
                    generation,
                    node,
                    incarnation,
                    lsn,
                    &sidecar.lineage_root,
                )?)?;
                ws.push_config((sidecar_key, RedbPersistence::encode_config_value(&sidecar)?))?;
                sidecar
            } else {
                self.load_row_lineage_sidecar(&state, &sidecar_key)?
                    .filter(|s| s.local_row_id == Some(row.row_id))
                    .ok_or_else(crate::custody::canonical::invalid)?
            };
            let lineage = crate::protocol::WireRowLineage {
                table_generation: sidecar.table_generation,
                author_node_id: sidecar.author_node_id,
                author_database_incarnation: sidecar.author_database_incarnation,
                author_local_mutation_position: sidecar.author_local_mutation_position,
                lineage_root: sidecar.lineage_root,
                attestation: sidecar.lineage_attestation,
            };
            Self::verify_lineage_attestation(tenant, &change.table, &change.natural_key, &lineage)?;
            result.insert(
                (
                    change.table.clone(),
                    rmp_serde::to_vec(&change.natural_key)
                        .map_err(|_| crate::custody::canonical::invalid())?,
                    change.lsn,
                ),
                lineage,
            );
        }
        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn staged_unbound_creation_lineage(
        &self,
        tx: TxId,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
        local_row_id: RowId,
    ) -> Result<Option<DurableUnboundCreationLineage>> {
        let key =
            Self::durable_unbound_creation_config_key(table, natural_key, generation, local_row_id);
        self.tx_mgr.with_read(tx, |ws| {
            ws.config_writes
                .iter()
                .rev()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, encoded)| RedbPersistence::decode_config_value(encoded))
                .transpose()
        })?
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn bind_unbound_creation_lineage_with_state(
        &self,
        state: &mut InMemoryLineageState,
        tenant_id: &TenantId,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
        local_row_id: RowId,
        author_node_id: &str,
        author_database_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
    ) -> Result<DurableRowLineageSidecar> {
        let sidecar_key = Self::durable_row_lineage_config_key(table, natural_key, generation);
        if let Some(mut existing) = self.load_row_lineage_sidecar(state, &sidecar_key)?
            && existing.table_generation == generation
            && existing.local_row_id == Some(local_row_id)
        {
            if !existing.locally_created {
                return Err(Error::SyncError((format!(
                    "row {} {:?} has inherited lineage and cannot be rebound as a local creation",
                    table, natural_key
                )).into()));
            }
            if existing.author_node_id == author_node_id
                && existing.author_database_incarnation == author_database_incarnation
            {
                if existing.lineage_attestation.is_empty() {
                    let bytes = Self::lineage_attestation_bytes(
                        tenant_id,
                        table,
                        natural_key,
                        existing.table_generation,
                        &existing.author_node_id,
                        existing.author_database_incarnation,
                        existing.author_local_mutation_position,
                        &existing.lineage_root,
                    )?;
                    existing.lineage_attestation = signer(&bytes)?;
                    self.store_row_lineage_sidecar(state, &sidecar_key, &existing)?;
                }
                return Ok(existing);
            }
            return Err(Error::SyncError(
                (format!(
                    "row {} {:?} is already bound to a different creation identity",
                    table, natural_key
                ))
                .into(),
            ));
        }
        let evidence = self
            .load_unbound_creation_lineage(
                state,
                &Self::durable_unbound_creation_config_key(
                    table,
                    natural_key,
                    generation,
                    local_row_id,
                ),
            )?
            .filter(|evidence| {
                evidence.table_generation == generation
                    && evidence.local_row_id == local_row_id
                    && evidence.natural_key == *natural_key
            })
            .and_then(|evidence| evidence.creation_lsn.map(|lsn| (evidence, lsn)))
            .ok_or_else(|| {
                Error::SyncError(
                    (format!(
                        "row {} {:?} lacks committed creation evidence for provenance binding",
                        table, natural_key
                    ))
                    .into(),
                )
            })?;
        let (evidence, creation_lsn) = evidence;
        let sidecar = DurableRowLineageSidecar {
            author_node_id: author_node_id.to_string(),
            author_database_incarnation,
            author_local_mutation_position: creation_lsn,
            table_generation: evidence.table_generation,
            lineage_root: format!(
                "author:{}:{}:{}",
                author_node_id,
                author_database_incarnation.to_hex(),
                creation_lsn.0
            ),
            lineage_attestation: Vec::new(),
            local_row_id: Some(evidence.local_row_id),
            locally_created: true,
        };
        let mut sidecar = sidecar;
        let bytes = Self::lineage_attestation_bytes(
            tenant_id,
            table,
            natural_key,
            sidecar.table_generation,
            &sidecar.author_node_id,
            sidecar.author_database_incarnation,
            sidecar.author_local_mutation_position,
            &sidecar.lineage_root,
        )?;
        sidecar.lineage_attestation = signer(&bytes)?;
        // Historical changes may bind an older local life. Its evidence is
        // immutable, but the current sidecar belongs to the newer life.
        if self
            .load_row_lineage_sidecar(state, &sidecar_key)?
            .is_none_or(|current| current.local_row_id == Some(local_row_id))
        {
            self.store_row_lineage_sidecar(state, &sidecar_key, &sidecar)?;
        }
        Ok(sidecar)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_outbound_lineage_signatures(
        &self,
        row: &RowChange,
        generation: u64,
        selected_row_id: Option<RowId>,
        tenant_id: &TenantId,
        author: &str,
        incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        Vec<(Vec<u8>, Vec<u8>)>,
        contextdb_core::read_memory::ReadCredit,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Only numeric creation positions escape the directory guard.
        let positions = {
            let state = self.lineage_state_lock.lock();
            let record = self.load_lineage_record(
                &state,
                &Self::durable_lineage_config_key(&row.table, &row.natural_key, generation),
            )?;
            let sidecar = self.load_row_lineage_sidecar(
                &state,
                &Self::durable_row_lineage_config_key(&row.table, &row.natural_key, generation),
            )?;
            let bound_sidecar_id = sidecar
                .as_ref()
                .filter(|r| !r.locally_created || !r.lineage_attestation.is_empty())
                .and_then(|r| r.local_row_id);
            let ids = [
                selected_row_id.filter(|id| Some(*id) != bound_sidecar_id),
                record
                    .as_ref()
                    .filter(|r| r.locally_created && r.lineage_attestation.is_empty())
                    .and_then(|r| r.local_row_id),
                sidecar
                    .as_ref()
                    .filter(|r| r.locally_created && r.lineage_attestation.is_empty())
                    .and_then(|r| r.local_row_id),
            ];
            let mut positions = [None; 3];
            for (slot, id) in positions.iter_mut().zip(ids) {
                if let Some(id) = id {
                    *slot = self
                        .load_unbound_creation_lineage(
                            &state,
                            &Self::durable_unbound_creation_config_key(
                                &row.table,
                                &row.natural_key,
                                generation,
                                id,
                            ),
                        )?
                        .and_then(|e| e.creation_lsn);
                }
            }
            positions
        };
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let count = positions
            .iter()
            .enumerate()
            .filter(|(i, p)| p.is_some() && !positions[..*i].contains(p))
            .count();
        credit.try_grow_for(
            count * std::mem::size_of::<(Vec<u8>, Vec<u8>)>(),
            "sync_lineage",
            "prepared_signatures",
            "Raise reader memory to prepare lineage.",
        )?;
        let mut prepared = Vec::with_capacity(count);
        for (i, position) in positions.iter().enumerate() {
            let Some(position) = position else { continue };
            if positions[..i].contains(&Some(*position)) {
                continue;
            }
            let root = format!("author:{}:{}:{}", author, incarnation.to_hex(), position.0);
            let bytes = Self::lineage_attestation_bytes(
                tenant_id,
                &row.table,
                &row.natural_key,
                generation,
                author,
                incarnation,
                *position,
                &root,
            )?;
            let signature = signer(&bytes)?;
            // The signer hands back caller-owned bytes. Admit their adoption
            // before retaining either buffer in this operation's prepared set.
            credit.try_grow_for(
                bytes.capacity().saturating_add(signature.capacity()),
                "sync_lineage",
                "prepared_signatures",
                "Raise reader memory to retain lineage.",
            )?;
            prepared.push((bytes, signature));
        }
        Ok((prepared, credit))
    }

    /// Bind one still-pending delete while holding the shared lineage state.
    /// A reply that has accepted or retired this exact delete wins before this
    /// method can recreate it, for either backend.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn pending_delete_lineage(
        &self,
        row: &RowChange,
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<crate::protocol::WireRowLineage>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        // All lineage and coordination locks nest inside the commit boundary.
        // The stores inherit this guard and owe publication after it unlocks.
        let table_generation =
            self.durable_lineage_table_generation(&row.table, read_memory.clone())?;
        let bind = |signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>| {
            let record_key =
                Self::durable_lineage_config_key(&row.table, &row.natural_key, table_generation);
            let mut lineage_state = self.lineage_state_lock.lock();
            let Some(mut record) = self.load_lineage_record(&lineage_state, &record_key)? else {
                return Ok(None);
            };
            if record.table != row.table
                || record.natural_key != row.natural_key
                || record.table_generation != table_generation
                || record.delete_lsn != row.lsn.0
                || record.delete_obligation != DurableDeleteObligation::Pending
            {
                return Ok(None);
            }
            if record.locally_created {
                let locally_authenticated = record.author_node_id.is_some()
                    && record.author_database_incarnation.is_some()
                    && !record.lineage_attestation.is_empty();
                if locally_authenticated {
                    let expected_incarnation = local_incarnation.to_hex();
                    if record.author_node_id.as_deref() != Some(local_node_id)
                        || record.author_database_incarnation.as_deref()
                            != Some(expected_incarnation.as_str())
                    {
                        return Err(Error::SyncError((format!(
                            "pending delete {} {:?} is already bound to a different creation identity",
                            record.table, record.natural_key
                        )).into()));
                    }
                    let sidecar = Self::local_delete_sidecar_from_record(&record)?;
                    let sidecar_key = Self::durable_row_lineage_config_key(
                        &record.table,
                        &record.natural_key,
                        record.table_generation,
                    );
                    if self.local_delete_sidecar_slot_is_eligible(
                        &lineage_state,
                        &sidecar_key,
                        sidecar
                            .local_row_id
                            .expect("local delete sidecar has a row id"),
                    )? {
                        self.store_lineage_record_and_row_sidecar(
                            &mut lineage_state,
                            &record_key,
                            &record,
                            &sidecar_key,
                            &sidecar,
                        )?;
                    }
                } else {
                    // A fresh same-key row may now own the current sidecar. Bind
                    // this old delete from its exact committed creation evidence;
                    // only republish its sidecar when that slot is still safe.
                    let local_row_id = record.local_row_id.ok_or_else(|| {
                        Error::SyncError(
                            ("locally created pending delete lacks the pinned local row id"
                                .to_string())
                            .into(),
                        )
                    })?;
                    let evidence = self
                    .load_unbound_creation_lineage(
                        &lineage_state,
                        &Self::durable_unbound_creation_config_key(
                            &record.table,
                            &record.natural_key,
                            record.table_generation,
                            local_row_id,
                        ),
                    )?
                    .filter(|evidence| {
                        evidence.table == record.table
                            && evidence.natural_key == record.natural_key
                            && evidence.table_generation == record.table_generation
                            && evidence.local_row_id == local_row_id
                    })
                    .and_then(|evidence| evidence.creation_lsn.map(|lsn| (evidence, lsn)))
                    .ok_or_else(|| {
                        Error::SyncError((format!(
                            "pending delete {} {:?} lacks committed creation evidence for provenance binding",
                            record.table, record.natural_key
                        )).into())
                    })?;
                    let (_, creation_lsn) = evidence;
                    let lineage_root = format!(
                        "author:{}:{}:{}",
                        local_node_id,
                        local_incarnation.to_hex(),
                        creation_lsn.0
                    );
                    let bytes = Self::lineage_attestation_bytes(
                        tenant_id,
                        &record.table,
                        &record.natural_key,
                        record.table_generation,
                        local_node_id,
                        local_incarnation,
                        creation_lsn,
                        &lineage_root,
                    )?;
                    record.author_node_id = Some(local_node_id.to_string());
                    record.author_database_incarnation = Some(local_incarnation.to_hex());
                    record.author_local_mutation_position = creation_lsn.0;
                    record.lineage_root = lineage_root;
                    record.lineage_attestation = signer(&bytes)?;
                    let sidecar = Self::local_delete_sidecar_from_record(&record)?;
                    let sidecar_key = Self::durable_row_lineage_config_key(
                        &record.table,
                        &record.natural_key,
                        record.table_generation,
                    );
                    if self.local_delete_sidecar_slot_is_eligible(
                        &lineage_state,
                        &sidecar_key,
                        local_row_id,
                    )? {
                        self.store_lineage_record_and_row_sidecar(
                            &mut lineage_state,
                            &record_key,
                            &record,
                            &sidecar_key,
                            &sidecar,
                        )?;
                    } else {
                        self.store_lineage_record(&mut lineage_state, &record_key, &record)?;
                    }
                }
            }
            let author_node_id = record.author_node_id.clone().ok_or_else(|| {
                Error::SyncError((
                "pending delete lacks its creation lineage; refusing outbound provenance repair"
                    .to_string()).into())
            })?;
            let author_database_incarnation = record
                .author_database_incarnation
                .as_deref()
                .and_then(Incarnation::from_hex)
                .ok_or_else(|| {
                    Error::SyncError(
                        ("delete lineage has an invalid author incarnation".to_string()).into(),
                    )
                })?;
            Ok(Some(crate::protocol::WireRowLineage {
                author_node_id,
                author_database_incarnation,
                author_local_mutation_position: Lsn(record.author_local_mutation_position),
                table_generation: record.table_generation,
                lineage_root: record.lineage_root,
                attestation: record.lineage_attestation,
            }))
        };
        for _ in 0..3 {
            let (prepared, _credit) = self.prepare_outbound_lineage_signatures(
                row,
                table_generation,
                None,
                tenant_id,
                local_node_id,
                local_incarnation,
                signer,
                read_memory.clone(),
            )?;
            let changed = std::cell::Cell::new(false);
            let prepared_signer = |bytes: &[u8]| {
                if let Some((_, signature)) =
                    prepared.iter().find(|(candidate, _)| candidate == bytes)
                {
                    Ok(signature.clone())
                } else {
                    changed.set(true);
                    Err(Error::ConditionalUpdateConflict { count: 1 })
                }
            };
            let result = if self.tx_mgr.commit_boundary_held_by_current_thread() {
                bind(&prepared_signer)
            } else {
                self.tx_mgr
                    .with_maintenance_publication(|_| bind(&prepared_signer))
            };
            if !changed.get() {
                return result;
            }
        }
        Err(Error::ConditionalUpdateConflict { count: 1 })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn transaction_lineage_table_generation(
        &self,
        tx: TxId,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if crate::persistence::received_working_source::WorkingReadSource::current_for(
            self.tx_mgr.identity(),
        )?
        .is_some()
        {
            // A mutable working declaration is not owned by the old immutable
            // public root's identity. Read its actual private generation.
            self.tx_mgr.with_read(tx, |_| ())?;
            return self.durable_lineage_table_generation(table, read_memory);
        }
        let Some(root) = self.writer_read_root()? else {
            return self.durable_lineage_table_generation(table, read_memory);
        };
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            return self.transaction_lineage_table_generation_owned(tx, table, root, read_memory);
        }
        let identity = root.storage_identity();
        self.tx_mgr.with_read(tx, |_| ())?;
        let memo = {
            let mut pending = self.pending_commit_metadata.lock();
            let metadata = pending.entry(tx).or_default();
            if metadata.lineage_generations.is_none() {
                let control = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    std::mem::size_of::<PendingLineageGenerations>()
                        + 2 * std::mem::size_of::<usize>(),
                    "relational",
                    "lineage_generation_memo",
                    "Reduce concurrent staged tables or raise MEMORY_LIMIT.",
                )?;
                metadata.lineage_generations = Some(Arc::new(PendingLineageGenerations {
                    entries: Mutex::new(None),
                    _control: control,
                }));
            }
            metadata
                .lineage_generations
                .as_ref()
                .expect("admitted memo")
                .clone()
        };
        let mut entries = memo.entries.lock();
        let mut entry = entries.as_deref();
        while let Some(current) = entry {
            if current.table == table && current.root == identity {
                return Ok(current.generation);
            }
            entry = current.next.as_deref();
        }
        // Use the same root that stamps the memo, even if another transaction
        // publishes between the identity capture and the scalar read.
        let _scope = self.enter_writer_read(Some(root), read_memory.clone());
        let generation = self.durable_lineage_table_generation(table, read_memory)?;
        let mut entry = entries.as_deref_mut();
        while let Some(current) = entry {
            if current.table == table {
                current.root = identity;
                current.generation = generation;
                return Ok(generation);
            }
            entry = current.next.as_deref_mut();
        }
        let memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            std::mem::size_of::<PendingLineageGeneration>() + table.len(),
            "relational",
            "lineage_generation_memo",
            "Reduce concurrent staged tables or raise MEMORY_LIMIT.",
        )?;
        let next = entries.take();
        *entries = Some(Box::new(PendingLineageGeneration {
            table: table.to_string(),
            root: identity,
            generation,
            next,
            _memory: memory,
        }));
        Ok(generation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn transaction_lineage_table_generation_owned(
        &self,
        tx: TxId,
        table: &str,
        root: crate::persistence::WriterSnapshotHandle,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.tx_mgr.with_read(tx, |_| ())?;
        let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr else {
            return Err(DatabaseTransactionHost::producer_gap(
                "original lineage memo",
            ));
        };
        let existing = handle.with_host(|host| {
            let records = host.metadata.records.lock();
            EngineMetadataJournal::record(&records, tx)
                .and_then(|record| record.metadata.lineage_generations.clone())
        });
        let memo = if let Some(memo) = existing {
            memo
        } else {
            let candidate = handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    event_bus::OwnedEventShared::try_new(
                        NativeLineageGenerations {
                            entries: Mutex::new(None),
                        },
                        original,
                        &host.funder,
                    )
                })
            })?;
            self.publish_owned_metadata(tx, |journal| {
                journal.prepare_metadata_memo(tx, Some(candidate.clone()))
            })?;
            candidate
        };
        let identity = root.storage_identity();
        {
            let entries = memo.entries.lock();
            let mut cursor = entries.as_ref();
            while let Some(node) = cursor {
                let entry = node.get();
                if entry.table == table && entry.root == identity {
                    return Ok(entry.generation);
                }
                cursor = entry.next.as_ref();
            }
        }
        // No active-map, journal or memo guard crosses a reader provider.
        let generation = {
            let _scope = self.enter_writer_read(Some(root), read_memory.clone());
            self.durable_lineage_table_generation(table, read_memory)?
        };
        {
            let mut entries = memo.entries.lock();
            let mut cursor = entries.as_mut();
            while let Some(node) = cursor {
                let entry = node.get_mut();
                if entry.table == table {
                    entry.root = identity;
                    entry.generation = generation;
                    return Ok(generation);
                }
                cursor = entry.next.as_mut();
            }
        }
        let mut candidate = self.with_preparation_original(|original| {
            let (table, memory) =
                crate::memory_accounting::copy_owned_native_string(original, table)?;
            MetadataNode::prepare(
                NativeLineageGeneration {
                    table,
                    root: identity,
                    generation,
                    next: None,
                    _table_memory: memory,
                },
                original,
            )
        })?;
        let mut entries = memo.entries.lock();
        let mut cursor = entries.as_mut();
        while let Some(node) = cursor {
            let entry = node.get_mut();
            if entry.table == table {
                entry.root = identity;
                entry.generation = generation;
                drop(entries);
                drop(candidate);
                return Ok(generation);
            }
            cursor = entry.next.as_mut();
        }
        candidate.get_mut().next = entries.take();
        *entries = Some(candidate);
        Ok(generation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn durable_lineage_table_generation(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(_persistence) = &self.persistence {
            let _operation = self.open_operation()?;
            return self
                .read_config_value::<u64>(
                    &Self::durable_lineage_table_generation_key(table),
                    read_memory.clone(),
                )?
                .filter(|generation| *generation != 0)
                .ok_or_else(|| {
                    crate::funded_error::corruption_display(
                        Ok(read_memory.clone()),
                        Self::durable_lineage_table_generation_key(table),
                        format_args!(
                            "table {table} has schema metadata but no authoritative generation"
                        ),
                    )
                });
        }
        self.in_memory_table_generations
            .lock()
            .get(table)
            .copied()
            .filter(|generation| *generation != 0)
            .ok_or_else(|| {
                Error::SyncError(
                    (format!("table {table} has no authoritative schema generation")).into(),
                )
            })
    }

    /// Return the authoritative generation counter even when no live table
    /// currently owns it.  A dropped table deliberately retains this counter
    /// so a later same-name CREATE cannot reuse its former identity.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_table_generation_counter(&self, table: &str) -> Result<u64> {
        if let Some(persistence) = &self.persistence {
            return persistence
                .load_config_value::<u64>(&Self::durable_lineage_table_generation_key(table))
                .map(|generation| generation.unwrap_or(0));
        }
        Ok(self
            .in_memory_table_generations
            .lock()
            .get(table)
            .copied()
            .unwrap_or(0))
    }

    /// Allocate the next generation before a local CREATE is persisted.  A
    /// missing record is valid only for this first CREATE; reopening a schema
    /// without its record is corruption and is never reconstructed from DDL
    /// history.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn next_table_generation_for_create(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(_persistence) = &self.persistence {
            let _operation = self.open_operation()?;
            return self
                .read_config_value::<u64>(
                    &Self::durable_lineage_table_generation_key(table),
                    read_memory.clone(),
                )?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| {
                    Error::SyncError((format!("schema generation overflow for {table}")).into())
                });
        }
        self.in_memory_table_generations
            .lock()
            .get(table)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                Error::SyncError((format!("schema generation overflow for {table}")).into())
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_created_table_generation_and_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        generation: u64,
        lsn: Lsn,
    ) -> Result<()> {
        let change = ddl_change_from_meta(name, meta);
        let sidecar = DurableDdlGenerationSidecar {
            table: Some(name.to_string()),
            table_generation: Some(generation),
        };
        let memory_history = self.prepare_memory_schema_history(
            lsn,
            0,
            std::slice::from_ref(&change),
            std::slice::from_ref(&sidecar),
        )?;
        if let Some(source) =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                self.tx_mgr.identity(),
            )?
        {
            source.project_schema(name, Some(meta), std::slice::from_ref(&change))?;
        }
        let generation_key = Self::durable_lineage_table_generation_key(name);
        let sidecar_key = Self::durable_ddl_generation_sidecar_key(lsn, 0);
        let encoded_generation = RedbPersistence::encode_config_value(&generation)?;
        let encoded_sidecar = RedbPersistence::encode_config_value(&sidecar)?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.flush_table_meta_with_config_values_and_append_ddl_log(
                    name,
                    meta,
                    vec![
                        (generation_key.as_str(), encoded_generation),
                        (sidecar_key.as_str(), encoded_sidecar),
                    ],
                    lsn,
                    &change,
                )
            })?;
        } else {
            self.in_memory_table_generations
                .lock()
                .insert(name.to_string(), generation);
        }
        self.publish_memory_schema_history(memory_history);
        Ok(())
    }

    pub(crate) fn ddl_affected_table(ddl: &DdlChange) -> Option<&str> {
        match ddl {
            DdlChange::CreateTable { name, .. }
            | DdlChange::DropTable { name }
            | DdlChange::AlterTable { name, .. } => Some(name),
            DdlChange::CreateIndex { table, .. } | DdlChange::DropIndex { table, .. } => {
                Some(table)
            }
            DdlChange::CreateTrigger { table, .. }
            | DdlChange::CreateTriggerIncludingSync { table, .. }
            | DdlChange::CreateEventType { table, .. }
            | DdlChange::CreateRoute { table, .. }
            | DdlChange::DropRoute { table, .. }
                if !table.is_empty() =>
            {
                Some(table)
            }
            DdlChange::DropTrigger { .. }
            | DdlChange::CreateSink { .. }
            | DdlChange::CreateTrigger { .. }
            | DdlChange::CreateTriggerIncludingSync { .. }
            | DdlChange::CreateEventType { .. }
            | DdlChange::CreateRoute { .. }
            | DdlChange::DropRoute { .. } => None,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ddl_generation_sidecar_values(
        &self,
        lsn: Lsn,
        ddl: &[DdlChange],
        start: u32,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.ddl_generation_sidecars(ddl, read_memory.clone())?
            .into_iter()
            .enumerate()
            .map(|(offset, sidecar)| {
                let key = Self::durable_ddl_generation_sidecar_key(lsn, start + offset as u32);
                let encoded = RedbPersistence::encode_config_value(&sidecar)?;
                Ok((key, encoded))
            })
            .collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ddl_generation_sidecars(
        &self,
        ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurableDdlGenerationSidecar>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        ddl.iter()
            .map(|change| {
                let table = Self::ddl_affected_table(change).map(str::to_string);
                let table_generation = table
                    .as_deref()
                    .map(|table| self.durable_lineage_table_generation(table, read_memory.clone()))
                    .transpose()?;
                Ok(DurableDdlGenerationSidecar {
                    table,
                    table_generation,
                })
            })
            .collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_lineage_record(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<DurableLineageRecord>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
        self.durable_lineage_record_at_generation(table, natural_key, generation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_lineage_record_at_generation(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> Result<Option<DurableLineageRecord>> {
        let lineage_state = self.lineage_state_lock.lock();
        self.load_lineage_record(
            &lineage_state,
            &Self::durable_lineage_config_key(table, natural_key, generation),
        )
    }

    // A retained occurrence can outlive its current table and indexes. Resolve
    // its storage identity from the exact change group, then authenticate the
    // addressed body; a current natural-key index cannot prove that identity.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn exact_outbound_row_occurrence(
        &self,
        row: &RowChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.table_meta(&row.table).is_some() {
            let snapshot = self.history_change_snapshot_at(
                row.lsn,
                read_memory.clone(),
                &unbounded_checkpoint,
            )?;
            if let Some(Some(row_id)) = self.indexed_visible_row_id_by_columns(
                &row.table,
                &row.natural_key.key_columns(),
                &row.natural_key.key_values(),
                snapshot,
                &HashSet::new(),
                None,
                read_memory.clone(),
            )? && let Some(found) = self.committed_indexed_point_row_at(
                &row.table,
                row_id,
                snapshot,
                read_memory.clone(),
            )? && found.lsn == row.lsn
            {
                return Ok(Some(found));
            }
        }
        let mut changes = self
            .change_replay_since_unlocked(Lsn(row.lsn.0.saturating_sub(1)), read_memory.clone())?;
        while let Some(window) = changes.next_entry()? {
            let entry = window.value;
            if entry.lsn() > row.lsn {
                break;
            }
            if let ChangeLogEntry::RowInsert { table, row_id, lsn } = entry {
                if table != row.table || lsn != row.lsn {
                    continue;
                }
                if let Some(found) = self.row_for_change(
                    &table,
                    row_id,
                    lsn,
                    read_memory.clone(),
                    &unbounded_checkpoint,
                    None,
                )? && row.natural_key.matches_values(&found.values)
                {
                    return Ok(Some(found));
                }
            }
        }
        if self.table_meta(&row.table).is_some() {
            return self
                .visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    self.snapshot_for_read(),
                    &HashSet::new(),
                    read_memory,
                )
                .map(|current| current.filter(|current| current.lsn == row.lsn));
        }
        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn outbound_row_generation(
        &self,
        row: &RowChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<u64> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let generation = if let Some(root) = self.writer_read_root()? {
            let owner =
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone());
            if let Some(declaration) =
                RedbPersistence::declared_meta_in_read(&root, owner.clone(), &row.table, row.lsn)?
            {
                let (_, lsn, ordinal) = &declaration.value;
                root.config_point::<DurableDdlGenerationSidecar>(
                    owner,
                    &Self::durable_ddl_generation_sidecar_key(*lsn, *ordinal),
                )?
                .map(|window| {
                    if window.value.table.as_deref() != Some(row.table.as_str()) {
                        return Err(Error::SyncError(
                            "selected historical row declaration names another table".into(),
                        ));
                    }
                    window.value.table_generation.ok_or_else(|| {
                        Error::SyncError(
                            "selected historical row declaration has no generation evidence".into(),
                        )
                    })
                })
                .transpose()?
            } else {
                None
            }
        } else {
            // Borrow the immutable occurrence's sidecar. No historical table
            // name or decoded source window escapes its funding owner.
            self.in_memory_ddl_generations
                .lock()
                .iter()
                .filter(|((lsn, _), (table, generation))| {
                    *lsn <= row.lsn
                        && table.as_deref() == Some(row.table.as_str())
                        && generation.is_some()
                })
                .max_by_key(|(coordinate, _)| **coordinate)
                .and_then(|(_, (_, generation))| *generation)
        };
        if let Some(generation) = generation {
            return Ok(generation);
        }
        if self.table_meta(&row.table).is_some()
            && self
                .visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    self.snapshot_for_read(),
                    &HashSet::new(),
                    read_memory.clone(),
                )?
                .is_some_and(|current| current.lsn == row.lsn)
        {
            self.durable_lineage_table_generation(&row.table, read_memory)
        } else {
            Err(Error::SyncError(
                "selected historical row lacks immutable declaration generation evidence".into(),
            ))
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn outbound_row_lineages(
        &self,
        changes: &ChangeSet,
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OutboundRowLineages> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        // All lineage and coordination locks nest inside the commit boundary.
        // The stores inherit this guard and owe publication after it unlocks.

        let mut out = HashMap::new();
        // Bind before iterating so an invisible historical INSERT and its
        // later DELETE share one creator sidecar regardless of their order in
        // the outbound change set.
        for row in changes.rows.iter().filter(|row| row.deleted) {
            let _ = self.pending_delete_lineage(
                row,
                tenant_id,
                local_node_id,
                local_incarnation,
                signer,
                read_memory.clone(),
            )?;
        }
        for row in &changes.rows {
            let _row_outputs = self.writer_output_scope();
            // A locally staged delete is no longer a live row, so its
            // config-keyed obligation is the authoritative provenance record.
            // Redb retains it across reopen; memory retains it for this
            // database life. It is stamped with the fabric identity before
            // encoding; a missing stamp is a send failure in SyncClient, never
            // attributed to the
            // relay that happens to carry it.
            if row.deleted {
                if let Some(lineage) = self.pending_delete_lineage(
                    row,
                    tenant_id,
                    local_node_id,
                    local_incarnation,
                    signer,
                    read_memory.clone(),
                )? {
                    out.insert(
                        (
                            row.table.clone(),
                            rmp_serde::to_vec(&row.natural_key)
                                .expect("sync natural key serializes"),
                            row.lsn,
                        ),
                        lineage,
                    );
                    continue;
                }
                let generation =
                    self.durable_lineage_table_generation(&row.table, read_memory.clone())?;
                let record_key =
                    Self::durable_lineage_config_key(&row.table, &row.natural_key, generation);
                let record = {
                    let lineage_state = self.lineage_state_lock.lock();
                    self.load_lineage_record(&lineage_state, &record_key)?
                };
                let Some(record) = record else {
                    return Err(Error::SyncError(
                        ("selected delete has neither matching pending nor exact accepted lineage"
                            .to_string())
                        .into(),
                    ));
                };
                if record.table != row.table
                    || record.natural_key != row.natural_key
                    || record.table_generation != generation
                    || record.delete_obligation != DurableDeleteObligation::Accepted
                    || record.delete_lsn != row.lsn.0
                {
                    return Err(Error::SyncError(
                        ("selected delete has neither matching pending nor exact accepted lineage"
                            .to_string())
                        .into(),
                    ));
                }
                let author_node_id = record.author_node_id.clone().ok_or_else(|| {
                    Error::SyncError(
                        ("selected accepted delete lineage lacks an author".to_string()).into(),
                    )
                })?;
                let author_database_incarnation =
                    record
                        .author_database_incarnation
                        .as_deref()
                        .and_then(Incarnation::from_hex)
                        .ok_or_else(|| {
                            Error::SyncError((
                            "selected accepted delete lineage has an invalid author incarnation"
                                .to_string()).into())
                        })?;
                let expected_root = format!(
                    "author:{}:{}:{}",
                    author_node_id,
                    author_database_incarnation.to_hex(),
                    record.author_local_mutation_position,
                );
                if record.lineage_root != expected_root {
                    return Err(Error::SyncError(
                        ("selected accepted delete lineage has an invalid creator root"
                            .to_string())
                        .into(),
                    ));
                }
                let lineage = crate::protocol::WireRowLineage {
                    author_node_id,
                    author_database_incarnation,
                    author_local_mutation_position: Lsn(record.author_local_mutation_position),
                    table_generation: record.table_generation,
                    lineage_root: record.lineage_root,
                    attestation: record.lineage_attestation,
                };
                Self::verify_lineage_attestation(tenant_id, &row.table, &row.natural_key, &lineage)
                    .map_err(|_| {
                        Error::SyncError(
                            ("selected accepted delete lineage has an invalid attestation"
                                .to_string())
                            .into(),
                        )
                    })?;
                out.insert(
                    (
                        row.table.clone(),
                        rmp_serde::to_vec(&row.natural_key).expect("sync natural key serializes"),
                        row.lsn,
                    ),
                    lineage,
                );
                continue;
            }

            // Push does not invent the creation position: before wire
            // encoding it binds exact committed unbound creation evidence to
            // the stable authenticated identity, and retries reuse that bound
            // sidecar. Sampling a live row/current LSN here races compaction
            // and turns a retry into a different creator tuple.
            let live_row = self.exact_outbound_row_occurrence(row, read_memory.clone())?;
            let generation = self.outbound_row_generation(row, read_memory.clone())?;
            let sidecar_key =
                Self::durable_row_lineage_config_key(&row.table, &row.natural_key, generation);
            let bind = |signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>| {
                let mut lineage_state = self.lineage_state_lock.lock();
                let sidecar = self
                    .load_row_lineage_sidecar(&lineage_state, &sidecar_key)?
                    .filter(|sidecar| {
                        if sidecar.locally_created
                            && sidecar.author_local_mutation_position > row.lsn
                        {
                            return false;
                        }
                        match (
                            sidecar.local_row_id,
                            live_row.as_ref().map(|live| live.row_id),
                        ) {
                            // A visible row with no bound local instance is a
                            // fresh explicit same-key creation after a delete. It
                            // must have been staged at its INSERT, never invented
                            // during send.
                            (None, Some(_)) => false,
                            // Without the selected historical row, today's
                            // sidecar cannot prove that change's creator.
                            (_, None) => false,
                            (Some(bound), Some(live)) => bound == live,
                        }
                    });
                // A received creator's position belongs to its origin, not
                // our local change stream. Bind its older life by local row
                // identity and the retained deletion record instead.
                let sidecar = if sidecar.is_none() {
                    let record_key =
                        Self::durable_lineage_config_key(&row.table, &row.natural_key, generation);
                    match self
                        .load_lineage_record(&lineage_state, &record_key)?
                        .filter(|record| {
                            !record.locally_created
                                && record.table == row.table
                                && record.natural_key == row.natural_key
                                && record.table_generation == generation
                                && record.local_row_id == live_row.as_ref().map(|live| live.row_id)
                                && record.local_row_id.is_some()
                                && row.lsn.0 <= record.delete_lsn
                                && !record.lineage_attestation.is_empty()
                        }) {
                        Some(record) => Some(DurableRowLineageSidecar {
                            author_node_id: record.author_node_id.ok_or_else(|| {
                                Error::SyncError("retained lineage lacks its creator".into())
                            })?,
                            author_database_incarnation: record
                                .author_database_incarnation
                                .as_deref()
                                .and_then(Incarnation::from_hex)
                                .ok_or_else(|| {
                                    Error::SyncError(
                                        "retained lineage has an invalid creator incarnation"
                                            .into(),
                                    )
                                })?,
                            author_local_mutation_position: Lsn(
                                record.author_local_mutation_position
                            ),
                            table_generation: record.table_generation,
                            lineage_root: record.lineage_root,
                            lineage_attestation: record.lineage_attestation,
                            local_row_id: record.local_row_id,
                            locally_created: false,
                        }),
                        None => None,
                    }
                } else {
                    sidecar
                };
                Ok(match sidecar {
                    Some(sidecar) => {
                        if sidecar.locally_created {
                            let local_row_id =
                                sidecar.local_row_id.ok_or_else(|| {
                                    Error::SyncError((format!(
                                        "locally created row {} {:?} lacks its pinned local row",
                                        row.table, row.natural_key
                                    )).into())
                                })?;
                            self.bind_unbound_creation_lineage_with_state(
                                &mut lineage_state,
                                tenant_id,
                                &row.table,
                                &row.natural_key,
                                generation,
                                local_row_id,
                                local_node_id,
                                local_incarnation,
                                signer,
                            )?
                        } else {
                            sidecar
                        }
                    }
                    None => {
                        let local_row_id =
                            live_row.as_ref().map(|row| row.row_id).ok_or_else(|| {
                                Error::SyncError((format!(
                                    "row {} {:?} has no pinned local row for creation-lineage binding",
                                    row.table, row.natural_key
                                )).into())
                            })?;
                        self.bind_unbound_creation_lineage_with_state(
                            &mut lineage_state,
                            tenant_id,
                            &row.table,
                            &row.natural_key,
                            generation,
                            local_row_id,
                            local_node_id,
                            local_incarnation,
                            signer,
                        )?
                    }
                })
            };
            let mut bound = None;
            for _ in 0..3 {
                let (prepared, _credit) = self.prepare_outbound_lineage_signatures(
                    row,
                    generation,
                    live_row.as_ref().map(|r| r.row_id),
                    tenant_id,
                    local_node_id,
                    local_incarnation,
                    signer,
                    read_memory.clone(),
                )?;
                let changed = std::cell::Cell::new(false);
                let prepared_signer = |bytes: &[u8]| {
                    if let Some((_, signature)) =
                        prepared.iter().find(|(candidate, _)| candidate == bytes)
                    {
                        Ok(signature.clone())
                    } else {
                        changed.set(true);
                        Err(Error::ConditionalUpdateConflict { count: 1 })
                    }
                };
                let result = if self.tx_mgr.commit_boundary_held_by_current_thread() {
                    bind(&prepared_signer)
                } else {
                    self.tx_mgr
                        .with_maintenance_publication(|_| bind(&prepared_signer))
                };
                if !changed.get() {
                    bound = Some(result?);
                    break;
                }
            }
            let sidecar = bound.ok_or(Error::ConditionalUpdateConflict { count: 1 })?;
            out.insert(
                (
                    row.table.clone(),
                    rmp_serde::to_vec(&row.natural_key).expect("sync natural key serializes"),
                    row.lsn,
                ),
                crate::protocol::WireRowLineage {
                    author_node_id: sidecar.author_node_id,
                    author_database_incarnation: sidecar.author_database_incarnation,
                    author_local_mutation_position: sidecar.author_local_mutation_position,
                    table_generation: sidecar.table_generation,
                    lineage_root: sidecar.lineage_root,
                    attestation: sidecar.lineage_attestation,
                },
            );
        }
        Ok(out)
    }

    /// Advance one pull-page projection only from the exact authenticated DDL
    /// vector that accompanied this unit. The generation overlay never
    /// reconstructs evidence from schema text, while the direction overlay
    /// mirrors the CREATE/ALTER/DROP semantics of
    /// `latest_declared_table_directions` in received-vector order.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn advance_authenticated_pull_schema_projection(
        &self,
        table_generations: &mut HashMap<String, u64>,
        directions: &mut HashMap<String, SyncDirection>,
        changes: &ChangeSet,
        received_ddl: Option<&crate::protocol::ReceivedDdlContext>,
    ) -> Result<()> {
        if changes.ddl.is_empty() {
            return Ok(());
        }
        let received_ddl = received_ddl.ok_or_else(|| {
            Error::SyncError(
                ("pull schema projection requires authenticated DDL provenance".to_string()).into(),
            )
        })?;
        if changes.ddl.len() != changes.ddl_lsn.len()
            || changes.ddl.len() != received_ddl.entries.len()
        {
            return Err(Error::SyncError(
                ("authenticated pull schema projection does not cover the exact DDL vector"
                    .to_string())
                .into(),
            ));
        }
        for ((change, source_lsn), entry) in changes
            .ddl
            .iter()
            .zip(changes.ddl_lsn.iter())
            .zip(received_ddl.entries.iter())
        {
            if *source_lsn != entry.source_ddl_lsn {
                return Err(Error::SyncError(
                    ("pull schema projection source LSN differs from authenticated DDL provenance"
                        .to_string())
                    .into(),
                ));
            }
            let table = Self::ddl_affected_table(change);
            if table != entry.table.as_deref() {
                return Err(Error::SyncError(
                    ("pull schema projection table differs from authenticated DDL provenance"
                        .to_string())
                    .into(),
                ));
            }
            if let (Some(table), Some(generation)) = (table, entry.table_generation) {
                table_generations.insert(table.to_string(), generation);
            }
            match change {
                DdlChange::CreateTable {
                    name, constraints, ..
                } => {
                    let direction = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                        .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION);
                    directions.insert(name.clone(), direction);
                }
                DdlChange::AlterTable {
                    name, constraints, ..
                } => {
                    if let Some(direction) = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                    {
                        directions.insert(name.clone(), direction);
                    }
                }
                // As the durable projection does, retain the final declared
                // direction after DROP so the former generation cannot become
                // eligible merely because its live metadata disappears.
                DdlChange::DropTable { .. } => {}
                _ => {}
            }
        }
        Ok(())
    }

    /// Verify every received row against an explicitly authenticated
    /// page-generation projection. A table missing from the projection uses
    /// the durable local generation; an entry in the projection is never
    /// inferred from DDL text or another unit's context.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_received_row_lineages_against_generation_projection(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        table_generations: &HashMap<String, u64>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for row in &changes.rows {
            let generation = table_generations
                .get(&row.table)
                .copied()
                .map(Ok)
                .unwrap_or_else(|| {
                    self.durable_lineage_table_generation(&row.table, read_memory.clone())
                })?;
            Self::validate_received_row_lineage_generation(tenant_id, row, lineages, generation)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_received_row_lineage_generation(
        tenant_id: &TenantId,
        row: &RowChange,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        generation: u64,
    ) -> Result<()> {
        let mut matching = lineages.iter().filter(|(table, key, lsn, _)| {
            *table == row.table && *key == row.natural_key && *lsn == row.lsn
        });
        let lineage = matching.next().filter(|_|matching.next().is_none()).map(|entry|&entry.3)
            .ok_or_else(||Error::SyncError((format!("authenticated sync requires exactly one lineage for {} {:?} at source LSN {}",
                row.table,row.natural_key,row.lsn.0)).into()))?;
        if lineage.table_generation != generation {
            return Err(Error::SyncError((format!("wire row lineage generation {} does not match the authenticated projected generation {} for table {}",
                lineage.table_generation,generation,row.table)).into()));
        }
        let expected_root = format!(
            "author:{}:{}:{}",
            lineage.author_node_id,
            lineage.author_database_incarnation.to_hex(),
            lineage.author_local_mutation_position.0,
        );
        if lineage.lineage_root != expected_root {
            return Err(Error::SyncError(
                ("wire row lineage root does not match its immutable creator tuple".to_string())
                    .into(),
            ));
        }
        Self::verify_lineage_attestation(tenant_id, &row.table, &row.natural_key, lineage)?;
        Ok(())
    }

    /// Validate authenticated provenance before any row mutation. Every data
    /// row has exactly one full-identity sidecar, its root must be the creator
    /// tuple's canonical root, and its table generation must still be live.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_received_row_lineages(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !changes.ddl.is_empty() {
            return Err(Error::SyncError(
                ("received rows beside schema DDL require an authenticated whole-unit schema plan"
                    .to_string())
                .into(),
            ));
        }
        self.validate_received_row_lineages_against_generation_projection(
            tenant_id,
            changes,
            lineages,
            &HashMap::new(),
            read_memory.clone(),
        )
    }

    /// The authenticated schema path validates rows against the generation
    /// projected by the ordered received DDL vector, not the receiver's
    /// pre-apply metadata. The raw path above remains deliberately
    /// fail-closed when DDL is present.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_received_row_lineages_with_received_ddl(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: &crate::protocol::ReceivedDdlContext,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_source_ordered_row_lineages(
            tenant_id,
            changes,
            lineages,
            received_ddl,
            &HashMap::new(),
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_source_ordered_row_lineages(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received: &crate::protocol::ReceivedDdlContext,
        initial: &HashMap<String, u64>,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        for row in &changes.rows {
            let generation = received
                .entries
                .iter()
                .rev()
                .find(|entry| {
                    entry.source_ddl_lsn <= row.lsn
                        && entry.table.as_deref() == Some(row.table.as_str())
                        && entry.table_generation.is_some()
                })
                .and_then(|entry| entry.table_generation)
                .or_else(|| initial.get(&row.table).copied())
                .map(Ok)
                .unwrap_or_else(|| {
                    self.durable_lineage_table_generation(&row.table, memory.clone())
                })?;
            Self::validate_received_row_lineage_generation(tenant_id, row, lineages, generation)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn classify_source_ordered_pull_replays(
        &self,
        tenant: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received: Option<&crate::protocol::ReceivedDdlContext>,
        initial: &HashMap<String, u64>,
        dependency: bool,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AcceptedDeleteSuppression> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        if let Some(received) = received {
            self.validate_source_ordered_row_lineages(
                tenant,
                &changes,
                lineages,
                received,
                initial,
                memory.clone(),
            )?;
        } else {
            self.validate_received_row_lineages_against_generation_projection(
                tenant,
                &changes,
                lineages,
                initial,
                memory.clone(),
            )?;
        }
        // Authenticated pull consumes historical members whose exact creator
        // is already durably terminated. Do this before the raw classifier:
        // raw callers must still refuse when a fresh full-key row is visible.
        let mut changes = changes;
        let mut suppressed = false;
        if !dependency {
            let (_catalog_charge, meta) = self.projected_sync_table_meta(&changes)?;
            let snapshot = self.snapshot_for_read();
            let empty_skip_deleted = HashSet::new();
            let mut vector_cursor = 0;
            let mut removed_vectors = HashSet::new();
            let mut retained = Vec::with_capacity(changes.rows.len());
            for row in changes.rows {
                let vector_end =
                    (meta.get(&row.table).is_some_and(|table| {
                        table
                            .columns
                            .iter()
                            .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    }) && next_vector_row_group_matches(&changes.vectors, vector_cursor, &row))
                    .then(|| vector_row_group_end(&changes.vectors, vector_cursor));
                let lineage = lineages.iter().find(|(table, key, lsn, _)| {
                    *table == row.table && *key == row.natural_key && *lsn == row.lsn
                });
                let terminated = if let Some((_, _, _, lineage)) = lineage {
                    self.durable_lineage_record_at_generation(
                        &row.table,
                        &row.natural_key,
                        lineage.table_generation,
                    )?
                    .is_some_and(|record| {
                        record.delete_obligation == DurableDeleteObligation::Accepted
                            && terminated_creator_matches(&record, lineage)
                    }) && (self.table_meta(&row.table).is_none()
                        || self
                            .visible_row_by_natural_key(
                                &row.table,
                                &row.natural_key,
                                snapshot,
                                &empty_skip_deleted,
                                memory.clone(),
                            )?
                            .is_none())
                } else {
                    false
                };
                if terminated {
                    suppressed = true;
                    if let Some(end) = vector_end {
                        removed_vectors.extend(vector_cursor..end);
                    }
                } else {
                    retained.push(row);
                }
                if let Some(end) = vector_end {
                    vector_cursor = end;
                }
            }
            changes.rows = retained;
            changes.vectors = changes
                .vectors
                .into_iter()
                .enumerate()
                .filter_map(|(index, vector)| (!removed_vectors.contains(&index)).then_some(vector))
                .collect();
        }
        let mut result = self.suppress_accepted_lineage_replays_with_generation_projection(
            changes, lineages, dependency, initial, memory,
        )?;
        result.suppressed_live_replay |= suppressed;
        Ok(result)
    }

    /// The table generation each table will hold once `received_ddl` is
    /// installed, in source order (a later entry for a table wins).
    pub(super) fn received_ddl_generation_projection(
        received_ddl: &crate::protocol::ReceivedDdlContext,
    ) -> HashMap<String, u64> {
        let mut projected_generations = HashMap::<String, u64>::new();
        for entry in &received_ddl.entries {
            if let (Some(table), Some(generation)) = (&entry.table, entry.table_generation) {
                projected_generations.insert(table.clone(), generation);
            }
        }
        projected_generations
    }

    /// An authenticated push can introduce a creator only from the sending
    /// edge's current database incarnation. A relayed creator must already
    /// be the committed config-keyed provenance for this exact
    /// table/key/generation; Redb retains it across reopen and memory retains
    /// it for this database life. A peer cannot forge a new ancestry by
    /// choosing someone else's author tuple.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_incoming_push_lineages(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        authenticated_peer: &str,
        sender_incarnation: Incarnation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages(tenant_id, changes, lineages, read_memory.clone())?;
        self.validate_incoming_push_lineage_authorship(
            changes,
            lineages,
            authenticated_peer,
            sender_incarnation,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_incoming_push_lineage_authorship(
        &self,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        authenticated_peer: &str,
        sender_incarnation: Incarnation,
    ) -> Result<()> {
        for row in &changes.rows {
            let (_, _, _, lineage) = lineages
                .iter()
                .find(|(table, natural_key, lsn, _)| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .expect("validated exact row lineage");
            if lineage.author_node_id == authenticated_peer
                && lineage.author_database_incarnation == sender_incarnation
            {
                continue;
            }
            let lineage_state = self.lineage_state_lock.lock();
            let known = self.load_row_lineage_sidecar(
                &lineage_state,
                &Self::durable_row_lineage_config_key(
                    &row.table,
                    &row.natural_key,
                    lineage.table_generation,
                ),
            )?;
            if !known.is_some_and(|known| Self::row_lineage_matches_wire(&known, lineage)) {
                // The creator's valid signature is portable provenance. A
                // freshly rebuilt hub need not have received this lineage
                // before it relays the row onward.
                if lineage.author_node_id.is_empty() {
                    return Err(Error::SyncError(
                        (format!(
                            "relayed row {} {:?} has no creator provenance",
                            row.table, row.natural_key
                        ))
                        .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn row_lineage_matches_wire(
        known: &DurableRowLineageSidecar,
        lineage: &crate::protocol::WireRowLineage,
    ) -> bool {
        known.table_generation == lineage.table_generation
            && known.author_node_id == lineage.author_node_id
            && known.author_database_incarnation == lineage.author_database_incarnation
            && known.author_local_mutation_position == lineage.author_local_mutation_position
            && known.lineage_root == lineage.lineage_root
            && known.lineage_attestation == lineage.attestation
    }

    /// Identify only fresh deletes authored by the creator whose immutable
    /// lineage the receiver already committed for this exact row instance.
    /// This is the narrow continuation KEEP FIRST permits: a competing edge,
    /// a relay, a stale ordered replay, or a same-key replacement all fail at
    /// least one of these checks.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticated_fresh_creator_deletes(
        &self,
        rows: &[RowChange],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        receipt: Option<&SyncApplyReceipt>,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<AuthenticatedFreshCreatorDelete>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(receipt) = receipt else {
            return Ok(Vec::new());
        };
        let snapshot = self.snapshot();
        let empty_cache = HashMap::new();
        let empty_deleted = HashSet::new();
        let mut permitted = Vec::new();

        for row in rows {
            let _row_outputs = self.writer_output_scope();
            if !row.deleted || Self::resolve_incoming_arrival(row, arrivals).is_some() {
                continue;
            }
            let Some((_, _, _, lineage)) = lineages.iter().find(|(table, natural_key, lsn, _)| {
                table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
            }) else {
                continue;
            };
            if lineage.author_node_id != receipt.node_id
                || lineage.author_database_incarnation != receipt.incarnation
            {
                continue;
            }
            if self.table_meta(&row.table).is_none() {
                continue;
            }
            let Some(local) = sync_visible_point_lookup(
                self,
                &empty_cache,
                &row.table,
                &row.natural_key,
                snapshot,
                &empty_deleted,
                read_memory.clone(),
            )?
            else {
                continue;
            };
            let lineage_key = Self::durable_row_lineage_config_key(
                &row.table,
                &row.natural_key,
                lineage.table_generation,
            );
            let lineage_state = self.lineage_state_lock.lock();
            let known = self.load_row_lineage_sidecar(&lineage_state, &lineage_key)?;
            if known.as_ref().is_some_and(|known| {
                known.local_row_id == Some(local.row_id)
                    && Self::row_lineage_matches_wire(known, lineage)
            }) {
                permitted.push(AuthenticatedFreshCreatorDelete {
                    table: row.table.clone(),
                    natural_key: row.natural_key.clone(),
                    lsn: row.lsn,
                    row_id: local.row_id,
                    values: local.values,
                });
            }
        }
        Ok(permitted)
    }

    pub(super) fn authenticated_fresh_creator_delete_permission<'a>(
        row: &RowChange,
        local: &VersionedRow,
        policy: ConflictPolicy,
        permitted: &'a [AuthenticatedFreshCreatorDelete],
    ) -> Option<&'a AuthenticatedFreshCreatorDelete> {
        (policy == ConflictPolicy::InsertIfNotExists)
            .then(|| {
                permitted.iter().find(|permission| {
                    permission.table == row.table
                        && permission.natural_key == row.natural_key
                        && permission.lsn == row.lsn
                        && permission.row_id == local.row_id
                })
            })
            .flatten()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_incoming_push_lineages_with_received_ddl(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        authenticated_peer: &str,
        sender_incarnation: Incarnation,
        received_ddl: &crate::protocol::ReceivedDdlContext,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages_with_received_ddl(
            tenant_id,
            changes,
            lineages,
            received_ddl,
            read_memory.clone(),
        )?;
        self.validate_incoming_push_lineage_authorship(
            changes,
            lineages,
            authenticated_peer,
            sender_incarnation,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_purged_lineage_descendants(
        &self,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
    ) -> Result<PurgedLineageRefusal> {
        // The purge journal retains every known source lineage,
        // while the local lifecycle record names the materialized incumbent.
        let purged_copies = self.authoritative_purge_delivery_items_since(Lsn(0))?;
        let (_catalog_charge, projected_meta) = self.projected_sync_table_meta(&changes)?;
        let policies = Self::declared_sync_policies(true);
        let mut retained_rows = Vec::with_capacity(changes.rows.len());
        let mut refused_rows = Vec::new();
        let mut conflicts = Vec::new();
        let mut refused_vector_indexes = HashSet::new();
        let mut vector_cursor = 0usize;
        for row in &changes.rows {
            let matching = lineages
                .iter()
                .filter(|(table, natural_key, lsn, _)| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(Error::SyncError((format!(
                    "purged-lineage classification requires exactly one validated lineage for {} {:?} at source LSN {}",
                    row.table, row.natural_key, row.lsn.0
                )).into()));
            }
            let lineage = &matching[0].3;
            let purged_lives = self.purged_lineage_records_at_generation(
                &row.table,
                &row.natural_key,
                lineage.table_generation,
            )?;
            let creator_incarnation = lineage.author_database_incarnation.to_hex();
            let matches_purged_lineage = purged_copies.iter().any(|item| {
                item.table == row.table
                    && item.natural_key == row.natural_key
                    && item.table_generation == lineage.table_generation
                    && item.purged_lineage_roots.contains(&lineage.lineage_root)
            }) || purged_lives.iter().any(|record| {
                record.delete_obligation == DurableDeleteObligation::Purged
                    && record.table == row.table
                    && record.natural_key == row.natural_key
                    && record.table_generation == lineage.table_generation
                    && record.lineage_root == lineage.lineage_root
                    && record.author_node_id.as_deref() == Some(lineage.author_node_id.as_str())
                    && record.author_database_incarnation.as_deref()
                        == Some(creator_incarnation.as_str())
                    && record.author_local_mutation_position
                        == lineage.author_local_mutation_position.0
            });
            let row_has_vector = projected_meta.get(&row.table).is_some_and(|meta| {
                meta.columns
                    .iter()
                    .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
            });
            let vector_group = (row_has_vector
                && next_vector_row_group_matches(&changes.vectors, vector_cursor, row))
            .then(|| vector_row_group_end(&changes.vectors, vector_cursor));
            if matches_purged_lineage {
                if let Some(end) = vector_group {
                    refused_vector_indexes.extend(vector_cursor..end);
                    vector_cursor = end;
                }
                refused_rows.push(row.clone());
                conflicts.push(Conflict {
                    natural_key: row.natural_key.clone(),
                    resolution: Self::sync_conflict_policy_for_table(
                        &policies,
                        self.table_meta(&row.table).as_ref(),
                        &row.table,
                    ),
                    reason: Some(crate::sync_types::PURGED_LINEAGE_CONFLICT_REASON.to_string()),
                    table: Some(row.table.clone()),
                    mutation_kind: Some(if row.deleted { "delete" } else { "edit" }.to_string()),
                    winning_author_node_id: None,
                    hub_acceptance_position: None,
                    refusal_cause: None,
                });
                continue;
            }
            retained_rows.push(row.clone());
            if let Some(end) = vector_group {
                vector_cursor = end;
            }
        }
        let vectors = changes
            .vectors
            .into_iter()
            .enumerate()
            .filter_map(|(index, vector)| {
                (!refused_vector_indexes.contains(&index)).then_some(vector)
            })
            .collect();
        Ok(PurgedLineageRefusal {
            changes: ChangeSet {
                rows: retained_rows,
                vectors,
                edges: changes.edges,
                ddl: changes.ddl,
                ddl_lsn: changes.ddl_lsn,
            },
            refused_rows,
            conflicts,
        })
    }

    /// Return one terminal, row-addressable refusal for each incoming row
    /// whose authenticated lineage belongs to a retired table generation.
    /// A purged root keeps its stronger permanent-erasure reason; every other
    /// row from that generation is refused as removed-generation work.  The
    /// caller may send this receipt only when it covers the whole request, so
    /// no unclassified member can mutate beside it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn retired_generation_refusals(
        &self,
        tenant_id: &TenantId,
        changes: &ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        authenticated_peer: &str,
        sender_incarnation: Incarnation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<Conflict>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Verify the immutable, portable creator evidence before consulting a
        // retired generation.  A relay is allowed to carry a creator's signed
        // lineage, but an unauthenticated or malformed row must never learn
        // whether that old key was purged or merely belongs to a removed table.
        for row in &changes.rows {
            let matching = lineages
                .iter()
                .filter(|(table, natural_key, lsn, _)| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(Error::SyncError((format!(
                    "authenticated sync requires exactly one lineage for {} {:?} at source LSN {}",
                    row.table, row.natural_key, row.lsn.0
                )).into()));
            }
            let lineage = &matching[0].3;
            let expected_root = format!(
                "author:{}:{}:{}",
                lineage.author_node_id,
                lineage.author_database_incarnation.to_hex(),
                lineage.author_local_mutation_position.0,
            );
            if lineage.lineage_root != expected_root {
                return Err(Error::SyncError(
                    ("wire row lineage root does not match its immutable creator tuple"
                        .to_string())
                    .into(),
                ));
            }
            Self::verify_lineage_attestation(tenant_id, &row.table, &row.natural_key, lineage)?;
        }
        self.validate_incoming_push_lineage_authorship(
            changes,
            lineages,
            authenticated_peer,
            sender_incarnation,
        )?;

        let policies = Self::declared_sync_policies(true);
        let mut conflicts = Vec::new();
        for row in &changes.rows {
            let matching = lineages
                .iter()
                .filter(|(table, natural_key, lsn, _)| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(Error::SyncError((format!(
                    "authenticated sync requires exactly one lineage for {} {:?} at source LSN {}",
                    row.table, row.natural_key, row.lsn.0
                )).into()));
            }
            let lineage = &matching[0].3;
            let current_generation =
                self.durable_lineage_table_generation(&row.table, read_memory.clone())?;
            // Only an older, retired table generation has a terminal
            // no-retry outcome. A future generation is not evidence of
            // retirement; leave it for the ordinary authenticated projection
            // check, which fails closed rather than acknowledging unknown data.
            if lineage.table_generation >= current_generation {
                continue;
            }
            let purged_lives = self.purged_lineage_records_at_generation(
                &row.table,
                &row.natural_key,
                lineage.table_generation,
            )?;
            let creator_incarnation = lineage.author_database_incarnation.to_hex();
            let purged_lineage = purged_lives.iter().any(|record| {
                record.delete_obligation == DurableDeleteObligation::Purged
                    && record.lineage_root == lineage.lineage_root
                    && record.author_node_id.as_deref() == Some(lineage.author_node_id.as_str())
                    && record.author_database_incarnation.as_deref()
                        == Some(creator_incarnation.as_str())
                    && record.author_local_mutation_position
                        == lineage.author_local_mutation_position.0
            });
            conflicts.push(Conflict {
                natural_key: row.natural_key.clone(),
                resolution: Self::sync_conflict_policy_for_table(
                    &policies,
                    self.table_meta(&row.table).as_ref(),
                    &row.table,
                ),
                reason: Some(
                    if purged_lineage {
                        crate::sync_types::PURGED_LINEAGE_CONFLICT_REASON
                    } else {
                        crate::sync_types::REMOVED_GENERATION_CONFLICT_REASON
                    }
                    .to_string(),
                ),
                table: Some(row.table.clone()),
                mutation_kind: Some(if row.deleted { "delete" } else { "edit" }.to_string()),
                winning_author_node_id: None,
                hub_acceptance_position: None,
                refusal_cause: None,
            });
        }
        Ok(conflicts)
    }

    /// Commit the authenticated edge receipt for a push whose every row was
    /// terminally refused before the normal row-apply path.  The receipt is
    /// the durable acknowledgement boundary: a dropped response can then be
    /// reconciled by the edge without retransmitting the same retired rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn commit_terminal_sync_refusals_with_receipt(
        &self,
        receipt: SyncApplyReceipt,
        row_count: usize,
        conflicts: Vec<Conflict>,
        outcome_key: &SyncPushOutcomeKey,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ApplyResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let push_lock = self.sync_push_apply_lock_for_key(outcome_key);
        let _push_apply = push_lock.lock();
        if let Some(result) = self.replay_sync_push_outcome(outcome_key)? {
            return Ok(result);
        }
        if row_count == 0 || conflicts.len() != row_count {
            return Err(Error::SyncError(
                ("terminal sync refusal receipt must cover every transmitted row".to_string())
                    .into(),
            ));
        }
        let result = ApplyResult {
            applied_rows: 0,
            skipped_rows: row_count,
            conflicts,
            new_lsn: self.current_lsn(),
        };
        let context = SyncPushOutcomeContext {
            key: outcome_key.clone(),
            committed_lsn: None,
            refused_rows: 0,
            conflicts: Vec::new(),
        };
        self.commit_sync_apply_outcome_only(
            &receipt,
            Some(&context),
            &result,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        if self.persistence.is_none() {
            let edge_key = Self::applied_push_watermark_node_incarnation_key(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
            );
            let mut watermarks = self.in_memory_applied_push_watermarks.lock();
            for key in [edge_key, receipt.tenant_id.as_str().to_string()] {
                watermarks
                    .entry(key)
                    .and_modify(|current| *current = (*current).max(receipt.source_lsn))
                    .or_insert(receipt.source_lsn);
            }
        }
        self.replay_sync_push_outcome(outcome_key)?.ok_or_else(|| {
            Error::SyncError(("terminal push omitted its durable apply outcome".to_string()).into())
        })
    }

    /// Capabilities needed to replay this table's authored schema history.
    /// Current metadata alone cannot release an immutable CREATE/ALTER vector.
    #[cfg(feature = "sync-orchestration")]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authored_schema_capabilities(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<BTreeSet<crate::sync_types::SchemaSyncCapability>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            return RedbPersistence::authored_capabilities_in_read(
                &root,
                &crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                table,
            );
        }
        let mut capabilities = BTreeSet::new();
        for (_, ddl) in self
            .checked_ddl_entries_since(Lsn(0), read_memory.clone())?
            .iter()
        {
            if let DdlChange::CreateTable {
                name,
                columns,
                constraints,
                ..
            }
            | DdlChange::AlterTable {
                name,
                columns,
                constraints,
                ..
            } = ddl
                && name == table
            {
                let columns = columns
                    .iter()
                    .map(|(name, text)| rough_sync_column_def(name, text))
                    .collect::<Vec<_>>();
                capabilities.extend(crate::sync_types::schema_capabilities_in_columns(&columns));
                // A table whose authored history ever carried a MAINTAIN
                // COUNT declaration keeps the capability even after a later
                // ALTER cleared it: the peer must be able to replay the whole
                // authored vector before this table leaves holdback.
                if constraints.iter().any(|constraint| {
                    constraint
                        .trim_start()
                        .to_ascii_uppercase()
                        .starts_with("MAINTAIN COUNT")
                }) {
                    capabilities
                        .insert(crate::sync_types::SchemaSyncCapability::MaintainedIndexCounts);
                }
            }
        }
        Ok(capabilities)
    }

    pub(super) fn changeset_is_wholly_empty(changes: &ChangeSet) -> bool {
        changes.rows.is_empty()
            && changes.vectors.is_empty()
            && changes.edges.is_empty()
            && changes.ddl.is_empty()
            && changes.ddl_lsn.is_empty()
    }

    /// Ordinary pulls suppress an exact replay only after the accepted delete
    /// still owns an absent full natural key. Dependency-complete units remain
    /// atomic: a matching terminated member rejects the whole unit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn drop_accepted_lineage_replays_against_generation_projection(
        &self,
        tenant_id: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        table_generations: &HashMap<String, u64>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AcceptedDeleteSuppression> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages_against_generation_projection(
            tenant_id,
            &changes,
            lineages,
            table_generations,
            read_memory.clone(),
        )?;
        self.suppress_accepted_lineage_replays_with_generation_projection(
            changes,
            lineages,
            false,
            table_generations,
            read_memory.clone(),
        )
    }

    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn reject_dependency_complete_accepted_lineage_replays(
        &self,
        tenant_id: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<AcceptedDeleteSuppression> {
        self.validate_received_row_lineages(tenant_id, &changes, lineages, read_memory.clone())?;
        self.suppress_accepted_lineage_replays(changes, lineages, true, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn reject_dependency_complete_accepted_lineage_replays_against_generation_projection(
        &self,
        tenant_id: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        table_generations: &HashMap<String, u64>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AcceptedDeleteSuppression> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages_against_generation_projection(
            tenant_id,
            &changes,
            lineages,
            table_generations,
            read_memory.clone(),
        )?;
        self.suppress_accepted_lineage_replays_with_generation_projection(
            changes,
            lineages,
            true,
            table_generations,
            read_memory.clone(),
        )
    }

    /// Push has no ordinary-pull cursor to consume a suppressed replay. Keep
    /// its accepted-delete check strict and return the original changes only
    /// after every live member has cleared the dependency-style classifier.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reject_accepted_lineage_replays(
        &self,
        tenant_id: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages(tenant_id, &changes, lineages, read_memory.clone())?;
        Ok(self
            .suppress_accepted_lineage_replays(changes, lineages, true, read_memory.clone())?
            .changes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reject_accepted_lineage_replays_with_received_ddl(
        &self,
        tenant_id: &TenantId,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        received_ddl: &crate::protocol::ReceivedDdlContext,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.validate_received_row_lineages_with_received_ddl(
            tenant_id,
            &changes,
            lineages,
            received_ddl,
            read_memory.clone(),
        )?;
        // The rows belong to the generations the received schema installs,
        // so the accepted-delete check reads lineage at those generations: a
        // table this unit creates has no live generation yet, and a table it
        // recreates must not be judged by its retired one.
        Ok(self
            .suppress_accepted_lineage_replays_with_generation_projection(
                changes,
                lineages,
                true,
                &Self::received_ddl_generation_projection(received_ddl),
                read_memory.clone(),
            )?
            .changes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn suppress_accepted_lineage_replays(
        &self,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        dependency_complete: bool,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AcceptedDeleteSuppression> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.suppress_accepted_lineage_replays_with_generation_projection(
            changes,
            lineages,
            dependency_complete,
            &HashMap::new(),
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn suppress_accepted_lineage_replays_with_generation_projection(
        &self,
        changes: ChangeSet,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        dependency_complete: bool,
        table_generations: &HashMap<String, u64>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AcceptedDeleteSuppression> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // A preceding unit can publish while the reply retains its admission
        // root. Classify accepted lineage at one current committed frontier,
        // pairing its logical snapshot with the durable source it describes.
        let published = self.tx_mgr.capture_published_read();
        let snapshot = published.snapshot;
        let _writer_read = self.enter_writer_read(
            writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        let (_catalog_charge, projected_meta) = self.projected_sync_table_meta(&changes)?;
        let skip_deleted = HashSet::new();
        let mut retained_rows = Vec::with_capacity(changes.rows.len());
        let mut suppressed_vector_indexes = HashSet::new();
        let mut vector_cursor = 0usize;
        let mut suppressed_live_replay = false;
        for row in &changes.rows {
            let row_has_vector = projected_meta.get(&row.table).is_some_and(|meta| {
                meta.columns
                    .iter()
                    .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
            });
            let vector_group = (row_has_vector
                && next_vector_row_group_matches(&changes.vectors, vector_cursor, row))
            .then(|| vector_row_group_end(&changes.vectors, vector_cursor));
            if row.deleted {
                retained_rows.push(row.clone());
                if let Some(end) = vector_group {
                    vector_cursor = end;
                }
                continue;
            }
            let incoming_generation = lineages
                .iter()
                .find(|(table, key, lsn, _)| {
                    *table == row.table && *key == row.natural_key && *lsn == row.lsn
                })
                .map(|entry| entry.3.table_generation);
            let record = if let Some(generation) = incoming_generation
                .as_ref()
                .or_else(|| table_generations.get(&row.table))
            {
                self.durable_lineage_record_at_generation(
                    &row.table,
                    &row.natural_key,
                    *generation,
                )?
            } else {
                self.durable_lineage_record(&row.table, &row.natural_key, read_memory.clone())?
            };
            let Some(record) = record else {
                retained_rows.push(row.clone());
                if let Some(end) = vector_group {
                    vector_cursor = end;
                }
                continue;
            };
            if record.delete_obligation != DurableDeleteObligation::Accepted {
                retained_rows.push(row.clone());
                if let Some(end) = vector_group {
                    vector_cursor = end;
                }
                continue;
            }
            let incoming = lineages.iter().find(|(table, natural_key, lsn, _)| {
                table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
            });
            // An absent sidecar fails closed: it cannot prove that it is a
            // new lineage entitled to resurrect this key.
            let same_terminated_lineage = incoming
                .is_none_or(|(_, _, _, lineage)| terminated_creator_matches(&record, lineage));
            if same_terminated_lineage {
                if dependency_complete {
                    // The store already agrees: the pushing edge's own copy
                    // is the SAME terminated lineage the hub already
                    // committed as deleted, no fresh same-key row exists.
                    // This is benign convergence, not a definitive failure
                    // -- see `Error::SyncReplayOfAcceptedDelete`'s doc
                    // comment for the caller-facing contract this feeds
                    // (`WirePushError::ReplaysAcceptedDelete` on the wire).
                    return Err(Error::SyncReplayOfAcceptedDelete {
                        table: row.table.clone(),
                        key: row.natural_key.pairs(),
                    });
                }
                let visible = if self.table_meta(&row.table).is_some() {
                    self.visible_row_by_natural_key(
                        &row.table,
                        &row.natural_key,
                        snapshot,
                        &skip_deleted,
                        read_memory.clone(),
                    )?
                } else {
                    None
                };
                if visible.is_some() {
                    return Err(Error::SyncError((format!(
                        "received row {} {:?} replays a lineage terminated by an accepted delete but a fresh same-key row is visible",
                        row.table, row.natural_key
                    )).into()));
                }
                suppressed_live_replay = true;
                if let Some(end) = vector_group {
                    suppressed_vector_indexes.extend(vector_cursor..end);
                    vector_cursor = end;
                }
                continue;
            }
            retained_rows.push(row.clone());
            if let Some(end) = vector_group {
                vector_cursor = end;
            }
        }
        let vectors = changes
            .vectors
            .into_iter()
            .enumerate()
            .filter_map(|(index, vector)| {
                (!suppressed_vector_indexes.contains(&index)).then_some(vector)
            })
            .collect();
        Ok(AcceptedDeleteSuppression {
            changes: ChangeSet {
                rows: retained_rows,
                vectors,
                edges: changes.edges,
                ddl: changes.ddl,
                ddl_lsn: changes.ddl_lsn,
            },
            suppressed_live_replay,
        })
    }

    /// Put a local SQL delete into the same storage transaction as the row
    /// removal. Redb retains the record across close/open; an in-memory
    /// database retains it for its shared handle life. Without it the vanished
    /// row cannot be offered to the hub again.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stage_local_delete_obligation(
        &self,
        tx: TxId,
        table: &str,
        row: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        if !matches!(
            crate::executor::effective_sync_direction(&meta),
            SyncDirection::Push | SyncDirection::Both
        ) {
            // Direction governs ordinary rows, including whether an edge owes
            // a local delete to its hub. Pull-only and OFF tables must remain
            // eligible for their ordinary authoritative pull.
            return Ok(());
        }
        let Some(natural_key) = natural_key_from_row_values(&meta, &row.values) else {
            return Ok(());
        };
        let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
        let staged_evidence =
            self.staged_unbound_creation_lineage(tx, table, &natural_key, generation, row.row_id)?;
        let lineage_state = self.lineage_state_lock.lock();
        let inherited = self
            .load_row_lineage_sidecar(
                &lineage_state,
                &Self::durable_row_lineage_config_key(table, &natural_key, generation),
            )?
            .filter(|lineage| {
                lineage.table_generation == generation && lineage.local_row_id == Some(row.row_id)
            });
        let evidence = if inherited.is_none() {
            let creation_key = Self::durable_unbound_creation_config_key(
                table,
                &natural_key,
                generation,
                row.row_id,
            );
            Some(
                staged_evidence
                    .or(self.load_unbound_creation_lineage(&lineage_state, &creation_key)?)
                    .filter(|evidence| {
                        evidence.table_generation == generation
                            && evidence.local_row_id == row.row_id
                            && evidence.natural_key == natural_key
                    })
                    .ok_or_else(|| {
                        Error::SyncError(
                            (format!(
                                "DELETE from {} {:?} lacks creation evidence",
                                table, natural_key
                            ))
                            .into(),
                        )
                    })?,
            )
        } else {
            None
        };
        let author_node_id = inherited
            .as_ref()
            .map(|lineage| lineage.author_node_id.clone());
        let author_database_incarnation = inherited
            .as_ref()
            .map(|lineage| lineage.author_database_incarnation.to_hex());
        let author_local_mutation_position = inherited
            .as_ref()
            .map(|lineage| lineage.author_local_mutation_position.0)
            .or_else(|| {
                evidence
                    .as_ref()
                    .and_then(|evidence| evidence.creation_lsn.map(|lsn| lsn.0))
            })
            .or_else(|| evidence.as_ref().map(|_| 0))
            .expect("inherited lineage or unbound evidence exists");
        let lineage_root = inherited
            .as_ref()
            .map(|lineage| lineage.lineage_root.clone())
            .unwrap_or_else(|| {
                format!(
                    "unbound:{}:{}",
                    Self::hex_component(&sync_identity_key(table, &natural_key)),
                    evidence
                        .as_ref()
                        .and_then(|evidence| evidence.creation_lsn)
                        .map(|lsn| lsn.0)
                        .unwrap_or(0)
                )
            });
        let delete_lsn = 0;
        let record = DurableLineageRecord {
            table: table.to_string(),
            natural_key: natural_key.clone(),
            table_generation: generation,
            local_row_id: Some(row.row_id),
            locally_created: inherited
                .as_ref()
                .map(|lineage| lineage.locally_created)
                .unwrap_or(true),
            lineage_root,
            lineage_attestation: inherited
                .as_ref()
                .map(|lineage| lineage.lineage_attestation.clone())
                .unwrap_or_default(),
            author_node_id,
            author_database_incarnation,
            author_local_mutation_position,
            delete_lsn,
            delete_obligation: DurableDeleteObligation::Pending,
            accepted_hub_lsn: None,
            bound_hub_node_id: None,
            purge_frontier: None,
        };
        let key = Self::durable_lineage_config_key(table, &natural_key, generation);
        let generation_key = Self::durable_lineage_table_generation_key(table);
        let record = RedbPersistence::encode_config_value(&record)?;
        let generation = RedbPersistence::encode_config_value(&generation)?;
        // Commit-time purge fencing reads the write set before lineage state.
        // Release lineage state before staging this obligation so concurrent
        // delete staging follows that same lock order instead of inverting it.
        drop(lineage_state);
        self.tx_mgr
            .append_config_writes(tx, [(generation_key, generation), (key, record)])
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn stamp_pending_delete_author_tuple(
        &self,
        rows: &[RowChange],
        _author_node_id: &str,
        _author_database_incarnation: Incarnation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let lineage_state = self.lineage_state_lock.lock();
        for row in rows.iter().filter(|row| row.deleted) {
            let key = Self::durable_lineage_config_key(
                &row.table,
                &row.natural_key,
                self.durable_lineage_table_generation(&row.table, read_memory.clone())?,
            );
            let Some(record) = self.load_lineage_record(&lineage_state, &key)? else {
                continue;
            };
            if record.delete_obligation != DurableDeleteObligation::Pending
                || record.delete_lsn != row.lsn.0
            {
                continue;
            }
            if record.author_node_id.is_none() {
                return Err(Error::SyncError((
                    "pending delete lacks its creation lineage; refusing outbound provenance repair"
                        .to_string()).into()));
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_pending_deletes_since(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurablePendingDelete>> {
        self.durable_pending_deletes_since_selected(since_lsn, false, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_pending_deletes_since_selected(
        &self,
        since_lsn: Lsn,
        outbound: bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurablePendingDelete>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let lineage_state = self.lineage_state_lock.lock();
        let records = self.list_lineage_records(&lineage_state)?;
        let mut changes = Vec::new();
        for (_, record) in records {
            if record.table_generation
                != self.durable_lineage_table_generation(&record.table, read_memory.clone())?
                || record.delete_obligation != DurableDeleteObligation::Pending
                || record.delete_lsn <= since_lsn.0
            {
                continue;
            }
            if outbound
                && !self.outbound_direction_includes(
                    &record.table,
                    Lsn(record.delete_lsn),
                    read_memory.clone(),
                )?
            {
                continue;
            }
            changes.push(DurablePendingDelete {
                row: RowChange {
                    table: record.table,
                    natural_key: record.natural_key,
                    values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                    deleted: true,
                    lsn: Lsn(record.delete_lsn),
                    created_at: None,
                },
                local_row_id: record.local_row_id,
            });
        }
        Ok(changes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn has_durable_pending_delete_obligations(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let lineage_state = self.lineage_state_lock.lock();
        Ok(self
            .list_lineage_records(&lineage_state)?
            .into_iter()
            .map(|(_, record)| {
                Ok(record.delete_obligation == DurableDeleteObligation::Pending
                    && record.table_generation
                        == self
                            .durable_lineage_table_generation(&record.table, read_memory.clone())?)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|pending| pending))
    }

    pub(super) fn reconcile_durable_pending_deletes(
        changes: ChangeSet,
        pending: Vec<DurablePendingDelete>,
    ) -> ChangeSet {
        Self::reconcile_durable_pending_deletes_projected(
            changes,
            pending,
            &ChangeProjection::unbounded(),
        )
        .expect("an unbounded reconciliation never refuses")
    }

    /// The pending deletions reconciled into a change set under a history
    /// answer's bounds: the checkpoint runs per deletion, and a deletion's
    /// place in the answer is admitted before it is added -- whatever older
    /// rows the same deletion removed first.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reconcile_durable_pending_deletes_projected(
        mut changes: ChangeSet,
        pending: Vec<DurablePendingDelete>,
        projection: &ChangeProjection<'_>,
    ) -> Result<ChangeSet> {
        for pending in pending {
            (projection.checkpoint)()?;
            let row = pending.row;
            let identity = sync_identity_key(&row.table, &row.natural_key);
            // A live row at the SAME commit position as the obligation is that
            // commit's replacement of this key: one transaction deleted the old
            // row and re-inserted the key with fresh content. It is not stale
            // history to prune ahead of the deletion — it IS what the commit
            // left behind. Pruning it published the deletion alone, so every
            // receiver erased a key its author had just rewritten.
            changes.rows.retain(|existing| {
                existing.deleted
                    || sync_identity_key(&existing.table, &existing.natural_key) != identity
                    || existing.lsn >= row.lsn
            });
            if let Some(local_row_id) = pending.local_row_id {
                changes.vectors.retain(|vector| {
                    vector.index.table != row.table
                        || vector.row_id != local_row_id
                        || vector.vector.is_empty()
                        || vector.lsn > row.lsn
                });
            }
            // The obligation is discharged by that same-commit replacement:
            // the key is not gone, it holds new content. Re-adding the delete
            // beside it would hand a receiver a deletion and an insertion of
            // one key at one position, and whichever it applied last would
            // decide whether the row survived.
            let replaced_in_the_same_commit = changes.rows.iter().any(|existing| {
                !existing.deleted
                    && existing.table == row.table
                    && existing.natural_key == row.natural_key
                    && existing.lsn == row.lsn
            });
            let already_carries_the_delete = changes.rows.iter().any(|existing| {
                existing.deleted
                    && existing.table == row.table
                    && existing.natural_key == row.natural_key
                    && existing.lsn == row.lsn
            });
            if !replaced_in_the_same_commit && !already_carries_the_delete {
                if projection.bounded {
                    (projection.admit)(answer_list_entry_bytes::<RowChange>(0))?;
                }
                changes.rows.push(row);
            }
        }
        Ok(changes)
    }

    /// The pending deletions a bounded history answer carries, read from
    /// where this handle's durable lineage lives: a committed image's own
    /// committed configuration, or the live store. The checkpoint runs per
    /// record, each record's decoding and each deletion are admitted before
    /// they are made, and a record that cannot be read refuses the answer
    /// instead of leaving that deletion out.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_pending_deletes_projected(
        &self,
        since_lsn: Lsn,
        projection: &ChangeProjection<'_>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurablePendingDelete>> {
        self.durable_pending_deletes_selected(since_lsn, projection, false, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_pending_deletes_selected(
        &self,
        since_lsn: Lsn,
        projection: &ChangeProjection<'_>,
        outbound: bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DurablePendingDelete>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            return self.durable_pending_deletes_at_root(
                &root,
                since_lsn,
                projection,
                outbound,
                read_memory,
            );
        }
        let Some(startup) = self.committed_image_startup.as_ref() else {
            let pending = self.durable_pending_deletes_since_selected(
                since_lsn,
                outbound,
                read_memory.clone(),
            )?;
            if projection.bounded {
                (projection.admit)(
                    pending
                        .iter()
                        .map(|pending| {
                            answer_list_entry_bytes::<DurablePendingDelete>(
                                pending.row.held_bytes(),
                            )
                        })
                        .sum(),
                )?;
            }
            return Ok(pending);
        };
        let prefix = Self::DURABLE_LINEAGE_CONFIG_PREFIX;
        let mut pending = Vec::new();
        // Lineage grows with deleted history, so the image reads it here, on
        // request, rather than holding every record from open.
        startup.config_source.visit_image_lineage_config(
            prefix,
            projection,
            &mut |key, bytes| {
                // The decoded record is source scratch, not retained answer
                // output. Its credit must end with this callback, even when
                // the record is terminal, below the frontier, or excluded.
                let decode_memory = crate::read_image_memory::ImageMemory::charging_reader_for(
                    (*self.finalization.accountant).clone(),
                    read_memory.clone(),
                    "reader",
                    "pending_delete_window",
                    "Raise the declared memory limit to decode one lineage record.",
                );
                let mut decode_credit =
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                if projection.bounded {
                    decode_credit.try_grow_for(
                        std::mem::size_of::<DurableLineageRecord>()
                            .saturating_add(bytes.len().saturating_mul(3)),
                        "reader",
                        "pending_delete_window",
                        "Raise the declared memory limit to decode one lineage record.",
                    )?;
                }
                #[cfg(any(test, feature = "test-seams"))]
                let bytes = if PENDING_DELETION_READ_FAULT.with(|fault| fault.replace(false)) {
                    &[0xff_u8, 0xfe, 0xfd, 0xfc][..]
                } else {
                    bytes
                };
                let record = decode_memory
                    .scope(|| RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes))
                    .map_err(|error| {
                        crate::funded_error::corruption_display(
                            Ok(read_memory.clone()),
                            self.committed_image_diagnostic_path().display(),
                            format_args!("durable lineage record {key} cannot be read: {error}"),
                        )
                    })?;
                let generation_key = Self::durable_lineage_table_generation_key(&record.table);
                let generation = startup
                    .config_values
                    .get(&generation_key)
                    .map(|bytes| RedbPersistence::decode_config_value::<u64>(bytes))
                    .transpose()
                    .map_err(|error| {
                        crate::funded_error::corruption_display(
                            Ok(read_memory.clone()),
                            self.committed_image_diagnostic_path().display(),
                            format_args!(
                                "table generation {generation_key} cannot be read: {error}"
                            ),
                        )
                    })?
                    .filter(|generation| *generation != 0)
                    .ok_or_else(|| {
                        crate::funded_error::corruption_display(
                            Ok(read_memory.clone()),
                            self.committed_image_diagnostic_path().display(),
                            format_args!(
                                "table {} has lineage but no authoritative generation",
                                record.table
                            ),
                        )
                    })?;
                if record.table_generation != generation
                    || record.delete_obligation != DurableDeleteObligation::Pending
                    || record.delete_lsn <= since_lsn.0
                {
                    return Ok(());
                }
                if outbound
                    && !self.outbound_direction_includes(
                        &record.table,
                        Lsn(record.delete_lsn),
                        read_memory.clone(),
                    )?
                {
                    return Ok(());
                }
                let row = RowChange {
                    table: record.table,
                    natural_key: record.natural_key,
                    values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                    deleted: true,
                    lsn: Lsn(record.delete_lsn),
                    created_at: None,
                };
                if projection.bounded {
                    (projection.admit)(answer_list_entry_bytes::<DurablePendingDelete>(
                        row.held_bytes(),
                    ))?;
                }
                pending.push(DurablePendingDelete {
                    row,
                    local_row_id: record.local_row_id,
                });
                Ok(())
            },
        )?;
        Ok(pending)
    }

    /// Walk the authoritative lineage family at the operation's existing
    /// root. A discarded record owns just its current cursor window; only a
    /// selected delete acquires answer credit and survives that window.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_pending_deletes_at_root(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        since_lsn: Lsn,
        projection: &ChangeProjection<'_>,
        outbound: bool,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Vec<DurablePendingDelete>> {
        let prefix = Self::DURABLE_LINEAGE_CONFIG_PREFIX;
        let mut cursor = root.lineage_cursor_from(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            prefix,
            prefix,
        )?;
        let mut pending = Vec::new();
        while let Some(window) = cursor
            .next_before(&mut || (projection.checkpoint)(), |_, bytes| {
                RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes)
            })?
        {
            let record = window.value;
            // Generation validation precedes every eligibility exclusion, as
            // it did on the collected route, and reuses this captured root.
            if record.table_generation
                != self.durable_lineage_table_generation(&record.table, read_memory.clone())?
                || record.delete_obligation != DurableDeleteObligation::Pending
                || record.delete_lsn <= since_lsn.0
            {
                continue;
            }
            if outbound
                && !self.outbound_direction_includes(
                    &record.table,
                    Lsn(record.delete_lsn),
                    read_memory.clone(),
                )?
            {
                continue;
            }
            if projection.bounded {
                let row_bytes = std::mem::size_of::<RowChange>()
                    .saturating_add(held::text(&record.table))
                    .saturating_add(record.natural_key.held_bytes())
                    .saturating_add(held::map::<String, Value>(1))
                    .saturating_add(held::text("__deleted"));
                (projection.admit)(answer_list_entry_bytes::<DurablePendingDelete>(row_bytes))?;
            }
            pending.push(DurablePendingDelete {
                row: RowChange {
                    table: record.table,
                    natural_key: record.natural_key,
                    values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                    deleted: true,
                    lsn: Lsn(record.delete_lsn),
                    created_at: None,
                },
                local_row_id: record.local_row_id,
            });
        }
        Ok(pending)
    }

    /// The file a committed image was read from, for its damage reports.
    pub(super) fn committed_image_diagnostic_path(&self) -> &std::path::Path {
        self.retained_relational_source
            .as_ref()
            .map(|source| source.database_path())
            .or_else(|| {
                self.image_history
                    .as_ref()
                    .map(|history| history.source.database_path())
            })
            .unwrap_or_else(|| std::path::Path::new(""))
    }

    sync_test_seam! {
    /// Add accepted delete ancestry that an explicit destination move selected.
    /// These records share the ordinary lineage lifecycle; this view makes
    /// already-held absence transportable to the new hub. Redb carries that
    /// state across restart, while memory carries it only for this database
    /// life.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from in_memory_lineage_tests.rs"))]
    fn append_destination_reupload_deletes(
        &self,
        changes: ChangeSet,
        held_through_lsn: Lsn,
    ) -> Result<ChangeSet> {
        let read_memory = self.declared_reader_memory()?;
        self.append_destination_reupload_deletes_with_read_memory(
            changes,
            held_through_lsn,
            read_memory,
        )
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn append_destination_reupload_deletes_with_read_memory(
        &self,
        mut changes: ChangeSet,
        held_through_lsn: Lsn,
    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        #[cfg(feature = "test-seams")]
        crate::outbound_ddl_test_seams::note_event(
            crate::outbound_ddl_test_seams::OutboundDdlEvent::DeleteFrontier(held_through_lsn),
        );

        let lineage_state = self.lineage_state_lock.lock();
        let records = self.list_lineage_records(&lineage_state)?;
        for (_, record) in records {
            if record.delete_obligation != DurableDeleteObligation::Accepted
                || record.delete_lsn == 0
                || record.delete_lsn > held_through_lsn.0
                || record.table_generation
                    != self.durable_lineage_table_generation(&record.table, read_memory.clone())?
            {
                continue;
            }
            let row = RowChange {
                table: record.table,
                natural_key: record.natural_key,
                values: HashMap::from([("__deleted".to_string(), Value::Bool(true))]),
                deleted: true,
                lsn: Lsn(record.delete_lsn),
                created_at: None,
            };
            if !changes.rows.iter().any(|existing| {
                existing.deleted
                    && existing.table == row.table
                    && existing.natural_key == row.natural_key
                    && existing.lsn == row.lsn
            }) {
                changes.rows.push(row);
            }
        }
        Ok(changes)
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
    pub(super) fn record_durable_delete_acceptance(
        &self,
        rows: &[RowChange],
        hub_lsn: Lsn,
        hub_node_id: Option<&str>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let operation = || {
            let mut lineage_state = self.lineage_state_lock.lock();
            for row in rows.iter().filter(|row| row.deleted) {
                let key = Self::durable_lineage_config_key(
                    &row.table,
                    &row.natural_key,
                    self.durable_lineage_table_generation(&row.table, read_memory.clone())?,
                );
                let Some(mut record) = self.load_lineage_record(&lineage_state, &key)? else {
                    continue;
                };
                if record.delete_lsn != row.lsn.0
                    || record.delete_obligation != DurableDeleteObligation::Pending
                {
                    continue;
                }
                record.delete_obligation = DurableDeleteObligation::Accepted;
                record.accepted_hub_lsn = Some(hub_lsn.0);
                record.bound_hub_node_id = hub_node_id.map(ToOwned::to_owned);
                self.store_lineage_record(&mut lineage_state, &key, &record)?;
            }
            Ok(())
        };
        if self.tx_mgr.commit_boundary_held_by_current_thread() {
            operation()
        } else {
            self.tx_mgr.with_maintenance_publication(|_| operation())
        }
    }

    pub(super) fn clear_sync_tombstone(&self, table: &str, natural_key: &NaturalKey) {
        self.sync_tombstone_arrivals
            .write()
            .remove(&Self::sync_tombstone_key(table, natural_key));
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_for_delete_change(
        &self,
        row: &RowChange,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.find_history_change(
            row.lsn,
            row.lsn,
            |entry| {
                let ChangeLogEntry::RowDelete {
                    table,
                    row_id,
                    natural_key,
                    lsn,
                } = entry
                else {
                    return None;
                };
                (table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn)
                    .then_some(*row_id)
            },
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn find_history_change<T>(
        &self,
        first: Lsn,
        last: Lsn,
        mut select: impl FnMut(&ChangeLogEntry) -> Option<T>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<T>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            let mut cursor = root.change_log_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                first,
                last.min(root.lsn),
            )?;
            while let Some(window) = cursor.next_change()? {
                if let Some(value) = select(&window.value) {
                    return Ok(Some(value));
                }
            }
            return Ok(None);
        }
        Ok(self
            .change_log
            .read()
            .iter()
            .rev()
            .filter(|entry| entry.lsn() >= first && entry.lsn() <= last)
            .find_map(select))
    }

    /// A hub restored before an acknowledged local delete can replay its old
    /// live row. While this database is still open, the accepted-local delete
    /// records the hub order that made it obsolete; do not resurrect that row
    /// from an older ordered arrival. This intentionally does not reconstruct
    /// tombstones on restart; this in-memory retention fence lasts only for
    /// the lifetime of the open database.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn incoming_row_is_stale_against_sync_tombstone(
        &self,
        row: &RowChange,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
    ) -> Result<bool> {
        // A cursor that just adopted a different source compares no prior
        // source ordering. Its authoritative from-zero history must be able
        // to restore a row even when this session still remembers a delete
        // accepted by the old source.
        if adoption != SyncAdoption::Continuing {
            return Ok(false);
        }
        let Some(incoming_arrival) = Self::resolve_incoming_arrival(row, arrivals) else {
            return Ok(false);
        };
        // Durable accepted deletes are intentionally NOT compared to a hub
        // arrival clock here.  Rebind/source-adoption safety is decided by
        // `drop_accepted_lineage_replays_against_generation_projection` before
        // apply, using the immutable root/creator tuple; clocks from a replaced
        // hub are incomparable and must never suppress a later genuine same-key
        // lineage.
        Ok(self
            .sync_tombstone_arrivals
            .read()
            .get(&Self::sync_tombstone_key(&row.table, &row.natural_key))
            .is_some_and(|marker| match marker.kind {
                // The hub may refuse a keep-first/server-wins delete and
                // re-emit its retained row in THAT SAME commit. That equal
                // arrival is authoritative repair, not a restored-hub replay.
                contextdb_relational::store::SyncSourceKind::AcceptedLocal => {
                    marker.accepted_local_order_invalidated || incoming_arrival < marker.arrival
                }
                contextdb_relational::store::SyncSourceKind::AcceptedLocalPending => true,
                // A pulled tombstone already describes the hub's own
                // authoritative delete; its equal-arrival echo remains stale.
                contextdb_relational::store::SyncSourceKind::Pulled => {
                    incoming_arrival <= marker.arrival
                }
            }))
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Persist the accepting hub's order after a push acknowledgement. The
    /// acknowledgement's existing `ApplyResult::new_lsn` is the hub commit
    /// position, so no wire field is needed. The commit lock makes the
    /// current-row comparison and the provenance write one ordering boundary:
    /// a later local write has a different row LSN and is never marked as if
    /// the hub had accepted it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn record_hub_accepted_rows(
        &self,
        rows: &[RowChange],
        hub_lsn: Lsn,
        hub_node_id: Option<&str>,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.record_hub_accepted_rows_with_read_memory(rows, hub_lsn, hub_node_id, read_memory)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_hub_accepted_rows_with_read_memory(
        &self,
        rows: &[RowChange],
        hub_lsn: Lsn,
        hub_node_id: Option<&str>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        self.tx_mgr.with_maintenance_publication(|publication| {
            let snapshot = self.snapshot();
            let empty_skip_deleted = HashSet::new();
            let mut source_entries = Vec::new();
            let mut tombstones = Vec::new();
            for row in rows {
                if row.deleted {
                    let row_id = self.row_id_for_delete_change(row, read_memory.clone())?;
                    let current = self
                        .visible_row_by_natural_key(
                            &row.table,
                            &row.natural_key,
                            snapshot,
                            &empty_skip_deleted,
                         read_memory.clone())?
                        .is_none();
                    if current {
                        tombstones.push((
                            Self::sync_tombstone_key(&row.table, &row.natural_key),
                            SyncTombstoneArrival {
                                arrival: hub_lsn,
                                delete_lsn: row.lsn,
                                row_id,
                                kind: contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                                accepted_local_order_invalidated: false,
                            },
                        ));
                    }
                    continue;
                }
                let Some(local) = self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    &empty_skip_deleted,
                 read_memory.clone())?
                else {
                    continue;
                };
                if local.lsn == row.lsn {
                    // Re-delivery after a hub restore has the same local
                    // source LSN but a NEW hub ordering position. Refresh an
                    // existing AcceptedLocal marker as well as creating one.
                    source_entries.push((row.table.clone(), local.row_id, hub_lsn));
                }
            }
            // A later same-key local INSERT must not hide the fact that this
            // exact earlier DELETE was acknowledged. Its row/tombstone sidecar
            // stays untouched, but the durable delete lifecycle still moves
            // Pending -> Accepted before the caller advances its watermark.
            publication.reserve()?;
            self.record_durable_delete_acceptance(rows, hub_lsn, hub_node_id, read_memory.clone())?;
            if source_entries.is_empty() && tombstones.is_empty() {
                publication.complete();
                return Ok(());
            }
            if let Some(persistence) = &self.persistence {
                persistence.append_sync_source_provenance_batch(
                    &source_entries
                        .iter()
                        .map(|(table, row_id, lsn)| (table.clone(), *row_id, *lsn, 1))
                        .collect::<Vec<_>>(),
                )?;
            }
            if !source_entries.is_empty() {
                self.relational_store
                    .set_sync_source_lsns(source_entries.clone());
                self.relational_store
                    .set_sync_source_kinds(source_entries.into_iter().map(|(table, row_id, _)| {
                        (
                            table,
                            row_id,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                        )
                    }));
            }
            if !tombstones.is_empty() {
                self.sync_tombstone_arrivals.write().extend(tombstones);
            }
            publication.complete();
            Ok(())
        })
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Apply every durable/in-memory semantic effect of one authenticated push
    /// reply only while that replying hub is still authoritative. Destination
    /// change holds the same peer lock over its reset and rebind, so a reply
    /// lands wholly before the move or is refused wholly after it: an old hub
    /// cannot mark rows accepted, retire delete obligations, or leave
    /// reconciliation markers in the new destination's state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn record_hub_push_reply_effects_for_hub(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        ordinary_refused_rows: &[RowChange],
        purge_refused_rows: &[RowChange],
        accepted_rows: &[RowChange],
        hub_lsn: Lsn,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.record_hub_push_reply_effects_for_hub_with_read_memory(
            tenant_id,
            hub_node_id,
            ordinary_refused_rows,
            purge_refused_rows,
            accepted_rows,
            hub_lsn,
            read_memory,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_hub_push_reply_effects_for_hub_with_read_memory(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        ordinary_refused_rows: &[RowChange],
        purge_refused_rows: &[RowChange],
        accepted_rows: &[RowChange],
        hub_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.with_authoritative_hub_reply(hub_node_id, |db| {
            db.record_hub_push_reply_effects_while_authoritative(
                tenant_id,
                hub_node_id,
                ordinary_refused_rows,
                purge_refused_rows,
                accepted_rows,
                hub_lsn,
             read_memory.clone())
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_hub_push_reply_effects_while_authoritative(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        ordinary_refused_rows: &[RowChange],
        purge_refused_rows: &[RowChange],
        accepted_rows: &[RowChange],
        hub_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_hub_push_reply_while_authoritative(
            tenant_id, hub_node_id, ordinary_refused_rows, purge_refused_rows, accepted_rows,
            hub_lsn, None, None,
         read_memory.clone())
    }

    /// One authoritative reply retires only its exact source versions. Its
    /// refusal markers, accepted ordering, delete lifecycle and push progress
    /// become durable together before any memory state is published.
    #[allow(clippy::too_many_arguments)]
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn commit_hub_push_reply_while_authoritative(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        ordinary_refused_rows: &[RowChange],
        purge_refused_rows: &[RowChange],
        accepted_rows: &[RowChange],
        hub_lsn: Lsn,
        confirmed_through: Option<Lsn>,
        pending: Option<(Lsn, &ApplyResult)>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::Other("a hub push reply requires a file-backed database".into())
        })?;

        if ordinary_refused_rows.is_empty()
            && purge_refused_rows.is_empty()
            && accepted_rows.is_empty()
            && pending.is_none()
            && let Some(through) = confirmed_through
        {
            self.publish_direct_metadata(|| {
                #[cfg(feature = "test-seams")]
                crate::outbound_ddl_test_seams::note_pre_reply_high_water(
                    self.accountant.high_water_for_test(),
                );
                persistence.flush_sync_progress_value(
                    &tenant_id.config_key("sync_push_watermark"),
                    &through.0,
                )
            })?;
            return Ok(());
        }
        self.tx_mgr.with_maintenance_publication(|publication| {
            // Reply effects classify current rows at one committed publication,
            // pairing its logical snapshot with the durable root it describes.
            let published = self.tx_mgr.capture_published_read();
            let snapshot = published.snapshot;
            let _writer_read = self.enter_writer_read(writer_read::durable_root(published.handle)?, read_memory.clone());
            let no_deleted = HashSet::new();
            let context = TerminalRefusalPullContext {
                tenant_id: tenant_id.clone(), hub_node_id: hub_node_id.to_string(), generation: 0,
            };
            let context_key = Self::terminal_refusal_context_key(&context);
            let mut scans = self.terminal_refusal_scans.write();
            let lineage = self.lineage_state_lock.lock();
            let prior_scan = scans.get(&context_key).cloned().or(
                self.persistence.as_ref().map(|p| p.load_config_value::<TerminalRefusalScanState>(
                    &Self::terminal_refusal_scan_config_key(&context),
                )).transpose()?.flatten(),
            );
            let generation = prior_scan.map_or(1, |scan| scan.generation.saturating_add(1));
            let mut writes = Vec::new();
            let mut deletes = Vec::new();
            let mut refused_markers = Vec::new();
            let mut accepted_deletes = Vec::new();
            let mut sources = Vec::new();
            let mut tombstones = Vec::new();
            for row in ordinary_refused_rows {
                if !self.table_meta(&row.table).is_some_and(|meta|
                    matches!(crate::executor::effective_sync_direction(&meta), SyncDirection::Pull | SyncDirection::Both)) {
                    continue;
                }
                let current = self.visible_row_by_natural_key(&row.table, &row.natural_key, snapshot, &no_deleted, read_memory.clone())?;
                let row_id = match current {
                    Some(current) if !row.deleted && current.lsn == row.lsn => Some(current.row_id),
                    None if row.deleted => self.row_id_for_delete_change(row, read_memory.clone())?,
                    _ => None,
                };
                if let Some(row_id) = row_id {
                    let record = TerminalRefusalMarkerRecord {
                        table: row.table.clone(), natural_key: row.natural_key.clone(),
                        lsn: row.lsn.0, row_id: row_id.0, active: true, generation,
                    };
                    writes.push((Self::terminal_refusal_marker_config_key(&context, &row.table, &row.natural_key),
                        RedbPersistence::encode_config_value(&record)?));
                    refused_markers.push((Self::terminal_refusal_marker_key(&context, &row.table, &row.natural_key), record));
                }
            }
            let reset_scan = TerminalRefusalScanState { source: None, next_lsn: Lsn(0), active: false, generation };
            if !refused_markers.is_empty() {
                writes.push((Self::terminal_refusal_scan_config_key(&context), RedbPersistence::encode_config_value(&reset_scan)?));
            }
            for row in ordinary_refused_rows.iter().chain(purge_refused_rows).filter(|row| row.deleted) {
                let key = Self::durable_lineage_config_key(&row.table, &row.natural_key,
                    self.durable_lineage_table_generation(&row.table, read_memory.clone())?);
                if self.load_lineage_record(&lineage, &key)?.is_some_and(|record|
                    record.delete_lsn == row.lsn.0 && record.delete_obligation == DurableDeleteObligation::Pending) {
                    deletes.push(key);
                }
            }
            for row in accepted_rows {
                let current = self.visible_row_by_natural_key(&row.table, &row.natural_key, snapshot, &no_deleted, read_memory.clone())?;
                if row.deleted {
                    let key = Self::durable_lineage_config_key(&row.table, &row.natural_key,
                        self.durable_lineage_table_generation(&row.table, read_memory.clone())?);
                    if let Some(mut record) = self.load_lineage_record(&lineage, &key)?
                        && record.delete_lsn == row.lsn.0 && record.delete_obligation == DurableDeleteObligation::Pending {
                        record.delete_obligation = DurableDeleteObligation::Accepted;
                        record.accepted_hub_lsn = Some(hub_lsn.0);
                        record.bound_hub_node_id = Some(hub_node_id.to_string());
                        writes.push((key.clone(), RedbPersistence::encode_config_value(&record)?));
                        accepted_deletes.push((key, record));
                    }
                    if current.is_none() {
                        tombstones.push((Self::sync_tombstone_key(&row.table, &row.natural_key), SyncTombstoneArrival {
                            arrival: hub_lsn, delete_lsn: row.lsn, row_id: self.row_id_for_delete_change(row, read_memory.clone())?,
                            kind: contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                            accepted_local_order_invalidated: false,
                        }));
                    }
                } else if let Some(current) = current && current.lsn == row.lsn {
                    sources.push((row.table.clone(), current.row_id, hub_lsn, 1));
                }
            }
            if let Some(through) = confirmed_through {
                writes.push((tenant_id.config_key("sync_push_watermark"), RedbPersistence::encode_config_value(&through.0)?));
            }
            if let Some((through, result)) = pending {
                writes.push((tenant_id.config_key("sync_pending_push_confirmation"), RedbPersistence::encode_config_value(&through.0)?));
                writes.push((tenant_id.config_key("sync_pending_push_result"), RedbPersistence::encode_config_value(&crate::protocol::WireApplyResult::from(result.clone()))?));
            }
            publication.reserve()?;
            #[cfg(feature = "test-seams")]
            crate::outbound_ddl_test_seams::note_pre_reply_high_water(
                self.accountant.high_water_for_test(),
            );
            persistence.commit_sync_reply_effects(&sources, &writes, &deletes)?;
            self.relational_store.set_sync_source_lsns(sources.iter().map(|(table, row, lsn, _)| (table.clone(), *row, *lsn)).collect::<Vec<_>>());
            self.relational_store.set_sync_source_kinds(sources.into_iter().map(|(table, row, _, _)|
                (table, row, contextdb_relational::store::SyncSourceKind::AcceptedLocal)));
            self.sync_tombstone_arrivals.write().extend(tombstones);
            if !refused_markers.is_empty() {
                self.terminal_refusal_markers.write().extend(refused_markers);
                scans.insert(context_key, reset_scan);
            }
            publication.complete();
            Ok(())
        })
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn complete_sync_push_confirmation_while_authoritative(
        &self, tenant_id: &TenantId, through: Lsn,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.commit_sync_reply_effects(&[], &[
                (tenant_id.config_key("sync_push_watermark"), RedbPersistence::encode_config_value(&through.0)?),
            ], &[
                tenant_id.config_key("sync_pending_push_confirmation"),
                tenant_id.config_key("sync_pending_push_result"),
            ])
            })?;
        } else {
            let mut progress = self.in_memory_sync_progress.lock();
            let progress = progress.tenants.entry(tenant_id.as_str().to_string()).or_default();
            progress.push_watermark = through;
            progress.pending_push_confirmation = None;
            progress.pending_push_result = None;
        }
        Ok(())
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn persisted_sync_pending_push_result(&self, tenant_id: &TenantId) -> Result<Option<ApplyResult>> {
        let result = if let Some(persistence) = &self.persistence {
            persistence.load_config_value::<crate::protocol::WireApplyResult>(&tenant_id.config_key("sync_pending_push_result"))?
        } else {
            self.in_memory_sync_progress.lock().tenants.get(tenant_id.as_str())
                .and_then(|progress| progress.pending_push_result.clone())
        };
        Ok(result.map(Into::into))
    }

    /// Serialize every state change derived from one authenticated hub reply
    /// with an explicit destination move. Callers use the closure for their
    /// in-memory client state; database writers called from that closure must
    /// use their `_while_authoritative` form so they do not re-lock the peer.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn with_authoritative_hub_reply<T>(
        &self,
        hub_node_id: &str,
        effect: impl FnOnce(&Database) -> Result<T>,
    ) -> Result<T> {
        let mut operation = self.open_operation()?;
        // Effects can commit more than once; each nested operation captures
        // its current root rather than inheriting this opening root.
        drop(operation._writer_read.take());
        let _authority = self.lock_authoritative_hub(hub_node_id)?;
        effect(self)
    }

    /// Validate a pull page at entry, then let each unit and metadata effect
    /// recheck its hub under its own schema/operation/destination scopes.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn with_authoritative_hub_received_apply<T>(
        &self,
        hub_node_id: &str,
        purge_bearing: bool,
        effect: impl FnOnce(&Database) -> Result<T>,
    ) -> Result<T> {
        // Let checked-out callbacks finish their database work before this
        // page takes schema/operation/destination scopes. Keep checkout paused
        // across every unit; each purge kernel shares this pause lease.
        let _queue_drain = purge_bearing
            .then(|| self.event_bus.begin_authoritative_purge_queue_drain())
            .transpose()?;
        {
            let _operation = self.open_operation_after_public_tx_control_wait("sync pull apply")?;
            let _authority = self.lock_authoritative_hub(hub_node_id)?;
        }
        effect(self)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(super) fn lock_authoritative_hub(
        &self,
        hub_node_id: &str,
    ) -> Result<parking_lot::MutexGuard<'_, Option<String>>> {
        let established = self.retention_sync_peer.lock();
        self.ensure_authoritative_hub_locked(hub_node_id, &established)?;
        Ok(established)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn ensure_authoritative_hub_locked(
        &self,
        hub_node_id: &str,
        established: &Option<String>,
    ) -> Result<()> {
        if established.as_deref() != Some(hub_node_id) {
            return Err(Error::SyncError((format!(
                "authenticated hub {hub_node_id} is no longer this edge's authoritative destination"
            )).into()));
        }
        if let Some(persistence) = &self.persistence {
            let durable =
                persistence.load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY)?;
            if durable.as_deref() != Some(hub_node_id) {
                return Err(Error::SyncError((format!(
                    "authenticated hub {hub_node_id} is no longer this edge's durable authoritative destination"
                )).into()));
            }
        }
        Ok(())
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_delete_is_pending_for_test(
        &self,
        table: &str,
        natural_key: &NaturalKey,
    ) -> Result<bool> {
        let key = Self::durable_lineage_config_key(
            table,
            natural_key,
            self.durable_lineage_table_generation(table, self.declared_reader_memory()?)?,
        );
        let lineage_state = self.lineage_state_lock.lock();
        Ok(self
            .load_lineage_record(&lineage_state, &key)?
            .is_some_and(|record| {
                record.delete_obligation == DurableDeleteObligation::Pending
            }))
    }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn has_terminal_refusal_markers_for_test(
        &self,
        tenant_id: &TenantId,
        hub_node_id: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.has_terminal_refusal_markers(&TerminalRefusalPullContext {
            tenant_id: tenant_id.clone(),
            hub_node_id: hub_node_id.to_string(),
            generation: 0,
        }, &mut crate::vector_read_memory::admission(read_memory.clone()), read_memory.clone())
    }
    }

    sync_test_seam! {
    /// A status reply from this authenticated edge's hub is behind the local
    /// push frontier, proving the hub was restored before some acknowledged
    /// local work. Mark only the affected `AcceptedLocal` order anchors as
    /// pending fresh ordering before the caller lowers its push watermark and
    /// resends; `Pulled` anchors still describe data this node received and
    /// must remain intact.
    ///
    /// The commit lock makes the current-version check and durable sidecar
    /// state change one boundary. A concurrent local write cannot be
    /// accidentally reclassified after this method chose its entries.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from sync_receipt_atomicity_tests.rs"))]
    fn invalidate_accepted_local_ordering_after_hub_regression(
        &self,
        hub_applied_source_watermark: Lsn,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.invalidate_accepted_local_ordering_after_hub_regression_with_read_memory(
            hub_applied_source_watermark,
            read_memory,
        )
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn invalidate_accepted_local_ordering_after_hub_regression_with_read_memory(
        &self,
        hub_applied_source_watermark: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        self.tx_mgr.with_maintenance_publication(|publication| {
            let mut entries = Vec::new();
            self.visit_authoritative_live_rows_for_outbound(|table, row| {
                let (source_lsn, kind) = self.relational_sync_source_at_read_root(table, row.row_id, read_memory.clone())?;
                if row.lsn > hub_applied_source_watermark && matches!(kind, Some(contextdb_relational::store::SyncSourceKind::AcceptedLocal)) {
                    entries.push((table.to_owned(), row.row_id, source_lsn.ok_or_else(|| Error::SyncError("AcceptedLocal lacks source LSN".into()))?));
                }
                Ok(())
            }, read_memory.clone())?;
            if !entries.is_empty() {
                publication.reserve()?;
                if let Some(persistence) = &self.persistence {
                    persistence.append_sync_source_provenance_batch(
                        &entries
                            .iter()
                            .map(|(table, row_id, lsn)| (table.clone(), *row_id, *lsn, 2))
                            .collect::<Vec<_>>(),
                    )?;
                }
                self.relational_store
                    .set_sync_source_kinds(entries.into_iter().map(|(table, row_id, _)| {
                        (
                            table,
                            row_id,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocalPending,
                        )
                    }));
                publication.complete();
            }
            for marker in self.sync_tombstone_arrivals.write().values_mut() {
                if matches!(
                    marker.kind,
                    contextdb_relational::store::SyncSourceKind::AcceptedLocal
                ) && marker.delete_lsn > hub_applied_source_watermark
                {
                    marker.accepted_local_order_invalidated = true;
                }
            }
            Ok(())
        })
    }
    }

    sync_test_seam! {
    /// A status confirms source work landed but not the hub order it received.
    /// Mark current, locally outbound work in the supplied source interval
    /// Pending before a pull can consume its echo. `through_inclusive = None`
    /// intentionally includes later local writes too: a restored-hub ordinary
    /// pull must not overwrite any current local work past its frontier.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from sync_receipt_atomicity_tests.rs"))]
    fn mark_outbound_rows_pending(
        &self,
        after_exclusive: Lsn,
        through_inclusive: Option<Lsn>,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.mark_outbound_rows_pending_with_read_memory(
            after_exclusive,
            through_inclusive,
            read_memory,
        )
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn mark_outbound_rows_pending_with_read_memory(
        &self,
        after_exclusive: Lsn,
        through_inclusive: Option<Lsn>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        self.tx_mgr.with_maintenance_publication(|publication| {
            let mut entries = Vec::new();
            self.visit_authoritative_live_rows_for_outbound(|table, row| {
                let (_, kind) = self.relational_sync_source_at_read_root(table, row.row_id, read_memory.clone())?;
                if row.lsn > after_exclusive && through_inclusive.is_none_or(|through| row.lsn <= through) && !matches!(kind, Some(contextdb_relational::store::SyncSourceKind::Pulled | contextdb_relational::store::SyncSourceKind::AcceptedLocal)) {
                    entries.push((table.to_owned(), row.row_id, row.lsn));
                }
                Ok(())
            }, read_memory.clone())?;
            let snapshot = self.snapshot();
            let empty_deleted = HashSet::new();
            let mut tombstones: HashMap<(String, String), (Lsn, RowId)> = HashMap::new();
            for entry in self.change_log_since_unlocked(after_exclusive, read_memory.clone())? {
                let ChangeLogEntry::RowDelete {
                    table,
                    row_id,
                    natural_key,
                    lsn,
                } = entry
                else {
                    continue;
                };
                if through_inclusive.is_some_and(|through| lsn > through) {
                    continue;
                }
                let key = Self::sync_tombstone_key(&table, &natural_key);
                if self
                    .visible_row_by_natural_key(&table, &natural_key, snapshot, &empty_deleted, read_memory.clone())?
                    .is_some()
                {
                    continue;
                }
                let pulled = (*self
                    .sync_tombstone_arrivals)
                    .read()
                    .get(&key)
                    .is_some_and(|marker| {
                        matches!(
                            marker.kind,
                            contextdb_relational::store::SyncSourceKind::Pulled
                        )
                    });
                if !pulled {
                    tombstones
                        .entry(key)
                        .and_modify(|latest| {
                            if lsn > latest.0 {
                                *latest = (lsn, row_id);
                            }
                        })
                        .or_insert((lsn, row_id));
                }
            }
            if !entries.is_empty() {
                publication.reserve()?;
                if let Some(persistence) = &self.persistence {
                    persistence.append_sync_source_provenance_batch(
                        &entries
                            .iter()
                            .map(|(table, row_id, lsn)| (table.clone(), *row_id, *lsn, 2))
                            .collect::<Vec<_>>(),
                    )?;
                }
                self.relational_store.set_sync_source_lsns(entries.clone());
                self.relational_store
                    .set_sync_source_kinds(entries.into_iter().map(|(table, row_id, _)| {
                        (
                            table,
                            row_id,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocalPending,
                        )
                    }));
                publication.complete();
            }
            if !tombstones.is_empty() {
                let mut markers = self.sync_tombstone_arrivals.write();
                for (key, (delete_lsn, row_id)) in tombstones {
                    markers.insert(
                        key,
                        SyncTombstoneArrival {
                            arrival: delete_lsn,
                            delete_lsn,
                            row_id: Some(row_id),
                            kind: contextdb_relational::store::SyncSourceKind::AcceptedLocalPending,
                            accepted_local_order_invalidated: false,
                        },
                    );
                }
            }
            Ok(())
        })
    }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn reconciled_push_rows_have_arrivals(
        &self,
        changes: &ChangeSet,
        lineages: &OutboundRowLineages,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        for row in &changes.rows {
            if row.deleted {
                return Ok(false);
            }
            let key = rmp_serde::to_vec(&row.natural_key)
                .map_err(|error| Error::SyncError((error.to_string()).into()))?;
            let Some(_expected) = lineages.get(&(row.table.clone(), key, row.lsn)) else {
                return Ok(false);
            };
            let meta = self.table_meta(&row.table);
            let mut confirmed = false;
            self.visit_authoritative_row_versions(
                |table, local| {
                    if table == row.table && local.lsn == row.lsn {
                        let (arrival, kind) = self.relational_sync_source_at_read_root(
                            table,
                            local.row_id,
                            read_memory.clone(),
                        )?;
                        if arrival.is_some()
                            && matches!(
                                kind,
                                Some(contextdb_relational::store::SyncSourceKind::AcceptedLocal)
                            )
                            && self.sync_incoming_row_matches_committed(
                                &row.values,
                                local,
                                table,
                                meta.as_ref(),
                                read_memory.clone(),
                            )?
                        {
                            confirmed = true;
                        }
                    }
                    Ok(())
                },
                read_memory.clone(),
            )?;
            if !confirmed {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn has_unconfirmed_push_rows(
        &self,
        after: Lsn,
        through: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if after >= through {
            return Ok(false);
        }
        let _operation = self.open_operation()?;
        let published = self.tx_mgr.capture_published_read();
        let root = writer_read::durable_root(published.handle)?;
        let _writer_read = self.enter_writer_read(root.clone(), read_memory.clone());
        let unconfirmed = |table: &str, row_id: RowId, deleted: bool| -> Result<bool> {
            let (_, kind) =
                self.relational_sync_source_at_read_root(table, row_id, read_memory.clone())?;
            Ok(!matches!(
                kind,
                Some(contextdb_relational::store::SyncSourceKind::AcceptedLocal)
            ) && (deleted
                || !matches!(
                    kind,
                    Some(contextdb_relational::store::SyncSourceKind::Pulled)
                )))
        };
        fn identity(entry: &ChangeLogEntry) -> Option<(&str, RowId, bool)> {
            match entry {
                ChangeLogEntry::RowInsert { table, row_id, .. } => {
                    Some((table.as_str(), *row_id, false))
                }
                ChangeLogEntry::RowDelete { table, row_id, .. } => {
                    Some((table.as_str(), *row_id, true))
                }
                _ => None,
            }
        }
        if let Some(root) = root {
            if after >= root.lsn {
                return Ok(false);
            }
            let owner =
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone());
            let mut changes =
                root.change_log_cursor(owner.clone(), Lsn(after.0 + 1), through.min(root.lsn))?;
            while let Some(change) = changes.next_change()? {
                let Some((table, row_id, deleted)) = identity(&change.value) else {
                    continue;
                };
                if self.table_meta(table).is_none() {
                    continue;
                }
                let mut rows = root.row_cursor(owner.clone(), table, row_id)?;
                if rows.next_row()?.is_some() && unconfirmed(table, row_id, deleted)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let changes = self.change_log.read();
        let start = changes.partition_point(|entry| entry.lsn() <= after);
        let end = changes.partition_point(|entry| entry.lsn() <= through);
        for entry in &changes[start..end] {
            let Some((table, row_id, deleted)) = identity(entry) else {
                continue;
            };
            if self.table_meta(table).is_some() && unconfirmed(table, row_id, deleted)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Select only authenticated echoes of the local lineage before applying a
    /// pull page. Equal values from a different creator do not acknowledge the
    /// local mutation. This reuses ordinary row provenance, with no new receipt.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn matching_pending_reconciliation_rows(
        &self,
        rows: &[RowChange],
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
    ) -> Result<Vec<RowChange>> {
        let mut matching = Vec::new();
        let lineage_state = self.lineage_state_lock.lock();
        for row in rows {
            let Some((_, _, _, lineage)) = lineages.iter().find(|(table, key, lsn, _)| {
                table == &row.table && key == &row.natural_key && *lsn == row.lsn
            }) else {
                continue;
            };
            let known = self.load_row_lineage_sidecar(
                &lineage_state,
                &Self::durable_row_lineage_config_key(
                    &row.table,
                    &row.natural_key,
                    lineage.table_generation,
                ),
            )?;
            if known
                .as_ref()
                .is_some_and(|known| Self::row_lineage_matches_wire(known, lineage))
            {
                matching.push(row.clone());
            }
        }
        Ok(matching)
    }

    sync_test_seam! {
    /// After status confirmed a re-send landed, a byte-identical pull echo
    /// has no row mutation to carry through the normal sync write set. Refresh
    /// matching Pending versions explicitly, in one durable provenance batch
    /// under the commit lock. Production callers first select authenticated
    /// lineage matches; retained history keeps evidence if a later page edit
    /// has already replaced the visible row.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from sync_receipt_atomicity_tests.rs"))]
    fn refresh_confirmed_pending_rows(
        &self,
        rows: &[RowChange],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.refresh_confirmed_pending_rows_with_read_memory(rows, arrivals, read_memory)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn refresh_confirmed_pending_rows_with_read_memory(
        &self,
        rows: &[RowChange],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        self.tx_mgr.with_maintenance_publication(|publication| {
            let root = self.writer_read_root()?;
            let _writer_read = self.enter_writer_read(root.clone(), read_memory.clone());
            let mut source = root
                .map(|root| self.statement_relational_source(root, read_memory.clone()))
                .transpose()?;
            let mut entries = Vec::new();
            let mut tombstone_refreshes = Vec::new();
            for row in rows.iter().filter(|row| !row.deleted) {
                let Some(arrival) = Self::resolve_incoming_arrival(row, arrivals) else {
                    continue;
                };
                let meta = self.table_meta(&row.table);
                let table = row.table.as_str();
                self.visit_row_versions_for_natural_key(&mut source, table, &row.natural_key, |current| {
                    if matches!(self.relational_sync_source_at_read_root(table, current.row_id, read_memory.clone())?.1,
                            Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending))
                        && self.sync_incoming_row_matches_committed(&row.values, current, table, meta.as_ref(), read_memory.clone())? {
                        entries.push((table.to_owned(), current.row_id, arrival));
                    }
                    Ok(())
                }, read_memory.clone())?;
            }
            for row in rows.iter().filter(|row| row.deleted) {
                let Some(arrival) = Self::resolve_incoming_arrival(row, arrivals) else {
                    continue;
                };
                let key = Self::sync_tombstone_key(&row.table, &row.natural_key);
                if (*self
                    .sync_tombstone_arrivals)
                    .read()
                    .get(&key)
                    .is_some_and(|marker| {
                        matches!(
                            marker.kind,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocal
                                | contextdb_relational::store::SyncSourceKind::AcceptedLocalPending
                        ) && (marker.accepted_local_order_invalidated
                            || matches!(
                                marker.kind,
                                contextdb_relational::store::SyncSourceKind::AcceptedLocalPending
                            ))
                            && marker.delete_lsn == row.lsn
                    })
                {
                    tombstone_refreshes.push((key, arrival));
                }
            }
            if !entries.is_empty() {
                publication.reserve()?;
                if let Some(persistence) = &self.persistence {
                    persistence.append_sync_source_provenance_batch(
                        &entries
                            .iter()
                            .map(|(table, row_id, lsn)| (table.clone(), *row_id, *lsn, 1))
                            .collect::<Vec<_>>(),
                    )?;
                }
                self.relational_store.set_sync_source_lsns(entries.clone());
                self.relational_store
                    .set_sync_source_kinds(entries.into_iter().map(|(table, row_id, _)| {
                        (
                            table,
                            row_id,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocal,
                        )
                    }));
                publication.complete();
            }
            if !tombstone_refreshes.is_empty() {
                let mut tombstones = self.sync_tombstone_arrivals.write();
                for (key, arrival) in tombstone_refreshes {
                    if let Some(marker) = tombstones.get_mut(&key) {
                        marker.arrival = arrival;
                        marker.kind = contextdb_relational::store::SyncSourceKind::AcceptedLocal;
                        marker.accepted_local_order_invalidated = false;
                    }
                }
            }
            Ok(())
        })
    }
    }

    /// Test-introspection: the commit index as this handle would report it,
    /// in LSN order. A fixture that wants the store's own answer asks for it
    /// here rather than assembling one from counters it watched go by.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn commit_index_for_test(&self) -> Vec<(Lsn, TxId)> {
        let _operation = self.assert_open_operation();
        if self.image_history.is_some() {
            let memory = self
                .declared_reader_memory()
                .expect("test reader memory admission");
            let charge = crate::read_image_memory::ImageMemory::charging_reader(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                memory.clone(),
            );
            let context = contextdb_tx::ReadCommitContext::new(memory, &|| Ok(()));
            self.commit_index_for_read_image(&context, &charge)
                .expect("test captured commit output")
        } else {
            self.tx_mgr.commit_index_snapshot().into_iter().collect()
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_index_for_read_image(
        &self,
        context: &contextdb_tx::ReadCommitContext<'_>,
        charge: &crate::read_image_memory::ImageMemoryHandle,
    ) -> Result<Vec<(Lsn, TxId)>> {
        use std::ops::Bound;
        let mut cursor = self.tx_mgr.read_commit_directory_cursor(
            Bound::Unbounded,
            Bound::Included(self.tx_mgr.current_lsn()),
            context,
        )?;
        struct OutputCredit {
            charge: crate::read_image_memory::ImageMemoryHandle,
            bytes: usize,
        }
        impl Drop for OutputCredit {
            fn drop(&mut self) {
                self.charge.release(self.bytes);
            }
        }
        let mut output_credit = OutputCredit {
            charge: charge.clone(),
            bytes: 0,
        };
        // On refusal the vector deallocates before this credit releases.
        let mut entries = Vec::new();
        while let Some(pair) = cursor.next_entry(context)? {
            context.checkpoint()?;
            if entries.len() == entries.capacity() {
                let old = entries.capacity();
                let next = old.saturating_mul(2).max(4);
                let bytes = next
                    .checked_mul(std::mem::size_of::<(Lsn, TxId)>())
                    .and_then(|bytes| bytes.checked_add(crate::sync_types::held::ALLOCATION_BYTES))
                    .ok_or_else(|| Error::Other("commit output capacity overflow".into()))?;
                // Reallocation may retain the old buffer until the new one is
                // installed. Admit both, then release the old physical buffer.
                let retained = output_credit
                    .bytes
                    .checked_add(bytes)
                    .ok_or_else(|| Error::Other("commit output charge overflow".into()))?;
                charge.reserve(bytes)?;
                output_credit.bytes = retained;
                entries
                    .try_reserve_exact(next - entries.len())
                    .map_err(|error| {
                        Error::Other((format!("allocate commit output: {error}")).into())
                    })?;
                if old != 0 {
                    let returned = old * std::mem::size_of::<(Lsn, TxId)>()
                        + crate::sync_types::held::ALLOCATION_BYTES;
                    charge.release(returned);
                    output_credit.bytes -= returned;
                }
            }
            entries.push(pair);
        }
        context.checkpoint()?;
        // Successful output inherits this existing answer owner's credit.
        output_credit.bytes = 0;
        Ok(entries)
    }
}

// Shared durable classification; callers decide whether a historical member
// can be consumed or must fail the raw absent-key guard.
fn terminated_creator_matches(
    record: &DurableLineageRecord,
    lineage: &crate::protocol::WireRowLineage,
) -> bool {
    let creator_matches = match (
        record.author_node_id.as_deref(),
        record.author_database_incarnation.as_deref(),
    ) {
        (Some(author), Some(incarnation)) => {
            lineage.author_node_id == author
                && lineage.author_database_incarnation.to_hex() == incarnation
                && lineage.author_local_mutation_position.0 == record.author_local_mutation_position
        }
        // An older durable delete without provenance cannot prove a fresh
        // same-key lineage until the creator root differs.
        _ => true,
    };
    lineage.lineage_root == record.lineage_root && creator_matches
}
