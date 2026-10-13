//! Incremental metadata admission and publication.
use super::*;
use crate::custody::{records::*, store::Store};

pub(crate) struct CustodyDelta {
    _publication: crate::custody::store::Publication,
    records: BTreeMap<String, Option<Record>>,
    bytes: BTreeMap<String, Option<Vec<u8>>>,
}

impl Database {
    // The existing outbound builder delegates manifested membership.
    pub(crate) fn dependency_complete_outbound_units(
        &self,
        changes: ChangeSet,
        confirmed: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<OutboundSyncUnit>> {
        crate::custody::outbound::units(
            self,
            changes,
            confirmed,
            vector_copy_owner,
            read_memory.clone(),
        )
    }
    pub(crate) fn custody_parent_refs(
        &self,
        row: &RowChange,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<crate::custody::records::RowRef>> {
        let mut parents = Vec::new();
        if let Some(meta) = self.table_meta(&row.table) {
            let refs = meta
                .columns
                .iter()
                .filter_map(|c| {
                    c.references.as_ref().map(|r| {
                        (
                            r.table.clone(),
                            vec![c.name.clone()],
                            vec![r.column.clone()],
                        )
                    })
                })
                .chain(meta.composite_foreign_keys.iter().map(|r| {
                    (
                        r.parent_table.clone(),
                        r.child_columns.clone(),
                        r.parent_columns.clone(),
                    )
                }));
            for (table, columns, parent_columns) in refs {
                let Some(values) = columns
                    .iter()
                    .map(|c| row.values.get(c).filter(|v| **v != Value::Null).cloned())
                    .collect::<Option<Vec<_>>>()
                else {
                    continue;
                };
                if let Some(parent) = self.current_sync_row_by_columns(
                    &table,
                    &parent_columns,
                    &values,
                    self.snapshot(),
                    &HashSet::new(),
                    vector_copy_owner,
                    read_memory.clone(),
                )? {
                    parents.push(crate::custody::records::RowRef {
                        table,
                        key: parent.natural_key,
                    });
                }
            }
        }
        Ok(parents)
    }
    pub(super) fn with_custody_store<T>(
        &self,
        f: impl FnOnce(&mut Store) -> Result<T>,
    ) -> Result<T> {
        let mut cache = self.custody_cache.lock();
        if cache.is_none() {
            let mut entries = BTreeMap::new();
            if let Some(persistence) = &self.persistence
                && persistence.custody_groups_complete()?
            {
                // The delivery journal stays durable and is read through the
                // derived group index. Only the bounded tenant policy and
                // binding records are mirrored.
                for prefix in ["tenant_table_policy.v1.", "tenant_policy_binding.v1."] {
                    entries.extend(persistence.load_config_values_raw_with_prefix(prefix)?);
                }
                *cache = Some(Store::durable(persistence.clone(), entries)?);
                return f(cache.as_mut().expect("loaded custody metadata"));
            } else if let Some(persistence) = &self.persistence {
                // Never dump unrelated config; each owned journal is a Redb prefix range.
                for prefix in [
                    "tenant_table_policy.v1.",
                    "tenant_policy_binding.v1.",
                    "delivery_",
                ] {
                    entries.extend(persistence.load_config_values_raw_with_prefix(prefix)?);
                }
            } else {
                if let Some(image) = &self.committed_image_startup {
                    entries.extend(image.config_source.load_image_custody_config()?);
                }
                entries.extend(
                    self.custody_metadata
                        .lock()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
            }
            *cache = Some(Store::from_entries(entries)?);
        }
        f(cache.as_mut().expect("loaded custody metadata"))
    }

    /// Measurement only; no mutation or production API.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn __custody_metadata_work_for_test(&self) -> Result<(u64, u64, u64)> {
        self.with_custody_store(|store| {
            Ok((
                store.selected_records,
                store.read_visits,
                store.validation_visits,
            ))
        })
    }
    /// Measurement only: `(hydrated delivery-journal records, decoded custody
    /// records, resident delivery-journal records, resident delivery-journal
    /// bytes)`. The first two are process-wide counters; the resident pair
    /// observes this handle's cache without loading it.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn __custody_writer_work_for_test(&self) -> (u64, u64, u64, u64) {
        let (hydrated, decoded) = crate::custody::store::work::read();
        let (records, bytes) = self
            .custody_cache
            .lock()
            .as_ref()
            .map_or((0, 0), Store::resident_delivery_for_test);
        (hydrated, decoded, records, bytes)
    }
    /// Measurement only: committed custody CONFIG keys under `prefix`
    /// (for example `delivery_root.v1.`), counted without decoding values.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn __custody_durable_key_count_for_test(&self, prefix: &str) -> Result<u64> {
        self.persistence
            .as_ref()
            .map_or(Ok(0), |p| p.count_config_keys_with_prefix_for_test(prefix))
    }
    /// Measurement only: answer the same custody selections from a freshly
    /// loaded resident journal and from the durable group index, as encoded
    /// records. `None` when this store has no complete durable index.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub fn __custody_selection_parity_for_test(
        &self,
        root_table: &str,
        root_keys: &[crate::sync_types::NaturalKey],
    ) -> Result<Option<Vec<(String, Vec<Vec<u8>>, Vec<Vec<u8>>)>>> {
        use crate::custody::{canonical::hex, records::RowRef};
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        if !persistence.custody_groups_complete()? {
            return Ok(None);
        }
        let mut all = BTreeMap::new();
        let mut tenant = BTreeMap::new();
        for prefix in [
            "tenant_table_policy.v1.",
            "tenant_policy_binding.v1.",
            "delivery_",
        ] {
            for (key, value) in persistence.load_config_values_raw_with_prefix(prefix)? {
                if prefix.starts_with("tenant_") {
                    tenant.insert(key.clone(), value.clone());
                }
                all.insert(key, value);
            }
        }
        let mut resident = Store::from_entries(all)?;
        let mut durable = Store::durable(persistence.clone(), tenant)?;
        let read_memory = self.declared_reader_memory()?;
        let memory = || {
            crate::read_image_memory::ImageMemory::charging_reader(
                self.accountant.clone(),
                read_memory.clone(),
            )
        };
        let encoded = |records: &[Record]| records.iter().map(encode).collect::<Result<Vec<_>>>();
        let rows = root_keys
            .iter()
            .map(|key| RowRef {
                table: root_table.to_owned(),
                key: key.clone(),
            })
            .collect::<Vec<_>>();
        let mut selectors = vec![
            "delivery_".to_owned(),
            "tenant_".to_owned(),
            "delivery_root.v1.".to_owned(),
            "@authority".to_owned(),
            "@policies".to_owned(),
        ];
        for row in &rows {
            selectors.push(format!("@row:{}", hex(&row.bytes())));
            selectors.push(format!("@{}", crate::custody::store::root_group(row)));
        }
        for record in resident.select(&selectors[..1], memory())?.iter() {
            if let Record::Manifest(m) = record {
                selectors.push(format!(
                    "@{}",
                    crate::custody::store::submission_group(m.id)
                ));
            }
        }
        let mut out = Vec::new();
        for selector in &selectors {
            let one = std::slice::from_ref(selector);
            out.push((
                selector.clone(),
                encoded(&resident.select(one, memory())?)?,
                encoded(&durable.select(one, memory())?)?,
            ));
        }
        out.push((
            "union".into(),
            encoded(&resident.select(&selectors, memory())?)?,
            encoded(&durable.select(&selectors, memory())?)?,
        ));
        out.push((
            format!("table:{root_table}"),
            encoded(&resident.select_table(root_table, memory())?)?,
            encoded(&durable.select_table(root_table, memory())?)?,
        ));
        out.push((
            "rows".into(),
            encoded(&resident.select_rows(&rows, memory())?)?,
            encoded(&durable.select_rows(&rows, memory())?)?,
        ));
        out.push((
            "pending".into(),
            encoded(&resident.pending(memory())?)?,
            encoded(&durable.pending(memory())?)?,
        ));
        Ok(Some(out))
    }
    pub(crate) fn custody_records(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        self.custody_records_in(&["tenant_".into(), "delivery_".into()], read_memory.clone())
    }
    pub(crate) fn custody_records_in(
        &self,
        selectors: &[String],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory.clone(),
        );
        self.with_custody_store(|store| {
            let records = store.select(selectors, memory.clone())?;
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    fn verify_selected_custody_records(&self, store: &Store, records: &[Record]) -> Result<()> {
        // A durable answer was decoded and verified from the persisted bytes.
        if store.is_durable() {
            return Ok(());
        }
        if let Some(persistence) = &self.persistence {
            let checking = store.publications.load(Ordering::SeqCst) == 0;
            let keys = records
                .iter()
                .map(Record::key)
                .collect::<Result<Vec<_>>>()?;
            let persisted = persistence.load_custody_records_raw(&keys)?;
            for (key, record) in keys.iter().zip(records) {
                // Byte equality to the signature-admitted immutable record preserves
                // corruption-on-read without decoding/verifying the whole journal.
                if checking
                    && store.publications.load(Ordering::SeqCst) == 0
                    && persisted.get(key) != Some(&encode(record)?)
                {
                    return Err(crate::custody::canonical::invalid());
                }
            }
        }
        Ok(())
    }
    pub(crate) fn custody_table(
        &self,
        table: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory.clone(),
        );
        self.with_custody_store(|store| {
            let records = store.select_table(table, memory.clone())?;
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    pub(crate) fn custody_rows(
        &self,
        rows: &[crate::custody::records::RowRef],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory.clone(),
        );
        self.with_custody_store(|store| {
            let records = store.select_rows(rows, memory.clone())?;
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    pub(crate) fn custody_authority(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        self.custody_records_in(
            &["@authority".into(), "@policies".into()],
            read_memory.clone(),
        )
    }
    pub(crate) fn custody_submission(
        &self,
        submission: uuid::Uuid,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        self.custody_records_in(
            &[format!(
                "@{}",
                crate::custody::store::submission_group(submission)
            )],
            read_memory.clone(),
        )
    }
    pub(crate) fn custody_root(
        &self,
        root: &crate::custody::records::RowRef,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        let roots = self.custody_records_in(
            &[format!("@{}", crate::custody::store::root_group(root))],
            read_memory.clone(),
        )?;
        let ids = roots
            .iter()
            .filter_map(|r| match r {
                Record::Manifest(m) => Some(m.id),
                Record::Terminal { record: t, .. } => Some(t.submission),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        self.custody_records_in(
            &ids.into_iter()
                .map(|id| format!("@{}", crate::custody::store::submission_group(id)))
                .collect::<Vec<_>>(),
            read_memory.clone(),
        )
    }
    pub(crate) fn custody_pending_manifests(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::custody::store::AdmittedRecords> {
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory,
        );
        self.with_custody_store(|store| store.pending(memory))
    }
    // Only complete adjudication may publish a batch bookmark.
    pub(crate) fn finish_custody_push(
        &self,
        receipt: &SyncApplyReceipt,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.commit_sync_apply_receipt_only(receipt, vector_copy_owner, read_memory.clone())?;
        if self.persistence.is_none() {
            let key = Self::applied_push_watermark_node_incarnation_key(
                &receipt.tenant_id,
                &receipt.node_id,
                receipt.incarnation,
            );
            self.in_memory_applied_push_watermarks
                .lock()
                .entry(key)
                .and_modify(|old| *old = (*old).max(receipt.source_lsn))
                .or_insert(receipt.source_lsn);
        }
        Ok(())
    }
    // Retain ordinary arbitration, provenance and diagnostics
    // while holding its receipt at the already-adjudicated batch frontier.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_custody_remainder(
        &self,
        changes: ChangeSet,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        hub: &str,
        tenant: &TenantId,
        edge: &str,
        incarnation: Incarnation,
        lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        ddl: Option<&crate::protocol::ReceivedDdlContext>,
        dependency_complete: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<ApplyResult> {
        let frontier = self
            .persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
                tenant,
                edge,
                incarnation,
                read_memory.clone(),
            )?
            .unwrap_or(Lsn(0));
        self.apply_authenticated_received_changes_with_outcome(
            changes,
            arrivals,
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: edge.into(),
                incarnation,
                source_lsn: frontier,
                dependency_complete,
                hub_contact_ms: None,
            },
            Some(hub),
            lineages,
            ddl,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }
    pub(crate) fn custody_row(
        &self,
        reference: &crate::custody::records::RowRef,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<VersionedRow>> {
        // Custody digests cover every committed column, so a file-backed
        // writer's paged-out bodies are read back for the row it hands on.
        let Some(mut row) = self.visible_row_by_natural_key(
            &reference.table,
            &reference.key,
            self.snapshot(),
            &HashSet::new(),
            read_memory.clone(),
        )?
        else {
            return Ok(None);
        };
        let shed = self.shed_committed_values_for_change(
            &reference.table,
            &row,
            &crate::database::ChangeProjection::unbounded(),
            read_memory.clone(),
        )?;
        row.values.extend(shed);
        Ok(Some(row))
    }

    // A replacement or deletion ends ownership of the
    // previous committed manifest. Preserve its signed history and terminals;
    // current rows require a registration from their own writing transaction.
    fn retire_changed_custody_roots(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        use crate::custody::{canonical::hex, records::RowRef};
        if ws.relational_deletes.is_empty() {
            return Ok(());
        }
        let mut selectors = BTreeSet::new();
        for (table, rows) in
            self.deleted_row_snapshots_by_table(ws, snapshot, read_memory.clone())?
        {
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            for row in rows.values() {
                if let Some(key) =
                    crate::sync_types::natural_key_from_row_values(&meta, &row.values)
                {
                    selectors.insert(format!(
                        "@row:{}",
                        hex(&RowRef {
                            table: table.clone(),
                            key
                        }
                        .bytes())
                    ));
                }
            }
        }
        if selectors.is_empty() {
            return Ok(());
        }
        let selected = self.custody_records_in(
            &selectors.into_iter().collect::<Vec<_>>(),
            read_memory.clone(),
        )?;
        for record in selected.iter() {
            let Record::Manifest(m) = record else {
                continue;
            };
            let root = Record::Root {
                life: m.root_life.clone(),
                submission: m.id,
            };
            let key = root.key()?;
            let owned = self.custody_submission(m.id, read_memory.clone())?;
            if !owned.iter().any(|r| {
                matches!(r, Record::Root { life, submission }
                if *life == m.root_life && *submission == m.id)
            }) {
                continue;
            }
            // A re-registration can replace the same local ownership key in this commit.
            if !ws.config_writes.iter().any(|(written, _)| *written == key)
                && !ws.config_deletes.contains(&key)
            {
                ws.config_deletes.push(key);
            }
        }
        Ok(())
    }

    pub(super) fn prepare_custody_commit(
        &self,
        ws: &mut WriteSet,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<CustodyDelta>> {
        self.retire_changed_custody_roots(
            ws,
            self.scoped_writer_root()
                .map_or_else(|| self.snapshot_for_read(), |root| root.snapshot),
            read_memory.clone(),
        )?;
        if !ws.config_writes.iter().any(|(key, _)| owned_key(key))
            && !ws.config_deletes.iter().any(|key| owned_key(key))
        {
            return Ok(None);
        }
        // Validation scratch is writer work for this commit, so it is admitted
        // to the writer accountant rather than the reader that requested it.
        let memory = crate::read_image_memory::ImageMemory::charging_for(
            self.accountant.clone(),
            "writer",
            "validate_custody_commit",
            "Reduce the custody records one commit touches or raise MEMORY_LIMIT.",
        );
        let (records, publication) = self.with_custody_store(|store| {
            let records = store.validate_delta(&ws.config_writes, &ws.config_deletes, memory)?;
            Ok((
                records,
                crate::custody::store::Publication::begin(&store.publications),
            ))
        })?;
        let mut bytes = BTreeMap::new();
        if self.persistence.is_none() {
            for (key, record) in &records {
                bytes.insert(key.clone(), record.as_ref().map(encode).transpose()?);
            }
        }
        Ok(Some(CustodyDelta {
            _publication: publication,
            records,
            bytes,
        }))
    }
    pub(super) fn publish_custody_delta(&self, delta: CustodyDelta) {
        let mut cache = self.custody_cache.lock();
        // A discard may have invalidated the old view while publishing its rows.
        if let Some(store) = cache.as_mut() {
            for (key, record) in delta.records {
                match record {
                    Some(record) => store.insert(key, record),
                    None => store.remove(&key),
                }
                .expect("prevalidated custody key/index delta");
            }
        }
        if !delta.bytes.is_empty() {
            let mut memory = self.custody_metadata.lock();
            for (key, bytes) in delta.bytes {
                if let Some(bytes) = bytes {
                    memory.insert(key, bytes);
                } else {
                    memory.remove(&key);
                }
            }
        }
    }
}

// Prepared cache changes own native keys, decoded records and encoded memory
// bytes independently; no mutation or naked delta escapes this owner.
struct OwnedCustodyChange {
    key: crate::custody::preparation::OwnedPreparationValue<String>,
    record: Option<crate::custody::preparation::OwnedPreparationValue<Record>>,
    bytes: Option<crate::custody::preparation::OwnedPreparationValue<Vec<u8>>>,
}
pub(super) struct OwnedCustodyDelta {
    changes: crate::custody::preparation::OwnedPreparationValue<Vec<OwnedCustodyChange>>,
    _publication: crate::custody::store::Publication,
}
fn custody_groups_owned(
    db: &Database,
    record: &Record,
) -> Result<
    crate::custody::preparation::OwnedPreparationValue<
        Vec<crate::custody::preparation::OwnedPreparationValue<String>>,
    >,
> {
    use crate::custody::{authority::*, canonical::*, preparation::*};
    let count = match record {
        Record::Manifest(m) => m.rows.len().checked_add(2).ok_or_else(invalid)?,
        Record::Terminal { .. } => 3,
        Record::MaterializedOwner(_) | Record::Binding(_) | Record::ObjectAdmission(_) => 2,
        Record::CheckpointFrontier(_) | Record::CheckpointNode(_) | Record::EdgeCheckpoint(_) => 0,
        _ => 1,
    };
    OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<String>>(count)?,
        || {
            let mut groups = Vec::with_capacity(count);
            let mut reference = |prefix: &str, r: &RowRef| -> Result<()> {
                let bytes = finite_bytes_owned(db, |e| {
                    r.encode_finite(e);
                    Ok(())
                })?;
                groups.push(bytes.with_value(|bytes| {
                    prepare_string_owned(db, format_args!("{prefix}:{}", BorrowedHex(bytes)))
                })?);
                Ok(())
            };
            let submission = match record {
                Record::Manifest(m) => {
                    reference("root", &m.seal.root)?;
                    for row in &m.rows {
                        reference("row", &row.reference)?;
                    }
                    Some(m.id)
                }
                Record::Terminal { record: t, .. } => {
                    reference("root", &t.root)?;
                    let digest = object_digest_finite(1, |e| t.bytes_finite(e), &t.signature)?;
                    groups.push(prepare_string_owned(
                        db,
                        format_args!("object:{}", BorrowedHex(&digest)),
                    )?);
                    Some(t.submission)
                }
                Record::MaterializedOwner(o) => {
                    reference("root", &o.source.reference)?;
                    Some(o.submission)
                }
                Record::Root { submission, .. }
                | Record::MemberOwner { submission, .. }
                | Record::Diagnostic { submission, .. }
                | Record::DiagnosticOwner { submission, .. }
                | Record::SourceHistory { submission, .. }
                | Record::PolicyProfile { submission, .. }
                | Record::Order { submission, .. } => Some(*submission),
                Record::Binding(b) => {
                    groups.push(prepare_string_owned(db, format_args!("authority"))?);
                    let digest = object_digest_finite(0, |e| b.bytes_finite(e), &b.signature)?;
                    groups.push(prepare_string_owned(
                        db,
                        format_args!("object:{}", BorrowedHex(&digest)),
                    )?);
                    None
                }
                Record::ObjectAdmission(a) => {
                    groups.push(prepare_string_owned(
                        db,
                        format_args!("object:{}", BorrowedHex(&a.object_digest)),
                    )?);
                    groups.push(prepare_string_owned(
                        db,
                        format_args!("certificate:{}", BorrowedHex(&a.certificate)),
                    )?);
                    None
                }
                Record::CheckpointFrontier(_)
                | Record::CheckpointNode(_)
                | Record::EdgeCheckpoint(_) => None,
                Record::Policy(_) => {
                    groups.push(prepare_string_owned(db, format_args!("policies"))?);
                    None
                }
                Record::Control(_)
                | Record::State(_)
                | Record::Birth(_)
                | Record::Certificate(_)
                | Record::Destination(_) => {
                    groups.push(prepare_string_owned(db, format_args!("authority"))?);
                    None
                }
            };
            if let Some(id) = submission {
                groups.push(prepare_string_owned(db, format_args!("submission:{id}"))?);
            }
            if groups.len() != count || groups.capacity() != count {
                return Err(invalid());
            }
            Ok(groups)
        },
    )
}
fn custody_record_matches_group(db: &Database, record: &Record, group: &str) -> Result<bool> {
    custody_groups_owned(db, record)?
        .with_value(|groups| Ok(groups.iter().any(|g| g.with_value(|g| g == group))))
}
fn custody_companions_owned(
    db: &Database,
    cache: &Store,
    record: &Record,
    delta: &[OwnedCustodyChange],
) -> Result<crate::custody::preparation::OwnedPreparationRecords> {
    use crate::custody::{authority::*, canonical::*, preparation::*};
    let selectors = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<String>>(2)?,
        || {
            let mut selectors = Vec::with_capacity(2);
            match record {
                Record::Control(_) => {
                    selectors.push(prepare_string_owned(db, format_args!("@authority"))?)
                }
                Record::ObjectAdmission(a) => {
                    selectors.push(prepare_string_owned(
                        db,
                        format_args!("@object:{}", BorrowedHex(&a.object_digest)),
                    )?);
                    selectors.push(prepare_string_owned(
                        db,
                        format_args!("delivery_peer_evidence.v1.{}", BorrowedHex(&a.certificate)),
                    )?);
                }
                Record::Terminal { record: t, .. } => {
                    selectors.push(prepare_string_owned(
                        db,
                        format_args!("@submission:{}", t.submission),
                    )?);
                    let digest = object_digest_finite(1, |e| t.bytes_finite(e), &t.signature)?;
                    selectors.push(prepare_string_owned(
                        db,
                        format_args!("@object:{}", BorrowedHex(&digest)),
                    )?);
                }
                Record::Root { submission, .. } => selectors.push(prepare_string_owned(
                    db,
                    format_args!("@submission:{submission}"),
                )?),
                Record::MaterializedOwner(o) => selectors.push(prepare_string_owned(
                    db,
                    format_args!("@submission:{}", o.submission),
                )?),
                _ => {}
            }
            Ok(selectors)
        },
    )?;
    let capacity = cache
        .records
        .len()
        .checked_add(delta.len())
        .ok_or_else(invalid)?;
    let selected = |key: &str, record: &Record| {
        selectors.with_value(|selectors| -> Result<bool> {
            for selector in selectors {
                let matches = selector.with_value(|selector| match selector.strip_prefix('@') {
                    Some(group) => custody_record_matches_group(db, record, group),
                    None => Ok(key.starts_with(selector)),
                })?;
                if matches {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    };
    let keys =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationValue<String>>(capacity)?,
            || {
                let mut keys = Vec::with_capacity(capacity);
                for (key, candidate) in &cache.records {
                    if selected(key, candidate)? {
                        keys.push(prepare_string_owned(db, format_args!("{key}"))?);
                    }
                }
                for change in delta {
                    if let Some(record) = &change.record {
                        if change
                            .key
                            .with_value(|key| record.with_value(|record| selected(key, record)))?
                        {
                            keys.push(change.key.with_value(|key| {
                                prepare_string_owned(db, format_args!("{key}"))
                            })?);
                        }
                    }
                }
                keys.sort_unstable_by(|a, b| a.with_value(|a| b.with_value(|b| a.cmp(b))));
                keys.dedup_by(|a, b| a.with_value(|a| b.with_value(|b| a == b)));
                Ok(keys)
            },
        )?;
    keys.with_value(|keys| {
        let mut records = OwnedPreparationRecords::try_new(db, keys.len(), keys.len())?;
        for key in keys {
            key.with_value(|key| -> Result<()> {
                if let Some(change) = delta
                    .iter()
                    .find(|change| change.key.with_value(|k| k == key))
                {
                    if let Some(record) = &change.record {
                        record.with_value(|record| records.copy_record_finite(db, record))?;
                    }
                } else if let Some(record) = cache.records.get(key) {
                    records.copy_record_finite(db, record)?;
                }
                Ok(())
            })?;
        }
        Ok(records)
    })
}
// Each temporary carries only its own original read credit. Field order frees
// the decoded/row payload before returning that credit, including unwind.
struct SelectedCustodyRecord {
    record: Record,
    memory: Arc<crate::read_image_memory::ImageMemory>,
}
struct SelectedCustodyRow {
    row: Option<VersionedRow>,
    credit: contextdb_core::read_memory::ReadCredit,
}
impl Database {
    fn with_initialized_custody_store_owned<R>(
        &self,
        f: impl for<'s> FnOnce(&'s mut Store) -> Result<R>,
    ) -> Result<R> {
        let mut cache = self.custody_cache.lock();
        // Initialization is a separate original-funded route prerequisite.
        // Do not rebuild a shadow cache or invoke an unpriced initializer here.
        let store = cache
            .as_mut()
            .ok_or_else(crate::custody::canonical::invalid)?;
        // This route reads the resident maps directly. A durable store keeps
        // no delivery record resident, so it refuses rather than answer from
        // an empty journal.
        if store.is_durable() {
            return Err(crate::custody::canonical::invalid());
        }
        f(store)
    }

    fn committed_custody_row_owned(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<SelectedCustodyRow> {
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut copy = |row: &VersionedRow| -> Result<VersionedRow> {
            credit.try_grow_for(
                crate::memory_accounting::preparation_row_copy_bytes(row)?,
                "reader",
                "custody_changed_root_row",
                "Reduce selected custody rows or raise the declared reader memory limit.",
            )?;
            crate::memory_accounting::preparation_copy_row(row)
        };
        // Preserve committed_row_at's captured writer-root precedence; a newer
        // resident row must never replace this operation's original frontier.
        let row = if let Some(root) = self.writer_read_root()? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
                root,
                read_memory.clone(),
            )?;
            let window =
                contextdb_relational::source::visible_row(&source, table, row_id, snapshot)?;
            window
                .as_ref()
                .map(|window| copy(window.value()))
                .transpose()?
        } else {
            let resident = {
                let tables = self.relational_store.tables.read();
                let row = tables.get(table).and_then(|rows| {
                    self.relational_store
                        .visible_version_position(table, rows, row_id, snapshot)
                        .and_then(|position| rows.get(position))
                });
                row.map(&mut copy).transpose()?
            };
            drop(copy);
            if resident.is_some() || self.retained_relational_source.is_none() {
                resident
            } else {
                self.committed_row_at_admitted(table, row_id, snapshot, &mut credit, read_memory)?
            }
        };
        Ok(SelectedCustodyRow { row, credit })
    }

    fn retire_changed_custody_roots_owned(
        &self,
        guard: &contextdb_tx::manager::EngineGuard<'_>,
        cache: &Store,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<
        crate::custody::preparation::OwnedPreparationValue<
            Vec<crate::custody::preparation::OwnedPreparationValue<String>>,
        >,
    > {
        use crate::custody::{canonical::*, preparation::*};
        if guard.read().relational_deletes.is_empty() {
            return OwnedPreparationValue::try_produce(self, 0, || Ok(Vec::new()));
        }
        let deleted = OwnedPreparationValue::try_produce(
            self,
            preparation_array_bytes::<OwnedPreparationValue<RowRef>>(
                guard.read().relational_deletes.len(),
            )?,
            || {
                let mut deleted = Vec::with_capacity(guard.read().relational_deletes.len());
                if deleted.capacity() != guard.read().relational_deletes.len() {
                    return Err(invalid());
                }
                for (table, row_id, _) in guard.read().relational_deletes {
                    let selected = self.committed_custody_row_owned(
                        table,
                        *row_id,
                        snapshot,
                        read_memory.clone(),
                    )?;
                    let Some(row) = selected.row.as_ref() else {
                        continue;
                    };
                    if let Some(reference) = self
                        .with_owned_delivery_meta_if_present(table, |meta| {
                            reference_from_row_owned(self, table, meta, &row.values)
                        })?
                        .flatten()
                    {
                        deleted.push(reference);
                    }
                    // selected.row physically frees before selected.credit.
                }
                Ok(deleted)
            },
        )?;
        let selected_root = |record: &Record| {
            let Record::Root { life, submission } = record else {
                return false;
            };
            cache.records.values().any(|record| {
                let Record::Manifest(manifest) = record else {
                    return false;
                };
                manifest.root_life == *life
                    && manifest.id == *submission
                    && deleted.with_value(|deleted| {
                        deleted.iter().any(|deleted| {
                            deleted.with_value(|deleted| {
                                manifest.rows.iter().any(|row| row.reference == *deleted)
                            })
                        })
                    })
            })
        };
        let needed = |key: &String, record: &Record| {
            selected_root(record)
                && !guard
                    .read()
                    .config_writes
                    .iter()
                    .any(|(written, _)| written == key)
                && !guard.read().config_deletes.contains(key)
        };
        let count = cache
            .records
            .iter()
            .filter(|(key, record)| needed(key, record))
            .count();
        OwnedPreparationValue::try_produce(
            self,
            preparation_array_bytes::<OwnedPreparationValue<String>>(count)?,
            || {
                let mut keys = Vec::with_capacity(count);
                for (key, record) in &cache.records {
                    if needed(key, record) {
                        // Delete the authentic initialized Store entry, not a
                        // reconstructed key whose encoding could differ.
                        keys.push(prepare_string_owned(self, format_args!("{key}"))?);
                    }
                }
                if keys.len() != count || keys.capacity() != count {
                    return Err(invalid());
                }
                Ok(keys)
            },
        )
    }
    // Preparation only borrows the actual initialized cache. Cache installation
    // remains the actual commit caller's responsibility after durable apply.
    pub(super) fn prepare_custody_commit_owned(
        &self,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<OwnedCustodyDelta>> {
        let view = guard.read();
        if view.relational_deletes.is_empty()
            && !view.config_writes.iter().any(|(key, _)| owned_key(key))
            && !view.config_deletes.iter().any(|key| owned_key(key))
        {
            return Ok(None);
        }
        self.with_initialized_custody_store_owned(|cache| {
            guard.with_tx_checkpoint(|guard| {
                use crate::custody::{canonical::*, preparation::*};
                let retired = self.retire_changed_custody_roots_owned(
                    guard,
                    cache,
                    self.scoped_writer_root()
                        .map_or_else(|| self.snapshot_for_read(), |root| root.snapshot),
                    read_memory.clone(),
                )?;
                let view = guard.read();
                if retired.with_value(|retired| retired.is_empty())
                    && !view.config_writes.iter().any(|(key, _)| owned_key(key))
                    && !view.config_deletes.iter().any(|key| owned_key(key))
                {
                    return Ok(None);
                }
                let count = view
                    .config_writes
                    .iter()
                    .filter(|(key, _)| owned_key(key))
                    .count()
                    .checked_add(
                        view.config_deletes
                            .iter()
                            .filter(|key| owned_key(key))
                            .count(),
                    )
                    .and_then(|count| {
                        count.checked_add(retired.with_value(|retired| retired.len()))
                    })
                    .ok_or_else(invalid)?;
                let mut visits = 0u64;
                let changes = OwnedPreparationValue::try_produce(
                    self,
                    preparation_array_bytes::<OwnedCustodyChange>(count)?,
                    || {
                        let mut changes: Vec<OwnedCustodyChange> = Vec::with_capacity(count);

                        retired.with_value(|retired| -> Result<()> {
                            for key in retired {
                                let change =
                                    key.with_value(|key| -> Result<OwnedCustodyChange> {
                                        Ok(OwnedCustodyChange {
                                            key: prepare_string_owned(self, format_args!("{key}"))?,
                                            record: None,
                                            bytes: None,
                                        })
                                    })?;
                                install_custody_change_owned(&mut changes, change);
                            }
                            Ok(())
                        })?;
                        for key in view.config_deletes.iter().filter(|key| owned_key(key)) {
                            install_custody_change_owned(
                                &mut changes,
                                OwnedCustodyChange {
                                    key: prepare_string_owned(self, format_args!("{key}"))?,
                                    record: None,
                                    bytes: None,
                                },
                            );
                        }
                        for (key, bytes) in
                            view.config_writes.iter().filter(|(key, _)| owned_key(key))
                        {
                            let unchanged = if let Some(old) = cache.records.get(key) {
                                let encoded = prepare_named_encoding_owned(self, old)?;
                                encoded.with_value(|old| old == bytes)
                            } else {
                                false
                            };
                            if unchanged
                                && !changes
                                    .iter()
                                    .any(|change| change.key.with_value(|old| old == key))
                            {
                                continue;
                            }
                            let memory = crate::read_image_memory::ImageMemory::operation_reader(
                                self.bounded_read_accountant(),
                                read_memory.clone(),
                            )?;
                            let record: Record = memory.decode_msgpack(bytes)?;
                            let selected = SelectedCustodyRecord { record, memory };
                            let record = &selected.record;
                            let encoded = prepare_named_encoding_owned(self, record)?;
                            if !encoded.with_value(|encoded| encoded == bytes)
                                || !record_key_owned(self, record)?
                                    .with_value(|actual| actual == key)
                            {
                                return Err(invalid());
                            }
                            record.verify_owned(self, read_memory.clone())?;
                            let copied = copy_record_finite_owned(self, record)?;
                            install_custody_change_owned(
                                &mut changes,
                                OwnedCustodyChange {
                                    key: prepare_string_owned(self, format_args!("{key}"))?,
                                    record: Some(copied),
                                    bytes: None,
                                },
                            );
                        }

                        changes.sort_unstable_by(|a, b| {
                            a.key.with_value(|a| b.key.with_value(|b| a.cmp(b)))
                        });
                        let capacity =
                            cache.records.len().checked_add(count).ok_or_else(invalid)?;
                        let affected = OwnedPreparationValue::try_produce(
                            self,
                            preparation_array_bytes::<OwnedPreparationValue<String>>(capacity)?,
                            || {
                                let mut affected: Vec<OwnedPreparationValue<String>> =
                                    Vec::with_capacity(capacity);
                                let mut add = |key: &str| -> Result<()> {
                                    if !affected.iter().any(|old| old.with_value(|old| old == key))
                                    {
                                        affected.push(prepare_string_owned(
                                            self,
                                            format_args!("{key}"),
                                        )?);
                                    }
                                    Ok(())
                                };
                                for change in &changes {
                                    change.key.with_value(|key| add(key))?;
                                    let mut check = |record: &Record| -> Result<()> {
                                        let groups = custody_groups_owned(self, record)?;
                                        groups.with_value(|groups| -> Result<()> {
                                            for group in groups {
                                                group.with_value(|group| -> Result<()> {
                                                    if matches!(record, Record::ObjectAdmission(_))
                                                        && group.starts_with("certificate:")
                                                    {
                                                        return Ok(());
                                                    }
                                                    for (key, prior) in &cache.records {
                                                        if custody_record_matches_group(
                                                            self, prior, group,
                                                        )? {
                                                            add(key)?;
                                                        }
                                                    }
                                                    Ok(())
                                                })?;
                                            }
                                            Ok(())
                                        })?;
                                        if let Record::Certificate(c) = record {
                                            let group = prepare_string_owned(
                                                self,
                                                format_args!(
                                                    "certificate:{}",
                                                    BorrowedHex(&c.id_finite()?)
                                                ),
                                            )?;
                                            group.with_value(|group| -> Result<()> {
                                                for (key, prior) in &cache.records {
                                                    if custody_record_matches_group(
                                                        self, prior, group,
                                                    )? {
                                                        add(key)?;
                                                    }
                                                }
                                                Ok(())
                                            })?;
                                        }
                                        Ok(())
                                    };
                                    change.key.with_value(|key| -> Result<()> {
                                        if let Some(prior) = cache.records.get(key) {
                                            check(prior)?;
                                        }
                                        Ok(())
                                    })?;
                                    if let Some(record) = &change.record {
                                        record.with_value(|record| check(record))?;
                                    }
                                }
                                drop(add);
                                affected.sort_unstable_by(|a, b| {
                                    a.with_value(|a| b.with_value(|b| a.cmp(b)))
                                });
                                Ok(affected)
                            },
                        )?;
                        visits = affected.with_value(|affected| affected.len()) as u64;
                        affected.with_value(|affected| -> Result<()> {
                            for key in affected {
                                key.with_value(|key| -> Result<()> {
                                    let validate = |record: &Record| {
                                        custody_companions_owned(self, cache, record, &changes)?
                                            .with_record_references(self, |companions| {
                                                validate_record_owned(self, record, companions)
                                            })
                                    };
                                    if let Some(change) = changes.iter().find(|change| {
                                        change.key.with_value(|actual| actual == key)
                                    }) {
                                        if let Some(record) = &change.record {
                                            record.with_value(|record| validate(record))?;
                                        }
                                    } else if let Some(record) = cache.records.get(key) {
                                        validate(record)?;
                                    }
                                    Ok(())
                                })?;
                            }
                            Ok(())
                        })?;
                        if self.persistence.is_none() {
                            for change in &mut changes {
                                if let Some(record) = &change.record {
                                    change.bytes = Some(record.with_value(|record| {
                                        prepare_named_encoding_owned(self, record)
                                    })?);
                                }
                            }
                        }
                        Ok(changes)
                    },
                )?;
                // All roots/companions, original-copy invoices and encoded bodies are
                // validated before the first staged mutation. The actual checkpoint
                // inverses every staged delete on typed refusal or unwind.
                retired.with_value(|retired| -> Result<()> {
                    for key in retired {
                        key.with_value(|key| {
                            self.stage_owned_delivery_aux(guard, key.len(), || {
                                Ok(contextdb_tx::staged_source::AuxBody::ConfigDelete(
                                    key.to_owned(),
                                ))
                            })
                        })?;
                    }
                    Ok(())
                })?;
                cache.validation_visits += visits;
                Ok(Some(OwnedCustodyDelta {
                    changes,
                    _publication: crate::custody::store::Publication::begin(&cache.publications),
                }))
            })
        })
    }
}

fn install_custody_change_owned(changes: &mut Vec<OwnedCustodyChange>, change: OwnedCustodyChange) {
    if let Some(slot) = changes.iter_mut().find(|old| {
        old.key
            .with_value(|old| change.key.with_value(|key| old == key))
    }) {
        *slot = change;
    } else {
        assert!(
            changes.len() < changes.capacity(),
            "preadmitted custody delta slot"
        );
        changes.push(change);
    }
}
