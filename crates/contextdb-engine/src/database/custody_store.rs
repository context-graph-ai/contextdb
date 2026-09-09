//! Statements 7/11/13/15/17: incremental metadata admission and publication.
use super::*;
use crate::custody::{records::*, store::Store};

pub(crate) struct CustodyDelta {
    _publication: crate::custody::store::Publication,
    records: BTreeMap<String, Option<Record>>,
    bytes: BTreeMap<String, Option<Vec<u8>>>,
}

impl Database {
    // Statement 9/root 27: the existing outbound builder delegates manifested membership.
    pub(crate) fn dependency_complete_outbound_units(
        &self,
        changes: ChangeSet,
        confirmed: Lsn,
    ) -> Result<Vec<OutboundSyncUnit>> {
        crate::custody::outbound::units(self, changes, confirmed)
    }
    pub(crate) fn custody_parent_refs(
        &self,
        row: &RowChange,
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
            if let Some(persistence) = &self.persistence {
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
                    entries.extend(
                        image
                            .config_values
                            .iter()
                            .filter(|(k, _)| owned_key(k))
                            .map(|(k, v)| (k.clone(), v.clone())),
                    );
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

    /// Statements 11/14/15: measurement only; no mutation or production API.
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
    pub(crate) fn custody_records(&self) -> Result<Vec<Record>> {
        self.custody_records_in(&["tenant_".into(), "delivery_".into()])
    }
    pub(crate) fn custody_records_in(&self, selectors: &[String]) -> Result<Vec<Record>> {
        self.with_custody_store(|store| {
            let records = store.select(selectors);
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    fn verify_selected_custody_records(&self, store: &Store, records: &[Record]) -> Result<()> {
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
    pub(crate) fn custody_table(&self, table: &str) -> Result<Vec<Record>> {
        self.with_custody_store(|store| {
            let records = store.select_table(table);
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    pub(crate) fn custody_rows(
        &self,
        rows: &[crate::custody::records::RowRef],
    ) -> Result<Vec<Record>> {
        self.with_custody_store(|store| {
            let records = store.select_rows(rows)?;
            self.verify_selected_custody_records(store, &records)?;
            Ok(records)
        })
    }
    pub(crate) fn custody_authority(&self) -> Result<Vec<Record>> {
        self.custody_records_in(&["@authority".into(), "@policies".into()])
    }
    pub(crate) fn custody_submission(&self, submission: uuid::Uuid) -> Result<Vec<Record>> {
        self.custody_records_in(&[format!(
            "@{}",
            crate::custody::store::submission_group(submission)
        )])
    }
    pub(crate) fn custody_root(
        &self,
        root: &crate::custody::records::RowRef,
    ) -> Result<Vec<Record>> {
        let roots =
            self.custody_records_in(&[format!("@{}", crate::custody::store::root_group(root))])?;
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
        )
    }
    pub(crate) fn custody_pending_manifests(&self) -> Result<Vec<Record>> {
        self.with_custody_store(|store| Ok(store.pending()))
    }
    // Statements 9/10/14: only complete adjudication may publish a batch bookmark.
    pub(crate) fn finish_custody_push(&self, receipt: &SyncApplyReceipt) -> Result<()> {
        self.commit_sync_apply_receipt_only(receipt)?;
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
    // Statements 9/10/14: retain ordinary arbitration, provenance and diagnostics
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
    ) -> Result<ApplyResult> {
        let frontier = self
            .persisted_sync_applied_push_watermark_for_node_incarnation(tenant, edge, incarnation)?
            .unwrap_or(Lsn(0));
        self.apply_authenticated_received_changes_with_receipt_and_lineages(
            changes,
            arrivals,
            SyncAdoption::Continuing,
            SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: edge.into(),
                incarnation,
                source_lsn: frontier,
                dependency_complete,
            },
            Some(hub),
            lineages,
            ddl,
        )
    }
    pub(crate) fn custody_row(
        &self,
        reference: &crate::custody::records::RowRef,
    ) -> Result<Option<VersionedRow>> {
        self.visible_row_by_natural_key(
            &reference.table,
            &reference.key,
            self.snapshot(),
            &HashSet::new(),
        )
    }

    // Statements 7/9/10/14: a replacement or deletion ends ownership of the
    // previous committed manifest. Preserve its signed history and terminals;
    // current rows require a registration from their own writing transaction.
    fn retire_changed_custody_roots(&self, ws: &mut WriteSet) -> Result<()> {
        use crate::custody::{canonical::hex, records::RowRef};
        if ws.relational_deletes.is_empty() {
            return Ok(());
        }
        let mut selectors = BTreeSet::new();
        for (table, rows) in self.deleted_row_snapshots_by_table(ws) {
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
        for record in self.custody_records_in(&selectors.into_iter().collect::<Vec<_>>())? {
            let Record::Manifest(m) = record else {
                continue;
            };
            let root = Record::Root {
                life: m.root_life.clone(),
                submission: m.id,
            };
            let key = root.key()?;
            let owned = self.custody_submission(m.id)?;
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

    pub(super) fn prepare_custody_commit(&self, ws: &mut WriteSet) -> Result<Option<CustodyDelta>> {
        self.retire_changed_custody_roots(ws)?;
        if !ws.config_writes.iter().any(|(key, _)| owned_key(key))
            && !ws.config_deletes.iter().any(|key| owned_key(key))
        {
            return Ok(None);
        }
        let (records, publication) = self.with_custody_store(|store| {
            let records = store.validate_delta(&ws.config_writes, &ws.config_deletes)?;
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
