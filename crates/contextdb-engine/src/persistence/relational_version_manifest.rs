//! Relational bodies and their private authenticated version membership share
//! the caller's existing publication transaction.
use super::writer_source::BorrowedRelationalIndexKey;
use super::*;
use content_identity::version_manifest as manifest;

#[cfg(all(test, feature = "test-seams"))]
#[path = "commit_row_scaling_tests.rs"]
mod commit_row_scaling_tests;

#[path = "position_tree.rs"]
pub(super) mod position_tree;

impl RedbPersistence {
    /// Coalesced postings need an interval subtraction only when another
    /// physical version of this row survives. Fully retired postings keep the
    /// existing manifest reconciliation path and require no payload copy.
    pub(super) fn relational_manifest_has_successor_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
    ) -> Result<bool> {
        if physical.len() != 24 {
            return Err(self.relational_manifest_damage("invalid physical version identity"));
        }
        let mut admission = self.proof_admission();
        admission.admit(
            table
                .len()
                .checked_add(72)
                .ok_or_else(|| self.relational_manifest_damage("identity allocation overflow"))?,
        )?;
        let mut prefix = manifest::manifest_prefix(table)?;
        prefix.extend_from_slice(physical);
        let coordinate = manifest::writer_manifest_next(write, &prefix, false, &mut admission)
            .map_err(|e| self.relational_manifest_error(e))?
            .filter(|key| key.starts_with(&prefix))
            .ok_or_else(|| self.relational_manifest_damage("physical version member is missing"))?;
        let next = manifest::writer_manifest_next(write, &coordinate, true, &mut admission)
            .map_err(|e| self.relational_manifest_error(e))?;
        let Some(next) = next else {
            return Ok(false);
        };
        if next.starts_with(&prefix) {
            return Err(self.relational_manifest_damage("duplicate physical version member"));
        }
        let witness =
            manifest::parse_manifest(&next).map_err(|e| self.relational_manifest_error(e))?;
        Ok(witness.table == table && witness.identity[..8] == physical[..8])
    }

    pub(super) fn replace_relational_manifest_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<()> {
        self.replace_relational_manifest_pending_in_write(write, table, physical, old, new, None)
    }

    pub(super) fn replace_relational_manifest_pending_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        mut pending: Option<&mut PendingManifestPublication>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        let _family_scope =
            content_identity::enter_passive_family(content_identity::LogicalFamily::ChangeAndDdl);
        let mut admission = self.proof_admission();
        admission.admit(
            table
                .len()
                .checked_add(72)
                .ok_or_else(|| self.relational_manifest_damage("identity allocation overflow"))?,
        )?;
        let mut prefix = manifest::manifest_prefix(table)?;
        prefix.extend_from_slice(physical);
        let expected = manifest::writer_manifest_next_pending(
            write,
            &prefix,
            false,
            &mut admission,
            &pending.as_deref().map(|p| &p.mutations),
        )
        .map_err(|e| self.relational_manifest_error(e))?;
        let old_coordinate = old
            .map(|body| manifest::manifest_for_body(table, physical, body, &mut admission))
            .transpose()
            .map_err(|e| self.relational_manifest_error(e))?;
        let addressed = expected.as_deref().filter(|key| key.starts_with(&prefix));
        if addressed != old_coordinate.as_deref() {
            return Err(self.relational_manifest_damage(
                "persisted predecessor and rooted version member disagree",
            ));
        }
        if let Some(coordinate) = &old_coordinate {
            let next = manifest::writer_manifest_next_pending(
                write,
                coordinate,
                true,
                &mut admission,
                &pending.as_deref().map(|p| &p.mutations),
            )
            .map_err(|e| self.relational_manifest_error(e))?;
            if next.as_deref().is_some_and(|key| key.starts_with(&prefix)) {
                return Err(self.relational_manifest_damage("duplicate physical version member"));
            }
        }
        let new_coordinate = new
            .map(|body| manifest::manifest_for_body(table, physical, body, &mut admission))
            .transpose()
            .map_err(|e| self.relational_manifest_error(e))?;
        if old_coordinate == new_coordinate {
            return Ok(());
        }
        if let Some(pending) = pending.as_mut() {
            if let Some(key) = &old_coordinate {
                pending.push(key, false)?;
            }
            if let Some(key) = &new_coordinate {
                pending.push(key, true)?;
            }
            return Ok(());
        }
        let capacity = old_coordinate
            .iter()
            .chain(new_coordinate.iter())
            .try_fold(
                2 * std::mem::size_of::<content_identity::ContentIdentityMutation>(),
                |n, key| {
                    n.checked_add(key.len())
                        .and_then(|n| n.checked_add(64))
                        .ok_or_else(|| {
                            self.relational_manifest_damage("mutation allocation overflow")
                        })
                },
            )?;
        admission.admit(capacity)?;
        let mut mutations = Vec::with_capacity(2);
        if let Some(key) = old_coordinate {
            mutations.push(manifest::manifest_mutation(&key, false)?);
        }
        if let Some(key) = new_coordinate {
            mutations.push(manifest::manifest_mutation(&key, true)?);
        }
        Self::apply_content_identity_mutations_in_write(write, &mutations, &mut admission)?;
        Ok(())
    }

    pub(super) fn relational_manifest_damage(&self, reason: impl std::fmt::Display) -> Error {
        Error::StoreCorrupted {
            path: self.path.display().to_string(),
            reason: format!("relational version manifest: {reason}"),
        }
    }
    pub(super) fn relational_manifest_error(&self, error: Error) -> Error {
        match error {
            Error::MemoryBudgetExceeded { .. }
            | Error::StoreCorrupted { .. }
            | Error::ReadCancelled
            | Error::ReadFailure(_) => error,
            other => self.relational_manifest_damage(other),
        }
    }

    /// Authenticate every retained member and both table boundaries before
    /// retiring a whole table generation. The loop never collects a table.
    pub(super) fn retire_relational_manifests_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        let name = Self::rel_table_name(table);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        let relation = write.open_table(definition).map_err(Self::storage_error)?;
        let mut physical = relation.iter().map_err(Self::storage_error)?;
        let mut range_admission = self.proof_admission();
        range_admission.admit(
            table
                .len()
                .checked_add(72)
                .and_then(|n| n.checked_mul(2))
                .ok_or_else(|| self.relational_manifest_damage("range bound overflow"))?,
        )?;
        let prefix = manifest::manifest_prefix(table)?;
        let mut after = prefix.clone();
        let mut exclusive = false;
        loop {
            let mut admission = self.proof_admission();
            let next = manifest::writer_manifest_next(write, &after, exclusive, &mut admission)
                .map_err(|e| self.relational_manifest_error(e))?;
            let Some(coordinate) = next.filter(|key| key.starts_with(&prefix)) else {
                if physical
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .is_some()
                {
                    return Err(
                        self.relational_manifest_damage("unwitnessed physical table suffix")
                    );
                }
                return Ok(());
            };
            let witness = manifest::parse_manifest(&coordinate)
                .map_err(|e| self.relational_manifest_error(e))?;
            let Some((key, body)) = physical.next().transpose().map_err(Self::storage_error)?
            else {
                return Err(
                    self.relational_manifest_damage("rooted table version is physically missing")
                );
            };
            if key.value() != witness.identity {
                return Err(
                    self.relational_manifest_damage("table version order or membership disagrees")
                );
            }
            self.replace_relational_manifest_in_write(
                write,
                table,
                key.value(),
                Some(body.value()),
                None,
            )?;
            after = coordinate;
            exclusive = true;
        }
    }
}

pub(super) struct ManifestKey {
    pub(super) coordinate: Vec<u8>,
    _charge: contextdb_core::read_memory::ReadCredit,
}

impl RetainedReadSource {
    /// Independent authenticated expectation, including an empty seek and the
    /// first member beyond the addressed row group.
    pub(super) fn manifest_next<E>(
        &self,
        start: Vec<u8>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<ManifestKey>, E>
    where
        E: From<Error>,
    {
        let owner = ReadHistoryProofOwner::new(read_memory.clone()).map_err(E::from)?;
        let proof = content_identity::ProofMemory::growable(0, owner.clone());
        let mut found = None;
        let mut key_credit = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let touch = std::cell::RefCell::new(&mut *before_touch);
        let complete =
            self.content_identity.visit_history_from::<E>(
                content_identity::HistoryKind::RelationalVersion,
                std::ops::Bound::Included(start),
                false,
                &proof,
                &mut || (touch.borrow_mut())(),
                &mut |coordinate, committed| {
                    content_identity::checkpoint_operation(
                        &mut || (touch.borrow_mut())(),
                        |checkpoint| {
                            manifest::parse_manifest_with_checkpoint(coordinate, checkpoint)
                                .map_err(|e| self.content_identity_error(e))
                        },
                    )?;
                    if committed != content_identity::version_manifest::manifest_marker_hash() {
                        return Err(E::from(self.content_identity_corruption(
                            "version manifest marker does not authenticate",
                        )));
                    }
                    key_credit
                        .try_grow_for(
                            coordinate
                                .len()
                                .checked_add(std::mem::size_of::<ManifestKey>())
                                .ok_or_else(|| {
                                    E::from(self.content_identity_corruption(
                                        "manifest key layout overflow",
                                    ))
                                })?,
                            "reader",
                            "version_manifest_key",
                            "Raise the declared reader memory limit.",
                        )
                        .map_err(E::from)?;
                    found = Some(content_identity::checkpoint_operation(
                        &mut || (touch.borrow_mut())(),
                        |checkpoint| manifest::copy_with_checkpoint(coordinate, checkpoint),
                    )?);
                    Ok(false)
                },
            )?;
        if let Some(error) = owner.refusal.lock().take() {
            return Err(E::from(error));
        }
        if !complete {
            return Err(E::from(self.unprovable_history(
                &self._source_memory.memory.accountant,
                content_identity::HistoryKind::RelationalVersion,
            )));
        }
        Ok(found.map(|coordinate| ManifestKey {
            coordinate,
            _charge: key_credit,
        }))
    }

    /// Captured-frontier row reading preserves the existing native traversal
    /// and current-row authentication without descending the history manifest.
    pub(super) fn current_relational_group<E: From<Error>>(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), E>,
        mut observed: Option<&mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        last: &mut Option<[u8; 24]>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        before()?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            std::mem::size_of::<Option<(VersionedRow, Arc<crate::read_image_memory::ImageMemory>)>>(
            ) + 2
                * (std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                    + 2 * std::mem::size_of::<usize>()),
            "reader",
            "current_row_controls",
            "Raise the declared reader memory limit.",
        )
        .map_err(E::from)?;
        let _name = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            table.len().checked_add(4).ok_or_else(|| {
                E::from(self.content_identity_corruption("row table name overflow"))
            })?,
            "reader",
            "current_row_table",
            "Raise the declared reader memory limit.",
        )
        .map_err(E::from)?;
        let name = RedbPersistence::rel_table_name(table);
        let relation = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
            .map_err(|error| E::from(RedbPersistence::relational_table_error(table, error)))?;
        let (low, high) = RedbPersistence::rel_row_key_range(row_id);
        let mut versions = relation
            .range(low.as_slice()..high.as_slice())
            .map_err(|error| RedbPersistence::relational_storage_error(table, error))
            .map_err(E::from)?;
        let enumerate = observed.is_some();
        let mut selected = None;
        loop {
            before()?;
            let entry = if enumerate {
                versions.next()
            } else {
                versions.next_back()
            };
            let Some((key, body)) = entry
                .transpose()
                .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                .map_err(E::from)?
            else {
                break;
            };
            let identity: [u8; 24] = key.value().try_into().map_err(|_| {
                E::from(self.content_identity_corruption("current row key has invalid width"))
            })?;
            let memory = crate::read_image_memory::ImageMemory::charging_reader(
                self._source_memory.memory.accountant.clone(),
                read_memory.clone(),
            );
            before()?;
            let row = memory
                .scope(|| RedbPersistence::decode_relational_source_row(table, meta, body.value()))
                .map_err(E::from)?;
            self.authenticate_relational_physical_key(table, &identity, &row)
                .map_err(E::from)?;
            *last = Some(identity);
            if let Some(callback) = observed.as_deref_mut() {
                callback(&row)?;
            }
            if row.created_tx.0 <= snapshot.0 {
                selected = Some((row, memory));
                if !enumerate {
                    break;
                }
            }
        }
        let Some((row, memory)) = selected else {
            return Ok(None);
        };
        before()?;
        if !self
            .authenticate_relational_row(table, meta, &row)
            .map_err(E::from)?
        {
            return Ok(None);
        }
        before_clone(crate::read_image_memory::RowReadAdmission::Move(&memory))?;
        drop(memory);
        Ok(Some(row))
    }

    pub(super) fn reconcile_relational_group<E>(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        exact_lsn: Option<Lsn>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        observed: Option<&mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        last_proved: &mut Option<[u8; 24]>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let witness_names = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory.clone(),
        );
        witness_names
            .reserve(table.len().saturating_add(64))
            .map_err(E::from)?;
        self.verify_history_root_in_read(read, &RedbPersistence::rel_table_name(table))
            .map_err(E::from)?;
        // Public CURRENT keeps its existing projection path; addressed private
        // doors call the complete group reconciler explicitly below.
        if exact_lsn.is_none() && snapshot == self.logical_frontier {
            return self.current_relational_group(
                read,
                table,
                meta,
                row_id,
                snapshot,
                read_memory,
                before_touch,
                observed,
                before_clone,
                last_proved,
            );
        }
        self.reconcile_addressed_relational_group(
            read,
            table,
            meta,
            row_id,
            snapshot,
            exact_lsn,
            read_memory,
            before_touch,
            observed,
            before_clone,
            last_proved,
        )
    }

    pub(super) fn reconcile_addressed_relational_group<E>(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        exact_lsn: Option<Lsn>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        mut observed: Option<&mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        last_proved: &mut Option<[u8; 24]>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        before_touch()?;
        // Preserve the first-history-use gate on the very same capture.
        let checkpoint_error = std::cell::RefCell::new(None);
        let checkpoint = std::cell::RefCell::new(&mut *before_touch);
        let gate = self.check_history_directory_in_read(read, read_memory.clone(), &|| {
            if let Err(error) = (checkpoint.borrow_mut())() {
                *checkpoint_error.borrow_mut() = Some(error);
                return Err(Error::Other("reader checkpoint refused".into()));
            }
            Ok(())
        });
        if let Some(error) = checkpoint_error.into_inner() {
            return Err(error);
        }
        gate.map_err(E::from)?;
        drop(checkpoint);
        let controls = std::mem::size_of::<
            Option<(
                VersionedRow,
                Arc<crate::read_image_memory::ImageMemory>,
                contextdb_core::read_memory::ReadCredit,
            )>,
        >()
        .checked_add(std::mem::size_of::<ReadHistoryProofOwner>())
        .and_then(|n| {
            n.checked_add(std::mem::size_of::<
                std::cell::RefCell<&mut dyn FnMut() -> std::result::Result<(), E>>,
            >())
        })
        .ok_or_else(|| {
            E::from(self.content_identity_corruption("version cursor layout overflow"))
        })?;
        let mut credit = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            controls,
            "reader",
            "version_group_controls",
            "Raise the declared reader memory limit.",
        )
        .map_err(E::from)?;
        credit
            .try_grow_for(
                72usize
                    .checked_add(table.len())
                    .and_then(|bytes| bytes.checked_mul(3))
                    .and_then(|bytes| bytes.checked_add(table.len().checked_add(4)?))
                    .ok_or_else(|| {
                        E::from(self.content_identity_corruption("version key length overflow"))
                    })?,
                "reader",
                "version_group_bounds",
                "Raise the declared reader memory limit.",
            )
            .map_err(E::from)?;
        let mut prefix = content_identity::checkpoint_operation(before_touch, |checkpoint| {
            manifest::prefix_with_checkpoint(table, checkpoint)
        })?;
        prefix.extend_from_slice(&row_id.0.to_be_bytes());
        let name = content_identity::checkpoint_operation(before_touch, |checkpoint| {
            manifest::relation_name_with_checkpoint(table, checkpoint)
        })?;
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        let relation = read.open_table(definition).map_err(|error| {
            E::from(match error {
                redb::TableError::TableDoesNotExist(_) => self.content_identity_corruption(
                    format!("relational table {table} is absent from the retained root"),
                ),
                other => RedbPersistence::relational_table_error(table, other),
            })
        })?;
        let (low, high) = RedbPersistence::rel_row_key_range(row_id);
        let mut physical = relation
            .range(low.as_slice()..high.as_slice())
            .map_err(|error| RedbPersistence::relational_storage_error(table, error))
            .map_err(E::from)?;
        let owner = ReadHistoryProofOwner::new(read_memory.clone()).map_err(E::from)?;
        let proof = content_identity::ProofMemory::growable(0, owner.clone());
        let touch = std::cell::RefCell::new(&mut *before_touch);
        let mut selected = None::<(
            VersionedRow,
            Arc<crate::read_image_memory::ImageMemory>,
            contextdb_core::read_memory::ReadCredit,
        )>;
        let mut previous_identity = None::<[u8; 24]>;
        let complete = self.content_identity.visit_history_from::<E>(
            content_identity::HistoryKind::RelationalVersion,
            std::ops::Bound::Included(content_identity::checkpoint_operation(
                &mut || (touch.borrow_mut())(),
                |checkpoint| manifest::copy_with_checkpoint(&prefix, checkpoint),
            )?),
            false,
            &proof,
            &mut || (touch.borrow_mut())(),
            &mut |coordinate, marker| {
                let witness = content_identity::checkpoint_operation(
                    &mut || (touch.borrow_mut())(),
                    |checkpoint| {
                        manifest::parse_manifest_with_checkpoint(coordinate, checkpoint)
                            .map_err(|e| self.content_identity_error(e))
                    },
                )?;
                if !content_identity::checkpoint_operation(
                    &mut || (touch.borrow_mut())(),
                    |checkpoint| manifest::starts_with_checkpoint(coordinate, &prefix, checkpoint),
                )? {
                    return Ok(false);
                }
                if previous_identity.is_some_and(|prior| prior >= witness.identity) {
                    return Err(E::from(self.content_identity_corruption(
                        "duplicate or reordered physical version manifest",
                    )));
                }
                previous_identity = Some(witness.identity);
                if marker != manifest::manifest_marker_hash() {
                    return Err(E::from(
                        self.content_identity_corruption("invalid version manifest marker"),
                    ));
                }
                (touch.borrow_mut())()?;
                let Some((key, body)) = physical
                    .next()
                    .transpose()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                    .map_err(E::from)?
                else {
                    return Err(E::from(self.content_identity_corruption(
                        "rooted relational version is physically missing",
                    )));
                };
                #[cfg(feature = "test-seams")]
                crate::writer_query_test_seams::note_target_entry(table);
                if key.value() != witness.identity {
                    return Err(E::from(self.content_identity_corruption(
                        "extra, swapped or reordered physical relational version",
                    )));
                }
                (touch.borrow_mut())()?;
                let mut admission =
                    content_identity::ProofAdmission::for_reader(read_memory.clone());
                let mut checkpoint_failure = None;
                let actual = manifest::manifest_for_body_with_checkpoint(
                    table,
                    key.value(),
                    body.value(),
                    &mut admission,
                    &mut || match (touch.borrow_mut())() {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            checkpoint_failure = Some(error);
                            Err(Error::Other("reader checkpoint refused".into()))
                        }
                    },
                );
                if let Some(error) = checkpoint_failure {
                    return Err(error);
                }
                if let Some(error) = admission.take_reader_refusal() {
                    return Err(E::from(error));
                }
                let actual = actual.map_err(|e| E::from(self.content_identity_error(e)))?;
                if !content_identity::checkpoint_operation(
                    &mut || (touch.borrow_mut())(),
                    |checkpoint| manifest::equal_with_checkpoint(&actual, coordinate, checkpoint),
                )? {
                    return Err(E::from(self.content_identity_corruption(
                        "relational persisted body or closure does not authenticate",
                    )));
                }
                let created = TxId(u64::from_be_bytes(
                    witness.identity[8..16].try_into().expect("identity"),
                ));
                let lsn = Lsn(u64::from_be_bytes(
                    witness.identity[16..].try_into().expect("identity"),
                ));
                self.authenticate_version_bounds(table, lsn, created, witness.deleted_tx)
                    .map_err(E::from)?;
                let eligible = exact_lsn.map_or(created.0 <= snapshot.0, |wanted| {
                    wanted == lsn && selected.is_none()
                });
                if eligible {
                    let decode_controls = contextdb_core::read_memory::ReadCredit::try_new(
                        read_memory.clone(),
                        std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                            + 2 * std::mem::size_of::<usize>(),
                        "reader",
                        "version_decode_owner",
                        "Raise the declared reader memory limit.",
                    )
                    .map_err(E::from)?;
                    let memory = crate::read_image_memory::ImageMemory::charging_reader(
                        self._source_memory.memory.accountant.clone(),
                        read_memory.clone(),
                    );
                    let row = memory
                        .scope(|| {
                            RedbPersistence::decode_relational_source_row(table, meta, body.value())
                        })
                        .map_err(E::from)?;
                    selected = Some((row, memory, decode_controls));
                }
                Ok(true)
            },
        )?;
        if let Some(error) = owner.refusal.lock().take() {
            return Err(E::from(error));
        }
        if !complete {
            return Err(E::from(self.unprovable_history(
                &self._source_memory.memory.accountant,
                content_identity::HistoryKind::RelationalVersion,
            )));
        }
        (touch.borrow_mut())()?;
        if physical
            .next()
            .transpose()
            .map_err(|error| RedbPersistence::relational_storage_error(table, error))
            .map_err(E::from)?
            .is_some()
        {
            return Err(E::from(self.content_identity_corruption(
                "unwitnessed relational group suffix",
            )));
        }
        drop(touch);
        // Per-version observations are deferred until the whole group and its
        // upper boundary have reconciled. Re-read the immutable capture.
        if let Some(observed) = observed.as_mut() {
            let mut second = relation
                .range(low.as_slice()..high.as_slice())
                .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                .map_err(E::from)?;
            loop {
                before_touch()?;
                let Some((_, body)) = second
                    .next()
                    .transpose()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                    .map_err(E::from)?
                else {
                    break;
                };
                let _decode_controls = contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                        + 2 * std::mem::size_of::<usize>(),
                    "reader",
                    "version_observed_owner",
                    "Raise the declared reader memory limit.",
                )
                .map_err(E::from)?;
                let memory = crate::read_image_memory::ImageMemory::charging_reader(
                    self._source_memory.memory.accountant.clone(),
                    read_memory.clone(),
                );
                let row = memory
                    .scope(|| {
                        RedbPersistence::decode_relational_source_row(table, meta, body.value())
                    })
                    .map_err(E::from)?;
                observed(&row)?;
            }
        }
        *last_proved = previous_identity;
        if let Some((row, memory, _decode_controls)) =
            selected.filter(|(row, _, _)| exact_lsn.is_some() || row.visible_at(snapshot))
        {
            if previous_identity
                == Some(
                    RedbPersistence::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn)
                        .as_slice()
                        .try_into()
                        .expect("physical identity"),
                )
                && row.deleted_tx.is_none()
            {
                let mut checkpoint_failure = None;
                let result = self.content_identity.verify_current_row_projection(
                    table,
                    meta,
                    &row,
                    read_memory.clone(),
                    &mut || match before_touch() {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            checkpoint_failure = Some(error);
                            Err(Error::Other("reader checkpoint refused".into()))
                        }
                    },
                );
                if let Some(error) = checkpoint_failure {
                    return Err(error);
                }
                result.map_err(|e| E::from(self.content_identity_error(e)))?;
            }
            before_clone(crate::read_image_memory::RowReadAdmission::Move(&memory))?;
            drop(memory);
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

impl CheckpointReadSource {
    /// Export validates the complete captured table before any version is
    /// filtered or copied. A directory-driven selection cannot hide a gap.
    pub(crate) fn verify_relational_version_manifests(
        &self,
        table: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        checkpoint()?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            content_identity::history_open_control::OPEN_CONTROL_SCRATCH_BYTES
                + content_identity::history_open_control::DIRECTORY_CHECK_SCRATCH_BYTES,
            "reader",
            "export_version_controls",
            "Raise the declared reader memory limit.",
        )?;
        let bounds = RedbPersistence::read_authenticated_bounds_in_read(&self.read)?;
        let control =
            content_identity::history_open_control::acquire(&self.read, bounds.commit_count)?;
        checkpoint()?;
        control.check_directory(&self.read)?;
        let _key_credit =
            contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                3usize
                    .checked_mul(table.len().checked_add(72).ok_or_else(|| {
                        self.content_identity_corruption("manifest bound overflow")
                    })?)
                    .ok_or_else(|| self.content_identity_corruption("manifest bound overflow"))?,
                "reader",
                "export_version_bounds",
                "Raise the declared reader memory limit.",
            )?;
        let prefix = manifest::prefix_with_checkpoint(table, checkpoint)?;
        let name = manifest::relation_name_with_checkpoint(table, checkpoint)?;
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        let relation = self
            .read
            .open_table(definition)
            .map_err(|error| RedbPersistence::relational_table_error(table, error))?;
        let mut physical = relation
            .iter()
            .map_err(|error| RedbPersistence::relational_storage_error(table, error))?;
        let owner = ReadHistoryProofOwner::new(read_memory.clone())?;
        let proof = content_identity::ProofMemory::growable(0, owner.clone());
        let mut previous = None::<[u8; 24]>;
        let touch = std::cell::RefCell::new(&mut *checkpoint);
        let complete = self.content_identity.visit_history_from::<Error>(
            content_identity::HistoryKind::RelationalVersion,
            std::ops::Bound::Included(content_identity::checkpoint_operation(
                &mut || (touch.borrow_mut())(),
                |checkpoint| manifest::copy_with_checkpoint(&prefix, checkpoint),
            )?),
            false,
            &proof,
            &mut || (touch.borrow_mut())(),
            &mut |coordinate, marker| {
                let witness = content_identity::checkpoint_operation(
                    &mut || (touch.borrow_mut())(),
                    |checkpoint| {
                        manifest::parse_manifest_with_checkpoint(coordinate, checkpoint)
                            .map_err(|e| self.content_identity_error(e))
                    },
                )?;
                if !content_identity::checkpoint_operation(
                    &mut || (touch.borrow_mut())(),
                    |checkpoint| manifest::starts_with_checkpoint(coordinate, &prefix, checkpoint),
                )? {
                    return Ok(false);
                }
                if marker != manifest::manifest_marker_hash()
                    || previous.is_some_and(|old| old >= witness.identity)
                {
                    return Err(self.content_identity_corruption(
                        "export manifest duplicate, order or marker mismatch",
                    ));
                }
                previous = Some(witness.identity);
                (touch.borrow_mut())()?;
                let Some((key, body)) = physical
                    .next()
                    .transpose()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))?
                else {
                    return Err(self.content_identity_corruption(
                        "export manifest member is physically missing",
                    ));
                };
                if key.value() != witness.identity {
                    return Err(self
                        .content_identity_corruption("export physical gap or unexpected version"));
                }
                let mut admission =
                    content_identity::ProofAdmission::for_reader(read_memory.clone());
                let mut checkpoint_failure = None;
                let actual = manifest::manifest_for_body_with_checkpoint(
                    table,
                    key.value(),
                    body.value(),
                    &mut admission,
                    &mut || match (touch.borrow_mut())() {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            checkpoint_failure = Some(error);
                            Err(Error::Other("export checkpoint refused".into()))
                        }
                    },
                );
                if let Some(error) = checkpoint_failure {
                    return Err(error);
                }
                if let Some(error) = admission.take_reader_refusal() {
                    return Err(error);
                }
                let actual = actual.map_err(|e| self.content_identity_error(e))?;
                if !manifest::equal_with_checkpoint(&actual, coordinate, &mut || {
                    (touch.borrow_mut())()
                })? {
                    return Err(self.content_identity_corruption(
                        "export row body or closure does not authenticate",
                    ));
                }
                Ok(true)
            },
        )?;
        if let Some(error) = owner.refusal.lock().take() {
            return Err(error);
        }
        if !complete {
            return Err(self.content_identity_corruption("export proof range did not complete"));
        }
        (touch.borrow_mut())()?;
        if physical
            .next()
            .transpose()
            .map_err(|error| RedbPersistence::relational_storage_error(table, error))?
            .is_some()
        {
            return Err(self.content_identity_corruption("export table has an unwitnessed suffix"));
        }
        Ok(())
    }
}

impl RedbPersistence {
    /// The mutation owns this collection until the same transaction finishes.
    /// Its decoder and vector slots reserve against the existing writer owner.
    pub(super) fn collect_relational_rows_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        memory: &Arc<crate::read_image_memory::ImageMemory>,
    ) -> Result<Vec<VersionedRow>> {
        memory.scope(|| {
            memory.reserve(
                table
                    .len()
                    .checked_add(4)
                    .ok_or_else(|| self.relational_manifest_damage("table name overflow"))?,
            )?;
            let name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&name);
            let relation = write.open_table(definition).map_err(Self::storage_error)?;
            let mut bounds = self.proof_admission();
            bounds.admit(
                table
                    .len()
                    .checked_add(72)
                    .and_then(|n| n.checked_mul(2))
                    .ok_or_else(|| self.relational_manifest_damage("collection bound overflow"))?,
            )?;
            let prefix = manifest::manifest_prefix(table)?;
            let mut after = prefix.clone();
            let mut exclusive = false;
            let mut physical = relation.iter().map_err(Self::storage_error)?;
            let mut rows = Vec::new();
            loop {
                let mut proof = self.proof_admission();
                let next = manifest::writer_manifest_next(write, &after, exclusive, &mut proof)
                    .map_err(|e| self.relational_manifest_error(e))?;
                let Some(coordinate) = next.filter(|key| key.starts_with(&prefix)) else {
                    if physical
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .is_some()
                    {
                        return Err(
                            self.relational_manifest_damage("unwitnessed mutation row suffix")
                        );
                    }
                    return Ok(rows);
                };
                let witness = manifest::parse_manifest(&coordinate)
                    .map_err(|e| self.relational_manifest_error(e))?;
                let Some((key, body)) = physical.next().transpose().map_err(Self::storage_error)?
                else {
                    return Err(self.relational_manifest_damage("mutation row member is missing"));
                };
                if key.value() != witness.identity {
                    return Err(
                        self.relational_manifest_damage("mutation row membership order differs")
                    );
                }
                let actual =
                    manifest::manifest_for_body(table, key.value(), body.value(), &mut proof)
                        .map_err(|e| self.relational_manifest_error(e))?;
                if actual != coordinate {
                    return Err(self.relational_manifest_damage("mutation row body differs"));
                }
                if rows.len() == rows.capacity() {
                    let old_capacity = rows.capacity();
                    let capacity =
                        old_capacity
                            .checked_mul(2)
                            .map(|n| n.max(4))
                            .ok_or_else(|| {
                                self.relational_manifest_damage("collection capacity overflow")
                            })?;
                    let bytes = capacity
                        .checked_mul(std::mem::size_of::<VersionedRow>())
                        .ok_or_else(|| {
                            self.relational_manifest_damage("collection layout overflow")
                        })?;
                    memory.reserve(bytes)?;
                    rows.try_reserve_exact(capacity - rows.len())
                        .map_err(|e| Error::Other(format!("row collection allocation: {e}")))?;
                    memory.release(old_capacity * std::mem::size_of::<VersionedRow>());
                }
                rows.push(Self::decode_versioned_row(body.value(), None)?);
                after = coordinate;
                exclusive = true;
            }
        })
    }

    /// Predecessor selection is complete with respect to the rooted version
    /// universe, so a missing newer physical version cannot select an older
    /// authentic version for closure.
    pub(super) fn verify_relational_group_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        row_id: RowId,
        relation: &impl ReadableTable<&'static [u8], &'static [u8]>,
    ) -> Result<()> {
        self.verify_relational_group_pending_in_write(
            write,
            table,
            row_id,
            relation,
            &[] as &[content_identity::ContentIdentityMutation],
        )
    }

    pub(super) fn verify_relational_group_pending_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        row_id: RowId,
        relation: &impl ReadableTable<&'static [u8], &'static [u8]>,
        pending: &(impl manifest::ManifestOverlay + ?Sized),
    ) -> Result<()> {
        let mut admission = self.proof_admission();
        admission.admit(
            table
                .len()
                .checked_add(72)
                .and_then(|n| n.checked_mul(3))
                .ok_or_else(|| self.relational_manifest_damage("writer group bound overflow"))?,
        )?;
        let mut prefix = manifest::manifest_prefix(table)?;
        prefix.extend_from_slice(&row_id.0.to_be_bytes());
        let mut after = prefix.clone();
        let mut exclusive = false;
        let (low, high) = Self::rel_row_key_range(row_id);
        let mut physical = relation
            .range(low.as_slice()..high.as_slice())
            .map_err(Self::storage_error)?;
        let mut previous = None;
        loop {
            let mut window = self.proof_admission();
            let next = manifest::writer_manifest_next_pending(
                write,
                &after,
                exclusive,
                &mut window,
                pending,
            )
            .map_err(|e| self.relational_manifest_error(e))?;
            let Some(coordinate) = next.filter(|key| key.starts_with(&prefix)) else {
                if physical
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .is_some()
                {
                    return Err(self.relational_manifest_damage("unwitnessed writer group suffix"));
                }
                return Ok(());
            };
            let witness = manifest::parse_manifest(&coordinate)
                .map_err(|e| self.relational_manifest_error(e))?;
            if previous.is_some_and(|old| old >= witness.identity) {
                return Err(
                    self.relational_manifest_damage("duplicate or reordered writer version")
                );
            }
            previous = Some(witness.identity);
            let Some((key, body)) = physical.next().transpose().map_err(Self::storage_error)?
            else {
                return Err(
                    self.relational_manifest_damage("writer group version is physically missing")
                );
            };
            if key.value() != witness.identity {
                return Err(
                    self.relational_manifest_damage("writer group physical order disagrees")
                );
            }
            let actual = manifest::manifest_for_body(table, key.value(), body.value(), &mut window)
                .map_err(|e| self.relational_manifest_error(e))?;
            if actual != coordinate {
                return Err(self.relational_manifest_damage(
                    "writer group body or closure is unauthenticated",
                ));
            }
            after = coordinate;
            exclusive = true;
        }
    }
}

impl RetainedReadSource {
    pub(crate) fn historical_index_preflight<E: From<Error>>(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if snapshot == self.logical_frontier {
            return Ok(());
        }
        let mut refused = None;
        let result = self.historical_index_preflight_core(
            table,
            meta,
            declaration,
            snapshot,
            read_memory,
            &mut || {
                before().map_err(|error| {
                    refused = Some(error);
                    Error::ReadCancelled
                })
            },
        );
        match refused {
            Some(error) => Err(error),
            None => result.map_err(E::from),
        }
    }

    fn historical_index_preflight_core(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        if snapshot.0 > self.logical_frontier.0 {
            return Err(Error::Other(
                "index snapshot exceeds captured frontier".into(),
            ));
        }
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory.clone(),
        );
        let _state = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            std::mem::size_of::<(
                u64,
                u64,
                contextdb_relational::mem::BoundedPhysicalCursor,
                Option<Vec<u8>>,
            )>(),
            "reader",
            "index_preflight_controls",
            "Raise the declared reader memory limit.",
        )?;
        graph_read::with_pages(read_memory.clone(), || {
            memory.scope(|| {
                before()?;
                let read = self.begin_read()?;
                // The existing name codec emits twelve ASCII prefix bytes and
                // sixty-four hex bytes; its hasher is fixed stack scratch.
                memory.reserve(
                    76usize
                        .checked_add(std::mem::size_of::<blake3::Hasher>())
                        .ok_or_else(|| {
                            self.content_identity_corruption("index table name overflow")
                        })?,
                )?;
                let name = RedbPersistence::relational_index_table_name(table, &declaration.name);
                let index = read
                    .open_table(TableDefinition::<BorrowedRelationalIndexKey, &[u8]>::new(
                        &name,
                    ))
                    .map_err(|error| RedbPersistence::relational_table_error(table, error))?;
                let mut rows = contextdb_relational::mem::BoundedPhysicalCursor::default();
                let mut expected = 0u64;
                loop {
                    let mut row_credit =
                        contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                    let row = self.bounded_relational_physical_next_admitted(
                        table,
                        meta,
                        snapshot,
                        &mut rows,
                        before,
                        &mut |_| Ok(()),
                        &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                            admission.retain_into(
                                &mut row_credit,
                                "reader",
                                "index_preflight_row",
                                "Raise the declared reader memory limit.",
                            )
                        },
                        read_memory.clone(),
                    )?;
                    let Some(row) = row else {
                        break;
                    };
                    before()?;
                    let endpoint_memory = crate::read_image_memory::ImageMemory::charging_reader(
                        self._source_memory.memory.accountant.clone(),
                        read_memory.clone(),
                    );
                    let low = DurableRelationalIndexKey(
                        admitted_relational_row_index_endpoint_with_checkpoint(
                            declaration,
                            &row,
                            TxId(0),
                            &endpoint_memory,
                            before,
                        )?,
                    );
                    before()?;
                    let high = DurableRelationalIndexKey(
                        admitted_relational_row_index_endpoint_with_checkpoint(
                            declaration,
                            &row,
                            TxId(u64::MAX),
                            &endpoint_memory,
                            before,
                        )?,
                    );
                    let mut count = 0u64;
                    for posting in index
                        .range::<&[u8]>(low.0.as_slice()..=high.0.as_slice())
                        .map_err(|error| RedbPersistence::relational_storage_error(table, error))?
                    {
                        before()?;
                        let decode = crate::read_image_memory::ImageMemory::charging_reader(
                            self._source_memory.memory.accountant.clone(),
                            read_memory.clone(),
                        );
                        let (key_guard, value_guard) = posting.map_err(|error| {
                            RedbPersistence::relational_storage_error(table, error)
                        })?;
                        let encoded = key_guard.value();
                        let (position, deleted) = self.decode_preflight_posting(
                            encoded,
                            value_guard.value(),
                            &decode,
                            before,
                        )?;
                        RedbPersistence::validated_index_position(declaration, meta, &position)
                            .map_err(|reason| self.content_identity_corruption(reason))?;
                        if position.created_tx.0 > self.logical_frontier.0
                            || deleted.is_some_and(|tx| {
                                tx <= position.created_tx || tx.0 > self.logical_frontier.0
                            })
                        {
                            return Err(self.content_identity_corruption(
                                "historical posting lifecycle exceeds capture",
                            ));
                        }
                        if position.created_tx.0 <= snapshot.0
                            && deleted.is_none_or(|tx| tx.0 > snapshot.0)
                        {
                            before()?;
                            let canonical = admitted_relational_row_index_endpoint_with_checkpoint(
                                declaration,
                                &row,
                                position.created_tx,
                                &decode,
                                before,
                            )?;
                            if position.row_id != row.row_id
                                || !manifest::equal_with_checkpoint(&canonical, encoded, before)?
                            {
                                return Err(self.content_identity_corruption(
                                    "historical posting differs from authenticated row key",
                                ));
                            }
                            count = count.checked_add(1).ok_or_else(|| {
                                self.content_identity_corruption("posting count overflow")
                            })?;
                        }
                    }
                    if count != 1 {
                        return Err(self.content_identity_corruption(
                            "visible row lacks exactly one historical posting",
                        ));
                    }
                    expected = expected.checked_add(1).ok_or_else(|| {
                        self.content_identity_corruption("visible row count overflow")
                    })?;
                }
                let mut previous = None::<(Vec<u8>, contextdb_core::read_memory::ReadCredit)>;
                let mut actual = 0u64;
                for posting in index
                    .iter()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))?
                {
                    before()?;
                    let decode = crate::read_image_memory::ImageMemory::charging_reader(
                        self._source_memory.memory.accountant.clone(),
                        read_memory.clone(),
                    );
                    let (key_guard, value_guard) = posting
                        .map_err(|error| RedbPersistence::relational_storage_error(table, error))?;
                    let encoded = key_guard.value();
                    let (position, deleted) = self.decode_preflight_posting(
                        encoded,
                        value_guard.value(),
                        &decode,
                        before,
                    )?;
                    RedbPersistence::validated_index_position(declaration, meta, &position)
                        .map_err(|reason| self.content_identity_corruption(reason))?;
                    if position.created_tx.0 > self.logical_frontier.0
                        || deleted.is_some_and(|tx| {
                            tx <= position.created_tx || tx.0 > self.logical_frontier.0
                        })
                    {
                        return Err(self.content_identity_corruption(
                            "historical posting lifecycle exceeds capture",
                        ));
                    }
                    if let Some((prior, _)) = &previous
                        && content_identity::validated_preflight_posting_order(
                            prior, encoded, before,
                        )
                        .map_err(|error| match error {
                            Error::ReadCancelled
                            | Error::ReadFailure(_)
                            | Error::MemoryBudgetExceeded { .. } => error,
                            other => self.content_identity_error(other),
                        })? != std::cmp::Ordering::Less
                    {
                        return Err(self.content_identity_corruption(
                            "duplicate or reordered historical posting",
                        ));
                    }
                    let previous_credit = contextdb_core::read_memory::ReadCredit::try_new(
                        read_memory.clone(),
                        encoded.len(),
                        "reader",
                        "index_preflight_previous",
                        "Raise the declared reader memory limit.",
                    )?;
                    previous = Some((encoded.to_vec(), previous_credit));
                    let mut row_credit =
                        contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                    let mut last = None;
                    let row = self.reconcile_relational_group(
                        &read,
                        table,
                        meta,
                        position.row_id,
                        snapshot,
                        None,
                        read_memory.clone(),
                        before,
                        None,
                        &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                            admission.retain_into(
                                &mut row_credit,
                                "reader",
                                "index_preflight_row",
                                "Raise the declared reader memory limit.",
                            )
                        },
                        &mut last,
                    )?;
                    if position.created_tx.0 <= snapshot.0
                        && deleted.is_none_or(|tx| tx.0 > snapshot.0)
                    {
                        let row = row.ok_or_else(|| {
                            self.content_identity_corruption(
                                "visible historical posting has no authenticated visible row",
                            )
                        })?;
                        before()?;
                        let canonical = admitted_relational_row_index_endpoint_with_checkpoint(
                            declaration,
                            &row,
                            position.created_tx,
                            &decode,
                            before,
                        )?;
                        if !manifest::equal_with_checkpoint(&canonical, encoded, before)? {
                            return Err(self.content_identity_corruption(
                                "extra or moved historical visible candidate",
                            ));
                        }
                        actual = actual.checked_add(1).ok_or_else(|| {
                            self.content_identity_corruption("visible posting count overflow")
                        })?;
                    }
                }
                if actual != expected {
                    return Err(
                        self.content_identity_corruption("historical candidate counts differ")
                    );
                }
                before()?;
                Ok(())
            })
        })
    }
}

impl RetainedReadSource {
    fn decode_preflight_posting(
        &self,
        key: &[u8],
        value: &[u8],
        memory: &Arc<crate::read_image_memory::ImageMemory>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<(RelationalIndexPosition, Option<TxId>)> {
        memory.scope(|| {
            before()?;
            let (encoded, consumed): (EncodedRelationalIndexPosition, usize) =
                crate::read_image_memory::decode(key)
                    .map_err(|error| self.content_identity_error(error))?;
            if consumed != key.len() {
                return Err(
                    self.content_identity_corruption("historical posting has trailing bytes")
                );
            }
            before()?;
            memory.reserve(
                encoded
                    .key
                    .len()
                    .checked_mul(std::mem::size_of::<DirectedValue>())
                    .ok_or_else(|| self.content_identity_corruption("posting layout overflow"))?,
            )?;
            let position: RelationalIndexPosition = encoded.into();
            let canonical = admitted_relational_index_endpoint_with_checkpoint(
                &position.key,
                position.row_id,
                position.created_tx,
                memory,
                before,
            )?;
            if !manifest::equal_with_checkpoint(&canonical, key, before)? {
                return Err(self.content_identity_corruption("historical posting is not canonical"));
            }
            let deleted = RedbPersistence::decode_exact::<Option<TxId>>(value)
                .map_err(|error| self.content_identity_error(error))?;
            Ok((position, deleted))
        })
    }
}

/// Publication-ordered manifest deltas plus a height-balanced search tree over
/// their positions, ordered by (coordinate, position). Delta `i` is tree node
/// `i`, so a node holds two child positions and a height, never a copied
/// coordinate. Indexing a delta and every overlay seek descend the tree once:
/// O(log n) coordinate comparisons instead of a rescan of every delta the rows
/// before it staged.
pub(super) struct PendingManifestMutations {
    index: position_tree::PositionTree,
    list: Vec<content_identity::ContentIdentityMutation>,
}
impl std::ops::Deref for PendingManifestMutations {
    type Target = [content_identity::ContentIdentityMutation];
    fn deref(&self) -> &Self::Target {
        &self.list
    }
}
impl PendingManifestMutations {
    fn coordinate(&self, position: u32) -> &[u8] {
        manifest::overlay_coordinate(&self.list[position as usize])
    }

    /// Index the delta just pushed at `position` (the last one). Its node's
    /// storage was admitted and reserved before the delta was pushed. A later
    /// delta at an equal coordinate sorts after every earlier one.
    fn index_last(&mut self, position: u32) {
        debug_assert_eq!(position as usize + 1, self.list.len());
        let coordinate = self.coordinate(position);
        let vacancy = self
            .index
            .search(|node| {
                #[cfg(any(test, feature = "test-seams"))]
                manifest::commit_row_work::count_overlay_comparisons(1);
                if coordinate < self.coordinate(node) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            })
            .expect_err("an index of positions never finds the position being added");
        let steps = self.index.insert_at(vacancy, position);
        #[cfg(any(test, feature = "test-seams"))]
        manifest::commit_row_work::count_overlay_comparisons(steps);
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = steps;
    }

    /// The position of the latest delta at `coordinate`: the largest
    /// (coordinate, position) not after it, when its coordinate is equal.
    fn latest_position(&self, coordinate: &[u8]) -> Option<u32> {
        let found = self.index.last_where(|node| {
            #[cfg(any(test, feature = "test-seams"))]
            manifest::commit_row_work::count_overlay_comparisons(1);
            self.coordinate(node) <= coordinate
        });
        #[cfg(any(test, feature = "test-seams"))]
        manifest::commit_row_work::count_overlay_comparisons(1);
        found.filter(|&position| self.coordinate(position) == coordinate)
    }

    /// The smallest pending coordinate after `bound` (at it when inclusive).
    fn next_coordinate(&self, bound: &[u8], exclusive: bool) -> Option<&[u8]> {
        self.index
            .first_where(|node| {
                #[cfg(any(test, feature = "test-seams"))]
                manifest::commit_row_work::count_overlay_comparisons(1);
                let key = self.coordinate(node);
                if exclusive { key > bound } else { key >= bound }
            })
            .map(|position| self.coordinate(position))
    }
}
impl manifest::ManifestOverlay for PendingManifestMutations {
    fn latest_present(&self, coordinate: &[u8]) -> Option<bool> {
        self.latest_position(coordinate)
            .map(|position| self.list[position as usize].logical_value.is_some())
    }

    fn first_present_from(&self, start: &[u8], exclusive: bool) -> Option<&[u8]> {
        let (mut bound, mut exclusive) = (start, exclusive);
        loop {
            let candidate = self.next_coordinate(bound, exclusive)?;
            if self
                .latest_position(candidate)
                .is_some_and(|position| self.list[position as usize].logical_value.is_some())
            {
                return Some(candidate);
            }
            (bound, exclusive) = (candidate, true);
        }
    }
}

/// Finite writer-funded publication deltas. Field order frees every mutation
/// and the native container before its original proof invoices are returned.
pub(super) struct PendingManifestPublication {
    pub(super) mutations: PendingManifestMutations,
    admission: content_identity::OwnedProofAdmission,
}
impl PendingManifestPublication {
    pub(super) fn new(admission: content_identity::OwnedProofAdmission) -> Self {
        Self {
            mutations: PendingManifestMutations {
                index: position_tree::PositionTree::new(),
                list: Vec::new(),
            },
            admission,
        }
    }
    fn push(&mut self, coordinate: &[u8], present: bool) -> Result<()> {
        let list = &mut self.mutations.list;
        if list.len() == list.capacity() {
            let old = list.capacity();
            let capacity = old
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| Error::Other("manifest publication capacity overflow".into()))?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<content_identity::ContentIdentityMutation>())
                .ok_or_else(|| Error::Other("manifest publication layout overflow".into()))?;
            self.admission.admit(bytes)?;
            let mut next = Vec::with_capacity(capacity);
            next.extend(std::mem::take(list));
            *list = next;
            self.admission
                .release(old * std::mem::size_of::<content_identity::ContentIdentityMutation>());
        }
        let index = &mut self.mutations.index;
        if index.len() == index.capacity() {
            let node = position_tree::PositionTree::NODE_BYTES;
            let old = index.capacity();
            let capacity = old
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| Error::Other("manifest publication capacity overflow".into()))?;
            let bytes = capacity
                .checked_mul(node)
                .ok_or_else(|| Error::Other("manifest publication layout overflow".into()))?;
            self.admission.admit(bytes)?;
            index.grow(capacity);
            self.admission.release(old * node);
        }
        let position = u32::try_from(self.mutations.list.len())
            .ok()
            .filter(|position| *position != position_tree::NO_POSITION)
            .ok_or_else(|| Error::Other("manifest publication position overflow".into()))?;
        let bytes = manifest::manifest_mutation_bytes(coordinate, present)?;
        self.admission.admit(bytes)?;
        let mutation = manifest::manifest_mutation(coordinate, present)?;
        debug_assert_eq!(
            mutation.logical_key.capacity()
                + mutation.logical_value.as_ref().map_or(0, Vec::capacity),
            bytes
        );
        self.mutations.list.push(mutation);
        self.mutations.index_last(position);
        Ok(())
    }
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(crate) fn visit_committed_change_and_ddl_members_for_test(
        &self,
        tx: TxId,
        tables: &[&str],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        accountant: crate::memory_accounting::AccountantHandle,
        mut visitor: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let owner = writer_source::SourceWindowOwner::Read(read_memory.clone());
        let _controls = owner.reserve(
            std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                + std::mem::size_of::<redb::ReadTransaction>()
                + 3 * std::mem::size_of::<redb::ReadOnlyTable<&str, &[u8]>>()
                + 3 * std::mem::size_of::<redb::Range<'_, &str, &[u8]>>()
                + 2 * std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                + 512,
            "committed_member_controls",
        )?;
        graph_read::with_pages(read_memory.clone(), || {
            self.with_db(|db| {
                let read = db.begin_read().map_err(Self::storage_error)?;
                let decode = crate::read_image_memory::ImageMemory::charging_reader_for(
                    accountant.clone(),
                    read_memory.clone(),
                    "history_source",
                    "committed_member_controls",
                    "Raise the declared reader memory limit.",
                );
                decode.scope(|| {
                    let captured = Self::read_authenticated_bounds_in_read(&read)?;
                    let verifier = content_identity::ContentIdentityReadVerifier::open(
                        &read,
                        Path::new("committed member diagnostic"),
                        None,
                    )?;
                    let commits = read
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut committed_lsn = None;
                    for entry in commits.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        if value.value() == tx.0 {
                            if committed_lsn.is_some() {
                                return Err(
                                    verifier.corrupt("transaction has duplicate committed LSNs")
                                );
                            }
                            let _key_credit = owner.reserve(128, "committed_member_key")?;
                            let mutation =
                                content_identity::commit_index_mutation(Lsn(key.value()), tx);
                            verifier.verify_with_read_memory(&mutation, read_memory.clone())?;
                            visitor(&mutation.logical_key)?;
                            committed_lsn = Some(Lsn(key.value()));
                        }
                    }
                    let lsn = committed_lsn.ok_or_else(|| {
                        verifier.corrupt("requested transaction is not committed")
                    })?;
                    let proof_owner = ReadHistoryProofOwner::new(read_memory.clone())?;
                    let proof = content_identity::ProofMemory::growable(0, proof_owner.clone());
                    for (kind, definition) in [
                        (content_identity::HistoryKind::ChangeLog, CHANGE_LOG_TABLE),
                        (content_identity::HistoryKind::DdlLog, DDL_LOG_TABLE),
                    ] {
                        let table = match read.open_table(definition) {
                            Ok(table) => Some(table),
                            Err(redb::TableError::TableDoesNotExist(_)) => None,
                            Err(error) => return Err(Self::storage_error(error)),
                        };
                        let mut physical = table
                            .as_ref()
                            .map(|table| table.iter())
                            .transpose()
                            .map_err(Self::storage_error)?;
                        let complete = verifier.visit_history::<Error>(
                            kind,
                            false,
                            &proof,
                            &mut || Ok(()),
                            &mut |coordinate, expected| {
                                let (key, body) = physical
                                    .as_mut()
                                    .and_then(|range| range.next())
                                    .transpose()
                                    .map_err(Self::storage_error)?
                                    .ok_or_else(|| {
                                        verifier.corrupt(
                                            "committed history member is physically missing",
                                        )
                                    })?;
                                if key.value().as_bytes() != coordinate {
                                    return Err(
                                        verifier.corrupt("committed history physical key mismatch")
                                    );
                                }
                                let _window = owner.reserve(
                                    std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                                        + 64,
                                    "committed_member_decode",
                                )?;
                                let scratch =
                                    crate::read_image_memory::ImageMemory::charging_reader_for(
                                        accountant.clone(),
                                        read_memory.clone(),
                                        "history_source",
                                        "committed_member",
                                        "Raise the declared reader memory limit.",
                                    );
                                scratch.scope(|| {
                                    let (actual, member_lsn) = if kind
                                        == content_identity::HistoryKind::ChangeLog
                                    {
                                        let record = Self::decode_stored_change_log(body.value())?;
                                        let actual =
                                            Self::stored_change_log_value_hash(body.value())?;
                                        (actual, record.entry.lsn())
                                    } else {
                                        let change: DdlChange = Self::decode(body.value())?;
                                        let actual =
                                            content_identity::history_record_value_hash(&change)?;
                                        let raw_lsn = key
                                            .value()
                                            .split_once(':')
                                            .map(|(lsn, _)| lsn)
                                            .unwrap_or(key.value());
                                        let member_lsn = raw_lsn
                                            .parse::<u64>()
                                            .map(Lsn)
                                            .map_err(|error| verifier.corrupt(error))?;
                                        (actual, member_lsn)
                                    };
                                    if actual != expected {
                                        return Err(verifier.corrupt(
                                            "committed history body does not authenticate",
                                        ));
                                    }
                                    if member_lsn == lsn {
                                        if kind == content_identity::HistoryKind::ChangeLog {
                                            graph_read::with_graph_proof_in_read(
                                                &read,
                                                None,
                                                &owner,
                                                scratch.clone(),
                                                |context, read, memory| {
                                                    context.verify_stored_graph_history(
                                                        read,
                                                        key.value(),
                                                        body.value(),
                                                        memory,
                                                    )
                                                },
                                            )?;
                                        }
                                        let mut admission =
                                            content_identity::ProofAdmission::for_reader(
                                                read_memory.clone(),
                                            );
                                        content_identity::visit_history_logical_key_for_test(
                                            kind,
                                            key.value().as_bytes(),
                                            &mut admission,
                                            &mut visitor,
                                        )?;
                                    }
                                    Ok(true)
                                })
                            },
                        )?;
                        if let Some(error) = proof_owner.refusal.lock().take() {
                            return Err(error);
                        }
                        if !complete
                            || physical
                                .as_mut()
                                .and_then(|range| range.next())
                                .transpose()
                                .map_err(Self::storage_error)?
                                .is_some()
                        {
                            return Err(
                                verifier.corrupt("committed history contains unwitnessed members")
                            );
                        }
                    }
                    for table in tables {
                        let mut bounds =
                            content_identity::ProofAdmission::for_reader(read_memory.clone());
                        bounds.admit(3 * (table.len() + 72) + table.len() + 4)?;
                        let prefix = manifest::manifest_prefix(table)?;
                        let name = Self::rel_table_name(table);
                        let relation = read
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
                            .map_err(|error| Self::relational_table_error(table, error))?;
                        let mut physical = relation
                            .iter()
                            .map_err(|error| Self::relational_storage_error(table, error))?;
                        let mut previous = None;
                        let complete = verifier.visit_history_from::<Error>(
                            content_identity::HistoryKind::RelationalVersion,
                            std::ops::Bound::Included(prefix.clone()),
                            false,
                            &proof,
                            &mut || Ok(()),
                            &mut |coordinate, marker| {
                                if !coordinate.starts_with(&prefix) {
                                    return Ok(false);
                                }
                                let witness = manifest::parse_manifest(coordinate)?;
                                if marker != manifest::manifest_marker_hash()
                                    || previous.is_some_and(|old| old >= witness.identity)
                                {
                                    return Err(verifier.corrupt(
                                        "version manifest duplicate, order or marker mismatch",
                                    ));
                                }
                                previous = Some(witness.identity);
                                let (key, body) = physical
                                    .next()
                                    .transpose()
                                    .map_err(|error| Self::relational_storage_error(table, error))?
                                    .ok_or_else(|| {
                                        verifier.corrupt(
                                            "version manifest member is physically missing",
                                        )
                                    })?;
                                if key.value() != witness.identity {
                                    return Err(
                                        verifier.corrupt("version manifest physical key mismatch")
                                    );
                                }
                                let mut admission = content_identity::ProofAdmission::for_reader(
                                    read_memory.clone(),
                                );
                                let actual = manifest::manifest_for_body(
                                    table,
                                    key.value(),
                                    body.value(),
                                    &mut admission,
                                )?;
                                if actual != coordinate {
                                    return Err(verifier
                                        .corrupt("version manifest body does not authenticate"));
                                }
                                let actual_witness = manifest::parse_manifest(&actual)?;
                                let created = TxId(u64::from_be_bytes(
                                    actual_witness.identity[8..16]
                                        .try_into()
                                        .expect("manifest identity"),
                                ));
                                let version_lsn = Lsn(u64::from_be_bytes(
                                    actual_witness.identity[16..]
                                        .try_into()
                                        .expect("manifest identity"),
                                ));
                                validate_read_version_bounds(
                                    &read,
                                    table,
                                    version_lsn,
                                    created,
                                    actual_witness.deleted_tx,
                                    TxId(captured.identity_tx_floor),
                                    Lsn(captured.source_lsn_floor),
                                )
                                .map_err(|error| {
                                    verifier.corrupt(format!("version bounds: {error:?}"))
                                })?;
                                if created == tx || actual_witness.deleted_tx == Some(tx) {
                                    content_identity::visit_history_logical_key_for_test(
                                        content_identity::HistoryKind::RelationalVersion,
                                        &actual,
                                        &mut admission,
                                        &mut visitor,
                                    )?;
                                }
                                Ok(true)
                            },
                        )?;
                        if let Some(error) = proof_owner.refusal.lock().take() {
                            return Err(error);
                        }
                        if !complete
                            || physical
                                .next()
                                .transpose()
                                .map_err(|error| Self::relational_storage_error(table, error))?
                                .is_some()
                        {
                            return Err(
                                verifier.corrupt("version table contains unwitnessed members")
                            );
                        }
                    }
                    Ok(())
                })
            })
        })
    }
}
