use super::*;
use content_identity::original_version_manifest as manifest;
pub(super) struct OriginalPendingManifestPublication {
    pub(super) mutations: Vec<content_identity::ContentIdentityMutation>,
    admission: content_identity::original_writer::ProofAdmission,
}
impl OriginalPendingManifestPublication {
    pub(super) fn new(admission: content_identity::original_writer::ProofAdmission) -> Self {
        Self {
            mutations: Vec::new(),
            admission,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn push(&mut self, coordinate: &[u8], present: bool) -> Result<()> {
        if self.mutations.len() == self.mutations.capacity() {
            let old = self.mutations.capacity();
            let capacity = old
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| Error::Other("manifest publication capacity overflow".into()))?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<content_identity::ContentIdentityMutation>())
                .ok_or_else(|| Error::Other("manifest publication layout overflow".into()))?;
            self.admission.admit(bytes)?;
            let mut next = Vec::with_capacity(capacity);
            next.extend(std::mem::take(&mut self.mutations));
            self.mutations = next;
            self.admission
                .release(old * std::mem::size_of::<content_identity::ContentIdentityMutation>());
        }
        let bytes = manifest::manifest_mutation_bytes(coordinate, present)?;
        self.admission.admit(bytes)?;
        let mutation = manifest::manifest_mutation(coordinate, present)?;
        debug_assert_eq!(
            mutation.logical_key.capacity()
                + mutation
                    .logical_value
                    .as_ref()
                    .map_or(0, content_identity::LogicalMutationValue::heap_capacity),
            bytes
        );
        self.mutations.push(mutation);
        Ok(())
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_relational_manifests_batch_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        pending: &mut content_identity::original_writer::OriginalIdentityBatch<'_, '_>,
    ) -> Result<()> {
        let names = OriginalPersistenceFrame::new(original)?;
        names.reserve(table.len().saturating_mul(2).saturating_add(256))?;
        let name = Self::rel_table_name(table);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        let relation = if write
            .table_exists(definition)
            .map_err(Self::storage_error)?
        {
            Some(write.open_table(definition).map_err(Self::storage_error)?)
        } else {
            None
        };
        let mut physical = relation
            .as_ref()
            .map(|relation| relation.iter())
            .transpose()
            .map_err(Self::storage_error)?;
        let prefix = manifest::manifest_prefix(table)?;
        struct Coordinate {
            bytes: Vec<u8>,
            _admission: content_identity::original_writer::ProofAdmission,
        }
        let mut after: Option<Coordinate> = None;
        loop {
            let mut admission = content_identity::original_writer::ProofAdmission::new(
                original,
                self.proof_scratch_owner(),
            )?;
            let next = manifest::writer_manifest_next_paged(
                write,
                &prefix,
                after
                    .as_ref()
                    .map_or(prefix.as_slice(), |at| at.bytes.as_slice()),
                after.is_some(),
                &mut admission,
                pending,
            )
            .map_err(|e| self.relational_manifest_error(e))?;
            let Some(coordinate) = next.filter(|key| key.starts_with(&prefix)) else {
                if physical
                    .as_mut()
                    .and_then(|iter| iter.next())
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
            let Some((key, body)) = physical
                .as_mut()
                .and_then(|iter| iter.next())
                .transpose()
                .map_err(Self::storage_error)?
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
            self.replace_relational_manifest_batch_in_write_original(
                original,
                write,
                table,
                key.value(),
                Some(body.value()),
                None,
                pending,
            )?;
            // The exact coordinate allocation dies before its own original debit.
            after = Some(Coordinate {
                bytes: coordinate,
                _admission: admission,
            });
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_relational_manifest_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<()> {
        self.replace_relational_manifest_pending_in_write_original(
            original, write, table, physical, old, new, None,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_relational_manifest_pending_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        pending: Option<&mut OriginalPendingManifestPublication>,
    ) -> Result<()> {
        self.replace_relational_manifest_with_overlay_in_write_original(
            original, write, table, physical, old, new, pending, None,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_relational_manifest_batch_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        pending: &mut content_identity::original_writer::OriginalIdentityBatch<'_, '_>,
    ) -> Result<()> {
        self.replace_relational_manifest_with_overlay_in_write_original(
            original,
            write,
            table,
            physical,
            old,
            new,
            None,
            Some(pending),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_relational_manifest_with_overlay_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        physical: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        mut pending: Option<&mut OriginalPendingManifestPublication>,
        paged: Option<&mut content_identity::original_writer::OriginalIdentityBatch<'_, '_>>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        let _family_scope =
            content_identity::enter_passive_family(content_identity::LogicalFamily::ChangeAndDdl);
        let mut admission = content_identity::original_writer::ProofAdmission::new(
            original,
            self.proof_scratch_owner(),
        )?;
        admission.admit(
            table
                .len()
                .checked_add(72)
                .ok_or_else(|| self.relational_manifest_damage("identity allocation overflow"))?,
        )?;
        let mut prefix = manifest::manifest_prefix(table)?;
        prefix.extend_from_slice(physical);
        let expected = if let Some(paged) = paged.as_deref() {
            manifest::writer_manifest_next_paged(
                write,
                &prefix,
                &prefix,
                false,
                &mut admission,
                paged,
            )
        } else {
            manifest::writer_manifest_next_pending(
                write,
                &prefix,
                &prefix,
                false,
                &mut admission,
                pending.as_ref().map_or(&[], |p| p.mutations.as_slice()),
            )
        }
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
            let next = if let Some(paged) = paged.as_deref() {
                manifest::writer_manifest_next_paged(
                    write,
                    &prefix,
                    coordinate,
                    true,
                    &mut admission,
                    paged,
                )
            } else {
                manifest::writer_manifest_next_pending(
                    write,
                    &prefix,
                    coordinate,
                    true,
                    &mut admission,
                    pending.as_ref().map_or(&[], |p| p.mutations.as_slice()),
                )
            }
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
        if let Some(paged) = paged {
            for (coordinate, present) in old_coordinate
                .iter()
                .map(|k| (k, false))
                .chain(new_coordinate.iter().map(|k| (k, true)))
            {
                admission.admit(manifest::manifest_mutation_bytes(coordinate, present)?)?;
                let mutation = manifest::manifest_mutation(coordinate, present)?;
                paged.push(&mutation, None)?;
            }
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
        Self::apply_owned_content_identity_with_manifests_in_write(
            write,
            &mutations,
            &[],
            &mut admission,
        )?;
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn verify_relational_group_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        row_id: RowId,
        relation: &impl ReadableTable<&'static [u8], &'static [u8]>,
    ) -> Result<()> {
        self.verify_relational_group_pending_in_write_original(
            original,
            write,
            table,
            row_id,
            relation,
            &[],
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn verify_relational_group_pending_in_write_original(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        write: &redb::WriteTransaction,
        table: &str,
        row_id: RowId,
        relation: &impl ReadableTable<&'static [u8], &'static [u8]>,
        pending: &[content_identity::ContentIdentityMutation],
    ) -> Result<()> {
        // The group's tail only, as the unaccounted writer checks it: the
        // newest physical version is the newest witnessed member, with a
        // matching body, and nothing is witnessed after it.
        let mut admission = content_identity::original_writer::ProofAdmission::new(
            original,
            self.proof_scratch_owner(),
        )?;
        admission.admit(
            table
                .len()
                .checked_add(72)
                .and_then(|n| n.checked_mul(3))
                .ok_or_else(|| self.relational_manifest_damage("writer group bound overflow"))?,
        )?;
        let mut prefix = manifest::manifest_prefix(table)?;
        prefix.extend_from_slice(&row_id.0.to_be_bytes());
        let (low, high) = Self::rel_row_key_range(row_id);
        let latest = relation
            .range(low.as_slice()..high.as_slice())
            .map_err(Self::storage_error)?
            .next_back()
            .transpose()
            .map_err(Self::storage_error)?;
        let mut window = content_identity::original_writer::ProofAdmission::new(
            original,
            self.proof_scratch_owner(),
        )?;
        let Some((key, body)) = latest else {
            let witnessed = manifest::writer_manifest_next_pending(
                write,
                &prefix,
                &prefix,
                false,
                &mut window,
                pending,
            )
            .map_err(|e| self.relational_manifest_error(e))?;
            if witnessed.is_some_and(|coordinate| coordinate.starts_with(&prefix)) {
                return Err(
                    self.relational_manifest_damage("writer group version is physically missing")
                );
            }
            return Ok(());
        };
        let mut at = manifest::manifest_prefix(table)?;
        at.extend_from_slice(key.value());
        let coordinate = manifest::writer_manifest_next_pending(
            write,
            &prefix,
            &at,
            false,
            &mut window,
            pending,
        )
        .map_err(|e| self.relational_manifest_error(e))?
        .filter(|coordinate| coordinate.starts_with(&prefix))
        .ok_or_else(|| self.relational_manifest_damage("unwitnessed writer group suffix"))?;
        let witness =
            manifest::parse_manifest(&coordinate).map_err(|e| self.relational_manifest_error(e))?;
        // The seek starts at the newest physical version, so a different
        // first witness is a newer member with no physical version.
        if key.value() != witness.identity {
            return Err(
                self.relational_manifest_damage("writer group version is physically missing")
            );
        }
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::count_group_version_authenticated();
        let actual = manifest::manifest_for_body(table, key.value(), body.value(), &mut window)
            .map_err(|e| self.relational_manifest_error(e))?;
        if actual != coordinate {
            return Err(
                self.relational_manifest_damage("writer group body or closure is unauthenticated")
            );
        }
        let after = manifest::writer_manifest_next_pending(
            write,
            &prefix,
            &coordinate,
            true,
            &mut window,
            pending,
        )
        .map_err(|e| self.relational_manifest_error(e))?;
        if after.is_some_and(|coordinate| coordinate.starts_with(&prefix)) {
            return Err(self.relational_manifest_damage(
                "duplicate or physically missing writer group version",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod refusal_tests;
