//! Durable transaction history without a resident directory. These history
//! operations pin their own committed root; statement roots are captured only
//! through TransactionManager::capture_published_read.
use super::*;
use crate::memory_accounting::OwnedMemoryReservation;
use contextdb_tx::{CommitDirectory, CommitDirectoryCursor};
use std::ops::Bound;

struct DurableCommitCursor {
    read: redb::ReadTransaction,
    lower: Bound<u64>,
    upper: Bound<u64>,
    after: Option<u64>,
    finished: bool,
    _control: OwnedMemoryReservation,
}

fn empty_bounds(lower: &Bound<u64>, upper: &Bound<u64>) -> bool {
    let first = match lower {
        Bound::Unbounded => Some(0),
        Bound::Included(value) => Some(*value),
        Bound::Excluded(value) => value.checked_add(1),
    };
    let last = match upper {
        Bound::Unbounded => Some(u64::MAX),
        Bound::Included(value) => Some(*value),
        Bound::Excluded(value) => value.checked_sub(1),
    };
    match (first, last) {
        (Some(first), Some(last)) => first > last,
        _ => true,
    }
}

impl CommitDirectoryCursor for DurableCommitCursor {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>> {
        if self.finished {
            return Ok(None);
        }
        let table = self
            .read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let lower = self.after.map_or(self.lower, Bound::Excluded);
        if empty_bounds(&lower, &self.upper) {
            self.finished = true;
            return Ok(None);
        }
        let next = table
            .range((lower, self.upper))
            .map_err(RedbPersistence::storage_error)?
            .next()
            .transpose()
            .map_err(RedbPersistence::storage_error)?
            .map(|(lsn, tx)| {
                #[cfg(feature = "test-seams")]
                crate::writer_query_test_seams::note_history(
                    crate::writer_query_test_seams::HistoryFamily::CommitIndex,
                );
                (Lsn(lsn.value()), TxId(tx.value()))
            });
        if let Some((lsn, _)) = next {
            self.after = Some(lsn.0);
        } else {
            self.finished = true;
        }
        Ok(next)
    }
}

impl RedbPersistence {
    fn reserve_commit_directory_control(&self) -> Result<OwnedMemoryReservation> {
        self.prepare_proof_owner()?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("durable commit directory requires the attached memory accountant".into())
        })?;
        OwnedMemoryReservation::try_new_for(
            accountant,
            std::mem::size_of::<DurableCommitCursor>(),
            "writer_source",
            "commit_directory",
            "Raise MEMORY_LIMIT to admit the durable commit-directory cursor.",
        )
    }

    fn apply_commit_directory_identity(
        &self,
        write: &redb::WriteTransaction,
        mutation: content_identity::ContentIdentityMutation,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        // One admitted proof window per entry, released before advancing.
        Self::apply_content_identity_mutations_in_write(
            write,
            &[mutation],
            &mut self.proof_admission(),
        )?;
        Ok(())
    }
}

impl CommitDirectory for RedbPersistence {
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>> {
        let control = self.reserve_commit_directory_control()?;
        let lower = lower.map(|lsn| lsn.0);
        let upper = upper.map(|lsn| lsn.0);
        self.with_db(|db| {
            Ok(Box::new(DurableCommitCursor {
                read: db.begin_read().map_err(Self::storage_error)?,
                lower,
                upper,
                after: None,
                finished: empty_bounds(&lower, &upper),
                _control: control,
            }) as Box<dyn CommitDirectoryCursor>)
        })
    }

    fn floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>> {
        let _control = self.reserve_commit_directory_control()?;
        let upper = upper.map(|lsn| lsn.0);
        if empty_bounds(&Bound::Unbounded, &upper) {
            return Ok(None);
        }
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            Ok(table
                .range((Bound::<u64>::Unbounded, upper))
                .map_err(Self::storage_error)?
                .next_back()
                .transpose()
                .map_err(Self::storage_error)?
                .map(|(lsn, tx)| {
                    #[cfg(feature = "test-seams")]
                    crate::writer_query_test_seams::note_history(
                        crate::writer_query_test_seams::HistoryFamily::CommitIndex,
                    );
                    (Lsn(lsn.value()), TxId(tx.value()))
                }))
        })
    }

    fn census(&self) -> Result<(usize, Lsn)> {
        let _control = self.reserve_commit_directory_control()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            let count = usize::try_from(table.len().map_err(Self::storage_error)?)
                .map_err(|_| Error::Other("commit directory cardinality exceeds usize".into()))?;
            let first = table
                .first()
                .map_err(Self::storage_error)?
                .map_or(Lsn(0), |(lsn, _)| {
                    #[cfg(feature = "test-seams")]
                    crate::writer_query_test_seams::note_history(
                        crate::writer_query_test_seams::HistoryFamily::CommitIndex,
                    );
                    Lsn(lsn.value())
                });
            Ok((count, first))
        })
    }

    fn contains(&self, lsn: Lsn) -> Result<bool> {
        let _control = self.reserve_commit_directory_control()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            let present = table.get(lsn.0).map_err(Self::storage_error)?.is_some();
            #[cfg(feature = "test-seams")]
            if present {
                crate::writer_query_test_seams::note_history(
                    crate::writer_query_test_seams::HistoryFamily::CommitIndex,
                );
            }
            Ok(present)
        })
    }

    fn remove(&self, lsns: &[Lsn]) -> Result<u64> {
        if lsns.is_empty() {
            return Ok(0);
        }
        let _control = self.reserve_commit_directory_control()?;
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            // Preserve the old logical frontier even if every entry retires.
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            let mut removed = 0;
            for lsn in lsns {
                let present = write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?
                    .remove(lsn.0)
                    .map_err(Self::storage_error)?
                    .is_some();
                if present {
                    removed += 1;
                    self.apply_commit_directory_identity(
                        &write,
                        content_identity::remove_commit_index_mutation(*lsn),
                    )?;
                }
            }
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64> {
        let _control = self.reserve_commit_directory_control()?;
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            // Preserve the old logical frontier even if every entry retires.
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            let previous_count = write
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?
                .len()
                .map_err(Self::storage_error)?;
            let mut after = None;
            loop {
                let next = {
                    let table = write
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    table
                        .range((
                            after.map_or(Bound::Unbounded, Bound::Excluded),
                            Bound::<u64>::Unbounded,
                        ))
                        .map_err(Self::storage_error)?
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .map(|(lsn, _)| Lsn(lsn.value()))
                };
                let Some(lsn) = next else {
                    break;
                };
                after = Some(lsn.0);
                if !retained.contains_key(&lsn) {
                    write
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?
                        .remove(lsn.0)
                        .map_err(Self::storage_error)?;
                    self.apply_commit_directory_identity(
                        &write,
                        content_identity::remove_commit_index_mutation(lsn),
                    )?;
                }
            }
            let retained_count = retained.len() as u64;
            for (lsn, tx) in retained {
                let changed = {
                    let mut table = write
                        .open_table(COMMIT_INDEX_TABLE)
                        .map_err(Self::storage_error)?;
                    let old = table
                        .get(lsn.0)
                        .map_err(Self::storage_error)?
                        .map(|value| value.value());
                    if old == Some(tx.0) {
                        false
                    } else {
                        table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                        true
                    }
                };
                if changed {
                    self.apply_commit_directory_identity(
                        &write,
                        content_identity::commit_index_mutation(lsn, tx),
                    )?;
                }
            }
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(previous_count.saturating_sub(retained_count))
        })
    }

    fn record_applied(&self, _lsn: Lsn, _tx: TxId) {
        // Rows/logs and the directory entry are already atomically durable.
        // Never perform a second, fallible write after successful apply.
    }
}
