//! The native index participates in the authoritative row transaction.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;
use crate::composite_store::{
    PreparedVectorPartitionBinding, PreparedVectorPartitionMutation as Mutation,
    PreparedVectorPartitionMutationBatch,
};
use contextdb_core::{ColumnDef, VectorEntry};
use contextdb_vector::ivf::{RowIdentity, Vector};
use redb::ReadableTable;

fn column(binding: &PreparedVectorPartitionBinding) -> Result<&ColumnDef> {
    binding
        .table_meta
        .columns
        .get(binding.declaration)
        .ok_or_else(corrupt)
}
fn routes(
    mutation: &Mutation,
) -> (
    &VectorIndexRef,
    &PreparedVectorPartitionBinding,
    Option<&PreparedVectorPartitionBinding>,
) {
    match mutation {
        Mutation::Insert { index, after, .. } => (index, after, None),
        Mutation::Delete { index, before, .. } => (index, before, None),
        Mutation::Move {
            index,
            before,
            after,
            ..
        } => (index, before, Some(after)),
    }
}
fn previous_summary(
    read: &redb::ReadTransaction,
    route: &[u8],
) -> Result<super::super::vector_membership_pages::MembershipSummary> {
    use super::super::vector_membership_pages::{
        MembershipSummary, SOURCE, SUMMARY, decode_summary,
    };
    match read.open_table(SUMMARY) {
        Ok(table) => {
            if let Some(value) = table.get(route).map_err(RedbPersistence::storage_error)? {
                return decode_summary(route, value.value());
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(RedbPersistence::storage_error(e)),
    }
    match read.open_table(SOURCE) {
        Ok(table) => {
            if let Some(value) = table
                .range(route..)
                .map_err(RedbPersistence::storage_error)?
                .next()
                && value
                    .map_err(RedbPersistence::storage_error)?
                    .0
                    .value()
                    .starts_with(route)
            {
                return Err(corrupt());
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(RedbPersistence::storage_error(e)),
    }
    Ok(MembershipSummary::default())
}
fn old_body(
    persistence: &RedbPersistence,
    write: &redb::WriteTransaction,
    original: &crate::memory_accounting::OwnedMemoryReservation,
    index: &VectorIndexRef,
    binding: &PreparedVectorPartitionBinding,
) -> Result<VectorEntry> {
    let frame = super::super::OriginalPersistenceFrame::new(original)?;
    let key = RedbPersistence::vector_identity_key(
        index,
        binding.row.row_id,
        binding.vector_created_tx,
        binding.vector_lsn,
    );
    let table = write
        .open_table(super::super::VECTORS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let value = table
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(corrupt)?;
    let entry = RedbPersistence::decode_vector_entry_original(&frame, value.value())?;
    if entry.index != *index
        || entry.row_id != binding.row.row_id
        || entry.lsn != binding.vector_lsn
        || entry.created_tx != binding.vector_created_tx
    {
        return Err(corrupt());
    }
    let _ = persistence;
    Ok(entry)
}

impl write::WriteSession<'_> {
    pub(crate) fn apply_rows(
        &mut self,
        persistence: &RedbPersistence,
        previous: &redb::ReadTransaction,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        ws: &contextdb_tx::WriteSet,
        batch: &PreparedVectorPartitionMutationBatch,
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        if !crate::database::vector_ivf::CLUSTERED_INDEX_SHIPS {
            return Ok(());
        }
        // The immutable prepared write set already owns these bindings. Keep
        // only one tuple window; no resident population directory is introduced.
        for (at, mutation) in batch.mutations.iter().enumerate() {
            let (index, first, second) = routes(mutation);
            for binding in std::iter::once(first).chain(second) {
                if second.is_some()
                    && std::ptr::eq(binding, second.unwrap())
                    && binding.partition_key == first.partition_key
                {
                    continue;
                }
                let seen = batch.mutations[..at].iter().any(|m| {
                    let (i, a, b) = routes(m);
                    i == index
                        && (a.partition_key == binding.partition_key
                            || b.is_some_and(|b| b.partition_key == binding.partition_key))
                });
                if seen {
                    continue;
                }
                let declared = column(binding)?;
                if declared.search_mode == contextdb_core::VectorSearchMode::Exact
                    && declared.search_target_accuracy_hundredths.is_none()
                    && declared.search_expected_vectors.is_none()
                    && declared.ivf_lists.is_none()
                    && declared.ivf_probes.is_none()
                    && declared.ivf_refine_factor_hundredths.is_none()
                    && declared.vector_engine_policy == Default::default()
                {
                    continue;
                }
                let tuple = &binding.partition_key;
                self.with_scope(index, tuple, &|| Ok(()), &|_| Ok(()), |scope| {
                    let route = RedbPersistence::vector_partition_prefix(index, tuple);
                    let before = previous_summary(previous, &route)?;
                    let after = write::source_summary(write, &route)?;
                    let declared = column(binding)?;
                    #[cfg(feature = "test-seams")]
                    let test_column = persistence.native_ivf_test_column(declared)?;
                    #[cfg(feature = "test-seams")]
                    let declared = &test_column;
                    let first_family = scope.record.borrow().families.iter().all(Option::is_none);
                    scope.prepare_row_roots()?;
                    let usage = original.accountant().usage();
                    let source_identity = training_identity(tuple, before.source_identity());
                    let controls = std::mem::size_of::<write::WriteOperation<'_, '_, '_>>();
                    // Source decode and finalization retain their own original
                    // invoice, and coexist with the complete writer workspace.
                    let source_bytes = match declared.column_type {
                        contextdb_core::ColumnType::Vector(d) => {
                            d.checked_mul(8).ok_or_else(corrupt)?
                        }
                        _ => return Err(corrupt()),
                    };
                    let available = usage
                        .available
                        .unwrap_or(usize::MAX)
                        .saturating_sub(controls)
                        .saturating_sub(source_bytes);
                    let resolved =
                        crate::database::vector_ivf::resolved::resolve_with_native_receipts(
                            index,
                            declared,
                            after.live_entries,
                            usage.limit.map_or(u64::MAX, |n| n as u64),
                            available,
                            source_identity,
                            scope.native_entry_backing_quote()?,
                            Some(
                                batch
                                    .mutations
                                    .iter()
                                    .filter(|mutation| {
                                        let (i, a, b) = routes(mutation);
                                        i == index
                                            && (&a.partition_key == tuple
                                                || b.is_some_and(|b| &b.partition_key == tuple))
                                    })
                                    .count()
                                    .checked_mul(2)
                                    .ok_or_else(corrupt)?,
                            ),
                        )?;
                    {
                        let mut record = scope.record.borrow_mut();
                        if first_family {
                            record.policy_revision = declared.vector_policy_revision;
                            record.build_budget_bytes = resolved.build_budget_bytes;
                        }
                    }
                    scope.prepare_source_changes(&before, &after)?;
                    scope.with_row_writer(resolved.policy, resolved.training, |writer| {
                        for m in &batch.mutations {
                            let (i, _, _) = routes(m);
                            if i != index {
                                continue;
                            }
                            match m {
                                Mutation::Delete { before, .. }
                                    if &before.partition_key == tuple =>
                                {
                                    let expected = RowIdentity {
                                        row: before.row.row_id.0,
                                        commit_lsn: before.vector_lsn.0,
                                    };
                                    let present = writer.current_row_identity(0, expected.row)?;
                                    if present.is_none() {
                                        let old =
                                            old_body(persistence, write, original, index, before)?;
                                        scope.with_source_row_identity(expected, || {
                                            writer.commit_insert(Vector {
                                                partition: 0,
                                                row: expected.row,
                                                values: &old.vector,
                                            })
                                        })?;
                                    }
                                    writer.delete_row(0, expected)?;
                                    scope.after_row_staged(
                                        RowIdentity {
                                            row: expected.row,
                                            commit_lsn: batch.commit_lsn.0,
                                        },
                                        true,
                                    )?;
                                }
                                Mutation::Insert { after, entry, .. }
                                    if &after.partition_key == tuple =>
                                {
                                    let entry = batch.resolve_insert(ws, *entry, index, after)?;
                                    writer.commit_insert(Vector {
                                        partition: 0,
                                        row: entry.row_id.0,
                                        values: &entry.vector,
                                    })?;
                                    scope.after_row_staged(
                                        RowIdentity {
                                            row: entry.row_id.0,
                                            commit_lsn: batch.commit_lsn.0,
                                        },
                                        false,
                                    )?;
                                }
                                Mutation::Move { before, after, .. } => {
                                    let expected = RowIdentity {
                                        row: before.row.row_id.0,
                                        commit_lsn: before.vector_lsn.0,
                                    };
                                    if &before.partition_key == tuple {
                                        let present =
                                            writer.current_row_identity(0, expected.row)?;
                                        if present.is_none() {
                                            let old = old_body(
                                                persistence,
                                                write,
                                                original,
                                                index,
                                                before,
                                            )?;
                                            scope.with_source_row_identity(expected, || {
                                                writer.commit_insert(Vector {
                                                    partition: 0,
                                                    row: expected.row,
                                                    values: &old.vector,
                                                })
                                            })?;
                                        }
                                        writer.delete_row(0, expected)?;
                                    }
                                    if &after.partition_key == tuple {
                                        let old =
                                            old_body(persistence, write, original, index, before)?;
                                        writer.commit_insert(Vector {
                                            partition: 0,
                                            row: expected.row,
                                            values: &old.vector,
                                        })?;
                                    }
                                    scope.after_row_staged(
                                        RowIdentity {
                                            row: expected.row,
                                            commit_lsn: batch.commit_lsn.0,
                                        },
                                        &after.partition_key != tuple,
                                    )?;
                                }
                                _ => {}
                            }
                        }
                        Ok(())
                    })
                })?;
            }
        }
        Ok(())
    }
}
