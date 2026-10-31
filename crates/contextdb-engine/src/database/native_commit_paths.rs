use super::*;

#[cfg(test)]
pub(super) fn r1_database_field_for_test(field: usize) {
    R1_DATABASE_PARTIAL_FIELD_FOR_TEST.with(|slot| {
        if slot.get() == Some(field) {
            slot.set(None);
            panic!("CDB127 injected partial database construction at field {field}");
        }
    });
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_owned_delivery_meta_if_present<R>(
        &self,
        table: &str,
        f: impl for<'m> FnOnce(&'m TableMeta) -> Result<R>,
    ) -> Result<Option<R>> {
        let _operation = self.assert_open_operation();
        if let Some(root) = self.scoped_writer_root() {
            root.borrow_captured_table_meta(table).map(f).transpose()
        } else {
            let catalog = self.relational_store.table_meta.read();
            catalog.get(table).map(f).transpose()
        }
    }
}

impl Database {
    // The iterator borrows the original one-shot bodies inside the detached
    // HRTB callback. Never collect, clone, or reload those guards here.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn revalidate_conditional_updates_in_prepared_write_set<'conditions>(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        snapshot: SnapshotId,
        guards: impl DoubleEndedIterator<Item = &'conditions PendingConditionalUpdateGuard>,
        mut stats: Option<&mut CommitStageStats>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        let mut guards = guards.rev().peekable();
        if guards.peek().is_none() {
            return Ok(CommitValidationOutcome::default());
        }
        // All published removals retain their original bodies and invoices in
        // the same guard's inverse until this complete pass settles. Refusal or
        // unwind restores even a removal published for an earlier guard.
        held.with_tx_checkpoint(|held| {
            let mut outcome = CommitValidationOutcome::default();
            for guard in guards {
                let mut read_credit =
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                if let Some(stats) = stats.as_deref_mut() {
                    stats.rows_validated = stats.rows_validated.saturating_add(1);
                }
                let matches = self.with_owned_delivery_meta_if_present(&guard.table, |meta| {
                    // The vector supplement consumes owned column names. Admit
                    // their native buffer and strings before constructing any.
                    let bytes = guard.predicates.iter().try_fold(
                        preparation_array_bytes::<String>(guard.predicates.len())?,
                        |bytes, (column, _)| {
                            bytes.checked_add(column.len()).ok_or_else(|| {
                                Error::Other("conditional column allocation size overflow".into())
                            })
                        },
                    )?;
                    let predicate_columns =
                        OwnedPreparationValue::try_produce(self, bytes, || {
                            let mut columns = Vec::with_capacity(guard.predicates.len());
                            if columns.capacity() != guard.predicates.len() {
                                return Err(Error::Other(
                                    "conditional column capacity mismatch".into(),
                                ));
                            }
                            for (column, _) in &guard.predicates {
                                let copied = column.as_str().to_owned();
                                if copied.capacity() != column.len() {
                                    return Err(Error::Other(
                                        "conditional column capacity mismatch".into(),
                                    ));
                                }
                                columns.push(copied);
                            }
                            Ok(columns)
                        })?;
                    predicate_columns.with_value(|predicate_columns| {
                        let resident = self.committed_row_at_admitted(
                            &guard.table,
                            guard.row_id,
                            snapshot,
                            &mut read_credit,
                            read_memory.clone(),
                        )?;
                        let needs_body = resident.as_ref().is_some_and(|row| {
                            predicate_columns.iter().any(|column| {
                                !meta.columns.iter().any(|declared| {
                                    declared.name == *column
                                        && matches!(declared.column_type, ColumnType::Vector(_))
                                }) && !row.values.contains_key(column)
                            })
                        });
                        let row = if needs_body {
                            self.committed_row_at_admitted(
                                &guard.table,
                                guard.row_id,
                                snapshot,
                                &mut read_credit,
                                read_memory.clone(),
                            )?
                        } else {
                            resident
                        };
                        match row {
                            Some(mut row) => {
                                self.supplement_selected_vector_columns(
                                    &guard.table,
                                    snapshot,
                                    None,
                                    Some(predicate_columns),
                                    std::slice::from_mut(&mut row),
                                    vector_copy_owner,
                                    read_memory.clone(),
                                )?;
                                Ok(row_conditions_match(&row, meta, &guard.predicates))
                            }
                            None => Ok(false),
                        }
                    })
                })?;
                let matches = match matches {
                    Some(matches) => matches,
                    None => {
                        // Preserve table_meta's missing received-schema observation
                        // while borrowing the captured metadata above.
                        if let Some(access) = &self.received_staging_access {
                            access.record(&guard.table);
                        }
                        false
                    }
                };
                if matches {
                    continue;
                }
                let removal = held.prepare_remove_conditional_slice(
                    Self::conditional_counts_for_preparation(guard.before),
                    Self::conditional_counts_for_preparation(guard.after),
                )?;
                // The prepared removal owns the actual invoice and inverse;
                // refunding the accountant here would release a live body twice.
                held.publish_tx(removal);
                if guard.fail_on_conflict {
                    outcome.conditional_conflict_count =
                        outcome.conditional_conflict_count.saturating_add(1);
                } else {
                    outcome.conditional_noop_count =
                        outcome.conditional_noop_count.saturating_add(1);
                }
            }
            Ok(outcome)
        })
    }

    pub(super) fn conditional_counts_for_preparation(
        counts: WriteSetCounts,
    ) -> contextdb_tx::manager::WriteSetCounts {
        contextdb_tx::manager::WriteSetCounts {
            relational_inserts: counts.relational_inserts,
            relational_deletes: counts.relational_deletes,
            adj_inserts: counts.adj_inserts,
            adj_deletes: counts.adj_deletes,
            row_adj_inserts: counts.row_adj_inserts,
            row_adj_deletes: counts.row_adj_deletes,
            vector_inserts: counts.vector_inserts,
            vector_deletes: counts.vector_deletes,
            vector_moves: counts.vector_moves,
        }
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rewrite_txid_placeholders_in_prepared_write_set(
        &self,
        origin_tx: TxId,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
    ) -> Result<()> {
        let length = held.read().relational_inserts.len();
        if length == 0 {
            return Ok(());
        }
        held.with_tx_checkpoint(|held| {
            for position in 0..length {
                let prepared = {
                    let read = held.read();
                    let (table, row) = &read.relational_inserts[position];
                    let prepared = self.with_owned_delivery_meta_if_present(table, |meta| {
                        let changes = meta.columns.iter().any(|column| {
                            matches!(column.column_type, ColumnType::TxId)
                                && matches!(row.values.get(&column.name),
                                    Some(Value::TxId(tx)) if *tx == origin_tx
                                        && *tx != row.created_tx)
                        });
                        if !changes {
                            return Ok(None);
                        }
                        let mut original = held.copy_existing_row_at(position)?;
                        original.rewrite_txid_placeholders(origin_tx, meta);
                        held.prepare_replace_admitted_row_at_from_meta(
                            position, table, original, meta,
                        )
                        .map(Some)
                    })?;
                    if prepared.is_none()
                        && let Some(access) = &self.received_staging_access
                    {
                        access.record(table);
                    }
                    prepared.flatten()
                };
                if let Some(prepared) = prepared {
                    held.publish(prepared);
                }
            }
            Ok(())
        })
    }
}

impl Database {
    /// Original slot winners are copied before any exact source removal. This
    /// consumer is called before the corresponding row slot is replaced.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reconcile_commit_time_upsert_vectors_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        row_position: usize,
        table: &str,
        incoming_row_id: RowId,
        conflict_row_id: RowId,
        replacement_values: &HashMap<ColName, Value>,
        replacement_tx: TxId,
        replacement_lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        self.with_owned_delivery_meta_if_present(table, |meta| {
            if !meta
                .columns
                .iter()
                .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
            {
                return Ok(());
            }
            held.with_tx_checkpoint(|held| {
                struct OriginalCommitVectorPlan {
                    actions: Vec<(usize, Option<contextdb_tx::AdmittedEagerSourceVector>, bool)>,
                    _credit: OwnedMemoryReservation,
                }
                let credit =
                    self.reserve_owned_preparation(preparation_array_bytes::<(
                        usize,
                        Option<contextdb_tx::AdmittedEagerSourceVector>,
                        bool,
                    )>(meta.columns.len())?)?;
                let mut plan = OriginalCommitVectorPlan {
                    actions: Vec::with_capacity(meta.columns.len()),
                    _credit: credit,
                };
                if plan.actions.capacity() != meta.columns.len() {
                    return Err(Error::Other("source native capacity mismatch".into()));
                }
                // The last pending entry for each column wins, as in the
                // original reconciliation. Losing bodies remain in the inverse
                // until the complete checkpoint settles; no manual refund.
                for (ordinal, column) in meta.columns.iter().enumerate() {
                    if !matches!(column.column_type, ColumnType::Vector(_)) {
                        continue;
                    }
                    let index = OwnedPreparationValue::try_produce(
                        self,
                        table.len().checked_add(column.name.len()).ok_or_else(|| {
                            Error::Other("source allocation size overflow".into())
                        })?,
                        || {
                            let index = VectorIndexRef::new(table, &column.name);
                            if index.table.capacity() != table.len()
                                || index.column.capacity() != column.name.len()
                            {
                                return Err(Error::Other("source native capacity mismatch".into()));
                            }
                            Ok(index)
                        },
                    )?;
                    let existing = index.with_value(|index| {
                        self.vector_store.load_live_entry_for_row(
                            index,
                            conflict_row_id,
                            snapshot,
                            vector_copy_owner,
                        )
                    })?;
                    let final_vector = match replacement_values.get(&column.name) {
                        Some(Value::Vector(values)) => Some(values),
                        _ => None,
                    };
                    let Some(final_vector) = final_vector else {
                        plan.actions.push((ordinal, None, existing.is_some()));
                        continue;
                    };
                    if existing
                        .as_ref()
                        .is_some_and(|entry| entry.vector == *final_vector)
                    {
                        plan.actions.push((ordinal, None, false));
                        continue;
                    }
                    let read = held.read();
                    let winner = read
                        .vector_inserts
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(_, entry)| {
                            entry.index.table == table
                                && entry.row_id == incoming_row_id
                                && entry.index.column == column.name
                        });
                    let original = match winner {
                        Some((position, entry)) if entry.vector == *final_vector => {
                            held.copy_existing_vector_at(position)?
                        }
                        _ => held.copy_vector_from_original_row_at(
                            row_position,
                            &column.name,
                            final_vector,
                            meta,
                        )?,
                    };
                    plan.actions
                        .push((ordinal, Some(original), existing.is_some()));
                }
                self.remove_vector_positions_in_restricted_source(held, |entry| {
                    entry.index.table == table && entry.row_id == incoming_row_id
                })?;
                for (ordinal, original, delete_existing) in plan.actions.drain(..) {
                    let column = &meta.columns[ordinal];
                    if delete_existing {
                        let already_deleted =
                            held.read().vector_deletes.iter().any(|(index, row, _)| {
                                index.table == table
                                    && index.column == column.name
                                    && *row == conflict_row_id
                            });
                        if !already_deleted {
                            let input = OwnedPreparationValue::try_produce(
                                self,
                                table.len().checked_add(column.name.len()).ok_or_else(|| {
                                    Error::Other("source allocation size overflow".into())
                                })?,
                                || {
                                    let index = VectorIndexRef::new(table, &column.name);
                                    if index.table.capacity() != table.len()
                                        || index.column.capacity() != column.name.len()
                                    {
                                        return Err(Error::Other(
                                            "source native capacity mismatch".into(),
                                        ));
                                    }
                                    Ok((index, conflict_row_id, replacement_tx))
                                },
                            )?;
                            let prepared = input.with_value(|input| {
                                let batch = held.copy_aux(
                                    contextdb_tx::staged_source::AuxBorrow::VectorDeletes(
                                        std::slice::from_ref(input),
                                    ),
                                )?;
                                held.prepare_aux_splice(
                                    held.read().vector_deletes.len(),
                                    &[],
                                    batch,
                                )
                            })?;
                            held.publish_tx(prepared);
                        }
                    }
                    if let Some(original) = original {
                        self.remove_vector_positions_in_restricted_source(held, |entry| {
                            entry.index.table == table
                                && entry.index.column == column.name
                                && entry.row_id == conflict_row_id
                        })?;
                        let prepared = held.prepare_append_retargeted_original_vector(
                            original,
                            conflict_row_id,
                            replacement_tx,
                            replacement_lsn,
                        )?;
                        held.publish(prepared);
                    }
                }
                Ok(())
            })
        })?
        .map_or(Ok(()), Ok)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_vector_positions_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        matches: impl Fn(&VectorEntry) -> bool,
    ) -> Result<()> {
        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        let read = held.read();
        let count = read
            .vector_inserts
            .iter()
            .filter(|entry| matches(entry))
            .count();
        if count == 0 {
            return Ok(());
        }
        let positions = OwnedPreparationValue::try_produce(
            self,
            preparation_array_bytes::<usize>(count)?,
            || {
                let mut positions = Vec::with_capacity(count);
                if positions.capacity() != count {
                    return Err(Error::Other("source native capacity mismatch".into()));
                }
                for (position, entry) in read.vector_inserts.iter().enumerate() {
                    if matches(entry) {
                        positions.push(position);
                    }
                }
                Ok(positions)
            },
        )?;
        let removal =
            positions.with_value(|positions| held.prepare_remove_vector_positions(positions))?;
        held.publish(removal);
        Ok(())
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn collect_commit_upsert_refs<'a>(
        &self,
        intents: impl DoubleEndedIterator<Item = &'a PendingUpsertIntent>,
    ) -> Result<MetadataBuffer<&'a PendingUpsertIntent>> {
        let mut held = MetadataBuffer::empty();
        for intent in intents {
            let capacity = held
                .values
                .len()
                .checked_add(1)
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            let mut next = self.with_preparation_original(|original| {
                MetadataBuffer::prepare(capacity, original)
            })?;
            held.move_into(&mut next);
            next.push(intent);
            held = next;
        }
        Ok(held)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_meta_owned(
        &self,
        table: &str,
    ) -> Result<crate::custody::preparation::OwnedPreparationValue<TableMeta>> {
        use crate::custody::preparation::OwnedPreparationValue;
        self.with_owned_delivery_meta_if_present(table, |meta| {
            let bytes = crate::memory_accounting::preparation_debug_native_bound(meta)?;
            OwnedPreparationValue::try_produce(self, bytes, || Ok(meta.clone()))
        })?
        .ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_preparation_read_bytes(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
    ) -> Result<usize> {
        let invalid = EngineMetadataJournal::identity_error;
        let read = held.read();
        let mut bytes = {
            let catalog = self.relational_store().table_meta.read();
            crate::memory_accounting::preparation_debug_native_bound(&*catalog)?
                .checked_mul(4)
                .ok_or_else(invalid)?
        };
        // Each borrowed ordinary row may contribute tuple-key clones and
        // schema clones to read-only validators. Reserve them before entry.
        for (table, row) in read.relational_inserts {
            let body = crate::memory_accounting::preparation_row_copy_bytes(row)?;
            let meta = self
                .with_owned_delivery_meta_if_present(table, |meta| {
                    let count = meta
                        .columns
                        .len()
                        .checked_add(meta.indexes.len())
                        .and_then(|n| n.checked_add(meta.enforced_unique_tuples().count()))
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(invalid)?;
                    let copies = body
                        .checked_add(table.len())
                        .and_then(|n| {
                            n.checked_add(
                                std::mem::size_of::<StagedTupleKey>()
                                    + std::mem::size_of::<VersionedRow>(),
                            )
                        })
                        .and_then(|n| n.checked_mul(count))
                        .ok_or_else(invalid)?;
                    copies
                        .checked_add(crate::memory_accounting::preparation_debug_native_bound(
                            meta,
                        )?)
                        .ok_or_else(invalid)
                })?
                .unwrap_or(body);
            bytes = bytes
                .checked_add(meta.checked_mul(4).ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
        }
        for (table, _, _) in read.relational_deletes {
            bytes = bytes
                .checked_add(table.len().checked_mul(8).ok_or_else(invalid)?)
                .and_then(|n| n.checked_add(8 * std::mem::size_of::<VersionedRow>()))
                .ok_or_else(invalid)?;
        }
        for entry in read.vector_inserts {
            let names = entry
                .index
                .table
                .len()
                .checked_add(entry.index.column.len())
                .ok_or_else(invalid)?;
            bytes = bytes
                .checked_add(names.checked_mul(8).ok_or_else(invalid)?)
                .and_then(|n| n.checked_add(8 * std::mem::size_of::<VectorEntry>()))
                .ok_or_else(invalid)?;
        }
        for (index, _, _) in read.vector_deletes {
            bytes = bytes
                .checked_add(index.table.len())
                .and_then(|n| n.checked_add(index.column.len()))
                .and_then(|n| n.checked_add(8 * std::mem::size_of::<VectorEntry>()))
                .ok_or_else(invalid)?;
        }
        for (index, _, _, _) in read.vector_moves {
            bytes = bytes
                .checked_add(index.table.len())
                .and_then(|n| n.checked_add(index.column.len()))
                .and_then(|n| n.checked_add(8 * std::mem::size_of::<VectorEntry>()))
                .ok_or_else(invalid)?;
        }
        Ok(bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn build_commit_upsert_row_owned(
        &self,
        table: &str,
        incoming: &VersionedRow,
        conflict: &VersionedRow,
        intent: &PendingUpsertIntent,
    ) -> Result<crate::custody::preparation::OwnedPreparationValue<VersionedRow>> {
        use crate::custody::preparation::OwnedPreparationValue;
        use crate::memory_accounting::{
            preparation_debug_native_bound, preparation_row_copy_bytes,
        };
        let invalid = EngineMetadataJournal::identity_error;
        // Count borrowed inputs without making an expression, plan or value
        // clone. The evaluator only returns cloned inputs or scalar values;
        // its coercions additionally parse text vectors. The admitted envelope
        // includes all expression scratch and overlapping map replacements.
        let input = preparation_row_copy_bytes(incoming)?
            .checked_add(preparation_row_copy_bytes(conflict)?)
            .and_then(|n| {
                n.checked_add(
                    crate::memory_accounting::preparation_values_copy_bytes(&intent.insert_values)
                        .ok()?,
                )
            })
            .ok_or_else(invalid)?;
        let mut working = input.checked_mul(4).ok_or_else(invalid)?;
        working = working
            .checked_add(
                std::mem::size_of::<(ColName, Expr)>()
                    .checked_mul(intent.update_columns.len())
                    .ok_or_else(invalid)?,
            )
            .and_then(|n| {
                n.checked_add(
                    std::mem::size_of::<String>().checked_mul(intent.conflict_columns.len())?,
                )
            })
            .ok_or_else(invalid)?;
        for column in &intent.conflict_columns {
            working = working.checked_add(column.len()).ok_or_else(invalid)?;
        }
        for (column, expression) in &intent.update_columns {
            let sizes = crate::memory_accounting::preparation_assignment_sizes(
                expression,
                &conflict.values,
                &intent.params,
            )?;
            working = working
                .checked_add(column.len())
                .and_then(|n| n.checked_add(sizes.0))
                .and_then(|n| n.checked_add(sizes.1))
                .and_then(|n| n.checked_add(sizes.2.checked_mul(8)?))
                .ok_or_else(invalid)?;
        }
        let meta_bytes = self
            .with_owned_delivery_meta_if_present(table, |meta| {
                preparation_debug_native_bound(meta)
            })?
            .unwrap_or(0);
        let members = incoming
            .values
            .len()
            .checked_add(conflict.values.len())
            .and_then(|n| n.checked_add(intent.insert_values.len()))
            .and_then(|n| n.checked_add(intent.update_columns.len()))
            .ok_or_else(invalid)?;
        let bytes = working
            .checked_add(meta_bytes.checked_mul(4).ok_or_else(invalid)?)
            .and_then(|n| {
                n.checked_add(
                    crate::memory_accounting::preparation_row_map_bytes(members)
                        .ok()?
                        .checked_mul(4)?,
                )
            })
            .ok_or_else(invalid)?;
        OwnedPreparationValue::try_produce(self, bytes, || {
            let on_conflict = OnConflictPlan {
                columns: intent.conflict_columns.clone(),
                update_columns: intent.update_columns.clone(),
            };
            let mut inserted =
                crate::memory_accounting::preparation_copy_values(&intent.insert_values)?;
            self.rewrite_txid_placeholders_in_values(
                table,
                intent.active_tx,
                incoming.created_tx,
                &mut inserted,
            );
            let mut values = apply_on_conflict_updates(
                self,
                table,
                inserted.clone(),
                conflict,
                &on_conflict,
                &intent.params,
                Some(intent.active_tx),
            )?;
            self.rewrite_txid_placeholders_in_values(
                table,
                intent.active_tx,
                incoming.created_tx,
                &mut values,
            );
            for (column, value) in &incoming.values {
                if inserted.get(column) != Some(value) {
                    values.insert(column.clone(), value.clone());
                }
            }
            Ok(VersionedRow {
                row_id: conflict.row_id,
                values,
                created_tx: incoming.created_tx,
                deleted_tx: incoming.deleted_tx,
                lsn: incoming.lsn,
                created_at: incoming.created_at,
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_commit_relational_delete(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        row: RowId,
        tx: TxId,
    ) -> Result<()> {
        use crate::custody::preparation::OwnedPreparationValue;
        if held
            .read()
            .relational_deletes
            .iter()
            .any(|(name, id, _)| name == table && *id == row)
        {
            return Ok(());
        }
        let input = OwnedPreparationValue::try_produce(self, table.len(), || {
            Ok((table.to_owned(), row, tx))
        })?;
        let prepared = input.with_value(|input| {
            let batch =
                held.copy_aux(contextdb_tx::staged_source::AuxBorrow::RelationalDeletes(
                    std::slice::from_ref(input),
                ))?;
            held.prepare_aux_splice(held.read().relational_deletes.len(), &[], batch)
        })?;
        held.publish_tx(prepared);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_commit_time_upsert_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        insert_index: usize,
        conflict: &VersionedRow,
        intent: &PendingUpsertIntent,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::copy_row_owned;
        let incoming = {
            let read = held.read();
            let Some((table, row)) = read.relational_inserts.get(insert_index) else {
                return Err(EngineMetadataJournal::identity_error());
            };
            if intent.table != *table || intent.row_id != row.row_id {
                return Ok(None);
            }
            copy_row_owned(self, row)?
        };
        incoming.with_value(|incoming| {
            let replacement =
                self.build_commit_upsert_row_owned(&intent.table, incoming, conflict, intent)?;
            replacement.with_value(|replacement| {
                self.validate_commit_time_upsert_replacement(
                    &intent.table,
                    conflict,
                    &replacement.values,
                    snapshot,
                    read_memory.clone(),
                )?;
                // O must accept both changed bodies before any publication.
                // Vector reconciliation changes the root/revision, so prepare
                // the row against the exact post-vector guard after that pass.
                held.with_tx_checkpoint(|held| {
                    self.reconcile_commit_time_upsert_vectors_in_restricted_source(
                        held,
                        insert_index,
                        &intent.table,
                        incoming.row_id,
                        conflict.row_id,
                        &replacement.values,
                        replacement.created_tx,
                        replacement.lsn,
                        snapshot,
                        vector_copy_owner,
                    )?;
                    let prepared = self
                        .with_owned_delivery_meta_if_present(&intent.table, |meta| {
                            held.prepare_changed_row_at_from_meta(
                                insert_index,
                                &intent.table,
                                incoming,
                                replacement,
                                meta,
                            )
                        })?
                        .ok_or_else(|| Error::TableNotFound(intent.table.clone()))?;
                    held.publish(prepared);
                    self.replace_row_graph_in_held_guard(
                        held,
                        &intent.table,
                        incoming.row_id,
                        replacement,
                        read_memory.clone(),
                    )?;

                    self.append_commit_relational_delete(
                        held,
                        &intent.table,
                        conflict.row_id,
                        replacement.created_tx,
                    )?;
                    let changed = replacement
                        .values
                        .iter()
                        .any(|(name, value)| conflict.values.get(name) != Some(value));
                    if changed {
                        let meta = self.commit_meta_owned(&intent.table)?;
                        meta.with_value(|meta| {
                            let state = meta
                                .state_machine
                                .as_ref()
                                .and_then(|sm| replacement.values.get(&sm.column))
                                .and_then(Value::as_text);
                            let uuid = replacement
                                .values
                                .get("id")
                                .and_then(Value::as_uuid)
                                .copied();
                            self.propagate_state_change_in_restricted_source(
                                held,
                                &intent.table,
                                uuid,
                                state,
                                replacement.created_tx,
                                snapshot,
                                vector_copy_owner,
                                read_memory.clone(),
                            )
                        })?;
                    }
                    Ok(Some(conflict.row_id))
                })
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn apply_original_commit_time_upsert_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        position: usize,
        skip_deleted: &HashSet<RowId>,
        intents: &[&PendingUpsertIntent],
        snapshot: SnapshotId,
        mut stats: Option<&mut CommitStageStats>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, copy_row_owned};
        let read = held.read();
        let (table, row) = &read.relational_inserts[position];
        let row = copy_row_owned(self, row)?;
        let table = OwnedPreparationValue::try_produce(self, table.len(), || Ok(table.clone()))?;
        table.with_value(|table| {
            row.with_value(|row| {
                for intent in intents
                    .iter()
                    .filter(|intent| intent.table == *table && intent.row_id == row.row_id)
                {
                    if intent.conflict_columns.is_empty() {
                        continue;
                    }
                    let input_bytes =
                        crate::memory_accounting::preparation_debug_native_bound(*intent)?;
                    let keys = OwnedPreparationValue::try_produce(self, input_bytes, || {
                        let mut values = crate::memory_accounting::preparation_copy_values(
                            &intent.insert_values,
                        )?;
                        self.rewrite_txid_placeholders_in_values(
                            table,
                            intent.active_tx,
                            row.created_tx,
                            &mut values,
                        );
                        let mut keys = Vec::with_capacity(intent.conflict_columns.len());
                        for column in &intent.conflict_columns {
                            match values.get(column) {
                                Some(Value::Null) | None => return Ok(None),
                                Some(value) => keys.push(value.clone()),
                            }
                        }
                        Ok(Some(keys))
                    })?;
                    let replacement = keys.with_value(|keys| {
                        let Some(keys) = keys else {
                            return Ok(None);
                        };
                        let id = self.committed_unique_conflict_row_id_in_restricted_source(
                            table,
                            &intent.conflict_columns,
                            keys,
                            snapshot,
                            skip_deleted,
                            stats.as_deref_mut(),
                            read_memory.clone(),
                        )?;
                        let Some(id) = id else {
                            return Ok(None);
                        };
                        let mut credit =
                            contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                        let conflict = self.committed_row_at_admitted(
                            table,
                            id,
                            snapshot,
                            &mut credit,
                            read_memory.clone(),
                        )?;
                        match conflict {
                            Some(conflict) => self.apply_commit_time_upsert_in_restricted_source(
                                held,
                                position,
                                &conflict,
                                intent,
                                snapshot,
                                vector_copy_owner,
                                read_memory.clone(),
                            ),
                            None => Ok(None),
                        }
                    })?;
                    if replacement.is_some() {
                        return Ok(replacement);
                    }
                }
                Ok(None)
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rewrite_commit_time_upserts_in_prepared_write_set<'a>(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        snapshot: SnapshotId,
        intents: impl DoubleEndedIterator<Item = &'a PendingUpsertIntent>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::OwnedPreparationValue;
        let intents = self.collect_commit_upsert_refs(intents)?;
        if intents.values.is_empty() {
            return Ok(());
        }
        held.with_tx_checkpoint(|held| {
            let mut position = 0;
            while position < held.read().relational_inserts.len() {
                let table = &held.read().relational_inserts[position].0;
                let table =
                    OwnedPreparationValue::try_produce(self, table.len(), || Ok(table.clone()))?;
                let rewritten = table.with_value(|table| {
                    let bytes = self.commit_preparation_read_bytes(held)?;
                    let skip = OwnedPreparationValue::try_produce(self, bytes, || {
                        Ok(held.with_legacy(|ws| Self::deleted_row_ids_for_table(ws, table)))
                    })?;
                    skip.with_value(|skip| {
                        self.apply_original_commit_time_upsert_in_restricted_source(
                            held,
                            position,
                            skip,
                            &intents.values,
                            snapshot,
                            None,
                            vector_copy_owner,
                            read_memory.clone(),
                        )
                    })
                })?;
                if let Some(id) = rewritten {
                    position = table
                        .with_value(|table| {
                            held.with_legacy(|ws| {
                                Self::relational_insert_index_by_row(ws, table, id, position)
                            })
                        })
                        .unwrap_or_else(|| position.min(held.read().relational_inserts.len()));
                } else {
                    position += 1;
                }
            }
            Ok(())
        })
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_commit_unique_scratch(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<NativeCommitUniqueScratch> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let credit = self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
        let columns = self.staged_tuple_key_columns_by_table();
        let lookup = held.with_legacy(|ws| StagedTupleLookup::from_write_set(&columns, ws));
        let deleted = held.with_legacy(Self::deleted_row_ids_by_table);
        let mut selected_reader_credit =
            contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut deleted_rows = HashMap::<String, HashMap<RowId, VersionedRow>>::new();
        for (table, row, _) in held.read().relational_deletes {
            if deleted_rows
                .get(table)
                .is_some_and(|rows| rows.contains_key(row))
            {
                continue;
            }
            let selected = columns.get(table).map_or(&[][..], Vec::as_slice);
            let original = if self.persistence.is_some() {
                self.constraint_row_at_admitted(
                    table,
                    *row,
                    snapshot,
                    selected,
                    &mut selected_reader_credit,
                    read_memory.clone(),
                )?
            } else {
                self.committed_row_at_admitted(
                    table,
                    *row,
                    snapshot,
                    &mut selected_reader_credit,
                    read_memory.clone(),
                )?
            };
            if let Some(original) = original {
                deleted_rows
                    .entry(table.clone())
                    .or_default()
                    .insert(*row, original);
            }
        }
        Ok(NativeCommitUniqueScratch {
            _columns: columns,
            lookup,
            deleted,
            deleted_rows,
            _selected_reader_credit: selected_reader_credit,
            _credit: credit,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_unique_constraints_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        snapshot: SnapshotId,
        intents: &[&PendingUpsertIntent],
        stats: &mut CommitStageStats,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, copy_row_owned};
        if held.read().relational_inserts.is_empty() {
            return Ok(());
        }
        held.with_tx_checkpoint(|held| {
            let mut scratch =
                self.prepare_commit_unique_scratch(held, snapshot, read_memory.clone())?;
            let empty_deleted = HashSet::new();
            let mut position = 0;
            while position < held.read().relational_inserts.len() {
                let (table, incoming) = &held.read().relational_inserts[position];
                let table =
                    OwnedPreparationValue::try_produce(self, table.len(), || Ok(table.clone()))?;
                let row = copy_row_owned(self, incoming)?;
                let rewritten = table.with_value(|table| {
                    row.with_value(|row| {
                        let meta = self.commit_meta_owned(table)?;
                        let skip = scratch.deleted.get(table).unwrap_or(&empty_deleted);
                        stats.rows_validated = stats.rows_validated.saturating_add(1);
                        if let Some(id) = self
                            .apply_original_commit_time_upsert_in_restricted_source(
                                held,
                                position,
                                skip,
                                intents,
                                snapshot,
                                Some(stats),
                                vector_copy_owner,
                                read_memory.clone(),
                            )?
                        {
                            return Ok(Some(id));
                        }
                        meta.with_value(|meta| {
                            // The scalar declaration order precedes enforced tuple
                            // order exactly as in the original uniqueness pass.
                            for columns in meta
                                .columns
                                .iter()
                                .filter(|column| column.primary_key || column.unique)
                                .map(|column| vec![column.name.clone()])
                                .chain(meta.enforced_unique_tuples().cloned())
                            {
                                let mut values = Vec::with_capacity(columns.len());
                                let mut has_null = false;
                                for column in &columns {
                                    match row.values.get(column) {
                                        Some(Value::Null) | None => {
                                            has_null = true;
                                            break;
                                        }
                                        Some(value) => values.push(value.clone()),
                                    }
                                }
                                if has_null {
                                    continue;
                                }
                                let staged = if Self::values_can_be_tuple_key(&values) {
                                    scratch.lookup.staged_unique_conflict(
                                        table, row.row_id, &columns, &values, stats,
                                    )
                                } else {
                                    held.with_legacy(|ws| {
                                        Self::staged_unique_conflict_in_write_set(
                                            ws,
                                            position,
                                            table,
                                            &columns,
                                            &values,
                                            Some(stats),
                                        )
                                    })
                                };
                                if staged.is_some() {
                                    return Err(Error::UniqueViolation {
                                        table: table.clone(),
                                        column: columns.first().cloned().unwrap_or_default(),
                                    });
                                }
                                if !self.committed_unique_probe_required_for_replacement(
                                    row.row_id,
                                    &columns,
                                    &values,
                                    skip,
                                    scratch.deleted_rows.get(table),
                                ) {
                                    continue;
                                }
                                let conflict = self
                                    .committed_unique_conflict_row_id_in_restricted_source(
                                        table,
                                        &columns,
                                        &values,
                                        snapshot,
                                        skip,
                                        Some(stats),
                                        read_memory.clone(),
                                    )?;
                                let Some(id) = conflict else {
                                    continue;
                                };
                                // The original conflict index selects the first
                                // recorded matching intent, never the last one.
                                let intent = intents.iter().copied().find(|intent| {
                                    intent.table == *table
                                        && intent.row_id == row.row_id
                                        && intent.conflict_columns == columns
                                });
                                if let Some(intent) = intent {
                                    let mut credit = contextdb_core::read_memory::ReadCredit::new(
                                        read_memory.clone(),
                                    );
                                    let conflict = self.committed_row_at_admitted(
                                        table,
                                        id,
                                        snapshot,
                                        &mut credit,
                                        read_memory.clone(),
                                    )?;
                                    if let Some(conflict) = conflict
                                        && let Some(id) = self
                                            .apply_commit_time_upsert_in_restricted_source(
                                                held,
                                                position,
                                                &conflict,
                                                intent,
                                                snapshot,
                                                vector_copy_owner,
                                                read_memory.clone(),
                                            )?
                                    {
                                        return Ok(Some(id));
                                    }
                                }
                                return Err(Error::UniqueViolation {
                                    table: table.clone(),
                                    column: columns.first().cloned().unwrap_or_default(),
                                });
                            }
                            Ok(None)
                        })
                    })
                })?;
                if let Some(id) = rewritten {
                    // Build a paid successor before retiring old lookup keys;
                    // no changed row clone is installed into an old envelope.
                    scratch =
                        self.prepare_commit_unique_scratch(held, snapshot, read_memory.clone())?;
                    position = table
                        .with_value(|table| {
                            held.with_legacy(|ws| {
                                Self::relational_insert_index_by_row(ws, table, id, position)
                            })
                        })
                        .unwrap_or_else(|| position.min(held.read().relational_inserts.len()));
                } else {
                    position += 1;
                }
            }
            Ok(())
        })
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enqueue_commit_propagation_entry(
        &self,
        queue: &mut MetadataBuffer<
            crate::custody::preparation::OwnedPreparationValue<PropagationQueueEntry>,
        >,
        table: &str,
        uuid: uuid::Uuid,
        target: &str,
        depth: u32,
        abort: bool,
    ) -> Result<()> {
        use crate::custody::preparation::OwnedPreparationValue;
        let bytes = table
            .len()
            .checked_add(target.len())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        let entry = OwnedPreparationValue::try_produce(self, bytes, || {
            Ok(PropagationQueueEntry {
                table: table.to_owned(),
                uuid,
                target_state: target.to_owned(),
                depth,
                abort_on_failure: abort,
            })
        })?;
        if queue.values.len() == queue.values.capacity() {
            let capacity = queue
                .values
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_next_power_of_two())
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            let mut next = self.with_preparation_original(|original| {
                MetadataBuffer::prepare(capacity, original)
            })?;
            queue.move_into(&mut next);
            *queue = next;
        }
        queue.push(entry);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_commit_vector_delete(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        index: &VectorIndexRef,
        row: RowId,
        tx: TxId,
    ) -> Result<()> {
        use crate::custody::preparation::OwnedPreparationValue;
        if held
            .read()
            .vector_deletes
            .iter()
            .any(|(have, id, _)| have == index && *id == row)
        {
            return Ok(());
        }
        let bytes = index
            .table
            .len()
            .checked_add(index.column.len())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        let input =
            OwnedPreparationValue::try_produce(self, bytes, || Ok((index.clone(), row, tx)))?;
        let prepared = input.with_value(|input| {
            let batch = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::VectorDeletes(
                std::slice::from_ref(input),
            ))?;
            held.prepare_aux_splice(held.read().vector_deletes.len(), &[], batch)
        })?;
        held.publish_tx(prepared);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_vector_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        tx: TxId,
        index: &VectorIndexRef,
        row: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        self.vector_store.with_index_layout(index, |_| ())?;
        let _scratch = self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
        let existing = self.point_commit_row_in_restricted_source(
            held,
            &index.table,
            snapshot,
            &|have| have.row_id == row,
            read_memory.clone(),
        )?;
        let Some(existing) = existing else {
            return Err(Error::NotFound(format!(
                "row {row} in table {}",
                index.table
            )));
        };
        existing.with_value(|existing| {
            self.assert_row_write_allowed(
                &index.table,
                row,
                &existing.values,
                snapshot,
                read_memory.clone(),
            )
        })?;
        let live =
            self.vector_store
                .has_live_entry_for_row(index, row, snapshot, vector_copy_owner)?;
        held.with_tx_checkpoint(|held| {
            let canceled = held
                .read()
                .vector_inserts
                .iter()
                .any(|entry| entry.index == *index && entry.row_id == row);
            self.remove_vector_positions_in_restricted_source(held, |entry| {
                entry.index == *index && entry.row_id == row
            })?;
            let read = held.read();
            let count = read
                .vector_moves
                .iter()
                .filter(|(have, _, target, _)| have == index && *target == row)
                .count();
            let moves = OwnedPreparationValue::try_produce(
                self,
                preparation_array_bytes::<(usize, RowId)>(count)?,
                || {
                    let mut moves = Vec::with_capacity(count);
                    for (position, (have, source, target, _)) in
                        read.vector_moves.iter().enumerate()
                    {
                        if have == index && *target == row {
                            moves.push((position, *source));
                        }
                    }
                    Ok(moves)
                },
            )?;
            moves.with_value(|moves| {
                let positions = OwnedPreparationValue::try_produce(
                    self,
                    preparation_array_bytes::<usize>(moves.len())?,
                    || {
                        Ok(moves
                            .iter()
                            .map(|(position, _)| *position)
                            .collect::<Vec<_>>())
                    },
                )?;
                if !moves.is_empty() {
                    let prepared = positions.with_value(|positions| {
                        let empty = held
                            .copy_aux(contextdb_tx::staged_source::AuxBorrow::VectorMoves(&[]))?;
                        held.prepare_aux_splice(0, positions, empty)
                    })?;
                    held.publish_tx(prepared);
                }
                let pending_move_from = held
                    .read()
                    .vector_moves
                    .iter()
                    .any(|(have, source, _, _)| have == index && *source == row);
                for (_, source) in moves {
                    self.append_commit_vector_delete(held, index, *source, tx)?;
                }
                if !pending_move_from && ((!canceled && moves.is_empty()) || live) {
                    self.append_commit_vector_delete(held, index, row, tx)?;
                }
                Ok(())
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_commit_prior_row_slots(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        row: RowId,
        prior_length: usize,
    ) -> Result<()> {
        for position in (0..prior_length).rev() {
            let read = held.read();
            if read.relational_inserts[position].0 != table
                || read.relational_inserts[position].1.row_id != row
            {
                continue;
            }
            let before = contextdb_tx::manager::WriteSetCounts {
                relational_inserts: position,
                relational_deletes: read.relational_deletes.len(),
                adj_inserts: read.adj_inserts.len(),
                adj_deletes: read.adj_deletes.len(),
                row_adj_inserts: read.row_adj_inserts.len(),
                row_adj_deletes: read.row_adj_deletes.len(),
                vector_inserts: read.vector_inserts.len(),
                vector_deletes: read.vector_deletes.len(),
                vector_moves: read.vector_moves.len(),
            };
            let after = contextdb_tx::manager::WriteSetCounts {
                relational_inserts: position + 1,
                ..before
            };
            let prepared = held.prepare_remove_conditional_slice(before, after)?;
            held.publish_tx(prepared);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn upsert_state_row_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        state_column: &str,
        existing: &VersionedRow,
        target: &str,
        tx: TxId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, copy_row_owned};
        let bytes = crate::memory_accounting::preparation_row_copy_bytes(existing)?
            .checked_add(crate::memory_accounting::preparation_row_map_bytes(
                existing
                    .values
                    .len()
                    .checked_add(1)
                    .ok_or_else(EngineMetadataJournal::identity_error)?,
            )?)
            .and_then(|n| n.checked_add(state_column.len()))
            .and_then(|n| n.checked_add(target.len()))
            .and_then(|n| n.checked_mul(2))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        let replacement = OwnedPreparationValue::try_produce(self, bytes, || {
            let mut values = crate::memory_accounting::preparation_copy_values(&existing.values)?;
            values.insert(state_column.to_owned(), Value::Text(target.to_owned()));
            Ok(VersionedRow {
                row_id: existing.row_id,
                values,
                created_tx: tx,
                deleted_tx: None,
                lsn: held.read().commit_lsn.unwrap_or(Lsn(0)),
                created_at: Some(current_wallclock()),
            })
        })?;
        replacement.with_value(|replacement| {
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &existing.values,
                snapshot,
                read_memory.clone(),
            )?;
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &replacement.values,
                snapshot,
                read_memory.clone(),
            )?;
            if self.relational_store().is_immutable(table) {
                return Err(Error::ImmutableTable(table.to_owned()));
            }
            let old = existing
                .values
                .get(state_column)
                .and_then(Value::as_text)
                .unwrap_or("");
            if old == target {
                return Ok(false);
            }
            if !self
                .relational_store()
                .validate_state_transition(table, state_column, old, target)
            {
                return Err(Error::InvalidStateTransition(format!("{old} -> {target}")));
            }
            if existing.values.get(state_column).and_then(Value::as_text) == Some(target) {
                return Ok(false);
            }
            self.validate_primary_key_values_present(table, &replacement.values)?;
            let prior_length = held.read().relational_inserts.len();
            let selected = held
                .read()
                .relational_inserts
                .iter()
                .rposition(|(name, row)| name == table && row.row_id == existing.row_id);
            let prepared = if let Some(position) = selected {
                let expected = copy_row_owned(self, &held.read().relational_inserts[position].1)?;
                expected.with_value(|expected| {
                    self.with_owned_delivery_meta_if_present(table, |meta| {
                        held.prepare_append_changed_row_at_from_meta(
                            position,
                            table,
                            expected,
                            replacement,
                            meta,
                        )
                    })
                })?
            } else {
                self.with_owned_delivery_meta_if_present(table, |meta| {
                    held.prepare_new_commit_row_from_meta(table, replacement, meta)
                })?
            }
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            held.publish(prepared);
            self.replace_row_graph_in_held_guard(
                held,
                table,
                existing.row_id,
                replacement,
                read_memory.clone(),
            )?;

            self.remove_commit_prior_row_slots(held, table, existing.row_id, prior_length)?;
            let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let committed = self.committed_row_at_admitted(
                table,
                existing.row_id,
                self.snapshot_for_read(),
                &mut credit,
                read_memory.clone(),
            )?;
            if committed.is_some() {
                self.append_commit_relational_delete(held, table, existing.row_id, tx)?;
            }
            Ok(true)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn enqueue_propagation_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        metas: &HashMap<String, TableMeta>,
        queue: &mut MetadataBuffer<
            crate::custody::preparation::OwnedPreparationValue<PropagationQueueEntry>,
        >,
        source: PropagationSource<'_>,
        fk_first: bool,
        tx: TxId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let fk = |held: &mut contextdb_tx::manager::EngineGuard<'_>,
                  queue: &mut MetadataBuffer<_>|
         -> Result<()> {
            for (table, meta) in metas {
                for rule in &meta.propagation_rules {
                    let PropagationRule::ForeignKey {
                        fk_column,
                        referenced_table,
                        trigger_state,
                        target_state,
                        max_depth,
                        abort_on_failure,
                        ..
                    } = rule
                    else {
                        continue;
                    };
                    if referenced_table != source.table
                        || trigger_state != source.state
                        || source.depth >= *max_depth
                    {
                        continue;
                    }
                    let _credit =
                        self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
                    let mut admission_failure = None;
                    let scanned = self.visit_commit_rows_in_restricted_source(
                        held,
                        table,
                        snapshot,
                        &|row| row.values.get(fk_column) == Some(&Value::Uuid(source.uuid)),
                        &mut |row| {
                            if let Some(uuid) =
                                row.values.get("id").and_then(Value::as_uuid).copied()
                                && let Err(error) = self.enqueue_commit_propagation_entry(
                                    queue,
                                    table,
                                    uuid,
                                    target_state,
                                    source.depth + 1,
                                    *abort_on_failure,
                                )
                            {
                                admission_failure = Some(error);
                                return Ok(std::ops::ControlFlow::Break(()));
                            }
                            Ok(std::ops::ControlFlow::Continue(()))
                        },
                        read_memory.clone(),
                    );
                    if let Some(error) = admission_failure {
                        return Err(error);
                    }
                    scanned?;
                }
            }
            Ok(())
        };
        if fk_first {
            fk(held, queue)?;
        }
        if let Some(meta) = metas.get(source.table) {
            for rule in &meta.propagation_rules {
                let PropagationRule::Edge {
                    edge_type,
                    direction,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                } = rule
                else {
                    continue;
                };
                if trigger_state != source.state || source.depth >= *max_depth {
                    continue;
                }
                let capture = self.graph_cursor_held_read_guard(held, tx, read_memory.clone())?;
                let _captured = capture.enter();
                let (bfs, _, _bfs_memory) = self.graph_bfs_counted(
                    Some(tx),
                    source.uuid,
                    Some(std::slice::from_ref(edge_type)),
                    *direction,
                    1..=1,
                    snapshot,
                    read_memory.clone(),
                )?;
                for node in bfs.nodes {
                    let _credit =
                        self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
                    let row = self.point_commit_row_in_restricted_source(
                        held,
                        source.table,
                        snapshot,
                        &|row| row.values.get("id") == Some(&Value::Uuid(node.id)),
                        read_memory.clone(),
                    )?;
                    if row.is_some() {
                        self.enqueue_commit_propagation_entry(
                            queue,
                            source.table,
                            node.id,
                            target_state,
                            source.depth + 1,
                            *abort_on_failure,
                        )?;
                    }
                }
            }
            for rule in &meta.propagation_rules {
                let PropagationRule::VectorExclusion { trigger_state } = rule else {
                    continue;
                };
                if trigger_state != source.state {
                    continue;
                }
                let Some(column) = meta
                    .columns
                    .iter()
                    .find(|column| matches!(column.column_type, ColumnType::Vector(_)))
                else {
                    continue;
                };
                use crate::custody::preparation::OwnedPreparationValue;
                let names = source
                    .table
                    .len()
                    .checked_add(column.name.len())
                    .ok_or_else(EngineMetadataJournal::identity_error)?;
                let index = OwnedPreparationValue::try_produce(self, names, || {
                    Ok(VectorIndexRef::new(source.table, &column.name))
                })?;
                let _credit =
                    self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
                let rows = self.commit_row_ids_for_uuid_in_restricted_source(
                    held,
                    source.table,
                    source.uuid,
                    snapshot,
                    read_memory.clone(),
                )?;
                for row in rows.values.iter().copied() {
                    index.with_value(|index| {
                        self.delete_vector_in_restricted_source(
                            held,
                            tx,
                            index,
                            row,
                            snapshot,
                            vector_copy_owner,
                            read_memory.clone(),
                        )
                    })?;
                }
            }
        }
        if !fk_first {
            fk(held, queue)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn propagate_state_change_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
        tx: TxId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::OwnedPreparationValue;
        let (Some(uuid), Some(state)) = (row_uuid, new_state) else {
            return Ok(());
        };
        if !self.propagation_rules_can_react(table, state) || *held.read().propagation_in_progress {
            return Ok(());
        }
        let metas = {
            let catalog = self.relational_store().table_meta.read();
            let bytes = crate::memory_accounting::preparation_debug_native_bound(&*catalog)?;
            OwnedPreparationValue::try_produce(self, bytes, || Ok(catalog.clone()))?
        };
        held.with_tx_checkpoint(|held| metas.with_value(|metas| {
            let flags = |held: &contextdb_tx::manager::EngineGuard<'_>, active| {
                let read = held.read();
                contextdb_tx::manager::EngineFlags { commit_lsn: *read.commit_lsn, finalized_tx: *read.finalized_tx,
                    requires_commit_lsn: *read.requires_commit_lsn, visibility_floor: *read.visibility_floor,
                    propagation_in_progress: active }
            };
            let prepared = held.prepare_flags(flags(held, true))?;
            held.publish_tx(prepared);
            let mut queue = MetadataBuffer::empty();
            let mut visited: MetadataBuffer<(OwnedPreparationValue<String>, uuid::Uuid)> = MetadataBuffer::empty();
            let mut abort_violation = None;
            self.enqueue_propagation_in_restricted_source(held, metas, &mut queue,
                PropagationSource { table, uuid, state, depth: 0 }, true, tx, snapshot, vector_copy_owner, read_memory.clone())?;
            while !queue.values.is_empty() {
                let entry = queue.values.remove(0);
                entry.with_value(|entry| -> Result<()> {
                    if visited.values.iter().any(|(table, uuid)| *uuid == entry.uuid && table.with_value(|table| *table == entry.table)) { return Ok(()); }
                    let name = OwnedPreparationValue::try_produce(self, entry.table.len(), || Ok(entry.table.clone()))?;
                    if visited.values.len() == visited.values.capacity() {
                        let capacity = visited.values.len().checked_add(1).and_then(|n| n.checked_next_power_of_two())
                            .ok_or_else(EngineMetadataJournal::identity_error)?;
                        let mut next = self.with_preparation_original(|original| MetadataBuffer::prepare(capacity, original))?;
                        visited.move_into(&mut next); visited = next;
                    }
                    visited.push((name, entry.uuid));
                    let Some(meta) = metas.get(&entry.table) else { return Ok(()); };
                    let Some(sm) = &meta.state_machine else {
                        eprintln!("warning: propagation target table {} has no state machine", entry.table);
                        if entry.abort_on_failure && abort_violation.is_none() {
                            abort_violation = Some(Error::PropagationAborted { table: entry.table.clone(), column: String::new(),
                                from: String::new(), to: entry.target_state.clone() });
                        }
                        return Ok(());
                    };
                    let _credit = self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
                    let existing = self.point_commit_row_in_restricted_source(held, &entry.table, snapshot,
                        &|row| row.values.get("id") == Some(&Value::Uuid(entry.uuid)), read_memory.clone())?;
                    let Some(existing) = existing else { return Ok(()); };
                    existing.with_value(|existing| {
                    let from = existing.values.get(&sm.column).and_then(Value::as_text).unwrap_or("");
                    match self.upsert_state_row_in_restricted_source(held, &entry.table, &sm.column, existing,
                        &entry.target_state, tx, snapshot, read_memory.clone()) {
                        Ok(false) => return Ok(()), Ok(true) => (),
                        Err(Error::InvalidStateTransition(_)) => {
                            eprintln!("warning: skipped invalid propagated transition {}.{} {} -> {}", entry.table, sm.column, from, entry.target_state);
                            if entry.abort_on_failure && abort_violation.is_none() {
                                abort_violation = Some(Error::PropagationAborted { table: entry.table.clone(), column: sm.column.clone(),
                                    from: from.to_owned(), to: entry.target_state.clone() });
                            }
                            return Ok(());
                        }
                        Err(error) => return Err(error),
                    }
                    self.enqueue_propagation_in_restricted_source(held, metas, &mut queue,
                        PropagationSource { table: &entry.table, uuid: entry.uuid, state: &entry.target_state, depth: entry.depth },
                        false, tx, snapshot, vector_copy_owner, read_memory.clone())
                    })
                })?;
            }
            if let Some(error) = abort_violation { return Err(error); }
            let prepared = held.prepare_flags(flags(held, false))?;
            held.publish_tx(prepared);
            Ok(())
        }))
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn discard_gone_deletes_in_restricted_source(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        let count = held.read().relational_deletes.len();
        if count == 0 {
            return Ok(());
        }
        let mut bytes = preparation_array_bytes::<(usize, String, RowId)>(count)?;
        for (table, _, _) in held.read().relational_deletes {
            bytes = bytes
                .checked_add(table.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?;
        }
        let discarded = OwnedPreparationValue::try_produce(self, bytes, || {
            let mut discarded = Vec::with_capacity(count);
            let read = held.read();
            for (position, (table, row, _)) in read.relational_deletes.iter().enumerate() {
                if read
                    .relational_inserts
                    .iter()
                    .any(|(name, incoming)| name == table && incoming.row_id == *row)
                {
                    continue;
                }
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let live = self.committed_row_at_admitted(
                    table,
                    *row,
                    snapshot,
                    &mut credit,
                    read_memory.clone(),
                )?;
                if live.is_none() {
                    discarded.push((position, table.clone(), *row));
                }
            }
            Ok(discarded)
        })?;
        discarded.with_value(|discarded| {
            if discarded.is_empty() {
                return Ok(());
            }
            let positions = OwnedPreparationValue::try_produce(
                self,
                preparation_array_bytes::<usize>(discarded.len())?,
                || {
                    Ok(discarded
                        .iter()
                        .map(|(position, _, _)| *position)
                        .collect::<Vec<_>>())
                },
            )?;
            let prepared = positions.with_value(|positions| {
                let empty = held.copy_aux(
                    contextdb_tx::staged_source::AuxBorrow::RelationalDeletes(&[]),
                )?;
                held.prepare_aux_splice(0, positions, empty)
            })?;
            held.publish_tx(prepared);
            let count = held.read().config_writes.len();
            let positions = OwnedPreparationValue::try_produce(
                self,
                preparation_array_bytes::<usize>(count)?,
                || {
                    let mut selected = Vec::with_capacity(count);
                    for (position, (key, encoded)) in held.read().config_writes.iter().enumerate() {
                        if !key.starts_with(Self::DURABLE_LINEAGE_CONFIG_PREFIX) {
                            continue;
                        }
                        let bytes = encoded
                            .len()
                            .checked_mul(std::mem::size_of::<DurableLineageRecord>())
                            .and_then(|n| n.checked_mul(2))
                            .ok_or_else(EngineMetadataJournal::identity_error)?;
                        let _credit = self.reserve_owned_preparation(bytes)?;
                        let Ok(record) =
                            RedbPersistence::decode_config_value::<DurableLineageRecord>(encoded)
                        else {
                            continue;
                        };
                        if record.delete_obligation == DurableDeleteObligation::Pending
                            && record.local_row_id.is_some_and(|id| {
                                discarded
                                    .iter()
                                    .any(|(_, table, row)| *table == record.table && *row == id)
                            })
                        {
                            selected.push(position);
                        }
                    }
                    Ok(selected)
                },
            )?;
            positions.with_value(|positions| {
                if positions.is_empty() {
                    return Ok(());
                }
                let empty =
                    held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(&[]))?;
                let prepared = held.prepare_aux_splice(0, positions, empty)?;
                held.publish_tx(prepared);
                Ok(())
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_validate_in_prepared_write_set(
        &self,
        origin_tx: TxId,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        peer: &mut OwnedTerminalMetadata,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut empty = EnginePendingCommitMetadata::empty();
        let metadata = if let Some(node) = peer.node.as_mut() {
            let record = node.get_mut();
            if record.tx != origin_tx || record.depth != 0 || record.next.is_some() {
                return Err(EngineMetadataJournal::identity_error());
            }
            &mut record.metadata
        } else {
            &mut empty
        };
        // These original buffers are one-shot. Move their complete paid
        // owners, leaving epochs in the exact singleton across callback passes.
        let conditional = std::mem::replace(
            &mut metadata.conditional_update_guards,
            MetadataBuffer::empty(),
        );
        let lineage = std::mem::replace(&mut metadata.sync_lineage_guards, MetadataBuffer::empty());
        let upserts = std::mem::replace(&mut metadata.upsert_intents, MetadataBuffer::empty());
        let published = self.tx_mgr.capture_published_read();
        let snapshot = published.snapshot;
        let root = writer_read::durable_root(published.handle)?;
        let _writer_read = self.enter_writer_read(root, read_memory.clone());
        let mut stats = CommitStageStatsGuard::new(self);
        #[cfg(any(test, feature = "test-seams"))]
        let mut timer = CommitStageTimer::new();
        held.with_tx_checkpoint(|held| {
            for entry in &lineage.values {
                let guard = entry.body.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let row = self.committed_row_at_admitted(&guard.table, guard.row_id, snapshot, &mut credit, read_memory.clone())?;
                if !row.is_some_and(|row| row.values == guard.values) {
                    return Err(Error::SyncError((format!("authenticated creator delete target {} row {} changed before commit; retry the sync unit",
                        guard.table, guard.row_id.0)).into()));
                }
            }
            let conditions = conditional.values.iter().map(|entry|
                entry.body.as_ref().expect("validated original conditional body"));
            let outcome = self.revalidate_conditional_updates_in_prepared_write_set(held, snapshot, conditions,
                Some(stats.stats_mut()), vector_copy_owner, read_memory.clone())?;
            #[cfg(any(test, feature = "test-seams"))]
            timer.record(stats.stats_mut(), 0);
            let check_vectors = |held: &contextdb_tx::manager::EngineGuard<'_>, owner: &mut contextdb_vector::VectorCopyAdmission| {
                let _credit = self.reserve_owned_preparation(self.commit_preparation_read_bytes(held)?)?;
                let empty_epochs = HashMap::new();
                let epochs = metadata.vector_schema_epochs.as_ref()
                    .and_then(|entry| entry.body.as_ref()).unwrap_or(&empty_epochs);
                held.with_legacy(|ws| {
                    self.validate_vector_schema_epochs(ws, epochs)?;
                    self.validate_vector_write_set_schema(ws, owner, read_memory.clone())
                })
            };
            check_vectors(held, vector_copy_owner)?;
            self.discard_gone_deletes_in_restricted_source(held, snapshot, read_memory.clone())?;
            let intents = self.collect_commit_upsert_refs(upserts.values.iter().map(|entry|
                entry.body.as_ref().expect("validated original upsert body")))?;
            self.validate_unique_constraints_in_restricted_source(held, snapshot, &intents.values,
                stats.stats_mut(), vector_copy_owner, read_memory.clone())?;
            #[cfg(any(test, feature = "test-seams"))]
            timer.record(stats.stats_mut(), 1);
            let changes = self.accepted_commit_state_changes(held.read(), snapshot, read_memory.clone())?;
            changes.with_value(|changes| -> Result<()> {
                for (table, uuid, state, tx) in changes {
                    self.propagate_state_change_in_restricted_source(held, table, Some(*uuid), Some(state), *tx,
                        snapshot, vector_copy_owner, read_memory.clone())?;
                }
                Ok(())
            })?;
            self.validate_unique_constraints_in_restricted_source(held, snapshot, &[],
                stats.stats_mut(), vector_copy_owner, read_memory.clone())?;
            self.canonicalize_row_graph_in_held_guard(held)?;
            let scratch = self.prepare_commit_unique_scratch(held, snapshot, read_memory.clone())?;
            held.with_legacy(|ws| self.validate_foreign_keys_in_restricted_read_source(ws, snapshot,
                &scratch.deleted_rows, stats.stats_mut(), read_memory.clone()))?;
            #[cfg(any(test, feature = "test-seams"))]
            timer.record(stats.stats_mut(), 2);
            held.with_legacy(|ws| self.validate_composite_foreign_keys_in_write_set(ws, snapshot,
                &scratch.deleted_rows, stats.stats_mut(), read_memory.clone()))?;
            #[cfg(any(test, feature = "test-seams"))]
            timer.record(stats.stats_mut(), 3);
            check_vectors(held, vector_copy_owner)?;
            #[cfg(any(test, feature = "test-seams"))]
            timer.record(stats.stats_mut(), 4);
            stats.flush();
            Ok(outcome)
        })
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_commit_rows_in_restricted_source(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
        visit: &mut dyn FnMut(&VersionedRow) -> Result<std::ops::ControlFlow<()>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let read = held.read();
        let mut cursor = self.bounded_physical_table_cursor(table)?;
        loop {
            let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let row = self.bounded_physical_table_row_next::<Error>(
                read_memory.clone(),
                table,
                snapshot,
                &mut cursor,
                || Ok(()),
                |bytes| {
                    credit.try_grow_for(
                        bytes,
                        "reader",
                        "commit_row_source",
                        "Reduce selected rows or raise the declared read memory limit.",
                    )
                },
            )?;
            let Some(row) = row else {
                break;
            };
            if !row.visible_at(snapshot)
                || read
                    .relational_deletes
                    .iter()
                    .any(|(name, id, _)| name == table && *id == row.row_id)
                || row_matches_delete_predicates(read.relational_delete_predicates, table, &row)
                || !predicate(&row)
            {
                continue;
            }
            if visit(&row)?.is_break() {
                return Ok(());
            }
        }
        // Original final staged order is the ascending ordinal of each last
        // row-id winner. Deleted staged rows stay only when that identity was
        // present in the original committed snapshot, before delete filtering.
        for (position, (name, row)) in read.relational_inserts.iter().enumerate() {
            if name != table
                || read.relational_inserts[position + 1..]
                    .iter()
                    .any(|(later_name, later)| later_name == table && later.row_id == row.row_id)
            {
                continue;
            }
            if read
                .relational_deletes
                .iter()
                .any(|(name, id, _)| name == table && *id == row.row_id)
            {
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let committed = self.committed_row_at_admitted(
                    table,
                    row.row_id,
                    snapshot,
                    &mut credit,
                    read_memory.clone(),
                )?;
                if committed.is_none() {
                    continue;
                }
            }
            if predicate(row) && visit(row)?.is_break() {
                return Ok(());
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn point_commit_row_in_restricted_source(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<crate::custody::preparation::OwnedPreparationValue<VersionedRow>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut selected = None;
        self.visit_commit_rows_in_restricted_source(
            held,
            table,
            snapshot,
            predicate,
            &mut |row| {
                selected = Some(crate::custody::preparation::copy_row_owned(self, row)?);
                Ok(std::ops::ControlFlow::Break(()))
            },
            read_memory,
        )?;
        Ok(selected)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_row_ids_for_uuid_in_restricted_source(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        uuid: uuid::Uuid,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<MetadataBuffer<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut rows = MetadataBuffer::empty();
        self.visit_commit_rows_in_restricted_source(
            held,
            table,
            snapshot,
            &|row| row.values.get("id") == Some(&Value::Uuid(uuid)),
            &mut |row| {
                if rows.values.len() == rows.values.capacity() {
                    let capacity = rows
                        .values
                        .len()
                        .checked_add(1)
                        .and_then(|n| n.checked_next_power_of_two())
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    let mut next = self.with_preparation_original(|original| {
                        MetadataBuffer::prepare(capacity, original)
                    })?;
                    rows.move_into(&mut next);
                    rows = next;
                }
                rows.push(row.row_id);
                Ok(std::ops::ControlFlow::Continue(()))
            },
            read_memory,
        )?;
        Ok(rows)
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_foreign_keys_in_restricted_read_source(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
        deleted_rows_by_table: &HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let staged_lookup = StagedTupleLookup::rebuild(self, ws)?;
        let deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for column in &meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                let staged_deletes = deleted_by_table
                    .get(&reference.table)
                    .unwrap_or(&empty_deleted);
                let parent_columns = vec![reference.column.clone()];
                let parent_values = std::slice::from_ref(value);
                let staged_match = if Self::values_can_be_tuple_key(parent_values) {
                    staged_lookup.contains_tuple(
                        &reference.table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, &reference.table, &parent_columns, parent_values)
                };
                let committed_match = if staged_match {
                    true
                } else {
                    self.visible_commit_row_exists_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        staged_deletes,
                        Some(stats),
                        read_memory.clone(),
                    )?
                };
                if !staged_match && !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
            }
        }

        let reverse_refs = {
            let metas = self.relational_store.table_meta.read();
            metas
                .iter()
                .flat_map(|(child_table, meta)| {
                    meta.columns.iter().filter_map(|column| {
                        column.references.as_ref().map(|reference| {
                            (
                                reference.table.clone(),
                                reference.column.clone(),
                                child_table.clone(),
                                column.name.clone(),
                            )
                        })
                    })
                })
                .collect::<Vec<_>>()
        };

        for (parent_table, parent_row_id, _) in &ws.relational_deletes {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let Some(parent_row) = deleted_rows_by_table
                .get(parent_table)
                .and_then(|rows| rows.get(parent_row_id))
            else {
                continue;
            };
            for (ref_table, ref_column, child_table, child_column) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_row.values.get(ref_column) else {
                    continue;
                };
                if *parent_value == Value::Null {
                    continue;
                }
                let parent_columns = vec![ref_column.clone()];
                let parent_values = std::slice::from_ref(parent_value);
                let parent_replaced_with_same_key = if Self::values_can_be_tuple_key(parent_values)
                {
                    staged_lookup.contains_tuple(
                        parent_table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, parent_table, &parent_columns, parent_values)
                };
                if parent_replaced_with_same_key {
                    continue;
                }
                let child_deletes = deleted_by_table.get(child_table).unwrap_or(&empty_deleted);
                if self.visible_commit_row_exists_by_column(
                    child_table,
                    child_column,
                    parent_value,
                    snapshot,
                    child_deletes,
                    Some(stats),
                    read_memory.clone(),
                )? {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
            }
        }

        Ok(())
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_unique_conflict_row_id_in_restricted_source(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        mut stats: Option<&mut CommitStageStats>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }
        if let Some(row) = self.indexed_visible_row_id_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats.as_deref_mut(),
            read_memory.clone(),
        )? {
            return Ok(row);
        }
        let mut cursor = self.bounded_physical_table_cursor(table)?;
        let before = self.relational_store.scan_rows_touched();
        let mut selected = None;
        // Finish the original complete scan even after selecting its first
        // match. This preserves scan/refusal and work-counter semantics while
        // retaining only one admitted source body at any time.
        loop {
            let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let row = self.bounded_physical_table_row_next::<Error>(
                read_memory.clone(),
                table,
                snapshot,
                &mut cursor,
                || Ok(()),
                |bytes| {
                    credit.try_grow_for(
                        bytes,
                        "reader",
                        "commit_unique_source",
                        "Reduce selected rows or raise the declared read memory limit.",
                    )
                },
            )?;
            let Some(row) = row else {
                break;
            };
            if selected.is_none()
                && row.visible_at(snapshot)
                && !skip_deleted.contains(&row.row_id)
                && columns
                    .iter()
                    .zip(values)
                    .all(|(column, value)| row.values.get(column) == Some(value))
            {
                selected = Some(row.row_id);
            }
        }
        if let Some(stats) = stats {
            stats.scan_rows_touched = stats.scan_rows_touched.saturating_add(
                self.relational_store
                    .scan_rows_touched()
                    .saturating_sub(before),
            );
        }
        Ok(selected)
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visible_commit_row_exists_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::preparation::OwnedPreparationValue;
        let columns = OwnedPreparationValue::try_produce(
            self,
            column
                .len()
                .checked_add(std::mem::size_of::<String>())
                .ok_or_else(EngineMetadataJournal::identity_error)?,
            || Ok(vec![column.to_owned()]),
        )?;
        columns.with_value(|columns| {
            self.committed_unique_conflict_row_id_in_restricted_source(
                table,
                columns,
                std::slice::from_ref(value),
                snapshot,
                skip_deleted,
                stats,
                read_memory,
            )
            .map(|row| row.is_some())
        })
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_original_discard_non_event_set_batch(
        &self,
        selections: &[AuthoritativePurgeSelection],
        history: &crate::persistence::discard_history::OriginalDiscardHistory,
        frame: &crate::persistence::OriginalPersistenceFrame<'_>,
        encoded_source: &crate::persistence::OwnedEncodedEventSource<'_>,
        sink_names: &BTreeSet<String>,
        report_shape: crate::custody::erasure::PurgeReportShape,
        _frontier: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OriginalPurgeNonEventPreparedSet> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        use crate::custody::erasure::PurgeReportShape;
        let report_requested = report_shape != PurgeReportShape::None;
        // Decode selected durable windows from the authentic Original. The
        // caller's requested report remains a separate, admitted output.
        let read_memory =
            crate::memory_accounting::persistence_original_projection(frame.original)?
                .read_budget()?;
        let _operation = self.open_operation()?;
        let published = self.tx_mgr.capture_published_read();
        let _writer_read = self.enter_writer_read(
            writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        // An explicit node-local instruction may have no hub rows.

        // Ownership is a (table,row) pair throughout one union.
        let table_count = selections
            .iter()
            .enumerate()
            .filter(|(position, selection)| {
                !selections[..*position]
                    .iter()
                    .any(|prior| prior.table == selection.table)
            })
            .count();
        frame.tree::<String, HashSet<RowId>>(table_count)?;
        frame.array::<AuthoritativePurgeLineageConfigOwners>(selections.len())?;
        for candidate in selections {
            frame.reserve(candidate.table.len())?;
            frame.record(&candidate.natural_key)?;
        }
        let mut selected_by_table = BTreeMap::<String, HashSet<RowId>>::new();
        for candidate in selections {
            if !selected_by_table.contains_key(&candidate.table) {
                let count = selections
                    .iter()
                    .filter(|selection| selection.table == candidate.table)
                    .count();
                frame.map::<RowId, ()>(count)?;
                selected_by_table.insert(candidate.table.clone(), HashSet::with_capacity(count));
            }
            if !selected_by_table
                .get_mut(&candidate.table)
                .expect("prepared selected table")
                .insert(candidate.local_row_id)
            {
                return Err(Error::SyncError(
                    "erasure selected one pinned row twice".into(),
                ));
            }
        }
        let selected = |table: &str, row_id: &RowId| {
            selected_by_table
                .get(table)
                .is_some_and(|ids| ids.contains(row_id))
        };
        let lifecycle_templates = selections
            .iter()
            .map(|candidate| {
                self.validate_authoritative_purge_selection(candidate, read_memory.clone())
                    .and_then(|witness| {
                        Self::authoritative_purge_lifecycle_template(candidate, &witness)
                            .map(|template| (template, witness.lifecycle.is_some()))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let prepared_snapshot = self.snapshot();
        // The actual discard caller requests no survivor report. In that
        // route retain only original-paid identities, never selected row bodies.
        let selected_table_rows = if report_requested {
            selected_by_table
                .iter()
                .map(|(table, ids)| {
                    self.authoritative_purge_selected_row_versions(table, ids, read_memory.clone())
                        .map(|rows| (table.clone(), rows))
                })
                .collect::<Result<BTreeMap<_, _>>>()?
        } else {
            BTreeMap::new()
        };
        use crate::persistence::discard_history::{self, ROWS};
        let surviving_forward_entries = Vec::<AdjEntry>::new();
        // Resident arrival removals are bounded by the existing resident map,
        // not by the durable selected occurrence count. Durable config keys go
        // straight to the same-write selection, one marker per window.
        let resident_arrival_count = self.sync_graph_arrivals.read().len();
        frame.array::<(GraphIdentity, DurableGraphArrival)>(resident_arrival_count)?;
        let removed_graph_arrivals: Vec<(GraphIdentity, DurableGraphArrival)> =
            Vec::with_capacity(resident_arrival_count);
        for candidate in selections {
            let mut found = false;
            self.visit_original_discard_selected_versions(
                &candidate.table,
                candidate.local_row_id,
                |row| {
                    found = true;
                    history.push(
                        frame.original,
                        ROWS,
                        &(&candidate.table, row.row_id, row.created_tx, row.lsn),
                    )?;
                    self.with_row_graph_witness(
                        &candidate.table,
                        candidate.table_generation,
                        row,
                        read_memory.clone(),
                        |edge, owner| history.push_row_graph(frame.original, edge, owner),
                    )?;
                    Ok(true)
                },
                read_memory.clone(),
            )?;
            if !found {
                return Err(Error::SyncError("erasure lost a pinned row".into()));
            }
        }
        history.select_changes(
            self.persistence
                .as_ref()
                .ok_or_else(EngineMetadataJournal::identity_error)?,
            frame.original,
            |entry| match entry {
                ChangeLogEntry::RowInsert { table, row_id, .. }
                | ChangeLogEntry::RowDelete { table, row_id, .. } => selected(table, row_id),
                ChangeLogEntry::VectorInsert { index, row_id, .. }
                | ChangeLogEntry::VectorDelete { index, row_id, .. } => {
                    selected(&index.table, row_id)
                }
                _ => false,
            },
        )?;
        let graph_arrival_config_keys = Vec::new();
        // The relational selector names rows, not vector partitions. Resolve
        // those rows through the lightweight vector directories and fault in
        // only partitions that contain a selected lineage before building the
        // purge image. Reading every registered partition here would turn a
        // one-row forget into a whole-database memory spike.
        for (table, row_ids) in &selected_by_table {
            self.vector_store.ensure_raw_partitions_containing_rows(
                table,
                row_ids,
                vector_copy_owner,
            )?;
        }
        let mut all_partitioned_vector_entries = Vec::new();
        for (table, row_ids) in &selected_by_table {
            all_partitioned_vector_entries.extend(
                self.vector_store
                    .durable_entries_of_partitions_containing_rows(
                        table,
                        row_ids,
                        vector_copy_owner,
                    )?,
            );
        }
        let selected_partitioned_vector_entries = all_partitioned_vector_entries
            .iter()
            .filter(|entry| selected(&entry.entry.index.table, &entry.entry.row_id))
            .cloned()
            .collect::<Vec<_>>();
        for entry in &all_partitioned_vector_entries {
            frame.record(&entry.entry)?;
        }
        let mut vector_entries = Vec::new();
        let mut vector_partition_selectors = Vec::new();
        for selected in &selected_partitioned_vector_entries {
            if !vector_entries.contains(&selected.entry) {
                vector_entries.push(selected.entry.clone());
            }
            let selector = VectorPartitionVersionSelector {
                index: selected.entry.index.clone(),
                partition_key: selected.partition_key.clone(),
                row_id: selected.entry.row_id,
                vector_created_tx: selected.entry.created_tx,
                vector_lsn: selected.entry.lsn,
            };
            if !vector_partition_selectors.contains(&selector) {
                vector_partition_selectors.push(selector);
            }
        }
        let persistence = self.persistence.as_ref().ok_or_else(|| {
            Error::SyncError(
                ("authoritative purge requires a file-backed database".to_string()).into(),
            )
        })?;
        let vector_partition_identities =
            persistence.resolve_vector_partition_version_identities(&vector_partition_selectors)?;
        let mut affected_vector_partitions = selected_partitioned_vector_entries
            .iter()
            .map(|entry| {
                VectorPartitionRef::new(entry.entry.index.clone(), entry.partition_key.clone())
            })
            .collect::<Vec<_>>();
        affected_vector_partitions.sort_by(|left, right| {
            left.index
                .table
                .cmp(&right.index.table)
                .then(left.index.column.cmp(&right.index.column))
                .then(left.partition_key.cmp(&right.partition_key))
        });
        affected_vector_partitions.dedup();

        let mut disk_source_provenance = Vec::new();
        for (table, ids) in &selected_by_table {
            disk_source_provenance
                .extend(self.authoritative_purge_disk_source_provenance(table, ids)?);
        }
        let lineage_config_owners = selections
            .iter()
            .zip(lifecycle_templates)
            .map(
                |(candidate, (lifecycle_template, lifecycle_record_is_selected_life))| {
                    AuthoritativePurgeLineageConfigOwners {
                        row_sidecar_key: Self::durable_row_lineage_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                        ),
                        creation_lineage_key: Self::durable_unbound_creation_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                            candidate.local_row_id,
                        ),
                        accepted_author_key: format!(
                            "sync_row_author.{}",
                            Self::hex_component(&sync_identity_key(
                                &candidate.table,
                                &candidate.natural_key
                            ))
                        ),
                        accepted_author_memory_key: (
                            candidate.table.clone(),
                            sync_identity_key(&candidate.table, &candidate.natural_key),
                        ),
                        erase_current_key_metadata: true,
                        lifecycle_record_key: Self::durable_lineage_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                        ),
                        lifecycle_record_is_selected_life,
                        lifecycle_template,
                    }
                },
            )
            .collect::<Vec<_>>();
        // The future purge kernel consumed its phase-one drain guard only
        // after acquiring the transaction commit mutex. This phase-two token
        // already freezes queue mutation and remains in `event_bus` through
        // the matching durable purge commit/direct swap.
        let mut durable_sink_entries = Vec::new();
        let mut survivor_report = OwnedQueryResult::empty();
        if report_shape == PurgeReportShape::PerTable {
            survivor_report.columns =
                vec!["table".into(), "rows_affected".into(), "survivors".into()];
        }
        let mut projections = Vec::new();
        let mut affected_rows = BTreeMap::new();
        for (table, ids) in &selected_by_table {
            durable_sink_entries.extend(self.original_discard_durable_sink_entries(
                frame,
                encoded_source,
                table,
                ids,
                sink_names,
            )?);
            let before = selected_table_rows
                .get(table)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let after = if self.relational_store.uses_disk_backed_committed_bodies() {
                if report_requested && table == "work_jobs" {
                    self.committed_rows_at(table, prepared_snapshot, read_memory.clone())?
                        .into_iter()
                        .filter(|row| !ids.contains(&row.row_id))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else {
                self.relational_store
                    .tables
                    .read()
                    .get(table)
                    .into_iter()
                    .flatten()
                    .filter(|row| !ids.contains(&row.row_id))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let report = Self::prepare_authoritative_purge_survivor_report(
                report_requested,
                table,
                ids,
                before,
                &after,
                prepared_snapshot,
            )?;
            if report_shape == PurgeReportShape::PerTable {
                survivor_report.rows.push(vec![
                    Value::Text(table.clone()),
                    Value::Int64(ids.len() as i64),
                    Value::Json(
                        serde_json::from_slice::<serde_json::Value>(
                            &frame.canonical(&report.rows)?,
                        )
                        .map_err(|e| Error::Other((e.to_string()).into()))?,
                    ),
                ]);
            } else if report_shape == PurgeReportShape::Single {
                // A single-table selection preserves the established single-table report.
                survivor_report = report;
            }
            if !self.relational_store.table_meta.read().contains_key(table) {
                return Err(Error::TableNotFound(table.clone()));
            }
            projections.push((table.clone(), after.clone()));
            affected_rows.insert(
                table.clone(),
                if self.relational_store.uses_disk_backed_committed_bodies() {
                    Vec::new()
                } else {
                    after
                },
            );
        }
        let relational = self
            .relational_store
            .prepare_erasure_publication(projections, selected)?;
        // The fallible spool iterator latches storage/admission failure. It is
        // checked before a partially prepared history replacement can escape.
        let count = discard_history::row_count(history.write())?;
        let mut ordinal = 0u64;
        let mut failure = None;
        let keys = std::iter::from_fn(|| {
            if ordinal == count || failure.is_some() {
                return None;
            }
            let result = discard_history::with_record::<discard_history::Row, _>(
                history.write(),
                frame.original,
                ROWS,
                ordinal,
                |(_, row, _, lsn)| Ok((*row, *lsn)),
            );
            ordinal += 1;
            match result {
                Ok(key) => Some(key),
                Err(error) => {
                    failure = Some(error);
                    None
                }
            }
        });
        let erased_history = self
            .relational_store
            .prepare_schema_history_erasure(keys, read_memory.clone())?;
        if let Some(error) = failure {
            return Err(error);
        }
        let mut relational = relational.with_history(erased_history);
        relational.admit_current_memberships(read_memory.clone())?;
        // This operation requires file backing. Durable history stays in its
        // rooted source; publication does not rebuild a retained log Vec.
        let replacement_change_log = Vec::new();
        let mut replacement_change_log_table_index = ChangeLogTableIndex::new();
        let mut replacement_change_log_lsn_refcounts = ChangeLogLsnRefcounts::new();
        record_change_log_entries(
            &mut replacement_change_log_table_index,
            &mut replacement_change_log_lsn_refcounts,
            &replacement_change_log,
        );
        let survivor_graph_entries = surviving_forward_entries;
        let graph = GraphStore::prepare_received_schema_publication(
            if self.graph_store.is_source_backed() {
                Vec::new()
            } else {
                survivor_graph_entries
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect()
            },
        );
        let vector_registry_entries = all_partitioned_vector_entries
            .into_iter()
            .filter(|entry| !selected(&entry.entry.index.table, &entry.entry.row_id))
            .collect::<Vec<_>>();
        // Borrow the live registry to select declarations before copying;
        // unrelated columns retain their exact cells and policy owners.
        let vector_schemas = self.vector_store.copy_erasure_layouts(
            |table| selected_by_table.contains_key(table),
            vector_copy_owner,
        )?;
        admit_erasure_vector_input_copies(
            &vector_schemas,
            &vector_registry_entries,
            &affected_vector_partitions,
            vector_copy_owner,
        )?;
        let vector = self
            .vector_store
            .prepare_selected_authoritative_purge_publication(
                vector_schemas.clone(),
                vector_registry_entries.clone(),
                affected_vector_partitions.clone(),
                crate::memory_accounting::persistence_original_projection(frame.original)?
                    .vector_budget()?,
                vector_copy_owner,
            )?;
        let old_edges = if self.graph_store.is_source_backed() {
            Vec::new()
        } else {
            self.collect_original_graph_versions(frame, |_| true, read_memory.clone())?
        };
        let (old_bytes, new_bytes) = {
            let rows = self.relational_store.tables.read();
            let catalog = self.relational_store.table_meta.read();
            let mut old_bytes = 0usize;
            let mut new_bytes = 0usize;
            for (table, after) in &affected_rows {
                if let Some(meta) = catalog.get(table) {
                    old_bytes = old_bytes.saturating_add(self.received_schema_table_rows_bytes(
                        table,
                        meta,
                        rows.get(table).map(Vec::as_slice).unwrap_or(&[]),
                    ));
                    new_bytes = new_bytes
                        .saturating_add(self.received_schema_table_rows_bytes(table, meta, after));
                }
            }
            for edge in &old_edges {
                old_bytes = old_bytes.saturating_add(edge.retained_graph_bytes());
            }
            for edge in &survivor_graph_entries {
                new_bytes = new_bytes.saturating_add(edge.retained_graph_bytes());
            }
            (old_bytes, new_bytes)
        };
        let memory_swap = PreparedMemorySwap::accounting(old_bytes, new_bytes, 0);
        frame.array::<(String, Vec<u8>)>(lineage_config_owners.len())?;
        let mut accepted_author_memory_keys_removed =
            Vec::with_capacity(lineage_config_owners.len());
        for owner in &lineage_config_owners {
            frame.reserve(
                owner
                    .accepted_author_memory_key
                    .0
                    .len()
                    .saturating_add(owner.accepted_author_memory_key.1.len()),
            )?;
            accepted_author_memory_keys_removed.push(owner.accepted_author_memory_key.clone());
        }
        let publication_replacements = OriginalPurgeNonEventPublication {
            relational,
            change_log: replacement_change_log,
            change_log_table_index: replacement_change_log_table_index,
            change_log_lsn_refcounts: replacement_change_log_lsn_refcounts,
            graph,
            vector,
            accepted_author_memory_keys_removed,
            graph_arrival_memory_removed: removed_graph_arrivals,
            memory_swap,
        };
        Ok(OriginalPurgeNonEventPreparedSet {
            vector_entries,
            vector_partition_identities,
            disk_source_provenance,
            lineage_config_owners,
            durable_sink_entries,
            graph_arrival_config_keys,
            publication_replacements,
            survivor_report,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn original_discard_durable_sink_entries(
        &self,
        frame: &crate::persistence::OriginalPersistenceFrame<'_>,
        source: &crate::persistence::OwnedEncodedEventSource<'_>,
        table: &str,
        selected: &HashSet<RowId>,
        names: &BTreeSet<String>,
    ) -> Result<Vec<AuthoritativePurgeSinkEntryKey>> {
        let mut count = 0usize;
        for sink in names {
            source.visit(sink, |_, bytes| {
                let entry = RedbPersistence::decode_owned_event::<event_bus::SinkQueueEntry>(
                    frame.original,
                    bytes,
                )?;
                if entry.value.event.table == table && selected.contains(&entry.value.row_id) {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::Other("discard sink entry count overflow".into()))?;
                }
                Ok(())
            })?;
        }
        frame.array::<AuthoritativePurgeSinkEntryKey>(count)?;
        let mut entries = Vec::with_capacity(count);
        for sink in names {
            source.visit(sink, |id, bytes| {
                let entry = RedbPersistence::decode_owned_event::<event_bus::SinkQueueEntry>(
                    frame.original,
                    bytes,
                )?;
                if entry.value.event.table == table && selected.contains(&entry.value.row_id) {
                    entries.push(AuthoritativePurgeSinkEntryKey {
                        sink: frame.copy_text(sink)?,
                        queue_id: id,
                        row_id: entry.value.row_id,
                        durable_digest: Some(*blake3::hash(bytes).as_bytes()),
                    });
                }
                Ok(())
            })?;
        }
        Ok(entries)
    }
}

#[cfg(feature = "sync-orchestration")]
impl Database {
    /// Read a committed incarnation with the SAME begin_request identity.
    /// The delivery caller must call this independently before fetch/cursor
    /// validation and again for its response, as its Legacy handler does.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn existing_sync_incarnation_native(
        &self,
        tenant: &TenantId,
        operation: &crate::owner_read::NativeReadRequest,
        context: &crate::executor::bounded::NativeReadOpenContext,
    ) -> std::result::Result<
        Option<Incarnation>,
        crate::persistence::writer_source::NativeConfigFailure,
    > {
        use crate::persistence::native_storage_admission::NativeStorageFailure;
        use crate::persistence::writer_source::NativeConfigFailure;
        let _operation = self
            .open_operation()
            .map_err(|e| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e)))?;
        context
            .checkpoint()
            .map_err(|e| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e)))?;
        if !operation.owner().same_owner(
            &context
                .owner()
                .map_err(|e| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e)))?,
        ) {
            return Err(NativeConfigFailure::OwnerMismatch);
        }
        if let Some(incarnation) = self.sync_incarnations.lock().get(tenant.as_str()).copied() {
            return Ok(Some(incarnation));
        }
        if self.persistence.is_none() {
            return Ok(None);
        }
        self.read_native_committed_config_text("sync_incarnation", tenant, operation, context)?
            .map(|window| window.incarnation())
            .transpose()
    }
}

#[cfg(feature = "test-seams")]
impl Database {
    /// Measurement only: run the owned no-op received-schema flush for one
    /// probe DDL entry at the next LSN, optionally with the growth admission of
    /// its DDL-history identity mutations refusing as an exhausted MEMORY_LIMIT
    /// does.
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __flush_received_noop_ddl_for_test(&self, refuse_identity_growth: bool) -> Result<()> {
        let persistence = self.persistence.as_ref().expect("file-backed fixture");
        crate::persistence::refuse_identity_growth_at_for_test(
            refuse_identity_growth.then_some("received_noop_ddl_history"),
        );
        let result = persistence.flush_received_schema_noop_ddl_for_test(
            self.accountant.clone(),
            Lsn(self.current_lsn().0 + 1),
            crate::sync_types::DdlChange::DropTable {
                name: "identity_refusal_probe".into(),
            },
        );
        crate::persistence::refuse_identity_growth_at_for_test(None);
        result
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn accepted_commit_state_changes(
        &self,
        read: contextdb_tx::manager::EngineRead<'_>,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<
        crate::custody::preparation::OwnedPreparationValue<Vec<(String, uuid::Uuid, String, TxId)>>,
    > {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let bytes = read.relational_inserts.iter().fold(
            read.relational_inserts
                .len()
                .saturating_mul(std::mem::size_of::<(String, uuid::Uuid, String, TxId)>()),
            |bytes, (table, row)| {
                bytes.saturating_add(table.len()).saturating_add(
                    row.values
                        .values()
                        .filter_map(Value::as_text)
                        .map(str::len)
                        .sum::<usize>(),
                )
            },
        );
        crate::custody::preparation::OwnedPreparationValue::try_produce(self, bytes, || {
            let mut changes = Vec::with_capacity(read.relational_inserts.len());
            for (position, (table, row)) in read.relational_inserts.iter().enumerate() {
                if read.relational_inserts[position + 1..]
                    .iter()
                    .any(|(name, later)| name == table && later.row_id == row.row_id)
                {
                    continue;
                }
                let Some(meta) = self.table_meta(table) else {
                    continue;
                };
                let Some(sm) = meta.state_machine else {
                    continue;
                };
                let Some(state) = row.values.get(&sm.column).and_then(Value::as_text) else {
                    continue;
                };
                let Some(uuid) = row.values.get("id").and_then(Value::as_uuid).copied() else {
                    continue;
                };
                if !self.propagation_rules_can_react(table, state) {
                    continue;
                }
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let before = self.committed_row_at_admitted(
                    table,
                    row.row_id,
                    snapshot,
                    &mut credit,
                    read_memory.clone(),
                )?;
                if before
                    .as_ref()
                    .and_then(|row| row.values.get(&sm.column))
                    .and_then(Value::as_text)
                    == Some(state)
                {
                    continue;
                }
                if before.is_none()
                    && read
                        .relational_deletes
                        .iter()
                        .any(|(name, id, _)| name == table && *id == row.row_id)
                {
                    continue;
                }
                changes.push((table.clone(), uuid, state.to_owned(), row.created_tx));
            }
            Ok(changes)
        })
    }
}

impl Database {
    /// Delivery records keep their established named MessagePack encoding.
    /// The temporary encoder and key die before their original reservation;
    /// the lower auxiliary target receives a distinct genuine source invoice.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn append_original_delivery_record(
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        original: &OwnedMemoryReservation,
        record: &crate::custody::records::Record,
    ) -> Result<()> {
        struct Counter(usize);
        impl std::io::Write for Counter {
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
        record.verify()?;
        let mut count = Counter(0);
        serde::Serialize::serialize(
            record,
            &mut rmp_serde::Serializer::new(&mut count).with_struct_map(),
        )
        .map_err(|error| Error::SyncError((format!("delivery metadata encode: {error}")).into()))?;
        // Keys contain the record namespace and bounded digest/identity
        // encodings. Retain overlap with their construction and named record.
        let temporary = crate::memory_accounting::reserve_preparation_from_original(
            original,
            count
                .0
                .checked_mul(6)
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(EngineMetadataJournal::identity_error)?,
            "delivery",
            "terminal_record_encoding",
            "Raise MEMORY_LIMIT to prepare delivery metadata.",
        )?;
        let key = record.key()?;
        let mut encoded = Vec::with_capacity(count.0);
        serde::Serialize::serialize(
            record,
            &mut rmp_serde::Serializer::new(&mut encoded).with_struct_map(),
        )
        .map_err(|error| Error::SyncError((format!("delivery metadata encode: {error}")).into()))?;
        if encoded.len() != count.0 || encoded.capacity() != count.0 {
            return Err(EngineMetadataJournal::identity_error());
        }
        let body = (key, encoded);
        let copied = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(
            std::slice::from_ref(&body),
        ))?;
        let prepared = held.prepare_aux_splice(held.read().config_writes.len(), &[], copied)?;
        held.publish_tx(prepared);
        drop(body);
        drop(temporary);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_owned_ordinary(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
        sync_pull_trigger_audit_projection: Option<&BTreeMap<String, TriggerDeclaration>>,
        sync_trigger_derived_rows_are_local: bool,
        mut delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<CommitValidationOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let discard_selections = self.capture_original_discard_selections(host, tx)?;
        let _discard_fence_memory = host.events.with_owned_persistence_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            frame.tree::<String, ()>(discard_selections.values.len().saturating_mul(2))?;
            for selection in &discard_selections.values {
                frame.reserve(selection.table.len().saturating_mul(2))?;
            }
            Ok(frame.into_reservation())
        })?;
        let discard_drain = std::cell::RefCell::new(if discard_selections.values.is_empty() {
            None
        } else {
            Some(self.event_bus.begin_authoritative_purge_queue_drain()?)
        });
        let mut discard_hashes = if discard_selections.values.is_empty() {
            None
        } else {
            Some(self.capture_original_discard_blob_hashes(
                host,
                &discard_selections.values,
                read_memory.clone(),
            )?)
        };
        let mut discard_blob_guard = match discard_hashes.as_ref() {
            Some(selector) => Some(
                self.blob_repository
                    .acquire_exclusive_selector(selector.clone())?,
            ),
            None => None,
        };
        let discard_publication = if discard_selections.values.is_empty() {
            None
        } else {
            let mut tables = BTreeSet::new();
            for selection in &discard_selections.values {
                if !tables.contains(&selection.table) {
                    tables.insert(selection.table.clone());
                }
            }
            Some(self.read_publication_gate.acquire_write(&tables))
        };
        let mut discard_terminal = discard::OriginalDiscardTerminal::new();
        let vectors = std::cell::RefCell::new(vector_copy_owner);
        let run = std::cell::RefCell::new(trigger::TriggerDispatchRun::owned(self)?);
        let sync_dispatch = std::cell::Cell::new(false);
        let sync_baseline = std::cell::RefCell::new(None::<OriginalOrdinarySyncBaseline>);
        let validation_noops = std::cell::Cell::new(0u64);
        let custody_delta = std::cell::RefCell::new(None);
        let in_memory_lineage = std::cell::RefCell::new(None);
        let graph_arrivals = std::cell::RefCell::new(None::<OriginalOrdinaryGraphArrivals>);
        let trigger_registration =
            std::cell::RefCell::new(None::<trigger::OriginalTriggerDdlRegistration>);
        let event_ddl = std::cell::RefCell::new(None::<event_bus::OwnedStagedEventBusDdlCommit>);
        let event_registration =
            std::cell::RefCell::new(None::<event_bus::OwnedEventStageRegistration>);
        let event_publication = std::cell::RefCell::new(None::<event_bus::OwnedCommitEventStage>);
        let audit_memory = std::cell::RefCell::new(None::<trigger::OriginalAuditMemoryPublication>);
        let audit_entries = std::cell::RefCell::new(None::<trigger::OriginalOrdinaryAudits>);
        let delete_release = std::cell::RefCell::new(DeleteReleaseBytes::default());
        let publication_watch = std::cell::RefCell::new(None::<PublicationInterruptionWatch>);
        let local_schema_registration =
            std::cell::RefCell::new(None::<schema_stage_registry::LocalRegistration>);
        let removal_guard = std::cell::RefCell::new(None::<ActiveRemovalPassGuard>);
        let snapshot_registration =
            std::cell::RefCell::new(None::<CommitStageRegistration<Arc<[SnapshotId]>>>);
        let terminal = loop {
            let mut expanded = None;
            let attempt = host.events.with_owned_persistence_original(|original| {
            if self.tx_mgr.identity()!=host.transactions.identity() {return Err(EngineMetadataJournal::identity_error());}
            discard::with_original_discard_terminal(&mut discard_terminal, || Ok(host.commit_interval_terminal_scoped_if(tx, 1, read_memory.clone(),
                || {
                    if let Some(guard)=discard_blob_guard.as_ref() {
                        let published=self.tx_mgr.capture_published_read();
                        let _source=self.enter_writer_read(writer_read::durable_root(published.handle)?,read_memory.clone());
                        let selected=self.capture_original_discard_blob_hashes(host,&discard_selections.values,read_memory.clone())?;
                        if !selected.is_subset_of(guard.selector())? {
                            let root=self.writer_read_root()?.ok_or_else(EngineMetadataJournal::identity_error)?;
                            let mut union=crate::persistence::blob_selector::BlobSelectorBuilder::new(
                                self.persistence.clone().ok_or_else(EngineMetadataJournal::identity_error)?,root,&host.events,original)?;
                            union.extend(discard_hashes.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?)?; union.extend(&selected)?;
                            expanded=Some(union.seal()?);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
                || {
                    let events = !event_bus_ddl.is_empty() && self.event_bus_definitions_with_ddl(None, event_bus_ddl)?
                        != *self.event_bus.definitions.lock();
                    let triggers = if trigger_ddl.is_empty() { false } else {
                        self.prepare_original_trigger_projection(original, trigger_ddl)?.declarations
                            != *self.trigger.declarations.lock() || Self::trigger_ddl_has_transient_lifecycle(trigger_ddl)
                    };
                    Ok(events || triggers)
                },
                |scope, _start, lsn, visibility| {
                    let published = self.tx_mgr.capture_published_read();
                    let _writer_read = self.enter_writer_read(writer_read::durable_root(published.handle)
                        .map_err(TemporaryFailure::from_error)?, read_memory.clone());
                    if !discard_selections.values.is_empty() {
                        scope.prepare_temporary(tx, |_| Ok(()), |held, _peer, ()| {
                            held.with_tx_checkpoint(|held| {
                                for position in (0..held.read().relational_deletes.len()).rev() {
                                    let remove = {
                                        let read = held.read();
                                        let (table, row, _) = &read.relational_deletes[position];
                                        discard_selections.values.iter().any(|selected| selected.table == *table && selected.local_row_id == *row)
                                    };
                                    if remove {
                                        let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::RelationalDeletes(&[]))?;
                                        let prepared = held.prepare_aux_splice(position, &[position], empty)?;
                                        held.publish_tx(prepared);
                                    }
                                }
                                for selected in &discard_selections.values {
                                    let window = crate::persistence::OriginalPersistenceFrame::new(original)?;
                                    window.reserve(selected.table.len().saturating_add(discard::discard_selection_copy_bytes(selected)?).saturating_mul(4).saturating_add(1024))?;
                                    let key = Self::durable_row_lineage_config_key(&selected.table, &selected.natural_key, selected.table_generation);
                                    let copied = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigDeletes(std::slice::from_ref(&key)))?;
                                    let prepared = held.prepare_aux_splice(held.read().config_deletes.len(), &[], copied)?;
                                    held.publish_tx(prepared);
                                }
                                Ok(())
                            })
                        })?;
                    }
                    *local_schema_registration.borrow_mut() = self.prepare_and_register_local_schema_stage(
                        tx, visibility, lsn, read_memory.clone()).map_err(TemporaryFailure::from_error)?;
                    self.tx_mgr.with_ordinary_read(tx, |ws| self.reject_purged_lineage_recreation(ws, read_memory.clone()))
                        .map_err(TemporaryFailure::from_error)?.map_err(TemporaryFailure::from_error)?;
                    scope.prepare_temporary(tx, |_| Ok(()), |held, _peer, ()| {
                        held.with_tx_checkpoint(|held| {
                            for position in 0..held.read().relational_inserts.len() {
                                let prepared = {
                                    let read = held.read();
                                    let (table, _) = &read.relational_inserts[position];
                                    self.with_owned_delivery_meta_if_present(table, |meta|
                                        held.prepare_rebind_row_at_from_meta(position, table, meta))?.flatten()
                                };
                                if let Some(prepared) = prepared { held.publish(prepared); }
                            }
                            Ok(())
                        })
                    })?;
                    if source != CommitSource::SyncPull {
                        let outcome = self.prepare_active_trigger_write_set_for_dispatch_owned(scope, tx,
                            &mut vectors.borrow_mut(), read_memory.clone())?;
                        Self::reject_user_conditional_update_conflicts(source, &outcome).map_err(TemporaryFailure::from_error)?;
                        validation_noops.set(validation_noops.get().saturating_add(outcome.conditional_noop_count));
                    }
                    let dispatch = if source == CommitSource::SyncPull {
                        self.has_matching_sync_trigger_for_tx(tx, sync_pull_trigger_audit_projection)
                    } else { self.has_matching_local_trigger_for_tx(tx, read_memory.clone()) }
                        .map_err(TemporaryFailure::from_error)?;
                    sync_dispatch.set(source == CommitSource::SyncPull && dispatch);
                    if dispatch {
                        if source == CommitSource::SyncPull {
                            let outcome = self.prepare_active_sync_trigger_write_set_for_dispatch_owned(scope, tx)?;
                            validation_noops.set(validation_noops.get().saturating_add(outcome.conditional_noop_count));
                            *sync_baseline.borrow_mut() = Some(self.capture_original_ordinary_sync_baseline(tx, original)
                                .map_err(TemporaryFailure::from_error)?);
                        }
                        self.dispatch_triggers_for_tx_owned(scope, tx, sync_pull_trigger_audit_projection,
                            source == CommitSource::SyncPull, &mut run.borrow_mut(),
                            &mut vectors.borrow_mut(), read_memory.clone())?;
                        if source == CommitSource::SyncPull && !sync_trigger_derived_rows_are_local {
                            scope.prepare_temporary(tx, |_| Ok(()), |held, _peer, ()| {
                                let baseline = sync_baseline.borrow();
                                let baseline = baseline.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                                held.with_tx_checkpoint(|held| {
                                    for position in 0..held.read().relational_inserts.len() {
                                        let prepared = {
                                            let read = held.read();
                                            let (table, row) = &read.relational_inserts[position];
                                            if baseline.contains_row(table, row.row_id) { continue; }
                                            held.prepare_sidecars(table, row.row_id, Some((SYNC_SOURCE_LSN_OWN_COMMIT, 0)),
                                                read.relational_insert_admitted_bytes.get(table).and_then(|rows| rows.get(&row.row_id)).copied())?
                                        };
                                        held.publish_tx(prepared);
                                    }
                                    Ok(())
                                })
                            })?;
                        }
                        let outcome = if source == CommitSource::SyncPull {
                            self.prepare_active_sync_trigger_write_set_for_dispatch_owned(scope, tx)?
                        } else { self.prepare_active_trigger_write_set_for_dispatch_owned(scope, tx,
                            &mut vectors.borrow_mut(), read_memory.clone())? };
                        Self::reject_user_conditional_update_conflicts(source, &outcome).map_err(TemporaryFailure::from_error)?;
                        validation_noops.set(validation_noops.get().saturating_add(outcome.conditional_noop_count));
                    }
                    Ok(())
                },
                |held, peer, identity| {
                    let selected = peer.node.as_ref().map(|node| node.get().metadata.discard_selections.values.as_slice()).unwrap_or(&[]);
                    if selected.len() != discard_selections.values.len() || selected.iter().zip(&discard_selections.values)
                        .any(|(actual, expected)| actual.body.as_ref() != Some(expected)) {
                        return Err(EngineMetadataJournal::identity_error());
                    }
                    if held.read().is_empty() { return Ok(()); }
                    if source == CommitSource::SyncPull {
                        if sync_dispatch.get() {
                            let baseline = sync_baseline.borrow();
                            let baseline = baseline.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                            self.rewrite_txid_placeholders_after_sync_baseline(tx, held, baseline)?;
                        }
                        held.with_tx_checkpoint(|held| {
                            for position in (0..held.read().config_writes.len()).rev() {
                                let remove = {
                                    let read = held.read();
                                    let key = &read.config_writes[position].0;
                                    key.starts_with("sync_creation_lineage.v1.") && (!sync_trigger_derived_rows_are_local
                                        || sync_baseline.borrow().as_ref().is_some_and(|baseline| baseline.creation_keys.iter().any(|old| old == key))
                                        || self.private_source_creation_key(tx, key))
                                };
                                if remove {
                                    let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(&[]))?;
                                    let prepared = held.prepare_aux_splice(position, &[position], empty)?;
                                    held.publish_tx(prepared);
                                }
                            }
                            Ok(())
                        })?;
                    }
                    self.stamp_unbound_creation_lineages_in_prepared_write_set(host, original, held)?;
                    if source != CommitSource::SyncPull { self.rewrite_txid_placeholders_in_prepared_write_set(tx, held)?; }
                    held.with_legacy(|ws| self.reject_purged_lineage_recreation(ws, read_memory.clone()))?;
                    let outcome = self.commit_validate_in_prepared_write_set(tx, held, peer,
                        &mut vectors.borrow_mut(), read_memory.clone())?;
                    Self::reject_user_conditional_update_conflicts(source, &outcome)?;
                    validation_noops.set(validation_noops.get().saturating_add(outcome.conditional_noop_count));
                    let lsn = (*held.read().commit_lsn).ok_or_else(EngineMetadataJournal::identity_error)?;
                    if self.prepared_source_could_empty_vector_partition(held, original, read_memory.clone())? {
                        let excluded = COMMITTING_STATEMENT_SNAPSHOT_REGISTRATION.with(std::cell::Cell::get);
                        *removal_guard.borrow_mut() = Some(self.snapshot_registry.begin_removal_pass_excluding(self.tx_mgr.current_tx_max(), excluded));
                    }
                    let snapshots = removal_guard.borrow().as_ref()
                        .map(|guard| guard.registered_snapshots.clone()).unwrap_or_default();
                    if self.vector_partition_snapshot_stages.lock().insert(lsn, snapshots).is_some() {
                        return Err(Error::Other("vector snapshot stage already exists for commit LSN".into()));
                    }
                    *snapshot_registration.borrow_mut() = Some(CommitStageRegistration::new(&self.vector_partition_snapshot_stages, lsn));
                    *graph_arrivals.borrow_mut() = Some(self.stage_original_graph_arrivals(held, original,
                        source == CommitSource::SyncPull && !sync_trigger_derived_rows_are_local)?);
                    let (trigger_start, ddl) = self.stage_owned_event_bus_ddl_for_commit(&host.events, lsn,
                        event_bus_ddl, read_memory.clone())?;
                    *event_ddl.borrow_mut() = ddl;
                    *trigger_registration.borrow_mut() = self.stage_original_trigger_ddl(host, original, identity,
                        lsn, trigger_ddl, trigger_start, read_memory.clone())?;
                    held.with_legacy(|ws| self.plugin.pre_commit(ws, source))?;
                    if let Some(node) = peer.node.as_ref() {
                        for entry in &node.get().metadata.delivery_registrations.values {
                            let registration = entry.body.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                            for record in registration.prepare(self, held, read_memory.clone())? {
                                Self::append_original_delivery_record(held, original, &record)?;
                            }
                        }
                    }
                    if let Some(prepare) = delivery_prepare.as_mut() {
                        for record in prepare.prepare_owned(self, held)? {
                            Self::append_original_delivery_record(held, original, &record)?;
                        }
                    }
                    *custody_delta.borrow_mut() = self.prepare_custody_commit_in_prepared_write_set(host, original,
                        held, read_memory.clone())?;
                    let events = self.prepare_owned_sink_events_in_prepared_write_set(&host.events, host,
                        held, event_bus_ddl, read_memory.clone())?;
                    if self.persistence.is_some() && !events.is_empty() {
                        let copied = Self::prepare_original_terminal_config(held, original,
                            event_bus::NEXT_ACTION_ID_CONFIG_KEY, &self.event_bus.next_action_id())?;
                        let prepared = held.prepare_aux_splice(held.read().config_writes.len(), &[], copied)?; held.publish_tx(prepared);
                    }
                    let schema = self.local_schema_stages.lock().get(&lsn)
                        .and_then(|stage| stage.native_event_bus.borrow_mut().take());
                    let mut event_stage = event_bus::OwnedCommitEventStage {
                        identity, events, ddl: event_ddl.borrow_mut().take(), schema,
                    };
                    if let Some(drain) = discard_drain.borrow_mut().take() {
                        // Fold this commit's own definition leases into its append
                        // before freezing the queue, so it cannot wait on itself.
                        host.events.merge_owned_commit_escrow(&mut event_stage)?;
                        discard::bind_original_discard_identity(identity)?;
                        let holder = self.prepare_original_discard_commit(host, original, identity,
                            held, &discard_selections.values, drain,
                            discard_blob_guard.as_ref().expect("discard hash fence"),
                            discard_publication.as_ref().expect("discard publication fence"),
                            &mut event_stage.events, read_memory.clone())?;
                        discard::install_original_discard_stage(holder)?;
                        *custody_delta.borrow_mut() = self.prepare_custody_commit_in_prepared_write_set(host,
                            original, held, read_memory.clone())?;
                    }
                    *event_registration.borrow_mut() = Some(host.events.stage_owned_commit_events(lsn, event_stage)?);
                    *audit_entries.borrow_mut() = Some(held.with_legacy(|ws|
                        self.prepare_original_ordinary_audits(original, ws, source, lsn, &run.borrow().pending,
                            sync_pull_trigger_audit_projection, read_memory.clone()))?);
                    if !audit_entries.borrow().as_ref().ok_or_else(EngineMetadataJournal::identity_error)?.entries.is_empty() {
                        *audit_memory.borrow_mut() = Some(self.prepare_original_audit_memory(original,
                            &audit_entries.borrow().as_ref().ok_or_else(EngineMetadataJournal::identity_error)?.entries)?);
                    }
                    if self.persistence.is_some() { self.trigger.stage_original_persistence_audits(original, lsn,
                        &audit_entries.borrow().as_ref().ok_or_else(EngineMetadataJournal::identity_error)?.entries)?; }
                    *delete_release.borrow_mut() = held.with_legacy(|ws| self.delete_release_bytes_for_write_set(
                        ws, &mut vectors.borrow_mut(), read_memory.clone()))?;
                    if self.persistence.is_none() {
                        *in_memory_lineage.borrow_mut() = Some(held.with_legacy(|ws|
                            self.prepare_in_memory_lineage_delta(&ws.config_writes))?);
                    }
                    *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(&self.publication_interrupted));
                    Ok(())
                },
                |lsn, ws| {
                    if let Some(registration) = snapshot_registration.borrow_mut().as_mut() { registration.ownership_transferred(); }
                    if let Some(registration) = event_registration.borrow_mut().take() {
                        let stage = registration.take().expect("committed original event stage retained");
                        *event_publication.borrow_mut() = Some(match stage.try_into_inner() {
                            Ok(stage) => stage,
                            Err(_) => panic!("committed original event stage still borrowed after apply"),
                        });
                    }
                    if let Some(mut stage) = self.take_local_schema_stage(lsn) {
                        *stage.native_event_bus.get_mut() = event_publication.borrow_mut().as_mut().and_then(|events| events.schema.take());
                        if let Some(registration) = local_schema_registration.borrow_mut().as_mut() { registration.ownership_transferred(); }
                        self.publish_local_schema_stage(stage);
                    }
                    if let Some(delta) = custody_delta.borrow_mut().take() { self.publish_custody_delta(delta); }
                    if let Some(delta) = in_memory_lineage.borrow_mut().take() { self.publish_in_memory_lineage_delta(delta); }
                    if self.persistence.is_none() && let Some(mut arrivals) = graph_arrivals.borrow_mut().take() {
                        self.sync_graph_arrivals.write().extend(arrivals.markers.drain(..));
                    }
                    if let Some(stage) = event_publication.borrow_mut().as_mut()
                        && let Some(ddl) = stage.ddl.take() { self.publish_owned_event_ddl_after_apply(&host.events, lsn, ddl); }
                    if let Some(registration) = trigger_registration.borrow_mut().take() { self.publish_original_trigger_ddl(registration); }
                    if !ws.is_empty() { self.release_delete_allocations_from_bytes(&delete_release.borrow()); }
                })))
          })?;
            match attempt {
                Ok(Some(terminal)) => break Ok(terminal),
                Err(failure) => break Err(failure),
                Ok(None) => {
                    // Every old hash loses exclusion together; reacquire the
                    // complete expanded union before another terminal attempt.
                    drop(discard_blob_guard.take());
                    discard_hashes = Some(
                        expanded
                            .take()
                            .ok_or_else(EngineMetadataJournal::identity_error)?,
                    );
                    discard_blob_guard = Some(
                        self.blob_repository.acquire_exclusive_selector(
                            discard_hashes
                                .as_ref()
                                .ok_or_else(EngineMetadataJournal::identity_error)?
                                .clone(),
                        )?,
                    );
                }
            }
        };
        if let Some(watch) = publication_watch.borrow_mut().take() {
            watch.completed();
        }
        let (lsn, pair) = match terminal {
            Ok(terminal) => terminal,
            Err(failure) => {
                let (error, pair) = failure.into_parts();
                if let Some(pair) = &pair {
                    pair.with_legacy(|ws| {
                        self.plugin
                            .commit_failed(ws, source, error.engine_diagnostic());
                        if let Some(lsn) = ws.commit_lsn {
                            let _ = self.event_bus.take_staged_sink_events_for_persistence(lsn);
                            self.discard_staged_event_bus_ddl_commit(lsn);
                            self.discard_staged_trigger_ddl_commit(lsn);
                            self.discard_staged_trigger_audits_for_persistence(lsn);
                        }
                    });
                }
                self.remove_original_terminal_stages(tx);
                drop(pair);
                let _ = self.append_rolled_back_trigger_audits(&run.borrow().pending, tx, &error);
                return Err(error);
            }
        };
        #[cfg(feature = "test-seams")]
        self.publication_committed_for_test(tx, lsn);
        let committed_tx = pair.with_legacy(|ws| -> Result<Option<TxId>> {
            if !ws.is_empty() {
                self.plugin.post_commit(ws, source);
                if let Some(stage) = event_publication.borrow_mut().take() {
                    host.events
                        .publish_owned_sink_events_to_memory(stage.events);
                }
                if let Some(publication) = audit_memory.borrow_mut().take() {
                    self.publish_original_audit_memory(publication);
                }
                self.publish_commit_event_if_subscribers(ws, source, lsn);
            }
            Ok(ws.finalized_tx)
        })?;
        drop(pair);
        Ok(CommitValidationOutcome {
            committed_tx,
            conditional_noop_count: validation_noops.get(),
            conditional_conflict_count: 0,
        })
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepared_source_could_empty_vector_partition(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        original: &OwnedMemoryReservation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let read = held.read();
        if !read.vector_moves.is_empty()
            || read.vector_deletes.iter().any(|(index, row, _)| {
                !read
                    .vector_inserts
                    .iter()
                    .any(|entry| entry.index == *index && entry.row_id == *row)
            })
        {
            return Ok(true);
        }
        let published = self.tx_mgr.capture_published_read();
        let snapshot = published.snapshot;
        let _writer_read = self.enter_writer_read(
            writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        for (table, row, _) in read.relational_deletes {
            if read
                .relational_inserts
                .iter()
                .any(|(name, entry)| name == table && entry.row_id == *row)
            {
                continue;
            }
            if self
                .with_owned_delivery_meta_if_present(table, |meta| {
                    Ok(meta.columns.iter().any(|column| {
                        matches!(column.column_type, ColumnType::Vector(_))
                            && column.partition_key_columns.is_some()
                    }))
                })?
                .unwrap_or(false)
            {
                return Ok(true);
            }
        }
        for (table, next) in read.relational_inserts {
            let changed = self.with_owned_delivery_meta_if_present(table, |meta| {
                if !meta.columns.iter().any(|column| {
                    matches!(column.column_type, ColumnType::Vector(_))
                        && column.partition_key_columns.is_some()
                }) {
                    return Ok(false);
                }
                let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                let Some(previous) = self.committed_row_at_admitted(
                    table,
                    next.row_id,
                    snapshot,
                    &mut credit,
                    read_memory.clone(),
                )?
                else {
                    return Ok(false);
                };
                for column in &meta.columns {
                    if !matches!(column.column_type, ColumnType::Vector(_))
                        || column.partition_key_columns.is_none()
                    {
                        continue;
                    }
                    let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
                    frame.reserve(table.len().saturating_add(column.name.len()))?;
                    let index = VectorIndexRef {
                        table: crate::memory_accounting::preparation_copy_string(table)?,
                        column: crate::memory_accounting::preparation_copy_string(&column.name)?,
                    };
                    let before =
                        crate::persistence::received_working_vector::derive_partition_original(
                            &frame, &index, meta, column, &previous,
                        )?;
                    let after =
                        crate::persistence::received_working_vector::derive_partition_original(
                            &frame, &index, meta, column, next,
                        )?;
                    if before != after {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
            if changed.unwrap_or(false) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
impl OriginalOrdinarySyncBaseline {
    pub(super) fn contains_row(&self, table: &str, row: RowId) -> bool {
        self.rows
            .iter()
            .any(|(name, id)| name == table && *id == row)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn quote(read: &contextdb_tx::manager::EngineRead<'_>) -> Result<usize> {
        use crate::custody::preparation::preparation_array_bytes;
        let keys = read
            .config_writes
            .iter()
            .filter(|(key, _)| key.starts_with("sync_creation_lineage.v1."));
        let count = keys.clone().count();
        let mut bytes = preparation_array_bytes::<String>(count)?
            .checked_add(preparation_array_bytes::<(String, RowId)>(
                read.relational_inserts.len(),
            )?)
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        for name in keys.map(|(key, _)| key.as_str()).chain(
            read.relational_inserts
                .iter()
                .map(|(name, _)| name.as_str()),
        ) {
            bytes = bytes
                .checked_add(name.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?;
        }
        Ok(bytes)
    }
}
impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn capture_original_ordinary_sync_baseline(
        &self,
        tx: TxId,
        original: &OwnedMemoryReservation,
    ) -> Result<OriginalOrdinarySyncBaseline> {
        loop {
            let bytes = self
                .tx_mgr
                .with_read(tx, |read| OriginalOrdinarySyncBaseline::quote(&read))??;
            let memory = crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "trigger",
                "ordinary_received_baseline",
                "Reduce pending writes or raise MEMORY_LIMIT.",
            )?;
            let copied = self.tx_mgr.with_read(
                tx,
                |read| -> Result<Option<OriginalOrdinarySyncBaseline>> {
                    if OriginalOrdinarySyncBaseline::quote(&read)? > bytes {
                        return Ok(None);
                    }
                    let count = read
                        .config_writes
                        .iter()
                        .filter(|(key, _)| key.starts_with("sync_creation_lineage.v1."))
                        .count();
                    let mut creation_keys = Vec::with_capacity(count);
                    let mut rows = Vec::with_capacity(read.relational_inserts.len());
                    if creation_keys.capacity() != count
                        || rows.capacity() != read.relational_inserts.len()
                    {
                        return Err(EngineMetadataJournal::identity_error());
                    }
                    for (key, _) in read
                        .config_writes
                        .iter()
                        .filter(|(key, _)| key.starts_with("sync_creation_lineage.v1."))
                    {
                        creation_keys.push(crate::memory_accounting::preparation_copy_string(key)?);
                    }
                    for (table, row) in read.relational_inserts {
                        rows.push((
                            crate::memory_accounting::preparation_copy_string(table)?,
                            row.row_id,
                        ));
                    }
                    Ok(Some(OriginalOrdinarySyncBaseline {
                        creation_keys,
                        rows,
                        _memory: memory,
                    }))
                },
            )??;
            if let Some(copied) = copied {
                return Ok(copied);
            }
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rewrite_txid_placeholders_after_sync_baseline(
        &self,
        tx: TxId,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        baseline: &OriginalOrdinarySyncBaseline,
    ) -> Result<()> {
        held.with_tx_checkpoint(|held| {
            for position in 0..held.read().relational_inserts.len() {
                let prepared = {
                    let read = held.read();
                    let (table, row) = &read.relational_inserts[position];
                    if baseline.contains_row(table, row.row_id) { continue; }
                    self.with_owned_delivery_meta_if_present(table, |meta| {
                        if !meta.columns.iter().any(|column| matches!(column.column_type, ColumnType::TxId)
                            && matches!(row.values.get(&column.name), Some(Value::TxId(value)) if *value == tx && *value != row.created_tx)) {
                            return Ok(None);
                        }
                        let mut original = held.copy_existing_row_at(position)?;
                        original.rewrite_txid_placeholders(tx, meta);
                        held.prepare_replace_admitted_row_at_from_meta(position, table, original, meta).map(Some)
                    })?.flatten()
                };
                if let Some(prepared) = prepared { held.publish(prepared); }
            }
            Ok(())
        })
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_original_graph_arrivals(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        original: &OwnedMemoryReservation,
        pulled: bool,
    ) -> Result<OriginalOrdinaryGraphArrivals> {
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        let Some(local_lsn) = *held.read().commit_lsn else {
            return Ok(OriginalOrdinaryGraphArrivals {
                markers: Vec::new(),
                _memory: frame.into_reservation(),
            });
        };
        let marker_count = if self.persistence.is_none() {
            held.read()
                .adj_deletes
                .len()
                .checked_add(held.read().adj_inserts.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?
        } else {
            0
        };
        frame.array::<(GraphIdentity, FundedGraphArrival)>(marker_count)?;
        let mut markers = Vec::with_capacity(marker_count);
        if markers.capacity() != marker_count {
            return Err(EngineMetadataJournal::identity_error());
        }
        // Keep the old delete-before-insert ordering so a reinsert wins a tie.
        for deleted in [true, false] {
            let count = if deleted {
                held.read().adj_deletes.len()
            } else {
                held.read().adj_inserts.len()
            };
            for position in 0..count {
                let window = crate::persistence::OriginalPersistenceFrame::new(original)?;
                let (record, key) = {
                    let read = held.read();
                    let (source, edge_type, target) = if deleted {
                        let (source, kind, target, _) = &read.adj_deletes[position];
                        (*source, kind.as_str(), *target)
                    } else {
                        let edge = &read.adj_inserts[position];
                        (edge.source, edge.edge_type.as_str(), edge.target)
                    };
                    window.reserve(edge_type.len().saturating_mul(4).saturating_add(1024))?;
                    let record = DurableGraphArrival {
                        source,
                        target,
                        edge_type: crate::memory_accounting::preparation_copy_string(edge_type)?,
                        local_lsn,
                        deleted,
                        pulled,
                    };
                    let key = Self::graph_arrival_config_key(&record)?;
                    (record, key)
                };
                let copied = Self::prepare_original_terminal_config(held, original, &key, &record)?;
                let prepared =
                    held.prepare_aux_splice(held.read().config_writes.len(), &[], copied)?;
                held.publish_tx(prepared);
                if self.persistence.is_none() {
                    markers.push(self.prepare_graph_arrival_memory(&record)?);
                }
            }
        }
        // Only the in-memory route returns markers; its map entries retain their
        // independent existing Root funding. Durable routes return an empty Vec.
        Ok(OriginalOrdinaryGraphArrivals {
            markers,
            _memory: frame.into_reservation(),
        })
    }
}
