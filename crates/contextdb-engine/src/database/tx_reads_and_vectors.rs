use super::*;

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_row_at_admitted(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        read_credit: &mut contextdb_core::read_memory::ReadCredit,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.bounded_row_by_identity(
            read_memory.clone(),
            None,
            table,
            row_id,
            snapshot,
            &mut || Ok(()),
            &mut |bytes| {
                read_credit.try_grow_for(
                    bytes,
                    "reader",
                    "conditional_update_source",
                    "Reduce the selected rows or raise the declared read memory limit.",
                )
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn committed_row_at(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            let source = self.statement_relational_source(root, read_memory.clone())?;
            let window =
                contextdb_relational::source::visible_row(&source, table, row_id, snapshot)?;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let row = window
                .map(|window| {
                    output.try_grow_for(
                        window.value().estimated_bytes(),
                        "writer_source",
                        "row_output",
                        "Raise MEMORY_LIMIT to return this row.",
                    )?;
                    Ok(window.value().clone())
                })
                .transpose();
            self.retain_writer_output(output);
            return row;
        }
        if let Some(row) = self.relational_store.row_by_id(table, row_id, snapshot) {
            return Ok(Some(row));
        }
        if self.retained_relational_source.is_some() {
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let row =
                self.committed_row_at_admitted(table, row_id, snapshot, &mut output, read_memory);
            self.retain_writer_output(output);
            return row;
        }
        Ok(None)
    }

    /// Rehydrate the identity selected by the captured, witnessed index point
    /// lookup above without enumerating its retained version manifest again.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_indexed_point_row_at(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(root) = self.writer_read_root()? else {
            return self.committed_row_at(table, row_id, snapshot, read_memory);
        };
        let source = self.statement_relational_source(root, read_memory.clone())?;
        let window = source.indexed_point_row(table, row_id, snapshot)?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let row = window
            .map(|window| {
                output.try_grow_for(
                    window.value().estimated_bytes(),
                    "writer_source",
                    "row_output",
                    "Raise the declared reader memory limit to return this row.",
                )?;
                Ok(window.value().clone())
            })
            .transpose();
        self.retain_writer_output(output);
        row
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn committed_rows_at(
        &self,
        table: &str,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            let source =
                self.statement_relational_source_at_snapshot(root, read_memory.clone(), snapshot)?;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let rows = self
                .relational
                .scan_with_source(None, table, snapshot, &source, |bytes| {
                    output.try_grow_for(
                        bytes,
                        "writer_source",
                        "scan_output",
                        "Raise MEMORY_LIMIT to return these rows.",
                    )
                });
            self.retain_writer_output(output);
            return rows;
        }
        if self.retained_relational_source.is_some() {
            let mut cursor = self.bounded_physical_table_cursor(table)?;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut rows = Vec::new();
            while let Some(row) = self.bounded_physical_table_row_next(
                read_memory.clone(),
                table,
                snapshot,
                &mut cursor,
                || Ok::<(), Error>(()),
                |bytes| {
                    output.try_grow_for(
                        bytes,
                        "reader",
                        "scan_output",
                        "Reduce the selected rows or raise the declared read memory limit.",
                    )
                },
            )? {
                rows.push(row);
            }
            self.retain_writer_output(output);
            return Ok(rows);
        }
        let tables = self.relational_store.tables.read();
        let rows = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        self.relational_store
            .bump_scan_rows_touched(rows.len() as u64);
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let mut result = Vec::new();
        for row in rows.iter().filter(|row| row.visible_at(snapshot)) {
            output.try_grow_for(
                bounded_rank_row_clone_planned_bytes(row)?,
                "reader",
                "scan_output",
                "Reduce the selected rows or raise the executing reader's memory limit.",
            )?;
            result.reserve_exact(1);
            result.push(row.clone());
        }
        self.retain_writer_output(output);
        Ok(result)
    }

    pub(super) fn local_table_projection_streamed(
        &self,
        table: &str,
        arriving: &TableMeta,
    ) -> bool {
        self.relational_store.uses_disk_backed_committed_bodies()
            && self.table_meta(table).is_some_and(|previous| {
                !previous
                    .columns
                    .iter()
                    .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .any(|old| {
                        arriving
                            .columns
                            .iter()
                            .find(|new| new.name == old.name)
                            .is_some_and(|new| {
                                old.column_type != new.column_type
                                    || old.quantization != new.quantization
                                    || old.partition_key_columns != new.partition_key_columns
                            })
                    })
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn local_table_projection_rows(
        &self,
        table: &str,
        arriving: &TableMeta,
    ) -> Result<Vec<VersionedRow>> {
        if self.local_table_projection_streamed(table, arriving) {
            // The durable preparation streams its addressed source directly.
            return Ok(Vec::new());
        }
        #[cfg(test)]
        ef_search_metadata_contract_tests::note_projection_rows(
            self.relational_store
                .tables
                .read()
                .get(table)
                .map_or(0, Vec::len) as u64,
        );
        #[cfg(test)]
        consolidation_metadata_contract_tests::note_projection_rows(1);
        self.table_versions_for_projection(table)
    }

    /// Schema replacement rewrites every retained MVCC version, including
    /// tombstoned history. A file-backed writer therefore has to source the
    /// projection from Redb rather than its lightweight resident directory.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn table_versions_for_projection(&self, table: &str) -> Result<Vec<VersionedRow>> {
        if self.relational_store.uses_disk_backed_committed_bodies() {
            self.table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            let read_memory = self.engine_sync_read_memory()?;
            let root = self.writer_read_root()?.ok_or_else(|| {
                Error::Other(
                    "historical schema projection requires its captured durable root".into(),
                )
            })?;
            let source = self.statement_relational_source(root, read_memory.clone())?;
            source.verify_historical_versions(table)?;
            use contextdb_relational::source::RelationalReadSource;
            let mut cursor = source.row_cursor(
                table,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                false,
            )?;
            let mut rows = Vec::new();
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory);
            while let Some(row) = cursor.next_window()? {
                output.try_grow_for(
                    std::mem::size_of::<VersionedRow>()
                        .checked_add(row.value().estimated_bytes())
                        .ok_or_else(|| {
                            Error::Other("schema projection output size overflow".into())
                        })?,
                    "ddl",
                    "historical_schema_projection",
                    "Raise MEMORY_LIMIT to admit schema projection working memory.",
                )?;
                rows.reserve_exact(1);
                rows.push(row.value().clone());
            }
            self.retain_writer_output(output);
            return Ok(rows);
        }
        Ok(self
            .relational_store
            .tables
            .read()
            .get(table)
            .cloned()
            .unwrap_or_default())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn overlay_transaction_rows(
        &self,
        tx: TxId,
        table: &str,
        rows: &mut Vec<VersionedRow>,
    ) -> Result<()> {
        self.tx_mgr.with_ordinary_read(tx, |ws| {
            let committed_row_ids: HashSet<RowId> = rows.iter().map(|row| row.row_id).collect();
            let deleted_row_ids: HashSet<RowId> = ws
                .relational_deletes
                .iter()
                .filter(|(candidate, _, _)| candidate == table)
                .map(|(_, row_id, _)| *row_id)
                .collect();
            rows.retain(|row| {
                !deleted_row_ids.contains(&row.row_id)
                    && !row_matches_delete_predicates(&ws.relational_delete_predicates, table, row)
            });
            let mut seen = HashSet::new();
            let mut inserts = ws
                .relational_inserts
                .iter()
                .rev()
                .filter(|(candidate, row)| {
                    candidate == table
                        && seen.insert(row.row_id)
                        && (!deleted_row_ids.contains(&row.row_id)
                            || committed_row_ids.contains(&row.row_id))
                })
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            inserts.reverse();
            rows.extend(inserts);
        })
    }

    /// Every row the statement's snapshot (and transaction, if any) can see, before any access
    /// gate. Only for a caller that applies the complete predicate and then the refusing gate.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scan_rows_ungated_for_select(
        &self,
        tx: Option<TxId>,
        table: &str,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(tx) = tx {
            return self.scan_in_tx_raw(tx, table, snapshot, vector_copy_owner, read_memory);
        }
        let _operation = self.open_operation()?;
        let mut rows = self.committed_rows_at(table, snapshot, read_memory.clone())?;
        self.supplement_quantized_vectors(
            table,
            snapshot,
            &mut rows,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(rows)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scan_in_tx_raw(
        &self,
        tx: TxId,
        table: &str,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(any(test, feature = "test-seams"))]
        observe_read_execution_convergence(ReadExecutionConvergenceEvent::EagerRowSourceTouch);
        let mut rows = self.committed_rows_at(table, snapshot, read_memory.clone())?;
        self.overlay_transaction_rows(tx, table, &mut rows)?;
        self.supplement_quantized_vectors(
            table,
            snapshot,
            &mut rows,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(rows)
    }

    /// Compute the in-tx overlay (deleted row_ids + matching staged inserts)
    /// for an index-driven scan of `table` matching `shape` on `column`.
    /// Internal helper for the IndexScan executor arm.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn index_scan_tx_overlay(
        &self,
        tx: TxId,
        table: &str,
        column: &str,
        shape: &crate::executor::IndexPredicateShape,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<IndexScanTxOverlay> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::executor::{IndexPredicateShape, range_includes};
        let mut overlay = IndexScanTxOverlay::default();
        let mut memory = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            0,
            "query",
            "staged_index_output",
            "Reduce query output or raise MEMORY_LIMIT.",
        )?;
        self.tx_mgr.with_ordinary_read(tx, |ws| {
            for (t, _row_id, _) in &ws.relational_deletes {
                if t == table {
                    memory.try_grow_for(
                        64,
                        "query",
                        "staged_index_output",
                        "Reduce query output or raise MEMORY_LIMIT.",
                    )?;
                    overlay.deleted_row_ids.insert(*_row_id);
                }
            }
            for (t, row) in &ws.relational_inserts {
                if t != table {
                    continue;
                }
                let v = row.values.get(column).cloned().unwrap_or(Value::Null);
                let include = match shape {
                    IndexPredicateShape::Equality(target) => v == *target,
                    IndexPredicateShape::NotEqual(target) => v != *target,
                    IndexPredicateShape::InList(list) => list.contains(&v),
                    IndexPredicateShape::Range { lower, upper } => range_includes(&v, lower, upper),
                    IndexPredicateShape::IsNull => v == Value::Null,
                    IndexPredicateShape::IsNotNull => v != Value::Null,
                };
                if include {
                    memory.try_grow_for(
                        row.estimated_bytes()
                            .saturating_add(std::mem::size_of::<VersionedRow>() * 4),
                        "query",
                        "staged_index_output",
                        "Reduce query output or raise MEMORY_LIMIT.",
                    )?;
                    overlay.matching_inserts.push(row.clone());
                }
            }
            Ok::<_, Error>(())
        })??;
        self.retain_writer_output(memory);
        Ok(overlay)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn scan_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.scan_filter_with_read_memory(
            table,
            snapshot,
            predicate,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scan_filter_with_read_memory(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let rows = self.committed_rows_at(table, snapshot, read_memory.clone())?;
        let mut rows = self.filter_rows_for_read(table, rows, snapshot, read_memory.clone())?;
        self.supplement_rows_for_caller(
            table,
            snapshot,
            &mut rows,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(rows.into_iter().filter(|row| predicate(row)).collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.point_lookup_with_read_memory(
            table,
            col,
            value,
            snapshot,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    /// [`Self::point_lookup`] funded by an explicit read budget, for fixtures
    /// that read back rows larger than the default.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn point_lookup_with_read_memory_for_test(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.point_lookup_with_read_memory(
            table,
            col,
            value,
            self.snapshot(),
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn point_lookup_with_read_memory(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        self.assert_table_read_allowed(table)?;
        let Some(row) =
            self.relational_point_with_tx(None, table, col, value, snapshot, read_memory.clone())?
        else {
            return Ok(None);
        };
        let row = self
            .committed_row_at(table, row.row_id, snapshot, read_memory.clone())?
            .ok_or_else(|| {
                crate::funded_error::corruption_text(
                    Ok(read_memory.clone()),
                    table,
                    "selected committed point has no authenticated visible row".into(),
                )
            })?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        if self.read_allowed_for_row(table, &meta, &row, snapshot, read_memory.clone())? {
            let mut found = [row];
            let selected_row_ids = HashSet::from([found[0].row_id]);
            self.vector_store.ensure_raw_partitions_containing_rows(
                table,
                &selected_row_ids,
                vector_copy_owner,
            )?;
            self.supplement_rows_for_caller(
                table,
                snapshot,
                &mut found,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let [row] = found;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn point_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let row = self.relational_point_with_tx(
            Some(tx),
            table,
            col,
            value,
            snapshot,
            read_memory.clone(),
        )?;
        let Some(row) = row else {
            return Ok(None);
        };
        let staged = self.tx_mgr.with_read(tx, |ws| {
            ws.relational_inserts
                .iter()
                .rev()
                .any(|(staged_table, staged)| staged_table == table && staged.row_id == row.row_id)
        })?;
        if staged {
            Ok(Some(row))
        } else {
            self.committed_row_at(table, row.row_id, snapshot, read_memory.clone())
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn record_relational_delete_predicate(
        &self,
        tx: TxId,
        table: String,
        predicates: Vec<(ColName, Value)>,
    ) -> Result<()> {
        if predicates.is_empty() {
            return Ok(());
        }
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            let predicate = RelationalDeletePredicate { table, predicates };
            return handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |held| {
                        if held
                            .read()
                            .relational_delete_predicates
                            .contains(&predicate)
                        {
                            return Ok(());
                        }
                        let batch =
                            held.copy_aux(contextdb_tx::staged_source::AuxBorrow::Predicates(
                                std::slice::from_ref(&predicate),
                            ))?;
                        let prepared = held.prepare_aux_splice(
                            held.read().relational_delete_predicates.len(),
                            &[],
                            batch,
                        )?;
                        held.publish_tx(prepared);
                        Ok(())
                    })
                })
            });
        }
        self.tx_mgr.with_write_set(tx, |ws| {
            let predicate = RelationalDeletePredicate { table, predicates };
            if !ws.relational_delete_predicates.contains(&predicate) {
                ws.relational_delete_predicates.push(predicate);
            }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn conflict_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        columns: &[ColName],
        values: &[Value],
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if columns.is_empty() || columns.len() != values.len() {
            return Err(Error::Other(
                ("ON CONFLICT target must include matching columns and values".to_string()).into(),
            ));
        }
        if let ([column], [value]) = (columns, values) {
            return self.point_lookup_in_tx(
                tx,
                table,
                column,
                value,
                snapshot,
                read_memory.clone(),
            );
        }
        let deleted_row_ids = self
            .tx_mgr
            .with_ordinary_read(tx, |ws| Self::deleted_row_ids_for_table(ws, table))?;
        if let Some(committed) = self.committed_unique_conflict(
            table,
            columns,
            values,
            snapshot,
            &deleted_row_ids,
            None,
            read_memory.clone(),
        )? {
            return Ok(Some(committed));
        }

        if self.writer_read_root()?.is_some() {
            return self.writer_staged_row_matching(
                tx,
                table,
                snapshot,
                true,
                |row| {
                    columns
                        .iter()
                        .zip(values)
                        .all(|(column, value)| row.values.get(column) == Some(value))
                },
                read_memory.clone(),
            );
        }

        self.tx_mgr.with_ordinary_read(tx, |ws| {
            let mut seen_inserts = HashSet::new();
            ws.relational_inserts
                .iter()
                .rev()
                .filter(|(insert_table, row)| {
                    insert_table == table
                        && seen_inserts.insert(row.row_id)
                        && (!deleted_row_ids.contains(&row.row_id)
                            || self
                                .relational_store
                                .row_by_id(table, row.row_id, snapshot)
                                .is_some())
                })
                .find(|(_, row)| {
                    columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
                })
                .map(|(_, row)| row.clone())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn unique_row_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        columns: &[ColName],
        values: &[Value],
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }

        let skip_deleted = HashSet::new();
        if let Some(committed) = self.committed_unique_conflict(
            table,
            columns,
            values,
            snapshot,
            &skip_deleted,
            None,
            read_memory.clone(),
        )? {
            let deleted_in_tx = self.tx_mgr.with_ordinary_read(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .any(|(delete_table, row_id, _)| {
                        delete_table == table && *row_id == committed.row_id
                    })
            })?;
            if !deleted_in_tx {
                return Ok(Some(committed));
            }
        }

        if self.writer_read_root()?.is_some() {
            return self.writer_staged_row_matching(
                tx,
                table,
                snapshot,
                true,
                |row| {
                    columns
                        .iter()
                        .zip(values)
                        .all(|(column, value)| row.values.get(column) == Some(value))
                },
                read_memory.clone(),
            );
        }

        if let Some(staged) = self.tx_mgr.with_ordinary_read(tx, |ws| {
            ws.relational_inserts
                .iter()
                .rev()
                .find_map(|(insert_table, row)| {
                    if insert_table != table
                        || tuple_values_for_row(row, columns).as_deref() != Some(values)
                    {
                        return None;
                    }
                    let deleted_in_tx =
                        ws.relational_deletes
                            .iter()
                            .any(|(delete_table, row_id, _)| {
                                delete_table == table && *row_id == row.row_id
                            });
                    let replaces_committed_row = deleted_in_tx
                        && self
                            .relational_store
                            .row_by_id(table, row.row_id, snapshot)
                            .is_some();
                    (!deleted_in_tx || replaces_committed_row).then(|| row.clone())
                })
        })? {
            return Ok(Some(staged));
        }

        Ok(None)
    }

    pub(crate) fn logical_row_ids_for_uuid(
        &self,
        tx: TxId,
        table: &str,
        uuid: uuid::Uuid,
    ) -> Vec<RowId> {
        let mut row_ids = HashSet::new();

        if let Some(rows) = self.relational_store.tables.read().get(table) {
            for row in rows {
                if row.values.get("id") == Some(&Value::Uuid(uuid)) {
                    row_ids.insert(row.row_id);
                }
            }
        }

        let _ = self.tx_mgr.with_ordinary_read(tx, |ws| {
            for (insert_table, row) in &ws.relational_inserts {
                if insert_table == table && row.values.get("id") == Some(&Value::Uuid(uuid)) {
                    row_ids.insert(row.row_id);
                }
            }
        });

        row_ids.into_iter().collect()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: HashMap<String, Value>,
    ) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.insert_edge_with_read_memory(tx, source, target, edge_type, properties, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_edge_with_read_memory(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        mut properties: HashMap<String, Value>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation_after_write_control_wait(tx, "insert_edge")?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let snapshot = self.snapshot();
        self.assert_node_write_allowed(source, snapshot, read_memory.clone())?;
        self.assert_node_write_allowed(target, snapshot, read_memory.clone())?;
        self.assert_graph_edge_write_allowed(
            Some(tx),
            source,
            target,
            &edge_type,
            snapshot,
            read_memory.clone(),
        )?;
        let existing_row = self.has_implicit_row_graph_binding(
            tx,
            source,
            target,
            &edge_type,
            read_memory.clone(),
        )?;
        if existing_row && properties.is_empty() {
            return Ok(false);
        }
        if matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            // The graph executor creates independent source-funded originals;
            // the Legacy logical estimate must not debit or refund them again.
            return self
                .graph
                .insert_edge(tx, source, target, edge_type, properties, read_memory)
                .map(|inserted| inserted && !existing_row);
        }
        // An existing edge can stage a replacement while still returning
        // false. Price its complete merged properties before staging, and
        // settle against the staged entry rather than the public new-edge flag.
        let private_graph = self.prepare_private_graph_mutation(tx, source, &edge_type, target)?;
        let mut pending_properties = private_graph
            .as_ref()
            .map(|capture| capture.merged_pending_properties(&properties))
            .transpose()?
            .flatten();
        if pending_properties
            .as_ref()
            .is_some_and(|pending| !pending.changed)
        {
            self.finish_private_graph_mutation(tx, private_graph)?;
            return Ok(false);
        }
        let replace_pending = pending_properties.is_some();
        if let Some(pending) = pending_properties.as_mut() {
            properties = std::mem::take(&mut pending.values);
        }
        let deleted_in_ws = self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_deletes
                .iter()
                .any(|(s, kind, t, _)| *s == source && *t == target && kind == &edge_type)
        })?;
        if !replace_pending
            && !properties.is_empty()
            && !deleted_in_ws
            && let Some(mut existing) = self.edge_properties_unchecked_with_read_memory(
                source,
                target,
                &edge_type,
                snapshot,
                read_memory.clone(),
            )?
        {
            existing.extend(properties);
            properties = existing;
        }
        let staged_bytes = |ws: &mut WriteSet| {
            ws.adj_inserts
                .iter()
                .filter(|edge| {
                    edge.source == source && edge.target == target && edge.edge_type == edge_type
                })
                .map(standalone_edge_retained_bytes)
                .sum::<usize>()
        };
        let before = self.tx_mgr.with_write_set(tx, staged_bytes)?;
        let bytes = estimate_edge_bytes(source, target, &edge_type, &properties);
        self.finalization.accountant.try_allocate_for(
            bytes,
            "graph_insert",
            "insert_edge",
            "Reduce edge fan-out or raise MEMORY_LIMIT before inserting more graph edges.",
        )?;

        let staged = if replace_pending {
            self.graph
                .delete_edge(tx, source, target, &edge_type, read_memory.clone())
                .and_then(|()| {
                    self.graph
                        .insert_edge(
                            tx,
                            source,
                            target,
                            edge_type.clone(),
                            properties,
                            read_memory.clone(),
                        )
                        .map(|_| false)
                })
        } else {
            self.graph.insert_edge(
                tx,
                source,
                target,
                edge_type.clone(),
                properties,
                read_memory.clone(),
            )
        };
        match staged {
            Ok(inserted) => {
                let after = self.tx_mgr.with_write_set(tx, staged_bytes)?;
                let unused = before
                    .checked_add(bytes)
                    .and_then(|held| held.checked_sub(after))
                    .expect("the staged edge is covered by its admitted merged properties");
                self.finalization.accountant.release(unused);
                self.finish_private_graph_mutation(tx, private_graph)?;
                Ok(inserted && !existing_row)
            }
            Err(err) => {
                self.finalization.accountant.release(bytes);
                if replace_pending {
                    self.finalization.accountant.release(before);
                }
                Err(err)
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn delete_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        self.delete_edge_with_read_memory(tx, source, target, edge_type, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delete_edge_with_read_memory(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation_after_write_control_wait(tx, "delete_edge")?;
        let snapshot = self.snapshot();
        self.assert_node_write_allowed(source, snapshot, read_memory.clone())?;
        self.assert_node_write_allowed(target, snapshot, read_memory.clone())?;
        self.assert_graph_edge_write_allowed(
            Some(tx),
            source,
            target,
            edge_type,
            snapshot,
            read_memory.clone(),
        )?;
        let private_graph = self.prepare_private_graph_mutation(tx, source, edge_type, target)?;
        let removed_bytes = private_graph
            .as_ref()
            .map_or(0, |capture| capture.pending_bytes());
        self.graph
            .delete_edge(tx, source, target, edge_type, read_memory.clone())?;
        if removed_bytes != 0 {
            self.finalization.accountant.release(removed_bytes);
        }
        self.finish_private_graph_mutation(tx, private_graph)
    }

    /// Traversal storage is admitted on the reader budget during execution.
    /// The returned result is caller-owned; its execution credit is released
    /// at this public boundary, as for other returned query results.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn query_bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult> {
        let read_memory = self.declared_reader_memory()?;
        self.query_bfs_with_read_memory(
            start,
            edge_types,
            direction,
            max_depth,
            snapshot,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn query_bfs_with_read_memory(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        max_depth: u32,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<TraversalResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        if let Some(tx) = self.private_graph_read_tx(snapshot)? {
            return self
                .graph_bfs_counted(
                    Some(tx),
                    start,
                    edge_types,
                    direction,
                    1..=max_depth,
                    snapshot,
                    read_memory,
                )
                .map(|(result, _, _credit)| result);
        }
        if self.access_is_admin() {
            self.graph
                .bfs_at_source(
                    start,
                    edge_types,
                    direction,
                    1,
                    max_depth,
                    snapshot,
                    self.graph_snapshot(snapshot, read_memory.clone())?,
                    read_memory.clone(),
                )
                .map(|(result, _credit)| result)
        } else {
            self.query_bfs_gated(
                start,
                edge_types,
                direction,
                1,
                max_depth,
                snapshot,
                read_memory.clone(),
            )
            .map(|(result, _credit)| result)
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn edge_count(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<usize> {
        let read_memory = self.declared_reader_memory()?;
        self.edge_count_with_read_memory(source, edge_type, snapshot, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn edge_count_with_read_memory(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<usize> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let tx = self.private_graph_read_tx(snapshot)?;
        let input = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            edge_type.len(),
            "graph",
            "count_type",
            "Raise the declared reader memory limit to count these edges.",
        )?;
        let edge_types = [edge_type.to_owned()];
        let memory = std::cell::RefCell::new(contextdb_core::read_memory::ReadCredit::new(
            read_memory.clone(),
        ));
        let retain = |bytes| {
            memory.borrow_mut().try_grow_for(
                bytes,
                "graph",
                "count_bindings",
                "Raise the declared reader memory limit to count these edges.",
            )
        };
        let release = |bytes| {
            memory
                .borrow_mut()
                .try_shrink(bytes)
                .expect("count releases admitted memory")
        };
        let Some(mut cursor) = self.bounded_graph_bfs_cursor::<Error>(
            tx,
            source,
            Some(&edge_types),
            Direction::Outgoing,
            1,
            1,
            snapshot,
            |_| Ok(()),
            retain,
            release,
            read_memory.clone(),
        )?
        else {
            return Ok(0);
        };
        drop(edge_types);
        drop(input);
        cursor.inner.stream_single_hop_bindings()?;
        let mut count = 0usize;
        while let Some(answer) = self.bounded_graph_bfs_next::<Error>(
            &mut cursor,
            || Ok(()),
            || Ok(()),
            |_| Ok(()),
            retain,
            release,
            || Ok(()),
            retain,
            release,
            || {},
            None,
            tx,
            read_memory.clone(),
        )? {
            let bytes = answer.reserved_bytes;
            drop(answer);
            release(bytes);
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::Other("graph count overflow".into()))?;
        }
        drop(cursor);
        Ok(count)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn get_edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<HashMap<String, Value>>> {
        let read_memory = self.declared_reader_memory()?;
        self.get_edge_properties_with_read_memory(source, target, edge_type, snapshot, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn get_edge_properties_with_read_memory(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        if !self.access_is_admin()
            && (!self.node_read_allowed(source, snapshot, read_memory.clone())?
                || !self.node_read_allowed(target, snapshot, read_memory.clone())?
                || !self.edge_read_allowed(
                    source,
                    target,
                    edge_type,
                    snapshot,
                    read_memory.clone(),
                )?)
        {
            return Ok(None);
        }
        if let Some(tx) = self.private_graph_read_tx(snapshot)?
            && let Some(properties) =
                self.private_graph_properties(tx, source, edge_type, target, read_memory.clone())?
        {
            return Ok(properties);
        }
        self.edge_properties_unchecked_with_read_memory(
            source,
            target,
            edge_type,
            snapshot,
            read_memory,
        )
    }

    // Internal write pricing follows write authorization; it must see the
    // complete entry even when the writer cannot expose it as a read answer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn edge_properties_unchecked_with_read_memory(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<HashMap<String, Value>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.graph_snapshot(snapshot, read_memory.clone())? {
            let mut cursor = root.cursor(contextdb_graph::source::GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: Some((target, edge_type)),
            })?;
            let mut properties = None;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            while let Some(window) = cursor.next_edge()? {
                let edge = window.edge();
                if edge.visible_at(snapshot) {
                    let bytes = edge
                        .properties
                        .iter()
                        .map(|(name, value)| {
                            name.len()
                                .saturating_add(value.estimated_bytes())
                                .saturating_add(96)
                        })
                        .sum();
                    output.try_grow_for(
                        bytes,
                        "writer_source",
                        "edge_property_output",
                        "Raise MEMORY_LIMIT to return edge properties.",
                    )?;
                    properties = Some(edge.properties.clone());
                    break;
                }
            }
            self.retain_writer_output(output);
            return Ok(properties);
        }
        let graph = self.graph_store.forward_adj.read();
        let properties = graph.get(&source).and_then(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry.target == target
                        && entry.edge_type == edge_type
                        && entry.visible_at(snapshot)
                })
                .min_by_key(|entry| entry.row_owner.is_some())
                .map(|entry| &entry.properties)
        });
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let props = if let Some(properties) = properties {
            let bytes = properties
                .iter()
                .map(|(name, value)| {
                    name.len()
                        .saturating_add(value.estimated_bytes())
                        .saturating_add(96)
                })
                .sum();
            output.try_grow_for(
                bytes,
                "reader",
                "edge_property_output",
                "Raise the declared read memory limit to return edge properties.",
            )?;
            Some(properties.clone())
        } else {
            None
        };
        drop(graph);
        self.retain_writer_output(output);
        Ok(props)
    }

    /// Put every vector column of a row into the vector search store.
    ///
    /// A `VECTOR(N)` column IS a named index, so the vector store is the one
    /// place a vector lives and the row-write API is what puts it there.
    /// Before this, only the SQL executor made the second call, so a caller
    /// that wrote a row through this API left the column unindexed -- present
    /// in the row, absent from every vector search, and charged nothing.
    ///
    /// Answers with what each write was charged, so a caller whose statement
    /// fails afterwards hands back exactly what it took.
    /// Leave only quantized vector columns empty in the row that is about to
    /// be stored.
    ///
    /// F32 remains an ordinary relational value. SQ4/SQ8 keep their body only
    /// in the vector store; their relational slot remains NULL internally and
    /// is filled from the vector store on caller-facing projection.
    pub(crate) fn blank_quantized_vector_values(
        &self,
        table: &str,
        values: &mut HashMap<ColName, Value>,
    ) {
        let Some(meta) = self.table_meta(table) else {
            return;
        };
        for column in &meta.columns {
            if !matches!(column.column_type, contextdb_core::ColumnType::Vector(_)) {
                continue;
            }
            if matches!(column.quantization, VectorQuantization::F32) {
                continue;
            }
            if let Some(slot) = values.get_mut(&column.name)
                && matches!(slot, Value::Vector(_))
            {
                *slot = Value::Null;
            }
        }
    }

    /// Fill in the quantized vector columns of rows on their way to an
    /// answer, from the store that holds them.
    ///
    /// The row does not keep a copy, so a reader that projects the column
    /// takes the value from the store the column declared. What comes back is
    /// what every other route already answers with -- the stored
    /// approximation, not the caller's original -- so no answer changes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn supplement_quantized_vectors(
        &self,
        table: &str,
        snapshot: SnapshotId,
        rows: &mut [VersionedRow],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let vector_columns: Vec<String> = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
            .map(|column| column.name.clone())
            .collect();
        if vector_columns.is_empty() {
            return Ok(());
        }
        for column in vector_columns {
            for row in rows.iter_mut() {
                if !matches!(row.values.get(&column), Some(Value::Null) | None) {
                    continue;
                }
                if let Some(vector) = self.load_row_vector_for_column(
                    None,
                    table,
                    &column,
                    row,
                    snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                )? {
                    row.values.insert(column.clone(), Value::Vector(vector));
                }
            }
        }
        Ok(())
    }

    /// Fill only vector columns the caller's expression actually reads.
    /// `None` is the explicit `SELECT *` case; `Some(&[])` deliberately loads
    /// no vector body. Lazy partitions may fail while loading, so this route
    /// must preserve that error instead of returning a row with a false NULL.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn supplement_selected_vector_columns(
        &self,
        table: &str,
        snapshot: SnapshotId,
        tx: Option<TxId>,
        selected: Option<&[String]>,
        rows: &mut [VersionedRow],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let vector_columns: Vec<String> = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
            .map(|column| column.name.clone())
            .filter(|column| selected.is_none_or(|selected| selected.contains(column)))
            .collect();
        for column in vector_columns {
            for row in rows.iter_mut() {
                if !matches!(row.values.get(&column), Some(Value::Null) | None) {
                    continue;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_vector_body_access();
                contextdb_core::find_attribution::count("vector_body_accesses", 1);
                let vector = self.load_row_vector_for_column(
                    tx,
                    table,
                    &column,
                    row,
                    snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if let Some(vector) = vector {
                    row.values.insert(column.clone(), Value::Vector(vector));
                }
            }
        }
        Ok(())
    }

    /// What one row hands to a peer: its own values, with every quantized
    /// vector column read back out of the store that holds it.
    ///
    /// The row keeps no copy of such a column, so a changeset assembled
    /// straight from `row.values` would carry nothing where a vector belongs.
    /// Filling it here is what keeps the payload a peer receives exactly what
    /// it received before -- the same dequantized vector, under the same
    /// column name, in the same shape -- on every route that builds one.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn outbound_row_values(
        &self,
        table: &str,
        row: &VersionedRow,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<ColName, Value>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut values = row.values.clone();
        values.extend(self.shed_committed_values_for_change(
            table,
            row,
            &ChangeProjection::unbounded(),
            read_memory.clone(),
        )?);
        self.fill_quantized_from_store(
            table,
            row,
            &mut values,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        Ok(values)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn fill_quantized_from_store(
        &self,
        table: &str,
        row: &VersionedRow,
        values: &mut HashMap<ColName, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.fill_quantized_columns(
            table,
            row,
            SnapshotId(row.created_tx.0),
            values,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn fill_quantized_columns(
        &self,
        table: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
        values: &mut HashMap<ColName, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for column in self.quantized_vector_columns(table) {
            if !matches!(values.get(&column), Some(Value::Null) | None) {
                continue;
            }
            if let Some(vector) = self.load_row_vector_for_column(
                None,
                table,
                &column,
                row,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                values.insert(column, Value::Vector(vector));
            }
        }
        Ok(())
    }

    /// Fill the quantized vector columns of whole rows on their way back to a
    /// caller of the row API.
    ///
    /// `scan_filter` and `point_lookup` hand the caller the row itself -- there
    /// is no projection to say which columns are wanted -- so every column the
    /// row declares has to be answerable. A quantized column lives only in its
    /// vector index, so a row handed back unfilled carries NULL where the
    /// embedding belongs: a predicate testing it never matches, and a caller
    /// reading it is told the row has no vector.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn supplement_rows_for_caller(
        &self,
        table: &str,
        snapshot: SnapshotId,
        rows: &mut [VersionedRow],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.quantized_vector_columns(table).is_empty() {
            return Ok(());
        }
        for row in rows.iter_mut() {
            let mut values = std::mem::take(&mut row.values);
            let result = self.fill_quantized_columns(
                table,
                row,
                snapshot,
                &mut values,
                vector_copy_owner,
                read_memory.clone(),
            );
            row.values = values;
            result?;
        }
        Ok(())
    }

    /// What a column will hold once the value given for it is written.
    ///
    /// A quantized column keeps an approximation of the vector it is given, so
    /// this is what a diff has to compare against the stored row -- comparing
    /// the caller's full-precision input instead reports a change that writing
    /// it would not make.
    pub(super) fn stored_form_of_value(&self, table: &str, column: &str, value: &Value) -> Value {
        let Value::Vector(vector) = value else {
            return value.clone();
        };
        let Some(meta) = self.table_meta(table) else {
            return value.clone();
        };
        let Some(declared) = meta
            .columns
            .iter()
            .find(|candidate| candidate.name == column)
        else {
            return value.clone();
        };
        if !matches!(declared.column_type, contextdb_core::ColumnType::Vector(_))
            || matches!(
                declared.quantization,
                contextdb_core::VectorQuantization::F32
            )
        {
            return value.clone();
        }
        Value::Vector(contextdb_vector::stored_vector_value(
            vector,
            declared.quantization,
        ))
    }

    /// Fallible row projection for the lazy raw-vector store. The exact row
    /// version is preferred, and only a directory that can answer it is
    /// loaded. A transaction's staged vector remains authoritative over the
    /// committed body.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_score_committed_vector_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        before_checkpoint: &mut dyn FnMut() -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
        before_distance: &mut dyn FnMut() -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
        before_retain: &mut dyn FnMut(
            usize,
        ) -> std::result::Result<
            (),
            crate::executor::BoundedExecutionError,
        >,
        release_retained: &mut dyn FnMut(usize),
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<f32>, crate::executor::BoundedExecutionError> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector_store.bounded_score_live_row(
                index,
                row_id,
                snapshot,
                query,
                before_checkpoint,
                before_distance,
                before_retain,
                release_retained,
                vector_copy_owner,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_selected_row_vector(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        loaded: Option<Vec<f32>>,
        snapshot: SnapshotId,
    ) -> Result<Option<Vec<f32>>> {
        let Some(vector) = loaded else {
            return Ok(None);
        };
        // Historical loaders prove the retained body and owning placement.
        // The current family names its successor, so only a current read can
        // additionally compare this value against the current projection.
        if snapshot == self.snapshot()
            && let Some(source) = &self.retained_relational_source
        {
            let quantization = self
                .table_meta(&index.table)
                .and_then(|meta| {
                    meta.columns
                        .into_iter()
                        .find(|column| column.name == index.column)
                })
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?
                .quantization;
            source.authenticate_selected_vector(index, row_id, &vector, quantization)?;
        }
        Ok(Some(vector))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_row_vector_for_column(
        &self,
        tx: Option<TxId>,
        table: &str,
        column: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Vec<f32>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _historical_intent = if snapshot.0 < self.snapshot().0 {
            self.vector_historical_read_intent(snapshot, read_memory.clone())
        } else {
            None
        };
        let _vector_capture = if _historical_intent.is_some() {
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?
        } else {
            None
        };
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let index = VectorIndexRef::new(table, column.to_owned());
            if let Some(Value::Vector(vector)) = row.values.get(column) {
                vector_copy_owner
                    .reserve(vector.capacity().saturating_mul(std::mem::size_of::<f32>()))?;
                return Ok(Some(vector.clone()));
            }
            let staged = if let Some(tx) = tx {
                self.tx_mgr.with_write_set(tx, |ws| {
                    ws.relational_inserts.iter().any(|(candidate, staged)| {
                        candidate == table
                            && staged.row_id == row.row_id
                            && staged.created_tx == row.created_tx
                            && staged.lsn == row.lsn
                    })
                })?
            } else {
                false
            };
            if staged {
                let loaded = self
                    .vector_entry_for_row_in_tx(
                        tx,
                        &index,
                        row.row_id,
                        snapshot,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                    .map(|entry| entry.map(|entry| entry.vector))?;
                return self.authenticate_selected_row_vector(&index, row.row_id, loaded, snapshot);
            }
            if let Some(vector) = self.vector_store.load_vector_for_row_lsn(
                &index,
                row.row_id,
                row.lsn,
                row.created_tx,
                snapshot,
                vector_copy_owner,
            )? {
                return self.authenticate_selected_row_vector(
                    &index,
                    row.row_id,
                    Some(vector),
                    snapshot,
                );
            }
            // Writable open keeps only the current membership in each dormant
            // partition directory so retired bodies do not become permanent live
            // residency. An older relational snapshot can still name its exact
            // checksummed membership; load that one body directly without
            // widening the live directory or its accountant ownership.
            if let Some(persistence) = self.persistence.as_ref() {
                let meta = self
                    .table_meta(table)
                    .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
                let declaration = meta
                    .columns
                    .iter()
                    .find(|declared| declared.name == column)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: index.clone(),
                    })?;
                let dimension = match declaration.column_type {
                    ColumnType::Vector(dimension) => dimension,
                    _ => {
                        return Err(Error::UnknownVectorIndex {
                            index: index.clone(),
                        });
                    }
                };
                let partition_key = crate::composite_store::vector_partition_key_for_row(
                    &index,
                    &meta,
                    declaration,
                    row,
                )?;
                let membership = persistence
                    .load_vector_partition_memberships_for_row(&index, &partition_key, row.row_id)?
                    .into_iter()
                    .filter(|membership| {
                        membership.index == index
                            && membership.row_id == row.row_id
                            && membership.row_created_tx == row.created_tx
                            && membership.row_lsn == row.lsn
                            && membership.visible_from.0 <= snapshot.0
                            && membership
                                .deleted_tx
                                .is_none_or(|deleted| deleted.0 > snapshot.0)
                    })
                    .max_by_key(|membership| (membership.vector_lsn, membership.vector_created_tx));
                if let Some(membership) = membership {
                    vector_copy_owner.reserve(
                        dimension
                            .saturating_mul(std::mem::size_of::<f32>())
                            .saturating_add(std::mem::size_of::<VectorEntry>())
                            .saturating_add(index.table.len())
                            .saturating_add(index.column.len())
                            .saturating_mul(4),
                    )?;
                    let loaded = persistence
                        .load_vector_entries_for_memberships(&[membership], dimension, None)
                        .map(|mut entries| entries.pop().map(|entry| entry.vector))?;
                    return self
                        .authenticate_selected_row_vector(&index, row.row_id, loaded, snapshot);
                }
            }
            Ok(None)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_row_vector_for_column_identity(
        &self,
        tx: Option<TxId>,
        table: &str,
        column: &str,
        row_id: RowId,
        lsn: Lsn,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Vec<f32>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let row = self.find_row_by_id_in_tx(tx, table, row_id, snapshot, read_memory.clone())?;
        if row.lsn != lsn {
            return Ok(None);
        }
        self.load_row_vector_for_column(
            tx,
            table,
            column,
            &row,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Which of a table's vector columns keep their value in the vector store.
    pub(crate) fn quantized_vector_columns(&self, table: &str) -> Vec<String> {
        self.table_meta(table)
            .map(|meta| {
                meta.columns
                    .iter()
                    .filter(|column| {
                        matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    })
                    .map(|column| column.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The vector columns a row about to be written carries, taken before the
    /// row's values are handed to the relational store.
    pub(crate) fn row_vector_values(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
    ) -> Vec<(VectorIndexRef, Vec<f32>)> {
        let Some(meta) = self.table_meta(table) else {
            return Vec::new();
        };
        meta.columns
            .iter()
            .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
            .filter_map(|column| match values.get(&column.name) {
                Some(Value::Vector(vector)) => Some((
                    VectorIndexRef::new(table, column.name.clone()),
                    vector.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    /// Place a staged row's vector columns, and if any of them cannot be
    /// placed, take the row back out of the write set.
    ///
    /// The vector store's write needs the row to be staged already -- it
    /// checks that the identity it is given may be written -- so the vectors
    /// cannot go first. What CAN hold is the invariant the executor's
    /// accounting depends on: an insert that fails leaves NOTHING staged, so
    /// the caller's compensation release is the only one that hands the row's
    /// charge back. A row left staged behind a failure is released a second
    /// time by the rollback.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_row_vectors_or_unstage(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: &HashMap<ColName, Value>,
        _staged_before: usize,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let captured = self.row_vector_values(table, values);
        if captured.is_empty() {
            return Ok(());
        }
        // The caller's checkpoint owns complete compensation, including the
        // immutable overlay; truncating only ordinary rows here loses owners.
        self.write_row_vectors(tx, row_id, captured, vector_copy_owner, read_memory.clone())
    }

    /// How many rows this transaction has staged, read before one more is.
    pub(crate) fn staged_row_count(&self, tx: TxId) -> usize {
        self.tx_mgr
            .with_read(tx, |ws| ws.relational_inserts.len())
            .unwrap_or(0)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn write_row_vectors(
        &self,
        tx: TxId,
        row_id: RowId,
        captured: Vec<(VectorIndexRef, Vec<f32>)>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if captured.is_empty() {
            return Ok(());
        }
        let _vector_schema =
            self.vector_schema_read_many(captured.iter().map(|(index, _)| index.clone()));
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
                let result = (|| {
                    let vectors = self.with_preparation_original(|original| {
                        let bytes =
                            std::alloc::Layout::array::<OriginalDatabaseVector>(captured.len())
                                .map_err(|_| {
                                    Error::Other("original vector control size overflow".into())
                                })?
                                .size();
                        let control = crate::memory_accounting::reserve_preparation_from_original(
                            original,
                            bytes,
                            "vector",
                            "original_vector_controls",
                            "Reduce staged vectors or raise MEMORY_LIMIT.",
                        )?;
                        let mut vectors = OriginalDatabaseVectors {
                            entries: Vec::with_capacity(captured.len()),
                            control,
                        };
                        if vectors.entries.capacity() != captured.len() {
                            return Err(Error::Other("source native capacity mismatch".into()));
                        }
                        for (index, vector) in &captured {
                            self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
                            self.vector_store.validate_vector(index, vector.len())?;
                            self.assert_row_id_write_allowed(
                                Some(tx),
                                &index.table,
                                row_id,
                                self.snapshot_for_read(),
                                read_memory.clone(),
                            )?;
                            let admission =
                                crate::memory_accounting::reserve_preparation_from_original(
                                    original,
                                    crate::memory_accounting::preparation_vector_values_copy_bytes(
                                        &index.table,
                                        &index.column,
                                        vector.len(),
                                    )?,
                                    "vector",
                                    "original_vector_body",
                                    "Reduce staged vectors or raise MEMORY_LIMIT.",
                                )?;
                            vectors.entries.push(OriginalDatabaseVector {
                                entry: crate::memory_accounting::preparation_copy_vector_values(
                                    &index.table,
                                    &index.column,
                                    row_id,
                                    vector.as_slice(),
                                    tx,
                                )?,
                                // This API produces an independent physical vector
                                // original; it does not invent a scalar-row owner.
                                scalar: false,
                                cancellation: None,
                                admission,
                            });
                        }
                        Ok(vectors)
                    })?;
                    self.stage_original_row_vectors(host, tx, vectors, read_memory.clone())
                })();
                if result.is_err() {
                    self.restore_write_set_checkpoint(tx, checkpoint)?;
                } else {
                    checkpoint.settle();
                }
                result
            });
        }
        // Placement validates and reserves the whole batch before publishing any
        // vector write. A later column refusal cannot leave a staged prefix.
        let control_bytes = captured.len().saturating_mul(
            std::mem::size_of::<crate::memory_accounting::OwnedMemoryReservation>()
                .saturating_add(std::mem::size_of::<bool>())
                .saturating_add(
                    std::mem::size_of::<crate::memory_accounting::TransferredRowReservation>()
                        .saturating_add(2 * std::mem::size_of::<usize>()),
                )
                .saturating_add(std::mem::size_of::<
                    contextdb_tx::ordered_staged::PreparedStagedVector,
                >())
                .saturating_add(std::mem::size_of::<(
                    Option<contextdb_tx::ordered_staged::PreparedVectorUndo>,
                    Arc<crate::memory_accounting::TransferredRowReservation>,
                    usize,
                )>()),
        );
        let _control = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            control_bytes,
            "vector",
            "placement_controls",
            "Reduce vector columns or raise MEMORY_LIMIT.",
        )?;
        let mut reservations = Vec::with_capacity(captured.len());
        for (index, vector) in &captured {
            self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
            self.vector_store.validate_vector(index, vector.len())?;
            self.assert_row_id_write_allowed(
                Some(tx),
                &index.table,
                row_id,
                self.snapshot_for_read(),
                read_memory.clone(),
            )?;
            let bytes =
                self.vector_insert_accounted_bytes(index, vector.len(), vector_copy_owner)?;
            reservations.push(
                crate::memory_accounting::OwnedMemoryReservation::try_new_for_label(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    bytes,
                    "insert",
                    self.finalization
                        .accountant
                        .prepare_operation_label(format_args!(
                            "vector_insert@{}.{}",
                            index.table, index.column
                        ))?,
                    "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                )?,
            );
        }
        let existing_live = captured
            .iter()
            .map(|(index, _)| {
                self.vector_store.has_live_entry_for_row(
                    index,
                    row_id,
                    self.snapshot_for_read(),
                    vector_copy_owner,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.record_vector_schema_epochs(tx, captured.iter().map(|(index, _)| index.clone()))?;
        self.tx_mgr.with_write_set(tx, |ws| {
            let mut prepared_undo = Vec::with_capacity(captured.len());
            let mut prepared_vectors = Vec::with_capacity(captured.len());
            for (index, vector) in &captured {
                prepared_vectors.push(ws.ordered_staged_rows.prepare_vector_capability(
                    index,
                    row_id,
                    vector,
                    |bytes| {
                        Ok(
                            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                crate::memory_accounting::AccountantHandle::clone(
                                    &self.finalization.accountant,
                                ),
                                bytes,
                                "vector",
                                "staged_immutable_owner",
                                "Reduce staged vectors or raise MEMORY_LIMIT.",
                            )?
                            .into_admitted_arc()?,
                        )
                    },
                )?);
                let bytes = ws
                    .vector_inserts
                    .iter()
                    .filter(|entry| entry.index == *index && entry.row_id == row_id)
                    .map(|entry| {
                        self.vector_insert_accounted_bytes(
                            &entry.index,
                            entry.vector.len(),
                            vector_copy_owner,
                        )
                    })
                    .try_fold(0usize, |total, bytes| {
                        bytes.map(|bytes| total.saturating_add(bytes))
                    })?;
                let admission =
                    crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        0,
                    )?;
                // Ownership transfers only on publication, so a failed prepare
                // must leave the original admission with its staged entries.
                let undo = ws.ordered_staged_rows.prepare_vector_undo(
                    index,
                    row_id,
                    &ws.vector_inserts,
                    &ws.vector_moves,
                    &ws.vector_deletes,
                    admission.clone(),
                    |bytes| {
                        Ok(
                            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                crate::memory_accounting::AccountantHandle::clone(
                                    &self.finalization.accountant,
                                ),
                                bytes,
                                "vector",
                                "mutation_undo",
                                "Reduce staged vectors or raise MEMORY_LIMIT.",
                            )?
                            .into_admitted_arc()?,
                        )
                    },
                );
                prepared_undo.push((undo?, admission, bytes));
            }
            let mut existing_live = existing_live.into_iter();
            for (((index, vector), (undo, admission, bytes)), prepared_vector) in captured
                .into_iter()
                .zip(prepared_undo.drain(..))
                .zip(prepared_vectors)
            {
                let existing_live = existing_live.next().expect("prepared vector identity");
                if let Some(undo) = undo {
                    admission.adopt(bytes);
                    ws.ordered_staged_rows.publish_vector_undo(
                        undo,
                        &mut ws.vector_inserts,
                        &mut ws.vector_moves,
                        &mut ws.vector_deletes,
                        true,
                        self.vector_insert_accounted_bytes(
                            &index,
                            vector.len(),
                            vector_copy_owner,
                        )?,
                    );
                }
                let mut position = 0;
                while position < ws.vector_inserts.len() {
                    if ws.vector_inserts[position].index == index
                        && ws.vector_inserts[position].row_id == row_id
                    {
                        let prior = ws.vector_inserts.remove(position);
                        self.finalization
                            .accountant
                            .release(self.vector_insert_accounted_bytes(
                                &prior.index,
                                prior.vector.len(),
                                vector_copy_owner,
                            )?);
                    } else {
                        position += 1;
                    }
                }
                let mut position = 0;
                while position < ws.vector_moves.len() {
                    if ws.vector_moves[position].0 == index && ws.vector_moves[position].2 == row_id
                    {
                        let (_, old_row, _, _) = ws.vector_moves.remove(position);
                        if !ws
                            .vector_deletes
                            .iter()
                            .any(|(pending, row, _)| *pending == index && *row == old_row)
                        {
                            ws.vector_deletes.push((index.clone(), old_row, tx));
                        }
                    } else {
                        position += 1;
                    }
                }
                if existing_live
                    && !ws
                        .vector_deletes
                        .iter()
                        .any(|(pending, row, _)| *pending == index && *row == row_id)
                {
                    ws.vector_deletes.push((index.clone(), row_id, tx));
                }
                ws.ordered_staged_rows
                    .publish_vector_capability(&index, prepared_vector);
                ws.vector_inserts.push(VectorEntry {
                    index,
                    row_id,
                    vector,
                    created_tx: tx,
                    deleted_tx: None,
                    lsn: Lsn(0),
                });
            }
            Ok::<_, Error>(())
        })??;
        // The existing write-set accounting owns these charges after install.
        for reservation in reservations {
            reservation.transfer_to_staged();
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.insert_vector_with_read_memory(
            tx,
            index,
            row_id,
            vector,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_vector_with_read_memory(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation_after_write_control_wait(tx, "insert_vector")?;
        let _vector_schema = self.vector_schema_read(&index);
        self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
        self.vector_store.with_index_layout(&index, |_| ())?;
        self.assert_existing_row_id_write_allowed(
            Some(tx),
            &index.table,
            row_id,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        if let Some(expected) = self.pending_vector_dimension(tx, &index)?
            && expected != vector.len()
        {
            return Err(self.direct_vector_dimension_error(&index, expected, vector.len()));
        }
        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = self
            .insert_vector_strict(
                tx,
                index.clone(),
                row_id,
                vector,
                vector_copy_owner,
                read_memory.clone(),
            )
            .map_err(|err| match err {
                Error::VectorIndexDimensionMismatch {
                    expected, actual, ..
                } => self.direct_vector_dimension_error(&index, expected, actual),
                other => other,
            });
        if result.is_err() {
            let _ = self.restore_write_set_checkpoint(tx, checkpoint);
        } else {
            checkpoint.settle();
        }
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn insert_vector_strict(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.write_row_vectors(
            tx,
            row_id,
            vec![(index, vector)],
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn delete_vector(&self, tx: TxId, index: VectorIndexRef, row_id: RowId) -> Result<()> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.delete_vector_with_read_memory(tx, index, row_id, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn delete_vector_with_read_memory(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation_after_write_control_wait(tx, "delete_vector")?;
        let _vector_schema = self.vector_schema_read(&index);
        self.ensure_trigger_table_ready(&index.table, "delete_vector")?;
        self.vector_store.with_index_layout(&index, |_| ())?;
        self.assert_row_id_write_allowed(
            Some(tx),
            &index.table,
            row_id,
            self.snapshot_for_read(),
            read_memory.clone(),
        )?;
        let checkpoint = self.write_set_checkpoint(tx)?;
        let result = self.delete_vector_within_operation(
            tx,
            index,
            row_id,
            vector_copy_owner,
            read_memory.clone(),
        );
        if result.is_err() {
            let _ = self.restore_write_set_checkpoint(tx, checkpoint);
        } else {
            checkpoint.settle();
        }
        result
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_vector_within_operation(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The reverse membership lookup decodes temporary records even when
        // the row is becoming NULL. Keep them on this caller's exact budget.
        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader_per_call_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory,
            "reader",
            "materialize_answer",
            "Reduce the statement's output or raise the declared reader memory limit.",
        )?
        .enter();

        self.record_vector_schema_epoch(tx, &index)?;
        let existing_live = self.vector_store.has_live_entry_for_row(
            &index,
            row_id,
            self.snapshot_for_read(),
            vector_copy_owner,
        )?;
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                self.delete_original_vector_within_operation(
                    host,
                    tx,
                    &index,
                    row_id,
                    existing_live,
                )
            });
        }
        let canceled_inserts = self.tx_mgr.with_write_set(tx, |ws| {
            let bytes = ws
                .vector_inserts
                .iter()
                .filter(|entry| entry.index == index && entry.row_id == row_id)
                .map(|entry| {
                    self.vector_insert_accounted_bytes(
                        &entry.index,
                        entry.vector.len(),
                        vector_copy_owner,
                    )
                })
                .try_fold(0usize, |total, bytes| {
                    bytes.map(|bytes| total.saturating_add(bytes))
                })?;
            let admission = crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                0,
            )?;
            let undo = ws.ordered_staged_rows.prepare_vector_undo(
                &index,
                row_id,
                &ws.vector_inserts,
                &ws.vector_moves,
                &ws.vector_deletes,
                admission.clone(),
                |bytes| {
                    Ok(
                        crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                            bytes,
                            "vector",
                            "mutation_undo",
                            "Reduce staged vectors or raise MEMORY_LIMIT.",
                        )?
                        .into_admitted_arc()?,
                    )
                },
            )?;
            if let Some(undo) = undo {
                admission.adopt(bytes);
                ws.ordered_staged_rows.publish_vector_undo(
                    undo,
                    &mut ws.vector_inserts,
                    &mut ws.vector_moves,
                    &mut ws.vector_deletes,
                    true,
                    0,
                );
            }
            ws.ordered_staged_rows
                .remove_vector_capability(&index, row_id);
            let mut canceled_inserts = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == index && ws.vector_inserts[pos].row_id == row_id
                {
                    canceled_inserts.push(ws.vector_inserts.remove(pos));
                } else {
                    pos += 1;
                }
            }
            let mut moved_sources = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_moves.len() {
                let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
                if *move_index == index && *new_row_id == row_id {
                    moved_sources.push(*old_row_id);
                    ws.vector_moves.remove(pos);
                } else {
                    pos += 1;
                }
            }
            let pending_move_from_row =
                ws.vector_moves
                    .iter()
                    .any(|(move_index, old_row_id, _, _)| {
                        *move_index == index && *old_row_id == row_id
                    });
            for old_row_id in moved_sources {
                if !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    })
                {
                    ws.vector_deletes.push((index.clone(), old_row_id, tx));
                }
            }
            let already_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == row_id
                    });
            if !pending_move_from_row && existing_live && !already_deleted {
                ws.vector_deletes.push((index, row_id, tx));
            }
            Ok::<_, Error>(canceled_inserts)
        })??;
        for entry in canceled_inserts {
            let bytes = self.vector_insert_accounted_bytes(
                &entry.index,
                entry.vector.len(),
                vector_copy_owner,
            )?;
            // The canceled body and its owned index strings are destroyed
            // before returning their existing insertion invoice.
            drop(entry);
            self.finalization.accountant.release(bytes);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_original_vector_cancellation(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
        row: RowId,
        existing_live: bool,
    ) -> Result<crate::custody::preparation::OwnedPreparationValue<NativeVectorCancellation>> {
        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        let overflow = || Error::Other("vector delete plan size overflow".into());
        let count = self.tx_mgr.with_read(tx, |read| {
            read.vector_moves
                .iter()
                .filter(|(name, _, to, _)| name == index && *to == row)
                .count()
        })?;
        let targets = count.checked_add(1).ok_or_else(overflow)?;
        let mut bytes = preparation_array_bytes::<usize>(count)?
            .checked_add(preparation_array_bytes::<RowId>(count)?)
            .ok_or_else(overflow)?;
        for extra in [
            preparation_array_bytes::<RowId>(targets)?,
            preparation_array_bytes::<bool>(targets)?,
            preparation_array_bytes::<(VectorIndexRef, RowId, TxId)>(targets)?,
        ] {
            bytes = bytes.checked_add(extra).ok_or_else(overflow)?;
        }
        let names = index
            .table
            .len()
            .checked_add(index.column.len())
            .ok_or_else(overflow)?
            .checked_mul(targets)
            .ok_or_else(overflow)?;
        bytes = bytes.checked_add(names).ok_or_else(overflow)?;
        // No vector-body image: quote scalar coordinates and temporary input
        // names, admit outside guards, then copy one checked pending plan.
        let plan = OwnedPreparationValue::try_produce(self, bytes, || {
            self.tx_mgr.with_read(tx, |read| {
                let actual = read
                    .vector_moves
                    .iter()
                    .filter(|(name, _, to, _)| name == index && *to == row)
                    .count();
                if actual != count {
                    return Err(Error::Other(
                        "vector moves changed during delete preparation".into(),
                    ));
                }
                let mut plan = NativeVectorCancellation {
                    positions: std::cell::RefCell::new(Vec::with_capacity(count)),
                    moved_sources: Vec::with_capacity(count),
                    candidates: Vec::with_capacity(targets),
                    present: Vec::with_capacity(targets),
                    deletes: Vec::with_capacity(targets),
                    pending_move_from: read
                        .vector_moves
                        .iter()
                        .any(|(name, from, to, _)| name == index && *from == row && *to != row),
                };
                if plan.positions.get_mut().capacity() != count
                    || plan.moved_sources.capacity() != count
                    || plan.candidates.capacity() != targets
                    || plan.present.capacity() != targets
                    || plan.deletes.capacity() != targets
                {
                    return Err(Error::Other("source native capacity mismatch".into()));
                }
                for (position, (name, from, to, _)) in read.vector_moves.iter().enumerate() {
                    if name == index && *to == row {
                        plan.positions.get_mut().push(position);
                        plan.moved_sources.push(*from);
                        if !plan.candidates.contains(from) {
                            plan.candidates.push(*from);
                        }
                    }
                }
                if !plan.pending_move_from && existing_live && !plan.candidates.contains(&row) {
                    plan.candidates.push(row);
                }
                for candidate in &plan.candidates {
                    let present = read
                        .vector_deletes
                        .iter()
                        .any(|(name, id, _)| name == index && id == candidate);
                    plan.present.push(present);
                    if !present {
                        plan.deletes.push((
                            VectorIndexRef {
                                table: crate::memory_accounting::preparation_copy_string(
                                    &index.table,
                                )?,
                                column: crate::memory_accounting::preparation_copy_string(
                                    &index.column,
                                )?,
                            },
                            *candidate,
                            tx,
                        ));
                    }
                }
                Ok(plan)
            })?
        })?;
        Ok(plan)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_original_vector_cancellation(
        &self,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        index: &VectorIndexRef,
        row: RowId,
        plan: &NativeVectorCancellation,
    ) -> Result<()> {
        let read = guard.read();
        let current = read
            .vector_moves
            .iter()
            .filter(|(name, _, to, _)| name == index && *to == row);
        let sources_match = current
            .clone()
            .map(|(_, from, _, _)| from)
            .eq(plan.moved_sources.iter());
        let pending_from = read
            .vector_moves
            .iter()
            .any(|(name, from, to, _)| name == index && *from == row && *to != row);
        let deletes_match =
            plan.candidates
                .iter()
                .zip(&plan.present)
                .all(|(candidate, present)| {
                    read.vector_deletes
                        .iter()
                        .any(|(name, id, _)| name == index && id == candidate)
                        == *present
                });
        if !sources_match || pending_from != plan.pending_move_from || !deletes_match {
            return Err(Error::Other(
                "vector mutation source changed during delete preparation".into(),
            ));
        }
        // Other columns can remove earlier array positions while preserving
        // this column's exact source sequence. Rebase only these scalar offsets
        // into the same preadmitted capacity; never gather another body image.
        let mut positions = plan.positions.borrow_mut();
        positions.clear();
        for (position, (name, _, to, _)) in read.vector_moves.iter().enumerate() {
            if name == index && *to == row {
                positions.push(position);
            }
        }
        guard.with_tx_checkpoint(|guard| {
            let deletes = (!plan.deletes.is_empty())
                .then(|| {
                    guard.copy_aux(contextdb_tx::staged_source::AuxBorrow::VectorDeletes(
                        &plan.deletes,
                    ))
                })
                .transpose()?;
            let moves = (!positions.is_empty())
                .then(|| guard.copy_aux(contextdb_tx::staged_source::AuxBorrow::VectorMoves(&[])))
                .transpose()?;
            let removal = guard.prepare_remove_vector(index, row)?;
            guard.publish(removal);
            if let Some(empty) = moves {
                let splice = guard.prepare_aux_splice(0, &positions, empty)?;
                guard.publish_tx(splice);
            }
            if let Some(deletes) = deletes {
                let at = guard.read().vector_deletes.len();
                let splice = guard.prepare_aux_splice(at, &[], deletes)?;
                guard.publish_tx(splice);
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn delete_original_vector_within_operation(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        index: &VectorIndexRef,
        row: RowId,
        existing_live: bool,
    ) -> Result<()> {
        let plan = self.prepare_original_vector_cancellation(tx, index, row, existing_live)?;
        plan.with_value(|plan| {
            host.transactions.with_transactions(|lower| {
                lower.with_engine(tx, |guard| {
                    self.publish_original_vector_cancellation(guard, index, row, plan)
                })
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn move_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        old_row_id: RowId,
        new_row_id: RowId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Reverse membership decoding is part of this move, on the exact
        // supplied read capability. Each temporary record ends its own invoice.
        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader_per_call_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
            "reader",
            "materialize_answer",
            "Reduce the statement's output or raise the declared reader memory limit.",
        )?
        .enter();

        let _vector_schema = self.vector_schema_read(&index);
        self.record_vector_schema_epoch(tx, &index)?;
        self.ensure_trigger_table_ready(&index.table, "move_vector")?;
        if !self.vector_store.contains_index(&index) {
            return Err(Error::UnknownVectorIndex { index });
        }
        if old_row_id == new_row_id {
            let meta = self
                .table_meta(&index.table)
                .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
            let declaration = meta
                .columns
                .iter()
                .find(|column| column.name == index.column)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            if declaration.partition_key_columns.is_none() {
                return Ok(());
            }
            let snapshot = self.snapshot_for_read();
            let Some(old_row) =
                self.committed_row_at(&index.table, old_row_id, snapshot, read_memory.clone())?
            else {
                return Ok(());
            };
            let new_row = self.find_row_by_id_in_tx(
                Some(tx),
                &index.table,
                new_row_id,
                snapshot,
                read_memory.clone(),
            )?;
            let old_key = crate::composite_store::vector_partition_key_for_row(
                &index,
                &meta,
                declaration,
                &old_row,
            )?;
            let new_key = crate::composite_store::vector_partition_key_for_row(
                &index,
                &meta,
                declaration,
                &new_row,
            )?;
            if old_key == new_key {
                if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
                    return handle.with_host(|host| {
                        self.move_original_vector(
                            host, tx, &index, old_row_id, new_row_id, false, true,
                        )
                    });
                }
                self.tx_mgr.with_write_set(tx, |ws| {
                    let admission =
                        crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                            0,
                        )?;
                    let undo = ws.ordered_staged_rows.prepare_vector_move_cancellation(
                        &index,
                        old_row_id,
                        &ws.vector_moves,
                        admission,
                        |bytes| {
                            Ok(
                                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                    crate::memory_accounting::AccountantHandle::clone(
                                        &self.finalization.accountant,
                                    ),
                                    bytes,
                                    "vector",
                                    "mutation_undo",
                                    "Reduce staged vectors or raise MEMORY_LIMIT.",
                                )?
                                .into_admitted_arc()?,
                            )
                        },
                    )?;
                    ws.vector_moves.retain(|(i, from, to, _)| {
                        !(i == &index && *from == old_row_id && *to == new_row_id)
                    });
                    if let Some(undo) = undo {
                        ws.ordered_staged_rows
                            .publish_vector_move_undo(undo, Vec::new());
                    }
                    Ok::<_, Error>(())
                })??;
                return Ok(());
            }
        }
        let existing_live = self.vector_store.has_live_entry_for_row(
            &index,
            old_row_id,
            self.snapshot_for_read(),
            vector_copy_owner,
        )?;
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                self.move_original_vector(
                    host,
                    tx,
                    &index,
                    old_row_id,
                    new_row_id,
                    existing_live,
                    false,
                )
            });
        }
        let replaced_inserts = self.tx_mgr.with_write_set(tx, |ws| {
            let moving_position = ws
                .vector_inserts
                .iter()
                .rposition(|entry| entry.index == index && entry.row_id == old_row_id);
            let bytes = ws
                .vector_inserts
                .iter()
                .enumerate()
                .filter(|(position, entry)| {
                    entry.index == index
                        && ((new_row_id != old_row_id && entry.row_id == new_row_id)
                            || (entry.row_id == old_row_id && Some(*position) != moving_position))
                })
                .map(|(_, entry)| {
                    self.vector_insert_accounted_bytes(
                        &entry.index,
                        entry.vector.len(),
                        vector_copy_owner,
                    )
                })
                .try_fold(0usize, |total, bytes| {
                    bytes.map(|bytes| total.saturating_add(bytes))
                })?;
            let admission = crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                0,
            )?;
            let undo = ws.ordered_staged_rows.prepare_vector_move_undo(
                &index,
                old_row_id,
                new_row_id,
                &ws.vector_inserts,
                &ws.vector_moves,
                admission.clone(),
                |bytes| {
                    Ok(
                        crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                            bytes,
                            "vector",
                            "mutation_undo",
                            "Reduce staged vectors or raise MEMORY_LIMIT.",
                        )?
                        .into_admitted_arc()?,
                    )
                },
            )?;
            let prepared_vector = ws.ordered_staged_rows.prepare_moved_vector_capability(
                &index,
                old_row_id,
                new_row_id,
                |bytes| {
                    Ok(
                        crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                            crate::memory_accounting::AccountantHandle::clone(
                                &self.finalization.accountant,
                            ),
                            bytes,
                            "vector",
                            "staged_immutable_owner",
                            "Reduce staged vectors or raise MEMORY_LIMIT.",
                        )?
                        .into_admitted_arc()?,
                    )
                },
            )?;
            let old_row_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    });

            let mut moving_insert = None;
            let mut replaced_inserts = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == index
                    && ws.vector_inserts[pos].row_id == old_row_id
                {
                    let entry = ws.vector_inserts.remove(pos);
                    if let Some(previous) = moving_insert.replace(entry) {
                        replaced_inserts.push(previous);
                    }
                } else {
                    pos += 1;
                }
            }

            let mut moved_any = moving_insert.is_some();
            let mut has_move_from_old = false;
            for (move_index, source_row_id, destination_row_id, _) in &mut ws.vector_moves {
                if *move_index != index {
                    continue;
                }
                if *destination_row_id == old_row_id {
                    *destination_row_id = new_row_id;
                    moved_any = true;
                }
                if *source_row_id == old_row_id {
                    *destination_row_id = new_row_id;
                    has_move_from_old = true;
                    moved_any = true;
                }
            }

            if !moved_any && existing_live && !old_row_deleted {
                ws.vector_moves
                    .push((index.clone(), old_row_id, new_row_id, tx));
                moved_any = true;
                has_move_from_old = true;
            }

            if moved_any {
                let mut pos = 0;
                while pos < ws.vector_inserts.len() {
                    if ws.vector_inserts[pos].index == index
                        && ws.vector_inserts[pos].row_id == new_row_id
                    {
                        replaced_inserts.push(ws.vector_inserts.remove(pos));
                    } else {
                        pos += 1;
                    }
                }
            }

            if let Some(mut entry) = moving_insert {
                entry.row_id = new_row_id;
                ws.vector_inserts.push(entry);
            } else if has_move_from_old {
                let mut seen_move_from_old = false;
                ws.vector_moves.retain(|(move_index, source_row_id, _, _)| {
                    if *move_index == index && *source_row_id == old_row_id {
                        if seen_move_from_old {
                            false
                        } else {
                            seen_move_from_old = true;
                            true
                        }
                    } else {
                        true
                    }
                });
            }

            if moved_any {
                ws.ordered_staged_rows
                    .remove_vector_capability(&index, old_row_id);
                ws.ordered_staged_rows
                    .remove_vector_capability(&index, new_row_id);
                if let Some(prepared) = prepared_vector {
                    ws.ordered_staged_rows
                        .publish_vector_capability(&index, prepared);
                }
            }
            if let Some(undo) = undo {
                admission.adopt(bytes);
                ws.ordered_staged_rows
                    .publish_vector_move_undo(undo, replaced_inserts);
                Ok::<_, Error>(Vec::new())
            } else {
                Ok::<_, Error>(replaced_inserts)
            }
        })??;
        for replaced in replaced_inserts {
            self.finalization
                .accountant
                .release(self.vector_insert_accounted_bytes(
                    &replaced.index,
                    replaced.vector.len(),
                    vector_copy_owner,
                )?);
        }
        Ok(())
    }

    pub(super) fn vector_move_source_digest(
        read: &contextdb_tx::manager::EngineRead<'_>,
        index: &VectorIndexRef,
        old: RowId,
        new: RowId,
    ) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        // Count/length-framed families plus fixed-width coordinates: this is
        // an optimistic pending-image witness, never an ownership certificate.
        hash.update(&(read.vector_moves.len() as u64).to_le_bytes());
        for (index, from, to, tx) in read.vector_moves {
            hash.update(&(index.table.len() as u64).to_le_bytes());
            hash.update(index.table.as_bytes());
            hash.update(&(index.column.len() as u64).to_le_bytes());
            hash.update(index.column.as_bytes());
            hash.update(&from.0.to_le_bytes());
            hash.update(&to.0.to_le_bytes());
            hash.update(&tx.0.to_le_bytes());
        }
        let selected = |entry: &&VectorEntry| {
            entry.index == *index && (entry.row_id == old || entry.row_id == new)
        };
        hash.update(&(read.vector_inserts.iter().filter(selected).count() as u64).to_le_bytes());
        for entry in read.vector_inserts.iter().filter(selected) {
            hash.update(&entry.row_id.0.to_le_bytes());
            hash.update(&entry.created_tx.0.to_le_bytes());
            hash.update(&[u8::from(entry.deleted_tx.is_some())]);
            hash.update(&entry.deleted_tx.map_or(0, |tx| tx.0).to_le_bytes());
            hash.update(&entry.lsn.0.to_le_bytes());
            hash.update(&(entry.vector.len() as u64).to_le_bytes());
            for value in &entry.vector {
                hash.update(&value.to_bits().to_le_bytes());
            }
        }
        hash.update(
            &(read
                .vector_deletes
                .iter()
                .filter(|(have, _, _)| have == index)
                .count() as u64)
                .to_le_bytes(),
        );
        for (_, row, tx) in read
            .vector_deletes
            .iter()
            .filter(|(have, _, _)| have == index)
        {
            hash.update(&row.0.to_le_bytes());
            hash.update(&tx.0.to_le_bytes());
        }
        *hash.finalize().as_bytes()
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn move_original_vector(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        index: &VectorIndexRef,
        old: RowId,
        new: RowId,
        existing_live: bool,
        cancel_only: bool,
    ) -> Result<()> {
        use crate::custody::preparation::{OwnedPreparationValue, preparation_array_bytes};
        struct Plan {
            moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
            remove: Vec<usize>,
            moving: std::cell::RefCell<Option<OriginalDatabaseVector>>,
            digest: [u8; 32],
            changed: bool,
        }
        let overflow = || Error::Other("vector move plan size overflow".into());
        let (digest, count, names, moving_bytes) = self.tx_mgr.with_read(tx, |read| {
            let names = read
                .vector_moves
                .iter()
                .try_fold(0usize, |sum, (index, _, _, _)| {
                    sum.checked_add(index.table.len())
                        .and_then(|sum| sum.checked_add(index.column.len()))
                        .ok_or_else(overflow)
                })?;
            let moving = if cancel_only {
                None
            } else {
                read.vector_inserts
                    .iter()
                    .rfind(|entry| entry.index == *index && entry.row_id == old)
                    .map(crate::memory_accounting::preparation_vector_copy_bytes)
                    .transpose()?
            };
            Ok::<_, Error>((
                Self::vector_move_source_digest(&read, index, old, new),
                read.vector_moves.len(),
                names,
                moving,
            ))
        })??;
        let capacity = count.checked_add(1).ok_or_else(overflow)?;
        let bytes = preparation_array_bytes::<(VectorIndexRef, RowId, RowId, TxId)>(capacity)?
            .checked_add(preparation_array_bytes::<usize>(count)?)
            .and_then(|sum| sum.checked_add(names))
            .and_then(|sum| sum.checked_add(index.table.len()))
            .and_then(|sum| sum.checked_add(index.column.len()))
            .ok_or_else(overflow)?;
        let mut moving_admission = moving_bytes
            .map(|bytes| {
                host.events.with_owned_persistence_original(|original| {
                    crate::memory_accounting::reserve_preparation_from_original(
                        original,
                        bytes,
                        "vector",
                        "original_moved_vector",
                        "Reduce staged vectors or raise MEMORY_LIMIT.",
                    )
                })
            })
            .transpose()?;
        let plan = OwnedPreparationValue::try_produce(self, bytes, || {
            self.tx_mgr.with_read(tx, |read| {
                if Self::vector_move_source_digest(&read, index, old, new) != digest {
                    return Err(Error::Other(
                        "vector move source changed during preparation".into(),
                    ));
                }
                let mut plan = Plan {
                    moves: Vec::with_capacity(capacity),
                    remove: Vec::with_capacity(count),
                    moving: std::cell::RefCell::new(None),
                    digest,
                    changed: false,
                };
                if plan.moves.capacity() != capacity || plan.remove.capacity() != count {
                    return Err(Error::Other("source native capacity mismatch".into()));
                }
                if !cancel_only
                    && let Some(entry) = read
                        .vector_inserts
                        .iter()
                        .rfind(|entry| entry.index == *index && entry.row_id == old)
                {
                    let admission = moving_admission
                        .take()
                        .ok_or_else(|| Error::Other("moved original lost its admission".into()))?;
                    let mut body = OriginalDatabaseVector {
                        entry: crate::memory_accounting::preparation_copy_vector(entry)?,
                        scalar: false,
                        cancellation: None,
                        admission,
                    };
                    body.entry.row_id = new;
                    *plan.moving.get_mut() = Some(body);
                    plan.changed = true;
                }
                let mut from_old = false;
                for (position, (have, from, to, created)) in read.vector_moves.iter().enumerate() {
                    plan.remove.push(position);
                    if cancel_only && have == index && *from == old && *to == new {
                        plan.changed = true;
                        continue;
                    }
                    let mut destination = *to;
                    if !cancel_only && have == index {
                        if *to == old {
                            destination = new;
                            plan.changed = true;
                        }
                        if *from == old {
                            destination = new;
                            plan.changed = true;
                            if plan.moving.get_mut().is_none() && from_old {
                                continue;
                            }
                            from_old = true;
                        }
                    }
                    plan.moves.push((
                        VectorIndexRef {
                            table: crate::memory_accounting::preparation_copy_string(&have.table)?,
                            column: crate::memory_accounting::preparation_copy_string(
                                &have.column,
                            )?,
                        },
                        *from,
                        destination,
                        *created,
                    ));
                }
                let deleted = read
                    .vector_deletes
                    .iter()
                    .any(|(have, row, _)| have == index && *row == old);
                if !cancel_only && !plan.changed && existing_live && !deleted {
                    plan.moves.push((
                        VectorIndexRef {
                            table: crate::memory_accounting::preparation_copy_string(&index.table)?,
                            column: crate::memory_accounting::preparation_copy_string(
                                &index.column,
                            )?,
                        },
                        old,
                        new,
                        tx,
                    ));
                    plan.changed = true;
                }
                Ok(plan)
            })?
        })?;
        plan.with_value(|plan| {
            if !plan.changed {
                return Ok(());
            }
            host.transactions.with_transactions(|lower| {
                lower.with_engine(tx, |guard| {
                    if Self::vector_move_source_digest(&guard.read(), index, old, new)
                        != plan.digest
                    {
                        return Err(Error::Other(
                            "vector move source changed before publication".into(),
                        ));
                    }
                    guard.with_tx_checkpoint(|guard| {
                        let moves = guard.copy_aux(
                            contextdb_tx::staged_source::AuxBorrow::VectorMoves(&plan.moves),
                        )?;
                        if !cancel_only {
                            let removal = guard.prepare_remove_vector(index, old)?;
                            guard.publish(removal);
                            if new != old {
                                let removal = guard.prepare_remove_vector(index, new)?;
                                guard.publish(removal);
                            }
                        }
                        let replacement = guard.prepare_aux_splice(0, &plan.remove, moves)?;
                        guard.publish_tx(replacement);
                        if let Some(mut moving) = plan.moving.borrow_mut().take() {
                            let invoice = moving
                                .admission
                                .into_source_vector_invoice_for(&host.funder)?;
                            let body = guard.prepare_vector(moving.entry, None, invoice)?;
                            guard.publish(body);
                        }
                        Ok(())
                    })
                })
            })
        })
    }

    pub(super) fn vector_membership_owner(
        &self,
    ) -> Option<crate::persistence::vector_membership_source::MembershipOwner> {
        use crate::persistence::vector_membership_source::MembershipOwner;
        self.persistence
            .as_ref()
            .map(|source| MembershipOwner::Writable(source.clone()))
            .or_else(|| {
                self.retained_graph_source
                    .as_ref()
                    .map(|source| MembershipOwner::Retained(source.clone()))
            })
    }

    pub(super) fn vector_historical_read_intent(
        &self,
        snapshot: SnapshotId,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Option<crate::persistence::vector_membership_source::HistoricalReadIntentScope> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        self.vector_membership_owner().map(|owner| {
            crate::persistence::vector_membership_source::enter_historical_read_intent(
                owner, snapshot, memory,
            )
        })
    }

    // Every resumed storage call derives its mark from the retained capture;
    // a guard from a previous fetch is never treated as still executing.
    pub(super) fn with_vector_read_admission<T>(
        &self,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        operation: impl FnOnce(&mut contextdb_vector::VectorCopyAdmission) -> T,
    ) -> T {
        if self
            .vector_membership_owner()
            .is_some_and(|owner| owner.historical_operation())
        {
            copies.with_historical_durable_reads(operation)
        } else {
            operation(copies)
        }
    }

    /// Historical vector consumers keep the same physical capture and reader
    /// child through candidate discovery, resident/cold selection and scoring.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_read_scope(
        &self,
        snapshot: SnapshotId,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        copies: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<crate::persistence::vector_membership_source::VectorReadScope>> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        self.vector_read_preflight(snapshot, memory, copies, None)
    }

    /// Hold this guard before any engine-derived restriction or empty result.
    /// The supplied context is the executing bounded kernel's context.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_read_preflight(
        &self,
        snapshot: SnapshotId,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        checkpoint: Option<crate::executor::bounded::ReadOpenContext>,
    ) -> Result<Option<crate::persistence::vector_membership_source::VectorReadScope>> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        use crate::persistence::vector_membership_source::{self, MembershipOwner};
        let source = if let Some(persistence) = &self.persistence {
            MembershipOwner::Writable(persistence.clone())
        } else if let Some(source) = &self.retained_graph_source {
            MembershipOwner::Retained(source.clone())
        } else {
            return Ok(None);
        };
        let scope = vector_membership_source::enter_read_operation(
            source,
            snapshot,
            memory.clone(),
            copies,
            &mut |partition| {
                if self
                    .vector_store
                    .try_partition_state(&partition.index, &partition.partition_key)
                    .is_none()
                {
                    return Err(crate::funded_error::corruption_text(
                        Ok(memory.clone()),
                        &partition.index.table,
                        "captured vector route has no installed partition source".into(),
                    ));
                }
                Ok(())
            },
            checkpoint,
        )?;
        // Declaration-owned empty routes are an independent schema obligation.
        // They are not discovered by walking surviving membership records.
        if let Some(root) = self.writer_read_root()? {
            if self
                .vector_membership_owner()
                .is_some_and(|owner| owner.historical_operation())
            {
                vector_membership_source::check_declared_routes(
                    Some(&root),
                    None,
                    &self.vector_store,
                    copies,
                )?;
            }
        } else if self
            .vector_membership_owner()
            .is_some_and(|owner| owner.historical_operation())
        {
            let tables = self.relational_store.table_meta.read();
            vector_membership_source::check_declared_routes(
                None,
                Some(&tables),
                &self.vector_store,
                copies,
            )?;
        }
        Ok(Some(scope))
    }

    /// The reservation transfer through which the store retains a durable
    /// generation a read loads, funded by that read's executing owner.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_load_transfer(
        &self,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        contextdb_core::read_memory::ReadCredit,
        crate::memory_accounting::VectorLoadReservationTransfer,
    )> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn callback(
            source: impl Into<contextdb_vector::MemoryBudgetOwner>,
            original: contextdb_core::read_memory::ReadMemoryOwner,
        ) -> impl Fn(&crate::memory_accounting::MemoryAccountant, usize) -> Result<()> + Send + Sync
        {
            let source: contextdb_vector::MemoryBudgetOwner = source.into();

            move |destination: &crate::memory_accounting::MemoryAccountant, bytes: usize| {
                destination.transfer_vector_load_reservation(&original, bytes, |prepaid, moved| {
                    if prepaid {
                        *moved = true;
                    } else {
                        source.release(bytes);
                    }
                    Ok(())
                })
            }
        }
        let prototype = callback(executing_owner.clone(), read_memory.clone());
        let control_bytes = std::alloc::Layout::new::<[usize; 2]>()
            .extend(std::alloc::Layout::for_value(&prototype))
            .map(|(layout, _)| layout.pad_to_align().size())
            .map_err(|_| {
                Error::Other(("vector load transfer layout overflows".to_string()).into())
            })?;
        drop(prototype);
        let funding = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            control_bytes,
            "vector_generation",
            "load_transfer_callback",
            "Raise reader memory to fund the vector load transfer callback.",
        )?;
        // The invoice lives outside the Arc it funds. Capturing the invoice
        // inside its own closure would refund before Arc physically frees it.
        Ok((funding, Arc::new(callback(executing_owner, read_memory))))
    }

    /// One owner-door vector search over every admitted partition. Copied
    /// search output uses the supplied executing read owner; durable generation
    /// loading retains its existing store reservation transfer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn owner_vector_search(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<(
        contextdb_vector::mem::SearchScores,
        RetainedVectorSearchTrace,
    )> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_load_transfer_funding, load_transfer) =
            self.vector_load_transfer(executing_owner.clone(), read_memory)?;
        let (rows, trace) = crate::memory_accounting::with_vector_load_reservation_transfer(
            &self.finalization.accountant,
            load_transfer,
            || {
                if self
                    .vector_membership_owner()
                    .is_some_and(|owner| owner.historical_operation())
                {
                    vector_copy_owner.with_historical_durable_reads(|owner| {
                        self.vector.search_with_mode_admission(
                            index,
                            query,
                            k,
                            candidates,
                            (snapshot, mode),
                            owner,
                        )
                    })
                } else {
                    self.vector.search_with_mode_owned(
                        index,
                        query,
                        k,
                        candidates,
                        snapshot,
                        mode,
                        executing_owner.clone(),
                    )
                }
            },
        )?;
        Ok((
            rows,
            RetainedVectorSearchTrace::from_owned(trace, executing_owner)?,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn query_vector(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        let read_memory = self.declared_reader_memory()?;
        let executing_owner = crate::vector_read_memory::budget(read_memory.clone());
        let mut vector_copy_owner =
            contextdb_vector::VectorCopyAdmission::new(executing_owner.clone());
        let scores = self.query_vector_with_read_memory(
            index,
            query,
            k,
            candidates,
            snapshot,
            &mut vector_copy_owner,
            read_memory,
            executing_owner.clone(),
        )?;
        let (rows, _credit) = scores.into_prepaid(&executing_owner)?;
        Ok(rows)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn query_vector_with_read_memory(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<contextdb_vector::mem::SearchScores> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.clear_last_vector_search_trace();
        let _operation = self.open_operation()?;
        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        let _membership_capture = self.relational_store.begin_membership_capture();
        let _snapshot_registration = self.snapshot_registry.register_statement(snapshot);
        let _vector_schema = self.vector_schema_read(&index);
        if !self.vector_store.contains_index(&index) {
            return Err(Error::UnknownVectorIndex { index });
        }
        let _vector_capture = if k == 0 {
            None
        } else {
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?
        };
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let search_mode = self.resolved_vector_search_mode(&index, None)?;
            let effective_candidates = self.effective_vector_read_candidates(
                &index,
                snapshot,
                candidates,
                search_mode,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            // A durable generation this search loads is retained by the store
            // through the same reservation transfer every vector search door
            // installs; the executing read owner funds it.
            let (_load_transfer_funding, load_transfer) =
                self.vector_load_transfer(executing_owner.clone(), read_memory.clone())?;
            let (rows, trace) = crate::memory_accounting::with_vector_load_reservation_transfer(
                &self.finalization.accountant,
                load_transfer,
                || {
                    if self
                        .vector_membership_owner()
                        .is_some_and(|owner| owner.historical_operation())
                    {
                        vector_copy_owner.with_historical_durable_reads(|owner| {
                            self.vector.search_with_mode_admission(
                                index,
                                query,
                                k,
                                effective_candidates.as_ref(),
                                (snapshot, search_mode),
                                owner,
                            )
                        })
                    } else {
                        self.vector.search_with_mode_owned(
                            index,
                            query,
                            k,
                            effective_candidates.as_ref(),
                            snapshot,
                            search_mode,
                            executing_owner.clone(),
                        )
                    }
                },
            )?;
            self.publish_vector_search_trace(trace);
            Ok(rows)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn semantic_search(&self, query: SemanticQuery) -> Result<Vec<SearchResult>> {
        let read_memory = self.declared_reader_memory()?;
        let executing_owner = crate::vector_read_memory::budget(read_memory.clone());
        let mut vector_copy_owner =
            contextdb_vector::VectorCopyAdmission::new(executing_owner.clone());
        self.semantic_search_with_read_memory(
            query,
            &mut vector_copy_owner,
            read_memory,
            executing_owner.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn semantic_search_with_read_memory(
        &self,
        query: SemanticQuery,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<Vec<SearchResult>> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        self.semantic_search_with_candidates(
            query,
            None,
            vector_copy_owner,
            read_memory.clone(),
            executing_owner.clone(),
        )
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __semantic_search_in_tx_for_test(
        &self,
        tx: TxId,
        query: SemanticQuery,
    ) -> Result<Vec<SearchResult>> {
        let _operation = self.open_operation()?;
        let index = VectorIndexRef::new(query.table.clone(), query.vector_column.clone());
        let _vector_schema = self.vector_schema_read(&index);
        let read_memory = self.declared_reader_memory()?;
        let executing_owner = crate::vector_read_memory::budget(read_memory.clone());
        let mut vector_copy_owner =
            contextdb_vector::VectorCopyAdmission::new(executing_owner.clone());
        self.semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
            Some(tx),
            query,
            None,
            &mut vector_copy_owner,
            read_memory,
            executing_owner,
        )
        .map(|(results, trace)| {
            self.publish_vector_search_trace(trace.into_published());
            results
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn semantic_search_with_candidates(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<Vec<SearchResult>> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let index = VectorIndexRef::new(query.table.clone(), query.vector_column.clone());
        let _vector_schema = self.vector_schema_read(&index);
        self.semantic_search_with_candidates_under_schema_read(
            query,
            candidates,
            vector_copy_owner,
            read_memory.clone(),
            executing_owner.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn semantic_search_with_candidates_under_schema_read(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<Vec<SearchResult>> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.semantic_search_with_candidates_under_schema_read_with_strategy(
            query,
            candidates,
            vector_copy_owner,
            read_memory.clone(),
            executing_owner.clone(),
        )
        .map(|(results, _)| results)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn semantic_search_with_candidates_under_schema_read_with_strategy(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<(Vec<SearchResult>, bool)> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
            None,
            query,
            candidates,
            vector_copy_owner,
            read_memory.clone(),
            executing_owner.clone(),
        )
        .map(|(results, trace)| {
            let used_hnsw = trace.used_hnsw;
            self.publish_vector_search_trace(trace.into_published());
            (results, used_hnsw)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
        &self,
        tx: Option<TxId>,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<(Vec<SearchResult>, RetainedVectorSearchTrace)> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.clear_last_vector_search_trace();
        let index = VectorIndexRef::new(query.table.clone(), query.vector_column.clone());
        let _membership_capture = self.relational_store.begin_membership_capture();
        let snapshot = self.snapshot_for_read();
        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        let _vector_capture = if query.limit == 0 {
            None
        } else {
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?
        };
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let _snapshot_registration = self.snapshot_registry.register_statement(snapshot);
            let meta = self
                .table_meta(&query.table)
                .ok_or_else(|| Error::TableNotFound(query.table.clone()))?;
            let vector_column = meta
                .columns
                .iter()
                .find(|column| column.name == query.vector_column)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let mut search_mode = query.search_mode.unwrap_or(vector_column.search_mode);

            let mut candidate_bitmap = candidates;
            if query.where_clause.is_some()
                || self.bounded_read_requires_candidate_filter(&index.table)?
            {
                let where_clause = query.where_clause.as_deref().unwrap_or("");
                let (where_bitmap, complete_indexed_route) = self.semantic_where_candidate_bitmap(
                    tx,
                    &index,
                    where_clause,
                    snapshot,
                    search_mode == contextdb_core::VectorSearchMode::Indexed,
                    search_mode == contextdb_core::VectorSearchMode::Auto,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if search_mode == contextdb_core::VectorSearchMode::Auto && !complete_indexed_route
                {
                    search_mode = contextdb_core::VectorSearchMode::Exact;
                }
                candidate_bitmap = Some(match candidate_bitmap {
                    Some(mut existing) => {
                        existing &= where_bitmap;
                        existing
                    }
                    None => where_bitmap,
                });
            }

            let Some(sort_key) = query.sort_key.as_deref() else {
                // A score floor filters the nearest LIMIT candidates; it never
                // widens an indexed query to the whole authorized population.
                let (mut rows, trace) = self.query_vector_strict_in_tx_with_strategy(
                    tx,
                    index.clone(),
                    &query.query,
                    query.limit,
                    candidate_bitmap.as_ref(),
                    snapshot,
                    search_mode,
                    vector_copy_owner,
                    read_memory.clone(),
                    executing_owner.clone(),
                )?;
                if let Some(min_similarity) = query.min_similarity {
                    rows.retain(|(_, score)| *score >= min_similarity);
                    rows.truncate(query.limit);
                }
                let results = rows
                    .into_iter()
                    .map(|(row_id, vector_score)| {
                        let anchor = self.find_row_by_id_in_tx(
                            tx,
                            &query.table,
                            row_id,
                            snapshot,
                            read_memory.clone(),
                        )?;
                        let values = self.search_result_values_in_tx(
                            tx,
                            &index,
                            row_id,
                            snapshot,
                            anchor.values,
                            vector_copy_owner,
                            read_memory.clone(),
                        )?;
                        Ok(SearchResult {
                            row_id,
                            values,
                            vector_score,
                            rank: vector_score,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok((results, trace));
            };

            let Some(policy) = vector_column.rank_policy.as_ref() else {
                return Err(Error::RankPolicyNotFound {
                    index: rank_index_name(&query.table, &query.vector_column),
                    sort_key: sort_key.to_string(),
                });
            };
            if policy.sort_key != sort_key {
                return Err(Error::RankPolicyNotFound {
                    index: rank_index_name(&query.table, &query.vector_column),
                    sort_key: sort_key.to_string(),
                });
            }
            let formula = self.rank_formula(&query.table, &query.vector_column)?;
            let entry_count = self.vector_entry_count_in_tx(
                tx,
                &index,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let whole_pool = self.rank_policy_candidate_k(
                &index,
                entry_count,
                query.limit,
                true,
                candidate_bitmap.as_ref(),
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let internal_k = self.rank_policy_candidate_k(
                &index,
                entry_count,
                query.limit,
                search_mode == contextdb_core::VectorSearchMode::Exact,
                candidate_bitmap.as_ref(),
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let (mut raw, mut trace) =
                contextdb_vector::mem::with_search_policy_limit(query.limit, || {
                    self.query_vector_strict_in_tx_with_strategy(
                        tx,
                        index.clone(),
                        &query.query,
                        internal_k,
                        candidate_bitmap.as_ref(),
                        snapshot,
                        search_mode,
                        vector_copy_owner,
                        read_memory.clone(),
                        executing_owner.clone(),
                    )
                })?;
            if search_mode == contextdb_core::VectorSearchMode::Auto
                && !trace.used_hnsw
                && internal_k < whole_pool
                && raw.len() >= internal_k
            {
                // AUTO resolved to the exact route -- below the automatic
                // crossover, or because the graph could not serve this read --
                // and inherits that route's candidate meaning: the formula runs
                // over every allowed stored vector. The bounded pool it was
                // handed can have cut the best-scoring row before the formula
                // ever saw it, so the read is repeated over the whole pool,
                // behind the same score-budget refusal an explicit EXACT read
                // passes.
                drop(raw);
                let (rerun, rerun_trace) = self.query_vector_strict_in_tx_with_strategy(
                    tx,
                    index.clone(),
                    &query.query,
                    whole_pool,
                    candidate_bitmap.as_ref(),
                    snapshot,
                    search_mode,
                    vector_copy_owner,
                    read_memory.clone(),
                    executing_owner.clone(),
                )?;
                raw = rerun;
                trace = rerun_trace;
            }
            if let Some(min_similarity) = query.min_similarity {
                raw.retain(|(_, score)| *score >= min_similarity);
            }

            // Only the rows still in contention for the answer are held: one
            // evaluated candidate past `LIMIT` is enough to know which to drop, so
            // a rank over the whole table holds `LIMIT` rows' values, never every
            // row's. The scores themselves are what the exact budget pays for.
            let mut contenders = std::collections::BinaryHeap::new();
            for (row_id, vector_score) in raw {
                crate::executor::observe_bounded_rank_candidate();
                let anchor = self.find_row_by_id_in_tx(
                    tx,
                    &query.table,
                    row_id,
                    snapshot,
                    read_memory.clone(),
                )?;
                let joined = self.joined_row_for_rank_policy(
                    tx,
                    policy,
                    &anchor,
                    snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                self.rank_policy_eval_count.fetch_add(1, Ordering::SeqCst);
                let eval = formula.eval_with_resolver(vector_score, |column| {
                    self.resolve_rank_formula_column(policy, &anchor, joined.as_ref(), column)
                });
                let rank = match eval {
                    Ok(Some(rank)) => rank,
                    Ok(None) => f32::NAN,
                    Err(err) => {
                        let error_row_id =
                            if matches!(err, FormulaEvalError::CorruptJoinedColumn { .. }) {
                                joined.as_ref().map(|row| row.row_id).unwrap_or(row_id)
                            } else {
                                row_id
                            };
                        self.warn_rank_eval_error(
                            &query.table,
                            &query.vector_column,
                            error_row_id,
                            &err,
                        );
                        continue;
                    }
                };
                contenders.push(RankedSearchCandidate {
                    row_id,
                    vector_score,
                    rank,
                    anchor,
                    joined,
                });
                if contenders.len() > query.limit {
                    // The heap's top is the candidate that ranks last.
                    contenders.pop();
                }
            }
            let mut ranked = Vec::with_capacity(contenders.len());
            for candidate in contenders.into_sorted_vec() {
                let values = self.search_result_values_in_tx(
                    tx,
                    &index,
                    candidate.row_id,
                    snapshot,
                    merged_rank_values(&candidate.anchor, candidate.joined.as_ref()),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                ranked.push(SearchResult {
                    row_id: candidate.row_id,
                    values,
                    vector_score: candidate.vector_score,
                    rank: candidate.rank,
                });
            }
            Ok((ranked, trace))
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __rank_policy_eval_count(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.rank_policy_eval_count.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_rank_policy_eval_count(&self) {
        let _operation = self.assert_open_operation();
        self.rank_policy_eval_count.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __rank_policy_formula_parse_count(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.rank_policy_formula_parse_count.load(Ordering::SeqCst)
    }

    /// Acceptance-test accessor: threads currently in this database's trigger
    /// callback owner map. Zero at rest — nonzero after every callback
    /// completed means `TriggerCallbackThreadGuard::drop` leaked an entry.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn trigger_callback_owner_thread_count_for_test(&self) -> usize {
        self.trigger.owner_thread_count()
    }

    /// Acceptance-test accessor. Reads at steady state (after stop signal
    /// + writer-threads joined). Snapshots are not atomic across counters.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn trigger_progress_telemetry_snapshot_for_test(&self) -> TriggerProgressTelemetrySnapshot {
        TriggerProgressTelemetrySnapshot {
            wait_observed: self.trigger.wait_observed_count.load(Ordering::SeqCst),
            typed_err_observed_same_db: self
                .trigger
                .typed_err_observed_same_db_count
                .load(Ordering::SeqCst),
            deadlock_guard_timeout_observed: self
                .trigger
                .deadlock_guard_timeout_observed_count
                .load(Ordering::SeqCst),
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __inject_raw_joined_row_value_for_test(
        &self,
        table: &str,
        row_id: RowId,
        column: &str,
        _raw_bytes: Vec<u8>,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        self.corrupt_joined_values
            .write()
            .insert((table.to_string(), row_id, column.to_string()));
        Ok(())
    }

    /// Transaction, scope, snapshot, and mode are separate execution inputs;
    /// combining them would obscure the vector-query contract at each caller.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn query_vector_strict_in_tx_with_strategy(
        &self,
        tx: Option<TxId>,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        search_mode: contextdb_core::VectorSearchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<(
        contextdb_vector::mem::SearchScores,
        RetainedVectorSearchTrace,
    )> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.vector_store.validate_vector(&index, query.len())?;
        let _scope = self.writer_read_scope(read_memory.clone())?;
        let _vector_capture = if k == 0 {
            None
        } else {
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?
        };
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            if let Some(tx) = tx
                && self
                    .tx_mgr
                    .with_ordinary_read(tx, |ws| write_set_touches_vector_search(ws, &index))?
            {
                if k == 0 {
                    return self.tx_mgr.with_ordinary_read(tx, |ws| {
                        self.query_vector_strict_with_write_set_overlay(
                            ws,
                            StrictVectorOverlayRequest {
                                index: index.clone(),
                                query,
                                k,
                                candidates,
                                snapshot,
                                search_mode,
                            },
                            vector_copy_owner,
                            read_memory.clone(),
                            executing_owner.clone(),
                        )
                    })?;
                }
                let captured =
                    self.capture_vector_write_set::<Error>(tx, &index, true, &mut |_| Ok(()))?;
                if let Some(captured) = captured {
                    if captured.touches {
                        return self.query_vector_strict_with_captured_overlay(
                            &captured,
                            StrictVectorOverlayRequest {
                                index,
                                query,
                                k,
                                candidates,
                                snapshot,
                                search_mode,
                            },
                            vector_copy_owner,
                            read_memory.clone(),
                            executing_owner.clone(),
                        );
                    }
                } else {
                    // Raw public WriteSet payloads without a mutation-time owner
                    // retain their existing legacy semantics. This branch is not
                    // certified as mutex-free; it must not gain a new refusal.
                    let overlay_result = self.tx_mgr.with_ordinary_read(tx, |ws| {
                        write_set_touches_vector_search(ws, &index).then(|| {
                            self.query_vector_strict_with_write_set_overlay(
                                ws,
                                StrictVectorOverlayRequest {
                                    index: index.clone(),
                                    query,
                                    k,
                                    candidates,
                                    snapshot,
                                    search_mode,
                                },
                                vector_copy_owner,
                                read_memory.clone(),
                                executing_owner.clone(),
                            )
                        })
                    })?;
                    if let Some(result) = overlay_result {
                        return result;
                    }
                }
            }
            let effective_candidates = self.effective_vector_read_candidates(
                &index,
                snapshot,
                candidates,
                search_mode,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let (rows, trace) = self.owner_vector_search(
                index,
                query,
                k,
                effective_candidates.as_ref(),
                snapshot,
                search_mode,
                executing_owner.clone(),
                read_memory.clone(),
                vector_copy_owner,
            )?;
            self.last_vector_search_used_hnsw
                .store(trace.used_hnsw, Ordering::SeqCst);
            Ok((rows, trace))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn resolved_vector_search_mode(
        &self,
        index: &VectorIndexRef,
        requested: Option<contextdb_core::VectorSearchMode>,
    ) -> Result<contextdb_core::VectorSearchMode> {
        if let Some(requested) = requested {
            return Ok(requested);
        }
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        meta.columns
            .iter()
            .find(|column| column.name == index.column)
            .map(|column| column.search_mode)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn query_vector_strict_with_write_set_overlay(
        &self,
        ws: &WriteSet,
        request: StrictVectorOverlayRequest<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<(
        contextdb_vector::mem::SearchScores,
        RetainedVectorSearchTrace,
    )> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let StrictVectorOverlayRequest {
            index,
            query,
            k,
            candidates,
            snapshot,
            search_mode,
        } = request;
        if k == 0 {
            return Ok((
                contextdb_vector::mem::SearchScores::new(&index, executing_owner.clone())?,
                RetainedVectorSearchTrace::new(
                    VectorSearchDebugTrace {
                        index,
                        used_hnsw: false,
                        hnsw_len: None,
                        hnsw_candidate_count: 0,
                        hnsw_candidate_row_ids: Vec::new(),
                        final_row_ids: Vec::new(),
                        supplemented_row_count: 0,
                        fallback_reason: Some("empty_limit"),
                        partition_routes: Vec::new(),
                        hnsw_ef_search: None,
                        selected_generations: Vec::new(),
                        budget_receipt: None,
                    },
                    executing_owner.clone(),
                )?,
            ));
        }
        let layout =
            contextdb_vector::scoped_index_layout(&self.vector_store, &index, vector_copy_owner)?;
        let staged_entries = ws
            .vector_inserts
            .iter()
            .filter(|entry| entry.index == index && entry.deleted_tx.is_none())
            .count();
        let removed_entries = ws
            .vector_deletes
            .iter()
            .filter(|(delete_index, _, _)| delete_index == &index)
            .count()
            .saturating_add(
                ws.relational_deletes
                    .iter()
                    .filter(|(table, _, _)| table == &index.table)
                    .count(),
            );
        let moved_entries = ws
            .vector_moves
            .iter()
            .filter(|(move_index, _, _, _)| move_index == &index)
            .count();
        let overlay_bytes = estimate_active_tx_vector_overlay_bytes(
            staged_entries
                .saturating_add(removed_entries)
                .saturating_add(moved_entries),
            layout.dimension,
        );
        self.finalization.accountant.try_allocate_for_label(
            overlay_bytes,
            "query",
            self.finalization
                .accountant
                .prepare_operation_label(format_args!(
                    "active_tx_vector_overlay@{}.{}",
                    index.table, index.column
                ))?,
            "Reduce active transaction vector search scope or raise MEMORY_LIMIT.",
        )?;

        let result =
            (|| -> Result<(contextdb_vector::mem::SearchScores, RetainedVectorSearchTrace)> {
                let mut removed = HashSet::new();
                for (delete_table, row_id, _) in &ws.relational_deletes {
                    let has_same_row_replacement =
                        ws.relational_inserts.iter().any(|(insert_table, row)| {
                            insert_table == &index.table && row.row_id == *row_id
                        });
                    if delete_table == &index.table && !has_same_row_replacement {
                        removed.insert(*row_id);
                    }
                }
                for (delete_index, row_id, _) in &ws.vector_deletes {
                    if delete_index == &index {
                        removed.insert(*row_id);
                    }
                }
                let moved = ws
                    .vector_moves
                    .iter()
                    .filter(|(move_index, _, _, _)| move_index == &index)
                    .map(|(_, from_row_id, to_row_id, _)| (*from_row_id, *to_row_id))
                    .collect::<HashMap<_, _>>();
                let staged_identities = ws
                    .vector_inserts
                    .iter()
                    .filter(|entry| entry.index == index && entry.deleted_tx.is_none())
                    .map(|entry| entry.row_id)
                    .collect::<HashSet<_>>();

                let mut committed_candidates = candidates.cloned();
                if let Some(committed_candidates) = committed_candidates.as_mut() {
                    for (from, to) in &moved {
                        if committed_candidates.contains(to.0) {
                            committed_candidates.insert(from.0);
                        }
                    }
                }
                let effective_candidates = self.effective_vector_read_candidates(
                    &index,
                    snapshot,
                    committed_candidates.as_ref(),
                    search_mode,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                let mut committed_k = k
                    .saturating_add(removed.len())
                    .saturating_add(moved.len())
                    .saturating_add(staged_identities.len());
                let mut previous_progress = None;
                let (mut scored, mut trace) = loop {
                    let (committed, next_trace) = self.owner_vector_search(
                        index.clone(),
                        query,
                        committed_k,
                        effective_candidates.as_ref(),
                        snapshot,
                        search_mode,
                        executing_owner.clone(),
                        read_memory.clone(),
                        vector_copy_owner,
            )?;
                    let returned = committed.len();
                    let mut eligible =
                        contextdb_vector::mem::SearchScores::new(&index, executing_owner.clone())?;
                    if search_mode == contextdb_core::VectorSearchMode::Exact {
                        eligible = eligible.for_exact();
                    }
                    eligible.reserve(returned.saturating_add(staged_entries))?;
                    for (row_id, score) in committed {
                        if removed.contains(&row_id)
                            || staged_identities.contains(&row_id)
                            || moved.contains_key(&row_id)
                        {
                            continue;
                        }
                        let row_id = moved.get(&row_id).copied().unwrap_or(row_id);
                        if let Some(candidates) = candidates
                            && !candidates.contains(row_id.0)
                        {
                            continue;
                        }
                        if !self.row_id_read_allowed_in_write_set_for_query(
                            ws,
                            &index.table,
                            row_id,
                            snapshot,
                            read_memory.clone(),
                        )? {
                            continue;
                        }
                        eligible.try_push((row_id, score))?;
                    }
                    if eligible.len() >= k || previous_progress == Some((returned, eligible.len()))
                    {
                        break (eligible, next_trace);
                    }
                    previous_progress = Some((returned, eligible.len()));
                    let refill = k.saturating_sub(eligible.len()).max(1);
                    let next_k = committed_k
                        .saturating_add(refill)
                        .max(committed_k.saturating_mul(2));
                    if next_k == committed_k {
                        break (eligible, next_trace);
                    }
                    committed_k = next_k;
                };

                let mut newest_staged = HashMap::<RowId, &VectorEntry>::new();
                for entry in &ws.vector_inserts {
                    if entry.index == index && entry.deleted_tx.is_none() {
                        newest_staged.insert(entry.row_id, entry);
                    }
                }
                for (from, to) in &moved {
                    if removed.contains(to)
                        || staged_identities.contains(to)
                        || candidates.is_some_and(|ids| !ids.contains(to.0))
                    {
                        continue;
                    }
                    if !self.vector_staged_row_in_selected_partitions(ws, &index, *to)?
                        || !self.row_id_read_allowed_in_write_set_for_query(
                            ws,
                            &index.table,
                            *to,
                            snapshot,
                            read_memory.clone(),
                        )?
                    {
                        continue;
                    }
                    if let Some(score) = self.vector_store.bounded_score_live_row(
                        &index,
                        *from,
                        snapshot,
                        query,
                        || Ok::<_, Error>(()),
                        || Ok::<_, Error>(()),
                        |bytes| {
                            self.finalization.accountant.try_allocate_for(
                                bytes,
                                "vector_search",
                                "score_moved_vector",
                                "Reduce transaction scope or raise MEMORY_LIMIT.",
                            )
                        },
                        |bytes| self.finalization.accountant.release(bytes),
                        vector_copy_owner,
                    )? {
                        scored.try_push((*to, score))?;
                    }
                }
                for (row_id, entry) in newest_staged {
                    if !self.vector_staged_row_in_selected_partitions(ws, &index, row_id)? {
                        continue;
                    }
                    if let Some(candidates) = candidates
                        && !candidates.contains(row_id.0)
                    {
                        continue;
                    }
                    if !self.row_id_read_allowed_in_write_set_for_query(
                        ws,
                        &index.table,
                        row_id,
                        snapshot,
                        read_memory.clone(),
                    )? {
                        continue;
                    }
                    scored.try_push((row_id, cosine_similarity(&entry.vector, query)))?;
                }
                scored.sort_unstable_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                scored.truncate(k);
                trace.set_final_row_ids(&scored)?;
                trace.trace.supplemented_row_count = trace
                    .supplemented_row_count
                    .saturating_add(staged_entries.min(scored.len()));
                let used_hnsw = trace.used_hnsw;
                self.last_vector_search_used_hnsw
                    .store(used_hnsw, Ordering::SeqCst);
                        Ok((scored, trace))
            })();
        self.finalization.accountant.release(overlay_bytes);
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn query_vector_strict_with_captured_overlay(
        &self,
        captured: &CapturedVectorWriteSet,
        request: StrictVectorOverlayRequest<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        executing_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    ) -> Result<(
        contextdb_vector::mem::SearchScores,
        RetainedVectorSearchTrace,
    )> {
        let executing_owner: contextdb_vector::MemoryBudgetOwner = executing_owner.into();

        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let StrictVectorOverlayRequest {
            index,
            query,
            k,
            candidates,
            snapshot,
            search_mode,
        } = request;
        if k == 0 {
            return Ok((
                contextdb_vector::mem::SearchScores::new(&index, executing_owner.clone())?,
                RetainedVectorSearchTrace::new(
                    VectorSearchDebugTrace {
                        index,
                        used_hnsw: false,
                        hnsw_len: None,
                        hnsw_candidate_count: 0,
                        hnsw_candidate_row_ids: Vec::new(),
                        final_row_ids: Vec::new(),
                        supplemented_row_count: 0,
                        fallback_reason: Some("empty_limit"),
                        partition_routes: Vec::new(),
                        hnsw_ef_search: None,
                        selected_generations: Vec::new(),
                        budget_receipt: None,
                    },
                    executing_owner.clone(),
                )?,
            ));
        }
        let staged_entries = captured.staged_entries;
        let removed = &captured.vector.removed;
        let moved = &captured.vector.moved;
        let staged_identities = &captured.vector.staged_identities;
        (|| -> Result<(contextdb_vector::mem::SearchScores, RetainedVectorSearchTrace)> {
            let mut committed_candidates = candidates.cloned();
            if let Some(committed_candidates) = committed_candidates.as_mut() {
                for (from, to) in moved {
                    if committed_candidates.contains(to.0) {
                        committed_candidates.insert(from.0);
                    }
                }
            }
            let effective_candidates = self.effective_vector_read_candidates(
                &index,
                snapshot,
                committed_candidates.as_ref(),
                search_mode,
                vector_copy_owner,
                read_memory.clone(),
            )?;
            let mut committed_k = k
                .saturating_add(removed.len())
                .saturating_add(moved.len())
                .saturating_add(staged_identities.len());
            let mut previous_progress = None;
            let (mut scored, mut trace) = loop {
                let (committed, next_trace) = self.owner_vector_search(
                    index.clone(),
                    query,
                    committed_k,
                    effective_candidates.as_ref(),
                    snapshot,
                    search_mode,
                    executing_owner.clone(),
                        read_memory.clone(),
                    vector_copy_owner,
            )?;
                let returned = committed.len();
                let mut eligible =
                    contextdb_vector::mem::SearchScores::new(&index, executing_owner.clone())?;
                if search_mode == contextdb_core::VectorSearchMode::Exact {
                    eligible = eligible.for_exact();
                }
                eligible.reserve(returned.saturating_add(staged_entries))?;
                for (row_id, score) in committed {
                    if removed.contains(&row_id)
                        || staged_identities.contains(&row_id)
                        || moved.contains_key(&row_id)
                    {
                        continue;
                    }
                    let row_id = moved.get(&row_id).copied().unwrap_or(row_id);
                    if let Some(candidates) = candidates
                        && !candidates.contains(row_id.0)
                    {
                        continue;
                    }
                    if !self.row_id_read_allowed_in_captured_set_for_query(
                        captured,
                        &index.table,
                        row_id,
                        snapshot,
                        read_memory.clone(),
                    )? {
                        continue;
                    }
                    eligible.try_push((row_id, score))?;
                }
                if eligible.len() >= k || previous_progress == Some((returned, eligible.len())) {
                    break (eligible, next_trace);
                }
                previous_progress = Some((returned, eligible.len()));
                let refill = k.saturating_sub(eligible.len()).max(1);
                let next_k = committed_k
                    .saturating_add(refill)
                    .max(committed_k.saturating_mul(2));
                if next_k == committed_k {
                    break (eligible, next_trace);
                }
                committed_k = next_k;
            };

            for (from, to) in moved {
                if removed.contains(to)
                    || staged_identities.contains(to)
                    || candidates.is_some_and(|ids| !ids.contains(to.0))
                {
                    continue;
                }
                if !self.vector_captured_row_in_selected_partitions(captured, &index, *to)?
                    || !self.row_id_read_allowed_in_captured_set_for_query(
                        captured,
                        &index.table,
                        *to,
                        snapshot,
                        read_memory.clone(),
                    )?
                {
                    continue;
                }
                if let Some(score) = self.vector_store.bounded_score_live_row(
                    &index,
                    *from,
                    snapshot,
                    query,
                    || Ok::<_, Error>(()),
                    || Ok::<_, Error>(()),
                    |bytes| {
                        self.finalization.accountant.try_allocate_for(
                            bytes,
                            "vector_search",
                            "score_moved_vector",
                            "Reduce transaction scope or raise MEMORY_LIMIT.",
                        )
                    },
                    |bytes| self.finalization.accountant.release(bytes),
                    vector_copy_owner,
                )? {
                    scored.try_push((*to, score))?;
                }
            }
            for entry in &captured.vector.staged_vectors {
                let row_id = entry.row_id();
                if !self.vector_captured_row_in_selected_partitions(captured, &index, row_id)? {
                    continue;
                }
                if let Some(candidates) = candidates
                    && !candidates.contains(row_id.0)
                {
                    continue;
                }
                if !self.row_id_read_allowed_in_captured_set_for_query(
                    captured,
                    &index.table,
                    row_id,
                    snapshot,
                    read_memory.clone(),
                )? {
                    continue;
                }
                scored.try_push((row_id, cosine_similarity(entry.vector(), query)))?;
            }
            scored.sort_unstable_by(|left, right| {
                right
                    .1
                    .total_cmp(&left.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(k);
            trace.set_final_row_ids(&scored)?;
            trace.trace.supplemented_row_count = trace
                .supplemented_row_count
                .saturating_add(staged_entries.min(scored.len()));
            let used_hnsw = trace.used_hnsw;
            self.last_vector_search_used_hnsw
                .store(used_hnsw, Ordering::SeqCst);
                Ok((scored, trace))
        })()
    }

    pub(crate) fn register_rank_formula(
        &self,
        table: &str,
        column: &str,
        formula: Arc<RankFormula>,
    ) {
        let mut cache = self.rank_formula_cache.write();
        cache.insert((table.to_string(), column.to_string()), formula);
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    pub(crate) fn remove_rank_formula(&self, table: &str, column: &str) {
        let mut cache = self.rank_formula_cache.write();
        cache.remove(&(table.to_string(), column.to_string()));
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    pub(crate) fn remove_rank_formulas_for_table(&self, table: &str) {
        let mut cache = self.rank_formula_cache.write();
        cache.retain(|(policy_table, _), _| policy_table != table);
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rebuild_rank_formula_cache_from_meta(
        &self,
        metas: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        let cache = Self::prepare_rank_formula_cache(metas)?;
        self.publish_prepared_rank_formula_cache(cache);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_rank_formula_cache(
        metas: &HashMap<String, TableMeta>,
    ) -> Result<HashMap<(String, String), Arc<RankFormula>>> {
        let mut cache = HashMap::new();
        for (table, meta) in metas {
            for column in &meta.columns {
                if let Some(policy) = &column.rank_policy {
                    let formula = RankFormula::compile_for_index(
                        &rank_index_name(table, &column.name),
                        &policy.formula,
                    )?;
                    cache.insert((table.clone(), column.name.clone()), Arc::new(formula));
                }
            }
        }
        Ok(cache)
    }

    pub(super) fn publish_prepared_rank_formula_cache(
        &self,
        cache: HashMap<(String, String), Arc<RankFormula>>,
    ) {
        let count = cache.len() as u64;
        *self.rank_formula_cache.write() = cache;
        self.rank_policy_formula_parse_count
            .store(count, Ordering::SeqCst);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rank_formula(&self, table: &str, column: &str) -> Result<Arc<RankFormula>> {
        self.rank_formula_cache
            .read()
            .get(&(table.to_string(), column.to_string()))
            .cloned()
            .ok_or_else(|| {
                Error::Other(
                    (format!(
                        "rank policy formula cache missing for {}",
                        rank_index_name(table, column)
                    ))
                    .into(),
                )
            })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            self.vector_store
                .visible_entry_count(index, snapshot, vector_copy_owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_entry_count_in_tx(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<usize> {
        let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let base = self
                .vector_store
                .visible_entry_count(index, snapshot, vector_copy_owner)?;
            let Some(tx) = tx else {
                return Ok(base);
            };
            self.tx_mgr.with_read(tx, |ws| {
                base.saturating_add(
                    ws.vector_inserts
                        .iter()
                        .filter(|entry| entry.index == *index && entry.deleted_tx.is_none())
                        .count(),
                )
            })
        })
    }

    /// EXACT ranks every allowed vector; indexed ranking uses the column's
    /// resolved candidate breadth. The caller's LIMIT is resolved only once.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rank_policy_candidate_k(
        &self,
        index: &VectorIndexRef,
        entry_count: usize,
        limit: usize,
        exact_route: bool,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<usize> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if entry_count == 0 || limit == 0 {
            return Ok(limit);
        }
        if exact_route {
            return Ok(entry_count);
        }
        let bytes = candidates
            .map_or(0, |ids| ids.len() as usize)
            .saturating_mul(std::mem::size_of::<u64>());
        let _credit = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            bytes,
            "vector_search",
            "rank_candidate_ids",
            "Narrow the authorized scope or raise MEMORY_LIMIT.",
        )?;
        let ids = candidates.map(|ids| ids.iter().collect::<Vec<_>>());
        let groups = self.vector.bounded_candidate_groups(
            index,
            ids.as_deref(),
            snapshot,
            || Ok::<_, Error>(()),
            || Ok::<_, Error>(()),
            |bytes| {
                read_memory.try_reserve(
                    bytes,
                    "vector_search",
                    "rank_candidate_sources",
                    "Narrow the authorized scope or raise MEMORY_LIMIT.",
                )
            },
            |bytes| read_memory.release(bytes),
            vector_copy_owner,
        )?;
        let candidate_k = self.vector.bounded_rank_candidate_k_grouped(
            index,
            limit,
            &groups,
            || Ok::<_, Error>(()),
            vector_copy_owner,
        );
        read_memory.release(groups.into_retained_bytes());
        candidate_k
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn semantic_where_candidate_bitmap(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        where_clause: &str,
        snapshot: SnapshotId,
        require_bounded_route: bool,
        preflight_auto_fallback: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(RoaringTreemap, bool)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let table = index.table.as_str();
        let sql = if where_clause.is_empty() {
            format!("SELECT row_id AS __vector_candidate_identity FROM {table}")
        } else {
            format!(
                "SELECT row_id AS __vector_candidate_identity FROM {table} WHERE {where_clause}"
            )
        };
        let stmt = contextdb_parser::parse(&sql)?;
        let Statement::Select(select) = &stmt else {
            return Err(Error::ParseError("semantic WHERE parse failed".into()));
        };
        let params = HashMap::new();
        let plan = contextdb_planner::plan(&stmt)?;
        if let Some(expr) = select.body.where_clause.as_ref() {
            crate::executor::bind_vector_search_parameters(expr, Some(&plan), &params)?;
        }
        let route = crate::executor::vector_candidate_route(self, index, Some(&plan), &params)?;
        let access_filter = route.access_filter;
        let access_plan = route.candidates;
        let scope = route.scope;
        let directory_route = route.directory_route;
        let complete = route.complete;
        if require_bounded_route && !complete {
            return Err(Error::VectorFilteredRouteUnavailable {
                index: index.clone(),
                predicate_columns: crate::executor::vector_candidate_refusal_columns(
                    self,
                    index,
                    access_plan.as_ref(),
                    access_filter,
                ),
            });
        }
        if preflight_auto_fallback && !complete {
            let exact_score_bytes = self
                .vector_entry_count(index, snapshot, vector_copy_owner)?
                .saturating_mul(std::mem::size_of::<(RowId, f32)>());
            if crate::memory_accounting::ScopedMemoryReservation::try_new_for(
                self.accountant(),
                exact_score_bytes,
                "vector_search",
                "preflight_filtered_exact_scores",
                "Add a relational index for the filter, reduce vector volume, or raise MEMORY_LIMIT.",
            )
            .is_err()
            {
                return Err(Error::VectorFilteredRouteUnavailable {
                    index: index.clone(),
            predicate_columns: crate::executor::vector_candidate_refusal_columns(self, index, access_plan.as_ref(), access_filter),
                });
            }
        }
        let mut bitmap = RoaringTreemap::new();
        if directory_route {
            bitmap = self.vector_directory_candidate_bitmap(
                tx,
                index,
                snapshot,
                select.body.where_clause.as_ref(),
                &params,
                scope.as_ref(),
                vector_copy_owner,
                read_memory.clone(),
            )?;
        } else {
            let result = self.run_read_plan(
                access_plan.as_ref().unwrap_or(&plan),
                &params,
                tx,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
                None,
            )?;
            let row_id_column = result
                .columns
                .iter()
                .position(|column| column == "__vector_candidate_identity")
                .ok_or_else(|| {
                    Error::PlanError("semantic candidate source omitted row identity".into())
                })?;
            for row in result.result.rows {
                if let Some(Value::Int64(id)) = row.get(row_id_column) {
                    bitmap.insert(*id as u64);
                }
            }
        }
        Ok((bitmap, complete))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_directory_candidate_bitmap(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        filter: Option<&contextdb_parser::ast::Expr>,
        params: &HashMap<String, Value>,
        scope: Option<&crate::executor::VectorPartitionScope>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<RoaringTreemap> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let table = index.table.as_str();
            let mut bitmap = RoaringTreemap::new();
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
            let mut admit = |id| -> Result<()> {
                let row =
                    match self.find_row_by_id_in_tx(tx, table, id, snapshot, read_memory.clone()) {
                        Ok(row) => row,
                        Err(Error::NotFound(_)) => return Ok(()),
                        Err(error) => return Err(error),
                    };
                if let Some(expr) = filter
                    && !crate::executor::row_matches(&row, expr, params)?
                {
                    return Ok(());
                }
                if self.bounded_read_allowed_for_row(
                    tx,
                    table,
                    &meta,
                    &row,
                    snapshot,
                    &mut |_| Ok::<(), Error>(()),
                    read_memory.clone(),
                )? {
                    bitmap.insert(id.0);
                }
                Ok(())
            };
            let mut collect = || {
                self.vector.bounded_visit_visible_ids(
                    index,
                    snapshot,
                    || Ok(()),
                    |bytes| {
                        self.finalization.accountant.try_allocate_for(
                            bytes,
                            "vector_search",
                            "authorized_directory",
                            "Narrow the authorized scope or raise MEMORY_LIMIT.",
                        )
                    },
                    |bytes| self.finalization.accountant.release(bytes),
                    &mut admit,
                    vector_copy_owner,
                )
            };
            if let Some(prefixes) =
                scope.and_then(crate::executor::VectorPartitionScope::resolved_prefixes)
            {
                contextdb_vector::mem::with_selected_partition_prefixes(
                    index,
                    prefixes.to_vec(),
                    collect,
                )?;
            } else {
                collect()?;
            }
            if let Some(tx) = tx {
                let staged = self.tx_mgr.with_read(tx, |ws| {
                    ws.relational_inserts
                        .iter()
                        .filter(|(row_table, _)| row_table == table)
                        .map(|(_, row)| row.row_id)
                        .collect::<Vec<_>>()
                })?;
                for id in staged {
                    admit(id)?;
                }
            }
            Ok(bitmap)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn effective_vector_read_candidates(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        candidates: Option<&RoaringTreemap>,
        mode: contextdb_core::VectorSearchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RoaringTreemap>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            if candidates.is_none() && self.bounded_read_requires_candidate_filter(&index.table)? {
                return self
                    .semantic_where_candidate_bitmap(
                        None,
                        index,
                        "",
                        snapshot,
                        mode == contextdb_core::VectorSearchMode::Indexed,
                        mode == contextdb_core::VectorSearchMode::Auto,
                        vector_copy_owner,
                        read_memory.clone(),
                    )
                    .map(|(ids, _)| Some(ids));
            }
            self.effective_read_candidates(&index.table, snapshot, candidates, read_memory.clone())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn find_row_by_id_at(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<VersionedRow> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.committed_row_at(table, row_id, snapshot, read_memory.clone())?
            .ok_or_else(|| Error::NotFound(format!("row {row_id} in table {table}")))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn joined_row_for_rank_policy(
        &self,
        tx: Option<TxId>,
        policy: &RankPolicy,
        anchor: &VersionedRow,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if policy.anchor_column.is_empty() {
            return Err(Error::Other(
                (format!(
                    "rank policy on index {}.{} has no resolved anchor join column",
                    policy.joined_table, policy.joined_column
                ))
                .into(),
            ));
        }
        let join_value = anchor
            .values
            .get(&policy.anchor_column)
            .cloned()
            .unwrap_or(Value::Null);
        if join_value == Value::Null {
            return Ok(None);
        }

        if self.writer_read_root()?.is_some() {
            return match tx {
                Some(tx) => self.joined_row_for_rank_policy_in_tx(
                    tx,
                    policy,
                    &join_value,
                    snapshot,
                    vector_copy_owner,
                    read_memory.clone(),
                ),
                None => self.writer_rank_join(policy, &join_value, snapshot, read_memory.clone()),
            };
        }

        let indexes = self.relational_store.indexes.read();
        let storage = indexes
            .get(&policy.joined_table)
            .and_then(|table_indexes| table_indexes.get(&policy.protected_index))
            .ok_or_else(|| {
                Error::Other(
                    (format!(
                        "rank policy protected index `{}` missing on table `{}`",
                        policy.protected_index, policy.joined_table
                    ))
                    .into(),
                )
            })?;
        let Some((first_column, direction)) = storage.columns.first() else {
            return Err(Error::Other(
                (format!(
                    "rank policy protected index `{}` on `{}` has no columns",
                    policy.protected_index, policy.joined_table
                ))
                .into(),
            ));
        };
        if first_column != &policy.joined_column {
            return Err(Error::Other(
                (format!(
                    "rank policy protected index `{}` on `{}` no longer leads with `{}`",
                    policy.protected_index, policy.joined_table, policy.joined_column
                ))
                .into(),
            ));
        }
        if let Some(tx) = tx {
            drop(indexes);
            return self.joined_row_for_rank_policy_in_tx(
                tx,
                policy,
                &join_value,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            );
        }

        let key_component = match direction {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(join_value.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(join_value.clone())),
        };
        let mut best_row_id: Option<RowId> = None;
        let mut consider = |entries: &[contextdb_relational::IndexEntry]| {
            for entry in entries {
                if entry.visible_at(snapshot)
                    && best_row_id.is_none_or(|current| current < entry.row_id)
                {
                    best_row_id = Some(entry.row_id);
                }
            }
        };

        if storage.columns.len() == 1 {
            if let Some(entries) = storage.exact_postings(&vec![key_component.clone()]) {
                consider(entries);
            }
        } else if !storage.exact_only() {
            for (key, entries) in storage.tree.range(vec![key_component.clone()]..) {
                if key.first() != Some(&key_component) {
                    break;
                }
                consider(entries);
            }
        }
        drop(indexes);

        let Some(row_id) = best_row_id else {
            return Ok(None);
        };
        let Some(row) =
            self.committed_row_at(&policy.joined_table, row_id, snapshot, read_memory.clone())?
        else {
            return Ok(None);
        };
        let Some(value) = row.values.get(&policy.joined_column) else {
            return Ok(None);
        };
        if !values_equal_for_rank_join(value, &join_value) {
            return Ok(None);
        }
        if !self.row_read_allowed_for_change(
            &policy.joined_table,
            &row,
            snapshot,
            read_memory.clone(),
        )? {
            return Ok(None);
        }
        Ok(Some(row))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn joined_row_for_rank_policy_in_tx(
        &self,
        tx: TxId,
        policy: &RankPolicy,
        join_value: &Value,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let rows = self.scan_in_tx_raw(
            tx,
            &policy.joined_table,
            snapshot,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let mut best: Option<VersionedRow> = None;
        for row in rows {
            let Some(value) = row.values.get(&policy.joined_column) else {
                continue;
            };
            if !values_equal_for_rank_join(value, join_value) {
                continue;
            }
            if !self.row_read_allowed_for_change(
                &policy.joined_table,
                &row,
                snapshot,
                read_memory.clone(),
            )? {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|current| current.row_id < row.row_id)
            {
                best = Some(row);
            }
        }
        Ok(best)
    }

    pub(super) fn resolve_rank_formula_column(
        &self,
        policy: &RankPolicy,
        anchor: &VersionedRow,
        joined: Option<&VersionedRow>,
        column: &str,
    ) -> std::result::Result<Option<f32>, FormulaEvalError> {
        if let Some(value) = anchor.values.get(column) {
            return rank_value_to_number(value, column);
        }
        let Some(joined) = joined else {
            return Ok(None);
        };
        if self.corrupt_joined_values.read().contains(&(
            policy.joined_table.clone(),
            joined.row_id,
            column.to_string(),
        )) {
            return Err(FormulaEvalError::CorruptJoinedColumn {
                column: column.to_string(),
            });
        }
        let value = joined.values.get(column).unwrap_or(&Value::Null);
        rank_value_to_number(value, column)
    }

    pub(super) fn warn_rank_eval_error(
        &self,
        table: &str,
        column: &str,
        row_id: RowId,
        err: &FormulaEvalError,
    ) {
        let mut reason = err.reason();
        if reason.len() > 256 {
            reason.truncate(253);
            reason.push_str("...");
        }
        tracing::warn!(
            name: "rank_policy_eval_error",
            target: "rank_policy_eval_error",
            index = %rank_index_name(table, column),
            row_id = row_id.0,
            reason = %reason,
            "rank_policy_eval_error"
        );
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn search_result_values_in_tx(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        mut values: HashMap<String, Value>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<String, Value>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if matches!(values.get(&index.column), Some(Value::Null) | None) {
            let row =
                self.find_row_by_id_in_tx(tx, &index.table, row_id, snapshot, read_memory.clone())?;
            if let Some(vector) = self.load_row_vector_for_column(
                tx,
                &index.table,
                &index.column,
                &row,
                snapshot,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                values.insert(index.column.clone(), Value::Vector(vector));
            }
        }
        Ok(values)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn pending_vector_dimension(
        &self,
        tx: TxId,
        index: &VectorIndexRef,
    ) -> Result<Option<usize>> {
        self.tx_mgr.with_read(tx, |ws| {
            ws.vector_inserts
                .iter()
                .rev()
                .find(|entry| entry.index == *index && entry.deleted_tx.is_none())
                .map(|entry| entry.vector.len())
        })
    }

    pub(super) fn direct_vector_dimension_error(
        &self,
        index: &VectorIndexRef,
        expected: usize,
        actual: usize,
    ) -> Error {
        Error::VectorIndexDimensionMismatch {
            index: index.clone(),
            expected,
            actual,
        }
    }

    /// What admitting one vector body into the vector store costs, at
    /// admission and at the rollback that returns it alike. This is the raw
    /// body only: an F32 column's relational projection lives inside the row
    /// values and is carried by the row's own charge, never by the vector.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_insert_accounted_bytes(
        &self,
        index: &VectorIndexRef,
        dimension: usize,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<usize> {
        self.vector_store
            .with_index_layout(index, |layout| layout.quantization.storage_bytes(dimension))
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_last_query_vector_used_hnsw_for_test(&self) -> bool {
        let _operation = self.assert_open_operation();
        self.last_vector_search_used_hnsw.load(Ordering::SeqCst)
    }

    /// Borrow the current trace's receipt and original payer without copying its payload.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub(crate) fn retained_vector_trace_invoice_for_test(
        &self,
        visit: impl FnOnce(&contextdb_core::read_memory::ReadMemoryOwner),
    ) -> Option<contextdb_vector::mem::TraceInvoiceForTest> {
        self.last_vector_search_trace
            .read()
            .as_ref()
            .map(|trace| trace.complete_invoice_for_test(visit))
    }

    /// Observe the existing retained trace credit without copying its payload.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn last_query_vector_trace_credit_bytes_for_test(&self) -> usize {
        self.last_vector_search_trace
            .read()
            .as_ref()
            .map_or(0, |trace| trace.retained_credit_bytes_for_test())
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_last_query_vector_trace_for_test(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorSearchTraceObservation>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        self.last_vector_search_trace
            .read()
            .as_ref()
            .map(|trace| VectorSearchTraceObservation::copy_from(trace, read_memory))
            .transpose()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_last_query_vector_trace_for_test(&self) {
        let _operation = self.assert_open_operation();
        *self.last_vector_search_trace.write() = None;
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __take_last_query_vector_trace_for_test(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorSearchTraceObservation>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let mut slot = self.last_vector_search_trace.write();
        let copied = slot
            .as_ref()
            .map(|trace| VectorSearchTraceObservation::copy_from(trace, read_memory))
            .transpose()?;
        slot.take();
        Ok(copied)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_vector_full_index_entries_touched_for_test(&self) {
        let _operation = self.assert_open_operation();
        self.vector_store
            .reset_full_index_entries_touched_for_test();
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __vector_full_index_entries_touched_for_test(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.vector_store.full_index_entries_touched_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn captured_hnsw_observation_for_test(
        &self,
        index: &VectorIndexRef,
        topology: bool,
    ) -> Result<Option<(HnswGraphStats, usize, u64, u64)>> {
        let reader = self.declared_reader_memory()?;
        let _source = self.writer_read_scope(reader.clone())?;
        let mut owner = crate::vector_read_memory::admission(reader);
        match self.vector_store.try_state(index) {
            Some(state) => {
                state.captured_hnsw_observation_for_test(self.snapshot(), &mut owner, topology)
            }
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_len(&self, index: VectorIndexRef) -> Option<usize> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        match self.captured_hnsw_observation_for_test(&index, false) {
            Ok(Some(observation)) => Some(observation.1),
            Ok(None) => self.vector_hnsw_len_under_schema_read(&index),
            Err(_) => None,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn vector_hnsw_len_under_schema_read(
        &self,
        index: &VectorIndexRef,
    ) -> Option<usize> {
        self.vector_store
            .try_state(index)
            .and_then(|state| state.hnsw_len())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn assert_vector_index_exists_under_schema_read(
        &self,
        index: &VectorIndexRef,
    ) -> Result<()> {
        if self.vector_store.contains_index(index) {
            Ok(())
        } else {
            Err(Error::UnknownVectorIndex {
                index: index.clone(),
            })
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_vector_under_schema_read(
        &self,
        index: &VectorIndexRef,
        actual: usize,
    ) -> Result<()> {
        self.vector_store.validate_vector(index, actual)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_stats(&self, index: VectorIndexRef) -> Option<HnswGraphStats> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        match self.captured_hnsw_observation_for_test(&index, false) {
            Ok(Some(observation)) => Some(observation.0),
            Ok(None) => self
                .vector_store
                .try_state(&index)
                .and_then(|state| state.hnsw_stats()),
            Err(_) => None,
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_raw_search_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Vec<(RowId, f32)>> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store
            .raw_hnsw_search(&index, query, k)
            .and_then(Result::ok)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_raw_entry_count_for_row_for_test(
        &self,
        index: VectorIndexRef,
        row_id: RowId,
    ) -> Option<usize> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store
            .raw_hnsw_entry_count_for_row(&index, row_id)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_topology_digest_for_test(
        &self,
        index: VectorIndexRef,
    ) -> Option<u64> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        match self.captured_hnsw_observation_for_test(&index, true) {
            Ok(Some(observation)) => Some(observation.2),
            Ok(None) => self.vector_store.raw_hnsw_topology_digest_for_test(&index),
            Err(_) => None,
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_hnsw_build_serial_for_test(&self, index: VectorIndexRef) -> Option<u64> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        match self.captured_hnsw_observation_for_test(&index, false) {
            Ok(Some(observation)) => Some(observation.3),
            Ok(None) => self.vector_store.raw_hnsw_build_serial_for_test(&index),
            Err(_) => None,
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_vector_storage_bytes_per_entry(
        &self,
        index: VectorIndexRef,
    ) -> Result<Vec<usize>> {
        let _operation = self.open_operation()?;
        let _vector_schema = self.vector_schema_read(&index);
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory);
        self.vector_store
            .storage_bytes_per_entry(&index, &mut vector_copy_owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn has_live_vector(&self, row_id: RowId, snapshot: SnapshotId) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        self.has_live_vector_with_read_memory(row_id, snapshot, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn has_live_vector_with_read_memory(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let _vector_capture =
            self.vector_read_scope(snapshot, read_memory.clone(), &mut vector_copy_owner)?;
        self.with_vector_read_admission(&mut vector_copy_owner, |vector_copy_owner| {
            let _vector_schema =
                self.vector_schema_read_many(self.vector_store_schema_refs(vector_copy_owner)?);
            for info in self.vector_store.index_infos(vector_copy_owner)? {
                if self.vector_store.has_live_entry_for_row(
                    &info.index,
                    row_id,
                    snapshot,
                    vector_copy_owner,
                )? && self.row_id_read_allowed_for_change(
                    &info.index.table,
                    row_id,
                    snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// Materialize one live vector under the executing reader's owner.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_vector_entry(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<VectorEntry>> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.live_vector_entry_with_read_memory(
            row_id,
            snapshot,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn live_vector_entry_with_read_memory(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        let _vector_capture =
            self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?;
        self.with_vector_read_admission(vector_copy_owner, |vector_copy_owner| {
            let _vector_schema =
                self.vector_schema_read_many(self.vector_store_schema_refs(vector_copy_owner)?);
            for entry in
                self.vector_store
                    .load_live_entries_for_row(row_id, snapshot, vector_copy_owner)?
            {
                if self.row_id_read_allowed_for_change(
                    &entry.index.table,
                    row_id,
                    snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(Some(entry));
                }
            }
            let Some(persistence) = self.persistence.as_ref() else {
                return Ok(None);
            };
            let source = crate::persistence::vector_membership_source::MembershipOwner::Writable(
                persistence.clone(),
            );
            let Some(selected) =
                crate::persistence::vector_membership_source::select_live_fallback(
                    source.clone(),
                    row_id,
                    snapshot,
                    vector_copy_owner,
                    &mut |candidate| {
                        self.row_id_read_allowed_for_change(
                            &candidate.index.table,
                            row_id,
                            snapshot,
                            read_memory.clone(),
                        )
                    },
                )?
            else {
                return Ok(None);
            };
            let membership = selected.record();
            let (dimension, quantization) = self
                .vector_store
                .with_index_layout(&membership.index, |layout| {
                    (layout.dimension, layout.quantization)
                })?;
            vector_copy_owner.reserve(
                dimension
                    .checked_mul(std::mem::size_of::<f32>())
                    .and_then(|n| n.checked_add(std::mem::size_of::<VectorEntry>()))
                    .and_then(|n| n.checked_add(membership.index.table.len()))
                    .and_then(|n| n.checked_add(membership.index.column.len()))
                    .and_then(|n| n.checked_mul(4))
                    .ok_or_else(|| Error::Other("live vector body capacity overflow".into()))?,
            )?;
            let partition =
                VectorPartitionRef::new(membership.index.clone(), membership.partition_key.clone());
            selected
                .read_body(&source, &partition, dimension, quantization)
                .map(Some)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_store_has_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        self.vector_store
            .has_live_entry_for_row(index, row_id, snapshot, vector_copy_owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_store_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        // Mutation bookkeeping uses the existing unbounded live-entry sentinel.
        // It inherits an executing capture but cannot create historical intent.
        let _vector_capture = if snapshot.0 == u64::MAX {
            None
        } else {
            self.vector_read_scope(snapshot, read_memory, vector_copy_owner)?
        };
        self.with_vector_read_admission(vector_copy_owner, |owner| {
            self.vector_store
                .load_live_entry_for_row(index, row_id, snapshot, owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn natural_key_column_for_table(&self, table: &str) -> Result<String> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        natural_key_column_for_meta(&meta).ok_or_else(|| {
            Error::PlanError(format!(
                "ROW_VECTOR source table `{table}` has no natural key"
            ))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn row_id_for_natural_key_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowId>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_read(tx, |ws| {
                ws.relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| {
                        insert_table == table && row.values.get(key_col) == Some(key_value)
                    })
                    .map(|(_, row)| row.row_id)
            })?;
            if staged.is_some() {
                return Ok(staged);
            }
        }

        let Some(row_id) =
            self.row_id_for_natural_key(table, key_col, key_value, snapshot, read_memory.clone())?
        else {
            return Ok(None);
        };

        if let Some(tx) = tx {
            let deleted = self.tx_mgr.with_read(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
            })?;
            if deleted {
                return Ok(None);
            }
        }

        Ok(Some(row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_entry_for_row_in_tx(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VectorEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(tx) = tx {
            enum VectorEntryOverlay {
                Found(VectorEntry),
                Deleted,
                Moved { from: RowId, to: RowId },
                Unchanged,
            }

            let overlay = self.tx_mgr.with_ordinary_read(tx, |ws| {
                if let Some(entry) = ws.vector_inserts.iter().rev().find(|entry| {
                    entry.index == *index && entry.row_id == row_id && entry.deleted_tx.is_none()
                }) {
                    vector_copy_owner.reserve(
                        entry
                            .vector
                            .capacity()
                            .saturating_mul(std::mem::size_of::<f32>())
                            .saturating_add(std::mem::size_of::<VectorEntry>()),
                    )?;
                    return Ok(VectorEntryOverlay::Found(entry.clone()));
                }
                if ws
                    .vector_deletes
                    .iter()
                    .rev()
                    .any(|(deleted_index, deleted_row_id, _)| {
                        deleted_index == index && *deleted_row_id == row_id
                    })
                {
                    return Ok(VectorEntryOverlay::Deleted);
                }
                if let Some((_, from_row_id, to_row_id, _)) =
                    ws.vector_moves
                        .iter()
                        .rev()
                        .find(|(moved_index, from_row_id, to_row_id, _)| {
                            moved_index == index && (*from_row_id == row_id || *to_row_id == row_id)
                        })
                {
                    return Ok(VectorEntryOverlay::Moved {
                        from: *from_row_id,
                        to: *to_row_id,
                    });
                }
                Ok::<_, Error>(VectorEntryOverlay::Unchanged)
            })??;
            match overlay {
                VectorEntryOverlay::Found(entry) => return Ok(Some(entry)),
                VectorEntryOverlay::Deleted => return Ok(None),
                VectorEntryOverlay::Moved { from, to } => {
                    if from == row_id && from != to {
                        return Ok(None);
                    }
                    let _source_capture = self.writer_read_scope(read_memory.clone())?;
                    let _vector_capture =
                        self.vector_read_scope(snapshot, read_memory.clone(), vector_copy_owner)?;
                    let mut entry =
                        self.with_vector_read_admission(vector_copy_owner, |owner| {
                            self.vector_store
                                .load_live_entry_for_row(index, from, snapshot, owner)
                        })?;
                    if let Some(entry) = entry.as_mut() {
                        entry.row_id = to;
                    }
                    return Ok(entry);
                }
                VectorEntryOverlay::Unchanged => {}
            }
        }

        let _source_capture = self.writer_read_scope(read_memory.clone())?;
        let _vector_capture = self.vector_read_scope(snapshot, read_memory, vector_copy_owner)?;
        self.with_vector_read_admission(vector_copy_owner, |owner| {
            self.vector_store
                .load_live_entry_for_row(index, row_id, snapshot, owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn find_row_by_id_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<VersionedRow> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_ordinary_read(tx, |ws| {
                let staged_insert = ws
                    .relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
                    .map(|(_, row)| row.clone());
                if staged_insert.is_some() {
                    return staged_insert;
                }
                if ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
                {
                    return None;
                }
                None
            })?;
            if let Some(row) = staged {
                return Ok(row);
            }
        }
        self.find_row_by_id_at(table, row_id, snapshot, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_captured_row_in_selected_partitions(
        &self,
        captured: &CapturedVectorWriteSet,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Result<bool> {
        let Some(row) = captured.rows.as_ref().and_then(|rows| rows.row(row_id)) else {
            return Ok(true);
        };
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let declaration = meta
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let key =
            crate::composite_store::vector_partition_key_for_row(index, &meta, declaration, row)?;
        Ok(contextdb_vector::mem::partition_key_is_selected(
            index, &key,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_read_allowed_in_captured_set_for_query(
        &self,
        captured: &CapturedVectorWriteSet,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(row) = captured.rows.as_ref().and_then(|rows| rows.row(row_id)) {
            return self.borrowed_row_read_allowed_for_query(
                table,
                row,
                snapshot,
                read_memory.clone(),
            );
        }
        if captured.deleted_rows.contains(&row_id) {
            return Ok(false);
        }
        let Some(row) = self.committed_row_at(table, row_id, snapshot, read_memory.clone())? else {
            return Ok(false);
        };
        self.borrowed_row_read_allowed_for_query(table, &row, snapshot, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_staged_row_in_selected_partitions(
        &self,
        ws: &WriteSet,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Result<bool> {
        let Some(row) = ws.relational_inserts.iter().rev().find_map(|(table, row)| {
            (table == &index.table && row.row_id == row_id).then_some(row)
        }) else {
            return Ok(true);
        };
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let declaration = meta
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let key =
            crate::composite_store::vector_partition_key_for_row(index, &meta, declaration, row)?;
        Ok(contextdb_vector::mem::partition_key_is_selected(
            index, &key,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_id_read_allowed_in_write_set_for_query(
        &self,
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(row) = ws
            .relational_inserts
            .iter()
            .rev()
            .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
            .map(|(_, row)| row.clone())
        {
            return Ok(!self
                .filter_rows_for_read(table, vec![row], snapshot, read_memory.clone())?
                .is_empty());
        }
        if ws
            .relational_deletes
            .iter()
            .any(|(delete_table, deleted_row_id, _)| {
                delete_table == table && *deleted_row_id == row_id
            })
        {
            return Ok(false);
        }
        let row = self.committed_row_at(table, row_id, snapshot, read_memory.clone())?;
        let Some(row) = row else {
            return Ok(false);
        };
        Ok(!self
            .filter_rows_for_read(table, vec![row], snapshot, read_memory.clone())?
            .is_empty())
    }

    /// Put the graph adjacency in memory where the table drop leaves it.
    ///
    /// `edges` is what survives the drop, computed while the dropped table's
    /// own rows were still readable. The durable removal has already run by
    /// the time this is called, so recomputing the survivors here would scan a
    /// table that no longer has rows to identify its edges by, and every edge
    /// the dropped table owned would be kept.
    pub(crate) fn apply_graph_edges_after_table_drop(
        &self,
        edges: Vec<contextdb_graph::GraphAdjEntry>,
    ) {
        if self.graph_store.is_source_backed() {
            return;
        }
        self.graph_store.publish_prepared_received_schema(
            GraphStore::prepare_received_schema_publication(edges),
        );
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn graph_edges_after_table_drop(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<contextdb_graph::GraphAdjEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.graph_store.is_source_backed() {
            return Ok(Vec::new());
        }
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let mut result = Vec::new();
        for edge in self.graph_store.forward_adj.read().values().flatten() {
            if edge
                .row_owner
                .as_ref()
                .is_some_and(|owner| owner.table == table)
            {
                continue;
            }
            memory.try_grow_for(
                edge.estimated_bytes(),
                "graph",
                "table_drop_projection",
                "Raise MEMORY_LIMIT to prepare this table drop.",
            )?;
            result.push(edge.clone());
        }
        self.retain_writer_output(memory);
        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn metadata_table_page(
        &self,
        owner: contextdb_core::read_memory::ReadMemoryOwner,
        continuation: Option<&str>,
        page_bytes: u64,
    ) -> std::result::Result<
        crate::local_transport::query_funding::Funded<contextdb_core::read_contract::MetadataPage>,
        crate::metadata_page::MetadataPagingError,
    > {
        let _operation = self.assert_open_operation();
        if let Some(root) = self.scoped_writer_root() {
            return crate::metadata_page::borrowed_table_page(
                owner,
                root.captured_table_metadata().map(|(name, _)| name),
                continuation,
                page_bytes,
            );
        }
        self.relational_store.with_table_name_view(|names| {
            crate::metadata_page::borrowed_table_page(owner, names, continuation, page_bytes)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn metadata_schema(
        &self,
        table: &str,
        owner: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<
        Option<
            crate::local_transport::query_funding::Funded<crate::direct_file_reader::DirectSchema>,
        >,
    > {
        if let Some(captured) = self.captured_graph_decisions() {
            return captured
                .catalog
                .get(table)
                .map(|meta| crate::read_image::project_schema_funded(table, meta, owner))
                .transpose();
        }
        let _operation = self.assert_open_operation();
        let answer = if let Some(root) = self.scoped_writer_root() {
            root.borrow_captured_table_meta(table)
                .map(|meta| crate::read_image::project_schema_funded(table, meta, owner))
                .transpose()
        } else {
            self.relational_store
                .table_meta
                .read()
                .get(table)
                .map(|meta| crate::read_image::project_schema_funded(table, meta, owner))
                .transpose()
        }?;
        if answer.is_none()
            && let Some(access) = &self.received_staging_access
        {
            access.record(table);
        }
        Ok(answer)
    }

    pub(crate) fn with_metadata_catalog<R>(
        &self,
        consume: impl FnOnce(&mut dyn ExactSizeIterator<Item = (&str, &TableMeta)>) -> R,
    ) -> R {
        let _operation = self.assert_open_operation();
        if let Some(root) = self.scoped_writer_root() {
            return consume(&mut root.captured_table_metadata());
        }
        let catalog = self.relational_store.table_meta.read();
        consume(&mut catalog.iter().map(|(name, meta)| (name.as_str(), meta)))
    }

    pub fn table_names(&self) -> Vec<String> {
        let _operation = self.assert_open_operation();
        if let Some(root) = self.scoped_writer_root() {
            return root.captured_table_names();
        }
        self.relational_store.table_names()
    }

    pub(crate) fn missing_table_error(&self, table: &str) -> Error {
        if let Some(access) = &self.received_staging_access {
            access.record(table);
        }
        Error::TableNotFound(table.to_string())
    }

    pub fn table_meta(&self, table: &str) -> Option<TableMeta> {
        if let Some(captured) = self.captured_graph_decisions() {
            return captured.catalog.get(table).cloned();
        }
        let _operation = self.assert_open_operation();
        let meta = if let Some(root) = self.scoped_writer_root() {
            root.captured_table_meta(table)
        } else {
            self.relational_store.table_meta(table)
        };
        if meta.is_none()
            && let Some(access) = &self.received_staging_access
        {
            access.record(table);
        }
        meta
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn observe_received_schema_applies_for_test(
        &self,
        observer: &ReceivedSchemaApplyObserverForTest,
    ) -> ReceivedSchemaApplyObservationScopeForTest {
        let _operation = self.assert_open_operation();
        let previous = self
            .received_schema_apply_observer
            .lock()
            .replace(observer.inner.clone());
        #[cfg(any(test, feature = "test-seams"))]
        let durable = self.persistence.as_ref().map(|persistence| {
            let sink = observer.inner.clone();
            observer.inner.lock().observation.durable_observer_attached = true;
            persistence
                .with_db(|db| {
                    Ok(db.observe_table_writes_for_test(Arc::new(move |event| {
                        if !received_schema_apply_observation_is_active_for_test() {
                            return;
                        }
                        *sink
                            .lock()
                            .observation
                            .artifact_writes
                            .entry(event.clone())
                            .or_default() += 1;
                    })))
                })
                .expect("attach actual writes to this file-backed database")
        });
        ReceivedSchemaApplyObservationScopeForTest {
            #[cfg(any(test, feature = "test-seams"))]
            _durable: durable,
            slot: self.received_schema_apply_observer.clone(),
            installed: observer.inner.clone(),
            previous,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_custody_keys(
        &self,
        selections: &[AuthoritativePurgeSelection],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<String>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut current = Vec::new();
        let mut retired = Vec::new();
        for selection in selections {
            let reference = crate::custody::records::RowRef {
                table: selection.table.clone(),
                key: selection.natural_key.clone(),
            };
            if self.table_meta(&selection.table).is_none()
                || self.durable_lineage_table_generation(&selection.table, read_memory.clone())?
                    != selection.table_generation
            {
                retired.push((
                    reference,
                    selection.table_generation,
                    selection.local_row_id.0,
                    selection.lineage_root.clone(),
                ));
            } else {
                current.push(reference);
            }
        }
        let mut keys = crate::custody::erasure::metadata_keys(self, &current, read_memory.clone())?;
        keys.extend(crate::custody::erasure::metadata_keys_for_retired_lives(
            self,
            &retired,
            read_memory,
        )?);
        keys.sort();
        keys.dedup();
        Ok(keys)
    }
}
