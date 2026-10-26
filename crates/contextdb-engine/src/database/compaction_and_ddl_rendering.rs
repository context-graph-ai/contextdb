use super::*;

/// Collapse each logical row of the compaction-eligible `tables` down to its
/// single current (max-LSN) version, dropping every superseded version and — in
/// lockstep — the change-log entries that referenced those versions.
///
/// Sync safety (the hard constraint): `changes_since`'s change-log replay reads
/// the row version AT each logged LSN (`row_for_change`), and on a missing
/// version silently substitutes the latest. Removing a version WITHOUT removing
/// its change-log entry would therefore corrupt replay. This function removes
/// them together, CHANGE-LOG-FIRST, so a crash between the two persisted writes
/// leaves at worst extra un-referenced versions (harmless — never replayed),
/// never an orphaned entry. Because the current version and its entry are always
/// kept, every `changes_since` replay — full edge pull (since=0), incremental
/// pull, or watermark-regressed re-push — still reconstructs the current row and
/// converges to current truth. These tables are all `LatestWins`, so the dropped
/// intermediate versions had no consumer value: a peer applies only the latest.
#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn compact_currency_versions_inner(
    relational_store: &Arc<RelationalStore>,
    _graph_store: &Arc<GraphStore>,
    vector_store: &Arc<VectorStore>,
    accountant: &crate::memory_accounting::AccountantHandle,
    persistence: Option<&Arc<RedbPersistence>>,
    change_log: &Arc<RwLock<Vec<ChangeLogEntry>>>,
    change_log_table_index: &Arc<RwLock<ChangeLogTableIndex>>,
    change_log_lsn_refcounts: &Arc<RwLock<ChangeLogLsnRefcounts>>,
    tx_mgr: &DatabaseTransactionHost,
    snapshot_registry: &Arc<SnapshotFloorRegistry>,
    publication: &mut contextdb_tx::manager::MaintenancePublication<'_, DynStore>,
    publication_interrupted: &Arc<AtomicBool>,
    tables: &[&str],
    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> Result<CurrencyCompactionReport> {
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

    // This pass's own sampled state: the committed watermark right now, and
    // (inside `begin_removal_pass`, atomically with each other) every
    // currently-registered reader's snapshot (an in-flight statement, or a
    // caller-held `SnapshotPin`). A superseded version still visible to ANY
    // of those registered snapshots is deferred to a later cycle instead of
    // being physically removed out from under that reader -- see
    // `any_registered_snapshot_sees`. `tx_mgr.current_tx_max()` is read here
    // under the same commit-lock hold this whole pass runs inside (both call
    // sites wrap `compact_currency_versions_inner` in `with_commit_lock`), so
    // no commit can land between this sample and the registered-set sample
    // `begin_removal_pass` takes next -- every version this pass ends up
    // pruning was therefore superseded AT OR BEFORE `watermark`, which is
    // exactly what `SnapshotFloorRegistry::register`'s blocking rule relies
    // on.
    // Every persistence read in this cleanup uses the maintenance caller's
    // existing reader child, including metadata and occupancy rebuilding.
    let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
        accountant.clone(),
        read_memory.clone(),
    )?
    .enter();
    let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    let watermark = tx_mgr.current_tx_max();
    // Records the sampled registered set and watermark as an in-progress
    // removal pass for the rest of this function's duration (held through
    // every return path, success or error, via `Drop`): a `pin_snapshot`
    // call racing in AFTER this sample but BEFORE this pass's removals are
    // fully applied must not silently miss the version it is entitled to
    // see -- see `SnapshotFloorRegistry::register`'s blocking rule.
    let active_removal_pass = snapshot_registry.begin_removal_pass(watermark);
    // Test-only: lets a test pause HERE, between the registered-set/watermark
    // sample above and the removal work below, to prove a concurrent
    // `pin_snapshot` blocks while this pass is genuinely in flight.
    snapshot_registry.removal_pass_test_pause.maybe_pause();
    // One finite allocation, with no hash buckets or growth overlap. The
    // credit is declared first so every copied payload and its backing is
    // physically freed before the admission returns, including on unwind.
    let mut metadata_credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
    let metas = {
        let catalog = relational_store.table_meta.read();
        let count = tables
            .iter()
            .filter(|table| catalog.contains_key(**table))
            .count();
        let backing = std::alloc::Layout::array::<(String, TableMeta)>(count)
            .map_err(|_| EngineMetadataJournal::identity_error())?
            .size();
        let bytes = tables
            .iter()
            .try_fold(backing, |bytes, table| -> Result<usize> {
                let Some(meta) = catalog.get(*table) else {
                    return Ok(bytes);
                };
                let nested = cleanup_declaration_copy_bytes(meta)?;
                bytes
                    .checked_add(table.len())
                    .and_then(|bytes| bytes.checked_add(nested))
                    .ok_or_else(EngineMetadataJournal::identity_error)
            })?;
        metadata_credit.try_grow_for(
            bytes,
            "reader",
            "cleanup_schema",
            "Raise reader memory to inspect the selected cleanup declarations.",
        )?;
        let mut copies = Vec::with_capacity(count);
        if copies.capacity() != count {
            return Err(EngineMetadataJournal::identity_error());
        }
        for table in tables {
            if let Some(meta) = catalog.get(*table) {
                copies.push(((*table).to_owned(), meta.clone()));
            }
        }
        copies
    };
    let selected_meta = |table: &str| {
        metas
            .iter()
            .find(|(name, _)| name == table)
            .map(|(_, meta)| meta)
    };
    let frontier = MaintenanceFrontier::capture(
        relational_store,
        tx_mgr,
        crate::persistence::writer_source::SourceWindowOwner::writer(accountant.clone()),
    )?;
    let durable_source = frontier.is_durable();
    let tables_in_snapshot = tables
        .iter()
        .filter(|table| selected_meta(table).is_some())
        .count() as u64;
    let mut workspace = crate::memory_accounting::OwnedMemoryReservation::new(accountant.clone());

    let mut pruned_versions_by_table: HashMap<String, HashSet<(RowId, TxId)>> = HashMap::new();
    // Grouped by table (not a flat `(table, row_id, lsn)` set): every
    // per-entry lookup below is then a borrow-based `HashMap` lookup with
    // no `String` allocation, and `count_pruned_change_log_entries`/
    // `remove_pruned_change_log_entries` can walk one table's own entries
    // at a time.
    let mut pruned_change_keys: HashMap<String, HashSet<(RowId, Lsn)>> = HashMap::new();
    // The full relational key of every pruned version, in hand from the row
    // already being walked -- no scan is needed later to find it.
    let mut row_keys: Vec<(String, RowId, TxId, Lsn)> = Vec::new();
    // The same versions' identity, for matching a vector copy attached to a
    // released row version: a vector write shares its row's `created_tx`/
    // `lsn` (same commit), so this triple names it exactly.
    let mut pruned_row_identities: HashSet<(RowId, TxId, Lsn)> = HashSet::new();
    let mut released_row_bytes = 0usize;
    // See the retention pass above: the bytes this pass physically reclaims
    // and the bytes it returns to the accountant are different sets. A version
    // superseded by an update carries `deleted_tx` and was released at commit.
    let mut accounted_row_bytes = 0usize;
    let mut versions_deferred_for_readers = 0u64;
    let mut progress_memory =
        crate::memory_accounting::OwnedMemoryReservation::new(accountant.clone());
    let mut cleanup_progress = Vec::new();
    let mut cleanup_rotation = None;
    let mut ordered_tables = Vec::new();
    if durable_source {
        progress_memory.try_grow_for(
            512usize.saturating_add(tables.len().saturating_mul(64)),
            "version_cleanup",
            "progress_control",
            "Raise MEMORY_LIMIT to admit cleanup scheduling controls.",
        )?;
        ordered_tables.extend(
            tables
                .iter()
                .map(|table| (*blake3::hash(table.as_bytes()).as_bytes(), *table)),
        );
        ordered_tables.sort_unstable_by_key(|(hash, _)| *hash);
        if let Some(after) = frontier.cleanup_rotation()? {
            let start = ordered_tables.partition_point(|(hash, _)| *hash <= after);
            if start < ordered_tables.len() {
                ordered_tables.rotate_left(start);
            }
        }
    }
    let mut remaining = 128usize;
    let mut removal_allowance = 64usize;

    {
        let mut select = |table: &str, row: &VersionedRow, keeper_lsn: Lsn| -> Result<bool> {
            #[cfg(any(test, feature = "test-seams"))]
            if !durable_source {
                resident_history_test_seams::note(|work| work.cleanup_versions_examined += 1);
            }
            if keeper_lsn == row.lsn {
                // A deleted row's final version is still its relational
                // keeper (the delete stamps that version; the tombstone stays
                // for the relational and sync doors), but under `HISTORY
                // CURRENT ONLY` the row has no current version, so the vector
                // copy attached to it is a tombstone with nothing left to
                // serve: a deletion remains a vector tombstone only until
                // compaction, and is reclaimed once no registered snapshot
                // can still resolve it. Named here by the same identity
                // triple as a superseded copy; only the vector side is
                // pruned.
                if row.deleted_tx.is_some()
                    && !active_removal_pass.protects_history(row.lsn)
                    && !any_registered_snapshot_sees(
                        active_removal_pass.registered_snapshots(),
                        row,
                    )
                {
                    workspace.try_grow_for(
                        512usize.saturating_add(table.len().saturating_mul(8)),
                        "version_cleanup",
                        "prepare_currency",
                        "Raise MEMORY_LIMIT to admit the selected cleanup identities.",
                    )?;
                    pruned_row_identities.insert((row.row_id, row.created_tx, row.lsn));
                }
                return Ok(false);
            }
            // A superseded version still visible to ANY registered reader
            // (an in-flight statement, or a caller-held `SnapshotPin`) is
            // deferred to a later cycle rather than physically removed out
            // from under it -- checked against every snapshot this pass
            // sampled, not merely the lowest, so a version between two
            // registered snapshots (visible to the higher one but not the
            // lower) is still protected.
            if active_removal_pass.protects_history(row.lsn)
                || any_registered_snapshot_sees(active_removal_pass.registered_snapshots(), row)
            {
                versions_deferred_for_readers = versions_deferred_for_readers.saturating_add(1);
                return Ok(false);
            }
            workspace.try_grow_for(
                2048usize.saturating_add(table.len().saturating_mul(16)),
                "version_cleanup",
                "prepare_currency",
                "Raise MEMORY_LIMIT to admit the selected cleanup identities.",
            )?;
            pruned_versions_by_table
                .entry(table.to_string())
                .or_default()
                .insert((row.row_id, row.created_tx));
            pruned_change_keys
                .entry(table.to_string())
                .or_default()
                .insert((row.row_id, row.lsn));
            row_keys.push((table.to_string(), row.row_id, row.created_tx, row.lsn));
            pruned_row_identities.insert((row.row_id, row.created_tx, row.lsn));
            // The same door `account_loaded_state` charged through, so a
            // version physically removed below (`pruned_versions_by_table`,
            // already recorded above this guard) while its table's meta is
            // missing releases exactly what its load charged.
            let version_bytes =
                retained_row_bytes_with_optional_meta(table, row, selected_meta(table));
            released_row_bytes = released_row_bytes.saturating_add(version_bytes);
            if !frontier.is_durable()
                && (row.deleted_tx.is_none()
                    || relational_store.is_retention_expired(table, row.row_id, row.created_tx))
            {
                // An update stamps the version it supersedes with
                // `deleted_tx` and releases its charge at commit, so this
                // pass reclaims the memory without returning the bytes a
                // second time. A non-keeper version that was never marked
                // deleted still holds its charge, and this is its one
                // release.
                accounted_row_bytes = accounted_row_bytes.saturating_add(version_bytes);
            }
            Ok(true)
        };
        for ordinal in 0..tables.len() {
            let table = if durable_source {
                ordered_tables[ordinal].1
            } else {
                tables[ordinal]
            };
            if selected_meta(table).is_none() {
                continue;
            }
            if frontier.is_durable() {
                if remaining == 0 || removal_allowance == 0 || cleanup_progress.len() == 128 {
                    break;
                }
                progress_memory.try_grow_for(
                    2048usize.saturating_add(table.len().saturating_mul(16)),
                    "version_cleanup",
                    "progress_window",
                    "Raise MEMORY_LIMIT to admit a finite cleanup continuation.",
                )?;
                let progress = frontier.select_durable_cleanup(
                    table,
                    selected_meta(table).expect("selected cleanup declaration"),
                    &mut remaining,
                    &mut removal_allowance,
                    |row, keeper| select(table, row, keeper),
                )?;
                cleanup_progress.push((table.to_string(), progress));
                cleanup_rotation = Some(*blake3::hash(table.as_bytes()).as_bytes());
            } else {
                let mut keeper_lsn = HashMap::<RowId, Lsn>::new();
                let mut keeper_memory =
                    crate::memory_accounting::OwnedMemoryReservation::new(accountant.clone());
                frontier.visit(table, |row| {
                    #[cfg(any(test, feature = "test-seams"))]
                    resident_history_test_seams::note(|work| work.cleanup_versions_examined += 1);
                    if !keeper_lsn.contains_key(&row.row_id) {
                        keeper_memory.try_grow_for(
                            256,
                            "version_cleanup",
                            "keeper_identities",
                            "Raise MEMORY_LIMIT to admit the current-row keeper identities.",
                        )?;
                    }
                    keeper_lsn
                        .entry(row.row_id)
                        .and_modify(|lsn| *lsn = (*lsn).max(row.lsn))
                        .or_insert(row.lsn);
                    Ok(true)
                })?;
                frontier.visit(table, |row| {
                    select(table, row, keeper_lsn[&row.row_id]).map(|_| true)
                })?;
            }
        }
    }
    drop(frontier);

    let pruned_version_count: u64 = pruned_versions_by_table
        .values()
        .map(|versions| versions.len() as u64)
        .sum();
    // A pass with no superseded relational version may still owe the vector
    // copies of deleted rows (named into `pruned_row_identities` above), so
    // it returns early only when it has nothing at all to reclaim.
    if pruned_version_count == 0 && pruned_row_identities.is_empty() && cleanup_progress.is_empty()
    {
        return Ok(CurrencyCompactionReport {
            tables_in_memory_snapshot: tables_in_snapshot,
            versions_deferred_for_readers,
            ..CurrencyCompactionReport::default()
        });
    }

    // Vector copies attached to a released row version are part of the
    // reclaim set too: a declared vector-bearing table would otherwise stay
    // unbounded while this report claims success. Scoped to exactly the
    // vector columns of the tables THIS pass touches, never the whole vector
    // store: an unrelated vector-bearing table's population must not show up
    // in this pass's exclusive commit-lock hold time -- precisely the
    // proportional-to-ballast cost this rewrite exists to remove. The copies
    // are named from the partitions' durable directories
    // (`directory_versions_matching`), never by reading bodies: a partition
    // that is dormant after a restart stays dormant, and the superseded copy
    // it holds is still pruned durably by identity below.
    // Edges are NOT touched: edge identity is self-owned (`source`, `target`,
    // `edge_type` plus its own `created_tx`/`lsn`), versioned by no
    // relational row, so cleanup neither needs nor claims edge boundedness
    // (see `prune_vectors_and_edges_scoped`'s doc comment).
    let released_vectors: Vec<VectorVersionIdentity> = if pruned_row_identities.is_empty() {
        Vec::new()
    } else {
        let mut candidates = Vec::new();
        for table in tables {
            let Some(meta) = selected_meta(table) else {
                continue;
            };
            for column in &meta.columns {
                if !matches!(column.column_type, ColumnType::Vector(_)) {
                    continue;
                }
                let index_ref = VectorIndexRef::new(*table, column.name.clone());
                candidates.extend(vector_store.directory_versions_matching(
                    &index_ref,
                    &pruned_row_identities,
                    &mut vector_copy_owner,
                )?);
            }
        }
        candidates
    };
    // Bounded to the tables THIS pass touches (`count_pruned_change_log_
    // entries` walks each affected table's own `change_log_table_index`
    // shadow, never the whole change log) -- persist-then-mutate: nothing
    // here mutates `change_log` itself; the in-memory `Vec` is actually
    // mutated further down, after every persisted step has succeeded.
    let pruned_change_keys_vec: Vec<(String, RowId, Lsn)> = pruned_change_keys
        .iter()
        .flat_map(|(table, keys)| {
            keys.iter()
                .map(move |(row_id, lsn)| (table.clone(), *row_id, *lsn))
        })
        .collect();
    let (pruned_change_log_entries, removed_lsn_counts) = {
        if let Some(root) = writer_read::durable_root(tx_mgr.capture_published_read().handle)? {
            RedbPersistence::pruned_change_counts_at_root(
                &root,
                crate::persistence::writer_source::SourceWindowOwner::writer(accountant.clone()),
                &pruned_change_keys,
            )?
        } else {
            let table_index = change_log_table_index.read();
            count_pruned_change_log_entries(&table_index, &pruned_change_keys)
        }
    };

    // Select commit-index removals from the pre-pass counts before deleting any
    // nomination source. The anchor and unrelated references remain protected.
    let affected_lsns: HashSet<Lsn> = removed_lsn_counts.keys().copied().collect();
    let commit_index_candidates = {
        let lsn_refcounts = change_log_lsn_refcounts.read();
        scoped_commit_index_candidates(tx_mgr, &lsn_refcounts, &removed_lsn_counts, &affected_lsns)?
    };

    // Capture candidate membership before the same durable transaction removes
    // it. The file directory has no second resident map to remove afterward.
    let durable_directory_removals = if persistence.is_some() {
        commit_index_candidates
            .iter()
            .try_fold(0u64, |count, lsn| {
                tx_mgr
                    .try_commit_index_contains(*lsn)
                    .map(|present| count + u64::from(present))
            })?
    } else {
        0
    };

    // Only tables with selected row or vector removals belong to this
    // atomic cleanup. Unchanged tables' generation catalogs are handled by
    // catalog maintenance, without occupying this batch's removal workspace.
    let generation_cleanup = prepare_cleanup_generations(
        vector_store,
        persistence,
        row_keys
            .iter()
            .map(|(table, _, _, _)| table.as_str())
            .chain(
                released_vectors
                    .iter()
                    .map(|version| version.index.table.as_str()),
            ),
        active_removal_pass.registered_snapshots(),
        &mut vector_copy_owner,
    )?;
    let mut vector_keys_rewritten = 0u64;
    let mut first_failure_details: Option<VectorMaintenanceFailureDetails> = None;
    let schema_history = relational_store.prepare_schema_history_erasure(
        row_keys.iter().map(|(_, row, _, lsn)| (*row, *lsn)),
        accountant.read_budget()?,
    )?;
    let mut publication_watch = None;
    if let Some(persistence) = persistence {
        // Fail before any durable or in-memory removal. The same selected
        // identities remain available to a later cycle after an error.
        if take_currency_compaction_persist_fault_for_test() {
            return Err(Error::Other(
                ("injected persistence failure mid version-cleanup pass (test seam)".to_string())
                    .into(),
            ));
        }
        // Rows, change-log references, vector bodies, memberships, tombstones,
        // and eligible commit-index entries commit together. A crash at either
        // former transaction gap leaves the entire removal set retryable.
        // Sync-source provenance stays: only superseded row versions are removed.
        // Proof maintenance obeys the owning budget. A precommit refusal
        // leaves both durable and in-memory removal pending for a later
        // accounted pass; the error branch below disarms the publication watch.
        publication.reserve()?;
        let watch = PublicationInterruptionWatch::arm(publication_interrupted);
        let durable = std::cell::Cell::new(false);
        let persisted = persistence.prune_currency_scoped(
            &row_keys,
            &pruned_change_keys_vec,
            &released_vectors,
            &commit_index_candidates,
            &generation_cleanup,
            &durable,
            &cleanup_progress,
            cleanup_rotation,
        );
        let stats = match persisted {
            Ok(stats) => stats,
            Err(error) => {
                // An error after the durable boundary cannot certify rollback
                // or keep serving the old canonical root. Its armed watch
                // poisons this writer; only a precommit refusal disarms it.
                if !durable.get() {
                    watch.completed();
                }
                return Err(error);
            }
        };
        publication_watch = Some(watch);
        vector_keys_rewritten = stats.vector_keys_removed;
        first_failure_details = stats.proof_admission_refused;
    }

    // Publish memory only after the complete durable removal succeeds, under
    // the existing commit and snapshot-removal gates. The physical change-log
    // removal visits its affected suffix, not unrelated earlier entries.
    let released_log_bytes = if let Some(min_affected_lsn) = affected_lsns.iter().min().copied() {
        remove_pruned_change_log_entries(
            accountant,
            &mut change_log.write(),
            &pruned_change_keys,
            min_affected_lsn,
        )
    } else {
        0
    };
    remove_pruned_from_table_index(&mut change_log_table_index.write(), &pruned_change_keys);
    apply_removed_lsn_counts(&mut change_log_lsn_refcounts.write(), &removed_lsn_counts);
    // The REAL count: `tx_mgr`'s own in-memory removal, not the persisted
    // side effect's count -- see `scoped_commit_index_removal`'s own doc
    // comment (this is the discrepancy `version_cleanup_shrinks_the_commit_
    // index_to_reachable_entries` caught: an in-memory database's persisted
    // count is always 0 even though the in-memory index genuinely shrinks).
    let commit_index_keys_removed = if commit_index_candidates.is_empty() {
        0
    } else if persistence.is_some() {
        durable_directory_removals
    } else {
        let removed = tx_mgr.try_remove_commit_index_entries(&commit_index_candidates)?;
        if persistence.is_none() {
            accountant.release(
                (removed as usize).saturating_mul(crate::composite_store::COMMIT_INDEX_ENTRY_BYTES),
            );
        }
        removed
    };
    relational_store.publish_schema_history_erasure(schema_history);
    for (table, versions) in &pruned_versions_by_table {
        relational_store.remove_row_versions(table, versions);
        if let Some(meta) = selected_meta(table) {
            for index in &meta.indexes {
                relational_store.rebuild_index_preserving_current(table, &index.name);
            }
        }
    }
    // A partition state that carries the accountant returns each pruned raw
    // body from its own record inside the prune; what comes back here is
    // only the bytes a state without an accountant still leaves charged to
    // the per-insert admission, which this pass is the one release for.
    let released_vector_bytes =
        vector_store.prune_superseded_versions(&released_vectors, &mut vector_copy_owner)?;
    for (catalog, _) in &generation_cleanup {
        vector_store.reclaim_snapshot_free_partition_graph_generations(
            &VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone()),
            active_removal_pass.registered_snapshots(),
        );
    }

    accountant.release(accounted_row_bytes.saturating_add(released_vector_bytes));

    // A table whose only reclaimed state this pass was a deleted row's vector
    // copy was compacted too.
    let mut compacted_tables: Vec<String> = pruned_versions_by_table
        .keys()
        .cloned()
        .chain(
            released_vectors
                .iter()
                .map(|version| version.index.table.clone()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    compacted_tables.sort();
    let report = CurrencyCompactionReport {
        pruned_versions: pruned_version_count,
        pruned_change_log_entries,
        reclaimed_bytes: (if durable_source {
            accounted_row_bytes.saturating_add(released_log_bytes)
        } else {
            released_row_bytes
        })
        .saturating_add(released_vector_bytes) as u64,
        compacted_tables,
        redb_compacted: false,
        // No wholesale survivor rewrite. Selected change-log LSN groups are
        // still densely rewritten; this compatibility field does not count I/O.
        keys_rewritten: 0,
        vector_keys_rewritten,
        edge_keys_rewritten: 0,
        commit_lock_hold_micros: 0,
        redb_compact_micros: 0,
        commit_index_keys_removed,
        tables_in_memory_snapshot: tables_in_snapshot,
        versions_deferred_for_readers,
        first_failure: first_failure_details.map(|details| details.failure),
        first_failure_details,
    };
    if let Some(watch) = publication_watch {
        publication.complete();
        watch.completed();
    }
    drop(active_removal_pass);
    Ok(report)
}

/// Cheap pre-tick gate for the maintenance thread: are there at least
/// `threshold` superseded versions across the eligible `tables`? Reads only the
/// relational tables lock (never the commit lock) and early-returns, so a quiet
/// node does near-zero work per tick.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn currency_compaction_pending(
    relational_store: &Arc<RelationalStore>,
    manager: &DatabaseTransactionHost,
    accountant: &crate::memory_accounting::AccountantHandle,
    tables: &[&str],
    threshold: usize,
) -> Result<bool> {
    // This read-only probe keeps its own published root. It never substitutes
    // an empty resident directory for the durable candidate population.
    let frontier = MaintenanceFrontier::capture(
        relational_store,
        manager,
        crate::persistence::writer_source::SourceWindowOwner::writer(accountant.clone()),
    )?;
    let mut superseded = 0usize;
    for table in tables {
        if !frontier.has_table(table)? {
            continue;
        }
        {
            let metadata = relational_store.table_meta.read();
            if let Some(meta) = metadata.get(*table)
                && frontier.cleanup_in_progress(table, meta)?
            {
                return Ok(true);
            }
        }
        if frontier.is_durable() {
            let mut previous = None;
            frontier.visit(table, |row| {
                if previous == Some(row.row_id) {
                    superseded = superseded.saturating_add(1);
                }
                previous = Some(row.row_id);
                Ok(superseded < threshold)
            })?;
        } else {
            let mut distinct = HashSet::<RowId>::new();
            let mut scratch =
                crate::memory_accounting::OwnedMemoryReservation::new(accountant.clone());
            frontier.visit(table, |row| {
                if distinct.contains(&row.row_id) {
                    superseded = superseded.saturating_add(1);
                } else {
                    scratch.try_grow_for(
                        128,
                        "version_cleanup",
                        "pending_identities",
                        "Raise MEMORY_LIMIT to inspect current-row cleanup identities.",
                    )?;
                    distinct.insert(row.row_id);
                }
                Ok(superseded < threshold)
            })?;
        }
        if superseded >= threshold {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn export_io_error(dest: &Path, err: &dyn std::fmt::Display) -> Error {
    Error::ExportIo {
        path: dest.to_path_buf(),
        reason: err.to_string(),
    }
}

/// Storage-layer snapshot filter: versions created after the watermark are
/// skipped entirely; a version live at the watermark with a post-snapshot
/// tombstone exports as live, so post-snapshot markers never leak.
pub(super) fn export_row_at_snapshot(row: &VersionedRow, watermark: TxId) -> Option<VersionedRow> {
    if row.created_tx.0 > watermark.0 {
        return None;
    }
    let mut row = row.clone();
    if row.deleted_tx.is_some_and(|tx| tx.0 > watermark.0) {
        row.deleted_tx = None;
    }
    Some(row)
}

pub(super) fn export_edge_at_snapshot(entry: &AdjEntry, watermark: TxId) -> Option<AdjEntry> {
    if entry.created_tx.0 > watermark.0 {
        return None;
    }
    let mut entry = entry.clone();
    if entry.deleted_tx.is_some_and(|tx| tx.0 > watermark.0) {
        entry.deleted_tx = None;
    }
    Some(entry)
}

// Source windows are preparation work owned by this retention request. Keep
// their actual requested/headroom figures while preserving the public refusal
// identity; all source receipts are dropped by the failed preparation itself.
pub(super) fn retention_source_admission_error(
    mut error: Error,
    preparation: RetentionPreparation,
) -> Error {
    if let Error::MemoryBudgetExceeded {
        subsystem,
        operation,
        hint,
        ..
    } = &mut error
        && subsystem == "writer_source"
    {
        *subsystem = "retention".into();
        *operation = preparation.operation().into();
        *hint = "Raise MEMORY_LIMIT to admit the retention selection workspace.".into();
    }
    error
}

pub(super) fn log_pruning_error(err: &Error) {
    tracing::warn!(
        name: "retention_pruning_error",
        target: "retention_pruning",
        error = %err,
        "retention_pruning_error"
    );
}

impl RetentionPreparation {
    pub(super) fn operation(self) -> &'static str {
        match self {
            Self::Expiry => "prepare_expiry",
            Self::Prune => "prepare_prune",
        }
    }
}

// Candidate and protected-set buckets, growth slack and the longest possible
// FK blocker string (including the transient joined composite-column names).
// Payload values are borrowed, so TEXT/BLOB/vector width is absent here.
pub(super) fn retention_candidate_workspace_bytes(
    table: &str,
    metas: &HashMap<String, TableMeta>,
) -> usize {
    let mut blocker_bytes = 0usize;
    for (child, meta) in metas {
        for column in &meta.columns {
            if let Some(reference) = &column.references
                && reference.table == table
            {
                blocker_bytes = blocker_bytes.max(
                    128usize
                        .saturating_add(table.len())
                        .saturating_add(child.len())
                        .saturating_add(column.name.len())
                        .saturating_add(reference.column.len()),
                );
            }
        }
        for fk in &meta.composite_foreign_keys {
            if fk.parent_table == table {
                let columns = fk
                    .parent_columns
                    .iter()
                    .chain(&fk.child_columns)
                    .fold(0usize, |bytes, name| {
                        bytes.saturating_add(name.len().saturating_add(2))
                    });
                blocker_bytes = blocker_bytes.max(
                    128usize
                        .saturating_add(table.len())
                        .saturating_add(child.len())
                        .saturating_add(columns.saturating_mul(2)),
                );
            }
        }
    }
    512usize
        .saturating_add(table.len().saturating_mul(4))
        .saturating_add(blocker_bytes.saturating_mul(2))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn retention_prune_candidates(
    metas: &HashMap<String, TableMeta>,
    frontier: &MaintenanceFrontier<'_>,
    now: Wallclock,
    sync_watermark: Lsn,
    workspace: &mut crate::memory_accounting::OwnedMemoryReservation,
    preparation: RetentionPreparation,
) -> Result<RetentionCandidates> {
    let mut candidates = RetentionCandidates::default();
    frontier.set_cohort_clock(now, sync_watermark);
    for (table, meta) in metas {
        if meta.default_ttl_seconds.is_none() {
            continue;
        }
        let candidate_bytes = retention_candidate_workspace_bytes(table, metas);
        frontier.visit(table, |row| {
            #[cfg(any(test, feature = "test-seams"))]
            resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
            if matches!(preparation, RetentionPreparation::Expiry) && row.deleted_tx.is_some() {
                return Ok(true);
            }
            let arrived_by_sync = matches!(
                frontier.sync_source_kind(table, row.row_id)?,
                Some(contextdb_relational::store::SyncSourceKind::Pulled)
            );
            if row_is_prunable(row, meta, now, sync_watermark, arrived_by_sync) {
                workspace.try_grow_for(
                    candidate_bytes,
                    "retention",
                    preparation.operation(),
                    "Raise MEMORY_LIMIT to admit the retention selection workspace.",
                )?;
                candidates.insert(table, row);
            }
            Ok(true)
        })?;
    }
    Ok(candidates)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn fk_protected_prune_candidates(
    metas: &HashMap<String, TableMeta>,
    frontier: &MaintenanceFrontier<'_>,
    candidates: &RetentionCandidates,
) -> Result<RetentionCandidates> {
    let mut protected = RetentionCandidates::default();
    if candidates.is_empty() {
        return Ok(protected);
    }
    loop {
        let mut changed = false;
        for (table, meta) in metas {
            if meta.default_ttl_seconds.is_none() {
                continue;
            }
            frontier.visit(table, |row| {
                #[cfg(any(test, feature = "test-seams"))]
                resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
                if !candidates.contains(table, row) || row.deleted_tx.is_some() {
                    return Ok(true);
                }
                if prune_blocker_for_referenced_parent(
                    table, row, metas, frontier, candidates, &protected,
                )?
                .is_some()
                {
                    changed |= protected.insert(table, row);
                }
                Ok(true)
            })?;
        }
        if !changed {
            return Ok(protected);
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn prune_blocker_for_referenced_parent(
    parent_table: &str,
    parent: &VersionedRow,
    metas: &HashMap<String, TableMeta>,
    frontier: &MaintenanceFrontier<'_>,
    candidates: &RetentionCandidates,
    protected: &RetentionCandidates,
) -> Result<Option<String>> {
    if parent.deleted_tx.is_some() {
        return Ok(None);
    }
    for (child_table, meta) in metas {
        for column in &meta.columns {
            let Some(reference) = &column.references else {
                continue;
            };
            if reference.table != parent_table {
                continue;
            }
            let Some(value) = parent.values.get(&reference.column) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            if child_rows_contain_live_reference(
                metas,
                parent_table,
                child_table,
                frontier,
                std::slice::from_ref(&column.name),
                parent,
                std::slice::from_ref(&reference.column),
                candidates,
                protected,
            )? {
                return Ok(Some(format!(
                    "blocked pruning {}({}) because live {}({}) references it",
                    parent_table, reference.column, child_table, column.name
                )));
            }
        }
        for fk in &meta.composite_foreign_keys {
            if fk.parent_table != parent_table {
                continue;
            }
            if child_rows_contain_live_reference(
                metas,
                parent_table,
                child_table,
                frontier,
                &fk.child_columns,
                parent,
                &fk.parent_columns,
                candidates,
                protected,
            )? {
                return Ok(Some(format!(
                    "blocked pruning {}({}) because live {}({}) references it",
                    parent_table,
                    fk.parent_columns.join(", "),
                    child_table,
                    fk.child_columns.join(", ")
                )));
            }
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn child_rows_contain_live_reference(
    metas: &HashMap<String, TableMeta>,
    parent_table: &str,
    child_table: &str,
    frontier: &MaintenanceFrontier<'_>,
    child_columns: &[String],
    parent: &VersionedRow,
    parent_columns: &[String],
    candidates: &RetentionCandidates,
    protected: &RetentionCandidates,
) -> Result<bool> {
    if parent_columns
        .iter()
        .any(|column| matches!(parent.values.get(column), None | Some(Value::Null)))
    {
        return Ok(false);
    }
    let mut found = false;
    let values = parent_columns
        .iter()
        .map(|column| parent.values.get(column).expect("non-null parent key"))
        .collect::<Vec<_>>();
    frontier.visit_references(child_table, child_columns, &values, |row| {
        #[cfg(any(test, feature = "test-seams"))]
        if !frontier.is_durable() {
            resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
        }
        found = row.deleted_tx.is_none()
            && child_columns
                .iter()
                .zip(parent_columns)
                .all(|(column, value)| row.values.get(column) == parent.values.get(value))
            && (!candidates.contains(child_table, row) || protected.contains(child_table, row));
        if found && !candidates.contains(child_table, row) {
            frontier.note_cohort_blocker(metas, parent_table, parent, child_table, row)?;
        }
        Ok(!found)
    })?;
    Ok(found)
}

pub(super) fn row_is_prunable(
    row: &VersionedRow,
    meta: &TableMeta,
    now: Wallclock,
    sync_watermark: Lsn,
    arrived_by_sync: bool,
) -> bool {
    // `SYNC SAFE` is a delete-AFTER-DELIVERY promise, so it can only pin a row
    // that still owes a delivery — one this node WROTE and has not yet had
    // confirmed. A row that ARRIVED by sync has already been delivered, to
    // here; it owes nothing onward (a retained table is one-way, and this is
    // the receiving end), so gating it on a watermark that nothing will ever
    // advance would pin the hub's copy forever instead of retaining it.
    if meta.sync_safe && !arrived_by_sync && row.lsn >= sync_watermark {
        return false;
    }

    // The expiry judgement itself — EXPIRES-column precedence, then the
    // created_at + window fallback — is shared with the serve-time sync filter
    // so the two never disagree about a row. Only the SYNC SAFE delivery pin
    // above is prune-local; expiry is not.
    meta.retained_row_has_expired(&row.values, row.created_at, now)
}

pub(super) fn max_tx_across_all(relational: &RelationalStore, graph: &GraphStore) -> TxId {
    let relational_max = relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter())
        .flat_map(|row| std::iter::once(row.created_tx).chain(row.deleted_tx))
        .max()
        .unwrap_or(TxId(0));
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter())
        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
        .max()
        .unwrap_or(TxId(0));
    relational_max.max(graph_max)
}

/// Vector memberships are written only by commits, and every commit's LSN is
/// bounded by the authenticated commit directory, DDL log and source floor the
/// caller folds in -- the same bound a writer derives its next LSN from
/// (`writer_open_bounds_in_read`). Walking every membership here would make
/// read-only open proportional to stored vectors.
pub(super) fn max_lsn_across_all(relational: &RelationalStore, graph: &GraphStore) -> Lsn {
    let relational_max = relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter().map(|row| row.lsn))
        .max()
        .unwrap_or(Lsn(0));
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().map(|entry| entry.lsn))
        .max()
        .unwrap_or(Lsn(0));
    relational_max.max(graph_max)
}

/// Given a delete's threshold `lsn`, return the `deleted_tx` of the tombstone
/// with the greatest `lsn` strictly below the threshold, or `None` if there is
/// none. `versions` must be sorted ascending by `lsn` (stably, so equal-`lsn`
/// entries preserve their original store order). This reproduces the original
/// `filter(lsn < threshold).max_by_key(lsn).deleted_tx` semantics exactly,
/// including its tie-break: `max_by_key` returns the LAST of several equal
/// maxima, which is the highest-index (last-stored) equal-`lsn` entry here.
#[cfg(test)]
pub(super) fn resolve_tombstone(versions: &[(Lsn, TxId)], threshold: Lsn) -> Option<TxId> {
    let boundary = versions.partition_point(|(lsn, _)| *lsn < threshold);
    if boundary == 0 {
        None
    } else {
        Some(versions[boundary - 1].1)
    }
}

/// Reconstructs the `Lsn -> TxId` commit index from the live stores plus the
/// change log. Runs on EVERY `Database::open`, so it must be near-linear: it
/// builds per-(table, row_id) / per-edge / per-(index, row_id) tombstone
/// lookups ONCE by borrowing (never cloning whole tables or adjacency lists),
/// then resolves each delete with a binary search — O(R + D log k) total, where
/// the original was O(deletes × rows). The produced index is byte-identical to
/// the original algorithm for any input (proven by
/// `commit_index_reconstruction_tests::commit_index_matches_naive_across_shapes`).
#[cfg(test)]
pub(super) fn commit_index_across_all(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
    change_log: &[ChangeLogEntry],
) -> BTreeMap<Lsn, TxId> {
    let mut index = BTreeMap::new();
    let mut add_entry = |lsn: Lsn, tx: TxId| {
        if lsn != Lsn(0) {
            index
                .entry(lsn)
                .and_modify(|current: &mut TxId| *current = (*current).max(tx))
                .or_insert(tx);
        }
    };

    // Relational: one borrowed pass records every created_tx and collects
    // tombstones keyed by (table, row_id). The read guard is held for the whole
    // function so the &str table keys stay valid through delete resolution.
    let relational_tables = relational.tables.read();
    let mut row_tombstones: RowTombstoneIndex = HashMap::new();
    for (table, rows) in relational_tables.iter() {
        for row in rows {
            add_entry(row.lsn, row.created_tx);
            if let Some(deleted_tx) = row.deleted_tx {
                row_tombstones
                    .entry(table.as_str())
                    .or_default()
                    .entry(row.row_id)
                    .or_default()
                    .push((row.lsn, deleted_tx));
            }
        }
    }
    for by_row in row_tombstones.values_mut() {
        for versions in by_row.values_mut() {
            versions.sort_by_key(|(lsn, _)| *lsn);
        }
    }

    // Graph: same borrowed pass, tombstones keyed by (source, edge_type, target).
    let forward_adj = graph.forward_adj.read();
    let mut edge_tombstones: EdgeTombstoneIndex = HashMap::new();
    for entries in forward_adj.values() {
        for edge in entries {
            add_entry(edge.lsn, edge.created_tx);
            if let Some(deleted_tx) = edge.deleted_tx {
                edge_tombstones
                    .entry((edge.source, edge.edge_type.as_str(), edge.target))
                    .or_default()
                    .push((edge.lsn, deleted_tx));
            }
        }
    }
    for versions in edge_tombstones.values_mut() {
        versions.sort_by_key(|(lsn, _)| *lsn);
    }

    // Vector: the lightweight directory carries every transaction and LSN
    // needed here, so startup bookkeeping never faults a raw body.
    let declared_reader = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        contextdb_core::read_contract::ReadLimits::default().memory,
    )
    .expect("admit the declared fixture root");
    let mut copy_owner = crate::vector_read_memory::admission(
        declared_reader
            .child()
            .expect("admit the commit index fixture reader"),
    );
    let vector_entries = vector
        .raw_directory_entries(&mut copy_owner)
        .expect("read the directory identity fixture");
    let mut vector_tombstones: VectorTombstoneIndex = HashMap::new();
    for (vector_index, entry) in vector_entries {
        add_entry(entry.lsn, entry.created_tx);
        if let Some(deleted_tx) = entry.deleted_tx {
            vector_tombstones
                .entry((vector_index, entry.row_id))
                .or_default()
                .push((entry.lsn, deleted_tx));
        }
    }
    for versions in vector_tombstones.values_mut() {
        versions.sort_by_key(|(lsn, _)| *lsn);
    }

    for entry in change_log {
        match entry {
            ChangeLogEntry::RowInsert { .. }
            | ChangeLogEntry::EdgeInsert { .. }
            | ChangeLogEntry::VectorInsert { .. } => {}
            ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } => {
                if let Some(deleted_tx) = row_tombstones
                    .get(table.as_str())
                    .and_then(|by_row| by_row.get(row_id))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            } => {
                if let Some(deleted_tx) = edge_tombstones
                    .get(&(*source, edge_type.as_str(), *target))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::VectorDelete {
                index: vector_index,
                row_id,
                lsn,
            } => {
                if let Some(deleted_tx) = vector_tombstones
                    .get(&(vector_index.clone(), *row_id))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
        }
    }
    index
}

pub(super) fn is_fatal_sync_apply_error(err: &Error) -> bool {
    matches!(
        err,
        Error::MemoryBudgetExceeded { .. } | Error::DiskBudgetExceeded { .. }
    )
}

pub(super) fn is_sync_access_scope_error(err: &Error) -> bool {
    matches!(
        err,
        Error::ContextScopeViolation { .. }
            | Error::ScopeLabelViolation { .. }
            | Error::AclDenied { .. }
            | Error::PrincipalRequired { .. }
    )
}

pub(super) fn ddl_change_from_create_table(ct: &CreateTable) -> DdlChange {
    DdlChange::CreateTable {
        name: ct.name.clone(),
        columns: ct
            .columns
            .iter()
            .map(|col| {
                (
                    col.name.clone(),
                    sql_type_for_ast_column(col, &ct.propagation_rules),
                )
            })
            .collect(),
        constraints: create_table_constraints_from_ast(ct),
        foreign_keys: single_column_foreign_keys_from_ast(ct),
        composite_foreign_keys: ct
            .composite_foreign_keys
            .iter()
            .map(|fk| CompositeForeignKey {
                child_columns: fk.child_columns.clone(),
                parent_table: fk.parent_table.clone(),
                parent_columns: fk.parent_columns.clone(),
            })
            .collect(),
        composite_unique: ct.unique_constraints.clone(),
    }
}

pub(crate) fn ddl_change_from_meta(name: &str, meta: &TableMeta) -> DdlChange {
    ddl_change_from_meta_excluding(name, meta, &HashSet::new())
}

pub(super) fn ddl_change_from_meta_excluding(
    name: &str,
    meta: &TableMeta,
    excluded_columns: &HashSet<String>,
) -> DdlChange {
    DdlChange::CreateTable {
        name: name.to_string(),
        columns: meta
            .columns
            .iter()
            .filter(|col| !excluded_columns.contains(&col.name))
            .map(|col| {
                (
                    col.name.clone(),
                    sql_type_for_meta_column(col, &meta.propagation_rules),
                )
            })
            .collect(),
        constraints: create_table_constraints_from_meta(meta),
        foreign_keys: single_column_foreign_keys_from_meta(meta, excluded_columns),
        composite_foreign_keys: meta
            .composite_foreign_keys
            .iter()
            .filter(|fk| {
                fk.child_columns
                    .iter()
                    .all(|column| !excluded_columns.contains(column))
            })
            .cloned()
            .collect(),
        composite_unique: meta
            .unique_constraints
            .iter()
            .filter(|columns| {
                columns
                    .iter()
                    .all(|column| !excluded_columns.contains(column))
            })
            .cloned()
            .collect(),
    }
}

pub(super) fn single_column_foreign_keys_from_ast(ct: &CreateTable) -> Vec<SingleColumnForeignKey> {
    ct.columns
        .iter()
        .filter_map(|column| {
            column
                .references
                .as_ref()
                .map(|reference| SingleColumnForeignKey {
                    child_column: column.name.clone(),
                    parent_table: reference.table.clone(),
                    parent_column: reference.column.clone(),
                })
        })
        .collect()
}

pub(super) fn single_column_foreign_keys_from_meta(
    meta: &TableMeta,
    excluded_columns: &HashSet<String>,
) -> Vec<SingleColumnForeignKey> {
    meta.columns
        .iter()
        .filter(|column| !excluded_columns.contains(&column.name))
        .filter_map(|column| {
            column
                .references
                .as_ref()
                .map(|reference| SingleColumnForeignKey {
                    child_column: column.name.clone(),
                    parent_table: reference.table.clone(),
                    parent_column: reference.column.clone(),
                })
        })
        .collect()
}

pub(super) fn full_snapshot_ddl(metas: &HashMap<String, TableMeta>) -> Vec<DdlChange> {
    let mut names = metas.keys().cloned().collect::<Vec<_>>();
    names.sort();

    let mut emitted = HashSet::new();
    let mut ddl = Vec::new();
    while emitted.len() < names.len() {
        let before = emitted.len();
        for name in &names {
            if emitted.contains(name) {
                continue;
            }
            let Some(meta) = metas.get(name) else {
                continue;
            };
            let deps_ready = meta
                .columns
                .iter()
                .filter_map(|column| column.rank_policy.as_ref())
                .all(|policy| {
                    policy.joined_table == *name
                        || !metas.contains_key(&policy.joined_table)
                        || emitted.contains(&policy.joined_table)
                })
                && meta.columns.iter().all(|column| {
                    column.references.as_ref().is_none_or(|reference| {
                        reference.table == *name
                            || !metas.contains_key(&reference.table)
                            || emitted.contains(&reference.table)
                    })
                })
                && meta.composite_foreign_keys.iter().all(|fk| {
                    fk.parent_table == *name
                        || !metas.contains_key(&fk.parent_table)
                        || emitted.contains(&fk.parent_table)
                });
            if deps_ready {
                push_snapshot_table_ddl(&mut ddl, name, meta);
                emitted.insert(name.clone());
            }
        }
        if emitted.len() == before {
            for name in &names {
                if !emitted.contains(name)
                    && let Some(meta) = metas.get(name)
                {
                    push_snapshot_table_ddl(&mut ddl, name, meta);
                    emitted.insert(name.clone());
                }
            }
        }
    }
    ddl
}

pub(super) fn push_snapshot_table_ddl(ddl: &mut Vec<DdlChange>, name: &str, meta: &TableMeta) {
    let deferred_self_rank_columns = meta
        .columns
        .iter()
        .filter(|column| {
            column
                .rank_policy
                .as_ref()
                .is_some_and(|policy| policy.joined_table == name)
        })
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    ddl.push(ddl_change_from_meta_excluding(
        name,
        meta,
        &deferred_self_rank_columns,
    ));
    for index in &meta.indexes {
        if index.kind == contextdb_core::IndexKind::UserDeclared {
            ddl.push(DdlChange::CreateIndex {
                table: name.to_string(),
                name: index.name.clone(),
                columns: index.columns.clone(),
            });
        }
    }
    if !deferred_self_rank_columns.is_empty() {
        ddl.push(DdlChange::AlterTable {
            name: name.to_string(),
            columns: meta
                .columns
                .iter()
                .map(|col| {
                    (
                        col.name.clone(),
                        sql_type_for_meta_column(col, &meta.propagation_rules),
                    )
                })
                .collect(),
            constraints: create_table_constraints_from_meta(meta),
            foreign_keys: single_column_foreign_keys_from_meta(meta, &HashSet::new()),
            composite_foreign_keys: meta.composite_foreign_keys.clone(),
            composite_unique: meta.unique_constraints.clone(),
        });
    }
}

pub(super) fn sql_type_for_ast(data_type: &DataType) -> String {
    match data_type {
        DataType::Uuid => "UUID".to_string(),
        DataType::Text => "TEXT".to_string(),
        DataType::Integer => "INTEGER".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Vector(dim) => format!("VECTOR({dim})"),
        DataType::TxId => "TXID".to_string(),
    }
}

/// The one place a column's `ACL REFERENCES` clause is written. Both column
/// renderers -- the parsed-statement one and the stored-metadata one -- call
/// it, so a table declared through SQL and the same table read back out of the
/// store print the same access control rather than two shapes that only agree
/// by accident.
pub(super) fn append_scope_label(ty: &mut String, read: Option<&[String]>, write: &[String]) {
    let labels = |values: &[String]| {
        values
            .iter()
            .map(|value| format!("'{}'", sql_quote(value)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if let Some(read) = read {
        ty.push_str(&format!(
            " SCOPE_LABEL_READ ({}) WRITE ({})",
            labels(read),
            labels(write)
        ));
    } else {
        ty.push_str(&format!(" SCOPE_LABEL ({})", labels(write)));
    }
}

pub(super) fn append_acl_reference(ty: &mut String, ref_table: &str, ref_column: &str) {
    ty.push_str(&format!(" ACL REFERENCES {ref_table}({ref_column})"));
}

pub(super) fn sql_type_for_ast_column(
    col: &contextdb_parser::ast::ColumnDef,
    _rules: &[contextdb_parser::ast::AstPropagationRule],
) -> String {
    let mut ty = sql_type_for_ast(&col.data_type);
    append_ast_quantization(&mut ty, col.quantization);
    append_ast_vector_declaration(&mut ty, col);
    if col.context_id {
        ty.push_str(" CONTEXT_ID");
    }
    if let Some(scope) = col.scope_label.as_deref() {
        match scope {
            contextdb_parser::ast::ScopeLabelConstraint::Simple { labels } => {
                append_scope_label(&mut ty, None, labels)
            }
            contextdb_parser::ast::ScopeLabelConstraint::Split { read, write } => {
                append_scope_label(&mut ty, Some(read), write)
            }
        }
    }
    if col.expires {
        ty.push_str(" EXPIRES");
    }

    if let Some(reference) = &col.references {
        ty.push_str(&format!(
            " REFERENCES {}({})",
            reference.table, reference.column
        ));
        for rule in &reference.propagation_rules {
            if let contextdb_parser::ast::AstPropagationRule::FkState {
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } = rule
            {
                ty.push_str(&format!(
                    " ON STATE {} PROPAGATE SET {}",
                    trigger_state, target_state
                ));
                if max_depth.unwrap_or(10) != 10 {
                    ty.push_str(&format!(" MAX DEPTH {}", max_depth.unwrap_or(10)));
                }
                if *abort_on_failure {
                    ty.push_str(" ABORT ON FAILURE");
                }
            }
        }
    }
    if let Some(acl) = &col.acl_ref {
        append_acl_reference(&mut ty, &acl.ref_table, &acl.ref_column);
    }
    if col.primary_key {
        ty.push_str(" PRIMARY KEY");
    }
    if !col.nullable && !col.primary_key {
        ty.push_str(" NOT NULL");
    }
    if let Some(default) = &col.default {
        ty.push_str(&format!(
            " DEFAULT {}",
            crate::executor::stored_default_expr(default)
        ));
    }
    if col.unique {
        ty.push_str(" UNIQUE");
    }
    if col.immutable {
        ty.push_str(" IMMUTABLE");
    }
    if let Some(policy) = col.rank_policy.as_deref() {
        ty.push_str(&format!(
            " RANK_POLICY (JOIN {} ON {}, FORMULA '{}', SORT_KEY {})",
            policy.joined_table,
            policy.joined_column,
            sql_quote(&policy.formula),
            policy.sort_key
        ));
    }
    ty
}

pub(crate) fn sql_type_for_meta_column(
    col: &contextdb_core::ColumnDef,
    rules: &[PropagationRule],
) -> String {
    render_meta_column(col, rules)
}

pub(super) fn render_meta_column(
    col: &contextdb_core::ColumnDef,
    rules: &[PropagationRule],
) -> String {
    let column = crate::cli_render::MetadataColumn(col, rules);
    let text = format_args!("{column}");
    let bytes =
        crate::cli_render::metadata_text_len(text).expect("column declaration fits a String");
    let mut output = String::with_capacity(bytes);
    std::fmt::write(&mut output, text).expect("String rendering");
    output
}

pub(super) fn append_ast_quantization(
    ty: &mut String,
    quantization: contextdb_parser::ast::VectorQuantization,
) {
    let quantization = match quantization {
        contextdb_parser::ast::VectorQuantization::F32 => return,
        contextdb_parser::ast::VectorQuantization::SQ8 => "SQ8",
        contextdb_parser::ast::VectorQuantization::SQ4 => "SQ4",
    };
    ty.push_str(&format!(" WITH (quantization = '{quantization}')"));
}

pub(super) fn append_ast_vector_declaration(
    ty: &mut String,
    col: &contextdb_parser::ast::ColumnDef,
) {
    if !matches!(col.data_type, DataType::Vector(_)) {
        return;
    }
    if !col.partition_key_columns.is_empty() {
        ty.push_str(&format!(
            " PARTITION_KEY ({})",
            col.partition_key_columns.join(", ")
        ));
        match col.max_partitions.as_deref() {
            Some(max_partitions) => {
                let max_partitions = max_partitions
                    .parse::<u32>()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|_| max_partitions.to_owned());
                ty.push_str(&format!(" MAX_PARTITIONS {max_partitions}"));
            }
            None => ty.push_str(&format!(" MAX_PARTITIONS {DEFAULT_VECTOR_MAX_PARTITIONS}")),
        }
    }
    match col
        .search_mode
        .unwrap_or(contextdb_parser::ast::VectorSearchMode::Auto)
    {
        contextdb_parser::ast::VectorSearchMode::Auto => {}
        contextdb_parser::ast::VectorSearchMode::Exact => ty.push_str(" SEARCH_MODE EXACT"),
        contextdb_parser::ast::VectorSearchMode::Indexed => ty.push_str(" SEARCH_MODE INDEXED"),
    }
    if let Some(auto_index_at) = col.auto_index_at.as_deref() {
        let auto_index_at = auto_index_at
            .parse::<u32>()
            .map(|value| value.to_string())
            .unwrap_or_else(|_| auto_index_at.to_owned());
        ty.push_str(&format!(" AUTO_INDEX_AT {auto_index_at}"));
    }
    if let Some(ceiling) = col.auto_expansion_ceiling.as_deref() {
        let ceiling = ceiling
            .parse::<u32>()
            .map(|value| value.to_string())
            .unwrap_or_else(|_| ceiling.to_owned());
        ty.push_str(&format!(" AUTO_EXPANSION_CEILING {ceiling}"));
    }
    if let Some(flat_below) = col.flat_below.as_deref() {
        let flat_below = flat_below
            .parse::<u32>()
            .map(|value| value.to_string())
            .unwrap_or_else(|_| flat_below.to_owned());
        ty.push_str(&format!(" FLAT_BELOW {flat_below}"));
    }
    append_ast_hnsw_declaration(ty, &col.hnsw);
    append_ast_consolidation_declaration(ty, &col.consolidation);
}

pub(super) fn append_ast_hnsw_declaration(
    ty: &mut String,
    policy: &contextdb_parser::ast::VectorHnswOptions,
) {
    let render =
        |name: &str, value: &Option<contextdb_parser::ast::VectorPolicyValue>| -> Option<String> {
            match value.as_ref()? {
                contextdb_parser::ast::VectorPolicyValue::Default => None,
                contextdb_parser::ast::VectorPolicyValue::Value(value) => {
                    let value = value
                        .parse::<u32>()
                        .map(|value| value.to_string())
                        .unwrap_or_else(|_| value.clone());
                    Some(format!("{name} = {value}"))
                }
            }
        };
    let members = [
        render("M", &policy.m),
        render("EF_CONSTRUCTION", &policy.ef_construction),
        render("EF_SEARCH", &policy.ef_search),
        match policy.search_radius.as_ref() {
            Some(contextdb_parser::ast::VectorPolicyValue::Value(value)) => Some(format!(
                "SEARCH_RADIUS = {}",
                contextdb_core::parse_search_radius_millionths(value)
                    .map(contextdb_core::render_search_radius_millionths)
                    .unwrap_or_else(|| value.clone())
            )),
            _ => None,
        },
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    if !members.is_empty() {
        ty.push_str(&format!(" HNSW ({})", members.join(", ")));
    }
}

pub(super) fn append_ast_consolidation_declaration(
    ty: &mut String,
    policy: &contextdb_parser::ast::VectorConsolidationOptions,
) {
    if policy.disabled {
        ty.push_str(" CONSOLIDATION NONE");
        return;
    }
    let render =
        |name: &str, value: &Option<contextdb_parser::ast::VectorPolicyValue>| -> Option<String> {
            match value.as_ref()? {
                contextdb_parser::ast::VectorPolicyValue::Default => None,
                contextdb_parser::ast::VectorPolicyValue::Value(value) => {
                    Some(format!("{name} = {value}"))
                }
            }
        };
    let members = [
        render("CHANGE_PERCENT", &policy.change_percent),
        render("TOMBSTONE_PERCENT", &policy.tombstone_percent),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    if !members.is_empty() {
        ty.push_str(&format!(" CONSOLIDATION ({})", members.join(", ")));
    }
}

pub(super) fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn normalize_schema_type(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn sync_create_table_sql(
    name: &str,
    columns: &[(String, String)],
    constraints: &[String],
    foreign_keys: &[SingleColumnForeignKey],
    composite_foreign_keys: &[CompositeForeignKey],
    composite_unique: &[Vec<String>],
) -> String {
    let mut table_elements = columns
        .iter()
        .map(|(column, ty)| {
            format!(
                "{column} {}",
                sync_column_type_with_foreign_key(column, ty, foreign_keys)
            )
        })
        .collect::<Vec<_>>();
    let mut table_options = Vec::new();
    let mut seen_table_elements = table_elements
        .iter()
        .map(|element| schema_clause_key(element))
        .collect::<HashSet<_>>();

    for unique in composite_unique {
        let element = format!("UNIQUE ({})", unique.join(", "));
        if seen_table_elements.insert(schema_clause_key(&element)) {
            table_elements.push(element);
        }
    }
    for fk in composite_foreign_keys {
        let element = format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        );
        if seen_table_elements.insert(schema_clause_key(&element)) {
            table_elements.push(element);
        }
    }
    for constraint in constraints {
        if sync_constraint_is_vector_rename(constraint)
            || sync_constraint_is_column_rename(constraint)
            || sync_constraint_is_column_drop(constraint)
        {
            continue;
        }
        if sync_constraint_is_table_element(constraint) {
            if seen_table_elements.insert(schema_clause_key(constraint)) {
                table_elements.push(constraint.clone());
            }
        } else {
            table_options.push(constraint.clone());
        }
    }

    let mut sql = format!("CREATE TABLE {} ({})", name, table_elements.join(", "));
    if !table_options.is_empty() {
        sql.push(' ');
        sql.push_str(&table_options.join(" "));
    }
    sql
}

pub(super) fn alter_table_ddl_change(
    name: &str,
    meta: &TableMeta,
    extra_constraints: Vec<String>,
) -> DdlChange {
    let mut constraints = create_table_constraints_from_meta(meta);
    constraints.extend(extra_constraints);
    DdlChange::AlterTable {
        name: name.to_string(),
        columns: meta
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    sql_type_for_meta_column(c, &meta.propagation_rules),
                )
            })
            .collect(),
        constraints,
        foreign_keys: single_column_foreign_keys_from_meta(meta, &HashSet::new()),
        composite_foreign_keys: meta.composite_foreign_keys.clone(),
        composite_unique: meta.unique_constraints.clone(),
    }
}

pub(super) fn sync_column_type_with_foreign_key(
    column: &str,
    ty: &str,
    foreign_keys: &[SingleColumnForeignKey],
) -> String {
    if normalize_schema_type(ty)
        .to_ascii_uppercase()
        .contains(" REFERENCES ")
    {
        return ty.to_string();
    }
    let Some(fk) = foreign_keys.iter().find(|fk| fk.child_column == column) else {
        return ty.to_string();
    };
    format!(
        "{} REFERENCES {}({})",
        ty, fk.parent_table, fk.parent_column
    )
}

pub(super) fn sync_constraint_is_table_element(constraint: &str) -> bool {
    let upper = normalize_schema_type(constraint).to_ascii_uppercase();
    upper.starts_with("UNIQUE")
        || upper.starts_with("FOREIGN KEY")
        || upper.starts_with("PRIMARY KEY")
}

pub(super) fn sync_constraint_is_vector_rename(constraint: &str) -> bool {
    constraint
        .trim()
        .to_ascii_uppercase()
        .starts_with("VECTOR_RENAME(")
}

pub(super) fn schema_clause_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(|ch| ch.to_uppercase())
        .collect()
}

pub(super) fn ddl_dag_edge_types(constraints: &[String]) -> Vec<EdgeType> {
    let mut edge_types = Vec::new();
    for constraint in constraints {
        let upper = constraint.to_ascii_uppercase();
        let Some(dag_start) = upper.find("DAG") else {
            continue;
        };
        let Some(paren_offset) = upper[dag_start..].find('(') else {
            continue;
        };
        let values_start = dag_start + paren_offset + 1;
        let Some(end_offset) = upper[values_start..].find(')') else {
            continue;
        };
        let values_end = values_start + end_offset;
        for raw in constraint[values_start..values_end].split(',') {
            let edge_type = raw
                .trim()
                .trim_matches(|ch: char| ch == '\'' || ch == '"' || ch.is_whitespace());
            if !edge_type.is_empty() {
                edge_types.push(edge_type.to_string());
            }
        }
    }

    edge_types
}

pub(super) fn ddl_unique_constraints(constraints: &[String]) -> Vec<Vec<String>> {
    constraints
        .iter()
        .filter_map(|constraint| {
            let upper = normalize_schema_type(constraint).to_ascii_uppercase();
            if !upper.starts_with("UNIQUE") {
                return None;
            }
            let start = constraint.find('(')? + 1;
            let end = constraint[start..].find(')')? + start;
            let columns = constraint[start..end]
                .split(',')
                .map(|column| column.trim().to_string())
                .filter(|column| !column.is_empty())
                .collect::<Vec<_>>();
            (!columns.is_empty()).then_some(columns)
        })
        .collect()
}

/// The columns of a table-level `PRIMARY KEY (a, b, ...)` carried in an
/// arriving DDL's constraint strings — the receiver's reconstruction of the
/// multi-column sync identity. A single-column primary key rides its column's
/// type token, not a table-level constraint, so it is not seen here.
pub(super) fn ddl_primary_key_columns(constraints: &[String]) -> Vec<String> {
    for constraint in constraints {
        let upper = normalize_schema_type(constraint).to_ascii_uppercase();
        if !upper.starts_with("PRIMARY KEY") {
            continue;
        }
        let Some(start) = constraint.find('(') else {
            continue;
        };
        let Some(end_offset) = constraint[start + 1..].find(')') else {
            continue;
        };
        let end = start + 1 + end_offset;
        let columns = constraint[start + 1..end]
            .split(',')
            .map(|column| column.trim().to_string())
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if !columns.is_empty() {
            return columns;
        }
    }
    Vec::new()
}

pub(crate) fn sync_vector_rename_constraint(from: &str, to: &str) -> String {
    format!("VECTOR_RENAME({from},{to})")
}

/// Inspect only explicit signed operations; omission is never a column drop.
/// This mirrors the existing marker spelling rules using borrowed slices.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn refuse_dag_defining_column_sync_ddl(
    projected: &HashMap<String, TableMeta>,
    change: &DdlChange,
) -> Result<()> {
    let DdlChange::AlterTable {
        name, constraints, ..
    } = change
    else {
        return Ok(());
    };
    let Some(meta) = projected
        .get(name)
        .filter(|meta| !meta.dag_edge_types.is_empty())
    else {
        return Ok(());
    };
    for constraint in constraints {
        let trimmed = constraint.trim();
        for prefix in ["COLUMN_DROP(", "COLUMN_RENAME(", "VECTOR_RENAME("] {
            if !trimmed
                .get(..prefix.len())
                .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
            {
                continue;
            }
            let Some(body) = trimmed
                .get(prefix.len()..)
                .and_then(|body| body.strip_suffix(')'))
            else {
                continue;
            };
            let source = if prefix == "COLUMN_DROP(" {
                body
            } else {
                let Some((from, to)) = body.split_once(',') else {
                    continue;
                };
                if to
                    .trim()
                    .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
                    .is_empty()
                {
                    continue;
                }
                from
            };
            let source = source
                .trim()
                .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`');
            crate::executor::refuse_dag_defining_column_alter(meta, source)?;
        }
    }
    Ok(())
}

/// The author's own evidence that one non-vector column was renamed, carried
/// in the signed ALTER's constraints beside the full post-edit shape. A
/// receiver replays authenticated DDL exactly as authored, so it renames a
/// non-vector column
/// only when this marker names it, and never guesses a rename from the shape.
pub(crate) fn sync_column_rename_constraint(from: &str, to: &str) -> String {
    format!("COLUMN_RENAME({from},{to})")
}

pub(super) fn sync_constraint_is_column_rename(constraint: &str) -> bool {
    constraint
        .trim()
        .to_ascii_uppercase()
        .starts_with("COLUMN_RENAME(")
}

/// Every authored non-vector column rename a received ALTER declares.
pub(crate) fn sync_column_renames_from_constraints(
    constraints: &[String],
) -> Vec<(String, String)> {
    const PREFIX: &str = "COLUMN_RENAME(";
    let mut renames = Vec::new();
    for constraint in constraints {
        let trimmed = constraint.trim();
        if !trimmed.to_ascii_uppercase().starts_with(PREFIX) || !trimmed.ends_with(')') {
            continue;
        }
        let body = &trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)];
        let Some((from, to)) = body.split_once(',') else {
            continue;
        };
        let from = from
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        let to = to
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        if !from.is_empty() && !to.is_empty() {
            renames.push((from, to));
        }
    }
    renames
}

/// The author's own evidence that one column was dropped, carried in the
/// signed ALTER's constraints beside the post-edit shape. A receiver removes a
/// column -- and its stored values, vectors and index -- only when this marker
/// names it. A column the ALTER merely leaves out is kept: an omission is
/// never read as a drop, with or without the primary key restated.
pub(crate) fn sync_column_drop_constraint(column: &str) -> String {
    format!("COLUMN_DROP({column})")
}

pub(super) fn sync_constraint_is_column_drop(constraint: &str) -> bool {
    constraint
        .trim()
        .to_ascii_uppercase()
        .starts_with("COLUMN_DROP(")
}

/// Every column a received ALTER's author explicitly dropped.
pub(crate) fn sync_column_drops_from_constraints(constraints: &[String]) -> HashSet<String> {
    const PREFIX: &str = "COLUMN_DROP(";
    constraints
        .iter()
        .filter_map(|constraint| {
            let trimmed = constraint.trim();
            if !trimmed.to_ascii_uppercase().starts_with(PREFIX) || !trimmed.ends_with(')') {
                return None;
            }
            let column = trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)]
                .trim()
                .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`');
            (!column.is_empty()).then(|| column.to_string())
        })
        .collect()
}

/// The rename marker the author attaches to the ALTER a `RENAME COLUMN`
/// emits: the vector marker for a vector column, the column marker otherwise.
pub(crate) fn sync_rename_marker_for_column(
    meta: &TableMeta,
    from: &str,
    to: &str,
) -> Option<String> {
    let column = meta.columns.iter().find(|column| column.name == from)?;
    Some(if matches!(column.column_type, ColumnType::Vector(_)) {
        sync_vector_rename_constraint(from, to)
    } else {
        sync_column_rename_constraint(from, to)
    })
}

pub(crate) fn sync_vector_rename_from_constraints(
    constraints: &[String],
) -> Option<(String, String)> {
    const PREFIX: &str = "VECTOR_RENAME(";
    for constraint in constraints {
        let trimmed = constraint.trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with(PREFIX) || !trimmed.ends_with(')') {
            continue;
        }
        let body = &trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)];
        let (from, to) = body.split_once(',')?;
        let from = from
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        let to = to
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        if !from.is_empty() && !to.is_empty() {
            return Some((from, to));
        }
    }
    None
}

/// The local and arriving column lists, each normalized the same way.
pub(super) fn sync_table_column_lists(
    local_meta: &TableMeta,
    columns: &[(String, String)],
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let local_cols = local_meta
        .columns
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                normalize_schema_type(&sql_type_for_meta_column(c, &local_meta.propagation_rules)),
            )
        })
        .collect();
    let remote_cols = columns
        .iter()
        .map(|(col_name, col_type)| (col_name.clone(), normalize_schema_type(col_type)))
        .collect();
    (local_cols, remote_cols)
}

/// Whether an arriving CREATE declares the same columns, in any order, as the
/// table this node already holds under that name.
pub(super) fn sync_table_columns_match(
    local_meta: &TableMeta,
    columns: &[(String, String)],
) -> bool {
    let (mut local, mut remote) = sync_table_column_lists(local_meta, columns);
    local.sort();
    remote.sort();
    local == remote
}

pub(super) fn sync_table_shape_matches(
    local_meta: &TableMeta,
    remote_columns: &[(String, String)],
    remote_constraints: &[String],
    remote_foreign_keys: &[SingleColumnForeignKey],
    remote_composite_foreign_keys: &[CompositeForeignKey],
    remote_composite_unique: &[Vec<String>],
) -> bool {
    let remote_meta = rough_sync_table_meta(
        remote_columns,
        remote_constraints,
        remote_foreign_keys,
        remote_composite_foreign_keys,
        remote_composite_unique,
    );

    descriptor_multiset(single_column_foreign_keys_from_meta(
        local_meta,
        &HashSet::new(),
    )) == descriptor_multiset(single_column_foreign_keys_from_meta(
        &remote_meta,
        &HashSet::new(),
    )) && descriptor_multiset(local_meta.composite_foreign_keys.clone())
        == descriptor_multiset(remote_meta.composite_foreign_keys)
        && descriptor_multiset(local_meta.unique_constraints.clone())
            == descriptor_multiset(remote_meta.unique_constraints)
}

pub(super) fn descriptor_multiset<T>(mut values: Vec<T>) -> Vec<T>
where
    T: Ord,
{
    values.sort();
    values
}

pub(super) fn merge_sync_alter_existing_column(current: &mut ColumnDef, incoming: ColumnDef) {
    if matches!(
        (&current.column_type, &incoming.column_type),
        (ColumnType::Vector(_), ColumnType::Vector(_))
    ) {
        // A synced ALTER carries that moment's full column declaration, so an
        // absent policy member means "no longer declared" and clears the
        // receiver's override, exactly as `SET ... DEFAULT` does locally.
        let previous_topology = (current.hnsw_m, current.hnsw_ef_construction);
        current.partition_key_columns = incoming.partition_key_columns.clone();
        current.max_partitions = incoming.max_partitions;
        current.search_mode = incoming.search_mode;
        current.auto_index_at = incoming.auto_index_at;
        current.flat_below = incoming.flat_below;
        current.auto_expansion_ceiling = incoming.auto_expansion_ceiling;
        current.hnsw_search_radius_millionths = incoming.hnsw_search_radius_millionths;
        current.hnsw_m = incoming.hnsw_m;
        current.hnsw_ef_construction = incoming.hnsw_ef_construction;
        current.hnsw_ef_search = incoming.hnsw_ef_search;
        current.consolidation_change_percent = incoming.consolidation_change_percent;
        current.consolidation_tombstone_percent = incoming.consolidation_tombstone_percent;
        current.consolidation_disabled = incoming.consolidation_disabled;
        // The receiver owns its own revision counter: the wire carries
        // rendered SQL, not a revision, so `incoming.vector_policy_revision`
        // is always the default and is never copied. Advance on exactly the
        // axes the local ALTER door advances on, which schedules the
        // receiver's own bounded replacement build.
        if previous_topology != (current.hnsw_m, current.hnsw_ef_construction)
            && let Some(next) =
                crate::executor::next_vector_policy_revision(current.vector_policy_revision)
        {
            current.vector_policy_revision = next;
        }
    }
    if incoming.primary_key {
        current.primary_key = true;
        current.nullable = false;
    }
    if incoming.unique {
        current.unique = true;
    }
    if !incoming.nullable {
        current.nullable = false;
    }
    if incoming.expires {
        current.expires = true;
    }
    if incoming.immutable {
        current.immutable = true;
    }
    if incoming.context_id {
        current.context_id = true;
    }
    if incoming.references.is_some() {
        current.references = incoming.references;
    }
    if incoming.rank_policy.is_some() {
        current.rank_policy = incoming.rank_policy;
    }
}

/// The direction a synced DDL constraint declares, if it is a direction
/// clause at all. Matched on the rendered constraint text because that is the
/// shape table DDL travels in; the local doors read the same words off the
/// parsed statement.
pub(crate) fn constraint_declares_sync_direction(constraint: &str) -> Option<SyncDirection> {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    [
        SyncDirection::None,
        SyncDirection::Push,
        SyncDirection::Pull,
        SyncDirection::Both,
    ]
    .into_iter()
    .find(|direction| normalized == direction.sql())
}

/// The conflict policy a synced DDL constraint declares — so the receiving
/// machine reconstructs the SAME declaration the writer made rather than a
/// defaulted one, exactly as it does for the direction clause.
pub(super) fn constraint_declares_conflict_policy(
    constraint: &str,
) -> Option<contextdb_core::ConflictPolicy> {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    [
        contextdb_core::ConflictPolicy::KEEP_FIRST,
        contextdb_core::ConflictPolicy::KEEP_LATEST,
    ]
    .into_iter()
    .find(|policy| policy.declared_clause() == Some(normalized.as_str()))
}

/// The version-history policy a synced DDL constraint declares — so the
/// receiving machine reconstructs the SAME declaration the writer made
/// rather than a defaulted one, mirroring `constraint_declares_conflict_policy`.
pub(super) fn constraint_declares_history_policy(constraint: &str) -> Option<HistoryPolicy> {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    [HistoryPolicy::All, HistoryPolicy::CurrentOnly]
        .into_iter()
        .find(|policy| policy.declared_clause() == normalized.as_str())
}

/// The retention window a synced DDL constraint declares — seconds plus the
/// unit it was written in — so the receiving machine reconstructs the SAME
/// declaration the writer made rather than a defaulted one.
pub(super) fn constraint_declares_retention(constraint: &str) -> Option<(u64, RetainUnit)> {
    let upper = constraint.to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    if words.next() != Some("RETAIN") {
        return None;
    }
    let amount = words.next()?.parse::<u64>().ok()?;
    let unit = match words.next()? {
        "SECONDS" | "SECOND" => RetainUnit::Seconds,
        "MINUTES" | "MINUTE" => RetainUnit::Minutes,
        "HOURS" | "HOUR" => RetainUnit::Hours,
        "DAYS" | "DAY" => RetainUnit::Days,
        _ => return None,
    };
    Some((amount.saturating_mul(unit.seconds_multiplier()), unit))
}

/// Whether a synced DDL constraint declares the `SYNC SAFE` delivery promise.
pub(super) fn constraint_declares_sync_safe(constraint: &str) -> bool {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.starts_with("RETAIN ") && normalized.contains("SYNC SAFE")
}

/// Refuse arriving DDL that declares `SYNC SAFE` on a table with no key to
/// deliver rows by, on BOTH the CreateTable and AlterTable paths. Without this
/// a foreign edge could install locally the exact declaration every local door
/// rejects, and this engine would then delete undelivered rows on its behalf.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn refuse_keyless_sync_safe_sync_ddl(
    projected: &HashMap<String, TableMeta>,
    change: &DdlChange,
) -> Result<()> {
    let (name, meta) = match change {
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } => {
            if !constraints.iter().any(|c| constraint_declares_sync_safe(c)) {
                return Ok(());
            }
            (
                name,
                rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ),
            )
        }
        DdlChange::AlterTable {
            name, constraints, ..
        } => {
            if !constraints.iter().any(|c| constraint_declares_sync_safe(c)) {
                return Ok(());
            }
            match projected.get(name) {
                Some(meta) => (name, meta.clone()),
                None => return Ok(()),
            }
        }
        _ => return Ok(()),
    };
    // The arriving declaration promises delivery, so apply the same key
    // requirement the local CREATE and ALTER paths apply.
    let mut meta = meta;
    meta.sync_safe = true;
    crate::executor::refuse_sync_safe_without_key_for(name, &meta)
}

/// Refuse arriving table DDL whose delivery promise has nowhere to deliver, on
/// BOTH the CreateTable and AlterTable arrival paths — exactly as the local
/// CREATE and ALTER doors refuse it. Returns before any projection or apply
/// work, so a refused statement leaves existing metadata untouched. Without
/// this a peer could install here the declaration every local door rejects,
/// and this engine would hold rows that never expire on its behalf.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn refuse_undeliverable_promise_sync_ddl(
    projected: &HashMap<String, TableMeta>,
    change: &DdlChange,
) -> Result<()> {
    let (name, constraints) = match change {
        DdlChange::CreateTable {
            name, constraints, ..
        }
        | DdlChange::AlterTable {
            name, constraints, ..
        } => (name, constraints),
        _ => return Ok(()),
    };
    let arriving_direction = constraints
        .iter()
        .find_map(|constraint| constraint_declares_sync_direction(constraint));
    // An ALTER that names no direction leaves the one the table already has.
    let direction = arriving_direction.or_else(|| {
        matches!(change, DdlChange::AlterTable { .. })
            .then(|| {
                projected
                    .get(name)
                    .map(crate::executor::effective_sync_direction)
            })
            .flatten()
    });
    let sync_safe = constraints.iter().any(|c| constraint_declares_sync_safe(c))
        || (matches!(change, DdlChange::AlterTable { .. })
            && projected.get(name).is_some_and(|meta| meta.sync_safe));
    crate::executor::refuse_promise_with_no_delivery(name, sync_safe, direction)
}

/// Refuse arriving table DDL declaring `HISTORY CURRENT ONLY` under keep-first
/// delivery, on BOTH the CreateTable (fresh or adopting an existing table)
/// and AlterTable arrival paths — exactly as the local CREATE and ALTER
/// doors refuse it. `alter_table_ddl_change` always carries a table's FULL
/// current declared constraint set on every emitted AlterTable (not only
/// what one ALTER statement changed — see its own doc comment), so
/// `rough_sync_table_meta`'s reconstruction from `constraints` already IS the
/// post-change shape for both a fresh CreateTable and an arriving AlterTable;
/// no separate adopt-branch projection is needed here. Returns before any
/// projection or apply work, so a refused statement leaves existing metadata
/// untouched — matching `refuse_undeliverable_promise_sync_ddl` and
/// `refuse_keyless_sync_safe_sync_ddl`, which this sits alongside.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn refuse_reclaimed_history_sync_ddl(change: &DdlChange) -> Result<()> {
    let (name, incoming) = match change {
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        }
        | DdlChange::AlterTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } => (
            name,
            rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ),
        ),
        _ => return Ok(()),
    };
    crate::executor::refuse_reclaimed_history_under_keep_first(name, &incoming)
}

/// Refuse arriving DDL that would move one of the four engine-owned built-in
/// ledger tables ([`crate::executor::ENGINE_OWNED_LEDGER_TABLES`]) away from
/// its declared policy -- the arriving-DDL mirror of
/// [`crate::executor::refuse_engine_owned_policy_mutation`], guarding the
/// same four tables against a peer that produced the mutation the local
/// door refuses (a sender without these guards, or a hand-crafted changeset;
/// under a fixed engine no local commit can reach this door with an
/// off-canonical value at all). Judges THREE shapes, all of which land on the
/// SAME `TableMeta` policy fields at apply (`database.rs`'s `AlterTable` arm
/// and its `CreateTable`-ADOPT arm for a table that already exists in
/// `projected` -- see [`crate::executor::engine_owned_merged_policy`]):
///
/// - An `AlterTable` against one of the four names.
/// - A `CreateTable` naming one of the four when `projected` already carries
///   that table -- i.e. it will go through the adopt branch, not a fresh
///   create.
/// - A `CreateTable` naming one of the four when `projected` does NOT yet
///   carry that table -- i.e. a fresh create. This guards against a peer
///   that issues DROP + CREATE to circumvent the adopting-existing-table
///   check and land an off-policy shape under a reserved name.
///
/// Only refuses on an axis the arriving DDL EXPLICITLY declares a
/// non-canonical value for -- delegated to
/// [`crate::executor::refuse_engine_owned_policy_axes`], the same axis check
/// all three shapes ask. `alter_table_ddl_change` bakes a table's FULL
/// currently-declared shape into every emitted AlterTable (see
/// `refuse_reclaimed_history_sync_ddl`'s doc comment), and the adopt branch's
/// own arriving `CreateTable` carries the writer's full declared shape the
/// same way -- but a table's own multi-step self-heal
/// (`install_work_ledger_schema`'s work_capabilities reconcile sets SYNC
/// CONFLICT, then HISTORY, as two separate committed ALTERs) can commit --
/// and so sync -- a snapshot where one axis is already healed and the other
/// has not been reached yet. That in-progress snapshot must sync on, not be
/// refused, so a receiving edge can run its OWN installer to finish healing
/// it; an axis the arriving DDL is simply silent on is never judged here --
/// the residual silent case is what
/// [`crate::executor::engine_owned_merged_policy`] preserves at apply,
/// rather than clearing it to the incoming shape's absence.
///
/// For fresh CreateTable of a reserved name, ADDITIONALLY checks the column
/// shape via [`crate::executor::refuse_engine_owned_reserved_name_shape_wire`],
/// mirroring the local CREATE TABLE door's column-shape guard for reserved
/// names.
///
/// All three shapes ALSO run
/// [`crate::executor::refuse_hub_refereed_ledger_sync_conflict_mismatch`]
/// after the axis check above -- the narrower, SYNC-CONFLICT-only
/// counterpart for the five hub-refereed work-ledger tables outside
/// [`crate::executor::ENGINE_OWNED_LEDGER_TABLES`] (`work_jobs` /
/// `work_claims` / `work_results` / `work_failures` / `work_cancellations`),
/// which `refuse_engine_owned_policy_axes` does not judge at all (see that
/// function's own doc comment for why they need a separate door rather than
/// a widened four-table list). The fresh-CreateTable arm's guard is widened
/// to match either table set so a reserved-name column-shape and SYNC-
/// CONFLICT check both run for these five too, not only the four.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn refuse_engine_owned_policy_sync_ddl(
    projected: &HashMap<String, TableMeta>,
    change: &DdlChange,
) -> Result<()> {
    match change {
        DdlChange::AlterTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } => {
            // The column-shape guard, mirroring the local ALTER door
            // (`refuse_engine_owned_reserved_name_column_alter`): an
            // arriving AlterTable naming one of the nine reserved names
            // must restate their full canonical column shape.
            // `alter_table_ddl_change` always bakes a table's FULL current
            // shape into every emitted AlterTable (see
            // `refuse_reclaimed_history_sync_ddl`'s doc comment), so this is
            // not a narrower ask than the fresh-CreateTable door makes of a
            // fresh create. Judged BEFORE the policy axes below, and before
            // `merge_sync_alter_existing_column` at apply time can latch a
            // hidden attribute (UNIQUE / IMMUTABLE / ...) onto an
            // already-installed reserved table's column.
            crate::executor::refuse_engine_owned_reserved_name_shape_wire(
                name,
                columns,
                constraints,
                foreign_keys,
                composite_unique,
                composite_foreign_keys,
            )?;
            let incoming = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            crate::executor::refuse_engine_owned_policy_axes(name, &incoming)?;
            // The narrower SYNC-CONFLICT-only counterpart, for the five
            // hub-refereed work-ledger tables the full-axis door above does
            // not cover (see that function's own doc comment).
            crate::executor::refuse_hub_refereed_ledger_sync_conflict_mismatch(
                name,
                incoming.conflict_policy,
            )
        }
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } if projected.contains_key(name) => {
            // Same column-shape guard as the AlterTable arm above: a
            // CreateTable ADOPTING an already-existing reserved table lands
            // on the identical merge path a peer's own multi-step reconcile
            // uses, so it must be held to the same canonical column shape.
            crate::executor::refuse_engine_owned_reserved_name_shape_wire(
                name,
                columns,
                constraints,
                foreign_keys,
                composite_unique,
                composite_foreign_keys,
            )?;
            let incoming = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            crate::executor::refuse_engine_owned_policy_axes(name, &incoming)?;
            crate::executor::refuse_hub_refereed_ledger_sync_conflict_mismatch(
                name,
                incoming.conflict_policy,
            )
        }
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } if crate::executor::ENGINE_OWNED_LEDGER_TABLES.contains(&name.as_str())
            || crate::executor::is_hub_refereed_sync_conflict_table(name) =>
        {
            // Fresh create of a reserved table (not in projected) -- guard both
            // the column shape and the policy axes, exactly like the local
            // CREATE TABLE door does.
            crate::executor::refuse_engine_owned_reserved_name_shape_wire(
                name,
                columns,
                constraints,
                foreign_keys,
                composite_unique,
                composite_foreign_keys,
            )?;
            let incoming = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            crate::executor::refuse_engine_owned_policy_axes(name, &incoming)?;
            crate::executor::refuse_hub_refereed_ledger_sync_conflict_mismatch(
                name,
                incoming.conflict_policy,
            )
        }
        _ => Ok(()),
    }
}

/// Read an arriving `MAINTAIN COUNT ...` constraint with the REAL grammar,
/// never a substring match — the same reconstruct-then-parse
/// `rough_sync_column_def` uses, and for the same reason: a token inside a
/// block comment is invisible to the parser and must be invisible here too.
pub(super) fn constraint_declares_maintained_counts(
    constraint: &str,
) -> Option<contextdb_core::MaintainedCounts> {
    let sql = format!("CREATE TABLE __rough_sync_maintained_counts (__probe TEXT) {constraint}");
    let Ok(Statement::CreateTable(table)) = contextdb_parser::parse(&sql) else {
        return None;
    };
    table.maintained_counts.as_ref().and_then(|declared| {
        contextdb_planner::planner::map_maintained_counts_declaration(declared).ok()
    })
}

pub(crate) fn rough_sync_table_meta(
    columns: &[(String, String)],
    constraints: &[String],
    foreign_keys: &[SingleColumnForeignKey],
    composite_foreign_keys: &[CompositeForeignKey],
    composite_unique: &[Vec<String>],
) -> TableMeta {
    let mut column_defs = columns
        .iter()
        .map(|(name, ty)| rough_sync_column_def(name, ty))
        .collect::<Vec<_>>();
    let primary_key_columns = ddl_primary_key_columns(constraints);
    for column in &mut column_defs {
        if primary_key_columns.contains(&column.name) {
            column.nullable = false;
        }
    }
    for fk in foreign_keys {
        if let Some(column) = column_defs
            .iter_mut()
            .find(|column| column.name == fk.child_column)
        {
            column.references = Some(ForeignKeyReference {
                table: fk.parent_table.clone(),
                column: fk.parent_column.clone(),
            });
        }
    }
    let mut unique_constraints = ddl_unique_constraints(constraints);
    for unique in composite_unique {
        if !unique_constraints
            .iter()
            .any(|candidate| candidate == unique)
        {
            unique_constraints.push(unique.clone());
        }
    }

    let retention = constraints
        .iter()
        .find_map(|constraint| constraint_declares_retention(constraint));
    let maintained_counts = constraints
        .iter()
        .find_map(|constraint| constraint_declares_maintained_counts(constraint));

    let mut meta = TableMeta {
        columns: column_defs,
        immutable: constraints
            .iter()
            .any(|constraint| constraint.eq_ignore_ascii_case("IMMUTABLE")),
        state_machine: None,
        dag_edge_types: ddl_dag_edge_types(constraints),
        unique_constraints,
        natural_key_column: None,
        propagation_rules: Vec::new(),
        default_ttl_seconds: retention.map(|(seconds, _)| seconds),
        sync_safe: constraints.iter().any(|c| constraint_declares_sync_safe(c)),
        expires_column: None,
        indexes: Vec::new(),
        composite_foreign_keys: composite_foreign_keys.to_vec(),
        sync_direction: constraints
            .iter()
            .find_map(|constraint| constraint_declares_sync_direction(constraint)),
        retain_declared_unit: retention.map(|(_, unit)| unit),
        primary_key_columns,
        conflict_policy: constraints
            .iter()
            .find_map(|constraint| constraint_declares_conflict_policy(constraint)),
        history_policy: constraints
            .iter()
            .find_map(|constraint| constraint_declares_history_policy(constraint)),
        delivery_manifest_tables: None,
        edge_discard: None,
        maintained_counts,
    };
    // Decode policy clauses with the same grammar as DECLARE.
    for constraint in constraints {
        if let Ok(contextdb_parser::Statement::DeclareTenantTablePolicy(p)) =
            contextdb_parser::parse(&format!(
                "DECLARE TENANT TABLE POLICY __incoming_policy {constraint}"
            ))
        {
            if p.sync_direction.is_some() {
                meta.sync_direction = p.sync_direction;
            }
            if p.conflict_policy.is_some() {
                meta.conflict_policy = p.conflict_policy;
            }
            meta.immutable |= p.immutable;
            if let Some(r) = p.retain {
                meta.default_ttl_seconds = Some(r.duration_seconds);
                meta.retain_declared_unit = Some(r.declared_unit);
                meta.sync_safe = r.sync_safe;
            }
            if p.history.is_some() {
                meta.history_policy = p.history;
            }
            if p.delivery_manifest_tables.is_some() {
                meta.delivery_manifest_tables = p.delivery_manifest_tables;
            }
            if p.edge_discard.is_some() {
                meta.edge_discard = p.edge_discard;
            }
        }
    }
    meta.indexes = crate::executor::auto_indexes_for_table_meta(&meta);
    meta
}

/// Builds the applied `ColumnDef` for one arriving wire `(name, type-text)`
/// pair by running the type text through the REAL parser -- the same
/// reconstruct-then-parse the shape door
/// (`crate::executor::refuse_engine_owned_reserved_name_shape_wire`) already
/// uses to JUDGE an arriving column, now reused to APPLY it too, so the two
/// paths can never disagree about what a wire column text means. A later
/// review found: an earlier implementation read `PRIMARY KEY` / `UNIQUE` /
/// `EXPIRES` / `IMMUTABLE` / `REFERENCES` / `CONTEXT_ID` off the RAW string
/// via `.contains(...)`, so a token sitting inside a block comment (which
/// the grammar's own `WHITESPACE` rule treats as skippable, exactly like a
/// space -- e.g. `TEXT /*UNIQUE*/`) was invisible to the real parser but
/// still matched the substring check, silently latching an attribute the
/// arriving DDL never actually declared. Applies to any table, reserved or
/// not.
pub(crate) fn rough_sync_column_def(name: &str, ty: &str) -> ColumnDef {
    let sql = format!("CREATE TABLE __rough_sync_column_shape ({name} {ty})");
    if let Ok(Statement::CreateTable(table)) = contextdb_parser::parse(&sql)
        && let Some(ast_column) = table.columns.into_iter().next()
    {
        let rank_policy = ast_column
            .rank_policy
            .as_deref()
            .map(crate::executor::map_rank_policy);
        if let Ok(column) = crate::executor::core_column_from_ast(
            "__rough_sync_column_shape",
            &ast_column,
            rank_policy,
        ) {
            return column;
        }
    }
    // Defensive fallback only: by the time real apply reaches this
    // function, `ty` has already parsed successfully as part of the FULL
    // CREATE/ALTER TABLE statement in the DDL preflight
    // (`validate_sync_table_shape_ddl` / `validate_sync_alter_table_shape_ddl`),
    // so this arm should not be reachable for real traffic -- it exists
    // only for callers that run before that preflight (e.g. byte-size
    // estimation). Never guess an attribute a comment or odd spacing could
    // smuggle in: every flag below defaults OFF, the honest reading of "no
    // real token seen."
    ColumnDef {
        name: name.to_string(),
        column_type: ColumnType::Text,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        references: None,
        expires: false,
        immutable: false,
        quantization: VectorQuantization::F32,
        rank_policy: None,
        context_id: false,
        scope_label: None,
        acl_ref: None,
        partition_key_columns: None,
        max_partitions: None,
        search_mode: contextdb_core::VectorSearchMode::Auto,
        auto_index_at: None,
        flat_below: None,
        auto_expansion_ceiling: None,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_search_radius_millionths: None,
        vector_policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
        consolidation_change_percent: None,
        consolidation_tombstone_percent: None,
        consolidation_disabled: false,
    }
}

pub(super) fn resolve_sync_rank_policies(table_name: &str, meta: &mut TableMeta) {
    let joined_meta = meta.clone();
    let anchor_columns = joined_meta.columns.clone();
    for column in &mut meta.columns {
        let Some(policy) = column.rank_policy.as_mut() else {
            continue;
        };
        if policy.joined_table != table_name {
            continue;
        }
        if let Some(protected_index) =
            sync_rank_policy_protected_index(&joined_meta, &policy.joined_column)
        {
            policy.protected_index = protected_index;
        }
        if let Some(anchor_column) =
            resolve_sync_rank_policy_anchor_column(policy, &anchor_columns, &joined_meta)
        {
            policy.anchor_column = anchor_column;
        }
    }
}

pub(super) fn sync_rank_policy_protected_index(
    meta: &TableMeta,
    joined_column: &str,
) -> Option<String> {
    meta.indexes
        .iter()
        .filter(|index| index.kind == IndexKind::UserDeclared)
        .chain(meta.indexes.iter())
        .find(|index| {
            index
                .columns
                .first()
                .is_some_and(|(column, _)| column == joined_column)
        })
        .map(|index| index.name.clone())
}

pub(super) fn resolve_sync_rank_policy_anchor_column(
    policy: &RankPolicy,
    anchor_columns: &[ColumnDef],
    joined_meta: &TableMeta,
) -> Option<String> {
    let joined_column = joined_meta
        .columns
        .iter()
        .find(|column| column.name == policy.joined_column)?;
    let anchor_by_name = |name: &str| anchor_columns.iter().find(|column| column.name == name);
    let mut candidates = Vec::new();
    if joined_column.primary_key {
        let singular = sync_singular_table_name(&policy.joined_table);
        for name in [
            format!("{singular}_id"),
            format!("{}_id", policy.joined_table),
        ] {
            if anchor_by_name(&name).is_some() && !candidates.contains(&name) {
                candidates.push(name);
            }
        }
    }
    if candidates.is_empty() && anchor_by_name(&policy.joined_column).is_some() {
        candidates.push(policy.joined_column.clone());
    }
    if candidates.is_empty()
        && let Some(primary_key) = anchor_columns.iter().find(|column| column.primary_key)
    {
        candidates.push(primary_key.name.clone());
    }
    if candidates.is_empty() && anchor_by_name("id").is_some() {
        candidates.push("id".to_string());
    }
    let [anchor_column] = candidates.as_slice() else {
        return None;
    };
    let anchor_def = anchor_by_name(anchor_column)?;
    (anchor_def.column_type == joined_column.column_type).then(|| anchor_column.clone())
}

pub(super) fn sync_singular_table_name(table: &str) -> String {
    if let Some(stem) = table.strip_suffix("ies") {
        format!("{stem}y")
    } else if let Some(stem) = table.strip_suffix('s') {
        stem.to_string()
    } else {
        table.to_string()
    }
}

pub(super) fn create_table_constraints_from_ast(ct: &CreateTable) -> Vec<String> {
    let mut constraints = Vec::new();

    if ct.immutable {
        constraints.push("IMMUTABLE".to_string());
    }

    if let Some(sm) = &ct.state_machine {
        // The transition map is unordered; render it sorted by source state
        // so every machine spells the same declared constraint identically —
        // the spelling participates in schema identity comparisons.
        let mut clauses = sm
            .transitions
            .iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect::<Vec<_>>();
        clauses.sort();
        constraints.push(format!(
            "STATE MACHINE ({}: {})",
            sm.column,
            clauses.join(", ")
        ));
    }

    if !ct.dag_edge_types.is_empty() {
        let edge_types = ct
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("DAG({edge_types})"));
    }

    if let Some(retain) = &ct.retain {
        let mut clause = format!("RETAIN {}", ttl_seconds_to_sql(retain.duration_seconds));
        if retain.sync_safe {
            clause.push_str(" SYNC SAFE");
        }
        constraints.push(clause);
    }

    for unique_constraint in &ct.unique_constraints {
        constraints.push(format!("UNIQUE ({})", unique_constraint.join(", ")));
    }

    if !ct.primary_key_columns.is_empty() {
        constraints.push(format!(
            "PRIMARY KEY ({})",
            ct.primary_key_columns.join(", ")
        ));
    }

    for fk in &ct.composite_foreign_keys {
        constraints.push(format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        ));
    }

    for rule in &ct.propagation_rules {
        match rule {
            contextdb_parser::ast::AstPropagationRule::EdgeState {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let mut clause = format!(
                    "PROPAGATE ON EDGE {} {} STATE {} SET {}",
                    edge_type, direction, trigger_state, target_state
                );
                if max_depth.unwrap_or(10) != 10 {
                    clause.push_str(&format!(" MAX DEPTH {}", max_depth.unwrap_or(10)));
                }
                if *abort_on_failure {
                    clause.push_str(" ABORT ON FAILURE");
                }
                constraints.push(clause);
            }
            contextdb_parser::ast::AstPropagationRule::VectorExclusion { trigger_state } => {
                constraints.push(format!(
                    "PROPAGATE ON STATE {} EXCLUDE VECTOR",
                    trigger_state
                ));
            }
            contextdb_parser::ast::AstPropagationRule::FkState { .. } => {}
        }
    }

    constraints
}

pub(super) fn create_table_constraints_from_meta(meta: &TableMeta) -> Vec<String> {
    let mut constraints = Vec::new();
    // Preserve declared custody policy in schema transport and inspection.
    if let Some(tables) = &meta.delivery_manifest_tables {
        constraints.push(format!(
            "DELIVERY MANIFEST OVER {}",
            tables
                .iter()
                .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(mode) = meta.edge_discard {
        constraints.push(format!(
            "EDGE DISCARD {}",
            match mode {
                contextdb_core::EdgeDiscardMode::Never => "NEVER",
                contextdb_core::EdgeDiscardMode::AfterOutcome => "AFTER OUTCOME",
                contextdb_core::EdgeDiscardMode::Always => "ALWAYS",
            }
        ));
    }

    if meta.immutable {
        constraints.push("IMMUTABLE".to_string());
    }

    if let Some(sm) = &meta.state_machine {
        // Sorted for the same one-spelling reason as the AST renderer above.
        let mut clauses = sm
            .transitions
            .iter()
            .map(|(from, to)| format!("{from} -> [{}]", to.join(", ")))
            .collect::<Vec<_>>();
        clauses.sort();
        constraints.push(format!(
            "STATE MACHINE ({}: {})",
            sm.column,
            clauses.join(", ")
        ));
    }

    if !meta.dag_edge_types.is_empty() {
        let edge_types = meta
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("DAG({edge_types})"));
    }

    // Carried in the unit it was DECLARED in: the receiving machine
    // reconstructs this text back into its own meta, and a re-spelled window
    // would reconstruct a declaration nobody wrote. The window itself is
    // stored in seconds, so two edges that spell the same window differently
    // still agree on when rows expire.
    if let Some(ttl_seconds) = meta.default_ttl_seconds {
        constraints.push(retain_clause_from_meta(meta, ttl_seconds));
    }

    // The version-history policy travels the same way: rendered only when
    // declared, so a receiving machine reconstructs the SAME declaration the
    // writer made (never a defaulted one) on both a fresh CreateTable and a
    // synced-in AlterTable, whose emitter carries this table's full current
    // constraint set (`alter_table_ddl_change`), not only what one ALTER
    // statement changed.
    if let Some(policy) = meta.history_policy {
        constraints.push(policy.declared_clause().to_string());
    }

    // The direction travels with the definition, as its own constraint string,
    // so the receiving machine honors the policy that was declared rather than
    // one nobody wrote.
    if let Some(direction) = meta.sync_direction {
        constraints.push(direction.sql().to_string());
    }

    // The conflict policy travels the same way, so the hub honors the
    // declaration the table carries instead of a uniform rule it hardcodes.
    if let Some(clause) = meta
        .conflict_policy
        .and_then(|policy| policy.declared_clause())
    {
        constraints.push(clause.to_string());
    }

    // Which index-prefix counts the engine keeps current travels with the
    // definition too, so the receiver maintains the SAME set the author
    // declared rather than a default it picked. Rendered only when declared.
    if let Some(declared) = meta.maintained_counts.as_ref() {
        constraints.push(declared.declared_clause());
    }

    for unique_constraint in &meta.unique_constraints {
        constraints.push(format!("UNIQUE ({})", unique_constraint.join(", ")));
    }

    if !meta.primary_key_columns.is_empty() {
        constraints.push(format!(
            "PRIMARY KEY ({})",
            meta.primary_key_columns.join(", ")
        ));
    }

    for fk in &meta.composite_foreign_keys {
        constraints.push(format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        ));
    }

    for rule in &meta.propagation_rules {
        match rule {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let dir = match direction {
                    Direction::Incoming => "INCOMING",
                    Direction::Outgoing => "OUTGOING",
                    Direction::Both => "BOTH",
                };
                let mut clause = format!(
                    "PROPAGATE ON EDGE {} {} STATE {} SET {}",
                    edge_type, dir, trigger_state, target_state
                );
                if *max_depth != 10 {
                    clause.push_str(&format!(" MAX DEPTH {max_depth}"));
                }
                if *abort_on_failure {
                    clause.push_str(" ABORT ON FAILURE");
                }
                constraints.push(clause);
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                constraints.push(format!(
                    "PROPAGATE ON STATE {} EXCLUDE VECTOR",
                    trigger_state
                ));
            }
            PropagationRule::ForeignKey { .. } => {}
        }
    }

    constraints
}

/// Render the table-level `RETAIN` and edge / vector-exclusion `PROPAGATE`
/// clauses for a `TableMeta`, deterministically ordered for stable output.
/// Foreign-key propagation is rendered on its owning column (see
/// `sql_type_for_meta_column`), so `PropagationRule::ForeignKey` is skipped
/// here — matching the sync DDL emitter's split.
/// The declared retention clause, in the unit it was written in. Direction is
/// a separate clause: this one says only when rows expire, and whether their
/// expiry waits on delivery.
pub(super) fn retain_clause_from_meta(meta: &TableMeta, ttl_seconds: u64) -> String {
    let window = match meta.retain_declared_unit {
        Some(unit) if unit.seconds_multiplier() != 0 => {
            format!("{} {}", ttl_seconds / unit.seconds_multiplier(), unit.sql())
        }
        _ => ttl_seconds_to_sql(ttl_seconds),
    };
    let mut clause = format!("RETAIN {window}");
    if meta.sync_safe {
        clause.push_str(" SYNC SAFE");
    }
    clause
}

pub(super) fn ttl_seconds_to_sql(seconds: u64) -> String {
    if seconds.is_multiple_of(24 * 60 * 60) {
        format!("{} DAYS", seconds / (24 * 60 * 60))
    } else if seconds.is_multiple_of(60 * 60) {
        format!("{} HOURS", seconds / (60 * 60))
    } else if seconds.is_multiple_of(60) {
        format!("{} MINUTES", seconds / 60)
    } else {
        format!("{seconds} SECONDS")
    }
}

pub(crate) fn has_graph_edge_table_shape(meta: &TableMeta) -> bool {
    [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ]
    .into_iter()
    .all(|(name, column_type)| has_exact_column_type(meta, name, &column_type))
}

pub(super) fn has_exact_column_type(
    meta: &TableMeta,
    name: &str,
    column_type: &ColumnType,
) -> bool {
    let mut columns = meta.columns.iter().filter(|column| column.name == name);
    matches!(columns.next(), Some(column) if &column.column_type == column_type)
        && columns.next().is_none()
}
