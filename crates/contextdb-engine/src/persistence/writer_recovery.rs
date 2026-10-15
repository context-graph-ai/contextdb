//! Writable-open bounds: end-key probes and non-derivable identity floors.
use super::writer_source::{SourceWindowCredit, SourceWindowOwner};
use super::*;

pub(super) const RECOVERY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("writer_recovery");
const RETENTION_CLOCK_BOUNDS: TableDefinition<&str, u64> =
    TableDefinition::new("retention_clock_bounds");
const BOUNDS_KEY: &str = "bounds";
const COMMIT_ROOT_KEY: &str = "commit_directory_root";
/// One schema-scoped maximum, never one record per edge or per commit.
const READ_LSN_BOUNDS: TableDefinition<&str, u64> =
    TableDefinition::new("committed_read_lsn_bounds");
const READ_BOUNDS_ROOT_KEY: &str = "committed_read_bounds_roots";

/// Identity floors survive directory cleanup as well as noncanonical imports.
/// Logical publication frontiers must not rewind when the final entry retires.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct WriterRecoveryBounds {
    pub(crate) identity_tx_floor: u64,
    pub(crate) source_lsn_floor: u64,
    pub(crate) external_row_id_floor: u64,
    pub(crate) commit_count: u64,
    pub(crate) graph_retirement_frontier: u64,
    pub(crate) graph_occurrence_id_floor: u64,
}

/// Private evidence for the bounds authenticated from this exact read capture.
/// The constructor is confined to this module; decoded bounds cannot mint it.
pub(super) struct AuthenticatedReadBounds<'read> {
    read: &'read redb::ReadTransaction,
    bounds: WriterRecoveryBounds,
}

impl AuthenticatedReadBounds<'_> {
    pub(super) fn read(&self) -> &redb::ReadTransaction {
        self.read
    }

    pub(super) fn bounds(&self) -> &WriterRecoveryBounds {
        &self.bounds
    }

    pub(super) fn into_bounds(self) -> WriterRecoveryBounds {
        self.bounds
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WriterOpenBounds {
    pub(crate) max_row_id: RowId,
    pub(crate) max_tx: TxId,
    pub(crate) max_lsn: Lsn,
    pub(crate) commit_first: Option<(Lsn, TxId)>,
    pub(crate) commit_last: Option<(Lsn, TxId)>,
    pub(crate) commit_count: u64,
}

impl RedbPersistence {
    /// The private recovery record is versioned and exactly consumed. It is
    /// deliberately independent of the public serde codec.
    pub(super) fn decode_writer_recovery_bounds(bytes: &[u8]) -> Result<WriterRecoveryBounds> {
        if bytes.len() != 49 || bytes[0] != 1 {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "invalid recovery bounds version or exact length".into(),
            });
        }
        let field = |offset| {
            u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("checked recovery width"),
            )
        };
        Ok(WriterRecoveryBounds {
            identity_tx_floor: field(1),
            source_lsn_floor: field(9),
            external_row_id_floor: field(17),
            commit_count: field(25),
            graph_retirement_frontier: field(33),
            graph_occurrence_id_floor: field(41),
        })
    }

    fn encode_writer_recovery_bounds(bounds: WriterRecoveryBounds) -> [u8; 49] {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        for (index, field) in [
            bounds.identity_tx_floor,
            bounds.source_lsn_floor,
            bounds.external_row_id_floor,
            bounds.commit_count,
            bounds.graph_retirement_frontier,
            bounds.graph_occurrence_id_floor,
        ]
        .into_iter()
        .enumerate()
        {
            bytes[1 + index * 8..9 + index * 8].copy_from_slice(&field.to_be_bytes());
        }
        bytes
    }

    pub(super) fn read_writer_recovery_bounds_in_read(
        read: &redb::ReadTransaction,
    ) -> Result<WriterRecoveryBounds> {
        let table = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let value = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing writer recovery bounds".into(),
            })?;
        Self::decode_writer_recovery_bounds(value.value())
    }

    pub(super) fn read_graph_recovery_bounds_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<WriterRecoveryBounds> {
        let table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let value = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing writer recovery bounds".into(),
            })?;
        Self::decode_writer_recovery_bounds(value.value())
    }

    /// Graph callers may raise their two scalars, never overwrite the other
    /// recovery facts or publish a premature root fingerprint.
    pub(super) fn write_graph_recovery_bounds_in_write(
        write: &redb::WriteTransaction,
        requested: WriterRecoveryBounds,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(9)?;
        let mut bounds = Self::read_graph_recovery_bounds_in_write(write)?;
        bounds.graph_retirement_frontier = bounds
            .graph_retirement_frontier
            .max(requested.graph_retirement_frontier);
        bounds.graph_occurrence_id_floor = bounds
            .graph_occurrence_id_floor
            .max(requested.graph_occurrence_id_floor);
        let encoded = Self::encode_writer_recovery_bounds(bounds);
        write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    fn graph_roots_digest(
        mut root_for: impl FnMut(u8) -> Result<Option<[u8; 32]>>,
    ) -> Result<[u8; 32]> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"contextdb.graph.proof.v1");
        hasher.update(&[1]);
        for tag in 1..=5 {
            let root = root_for(tag)?;
            hasher.update(&[tag, u8::from(root.is_some())]);
            hasher.update(&root.unwrap_or([0; 32]));
        }
        Ok(*hasher.finalize().as_bytes())
    }

    pub(crate) fn config_prefix_exists_at_root(
        root: &WriterSnapshot,
        prefix: &str,
    ) -> Result<bool> {
        let read = root.owner_read()?;
        let table = match read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut entries = table.range(prefix..).map_err(Self::storage_error)?;
        match entries.next() {
            Some(entry) => {
                let (key, _) = entry.map_err(Self::storage_error)?;
                Ok(key.value().starts_with(prefix))
            }
            None => Ok(false),
        }
    }

    pub(crate) fn pruned_change_counts_at_root(
        root: &Arc<WriterSnapshot>,
        owner: SourceWindowOwner,
        pruned: &HashMap<String, HashSet<(RowId, Lsn)>>,
    ) -> Result<(u64, HashMap<Lsn, u64>)> {
        let mut counts = HashMap::new();
        let mut total = 0;
        for (table, identities) in pruned {
            let lsns = identities
                .iter()
                .map(|(_, lsn)| *lsn)
                .collect::<HashSet<_>>();
            for lsn in lsns {
                let mut cursor = root.table_change_cursor(owner.clone(), table, lsn, lsn)?;
                while let Some(window) = cursor.next_table_change()? {
                    #[cfg(feature = "test-seams")]
                    crate::database::resident_history_test_seams::note(|work| {
                        work.log_keys_examined += 2
                    });
                    let row = match &window.value {
                        ChangeLogEntry::RowInsert { row_id, .. }
                        | ChangeLogEntry::RowDelete { row_id, .. } => Some(*row_id),
                        _ => None,
                    };
                    if row.is_some_and(|row| identities.contains(&(row, lsn))) {
                        total += 1;
                        *counts.entry(lsn).or_insert(0) += 1;
                    }
                }
            }
        }
        Ok((total, counts))
    }

    pub(crate) fn commit_candidates_at_root<F>(
        root: &WriterSnapshot,
        floor_probe: F,
        removed: &HashMap<Lsn, u64>,
        affected: &HashSet<Lsn>,
    ) -> Result<Vec<Lsn>>
    where
        F: FnOnce(std::ops::Bound<Lsn>) -> Result<Option<(Lsn, TxId)>>,
    {
        let owner = root.internal_source_owner();
        let SourceWindowOwner::Write(accountant, _) = &owner else {
            unreachable!("internal writer retention owner")
        };
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> =
            accountant.read_budget()?;
        graph_read::with_pages(memory, || {
            let _controls = owner.reserve(
                2 * std::mem::size_of::<redb::ReadOnlyTable<&[u8], &[u8]>>()
                    + std::mem::size_of::<redb::Range<'_, &[u8], &[u8]>>()
                    + 2 * std::mem::size_of::<redb::AccessGuard<'_, &[u8]>>(),
                "graph_retirement_probe",
            )?;
            let read = root.owner_read()?;
            let lifecycle = read
                .open_table(graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE)
                .map_err(Self::storage_error)?;
            // This captured-root filter deliberately over-retains. The writer
            // separately checks final net R before any commit-directory mutation.
            // With a retained witness there is no inverse public-LSN index that
            // would authorize retiring any existing nonzero support entry.
            if lifecycle.first().map_err(Self::storage_error)?.is_some() {
                return Ok(Vec::new());
            }
            let contributions = read
                .open_table(graph_history_record::GRAPH_CONTRIBUTIONS)
                .map_err(Self::storage_error)?;
            let counts = read
                .open_table(writer_history::CHANGE_LOG_LSN_COUNT)
                .map_err(Self::storage_error)?;
            let mut floor = None;
            for entry in counts.iter().map_err(Self::storage_error)? {
                let (lsn, count) = entry.map_err(Self::storage_error)?;
                #[cfg(feature = "test-seams")]
                crate::database::resident_history_test_seams::note(|work| {
                    work.log_keys_examined += 1
                });
                let lsn = Lsn(lsn.value());
                if count.value() > removed.get(&lsn).copied().unwrap_or(0) {
                    floor = Some(lsn);
                    break;
                }
            }
            let anchor =
                floor_probe(floor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded))?
                    .map(|(lsn, _)| lsn);
            let mut candidates = Vec::new();
            for lsn in affected {
                #[cfg(feature = "test-seams")]
                crate::database::resident_history_test_seams::note(|work| {
                    work.log_keys_examined += 1
                });
                if Some(*lsn) != anchor
                    && root.change_log_occurrences(*lsn)? <= removed.get(lsn).copied().unwrap_or(0)
                {
                    if lsn.0 == 0 {
                        continue;
                    }
                    let _prefix = owner.reserve(
                        9 + std::mem::size_of::<redb::Range<'_, &[u8], &[u8]>>()
                            + 2 * std::mem::size_of::<redb::AccessGuard<'_, &[u8]>>(),
                        "graph_creation_probe",
                    )?;
                    let mut prefix = [0; 9];
                    prefix[0] = 1;
                    prefix[1..].copy_from_slice(&lsn.0.to_be_bytes());
                    // R is empty, so a valid publication has no witnessed deletion
                    // contributions. Still inspect tags instead of treating a
                    // physical deletion prefix as public commit membership.
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::read_work_probe::note_graph(
                        "graph_contribution",
                        "range",
                        &prefix,
                        Some(contextdb_core::Lsn(lsn.0)),
                        Some("commit_reclamation_candidate"),
                    );
                    let mut entries = contributions
                        .range(prefix.as_slice()..)
                        .map_err(Self::storage_error)?
                        .inspect(|_entry| {
                            #[cfg(any(test, feature = "test-seams"))]
                            if let Ok((key, _)) = _entry {
                                crate::read_work_probe::graph_storage_key(
                                    "graph_contribution",
                                    "iteration_step",
                                    key.value(),
                                );
                            }
                        });
                    let mut has_creation = false;
                    if let Some(entry) = entries.next() {
                        let (key, _) = entry.map_err(Self::storage_error)?;
                        let key = key.value();
                        if !key.starts_with(&prefix) {
                            candidates.push(*lsn);
                            continue;
                        }
                        if key.len() < 18 || !matches!(key[17], 0 | 1) {
                            return Err(Error::StoreCorrupted {
                                path: "graph_contributions_v1".into(),
                                reason: "invalid graph contribution key".into(),
                            });
                        }
                        if key[17] == 0 {
                            has_creation = true;
                        } else {
                            // A deletion cannot survive without positive-count R.
                            return Err(Error::StoreCorrupted {
                                path: "graph_contributions_v1".into(),
                                reason: "deletion contribution survives empty lifecycle state"
                                    .into(),
                            });
                        }
                    }
                    if has_creation {
                        continue;
                    }
                    candidates.push(*lsn);
                }
            }
            Ok(candidates)
        })
    }

    pub(crate) fn admit_retention_probe_key(
        root: &WriterSnapshot,
        owner: &SourceWindowOwner,
        bytes: usize,
    ) -> Result<SourceWindowCredit> {
        root.adapter_control(owner, bytes)
    }

    /// A monotone upper bound can prove that no retained row is future-dated.
    /// Deletes need not lower it: a high stale bound only keeps the exact scan.
    pub(crate) fn retention_clock_upper_in_read(
        root: &WriterSnapshot,
        table: &str,
    ) -> Result<Option<Wallclock>> {
        let read = root.owner_read()?;
        let bounds = match read.open_table(RETENTION_CLOCK_BOUNDS) {
            Ok(bounds) => bounds,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        Ok(bounds
            .get(table)
            .map_err(Self::storage_error)?
            .map(|value| Wallclock(value.value())))
    }

    pub(super) fn invalidate_retention_clock_in_write(
        write: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        write
            .open_table(RETENTION_CLOCK_BOUNDS)
            .map_err(Self::storage_error)?
            .remove(table)
            .map_err(Self::storage_error)?;
        Ok(())
    }

    pub(crate) fn writer_recovery_floors_in_read(
        snapshot: &WriterSnapshot,
    ) -> Result<WriterRecoveryBounds> {
        let read = snapshot.owner_read()?;
        Self::read_writer_recovery_bounds_in_read(&read)
    }

    /// A checkpoint carries non-derivable identity floors even if no retained
    /// row or commit key witnesses them. Destination counts and physical roots
    /// remain those of its own completed copy.
    pub(crate) fn flush_writer_recovery_floors(&self, source: WriterRecoveryBounds) -> Result<()> {
        let _scratch = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant.clone(),
                    512,
                    "storage",
                    "export_writer_floors",
                    "Raise MEMORY_LIMIT to export writer identity floors.",
                )
            })
            .transpose()?;
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            {
                let mut table = write
                    .open_table(RECOVERY_TABLE)
                    .map_err(Self::storage_error)?;
                let mut target: WriterRecoveryBounds = table
                    .get(BOUNDS_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode_writer_recovery_bounds(value.value()))
                    .transpose()?
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: "writer_recovery".into(),
                        reason: "missing destination writer recovery bounds".into(),
                    })?;
                target.identity_tx_floor = target.identity_tx_floor.max(source.identity_tx_floor);
                target.source_lsn_floor = target.source_lsn_floor.max(source.source_lsn_floor);
                target.external_row_id_floor = target
                    .external_row_id_floor
                    .max(source.external_row_id_floor);
                target.graph_retirement_frontier = target
                    .graph_retirement_frontier
                    .max(source.graph_retirement_frontier);
                target.graph_occurrence_id_floor = target
                    .graph_occurrence_id_floor
                    .max(source.graph_occurrence_id_floor);
                let encoded = Self::encode_writer_recovery_bounds(target);
                table
                    .insert(BOUNDS_KEY, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(32)?;
            Self::write_commit_directory_root_in_write(&write)?;
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(33)?;
            Self::write_read_bounds_roots_in_write(&write)?;
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(34)?;
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            {
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)
        })
    }

    /// Called in the same transaction as every persistence commit, including
    /// store creation and early schema flushes. `len` reads the tree's stored
    /// cardinality, not its entries. History is never enumerated here.
    pub(super) fn write_writer_recovery_bounds_in_write(
        write: &redb::WriteTransaction,
        ws: Option<&WriteSet>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(9)?;
        {
            let mut clocks = write
                .open_table(RETENTION_CLOCK_BOUNDS)
                .map_err(Self::storage_error)?;
            if let Some(ws) = ws {
                for (table, row) in &ws.relational_inserts {
                    if let Some(stamp) = row.created_at {
                        let previous = clocks
                            .get(table.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| value.value());
                        // An absent bound is unknown for an existing family.
                        // Only its first ordinary insert can establish complete
                        // coverage without scanning old versions.
                        if previous.is_none() {
                            let counts = write
                                .open_table(RECOVERY_TABLE)
                                .map_err(Self::storage_error)?;
                            let key = format!("row_versions:{table}");
                            let empty_before = counts
                                .get(key.as_str())
                                .map_err(Self::storage_error)?
                                .is_none_or(|value| value.value() == 0u64.to_be_bytes());
                            if !empty_before {
                                continue;
                            }
                        }
                        let maximum = previous.unwrap_or(0);
                        if stamp.0 > maximum {
                            clocks
                                .insert(table.as_str(), stamp.0)
                                .map_err(Self::storage_error)?;
                        }
                    }
                }
            }
        }
        let (commit_count, last) = {
            let commits = write
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            (
                commits.len().map_err(Self::storage_error)?,
                commits
                    .last()
                    .map_err(Self::storage_error)?
                    .map(|(lsn, tx)| (lsn.value(), tx.value())),
            )
        };
        // Metadata-only declarations need not have a row commit-index entry.
        // Preserve their frontier from one end-key seek, without decoding history.
        let ddl_lsn = {
            let ddl = write
                .open_table(DDL_LOG_TABLE)
                .map_err(Self::storage_error)?;
            ddl.last()
                .map_err(Self::storage_error)?
                .map(|(key, _)| Self::ddl_lsn_from_key(key.value()))
                .transpose()?
                .unwrap_or(Lsn(0))
        };
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_writer_recovery_bounds(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.commit_count = commit_count;
        bounds.source_lsn_floor = bounds.source_lsn_floor.max(ddl_lsn.0);
        if let Some((lsn, tx)) = last {
            bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx);
            bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn);
        }
        if let Some(ws) = ws {
            for (key, bytes) in &ws.config_writes {
                let (row, lsn) =
                    crate::database::Database::lineage_recovery_floors_for_write(key, bytes)?;
                bounds.external_row_id_floor = bounds.external_row_id_floor.max(row.0);
                bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn.0);
            }
            // Imports can name identities beyond the local commit frontier.
            let tx = Self::write_set_visibility_tx(ws)
                .into_iter()
                .chain(ws.visibility_floor)
                .max()
                .unwrap_or(TxId(0))
                .0;
            if tx > last.map_or(0, |(_, tx)| tx) {
                bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx);
            }
            let source_lsn = ws
                .relational_inserts
                .iter()
                .map(|(_, row)| row.lsn.0)
                .chain(ws.adj_inserts.iter().map(|edge| edge.lsn.0))
                .chain(ws.vector_inserts.iter().map(|entry| entry.lsn.0))
                .chain(ws.commit_lsn.map(|lsn| lsn.0))
                .max()
                .unwrap_or(0);
            if source_lsn > last.map_or(0, |(lsn, _)| lsn) {
                bounds.source_lsn_floor = bounds.source_lsn_floor.max(source_lsn);
            }
            // A final relational key may be reclaimed, and vector keys sort
            // by column/partition before row id. Preserve every applied row
            // identity in this non-derivable floor before either directory
            // can lose its last witness.
            if let Some(row_id) = ws
                .vector_inserts
                .iter()
                .map(|entry| entry.row_id.0)
                .chain(ws.relational_inserts.iter().map(|(_, row)| row.row_id.0))
                .max()
            {
                bounds.external_row_id_floor = bounds.external_row_id_floor.max(row_id);
            }
        }
        let encoded = Self::encode_writer_recovery_bounds(bounds);
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        drop(table);
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(13)?;
        Self::write_commit_directory_root_in_write(write)?;
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(14)?;
        Self::write_row_heads_root(write)?;
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(15)?;
        Self::write_writer_relational_counts_in_write(write)?;
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(16)?;
        if let Some(ws) = ws {
            for (table, row) in &ws.relational_inserts {
                Self::raise_table_read_lsn_in_write(write, table, row.lsn)?;
            }
            for entry in &ws.vector_inserts {
                Self::raise_table_read_lsn_in_write(write, &entry.index.table, entry.lsn)?;
            }
            if let Some(lsn) = ws.commit_lsn {
                for (table, _, _) in &ws.relational_deletes {
                    Self::raise_table_read_lsn_in_write(write, table, lsn)?;
                }
                for (index, _, _) in &ws.vector_deletes {
                    Self::raise_table_read_lsn_in_write(write, &index.table, lsn)?;
                }
                for (index, _, _, _) in &ws.vector_moves {
                    Self::raise_table_read_lsn_in_write(write, &index.table, lsn)?;
                }
                for predicate in &ws.relational_delete_predicates {
                    Self::raise_table_read_lsn_in_write(write, &predicate.table, lsn)?;
                }
            }
        }
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(17)?;
        vector_membership_pages::write_fingerprints(write)?;
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(18)?;
        Self::write_read_bounds_roots_in_write(write)
    }

    pub(super) fn raise_table_read_lsn_in_write(
        write: &redb::WriteTransaction,
        table: &str,
        lsn: Lsn,
    ) -> Result<()> {
        let mut bounds = write
            .open_table(READ_LSN_BOUNDS)
            .map_err(Self::storage_error)?;
        let previous = bounds
            .get(table)
            .map_err(Self::storage_error)?
            .map_or(0, |value| value.value());
        if lsn.0 > previous {
            bounds.insert(table, lsn.0).map_err(Self::storage_error)?;
        }
        Ok(())
    }

    /// A point lookup in the already authenticated root. Absence permits
    /// only zero-LSN bootstrap records, never an unbounded durable version.
    pub(super) fn table_read_lsn_in_read(
        read: &redb::ReadTransaction,
        table: &str,
    ) -> std::result::Result<Lsn, LoadReadImageError> {
        let bounds = read.open_table(READ_LSN_BOUNDS).map_err(read_table_error)?;
        let value = bounds.get(table).map_err(read_storage_error)?;
        Ok(Lsn(value.map_or(0, |value| value.value())))
    }

    /// Refreshed in the mutation's own transaction, after every recovery
    /// record and schema maximum has been installed. Physical relocation
    /// rebinds these exact same facts; it does not reconstruct their contents.
    pub(super) fn write_read_bounds_roots_in_write(write: &redb::WriteTransaction) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(9)?;
        {
            write
                .open_table(READ_LSN_BOUNDS)
                .map_err(Self::storage_error)?;
        }
        // Every writer commit carries the schema half of the paged binding
        // forward, before the recovery root below is taken: only a change
        // made outside the writer can leave schema metadata unbound.
        super::content_identity::rebind_vector_schema_meta_root_if_bound_in_write(write)?;
        // Table existence is distinct from an existing empty root. A write
        // open_table would silently create missing proof state, so inspect
        // the catalog before obtaining any graph descriptor.
        let mut present = [false; 5];
        for handle in write.list_tables().map_err(Self::storage_error)? {
            for (index, name) in [
                "graph_fwd",
                "graph_rev",
                "graph_contributions_v1",
                "graph_occurrence_locations_v1",
                "graph_predecessor_lifecycle_v1",
            ]
            .into_iter()
            .enumerate()
            {
                present[index] |= handle.name() == name;
            }
        }
        if present.into_iter().any(|exists| !exists) {
            return Err(Error::StoreCorrupted {
                path: "graph proof roots".into(),
                reason: "missing required graph proof table".into(),
            });
        }

        let mut roots = [0u8; 130];
        roots[..32].copy_from_slice(
            &{
                let acquired_root = write.table_root_fingerprint(RECOVERY_TABLE);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root("recovery", &[], &acquired_root);
                acquired_root
            }
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]),
        );
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(19)?;
        roots[32..64].copy_from_slice(
            &{
                let acquired_root = write.table_root_fingerprint(READ_LSN_BOUNDS);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root("scoped_bounds", &[], &acquired_root);
                acquired_root
            }
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]),
        );
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(20)?;
        roots[64..96].copy_from_slice(&Self::graph_roots_digest(|tag| {
            let table = match tag {
                1 => GRAPH_FWD_TABLE,
                2 => GRAPH_REV_TABLE,
                3 => graph_history_record::GRAPH_CONTRIBUTIONS,
                4 => graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
                5 => graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
                _ => unreachable!("fixed graph table tags"),
            };
            let acquired = {
                let acquired_root = write.table_root_fingerprint(table);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root(
                    match tag {
                        1 => "graph_fwd",
                        2 => "graph_rev",
                        3 => "graph_contribution",
                        4 => "graph_location",
                        _ => "graph_lifecycle",
                    },
                    &[tag],
                    &acquired_root,
                );
                acquired_root
            }
            .map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(20 + tag)?;
            Ok(acquired)
        })?);
        content_identity::history_open_control::finalize(write)?;
        roots[96] = 1;
        let identity_roots = write.table_root_fingerprint(CONTENT_IDENTITY_ROOTS_TABLE);
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph_root(
            CONTENT_IDENTITY_ROOTS_TABLE.name(),
            &[],
            &identity_roots,
        );
        roots[97..129].copy_from_slice(
            &identity_roots
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
        );
        roots[129] = 1;
        let mut metadata = write
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        if metadata
            .get(observed_graph_key!("metadata", "get", READ_BOUNDS_ROOT_KEY))
            .map_err(Self::storage_error)?
            .is_none_or(|existing| existing.value() != roots)
        {
            metadata
                .insert(READ_BOUNDS_ROOT_KEY, roots.as_slice())
                .map_err(Self::storage_error)?;
        }
        drop(metadata);
        #[cfg(any(test, feature = "test-seams"))]
        graph_observation_checkpoint(26)?;
        Ok(())
    }

    /// Both read routes authenticate the same constant-size committed roots.
    /// Missing bindings are corrupt in a greenfield store; there is no scan
    /// fallback and no upgrade or earlier-format compatibility path.
    pub(super) fn authenticate_read_bounds(
        read: &redb::ReadTransaction,
    ) -> Result<AuthenticatedReadBounds<'_>> {
        let bounds = Self::read_authenticated_bounds_in_read(read)?;
        Ok(AuthenticatedReadBounds { read, bounds })
    }

    pub(super) fn read_authenticated_bounds_in_read(
        read: &redb::ReadTransaction,
    ) -> Result<WriterRecoveryBounds> {
        let damage = |reason: &str| Error::StoreCorrupted {
            path: "committed read bounds".into(),
            reason: reason.into(),
        };
        let metadata = read
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        let expected = metadata
            .get(observed_graph_key!("metadata", "get", READ_BOUNDS_ROOT_KEY))
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing committed read-bounds root binding"))?;
        drop(
            read.open_table(READ_LSN_BOUNDS)
                .map_err(Self::storage_error)?,
        );
        // Table existence is distinct from an existing empty root. A write
        // open_table would silently create missing proof state, so inspect
        // the catalog before obtaining any graph descriptor.
        let mut present = [false; 5];
        for handle in read.list_tables().map_err(Self::storage_error)? {
            for (index, name) in [
                "graph_fwd",
                "graph_rev",
                "graph_contributions_v1",
                "graph_occurrence_locations_v1",
                "graph_predecessor_lifecycle_v1",
            ]
            .into_iter()
            .enumerate()
            {
                present[index] |= handle.name() == name;
            }
        }
        if present.into_iter().any(|exists| !exists) {
            return Err(Error::StoreCorrupted {
                path: "graph proof roots".into(),
                reason: "missing required graph proof table".into(),
            });
        }

        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
        let mut roots = [0u8; 130];
        roots[..32].copy_from_slice(
            &{
                let acquired_root = read.table_root_fingerprint(RECOVERY_TABLE);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root("recovery", &[], &acquired_root);
                acquired_root
            }
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]),
        );
        roots[32..64].copy_from_slice(
            &{
                let acquired_root = read.table_root_fingerprint(READ_LSN_BOUNDS);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root("scoped_bounds", &[], &acquired_root);
                acquired_root
            }
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]),
        );
        roots[64..96].copy_from_slice(&Self::graph_roots_digest(|tag| {
            let table = match tag {
                1 => GRAPH_FWD_TABLE,
                2 => GRAPH_REV_TABLE,
                3 => graph_history_record::GRAPH_CONTRIBUTIONS,
                4 => graph_history_record::GRAPH_OCCURRENCE_LOCATIONS,
                5 => graph_history_record::GRAPH_PREDECESSOR_LIFECYCLE,
                _ => unreachable!("fixed graph table tags"),
            };
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::bind_graph_root(read.storage_snapshot_identity());
            {
                let acquired_root = read.table_root_fingerprint(table);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph_root(
                    match tag {
                        1 => "graph_fwd",
                        2 => "graph_rev",
                        3 => "graph_contribution",
                        4 => "graph_location",
                        _ => "graph_lifecycle",
                    },
                    &[tag],
                    &acquired_root,
                );
                acquired_root
            }
            .map_err(Self::storage_error)
        })?);
        // Named catalog presence is independent of the root header (empty != absent).
        for definition in [
            CONTENT_IDENTITY_ROOTS_TABLE.name(),
            CONTENT_IDENTITY_NODES_TABLE.name(),
        ] {
            read.open_untyped_table(redb::TableDefinition::<&[u8], &[u8]>::new(definition))
                .map_err(Self::storage_error)?;
        }
        roots[96] = 1;
        let identity_roots = read.table_root_fingerprint(CONTENT_IDENTITY_ROOTS_TABLE);
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph_root(
            CONTENT_IDENTITY_ROOTS_TABLE.name(),
            &[],
            &identity_roots,
        );
        roots[97..129].copy_from_slice(
            &identity_roots
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
        );
        roots[129] = 1;
        if expected.value() != roots {
            return Err(damage(
                "committed read bounds differ from their authenticated roots",
            ));
        }
        let table = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let bytes = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing committed cross-subsystem bounds"))?;
        Self::decode_writer_recovery_bounds(bytes.value())
    }

    /// Constant-size root binding, also refreshed after physical relocation.
    /// It authenticates the committed directory without walking history.
    pub(super) fn write_commit_directory_root_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        let root = {
            let acquired_root = write.table_root_fingerprint(COMMIT_INDEX_TABLE);
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_graph_root("commit_directory", &[], &acquired_root);
            acquired_root
        }
        .map_err(Self::storage_error)?
        .unwrap_or([0; 32]);
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        if table
            .get(COMMIT_ROOT_KEY)
            .map_err(Self::storage_error)?
            .is_some_and(|existing| existing.value() == root)
        {
            return Ok(());
        }
        table
            .insert(COMMIT_ROOT_KEY, root.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    // A full row cursor has no selected posting to attest a missing body.
    // Its transaction-coupled cardinality certifies the physical family, even
    // when a corrupt deletion leaves the iterator empty. No identities or
    // row payloads are read to maintain or check this metadata.
    fn write_writer_relational_counts_in_write(write: &redb::WriteTransaction) -> Result<()> {
        let metadata = write.open_table(META_TABLE).map_err(Self::storage_error)?;
        let mut counts = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        for entry in metadata
            .range("table:".."table;")
            .map_err(Self::storage_error)?
        {
            let (name, _) = entry.map_err(Self::storage_error)?;
            let name =
                name.value()
                    .strip_prefix("table:")
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: "meta".into(),
                        reason: "invalid table declaration key".into(),
                    })?;
            let physical = Self::rel_table_name(name);
            // An existing relation's count comes from its directory header:
            // opening it for writing would make every commit open, close and
            // re-witness every declared table, not just the ones it changed.
            // A declared relation not yet created is opened once here, which
            // creates and witnesses it, so readers find its physical root.
            let count = match write
                .normal_table_len(&physical)
                .map_err(Self::storage_error)?
            {
                Some(count) => count,
                None => write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(&physical))
                    .map_err(Self::storage_error)?
                    .len()
                    .map_err(Self::storage_error)?,
            };
            let key = format!("row_versions:{name}");
            let value = count.to_be_bytes();
            if counts
                .get(key.as_str())
                .map_err(Self::storage_error)?
                .is_some_and(|existing| existing.value() == value)
            {
                continue;
            }
            counts
                .insert(key.as_str(), value.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(super) fn verify_writer_relational_count_in_read(
        read: &redb::ReadTransaction,
        table: &str,
        actual: u64,
    ) -> Result<()> {
        let counts = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let key = format!("row_versions:{table}");
        let value = counts
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "missing saved row cardinality".into(),
            })?;
        let expected = value
            .value()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "invalid saved row cardinality".into(),
            })?;
        if expected != actual {
            return Err(Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "row family cardinality differs from its committed record".into(),
            });
        }
        Ok(())
    }

    /// Schema-sized open work: one end-key probe per relation and two commit
    /// end probes. Callers must not reconstruct a directory by walking rows,
    /// graphs, vectors or change history after obtaining these bounds.
    pub(crate) fn writer_open_bounds<'a>(
        &self,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<WriterOpenBounds> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Self::writer_open_bounds_in_read(&read, tables)
        })
    }

    /// Authenticate only the commit directory at this already-authenticated root.
    pub(super) fn read_commit_directory_bounds_in_read(
        read: &redb::ReadTransaction,
        stored: WriterRecoveryBounds,
    ) -> Result<(u64, Option<(Lsn, TxId)>, Option<(Lsn, TxId)>)> {
        let record = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let expected_root = record
            .get(COMMIT_ROOT_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing authenticated commit directory root".into(),
            })?;
        let actual_root = {
            let acquired_root = read.table_root_fingerprint(COMMIT_INDEX_TABLE);
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_graph_root("commit_directory", &[], &acquired_root);
            acquired_root
        }
        .map_err(Self::storage_error)?
        .unwrap_or([0; 32]);
        if expected_root.value() != actual_root {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "commit directory differs from its committed root".into(),
            });
        }
        let commits = read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        let commit_first = commits
            .first()
            .map_err(Self::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        let commit_last = commits
            .last()
            .map_err(Self::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        let count = commits.len().map_err(Self::storage_error)?;
        if stored.commit_count != count {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "commit directory cardinality differs from recovery bounds".into(),
            });
        }
        Ok((count, commit_first, commit_last))
    }

    pub(super) fn writer_open_bounds_in_read<'a>(
        read: &redb::ReadTransaction,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<WriterOpenBounds> {
        Self::validate_ddl_selectors_in_read(read)?;
        Self::read_authenticated_bounds_in_read(read)?;
        let record = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let stored: WriterRecoveryBounds = record
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_writer_recovery_bounds(value.value()))
            .transpose()?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing writer recovery bounds".into(),
            })?;
        let (count, commit_first, commit_last) =
            Self::read_commit_directory_bounds_in_read(read, stored)?;
        let mut bounds = WriterOpenBounds {
            max_row_id: RowId(stored.external_row_id_floor),
            max_tx: TxId(
                stored
                    .identity_tx_floor
                    .max(commit_last.map_or(0, |(_, tx)| tx.0)),
            ),
            max_lsn: Lsn(stored
                .source_lsn_floor
                .max(commit_last.map_or(0, |(lsn, _)| lsn.0))),
            commit_first,
            commit_last,
            commit_count: count,
        };
        match read.open_table(DDL_LOG_TABLE) {
            Ok(ddl) => {
                if let Some((key, _)) = ddl.last().map_err(Self::storage_error)? {
                    bounds.max_lsn = bounds.max_lsn.max(Self::ddl_lsn_from_key(key.value())?);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        match read.open_table(writer_history::WRITER_CHANGE_LOG_TABLE) {
            Ok(log) => {
                if let Some((key, _)) = log.last().map_err(Self::storage_error)? {
                    let key = key.value();
                    if key.len() != 16 {
                        return Err(Error::StoreCorrupted {
                            path: "writer_change_log".into(),
                            reason: "log end key is not a 16-byte occurrence key".into(),
                        });
                    }
                    bounds.max_lsn = bounds.max_lsn.max(Lsn(u64::from_be_bytes(
                        key[..8].try_into().expect("validated occurrence width"),
                    )));
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        for table in tables {
            let name = Self::rel_table_name(table);
            let relation = match read.open_table(TableDefinition::<&[u8], &[u8]>::new(&name)) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(e) => return Err(Self::storage_error(e)),
            };
            if let Some((key, _)) = relation.last().map_err(Self::storage_error)? {
                let key = key.value();
                if key.len() != 24 {
                    return Err(Error::StoreCorrupted {
                        path: name,
                        reason: "relation end key is not a 24-byte version key".into(),
                    });
                }
                let row_id = u64::from_be_bytes(key[..8].try_into().expect("validated key width"));
                bounds.max_row_id = bounds.max_row_id.max(RowId(row_id));
            }
        }
        Ok(bounds)
    }
}

impl RedbPersistence {
    pub(super) fn raise_config_identity_floors_in_write(
        write: &redb::WriteTransaction,
        key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        let (row, lsn) = crate::database::Database::lineage_recovery_floors_for_write(key, bytes)?;
        if row.0 != 0 || lsn.0 != 0 {
            Self::raise_noncanonical_writer_floors_in_write(write, TxId(0), lsn, row)?;
        }
        Ok(())
    }

    /// Snapshot/import batching can install identities without a canonical
    /// commit entry in that batch. Preserve their floors before committing.
    pub(super) fn raise_noncanonical_writer_floors_in_write(
        write: &redb::WriteTransaction,
        tx: TxId,
        lsn: Lsn,
        external_row: RowId,
    ) -> Result<()> {
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_writer_recovery_bounds(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx.0);
        bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn.0);
        bounds.external_row_id_floor = bounds.external_row_id_floor.max(external_row.0);
        let encoded = Self::encode_writer_recovery_bounds(bounds);
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }
}
