//! Row-owned adjacency in the private received-schema image.
use super::*;
use contextdb_tx::OwnedAdjEntry;

impl Database {
    /// Materialize only a resident selected image. Durable source-backed
    /// graphs have no resident accounting image and must not be scanned here.
    pub(super) fn received_row_graph_image(
        graph: &GraphStore,
        affected_types: &HashSet<String>,
        affected_tables: &HashSet<String>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<contextdb_graph::GraphAdjEntry>> {
        if graph.is_source_backed() {
            return Ok(Vec::new());
        }
        let selected = |edge: &&contextdb_graph::GraphAdjEntry| {
            edge.row_owner.as_ref().map_or_else(
                || affected_types.contains(&edge.edge_type),
                |owner| affected_tables.contains(&owner.table),
            )
        };
        let forward = graph.forward_adj.read();
        let count = forward.values().flatten().filter(selected).count();
        copy_owner.reserve(
            count
                .checked_mul(std::mem::size_of::<contextdb_graph::GraphAdjEntry>())
                .ok_or_else(|| Error::Other("selected graph image size overflow".into()))?,
        )?;
        // Price every selected payload before cloning the first one. A refusal
        // leaves no partially allocated, unadmitted selected image.
        for edge in forward.values().flatten().filter(selected) {
            copy_owner.reserve(edge.estimated_bytes())?;
        }
        let mut result = Vec::with_capacity(count);
        result.extend(forward.values().flatten().filter(selected).cloned());
        Ok(result)
    }

    /// Prepare both directions while the admitted borrowed source image
    /// remains available for durable projection and memory delta accounting.
    pub(super) fn prepare_received_row_graph_image(
        edges: &[contextdb_graph::GraphAdjEntry],
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<PreparedGraphPublication> {
        // Each direction can have one map node per edge. Vec's initial
        // allocation is at most four slots here; subsequent doubling stays
        // within four slots per input entry across all node groups. Include
        // both direction arrays, the input clone, hash controls/unused buckets
        // and each map's node key and Vec header before construction.
        let per_edge = std::mem::size_of::<contextdb_graph::GraphAdjEntry>()
            .checked_mul(9)
            .and_then(|n| {
                std::mem::size_of::<(NodeId, Vec<contextdb_graph::GraphAdjEntry>)>()
                    .checked_add(std::mem::size_of::<usize>())
                    .and_then(|m| m.checked_mul(8))
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or_else(|| Error::Other("graph publication structure size overflow".into()))?;
        copy_owner.reserve(
            edges
                .len()
                .checked_mul(per_edge)
                .ok_or_else(|| Error::Other("graph publication allocation overflow".into()))?,
        )?;
        for edge in edges {
            // GraphAdjEntry accounts both independently cloned directions,
            // including standalone properties and row-owner string payloads.
            copy_owner.reserve(edge.estimated_bytes())?;
        }
        let copied = edges.to_vec();
        Ok(GraphStore::prepare_received_schema_publication(copied))
    }

    /// Copy only witnesses belonging to the affected retained row versions.
    /// The rows have already entered `working`; unrelated graph history is
    /// never enumerated, and equal endpoint siblings are resolved separately.
    pub(super) fn copy_received_row_graph_witnesses(
        &self,
        working: &Database,
        affected_tables: &HashSet<String>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let tables = working.relational_store.tables.read();
        for table in affected_tables {
            let Some(rows) = tables.get(table) else {
                continue;
            };
            if rows.is_empty() {
                continue;
            }
            let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
            for row in rows {
                self.with_row_graph_witness(
                    table,
                    generation,
                    row,
                    read_memory.clone(),
                    |edge, owner| {
                        // Match the detached database's drop accounting: each
                        // direction retains its own owner/table allocation.
                        let bytes = edge.estimated_bytes().saturating_add(
                            std::mem::size_of::<Option<contextdb_tx::RowEdgeOwner>>()
                                .saturating_add(owner.table.len())
                                .saturating_mul(2),
                        );
                        let charge = StagedCopyCharge::admit(
                            &self.finalization.accountant,
                            Arc::clone(&self.finalization.vector_accountant),
                            bytes,
                            "stage_received_unit_row_edge_image",
                        )?;
                        working.graph_store.insert_loaded_graph_entry(
                            contextdb_graph::GraphAdjEntry {
                                entry: edge.clone(),
                                row_owner: Some(owner.clone()),
                            },
                        );
                        charge.transfer();
                        #[cfg(any(test, feature = "test-seams"))]
                        record_received_schema_staged_graph_edge_for_test();
                        Ok(())
                    },
                )?;
            }
        }
        Ok(())
    }

    /// Select dropped owners before any detached DDL can discard their rows.
    /// `copy_owner` is retained by the received-schema stage through durable
    /// and memory publication, including the vector's backing allocation.
    pub(super) fn received_row_graph_drop_witnesses(
        &self,
        working: &Database,
        dropped_tables: &HashSet<String>,
        copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<OwnedAdjEntry>> {
        let tables = working.relational_store.tables.read();
        let count = dropped_tables.iter().try_fold(0usize, |count, table| {
            count
                .checked_add(tables.get(table).map_or(0, Vec::len))
                .ok_or_else(|| Error::Other("selected graph owner count overflow".into()))
        })?;
        let slots = count
            .checked_mul(std::mem::size_of::<OwnedAdjEntry>())
            .ok_or_else(|| Error::Other("selected graph owner allocation overflow".into()))?;
        copy_owner.reserve(slots)?;
        let mut selected = Vec::with_capacity(count);
        let read_memory = self.declared_reader_memory()?;
        for table in dropped_tables {
            let Some(rows) = tables.get(table) else {
                continue;
            };
            if rows.is_empty() {
                continue;
            }
            let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
            for row in rows {
                self.with_row_graph_witness(
                    table,
                    generation,
                    row,
                    read_memory.clone(),
                    |edge, owner| {
                        // The shared exact lookup rejects nonempty properties on a
                        // row-derived witness, so these are its only heap payloads.
                        copy_owner
                            .reserve(owner.table.len().saturating_add(edge.edge_type.len()))?;
                        selected.push(OwnedAdjEntry {
                            owner: owner.clone(),
                            entry: edge.clone(),
                        });
                        Ok(())
                    },
                )?;
            }
        }
        Ok(selected)
    }

    /// Restamp the accepted detached mutations, never their immutable delete
    /// before-owner. Call before the ordinary received-schema restamp rewrites
    /// the row insert identities. Its canonical reverse rebuild follows this.
    pub(super) fn restamp_received_row_graph(
        working: &Database,
        finalized: &mut WriteSet,
        local_lsn: Lsn,
        receiver_tx: TxId,
        accepted_positions: &BTreeMap<(String, RowId), Lsn>,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let position = |owner: &contextdb_tx::RowEdgeOwner| {
            accepted_positions.iter().find(|((table, row), _)| table == &owner.table && *row == owner.row_id)
                .map_or(local_lsn, |(_, lsn)| *lsn)
        };
        // A paged working graph stores its row authority in the private row
        // source. Verify each exact producer against that source instead of
        // demanding a second resident adjacency image.
        if working.graph_store.is_source_backed() {
            for owned in &finalized.row_adj_inserts {
                // The canonical prepared row is still in its original source
                // custody. The paged physical replacement is installed only
                // after this joint row/edge identity validation and restamp.
                let row = finalized.relational_inserts.iter().rev().find_map(|(table, row)| {
                    (table == &owned.owner.table && row.row_id == owned.owner.row_id)
                        .then_some(row)
                }).ok_or_else(|| Error::SyncError("received row graph has no canonical row authority".into()))?;
                if !super::row_graph::row_graph_matches_final_row(owned, &owned.owner.table, row) {
                    return Err(Error::SyncError("received row graph differs from its private row authority".into()));
                }
            }
        }
        // The common row producer stamps the receiver's authoritative table
        // generation. Restamping may change MVCC stamps, never substitute a
        // sender lineage generation or recycle the dropped before-owner.
        {
            let generations = working.in_memory_table_generations.lock();
            for owned in &finalized.row_adj_inserts {
                if generations.get(&owned.owner.table).copied()
                    != Some(owned.owner.table_generation)
                {
                    return Err(Error::SyncError(
                        "received row witness has the wrong receiver table generation".into()));
                }
            }
        }
        let slots = finalized
            .row_adj_inserts
            .len()
            .checked_mul(std::mem::size_of::<&OwnedAdjEntry>())
            .and_then(|n| {
                finalized
                    .row_adj_deletes
                    .len()
                    .checked_mul(std::mem::size_of::<&(OwnedAdjEntry, TxId)>())
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or_else(|| Error::Other("row graph restamp index size overflow".into()))?;
        let count = finalized
            .row_adj_inserts
            .len()
            .checked_add(finalized.row_adj_deletes.len())
            .ok_or_else(|| Error::Other("row graph restamp count overflow".into()))?;
        let slots = slots
            .checked_add(count)
            .ok_or_else(|| Error::Other("row graph restamp allocation overflow".into()))?;
        let _memory = OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&working.finalization.accountant),
            slots,
            "graph",
            "received_row_edge_restamp",
            "Raise MEMORY_LIMIT to restamp the accepted row witnesses.",
        )?;
        let mut seen = vec![0u8; count];
        let mut inserts: Vec<_> = finalized.row_adj_inserts.iter().collect();
        let mut deletes: Vec<_> = finalized.row_adj_deletes.iter().collect();
        inserts.sort_unstable_by(|a, b| a.owner.cmp(&b.owner));
        deletes.sort_unstable_by(|a, b| a.0.owner.cmp(&b.0.owner));
        if inserts.windows(2).any(|p| p[0].owner == p[1].owner)
            || deletes.windows(2).any(|p| p[0].0.owner == p[1].0.owner)
        {
            return Err(Error::SyncError(
                "received row graph repeats an exact owner".into()));
        }
        // Both directions are still private. No live graph state changes if
        // validation refuses; the detached image is simply discarded.
        for adjacency in [
            &working.graph_store.forward_adj,
            &working.graph_store.reverse_adj,
        ].into_iter().filter(|_| !working.graph_store.is_source_backed()) {
            seen.fill(0);
            for graph in adjacency.write().values_mut().flatten() {
                let Some(owner) = graph.row_owner.as_mut() else {
                    continue;
                };
                if let Ok(at) = inserts.binary_search_by(|edge| edge.owner.cmp(owner)) {
                    if seen[at] != 0 {
                        return Err(Error::SyncError(
                            "received graph image repeats an inserted owner".into()));
                    }
                    seen[at] = 1;
                    let before = inserts[at];
                    if graph.entry != before.entry {
                        return Err(Error::SyncError(
                            "received insert witness differs from its exact staged owner".into()));
                    }
                    let accepted_lsn = position(owner);
                    owner.created_tx = receiver_tx;
                    owner.lsn = accepted_lsn;
                    graph.entry.created_tx = receiver_tx;
                    graph.entry.lsn = accepted_lsn;
                    graph.entry.deleted_tx = None;
                } else if let Ok(at) = deletes.binary_search_by(|edge| edge.0.owner.cmp(owner)) {
                    let marker = inserts.len() + at;
                    if seen[marker] != 0 {
                        return Err(Error::SyncError(
                            "received graph image repeats a deleted owner".into()));
                    }
                    seen[marker] = 1;
                    let (before, detached_tx) = deletes[at];
                    if graph.entry.source != before.entry.source
                        || graph.entry.target != before.entry.target
                        || graph.entry.edge_type != before.entry.edge_type
                        || graph.entry.created_tx != before.entry.created_tx
                        || graph.entry.lsn != before.entry.lsn
                        || graph.entry.deleted_tx != Some(*detached_tx)
                        || !graph.entry.properties.is_empty()
                    {
                        return Err(Error::SyncError(
                            "received delete witness differs from its exact staged owner".into()));
                    }
                    graph.entry.deleted_tx = Some(receiver_tx);
                }
            }
            if seen.iter().any(|mark| *mark != 1) {
                return Err(Error::SyncError(
                    "received graph image is missing a finalized owner".into()));
            }
        }
        drop(inserts);
        drop(deletes);
        for owned in &mut finalized.row_adj_inserts {
            let accepted_lsn = position(&owned.owner);
            owned.owner.created_tx = receiver_tx;
            owned.owner.lsn = accepted_lsn;
            owned.entry.created_tx = receiver_tx;
            owned.entry.lsn = accepted_lsn;
            owned.entry.deleted_tx = None;
        }
        for (_, deleted_tx) in &mut finalized.row_adj_deletes {
            *deleted_tx = receiver_tx;
        }
        Ok(())
    }
}
