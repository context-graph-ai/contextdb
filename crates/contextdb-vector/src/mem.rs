use crate::{HnswIndex, store::VectorStore};
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

const HNSW_THRESHOLD: usize = 1000;

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchDebugTrace {
    pub index: VectorIndexRef,
    pub used_hnsw: bool,
    pub hnsw_len: Option<usize>,
    pub hnsw_candidate_count: usize,
    pub hnsw_candidate_row_ids: Vec<RowId>,
    pub final_row_ids: Vec<RowId>,
    pub supplemented_row_count: usize,
    pub fallback_reason: Option<&'static str>,
}

impl VectorSearchDebugTrace {
    fn brute_force(
        index: &VectorIndexRef,
        rows: &[(RowId, f32)],
        fallback_reason: &'static str,
        hnsw_len: Option<usize>,
    ) -> Self {
        Self {
            index: index.clone(),
            used_hnsw: false,
            hnsw_len,
            hnsw_candidate_count: 0,
            hnsw_candidate_row_ids: Vec::new(),
            final_row_ids: rows.iter().map(|(row_id, _)| *row_id).collect(),
            supplemented_row_count: 0,
            fallback_reason: Some(fallback_reason),
        }
    }

    fn hnsw(
        index: &VectorIndexRef,
        hnsw_len: usize,
        hnsw_candidate_row_ids: Vec<RowId>,
        rows: &[(RowId, f32)],
        supplemented_row_count: usize,
    ) -> Self {
        Self {
            index: index.clone(),
            used_hnsw: true,
            hnsw_len: Some(hnsw_len),
            hnsw_candidate_count: hnsw_candidate_row_ids.len(),
            hnsw_candidate_row_ids,
            final_row_ids: rows.iter().map(|(row_id, _)| *row_id).collect(),
            supplemented_row_count,
            fallback_reason: None,
        }
    }
}

pub struct MemVectorExecutor<S: WriteSetApplicator> {
    store: Arc<VectorStore>,
    tx_mgr: Arc<TxManager<S>>,
    accountant: Arc<MemoryAccountant>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    pub fn new(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TxManager<S>>,
        hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
    ) -> Self {
        Self::new_with_accountant(store, tx_mgr, hnsw, Arc::new(MemoryAccountant::no_limit()))
    }

    pub fn new_with_accountant(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TxManager<S>>,
        _hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: Arc<MemoryAccountant>,
    ) -> Self {
        Self {
            store,
            tx_mgr,
            accountant,
        }
    }

    fn brute_force_search_state(
        &self,
        _index: &VectorIndexRef,
        state: &crate::store::IndexState,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Vec<(RowId, f32)> {
        let mut scored: Vec<(RowId, f32)> = state.with_entries(|entries| {
            let mut scored = Vec::new();
            for entry in entries {
                if !entry.visible_at(snapshot) {
                    continue;
                }

                if let Some(cands) = candidates
                    && !cands.contains(entry.row_id.0)
                {
                    continue;
                }

                let sim = entry.vector.cosine_similarity(query);
                scored.push((entry.row_id, sim));
            }
            scored
        });

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    fn brute_force_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.store.with_registered_state(index, |state| {
            if query.len() != state.dimension() {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: index.clone(),
                    expected: state.dimension(),
                    actual: query.len(),
                });
            }

            Ok(self.brute_force_search_state(index, &state, query, k, candidates, snapshot))
        })?
    }

    fn build_hnsw_from_state(
        &self,
        index: &VectorIndexRef,
        state: &crate::store::IndexState,
    ) -> Option<HnswIndex> {
        let dim = state.dimension();
        let entry_count = state.vector_count();
        let final_bytes = estimate_hnsw_bytes(entry_count, dim, state.quantization());
        let reservation_bytes =
            estimate_hnsw_build_reservation(entry_count, dim, state.quantization());
        if self
            .accountant
            .try_allocate_for(
                reservation_bytes,
                "vector_index",
                &format!("build_hnsw@{}.{}", index.table, index.column),
                "Reduce vector volume or raise MEMORY_LIMIT so the HNSW index can be built.",
            )
            .is_err()
        {
            return None;
        }

        #[cfg(feature = "test-seams")]
        self.store
            .pause_registry()
            .maybe_pause(index, crate::test_seam::PauseWindow::Build);

        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.with_entries(|entries| HnswIndex::new(entries, dim, state.quantization()))
        }))
        .ok();
        if built.is_none() {
            self.accountant.release(reservation_bytes);
        } else {
            self.accountant
                .release(reservation_bytes.saturating_sub(final_bytes));
            state.set_hnsw_bytes_with_accountant(final_bytes, self.accountant.clone());
        }
        built
    }

    fn search_with_strategy(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.store.with_bulk_read(|| {
            if k == 0 {
                return Ok((
                    Vec::new(),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_limit", None),
                ));
            }
            enum BuildOutcome {
                Ready,
                Fallback,
            }
            enum SearchStep {
                Done(Vec<(RowId, f32)>, VectorSearchDebugTrace),
                BuildHnsw,
            }

            loop {
                let snapshot_tx = TxId::from_snapshot(snapshot);
                let step = self.store.with_registered_state(&index, |state| {
                    if query.len() != state.dimension() {
                        return Err(Error::VectorIndexDimensionMismatch {
                            index: index.clone(),
                            expected: state.dimension(),
                            actual: query.len(),
                        });
                    }
                    if state.entry_count() == 0 {
                        return Ok(SearchStep::Done(
                            Vec::new(),
                            VectorSearchDebugTrace::brute_force(&index, &[], "empty_index", None),
                        ));
                    }

                    if state.max_tx() > snapshot_tx {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "snapshot_has_newer_vectors",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }
                    if state.vector_count() < HNSW_THRESHOLD {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "below_hnsw_threshold",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }

                    let Some(lock) = state.hnsw().get() else {
                        return Ok(SearchStep::BuildHnsw);
                    };
                    let guard = lock.read();
                    let Some(hnsw) = guard.as_ref() else {
                        return Ok(SearchStep::BuildHnsw);
                    };
                    #[cfg(feature = "test-seams")]
                    self.store
                        .pause_registry()
                        .maybe_pause(&index, crate::test_seam::PauseWindow::Search);

                    let raw_candidates = hnsw.search(&index, query, k)?;
                    let hnsw_len = hnsw.len();
                    let hnsw_candidate_row_ids = raw_candidates
                        .iter()
                        .map(|(row_id, _)| *row_id)
                        .collect::<Vec<_>>();
                    let raw_candidate_count = raw_candidates.len();

                    if candidates.is_some() && raw_candidates.len() < hnsw_len {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "candidate_filter_requires_exact_scan",
                            Some(hnsw_len),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }

                    let supplement_missing = raw_candidate_count.saturating_add(64) >= hnsw_len;
                    let raw_row_ids = if supplement_missing {
                        raw_candidates
                            .iter()
                            .map(|(row_id, _)| *row_id)
                            .collect::<HashSet<_>>()
                    } else {
                        HashSet::new()
                    };
                    let (mut visible, supplemented_row_count) = state.with_entries(|entries| {
                        let mut visible = raw_candidates
                            .into_iter()
                            .filter_map(|(rid, _)| {
                                entries
                                    .iter()
                                    .find(|entry| entry.row_id == rid && entry.visible_at(snapshot))
                                    .and_then(|entry| {
                                        if let Some(cands) = candidates
                                            && !cands.contains(entry.row_id.0)
                                        {
                                            return None;
                                        }
                                        Some((entry.row_id, entry.vector.cosine_similarity(query)))
                                    })
                            })
                            .collect::<Vec<_>>();
                        let mut supplemented_row_count = 0usize;
                        if supplement_missing {
                            for entry in entries {
                                if raw_row_ids.contains(&entry.row_id)
                                    || !entry.visible_at(snapshot)
                                {
                                    continue;
                                }
                                visible.push((entry.row_id, entry.vector.cosine_similarity(query)));
                                supplemented_row_count += 1;
                            }
                        }
                        (visible, supplemented_row_count)
                    });

                    visible
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    if visible.len() < k && raw_candidate_count < hnsw_len {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "hnsw_candidates_insufficient",
                            Some(hnsw_len),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }
                    visible.truncate(k);
                    let trace = VectorSearchDebugTrace::hnsw(
                        &index,
                        hnsw_len,
                        hnsw_candidate_row_ids,
                        &visible,
                        supplemented_row_count,
                    );
                    Ok(SearchStep::Done(visible, trace))
                })??;

                match step {
                    SearchStep::Done(rows, trace) => return Ok((rows, trace)),
                    SearchStep::BuildHnsw => {
                        let outcome = self.store.with_index_maintenance(&index, || {
                            let Some(current) = self.store.try_state(&index) else {
                                return Err(Error::UnknownVectorIndex {
                                    index: index.clone(),
                                });
                            };
                            if current.max_tx() > snapshot_tx
                                || current.vector_count() < HNSW_THRESHOLD
                            {
                                return Ok(BuildOutcome::Fallback);
                            }
                            let current_lock = current.hnsw().get_or_init(|| RwLock::new(None));
                            let mut guard = current_lock.write();
                            if guard.is_none() {
                                let Some(hnsw) = self.build_hnsw_from_state(&index, &current)
                                else {
                                    return Ok(BuildOutcome::Fallback);
                                };
                                *guard = Some(hnsw);
                            }
                            Ok(BuildOutcome::Ready)
                        })?;
                        match outcome {
                            BuildOutcome::Ready => continue,
                            BuildOutcome::Fallback => {
                                let rows = self
                                    .brute_force_search(&index, query, k, candidates, snapshot)?;
                                let trace = VectorSearchDebugTrace::brute_force(
                                    &index,
                                    &rows,
                                    "hnsw_build_unavailable",
                                    None,
                                );
                                return Ok((rows, trace));
                            }
                        }
                    }
                }
            }
        })
    }

    #[doc(hidden)]
    pub fn search_with_strategy_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.search_with_strategy(index, query, k, candidates, snapshot)
    }
}

impl<S: WriteSetApplicator> VectorExecutor for MemVectorExecutor<S> {
    fn search(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.search_with_strategy(index, query, k, candidates, snapshot)
            .map(|(rows, _)| rows)
    }

    fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<()> {
        self.store.validate_vector(&index, vector.len())?;
        let entry = VectorEntry {
            index: index.clone(),
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };
        let existing_live = self
            .store
            .live_entry_for_row(&index, row_id, self.tx_mgr.snapshot())
            .is_some();

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_inserts
                .retain(|pending| !(pending.index == index && pending.row_id == row_id));
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
            if existing_live && !already_deleted {
                ws.vector_deletes.push((index.clone(), row_id, tx));
            }
            ws.vector_inserts.push(entry);
        })?;

        Ok(())
    }

    fn delete_vector(&self, tx: TxId, index: VectorIndexRef, row_id: RowId) -> Result<()> {
        self.store.state(&index)?;
        let existing_live = self
            .store
            .live_entry_for_row(&index, row_id, self.tx_mgr.snapshot())
            .is_some();
        self.tx_mgr.with_write_set(tx, |ws| {
            let insert_count = ws.vector_inserts.len();
            ws.vector_inserts
                .retain(|entry| !(entry.index == index && entry.row_id == row_id));
            let canceled_insert = ws.vector_inserts.len() != insert_count;
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
            let canceled_move_to_row = !moved_sources.is_empty();
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
            if !pending_move_from_row
                && ((!canceled_insert && !canceled_move_to_row) || existing_live)
                && !already_deleted
            {
                ws.vector_deletes.push((index, row_id, tx));
            }
        })?;

        Ok(())
    }
}

fn estimate_hnsw_bytes(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let entry_bytes = match quantization {
        VectorQuantization::F32 => quantization.storage_bytes(dimension),
        VectorQuantization::SQ8 => dimension.saturating_add(12),
        VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(12),
    };
    let exact_key_bytes = entry_bytes
        .saturating_add(std::mem::size_of::<RowId>())
        .saturating_add(64);
    entry_count.saturating_mul(
        entry_bytes
            .saturating_mul(3)
            .saturating_add(exact_key_bytes),
    )
}

fn estimate_hnsw_build_reservation(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let final_bytes = estimate_hnsw_bytes(entry_count, dimension, quantization);
    let (m, ef_construction, max_level_bound) = match quantization {
        VectorQuantization::F32 => match entry_count {
            0..=5000 => (16usize, 200usize, 16usize),
            5001..=50000 => (24, 400, 16),
            _ => (16, 200, 16),
        },
        _ => match entry_count {
            0..=5000 => (8usize, 32usize, 16usize),
            5001..=50000 => (12, 64, 16),
            _ => (12, 64, 16),
        },
    };
    let stored_vector_bytes = quantization.storage_bytes(dimension);
    let word = std::mem::size_of::<usize>();
    let sorted_entry_refs = entry_count.saturating_mul(word);
    let cloned_vectors_and_refs = entry_count.saturating_mul(
        stored_vector_bytes
            .saturating_add(word.saturating_mul(5))
            .saturating_add(std::mem::size_of::<RowId>()),
    );
    let map_and_exact_key_overhead = entry_count.saturating_mul(
        std::mem::size_of::<RowId>()
            .saturating_add(word.saturating_mul(3))
            .saturating_add(64),
    );
    let graph_link_upper_bound = entry_count
        .saturating_mul(m)
        .saturating_mul(max_level_bound)
        .saturating_mul(word.saturating_add(std::mem::size_of::<f32>()));
    let construction_scratch = entry_count.min(ef_construction).saturating_mul(
        word.saturating_mul(6)
            .saturating_add(std::mem::size_of::<f32>().saturating_mul(2)),
    );
    final_bytes
        .saturating_add(sorted_entry_refs)
        .saturating_add(cloned_vectors_and_refs)
        .saturating_add(map_and_exact_key_overhead)
        .saturating_add(graph_link_upper_bound)
        .saturating_add(construction_scratch)
}
