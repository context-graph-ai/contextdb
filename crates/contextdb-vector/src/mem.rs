use crate::{HnswIndex, store::VectorStore};
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

const HNSW_THRESHOLD: usize = 1000;

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

    fn brute_force_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        let state = self.store.state(index)?;
        if query.len() != state.dimension() {
            return Err(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: state.dimension(),
                actual: query.len(),
            });
        }

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
        Ok(scored)
    }

    fn build_hnsw_from_store(&self, index: &VectorIndexRef) -> Option<HnswIndex> {
        let _build_guard = self.store.build_lock();
        let state = self.store.try_state(index)?;
        let dim = state.dimension();
        let entry_count = state.entry_count();
        let estimated_bytes = estimate_hnsw_bytes(entry_count, dim, state.quantization());
        if self
            .accountant
            .try_allocate_for(
                estimated_bytes,
                "vector_index",
                &format!("build_hnsw@{}.{}", index.table, index.column),
                "Reduce vector volume or raise MEMORY_LIMIT so the HNSW index can be built.",
            )
            .is_err()
        {
            return None;
        }

        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.with_entries(|entries| HnswIndex::new(entries, dim, state.quantization()))
        }))
        .ok();
        if built.is_none() {
            self.accountant.release(estimated_bytes);
        } else {
            state.set_hnsw_bytes(estimated_bytes);
        }
        built
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
        if k == 0 {
            return Ok(Vec::new());
        }
        let Some(state) = self.store.try_state(&index) else {
            return Err(Error::UnknownVectorIndex { index });
        };
        if query.len() != state.dimension() {
            return Err(Error::VectorIndexDimensionMismatch {
                index,
                expected: state.dimension(),
                actual: query.len(),
            });
        }
        if state.entry_count() == 0 {
            return Ok(Vec::new());
        }

        let use_hnsw = state.entry_count() >= HNSW_THRESHOLD;
        if use_hnsw {
            let lock = state
                .hnsw()
                .get_or_init(|| RwLock::new(self.build_hnsw_from_store(&index)));

            {
                let mut guard = lock.write();
                if guard.is_none() {
                    *guard = self.build_hnsw_from_store(&index);
                }
            }

            let guard = lock.read();
            if let Some(hnsw) = guard.as_ref() {
                let raw_candidates = hnsw.search(&index, query, k)?;
                let raw_candidate_count = raw_candidates.len();

                if candidates.is_some() && raw_candidates.len() < hnsw.len() {
                    return self.brute_force_search(&index, query, k, candidates, snapshot);
                }

                let supplement_missing = raw_candidate_count.saturating_add(64) >= hnsw.len();
                let raw_row_ids = if supplement_missing {
                    raw_candidates
                        .iter()
                        .map(|(row_id, _)| *row_id)
                        .collect::<HashSet<_>>()
                } else {
                    HashSet::new()
                };
                let mut visible = state.with_entries(|entries| {
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
                    if supplement_missing {
                        for entry in entries {
                            if raw_row_ids.contains(&entry.row_id) || !entry.visible_at(snapshot) {
                                continue;
                            }
                            visible.push((entry.row_id, entry.vector.cosine_similarity(query)));
                        }
                    }
                    visible
                });

                visible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                if visible.len() < k && raw_candidate_count < hnsw.len() {
                    return self.brute_force_search(&index, query, k, candidates, snapshot);
                }
                visible.truncate(k);
                return Ok(visible);
            }
        }

        self.brute_force_search(&index, query, k, candidates, snapshot)
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
            index,
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_inserts.push(entry);
        })?;

        Ok(())
    }

    fn delete_vector(&self, tx: TxId, index: VectorIndexRef, row_id: RowId) -> Result<()> {
        self.store.state(&index)?;
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_deletes.push((index, row_id, tx));
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
    entry_count.saturating_mul(entry_bytes).saturating_mul(3)
}
