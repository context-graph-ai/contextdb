use crate::{HnswIndex, cosine::cosine_similarity, store::VectorStore};
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use parking_lot::RwLock;
use roaring::RoaringTreemap;
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

        let mut scored: Vec<(RowId, f32)> = Vec::new();
        for entry in state.all_entries().iter() {
            if !entry.visible_at(snapshot) {
                continue;
            }

            if let Some(cands) = candidates
                && !cands.contains(entry.row_id.0)
            {
                continue;
            }

            let sim = cosine_similarity(query, &entry.vector);
            scored.push((entry.row_id, sim));
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        Ok(scored)
    }

    fn build_hnsw_from_store(&self, index: &VectorIndexRef) -> Option<HnswIndex> {
        let _build_guard = self.store.build_lock();
        let state = self.store.try_state(index)?;
        let entries = state.all_entries();
        let dim = state.dimension();
        let estimated_bytes = estimate_hnsw_bytes(entries.len(), dim, state.quantization());
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
            HnswIndex::new(&entries, dim)
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
            if self.store.is_empty() {
                return Ok(Vec::new());
            }
            return Err(Error::UnknownVectorIndex { index });
        };
        if state.all_entries().is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != state.dimension() {
            if self.store.index_count() == 1 {
                return Err(Error::VectorDimensionMismatch {
                    expected: state.dimension(),
                    got: query.len(),
                });
            }
            return Err(Error::VectorIndexDimensionMismatch {
                index,
                expected: state.dimension(),
                actual: query.len(),
            });
        }

        let use_hnsw = state.all_entries().len() >= HNSW_THRESHOLD;
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
                let raw_candidates = match hnsw.search(query, k) {
                    Ok(raw) => raw,
                    Err(Error::VectorDimensionMismatch { expected, got }) => {
                        return Err(Error::VectorIndexDimensionMismatch {
                            index,
                            expected,
                            actual: got,
                        });
                    }
                    Err(err) => return Err(err),
                };

                if raw_candidates.len() < hnsw.len() {
                    return self.brute_force_search(&index, query, k, candidates, snapshot);
                }

                let entries = state.all_entries();
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
                                Some((entry.row_id, cosine_similarity(query, &entry.vector)))
                            })
                    })
                    .collect::<Vec<_>>();

                visible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
        if self.store.try_state(&index).is_none() {
            self.store
                .register_index(index.clone(), vector.len(), VectorQuantization::F32);
        }
        self.store
            .validate_vector(&index, vector.len())
            .map_err(|err| {
                if let Error::VectorIndexDimensionMismatch {
                    expected, actual, ..
                } = err
                {
                    Error::VectorDimensionMismatch {
                        expected,
                        got: actual,
                    }
                } else {
                    err
                }
            })?;
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
    entry_count
        .saturating_mul(quantization.storage_bytes(dimension))
        .saturating_mul(3)
}
