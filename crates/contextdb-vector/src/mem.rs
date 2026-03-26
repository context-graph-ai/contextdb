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
    hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    pub fn new(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TxManager<S>>,
        hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
    ) -> Self {
        Self {
            store,
            tx_mgr,
            hnsw,
        }
    }

    fn brute_force_search(
        &self,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Vec<(RowId, f32)> {
        let vectors = self.store.vectors.read();
        let mut scored: Vec<(RowId, f32)> = Vec::new();

        for entry in vectors.iter() {
            if !entry.visible_at(snapshot) {
                continue;
            }

            if let Some(cands) = candidates
                && !cands.contains(entry.row_id)
            {
                continue;
            }

            let sim = cosine_similarity(query, &entry.vector);
            scored.push((entry.row_id, sim));
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    fn build_hnsw_from_store(&self) -> Option<HnswIndex> {
        let entries = self.store.all_entries();
        let dim = self.store.dimension().unwrap_or(0);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HnswIndex::new(&entries, dim)
        }))
        .ok()
    }
}

impl<S: WriteSetApplicator> VectorExecutor for MemVectorExecutor<S> {
    fn search(
        &self,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let use_hnsw = self.store.vector_count() >= HNSW_THRESHOLD;
        if use_hnsw {
            let once_lock = self
                .hnsw
                .get_or_init(|| RwLock::new(self.build_hnsw_from_store()));

            {
                let mut guard = once_lock.write();
                if guard.is_none() {
                    *guard = self.build_hnsw_from_store();
                }
            }

            let guard = once_lock.read();
            if let Some(hnsw) = guard.as_ref() {
                let raw_candidates = hnsw.search(query, k)?;

                // If the HNSW graph has disconnected components, the search
                // may not reach all indexed vectors. Detect this and fall back
                // to brute-force so we never silently drop results.
                if raw_candidates.len() < hnsw.len() {
                    return Ok(self.brute_force_search(query, k, candidates, snapshot));
                }

                let vectors = self.store.vectors.read();
                let mut visible = raw_candidates
                    .into_iter()
                    .filter_map(|(rid, _)| {
                        vectors
                            .iter()
                            .find(|entry| entry.row_id == rid && entry.visible_at(snapshot))
                            .map(|entry| {
                                if let Some(cands) = candidates
                                    && !cands.contains(entry.row_id)
                                {
                                    return None;
                                }

                                Some((entry.row_id, cosine_similarity(query, &entry.vector)))
                            })
                            .unwrap_or(None)
                    })
                    .collect::<Vec<_>>();

                visible.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                visible.truncate(k);
                return Ok(visible);
            }
        }

        Ok(self.brute_force_search(query, k, candidates, snapshot))
    }

    fn insert_vector(&self, tx: TxId, row_id: RowId, vector: Vec<f32>) -> Result<()> {
        let got = vector.len();

        {
            let mut dim = self.store.dimension.write();
            match *dim {
                None => *dim = Some(got),
                Some(expected) if expected != got => {
                    return Err(Error::VectorDimensionMismatch { expected, got });
                }
                _ => {}
            }
        }

        let entry = VectorEntry {
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: 0,
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_inserts.push(entry);
        })?;

        Ok(())
    }

    fn delete_vector(&self, tx: TxId, row_id: RowId) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_deletes.push((row_id, tx));
        })?;

        Ok(())
    }
}
