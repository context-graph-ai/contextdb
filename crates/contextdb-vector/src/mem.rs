use crate::{cosine::cosine_similarity, store::VectorStore};
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use roaring::RoaringTreemap;
use std::sync::Arc;

pub struct MemVectorExecutor<S: WriteSetApplicator> {
    store: Arc<VectorStore>,
    tx_mgr: Arc<TxManager<S>>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    pub fn new(store: Arc<VectorStore>, tx_mgr: Arc<TxManager<S>>) -> Self {
        Self { store, tx_mgr }
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
        Ok(scored)
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
