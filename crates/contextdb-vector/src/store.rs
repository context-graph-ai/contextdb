use crate::HnswIndex;
use contextdb_core::{MemoryAccountant, RowId, VectorEntry};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

pub struct VectorStore {
    pub vectors: RwLock<Vec<VectorEntry>>,
    pub dimension: RwLock<Option<usize>>,
    pub hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
    hnsw_bytes: AtomicUsize,
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new(Arc::new(OnceLock::new()))
    }
}

impl VectorStore {
    pub fn new(hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>) -> Self {
        Self {
            vectors: RwLock::new(Vec::new()),
            dimension: RwLock::new(None),
            hnsw,
            hnsw_bytes: AtomicUsize::new(0),
        }
    }

    pub fn apply_inserts(&self, inserts: Vec<VectorEntry>) {
        {
            let mut vectors = self.vectors.write();
            for entry in &inserts {
                vectors.push(entry.clone());
            }
        }

        if let Some(rw_lock) = self.hnsw.get() {
            let guard = rw_lock.write();
            if let Some(hnsw) = guard.as_ref() {
                for entry in &inserts {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hnsw.insert(entry.row_id, &entry.vector);
                    }));
                }
            }
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(RowId, u64)>) {
        let mut vectors = self.vectors.write();
        for (row_id, deleted_tx) in deletes {
            for v in vectors.iter_mut() {
                if v.row_id == row_id && v.deleted_tx.is_none() {
                    v.deleted_tx = Some(deleted_tx);
                }
            }
        }
        drop(vectors);

        if let Some(rw_lock) = self.hnsw.get() {
            *rw_lock.write() = None;
        }
    }

    pub fn clear_hnsw(&self, accountant: &MemoryAccountant) {
        let bytes = self.hnsw_bytes.swap(0, Ordering::SeqCst);
        if bytes > 0 {
            accountant.release(bytes);
        }
        if let Some(rw_lock) = self.hnsw.get() {
            *rw_lock.write() = None;
        }
    }

    pub fn insert_loaded_vector(&self, entry: VectorEntry) {
        let dimension = entry.vector.len();
        let mut dim = self.dimension.write();
        if dim.is_none() {
            *dim = Some(dimension);
        }
        drop(dim);
        self.vectors.write().push(entry);
    }

    pub fn set_dimension(&self, dimension: usize) {
        let mut dim = self.dimension.write();
        if dim.is_none() {
            *dim = Some(dimension);
        }
    }

    pub fn vector_count(&self) -> usize {
        self.vectors.read().len()
    }

    pub fn all_entries(&self) -> Vec<VectorEntry> {
        self.vectors.read().clone()
    }

    pub fn dimension(&self) -> Option<usize> {
        *self.dimension.read()
    }

    pub fn has_hnsw_index(&self) -> bool {
        self.hnsw
            .get()
            .is_some_and(|rw_lock| rw_lock.read().as_ref().is_some())
    }

    pub fn set_hnsw_bytes(&self, bytes: usize) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
    }
}
