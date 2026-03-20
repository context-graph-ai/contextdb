use contextdb_core::{RowId, VectorEntry};
use parking_lot::RwLock;

pub struct VectorStore {
    pub vectors: RwLock<Vec<VectorEntry>>,
    pub dimension: RwLock<Option<usize>>,
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            vectors: RwLock::new(Vec::new()),
            dimension: RwLock::new(None),
        }
    }

    pub fn apply_inserts(&self, inserts: Vec<VectorEntry>) {
        let mut vectors = self.vectors.write();
        for entry in inserts {
            vectors.push(entry);
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
}
