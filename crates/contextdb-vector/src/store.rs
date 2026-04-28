use crate::HnswIndex;
use contextdb_core::{
    Error, MemoryAccountant, Result, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

pub struct IndexState {
    dimension: usize,
    quantization: VectorQuantization,
    vectors: RwLock<Vec<VectorEntry>>,
    hnsw: OnceLock<RwLock<Option<HnswIndex>>>,
    hnsw_bytes: AtomicUsize,
}

impl IndexState {
    fn new(dimension: usize, quantization: VectorQuantization) -> Self {
        Self {
            dimension,
            quantization,
            vectors: RwLock::new(Vec::new()),
            hnsw: OnceLock::new(),
            hnsw_bytes: AtomicUsize::new(0),
        }
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn quantization(&self) -> VectorQuantization {
        self.quantization
    }

    pub fn vector_count(&self) -> usize {
        self.vectors
            .read()
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .count()
    }

    pub fn byte_count(&self) -> usize {
        let payload_bytes = self
            .vectors
            .read()
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .map(|entry| self.quantization.storage_bytes(entry.vector.len()))
            .sum::<usize>();
        payload_bytes.saturating_add(self.hnsw_bytes.load(Ordering::SeqCst))
    }

    pub fn all_entries(&self) -> Vec<VectorEntry> {
        self.vectors.read().clone()
    }

    pub fn find_by_row_id(&self, row_id: RowId) -> Option<VectorEntry> {
        self.vectors
            .read()
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
            .cloned()
    }

    fn push_entry(&self, entry: VectorEntry) {
        self.vectors.write().push(entry);
    }

    fn tombstone_row(&self, row_id: RowId, deleted_tx: TxId) -> usize {
        let mut released = 0usize;
        let mut vectors = self.vectors.write();
        for entry in vectors.iter_mut() {
            if entry.row_id == row_id && entry.deleted_tx.is_none() {
                released = released.saturating_add(entry.estimated_bytes());
                entry.deleted_tx = Some(deleted_tx);
            }
        }
        released
    }

    fn rewrite_entry_index(&self, index: &VectorIndexRef) {
        for entry in self.vectors.write().iter_mut() {
            entry.index = index.clone();
        }
    }

    pub fn clear_hnsw(&self, accountant: &MemoryAccountant) {
        let bytes = self.hnsw_bytes.swap(0, Ordering::SeqCst);
        if bytes > 0 {
            accountant.release(bytes);
        }
        if let Some(lock) = self.hnsw.get() {
            *lock.write() = None;
        }
    }

    pub fn hnsw_len(&self) -> Option<usize> {
        self.hnsw
            .get()
            .and_then(|lock| lock.read().as_ref().map(|hnsw| hnsw.len()))
    }

    pub fn hnsw_stats(&self) -> Option<crate::HnswGraphStats> {
        self.hnsw
            .get()
            .and_then(|lock| lock.read().as_ref().map(|hnsw| hnsw.graph_stats()))
    }

    pub fn set_hnsw(&self, hnsw: Option<HnswIndex>, bytes: usize) {
        if hnsw.is_some() {
            self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        }
        let lock = self.hnsw.get_or_init(|| RwLock::new(None));
        *lock.write() = hnsw;
    }

    pub fn set_hnsw_bytes(&self, bytes: usize) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
    }

    pub fn hnsw(&self) -> &OnceLock<RwLock<Option<HnswIndex>>> {
        &self.hnsw
    }
}

pub struct VectorIndexInfo {
    pub index: VectorIndexRef,
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub vector_count: usize,
    pub bytes: usize,
}

pub struct VectorStore {
    registry: RwLock<HashMap<VectorIndexRef, Arc<IndexState>>>,
    build_mutex: Mutex<()>,
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new(Arc::new(OnceLock::new()))
    }
}

impl VectorStore {
    pub fn new(_legacy_hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>) -> Self {
        Self {
            registry: RwLock::new(HashMap::new()),
            build_mutex: Mutex::new(()),
        }
    }

    pub fn register_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        let mut registry = self.registry.write();
        registry
            .entry(index)
            .or_insert_with(|| Arc::new(IndexState::new(dimension, quantization)));
    }

    pub fn register_or_reconfigure_empty_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        let mut registry = self.registry.write();
        match registry.get(&index) {
            Some(state) if !state.all_entries().is_empty() => {}
            Some(state) if state.dimension() == dimension => {}
            Some(_) | None => {
                registry.insert(index, Arc::new(IndexState::new(dimension, quantization)));
            }
        }
    }

    pub fn deregister_index(&self, index: &VectorIndexRef, accountant: &MemoryAccountant) {
        if let Some(state) = self.registry.write().remove(index) {
            state.clear_hnsw(accountant);
        }
    }

    pub fn deregister_table(&self, table: &str, accountant: &MemoryAccountant) {
        let removed = {
            let mut registry = self.registry.write();
            let keys = registry
                .keys()
                .filter(|index| index.table == table)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| registry.remove(&key))
                .collect::<Vec<_>>()
        };
        for state in removed {
            state.clear_hnsw(accountant);
        }
    }

    pub fn rename_index(&self, old: &VectorIndexRef, new: VectorIndexRef) -> Result<()> {
        let mut registry = self.registry.write();
        if registry.contains_key(&new) {
            return Err(Error::Other(format!(
                "vector index already exists: {}.{}",
                new.table, new.column
            )));
        }
        let state = registry
            .remove(old)
            .ok_or_else(|| Error::UnknownVectorIndex { index: old.clone() })?;
        state.rewrite_entry_index(&new);
        registry.insert(new, state);
        Ok(())
    }

    pub fn state(&self, index: &VectorIndexRef) -> Result<Arc<IndexState>> {
        self.registry
            .read()
            .get(index)
            .cloned()
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })
    }

    pub fn try_state(&self, index: &VectorIndexRef) -> Option<Arc<IndexState>> {
        self.registry.read().get(index).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.registry.read().is_empty()
    }

    pub fn index_count(&self) -> usize {
        self.registry.read().len()
    }

    pub fn validate_vector(&self, index: &VectorIndexRef, actual: usize) -> Result<()> {
        let state = self.state(index)?;
        let expected = state.dimension();
        if expected != actual {
            return Err(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected,
                actual,
            });
        }
        Ok(())
    }

    pub fn apply_inserts(&self, inserts: Vec<VectorEntry>) {
        for entry in inserts {
            if let Some(state) = self.try_state(&entry.index) {
                state.push_entry(entry.clone());
                if let Some(lock) = state.hnsw().get() {
                    let guard = lock.write();
                    if let Some(hnsw) = guard.as_ref() {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            hnsw.insert(entry.row_id, &entry.vector);
                        }));
                    }
                }
            }
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(VectorIndexRef, RowId, TxId)>) {
        for (index, row_id, deleted_tx) in deletes {
            if let Some(state) = self.try_state(&index) {
                state.tombstone_row(row_id, deleted_tx);
                if let Some(lock) = state.hnsw().get() {
                    *lock.write() = None;
                }
            }
        }
    }

    pub fn apply_moves(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
    ) {
        for (index, old_row_id, new_row_id, tx) in moves {
            if let Some(state) = self.try_state(&index)
                && let Some(old) = state.find_by_row_id(old_row_id)
                && old.deleted_tx.is_none()
            {
                state.tombstone_row(old_row_id, tx);
                let mut moved = old;
                moved.row_id = new_row_id;
                moved.created_tx = tx;
                moved.deleted_tx = None;
                moved.lsn = lsn;
                state.push_entry(moved.clone());
                if let Some(lock) = state.hnsw().get() {
                    let guard = lock.write();
                    if let Some(hnsw) = guard.as_ref() {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            hnsw.insert(moved.row_id, &moved.vector);
                        }));
                    }
                }
            }
        }
    }

    pub fn insert_loaded_vector(&self, entry: VectorEntry) {
        let quantization = self
            .try_state(&entry.index)
            .map(|state| state.quantization())
            .unwrap_or(VectorQuantization::F32);
        self.register_or_reconfigure_empty_index(
            entry.index.clone(),
            entry.vector.len(),
            quantization,
        );
        if let Some(state) = self.try_state(&entry.index) {
            state.push_entry(entry);
        }
    }

    pub fn all_entries(&self) -> Vec<VectorEntry> {
        self.registry
            .read()
            .values()
            .flat_map(|state| state.all_entries())
            .collect()
    }

    pub fn prune_row_ids(
        &self,
        row_ids: &std::collections::HashSet<RowId>,
        accountant: &MemoryAccountant,
    ) -> usize {
        let mut released = 0usize;
        for state in self.registry.read().values() {
            let mut vectors = state.vectors.write();
            vectors.retain(|entry| {
                if row_ids.contains(&entry.row_id) {
                    released = released.saturating_add(entry.estimated_bytes());
                    false
                } else {
                    true
                }
            });
            drop(vectors);
            state.clear_hnsw(accountant);
        }
        released
    }

    pub fn entries_for_index(&self, index: &VectorIndexRef) -> Result<Vec<VectorEntry>> {
        Ok(self.state(index)?.all_entries())
    }

    pub fn vector_count(&self) -> usize {
        self.registry
            .read()
            .values()
            .map(|state| state.vector_count())
            .sum()
    }

    pub fn has_hnsw_index(&self) -> bool {
        self.registry
            .read()
            .values()
            .any(|state| state.hnsw_len().is_some())
    }

    pub fn has_hnsw_index_for(&self, index: &VectorIndexRef) -> bool {
        self.try_state(index)
            .and_then(|state| state.hnsw_len())
            .is_some()
    }

    pub fn clear_hnsw(&self, accountant: &MemoryAccountant) {
        for state in self.registry.read().values() {
            state.clear_hnsw(accountant);
        }
    }

    pub fn clear_hnsw_for(&self, index: &VectorIndexRef, accountant: &MemoryAccountant) {
        if let Some(state) = self.try_state(index) {
            state.clear_hnsw(accountant);
        }
    }

    pub fn find_by_row_id(&self, row_id: RowId) -> Option<VectorEntry> {
        self.registry
            .read()
            .values()
            .find_map(|state| state.find_by_row_id(row_id))
    }

    pub fn live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VectorEntry> {
        self.try_state(index).and_then(|state| {
            state
                .all_entries()
                .into_iter()
                .rev()
                .find(|entry| entry.row_id == row_id && entry.visible_at(snapshot))
        })
    }

    pub fn live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Vec<VectorEntry> {
        self.registry
            .read()
            .values()
            .flat_map(|state| state.all_entries())
            .filter(|entry| entry.row_id == row_id && entry.visible_at(snapshot))
            .collect()
    }

    pub fn vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: contextdb_core::Lsn,
    ) -> Option<Vec<f32>> {
        self.try_state(index).and_then(|state| {
            state
                .all_entries()
                .into_iter()
                .find(|entry| entry.row_id == row_id && entry.lsn == lsn)
                .map(|entry| entry.vector)
        })
    }

    pub fn index_infos(&self) -> Vec<VectorIndexInfo> {
        let mut infos = self
            .registry
            .read()
            .iter()
            .map(|(index, state)| VectorIndexInfo {
                index: index.clone(),
                dimension: state.dimension(),
                quantization: state.quantization(),
                vector_count: state.vector_count(),
                bytes: state.byte_count(),
            })
            .collect::<Vec<_>>();
        infos.sort_by(|a, b| {
            a.index
                .table
                .cmp(&b.index.table)
                .then(a.index.column.cmp(&b.index.column))
        });
        infos
    }

    pub fn build_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.build_mutex.lock()
    }
}
