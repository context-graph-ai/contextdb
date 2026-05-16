use crate::{HnswIndex, quantized::StoredVectorEntry};
use contextdb_core::{
    Error, MemoryAccountant, Result, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization,
};
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

thread_local! {
    static HELD_MAINTENANCE_LOCKS: RefCell<Vec<(usize, VectorIndexRef)>> = const { RefCell::new(Vec::new()) };
    static HELD_BULK_READ_LOCKS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    static HELD_BULK_WRITE_LOCKS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

struct MaintenanceStackGuard {
    store_id: usize,
    index: VectorIndexRef,
}

impl MaintenanceStackGuard {
    fn new(store_id: usize, index: &VectorIndexRef) -> Self {
        HELD_MAINTENANCE_LOCKS.with(|held| held.borrow_mut().push((store_id, index.clone())));
        Self {
            store_id,
            index: index.clone(),
        }
    }
}

impl Drop for MaintenanceStackGuard {
    fn drop(&mut self) {
        HELD_MAINTENANCE_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(popped, Some((self.store_id, self.index.clone())));
        });
    }
}

struct BulkReadStackGuard {
    store_id: usize,
}

impl BulkReadStackGuard {
    fn new(store_id: usize) -> Self {
        HELD_BULK_READ_LOCKS.with(|held| held.borrow_mut().push(store_id));
        Self { store_id }
    }
}

impl Drop for BulkReadStackGuard {
    fn drop(&mut self) {
        HELD_BULK_READ_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.store_id));
        });
    }
}

struct BulkWriteStackGuard {
    store_id: usize,
}

impl BulkWriteStackGuard {
    fn new(store_id: usize) -> Self {
        HELD_BULK_WRITE_LOCKS.with(|held| held.borrow_mut().push(store_id));
        Self { store_id }
    }
}

impl Drop for BulkWriteStackGuard {
    fn drop(&mut self) {
        HELD_BULK_WRITE_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.store_id));
        });
    }
}

pub struct IndexState {
    dimension: usize,
    quantization: VectorQuantization,
    maintenance: Mutex<()>,
    vectors: RwLock<Vec<StoredVectorEntry>>,
    hnsw: OnceLock<RwLock<Option<HnswIndex>>>,
    hnsw_bytes: AtomicUsize,
    hnsw_accountant: RwLock<Option<Arc<MemoryAccountant>>>,
}

impl IndexState {
    fn new(dimension: usize, quantization: VectorQuantization) -> Self {
        Self {
            dimension,
            quantization,
            maintenance: Mutex::new(()),
            vectors: RwLock::new(Vec::new()),
            hnsw: OnceLock::new(),
            hnsw_bytes: AtomicUsize::new(0),
            hnsw_accountant: RwLock::new(None),
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

    pub fn max_tx(&self) -> TxId {
        self.vectors
            .read()
            .iter()
            .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
            .max()
            .unwrap_or_default()
    }

    pub fn byte_count(&self) -> usize {
        let payload_bytes = self
            .vectors
            .read()
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .map(StoredVectorEntry::estimated_bytes)
            .sum::<usize>();
        payload_bytes.saturating_add(self.hnsw_bytes.load(Ordering::SeqCst))
    }

    pub fn all_entries(&self, index: &VectorIndexRef) -> Vec<VectorEntry> {
        self.vectors
            .read()
            .iter()
            .map(|entry| entry.to_vector_entry(index.clone()))
            .collect()
    }

    pub fn find_by_row_id(&self, index: &VectorIndexRef, row_id: RowId) -> Option<VectorEntry> {
        self.vectors
            .read()
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
            .map(|entry| entry.to_vector_entry(index.clone()))
    }

    fn stored_by_row_id(&self, row_id: RowId) -> Option<StoredVectorEntry> {
        self.vectors
            .read()
            .iter()
            .rev()
            .find(|entry| entry.row_id == row_id)
            .cloned()
    }

    pub(crate) fn with_entries<R>(&self, f: impl FnOnce(&[StoredVectorEntry]) -> R) -> R {
        let entries = self.vectors.read();
        f(&entries)
    }

    pub fn entry_count(&self) -> usize {
        self.vectors.read().len()
    }

    fn stored_entry(&self, entry: VectorEntry) -> StoredVectorEntry {
        StoredVectorEntry::from_vector_entry(entry, self.quantization)
    }

    fn push_entry(&self, entry: StoredVectorEntry) {
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

    pub fn clear_hnsw(&self, accountant: &MemoryAccountant) {
        self.clear_hnsw_with_optional_accountant(Some(accountant));
    }

    fn drop_hnsw_without_accounting(&self) {
        self.clear_hnsw_with_optional_accountant(None);
    }

    fn clear_hnsw_with_optional_accountant(&self, accountant: Option<&MemoryAccountant>) {
        let bytes = self.remove_hnsw_graph();
        let recorded_accountant = self.hnsw_accountant.write().take();
        if bytes == 0 {
            return;
        }
        if let Some(accountant) = recorded_accountant {
            accountant.release(bytes);
        } else if let Some(accountant) = accountant {
            accountant.release(bytes);
        }
    }

    fn remove_hnsw_graph(&self) -> usize {
        if let Some(lock) = self.hnsw.get() {
            *lock.write() = None;
        }
        self.hnsw_bytes.swap(0, Ordering::SeqCst)
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

    pub fn raw_hnsw_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Result<Vec<(RowId, f32)>>> {
        self.hnsw.get().and_then(|lock| {
            lock.read()
                .as_ref()
                .map(|hnsw| hnsw.search(index, query, k))
        })
    }

    pub fn raw_hnsw_entry_count_for_row(&self, row_id: RowId) -> Option<usize> {
        self.hnsw.get().and_then(|lock| {
            lock.read()
                .as_ref()
                .map(|hnsw| hnsw.raw_entry_count_for_row(row_id))
        })
    }

    pub fn set_hnsw(&self, hnsw: Option<HnswIndex>, bytes: usize) {
        if hnsw.is_some() {
            self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        } else {
            self.hnsw_bytes.store(0, Ordering::SeqCst);
            self.hnsw_accountant.write().take();
        }
        let lock = self.hnsw.get_or_init(|| RwLock::new(None));
        *lock.write() = hnsw;
    }

    pub fn set_hnsw_bytes(&self, bytes: usize) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        if bytes == 0 {
            self.hnsw_accountant.write().take();
        }
    }

    pub(crate) fn set_hnsw_bytes_with_accountant(
        &self,
        bytes: usize,
        accountant: Arc<MemoryAccountant>,
    ) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        *self.hnsw_accountant.write() = Some(accountant);
    }

    pub fn hnsw(&self) -> &OnceLock<RwLock<Option<HnswIndex>>> {
        &self.hnsw
    }

    pub fn storage_bytes_per_entry(&self) -> Vec<usize> {
        self.vectors
            .read()
            .iter()
            .map(StoredVectorEntry::estimated_bytes)
            .collect()
    }
}

pub struct VectorIndexInfo {
    pub index: VectorIndexRef,
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub vector_count: usize,
    pub bytes: usize,
}

#[derive(Default)]
struct PendingVectorChanges {
    deletes: Vec<(VectorIndexRef, RowId, TxId)>,
    inserts: Vec<VectorEntry>,
    moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
}

pub struct VectorStore {
    registry: RwLock<HashMap<VectorIndexRef, Arc<IndexState>>>,
    build_mutex: Mutex<()>,
    bulk_gate: RwLock<()>,
    #[cfg(feature = "test-seams")]
    pause_registry: crate::test_seam::PauseRegistry,
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
            bulk_gate: RwLock::new(()),
            #[cfg(feature = "test-seams")]
            pause_registry: crate::test_seam::PauseRegistry::default(),
        }
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn pause_registry(&self) -> &crate::test_seam::PauseRegistry {
        &self.pause_registry
    }

    pub fn register_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        self.with_bulk_read(|| {
            let state = {
                let mut registry = self.registry.write();
                registry
                    .entry(index.clone())
                    .or_insert_with(|| Arc::new(IndexState::new(dimension, quantization)))
                    .clone()
            };
            let _index_guard = state.maintenance.lock();
            let _stack_guard = MaintenanceStackGuard::new(self as *const Self as usize, &index);
        });
    }

    fn sorted_refs(refs: impl IntoIterator<Item = VectorIndexRef>) -> Vec<VectorIndexRef> {
        let mut refs = refs.into_iter().collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs
    }

    pub(crate) fn with_index_maintenance<R>(
        &self,
        index: &VectorIndexRef,
        f: impl FnOnce() -> R,
    ) -> R {
        let store_id = self as *const Self as usize;
        let already_held = HELD_MAINTENANCE_LOCKS.with(|held| {
            held.borrow()
                .iter()
                .any(|(held_store, held_index)| *held_store == store_id && held_index == index)
        });
        if already_held {
            return f();
        }

        self.with_bulk_read(|| {
            loop {
                let Some(state) = self.try_state(index) else {
                    return f();
                };
                let _index_guard = state.maintenance.lock();
                let still_current = self
                    .registry
                    .read()
                    .get(index)
                    .is_some_and(|current| Arc::ptr_eq(current, &state));
                if !still_current {
                    continue;
                }
                let _stack_guard = MaintenanceStackGuard::new(store_id, index);
                return f();
            }
        })
    }

    fn with_index_pair_maintenance<R>(
        &self,
        first: &VectorIndexRef,
        second: &VectorIndexRef,
        f: impl FnOnce() -> R,
    ) -> R {
        if first == second {
            return self.with_index_maintenance(first, f);
        }
        let refs = Self::sorted_refs([first.clone(), second.clone()]);
        self.with_index_maintenance(&refs[0], || self.with_index_maintenance(&refs[1], f))
    }

    pub(crate) fn with_bulk_read<R>(&self, f: impl FnOnce() -> R) -> R {
        let store_id = self as *const Self as usize;
        let already_held = HELD_BULK_READ_LOCKS.with(|held| held.borrow().contains(&store_id))
            || HELD_BULK_WRITE_LOCKS.with(|held| held.borrow().contains(&store_id));
        if already_held {
            return f();
        }

        let _bulk_read = self.bulk_gate.read();
        let _stack_guard = BulkReadStackGuard::new(store_id);
        f()
    }

    fn with_bulk_maintenance<R>(&self, f: impl FnOnce() -> R) -> R {
        let store_id = self as *const Self as usize;
        let already_held = HELD_BULK_WRITE_LOCKS.with(|held| held.borrow().contains(&store_id));
        if already_held {
            return f();
        }

        let _bulk_write = self.bulk_gate.write();
        let _stack_guard = BulkWriteStackGuard::new(store_id);
        f()
    }

    pub fn register_or_reconfigure_empty_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        let lock_index = index.clone();
        self.with_index_maintenance(&lock_index, || {
            let mut registry = self.registry.write();
            match registry.get(&index) {
                Some(state) if state.entry_count() != 0 => {}
                Some(state) if state.dimension() == dimension => {}
                Some(_) | None => {
                    registry.insert(index, Arc::new(IndexState::new(dimension, quantization)));
                }
            }
        });
    }

    pub fn deregister_index(&self, index: &VectorIndexRef, accountant: &MemoryAccountant) {
        self.with_index_maintenance(index, || {
            if let Some(state) = self.registry.write().remove(index) {
                state.clear_hnsw(accountant);
            }
        });
    }

    pub fn deregister_table(&self, table: &str, accountant: &MemoryAccountant) {
        loop {
            let keys = {
                let registry = self.registry.read();
                registry
                    .keys()
                    .filter(|index| index.table == table)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            if keys.is_empty() {
                return;
            }
            for index in Self::sorted_refs(keys) {
                self.deregister_index(&index, accountant);
            }
        }
    }

    pub fn rename_index(&self, old: &VectorIndexRef, new: VectorIndexRef) -> Result<()> {
        self.with_index_pair_maintenance(old, &new, || {
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
            registry.insert(new.clone(), state);
            Ok(())
        })
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

    pub(crate) fn with_registered_state<R>(
        &self,
        index: &VectorIndexRef,
        f: impl FnOnce(Arc<IndexState>) -> R,
    ) -> Result<R> {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            let state = registry
                .get(index)
                .cloned()
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Ok(f(state))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.with_bulk_read(|| self.registry.read().is_empty())
    }

    pub fn index_count(&self) -> usize {
        self.with_bulk_read(|| self.registry.read().len())
    }

    pub fn validate_vector(&self, index: &VectorIndexRef, actual: usize) -> Result<()> {
        self.with_bulk_read(|| {
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
        })
    }

    pub fn apply_inserts(&self, inserts: Vec<VectorEntry>) {
        self.apply_inserts_with_accountant(inserts, None);
    }

    pub fn apply_inserts_with_accountant(
        &self,
        inserts: Vec<VectorEntry>,
        accountant: Option<&MemoryAccountant>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, Vec<VectorEntry>>::new();
        for entry in inserts {
            by_index.entry(entry.index.clone()).or_default().push(entry);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(inserts) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_inserts_unlocked(inserts, accountant);
                });
            }
        }
    }

    fn apply_inserts_unlocked(
        &self,
        inserts: Vec<VectorEntry>,
        accountant: Option<&MemoryAccountant>,
    ) {
        for entry in inserts {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&entry.index, crate::test_seam::PauseWindow::Apply);
            if let Some(state) = self.try_state(&entry.index) {
                let stored_entry = state.stored_entry(entry);
                state.push_entry(stored_entry);
                if let Some(lock) = state.hnsw().get() {
                    let was_built = lock.read().is_some();
                    if was_built {
                        state.clear_hnsw_with_optional_accountant(accountant);
                    }
                }
            }
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(VectorIndexRef, RowId, TxId)>) {
        self.apply_deletes_with_accountant(deletes, None);
    }

    pub fn apply_deletes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        accountant: Option<&MemoryAccountant>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, Vec<(VectorIndexRef, RowId, TxId)>>::new();
        for delete in deletes {
            by_index.entry(delete.0.clone()).or_default().push(delete);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(deletes) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_deletes_unlocked(deletes, accountant);
                });
            }
        }
    }

    fn apply_deletes_unlocked(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        accountant: Option<&MemoryAccountant>,
    ) {
        for (index, row_id, deleted_tx) in deletes {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&index, crate::test_seam::PauseWindow::Apply);
            if let Some(state) = self.try_state(&index) {
                state.tombstone_row(row_id, deleted_tx);
                state.clear_hnsw_with_optional_accountant(accountant);
            }
        }
    }

    pub fn apply_moves(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
    ) {
        self.apply_moves_with_accountant(moves, lsn, None);
    }

    pub fn apply_moves_with_accountant(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&MemoryAccountant>,
    ) {
        let mut by_index =
            HashMap::<VectorIndexRef, Vec<(VectorIndexRef, RowId, RowId, TxId)>>::new();
        for row_move in moves {
            by_index
                .entry(row_move.0.clone())
                .or_default()
                .push(row_move);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(moves) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_moves_unlocked(moves, lsn, accountant);
                });
            }
        }
    }

    fn apply_moves_unlocked(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&MemoryAccountant>,
    ) {
        for (index, old_row_id, new_row_id, tx) in moves {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&index, crate::test_seam::PauseWindow::Apply);
            if let Some(state) = self.try_state(&index)
                && let Some(old) = state.stored_by_row_id(old_row_id)
                && old.deleted_tx.is_none()
            {
                state.tombstone_row(old_row_id, tx);
                state.clear_hnsw_with_optional_accountant(accountant);
                let mut moved = old;
                moved.row_id = new_row_id;
                moved.created_tx = tx;
                moved.deleted_tx = None;
                moved.lsn = lsn;
                state.push_entry(moved.clone());
            }
        }
    }

    pub fn apply_changes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        inserts: Vec<VectorEntry>,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&MemoryAccountant>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, PendingVectorChanges>::new();
        for delete in deletes {
            by_index
                .entry(delete.0.clone())
                .or_default()
                .deletes
                .push(delete);
        }
        for insert in inserts {
            by_index
                .entry(insert.index.clone())
                .or_default()
                .inserts
                .push(insert);
        }
        for row_move in moves {
            by_index
                .entry(row_move.0.clone())
                .or_default()
                .moves
                .push(row_move);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(changes) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_deletes_unlocked(changes.deletes, accountant);
                    self.apply_inserts_unlocked(changes.inserts, accountant);
                    self.apply_moves_unlocked(changes.moves, lsn, accountant);
                });
            }
        }
    }

    pub fn insert_loaded_vector(&self, entry: VectorEntry) {
        let index = entry.index.clone();
        self.with_index_maintenance(&index, || {
            let quantization = self
                .try_state(&index)
                .map(|state| state.quantization())
                .unwrap_or(VectorQuantization::F32);
            self.register_or_reconfigure_empty_index(
                index.clone(),
                entry.vector.len(),
                quantization,
            );
            if let Some(state) = self.try_state(&index) {
                let stored_entry = state.stored_entry(entry);
                state.push_entry(stored_entry);
            }
        });
    }

    pub fn replace_loaded_vectors(&self, entries: Vec<VectorEntry>) {
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            for state in self.registry.read().values() {
                state.vectors.write().clear();
                state.drop_hnsw_without_accounting();
            }
            for entry in entries {
                self.insert_loaded_vector(entry);
            }
        });
    }

    pub fn all_entries(&self) -> Vec<VectorEntry> {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .iter()
                .flat_map(|(index, state)| state.all_entries(index))
                .collect()
        })
    }

    pub fn prune_row_ids(
        &self,
        row_ids: &std::collections::HashSet<RowId>,
        accountant: &MemoryAccountant,
    ) -> usize {
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
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
        })
    }

    pub fn entries_for_index(&self, index: &VectorIndexRef) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| Ok(self.state(index)?.all_entries(index)))
    }

    pub fn vector_count(&self) -> usize {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .map(|state| state.vector_count())
                .sum()
        })
    }

    pub fn has_hnsw_index(&self) -> bool {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .any(|state| state.hnsw_len().is_some())
        })
    }

    pub fn has_hnsw_index_for(&self, index: &VectorIndexRef) -> bool {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.hnsw_len())
                .is_some()
        })
    }

    pub fn clear_hnsw(&self, accountant: &MemoryAccountant) {
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            for state in self.registry.read().values() {
                state.clear_hnsw(accountant);
            }
        });
    }

    pub fn clear_hnsw_for(&self, index: &VectorIndexRef, accountant: &MemoryAccountant) {
        self.with_index_maintenance(index, || {
            if let Some(state) = self.try_state(index) {
                state.clear_hnsw(accountant);
            }
        });
    }

    pub fn raw_hnsw_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Result<Vec<(RowId, f32)>>> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.raw_hnsw_search(index, query, k))
        })
    }

    pub fn raw_hnsw_entry_count_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Option<usize> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.raw_hnsw_entry_count_for_row(row_id))
        })
    }

    pub fn find_by_row_id(&self, row_id: RowId) -> Option<VectorEntry> {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .iter()
                .find_map(|(index, state)| state.find_by_row_id(index, row_id))
        })
    }

    pub fn live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VectorEntry> {
        self.with_bulk_read(|| {
            self.try_state(index).and_then(|state| {
                state.with_entries(|entries| {
                    entries
                        .iter()
                        .rev()
                        .find(|entry| entry.row_id == row_id && entry.visible_at(snapshot))
                        .map(|entry| entry.to_vector_entry(index.clone()))
                })
            })
        })
    }

    pub fn live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Vec<VectorEntry> {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .iter()
                .flat_map(|(index, state)| state.all_entries(index))
                .filter(|entry| entry.row_id == row_id && entry.visible_at(snapshot))
                .collect()
        })
    }

    pub fn vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: contextdb_core::Lsn,
    ) -> Option<Vec<f32>> {
        self.with_bulk_read(|| {
            self.try_state(index).and_then(|state| {
                state.with_entries(|entries| {
                    entries
                        .iter()
                        .find(|entry| entry.row_id == row_id && entry.lsn == lsn)
                        .map(|entry| entry.vector.to_f32())
                })
            })
        })
    }

    pub fn storage_bytes_per_entry(&self, index: &VectorIndexRef) -> Result<Vec<usize>> {
        self.with_bulk_read(|| Ok(self.state(index)?.storage_bytes_per_entry()))
    }

    pub fn index_infos(&self) -> Vec<VectorIndexInfo> {
        self.with_bulk_read(|| {
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
        })
    }

    pub fn build_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.build_mutex.lock()
    }
}
