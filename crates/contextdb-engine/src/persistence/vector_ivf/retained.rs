//! Actual old-codebook objects, lazily activated on the original maintenance owner.
use super::{memory::Invoice, *};
use contextdb_core::read_memory::ReadShared;
use contextdb_vector::{
    MemoryBudgetOwner,
    ivf::{
        RetainedCodebook,
        storage::{Manifest, ReadPages},
    },
};
use parking_lot::{Mutex, MutexGuard};
use std::alloc::Layout;

pub(super) type EntryOwner = ReadShared<Entry, Invoice>;
pub(super) type RegistryOwner = ReadShared<Registry, Invoice>;
pub(super) struct Entry {
    route: [u8; 32],
    scope: [u8; 64],
    index_name: [u8; 32],
    pub book: RetainedCodebook,
}
#[derive(Default)]
struct Entries {
    entries: Vec<EntryOwner>,
    // The complete exact-capacity reference array frees before this invoice.
    _invoice: Option<Invoice>,
}
#[derive(Default)]
pub(super) struct Registry {
    state: Mutex<Entries>,
}

pub(in crate::persistence) fn route_identity(
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb.clustered.retained.route.v1");
    hash.update(&(index.table.len() as u64).to_le_bytes());
    hash.update(index.table.as_bytes());
    hash.update(&(index.column.len() as u64).to_le_bytes());
    hash.update(index.column.as_bytes());
    hash.update(&control::tuple_digest(tuple));
    *hash.finalize().as_bytes()
}

pub(super) fn index_name_identity(table: &str, column: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb.clustered.retained.index-name.v1");
    hash.update(&(table.len() as u64).to_le_bytes());
    hash.update(table.as_bytes());
    hash.update(&(column.len() as u64).to_le_bytes());
    hash.update(column.as_bytes());
    *hash.finalize().as_bytes()
}

enum Change {
    Install {
        entry: EntryOwner,
        array: Option<Entries>,
        position: usize,
        replace: bool,
    },
    Remove {
        position: usize,
    },
}
/// No fallible allocation or admission remains after the native commit.
/// Dropping before commit discards the unpublished object and prepared overlap.
pub(super) struct Prepared<'a> {
    cache: &'a cache::NativeIvfCache,
    activation: Option<MutexGuard<'a, ()>>,
    registry: RegistryOwner,
    created: bool,
    committed: bool,
    change: Option<Change>,
    _invoice: Invoice,
}
impl Prepared<'_> {
    pub fn commit(mut self) {
        let mut retired_entry = None;
        let mut retired_array = None;
        {
            let mut state = self.registry.state.lock();
            match self.change.take().expect("prepared retention change") {
                Change::Install {
                    entry,
                    array,
                    position,
                    replace,
                } => {
                    if let Some(mut array) = array {
                        array.entries.insert(position, entry);
                        retired_array = Some(std::mem::replace(&mut *state, array));
                    } else if replace {
                        retired_entry =
                            Some(std::mem::replace(&mut state.entries[position], entry));
                    } else {
                        assert!(
                            state.entries.len() < state.entries.capacity(),
                            "retention insertion was prepaid"
                        );
                        state.entries.insert(position, entry);
                    }
                }
                Change::Remove { position } => {
                    retired_entry = Some(state.entries.remove(position));
                    if state.entries.is_empty() {
                        retired_array = Some(std::mem::take(&mut *state));
                    }
                }
            }
        }
        self.committed = true;
        drop(self.activation.take());
        drop(retired_entry);
        drop(retired_array);
    }
}
impl Drop for Prepared<'_> {
    fn drop(&mut self) {
        if !self.committed && self.created {
            // This guard owns the sole activation lock, so another preparer
            // cannot have replaced its newly installed empty registry.
            let old = self.cache.retained.lock().take();
            drop(old);
        }
        // Physical object destruction and its invoices run without this lock.
        drop(self.activation.take());
    }
}
impl cache::NativeIvfCache {
    pub(crate) fn fork(&self) -> Self {
        let cache = Self::default();
        *cache.retained.lock() = self.retained.lock().clone();
        cache
    }
    pub(super) fn adopt_retained(&self, source: &Self) {
        if std::ptr::eq(self, source) {
            return;
        }
        let registry = source.retained.lock().clone();
        *self.retained.lock() = registry;
    }
    pub(super) fn retained_entry(&self, route: [u8; 32], scope: [u8; 64]) -> Option<EntryOwner> {
        let registry = self.retained.lock().clone()?;
        // A source cache miss uses the ordinary checked reader constructor.
        let state = registry.state.try_lock()?;
        let position = state
            .entries
            .binary_search_by_key(&route, |entry| entry.route)
            .ok()?;
        let entry = &state.entries[position];
        (entry.scope == scope).then(|| entry.clone())
    }
    pub(super) fn prepare_retained<'a>(
        &'a self,
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        route: [u8; 32],
        scope: [u8; 64],
        desired: Option<Manifest>,
    ) -> Result<Option<Prepared<'a>>> {
        // A selected tuple's existing request checkpoint bounds lock waits.
        let activation = loop {
            source.check_cancel()?;
            if let Some(guard) = self.activation.try_lock() {
                break guard;
            }
            std::thread::yield_now();
        };
        let existing = self.retained.lock().clone();
        if let Some(registry) = &existing {
            let state = registry.state.lock();
            let found = state
                .entries
                .binary_search_by_key(&route, |entry| entry.route);
            match (found, desired) {
                (Ok(position), Some(manifest))
                    if state.entries[position].scope == scope
                        && state.entries[position].book.matches(manifest) =>
                {
                    return Ok(None);
                }
                (Err(_), None) => return Ok(None),
                _ => {}
            }
        } else if desired.is_none() {
            return Ok(None);
        }
        let invoice = Invoice::reserve(
            owner,
            std::mem::size_of::<Prepared<'_>>(),
            "clustered_retained_stage",
        )?;
        let created = existing.is_none();
        let registry = if let Some(registry) = existing {
            registry
        } else {
            let invoice = Invoice::reserve(
                owner,
                RegistryOwner::layout().size(),
                "clustered_retained_registry",
            )?;
            ReadShared::try_from_invoice_native(Registry::default(), invoice)
                .map_err(Error::NativeEngineOther)?
        };
        let found = registry
            .state
            .lock()
            .entries
            .binary_search_by_key(&route, |entry| entry.route);
        let change = if let Some(manifest) = desired {
            // One concrete control plus the real constructor's own values
            // invoice. The query never supplies a substitute payer or values.
            let control = Invoice::reserve(
                owner,
                EntryOwner::layout().size(),
                "clustered_retained_control",
            )?;
            let book = RetainedCodebook::load(owner, source, manifest)?;
            let index = source.index_identity();
            let entry = ReadShared::try_from_invoice_native(
                Entry {
                    route,
                    scope,
                    index_name: index_name_identity(&index.table, &index.column),
                    book,
                },
                control,
            )
            .map_err(Error::NativeEngineOther)?;
            let (position, replace) = match found {
                Ok(position) => (position, true),
                Err(position) => (position, false),
            };
            let mut array = None;
            if !replace {
                let state = registry.state.lock();
                if state.entries.len() == state.entries.capacity() {
                    let capacity = state.entries.len().checked_add(1).ok_or_else(corrupt)?;
                    let layout = Layout::array::<EntryOwner>(capacity).map_err(|_| corrupt())?;
                    // Both real arrays remain paid during replacement.
                    let invoice =
                        Invoice::reserve(owner, layout.size(), "clustered_retained_directory")?;
                    let mut entries = Vec::with_capacity(capacity);
                    entries.extend(state.entries.iter().cloned());
                    array = Some(Entries {
                        entries,
                        _invoice: Some(invoice),
                    });
                }
            }
            Change::Install {
                entry,
                array,
                position,
                replace,
            }
        } else {
            Change::Remove {
                position: found.expect("an existing retained entry"),
            }
        };
        if created {
            *self.retained.lock() = Some(registry.clone());
        }
        Ok(Some(Prepared {
            cache: self,
            activation: Some(activation),
            registry,
            created,
            committed: false,
            change: Some(change),
            _invoice: invoice,
        }))
    }
    pub(super) fn ensure_retained(
        &self,
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        route: [u8; 32],
        scope: [u8; 64],
        manifest: Manifest,
    ) -> Result<EntryOwner> {
        if let Some(entry) = self
            .retained_entry(route, scope)
            .filter(|entry| entry.book.matches(manifest))
        {
            return Ok(entry);
        }
        if let Some(prepared) =
            self.prepare_retained(owner, source, route, scope, Some(manifest))?
        {
            prepared.commit();
        }
        // The caller holds the existing commit boundary until its selected
        // source owns this clone. Only a short, read-only cache lookup can
        // contend here; the original request checkpoint bounds that wait.
        loop {
            if let Some(entry) = self.retained_entry(route, scope) {
                return Ok(entry);
            }
            source.check_cancel()?;
            std::thread::yield_now();
        }
    }
    /// A committed schema retirement has no future B->C step to release this loan.
    /// In-flight readers may finish with their existing clone; the global registry
    /// no longer keeps the retired object alive. Destruction runs outside its locks.
    pub(in crate::persistence) fn forget_retained_index_name(&self, table: &str, column: &str) {
        self.cancel_calibration_where(|capture| {
            capture.index_name == index_name_identity(table, column)
        });
        let identity = index_name_identity(table, column);
        loop {
            let activation = self.activation.lock();
            let registry = self.retained.lock().clone();
            let mut retired = None;
            let mut array = None;
            if let Some(registry) = registry {
                let mut state = registry.state.lock();
                if let Some(position) = state
                    .entries
                    .iter()
                    .position(|entry| entry.index_name == identity)
                {
                    retired = Some(state.entries.remove(position));
                    if state.entries.is_empty() {
                        array = Some(std::mem::take(&mut *state));
                    }
                }
            }
            drop(activation);
            let removed = retired.is_some();
            drop(retired);
            drop(array);
            if !removed {
                break;
            }
        }
    }
    /// Post-commit erasure releases cached objects without a new allocation.
    pub(in crate::persistence) fn forget_retained_route(&self, route: [u8; 32]) {
        self.cancel_calibration_where(|capture| capture.route == route);
        let _activation = self.activation.lock();
        let registry = self.retained.lock().clone();
        let mut retired = None;
        let mut array = None;
        if let Some(registry) = registry {
            let mut state = registry.state.lock();
            if let Ok(position) = state
                .entries
                .binary_search_by_key(&route, |entry| entry.route)
            {
                retired = Some(state.entries.remove(position));
                if state.entries.is_empty() {
                    array = Some(std::mem::take(&mut *state));
                }
            }
        }
        drop(_activation);
        drop(retired);
        drop(array);
    }
}

impl cache::NativeIvfCache {
    pub(in crate::persistence) fn clear_retained(&self) {
        self.cancel_calibration_where(|_| true);
        let activation = self.activation.lock();
        let registry = self.retained.lock().take();
        let old = registry
            .as_ref()
            .map(|registry| std::mem::take(&mut *registry.state.lock()));
        drop(activation);
        drop(old);
        drop(registry);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Selection<'a> {
    pub cache: &'a cache::NativeIvfCache,
    pub owner: &'a dyn Fn() -> Result<MemoryBudgetOwner>,
    pub boundary: &'a dyn Fn(&mut dyn FnMut() -> Result<()>) -> Result<bool>,
}
