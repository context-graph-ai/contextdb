//! Exact prepaid cancellation/registration controls for one request.
use super::*;
use contextdb_core::read_memory::{ReadMemoryOwner, ReadPaidArc};

#[derive(Clone)]
enum RegistrationHandle {
    Host(Arc<RegistrationCell>),
    Paid(ReadPaidArc<RegistrationCell>),
}
impl std::ops::Deref for RegistrationHandle {
    type Target = RegistrationCell;
    fn deref(&self) -> &RegistrationCell {
        match self { Self::Host(cell) => cell, Self::Paid(cell) => cell }
    }
}
struct RegistrationCell {
    cancellation: OwnerReadCancellation,
    links: Mutex<RegistrationLinks>,
}
#[derive(Default)]
struct RegistrationLinks {
    key: [u8; 16],
    born: u64,
    retired: Option<u64>,
    next: Option<RegistrationHandle>,
}
#[derive(Clone)]
pub(super) struct CancellationRegistration { node: RegistrationHandle }
impl std::fmt::Debug for CancellationRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <OwnerReadCancellation as std::fmt::Debug>::fmt(&self.node.cancellation, f)
    }
}
impl CancellationRegistration {
    pub(super) fn bytes() -> usize {
        OwnerReadCancellation::funded_bytes() + ReadPaidArc::<RegistrationCell>::bytes()
    }
    pub(super) fn from_prepaid(owner: ReadMemoryOwner, mut credit: ReadCredit) -> Self {
        let token = credit.try_split_off(OwnerReadCancellation::funded_bytes())
            .expect("prepaid exact cancellation layout");
        let cancellation = OwnerReadCancellation::from_prepaid(owner, token);
        Self { node: RegistrationHandle::Paid(ReadPaidArc::from_prepaid(
            RegistrationCell { cancellation, links: Mutex::new(RegistrationLinks::default()) }, credit)) }
    }
    pub(super) fn host(cancellation: OwnerReadCancellation) -> Self {
        Self { node: RegistrationHandle::Host(Arc::new(RegistrationCell {
            cancellation, links: Mutex::new(RegistrationLinks::default()),
        })) }
    }
    pub(super) fn cancellation(&self) -> OwnerReadCancellation { self.node.cancellation.clone() }
    pub(super) fn rearm_standing(&mut self) {
        let RegistrationHandle::Paid(node) = &mut self.node else {
            panic!("standing registration requires original funding");
        };
        // Registry and every shutdown snapshot release this cell before reuse.
        // The token separately waits for any cancellation-only aliases.
        let node = node.wait_unique();
        node.cancellation.rearm_standing();
        *node.links.get_mut().unwrap_or_else(|p| p.into_inner()) = RegistrationLinks::default();
    }
}

/// The registry owns intrusive links in the already admitted request cells.
/// There are no B-tree nodes, growth buffers, replacement tables or snapshots.
#[derive(Default)]
pub(super) struct CancellationRegistry {
    head: Option<RegistrationHandle>,
    len: usize,
    epoch: u64,
    snapshots: usize,
}
impl CancellationRegistry {
    pub(super) fn len(&self) -> usize { self.len }
    pub(super) fn is_empty(&self) -> bool { self.len == 0 }
    pub(super) fn get(&self, key: &[u8; 16]) -> Option<OwnerReadCancellation> {
        let mut current = self.head.clone();
        while let Some(node) = current {
            let links = node.links.lock().unwrap_or_else(|p| p.into_inner());
            if links.key == *key && links.retired.is_none() { return Some(node.cancellation.clone()); }
            current = links.next.clone();
        }
        None
    }
    pub(super) fn contains_key(&self, key: &[u8; 16]) -> bool { self.get(key).is_some() }
    fn advance(&mut self) -> u64 {
        self.epoch = self.epoch.checked_add(1).expect("cancellation registry epoch exhausted");
        self.epoch
    }
    pub(super) fn insert(&mut self, key: [u8; 16], entry: CancellationRegistration) -> Option<OwnerReadCancellation> {
        {
            let mut links = entry.node.links.lock().unwrap_or_else(|p| p.into_inner());
            links.key = key;
            links.born = self.advance();
            links.retired = None;
            links.next = self.head.take();
        }
        self.head = Some(entry.node);
        self.len += 1;
        None
    }
    pub(super) fn remove(&mut self, key: &[u8; 16]) -> Option<RetiredRegistrations> {
        let mut current = self.head.clone();
        while let Some(node) = current {
            let mut links = node.links.lock().unwrap_or_else(|p| p.into_inner());
            if links.key == *key && links.retired.is_none() {
                links.retired = Some(self.advance());
                self.len -= 1;
                drop(links);
                drop(node);
                return Some(self.sweep());
            }
            current = links.next.clone();
        }
        None
    }
    fn sweep(&mut self) -> RetiredRegistrations {
        let mut removed = RetiredRegistrations(None);
        if self.snapshots != 0 { return removed; }
        let mut previous: Option<RegistrationHandle> = None;
        let mut current = self.head.clone();
        while let Some(node) = current {
            let mut links = node.links.lock().unwrap_or_else(|p| p.into_inner());
            let next = links.next.clone();
            if links.retired.is_some() {
                if let Some(previous) = &previous {
                    previous.links.lock().unwrap_or_else(|p| p.into_inner()).next = next.clone();
                } else { self.head = next.clone(); }
                links.next = removed.0.take();
                drop(links);
                removed.0 = Some(node);
            } else {
                drop(links);
                previous = Some(node);
            }
            current = next;
        }
        removed
    }
    /// Snapshot membership is frozen by epoch; removed cells remain invoiced
    /// until every concurrent shutdown snapshot has released its guard.
    fn begin_snapshot(&mut self) -> u64 {
        self.snapshots = self.snapshots.checked_add(1).expect("registry snapshot count exhausted");
        self.epoch
    }
    fn next_at(&self, epoch: u64, after: Option<[u8; 16]>) -> Option<([u8; 16], OwnerReadCancellation)> {
        let mut current = self.head.clone();
        let mut selected: Option<([u8; 16], OwnerReadCancellation)> = None;
        while let Some(node) = current {
            let links = node.links.lock().unwrap_or_else(|p| p.into_inner());
            if links.born <= epoch && links.retired.is_none_or(|retired| retired > epoch)
                && after.is_none_or(|after| links.key > after)
                && selected.as_ref().is_none_or(|(key, _)| links.key < *key)
            { selected = Some((links.key, node.cancellation.clone())); }
            current = links.next.clone();
        }
        selected
    }
}
pub(super) struct RetiredRegistrations(Option<RegistrationHandle>);
impl Drop for RetiredRegistrations {
    fn drop(&mut self) {
        while let Some(node) = self.0.take() {
            self.0 = node.links.lock().unwrap_or_else(|p| p.into_inner()).next.take();
            drop(node);
        }
    }
}
impl Drop for CancellationRegistry {
    fn drop(&mut self) { drop(RetiredRegistrations(self.head.take())); }
}
pub(super) struct CancellationRegistrySnapshot<'a> {
    registry: &'a Mutex<CancellationRegistry>,
    epoch: u64,
    after: Option<[u8; 16]>,
}
impl<'a> CancellationRegistrySnapshot<'a> {
    pub(super) fn new(registry: &'a Mutex<CancellationRegistry>) -> Self {
        let epoch = registry.lock().expect("owner cancellation registry").begin_snapshot();
        Self { registry, epoch, after: None }
    }
}
impl Iterator for CancellationRegistrySnapshot<'_> {
    type Item = OwnerReadCancellation;
    fn next(&mut self) -> Option<Self::Item> {
        let selected = self.registry.lock().expect("owner cancellation registry")
            .next_at(self.epoch, self.after);
        selected.map(|(key, token)| { self.after = Some(key); token })
    }
}
impl Drop for CancellationRegistrySnapshot<'_> {
    fn drop(&mut self) {
        let retired = {
            let mut registry = self.registry.lock().expect("owner cancellation registry");
            registry.snapshots -= 1;
            registry.sweep()
        };
        drop(retired);
    }
}
