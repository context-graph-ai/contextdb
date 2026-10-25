//! Native-storage fixture only: byte pages, atomic transactions and observation.
//! All clustering, scoring, visibility and buffering run in contextdb_vector::ivf.
use contextdb_core::{Error, Result};
use contextdb_vector::{MemoryBudget, ivf::storage::*};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
pub struct Budget {
    pub live: AtomicUsize,
    pub peak: AtomicUsize,
}
impl std::fmt::Debug for Budget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("live", &self.live.load(Ordering::SeqCst))
            .finish()
    }
}
impl MemoryBudget for Budget {
    fn try_allocate_for(&self, n: usize, _s: &str, _o: &str, _h: &str) -> Result<()> {
        let old = self.live.load(Ordering::SeqCst);
        if old.saturating_add(n) > 64 * 1024 * 1024 {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "fixture".into(),
                operation: "ivf".into(),
                requested_bytes: n,
                available_bytes: (64usize * 1024 * 1024).saturating_sub(old),
                budget_limit_bytes: 64 * 1024 * 1024,
                hint: "fixture budget exhausted".into(),
            });
        }
        let now = self.live.fetch_add(n, Ordering::SeqCst) + n;
        self.peak.fetch_max(now, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, n: usize) {
        assert!(self.live.fetch_sub(n, Ordering::SeqCst) >= n);
    }
}
#[derive(Default)]
pub struct Pages {
    pages: BTreeMap<PageKey, Box<[u8; PAGE_BYTES]>>,
    manifests: BTreeMap<u64, Manifest>,
    builds: BTreeMap<u64, BuildState>,
    pub contexts: RefCell<BTreeSet<u64>>,
    pub cancelled: Cell<bool>,
    pub cancel_at_distance: Cell<Option<u64>>,
    pub comparisons: Cell<u64>,
    pub fail_next_write: Cell<bool>,
}
impl ReadPages for Pages {
    fn check_cancel(&self) -> Result<()> {
        if self.cancelled.get() {
            Err(Error::ReadCancelled)
        } else {
            Ok(())
        }
    }
    fn before_distance(&self, _kind: DistanceKind) -> Result<()> {
        self.check_cancel()?;
        let next = self.comparisons.get() + 1;
        self.comparisons.set(next);
        if self.cancel_at_distance.get() == Some(next) {
            Err(Error::ReadCancelled)
        } else {
            Ok(())
        }
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        Ok(self.builds.get(&p).copied())
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        Ok(self.manifests.get(&p).copied())
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        Ok(self
            .manifests
            .keys()
            .copied()
            .find(|p| after.is_none_or(|a| *p > a)))
    }
    fn next_unsealed(&self, after: Option<(u64, u64)>) -> Result<Option<TailRef>> {
        for m in self.manifests.values() {
            for slot in m.sealed_slots..m.slots {
                if after.is_none_or(|a| (m.partition, slot) > a) {
                    return Ok(Some(TailRef { manifest: *m, slot }));
                }
            }
        }
        Ok(None)
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.check_cancel()?;
        if key.kind == PageKind::Lists {
            self.contexts.borrow_mut().insert(key.partition);
        }
        if let Some(page) = self.pages.get(&key) {
            out.copy_from_slice(page.as_ref());
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
pub struct Transaction<'a> {
    parent: &'a mut Pages,
    pages: BTreeMap<PageKey, Box<[u8; PAGE_BYTES]>>,
    manifests: BTreeMap<u64, Manifest>,
    builds: BTreeMap<u64, BuildState>,
}
impl ReadPages for Transaction<'_> {
    fn check_cancel(&self) -> Result<()> {
        self.parent.check_cancel()
    }
    fn before_distance(&self, k: DistanceKind) -> Result<()> {
        self.parent.before_distance(k)
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        Ok(self.builds.get(&p).copied().or(self.parent.build_state(p)?))
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        Ok(self.manifests.get(&p).copied().or(self.parent.manifest(p)?))
    }
    fn next_partition(&self, a: Option<u64>) -> Result<Option<u64>> {
        Ok(self
            .parent
            .manifests
            .keys()
            .chain(self.manifests.keys())
            .copied()
            .filter(|p| a.is_none_or(|x| *p > x))
            .min())
    }
    fn next_unsealed(&self, after: Option<(u64, u64)>) -> Result<Option<TailRef>> {
        let mut p = None;
        while let Some(id) = self.next_partition(p)? {
            let m = self.manifest(id)?.unwrap();
            for slot in m.sealed_slots..m.slots {
                if after.is_none_or(|a| (id, slot) > a) {
                    return Ok(Some(TailRef { manifest: m, slot }));
                }
            }
            p = Some(id);
        }
        Ok(None)
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        if let Some(page) = self.pages.get(&key) {
            out.copy_from_slice(page.as_ref());
            Ok(true)
        } else {
            self.parent.read_admitted_page(key, out)
        }
    }
}
impl WritePages for Transaction<'_> {
    fn put_page(&mut self, key: PageKey, payload: &[u8; PAGE_BYTES]) -> Result<()> {
        self.check_cancel()?;
        if self.parent.fail_next_write.replace(false) {
            return Err(Error::ReadCancelled);
        }
        self.pages.insert(key, Box::new(*payload));
        Ok(())
    }
    fn stage_build(&mut self, state: BuildState) -> Result<()> {
        self.builds.insert(state.manifest.partition, state);
        Ok(())
    }
    fn publish_manifest(&mut self, m: Manifest) -> Result<()> {
        self.manifests.insert(m.partition, m);
        Ok(())
    }
    fn commit(self) -> Result<()> {
        self.parent.check_cancel()?;
        self.parent.pages.extend(self.pages);
        self.parent.manifests.extend(self.manifests);
        self.parent.builds.extend(self.builds);
        Ok(())
    }
}
impl PageStore for Pages {
    type Transaction<'a> = Transaction<'a>;
    fn begin(&mut self) -> Result<Transaction<'_>> {
        self.check_cancel()?;
        Ok(Transaction {
            parent: self,
            pages: BTreeMap::new(),
            manifests: BTreeMap::new(),
            builds: BTreeMap::new(),
        })
    }
}
