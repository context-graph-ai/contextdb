//! Native-storage fixture only: byte pages, atomic transactions and observation.
//! All clustering, scoring, visibility and buffering run in contextdb_vector::ivf.
use contextdb_core::{Error, Result, VectorIndexRef};
use contextdb_vector::ivf::storage::*;
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
};

#[derive(Clone)]
pub struct Pages {
    identity: VectorIndexRef,
    committed_lsn: u64,
    pub inspection: RefCell<BTreeMap<(u64, u64, u64), Verdict>>,
    pages: BTreeMap<PageKey, Box<[u8; PAGE_BYTES]>>,
    manifests: BTreeMap<u64, Manifest>,
    builds: BTreeMap<u64, BuildState>,
    pub writes: RefCell<BTreeMap<PageKind, u64>>,
    pub touched: RefCell<BTreeSet<PageKey>>,
    pub contexts: RefCell<BTreeSet<u64>>,
    pub cancelled: Cell<bool>,
    pub cancel_at_distance: Cell<Option<u64>>,
    pub comparisons: Cell<u64>,
    pub fail_next_write: Cell<bool>,
}
impl Default for Pages {
    fn default() -> Self {
        Self {
            identity: VectorIndexRef::new("fixture", "embedding"),
            committed_lsn: 0,
            inspection: RefCell::new(BTreeMap::new()),
            pages: BTreeMap::new(),
            manifests: BTreeMap::new(),
            builds: BTreeMap::new(),
            writes: RefCell::new(BTreeMap::new()),
            touched: RefCell::new(BTreeSet::new()),
            contexts: RefCell::new(BTreeSet::new()),
            cancelled: Cell::new(false),
            cancel_at_distance: Cell::new(None),
            comparisons: Cell::new(0),
            fail_next_write: Cell::new(false),
        }
    }
}
impl ReadPages for Pages {
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.inspection
            .borrow()
            .get(&(self.committed_lsn, p, g))
            .copied()
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.inspection
            .borrow_mut()
            .insert((self.committed_lsn, p, g), v);
    }

    fn snapshot_lsn(&self) -> Result<u64> {
        Ok(self.committed_lsn)
    }
    fn index_identity(&self) -> &VectorIndexRef {
        &self.identity
    }

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
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.check_cancel()?;
        self.touched.borrow_mut().insert(key);
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
    lsn: u64,
    pub inspection: RefCell<BTreeMap<(u64, u64, u64), Verdict>>,
    pages: BTreeMap<PageKey, Box<[u8; PAGE_BYTES]>>,
    manifests: BTreeMap<u64, Manifest>,
    builds: BTreeMap<u64, BuildState>,
}
impl ReadPages for Transaction<'_> {
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.inspection.borrow().get(&(self.lsn, p, g)).copied()
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.inspection.borrow_mut().insert((self.lsn, p, g), v);
    }

    fn snapshot_lsn(&self) -> Result<u64> {
        Ok(self.parent.committed_lsn)
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.parent.index_identity()
    }
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
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.check_cancel()?;
        self.parent.touched.borrow_mut().insert(key);
        if let Some(page) = self.pages.get(&key) {
            out.copy_from_slice(page.as_ref());
            Ok(true)
        } else {
            self.parent.read_admitted_page(key, out)
        }
    }
}
impl WritePages for Transaction<'_> {
    fn commit_lsn(&self) -> Result<u64> {
        Ok(self.lsn)
    }
    fn put_page(&mut self, key: PageKey, payload: &[u8; PAGE_BYTES]) -> Result<()> {
        self.check_cancel()?;
        *self.parent.writes.borrow_mut().entry(key.kind).or_default() += 1;
        self.parent.touched.borrow_mut().insert(key);
        if self.parent.fail_next_write.replace(false) {
            return Err(Error::ReadCancelled);
        }
        self.inspection.borrow_mut().clear();
        self.pages.insert(key, Box::new(*payload));
        Ok(())
    }
    fn stage_build(&mut self, state: BuildState) -> Result<()> {
        self.inspection.borrow_mut().clear();
        self.builds.insert(state.manifest.partition, state);
        Ok(())
    }
    fn publish_manifest(&mut self, m: Manifest) -> Result<()> {
        self.inspection.borrow_mut().clear();
        self.manifests.insert(m.partition, m);
        Ok(())
    }
    fn commit(self) -> Result<()> {
        self.parent.check_cancel()?;
        self.parent.inspection.borrow_mut().clear();
        self.parent.committed_lsn = self.lsn;
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
        let lsn = self
            .committed_lsn
            .checked_add(1)
            .expect("fixture transaction clock exhausted");
        Ok(Transaction {
            lsn,
            inspection: RefCell::new(BTreeMap::new()),
            parent: self,
            pages: BTreeMap::new(),
            manifests: BTreeMap::new(),
            builds: BTreeMap::new(),
        })
    }
}
