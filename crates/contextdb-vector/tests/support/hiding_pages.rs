//! Page-port adapters for the clustered kernel guards. `Hiding` wraps the
//! native page fixture: it can hide one page kind and it counts reads by kind,
//! for reads through the store and through every transaction the store
//! begins alike, so a guard pins what the kernel sees, not the path it reads
//! by. When the port
//! trait gains a member (the index identity, `fn index_identity(&self) ->
//! &VectorIndexRef`), `Hiding` and `HidingTransaction` delegate it to what
//! they wrap, and `port_identity` returns `port.index_identity().clone()`;
//! those are the only edits this file is meant to receive.
#![allow(dead_code)]
use super::ivf_pages;
use contextdb_core::{Result, VectorIndexRef};
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PAGE_BYTES, PageKey, PageKind, PageStore, ReadPages,
    TailRef, WritePages,
};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Hiding {
    pub inner: ivf_pages::Pages,
    pub hide: Cell<Option<PageKind>>,
    pub reads: RefCell<BTreeMap<PageKind, u64>>,
}
impl ReadPages for Hiding {
    fn check_cancel(&self) -> Result<()> {
        self.inner.check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        self.inner.before_distance(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        self.inner.manifest(partition)
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        self.inner.build_state(partition)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        self.inner.next_partition(after)
    }
    fn next_unsealed(&self, after: Option<(u64, u64)>) -> Result<Option<TailRef>> {
        self.inner.next_unsealed(after)
    }
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        *self.reads.borrow_mut().entry(key.kind).or_default() += 1;
        if self.hide.get() == Some(key.kind) {
            return Ok(false);
        }
        self.inner.read_admitted_page(key, output)
    }
}
impl PageStore for Hiding {
    type Transaction<'a>
        = HidingTransaction<'a>
    where
        Self: 'a;
    fn begin(&mut self) -> Result<HidingTransaction<'_>> {
        let Hiding { inner, hide, reads } = self;
        Ok(HidingTransaction {
            inner: inner.begin()?,
            hide: hide.get(),
            reads,
        })
    }
}

/// A fixture transaction whose reads see the same hidden kind and add to the
/// same read counts as the store that began it.
pub struct HidingTransaction<'a> {
    inner: ivf_pages::Transaction<'a>,
    hide: Option<PageKind>,
    reads: &'a RefCell<BTreeMap<PageKind, u64>>,
}
impl ReadPages for HidingTransaction<'_> {
    fn check_cancel(&self) -> Result<()> {
        self.inner.check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        self.inner.before_distance(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        self.inner.manifest(partition)
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        self.inner.build_state(partition)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        self.inner.next_partition(after)
    }
    fn next_unsealed(&self, after: Option<(u64, u64)>) -> Result<Option<TailRef>> {
        self.inner.next_unsealed(after)
    }
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        *self.reads.borrow_mut().entry(key.kind).or_default() += 1;
        if self.hide == Some(key.kind) {
            return Ok(false);
        }
        self.inner.read_admitted_page(key, output)
    }
}
impl WritePages for HidingTransaction<'_> {
    fn put_page(&mut self, key: PageKey, payload: &[u8; PAGE_BYTES]) -> Result<()> {
        self.inner.put_page(key, payload)
    }
    fn stage_build(&mut self, state: BuildState) -> Result<()> {
        self.inner.stage_build(state)
    }
    fn publish_manifest(&mut self, manifest: Manifest) -> Result<()> {
        self.inner.publish_manifest(manifest)
    }
    fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}

/// The index identity the page port supplies to the kernel, which the
/// kernel's typed errors name.
pub fn port_identity(_port: &impl ReadPages) -> VectorIndexRef {
    unimplemented!("the page port does not yet supply its index identity")
}
