//! Page-port adapters for the clustered kernel guards. `Hiding` wraps the
//! native page fixture: it can hide one page kind from readers that read
//! through the index's store, and it counts reads by kind. When the port
//! trait gains a member (the index identity, `fn index_identity(&self) ->
//! &VectorIndexRef`), this adapter delegates it to the wrapped fixture, and
//! `port_identity` returns `port.index_identity().clone()`; those two are the
//! only edits this file is meant to receive.
#![allow(dead_code)]
use super::ivf_pages;
use contextdb_core::{Result, VectorIndexRef};
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PAGE_BYTES, PageKey, PageKind, PageStore, ReadPages,
    TailRef,
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
        = ivf_pages::Transaction<'a>
    where
        Self: 'a;
    fn begin(&mut self) -> Result<ivf_pages::Transaction<'_>> {
        self.inner.begin()
    }
}

/// The index identity the page port supplies to the kernel, which the
/// kernel's typed errors name.
pub fn port_identity(_port: &impl ReadPages) -> VectorIndexRef {
    unimplemented!("the page port does not yet supply its index identity")
}
