use super::layout::DatabaseLayout;
use super::{PageHint, PageNumber, PageTrackerPolicy, TransactionalMemory};
use crate::db::{TableHandle, TransactionGuard};
use crate::transactions::{
    CHAIN_PAGINATION, DATA_FREED_TABLE, PageList, SYSTEM_FREED_TABLE, TransactionIdWithPagination,
};
use crate::tree_store::{BtreeHeader, LeafAccessor, TableTreeMut};
use crate::{Result, StorageError, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::Ordering};

pub(super) struct Sweep {
    roots: [Option<BtreeHeader>; 2],
    layout: DatabaseLayout,
    pin: Arc<TransactionGuard>,
    live: PageTrackerPolicy,
    phase: usize,
    position: (Option<String>, Vec<u32>),
    entry: usize,
    item: usize,
    chain: Option<PageNumber>,
    proof: Option<PageNumber>,
    region: u32,
    page: u32,
    failed: bool,
}

impl Sweep {
    fn remaining(&self) -> u64 {
        (self.region..self.layout.num_regions())
            .map(|region| {
                u64::from(
                    self.layout.region_layout(region).num_pages()
                        - if region == self.region { self.page } else { 0 },
                )
            })
            .sum()
    }

    fn mark_chain(&mut self, mem: &TransactionalMemory, allowance: &mut usize) -> Result<bool> {
        while let Some(number) = self.chain {
            if *allowance == 0 {
                return Ok(false);
            }
            if !self.live.insert(number) {
                return Err(StorageError::Corrupted("Cyclic recovery chain".into()));
            }
            self.chain = mem.next_commit_page(number)?;
            *allowance -= 1;
        }
        Ok(true)
    }

    fn waiting(
        &mut self,
        mem: &TransactionalMemory,
        bytes: &[u8],
        allowance: &mut usize,
    ) -> Result<bool> {
        let leaf = LeafAccessor::new(
            bytes,
            TransactionIdWithPagination::fixed_width(),
            PageList::fixed_width(),
        );
        while self.entry < leaf.num_pairs() {
            let entry = leaf.entry(self.entry).unwrap();
            let key = TransactionIdWithPagination::from_bytes(entry.key());
            let list = PageList::from_bytes(entry.value());
            if key.pagination_id == CHAIN_PAGINATION {
                if self.item == 0 {
                    self.chain = Some(list.get(0));
                    self.item = 1;
                }
                if !self.mark_chain(mem, allowance)? {
                    return Ok(false);
                }
            } else {
                while self.item < list.len() {
                    if *allowance == 0 {
                        return Ok(false);
                    }
                    let page = list.get(self.item);
                    mem.validate_page(page)?;
                    self.live.insert(page);
                    self.item += 1;
                    *allowance -= 1;
                }
            }
            self.entry += 1;
            self.item = 0;
        }
        self.entry = 0;
        Ok(true)
    }

    fn advance(&mut self, mem: &Arc<TransactionalMemory>, mut allowance: usize) -> Result<u64> {
        while self.phase < 2 && allowance > 0 {
            let tree = TableTreeMut::new(
                self.roots[self.phase],
                self.pin.clone(),
                mem.clone(),
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(PageTrackerPolicy::Ignore)),
            );
            let position = std::mem::take(&mut self.position);
            let stopped = tree.highest_index_pages(
                (position.0.as_deref(), &position.1),
                PageHint::Recovery,
                &mut BTreeMap::new(),
                |name, path, read, bytes| {
                    if let Some(bytes) = bytes {
                        if name == Some(DATA_FREED_TABLE.name())
                            || name == Some(SYSTEM_FREED_TABLE.name())
                        {
                            return self.waiting(mem, bytes, &mut allowance);
                        }
                    } else {
                        if !self.live.contains(&path.page_number()) {
                            if allowance == 0 {
                                return Ok(false);
                            }
                            mem.validate_page(path.page_number())?;
                            self.live.insert(path.page_number());
                            allowance -= 1;
                        }
                        if name == Some(DATA_FREED_TABLE.name())
                            || name == Some(SYSTEM_FREED_TABLE.name())
                        {
                            *read = true;
                        }
                    }
                    Ok(true)
                },
            )?;
            if let Some(position) = stopped {
                self.position = position;
                return Ok(self.remaining());
            }
            self.phase += 1;
        }
        if self.phase == 2 {
            self.chain = self.proof.take();
            self.phase = 3;
        }
        if self.phase == 3 {
            if !self.mark_chain(mem, &mut allowance)? {
                return Ok(self.remaining());
            }
            self.phase = 4;
            #[cfg(debug_assertions)]
            mem.allocated_pages.lock().unwrap().extend(self.live.iter());
        }
        if self.phase == 4 {
            let mut state = mem.state.lock().unwrap();
            while allowance > 0 && self.region < self.layout.num_regions() {
                let end = self.layout.region_layout(self.region).num_pages();
                if self.page == end {
                    self.region += 1;
                    self.page = 0;
                    continue;
                }
                if !self.live.covers(self.region, self.page) {
                    state.allocators.region_allocators[self.region as usize].free(self.page, 0);
                    let order =
                        state.allocators.region_allocators[self.region as usize].get_max_order();
                    state
                        .allocators
                        .region_tracker
                        .mark_free(order, self.region);
                }
                self.page += 1;
                allowance -= 1;
            }
        }
        Ok(self.remaining())
    }
}

impl TransactionalMemory {
    pub(crate) fn needs_recovery_after_proof(&self) {
        self.state.lock().unwrap().header.recovery_required = false;
        self.needs_recovery.store(false, Ordering::Release);
    }

    pub(crate) fn start_recovery(&self, pin: TransactionGuard) -> Result {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        self.recovery_end.store(layout.len(), Ordering::Release);
        *self.sweep.lock().unwrap() = Some(Sweep {
            roots: [
                state.header.primary_slot().system_root,
                state.header.primary_slot().user_root,
            ],
            layout,
            pin: Arc::new(pin),
            live: PageTrackerPolicy::new_tracking(),
            phase: 0,
            position: (None, Vec::new()),
            entry: 0,
            item: 0,
            chain: None,
            proof: state.header.primary_slot().commit_pages,
            region: 0,
            page: 0,
            failed: false,
        });
        Ok(())
    }

    #[cfg(debug_assertions)]
    pub(crate) fn recovery_old_page(&self, page: PageNumber) -> bool {
        let end = self.recovery_end.load(Ordering::Acquire);
        end != 0
            && page
                .address_range(
                    self.page_size.into(),
                    self.region_size,
                    self.region_header_with_padding_size,
                    self.page_size,
                )
                .start
                < end
    }

    pub(crate) fn recovery_state(&self) -> Result<(Option<u64>, u64, u64)> {
        let sweep = self.sweep.lock().unwrap();
        if sweep.as_ref().is_some_and(|sweep| sweep.failed) {
            return Err(StorageError::Corrupted(
                "Free space not found: recovery damage".into(),
            ));
        }
        let remaining = sweep.as_ref().map(Sweep::remaining);
        drop(sweep);
        Ok((
            remaining,
            self.file_len()?,
            self.file_size_limit.load(Ordering::Acquire),
        ))
    }

    pub(crate) fn recovery_step(self: &Arc<Self>, allowance: usize) -> Result<u64> {
        let mut state = self.sweep.lock().unwrap();
        let Some(sweep) = state.as_mut() else {
            return Ok(0);
        };
        if sweep.failed {
            return Err(StorageError::Corrupted(
                "Free space not found: recovery damage".into(),
            ));
        }
        let remaining = match sweep.advance(self, allowance) {
            Ok(remaining) => remaining,
            Err(error) => {
                sweep.failed = true;
                return Err(error);
            }
        };
        if remaining == 0 && sweep.phase == 4 {
            *state = None;
            self.recovery_end.store(0, Ordering::Release);
        }
        Ok(remaining)
    }
}
