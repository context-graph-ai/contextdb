use crate::db::TransactionGuard;
use crate::tree_store::btree_base::{
    AccessGuardMut, BRANCH, BranchAccessor, BranchMutator, BtreeHeader, Checksum, DEFERRED, LEAF,
    LeafAccessor, branch_checksum, leaf_checksum,
};
use crate::tree_store::btree_iters::BtreeExtractIf;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, PageMut, TransactionalMemory};
use crate::tree_store::{
    AccessGuardMutInPlace, AllPageNumbersBtreeIter, BtreeRangeIter, PageHint, PageNumber,
    PageTrackerPolicy,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cmp::max;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

pub(super) fn page_checksum<T: Page>(
    page: &T,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<Checksum> {
    match page.memory().first().copied() {
        Some(LEAF) => leaf_checksum(page, fixed_key_size, fixed_value_size),
        Some(BRANCH) => branch_checksum(page, fixed_key_size),
        _ => Err(crate::StorageError::Corrupted(format!(
            "page {:?} has an invalid btree node type",
            page.get_page_number()
        ))),
    }
}

/// Locate a fixed-key byte value without loading its payload. Allocator bitmap
/// sources use this descriptor only while their saved tree is protected.
pub(crate) fn fixed_key_value_location<K: Key>(
    root: Option<BtreeHeader>,
    mem: &TransactionalMemory,
    hint: PageHint,
    key: &K::SelfType<'_>,
) -> Result<Option<(PageNumber, std::ops::Range<usize>)>> {
    let Some(mut header) = root else {
        return Ok(None);
    };
    let width = K::fixed_width().ok_or_else(|| {
        crate::StorageError::Corrupted("windowed allocator lookup requires fixed keys".into())
    })?;
    let query = K::as_bytes(key);
    let corrupt = || crate::StorageError::Corrupted("invalid windowed allocator node".into());
    let mut ancestors = Vec::new();
    loop {
        if ancestors.contains(&header.root) {
            return Err(corrupt());
        }
        let prefix = mem.get_page_window(header.root, 0..4, hint)?;
        let count = usize::from(u16::from_le_bytes(prefix[2..4].try_into().unwrap()));
        if count == 0 {
            return Err(corrupt());
        }
        if prefix[0] == LEAF && !hint.verifies_path() {
            // Fixed keys follow the value-end offsets. No selected or unrelated
            // allocator payload enters the page cache during navigation.
            let keys_start = 4 + count * 4;
            let values_start = keys_start + count * width;
            let extent = usize::try_from(
                header
                    .root
                    .page_size_bytes(mem.get_page_size().try_into().unwrap()),
            )
            .map_err(|_| corrupt())?;
            if values_start > extent {
                return Err(corrupt());
            }
            let mut low = 0;
            let mut high = count;
            while low < high {
                let mid = low + (high - low) / 2;
                let offset = keys_start + mid * width;
                let stored = mem.get_page_window(header.root, offset..offset + width, hint)?;
                match K::compare(query.as_ref(), &stored) {
                    std::cmp::Ordering::Less => high = mid,
                    std::cmp::Ordering::Greater => low = mid + 1,
                    std::cmp::Ordering::Equal => {
                        let field = 4 + mid * 4;
                        let offsets = mem.get_page_window(
                            header.root,
                            field.saturating_sub(4).max(4)..field + 4,
                            hint,
                        )?;
                        let end =
                            u32::from_le_bytes(offsets[offsets.len() - 4..].try_into().unwrap())
                                as usize;
                        let start = if mid == 0 {
                            values_start
                        } else {
                            u32::from_le_bytes(offsets[..4].try_into().unwrap()) as usize
                        };
                        if start < values_start || end < start || end > extent {
                            return Err(corrupt());
                        }
                        return Ok(Some((header.root, start..end)));
                    }
                }
            }
            return Ok(None);
        }
        let page = mem.get_page(header.root, hint)?;
        if hint.verifies_path() && page_checksum(&page, Some(width), None)? != header.checksum {
            return Err(crate::StorageError::Corrupted(
                "allocator source checksum failed".into(),
            ));
        }
        match page.memory()[0] {
            LEAF => {
                let leaf = LeafAccessor::new(page.memory(), Some(width), None);
                return Ok(leaf.find_key::<K>(query.as_ref()).map(|index| {
                    let (start, end) = leaf.value_range(index).unwrap();
                    (header.root, start..end)
                }));
            }
            BRANCH => {
                ancestors.push(header.root);
                let branch = BranchAccessor::new(&page, Some(width));
                let (index, child) = branch.child_for_key::<K>(query.as_ref());
                header = BtreeHeader::new(child, branch.child_checksum(index).unwrap(), 0);
            }
            _ => return Err(corrupt()),
        }
    }
}

pub(crate) struct BtreeStats {
    pub(crate) tree_height: u32,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct PagePath {
    path: Vec<PageNumber>,
}

impl PagePath {
    pub(crate) fn new_root(page_number: PageNumber) -> Self {
        Self {
            path: vec![page_number],
        }
    }

    pub(crate) fn with_child(&self, page_number: PageNumber) -> Self {
        let mut path = self.path.clone();
        path.push(page_number);
        Self { path }
    }

    pub(crate) fn with_subpath(&self, other: &Self) -> Self {
        let mut path = self.path.clone();
        path.extend(&other.path);
        Self { path }
    }

    pub(crate) fn parents(&self) -> &[PageNumber] {
        &self.path[..self.path.len() - 1]
    }

    pub(crate) fn page_number(&self) -> PageNumber {
        self.path[self.path.len() - 1]
    }
}

pub(crate) struct UntypedBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    hint: PageHint,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl UntypedBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        hint: PageHint,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            hint,
            key_width,
            value_width,
        }
    }

    // Applies visitor to pages in the tree
    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            self.visit_pages_helper(PagePath::new_root(page_number), &mut visitor)?;
        }

        Ok(())
    }

    // The continuation is a position in the current tree, never a retained
    // page identity. A writer may split or merge pages between batches; that
    // can revisit or skip a position in this sweep, but relocation always
    // uses paths resolved inside the current write transaction. Later sweeps
    // revisit the complete tree without retaining an old storage snapshot.
    pub(crate) fn page_batch(
        &self,
        resume: &[usize],
        maximum: usize,
        multimap: bool,
    ) -> Result<(Vec<PagePath>, Option<Vec<usize>>, usize)> {
        let mut batch = PageBatch {
            resume,
            maximum,
            pages: Vec::new(),
            next: None,
            examined: 0,
        };
        if let Some(root) = self.root {
            self.page_batch_helper(
                PagePath::new_root(root.root),
                &mut Vec::new(),
                multimap,
                &mut batch,
            )?;
        }
        Ok((batch.pages, batch.next, batch.examined))
    }

    fn page_batch_helper(
        &self,
        path: PagePath,
        position: &mut Vec<usize>,
        multimap: bool,
        batch: &mut PageBatch<'_>,
    ) -> Result<bool> {
        if position.as_slice() < batch.resume && !batch.resume.starts_with(position) {
            return Ok(true);
        }
        if position.as_slice() >= batch.resume {
            if batch.pages.len() == batch.maximum {
                batch.next = Some(position.clone());
                return Ok(false);
            }
            batch.pages.push(path.clone());
        }
        batch.examined += 1;
        let page = self.mem.get_page(path.page_number(), self.hint)?;
        match page.memory()[0] {
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    position.push(i);
                    let complete = self.page_batch_helper(
                        path.with_child(accessor.child_page(i).unwrap()),
                        position,
                        multimap,
                        batch,
                    )?;
                    position.pop();
                    if !complete {
                        return Ok(false);
                    }
                }
            }
            LEAF if multimap => {
                for (i, header) in crate::multimap_table::parse_subtree_roots(
                    &page,
                    self.key_width,
                    self.value_width,
                )
                .into_iter()
                .enumerate()
                {
                    let subtree = Self::new(
                        Some(header),
                        self.mem.clone(),
                        self.hint,
                        self.value_width,
                        Some(0),
                    );
                    position.push(i);
                    let complete = subtree.page_batch_helper(
                        path.with_child(header.root),
                        position,
                        false,
                        batch,
                    )?;
                    position.pop();
                    if !complete {
                        return Ok(false);
                    }
                }
            }
            LEAF => {}
            _ => unreachable!(),
        }
        Ok(true)
    }

    fn visit_pages_helper<F>(&self, path: PagePath, visitor: &mut F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        visitor(&path)?;
        let page = self.mem.get_page(path.page_number(), self.hint)?;

        match page.memory()[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    let child_path = path.with_child(child_page);
                    self.visit_pages_helper(child_path, visitor)?;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

struct PageBatch<'a> {
    resume: &'a [usize],
    maximum: usize,
    pages: Vec<PagePath>,
    next: Option<Vec<usize>>,
    examined: usize,
}

pub(crate) struct UntypedBtreeMut {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl UntypedBtreeMut {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            freed_pages,
            key_width,
            value_width,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    // Recomputes the checksum for all pages that are uncommitted
    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut root = self.root;
        if let Some(BtreeHeader {
            root: ref p,
            ref mut checksum,
            length: _,
        }) = root
        {
            if !self.mem.uncommitted(*p) {
                // root page is clean
                return Ok(root);
            }

            *checksum = self.finalize_dirty_checksums_helper(*p)?;
            self.root = root;
        }

        Ok(root)
    }

    fn finalize_dirty_checksums_helper(&mut self, page_number: PageNumber) -> Result<Checksum> {
        assert!(self.mem.uncommitted(page_number));
        let mut page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => leaf_checksum(&page, self.key_width, self.value_width),
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                let mut new_children = vec![];
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        let new_checksum = self.finalize_dirty_checksums_helper(child_page)?;
                        new_children.push(Some((i, child_page, new_checksum)));
                    } else {
                        // Child is clean, skip it
                        new_children.push(None);
                    }
                }

                let mut mutator = BranchMutator::new(page.memory_mut());
                for (child_index, child_page, child_checksum) in new_children.into_iter().flatten()
                {
                    mutator.write_child_page(child_index, child_page, child_checksum);
                }

                branch_checksum(&page, self.key_width)
            }
            _ => unreachable!(),
        }
    }

    // Applies visitor to all dirty leaf pages in the tree
    pub(crate) fn dirty_leaf_visitor<F>(&mut self, visitor: F) -> Result
    where
        F: for<'a> Fn(PageMut<'a>) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            if !self.mem.uncommitted(page_number) {
                // root page is clean
                return Ok(());
            }

            let page = self.mem.get_page_mut(page_number)?;
            match page.memory()[0] {
                LEAF => {
                    visitor(page)?;
                }
                BRANCH => {
                    drop(page);
                    self.dirty_leaf_visitor_helper(page_number, &visitor)?;
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    fn dirty_leaf_visitor_helper<F>(&mut self, page_number: PageNumber, visitor: &F) -> Result
    where
        F: for<'a> Fn(PageMut<'a>) -> Result,
    {
        assert!(self.mem.uncommitted(page_number));
        let page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => {
                visitor(page)?;
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        self.dirty_leaf_visitor_helper(child_page, visitor)?;
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        if let Some(root) = self.get_root()
            && let Some((new_root, new_checksum)) =
                self.relocate_helper(root.root, relocation_map)?
        {
            self.root = Some(BtreeHeader::new(new_root, new_checksum, root.length));
            return Ok(true);
        }
        Ok(false)
    }

    // Relocates the given subtree to the pages specified in relocation_map
    fn relocate_helper(
        &mut self,
        page_number: PageNumber,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        let mut new_page = if let Some(new_page_number) = relocation_map.get(&page_number) {
            self.mem.get_page_mut(*new_page_number)?
        } else {
            return Ok(None);
        };
        let old_page = self.mem.get_page(page_number, PageHint::None)?;
        new_page.memory_mut().copy_from_slice(old_page.memory());

        let node_mem = old_page.memory();
        match node_mem[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&old_page, self.key_width);
                let mut mutator = BranchMutator::new(new_page.memory_mut());
                for i in 0..accessor.count_children() {
                    let child = accessor.child_page(i).unwrap();
                    if let Some((new_child, new_checksum)) =
                        self.relocate_helper(child, relocation_map)?
                    {
                        mutator.write_child_page(i, new_child, new_checksum);
                    }
                }
            }
            _ => unreachable!(),
        }

        let mut freed_pages = self.freed_pages.lock().unwrap();
        // No need to track allocations, because this method is only called during compaction when
        // there can't be any savepoints
        let mut ignore = PageTrackerPolicy::Ignore;
        if !self.mem.free_if_uncommitted(page_number, &mut ignore) {
            freed_pages.push(page_number);
            #[cfg(feature = "test-seams")]
            crate::commit_work::note_freed_page();
        }

        Ok(Some((new_page.get_page_number(), DEFERRED)))
    }
}

pub(crate) struct BtreeMut<'a, K: Key + 'static, V: Value + 'static> {
    pub(super) mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeMut<'_, K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            root,
            freed_pages,
            allocated_pages,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        self.root = tree.finalize_dirty_checksums()?;
        Ok(self.root)
    }

    #[allow(dead_code)]
    pub(crate) fn all_pages_iter(&self) -> Result<Option<AllPageNumbersBtreeIter>> {
        if let Some(root) = self.root.map(|x| x.root) {
            Ok(Some(AllPageNumbersBtreeIter::new(
                root,
                K::fixed_width(),
                V::fixed_width(),
                self.mem.clone(),
                PageHint::None,
            )?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        self.read_tree()?.visit_all_pages(visitor)
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn set_root(&mut self, root: Option<BtreeHeader>) {
        self.root = root;
    }

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        if tree.relocate(relocation_map)? {
            self.root = tree.get_root();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with value of length {}",
            &self.root,
            key,
            V::as_bytes(value).as_ref().len()
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    // Insert without allocating or freeing any pages. This requires that you've previously
    // inserted the same key, with a value of at least the same serialized length, earlier
    // in the same transaction. If those preconditions aren't satisfied, insert_inplace()
    // will panic; it won't allocate under any circumstances
    pub(crate) fn insert_inplace(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<()> {
        let mut fake_freed_pages = vec![];
        let fake_allocated_pages = Arc::new(Mutex::new(PageTrackerPolicy::Closed));
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.mem.clone(),
            fake_freed_pages.as_mut(),
            fake_allocated_pages,
        );
        operation.insert_inplace(key, value)?;
        assert!(fake_freed_pages.is_empty());
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
            PageHint::None,
        )
    }

    fn read_tree(&self) -> Result<Btree<K, V>> {
        Btree::new(
            self.get_root(),
            PageHint::None,
            self.transaction_guard.clone(),
            self.mem.clone(),
        )
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        self.read_tree()?.get(key)
    }

    pub(crate) fn get_mut(
        &mut self,
        key: &K::SelfType<'_>,
    ) -> Result<Option<AccessGuardMut<'_, V>>> {
        if let Some(ref mut root) = self.root {
            let key_bytes = K::as_bytes(key);
            let query = key_bytes.as_ref();
            let page_mut = if self.mem.uncommitted(root.root) {
                self.mem.get_page_mut(root.root)?
            } else {
                let mut freed_pages = self.freed_pages.lock().unwrap();
                let mut allocated = self.allocated_pages.lock().unwrap();
                let required: usize = root
                    .root
                    .page_size_bytes(self.mem.get_page_size().try_into().unwrap())
                    .try_into()
                    .unwrap();
                let mut new_page = self.mem.allocate(required, &mut allocated)?;
                let old_page = self.mem.get_page(root.root, PageHint::None)?;
                new_page.memory_mut().copy_from_slice(old_page.memory());
                drop(old_page);
                freed_pages.push(root.root);
                #[cfg(feature = "test-seams")]
                crate::commit_work::note_freed_page();

                root.root = new_page.get_page_number();
                root.checksum = DEFERRED;
                new_page
            };
            self.get_mut_helper(None, page_mut, query)
        } else {
            Ok(None)
        }
    }

    fn get_mut_helper<'txn>(
        &'txn mut self,
        parent: Option<(PageMut<'txn>, usize)>,
        mut page: PageMut<'txn>,
        query: &[u8],
    ) -> Result<Option<AccessGuardMut<'txn, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuardMut::new(
                        page,
                        start,
                        end - start,
                        entry_index,
                        parent,
                        self.mem.clone(),
                        self.allocated_pages.clone(),
                        self.root.as_mut().unwrap(),
                        K::fixed_width(),
                    );
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let (child_index, child_page) = {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    accessor.child_for_key::<K>(query)
                };
                let child_page_mut = if self.mem.uncommitted(child_page) {
                    self.mem.get_page_mut(child_page)?
                } else {
                    let mut freed_pages = self.freed_pages.lock().unwrap();
                    let mut allocated = self.allocated_pages.lock().unwrap();
                    let required: usize = child_page
                        .page_size_bytes(self.mem.get_page_size().try_into().unwrap())
                        .try_into()
                        .unwrap();
                    let mut new_page = self.mem.allocate(required, &mut allocated)?;
                    let old_child_page = self.mem.get_page(child_page, PageHint::None)?;
                    new_page
                        .memory_mut()
                        .copy_from_slice(old_child_page.memory());
                    drop(old_child_page);
                    freed_pages.push(child_page);
                    #[cfg(feature = "test-seams")]
                    crate::commit_work::note_freed_page();

                    let mut mutator = BranchMutator::new(page.memory_mut());
                    mutator.write_child_page(child_index, new_page.get_page_number(), DEFERRED);
                    new_page
                };
                self.get_mut_helper(Some((page, child_index)), child_page_mut, query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.first()
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.last()
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR> + 'a0, KR: Borrow<K::SelfType<'a0>> + 'a0>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>>
    where
        K: 'a0,
    {
        self.read_tree()?.range(range)
    }

    pub(crate) fn extract_from_if<
        'a,
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::SelfType<'a0>> + 'a0,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    >(
        &'a mut self,
        range: &'_ T,
        predicate: F,
    ) -> Result<BtreeExtractIf<'a, K, V, F>>
    where
        K: 'a0,
    {
        let iter = self.range(range)?;

        let result = BtreeExtractIf::new(
            &mut self.root,
            iter,
            predicate,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            self.mem.clone(),
        );

        Ok(result)
    }

    pub(crate) fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        mut predicate: F,
        range: impl RangeBounds<KR> + 'a,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let iter = self.range(&range)?;
        let mut freed = vec![];
        // Do not modify the existing tree, because we're iterating over it concurrently with the removals
        // TODO: optimize this to iterate and remove at the same time
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
            &mut self.root,
            self.mem.clone(),
            &mut freed,
            self.allocated_pages.clone(),
        );
        for entry in iter {
            let entry = entry?;
            if !predicate(entry.key(), entry.value()) {
                assert!(operation.delete(&entry.key())?.is_some());
            }
        }
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut allocated_pages = self.allocated_pages.lock().unwrap();
        for page in freed {
            if !self.mem.free_if_uncommitted(page, &mut allocated_pages) {
                freed_pages.push(page);
                #[cfg(feature = "test-seams")]
                crate::commit_work::note_freed_page();
            }
        }

        Ok(())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        self.read_tree()?.len()
    }
}

impl<'a, K: Key + 'a, V: MutInPlaceValue + 'a> BtreeMut<'a, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to `value_length`
    // Return type has the same lifetime as &self, because the tree must not be modified until the mutable guard is dropped
    pub(crate) fn insert_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, V>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root, key, value_length
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut value = vec![0u8; value_length];
        V::initialize(&mut value);
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let (_, guard) = operation.insert(key, &V::from_bytes(&value))?;
        Ok(guard)
    }
}

impl<K: Key + 'static> BtreeMut<'_, K, &'static [u8]> {
    /// Reserve an allocator-table value without building a value-sized heap buffer.
    /// A length-only source zero-fills the allocated B-tree destination. The
    /// returned guard borrows that destination; no source buffer or mapping exists.
    pub(crate) fn insert_zeroed_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, &'static [u8]>> {
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, &'static [u8]> = MutateHelper::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let (_, guard) =
            operation.insert_value(key, super::btree_base::LeafValue::Zeroes(value_length))?;
        Ok(guard)
    }
}

pub(crate) struct RawBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    hint: PageHint,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl RawBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: Arc<TransactionalMemory>,
        hint: PageHint,
    ) -> Self {
        Self {
            mem,
            root,
            hint,
            fixed_key_size,
            fixed_value_size,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            self.fixed_key_size,
            self.fixed_value_size,
            self.hint,
        )
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        if let Some(header) = self.root {
            self.verify_checksum_helper(header.root, header.checksum)
        } else {
            Ok(true)
        }
    }

    /// Authenticate only the saved root page during external admission.
    /// Its descendants remain protected by the transaction floor and are
    /// checked when the retained reader actually visits them.
    pub(crate) fn verify_root_checksum(&self) -> Result<bool> {
        let Some(header) = self.root else {
            return Ok(true);
        };
        let page = self.mem.get_page(header.root, self.hint)?;
        let computed = match page.memory().first().copied() {
            Some(LEAF) => leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size),
            Some(BRANCH) => branch_checksum(&page, self.fixed_key_size),
            _ => return Ok(false),
        };
        Ok(computed.is_ok_and(|computed| computed == header.checksum))
    }

    fn verify_checksum_helper(
        &self,
        page_number: PageNumber,
        expected_checksum: Checksum,
    ) -> Result<bool> {
        let page = self.mem.get_page(page_number, self.hint)?;
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                if let Ok(computed) =
                    leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size)
                {
                    expected_checksum == computed
                } else {
                    false
                }
            }
            BRANCH => {
                if let Ok(computed) = branch_checksum(&page, self.fixed_key_size) {
                    if expected_checksum != computed {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    if !self.verify_checksum_helper(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            _ => false,
        })
    }
}

pub(crate) struct Btree<K: Key + 'static, V: Value + 'static> {
    pub(super) mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    // Cache of the root page to avoid repeated lookups
    cached_root: Option<PageImpl>,
    root: Option<BtreeHeader>,
    pub(super) hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let cached_root = if let Some(header) = root {
            let page = mem.get_page(header.root, hint)?;
            if hint.verifies_path()
                && page_checksum(&page, K::fixed_width(), V::fixed_width())? != header.checksum
            {
                return Err(crate::StorageError::Corrupted(format!(
                    "retained snapshot root page {:?} failed checksum verification",
                    header.root
                )));
            }
            Some(page)
        } else {
            None
        };
        Ok(Self {
            mem,
            transaction_guard: guard,
            cached_root,
            root,
            hint,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn new_lazy(
        root: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            cached_root: None,
            root,
            hint,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        &self.transaction_guard
    }

    pub(crate) fn hint(&self) -> PageHint {
        self.hint
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            V::fixed_width(),
            self.mem.clone(),
            self.hint,
        )
        .verify_checksum()
    }

    pub(crate) fn verify_root_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            V::fixed_width(),
            self.mem.clone(),
            self.hint,
        )
        .verify_root_checksum()
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        let tree = UntypedBtree::new(
            self.root,
            self.mem.clone(),
            self.hint,
            K::fixed_width(),
            V::fixed_width(),
        );
        tree.visit_all_pages(visitor)
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'static, V>>> {
        if let Some(ref root_page) = self.cached_root {
            self.get_helper(root_page, K::as_bytes(key).as_ref())
        } else if let Some(root) = self.root {
            let page = self.mem.get_page(root.root, self.hint)?;
            self.verify_path_page(&page, root.checksum)?;
            self.get_helper(&page, K::as_bytes(key).as_ref())
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: &PageImpl, query: &[u8]) -> Result<Option<AccessGuard<'static, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuard::with_page(page.clone(), start..end);
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(page, K::fixed_width());
                let (child_index, child_page) = accessor.child_for_key::<K>(query);
                let expected = accessor.child_checksum(child_index).unwrap();
                let child_page = self.mem.get_page(child_page, self.hint)?;
                self.verify_path_page(&child_page, expected)?;
                self.get_helper(&child_page, query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.first_helper(root.clone())
        } else if let Some(root) = self.root {
            let page = self.mem.get_page(root.root, self.hint)?;
            self.verify_path_page(&page, root.checksum)?;
            self.first_helper(page)
        } else {
            Ok(None)
        }
    }

    fn first_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) = accessor.entry_ranges(0).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(0).unwrap();
                let expected = accessor.child_checksum(0).unwrap();
                let page = self.mem.get_page(child_page, self.hint)?;
                self.verify_path_page(&page, expected)?;
                self.first_helper(page)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.last_helper(root.clone())
        } else if let Some(root) = self.root {
            let page = self.mem.get_page(root.root, self.hint)?;
            self.verify_path_page(&page, root.checksum)?;
            self.last_helper(page)
        } else {
            Ok(None)
        }
    }

    fn last_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) =
                    accessor.entry_ranges(accessor.num_pairs() - 1).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = accessor.count_children() - 1;
                let child_page = accessor.child_page(child_index).unwrap();
                let expected = accessor.child_checksum(child_index).unwrap();
                let page = self.mem.get_page(child_page, self.hint)?;
                self.verify_path_page(&page, expected)?;
                self.last_helper(page)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a0>>>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>> {
        // The root page was read and checked against `self.root` when this
        // tree was opened; the range descends from that same page.
        BtreeRangeIter::new_with_root_page(
            range,
            self.root,
            match (&self.cached_root, self.root) {
                (Some(page), _) => Some(page.clone()),
                (None, Some(root)) => {
                    let page = self.mem.get_page(root.root, self.hint)?;
                    self.verify_path_page(&page, root.checksum)?;
                    Some(page)
                }
                (None, None) => None,
            },
            self.mem.clone(),
            self.hint,
        )
    }

    fn verify_path_page(&self, page: &PageImpl, expected: Checksum) -> Result<()> {
        if self.hint.verifies_path()
            && page_checksum(page, K::fixed_width(), V::fixed_width())? != expected
        {
            return Err(crate::StorageError::Corrupted(format!(
                "retained snapshot page {:?} failed checksum verification",
                page.get_page_number()
            )));
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
            self.hint,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        if let Some(p) = self.root.map(|x| x.root) {
            let mut pages = vec![self.mem.get_page(p, self.hint)?];
            while !pages.is_empty() {
                let mut next_children = vec![];
                for page in pages.drain(..) {
                    let node_mem = page.memory();
                    match node_mem[0] {
                        LEAF => {
                            eprint!("Leaf[ (page={:?})", page.get_page_number());
                            LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width())
                                .print_node::<K, V>(include_values);
                            eprint!("]");
                        }
                        BRANCH => {
                            let accessor = BranchAccessor::new(&page, K::fixed_width());
                            for i in 0..accessor.count_children() {
                                let child = accessor.child_page(i).unwrap();
                                next_children.push(self.mem.get_page(child, self.hint)?);
                            }
                            accessor.print_node::<K>();
                        }
                        _ => unreachable!(),
                    }
                    eprint!("  ");
                }
                eprintln!();

                pages = next_children;
            }
        }

        Ok(())
    }
}

impl<K: Key, V: Value> Drop for Btree<K, V> {
    fn drop(&mut self) {
        // Make sure that we clear our reference to the root page, before the transaction guard goes out of scope
        self.cached_root = None;
    }
}

pub(crate) fn btree_stats(
    root: Option<PageNumber>,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    hint: PageHint,
) -> Result<BtreeStats> {
    if let Some(root) = root {
        stats_helper(root, mem, fixed_key_size, fixed_value_size, hint)
    } else {
        Ok(BtreeStats {
            tree_height: 0,
            leaf_pages: 0,
            branch_pages: 0,
            stored_leaf_bytes: 0,
            metadata_bytes: 0,
            fragmented_bytes: 0,
        })
    }
}

fn stats_helper(
    page_number: PageNumber,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    hint: PageHint,
) -> Result<BtreeStats> {
    let page = mem.get_page(page_number, hint)?;
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
            let leaf_bytes = accessor.length_of_pairs(0, accessor.num_pairs());
            let overhead_bytes = accessor.total_length() - leaf_bytes;
            let fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            Ok(BtreeStats {
                tree_height: 1,
                leaf_pages: 1,
                branch_pages: 0,
                stored_leaf_bytes: leaf_bytes.try_into().unwrap(),
                metadata_bytes: overhead_bytes.try_into().unwrap(),
                fragmented_bytes,
            })
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, fixed_key_size);
            let mut max_child_height = 0;
            let mut leaf_pages = 0;
            let mut branch_pages = 1;
            let mut stored_leaf_bytes = 0;
            let mut metadata_bytes = accessor.total_length() as u64;
            let mut fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    let stats = stats_helper(child, mem, fixed_key_size, fixed_value_size, hint)?;
                    max_child_height = max(max_child_height, stats.tree_height);
                    leaf_pages += stats.leaf_pages;
                    branch_pages += stats.branch_pages;
                    stored_leaf_bytes += stats.stored_leaf_bytes;
                    metadata_bytes += stats.metadata_bytes;
                    fragmented_bytes += stats.fragmented_bytes;
                }
            }

            Ok(BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes,
                metadata_bytes,
                fragmented_bytes,
            })
        }
        _ => unreachable!(),
    }
}

#[cfg(feature = "contextdb-internal")]
struct NativeBorrowedPage<'a> {
    memory: &'a super::page_store::PageMemory,
    number: PageNumber,
}
#[cfg(feature = "contextdb-internal")]
impl Page for NativeBorrowedPage<'_> {
    fn memory(&self) -> &[u8] { self.memory }
    fn get_page_number(&self) -> PageNumber { self.number }
}

/// Validate every offset, ordered key and child encoding before ordinary
/// accessors can index the bytes. Returns the exact checksummed node end.
#[cfg(feature = "contextdb-internal")]
fn native_validate_byte_node(
    bytes: &[u8], number: PageNumber, directory: bool,
) -> std::result::Result<usize, crate::NativeColdReadError> {
    use crate::{NativeColdFault, NativeColdReadError};
    let corrupt = || {
        let mut fault = NativeColdFault::new("invalid native byte node layout");
        fault.page = number.to_le_bytes();
        NativeColdReadError::Corrupt(fault)
    };
    let header = bytes.get(..4).ok_or_else(corrupt)?;
    let count = usize::from(u16::from_le_bytes(header[2..4].try_into().unwrap()));
    if count == 0 { return Err(corrupt()); }
    let (offset_start, data_start, offset_count) = match header[0] {
        LEAF => (4, 4 + count * 8, count * 2),
        BRANCH => {
            let children = count + 1;
            let page_start = 8 + size_of::<Checksum>() * children;
            let offset_start = page_start + PageNumber::serialized_size() * children;
            let data_start = offset_start + 4 * count;
            let pages = bytes.get(page_start..offset_start).ok_or_else(corrupt)?;
            for page in pages.chunks_exact(PageNumber::serialized_size()) {
                if PageNumber::validated_from_le_bytes(page.try_into().unwrap()).is_none() {
                    return Err(corrupt());
                }
            }
            (offset_start, data_start, count)
        }
        _ => return Err(corrupt()),
    };
    let offsets = bytes.get(offset_start..data_start).ok_or_else(corrupt)?;
    if offsets.len() != offset_count * 4 { return Err(corrupt()); }
    let mut start = data_start;
    let mut previous: Option<&[u8]> = None;
    for (index, offset) in offsets.chunks_exact(4).enumerate() {
        let end = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
        let value = bytes.get(start..end).ok_or_else(corrupt)?;
        if index < count {
            if previous.is_some_and(|prior| prior >= value)
                || (directory && std::str::from_utf8(value).is_err()) {
                return Err(corrupt());
            }
            previous = Some(value);
        }
        start = end;
    }
    Ok(start)
}

/// Exact variable-byte descent over one captured root. Does not open a table,
/// decode owned metadata, construct iterator parents, or call get_page.
#[cfg(feature = "contextdb-internal")]
pub(crate) fn native_byte_get(
    root: Option<BtreeHeader>, mem: &TransactionalMemory, key: &[u8], directory: bool,
) -> std::result::Result<Option<AccessGuard<'static, &'static [u8]>>, crate::NativeColdReadError> {
    use crate::native_storage_admission::NativeStorageBuffer;
    use crate::{NativeColdFault, NativeColdReadError};
    if !crate::native_storage_admission::native_storage_active() {
        return Err(NativeColdReadError::AdmissionRefused);
    }
    let Some(mut header) = root else { return Ok(None); };
    let mut ancestors = NativeStorageBuffer::<PageNumber>::new();
    loop {
        if ancestors.as_slice().contains(&header.root) {
            let mut fault = NativeColdFault::new("cycle in native exact byte descent");
            fault.page = header.root.to_le_bytes();
            return Err(NativeColdReadError::Corrupt(fault));
        }
        // Admit navigation growth before reading the corresponding page.
        ancestors.push(header.root)?;
        let memory = mem.get_native_page_memory(header.root)?;
        let end = native_validate_byte_node(&memory, header.root, directory)?;
        let actual = super::page_store::xxh3_checksum(&memory[..end]);
        if actual != header.checksum {
            return Err(NativeColdReadError::Corrupt(NativeColdFault {
                reason: "captured native byte page failed checksum verification",
                page: header.root.to_le_bytes(), offset: end,
                expected: header.checksum, actual,
            }));
        }
        match memory[0] {
            LEAF => {
                let leaf = LeafAccessor::new(&memory, None, None);
                let range = leaf.find_key::<&[u8]>(key).map(|index| {
                    let (start, end) = leaf.value_range(index).expect("validated native leaf range");
                    start..end
                });
                return Ok(range.map(|range| AccessGuard::with_arc_page(memory, range)));
            }
            BRANCH => {
                let page = NativeBorrowedPage { memory: &memory, number: header.root };
                let branch = BranchAccessor::new(&page, None);
                let (index, child) = branch.child_for_key::<&[u8]>(key);
                header = BtreeHeader::new(child, branch.child_checksum(index).expect("validated child checksum"), 0);
            }
            _ => unreachable!("validated native node kind"),
        }
    }
}

/// Internal variable text-key range successor. The endpoint is raw bytes so
/// a prefix's exclusive successor need not itself be valid UTF-8. Every node
/// is fully validated/checksummed, and navigation/page/header storage is prepaid.
#[cfg(feature = "contextdb-internal")]
pub(crate) fn native_text_range_next(
    root: Option<BtreeHeader>, mem: &TransactionalMemory,
    lower: &[u8], exclusive: bool, upper: Option<&[u8]>,
        text_keys: bool, before_page: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>,
) -> std::result::Result<Option<(AccessGuard<'static, &'static [u8]>, AccessGuard<'static, &'static [u8]>)>, crate::NativeColdReadError> {
    use crate::native_storage_admission::NativeStorageBuffer;
    use crate::{NativeColdFault, NativeColdReadError};
    if !crate::native_storage_admission::native_storage_active() {
        return Err(NativeColdReadError::AdmissionRefused);
    }
    let Some(mut header) = root else { return Ok(None); };
    let mut parents = NativeStorageBuffer::<BtreeHeader>::new();
    // A monotone seek visits only the boundary descent and the next sibling's
    // left descent. Retaining visited identities also detects cross-sibling cycles.
    let mut visited = NativeStorageBuffer::<PageNumber>::new();
    let mut leftmost = false;
    loop {
        if visited.as_slice().contains(&header.root) {
            return Err(NativeColdReadError::Corrupt(NativeColdFault::new("cycle in native text range descent")));
        }
        before_page()?;
        visited.push(header.root)?;
        let memory = mem.get_native_page_memory(header.root)?;
        let end = native_validate_byte_node(&memory, header.root, text_keys)?;
        let actual = super::page_store::xxh3_checksum(&memory[..end]);
        if actual != header.checksum {
            return Err(NativeColdReadError::Corrupt(NativeColdFault {
                reason: "captured native text range page failed checksum verification",
                page: header.root.to_le_bytes(), offset: end,
                expected: header.checksum, actual,
            }));
        }
        if memory[0] == BRANCH {
            let page = NativeBorrowedPage { memory: &memory, number: header.root };
            let branch = BranchAccessor::new(&page, None);
            let index = if leftmost { 0 } else { branch.child_for_key::<&[u8]>(lower).0 };
            let child = branch.child_page(index).expect("validated child");
            let checksum = branch.child_checksum(index).expect("validated child checksum");
            // Save the next sibling as scalar metadata before any child fetch.
            if index + 1 < branch.count_children() {
                let sibling = BtreeHeader::new(branch.child_page(index + 1).expect("validated sibling"),
                    branch.child_checksum(index + 1).expect("validated sibling checksum"), 0);
                parents.push(sibling)?;
            }
            header = BtreeHeader::new(child, checksum, 0);
            continue;
        }
        let leaf = LeafAccessor::new(&memory, None, None);
        let (position, found) = if leftmost { (0, false) } else { leaf.position::<&[u8]>(lower) };
        let index = position + usize::from(found && exclusive && !leftmost);
        if let Some((key_range, value_range)) = leaf.entry_ranges(index) {
            let key = &memory[key_range.clone()];
            if upper.is_some_and(|end| key >= end) { return Ok(None); }
            // UTF-8 for all node keys was checked before indexing. Both guards
            // share the actual admitted complete page, never an owned record.
            return Ok(Some((AccessGuard::with_arc_page(memory.clone(), key_range),
                AccessGuard::with_arc_page(memory, value_range))));
        }
        drop(memory);
        let Some(sibling) = parents.pop() else { return Ok(None); };
        header = sibling;
        leftmost = true;
    }
}
