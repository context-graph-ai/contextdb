use crate::db::TransactionGuard;
use crate::error::TableError;
use crate::multimap_table::{
    finalize_tree_and_subtree_checksums, multimap_btree_stats, verify_tree_and_subtree_checksums,
};
use crate::native_storage_admission::{PendingAlloc, PendingName, pending_allocation_error};
use crate::tree_store::btree::{UntypedBtreeMut, btree_stats};
use crate::tree_store::btree_base::BtreeHeader;
use crate::tree_store::{
    Btree, BtreeMut, BtreeRangeIter, InternalTableDefinition, PageHint, PageNumber, PagePath,
    PageTrackerPolicy, RawBtree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{DatabaseStats, Result};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;
use std::ops::RangeFull;
use std::sync::{Arc, Mutex};
use std::{mem, thread};

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct PageListMut {
    data: [u8],
}

impl PageListMut {
    pub(crate) fn push_back(&mut self, value: PageNumber) {
        let len = u16::from_le_bytes(self.data[..size_of::<u16>()].try_into().unwrap());
        self.data[..size_of::<u16>()].copy_from_slice(&(len + 1).to_le_bytes());
        let len: usize = len.into();
        let start = size_of::<u16>() + PageNumber::serialized_size() * len;
        self.data[start..(start + PageNumber::serialized_size())]
            .copy_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn pop_back(&mut self) -> Option<PageNumber> {
        let len = u16::from_le_bytes(self.data[..size_of::<u16>()].try_into().unwrap());
        let remaining = len.checked_sub(1)?;
        self.data[..size_of::<u16>()].copy_from_slice(&remaining.to_le_bytes());
        let start = size_of::<u16>() + PageNumber::serialized_size() * usize::from(remaining);
        Some(PageNumber::from_le_bytes(
            self.data[start..start + PageNumber::serialized_size()]
                .try_into()
                .unwrap(),
        ))
    }

    pub(crate) fn clear(&mut self) {
        self.data[..size_of::<u16>()].fill(0);
    }
}

pub struct TableNameIter {
    inner: BtreeRangeIter<&'static str, InternalTableDefinition>,
    table_type: TableType,
}

impl Iterator for TableNameIter {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.inner.by_ref() {
            match entry {
                Ok(entry) => {
                    if entry.value().get_type() == self.table_type {
                        return Some(Ok(entry.key().to_string()));
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
        }
        None
    }
}

pub(crate) struct TableTree {
    tree: Btree<&'static str, InternalTableDefinition>,
    mem: Arc<TransactionalMemory>,
    #[cfg(feature = "contextdb-internal")]
    prepared_root: Option<super::page_store::PageMemory>,
}

impl TableTree {
    pub(crate) fn root(&self) -> Option<BtreeHeader> {
        self.tree.get_root()
    }

    pub(crate) fn hint(&self) -> PageHint {
        self.tree.hint()
    }

    pub(crate) fn stats(&self) -> Result<DatabaseStats> {
        let view = TableTreeMut::new(
            self.tree.get_root(),
            self.tree.transaction_guard().clone(),
            self.mem.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(PageTrackerPolicy::Ignore)),
        );
        view.stats()
    }
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        page_hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        Ok(Self {
            tree: Btree::new(master_root, page_hint, guard, mem.clone())?,
            mem,
            #[cfg(feature = "contextdb-internal")]
            prepared_root: None,
        })
    }


    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn new_prepared(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> std::result::Result<Self, crate::PreparedReadError> {
        let prepared_root = super::btree::prepared_directory_root(root, &mem)?;
        Ok(Self {
            tree: Btree::new_lazy(root, PageHint::Verified, guard, mem.clone()),
            mem,
            prepared_root,
        })
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn prepared_bytes_equal(
        &self,
        physical_name: &str,
        key: &[u8],
        expected: &[u8],
    ) -> std::result::Result<bool, crate::PreparedReadError> {
        let Some(directory) = super::btree::prepared_byte_get(
            self.root(), &self.mem, physical_name.as_bytes(), true, self.prepared_root.as_ref(),
        )? else { return Ok(false); };
        // Validate both stored names in place. The directory guard keeps the
        // original bytes alive until the scalar root has been extracted.
        let root = match super::table_tree_base::native_byte_table_root(directory.value()) {
            Ok(root) => root,
            Err(cause) => return Err(crate::PreparedReadError::TableDefinition(
                crate::PreparedTableDefinitionError::new(cause, directory))),
        };
        let value = super::btree::prepared_byte_get(root, &self.mem, key, false, None)?;
        Ok(value.is_some_and(|value| value.value() == expected))
    }
    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        self.tree.transaction_guard()
    }

    pub(crate) fn verify_checksums(&self) -> Result<bool> {
        if !self.tree.verify_checksum()? {
            return Ok(false);
        }

        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let definition = entry.value();
            match definition {
                InternalTableDefinition::Normal {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    if let Some(header) = table_root
                        && !RawBtree::new(
                            Some(header),
                            fixed_key_size,
                            fixed_value_size,
                            self.mem.clone(),
                            self.tree.hint(),
                        )
                        .verify_checksum()?
                    {
                        return Ok(false);
                    }
                }
                InternalTableDefinition::Multimap {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    if !verify_tree_and_subtree_checksums(
                        table_root,
                        fixed_key_size,
                        fixed_value_size,
                        self.mem.clone(),
                        self.tree.hint(),
                    )? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    pub(crate) fn verify_root_checksum(&self) -> Result<bool> {
        self.tree.verify_root_checksum()
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(&self, table_type: TableType) -> Result<Vec<String>> {
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::CatalogBegin,
            None,
            None,
            0,
        )?;
        let iter = self.tree.range::<RangeFull, &str>(&(..))?;
        let iter = TableNameIter {
            inner: iter,
            table_type,
        };
        let mut result = vec![];
        for table in iter {
            let table = table?;
            #[cfg(feature = "test-seams")]
            crate::commit_work::observe_root_work(
                crate::commit_work::RootWorkKindForTest::CatalogEntry,
                Some(&table),
                None,
                table.len(),
            )?;
            result.push(table);
        }
        Ok(result)
    }

    pub(crate) fn get_table_untyped(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::DirectoryLookup,
            Some(name),
            None,
            0,
        )?;
        if let Some(guard) = self.tree.get(&name)? {
            let definition = guard.value();
            definition.check_match_untyped(table_type, name)?;
            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: Key, V: Value>(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        Ok(
            if let Some(definition) = self.get_table_untyped(name, table_type)? {
                // Do additional checks on the types to be sure they match
                definition.check_match::<K, V>(table_type, name)?;
                Some(definition)
            } else {
                None
            },
        )
    }

    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        // All the pages in the table tree itself
        self.tree.visit_all_pages(&mut visitor)?;

        // All the normal tables
        for entry in self.list_tables(TableType::Normal)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), self.tree.hint(), |path| visitor(path))?;
        }

        for entry in self.list_tables(TableType::Multimap)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Multimap)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), self.tree.hint(), |path| visitor(path))?;
        }

        Ok(())
    }
}

pub(crate) struct TableTreeMut<'txn> {
    #[cfg(feature = "test-seams")]
    pub(crate) table_write_observer: crate::table_write_observer::ObserverSlot,
    tree: BtreeMut<'txn, &'static str, InternalTableDefinition>,
    guard: Arc<TransactionGuard>,
    mem: Arc<TransactionalMemory>,
    // Cached updates from tables that have been closed. These must be flushed to the btree.
    // The bool indicates whether the root has dirty (DEFERRED) checksums that need finalization.
    pending_table_updates: hashbrown::HashMap<
        PendingName,
        (Option<BtreeHeader>, u64, bool),
        std::hash::RandomState,
        PendingAlloc,
    >,
    pending_error: Option<crate::StorageError>,
    pending_failed: bool,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
}

impl TableTreeMut<'_> {
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        let pending_table_updates_hasher = {
            #[cfg(feature = "test-seams")]
            {
                crate::tree_store::fixture_random_state()
            }
            #[cfg(not(feature = "test-seams"))]
            {
                std::hash::RandomState::new()
            }
        };
        Self {
            tree: BtreeMut::new(
                master_root,
                guard.clone(),
                mem.clone(),
                freed_pages.clone(),
                allocated_pages.clone(),
            ),
            guard,
            mem,
            pending_table_updates: hashbrown::HashMap::with_hasher_in(
                pending_table_updates_hasher,
                PendingAlloc::current(),
            ),
            pending_error: None,
            pending_failed: false,
            #[cfg(feature = "test-seams")]
            table_write_observer: Default::default(),
            freed_pages,
            allocated_pages,
        }
    }

    pub(crate) fn set_root(&mut self, root: Option<BtreeHeader>) {
        self.tree.set_root(root);
        // Pending updates were staged for the old root and are invalid for the new one
        self.pending_table_updates.clear();
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        // All the pages in the table tree itself
        self.tree.visit_all_pages(&mut visitor)?;

        // All the normal tables
        for entry in self.list_tables(TableType::Normal)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), PageHint::None, |path| visitor(path))?;
        }

        for entry in self.list_tables(TableType::Multimap)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Multimap)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), PageHint::None, |path| visitor(path))?;
        }

        Ok(())
    }

    // Queues an update to the table root
    pub(crate) fn stage_update_table_root(
        &mut self,
        name: &str,
        table_root: Option<BtreeHeader>,
        length: u64,
    ) {
        if self.pending_failed {
            return;
        }
        let dirty = table_root
            .as_ref()
            .is_some_and(|header| self.mem.uncommitted(header.root));
        if let Err(error) = self.stage_pending(name, (table_root, length, dirty)) {
            self.pending_failed = true;
            self.pending_error = Some(error);
        }
    }

    fn stage_pending(&mut self, name: &str, update: (Option<BtreeHeader>, u64, bool)) -> Result {
        if let Some(entry) = self.pending_table_updates.get_mut(name) {
            *entry = update;
            return Ok(());
        }
        self.pending_table_updates
            .try_reserve(1)
            .map_err(|_| pending_allocation_error())?;
        let key = PendingName::try_new(name, self.pending_table_updates.allocator().clone())?;
        self.pending_table_updates.insert(key, update);
        Ok(())
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn pending_table_update_contains(&self, name: &str) -> bool {
        self.pending_table_updates.contains_key(name)
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn pending_table_updates_for_test(&self) -> (usize, usize) {
        (
            self.pending_table_updates.len(),
            self.pending_table_updates.capacity(),
        )
    }

    pub(crate) fn clear_root_updates_and_close(&mut self) {
        self.pending_table_updates.clear();
        self.allocated_pages.lock().unwrap_or_else(|error| error.into_inner()).close();
    }

    // Sealing does not consume allocation custody. A refused preparation can
    // still use the ordinary abort path, whose close is then the only close.
    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn seal_user_root(&mut self) -> Result<Option<BtreeHeader>> {
        self.flush_inner()
    }

    #[cfg(feature = "contextdb-internal")]
    pub(crate) fn close_sealed(&mut self) -> Result<(HashSet<PageNumber>, Vec<PageNumber>)> {
        // Acquire both locks before the irreversible single close.
        let mut allocated = self.allocated_pages.lock()?;
        let mut freed = self.freed_pages.lock()?;
        Ok((allocated.close(), mem::take(&mut *freed)))
    }

    pub(crate) fn flush_and_close(
        &mut self,
    ) -> Result<(Option<BtreeHeader>, HashSet<PageNumber>, Vec<PageNumber>)> {
        match self.flush_inner() {
            Ok(header) => {
                let allocated = self.allocated_pages.lock()?.close();
                let mut old = vec![];
                let mut freed_pages = self.freed_pages.lock()?;
                mem::swap(freed_pages.as_mut(), &mut old);
                Ok((header, allocated, old))
            }
            Err(err) => {
                // Ensure that the allocated pages get clear. Otherwise it will cause a panic
                // when they are dropped
                self.allocated_pages.lock()?.close();
                Err(err)
            }
        }
    }

    fn flush_inner(&mut self) -> Result<Option<BtreeHeader>> {
        #[cfg(feature = "test-seams")]
        let _directory_scope = crate::commit_work::root_work_table_scope("__redb_table_directory");
        self.flush_table_root_updates()?.finalize_dirty_checksums()
    }

    // Finalization changes only the selected table's pending header. Publication
    // remains a separate boundary, even when the native page address is unchanged.
    pub(crate) fn finalize_table_root(&mut self, name: &str) -> Result {
        if self.pending_failed {
            return Err(crate::StorageError::PreviousIo);
        }
        let result = self.finalize_table_root_inner(name);
        if result.is_err() {
            self.pending_failed = true;
        }
        result
    }

    fn finalize_table_root_inner(&mut self, name: &str) -> Result {
        let Some((new_root, new_length, dirty)) = self.pending_table_updates.get(name).copied()
        else {
            return Ok(());
        };
        if !dirty {
            return Ok(());
        }
        let definition = { self.tree.get(&name)?.expect("pending table exists").value() };
        #[cfg(feature = "test-seams")]
        let _table_scope = crate::commit_work::root_work_table_scope(name);
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::FinalizeBegin,
            Some(name),
            None,
            0,
        )?;
        let root = match definition {
            InternalTableDefinition::Normal {
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                let mut tree = UntypedBtreeMut::new(
                    new_root,
                    self.mem.clone(),
                    self.freed_pages.clone(),
                    fixed_key_size,
                    fixed_value_size,
                );
                tree.finalize_dirty_checksums()?
            }
            InternalTableDefinition::Multimap {
                fixed_key_size,
                fixed_value_size,
                ..
            } => finalize_tree_and_subtree_checksums(
                new_root,
                fixed_key_size,
                fixed_value_size,
                self.mem.clone(),
            )?,
        };
        #[cfg(feature = "test-seams")]
        crate::commit_work::observe_root_work(
            crate::commit_work::RootWorkKindForTest::FinalizeEnd,
            Some(name),
            None,
            0,
        )?;
        *self
            .pending_table_updates
            .get_mut(name)
            .expect("pending entry retained") = (root, new_length, false);
        Ok(())
    }

    pub(crate) fn flush_table_root_updates(&mut self) -> Result<&mut Self> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.pending_failed {
            return Err(crate::StorageError::PreviousIo);
        }
        if let Err(error) = self.flush_table_root_updates_inner() {
            self.pending_failed = true;
            return Err(error);
        }
        Ok(self)
    }

    fn flush_table_root_updates_inner(&mut self) -> Result {
        while !self.pending_table_updates.is_empty() {
            // Move one paid key out, retaining it until publication succeeds.
            // Error exits retain this entry and all not-yet-published entries.
            let first = self.pending_table_updates.keys().next().unwrap();
            let name = PendingName::try_new(first, self.pending_table_updates.allocator().clone())?;
            self.finalize_table_root(&name)?;
            let (root, length, _) = self.pending_table_updates[name.as_str()];
            let mut definition = {
                self.tree
                    .get(&name.as_str())?
                    .expect("pending table exists")
                    .value()
            };
            let unchanged = match &definition {
                InternalTableDefinition::Normal { table_root, .. }
                | InternalTableDefinition::Multimap { table_root, .. } => *table_root == root,
            } && definition.get_length() == length;
            if !unchanged {
                definition.set_header(root, length);
                #[cfg(feature = "test-seams")]
                crate::commit_work::observe_root_work(
                    crate::commit_work::RootWorkKindForTest::PublishBegin,
                    Some(&name),
                    None,
                    0,
                )?;
                self.tree.insert(&name.as_str(), &definition)?;
                #[cfg(feature = "test-seams")]
                crate::commit_work::observe_root_work(
                    crate::commit_work::RootWorkKindForTest::PublishEnd,
                    Some(&name),
                    None,
                    0,
                )?;
                #[cfg(feature = "test-seams")]
                crate::table_write_observer::record(
                    &self.table_write_observer,
                    "__redb_user_table_directory",
                    crate::TableWriteOperationForTest::DirectoryInsert,
                    name.as_bytes(),
                    InternalTableDefinition::as_bytes(&definition).len(),
                );
            }
            self.pending_table_updates.remove(name.as_str());
        }
        Ok(())
    }

    // Opens a table, calls the provided closure to insert entries into it, and then
    // flushes the table root. The flush is done using insert_inplace(), so it's guaranteed
    // that no pages will be allocated or freed after the closure returns
    pub(crate) fn open_table_and_flush_table_root<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(&mut BtreeMut<K, V>) -> Result,
    ) -> Result {
        assert!(self.pending_table_updates.is_empty());

        // Reserve space in the table tree
        // TODO: maybe we should have a more explicit method, like "force_uncommitted()"
        let existing = self
            .tree
            .insert(
                &name,
                &InternalTableDefinition::new::<K, V>(TableType::Normal, None, 0),
            )?
            .map(|x| x.value());
        if let Some(existing) = existing {
            self.tree.insert(&name, &existing)?;
        }

        let table_root = match self.tree.get(&name)?.unwrap().value() {
            InternalTableDefinition::Normal { table_root, .. } => table_root,
            InternalTableDefinition::Multimap { .. } => {
                unreachable!()
            }
        };

        // Open the table and call the provided closure on it
        let mut tree: BtreeMut<K, V> = BtreeMut::new(
            table_root,
            self.guard.clone(),
            self.mem.clone(),
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
        );
        f(&mut tree)?;

        // Finalize the table's checksums
        let table_root = tree.finalize_dirty_checksums()?;
        let table_length = tree.get_root().map(|x| x.length).unwrap_or_default();

        // Flush the root to the table tree, without allocating
        self.tree.insert_inplace(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, table_root, table_length),
        )?;

        Ok(())
    }

    // Creates a new table, calls the provided closure to insert entries into it, and then
    // flushes the table root. The flush is done using insert_inplace(), so it's guaranteed
    // that no pages will be allocated or freed after the closure returns
    pub(crate) fn create_table_and_flush_table_root<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(&mut Self, &mut BtreeMut<K, V>) -> Result,
    ) -> Result {
        assert!(self.pending_table_updates.is_empty());
        assert!(self.tree.get(&name)?.is_none());

        // Reserve space in the table tree
        self.tree.insert(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, None, 0),
        )?;

        // Create an empty table and call the provided closure on it
        let mut tree: BtreeMut<K, V> = BtreeMut::new(
            None,
            self.guard.clone(),
            self.mem.clone(),
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
        );
        f(self, &mut tree)?;

        // Finalize the table's checksums
        let table_root = tree.finalize_dirty_checksums()?;
        let table_length = tree.get_root().map(|x| x.length).unwrap_or_default();

        // Flush the root to the table tree, without allocating
        self.tree.insert_inplace(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, table_root, table_length),
        )?;

        Ok(())
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        self.tree.finalize_dirty_checksums()
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(&self, table_type: TableType) -> Result<Vec<String>> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        tree.list_tables(table_type)
    }

    pub(crate) fn get_table_untyped(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        let mut result = tree.get_table_untyped(name, table_type);

        if let Ok(Some(definition)) = result.as_mut()
            && let Some((updated_root, updated_length, _)) = self.pending_table_updates.get(name)
        {
            definition.set_header(*updated_root, *updated_length);
        }

        result
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: Key, V: Value>(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        let mut result = tree.get_table::<K, V>(name, table_type);

        if let Ok(Some(definition)) = result.as_mut()
            && let Some((updated_root, updated_length, _)) = self.pending_table_updates.get(name)
        {
            definition.set_header(*updated_root, *updated_length);
        }

        result
    }

    pub(crate) fn rename_table(
        &mut self,
        name: &str,
        new_name: &str,
        table_type: TableType,
    ) -> Result<(), TableError> {
        if let Some(definition) = self.get_table_untyped(name, table_type)? {
            if self.get_table_untyped(new_name, table_type)?.is_some() {
                return Err(TableError::TableExists(new_name.to_string()));
            }
            if let Some(update) = self.pending_table_updates.get(name).copied() {
                self.stage_pending(new_name, update)?;
                self.pending_table_updates.remove(name);
            }
            assert!(self.tree.remove(&name)?.is_some());
            #[cfg(feature = "test-seams")]
            crate::table_write_observer::record(
                &self.table_write_observer,
                "__redb_user_table_directory",
                crate::TableWriteOperationForTest::DirectoryRemove,
                name.as_bytes(),
                InternalTableDefinition::as_bytes(&definition).len(),
            );
            assert!(self.tree.insert(&new_name, &definition)?.is_none());
            #[cfg(feature = "test-seams")]
            crate::table_write_observer::record(
                &self.table_write_observer,
                "__redb_user_table_directory",
                crate::TableWriteOperationForTest::DirectoryInsert,
                new_name.as_bytes(),
                InternalTableDefinition::as_bytes(&definition).len(),
            );
        } else {
            return Err(TableError::TableDoesNotExist(name.to_string()));
        }

        Ok(())
    }

    pub(crate) fn delete_table(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<bool, TableError> {
        if let Some(definition) = self.get_table_untyped(name, table_type)? {
            // Collect all pages first, then free them. The walk reads each page to discover
            // its children, so we must not invalidate any page before the walk completes.
            let mut pages = vec![];
            definition.visit_all_pages(self.mem.clone(), PageHint::None, |path| {
                pages.push(path.page_number());
                Ok(())
            })?;
            let mut freed_pages = self.freed_pages.lock().unwrap();
            let mut allocated_pages = self.allocated_pages.lock().unwrap();
            for page in pages {
                if !self.mem.free_if_uncommitted(page, &mut allocated_pages) {
                    freed_pages.push(page);
                    #[cfg(feature = "test-seams")]
                    crate::commit_work::note_freed_page();
                }
            }
            drop(allocated_pages);
            drop(freed_pages);

            self.pending_table_updates.remove(name);

            let found = self.tree.remove(&name)?.is_some();
            #[cfg(feature = "test-seams")]
            crate::table_write_observer::record(
                &self.table_write_observer,
                "__redb_user_table_directory",
                crate::TableWriteOperationForTest::DirectoryRemove,
                name.as_bytes(),
                InternalTableDefinition::as_bytes(&definition).len(),
            );
            return Ok(found);
        }

        Ok(false)
    }

    pub(crate) fn get_or_create_table<K: Key, V: Value>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<(Option<BtreeHeader>, u64), TableError> {
        let table = if let Some(found) = self.get_table::<K, V>(name, table_type)? {
            found
        } else {
            let table = InternalTableDefinition::new::<K, V>(table_type, None, 0);
            self.tree.insert(&name, &table)?;
            #[cfg(feature = "test-seams")]
            crate::table_write_observer::record(
                &self.table_write_observer,
                "__redb_user_table_directory",
                crate::TableWriteOperationForTest::DirectoryInsert,
                name.as_bytes(),
                InternalTableDefinition::as_bytes(&table).len(),
            );
            table
        };

        match table {
            InternalTableDefinition::Normal {
                table_root,
                table_length,
                ..
            }
            | InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => Ok((table_root, table_length)),
        }
    }

    // Returns the paths to the n pages that are closest to the end of the database
    // The return value is sorted, according to path.page_number()'s Ord
    pub(crate) fn highest_index_pages(
        &self,
        n: usize,
        output: &mut BTreeMap<PageNumber, PagePath>,
    ) -> Result {
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length, _)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.set_header(*updated_root, *updated_length);
            }

            definition.visit_all_pages(self.mem.clone(), PageHint::None, |path| {
                output.insert(path.page_number(), path.clone());
                while output.len() > n {
                    output.pop_first();
                }
                Ok(())
            })?;
        }

        self.tree.visit_all_pages(|path| {
            output.insert(path.page_number(), path.clone());
            while output.len() > n {
                output.pop_first();
            }
            Ok(())
        })?;

        Ok(())
    }

    pub(crate) fn relocate_tables(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result {
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length, _)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.set_header(*updated_root, *updated_length);
            }

            let name = entry.key().to_owned();
            drop(entry);
            if let Some(new_root) = definition.relocate_tree(
                self.mem.clone(),
                self.freed_pages.clone(),
                relocation_map,
            )? {
                // Relocated roots have already-finalized checksums
                self.stage_pending(&name, (Some(new_root), definition.get_length(), true))?;
            }
        }

        self.tree.relocate(relocation_map)?;

        Ok(())
    }

    pub(crate) fn compaction_page_batch(
        &self,
        cursor: &mut crate::transactions::CompactionCursor,
        maximum: usize,
        protected_table: Option<&str>,
    ) -> Result<(Option<String>, Vec<PagePath>, usize)> {
        use std::ops::Bound;
        let target = if cursor.phase.is_multiple_of(2) {
            None
        } else {
            let start = match cursor.table.as_deref() {
                None => Bound::Unbounded,
                Some(name) if cursor.advance_table => Bound::Excluded(name),
                Some(name) => Bound::Included(name),
            };
            let entry = self
                .tree
                .range::<_, &str>(&(start, Bound::Unbounded))?
                .next();
            let Some(entry) = entry else {
                cursor.phase += 1;
                cursor.table = None;
                cursor.advance_table = false;
                cursor.position.clear();
                return Ok((None, Vec::new(), 0));
            };
            let entry = entry?;
            let name = entry.key().to_owned();
            if protected_table == Some(name.as_str()) {
                // Advance exactly one table without faulting any protected
                // source pages. A later call resumes the finite traversal.
                cursor.table = Some(name);
                cursor.advance_table = true;
                cursor.position.clear();
                return Ok((None, Vec::new(), 0));
            }
            if cursor.table.as_ref() != Some(&name) {
                cursor.position.clear();
            }
            cursor.table = Some(name.clone());
            cursor.advance_table = false;
            let mut definition = entry.value();
            if let Some((root, length, _)) = self.pending_table_updates.get(name.as_str()) {
                definition.set_header(*root, *length);
            }
            Some((name, definition))
        };
        let (pages, next, examined) = if let Some((_, definition)) = &target {
            definition.page_batch(self.mem.clone(), &cursor.position, maximum)?
        } else {
            let tree = crate::tree_store::UntypedBtree::new(
                self.tree.get_root(),
                self.mem.clone(),
                PageHint::None,
                <&str as Value>::fixed_width(),
                InternalTableDefinition::fixed_width(),
            );
            tree.page_batch(&cursor.position, maximum, false)?
        };
        if let Some(next) = next {
            cursor.position = next;
        } else {
            cursor.position.clear();
            if target.is_some() {
                cursor.advance_table = true;
            } else {
                cursor.phase += 1;
            }
        }
        Ok((target.map(|(name, _)| name), pages, examined))
    }

    pub(crate) fn relocate_compaction_batch(
        &mut self,
        target: Option<&str>,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result {
        if relocation_map.is_empty() {
            return Ok(());
        }
        if let Some(name) = target {
            let mut definition = {
                self.tree
                    .get(&name)?
                    .expect("selected table exists")
                    .value()
            };
            if let Some((root, length, _)) = self.pending_table_updates.get(name) {
                definition.set_header(*root, *length);
            }
            if let Some(root) = definition.relocate_tree(
                self.mem.clone(),
                self.freed_pages.clone(),
                relocation_map,
            )? {
                self.stage_pending(name, (Some(root), definition.get_length(), true))?;
            }
        } else {
            self.tree.relocate(relocation_map)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let master_tree_stats = self.tree.stats()?;
        let mut max_subtree_height = 0;
        let mut total_stored_bytes = 0;
        // Count the master tree leaf pages as branches, since they point to the data trees
        let mut branch_pages = master_tree_stats.branch_pages + master_tree_stats.leaf_pages;
        let mut leaf_pages = 0;
        // Include the master table in the overhead
        let mut total_metadata_bytes =
            master_tree_stats.metadata_bytes + master_tree_stats.stored_leaf_bytes;
        let mut total_fragmented = master_tree_stats.fragmented_bytes;

        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, length, _)) = self.pending_table_updates.get(entry.key()) {
                definition.set_header(*updated_root, *length);
            }
            match definition {
                InternalTableDefinition::Normal {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    let subtree_stats = btree_stats(
                        table_root.map(|x| x.root),
                        &self.mem,
                        fixed_key_size,
                        fixed_value_size,
                        PageHint::None,
                    )?;
                    max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
                    total_stored_bytes += subtree_stats.stored_leaf_bytes;
                    total_metadata_bytes += subtree_stats.metadata_bytes;
                    total_fragmented += subtree_stats.fragmented_bytes;
                    branch_pages += subtree_stats.branch_pages;
                    leaf_pages += subtree_stats.leaf_pages;
                }
                InternalTableDefinition::Multimap {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    let subtree_stats = multimap_btree_stats(
                        table_root.map(|x| x.root),
                        &self.mem,
                        fixed_key_size,
                        fixed_value_size,
                        PageHint::None,
                    )?;
                    max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
                    total_stored_bytes += subtree_stats.stored_leaf_bytes;
                    total_metadata_bytes += subtree_stats.metadata_bytes;
                    total_fragmented += subtree_stats.fragmented_bytes;
                    branch_pages += subtree_stats.branch_pages;
                    leaf_pages += subtree_stats.leaf_pages;
                }
            }
        }
        Ok(DatabaseStats {
            tree_height: master_tree_stats.tree_height + max_subtree_height,
            allocated_pages: self.mem.count_allocated_pages()?,
            leaf_pages,
            branch_pages,
            stored_leaf_bytes: total_stored_bytes,
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }
}

impl Drop for TableTreeMut<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            return;
        }
        assert!(self.allocated_pages.lock().unwrap().is_empty());
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    pub(crate) fn get_native_byte_value(
        &self,
        physical_name: &str,
        key: &[u8],
    ) -> std::result::Result<
        Option<crate::AccessGuard<'static, &'static [u8]>>,
        crate::NativeColdReadError,
    > {
        Self::captured_native_get(self.root(), &self.mem, physical_name, key, false)
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    pub(crate) fn get_native_text_value(
        &self,
        physical_name: &str,
        key: &str,
    ) -> std::result::Result<
        Option<crate::AccessGuard<'static, &'static [u8]>>,
        crate::NativeColdReadError,
    > {
        let directory =
            super::btree::native_byte_get(self.root(), &self.mem, physical_name.as_bytes(), true)?
                .ok_or(crate::NativeColdReadError::TableAbsent)?;
        let root = super::table_tree_base::native_text_table_root(directory.value())?;
        super::btree::native_byte_get(root, &self.mem, key.as_bytes(), false)
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    pub(crate) fn next_native_text_range(
        &self,
        physical_name: &str,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before_page: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>,
    ) -> std::result::Result<Option<crate::tree_store::NativeRangeEntry>, crate::NativeColdReadError>
    {
        before_page()?;
        let directory =
            super::btree::native_byte_get(self.root(), &self.mem, physical_name.as_bytes(), true)?
                .ok_or(crate::NativeColdReadError::TableAbsent)?;
        let root = super::table_tree_base::native_text_table_root(directory.value())?;
        super::btree::native_text_range_next(
            root,
            &self.mem,
            lower,
            exclusive,
            upper,
            true,
            before_page,
        )
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    pub(crate) fn next_native_byte_range(
        &self,
        physical_name: &str,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before_page: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>,
    ) -> std::result::Result<Option<crate::tree_store::NativeRangeEntry>, crate::NativeColdReadError>
    {
        before_page()?;
        let directory =
            super::btree::native_byte_get(self.root(), &self.mem, physical_name.as_bytes(), true)?
                .ok_or(crate::NativeColdReadError::TableAbsent)?;
        let root = super::table_tree_base::native_byte_table_root(directory.value())?;
        super::btree::native_text_range_next(
            root,
            &self.mem,
            lower,
            exclusive,
            upper,
            false,
            before_page,
        )
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    // Native descriptor authentication shared with the ordinary tree's byte reader.
    pub(crate) fn captured_native_table_root(
        root: Option<BtreeHeader>,
        mem: &TransactionalMemory,
        name: &str,
        u8_key: bool,
        text_key: bool,
    ) -> std::result::Result<Option<Option<BtreeHeader>>, crate::NativeColdReadError> {
        let Some(directory) = super::btree::native_byte_get(root, mem, name.as_bytes(), true)?
        else {
            return Ok(None);
        };
        let table = if u8_key {
            super::table_tree_base::native_u8_table_root(directory.value())?
        } else if text_key {
            super::table_tree_base::native_text_table_root(directory.value())?
        } else {
            super::table_tree_base::native_byte_table_root(directory.value())?
        };
        Ok(Some(table))
    }
    pub(crate) fn captured_native_text_get(
        root: Option<BtreeHeader>,
        mem: &TransactionalMemory,
        name: &str,
        key: &str,
    ) -> std::result::Result<
        Option<crate::AccessGuard<'static, &'static [u8]>>,
        crate::NativeColdReadError,
    > {
        let table = Self::captured_native_table_root(root, mem, name, false, true)?
            .ok_or(crate::NativeColdReadError::TableAbsent)?;
        super::btree::native_byte_get(table, mem, key.as_bytes(), false)
    }
    pub(crate) fn captured_native_get(
        root: Option<BtreeHeader>,
        mem: &TransactionalMemory,
        name: &str,
        key: &[u8],
        u8_key: bool,
    ) -> std::result::Result<
        Option<crate::AccessGuard<'static, &'static [u8]>>,
        crate::NativeColdReadError,
    > {
        let table = Self::captured_native_table_root(root, mem, name, u8_key, false)?
            .ok_or(crate::NativeColdReadError::TableAbsent)?;
        super::btree::native_key_get(table, mem, key, false, u8_key)
    }
}

#[cfg(test)]
mod test {
    use crate::Value;
    use crate::tree_store::table_tree_base::InternalTableDefinition;
    use crate::types::TypeName;

    #[test]
    fn round_trip() {
        let x = InternalTableDefinition::Multimap {
            table_root: None,
            table_length: 0,
            fixed_key_size: None,
            fixed_value_size: Some(5),
            key_alignment: 6,
            value_alignment: 7,
            key_type: TypeName::new("test::Key"),
            value_type: TypeName::new("test::Value"),
        };
        let y = InternalTableDefinition::from_bytes(InternalTableDefinition::as_bytes(&x).as_ref());
        assert_eq!(x, y);
    }
}

#[cfg(feature = "contextdb-internal")]
impl TableTree {
    pub(crate) fn next_native_directory_record(&self, lower: &[u8], exclusive: bool,
        before: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>)
        -> std::result::Result<Option<crate::tree_store::NativeRangeEntry>, crate::NativeColdReadError> {
        super::btree::native_text_range_next(self.root(), &self.mem, lower, exclusive, None, true, before)
    }
    pub(crate) fn next_native_u64_bytes_range(&self, name: &str, lower: u64, exclusive: bool,
        before: &mut dyn FnMut() -> std::result::Result<(), crate::NativeColdReadError>)
        -> std::result::Result<Option<crate::tree_store::NativeRangeEntry>, crate::NativeColdReadError> {
        before()?;
        let directory = super::btree::native_byte_get(self.root(), &self.mem, name.as_bytes(), true)?
            .ok_or(crate::NativeColdReadError::TableAbsent)?;
        let root = super::table_tree_base::native_u64_bytes_table_root(directory.value())?;
        super::btree::native_u64_range_next(root, &self.mem, &lower.to_le_bytes(), exclusive, None, before)
    }
}
