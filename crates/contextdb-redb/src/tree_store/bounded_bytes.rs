//! Reject-only inspection for byte tables. Selected records use ordinary pages.
use super::btree::page_checksum;
use super::page_store::{Page, PageImpl};
use super::{
    AccessGuard, BRANCH, BranchAccessor, Btree, BtreeHeader, BtreeMut, Checksum, LEAF,
    LeafAccessor, PageHint, PageNumber, TransactionalMemory,
};
use crate::db::TransactionGuard;
use crate::{Result, StorageError};
use std::{cmp::Ordering, marker::PhantomData, ops::Range, sync::Arc};

type ByteGuard<'a> = AccessGuard<'a, &'static [u8]>;
type ByteEntry<'a> = (ByteGuard<'a>, ByteGuard<'a>);

pub(super) struct Singleton {
    pub(super) key: Range<usize>,
    pub(super) value: Range<usize>,
}

pub(super) fn singleton(
    mem: &TransactionalMemory,
    page: PageNumber,
    hint: PageHint,
    key_width: Option<usize>,
    value_width: Option<usize>,
) -> Result<Option<Singleton>> {
    let page_size: u32 = mem.get_page_size().try_into().unwrap();
    let extent: usize = page.page_size_bytes(page_size).try_into().unwrap();
    // Retained-root verification authenticates complete pages. Reject-only
    // inspection cannot substitute for that checksum proof.
    if extent <= mem.get_page_size() || mem.uncommitted(page) || hint.verifies_path() {
        return Ok(None);
    }
    let header_len =
        4 + usize::from(key_width.is_none()) * 4 + usize::from(value_width.is_none()) * 4;
    let header = mem.get_page_window(page, 0..header_len, hint)?;
    if header[0] != LEAF || u16::from_le_bytes(header[2..4].try_into().unwrap()) != 1 {
        return Ok(None);
    }
    let key_end = key_width.map_or_else(
        || u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize,
        |width| header_len + width,
    );
    let value_end = value_width.map_or_else(
        || {
            let start = 4 + usize::from(key_width.is_none()) * 4;
            u32::from_le_bytes(header[start..start + 4].try_into().unwrap()) as usize
        },
        |width| key_end.saturating_add(width),
    );
    if key_end < header_len || value_end < key_end || value_end > extent {
        return Err(StorageError::Corrupted(
            "singleton offsets exceed allocation".into(),
        ));
    }
    if value_end < mem.get_page_size() {
        return Ok(None);
    }
    Ok(Some(Singleton {
        key: header_len..key_end,
        value: key_end..value_end,
    }))
}

// Compare incrementally so even a hostile key never creates a key-sized owner.
pub(super) fn compare_key(
    mem: &TransactionalMemory,
    page: PageNumber,
    stored: Range<usize>,
    query: &[u8],
    hint: PageHint,
) -> Result<Ordering> {
    let common = stored.len().min(query.len());
    for start in (0..common).step_by(512) {
        let end = (start + 512).min(common);
        let window = mem.get_page_window(page, stored.start + start..stored.start + end, hint)?;
        match query[start..end].cmp(&window) {
            Ordering::Equal => {}
            other => return Ok(other),
        }
    }
    Ok(query.len().cmp(&stored.len()))
}

fn starts_with(
    mem: &TransactionalMemory,
    page: PageNumber,
    stored: Range<usize>,
    prefix: &[u8],
    hint: PageHint,
) -> Result<bool> {
    if stored.len() < prefix.len() {
        return Ok(false);
    }
    Ok(compare_key(
        mem,
        page,
        stored.start..stored.start + prefix.len(),
        prefix,
        hint,
    )? == Ordering::Equal)
}

fn get(
    root: Option<BtreeHeader>,
    mem: &Arc<TransactionalMemory>,
    hint: PageHint,
    key: &[u8],
) -> Result<Option<ByteGuard<'static>>> {
    let Some(mut header) = root else {
        return Ok(None);
    };
    let mut ancestors = Vec::new();
    loop {
        if ancestors.contains(&header.root) {
            return Err(StorageError::Corrupted(
                "cycle in bounded byte-table navigation".into(),
            ));
        }
        if let Some(layout) = singleton(mem, header.root, hint, None, None)?
            && compare_key(mem, header.root, layout.key, key, hint)? != Ordering::Equal
        {
            return Ok(None);
        }
        let page = mem.get_page(header.root, hint)?;
        validate_node(page.memory())?;
        verify(&page, header.checksum, hint)?;
        match page.memory()[0] {
            LEAF => {
                let leaf = LeafAccessor::new(page.memory(), None, None);
                return Ok(leaf.find_key::<&[u8]>(key).map(|index| {
                    let (start, end) = leaf.value_range(index).unwrap();
                    AccessGuard::with_page(page.clone(), start..end)
                }));
            }
            BRANCH => {
                ancestors.push(header.root);
                let branch = BranchAccessor::new(&page, None);
                let (index, child) = branch.child_for_key::<&[u8]>(key);
                if ancestors.contains(&child) {
                    return Err(StorageError::Corrupted(
                        "cycle in bounded byte-table navigation".into(),
                    ));
                }
                header = BtreeHeader::new(child, branch.child_checksum(index).unwrap(), 0);
            }
            _ => return Err(StorageError::Corrupted("invalid byte-table node".into())),
        }
    }
}

fn verify(page: &PageImpl, checksum: Checksum, hint: PageHint) -> Result<()> {
    if hint.verifies_path()
        && page
            .to_arc()
            .memoized_byte_node_checksum(|| page_checksum(page, None, None))?
            != checksum
    {
        return Err(StorageError::Corrupted(
            "retained byte-table page failed checksum verification".into(),
        ));
    }
    Ok(())
}

// Validate complete byte nodes before invoking the ordinary zero-copy accessors.
// A cursor must never turn malformed navigation into an omitted record.
fn validate_node(bytes: &[u8]) -> Result<()> {
    let corrupt = || StorageError::Corrupted("invalid bounded byte-table layout".into());
    let header = bytes.get(..4).ok_or_else(corrupt)?;
    let count = usize::from(u16::from_le_bytes(header[2..4].try_into().unwrap()));
    if count == 0 {
        return Err(corrupt());
    }
    match header[0] {
        LEAF => {
            let data_start = 4 + 8 * count;
            let offsets = bytes.get(4..data_start).ok_or_else(corrupt)?;
            let mut end = data_start;
            for offset in offsets.chunks_exact(4) {
                let next = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
                if next < end || next > bytes.len() {
                    return Err(corrupt());
                }
                end = next;
            }
            let leaf = LeafAccessor::new(bytes, None, None);
            let mut previous: Option<&[u8]> = None;
            for index in 0..count {
                let (key, _) = leaf.entry_ranges(index).ok_or_else(corrupt)?;
                let key = &bytes[key];
                if previous.is_some_and(|prior| prior >= key) {
                    return Err(corrupt());
                }
                previous = Some(key);
            }
        }
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
            let offsets = bytes.get(offset_start..data_start).ok_or_else(corrupt)?;
            let mut start = data_start;
            let mut previous: Option<&[u8]> = None;
            for offset in offsets.chunks_exact(4) {
                let end = u32::from_le_bytes(offset.try_into().unwrap()) as usize;
                let key = bytes.get(start..end).ok_or_else(corrupt)?;
                if previous.is_some_and(|prior| prior >= key) {
                    return Err(corrupt());
                }
                previous = Some(key);
                start = end;
            }
        }
        _ => return Err(corrupt()),
    }
    Ok(())
}

enum HandleSource {
    Singleton {
        number: PageNumber,
        checksum: Checksum,
        key: Range<usize>,
    },
    Loaded {
        page: PageImpl,
        key: Range<usize>,
        value: Range<usize>,
    },
}

/// An opaque, root-pinned byte record. Bounded key checks only reject candidates.
#[doc(hidden)]
pub struct BoundedByteHandle<'a> {
    mem: Arc<TransactionalMemory>,
    _guard: Arc<TransactionGuard>,
    hint: PageHint,
    source: HandleSource,
    _table: PhantomData<&'a ()>,
}

impl BoundedByteHandle<'_> {
    /// False excludes this key. True must still be followed by full materialization
    /// before treating the record as descriptor, checksum, or mutation authority.
    pub fn key_matches(&self, key: &[u8]) -> Result<bool> {
        self.key_matches_parts(key.len(), std::iter::once(key))
    }

    /// Compare borrowed key fragments without creating a complete key owner.
    /// The declared length and the fragments must describe the same exact key.
    /// As with `key_matches`, true is only a possible match.
    pub fn key_matches_parts<'b>(
        &self,
        total_len: usize,
        parts: impl IntoIterator<Item = &'b [u8]>,
    ) -> Result<bool> {
        let stored = match &self.source {
            HandleSource::Singleton { key, .. } | HandleSource::Loaded { key, .. } => key,
        };
        if stored.len() != total_len {
            return Ok(false);
        }
        let mut offset = 0;
        for part in parts {
            if part.len() > total_len - offset {
                return Ok(false);
            }
            for chunk in part.chunks(512) {
                let range = stored.start + offset..stored.start + offset + chunk.len();
                let matches = match &self.source {
                    HandleSource::Singleton { number, .. } => {
                        let window = self.mem.get_page_window(*number, range, self.hint)?;
                        &*window == chunk
                    }
                    HandleSource::Loaded { page, .. } => &page.memory()[range] == chunk,
                };
                if !matches {
                    return Ok(false);
                }
                offset += chunk.len();
            }
        }
        Ok(offset == total_len)
    }

    /// Admit and decode the complete selected source page through ordinary reads.
    /// A refusal here does not affect the cursor's admitted navigation state.
    pub fn materialize(&self) -> Result<ByteEntry<'static>> {
        let (page, key, value) = match &self.source {
            HandleSource::Loaded { page, key, value } => (page.clone(), key.clone(), value.clone()),
            HandleSource::Singleton {
                number, checksum, ..
            } => {
                let page = self.mem.get_page(*number, self.hint)?;
                validate_node(page.memory())?;
                verify(&page, *checksum, self.hint)?;
                if page.memory()[0] != LEAF {
                    return Err(StorageError::Corrupted(
                        "bounded singleton changed node type".into(),
                    ));
                }
                let leaf = LeafAccessor::new(page.memory(), None, None);
                if leaf.num_pairs() != 1 {
                    return Err(StorageError::Corrupted(
                        "bounded singleton changed entry count".into(),
                    ));
                }
                let (key, value) = leaf.entry_ranges(0).unwrap();
                (page, key, value)
            }
        };
        Ok((
            AccessGuard::with_page(page.clone(), key),
            AccessGuard::with_page(page, value),
        ))
    }
}

/// Admitted navigation over one captured byte-table root. Navigation errors fuse
/// this cursor; opaque handles materialize independently and pin the same root.
#[doc(hidden)]
pub struct BoundedByteCursor<'a> {
    mem: Arc<TransactionalMemory>,
    guard: Arc<TransactionGuard>,
    hint: PageHint,
    next: Option<(PageNumber, Checksum)>,
    branches: Vec<(PageImpl, usize)>,
    leaf: Option<(PageImpl, usize)>,
    failed: bool,
    _table: PhantomData<&'a ()>,
}

impl<'a> BoundedByteCursor<'a> {
    fn new(tree: &Btree<&'static [u8], &'static [u8]>) -> Self {
        Self {
            mem: tree.mem.clone(),
            guard: tree.transaction_guard().clone(),
            hint: tree.hint,
            next: tree.get_root().map(|root| (root.root, root.checksum)),
            branches: Vec::new(),
            leaf: None,
            failed: false,
            _table: PhantomData,
        }
    }

    fn handle(&self, source: HandleSource) -> BoundedByteHandle<'a> {
        BoundedByteHandle {
            mem: self.mem.clone(),
            _guard: self.guard.clone(),
            hint: self.hint,
            source,
            _table: PhantomData,
        }
    }

    fn advance_branch(&mut self) {
        while let Some((page, index)) = self.branches.last_mut() {
            let branch = BranchAccessor::new(page, None);
            if *index < branch.count_children() {
                self.next = Some((
                    branch.child_page(*index).unwrap(),
                    branch.child_checksum(*index).unwrap(),
                ));
                *index += 1;
                return;
            }
            self.branches.pop();
        }
        self.next = None;
    }

    fn next_inner(&mut self) -> Result<Option<BoundedByteHandle<'a>>> {
        loop {
            if let Some((page, index)) = &mut self.leaf {
                let leaf = LeafAccessor::new(page.memory(), None, None);
                if let Some((key, value)) = leaf.entry_ranges(*index) {
                    *index += 1;
                    let page = page.clone();
                    return Ok(Some(self.handle(HandleSource::Loaded { page, key, value })));
                }
                self.leaf = None;
                self.advance_branch();
            }
            let Some((number, checksum)) = self.next.take() else {
                return Ok(None);
            };
            if self
                .branches
                .iter()
                .any(|(page, _)| page.get_page_number() == number)
            {
                return Err(StorageError::Corrupted(
                    "cycle in bounded byte-table navigation".into(),
                ));
            }
            if let Some(layout) = singleton(&self.mem, number, self.hint, None, None)? {
                let handle = self.handle(HandleSource::Singleton {
                    number,
                    checksum,
                    key: layout.key,
                });
                self.advance_branch();
                return Ok(Some(handle));
            }
            let page = self.mem.get_page(number, self.hint)?;
            validate_node(page.memory())?;
            verify(&page, checksum, self.hint)?;
            match page.memory()[0] {
                LEAF => self.leaf = Some((page, 0)),
                BRANCH => {
                    self.branches.push((page, 0));
                    self.advance_branch();
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<'a> Iterator for BoundedByteCursor<'a> {
    type Item = Result<BoundedByteHandle<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_inner() {
            Ok(handle) => handle.map(Ok),
            Err(error) => {
                self.failed = true;
                self.next = None;
                self.leaf = None;
                self.branches.clear();
                Some(Err(error))
            }
        }
    }
}

pub(crate) struct BoundedByteIter<'a> {
    mem: Arc<TransactionalMemory>,
    _guard: Arc<TransactionGuard>,
    hint: PageHint,
    next: Option<(PageNumber, Checksum)>,
    branches: Vec<(PageImpl, usize)>,
    leaf: Option<(PageImpl, usize)>,
    key_prefix: Option<&'a [u8]>,
    value_prefix: Option<(usize, &'a [u8])>,
    failed: bool,
}

impl<'a> BoundedByteIter<'a> {
    fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        guard: Arc<TransactionGuard>,
        hint: PageHint,
        key_prefix: Option<&'a [u8]>,
        value_prefix: Option<(usize, &'a [u8])>,
    ) -> Self {
        Self {
            mem,
            _guard: guard,
            hint,
            next: root.map(|r| (r.root, r.checksum)),
            branches: Vec::new(),
            leaf: None,
            key_prefix,
            value_prefix,
            failed: false,
        }
    }

    fn advance_branch(&mut self) {
        while let Some((page, index)) = self.branches.last_mut() {
            let branch = BranchAccessor::new(page, None);
            if *index < branch.count_children() {
                self.next = Some((
                    branch.child_page(*index).unwrap(),
                    branch.child_checksum(*index).unwrap(),
                ));
                *index += 1;
                return;
            }
            self.branches.pop();
        }
        self.next = None;
    }

    fn next_inner(&mut self) -> Result<Option<ByteEntry<'static>>> {
        loop {
            if let Some((page, index)) = &mut self.leaf {
                let leaf = LeafAccessor::new(page.memory(), None, None);
                while let Some((key, value)) = leaf.entry_ranges(*index) {
                    *index += 1;
                    if let Some(prefix) = self.key_prefix {
                        let bytes = &page.memory()[key.clone()];
                        if bytes < prefix {
                            continue;
                        }
                        if !bytes.starts_with(prefix) {
                            self.next = None;
                            self.branches.clear();
                            self.leaf = None;
                            return Ok(None);
                        }
                    }
                    if self.value_prefix.is_some_and(|(offset, prefix)| {
                        !page.memory()[value.clone()]
                            .get(offset..)
                            .is_some_and(|v| v.starts_with(prefix))
                    }) {
                        continue;
                    }
                    return Ok(Some((
                        AccessGuard::with_page(page.clone(), key),
                        AccessGuard::with_page(page.clone(), value),
                    )));
                }
                self.leaf = None;
                self.advance_branch();
            }
            let Some((number, checksum)) = self.next.take() else {
                return Ok(None);
            };
            if self
                .branches
                .iter()
                .any(|(page, _)| page.get_page_number() == number)
            {
                return Err(StorageError::Corrupted(
                    "cycle in bounded byte-table navigation".into(),
                ));
            }
            if let Some(layout) = singleton(&self.mem, number, self.hint, None, None)? {
                let mut rejected = false;
                if let Some(prefix) = self.key_prefix {
                    rejected =
                        !starts_with(&self.mem, number, layout.key.clone(), prefix, self.hint)?;
                    if rejected
                        && compare_key(&self.mem, number, layout.key, prefix, self.hint)?
                            == Ordering::Less
                    {
                        self.next = None;
                        self.branches.clear();
                        return Ok(None);
                    }
                }
                if !rejected && let Some((offset, prefix)) = self.value_prefix {
                    rejected = offset > layout.value.len()
                        || !starts_with(
                            &self.mem,
                            number,
                            layout.value.start + offset..layout.value.end,
                            prefix,
                            self.hint,
                        )?;
                }
                if rejected {
                    self.advance_branch();
                    continue;
                }
            }
            let page = self.mem.get_page(number, self.hint)?;
            validate_node(page.memory())?;
            verify(&page, checksum, self.hint)?;
            match page.memory()[0] {
                LEAF => self.leaf = Some((page, 0)),
                BRANCH => {
                    let index = self.key_prefix.map_or(0, |prefix| {
                        BranchAccessor::new(&page, None)
                            .child_for_key::<&[u8]>(prefix)
                            .0
                    });
                    self.branches.push((page, index));
                    self.advance_branch();
                }
                _ => return Err(StorageError::Corrupted("invalid byte-table node".into())),
            }
        }
    }
}

impl Iterator for BoundedByteIter<'_> {
    type Item = Result<ByteEntry<'static>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_inner() {
            Ok(value) => value.map(Ok),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

impl Btree<&'static [u8], &'static [u8]> {
    pub(crate) fn cursor_bounded(&self) -> BoundedByteCursor<'_> {
        BoundedByteCursor::new(self)
    }
    pub(crate) fn get_bounded(&self, key: &[u8]) -> Result<Option<ByteGuard<'static>>> {
        get(self.get_root(), &self.mem, self.hint, key)
    }
    pub(crate) fn iter_bounded<'a>(
        &self,
        key_prefix: Option<&'a [u8]>,
        value_prefix: Option<(usize, &'a [u8])>,
    ) -> BoundedByteIter<'a> {
        BoundedByteIter::new(
            self.get_root(),
            self.mem.clone(),
            self.transaction_guard().clone(),
            self.hint,
            key_prefix,
            value_prefix,
        )
    }
}

impl BtreeMut<'_, &'static [u8], &'static [u8]> {
    pub(crate) fn get_bounded(&self, key: &[u8]) -> Result<Option<ByteGuard<'static>>> {
        get(self.get_root(), &self.mem, PageHint::None, key)
    }
    pub(crate) fn iter_bounded<'a>(
        &self,
        guard: Arc<TransactionGuard>,
        key_prefix: Option<&'a [u8]>,
    ) -> BoundedByteIter<'a> {
        BoundedByteIter::new(
            self.get_root(),
            self.mem.clone(),
            guard,
            PageHint::None,
            key_prefix,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Builder, ReadableDatabase, ReadableTableMetadata, TableDefinition, observe_backend_reads,
        with_read_memory_admission,
    };
    use std::{
        io,
        sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };
    const VALUES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bounded_values");
    const HUGE: usize = 8 * 1024 * 1024;
    fn bounded<T>(operation: impl FnOnce() -> T) -> T {
        with_read_memory_admission(
            Arc::new(|bytes| {
                if bytes > 64 * 1024 {
                    Err(io::Error::from(io::ErrorKind::QuotaExceeded))
                } else {
                    Ok(())
                }
            }),
            operation,
        )
    }
    fn create(path: &std::path::Path, large_key: bool) {
        let db = Builder::new().set_cache_size(0).create(path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            if large_key {
                table
                    .insert(vec![b'z'; HUGE].as_slice(), b"other-route:value".as_slice())
                    .unwrap();
            } else {
                table
                    .insert(b"z:unrelated".as_slice(), vec![7; HUGE].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum NodeDamage {
        Empty,
        OffsetBeyondPage,
        BranchCycle {
            target_page: [u8; 8],
            target_checksum: [u8; 16],
        },
    }

    #[derive(Debug, Default)]
    struct NodeDamageState {
        first_read: Option<(u64, u8)>,
        damage: Option<(u64, NodeDamage)>,
        damaged_reads: usize,
    }

    #[derive(Clone, Debug)]
    struct DamagedNodeBackend {
        inner: Arc<dyn crate::StorageBackend>,
        state: Arc<std::sync::Mutex<NodeDamageState>>,
    }

    impl crate::StorageBackend for DamagedNodeBackend {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            crate::StorageBackend::read(&*self.inner, offset, out)?;
            let mut state = self.state.lock().unwrap();
            if let Some(&node_type) = out.first() {
                state.first_read.get_or_insert((offset, node_type));
            }
            if let Some((target, damage)) = state.damage
                && offset == target
            {
                state.damaged_reads += 1;
                if state.damaged_reads > 50 {
                    return Err(io::Error::other(
                        "exceeded read budget in damaged backend traversal",
                    ));
                }
                match damage {
                    NodeDamage::Empty => out[2..4].copy_from_slice(&0u16.to_le_bytes()),
                    NodeDamage::OffsetBeyondPage => {
                        let count = usize::from(u16::from_le_bytes(out[2..4].try_into().unwrap()));
                        let start = if out[0] == LEAF {
                            4
                        } else {
                            8 + (size_of::<Checksum>() + PageNumber::serialized_size())
                                * (count + 1)
                        };
                        out[start..start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
                    }
                    NodeDamage::BranchCycle {
                        target_page,
                        target_checksum,
                    } => {
                        if out[0] == BRANCH {
                            let count =
                                usize::from(u16::from_le_bytes(out[2..4].try_into().unwrap()));
                            let children = count + 1;
                            let checksum_start = 8;
                            out[checksum_start..checksum_start + 16]
                                .copy_from_slice(&target_checksum);
                            let page_start = 8 + 16 * children;
                            out[page_start..page_start + 8].copy_from_slice(&target_page);
                        }
                    }
                }
            }
            Ok(())
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> io::Result<()> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            crate::StorageBackend::write(&*self.inner, offset, data)
        }
    }

    fn malformed_complete_nodes(
        check: impl Fn(&crate::ReadOnlyTable<&'static [u8], &'static [u8]>),
    ) {
        for node_type in [LEAF, BRANCH] {
            let state = Arc::new(std::sync::Mutex::new(NodeDamageState::default()));
            let backend = DamagedNodeBackend {
                inner: Arc::new(crate::backends::InMemoryBackend::new()),
                state: state.clone(),
            };
            let db = Builder::new()
                .set_cache_size(0)
                .create_with_backend(backend.clone())
                .unwrap();
            let count = if node_type == LEAF { 2 } else { 192 };
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(VALUES).unwrap();
                for index in 0..count {
                    let key = format!("a:{index:04}");
                    table.insert(key.as_bytes(), [3; 128].as_slice()).unwrap();
                }
            }
            write.commit().unwrap();
            // Reopen so every payload fault reaches the backend instead of a writer buffer.
            drop(db);
            let db = Builder::new()
                .set_cache_size(0)
                .open_with_backend(backend)
                .unwrap();
            let read = db.begin_read().unwrap();
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            state.lock().unwrap().first_read = None;
            assert_eq!(
                table.get_bounded(b"a:0000").unwrap().unwrap().value(),
                [3; 128]
            );
            let (target, actual_type) = state.lock().unwrap().first_read.unwrap();
            assert_eq!(actual_type, node_type, "the damaged page is the table root");
            assert_eq!(
                table
                    .prefix_bounded(b"a:")
                    .unwrap()
                    .map(Result::unwrap)
                    .count(),
                count
            );
            for damage in [NodeDamage::Empty, NodeDamage::OffsetBeyondPage] {
                state.lock().unwrap().damage = Some((target, damage));
                let before = state.lock().unwrap().damaged_reads;
                check(&table);
                assert!(state.lock().unwrap().damaged_reads > before);
            }
            state.lock().unwrap().damage = None;
            assert_eq!(
                table.get_bounded(b"a:0000").unwrap().unwrap().value(),
                [3; 128]
            );
            assert_eq!(
                table
                    .prefix_bounded(b"a:")
                    .unwrap()
                    .map(Result::unwrap)
                    .count(),
                count
            );
        }
    }

    fn assert_corrupted_without_panic<T>(operation: impl FnOnce() -> Result<T>) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
        assert!(result.is_ok(), "malformed complete nodes must not panic");
        assert!(matches!(result.unwrap(), Err(StorageError::Corrupted(_))));
    }

    #[test]
    fn bounded_lookup_rejects_malformed_leaf_and_branch_without_panicking() {
        malformed_complete_nodes(|table| {
            assert_corrupted_without_panic(|| table.get_bounded(b"a:0000"));
        });
    }

    #[test]
    fn bounded_prefix_iteration_rejects_malformed_leaf_and_branch_without_panicking() {
        malformed_complete_nodes(|table| {
            assert_corrupted_without_panic(|| {
                let mut iter = table.prefix_bounded(b"a:").unwrap();
                let result = iter.next().expect("corrupted navigation is reported");
                assert!(iter.next().is_none(), "navigation errors fuse the iterator");
                result
            });
        });
    }

    fn run_with_timeout<T: Send>(timeout: std::time::Duration, f: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();
            s.spawn(move || {
                let _ = tx.send(f());
            });
            rx.recv_timeout(timeout)
                .expect("operation timed out — possible hang in cycle traversal")
        })
    }

    fn branch_cycle_fixture(check: impl Fn(&crate::ReadOnlyTable<&'static [u8], &'static [u8]>)) {
        let state = Arc::new(std::sync::Mutex::new(NodeDamageState::default()));
        let backend = DamagedNodeBackend {
            inner: Arc::new(crate::backends::InMemoryBackend::new()),
            state: state.clone(),
        };
        let db = Builder::new()
            .set_cache_size(0)
            .create_with_backend(backend.clone())
            .unwrap();
        let count = 192;
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            for index in 0..count {
                let key = format!("a:{index:04}");
                table.insert(key.as_bytes(), [3; 128].as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        // Reopen so every payload fault reaches the backend instead of a writer buffer.
        drop(db);
        let db = Builder::new()
            .set_cache_size(0)
            .open_with_backend(backend)
            .unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_byte_table_bounded(VALUES).unwrap();
        state.lock().unwrap().first_read = None;
        assert_eq!(
            table.get_bounded(b"a:0000").unwrap().unwrap().value(),
            [3; 128]
        );
        let (target, actual_type) = state.lock().unwrap().first_read.unwrap();
        assert_eq!(
            actual_type, BRANCH,
            "the damaged page is the table root branch"
        );

        let fingerprint = read.table_root_fingerprint(VALUES).unwrap().unwrap();
        let mut target_page = [0u8; 8];
        target_page.copy_from_slice(&fingerprint[0..8]);
        let mut target_checksum = [0u8; 16];
        target_checksum.copy_from_slice(&fingerprint[8..24]);

        state.lock().unwrap().damage = Some((
            target,
            NodeDamage::BranchCycle {
                target_page,
                target_checksum,
            },
        ));
        let before = state.lock().unwrap().damaged_reads;
        check(&table);
        let after = state.lock().unwrap().damaged_reads;
        assert!(after > before, "the damaged branch was read");
        assert!(
            after <= before + 2,
            "cycle rejected within bounded page reads"
        );

        state.lock().unwrap().damage = None;
        assert_eq!(
            table.get_bounded(b"a:0000").unwrap().unwrap().value(),
            [3; 128]
        );
        assert_eq!(
            table
                .prefix_bounded(b"a:")
                .unwrap()
                .map(Result::unwrap)
                .count(),
            count
        );
    }

    fn assert_cycle_corrupted_without_panic<T>(operation: impl FnOnce() -> Result<T> + Send) {
        run_with_timeout(std::time::Duration::from_secs(5), move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
            assert!(result.is_ok(), "branch cycles must not panic");
            match result.unwrap() {
                Err(StorageError::Corrupted(message)) => {
                    assert_eq!(message, "cycle in bounded byte-table navigation");
                }
                Err(err) => panic!("expected cycle corruption error, got {err:?}"),
                Ok(_) => panic!("expected cycle corruption error, got Ok(_)"),
            }
        });
    }

    #[test]
    fn bounded_lookup_rejects_branch_cycle_without_hanging() {
        branch_cycle_fixture(|table| {
            assert_cycle_corrupted_without_panic(|| table.get_bounded(b"a:0000"));
        });
    }

    #[test]
    fn bounded_prefix_iteration_rejects_branch_cycle_without_hanging() {
        branch_cycle_fixture(|table| {
            assert_cycle_corrupted_without_panic(|| {
                let mut iter = table.prefix_bounded(b"a:").unwrap();
                let result = iter.next().expect("corrupted navigation is reported");
                assert!(iter.next().is_none(), "navigation errors fuse the iterator");
                result
            });
        });
    }

    #[test]
    fn cursor_navigation_rejects_branch_cycle_without_hanging() {
        branch_cycle_fixture(|table| {
            assert_cycle_corrupted_without_panic(|| {
                let mut cursor = table.cursor_bounded();
                let result = cursor.next().expect("corrupted navigation is reported");
                assert!(cursor.next().is_none(), "navigation errors fuse the cursor");
                result
            });
        });
    }

    #[test]
    fn absent_lookup_and_prefix_neighbor_reject_without_materializing_the_extent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let largest = Arc::new(AtomicUsize::new(0));
        let observed = largest.clone();
        let _observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_max(bytes, AtomicOrdering::SeqCst);
        }));
        bounded(|| {
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            assert!(table.get_bounded(b"a:missing").unwrap().is_none());
            assert!(table.prefix_bounded(b"a:").unwrap().next().is_none());
            assert!(table.get_bounded(b"z:unrelated").is_err());
            assert!(
                table
                    .prefix_bounded(b"z:")
                    .unwrap()
                    .next()
                    .unwrap()
                    .is_err()
            );
        });
        assert!(largest.load(AtomicOrdering::SeqCst) <= 4096);
        // A refused read does not poison the backend or change the record.
        assert_eq!(
            read.open_table(VALUES)
                .unwrap()
                .get(b"z:unrelated".as_slice())
                .unwrap()
                .unwrap()
                .value()
                .len(),
            HUGE
        );
    }

    #[test]
    fn prefix_iteration_descends_and_stops_at_the_first_unrelated_extent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        bounded(|| {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(VALUES).unwrap();
                for prefix in [b"a:", b"b:"] {
                    for ordinal in 0u16..96 {
                        let mut key = prefix.to_vec();
                        key.extend_from_slice(&ordinal.to_le_bytes());
                        table
                            .insert(key.as_slice(), vec![3; 128].as_slice())
                            .unwrap();
                    }
                }
            }
            write.commit().unwrap();
        });
        let read = db.begin_read().unwrap();
        bounded(|| {
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            for prefix in [b"a:", b"b:"] {
                let keys = table
                    .prefix_bounded(prefix)
                    .unwrap()
                    .map(|entry| {
                        let (key, value) = entry.unwrap();
                        assert_eq!(value.value(), vec![3; 128]);
                        key.value().to_vec()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(keys.len(), 96);
                for (ordinal, key) in keys.iter().enumerate() {
                    assert_eq!(&key[..2], prefix);
                    assert_eq!(
                        usize::from(u16::from_le_bytes(key[2..4].try_into().unwrap())),
                        ordinal
                    );
                }
            }
        });
    }

    #[test]
    fn retained_root_inspection_keeps_full_checksum_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let roots = Arc::new(std::sync::Mutex::new(Vec::new()));
        let registered = roots.clone();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let db = Builder::new()
            .set_cache_size(0)
            .open_with_backend_and_external_snapshots(
                crate::backends::FileBackend::new(file).unwrap(),
                Arc::new(move || Ok(registered.lock().unwrap().clone())),
            )
            .unwrap();
        let pin = db.begin_read().unwrap();
        let snapshot = pin.storage_snapshot_identity();
        roots.lock().unwrap().push(snapshot);
        let external = db.begin_external_read(snapshot).unwrap();
        let table = external.open_byte_table_bounded(VALUES).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let _observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        bounded(|| {
            assert!(table.get_bounded(b"a:missing").is_err());
            assert!(
                table
                    .prefix_bounded(b"a:")
                    .unwrap()
                    .next()
                    .unwrap()
                    .is_err()
            );
        });
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            table
                .get_bounded(b"z:unrelated")
                .unwrap()
                .unwrap()
                .value()
                .len(),
            HUGE
        );
    }

    #[test]
    fn window_refusal_precedes_backend_read_and_window_charge_releases() {
        struct Charge(Arc<AtomicUsize>, usize);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, AtomicOrdering::SeqCst);
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_byte_table_bounded(VALUES).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let _observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        crate::with_read_page_charge(
            Arc::new(|_| Err(io::Error::from(io::ErrorKind::QuotaExceeded))),
            || {
                assert!(table.get_bounded(b"a:missing").is_err());
            },
        );
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 0);
        let held = Arc::new(AtomicUsize::new(0));
        let admitted = held.clone();
        crate::with_read_page_charge(
            Arc::new(move |bytes| {
                if bytes > 64 * 1024 {
                    return Err(io::Error::from(io::ErrorKind::QuotaExceeded));
                }
                admitted.fetch_add(bytes, AtomicOrdering::SeqCst);
                Ok(Box::new(Charge(admitted.clone(), bytes)))
            }),
            || {
                assert!(table.get_bounded(b"a:missing").unwrap().is_none());
                assert!(table.get_bounded(b"z:unrelated").is_err());
            },
        );
        assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
        assert!(reads.load(AtomicOrdering::SeqCst) < 4096);
    }

    #[test]
    fn value_prefix_rejection_and_insert_before_preserve_abort_reopen_and_pinned_reader() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, true);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let pinned = db.begin_read().unwrap();
        let root = pinned.table_root_fingerprint(VALUES).unwrap();
        bounded(|| {
            let table = pinned.open_byte_table_bounded(VALUES).unwrap();
            assert!(
                table
                    .iter_bounded_value_prefix(0, b"selected-route:")
                    .unwrap()
                    .next()
                    .is_none()
            );
            let write = db.begin_write().unwrap();
            write
                .open_table(VALUES)
                .unwrap()
                .insert(b"a:item".as_slice(), b"selected-route:value".as_slice())
                .unwrap();
            write.abort().unwrap();
        });
        assert_eq!(
            db.begin_read()
                .unwrap()
                .table_root_fingerprint(VALUES)
                .unwrap(),
            root
        );
        bounded(|| {
            let write = db.begin_write().unwrap();
            write
                .open_table(VALUES)
                .unwrap()
                .insert(b"a:item".as_slice(), b"selected-route:value".as_slice())
                .unwrap();
            write.commit().unwrap();
            let read = db.begin_read().unwrap();
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            let (key, value) = table
                .prefix_bounded(b"a:")
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(key.value(), b"a:item");
            assert_eq!(value.value(), b"selected-route:value");
            assert_eq!(
                table
                    .iter_bounded_value_prefix(0, b"selected-route:")
                    .unwrap()
                    .map(Result::unwrap)
                    .count(),
                1
            );
        });
        assert_eq!(pinned.open_table(VALUES).unwrap().len().unwrap(), 1);
        assert_eq!(
            pinned
                .open_table(VALUES)
                .unwrap()
                .get(vec![b'z'; HUGE].as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"other-route:value"
        );
        drop(pinned);
        drop(db);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let read = db.begin_read().unwrap();
        assert_eq!(read.open_table(VALUES).unwrap().len().unwrap(), 2);
        assert_eq!(
            read.open_table(VALUES)
                .unwrap()
                .get(vec![b'z'; HUGE].as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"other-route:value"
        );
    }

    #[test]
    fn small_separator_splits_both_orders_without_faulting_the_giant_value() {
        for inserted in [b"a:item".as_slice(), b"zz:item".as_slice()] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("bounded.db");
            create(&path, false);
            let db = Builder::new().set_cache_size(0).open(&path).unwrap();
            let pinned = db.begin_read().unwrap();
            let original_root = pinned.table_root_fingerprint(VALUES).unwrap();
            let largest = Arc::new(AtomicUsize::new(0));
            let observed = largest.clone();
            let observation = observe_backend_reads(Arc::new(move |bytes| {
                observed.fetch_max(bytes, AtomicOrdering::SeqCst);
            }));
            bounded(|| {
                let write = db.begin_write().unwrap();
                write
                    .open_table(VALUES)
                    .unwrap()
                    .insert(inserted, b"small".as_slice())
                    .unwrap();
                write.abort().unwrap();
            });
            assert_eq!(
                db.begin_read()
                    .unwrap()
                    .table_root_fingerprint(VALUES)
                    .unwrap(),
                original_root
            );
            bounded(|| {
                let write = db.begin_write().unwrap();
                write
                    .open_table(VALUES)
                    .unwrap()
                    .insert(inserted, b"small".as_slice())
                    .unwrap();
                write.commit().unwrap();
                let read = db.begin_read().unwrap();
                let table = read.open_byte_table_bounded(VALUES).unwrap();
                assert_eq!(table.len().unwrap(), 2);
                assert_eq!(
                    table.get_bounded(inserted).unwrap().unwrap().value(),
                    b"small"
                );
                assert!(table.get_bounded(b"z:unrelated").is_err());
            });
            assert!(largest.load(AtomicOrdering::SeqCst) <= 4096);
            drop(observation);
            let old_table = pinned.open_table(VALUES).unwrap();
            assert_eq!(old_table.len().unwrap(), 1);
            assert!(old_table.get(inserted).unwrap().is_none());
            assert_eq!(
                old_table
                    .get(b"z:unrelated".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                vec![7; HUGE]
            );
            drop(old_table);
            drop(pinned);
            drop(db);
            let mut db = Builder::new().set_cache_size(0).open(&path).unwrap();
            assert!(db.check_integrity().unwrap());
            let read = db.begin_read().unwrap();
            let table = read.open_table(VALUES).unwrap();
            assert_eq!(table.len().unwrap(), 2);
            assert_eq!(table.get(inserted).unwrap().unwrap().value(), b"small");
            assert_eq!(
                table
                    .get(b"z:unrelated".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                vec![7; HUGE]
            );
        }
    }

    #[test]
    fn insert_after_separator_window_refuses_before_read_and_releases_its_owner() {
        struct Charge(Arc<AtomicUsize>, usize);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, AtomicOrdering::SeqCst);
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let root = db
            .begin_read()
            .unwrap()
            .table_root_fingerprint(VALUES)
            .unwrap();
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(VALUES).unwrap();
        let held = Arc::new(AtomicUsize::new(0));
        let admitted = held.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        let separator_bytes =
            super::super::page_store::PageMemory::admitted_bytes_for_test(b"z:unrelated".len());
        crate::with_read_page_charge(
            Arc::new(move |bytes| {
                if bytes == separator_bytes || bytes > 64 * 1024 {
                    return Err(io::Error::from(io::ErrorKind::QuotaExceeded));
                }
                admitted.fetch_add(bytes, AtomicOrdering::SeqCst);
                Ok(Box::new(Charge(admitted.clone(), bytes)))
            }),
            || {
                assert!(
                    table
                        .insert(b"zz:item".as_slice(), b"small".as_slice())
                        .is_err()
                );
            },
        );
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 12 + b"zz:item".len());
        assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
        let admitted = held.clone();
        let separators = Arc::new(AtomicUsize::new(0));
        let counted = separators.clone();
        crate::with_read_page_charge(
            Arc::new(move |bytes| {
                if bytes > 64 * 1024 {
                    return Err(io::Error::from(io::ErrorKind::QuotaExceeded));
                }
                if bytes == separator_bytes {
                    counted.fetch_add(1, AtomicOrdering::SeqCst);
                }
                admitted.fetch_add(bytes, AtomicOrdering::SeqCst);
                Ok(Box::new(Charge(admitted.clone(), bytes)))
            }),
            || {
                assert!(
                    table
                        .insert(b"zz:item".as_slice(), b"small".as_slice())
                        .unwrap()
                        .is_none()
                );
            },
        );
        assert_eq!(separators.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
        drop(observation);
        drop(table);
        write.abort().unwrap();
        assert_eq!(
            db.begin_read()
                .unwrap()
                .table_root_fingerprint(VALUES)
                .unwrap(),
            root
        );
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(VALUES)
                .unwrap()
                .len()
                .unwrap(),
            1
        );
    }

    #[test]
    fn insert_after_giant_separator_uses_ordinary_extent_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, true);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let root = db
            .begin_read()
            .unwrap()
            .table_root_fingerprint(VALUES)
            .unwrap();
        let write = db.begin_write().unwrap();
        let mut table = write.open_table(VALUES).unwrap();
        let largest = Arc::new(AtomicUsize::new(0));
        let admitted = largest.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        with_read_memory_admission(
            Arc::new(move |bytes| {
                admitted.fetch_max(bytes, AtomicOrdering::SeqCst);
                if bytes > 64 * 1024 {
                    Err(io::Error::from(io::ErrorKind::QuotaExceeded))
                } else {
                    Ok(())
                }
            }),
            || {
                assert!(table.insert(b"{".as_slice(), b"small".as_slice()).is_err());
            },
        );
        assert!(largest.load(AtomicOrdering::SeqCst) > HUGE);
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 13);
        drop(observation);
        drop(table);
        write.abort().unwrap();
        assert_eq!(
            db.begin_read()
                .unwrap()
                .table_root_fingerprint(VALUES)
                .unwrap(),
            root
        );
        let write = db.begin_write().unwrap();
        write
            .open_table(VALUES)
            .unwrap()
            .insert(b"{".as_slice(), b"small".as_slice())
            .unwrap();
        write.commit().unwrap();
        drop(db);
        let mut db = Builder::new().set_cache_size(0).open(&path).unwrap();
        assert!(db.check_integrity().unwrap());
        let read = db.begin_read().unwrap();
        let table = read.open_table(VALUES).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        assert_eq!(
            table.get(b"{".as_slice()).unwrap().unwrap().value(),
            b"small"
        );
        assert_eq!(
            table
                .get(vec![b'z'; HUGE].as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"other-route:value"
        );
    }

    #[test]
    fn deleting_a_partial_leaf_does_not_fault_its_large_singleton_neighbor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bounded.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        bounded(|| {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(VALUES).unwrap();
                for key in [b"a:0", b"a:1", b"a:2"] {
                    table.insert(key.as_slice(), b"small".as_slice()).unwrap();
                }
            }
            write.commit().unwrap();
        });
        let pinned = db.begin_read().unwrap();
        bounded(|| {
            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(VALUES).unwrap();
                assert_eq!(
                    table.remove(b"a:1".as_slice()).unwrap().unwrap().value(),
                    b"small"
                );
            }
            write.commit().unwrap();
        });
        assert_eq!(pinned.open_table(VALUES).unwrap().len().unwrap(), 4);
        let read = db.begin_read().unwrap();
        assert_eq!(read.open_table(VALUES).unwrap().len().unwrap(), 3);
        bounded(|| {
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            assert!(table.get_bounded(b"a:1").unwrap().is_none());
            assert_eq!(
                table
                    .prefix_bounded(b"a:")
                    .unwrap()
                    .map(Result::unwrap)
                    .count(),
                2
            );
        });
        assert_eq!(
            read.open_table(VALUES)
                .unwrap()
                .get(b"z:unrelated".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            vec![7; HUGE]
        );
    }

    #[test]
    fn cursor_continues_after_refused_singleton_in_every_sibling_position() {
        for giant_key in [b"a".as_slice(), b"m", b"z"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("cursor.db");
            let db = Builder::new().set_cache_size(0).create(&path).unwrap();
            let write = db.begin_write().unwrap();
            write
                .open_table(VALUES)
                .unwrap()
                .insert(giant_key, vec![7; HUGE].as_slice())
                .unwrap();
            write.commit().unwrap();
            bounded(|| {
                let write = db.begin_write().unwrap();
                for key in [b"b".as_slice(), b"n", b"y"] {
                    write
                        .open_table(VALUES)
                        .unwrap()
                        .insert(key, b"healthy".as_slice())
                        .unwrap();
                }
                write.commit().unwrap();
            });
            let read = db.begin_read().unwrap();
            let table = read.open_byte_table_bounded(VALUES).unwrap();
            let largest = Arc::new(AtomicUsize::new(0));
            let observed = largest.clone();
            let observation = observe_backend_reads(Arc::new(move |bytes| {
                observed.fetch_max(bytes, AtomicOrdering::SeqCst);
            }));
            bounded(|| {
                let mut refused = 0;
                let mut healthy = Vec::new();
                for handle in table.cursor_bounded() {
                    let handle = handle.unwrap();
                    if handle.key_matches(giant_key).unwrap() {
                        let error = handle.materialize().err().unwrap();
                        assert!(
                            matches!(error, StorageError::Io(ref error) if error.kind() == io::ErrorKind::QuotaExceeded)
                        );
                        refused += 1;
                    } else {
                        let (key, value) = handle.materialize().unwrap();
                        assert_eq!(value.value(), b"healthy");
                        healthy.push(key.value().to_vec());
                    }
                }
                assert_eq!(refused, 1);
                assert_eq!(healthy, [b"b".to_vec(), b"n".to_vec(), b"y".to_vec()]);
            });
            assert!(largest.load(AtomicOrdering::SeqCst) <= 4096);
            drop(observation);
            let handle = table
                .cursor_bounded()
                .map(Result::unwrap)
                .find(|handle| handle.key_matches(giant_key).unwrap())
                .unwrap();
            assert_eq!(handle.materialize().unwrap().1.value(), vec![7; HUGE]);
        }
    }

    #[test]
    fn cursor_navigation_refusal_is_terminal_and_has_no_backend_read() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cursor.db");
        create(&path, false);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_byte_table_bounded(VALUES).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let _observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        let mut cursor = table.cursor_bounded();
        crate::with_read_page_charge(
            Arc::new(|_| Err(io::Error::from(io::ErrorKind::QuotaExceeded))),
            || {
                assert!(cursor.next().unwrap().is_err());
            },
        );
        assert!(cursor.next().is_none());
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 0);
        // A fresh cursor retries the same root after the admission condition changes.
        bounded(|| {
            let handle = table.cursor_bounded().next().unwrap().unwrap();
            assert!(handle.key_matches(b"z:unrelated").unwrap());
            assert!(handle.materialize().is_err());
        });
    }

    #[test]
    fn cursor_streamed_key_checks_release_windows_without_materializing_giant_keys() {
        struct Charge(Arc<AtomicUsize>, usize);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, AtomicOrdering::SeqCst);
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cursor.db");
        create(&path, true);
        let db = Builder::new().set_cache_size(0).open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_byte_table_bounded(VALUES).unwrap();
        let held = Arc::new(AtomicUsize::new(0));
        let admitted = held.clone();
        let largest = Arc::new(AtomicUsize::new(0));
        let observed = largest.clone();
        let _observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_max(bytes, AtomicOrdering::SeqCst);
        }));
        crate::with_read_page_charge(
            Arc::new(move |bytes| {
                if bytes > 64 * 1024 {
                    return Err(io::Error::from(io::ErrorKind::QuotaExceeded));
                }
                admitted.fetch_add(bytes, AtomicOrdering::SeqCst);
                Ok(Box::new(Charge(admitted.clone(), bytes)))
            }),
            || {
                let mut cursor = table.cursor_bounded();
                let handle = cursor.next().unwrap().unwrap();
                assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
                let chunk = [b'z'; 512];
                assert!(
                    handle
                        .key_matches_parts(
                            HUGE,
                            std::iter::repeat_n(chunk.as_slice(), HUGE / chunk.len())
                        )
                        .unwrap()
                );
                assert!(
                    !handle
                        .key_matches_parts(HUGE - 1, [chunk.as_slice()])
                        .unwrap()
                );
                assert!(!handle.key_matches_parts(HUGE, [chunk.as_slice()]).unwrap());
                assert!(
                    !handle
                        .key_matches_parts(HUGE, std::iter::once(b"x".as_slice()))
                        .unwrap()
                );
                assert!(handle.materialize().is_err());
                assert!(cursor.next().is_none());
                assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
            },
        );
        assert_eq!(held.load(AtomicOrdering::SeqCst), 0);
        assert!(largest.load(AtomicOrdering::SeqCst) <= 512);
    }

    #[test]
    fn cursor_handles_pin_the_old_root_across_abort_commit_reopen_and_integrity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cursor.db");
        let db = Builder::new().set_cache_size(0).create(&path).unwrap();
        let write = db.begin_write().unwrap();
        write
            .open_table(VALUES)
            .unwrap()
            .insert(b"a".as_slice(), vec![7; HUGE].as_slice())
            .unwrap();
        write.commit().unwrap();
        bounded(|| {
            let write = db.begin_write().unwrap();
            write
                .open_table(VALUES)
                .unwrap()
                .insert(b"z".as_slice(), b"old".as_slice())
                .unwrap();
            write.commit().unwrap();
        });
        let pinned = db.begin_read().unwrap();
        let root = pinned.table_root_fingerprint(VALUES).unwrap();
        let table = pinned.open_byte_table_bounded(VALUES).unwrap();
        let mut cursor = table.cursor_bounded();
        let giant = bounded(|| cursor.next().unwrap().unwrap());
        bounded(|| assert!(giant.materialize().is_err()));
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            table.remove(b"a".as_slice()).unwrap();
            table
                .insert(b"z".as_slice(), b"aborted".as_slice())
                .unwrap();
        }
        write.abort().unwrap();
        assert_eq!(
            db.begin_read()
                .unwrap()
                .table_root_fingerprint(VALUES)
                .unwrap(),
            root
        );
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(VALUES).unwrap();
            table.remove(b"a".as_slice()).unwrap();
            table.insert(b"z".as_slice(), b"new".as_slice()).unwrap();
        }
        write.commit().unwrap();
        assert_eq!(giant.materialize().unwrap().1.value(), vec![7; HUGE]);
        let (key, value) = bounded(|| cursor.next().unwrap().unwrap().materialize().unwrap());
        assert_eq!(key.value(), b"z");
        assert_eq!(value.value(), b"old");
        assert!(cursor.next().is_none());
        drop((key, value, giant, cursor));
        drop(table);
        drop(pinned);
        drop(db);
        let mut db = Builder::new().set_cache_size(0).open(&path).unwrap();
        assert!(db.check_integrity().unwrap());
        let read = db.begin_read().unwrap();
        let table = read.open_byte_table_bounded(VALUES).unwrap();
        let mut cursor = table.cursor_bounded();
        let (key, value) = cursor.next().unwrap().unwrap().materialize().unwrap();
        assert_eq!(key.value(), b"z");
        assert_eq!(value.value(), b"new");
        assert!(cursor.next().is_none());
    }

    #[test]
    fn cursor_layout_validation_rejects_malformed_navigation() {
        assert!(validate_node(&[BRANCH, 0, 0, 0]).is_err());
        let mut branch = vec![0; 64];
        branch[0] = BRANCH;
        branch[2..4].copy_from_slice(&1u16.to_le_bytes());
        // One separator requires two checksums/pages and one offset, beyond this page.
        assert!(validate_node(&branch).is_err());
        let mut leaf = vec![0; 32];
        leaf[0] = LEAF;
        leaf[2..4].copy_from_slice(&1u16.to_le_bytes());
        leaf[4..8].copy_from_slice(&20u32.to_le_bytes());
        leaf[8..12].copy_from_slice(&19u32.to_le_bytes());
        assert!(validate_node(&leaf).is_err());
        leaf[8..12].copy_from_slice(&33u32.to_le_bytes());
        assert!(validate_node(&leaf).is_err());
        assert!(validate_node(&[9, 0, 1, 0]).is_err());
    }

    #[test]
    fn cursor_retained_root_keeps_full_checksum_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cursor.db");
        create(&path, false);
        let roots = Arc::new(std::sync::Mutex::new(Vec::new()));
        let registered = roots.clone();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let db = Builder::new()
            .set_cache_size(0)
            .open_with_backend_and_external_snapshots(
                crate::backends::FileBackend::new(file).unwrap(),
                Arc::new(move || Ok(registered.lock().unwrap().clone())),
            )
            .unwrap();
        let pin = db.begin_read().unwrap();
        let snapshot = pin.storage_snapshot_identity();
        roots.lock().unwrap().push(snapshot);
        let external = db.begin_external_read(snapshot).unwrap();
        let table = external.open_byte_table_bounded(VALUES).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let observation = observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_add(bytes, AtomicOrdering::SeqCst);
        }));
        bounded(|| {
            let mut cursor = table.cursor_bounded();
            assert!(cursor.next().unwrap().is_err());
            assert!(cursor.next().is_none());
        });
        assert_eq!(reads.load(AtomicOrdering::SeqCst), 0);
        drop(observation);
        let handle = table.cursor_bounded().next().unwrap().unwrap();
        assert!(handle.key_matches(b"z:unrelated").unwrap());
        assert_eq!(handle.materialize().unwrap().1.value(), vec![7; HUGE]);
    }
    #[test]
    fn verified_bounded_lookup_memo_checks_every_expected_checksum_and_disk_corruption() {
        use super::super::page_store::PageMemory;
        use std::os::unix::fs::FileExt;
        for cache_size in [0, 1024 * 1024] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("memo.db");
            let db = Builder::new().set_cache_size(0).create(&path).unwrap();
            let write = db.begin_write().unwrap();
            write
                .open_table(VALUES)
                .unwrap()
                .insert(b"key".as_slice(), b"original".as_slice())
                .unwrap();
            write.commit().unwrap();
            drop(db);
            let disk = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let state = Arc::new(std::sync::Mutex::new(NodeDamageState::default()));
            let backend = DamagedNodeBackend {
                inner: Arc::new(
                    crate::backends::FileBackend::new(disk.try_clone().unwrap()).unwrap(),
                ),
                state: state.clone(),
            };
            let roots = Arc::new(std::sync::Mutex::new(Vec::new()));
            let registered = roots.clone();
            let db = Builder::new()
                .set_cache_size(cache_size)
                .open_with_backend_and_external_snapshots(
                    backend,
                    Arc::new(move || Ok(registered.lock().unwrap().clone())),
                )
                .unwrap();
            let pin = db.begin_read().unwrap();
            let snapshot = pin.storage_snapshot_identity();
            roots.lock().unwrap().push(snapshot);
            let external = db.begin_external_read(snapshot).unwrap();
            let table = external.open_byte_table_bounded(VALUES).unwrap();
            let cursor = table.cursor_bounded();
            let (number, expected) = cursor.next.unwrap();
            state.lock().unwrap().first_read = None;
            let before = PageMemory::byte_checksum_counts_for_test();
            assert_eq!(
                table.get_bounded(b"key").unwrap().unwrap().value(),
                b"original"
            );
            let computed = PageMemory::byte_checksum_counts_for_test();
            assert_eq!(
                computed[if cache_size == 0 { 2 } else { 0 }]
                    - before[if cache_size == 0 { 2 } else { 0 }],
                1
            );
            assert_eq!(
                table.get_bounded(b"key").unwrap().unwrap().value(),
                b"original"
            );
            let reused = PageMemory::byte_checksum_counts_for_test();
            if cache_size != 0 {
                assert_eq!(reused[0], computed[0]);
                assert_eq!(reused[3] - computed[3], 1);
            } else {
                assert_eq!(reused[2] - computed[2], 1);
            }
            let page = cursor.mem.get_page(number, cursor.hint).unwrap();
            assert_eq!(page.memory()[0], LEAF);
            let disk_bytes = std::fs::read(&path).unwrap();
            let offset = disk_bytes
                .windows(page.memory().len())
                .position(|bytes| bytes == page.memory())
                .expect("captured page exists on disk") as u64;
            validate_node(page.memory()).unwrap();
            let leaf = LeafAccessor::new(page.memory(), None, None);
            let (start, end) = leaf.value_range(0).unwrap();
            let mut corrupt = page.memory().to_vec();
            corrupt[start] ^= 1;
            let corrupt_expected = super::super::page_store::xxh3_checksum(&corrupt[..end]);
            assert_ne!(corrupt_expected, expected);
            disk.write_all_at(&corrupt[start..start + 1], offset + start as u64)
                .unwrap();
            disk.sync_data().unwrap();
            if cache_size == 0 {
                // Fresh fault of the real changed disk page: original root
                // expectation must yield typed corruption through public get.
                assert_corrupted_without_panic(|| table.get_bounded(b"key"));
            } else {
                // Resident immutable bytes keep existing visibility semantics.
                assert_eq!(
                    table.get_bounded(b"key").unwrap().unwrap().value(),
                    b"original"
                );
                let before_mismatch = PageMemory::byte_checksum_counts_for_test();
                // The same production point walker visits its memoized page
                // with the checksum a corrupt/new parent would supply.
                assert_corrupted_without_panic(|| {
                    get(
                        Some(BtreeHeader::new(number, corrupt_expected, 0)),
                        &cursor.mem,
                        cursor.hint,
                        b"key",
                    )
                });
                let after_mismatch = PageMemory::byte_checksum_counts_for_test();
                assert_eq!(after_mismatch[0], before_mismatch[0]);
                assert_eq!(after_mismatch[3] - before_mismatch[3], 1);
                assert_eq!(
                    table.get_bounded(b"key").unwrap().unwrap().value(),
                    b"original"
                );
            }
            // Restore this task-created store before redb close housekeeping.
            disk.write_all_at(&page.memory()[start..start + 1], offset + start as u64)
                .unwrap();
            disk.sync_data().unwrap();
        }
    }
}
