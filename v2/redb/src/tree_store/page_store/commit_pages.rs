use super::{Page, PageHint, PageImpl, PageNumber, TransactionalMemory, xxh3_checksum};
use crate::transactions::PageList;
use crate::{MutInPlaceValue, Result, StorageError, Value};
use std::cell::RefCell;

/// The most entries a list holds, its own pages among them: a check holds 8 bytes and a bit each.
const LONGEST: usize = 65_536;

/// A commit's list as it was written, in rising order of place, 8 bytes a page: its key, whose
/// last bit is set once a check has read the page, which catches a tree that loops.
pub(crate) struct Listed(RefCell<Vec<u64>>);

/// A page's place, then its order, then that bit: rising with the place for pages that do not
/// overlap.
fn key(page: PageNumber) -> u64 {
    (u64::from(page.region) << 32 | u64::from(page.page_index) << page.page_order) << 6
        | u64::from(page.page_order) << 1
}

impl Listed {
    /// `None` for a page the commit does not list, else whether this is its first read.
    pub(crate) fn first(&self, page: PageNumber) -> Option<bool> {
        let mut pages = self.0.borrow_mut();
        let at = pages.binary_search_by(|listed| (listed | 1).cmp(&(key(page) | 1)));
        let listed = &mut pages[at.ok()?];
        let first = *listed & 1 == 0;
        *listed |= 1;
        Some(first)
    }
}

pub(super) fn next(bytes: &[u8]) -> Option<PageNumber> {
    (bytes[..8] != [255; 8]).then(|| PageNumber::from_le_bytes(bytes[..8].try_into().unwrap()))
}

pub(super) fn read(mem: &TransactionalMemory, number: PageNumber) -> Result<PageImpl> {
    mem.validate_page(number)?;
    if number.page_order != 0 {
        return Err(StorageError::Corrupted("Invalid proof page".into()));
    }
    let page = mem.get_page(number, PageHint::None)?;
    let bytes = page.memory();
    let end = bytes.len() - 16;
    if xxh3_checksum(&bytes[..end]).to_le_bytes() != bytes[end..]
        || 10 + 8 * PageList::from_bytes(&bytes[8..end]).len() > end
    {
        return Err(StorageError::Corrupted("Invalid commit page chain".into()));
    }
    Ok(page)
}

pub(super) fn write(mem: &TransactionalMemory, mut head: Option<PageNumber>) -> Result {
    let allocated = mem.allocated_since_commit.lock().unwrap();
    let mut pages = allocated.iter();
    let capacity = (mem.get_page_size() - 26) / 8;
    while let Some(number) = head {
        let mut page = mem.get_page_mut(number)?;
        let bytes = page.memory_mut();
        head = next(bytes);
        let end = bytes.len() - 16;
        bytes[8..].fill(0);
        let list = PageList::from_bytes_mut(&mut bytes[8..end]);
        for number in pages.by_ref().take(capacity) {
            list.push_back(number);
        }
        let checksum = xxh3_checksum(&bytes[..end]);
        bytes[end..].copy_from_slice(&checksum.to_le_bytes());
    }
    assert!(
        pages.next().is_none(),
        "Commit proof was not reserved in prepare"
    );
    Ok(())
}

impl TransactionalMemory {
    pub(crate) fn prepare_commit_pages(&self) -> Result {
        if !self.incremental_recovery {
            return Ok(());
        }
        let mut head = self.commit_pages.lock().unwrap();
        let capacity = (self.get_page_size() - 26) / 8;
        let count = self.allocated_since_commit.lock().unwrap().counts().1;
        let chains = count.div_ceil(capacity - 1);
        // A longer commit writes no list and syncs its pages before its header
        for _ in 0..chains * usize::from(count + chains <= LONGEST) {
            let mut page = self.allocate_lowest(self.get_page_size())?;
            page.memory_mut()[..8].copy_from_slice(&head.map_or([255; 8], PageNumber::to_le_bytes));
            *head = Some(page.get_page_number());
        }
        Ok(())
    }
    pub(crate) fn next_commit_page(&self, number: PageNumber) -> Result<Option<PageNumber>> {
        Ok(next(read(self, number)?.memory()))
    }
    pub(crate) fn visit_commit_chain(
        &self,
        mut head: Option<PageNumber>,
        mut visitor: impl FnMut(PageNumber, &[u8]) -> Result,
    ) -> Result {
        // A chain longer than the longest list's is a loop
        let mut links = 0..=LONGEST / ((self.get_page_size() - 26) / 8);
        while let Some(number) = head {
            if links.next().is_none() {
                return Err(StorageError::Corrupted("Cyclic commit page chain".into()));
            }
            let page = read(self, number)?;
            head = next(page.memory());
            visitor(number, page.memory())?;
        }
        Ok(())
    }

    pub(crate) fn listed_commit_pages(&self) -> Result<Option<Listed>> {
        if !self.incremental_recovery || self.used_two_phase_commit() {
            return Ok(None);
        }
        let Some(head) = self.commit_pages_to_free() else {
            // A commit with no list took no page, or synced its pages before its header: only
            // its roots are new to a check
            let roots = [self.get_data_root(), self.get_system_root()].into_iter();
            let mut roots: Vec<_> = roots.flatten().map(|root| key(root.root)).collect();
            roots.sort_unstable();
            return Ok(Some(Listed(RefCell::new(roots))));
        };
        let mut listed: Vec<u64> = Vec::new();
        self.visit_commit_chain(Some(head), |_, bytes| {
            let list = PageList::from_bytes(&bytes[8..bytes.len() - 16]);
            listed.reserve_exact(list.len());
            for i in 0..list.len() {
                let number = list.get(i);
                self.validate_page(number)?;
                // The list is written in rising order: a page not past the one before is damage
                let end = |last: u64| ((last >> 6) + (1 << (last >> 1 & 31))) << 6;
                if listed.last().is_some_and(|&last| key(number) < end(last)) {
                    return Err(StorageError::Corrupted("Repeated commit page".into()));
                }
                listed.push(key(number));
            }
            Ok(())
        })?;
        Ok(Some(Listed(RefCell::new(listed))))
    }
}
