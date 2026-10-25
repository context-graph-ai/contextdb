//! An operation owns its revisited pages and the exact index backing.
//! Neither the shared file cache nor a subsequent reader inherits its invoice.
use crate::{Result, StorageError};
use crate::read_admission::{ReadMemoryCharge, admit_read_memory};
use crate::tree_store::PageMemory;
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScanKey(pub(crate) [u64; 4]);
impl ScanKey {
    fn bucket(self, capacity: usize) -> usize {
        let mut hash = 0x9e3779b97f4a7c15_u64;
        for word in self.0 {
            hash ^= word.wrapping_add(0x9e3779b97f4a7c15);
            hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d049bb133111eb);
            hash ^= hash >> 31;
        }
        (hash as usize) & (capacity - 1)
    }
}
struct Entry { key: ScanKey, page: PageMemory }
#[allow(dead_code)] // Receipts settle only after the array is physically freed.
enum Invoice {
    Legacy(Option<ReadMemoryCharge>),
    #[cfg(feature = "contextdb-internal")]
    Native(crate::NativeStorageCredit),
}
struct Table {
    slots: NonNull<Option<Entry>>,
    capacity: usize,
    invoice: Option<Invoice>,
}
// Slots are accessed only through the scan state mutex. Entries and invoices
// are Send; no pointer or reference escapes that lock.
unsafe impl Send for Table {}
impl Table {
    fn slot(&self, index: usize) -> &Option<Entry> {
        unsafe { &*self.slots.as_ptr().add(index) }
    }
    fn slot_mut(&mut self, index: usize) -> &mut Option<Entry> {
        unsafe { &mut *self.slots.as_ptr().add(index) }
    }
    fn insert(&mut self, entry: Entry) {
        let mut bucket = entry.key.bucket(self.capacity);
        while self.slot(bucket).is_some() { bucket = (bucket + 1) & (self.capacity - 1); }
        *self.slot_mut(bucket) = Some(entry);
    }
    fn new(capacity: usize, native: bool) -> Result<Self> {
        let layout = Layout::array::<Option<Entry>>(capacity)
            .map_err(|_| StorageError::ValueTooLarge(capacity))?;
        #[cfg(feature = "contextdb-internal")]
        let invoice = if native {
            Invoice::Native(crate::native_storage_admission::admit_native_storage(layout)
                .map_err(StorageError::Native)?)
        } else { Invoice::Legacy(admit_read_memory(layout.size())?) };
        #[cfg(not(feature = "contextdb-internal"))]
        let invoice = { let _ = native; Invoice::Legacy(admit_read_memory(layout.size())?) };
        let Some(slots) = NonNull::new(unsafe { alloc(layout) }.cast::<Option<Entry>>()) else {
            return Err(crate::native_storage_admission::native_storage_error(
                crate::native_storage_admission::NativeColdReadError::AllocatorFailed {
                    site: "scan page index", layout,
                }));
        };
        for index in 0..capacity { unsafe { slots.as_ptr().add(index).write(None) }; }
        Ok(Self { slots, capacity, invoice: Some(invoice) })
    }
}
struct FreeTable {
    slots: NonNull<Option<Entry>>,
    layout: Layout,
    _invoice: Option<Invoice>,
}
impl Drop for FreeTable {
    fn drop(&mut self) { unsafe { dealloc(self.slots.as_ptr().cast(), self.layout) }; }
}
impl Drop for Table {
    fn drop(&mut self) {
        let _free = FreeTable {
            slots: self.slots,
            layout: Layout::array::<Option<Entry>>(self.capacity).expect("allocated layout"),
            _invoice: self.invoice.take(),
        };
        // Array drop glue finishes other elements and frees backing on unwind.
        unsafe { std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
            self.slots.as_ptr(), self.capacity)) };
    }
}
#[derive(Default)]
pub(crate) struct ScanCache {
    table: Option<Table>, len: usize,
    counted_file: Option<(u64, u64)>, pinned_bytes: usize,
}
impl ScanCache {
    pub(crate) fn get(&self, key: ScanKey) -> Option<PageMemory> {
        let table = self.table.as_ref()?;
        let mut bucket = key.bucket(table.capacity);
        loop {
            let entry = table.slot(bucket).as_ref()?;
            if entry.key == key { return Some(entry.page.clone()); }
            bucket = (bucket + 1) & (table.capacity - 1);
        }
    }
    pub(crate) fn reserve(&mut self, native: bool) -> Result<()> {
        let capacity = self.table.as_ref().map_or(0, |table| table.capacity);
        if self.len < capacity / 2 { return Ok(()); }
        let next_capacity = capacity.checked_mul(2).filter(|n| *n != 0)
            .or_else(|| (capacity == 0).then_some(64))
            .ok_or(StorageError::ValueTooLarge(capacity))?;
        // The replacement is admitted before allocating while the old table
        // and every original page remain paid. Refusal leaves them untouched.
        let mut next = Table::new(next_capacity, native)?;
        if let Some(old) = &mut self.table {
            for index in 0..old.capacity {
                if let Some(entry) = old.slot_mut(index).take() { next.insert(entry); }
            }
        }
        self.table = Some(next);
        Ok(())
    }
    pub(crate) fn pinned_bytes(&self, key: ScanKey) -> usize {
        if self.counted_file == Some((key.0[0], key.0[1] & !1)) { self.pinned_bytes } else { 0 }
    }
    pub(crate) fn insert(&mut self, key: ScanKey, page: PageMemory, shared_bytes: usize) {
        let file = (key.0[0], key.0[1] & !1);
        if self.counted_file != Some(file) { self.counted_file = Some(file); self.pinned_bytes = 0; }
        let mut twin = key; twin.0[1] ^= 1;
        let already_counted = self.get(key).is_some() || self.get(twin)
            .is_some_and(|other| other.same_backing(&page));
        if !already_counted { self.pinned_bytes = self.pinned_bytes.saturating_add(shared_bytes); }
        let table = self.table.as_mut().expect("reserved scan index");
        let mut bucket = key.bucket(table.capacity);
        loop {
            match table.slot_mut(bucket) {
                Some(entry) if entry.key == key => { entry.page = page; return; }
                slot @ None => { *slot = Some(Entry { key, page }); self.len += 1; return; }
                Some(_) => bucket = (bucket + 1) & (table.capacity - 1),
            }
        }
    }
}
