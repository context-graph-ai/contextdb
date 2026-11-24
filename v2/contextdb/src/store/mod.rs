//! store: the folder and the data file; how each kind of record is keyed; views of one moment;
//! reader files and locks.
//!
//! It never interprets SQL or a table's rules: it keeps rows, index entries, counts, schema
//! records and the vector part's records under the keys it is given and hands them back exactly
//! as they were written.
//!
//! Two of the things the parts meet at are defined here: [`Read`], the view to read from, and
//! [`ChangeSet`], the change set to write with.

mod counts;
mod disk;
mod through;
pub(crate) const PART_BYTES: u64 = 1 << 20;
pub(crate) const NOTE: u8 = 253;
pub(crate) const SET: u8 = 254;
pub(crate) type SetRecord = ((Vec<u8>, Vec<u8>), Reservation);
mod drop;
mod folder;
mod keys;
mod record;
mod recovery;
#[cfg(test)]
mod tests;

use std::cell::Cell as Counter;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

pub use redb::StorageBackend;
use redb::{ReadTransaction, ReadableDatabase, TableDefinition, WriteTransaction};

use crate::database::{DatabaseOpenOptions, locked};
use crate::events::AuditQueue;
use crate::memory::{Budget, Passing, Reservation};
use crate::read::ReadSessionOptions;
use crate::types::{Error, Result, SortDirection, TriggerEvent, TxId, Value, heap_bytes};
pub(crate) use disk::{AUDIT, DELIVERY, LAST_AUDIT, READING, Record, SCHEDULE};
use disk::{Moment, Stored, Tables};
pub(crate) use drop::{Dropped, VERSION_KEYS};
use folder::{Hold, Locked};
pub(crate) use keys::{Entry, EntryShape, KEY_LIMIT, Span, cells_end, encoded_len, span};
pub(crate) use record::{
    delivery_parts, most, put_text, put_value, read_value, value_len, value_prefix,
};

/// The size of the page cache of a store opened under this memory limit, in bytes: a quarter
/// of the limit and never less than 8 MiB, which is also its size when there is no limit. It is
/// reserved from the budget when the store is opened and does not change until the store is
/// closed, because the storage library fixes its cache when the data file is opened.
pub(crate) fn cache_under(limit: Option<u64>, share: Option<u64>) -> u64 {
    let limit = limit.unwrap_or(0);
    share.map_or((limit / 4).max(8 << 20), |p| {
        limit / 100 * p + limit % 100 * p / 100
    })
}

/// What a refusal names when the memory limit cannot hold the page cache.
const CACHE_NEEDED: &str = "page cache needed to open the store";

/// A function that wraps a store's data file, so that a host sees or changes every read, write
/// and sync of it.
///
/// It is given in [`crate::DatabaseOpenOptions::storage`] or
/// [`crate::ReadSessionOptions::storage`]; with none given the file is read and written as it
/// is. The library opens and locks the data file itself and calls the function once per open.
/// It reads and writes only through the returned [`StorageBackend`]; the initial cache-setting
/// probe shares that backend through a read-only view. A session opened for reading hands over
/// a file opened for reading only. A host wraps
/// the file to keep the bytes somewhere else or to change them on the way; a test wraps it to
/// count the calls, to fail one or to stop at one.
///
/// An error returned by the function, or by a call of what it returned, is reported as
/// [`crate::Error::StoreIo`]. The two exceptions are the kinds `UnexpectedEof` and
/// `InvalidData`, which say the bytes are not what was stored and are reported as
/// [`crate::Error::StoreCorrupted`].
pub type WrapStorage = Arc<dyn Fn(File) -> io::Result<Box<dyn StorageBackend>> + Send + Sync>;

/// A table's number inside one store. `schema` gives it when the table is declared, and a
/// number is never given twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TableId(pub u32);

/// An index's number inside one store, given by `schema` like a [`TableId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexId(pub u32);

/// The number of a kept count, given by `schema` like an index's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CountId(pub u32);

/// A VECTOR column's number inside one store, given by `schema` like a [`TableId`]. Every record
/// the vector part keeps for the column begins with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VectorId(pub u32);

/// A record the vector part keeps. `store` decides the bytes of its key and where it is held;
/// the value is the vector part's own and comes back exactly as it was put.
///
/// Records of one kind are in the order of their fields, left to right: every record of one
/// column is one run of keys, and inside it every pending row or block of one partition is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorRecord<'a> {
    /// What the column keeps about one partition, under the partition's key tuple: the values
    /// of the PARTITION_KEY columns encoded as [`Key::from_values`] encodes them, ascending. A
    /// column with no PARTITION_KEY has one partition, under the key of no values.
    Partition(VectorId, &'a Key),
    /// A row whose vector is in no block yet: the partition's number, then the row's key. Its
    /// value is empty.
    Pending(VectorId, u32, &'a Key),
    /// One block: the number its blocks are kept under, then the block's own. A partition's
    /// tail blocks are kept under the partition's number and the blocks of an IVF under
    /// the IVF's; both come from the column's one run of numbers, so they never meet.
    Block(VectorId, u32, u32),
    /// An IVF of a partition's blocks, under the IVF's number: how far from its
    /// nearest group a search scans, and where each group's slots begin in its blocks.
    Ivf(VectorId, u32),
    /// The centroids of an IVF, some to a record: the IVF's number, then the record's
    /// place among them. Kept with the blocks, as records of the same size.
    Centroids(VectorId, u32, u32),
    /// An IVF being built for a partition, under the partition's number: how far it has
    /// come.
    Build(VectorId, u32),
    /// The group each slot of one block goes to in an IVF being built: the number of that
    /// IVF, then the block's place among the blocks it is built from.
    Assigned(VectorId, u32, u32),
    /// The binary codes of one block, one for each of its slots in their order: the two numbers
    /// of the block. Kept with the blocks, an eighth of a block each.
    Binary(VectorId, u32, u32),
    /// The mean the binary codes of a partition's tail blocks are taken from, under the
    /// partition's number.
    Mean(VectorId, u32),
    /// A row that had a sealed slot in a partition has left it: the partition's number and the
    /// row's key. The value is how many slots had been sealed into the partition's tail by
    /// then, so every slot of that key sealed earlier is stale.
    Left(VectorId, u32, &'a Key),
}

/// A number the store keeps for the vector part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorNumber {
    /// How many partitions a column has.
    Partitions(VectorId),
    /// The number the column's next new partition takes. It only ever rises.
    NextPartition(VectorId),
    /// How many pending records there are, over every column.
    Pending,
}

/// A vector cell that `write` asks the store to keep in one byte a number: where the cell is
/// in its row, and the scale and codes the vector part made of it. The row's value at that
/// place is each code times the scale, and that is what reading the row gives back. The store
/// works nothing out from a vector's numbers and recognises nothing in them.
pub(crate) struct Compact {
    pub at: usize,
    pub scale: f32,
    pub codes: Vec<i8>,
}

/// A primary key or an index entry, encoded so that comparing the bytes compares the values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Key(Vec<u8>);

impl Key {
    /// Encodes the values of a key in column order. Two keys compare as their values do, column
    /// by column; ascending, NULL after every value; a descending column is the reverse. A
    /// vector or JSON value cannot be part of a key.
    ///
    /// `directions` gives each column's declared direction; a column it does not reach is
    /// ascending, so a row key passes none. In a REAL column the two zeros are one key, and
    /// every NaN is one key above every number.
    ///
    /// The key of the leading columns of an index, made with their directions, is the bytes
    /// that every full key with those values begins with, and no other key begins with them.
    pub(crate) fn from_values(values: &[Value], directions: &[SortDirection]) -> Result<Key> {
        Ok(Key(keys::encode(values, directions, usize::MAX)?.0))
    }

    /// The bytes of the key, for a part that keeps keys inside a record of its own.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The key that [`Key::as_bytes`] gave these bytes for.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Key {
        Key(bytes.to_vec())
    }

    /// An index entry followed by its row key, in the index's stored order.
    pub(crate) fn joined(&self, row: &Key) -> Key {
        Key([self.0.as_slice(), &row.0].concat())
    }

    /// True when this key begins with every value of `prefix`, in order. Every key begins with
    /// the key of no values.
    pub(crate) fn starts_with(&self, prefix: &Key) -> bool {
        self.0.starts_with(&prefix.0)
    }

    /// The values the key was made from, ascending columns only: what [`Key::from_values`]
    /// was given. Bytes that are no such key are `None`.
    pub(crate) fn values(&self) -> Option<Vec<Value>> {
        keys::decode(&self.0)
    }

    /// True when the complete key contains exactly this many valid ascending cells.
    pub(crate) fn has_arity(&self, expected: usize) -> bool {
        let mut key = self.0.as_slice();
        (0..expected).all(|_| keys::cell(&mut key).is_some()) && key.is_empty()
    }

    /// The first key after every key that begins with this one, `None` when there is none.
    pub(crate) fn prefix_end(&self) -> Option<Key> {
        let last = self.0.iter().rposition(|byte| *byte != 0xFF)?;
        let mut end = self.0[..=last].to_vec();
        end[last] += 1;
        Some(Key(end))
    }
}

/// One stored row as a read returns it: one cell per column, in the order the row was written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Row {
    pub written: Option<u32>,
    pub cells: Vec<Cell>,
    pub bytes: u64,
}

impl Row {
    fn besides(self) -> impl Iterator<Item = Beside> {
        self.cells.into_iter().filter_map(|cell| match cell {
            Cell::Beside(kept) => Some(kept),
            Cell::Here(_) => None,
        })
    }
}

/// One column's value in a [`Row`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell {
    /// The value itself.
    Here(Value),
    /// A large text, JSON or vector value kept beside the row. Reading the row did not read it.
    Beside(Beside),
}

/// Where a large value is kept and how many bytes are stored for it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Beside {
    /// The bytes of the value's stored record. A large text or JSON value may be stored
    /// compressed, so this can be less than the bytes the loaded value takes.
    bytes: u64,
    /// The place of the first slice of the record; the others are under the places after it.
    place: u64,
}

/// Rows in key order.
pub(crate) type RowIter<'a> = Box<dyn Iterator<Item = Result<(Key, Row)>> + 'a>;

/// What [`Read::counted`] counts: the rows of a table, or the entries of an index.
#[derive(Clone, Copy)]
pub(crate) enum Keys {
    Table(TableId),
    Index(IndexId),
}

/// Index entries in entry order, each with the key of the row it points at and the place its
/// first cut value's kept bytes end, when it holds one.
pub(crate) type EntryIter<'a> = Box<dyn Iterator<Item = Result<(Key, Key, Option<usize>)>> + 'a>;

/// One unit for each row or entry counted.
pub(crate) type UnitIter<'a> = Box<dyn Iterator<Item = Result<()>> + 'a>;

/// The records of a kept count in key order, each with its count.
pub(crate) type KeptIter<'a> = Box<dyn Iterator<Item = Result<(Key, u64)>> + 'a>;

/// The rows a change set has staged: table, key, the new row or `None` for a delete, and
/// whether the transaction found no row at the key when it first staged it. A row is its
/// values, with NULL for each large value it left where it is, and those cells with their
/// places in the row.
pub(crate) type StagedRows<'a> =
    Box<dyn Iterator<Item = (TableId, &'a Key, Option<StagedValues<'a>>, bool)> + 'a>;

/// A staged row's values, and the large cells it left where they are.
pub(crate) type StagedValues<'a> = (&'a [Value], &'a [(usize, Beside)]);

/// The view to read rows from. `query` and `write` read a store only through this.
///
/// A [`View`] answers from one committed moment. A [`ChangeSet`] answers from its own staged
/// changes first and from its view otherwise, so a transaction reads what it has written.
/// Every call on one reader answers from the same moment, however long the reader lives.
pub(crate) trait Read {
    fn committed(&self) -> &View;
    fn setting(&self, at: usize) -> Result<Option<u64>>;
    fn valid(&self) -> Result<()> {
        Ok(())
    }
    /// The commit this reader shows.
    fn commit(&self) -> TxId;

    /// One row by key. Costs one walk down the tree: the pages on that path, through the cache.
    fn row(&self, table: TableId, key: &Key, cover: &mut Passing) -> Result<Option<Row>>;

    /// The rows of a table between two keys, in key order or, `back`, from the last key down.
    /// Pages are read as the iterator moves, so stopping early costs only what was visited.
    /// Nothing is collected.
    ///
    /// A bound made from the first columns of a key stands for every key that begins with
    /// those values: included, it takes them all in; excluded, it leaves them all out.
    fn rows<'a>(
        &'a self,
        table: TableId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
        cover: &'a mut Passing,
    ) -> Result<RowIter<'a>>;

    /// One unit for each row [`Read::rows`] or entry [`Read::index`] gives between two keys,
    /// with no row read and nothing copied.
    fn counted(&self, of: Keys, from: Bound<&Key>, to: Bound<&Key>) -> Result<UnitIter<'_>>;

    fn contains(&self, table: TableId, key: &Key) -> Result<bool> {
        let at = Bound::Included(key);
        let mut found = self.counted(Keys::Table(table), at, at)?;
        found.next().transpose().map(|found| found.is_some())
    }

    /// The entries of an index between two entries, in entry order or, `back`, the reverse.
    /// Costs like [`Read::rows`],
    /// and a bound of fewer columns means what it means there: it is never taken as one exact
    /// key. A bound applies to the entry alone, so an entry equal to an included bound is in
    /// range whatever row it points at. A complete entry joined to its row key instead bounds
    /// that single stored pair, so an excluded lower bound resumes after it.
    fn index(
        &self,
        index: IndexId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
    ) -> Result<EntryIter<'_>>;

    /// The records of a kept count between two keys, in key order.
    fn kept(&self, count: CountId, from: Bound<&Key>, to: Bound<&Key>) -> Result<KeptIter<'_>>;

    /// How many rows the table holds at this moment. One small record, never a scan.
    fn count(&self, table: TableId) -> Result<u64>;

    /// Loads a value kept beside its row. Grows `reservation` by the bytes stored for the
    /// value first and refuses with `MemoryBudgetExceeded` before reading anything if that does
    /// not fit. A value that is stored compressed can be refused a second time, after its
    /// stored bytes are read and before any byte of the value exists: the reservation is then
    /// grown by the bytes the value really takes.
    ///
    /// After a load the reservation holds the bytes the value takes, however it was stored;
    /// while a JSON or vector value is made it holds them twice. A load that fails gives back
    /// all it grew. A stored length the stored bytes do not support is `StoreCorrupted`.
    fn load(&self, beside: &Beside, reservation: &mut Reservation) -> Result<Value>;

    /// Calls `each` with the key of every row of the table that this reader has staged over
    /// its committed moment, in key order, until `each` answers false: the rows a transaction
    /// has written or removed and not yet committed. A [`View`] has none.
    fn staged(&self, table: TableId, each: &mut dyn FnMut(&Key) -> Result<bool>) -> Result<()>;

    /// Calls `each` with the vector part's records of the kind of `from`, in key order, from
    /// `from` itself or the first record after it, until `each` answers false or the kind has
    /// no more records. Records of other columns follow those of `from`'s column, so the
    /// caller stops where its own run ends.
    ///
    /// The value is lent, not copied: a block is handed over in place from the page cache, and
    /// costs one read of the file when it is not there. A partition's key tuple compares with
    /// `from` as a whole key, so the tuple of no values starts at a column's first partition.
    ///
    /// Vector records are staged only inside the commit function and by maintenance, after the last
    /// read of their change set. A [`ChangeSet`] therefore answers from its committed moment.
    fn vectors(
        &self,
        from: VectorRecord<'_>,
        each: &mut dyn FnMut(VectorRecord<'_>, &[u8]) -> Result<bool>,
    ) -> Result<()>;

    /// One of the numbers kept for the vector part; zero when it was never put.
    fn vector_number(&self, number: VectorNumber) -> Result<u64>;

    /// The refusal of a record that its own bytes, or the records it goes with, do not
    /// support: `StoreCorrupted`, naming the store's folder, with this reason. A part that
    /// keeps records of its own tells the damage it finds in them with this.
    fn damaged(&self, reason: &str) -> Error;
}

/// One committed moment of a store.
///
/// Cloning is cheap. While any clone lives, no page that this commit can reach is reused; for a
/// reader in another process that promise is what its file in `readers/` stands for, and the
/// file goes when the last clone is dropped.
#[derive(Clone)]
pub(crate) struct View {
    pub(crate) moment: Arc<Moment>,
    /// The rows read through this copy, when a statement counts the reads it makes beside its
    /// change set.
    read: Option<Arc<AtomicU64>>,
}

impl View {
    /// A copy of this view that counts every row read through it into `read`.
    pub(crate) fn counting(&self, read: &Arc<AtomicU64>) -> View {
        View {
            read: Some(read.clone()),
            ..self.clone()
        }
    }
    fn tally(&self, rows: u64) {
        if let Some(read) = &self.read {
            read.fetch_add(rows, AtomicOrdering::Relaxed);
        }
    }
}

type Space = (u64, u64, u64, u64, bool);
pub(super) type Compaction = (Option<Vec<u8>>, Option<usize>, bool, Arc<AtomicU64>);

/// The change set to write with: everything one transaction means to change, held in memory
/// until it is committed or dropped.
///
/// Its memory is charged to the budget it was made with, and every `put` refuses with
/// `MemoryBudgetExceeded` rather than grow past the limit. Dropping it discards the changes.
/// `write` decides which index entries and count changes go with a row; this type records them
/// and nothing more.
pub struct ChangeSet {
    pub(crate) rows_read: Counter<u64>,
    base: View,
    /// State retained across the parts of one private storage transaction.
    data: Option<Arc<redb::Database>>,
    first: Option<View>,
    write: Option<WriteTransaction>,
    written: bool,
    derived: u64,
    tally: BTreeMap<TableId, u64>,
    held: Reservation,
    queued: u64,
    /// Staged rows by table and key.
    rows: Staged<StagedRow>,
    /// Staged index entries by index, each entry joined to the key of its row: the length of
    /// the entry and where its first cut value's kept bytes end, or `None` for a removal.
    index: Staged<Option<Joined>>,
    /// The change to each table's kept row count.
    counts: BTreeMap<u32, i64>,
    /// The change to each record of a kept count, by count and key.
    kept: Staged<i64>,
    /// Staged schema records by name: the record, or `None` for a removal.
    schema: BTreeMap<String, Option<Vec<u8>>>,
    /// What is to be dropped whole, in the order it was staged.
    dropped: Vec<Dropped>,
    /// Tables whose replaced rows are versions, true when a removed row's is too (no RETAIN).
    keeps: BTreeMap<u32, bool>,
    /// A PURGE or an expiry: a removed row's versions go with it.
    pub(crate) erasing: bool,
    /// The row guards, in the order they were staged.
    guards: Vec<RowGuard>,
    /// The number the transaction took when it began; zero when it took none.
    number: TxId,
    pub(super) compaction: Option<Compaction>,
    /// Staged records of the vector part by their stored key: the value, or `None` for a
    /// removal.
    vectors: BTreeMap<Key, Option<Vec<u8>>>,
    /// Staged numbers of the vector part, by their place among the store's numbers.
    numbers: BTreeMap<u64, u64>,
    /// Stored rows, kept counts and beside slices whose framing must remain unchanged.
    copied: Vec<(usize, Vec<u8>, Vec<u8>)>,
    /// How many rows of tables that declare no primary key the change set has made.
    made: u64,
    /// The staging operation, or None when the row leaves its key.
    pub(crate) operation: Option<TriggerEvent>,
    /// Every staging call of a statement in order, each with what it put aside and the bytes it
    /// was counted as, so that the calls after a mark can be undone.
    calls: Vec<(Undo, u64)>,
    undo: Reservation,
    reserved: Reservation,
    drain: bool,
}

/// Staged records of several tables, indexes or counts, by number and then by key.
type Staged<C> = BTreeMap<u32, BTreeMap<Key, C>>;

/// The length of an index entry inside the key that joins it to its row, and where the
/// entry's first cut value's kept bytes end, when it holds one.
type Joined = (usize, Option<usize>);

/// The values a row must still hold when its transaction is committed: the row's table and
/// key, and each guarded cell by its position.
pub(crate) struct RowGuard {
    pub table: TableId,
    pub key: Key,
    pub columns: Vec<(usize, Value)>,
}

struct StagedRow {
    bytes: u64,
    written: Option<u32>,
    /// The new values, or `None` for a removal.
    values: Option<Vec<Value>>,
    /// The large values that stay where they are, each by its cell's position; NULL stands at
    /// that position in `values`.
    left: Vec<(usize, Beside)>,
    /// The vector cells to write as a scale and codes.
    compact: Vec<Compact>,
    /// What the transaction said when it first staged this key: it found no row there.
    new_key: bool,
}

/// What one staging call put aside: the record it changed and what was staged there before,
/// `None` when nothing was.
enum Undo {
    Row(u32, Key, Option<StagedRow>, Option<TriggerEvent>),
    Entry(u32, Key, Option<Option<Joined>>),
    Count(u32, Option<i64>),
    Kept(u32, Key, Option<i64>),
    Schema(String, Option<Option<Vec<u8>>>),
    Vector(Key, Option<Option<Vec<u8>>>),
    Number(u64, Option<u64>),
    Dropped,
}

/// One staged map entry besides its key and payload; undo capacity is charged separately.
const STAGED_BYTES: u64 = 320;
const _: () = assert!(size_of::<(Key, StagedRow)>() * 2 + 64 <= STAGED_BYTES as usize);
/// A commit notice's share for one table, the longest name (63 bytes) and its slot; the first
/// table also holds the notice's head (128 bytes).
const TABLE_SUMMARY: u64 = 143;
fn staged_bytes(key: &Key, held: u64) -> u64 {
    STAGED_BYTES + 2 * key.0.len() as u64 + held
}

impl ChangeSet {
    /// An empty change set over the moment its transaction began.
    pub(crate) fn new(base: View, budget: &Budget) -> Result<ChangeSet> {
        Ok(ChangeSet {
            rows_read: Counter::default(),
            base,
            data: None,
            first: None,
            write: None,
            written: false,
            derived: 0,
            tally: BTreeMap::new(),
            held: budget.reserve(0, "transaction summary")?,
            queued: 0,
            rows: BTreeMap::new(),
            index: BTreeMap::new(),
            counts: BTreeMap::new(),
            kept: BTreeMap::new(),
            schema: BTreeMap::new(),
            dropped: Vec::new(),
            keeps: BTreeMap::new(),
            erasing: false,
            guards: Vec::new(),
            number: TxId(0),
            compaction: None,
            vectors: BTreeMap::new(),
            numbers: BTreeMap::new(),
            copied: Vec::new(),
            made: 0,
            operation: None,
            calls: Vec::new(),
            undo: budget.reserve(0, "transaction undo")?,
            reserved: budget.reserve(0, "transaction")?,
            drain: false,
        })
    }

    fn write_room(&mut self, bytes: u64) -> Result<()> {
        self.undo.room(&mut self.calls, usize::MAX)?;
        let drain = u64::from(!self.drain) * redb::WriteTransaction::DRAIN_BYTES as u64;
        self.reserved.grow(bytes + drain)?;
        self.drain = true;
        Ok(())
    }

    /// The committed moment the staged changes are laid over.
    pub(crate) fn base(&self) -> &View {
        &self.base
    }

    /// Lays the same staged changes over a newer moment and returns the moment they were laid
    /// over before. The commit function does this, under its lock, when another transaction
    /// committed first, and then asks `write` to check the rules again.
    pub(crate) fn rebase(&mut self, newest: View) -> View {
        std::mem::replace(&mut self.base, newest)
    }

    /// A point between two staging calls, to [`ChangeSet::rewind`] to.
    pub(crate) fn mark(&self) -> usize {
        self.calls.len()
    }

    /// Undoes every staging call made after the mark and gives back the memory they held.
    /// `database` takes a mark before each statement and rewinds when the statement is refused.
    pub(crate) fn rewind(&mut self, mark: usize) {
        for (undo, bytes) in self.calls.drain(mark..).rev() {
            match undo {
                Undo::Row(table, key, before, _) => {
                    put_back(self.rows.entry(table).or_default(), key, before);
                }
                Undo::Entry(index, joined, before) => {
                    put_back(self.index.entry(index).or_default(), joined, before);
                }
                Undo::Count(table, before) => put_back(&mut self.counts, table, before),
                Undo::Kept(count, key, before) => {
                    put_back(self.kept.entry(count).or_default(), key, before);
                }
                Undo::Schema(name, before) => put_back(&mut self.schema, name, before),
                Undo::Vector(key, before) => put_back(&mut self.vectors, key, before),
                Undo::Number(at, before) => put_back(&mut self.numbers, at, before),
                Undo::Dropped => drop(self.dropped.pop()),
            }
            self.reserved.shrink(bytes);
        }
        let capacity = self.calls.capacity();
        self.calls.shrink_to_fit();
        self.undo
            .shrink(((capacity - self.calls.capacity()) * size_of::<(Undo, u64)>()) as u64);
    }

    /// Stages a row, new or replacing the row with this key. Large text, JSON and vector values
    /// are kept beside the row when it is written; the caller does not split them. A
    /// [`Cell::Beside`] is a value this row holds already, handed back as it was read: it is
    /// written as the same place and none of its bytes is read or written.
    ///
    /// `compact` names the vector cells to keep in one byte a number, each with its scale and
    /// codes: such a cell is written as the tag `0x88`, its count of numbers, the scale and one
    /// signed byte a number, and every other vector as its numbers.
    ///
    /// `new_key` says the transaction found no row at this key. The answer given the first
    /// time a key is staged is the one kept, and [`ChangeSet::rows_changed`] hands it back.
    pub(crate) fn put_row(
        &mut self,
        table: TableId,
        key: Key,
        cells: Vec<Cell>,
        compact: Vec<Compact>,
        new_key: bool,
    ) -> Result<()> {
        let mut left = Vec::new();
        let values = cells
            .into_iter()
            .enumerate()
            .map(|(slot, cell)| match cell {
                Cell::Here(value) => value,
                Cell::Beside(kept) => {
                    left.push((slot, kept));
                    Value::Null
                }
            });
        let values = Some(values.collect());
        self.stage_row(table, key, values, left, compact, new_key)
    }

    /// Stages the removal of a row and of the values kept beside it. A key first staged here
    /// is one a row was at.
    pub(crate) fn delete_row(&mut self, table: TableId, key: &Key) -> Result<()> {
        self.stage_row(table, key.clone(), None, Vec::new(), Vec::new(), false)
    }

    fn stage_row(
        &mut self,
        table: TableId,
        key: Key,
        values: Option<Vec<Value>>,
        left: Vec<(usize, Beside)>,
        compact: Vec<Compact>,
        new_key: bool,
    ) -> Result<()> {
        let held: u64 = values.iter().flatten().map(held).sum();
        let codes: usize = compact.iter().map(|cell| cell.codes.len()).sum();
        let bytes = self.counted(&key, held + codes as u64)?;
        if self.data.is_none() && !self.rows.contains_key(&table.0) {
            self.held
                .grow(TABLE_SUMMARY + 128 * u64::from(self.rows.is_empty()))?;
        }
        let rows = self.rows.entry(table.0).or_default();
        let new_key = rows.get(&key).map_or(new_key, |before| before.new_key);
        let row = StagedRow {
            bytes: size_of::<Row>() as u64
                + held
                + values.as_ref().map_or(0, |v| v.len()) as u64
                    * (size_of::<Cell>() - size_of::<Value>()) as u64,
            written: None,
            values,
            left,
            compact,
            new_key,
        };
        let before = rows.insert(key.clone(), row);
        self.remember((Undo::Row(table.0, key, before, self.operation), bytes))?;
        Ok(())
    }

    /// Stages an index entry pointing at a row.
    pub(crate) fn put_index(&mut self, index: IndexId, entry: Entry, row: Key) -> Result<()> {
        let joined = (entry.key.0.len(), entry.cut);
        self.stage_entry(index, entry.key.joined(&row), Some(joined))
    }

    /// Stages the removal of an index entry.
    pub(crate) fn delete_index(&mut self, index: IndexId, entry: &Key, row: &Key) -> Result<()> {
        self.stage_entry(index, entry.joined(row), None)
    }

    fn stage_entry(&mut self, index: IndexId, joined: Key, entry: Option<Joined>) -> Result<()> {
        let bytes = self.counted(&joined, 0)?;
        let entries = self.index.entry(index.0).or_default();
        let before = entries.insert(joined.clone(), entry);
        self.remember((Undo::Entry(index.0, joined, before), bytes))?;
        Ok(())
    }

    /// Stages a change to a table's kept row count.
    pub(crate) fn add_count(&mut self, table: TableId, delta: i64) -> Result<()> {
        self.write_room(STAGED_BYTES)?;
        let before = self.counts.get(&table.0).copied();
        *self.counts.entry(table.0).or_default() += delta;
        self.remember((Undo::Count(table.0, before), STAGED_BYTES))?;
        Ok(())
    }

    /// Stages a schema record under a name, new or in place of the record with that name.
    pub(crate) fn put_schema(&mut self, name: &str, record: Vec<u8>) -> Result<()> {
        self.stage_schema(name, Some(record))
    }

    /// Stages the removal of the schema record under a name.
    pub(crate) fn delete_schema(&mut self, name: &str) -> Result<()> {
        self.stage_schema(name, None)
    }

    fn stage_schema(&mut self, name: &str, record: Option<Vec<u8>>) -> Result<()> {
        let held = record.as_ref().map_or(0, Vec::len);
        let bytes = STAGED_BYTES + 2 * name.len() as u64 + held as u64;
        self.write_room(bytes)?;
        let before = self.schema.insert(name.to_owned(), record);
        self.remember((Undo::Schema(name.to_owned(), before), bytes))?;
        Ok(())
    }

    /// Whether this transaction erased the schema identity, even if it recreated it.
    pub(crate) fn erased_schema(&self, name: &str) -> bool {
        let gone = |(u, _): &(Undo, _)| matches!(u, Undo::Schema(k, Some(None)) if k == name);
        self.schema.get(name) == Some(&None) || self.calls.iter().any(gone)
    }

    /// How many rows of tables that declare no primary key this change set has made, the one
    /// just made among them: with the transaction's number, the key such a row is kept under.
    pub(crate) fn next_row_number(&mut self) -> u64 {
        self.made += 1;
        self.made
    }

    /// Stages a row guard: the values the cells at these positions of a row, as this
    /// transaction shows it, must still hold when the transaction is committed. Each position
    /// is turned into the cell's position in the stored row, undoing, last first, the drops of
    /// cells this transaction has staged for the table before the guard. It writes nothing,
    /// so it is no staging call and no rewind takes it back; [`ChangeSet::guards`] hands it to
    /// the rule check at commit.
    pub(crate) fn guard_row(
        &mut self,
        table: TableId,
        key: Key,
        mut columns: Vec<(usize, Value)>,
    ) -> Result<()> {
        for (at, _) in &mut columns {
            for dropped in self.dropped.iter().rev() {
                if let Dropped::Cell(of, slot) = *dropped
                    && of == table
                    && slot <= *at
                {
                    *at += 1;
                }
            }
        }
        let held = columns.iter().map(|(_, value)| held(value)).sum();
        self.reserved.grow(staged_bytes(&key, held))?;
        self.guards.push(RowGuard {
            table,
            key,
            columns,
        });
        Ok(())
    }

    /// Marks a table whose replaced rows `write` keeps as versions; see `keeps`.
    pub(crate) fn keep_versions(&mut self, table: TableId, removed: bool) -> Result<()> {
        if self.keeps.insert(table.0, removed).is_none() {
            self.held.grow(64)?;
        }
        Ok(())
    }

    /// Every staged row guard, for the rule check at commit.
    pub(crate) fn guards(&self) -> &[RowGuard] {
        &self.guards
    }

    /// Records the number the transaction took when it began. Its commit is written under
    /// that number when no commit has taken it or a higher one since, and under the next
    /// number after the newest commit otherwise.
    pub(crate) fn set_number(&mut self, number: TxId) {
        self.number = number;
    }

    pub(crate) fn changed_setting(&self, at: usize) -> Option<u64> {
        self.numbers.get(&(at as u64 + 3)).copied()
    }
    pub(crate) fn put_setting(&mut self, at: usize, value: u64) -> Result<()> {
        self.put_number(at as u64 + 3, value)
    }

    /// Stages a record of the vector part, new or in place of the record with that key. The
    /// value's bytes are charged to the change set's reservation until it is committed or
    /// dropped.
    pub(crate) fn put_vector(&mut self, record: VectorRecord<'_>, value: Vec<u8>) -> Result<()> {
        self.stage_vector(record, Some(value))
    }

    /// Stages the removal of a record of the vector part. Removing a record that is not there
    /// changes nothing.
    pub(crate) fn delete_vector(&mut self, record: VectorRecord<'_>) -> Result<()> {
        self.stage_vector(record, None)
    }

    fn stage_vector(&mut self, record: VectorRecord<'_>, value: Option<Vec<u8>>) -> Result<()> {
        let key = Key(keys::vector(record));
        let held = value.as_ref().map_or(0, Vec::len);
        let bytes = self.counted(&key, held as u64)?;
        self.stage_record(key, value, bytes)?;
        Ok(())
    }

    fn stage_record(&mut self, key: Key, value: Option<Vec<u8>>, bytes: u64) -> Result<()> {
        let before = self.vectors.insert(key.clone(), value);
        self.remember((Undo::Vector(key, before), bytes))
    }

    /// A named runtime cursor uses the same charged record journal as delivery.
    pub(crate) fn put_schedule(&mut self, name: &str, due: Option<u64>) -> Result<()> {
        let bytes = STAGED_BYTES + 2 * (name.len() + 1) as u64 + 8;
        self.write_room(bytes)?;
        let key = Key([&[SCHEDULE][..], name.as_bytes()].concat());
        self.stage_record(key, due.map(|n| n.to_le_bytes().to_vec()), bytes)?;
        Ok(())
    }

    pub(crate) fn put_audit(&mut self, (own, wait): &AuditQueue) -> Result<()> {
        if own.is_empty() && wait.is_empty() {
            return Ok(());
        }
        let head = self.base.moment.number(disk::LAST_AUDIT)?;
        let skip = (own.len() + wait.len()).saturating_sub(1024);
        let mut count = 0;
        for entry in wait.iter().chain(own).skip(skip) {
            let value = serde_json::to_vec(entry).expect("an audit entry encodes");
            let slot = ((head + count) % 1024) as u16;
            let key = Key([&[AUDIT][..], &slot.to_be_bytes()].concat());
            let bytes = self.counted(&key, value.len() as u64)?;
            self.stage_record(key, Some(value), bytes)?;
            count += 1;
        }
        self.put_number(disk::LAST_AUDIT, head + count)
    }

    /// Builds pending bytes under the journal's charge. Outer None skips; inner None removes.
    /// A new action takes its identity only after the route matched.
    pub(crate) fn put_delivery(
        &mut self,
        action: Option<u64>,
        row: Option<(TableId, &Key)>,
        build: impl FnOnce(
            &View,
            Option<StagedValues<'_>>,
            &mut Reservation,
        ) -> Result<Option<Option<Vec<u8>>>>,
    ) -> Result<bool> {
        let mark = self.reserved.bytes();
        self.write_room(STAGED_BYTES + 18)?;
        let staged = row
            .and_then(|(table, key)| self.rows.get(&table.0)?.get(key))
            .and_then(|row| Some((row.values.as_deref()?, row.left.as_slice())));
        let result = build(&self.base, staged, &mut self.reserved);
        match result {
            Ok(Some(value)) => {
                let bytes = self.reserved.bytes() - mark;
                let action = match action {
                    Some(action) => action,
                    None => {
                        let at = disk::LAST_ACTION;
                        let base = self.base.last_action()?;
                        let next = self.numbers.get(&at).copied().unwrap_or(base) + 1;
                        self.put_number(at, next)?;
                        next
                    }
                };
                let key = Key([&[DELIVERY][..], &action.to_be_bytes()].concat());
                self.stage_record(key, value, bytes)?;
                Ok(true)
            }
            other => {
                self.reserved.shrink(self.reserved.bytes() - mark);
                other.map(|_| false)
            }
        }
    }

    /// Exactly the number disk::write stores, after set_number and rebase.
    pub(crate) fn commit_number(&self) -> TxId {
        TxId(self.number.0.max(self.base.commit().0 + 1))
    }

    /// Stages one of the numbers kept for the vector part.
    pub(crate) fn put_vector_number(&mut self, number: VectorNumber, value: u64) -> Result<()> {
        self.put_number(disk::vector_number_at(number), value)
    }

    /// Stages a number at its physical key, also used to preserve export metadata.
    fn put_number(&mut self, at: u64, value: u64) -> Result<()> {
        // The fixed setting inventory fits in the opening allowance, as MEMORY_LIMIT did.
        let bytes = if (3..3 + crate::settings::SETTINGS.len() as u64).contains(&at) {
            0
        } else {
            STAGED_BYTES
        };
        self.write_room(bytes)?;
        let before = self.numbers.insert(at, value);
        self.remember((Undo::Number(at, before), bytes))
    }

    /// Counts one staging call against the budget before anything is kept: its key twice, once
    /// in the map and once in the list of calls, and what its values hold. Returns the bytes.
    fn counted(&mut self, key: &Key, held: u64) -> Result<u64> {
        let bytes = staged_bytes(key, held);
        self.write_room(bytes)?;
        Ok(bytes)
    }

    pub(crate) fn stamp_table(&mut self, table: TableId, written: u32) {
        let rows = self.rows.get_mut(&table.0).into_iter().flatten();
        for (_, row) in rows {
            row.written = row.values.is_some().then_some(written);
        }
    }

    /// Every staged row, for the rule check at commit. A large value the row left where it is
    /// was not read: it is NULL among the values and its cell is handed over beside them.
    pub(crate) fn rows_changed(&self) -> StagedRows<'_> {
        Box::new(self.rows.iter().flat_map(|(table, staged)| {
            staged.iter().map(|(key, row)| {
                let staged = row.values.as_deref().map(|values| (values, &*row.left));
                (TableId(*table), key, staged, row.new_key)
            })
        }))
    }

    /// Rows touched since a mark, with their current values and staging operation.
    pub(crate) fn rows_changed_since(
        &self,
        mark: usize,
    ) -> impl Iterator<Item = (TableId, &Key, StagedValues<'_>, TriggerEvent)> {
        (mark..self.calls.len()).filter_map(move |i| match &self.calls[i].0 {
            Undo::Row(t, k, _, Some(op)) => {
                let r = self.rows.get(t)?.get(k)?;
                self.calls[i + 1..]
                    .iter()
                    .all(|(u, _)| !matches!(u, Undo::Row(a, b, _, None) if a == t && b == k))
                    .then_some((TableId(*t), k, (r.values.as_deref()?, &*r.left), *op))
            }
            _ => None,
        })
    }

    pub(crate) fn compact_pages(
        &mut self,
        from: Option<Vec<u8>>,
        allowance: Option<usize>,
        drain: bool,
    ) -> Arc<AtomicU64> {
        let outcome = Arc::default();
        self.compaction = Some((from, allowance, drain, Arc::clone(&outcome)));
        outcome
    }

    /// The charge of a commit notice naming `tables` tables, taken from what the write holds.
    pub(crate) fn summary_charge(&mut self, tables: u64) -> Option<Reservation> {
        (tables > 0).then(|| self.held.split(128 + TABLE_SUMMARY * tables))
    }
    /// True when nothing is staged to be written; a row guard writes nothing. The commit
    /// function asks before it commits, so that a transaction that changed nothing writes
    /// nothing and spends no commit number.
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty() && !self.written && self.compaction.is_none()
    }

    /// A stored row of `table` as it is once the cells this change set drops are gone, so that
    /// a row is read by the same places inside the transaction as after its commit.
    fn shaped(&self, table: TableId, mut row: Row) -> Row {
        for dropped in &self.dropped {
            if let Dropped::Cell(of, slot) = *dropped
                && of == table
                && slot < row.cells.len()
            {
                row.cells.remove(slot);
            }
        }
        row
    }
}

/// Puts a staged record back as it was before one staging call.
fn put_back<K: Ord, C>(records: &mut BTreeMap<K, C>, key: K, before: Option<C>) {
    match before {
        Some(before) => drop(records.insert(key, before)),
        None => drop(records.remove(&key)),
    }
}

/// What a value is counted as while a transaction holds it: its cell and what it takes outside.
fn held(value: &Value) -> u64 {
    size_of::<Value>() as u64 + heap_bytes(value)
}

/// The staged records of one table or index inside a span, in key order or, `back`, the reverse.
fn staged_in<C>(
    staged: &Staged<C>,
    id: u32,
    (start, end): Span,
    back: bool,
) -> Box<dyn Iterator<Item = (&Key, &C)> + '_> {
    let records = staged.get(&id);
    let records = records.map(|records| records.range(Key(start)..Key(end)));
    let records = records.into_iter().flatten();
    if back {
        Box::new(records.rev())
    } else {
        Box::new(records)
    }
}

/// Stored records with a transaction's staged changes laid over them, in key order or, `back`,
/// both given and merged from the last key down: `laid` gives what a key holds from its stored
/// record, if any, and its staged change, and a key it gives nothing for is hidden.
fn overlay<'a, T: 'a, C: 'a>(
    stored: impl Iterator<Item = Result<(Key, T)>> + 'a,
    staged: impl Iterator<Item = (&'a Key, C)> + 'a,
    back: bool,
    mut laid: impl FnMut(Option<T>, C) -> Result<Option<T>> + 'a,
) -> Box<dyn Iterator<Item = Result<(Key, T)>> + 'a> {
    let (mut stored, mut staged) = (stored.peekable(), staged.peekable());
    Box::new(std::iter::from_fn(move || {
        loop {
            let stored_is = match (stored.peek(), staged.peek()) {
                (None, None) => return None,
                (Some(Ok((key, _))), Some((staged_key, _))) if back => {
                    key.cmp(staged_key).reverse()
                }
                (Some(Ok((key, _))), Some((staged_key, _))) => key.cmp(staged_key),
                // A failed read, or nothing staged from here on.
                (Some(_), _) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
            };
            let before = match stored_is {
                Ordering::Less => return stored.next(),
                Ordering::Equal => stored.next()?.ok().map(|(_, record)| record),
                Ordering::Greater => None,
            };
            let (key, change) = staged.next()?;
            match laid(before, change) {
                Ok(Some(record)) => return Some(Ok((key.clone(), record))),
                Err(error) => return Some(Err(error)),
                Ok(None) => {}
            }
        }
    }))
}

/// A staged row as a read returns it, `None` for a removal. Its values are in memory already,
/// so none is beside but those it left where they are.
fn shown(row: &StagedRow, cover: &mut Passing) -> Result<Option<Row>> {
    let Some(values) = &row.values else {
        return Ok(None);
    };
    let bytes = row.bytes;
    cover.cover(bytes)?;
    let mut cells: Vec<Cell> = values.iter().cloned().map(Cell::Here).collect();
    for (slot, kept) in &row.left {
        cells[*slot] = Cell::Beside(kept.clone());
    }
    let written = row.written;
    Ok(Some(Row {
        cells,
        written,
        bytes,
    }))
}

/// Parts an index entry from the key of its row.
fn split(joined: Result<(Key, Joined)>) -> Result<(Key, Key, Option<usize>)> {
    let (Key(mut entry), (length, cut)) = joined?;
    let row = entry.split_off(length);
    Ok((Key(entry), Key(row), cut))
}

/// One open store folder, held either for writing or for reading.
/// The page maps' memory, and the record that a failed growth could not be cut back.
type MapCover = Arc<(Mutex<Passing>, std::sync::atomic::AtomicBool)>;

pub(crate) struct Store {
    held: HeldFor,
    maps: MapCover,
    folder: Arc<Path>,
    /// The budget the page cache is reserved from.
    budget: Budget,
    _cache: Reservation,
    marks: Marks,
}

/// How a store is held. The storage library's handle comes first in each, so the data file is
/// let go before the lock on the folder is.
enum HeldFor {
    Writing {
        data: Arc<redb::Database>,
        pins: Pins,
        _locks: [Locked; 2],
        created: bool,
    },
    Reading {
        data: Arc<redb::ReadOnlyDatabase>,
        fixing: Mutex<Fixing>,
    },
}

/// One shared token per pinned writer commit; weak entries do not prolong a view's life.
type Pins = Arc<Mutex<BTreeMap<u64, Weak<AtomicBool>>>>;
/// A writer's moment holds its pin, the data file, and the IVFs its process met damaged.
type Pin = (Arc<AtomicBool>, Arc<redb::Database>, Marks);
pub(super) type Marks = Arc<Mutex<BTreeSet<(VectorId, u32)>>>;

/// What a reading store fixes a view with: the `readers/` folder, open as the short lock, the
/// file its newest view left there, or the store's lock held shared when it may leave none.
struct Fixing {
    readers: PathBuf,
    gate: File,
    hold: Weak<Hold>,
    shared: Option<Locked>,
}

/// A read of the storage library and, in a reading process, the file that holds its commit.
type Fixed = (ReadTransaction, Option<Arc<Hold>>, Option<Pin>);

impl Fixing {
    /// Pins every commit while verifying outside the short lock, then fixes the chosen view.
    fn fixed(&mut self, data: &redb::ReadOnlyDatabase, alone: bool) -> Stored<Fixed> {
        if self.shared.is_some() {
            return Ok((data.begin_read()?, None, None));
        }
        let pinned = self.pin()?;
        let read = data.begin_read()?;
        self.gate.lock_shared()?;
        let fixed = (|| {
            if !pinned.valid()? {
                return Err(Error::AuthoritativeErasure.into());
            }
            self.fix(read, alone)
        })();
        self.gate.unlock()?;
        fixed
    }

    /// Leaves a file holding every commit, under the short lock, before a commit is checked.
    fn pin(&self) -> Stored<Hold> {
        self.gate.lock_shared()?;
        let pinned = Hold::leave(&self.readers, 0);
        self.gate.unlock()?;
        Ok(pinned?)
    }

    /// Makes sure a file names the verified commit. The caller holds the short lock.
    fn fix(&mut self, read: ReadTransaction, alone: bool) -> Stored<Fixed> {
        let commit = read.commit_id();
        let hold = match self.hold.upgrade() {
            Some(hold) if hold.commit == commit && !alone => hold,
            _ => {
                let hold = Arc::new(Hold::leave(&self.readers, commit)?);
                if !alone {
                    self.hold = Arc::downgrade(&hold);
                }
                hold
            }
        };
        Ok((read, Some(hold), None))
    }
}

impl Store {
    pub(crate) fn valid(&self) -> Result<()> {
        if let HeldFor::Writing { data, .. } = &self.held {
            data.check_io_errors()
                .map_err(|e| disk::failed(&self.folder, "read", e))?;
        }
        folder::sound(&self.maps).map_err(|e| disk::failed(&self.folder, "read", e))
    }

    /// The storage library's own close; a view still held closes it when dropped, unheard.
    pub(crate) fn close(self) -> Result<()> {
        let HeldFor::Writing { data, .. } = self.held else {
            return Ok(());
        };
        let closed = Arc::into_inner(data).map_or(Ok(()), redb::Database::close);
        closed.map_err(|e| disk::failed(&self.folder, "close", e))
    }

    pub(crate) fn created(&self) -> bool {
        matches!(&self.held, HeldFor::Writing { created: true, .. })
    }

    /// The file's cache, for a store held for reading.
    pub(crate) fn pages(&self) -> Option<Arc<redb::ReadOnlyDatabase>> {
        match &self.held {
            HeldFor::Reading { data, .. } => Some(data.clone()),
            HeldFor::Writing { .. } => None,
        }
    }
    pub(crate) fn writer_running(&self) -> Result<bool> {
        folder::writer_running(&self.folder)
            .map_err(|e| disk::failed(&self.folder, "writer status", e))
    }

    /// Opens the folder for writing, creating the store if the folder is missing or empty.
    ///
    /// Waits for the folder's `lock` and holds it for the life of the value.
    /// Clears what a killed reader or writer left behind.
    ///
    /// Reserves the page cache from `budget`: the cache the budget's limit calls for, and with
    /// no limit there the cache that the limit the store remembers calls for. The caller sets
    /// the remembered limit on the budget afterwards, and the cache fits under it.
    pub(crate) fn open_for_writing(
        folder: &Path,
        budget: &Budget,
        options: &DatabaseOpenOptions,
    ) -> Result<Store> {
        folder::writing_with_cache(folder, budget, options, cache_under(budget.limit(), None))
    }

    /// Opens the folder for reading. Never creates anything: refuses with `StoreMissing` when
    /// there is no store. At open, waits for an owning writer to pass the readers gate; a
    /// writer waiting for store ownership does not hold readers back. Existing sessions keep
    /// reading, and each view holds the gate only while choosing and registering its commit.
    /// Reserves the page cache that the budget's limit calls for, not the remembered limit.
    pub(crate) fn open_for_reading(
        folder: &Path,
        budget: &Budget,
        options: &ReadSessionOptions,
    ) -> Result<Store> {
        let cache = cache_under(budget.limit(), options.limits.cache_share);
        let [_, bytes, _, check] = crate::memory::account(budget.limit(), cache, None);
        let reserved = budget.reserve(bytes + check, CACHE_NEEDED)?;
        let maps = Arc::new((Mutex::new(reserved.passing()), Default::default()));
        let held = HeldFor::reading(folder, options, cache, &maps)
            .map_err(|error| disk::failed(folder, "open", error))?;
        let Some(held) = held else {
            return Err(Error::StoreMissing {
                path: folder.to_path_buf(),
            });
        };
        Ok(Store {
            held,
            maps,
            folder: folder.into(),
            budget: budget.clone(),
            _cache: reserved,
            marks: Marks::default(),
        })
    }

    /// Refuses a ceiling below cache, transactions and answers, naming the smallest limit.
    /// MemoryBudgetExceeded/set_limit leaves the prior ceiling intact. Cache size changes at
    /// next open; NONE is accepted, and the caller persists declarations.
    pub(crate) fn count_cache_under(&self, limit: Option<u64>) -> Result<()> {
        let smallest = self.budget.used();
        match limit {
            Some(limit) if limit < smallest => Err(Error::MemoryBudgetExceeded {
                subsystem: "store",
                operation: "set_limit",
                requested_bytes: smallest,
                available_bytes: limit,
                budget_limit_bytes: limit,
                hint: "the bytes needed are the smallest limit this session can take, because \
                       its page cache keeps the size it was opened with; for a lower limit, \
                       open the store with the limit as an open option, which sizes the \
                       page cache from the limit, and declare the limit in that session for \
                       the store to remember it",
            }),
            _ => Ok(()),
        }
    }

    /// The newest committed moment; writing views count their pins in memory until dropped.
    ///
    /// On a reading store this takes the short shared lock, reads which commit is newest and
    /// leaves the reader's file naming it; a view of the commit this store already holds costs
    /// no file work.
    pub(crate) fn view(&self) -> Result<View> {
        self.view_on(&self.budget, false)
    }

    /// A view whose table roots are drawn on `standing` first and on fresh room beyond it.
    pub(crate) fn view_on(&self, standing: &Budget, alone: bool) -> Result<View> {
        let fixed: Stored<Fixed> = match &self.held {
            HeldFor::Writing { data, pins, .. } => (|| -> Stored<Fixed> {
                let mut held = locked(pins);
                let read = data.begin_read()?;
                let commit = read.commit_id();
                let pin = held
                    .get(&commit)
                    .and_then(Weak::upgrade)
                    .unwrap_or_else(|| {
                        let pin = Arc::new(AtomicBool::new(false));
                        held.retain(|_, pin| pin.strong_count() != 0);
                        held.insert(commit, Arc::downgrade(&pin));
                        pin
                    });
                Ok((read, None, Some((pin, data.clone(), self.marks.clone()))))
            })(),
            HeldFor::Reading { data, fixing } => {
                let mut fixing = fixing.lock().unwrap_or_else(PoisonError::into_inner);
                fixing.fixed(data, alone)
            }
        };
        // The short lock has gone before a page of the moment is read.
        let moment = fixed.and_then(|(read, hold, pin)| {
            let (fresh, named) = (&self.budget, "pinned view");
            let roots = (standing.reserve(0, named)?, fresh.reserve(0, named)?);
            Moment::open(Tables::Read(&read, roots), self.folder.clone(), hold, pin)
        });
        let moment = moment.map_err(|error| disk::failed(&self.folder, "read", error))?;
        Ok(View {
            moment: Arc::new(moment),
            read: None,
        })
    }

    pub(crate) fn cut_off(&self, before: Option<&View>) -> Result<()> {
        let before = before.map_or(0, |view| view.moment.physical);
        if let HeldFor::Writing { pins, .. } = &self.held {
            for (_, pin) in locked(pins).range(..before) {
                if let Some(pin) = pin.upgrade() {
                    pin.store(true, AtomicOrdering::Release);
                }
            }
        }
        let gate = File::open(self.folder.join("readers"))
            .map_err(|e| disk::failed(&self.folder, "purge", e))?;
        gate.lock()
            .map_err(|e| disk::failed(&self.folder, "purge", e))?;
        let result = folder::cut_off(&self.folder.join("readers"), before);
        let unlock = gate.unlock();
        result
            .and(unlock)
            .map_err(|e| disk::failed(&self.folder, "purge", e))
    }

    /// Copies records through bounded commits, then closes the destination before reporting.
    pub(crate) fn copy_view(view: &View, dest: &Path, budget: &Budget) -> Result<(u64, u64)> {
        let store = folder::writing_with_cache(dest, budget, &Default::default(), 0)?;
        let gate = File::open(view.folder().join("readers"))
            .map_err(|e| disk::failed(dest, "export", e))?;
        gate.lock_shared()
            .map_err(|e| disk::failed(dest, "export", e))?;
        view.valid()?;
        let records = view.copy_to(&store, budget);
        gate.unlock().map_err(|e| disk::failed(dest, "export", e))?;
        let records = records?;
        drop(store);
        let bytes = fs::metadata(dest.join("data"))
            .map_err(|error| disk::failed(dest, "export", error))?
            .len();
        Ok((records, bytes))
    }

    /// Allocator space and readers of an older physical commit; no data-tree walk.
    pub(crate) fn compaction_space(&self, allowance: &mut usize) -> Result<Space> {
        self.compaction(|t| Ok(t.compaction_space(allowance)?))
    }

    pub(crate) fn compaction_position(&self, moved: bool) -> Result<(Option<Vec<u8>>, bool)> {
        self.compaction(|t| Ok(t.compaction_position(moved)?))
    }

    pub(crate) fn has_free_tail(&self) -> Result<bool> {
        self.compaction(|t| Ok(t.has_free_tail()?))
    }

    pub(crate) fn compaction_backlog(&self, through: Option<u64>) -> Result<(u64, u64, u64)> {
        self.compaction(|t| Ok(t.compaction_drain_backlog(through)?))
    }

    pub(crate) fn compaction_state(&self) -> Result<(u64, u64, u64, bool)> {
        self.compaction(|t| Ok(t.compaction_state()?))
    }

    fn compaction<T>(&self, f: impl FnOnce(&redb::WriteTransaction) -> Stored<T>) -> Result<T> {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!()
        };
        let space = || -> Stored<_> { f(&data.begin_write()?) };
        space().map_err(|e| disk::failed(&self.folder, "compaction", e))
    }

    /// Writes a change set as one commit and waits for the disk.
    ///
    /// Whatever is dropped whole goes after the staged records, and takes with it those staged
    /// under its number: every row of a table with its large values, every entry of an index,
    /// or one cell of every row of a table, a storage page or a row at a time. Rows, the values kept beside them, index entries, counts, schema records, the vector
    /// part's records and numbers and the list of changed keys go in one storage transaction:
    /// after a kill either all of it is there or none. A large text or JSON value is compressed
    /// on its way in, in a buffer counted against the change set's reservation while it is
    /// used. The commit takes the number the change set's transaction took, or the next after
    /// the newest commit when that is higher. The caller holds the commit lock and has laid the change set over the newest
    /// moment; one over an older moment is refused with `StoreIo` and writes nothing. Returns
    /// the commit's number. Only a store opened for writing is given a change set to commit.
    pub(crate) fn commit(
        &self,
        mut changes: impl std::borrow::BorrowMut<ChangeSet>,
    ) -> Result<TxId> {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!("a store opened for reading is given nothing to commit")
        };
        match disk::write(data, changes.borrow_mut()) {
            Ok(commit) => Ok(TxId(commit)),
            Err(error) => Err(disk::failed(&self.folder, "commit", error)),
        }
    }
}
