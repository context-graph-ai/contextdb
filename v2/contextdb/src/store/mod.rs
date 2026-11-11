//! store: the folder and the data file; how each kind of record is keyed; views of one moment;
//! reader files and locks.
//!
//! It never interprets SQL or a table's rules: it keeps rows, index entries, counts, schema
//! records and the vector part's records under the keys it is given and hands them back exactly
//! as they were written.
//!
//! Two of the things the parts meet at are defined here: [`Read`], the view to read from, and
//! [`ChangeSet`], the change set to write with.

mod disk;
mod folder;
mod keys;
mod record;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, Weak};

pub use redb::StorageBackend;
use redb::{ReadTransaction, ReadableDatabase};

use crate::memory::{Budget, Reservation};
use crate::types::{Error, Result, SortDirection, TxId, Value};
use disk::{Moment, Stored};
use folder::{DataFile, Hold, Locked};
use keys::Span;

/// The size of the page cache of a store opened under this memory limit, in bytes: a quarter
/// of the limit and never less than 8 MiB, which is also its size when there is no limit. It is
/// reserved from the budget when the store is opened and does not change until the store is
/// closed, because the storage library fixes its cache when the data file is opened.
fn cache_under(limit: Option<u64>) -> u64 {
    (limit.unwrap_or(0) / 4).max(8 << 20)
}

/// What a refusal names when the memory limit cannot hold the page cache.
const CACHE_NEEDED: &str = "page cache needed to open the store";

/// A function that wraps a store's data file, so that a host sees or changes every read, write
/// and sync of it.
///
/// It is given in [`crate::DatabaseOpenOptions::storage`] or
/// [`crate::ReadSessionOptions::storage`]; with none given the file is read and written as it
/// is. The library opens and locks the data file itself, calls the function with the open file
/// each time it has opened it, and from then on reads and writes the file only through the
/// [`StorageBackend`] the function returned. That is once for each open, and twice for a writer
/// that names no memory limit over a store that remembers one: the data file is opened to
/// read the limit, and again with the page cache the limit calls for when that is another
/// size. A session opened for reading hands over a file opened for reading only. A host wraps
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
    /// One block of a partition: the partition's number, then the block's.
    Block(VectorId, u32, u32),
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
        keys::encode(values, directions).map(Key)
    }

    /// The bytes of the key, for a part that keeps keys inside a record of its own.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The key that [`Key::as_bytes`] gave these bytes for.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Key {
        Key(bytes.to_vec())
    }

    /// True when this key begins with every value of `prefix`, in order. Every key begins with
    /// the key of no values.
    pub(crate) fn starts_with(&self, prefix: &Key) -> bool {
        self.0.starts_with(&prefix.0)
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
    pub cells: Vec<Cell>,
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

/// Index entries in entry order, each with the key of the row it points at.
pub(crate) type EntryIter<'a> = Box<dyn Iterator<Item = Result<(Key, Key)>> + 'a>;

/// The rows a change set has staged: table, key, the new values or `None` for a delete, and
/// whether the transaction found no row at the key when it first staged it.
pub(crate) type StagedRows<'a> =
    Box<dyn Iterator<Item = (TableId, &'a Key, Option<&'a [Value]>, bool)> + 'a>;

/// The view to read rows from. `query` and `write` read a store only through this.
///
/// A [`View`] answers from one committed moment. A [`ChangeSet`] answers from its own staged
/// changes first and from its view otherwise, so a transaction reads what it has written.
/// Every call on one reader answers from the same moment, however long the reader lives.
pub(crate) trait Read {
    /// The commit this reader shows.
    fn commit(&self) -> TxId;

    /// One row by key. Costs one walk down the tree: the pages on that path, through the cache.
    fn row(&self, table: TableId, key: &Key) -> Result<Option<Row>>;

    /// The rows of a table between two keys, in key order. Pages are read as the iterator
    /// moves, so stopping early costs only what was visited. Nothing is collected.
    ///
    /// A bound made from the first columns of a key stands for every key that begins with
    /// those values: included, it takes them all in; excluded, it leaves them all out.
    fn rows(&self, table: TableId, from: Bound<&Key>, to: Bound<&Key>) -> Result<RowIter<'_>>;

    /// The entries of an index between two entries, in entry order. Costs like [`Read::rows`],
    /// and a bound of fewer columns means what it means there: it is never taken as one exact
    /// key. A bound applies to the entry alone, so an entry equal to an included bound is in
    /// range whatever row it points at.
    fn index(&self, index: IndexId, from: Bound<&Key>, to: Bound<&Key>) -> Result<EntryIter<'_>>;

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
    /// Vector records are staged only inside the commit function and by upkeep, after the last
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
    moment: Arc<Moment>,
}

/// The change set to write with: everything one transaction means to change, held in memory
/// until it is committed or dropped.
///
/// Its memory is charged to the budget it was made with, and every `put` refuses with
/// `MemoryBudgetExceeded` rather than grow past the limit. Dropping it discards the changes.
/// `write` decides which index entries and count changes go with a row; this type records them
/// and nothing more.
pub(crate) struct ChangeSet {
    base: View,
    /// Staged rows by table and key.
    rows: Staged<StagedRow>,
    /// Staged index entries by index, each entry joined to the key of its row: the length of
    /// the entry, or `None` for a removal.
    index: Staged<Option<usize>>,
    /// The change to each table's kept row count.
    counts: BTreeMap<u32, i64>,
    /// Staged schema records by name.
    schema: BTreeMap<String, Vec<u8>>,
    /// The memory limit the store is to remember, when one was staged: `Some(None)` for none.
    memory_limit: Option<Option<u64>>,
    /// Staged records of the vector part by their stored key: the value, or `None` for a
    /// removal.
    vectors: BTreeMap<Key, Option<Vec<u8>>>,
    /// Staged numbers of the vector part by their place among the store's numbers.
    vector_numbers: BTreeMap<u64, u64>,
    /// Every staging call of a statement in order, each with what it put aside and the bytes it
    /// was counted as, so that the calls after a mark can be undone.
    calls: Vec<(Undo, u64)>,
    reserved: Reservation,
}

/// Staged records of several tables or indexes, by number and then by key.
type Staged<C> = BTreeMap<u32, BTreeMap<Key, C>>;

struct StagedRow {
    /// The new values, or `None` for a removal.
    values: Option<Vec<Value>>,
    /// What the transaction said when it first staged this key: it found no row there.
    new_key: bool,
}

/// What one staging call put aside: the record it changed and what was staged there before,
/// `None` when nothing was.
enum Undo {
    Row(u32, Key, Option<StagedRow>),
    Entry(u32, Key, Option<Option<usize>>),
    Count(u32, i64),
    Schema(String, Option<Vec<u8>>),
    Vector(Key, Option<Option<Vec<u8>>>),
    VectorNumber(u64, Option<u64>),
}

/// What one staging call is counted as beside its key and its values: its place in the map
/// and in the list of calls.
const STAGED_BYTES: u64 = 128;

impl ChangeSet {
    /// An empty change set over the moment its transaction began.
    pub(crate) fn new(base: View, budget: &Budget) -> Result<ChangeSet> {
        Ok(ChangeSet {
            base,
            rows: BTreeMap::new(),
            index: BTreeMap::new(),
            counts: BTreeMap::new(),
            schema: BTreeMap::new(),
            memory_limit: None,
            vectors: BTreeMap::new(),
            vector_numbers: BTreeMap::new(),
            calls: Vec::new(),
            reserved: budget.reserve(0, "transaction")?,
        })
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
                Undo::Row(table, key, before) => {
                    put_back(self.rows.entry(table).or_default(), key, before);
                }
                Undo::Entry(index, joined, before) => {
                    put_back(self.index.entry(index).or_default(), joined, before);
                }
                Undo::Count(table, delta) => *self.counts.entry(table).or_default() -= delta,
                Undo::Schema(name, before) => put_back(&mut self.schema, name, before),
                Undo::Vector(key, before) => put_back(&mut self.vectors, key, before),
                Undo::VectorNumber(at, before) => put_back(&mut self.vector_numbers, at, before),
            }
            self.reserved.shrink(bytes);
        }
    }

    /// Stages a row, new or replacing the row with this key. Large text, JSON and vector values
    /// are kept beside the row when it is written; the caller does not split them.
    ///
    /// `new_key` says the transaction found no row at this key. The answer given the first
    /// time a key is staged is the one kept, and [`ChangeSet::rows_changed`] hands it back.
    pub(crate) fn put_row(
        &mut self,
        table: TableId,
        key: Key,
        values: Vec<Value>,
        new_key: bool,
    ) -> Result<()> {
        self.stage_row(table, key, Some(values), new_key)
    }

    /// Stages the removal of a row and of the values kept beside it. A key first staged here
    /// is one a row was at.
    pub(crate) fn delete_row(&mut self, table: TableId, key: &Key) -> Result<()> {
        self.stage_row(table, key.clone(), None, false)
    }

    fn stage_row(
        &mut self,
        table: TableId,
        key: Key,
        values: Option<Vec<Value>>,
        new_key: bool,
    ) -> Result<()> {
        let held: u64 = values.iter().flatten().map(value_bytes).sum();
        let bytes = self.counted(&key, held)?;
        let rows = self.rows.entry(table.0).or_default();
        let new_key = rows.get(&key).map_or(new_key, |before| before.new_key);
        let before = rows.insert(key.clone(), StagedRow { values, new_key });
        self.calls.push((Undo::Row(table.0, key, before), bytes));
        Ok(())
    }

    /// Stages an index entry pointing at a row.
    pub(crate) fn put_index(&mut self, index: IndexId, entry: Key, row: Key) -> Result<()> {
        let length = entry.0.len();
        self.stage_entry(index, Key([entry.0, row.0].concat()), Some(length))
    }

    /// Stages the removal of an index entry.
    pub(crate) fn delete_index(&mut self, index: IndexId, entry: &Key, row: &Key) -> Result<()> {
        self.stage_entry(index, Key([entry.0.as_slice(), &row.0].concat()), None)
    }

    fn stage_entry(&mut self, index: IndexId, joined: Key, entry: Option<usize>) -> Result<()> {
        let bytes = self.counted(&joined, 0)?;
        let entries = self.index.entry(index.0).or_default();
        let before = entries.insert(joined.clone(), entry);
        self.calls
            .push((Undo::Entry(index.0, joined, before), bytes));
        Ok(())
    }

    /// Stages a change to a table's kept row count.
    pub(crate) fn add_count(&mut self, table: TableId, delta: i64) -> Result<()> {
        self.reserved.grow(STAGED_BYTES)?;
        *self.counts.entry(table.0).or_default() += delta;
        self.calls.push((Undo::Count(table.0, delta), STAGED_BYTES));
        Ok(())
    }

    /// Stages a schema record under a name, new or in place of the record with that name.
    pub(crate) fn put_schema(&mut self, name: &str, record: Vec<u8>) -> Result<()> {
        let bytes = STAGED_BYTES + 2 * name.len() as u64 + record.len() as u64;
        self.reserved.grow(bytes)?;
        let before = self.schema.insert(name.to_owned(), record);
        self.calls
            .push((Undo::Schema(name.to_owned(), before), bytes));
        Ok(())
    }

    /// Stages the memory limit the store is to remember from this commit on, in bytes, or
    /// `None` for no limit. No statement of a transaction stages one, so a rewind leaves it.
    pub(crate) fn put_memory_limit(&mut self, limit: Option<u64>) -> Result<()> {
        self.memory_limit = Some(limit);
        Ok(())
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
        let before = self.vectors.insert(key.clone(), value);
        self.calls.push((Undo::Vector(key, before), bytes));
        Ok(())
    }

    /// Stages one of the numbers kept for the vector part.
    pub(crate) fn put_vector_number(&mut self, number: VectorNumber, value: u64) -> Result<()> {
        self.reserved.grow(STAGED_BYTES)?;
        let at = disk::vector_number_at(number);
        let before = self.vector_numbers.insert(at, value);
        self.calls
            .push((Undo::VectorNumber(at, before), STAGED_BYTES));
        Ok(())
    }

    /// Counts one staging call against the budget before anything is kept: its key twice, once
    /// in the map and once in the list of calls, and what its values hold. Returns the bytes.
    fn counted(&mut self, key: &Key, held: u64) -> Result<u64> {
        let bytes = STAGED_BYTES + 2 * key.0.len() as u64 + held;
        self.reserved.grow(bytes)?;
        Ok(bytes)
    }

    /// Every staged row, for the rule check at commit.
    pub(crate) fn rows_changed(&self) -> StagedRows<'_> {
        Box::new(self.rows.iter().flat_map(|(table, staged)| {
            staged
                .iter()
                .map(|(key, row)| (TableId(*table), key, row.values.as_deref(), row.new_key))
        }))
    }

    /// True when nothing is staged. The commit function asks before it commits, so that a
    /// transaction that changed nothing writes nothing and spends no commit number.
    pub(crate) fn is_empty(&self) -> bool {
        self.calls.is_empty() && self.memory_limit.is_none()
    }
}

/// Puts a staged record back as it was before one staging call.
fn put_back<K: Ord, C>(records: &mut BTreeMap<K, C>, key: K, before: Option<C>) {
    match before {
        Some(before) => drop(records.insert(key, before)),
        None => drop(records.remove(&key)),
    }
}

/// What a value is counted as while a transaction holds it.
fn value_bytes(value: &Value) -> u64 {
    let grown = match value {
        Value::Text(text) => text.len() as u64,
        Value::Vector(numbers) => 4 * numbers.len() as u64,
        Value::Json(json) => json_bytes(json),
        _ => 0,
    };
    size_of::<Value>() as u64 + grown
}

fn json_bytes(json: &serde_json::Value) -> u64 {
    let grown = match json {
        serde_json::Value::String(text) => text.len() as u64,
        serde_json::Value::Array(items) => items.iter().map(json_bytes).sum(),
        serde_json::Value::Object(members) => members
            .iter()
            .map(|(name, member)| name.len() as u64 + json_bytes(member))
            .sum(),
        _ => 0,
    };
    size_of::<serde_json::Value>() as u64 + grown
}

/// The staged records of one table or index inside a span, in key order.
fn staged_in<C>(
    staged: &Staged<C>,
    id: u32,
    (start, end): Span,
) -> impl Iterator<Item = (&Key, &C)> {
    let records = staged.get(&id);
    records
        .map(|records| records.range(Key(start)..Key(end)))
        .into_iter()
        .flatten()
}

/// Stored records with a transaction's staged ones laid over them, in key order: a staged
/// record stands in for the stored one with its key, and a staged removal, `None`, hides it.
fn overlay<'a, T: 'a>(
    stored: impl Iterator<Item = Result<(Key, T)>> + 'a,
    staged: impl Iterator<Item = (&'a Key, Option<T>)> + 'a,
) -> Box<dyn Iterator<Item = Result<(Key, T)>> + 'a> {
    let (mut stored, mut staged) = (stored.peekable(), staged.peekable());
    Box::new(std::iter::from_fn(move || {
        loop {
            let stored_is = match (stored.peek(), staged.peek()) {
                (None, None) => return None,
                (Some(Ok((key, _))), Some((staged_key, _))) => key.cmp(staged_key),
                // A failed read, or nothing staged from here on.
                (Some(_), _) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
            };
            match stored_is {
                Ordering::Less => return stored.next(),
                Ordering::Equal => drop(stored.next()),
                Ordering::Greater => {}
            }
            if let Some((key, Some(record))) = staged.next() {
                return Some(Ok((key.clone(), record)));
            }
        }
    }))
}

/// A staged row as a read returns it. Its values are in memory already, so none is beside.
fn shown(values: &[Value]) -> Row {
    Row {
        cells: values.iter().cloned().map(Cell::Here).collect(),
    }
}

/// Parts an index entry from the key of its row.
fn split(joined: Result<(Key, usize)>) -> Result<(Key, Key)> {
    let (Key(mut entry), length) = joined?;
    let row = entry.split_off(length);
    Ok((Key(entry), Key(row)))
}

impl Read for ChangeSet {
    fn commit(&self) -> TxId {
        self.base.commit()
    }

    fn row(&self, table: TableId, key: &Key) -> Result<Option<Row>> {
        match self.rows.get(&table.0).and_then(|staged| staged.get(key)) {
            Some(staged) => Ok(staged.values.as_deref().map(shown)),
            None => self.base.row(table, key),
        }
    }

    fn rows(&self, table: TableId, from: Bound<&Key>, to: Bound<&Key>) -> Result<RowIter<'_>> {
        let span = keys::span(from, to);
        let stored = self.base.moment.rows(table.0, &span)?;
        let staged = staged_in(&self.rows, table.0, span);
        let staged = staged.map(|(key, row)| (key, row.values.as_deref().map(shown)));
        Ok(overlay(stored, staged))
    }

    fn index(&self, index: IndexId, from: Bound<&Key>, to: Bound<&Key>) -> Result<EntryIter<'_>> {
        let span = keys::span(from, to);
        let stored = self.base.moment.joined(index.0, &span)?;
        let staged = staged_in(&self.index, index.0, span).map(|(joined, entry)| (joined, *entry));
        Ok(Box::new(overlay(stored, staged).map(split)))
    }

    fn count(&self, table: TableId) -> Result<u64> {
        let change = self.counts.get(&table.0).copied().unwrap_or(0);
        Ok(self.base.count(table)?.saturating_add_signed(change))
    }

    fn load(&self, beside: &Beside, reservation: &mut Reservation) -> Result<Value> {
        self.base.load(beside, reservation)
    }

    fn staged(&self, table: TableId, each: &mut dyn FnMut(&Key) -> Result<bool>) -> Result<()> {
        for key in self.rows.get(&table.0).into_iter().flat_map(BTreeMap::keys) {
            if !each(key)? {
                break;
            }
        }
        Ok(())
    }

    fn vectors(
        &self,
        from: VectorRecord<'_>,
        each: &mut dyn FnMut(VectorRecord<'_>, &[u8]) -> Result<bool>,
    ) -> Result<()> {
        self.base.vectors(from, each)
    }

    fn vector_number(&self, number: VectorNumber) -> Result<u64> {
        self.base.vector_number(number)
    }

    fn damaged(&self, reason: &str) -> Error {
        self.base.damaged(reason)
    }
}

/// One open store folder, held either for writing or for reading.
pub(crate) struct Store {
    held: HeldFor,
    folder: Arc<Path>,
    /// The budget the page cache is reserved from.
    budget: Budget,
    _cache: Reservation,
}

/// How a store is held. The storage library's handle comes first in each, so the data file is
/// let go before the lock on the folder is.
enum HeldFor {
    Writing {
        data: redb::Database,
        _lock: Locked,
    },
    Reading {
        data: redb::ReadOnlyDatabase,
        fixing: Mutex<Fixing>,
    },
}

/// What a reading store fixes a view with: the `readers/` folder, open as the short lock, and
/// the file its newest view left there.
struct Fixing {
    readers: PathBuf,
    gate: File,
    hold: Weak<Hold>,
}

/// A read of the storage library and, in a reading process, the file that holds its commit.
type Fixed = (ReadTransaction, Option<Arc<Hold>>);

impl Fixing {
    /// Fixes a view of the newest commit, holding the short lock for just that long.
    fn fixed(&mut self, data: &redb::ReadOnlyDatabase) -> Stored<Fixed> {
        self.gate.lock_shared()?;
        let fixed = self.fix(data);
        self.gate.unlock()?;
        fixed
    }

    /// Reads which commit is newest and makes sure a file names it.
    fn fix(&mut self, data: &redb::ReadOnlyDatabase) -> Stored<Fixed> {
        let read = data.begin_read()?;
        let commit = read.commit_id();
        let hold = match self.hold.upgrade() {
            Some(hold) if hold.commit == commit => hold,
            _ => {
                let hold = Arc::new(Hold::leave(&self.readers, commit)?);
                self.hold = Arc::downgrade(&hold);
                hold
            }
        };
        Ok((read, Some(hold)))
    }
}

impl HeldFor {
    /// Takes the folder for writing, or answers with the id of the process that holds it.
    /// With the folder comes the size of the page cache its data file was opened with.
    ///
    /// `limit` is the memory limit of the open's budget, and the cache is the one it calls
    /// for. With none, the limit in force is the one the store remembers, which only the open
    /// data file tells: the file is opened with the cache of no limit and, when the remembered
    /// limit calls for another size, let go and opened again with that size.
    fn writing(
        folder: &Path,
        storage: Option<&WrapStorage>,
        limit: Option<u64>,
    ) -> Stored<std::result::Result<(HeldFor, u64), u32>> {
        fs::create_dir_all(folder)?;
        let lock = match folder::take_lock(folder)? {
            Ok(lock) => lock,
            Err(holder) => return Ok(Err(holder)),
        };
        let readers = folder.join("readers");
        fs::create_dir_all(&readers)?;
        // Removes what readers that are gone left behind.
        folder::oldest_held(&readers, &File::open(&readers)?)?;
        let mut cache = cache_under(limit);
        let mut data = HeldFor::data(folder, storage, cache)?;
        disk::prepare(&data)?;
        if limit.is_none() {
            let remembered = cache_under(disk::remembered_limit(&data)?);
            if remembered != cache {
                drop(data);
                cache = remembered;
                data = HeldFor::data(folder, storage, cache)?;
            }
        }
        Ok(Ok((HeldFor::Writing { data, _lock: lock }, cache)))
    }

    /// Opens the data file of a folder held for writing, with a page cache of `cache` bytes.
    fn data(folder: &Path, storage: Option<&WrapStorage>, cache: u64) -> Stored<redb::Database> {
        let readers = folder.join("readers");
        let gate = File::open(&readers)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(folder.join("data"))?;
        let data = redb::Builder::new()
            .set_cache_size(cache as usize)
            // When the files cannot be looked through, every commit counts as held.
            .set_held_commits_callback(move || {
                folder::oldest_held(&readers, &gate).unwrap_or(Some(0))
            })
            .create_with_backend(DataFile::over(file, storage)?)?;
        Ok(data)
    }

    /// Opens the folder for reading with a page cache of `cache` bytes, or answers `None` when
    /// it has no data file.
    fn reading(
        folder: &Path,
        storage: Option<&WrapStorage>,
        cache: u64,
    ) -> Stored<Option<HeldFor>> {
        let file = match File::open(folder.join("data")) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let data = redb::Builder::new()
            .set_cache_size(cache as usize)
            .open_read_only_with_backend(DataFile::over(file, storage)?)?;
        let readers = folder.join("readers");
        let fixing = Mutex::new(Fixing {
            gate: File::open(&readers)?,
            readers,
            hold: Weak::new(),
        });
        Ok(Some(HeldFor::Reading { data, fixing }))
    }
}

impl Store {
    /// Opens the folder for writing, creating the store if the folder is missing or empty.
    ///
    /// Takes the folder's `lock` for the life of the value and refuses at once with
    /// `DatabaseLocked`, naming the holder, when another process has it. Clears what a killed
    /// reader or writer left behind. Costs a few milliseconds whatever the store's size:
    /// nothing is loaded.
    ///
    /// Reserves the page cache from `budget`: the cache the budget's limit calls for, and with
    /// no limit there the cache that the limit the store remembers calls for. The caller sets
    /// the remembered limit on the budget afterwards, and the cache fits under it.
    pub(crate) fn open_for_writing(
        folder: &Path,
        budget: &Budget,
        storage: Option<&WrapStorage>,
    ) -> Result<Store> {
        let limit = budget.limit();
        let mut cache = budget.reserve(cache_under(limit), CACHE_NEEDED)?;
        let held = HeldFor::writing(folder, storage, limit);
        match held.map_err(|error| disk::failed(folder, "open", error))? {
            Ok((held, opened_with)) => {
                // Another size is opened only with no limit in force, so nothing refuses this.
                if opened_with != cache.bytes() {
                    cache = budget.reserve(opened_with, CACHE_NEEDED)?;
                }
                Ok(Store {
                    held,
                    folder: folder.into(),
                    budget: budget.clone(),
                    _cache: cache,
                })
            }
            Err(holder_pid) => Err(Error::DatabaseLocked {
                holder_pid,
                path: folder.to_path_buf(),
            }),
        }
    }

    /// Opens the folder for reading. Never creates anything: refuses with `StoreMissing` when
    /// there is no store. Works whether or not a writer is running, and never makes a writer
    /// fail; a view holds one up only for the instant it has the short lock. Reserves the page
    /// cache that the budget's limit calls for: a reader does not take the limit the store
    /// remembers.
    pub(crate) fn open_for_reading(
        folder: &Path,
        budget: &Budget,
        storage: Option<&WrapStorage>,
    ) -> Result<Store> {
        let cache = cache_under(budget.limit());
        let held = HeldFor::reading(folder, storage, cache)
            .map_err(|error| disk::failed(folder, "open", error))?;
        let Some(held) = held else {
            return Err(Error::StoreMissing {
                path: folder.to_path_buf(),
            });
        };
        Ok(Store {
            held,
            folder: folder.into(),
            budget: budget.clone(),
            _cache: budget.reserve(cache, CACHE_NEEDED)?,
        })
    }

    /// Checks a memory limit that is about to take hold against the page cache of this open.
    ///
    /// The cache keeps the size it was opened with until the store is closed, and the budget
    /// counts all of it. A declared limit must hold, so the smallest limit this open can take
    /// is what the budget counts now: 1 MiB, the cache, and what open transactions and live
    /// results hold. A limit below that is refused with `MemoryBudgetExceeded`, operation
    /// `set_limit`, and nothing changes: the refusal names the smallest limit as the bytes
    /// needed and says to open the store with the limit as an open option. A limit at or above
    /// it, or none, is accepted: once `database` has set it on the budget it holds at once,
    /// the store remembers it, and it sizes the cache at the next open. `database` calls this
    /// on the one path of `SET MEMORY_LIMIT`, before anything changes.
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

    /// The newest committed moment.
    ///
    /// On a reading store this takes the short shared lock, reads which commit is newest and
    /// leaves the reader's file naming it; a view of the commit this store already holds costs
    /// no file work.
    pub(crate) fn view(&self) -> Result<View> {
        let fixed: Stored<Fixed> = match &self.held {
            HeldFor::Writing { data, .. } => match data.begin_read() {
                Ok(read) => Ok((read, None)),
                Err(error) => Err(error.into()),
            },
            HeldFor::Reading { data, fixing } => {
                let mut fixing = fixing.lock().unwrap_or_else(PoisonError::into_inner);
                fixing.fixed(data)
            }
        };
        // The short lock has gone before a page of the moment is read.
        let moment = fixed.and_then(|(read, hold)| Moment::open(&read, self.folder.clone(), hold));
        let moment = moment.map_err(|error| disk::failed(&self.folder, "read", error))?;
        Ok(View {
            moment: Arc::new(moment),
        })
    }

    /// Writes a change set as one commit and waits for the disk.
    ///
    /// Rows, the values kept beside them, index entries, counts, schema records, the vector
    /// part's records and numbers and the list of changed keys go in one storage transaction:
    /// after a kill either all of it is there or none. A large text or JSON value is compressed
    /// on its way in, in a buffer counted against the change set's reservation while it is
    /// used. The caller holds the commit lock and has laid the change set over the newest
    /// moment; one over an older moment is refused with `StoreIo` and writes nothing. Returns
    /// the commit's number. Only a store opened for writing is given a change set to commit.
    pub(crate) fn commit(&self, mut changes: ChangeSet) -> Result<TxId> {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!("a store opened for reading is given nothing to commit")
        };
        match disk::write(data, &mut changes) {
            Ok(commit) => Ok(TxId(commit)),
            Err(error) => Err(disk::failed(&self.folder, "commit", error)),
        }
    }
}
