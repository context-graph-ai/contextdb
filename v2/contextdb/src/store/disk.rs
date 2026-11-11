//! What the data file holds, how one committed moment of it is read, and how a commit is
//! written into it.
//!
//! Seven tables of the storage library hold everything. Rows and index entries are keyed by
//! the number of their table or index and then their key, so one table's rows are one run of
//! keys. A large value is cut into slices of 243 bytes, each under a place number of its own:
//! the slices of one value are under consecutive places, and its row says how many bytes there
//! are and which place is the first. The vector part's blocks have a table to themselves, and
//! its partition entries and pending entries share another. The last table holds the few
//! numbers the store keeps: the newest commit, the commit that last changed the schema, the
//! next free place, the declared memory limit, each table's row count, and the vector part's
//! three numbers.

use std::io::{self, ErrorKind};
use std::ops::{Bound, Range};
use std::path::Path;
use std::sync::Arc;

use redb::{ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition};

use super::folder::Hold;
use super::keys::{self, Span, stored};
use super::{Beside, Cell, ChangeSet, EntryIter, IndexId, Key, Read, Row, RowIter, TableId, View};
use super::{VectorNumber, VectorRecord};
use super::{record, split};
use crate::memory::Reservation;
use crate::types::{Error, Result, TxId, Value};

/// What a call into the storage library gives, before it is told as the engine tells it.
pub(super) type Stored<T> = std::result::Result<T, redb::Error>;

/// Rows: table number and key, to the row as `record` writes it.
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
/// Index entries: index number, entry and row key, to the length of the entry.
const INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index");
/// Large values: place, to one slice of the record `record` stores for the value.
const BESIDE: TableDefinition<u64, &[u8]> = TableDefinition::new("beside");
/// Schema records by name, as `schema` wrote them.
const SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");
/// The numbers the store keeps.
const NUMBERS: TableDefinition<u64, u64> = TableDefinition::new("numbers");
/// The vector part's blocks, under their keys as `keys::vector` writes them, and nothing else.
/// The storage library rewrites a whole leaf when any record in it changes, and it divides a
/// leaf by bytes, so a small record that shared a leaf with a block of 256 KiB would have the
/// block rewritten every time it changed.
const BLOCKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_blocks");
/// The vector part's partition entries and pending entries, under their keys as `keys::vector`
/// writes them.
const VECTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vectors");

/// The bytes of one slice of a large value; the last slice of a value may be shorter. A leaf
/// of the storage library is 4 bytes and, for each slice, 8 of key, 4 of length and the slice,
/// so sixteen slices fill a page of 4 KiB: 4,084 of its 4,096 bytes.
const SLICE: usize = 243;

const COMMIT: u64 = 0;
const SCHEMA_COMMIT: u64 = 1;
const NEXT_PLACE: u64 = 2;
/// Not there when no limit is declared.
const MEMORY_LIMIT: u64 = 3;

/// Where a table's row count is kept among the numbers.
fn count_of(table: u32) -> u64 {
    (1 << 32) | u64::from(table)
}

/// Where a number of the vector part is kept among the numbers.
pub(super) fn vector_number_at(number: VectorNumber) -> u64 {
    match number {
        VectorNumber::Partitions(column) => (2 << 32) | u64::from(column.0),
        VectorNumber::NextPartition(column) => (3 << 32) | u64::from(column.0),
        VectorNumber::Pending => 4 << 32,
    }
}

/// The places of the slices of a large value: its cell gives them, so nothing is read to
/// find them.
fn places(kept: &Beside) -> Range<u64> {
    kept.place..kept.place + kept.bytes.div_ceil(SLICE as u64)
}

/// Tells a failure of the storage library or of a file as the engine tells it: as damage when
/// stored data failed its check, and otherwise as a failure of `operation`, which is what the
/// store was doing.
pub(super) fn failed(
    folder: &Path,
    operation: &'static str,
    error: impl Into<redb::Error>,
) -> Error {
    let path = folder.to_path_buf();
    match error.into() {
        redb::Error::Corrupted(reason) => Error::StoreCorrupted { path, reason },
        // A read that came back short, or bytes the storage library does not know as its own,
        // are damage too.
        redb::Error::Io(error)
            if matches!(
                error.kind(),
                ErrorKind::UnexpectedEof | ErrorKind::InvalidData
            ) =>
        {
            Error::StoreCorrupted {
                path,
                reason: error.to_string(),
            }
        }
        other => Error::StoreIo {
            operation,
            path,
            reason: other.to_string(),
        },
    }
}

/// What a record that is not one the store wrote is told as: a failed check.
fn unsound() -> redb::Error {
    redb::Error::Corrupted("a stored record is not one the store wrote".to_owned())
}

fn number(numbers: &impl ReadableTable<u64, u64>, key: u64) -> Stored<u64> {
    Ok(numbers.get(key)?.map_or(0, |number| number.value()))
}

/// The memory limit the store remembers, in bytes; `None` when none is declared.
fn memory_limit(numbers: &impl ReadableTable<u64, u64>) -> Stored<Option<u64>> {
    Ok(numbers.get(MEMORY_LIMIT)?.map(|limit| limit.value()))
}

/// The memory limit the newest commit of a data file remembers.
pub(super) fn remembered_limit(data: &redb::Database) -> Stored<Option<u64>> {
    memory_limit(&data.begin_read()?.open_table(NUMBERS)?)
}

/// Makes the tables in a data file that does not have them yet.
pub(super) fn prepare(data: &redb::Database) -> Stored<()> {
    match data.begin_read()?.open_table(NUMBERS) {
        Ok(_) => return Ok(()),
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(other) => return Err(other.into()),
    }
    let write = data.begin_write()?;
    write.open_table(ROWS)?;
    write.open_table(INDEX)?;
    write.open_table(BESIDE)?;
    write.open_table(SCHEMA)?;
    write.open_table(NUMBERS)?;
    write.open_table(BLOCKS)?;
    write.open_table(VECTORS)?;
    write.commit()?;
    Ok(())
}

/// One committed moment: the seven tables as one read of the storage library shows them.
pub(super) struct Moment {
    rows: ReadOnlyTable<&'static [u8], &'static [u8]>,
    index: ReadOnlyTable<&'static [u8], &'static [u8]>,
    beside: ReadOnlyTable<u64, &'static [u8]>,
    schema: ReadOnlyTable<&'static str, &'static [u8]>,
    numbers: ReadOnlyTable<u64, u64>,
    blocks: ReadOnlyTable<&'static [u8], &'static [u8]>,
    vectors: ReadOnlyTable<&'static [u8], &'static [u8]>,
    commit: TxId,
    folder: Arc<Path>,
    /// In a reading process, the file that keeps this moment's pages from being reused.
    _hold: Option<Arc<Hold>>,
}

impl Moment {
    pub(super) fn open(
        read: &ReadTransaction,
        folder: Arc<Path>,
        hold: Option<Arc<Hold>>,
    ) -> Stored<Moment> {
        let numbers = read.open_table(NUMBERS)?;
        Ok(Moment {
            rows: read.open_table(ROWS)?,
            index: read.open_table(INDEX)?,
            beside: read.open_table(BESIDE)?,
            schema: read.open_table(SCHEMA)?,
            blocks: read.open_table(BLOCKS)?,
            vectors: read.open_table(VECTORS)?,
            commit: TxId(number(&numbers, COMMIT)?),
            numbers,
            folder,
            _hold: hold,
        })
    }

    pub(super) fn told<T>(&self, read: Stored<T>) -> Result<T> {
        read.map_err(|error| failed(&self.folder, "read", error))
    }

    /// The records of one table or index inside a span, in key order, each read with `read`.
    fn scan<T, F>(
        &self,
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        id: u32,
        (start, end): &Span,
        read: F,
    ) -> Result<impl Iterator<Item = Result<(Key, T)>> + use<T, F>>
    where
        F: Fn(&[u8]) -> Option<T>,
    {
        let folder = self.folder.clone();
        let range = table
            .range::<&[u8]>(stored(id, start).as_slice()..stored(id, end).as_slice())
            .map_err(|error| failed(&folder, "read", error))?;
        Ok(range.map(move |entry| {
            let (key, record) = entry.map_err(|error| failed(&folder, "read", error))?;
            let read = read(record.value()).ok_or_else(|| failed(&folder, "read", unsound()))?;
            Ok((Key(key.value()[4..].to_vec()), read))
        }))
    }

    /// The rows of a table inside a span.
    pub(super) fn rows(
        &self,
        table: u32,
        span: &Span,
    ) -> Result<impl Iterator<Item = Result<(Key, Row)>> + use<>> {
        self.scan(&self.rows, table, span, |row| {
            Some(Row {
                cells: record::decode_row(row),
            })
        })
    }

    /// The entries of an index inside a span, each still joined to the key of its row, with
    /// the length of the entry.
    pub(super) fn joined(
        &self,
        index: u32,
        span: &Span,
    ) -> Result<impl Iterator<Item = Result<(Key, usize)>> + use<>> {
        self.scan(&self.index, index, span, |length| {
            Some(u32::from_le_bytes(*length.first_chunk()?) as usize)
        })
    }

    /// Every schema record, in the order of their names.
    fn schema(&self) -> Stored<Vec<Vec<u8>>> {
        let mut records = Vec::new();
        for entry in self.schema.iter()? {
            records.push(entry?.1.value().to_vec());
        }
        Ok(records)
    }

    fn row(&self, table: u32, key: &Key) -> Stored<Option<Row>> {
        let row = self.rows.get(stored(table, &key.0).as_slice())?;
        Ok(row.map(|row| Row {
            cells: record::decode_row(row.value()),
        }))
    }

    /// Tells of a record that is not what was stored: damage, told as a failed check of the
    /// storage library is.
    fn damaged(&self, reason: &str) -> Error {
        let reason = redb::Error::Corrupted(reason.to_owned());
        failed(&self.folder, "read", reason)
    }

    /// Loads a large value: grows `reservation` by the bytes stored for it, gathers its slices
    /// with one range read into one buffer of that length, and has `record` make the value.
    fn kept_beside(&self, kept: &Beside, reservation: &mut Reservation) -> Result<Value> {
        reservation.grow(kept.bytes)?;
        let mut stored = Vec::with_capacity(kept.bytes as usize);
        let slices = self.beside.range::<u64>(places(kept));
        for slice in slices.map_err(|error| failed(&self.folder, "read", error))? {
            let (_, slice) = slice.map_err(|error| failed(&self.folder, "read", error))?;
            stored.extend_from_slice(slice.value());
        }
        if stored.len() as u64 != kept.bytes {
            return Err(self.damaged("a large value's slices are not the bytes its row records"));
        }
        // A record its own bytes do not support is damage.
        let loaded = record::load(stored, reservation)?;
        loaded.map_err(|reason| self.damaged(reason))
    }
}

impl Read for View {
    fn commit(&self) -> TxId {
        self.moment.commit
    }

    fn row(&self, table: TableId, key: &Key) -> Result<Option<Row>> {
        self.moment.told(self.moment.row(table.0, key))
    }

    fn rows(&self, table: TableId, from: Bound<&Key>, to: Bound<&Key>) -> Result<RowIter<'_>> {
        Ok(Box::new(self.moment.rows(table.0, &keys::span(from, to))?))
    }

    fn index(&self, index: IndexId, from: Bound<&Key>, to: Bound<&Key>) -> Result<EntryIter<'_>> {
        let joined = self.moment.joined(index.0, &keys::span(from, to))?;
        Ok(Box::new(joined.map(split)))
    }

    fn count(&self, table: TableId) -> Result<u64> {
        self.moment
            .told(number(&self.moment.numbers, count_of(table.0)))
    }

    fn load(&self, beside: &Beside, reservation: &mut Reservation) -> Result<Value> {
        let held = reservation.bytes();
        let loaded = self.moment.kept_beside(beside, reservation);
        if loaded.is_err() {
            // A load that made no value holds nothing: what it grew is given back.
            reservation.shrink(reservation.bytes() - held);
        }
        loaded
    }

    fn staged(&self, _table: TableId, _each: &mut dyn FnMut(&Key) -> Result<bool>) -> Result<()> {
        Ok(())
    }

    fn vectors(
        &self,
        from: VectorRecord<'_>,
        each: &mut dyn FnMut(VectorRecord<'_>, &[u8]) -> Result<bool>,
    ) -> Result<()> {
        let moment = &*self.moment;
        // Every key of a kind begins with the kind's byte, so the kind ends before the next.
        let start = keys::vector(from);
        let end = [start[0] + 1];
        let table = match start[0] {
            keys::BLOCK => &moment.blocks,
            _ => &moment.vectors,
        };
        let records = table.range::<&[u8]>(start.as_slice()..end.as_slice());
        let mut key = Key(Vec::new());
        for record in records.map_err(|error| failed(&moment.folder, "read", error))? {
            let (stored, value) = record.map_err(|error| failed(&moment.folder, "read", error))?;
            // The value is the bytes of the storage library's own page, lent for the call.
            let record = keys::vector_of(stored.value(), &mut key).ok_or_else(unsound);
            if !each(moment.told(record)?, value.value())? {
                break;
            }
        }
        Ok(())
    }

    fn vector_number(&self, number: VectorNumber) -> Result<u64> {
        let at = vector_number_at(number);
        self.moment.told(self::number(&self.moment.numbers, at))
    }

    fn damaged(&self, reason: &str) -> Error {
        self.moment.damaged(reason)
    }
}

impl View {
    /// Every schema record at this moment, as `schema` wrote them. A few small records.
    pub(crate) fn schema_records(&self) -> Result<Vec<Vec<u8>>> {
        self.moment.told(self.moment.schema())
    }

    /// The commit that last changed a schema record. A loaded catalog stays good while this
    /// does not move.
    pub(crate) fn schema_commit(&self) -> Result<TxId> {
        self.moment
            .told(number(&self.moment.numbers, SCHEMA_COMMIT))
            .map(TxId)
    }

    /// The memory limit the store remembers, in bytes; `None` when none is declared.
    pub(crate) fn memory_limit(&self) -> Result<Option<u64>> {
        self.moment.told(memory_limit(&self.moment.numbers))
    }
}

/// Writes a change set as one transaction of the storage library and waits for the disk.
/// Returns the number of the commit it made.
pub(super) fn write(data: &redb::Database, changes: &mut ChangeSet) -> Stored<u64> {
    // The storage library has a commit option, `set_quick_repair`, that lets reopening after a
    // kill skip its repair. It is not used: with it every commit rewrites the library's whole
    // free-page table, about 133 KB for each GiB of file. So an open after a kill reads every
    // page in use, while an open after a clean close reads almost nothing.
    let write = data.begin_write()?;
    let commit;
    {
        let mut numbers = write.open_table(NUMBERS)?;
        let newest = number(&numbers, COMMIT)?;
        let base = changes.base.moment.commit.0;
        if base != newest {
            return Err(io::Error::other(format!(
                "the change was made over commit {base} and the store is at commit {newest}, \
                 so nothing was written"
            ))
            .into());
        }
        commit = newest + 1;
        numbers.insert(COMMIT, commit)?;

        let mut rows = write.open_table(ROWS)?;
        let mut beside = write.open_table(BESIDE)?;
        let first_free = number(&numbers, NEXT_PLACE)?;
        let mut free = first_free;
        for (table, staged) in &changes.rows {
            for (key, row) in staged {
                let key = stored(*table, &key.0);
                // The row that was there, when there was one, still has its large values.
                let before = match &row.values {
                    Some(values) => {
                        let row = record::encode_row(values, |large| -> Stored<u64> {
                            // The compressor's buffer is counted until the value is written.
                            let room = record::compress(large, &mut changes.reserved);
                            let first = free;
                            for slice in large.chunks(SLICE) {
                                beside.insert(free, slice)?;
                                free += 1;
                            }
                            changes.reserved.shrink(room);
                            Ok(first)
                        })?;
                        let before = rows.insert(key.as_slice(), row.as_slice())?;
                        before.map(|row| record::decode_row(row.value()))
                    }
                    None => {
                        let before = rows.remove(key.as_slice())?;
                        before.map(|row| record::decode_row(row.value()))
                    }
                };
                for cell in before.into_iter().flatten() {
                    if let Cell::Beside(kept) = cell {
                        for place in places(&kept) {
                            beside.remove(place)?;
                        }
                    }
                }
            }
        }
        if free != first_free {
            numbers.insert(NEXT_PLACE, free)?;
        }

        let mut index = write.open_table(INDEX)?;
        for (id, staged) in &changes.index {
            for (joined, entry) in staged {
                let key = stored(*id, &joined.0);
                match entry {
                    Some(length) => {
                        let length = u32::try_from(*length).expect("an index entry is short");
                        index.insert(key.as_slice(), length.to_le_bytes().as_slice())?;
                    }
                    None => drop(index.remove(key.as_slice())?),
                }
            }
        }

        for (table, change) in &changes.counts {
            let count = number(&numbers, count_of(*table))?.saturating_add_signed(*change);
            numbers.insert(count_of(*table), count)?;
        }

        let mut schema = write.open_table(SCHEMA)?;
        for (name, record) in &changes.schema {
            schema.insert(name.as_str(), record.as_slice())?;
        }
        if !changes.schema.is_empty() {
            numbers.insert(SCHEMA_COMMIT, commit)?;
        }

        match changes.memory_limit {
            Some(Some(limit)) => drop(numbers.insert(MEMORY_LIMIT, limit)?),
            Some(None) => drop(numbers.remove(MEMORY_LIMIT)?),
            None => {}
        }

        let mut blocks = write.open_table(BLOCKS)?;
        let mut vectors = write.open_table(VECTORS)?;
        for (key, value) in &changes.vectors {
            let table = match key.0[0] {
                keys::BLOCK => &mut blocks,
                _ => &mut vectors,
            };
            match value {
                Some(value) => drop(table.insert(key.0.as_slice(), value.as_slice())?),
                None => drop(table.remove(key.0.as_slice())?),
            }
        }
        for (at, number) in &changes.vector_numbers {
            numbers.insert(*at, *number)?;
        }
    }
    write.commit()?;
    Ok(commit)
}
