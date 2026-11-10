//! What the data file holds, how one committed moment of it is read, and how a commit is
//! written into it.
//!
//! Five tables of the storage library hold everything. Rows and index entries are keyed by the
//! number of their table or index and then their key, so one table's rows are one run of keys.
//! Large values sit under a place number of their own. The last table holds the few numbers the
//! store keeps: the newest commit, the commit that last changed the schema, the next free place,
//! the declared memory limit, and each table's row count.

use std::io::{self, ErrorKind};
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use redb::{ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition};

use super::folder::Hold;
use super::keys::{self, Span, stored};
use super::{Beside, Cell, ChangeSet, EntryIter, IndexId, Key, Read, Row, RowIter, TableId, View};
use super::{record, split};
use crate::memory::Reservation;
use crate::types::{Error, Result, TxId, Value};

/// What a call into the storage library gives, before it is told as the engine tells it.
pub(super) type Stored<T> = std::result::Result<T, redb::Error>;

/// Rows: table number and key, to the row as `record` writes it.
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
/// Index entries: index number, entry and row key, to the length of the entry.
const INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index");
/// Large values: place, to the value as `record` writes it.
const BESIDE: TableDefinition<u64, &[u8]> = TableDefinition::new("beside");
/// Schema records by name, as `schema` wrote them.
const SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");
/// The numbers the store keeps.
const NUMBERS: TableDefinition<u64, u64> = TableDefinition::new("numbers");

const COMMIT: u64 = 0;
const SCHEMA_COMMIT: u64 = 1;
const NEXT_PLACE: u64 = 2;
/// Not there when no limit is declared.
const MEMORY_LIMIT: u64 = 3;

/// Where a table's row count is kept among the numbers.
fn count_of(table: u32) -> u64 {
    (1 << 32) | u64::from(table)
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

fn number(numbers: &impl ReadableTable<u64, u64>, key: u64) -> Stored<u64> {
    Ok(numbers.get(key)?.map_or(0, |number| number.value()))
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
    write.commit()?;
    Ok(())
}

/// One committed moment: the five tables as one read of the storage library shows them.
pub(super) struct Moment {
    rows: ReadOnlyTable<&'static [u8], &'static [u8]>,
    index: ReadOnlyTable<&'static [u8], &'static [u8]>,
    beside: ReadOnlyTable<u64, &'static [u8]>,
    schema: ReadOnlyTable<&'static str, &'static [u8]>,
    numbers: ReadOnlyTable<u64, u64>,
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
        F: Fn(&[u8]) -> T,
    {
        let folder = self.folder.clone();
        let range = table
            .range::<&[u8]>(stored(id, start).as_slice()..stored(id, end).as_slice())
            .map_err(|error| failed(&folder, "read", error))?;
        Ok(range.map(move |entry| {
            let (key, record) = entry.map_err(|error| failed(&folder, "read", error))?;
            Ok((Key(key.value()[4..].to_vec()), read(record.value())))
        }))
    }

    /// The rows of a table inside a span.
    pub(super) fn rows(
        &self,
        table: u32,
        span: &Span,
    ) -> Result<impl Iterator<Item = Result<(Key, Row)>> + use<>> {
        self.scan(&self.rows, table, span, |row| Row {
            cells: record::decode_row(row),
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
            u32::from_le_bytes(std::array::from_fn(|at| length[at])) as usize
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

    fn kept_beside(&self, place: u64) -> Stored<Value> {
        let kept = self.beside.get(place)?;
        let kept = kept.expect("the commit that keeps a row keeps its large values");
        Ok(record::decode_value(kept.value()))
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
        reservation.grow(beside.bytes)?;
        self.moment.told(self.moment.kept_beside(beside.place))
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
        let kept = self.moment.numbers.get(MEMORY_LIMIT);
        let kept = kept.map(|kept| kept.map(|limit| limit.value()));
        self.moment.told(kept.map_err(redb::Error::from))
    }
}

/// Writes a change set as one transaction of the storage library and waits for the disk.
/// Returns the number of the commit it made.
pub(super) fn write(data: &redb::Database, changes: &ChangeSet) -> Stored<u64> {
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
                            beside.insert(free, large)?;
                            free += 1;
                            Ok(free - 1)
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
                        beside.remove(kept.place)?;
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
    }
    write.commit()?;
    Ok(commit)
}
