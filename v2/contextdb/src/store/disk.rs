//! What the data file holds, how one committed moment of it is read, and how a commit is
//! written into it.
//!
//! Nine tables of the storage library hold everything. Rows and index entries are keyed by
//! the number of their table or index and then their key, so one table's rows are one run of
//! keys. A large value is cut into slices of 243 bytes, each under a place number of its own:
//! the slices of one value are under consecutive places, and its row says how many bytes there
//! are and which place is the first. The vector part's blocks have a table to themselves, and
//! its partition entries and pending entries share another. Kept counts and versions have one
//! each. The last table holds the few numbers the store keeps: the newest commit, the commit that last
//! changed the schema, the next free place, the declared memory limit, each table's row count,
//! and the vector part's three numbers.

use std::cell::Cell;
use std::io::{self, ErrorKind};
use std::ops::{Bound, Range};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use redb::{ReadOnlyTable, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition};

use super::folder::Hold;
use super::keys::{self, Span, stored};
use super::{
    Beside, ChangeSet, CountId, EntryIter, IndexId, Joined, KeptIter, Key, Keys, Read, Row,
    RowIter, TableId, UnitIter, View,
};
use super::{NOTE, SET, counts, drop, locked, record, split};
use super::{VectorId, VectorNumber, VectorRecord};
use crate::memory::{Budget, Passing, Reservation};
use crate::types::{Error, Result, TxId, Value};

/// What a call into the storage library gives, before it is told as the engine tells it.
pub(super) type Stored<T> = std::result::Result<T, Failed>;

#[derive(Debug)]
pub(crate) enum Failed {
    Storage(redb::Error),
    Budget(Error),
}
macro_rules! storage_errors {
    ($($error:ty),*) => {$(impl From<$error> for Failed {
        fn from(error: $error) -> Self { Self::Storage(error.into()) }
    })*};
}
storage_errors!(
    redb::Error,
    redb::StorageError,
    redb::TableError,
    redb::DatabaseError,
    redb::TransactionError,
    redb::CommitError,
    io::Error
);
impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        Self::Budget(error)
    }
}

/// Rows: table number and key, to the row as `record` writes it.
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows");
/// Index entries: index number, entry and row key, to the length of the entry in four bytes
/// and, for an entry that holds a cut value, the place its first one's kept bytes end in four
/// more.
const INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index");
/// Large values: place, to one slice of the record `record` stores for the value.
const BESIDE: TableDefinition<u64, &[u8]> = TableDefinition::new("beside");
/// Versions: a row's stored key and the commit that replaced it (eight bytes), to the old row.
const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
/// Kept counts: count number and the group's key, to the count in eight bytes.
const COUNTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("counts");
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
pub(crate) const DELIVERY: u8 = keys::LEFT + 1;
pub(crate) const SCHEDULE: u8 = DELIVERY + 1;
pub(crate) const AUDIT: u8 = SCHEDULE + 1;
pub(super) const LAST_ACTION: u64 = 5 << 32;
pub(crate) const LAST_AUDIT: u64 = 8 << 32;
const VECTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vectors");

/// The bytes of one slice of a large value; the last slice of a value may be shorter. A leaf
/// of the storage library is 4 bytes and, for each slice, 8 of key, 4 of length and the slice,
/// so sixteen slices fill a page of 4 KiB: 4,084 of its 4,096 bytes.
const SLICE: usize = 243;

const COMMIT: u64 = 0;
const SCHEMA_COMMIT: u64 = 1;
const NEXT_PLACE: u64 = 2;
const NEXT_TABLE: u64 = 6 << 32;
const PUBLICATION_OWED: u64 = 7 << 32;

/// Where a table's row count is kept among the numbers.
pub(super) fn count_of(table: u32) -> u64 {
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
fn places(kept: &Beside) -> Stored<Range<u64>> {
    let end = kept
        .place
        .checked_add(kept.bytes.div_ceil(SLICE as u64))
        .ok_or_else(unsound)?;
    Ok(kept.place..end)
}

fn save_slices(
    beside: &mut redb::Table<'_, u64, &'static [u8]>,
    free: &mut u64,
    bytes: &[u8],
    write: &redb::WriteTransaction,
) -> Stored<u64> {
    let first = *free;
    for slice in bytes.chunks(SLICE) {
        beside.insert_discard(*free, slice)?;
        checkpoint(write)?;
        *free += 1;
    }
    Ok(first)
}

/// Writes pending freed-page lists without committing; the fork owns the threshold.
pub(super) fn checkpoint(write: &redb::WriteTransaction) -> Stored<()> {
    Ok(write.flush_freed_pages()?)
}

/// Removes the slices of a large value.
pub(super) fn remove_slices(
    beside: &mut redb::Table<'_, u64, &'static [u8]>,
    kept: &Beside,
    write: &redb::WriteTransaction,
) -> Stored<()> {
    for place in places(kept)? {
        beside.remove(place)?;
        checkpoint(write)?;
    }
    Ok(())
}

/// Removes every version of a row's stored `key` and frees the large values they and `before`
/// held, each at its last holder in commit order, and none that `after` holds. An address is held
/// by a run of one key's holders, so each is compared with the next.
pub(super) fn release(
    (versions, beside): (&mut ByteTable<'_>, &mut redb::Table<'_, u64, &'static [u8]>),
    key: &[u8],
    before: Option<Row>,
    after: &[Beside],
    cover: &mut Passing,
    write: &redb::WriteTransaction,
) -> Stored<()> {
    let mut handover = |held: &[Beside], next: &[Beside]| {
        let mut gone = held.iter().filter(|kept| !next.contains(kept));
        gone.try_for_each(|kept| remove_slices(beside, kept, write))
    };
    let (from, to) = ([key, &[0; 8]].concat(), [key, &[0xFF; 8]].concat());
    let mut held = Vec::new();
    for version in
        versions.extract_from_if::<&[u8], _>(from.as_slice()..=to.as_slice(), |_, _| true)?
    {
        let now: Vec<_> = record::decode_record(version?.1.value(), cover)?
            .besides()
            .collect();
        handover(&held, &now)?;
        held = now;
    }
    if let Some(before) = before {
        let now: Vec<_> = before.besides().collect();
        handover(&held, &now)?;
        held = now;
    }
    handover(&held, after)
}

pub(super) type ByteTable<'a> = redb::Table<'a, &'static [u8], &'static [u8]>;

/// Tells a failure of the storage library or of a file as the engine tells it: as damage when
/// stored data failed its check, and otherwise as a failure of `operation`, which is what the
/// store was doing.
pub(super) fn failed(folder: &Path, operation: &'static str, error: impl Into<Failed>) -> Error {
    let path = folder.to_path_buf();
    match error.into() {
        Failed::Budget(error) => error,
        Failed::Storage(redb::Error::Io(error))
            if error
                .get_ref()
                .is_some_and(|e| e.is::<redb::FileSizeLimit>()) =>
        {
            let redb::FileSizeLimit(current_bytes, budget_limit_bytes) = *error
                .get_ref()
                .unwrap()
                .downcast_ref::<redb::FileSizeLimit>()
                .unwrap();
            Error::DiskLimitExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint: "raise or clear DISK_LIMIT, or let maintenance free pages",
            }
        }

        Failed::Storage(redb::Error::Io(error))
            if matches!(
                error.get_ref().and_then(|e| e.downcast_ref::<Error>()),
                Some(Error::MemoryBudgetExceeded { .. })
            ) =>
        {
            *error.into_inner().unwrap().downcast::<Error>().unwrap()
        }
        Failed::Storage(redb::Error::Corrupted(reason)) => Error::StoreCorrupted {
            path,
            reason: crate::types::excerpt(reason),
        },
        // A read that came back short, or bytes the storage library does not know as its own,
        // are damage too.
        Failed::Storage(redb::Error::Io(error))
            if matches!(
                error.kind(),
                ErrorKind::UnexpectedEof | ErrorKind::InvalidData
            ) =>
        {
            Error::StoreCorrupted {
                path,
                reason: crate::types::excerpt(error),
            }
        }
        Failed::Storage(other) => Error::StoreIo {
            operation,
            path,
            reason: crate::types::excerpt(other),
        },
    }
}

/// What a record that is not one the store wrote is told as: a failed check.
pub(super) fn unsound() -> Failed {
    redb::Error::Corrupted("a stored record is not one the store wrote".to_owned()).into()
}

fn number(numbers: &impl ReadableTable<u64, u64>, key: u64) -> Stored<u64> {
    Ok(numbers.get(key)?.map_or(0, |number| number.value()))
}

/// The stored setting: absent, explicit NONE, or a ceiling in bytes.
fn setting(numbers: &impl ReadableTable<u64, u64>, at: usize) -> Stored<Option<u64>> {
    let value = numbers.get(at as u64 + 3)?.map(|limit| limit.value());
    Ok(value.filter(|n| *n != crate::settings::RESET))
}

/// The memory limit explicitly remembered by the newest commit, without a default.
pub(super) fn remembered_limit(data: &impl ReadableDatabase) -> Stored<Option<u64>> {
    let read = data.begin_read()?;
    let numbers = read.open_table(NUMBERS)?;
    Ok(setting(&numbers, 0)?.and_then(crate::settings::limit))
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
    write.open_table(COUNTS)?;
    write.open_table(SCHEMA)?;
    write.open_table(NUMBERS)?;
    write.open_table(BLOCKS)?;
    write.open_table(VECTORS)?;
    write.open_table(VERSIONS)?;
    checkpoint(&write)?;
    write.commit()?;
    Ok(())
}

/// One committed moment: the eight tables as one read of the storage library shows them.
pub(crate) struct Moment {
    pub(super) physical: u64,
    rows: ReadOnlyTable<&'static [u8], &'static [u8]>,
    index: ReadOnlyTable<&'static [u8], &'static [u8]>,
    beside: ReadOnlyTable<u64, &'static [u8]>,
    counts: ReadOnlyTable<&'static [u8], &'static [u8]>,
    schema: ReadOnlyTable<&'static str, &'static [u8]>,
    numbers: ReadOnlyTable<u64, u64>,
    blocks: ReadOnlyTable<&'static [u8], &'static [u8]>,
    vectors: ReadOnlyTable<&'static [u8], &'static [u8]>,
    versions: ReadOnlyTable<&'static [u8], &'static [u8]>,
    commit: TxId,
    pub(super) folder: Arc<Path>,
    /// In a reading process, the file that keeps this moment's pages from being reused.
    _hold: Option<Arc<Hold>>,
    pub(super) _pin: Option<super::Pin>,
    _roots: Option<(Reservation, Reservation)>,
}

pub(super) enum Tables<'a> {
    Read(&'a ReadTransaction, (Reservation, Reservation)),
    Write(&'a redb::WriteTransaction, &'a Moment),
}
impl Tables<'_> {
    fn open_table<K: redb::Key + 'static, V: redb::Value + 'static>(
        &mut self,
        table: TableDefinition<K, V>,
    ) -> Stored<ReadOnlyTable<K, V>> {
        Ok(match self {
            Self::Read(read, (standing, roots)) => {
                let bytes = read.root_bytes(table)?;
                standing.grow(bytes).or_else(|_| roots.grow(bytes))?;
                read.open_table(table)?
            }
            Self::Write(write, _) => write.peek_table(table)?,
        })
    }
}

impl Moment {
    pub(super) fn open(
        mut read: Tables<'_>,
        folder: Arc<Path>,
        hold: Option<Arc<Hold>>,
        pin: Option<super::Pin>,
    ) -> Stored<Moment> {
        let numbers = read.open_table(NUMBERS)?;
        let (physical, commit) = match &read {
            Tables::Read(read, _) => (read.commit_id(), TxId(number(&numbers, COMMIT)?)),
            Tables::Write(_, first) => (first.physical, first.commit),
        };
        Ok(Moment {
            physical,
            rows: read.open_table(ROWS)?,
            index: read.open_table(INDEX)?,
            beside: read.open_table(BESIDE)?,
            counts: read.open_table(COUNTS)?,
            schema: read.open_table(SCHEMA)?,
            blocks: read.open_table(BLOCKS)?,
            vectors: read.open_table(VECTORS)?,
            versions: read.open_table(VERSIONS)?,
            commit,
            numbers,
            folder,
            _hold: hold,
            _pin: pin,
            _roots: match read {
                Tables::Read(_, roots) => Some(roots),
                Tables::Write(..) => None,
            },
        })
    }

    fn valid(&self) -> Result<()> {
        if let Some((_, data, _)) = &self._pin {
            data.check_io_errors()
                .map_err(|e| failed(&self.folder, "read", e))?;
        }
        if self
            ._pin
            .as_ref()
            .is_some_and(|(p, ..)| p.load(Ordering::Acquire))
            || !self
                ._hold
                .as_ref()
                .map_or(Ok(true), |h| h.valid())
                .map_err(|e| failed(&self.folder, "read", e))?
        {
            Err(Error::AuthoritativeErasure)
        } else {
            Ok(())
        }
    }

    pub(super) fn told<T, E: Into<Failed>>(&self, read: std::result::Result<T, E>) -> Result<T> {
        self.valid()?;
        read.map_err(|error| failed(&self.folder, "read", error))
    }

    /// One of the numbers the store keeps; zero when it was never put.
    pub(crate) fn number(&self, at: u64) -> Result<u64> {
        self.told(number(&self.numbers, at))
    }

    /// The records of one table, index or count inside a span, in key order or, `back`, the
    /// reverse, each read with `read` from its key and its value. Forward both are lent; the
    /// library lends no backward pair, so a key read backward is copied from its page.
    fn scan<T, F>(
        &self,
        table: &ReadOnlyTable<&'static [u8], &'static [u8]>,
        id: u32,
        (start, end): &Span,
        back: bool,
        mut read: F,
    ) -> Result<impl Iterator<Item = Result<T>> + use<T, F>>
    where
        F: FnMut(&[u8], &[u8]) -> Stored<T>,
    {
        let folder = self.folder.clone();
        let mut range = table
            .range::<&[u8]>(stored(id, start).as_slice()..stored(id, end).as_slice())
            .map_err(|error| failed(&folder, "read", error))?;
        Ok(std::iter::from_fn(move || {
            let mut take = |key: &[u8], record: &[u8]| {
                let key = key
                    .get(4..)
                    .ok_or_else(|| failed(&folder, "read", unsound()))?;
                read(key, record).map_err(|e| failed(&folder, "read", e))
            };
            let failed = |error| failed(&folder, "read", error);
            Some(if back {
                let (key, record) = match range.next_back()? {
                    Ok(pair) => pair,
                    Err(error) => return Some(Err(failed(error))),
                };
                take(key.value(), record.value())
            } else {
                let entry = range.next_lent().map_err(failed);
                entry
                    .transpose()?
                    .and_then(|(key, record)| take(key, record))
            })
        }))
    }

    /// The rows of a table inside a span.
    pub(super) fn rows<'a>(
        &self,
        table: u32,
        span: &Span,
        back: bool,
        cover: &'a mut Passing,
    ) -> Result<impl Iterator<Item = Result<(Key, Row)>> + use<'a>> {
        self.scan(&self.rows, table, span, back, move |key, row| {
            if key.len() > super::KEY_LIMIT {
                return Err(unsound());
            }
            let row = record::decode_record(row, cover)?;
            Ok((Key(key.to_vec()), row))
        })
    }

    pub(super) fn row_keys(
        &self,
        table: u32,
        span: &Span,
    ) -> Result<impl Iterator<Item = Result<(Key, ())>> + use<>> {
        self.scan(&self.rows, table, span, false, |key, _| {
            if key.len() > super::KEY_LIMIT {
                return Err(unsound());
            }
            Ok((Key(key.to_vec()), ()))
        })
    }

    /// The entries of an index inside a span, each still joined to the key of its row, with
    /// the length of the entry and the place its first cut value's kept bytes end. A record
    /// of another size than four or eight bytes, an entry longer than its key and a place past
    /// the entry are damage.
    pub(super) fn joined(
        &self,
        index: u32,
        span: &Span,
        back: bool,
    ) -> Result<impl Iterator<Item = Result<(Key, Joined)>> + use<>> {
        self.scan(&self.index, index, span, back, move |joined, record| {
            let entry = entry_of(joined, record)
                .filter(|(n, _)| *n <= super::KEY_LIMIT && joined.len() - n <= super::KEY_LIMIT)
                .ok_or_else(unsound)?;
            Ok((Key(joined.to_vec()), entry))
        })
    }

    /// The records of a kept count inside a span. One that is not eight bytes is damage.
    pub(super) fn kept(
        &self,
        count: u32,
        span: &Span,
    ) -> Result<impl Iterator<Item = Result<(Key, u64)>> + use<>> {
        self.scan(&self.counts, count, span, false, move |key, record| {
            let count = counts::read(record).ok_or_else(unsound)?;
            Ok((Key(key.to_vec()), count))
        })
    }

    fn row(&self, table: u32, key: &Key, cover: &mut Passing) -> Stored<Option<Row>> {
        let row = self.rows.get(stored(table, &key.0).as_slice())?;
        let row = row.map(|row| record::decode_record(row.value(), cover));
        row.transpose()
    }

    /// Tells of a record that is not what was stored: damage, told as a failed check of the
    /// storage library is.
    fn damaged(&self, reason: &str) -> Error {
        let reason = redb::Error::Corrupted(reason.to_owned());
        failed(&self.folder, "read", reason)
    }

    /// Reserves first, then gathers a large value or delivery body with one slice range.
    fn gather(&self, kept: &Beside, reservation: &mut Reservation) -> Result<Vec<u8>> {
        let places = self.told(places(kept))?;
        if places.end > self.number(NEXT_PLACE)? || kept.bytes > isize::MAX as u64 {
            return Err(self.damaged("a large value's address or length is outside the store"));
        }
        reservation.grow(kept.bytes)?;
        let mut stored = Vec::with_capacity(kept.bytes as usize);
        let slices = self.beside.range::<u64>(places);
        for slice in slices.map_err(|error| failed(&self.folder, "read", error))? {
            let (_, slice) = slice.map_err(|error| failed(&self.folder, "read", error))?;
            stored.extend_from_slice(slice.value());
        }
        if stored.len() as u64 != kept.bytes {
            return Err(self.damaged("a large value's slices are not the bytes its row records"));
        }
        Ok(stored)
    }
    fn kept_beside(&self, kept: &Beside, reservation: &mut Reservation) -> Result<Value> {
        let stored = self.gather(kept, reservation)?;
        // A record its own bytes do not support is damage.
        let loaded = record::load(stored, reservation)?;
        loaded.map_err(|reason| self.damaged(reason))
    }
}

/// What an index record holds beside its entry: see [`Moment::joined`].
fn entry_of(joined: &[u8], record: &[u8]) -> Option<Joined> {
    let number = |at: usize| Some(u32::from_le_bytes(*record.get(at..)?.first_chunk()?) as usize);
    let length = number(0)?;
    let cut = match record.len() {
        4 => None,
        8 => Some(number(4)?),
        _ => return None,
    };
    (length <= joined.len() && cut.is_none_or(|cut| cut <= length)).then_some((length, cut))
}

impl Read for View {
    fn committed(&self) -> &View {
        self
    }
    fn setting(&self, at: usize) -> Result<Option<u64>> {
        self.moment.told(setting(&self.moment.numbers, at))
    }
    fn valid(&self) -> Result<()> {
        self.moment.valid()
    }
    fn commit(&self) -> TxId {
        self.moment.commit
    }

    fn row(&self, table: TableId, key: &Key, cover: &mut Passing) -> Result<Option<Row>> {
        let row = self.moment.told(self.moment.row(table.0, key, cover))?;
        self.tally(u64::from(row.is_some()));
        Ok(row)
    }

    fn rows<'a>(
        &'a self,
        table: TableId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
        cover: &'a mut Passing,
    ) -> Result<RowIter<'a>> {
        let span = keys::span(from, to);
        let rows = self.moment.rows(table.0, &span, back, cover)?;
        Ok(Box::new(
            rows.inspect(|row| self.tally(u64::from(row.is_ok()))),
        ))
    }

    fn counted(&self, of: Keys, from: Bound<&Key>, to: Bound<&Key>) -> Result<UnitIter<'_>> {
        let (moment, span) = (&*self.moment, keys::span(from, to));
        let (records, id, index) = match of {
            Keys::Table(table) => (&moment.rows, table.0, false),
            Keys::Index(index) => (&moment.index, index.0, true),
        };
        let sound = move |k: &[u8], r: &[u8]| {
            (if index {
                entry_of(k, r)
                    .is_some_and(|(n, _)| n <= super::KEY_LIMIT && k.len() - n <= super::KEY_LIMIT)
            } else {
                k.len() <= super::KEY_LIMIT
            })
            .then_some(())
            .ok_or_else(unsound)
        };
        let found = moment.scan(records, id, &span, false, sound)?;
        Ok(Box::new(found.map(|found| found.map(drop))))
    }

    fn index(
        &self,
        index: IndexId,
        from: Bound<&Key>,
        to: Bound<&Key>,
        back: bool,
    ) -> Result<EntryIter<'_>> {
        let joined = self.moment.joined(index.0, &keys::span(from, to), back)?;
        Ok(Box::new(joined.map(split)))
    }

    fn kept(&self, count: CountId, from: Bound<&Key>, to: Bound<&Key>) -> Result<KeptIter<'_>> {
        let kept = self.moment.kept(count.0, &keys::span(from, to))?;
        Ok(Box::new(kept))
    }

    fn count(&self, table: TableId) -> Result<u64> {
        self.moment.number(count_of(table.0))
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
        let table = if keys::with_blocks(start[0]) {
            &moment.blocks
        } else {
            &moment.vectors
        };
        let read = |error| self.mark(failed(&moment.folder, "read", error));
        let records = table.range::<&[u8]>(start.as_slice()..end.as_slice());
        let mut records = records.map_err(read)?;
        let mut key = Key(Vec::new());
        // The key and the value are lent by the storage library until the walk moves on.
        while let Some((stored, value)) = records.next_lent().map_err(read)? {
            let record = keys::vector_of(stored, &mut key).ok_or_else(unsound);
            if !each(moment.told(record).map_err(|e| self.mark(e))?, value)? {
                break;
            }
        }
        Ok(())
    }

    fn vector_number(&self, number: VectorNumber) -> Result<u64> {
        self.moment.number(vector_number_at(number))
    }

    fn damaged(&self, reason: &str) -> Error {
        self.mark(self.moment.damaged(reason))
    }
}
thread_local!(pub(crate) static READING: Cell<Option<(VectorId, u32)>> = const { Cell::new(None) });

/// Decoded records and their original key/value byte lengths, lent to store inspection.
pub(crate) enum Record<'a> {
    Row(TableId, Row, u64),
    Index(IndexId, Key, Joined),
    Count(CountId),
    Version(TableId, Row),
    Delivery(&'a [u8], &'a [u8]),
    Schedule(&'a str, u64),
    Audit(&'a [u8]),
    Vector(VectorId, bool, VectorRecord<'a>, &'a [u8]),
}

impl View {
    /// `Hold::stamp` on a cursor's own reader file.
    pub(crate) fn stamp(&self, deadline: Option<Option<i64>>) -> Result<bool> {
        let hold = self.moment._hold.as_ref();
        let stamped = hold.map_or(Ok(true), |hold| hold.stamp(deadline));
        stamped.map_err(|e| failed(self.folder(), "read", e))
    }

    pub(crate) fn versions<'a>(
        &self,
        table: TableId,
        key: &Key,
        cover: &'a mut Passing,
    ) -> Result<impl Iterator<Item = Result<(TxId, Row)>> + use<'a>> {
        let span = (
            [&key.0[..], &[0; 8]].concat(),
            [&key.0[..], &[0xFF; 9]].concat(),
        );
        let m = &*self.moment;
        m.scan(&m.versions, table.0, &span, true, |key, row| {
            let commit = key
                .get(key.len().saturating_sub(8)..)
                .and_then(|c| c.try_into().ok());
            let commit = TxId(u64::from_be_bytes(commit.ok_or_else(unsound)?));
            Ok((commit, record::decode_record(row, cover)?))
        })
    }

    pub(crate) fn versioned(
        &self,
        table: TableId,
    ) -> Result<impl Iterator<Item = Result<Key>> + use<>> {
        let m = &*self.moment;
        let all = keys::span(Bound::Unbounded, Bound::Unbounded);
        m.scan(&m.versions, table.0, &all, false, |key, _| {
            let length = key.len().checked_sub(8).ok_or_else(unsound)?;
            Ok(Key(key[..length].to_vec()))
        })
    }

    /// `error`; when it is damage met by the writer's process, [`READING`] is marked.
    fn mark(&self, error: Error) -> Error {
        match (READING.take(), &error, &self.moment._pin) {
            (Some(at), Error::StoreCorrupted { .. }, Some((.., marks))) => locked(marks).insert(at),
            _ => false,
        };
        error
    }
    /// True when the writer's process met damage in the IVF `number` of `vector`.
    pub(crate) fn marked(&self, vector: VectorId, number: u32) -> bool {
        matches!(&self.moment._pin, Some((.., marks)) if locked(marks).contains(&(vector, number)))
    }
    pub(crate) fn folder(&self) -> &Path {
        &self.moment.folder
    }
    pub(crate) fn page_slack(&self) -> Result<f64> {
        use redb::ReadableTableMetadata as _;
        let m = &self.moment;
        let a = [m.rows.stats(), m.index.stats(), m.beside.stats()];
        let b = [m.counts.stats(), m.schema.stats(), m.numbers.stats()];
        let c = [m.blocks.stats(), m.vectors.stats()];
        let (mut held, mut slack) = (0, 0);
        for found in a.into_iter().chain(b).chain(c) {
            let found = m.told(found)?;
            held += found.stored_bytes() + found.metadata_bytes();
            slack += found.fragmented_bytes();
        }
        Ok(slack as f64 / (held + slack).max(1) as f64)
    }
    pub(crate) fn file_bytes(&self) -> Result<u64> {
        std::fs::metadata(self.folder().join("data"))
            .map(|m| m.len())
            .map_err(|e| failed(self.folder(), "read", e))
    }
    pub(crate) fn same_row(
        &self,
        other: &View,
        table: TableId,
        key: &Key,
        budget: &Budget,
    ) -> Result<bool> {
        let stored = super::keys::stored(table.0, key.as_bytes());
        let a = self.moment.told(self.moment.rows.get(stored.as_slice()))?;
        let b = other
            .moment
            .told(other.moment.rows.get(stored.as_slice()))?;
        if a.as_ref().map(|v| v.value()) == b.as_ref().map(|v| v.value()) {
            return Ok(true);
        }
        let (Some(a), Some(b)) = (a, b) else {
            return Ok(false);
        };
        let mut cover = budget.passing("compare row")?;
        let a = self
            .moment
            .told(record::decode_record(a.value(), &mut cover))?;
        let mut second = budget.passing("compare row")?;
        let b = other
            .moment
            .told(record::decode_record(b.value(), &mut second))?;
        Ok(a.written == b.written
            && a.cells.len() == b.cells.len()
            && a.cells.iter().zip(&b.cells).all(|(a, b)| match (a, b) {
                (super::Cell::Here(a), super::Cell::Here(b)) => {
                    crate::query::sort_order(a, b).is_eq()
                }
                _ => a == b,
            }))
    }

    /// Visits every physical record, decoding its store-owned framing exactly once.
    pub(crate) fn records(
        &self,
        budget: &Budget,
        mut f: impl FnMut(Record<'_>, u64) -> Result<()>,
    ) -> Result<()> {
        let mut cover = budget.passing("verify row")?;
        let m = &*self.moment;
        let mut lent = Key(Vec::new());
        self.pairs(|kind, key, value| {
            let bytes = (key.len() + value.len()) as u64;
            if kind == 4 && key.first().is_some_and(|k| *k >= DELIVERY) {
                return f(self.runtime_record(key, value)?, bytes);
            }
            if kind == 3 || kind == 4 {
                let record = keys::vector_of(key, &mut lent)
                    .ok_or_else(|| m.damaged("an invalid vector record key"))?;
                if keys::vector(record) != key || keys::with_blocks(key[0]) != (kind == 3) {
                    return Err(m.damaged("a vector record is under the wrong key"));
                }
                let id = VectorId(u32::from_be_bytes(std::array::from_fn(|i| key[i + 1])));
                f(Record::Vector(id, kind == 3, record, value), bytes)?;
                return Ok(());
            }
            let id = key
                .first_chunk::<4>()
                .ok_or_else(|| m.damaged("a record key lacks its owner"))?;
            let id = u32::from_be_bytes(*id);
            let key = Key(key[4..].to_vec());
            let record = match kind {
                0 => {
                    let row = m.told(record::decode_record(value, &mut cover))?;
                    let large = row
                        .cells
                        .iter()
                        .filter_map(|c| match c {
                            super::Cell::Beside(b) => {
                                Some(b.bytes.saturating_add(8 * b.bytes.div_ceil(SLICE as u64)))
                            }
                            _ => None,
                        })
                        .fold(0, u64::saturating_add);
                    Record::Row(TableId(id), row, large)
                }
                1 => {
                    let joined = m.told(entry_of(&key.0, value).ok_or_else(unsound))?;
                    Record::Index(IndexId(id), key, joined)
                }
                5 => Record::Version(
                    TableId(id),
                    m.told(record::decode_record(value, &mut cover))?,
                ),
                _ => {
                    m.told(counts::read(value).ok_or_else(unsound))?;
                    Record::Count(CountId(id))
                }
            };
            f(record, bytes)
        })
    }

    /// The lent byte-table walk shared by inspection and record copying.
    fn pairs(&self, mut each: impl FnMut(usize, &[u8], &[u8]) -> Result<()>) -> Result<()> {
        let m = &*self.moment;
        for (kind, table) in [
            &m.rows,
            &m.index,
            &m.counts,
            &m.blocks,
            &m.vectors,
            &m.versions,
        ]
        .into_iter()
        .enumerate()
        {
            let mut records = m.told(table.range::<&[u8]>(..))?;
            while let Some((key, value)) = m.told(records.next_lent())? {
                each(kind, key, value)?;
            }
        }
        Ok(())
    }

    /// Preserves physical framing while using the existing staging methods and writer.
    pub(super) fn copy_to(&self, dest: &super::Store, budget: &super::Budget) -> Result<u64> {
        let m = &*self.moment;
        let mut changes = ChangeSet::new(dest.view()?, budget)?;
        let mut records = 0;
        let mut key = Key(Vec::new());
        let mut stage = |kind, k: &[u8], v: &[u8]| -> Result<()> {
            if changes.reserved.bytes() >= super::PART_BYTES {
                let batch = std::mem::replace(&mut changes, ChangeSet::new(dest.view()?, budget)?);
                dest.commit(batch)?;
                changes.rebase(dest.view()?);
            }
            let held = budget.reserve((128 + 2 * k.len() + v.len()) as u64, "export")?;
            match kind {
                1 => {
                    let id = IndexId(u32::from_be_bytes(
                        *k.first_chunk()
                            .ok_or_else(|| m.damaged("an invalid index key"))?,
                    ));
                    let joined = &k[4..];
                    let (length, cut) = m.told(entry_of(joined, v).ok_or_else(unsound))?;
                    changes.put_index(
                        id,
                        super::Entry {
                            key: Key(joined[..length].to_vec()),
                            cut,
                        },
                        Key(joined[length..].to_vec()),
                    )?;
                }
                3 | 4 if kind == 3 || k.first().is_none_or(|k| *k < DELIVERY) => {
                    let record = m.told(keys::vector_of(k, &mut key).ok_or_else(unsound))?;
                    changes.put_vector(record, v.to_vec())?;
                }
                6 => changes.put_schema(
                    m.told(std::str::from_utf8(k).map_err(|_| unsound()))?,
                    v.to_vec(),
                )?,
                7 => changes.put_number(
                    u64::from_le_bytes(k.try_into().expect("a number key")),
                    u64::from_le_bytes(v.try_into().expect("a number value")),
                )?,
                _ => {
                    changes.reserved.grow(held.bytes())?;
                    changes.copied.push((kind, k.to_vec(), v.to_vec()));
                }
            }
            records += 1;
            Ok(())
        };
        self.pairs(&mut stage)?;
        let mut beside = m.told(m.beside.range::<u64>(..))?;
        while let Some((k, v)) = m.told(beside.next_lent())? {
            stage(8, &k.to_le_bytes(), v)?;
        }
        let mut schema = m.told(m.schema.range::<&str>(..))?;
        while let Some((k, v)) = m.told(schema.next_lent())? {
            stage(6, k.as_bytes(), v)?;
        }
        for number in m.told(m.numbers.range::<u64>(..))? {
            let (k, v) = m.told(number)?;
            if k.value() != COMMIT {
                stage(7, &k.value().to_le_bytes(), &v.value().to_le_bytes())?;
            }
        }
        stage(7, &COMMIT.to_le_bytes(), &m.commit.0.to_le_bytes())?;
        dest.commit(changes)?;
        Ok(records)
    }

    pub(crate) fn delivery_body<'a>(
        &self,
        bytes: &'a [u8],
        held: &mut Reservation,
    ) -> Result<std::borrow::Cow<'a, [u8]>> {
        let (_, body, kept) = self.moment.told(record::delivery_parts(bytes))?;
        match kept {
            Some(kept) => self.moment.gather(&kept, held).map(std::borrow::Cow::Owned),
            None => Ok(std::borrow::Cow::Borrowed(body)),
        }
    }

    pub(crate) fn publication_owed(&self) -> Result<bool> {
        self.moment.number(PUBLICATION_OWED).map(|n| n != 0)
    }

    pub(crate) fn last_action(&self) -> Result<u64> {
        self.moment.number(LAST_ACTION)
    }

    pub(crate) fn runtime_record<'a>(&self, key: &'a [u8], value: &'a [u8]) -> Result<Record<'a>> {
        if key.first() == Some(&DELIVERY) {
            return Ok(Record::Delivery(key, value));
        }
        if let [AUDIT, _, _] = *key {
            return Ok(Record::Audit(value));
        }
        let bad = || self.damaged("an invalid schedule cursor");
        let name = key
            .strip_prefix(&[SCHEDULE])
            .and_then(|b| std::str::from_utf8(b).ok())
            .filter(|n| !n.is_empty())
            .ok_or_else(bad)?;
        let due = u64::from_le_bytes(value.try_into().map_err(|_| bad())?);
        Ok(Record::Schedule(name, due))
    }

    /// Lends bounded delivery or schedule runtime records without cloning their bytes.
    pub(crate) fn deliveries(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        each: &mut impl FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        let m = &*self.moment;
        let mut records = m.told(m.vectors.range::<&[u8]>(range))?;
        while let Some((key, value)) = m.told(records.next_lent())? {
            if !each(key, value)? {
                break;
            }
        }
        Ok(())
    }

    /// Lends each stored declaration until its callback returns.
    pub(crate) fn each_schema(&self, mut f: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        for entry in self.moment.told(self.moment.schema.iter())? {
            let entry = self.moment.told(entry)?;
            f(entry.1.value())?;
        }
        Ok(())
    }

    /// The commit that last changed a schema record. A loaded catalog stays good while this
    /// does not move.
    pub(crate) fn schema_commit(&self) -> Result<TxId> {
        self.moment
            .told(number(&self.moment.numbers, SCHEMA_COMMIT))
            .map(TxId)
    }
}

impl ChangeSet {
    pub(crate) fn mark_publication(&mut self, owed: bool) -> Result<()> {
        self.put_number(PUBLICATION_OWED, u64::from(owed))
    }

    /// Takes a table identity, advancing its durable counter in the declaration's commit.
    pub(crate) fn take_table_number(&mut self) -> Result<TableId> {
        let next = match self.numbers.get(&NEXT_TABLE) {
            Some(next) => *next,
            None => self.base.moment.number(NEXT_TABLE)?,
        };
        let id = u32::try_from(next).map_err(|_| Error::SchemaInvalid {
            reason: "the store has exhausted its table numbers".into(),
        })?;
        self.put_number(NEXT_TABLE, next + 1)?;
        Ok(TableId(id))
    }
}

/// Writes a change set as one transaction of the storage library and waits for the disk.
/// Returns the number of the commit it made.
pub(super) fn write(data: &redb::Database, changes: &mut ChangeSet) -> Stored<u64> {
    // The storage library has a commit option, `set_quick_repair`, that lets reopening after a
    // kill skip its repair. It is not used: with it every commit rewrites the library's whole
    // free-page table, about 133 KB for each GiB of file. So an open after a kill reads every
    // page in use, while an open after a clean close reads almost nothing.
    if let Some((from, allowance, drain, outcome)) = &changes.compaction {
        let mut write = data.begin_write()?;
        let step = || -> Stored<_> {
            write.limit_file_growth()?;
            if *drain && *allowance == Some(0) {
                write.set_two_phase_commit(true);
            }
            let (pages, next) = match allowance {
                Some(0) => (0, from.clone()),
                Some(allowance) => write.compact_pages(*allowance, from.as_deref())?,
                None => (0, None),
            };
            let changed = next != *from;
            if changed {
                write.set_compaction_position(next.as_deref())?;
            }
            if pages == 0 && !changed && !*drain {
                return Ok(0);
            }
            checkpoint(&write)?;
            write.prepare_compaction_commit()?;
            write.commit()?;
            Ok(if pages > 0 || changed { pages + 1 } else { 0 })
        };
        let result = match step() {
            Err(Failed::Storage(redb::Error::Io(e)))
                if e.get_ref().is_some_and(|e| e.is::<redb::FileSizeLimit>()) =>
            {
                0
            }
            other => other?,
        };
        outcome.store(result as u64, Ordering::Relaxed);
        return Ok(changes.base().commit().0);
    }
    let mut write = match changes.data {
        Some(_) => changes.writing()?,
        None => begin(data, changes)?,
    };
    put(&write, changes)?;
    write.delete_table(super::through::NOTES)?;
    write.delete_table(super::through::SETS)?;
    if number(&write.open_table(NUMBERS)?, PUBLICATION_OWED)? != 0 {
        write.reserve(128)?;
    }
    write.commit()?;
    Ok(changes.commit_number().0)
}

pub(super) fn begin(data: &redb::Database, changes: &ChangeSet) -> Stored<redb::WriteTransaction> {
    let write = data.begin_write()?;
    {
        let mut numbers = write.open_table(NUMBERS)?;
        let newest = number(&numbers, COMMIT)?;
        let base = changes.base.moment.commit.0;
        if base != newest {
            return Err(io::Error::other("staged on an older commit; nothing was written").into());
        }
        // The transaction's own number, unless a commit has taken it or a higher one.
        numbers.insert_discard(COMMIT, changes.number.0.max(newest + 1))?;
    }
    Ok(write)
}

pub(super) fn put(write: &redb::WriteTransaction, changes: &mut ChangeSet) -> Stored<()> {
    put_derived(write, changes)?;
    let mut cover = changes.reserved.passing();
    {
        let mut numbers = write.open_table(NUMBERS)?;
        let mut rows = write.open_table(ROWS)?;
        let mut beside = write.open_table(BESIDE)?;
        let mut index = write.open_table(INDEX)?;
        let mut versions = write.open_table(VERSIONS)?;
        let first_free = number(&numbers, NEXT_PLACE)?;
        let mut free = first_free;
        let commit = changes.commit_number().0.to_be_bytes();
        for (table, staged) in &changes.rows {
            let keeps = changes.keeps.get(table).filter(|_| !changes.erasing);
            for (key, row) in staged {
                let key = stored(*table, &key.0);
                match rows.get(key.as_slice())? {
                    Some(old) if keeps.is_some_and(|removed| *removed || row.values.is_some()) => {
                        let at = [key.as_slice(), &commit].concat();
                        versions.insert_discard(at.as_slice(), old.value())?;
                    }
                    Some(old) => {
                        let old = record::decode_record(old.value(), &mut cover)?;
                        let after: Vec<_> = row.left.iter().map(|left| left.1.clone()).collect();
                        let tables = (&mut versions, &mut beside);
                        release(tables, &key, Some(old), &after, &mut cover, write)?;
                    }
                    None => {}
                }
                match &row.values {
                    Some(values) => {
                        let mut bytes = record::encode_row(
                            values,
                            &row.compact,
                            (row.written, &row.left),
                            &mut cover,
                            |large| -> Stored<u64> {
                                // The compressor's buffer is counted until the value is written.
                                let room = record::compress(large, &mut changes.reserved);
                                let first = free;
                                for slice in large.chunks(SLICE) {
                                    beside.insert_discard(free, slice)?;
                                    checkpoint(write)?;
                                    free += 1;
                                }
                                changes.reserved.shrink(room);
                                Ok(first)
                            },
                        )?;
                        let room = record::pack(&mut bytes, &mut changes.reserved);
                        rows.insert_discard(key.as_slice(), bytes.as_slice())?;
                        changes.reserved.shrink(room);
                    }
                    None => drop(rows.remove(key.as_slice())?),
                }
                checkpoint(write)?;
            }
        }
        for (id, staged) in &changes.index {
            for (joined, entry) in staged {
                let key = stored(*id, &joined.0);
                match entry {
                    Some((length, cut)) => {
                        let number = |number: usize| {
                            let number = u32::try_from(number).expect("an index entry is short");
                            number.to_le_bytes()
                        };
                        let record = [number(*length), number(cut.unwrap_or(0))];
                        let record = &record.as_flattened()[..if cut.is_some() { 8 } else { 4 }];
                        index.insert_discard(key.as_slice(), record)?;
                    }
                    None => drop(index.remove(key.as_slice())?),
                }
                checkpoint(write)?;
            }
        }

        for (table, change) in &changes.counts {
            let count = number(&numbers, count_of(*table))?.saturating_add_signed(*change);
            numbers.insert_discard(count_of(*table), count)?;
            checkpoint(write)?;
        }

        counts::write(&changes.kept, &mut write.open_table(COUNTS)?, write)?;

        // Last, so that a drop takes with it what the transaction staged under its number.
        let mut blocks = write.open_table(BLOCKS)?;
        let mut vectors = write.open_table(VECTORS)?;
        drop::write(
            changes,
            (&mut rows, &mut versions, &mut beside),
            &mut index,
            &mut write.open_table(COUNTS)?,
            &mut numbers,
            [&mut blocks, &mut vectors],
            write,
        )?;

        let mut schema = write.open_table(SCHEMA)?;
        for (name, record) in &changes.schema {
            match record {
                Some(record) => schema.insert_discard(name.as_str(), record.as_slice())?,
                None => drop(schema.remove(name.as_str())?),
            }
            checkpoint(write)?;
        }
        if !changes.schema.is_empty() {
            numbers.insert_discard(SCHEMA_COMMIT, changes.commit_number().0)?;
        }

        if free != first_free {
            numbers.insert_discard(NEXT_PLACE, free)?;
        }
        let mut kept = write.open_table(COUNTS)?;
        for (kind, key, value) in &changes.copied {
            match kind {
                0 => rows.insert_discard(key.as_slice(), value.as_slice())?,
                2 => kept.insert_discard(key.as_slice(), value.as_slice())?,
                4 => vectors.insert_discard(key.as_slice(), value.as_slice())?,
                5 => versions.insert_discard(key.as_slice(), value.as_slice())?,
                8 => drop(beside.insert(
                    u64::from_le_bytes(key.as_slice().try_into().expect("a slice place")),
                    value.as_slice(),
                )?),
                _ => unreachable!("only rows, counts, versions and slices need raw staging"),
            }
            checkpoint(write)?;
        }
    }
    checkpoint(write)?;
    Ok(())
}

pub(super) fn put_derived(write: &redb::WriteTransaction, changes: &mut ChangeSet) -> Stored<()> {
    use super::through::{NOTES, SETS};
    let mut blocks = write.open_table(BLOCKS)?;
    let mut vectors = write.open_table(VECTORS)?;
    let mut beside = write.open_table(BESIDE)?;
    let mut numbers = write.open_table(NUMBERS)?;
    let first_free = number(&numbers, NEXT_PLACE)?;
    let mut free = first_free;
    let mut temp = match changes.data {
        Some(_) => vec![write.open_table(NOTES)?, write.open_table(SETS)?],
        None => Vec::new(),
    };
    // Finish and release delivery bodies before the row encoder needs its buffer.
    for (key, staged) in changes
        .vectors
        .iter_mut()
        .filter(|(k, _)| k.0[0] == DELIVERY)
    {
        let mut value = staged.take();
        let bytes = value.as_ref().map_or(0, Vec::capacity) as u64;
        if let Some(value) = &mut value {
            record::keep_delivery(value, |body| {
                save_slices(&mut beside, &mut free, body, write)
            })?;
        }
        let old = vectors
            .get(key.0.as_slice())?
            .map(|before| record::delivery_parts(before.value()).map(|v| v.2))
            .transpose()?
            .flatten();
        let new = value
            .as_ref()
            .map(|v| record::delivery_parts(v).map(|v| v.2))
            .transpose()?
            .flatten();
        if let Some(old) = old.filter(|old| Some(old) != new.as_ref()) {
            remove_slices(&mut beside, &old, write)?;
        }
        match &value {
            Some(value) => vectors.insert_discard(key.0.as_slice(), value.as_slice())?,
            None => drop(vectors.remove(key.0.as_slice())?),
        }
        drop(value);
        changes.reserved.shrink(bytes);
        if changes.data.is_some() {
            changes.derived -= bytes;
        }
        checkpoint(write)?;
    }
    changes.vectors.retain(|k, _| k.0[0] != DELIVERY);
    for (key, value) in changes.vectors.iter().filter(|(k, _)| k.0[0] != DELIVERY) {
        let table = match key.0[0] {
            NOTE => &mut temp[0],
            SET => &mut temp[1],
            kind if keys::with_blocks(kind) => &mut blocks,
            _ => &mut vectors,
        };
        match value {
            Some(value) => table.insert_discard(key.0.as_slice(), value.as_slice())?,
            None => drop(table.remove(key.0.as_slice())?),
        }
        checkpoint(write)?;
    }
    for (at, number) in &changes.numbers {
        numbers.insert_discard(*at, *number)?;
        checkpoint(write)?;
    }
    if free != first_free {
        numbers.insert_discard(NEXT_PLACE, free)?;
    }
    Ok(())
}
