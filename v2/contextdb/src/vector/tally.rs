//! The partitions a transaction's statements have made and emptied so far, so that the INSERT
//! or UPDATE that would give a column one partition too many is refused when it runs.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::directory;
use crate::memory::{Budget, Reservation};
use crate::schema::{Table, VectorColumn};
use crate::store::{Cell, Key, Read, Row, VectorId, VectorNumber};
use crate::types::{Error, Result, Value};

/// What the rows a transaction has staged mean for the partition count of each VECTOR column
/// that has a PARTITION_KEY: for every key tuple a staged row has put a vector under or taken
/// one from, how many vectors the transaction now has there, and for every column, how many
/// partitions it would have if the transaction were committed as it stands.
///
/// It is kept in step with the staged rows, row by row, and is exact for the moment the
/// transaction began at together with what the transaction has staged. A partition that
/// another transaction commits meanwhile is not seen here: [`super::stage`] counts again
/// inside the commit function, over the newest moment.
pub(crate) struct Tally {
    /// Vectors under each key tuple a statement has touched: what the committed moment has
    /// there, changed by what the transaction has staged.
    live: BTreeMap<(VectorId, Key), u64>,
    /// Partitions of each column a statement has touched, counted the same way.
    partitions: BTreeMap<VectorId, u64>,
    /// What the statement that is running has noted, in order, so that it can be undone.
    noted: Vec<Note>,
    /// Never read: holds what the three lists above take.
    reserved: Reservation,
}

/// One vector that a statement put under a key tuple or took from it.
struct Note {
    place: (VectorId, Key),
    /// True for a vector put under the tuple, false for one taken from it.
    arrived: bool,
    /// True when the tuple had no vector until this one arrived.
    began: bool,
}

impl Note {
    /// What a note is counted as while its statement runs.
    fn bytes(&self) -> u64 {
        self.place.1.as_bytes().len() as u64 + 16
    }
}

/// Puts one vector under a key tuple, or takes one from it, and keeps the column's number of
/// partitions in step. True when the tuple had none and now has one.
fn shift(live: &mut u64, partitions: &mut u64, arrives: bool) -> bool {
    if arrives {
        *live += 1;
        *partitions += u64::from(*live == 1);
    } else {
        *live -= 1;
        *partitions -= u64::from(*live == 0);
    }
    arrives && *live == 1
}

impl Tally {
    /// The tally of a transaction that has staged nothing.
    pub(crate) fn new(budget: &Budget) -> Result<Tally> {
        Ok(Tally {
            live: BTreeMap::new(),
            partitions: BTreeMap::new(),
            noted: Vec::new(),
            reserved: budget.reserve(0, "transaction")?,
        })
    }

    /// Notes one row a statement stages in a table with a VECTOR column: `before` is the row as
    /// the transaction had it until now, `None` when there was none, and `after` is the row as
    /// staged, `None` for a removal. For each VECTOR column with a PARTITION_KEY, a vector that
    /// leaves its key tuple, arrives under one, or moves from one to another changes the count
    /// of both tuples; a row whose vector stays under the same tuple, or has none before and
    /// after, changes nothing, and its vector is never loaded.
    ///
    /// `read` is the transaction's change set: a tuple is looked up in the committed moment
    /// the first time the transaction touches it, and a key column kept beside the row is
    /// loaded from it. `write` calls this for every row, before or after it stages the row.
    pub(crate) fn note(
        &mut self,
        table: &Table,
        before: Option<&Row>,
        after: Option<&[Value]>,
        read: &dyn Read,
    ) -> Result<()> {
        for column in &table.vectors {
            if column.partition_key.is_empty() {
                continue;
            }
            let (left, entered) = tuples(table, column, before, after, read, &mut self.reserved)?;
            if left == entered {
                continue;
            }
            for (tuple, arrived) in [(left, false), (entered, true)] {
                if let Some(tuple) = tuple {
                    self.count((column.id, tuple), arrived, read)?;
                }
            }
        }
        Ok(())
    }

    /// Counts one vector more under a key tuple, or one fewer, and notes it. The column's
    /// partitions and the tuple's vectors are read from the committed moment the first time.
    fn count(&mut self, place: (VectorId, Key), arrived: bool, read: &dyn Read) -> Result<()> {
        let (vector, tuple) = (place.0, &place.1);
        let partitions = match self.partitions.entry(vector) {
            Entry::Occupied(counted) => counted.into_mut(),
            Entry::Vacant(first) => {
                first.insert(read.vector_number(VectorNumber::Partitions(vector))?)
            }
        };
        let live = match self.live.entry(place.clone()) {
            Entry::Occupied(counted) => counted.into_mut(),
            Entry::Vacant(first) => {
                let partition = directory::partition(read, vector, tuple)?;
                self.reserved.grow(tuple.as_bytes().len() as u64 + 64)?;
                first.insert(partition.map_or(0, |partition| partition.live))
            }
        };
        // A vector leaves a tuple that has one, in a column that has a partition.
        if !arrived && (*live == 0 || *partitions == 0) {
            return Err(read.damaged(directory::SHORT));
        }
        let mut note = Note {
            place,
            arrived,
            began: false,
        };
        self.reserved.grow(note.bytes())?;
        note.began = shift(live, partitions, arrived);
        self.noted.push(note);
        Ok(())
    }

    /// The last act of a statement that wrote rows. Refuses with
    /// `VectorPartitionLimitExceeded` when the statement put a vector under a key tuple that
    /// had none and the column would then have more partitions than its limit, and forgets
    /// what the statement noted; otherwise keeps it.
    pub(crate) fn settle(&mut self, table: &Table) -> Result<()> {
        for column in &table.vectors {
            let mut notes = self.noted.iter();
            let began = notes.any(|note| note.place.0 == column.id && note.began);
            let partitions = self.partitions.get(&column.id).copied().unwrap_or(0);
            if let Some(limit) = column.limit()
                && began
                && partitions > u64::from(limit)
            {
                self.forget();
                return Err(Error::VectorPartitionLimitExceeded {
                    index: table.vector_ref(column),
                    max_partitions: limit,
                });
            }
        }
        let kept: u64 = self.noted.iter().map(Note::bytes).sum();
        self.reserved.shrink(kept);
        self.noted.clear();
        Ok(())
    }

    /// Undoes what the running statement has noted. `database` calls this beside the rewind
    /// of the change set when a statement is refused, whatever refused it.
    pub(crate) fn forget(&mut self) {
        while let Some(note) = self.noted.pop() {
            if let Some(live) = self.live.get_mut(&note.place)
                && let Some(partitions) = self.partitions.get_mut(&note.place.0)
            {
                shift(live, partitions, !note.arrived);
            }
            self.reserved.shrink(note.bytes());
        }
    }
}

/// Where a column that a table's declaration names is in a row of the table.
pub(super) fn position(table: &Table, name: &str) -> usize {
    let mut columns = table.meta.columns.iter();
    columns
        .position(|column| column.name == name)
        .expect("a VECTOR column and the columns of its PARTITION_KEY are columns of its table")
}

/// The key tuple a row's vector was under in a VECTOR column, and the one it is under now.
///
/// `before` is the row as it was and `after` its values as they are staged; `None` is no row.
/// A row that is not there, and a row whose vector is NULL, has its vector under no tuple.
/// The tuple is the values of the PARTITION_KEY columns as [`Key::from_values`] encodes them,
/// ascending; a column with no PARTITION_KEY has the one tuple of no values. A key column
/// kept beside its row is loaded from `read` through `held`, which is given back what the
/// load took. No vector is loaded.
pub(super) fn tuples(
    table: &Table,
    column: &VectorColumn,
    before: Option<&Row>,
    after: Option<&[Value]>,
    read: &dyn Read,
    held: &mut Reservation,
) -> Result<(Option<Key>, Option<Key>)> {
    let vector = position(table, &column.column);
    let keys = column.partition_key.iter();
    let keys: Vec<usize> = keys.map(|name| position(table, name)).collect();
    let tuple = |values: Result<Vec<Value>>| Key::from_values(&values?, &[]).map(Some);
    let was = match before {
        Some(row) if !matches!(row.cells[vector], Cell::Here(Value::Null)) => {
            let taken = held.bytes();
            let values = keys.iter().map(|&at| match &row.cells[at] {
                Cell::Here(value) => Ok(value.clone()),
                Cell::Beside(beside) => read.load(beside, held),
            });
            let was = tuple(values.collect());
            held.shrink(held.bytes() - taken);
            was?
        }
        _ => None,
    };
    let is = match after {
        Some(values) if values[vector] != Value::Null => {
            tuple(Ok(keys.iter().map(|&at| values[at].clone()).collect()))?
        }
        _ => None,
    };
    Ok((was, is))
}
