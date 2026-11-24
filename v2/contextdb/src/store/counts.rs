//! Kept counts: one record for each group of a count, under the count's number and the group's
//! values as a key, holding how many rows the group has. A count that reaches zero has no
//! record.

use redb::{ReadableTable, Table};

use super::disk::{Stored, unsound};
use super::keys::{Span, stored};
use super::{ChangeSet, CountId, KeptIter, Key, Staged, Undo, overlay, staged_in};
use crate::types::Result;

impl ChangeSet {
    /// Stages a change to one record of a kept count.
    pub(crate) fn add_kept(&mut self, count: CountId, key: Key, delta: i64) -> Result<()> {
        let bytes = self.counted(&key, 0)?;
        let counts = self.kept.entry(count.0).or_default();
        let before = counts.get(&key).copied();
        *counts.entry(key.clone()).or_default() += delta;
        self.remember((Undo::Kept(count.0, key, before), bytes))
    }
}

/// A kept count as its record holds it; `None` for a record that is not eight bytes.
pub(super) fn read(record: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(record.try_into().ok()?))
}

/// The records of a kept count inside a span with a transaction's staged changes added to
/// them, in key order; a count the changes bring to zero is left out.
pub(super) fn kept<'a>(
    changes: &'a ChangeSet,
    count: CountId,
    span: &Span,
) -> Result<KeptIter<'a>> {
    let stored = changes.base.moment.kept(count.0, span)?;
    let staged = staged_in(&changes.kept, count.0, span.clone(), false);
    let laid = |before: Option<u64>, change: &i64| {
        Ok(Some(before.unwrap_or(0).saturating_add_signed(*change)).filter(|&count| count != 0))
    };
    Ok(overlay(stored, staged, false, laid))
}

/// Writes a commit's changes to the kept counts.
pub(super) fn write(
    staged: &Staged<i64>,
    counts: &mut Table<'_, &'static [u8], &'static [u8]>,
    write: &redb::WriteTransaction,
) -> Stored<()> {
    for (count, staged) in staged {
        for (key, change) in staged {
            let key = stored(*count, &key.0);
            let before = match counts.get(key.as_slice())? {
                Some(record) => read(record.value()).ok_or_else(unsound)?,
                None => 0,
            };
            match before.saturating_add_signed(*change) {
                0 => drop(counts.remove(key.as_slice())?),
                count => counts.insert_discard(key.as_slice(), count.to_le_bytes().as_slice())?,
            }
            super::disk::checkpoint(write)?;
        }
    }
    Ok(())
}
