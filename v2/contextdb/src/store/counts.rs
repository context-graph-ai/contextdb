//! Kept counts: one record for each group of a count, under the count's number and the group's
//! values as a key, holding how many rows the group has. A count that reaches zero has no
//! record.

use std::cmp::Ordering;

use redb::{ReadableTable, Table};

use super::disk::{Stored, unsound};
use super::keys::{Span, stored};
use super::{ChangeSet, CountId, KeptIter, Key, Staged, Undo, staged_in};
use crate::types::Result;

impl ChangeSet {
    /// Stages a change to one record of a kept count.
    pub(crate) fn add_kept(&mut self, count: CountId, key: Key, delta: i64) -> Result<()> {
        let bytes = self.counted(&key, 0)?;
        *self
            .kept
            .entry(count.0)
            .or_default()
            .entry(key.clone())
            .or_default() += delta;
        self.calls.push((Undo::Kept(count.0, key, delta), bytes));
        Ok(())
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
    let mut stored = changes.base.moment.kept(count.0, span)?.peekable();
    let mut staged = staged_in(&changes.kept, count.0, span.clone()).peekable();
    Ok(Box::new(std::iter::from_fn(move || {
        loop {
            let stored_is = match (stored.peek(), staged.peek()) {
                (None, None) => return None,
                (Some(Ok((key, _))), Some((staged_key, _))) => key.cmp(staged_key),
                // A failed read, or nothing staged from here on.
                (Some(_), _) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
            };
            let before = match stored_is {
                Ordering::Less => return stored.next(),
                Ordering::Equal => stored.next()?.map_or(0, |(_, count)| count),
                Ordering::Greater => 0,
            };
            let (key, change) = staged.next()?;
            match before.saturating_add_signed(*change) {
                0 => {}
                count => return Some(Ok((key.clone(), count))),
            }
        }
    })))
}

/// Writes a commit's changes to the kept counts.
pub(super) fn write(
    staged: &Staged<i64>,
    counts: &mut Table<'_, &'static [u8], &'static [u8]>,
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
                count => drop(counts.insert(key.as_slice(), count.to_le_bytes().as_slice())?),
            }
        }
    }
    Ok(())
}
