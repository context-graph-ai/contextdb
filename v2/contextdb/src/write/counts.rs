//! Kept counts: for each list of columns a table declared with MAINTAIN COUNT, the number of
//! its rows under each set of values of those columns.
//!
//! A count changes with the row: where a row is written and where a row is removed, and
//! nowhere else. It is never worked out by reading rows when a row is written; the rows are
//! read once, when a count is declared on a table that has them.

use std::collections::HashMap;

use super::{Given, Rules, pick, split};
use crate::memory::Budget;
use crate::query::Reader;
use crate::schema::Catalog;
use crate::store::{ChangeSet, CountId};
use crate::types::{Result, SortDirection, TxId, Value};

impl Rules<'_> {
    /// Stages what one row means for every count its table keeps: one more under the values
    /// of `row`, the row as it is written, and one fewer under the values of `was`, the row as
    /// it was. A new row has no `was` and a removed row no `row`. A count over columns in
    /// which the two agree is left alone.
    pub(super) fn count(
        &self,
        changes: &mut ChangeSet,
        was: Option<&[Value]>,
        row: Option<&[Value]>,
    ) -> Result<()> {
        for (at, (columns, _)) in self.counts.iter().enumerate() {
            if let (Some(was), Some(row)) = (was, row)
                && columns.iter().all(|&column| was[column] == row[column])
            {
                continue;
            }
            for (values, by) in [(row, 1), (was, -1)] {
                if let Some(values) = values {
                    self.add(changes, at, values, by)?;
                }
            }
        }
        Ok(())
    }

    /// Adds `by` to the count at this place among the table's counts, under the values `row`
    /// has in the count's columns.
    fn add(&self, changes: &mut ChangeSet, at: usize, row: &[Value], by: i64) -> Result<()> {
        let (columns, ways) = &self.counts[at];
        // The count's list as declared.
        let listed = || {
            let named = columns.iter().zip(ways).map(|(&at, way)| {
                let name = self.table.meta.columns[at].name.clone();
                match way {
                    SortDirection::Asc => name,
                    SortDirection::Desc => name + " DESC",
                }
            });
            named.collect::<Vec<_>>().join(", ")
        };
        let key = self.identity(listed, &pick(columns, row), ways)?;
        changes.add_kept(self.table.rules.counts[at].id, key, by)
    }
}

/// Stages the records of counts a declaration has just made, for the rows their table already
/// has: the rows are read once for all of them, a batch at a time, so that what is held here
/// does not grow with the table. The records are counted against the change set like any
/// staged change, and counts that do not fit the memory limit are refused there.
pub(crate) fn fill_kept_count(
    counts: &[CountId],
    catalog: &Catalog,
    changes: &mut ChangeSet,
    budget: &Budget,
) -> Result<()> {
    let (params, reader) = (HashMap::new(), Reader::admin());
    let given = Given {
        params: &params,
        catalog,
        budget,
        reader: &reader,
        now: 0,
        number: TxId(0),
    };
    for table in catalog.tables() {
        let kept = table.rules.counts.iter().enumerate();
        let made = kept.filter(|(_, count)| counts.contains(&count.id));
        let made: Vec<usize> = made.map(|(at, _)| at).collect();
        if made.is_empty() {
            continue;
        }
        let rules = Rules::of(table, &given)?;
        let mut needed = vec![false; table.meta.columns.len()];
        for &column in made.iter().flat_map(|&at| &rules.counts[at].0) {
            needed[column] = true;
        }
        let asked = rules.asked(None, &params, &[], &needed, &reader);
        rules.each(&asked, changes, |changes, _, cells| {
            let row = split(cells).0;
            made.iter()
                .try_for_each(|&at| rules.add(changes, at, &row, 1))
        })?;
    }
    Ok(())
}
