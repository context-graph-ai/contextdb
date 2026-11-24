//! A `COUNT(*)` answered with no row read.

use std::ops::Bound;

use super::expression::{EvalContext, Scope};
use super::filter::{Columns, pins, read as reading};
use super::visible::Reader;
use crate::schema::Table;
use crate::sql::Expr;
use crate::store::{Key, Read};
use crate::types::{Result, SortDirection, Value};

/// The number of rows of `table` that pass `filter`, when it is known with no row read: the
/// table's own count with no filter; one kept number when the filter gives one value for each
/// column of a kept count, and says nothing else.
///
/// When the statement is explained the choice is said and no number is read: the number is 0.
///
/// `None` otherwise, said as the count that would answer when the filter gives values alone:
/// the path the statement takes then counts the rows, by their keys or entries when it can.
pub(crate) fn count_without_rows(
    table: &Table,
    scope: &Scope,
    filter: Option<&Expr>,
    reader: &Reader,
    ctx: &EvalContext,
    read: &dyn Read,
) -> Result<Option<u64>> {
    let said = |line: &dyn Fn() -> String| ctx.said.map(|said| said.borrow_mut().push(line()));
    let declined = |why: &str| -> Result<Option<u64>> {
        let line = || format!("maintained count: not used \u{2014} {why}");
        if !table.rules.counts.is_empty() {
            said(&line);
        }
        Ok(None)
    };
    if !reader.is_admin() {
        return declined("this reader is narrowed");
    }
    let counted = |line: &dyn Fn() -> String, of: &dyn Fn() -> Result<u64>| {
        said(line).map_or_else(of, |()| Ok(0)).map(Some)
    };
    let Some(filter) = filter else {
        return counted(&|| used(table, &[]), &|| read.count(table.id));
    };
    let columns = Columns {
        scope,
        table: Some(table),
    };
    let reading = reading(Some(filter), &columns, ctx)?;
    // Each column the filter names is given one value, and it says nothing else.
    let pinned = pins(&reading).and_then(|pins| {
        let given = pins.into_iter().map(|(c, value)| Some((c, value?)));
        given.collect::<Option<Vec<_>>>()
    });
    let Some(pins) = pinned else {
        return declined("the filter does not give one value to each column");
    };
    // The values given for these columns, when every one of them is given.
    let given = |columns: &mut dyn Iterator<Item = usize>| -> Option<Vec<Value>> {
        let value = |column| {
            pins.iter()
                .find(|pin| pin.0 == column)
                .map(|pin| pin.1.clone())
        };
        columns.map(value).collect()
    };

    for count in &table.rules.counts {
        let columns = table.count_columns(count);
        let values = given(&mut columns.iter().map(|column| column.0));
        if let Some(values) = values.filter(|values| values.len() == pins.len()) {
            let directions: Vec<_> = columns.iter().map(|column| column.1).collect();
            let key = Key::from_values(&values, &directions)?;
            return counted(&|| used(table, columns), &|| {
                let mut kept = read.kept(count.id, Bound::Included(&key), Bound::Included(&key))?;
                // A group with no row has no record.
                Ok(kept.next().transpose()?.map_or(0, |(_, rows)| rows))
            });
        }
    }
    said(&|| undeclared(table, &names(table, pins.iter().map(|pin| pin.0))));
    Ok(None)
}

/// The names of `columns` of `table`, between commas.
pub(super) fn names(table: &Table, columns: impl Iterator<Item = usize>) -> String {
    let names: Vec<&str> = columns
        .map(|at| table.meta.columns[at].name.as_str())
        .collect();
    names.join(", ")
}

/// The line of an explanation for a count the kept count over `columns` answers.
pub(super) fn used(table: &Table, columns: &[(usize, SortDirection)]) -> String {
    let columns = names(table, columns.iter().map(|column| column.0));
    format!("maintained count: used ({columns})")
}

/// The line of an explanation for a count that no kept count answers: the statement that would
/// declare one with the columns `wanted`, beside the lists the table keeps already.
pub(super) fn undeclared(table: &Table, wanted: &str) -> String {
    let kept = table.rules.counts.iter().map(|count| {
        let columns = names(
            table,
            table.count_columns(count).iter().map(|column| column.0),
        );
        format!("({columns}), ")
    });
    let (name, kept) = (&table.name, kept.collect::<String>());
    format!(
        "maintained count: not declared \u{2014} ALTER TABLE {name} SET MAINTAIN COUNT {kept}({wanted}) makes this O(1)"
    )
}
