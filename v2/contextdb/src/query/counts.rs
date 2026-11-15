//! A `COUNT(*)` answered with no row read.

use std::ops::Bound;

use super::expression::{EvalContext, Scope};
use super::table::collect;
use super::visible::Reader;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::BinOp;
use crate::store::{Key, Keys, Read};
use crate::types::{Result, SortDirection, Value};

/// The number of rows of `table` that pass `filter`, when it is known with no row read: the
/// table's own count with no filter; one kept number when the filter gives one value for each
/// column of a kept count; otherwise the number of entries of the one index range the filter
/// gives, when it gives one value for each of an index's leading columns and nothing else.
///
/// When the statement is explained the choice is said and no number is read: the number is 0.
///
/// `None` when the rows have to be read: for a reader that is not shown every row, for a
/// filter that says more, for a table the transaction has staged a row of, and for a value too
/// long for its index entry, which is there as a digest.
pub(crate) fn count_without_rows(
    table: &Table,
    scope: &Scope,
    filter: Option<&Expr>,
    reader: &Reader,
    ctx: &EvalContext,
    read: &dyn Read,
) -> Result<Option<u64>> {
    if !reader.is_admin() {
        return Ok(None);
    }
    let said = |line: &dyn Fn() -> String| ctx.said.map(|said| said.borrow_mut().push(line()));
    let counted = |line: &dyn Fn() -> String, of: &dyn Fn() -> Result<u64>| {
        said(line).map_or_else(of, |()| Ok(0)).map(Some)
    };
    let Some(filter) = filter else {
        return counted(&|| used(table, &[]), &|| read.count(table.id));
    };
    let mut terms = Vec::new();
    if !collect(table, scope, filter, ctx, &mut terms)? {
        return Ok(None);
    }
    // Each column the filter names is given one value, once.
    let mut pins: Vec<(usize, &Value)> = Vec::new();
    for term in &terms {
        let [value] = term.values.as_slice() else {
            return Ok(None);
        };
        if term.op != BinOp::Eq || pins.iter().any(|pin| pin.0 == term.column) {
            return Ok(None);
        }
        pins.push((term.column, value));
    }
    let mut staged = false;
    read.staged(table.id, &mut |_| {
        staged = true;
        Ok(false)
    })?;
    if staged {
        return Ok(None);
    }
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
        let values = given(&mut count.columns.iter().map(|column| column.0));
        if let Some(values) = values.filter(|values| values.len() == pins.len()) {
            let directions: Vec<_> = count.columns.iter().map(|column| column.1).collect();
            let key = Key::from_values(&values, &directions)?;
            return counted(&|| used(table, &count.columns), &|| {
                let mut kept = read.kept(count.id, Bound::Included(&key), Bound::Included(&key))?;
                // A group with no row has no record.
                Ok(kept.next().transpose()?.map_or(0, |(_, rows)| rows))
            });
        }
    }
    let columns = &table.meta.columns;
    for (at, index) in table.meta.indexes.iter().enumerate() {
        let leading = index.columns.iter().take(pins.len());
        let mut leading =
            leading.filter_map(|(name, _)| columns.iter().position(|c| c.name == *name));
        // The key's own index has no entries, and an index over a JSON path none of a column.
        let holds = !index.name.starts_with("__pk_") && index.json_path.is_empty();
        if let Some(values) =
            given(&mut leading).filter(|values| holds && values.len() == pins.len())
        {
            let entry = table.entry_shape(at).entry(&values)?;
            if entry.cut.is_some() {
                return Ok(None);
            }
            let given = names(table, pins.iter().map(|pin| pin.0));
            said(&|| format!("IndexScan {{ index: {} }}", index.name));
            said(&|| format!("  predicates_pushed: [{given}]"));
            return counted(&|| undeclared(table, &given), &|| {
                let (id, to) = (table.index_ids[at], entry.key.prefix_end());
                let to = to.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
                read.counted(Keys::Index(id), Bound::Included(&entry.key), to, usize::MAX)
            });
        }
    }
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
        let columns = names(table, count.columns.iter().map(|column| column.0));
        format!("({columns}), ")
    });
    let (name, kept) = (&table.name, kept.collect::<String>());
    format!(
        "maintained count: not declared \u{2014} ALTER TABLE {name} SET MAINTAIN COUNT {kept}({wanted}) makes this O(1)"
    )
}
