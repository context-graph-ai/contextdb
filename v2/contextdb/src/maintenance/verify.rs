//! Inspection of one committed view, using the store's decoded record walk.
use std::collections::BTreeMap;
use std::ops::Bound::Included;

use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Table};
use crate::store::{Cell, CountId, Entry, Key, READING, Read, Record, Row, View};
use crate::types::{ContentSize, Kind, Result, Value, VerifyReport};
use crate::vector::verify_record;

pub(crate) fn verify(
    view: &View,
    catalog: &Catalog,
    budget: &Budget,
) -> Result<crate::Owned<VerifyReport>> {
    let tables = catalog.tables();
    let mut held = budget.reserve(0, "verify")?;
    let mut answer = budget.reserve(0, "verify answer")?;
    let mut report = VerifyReport::default();
    let mut indexes = BTreeMap::new();
    let mut counts = BTreeMap::new();
    let mut vectors = BTreeMap::new();
    for (at, table) in tables.iter().enumerate() {
        answer.room(&mut report.tables, usize::MAX)?;
        answer.grow(table.name.len() as u64)?;
        report.tables.push(ContentSize {
            table: table.name.clone(),
            ..ContentSize::default()
        });
        for (index, id) in table.index_ids.iter().enumerate() {
            held.grow(64)?;
            indexes.insert(*id, (at, index, 0u64, 0u64));
        }
        for count in &table.rules.counts {
            held.grow(96)?;
            counts.insert(count.id, (Key::from_bytes(&[]), 0u64, 0u64, 0u64));
        }
        for column in &table.vectors {
            held.grow(64)?;
            let owners = crate::vector::owners(view, column, table.dimension(column), &mut held);
            READING.set(None);
            vectors.insert(column.id, (at, column, owners?));
        }
    }
    let mut rows = vec![0u64; tables.len()];
    let mut cover = budget.passing("verify row")?;
    view.records(budget, |record, bytes| {
        let missing = || view.damaged("a record has no declared owner");
        let mut loaded = budget.reserve(0, "verify record")?;
        match record {
            Record::Row(id, value, large) => {
                let at = tables.iter().position(|t| t.id == id).ok_or_else(missing)?;
                let table = tables[at];
                if value.cells.len() > table.meta.columns.len() {
                    return Err(view.damaged("a row has undeclared cells"));
                }
                for cell in &value.cells {
                    if let Cell::Beside(beside) = cell {
                        view.load(beside, &mut loaded)?;
                        loaded.shrink(loaded.bytes());
                    }
                }
                for (index, id) in table.index_ids.iter().enumerate() {
                    if index_name(table, index).starts_with("__pk_") {
                        continue;
                    }
                    indexes.get_mut(id).unwrap().2 +=
                        u64::from(entry(view, table, index, &value, &mut loaded)?.1.is_some());
                    loaded.shrink(loaded.bytes());
                }
                rows[at] += 1;
                report.tables[at].rows += bytes;
                report.tables[at].large_values += large;
            }
            Record::Version(id, value) => {
                let at = tables.iter().position(|t| t.id == id).ok_or_else(missing)?;
                if value.cells.len() > tables[at].meta.columns.len() {
                    return Err(view.damaged("a version has undeclared cells"));
                }
                report.tables[at].versions += bytes;
            }
            Record::Index(id, joined, (length, cut)) => {
                let &(at, index, ..) = indexes.get(&id).ok_or_else(missing)?;
                let table = tables[at];
                let name = index_name(table, index);
                let damage = || view.damaged(&format!("index {name} disagrees with its rows"));
                let key = Key::from_bytes(&joined.as_bytes()[length..]);
                let row = view.row(table.id, &key, &mut cover)?.ok_or_else(damage)?;
                let (values, expected) = entry(view, table, index, &row, &mut loaded)?;
                if expected.is_none_or(|e| {
                    e.key.as_bytes() != &joined.as_bytes()[..length] || e.cut != cut
                }) {
                    return Err(damage());
                }
                indexes.get_mut(&id).unwrap().3 += 1;
                for count in &table.rules.counts {
                    if count.index != index {
                        continue;
                    }
                    let ways: Vec<_> = table
                        .count_columns(count)
                        .iter()
                        .map(|(_, way)| *way)
                        .collect();
                    let key = Key::from_values(&values[..ways.len()], &ways)?;
                    let (old, rows, ended, _) = counts.get_mut(&count.id).unwrap();
                    if *old != key {
                        if *rows > 0 {
                            ended_run(view, count.id, old, *rows)?;
                            *ended += 1;
                        }
                        held.grow(key.as_bytes().len() as u64)?;
                        held.shrink(old.as_bytes().len() as u64);
                        *old = key;
                        *rows = 0;
                    }
                    *rows += 1;
                }
                report.tables[at].indexes += bytes;
            }
            Record::Delivery(key, value) => {
                crate::events::validate(view, key, value, &mut loaded).map(drop)?
            }
            Record::Audit(value) => {
                serde_json::from_slice::<crate::TriggerAuditEntry>(value)
                    .map_err(|_| view.damaged("an invalid audit record"))?;
            }
            Record::Schedule(name, _) => {
                if !catalog.events.contains_key(&(Kind::Schedule, name.into())) {
                    return Err(view.damaged(&format!("undeclared schedule cursor '{name}'")));
                }
            }
            Record::Count(id) => {
                counts.get_mut(&id).ok_or_else(missing)?.3 += 1;
            }
            Record::Vector(id, block, record, value) => {
                let (at, column, owners) = vectors.get(&id).ok_or_else(missing)?;
                let v = verify_record(view, tables[*at], column, owners, record, value, &mut cover);
                READING.set(None);
                v?;
                if block {
                    report.tables[*at].blocks += bytes;
                } else {
                    report.tables[*at].vectors += bytes;
                }
            }
        }
        Ok(())
    })?;
    for (at, table) in tables.iter().enumerate() {
        if rows[at] != view.count(table.id)? {
            let name = &table.name;
            return Err(view.damaged(&format!("row count of {name} differs from its rows")));
        }
        for (index, id) in table.index_ids.iter().enumerate() {
            let (_, _, expected, actual) = indexes[id];
            if expected != actual {
                let name = index_name(table, index);
                return Err(view.damaged(&format!("index {name} has a different entry count")));
            }
        }
        for count in &table.rules.counts {
            let (key, rows, ended, records) = &counts[&count.id];
            if *rows > 0 {
                ended_run(view, count.id, key, *rows)?;
            }
            if *ended + u64::from(*rows > 0) != *records {
                let id = count.id;
                return Err(view.damaged(&format!("kept count {id:?} has a group with no rows")));
            }
        }
    }
    Ok(crate::Owned::charged(report, answer))
}

fn ended_run(view: &View, id: CountId, key: &Key, rows: u64) -> Result<()> {
    let mut found = view.kept(id, Included(key), Included(key))?;
    if found.next().transpose()?.is_none_or(|(_, n)| n != rows) {
        return Err(view.damaged(&format!("kept count {id:?} differs from its rows")));
    }
    Ok(())
}

fn entry(
    view: &View,
    table: &Table,
    index: usize,
    row: &Row,
    held: &mut Reservation,
) -> Result<(Vec<Value>, Option<Entry>)> {
    if index == table.meta.indexes.len() {
        return Ok((Vec::new(), table.retention_entry(row)));
    }
    let columns = &table.meta.indexes[index].columns;
    let values = columns.iter().map(|(name, _)| {
        let cell = table.cell(row, table.column(name)?);
        match cell.into_owned() {
            Cell::Here(value) => Ok(value),
            Cell::Beside(beside) => view.load(&beside, held),
        }
    });
    let values = values.collect::<Result<Vec<_>>>()?;
    let entry = table.entry(index, &values)?;
    Ok((values, entry))
}

fn index_name(table: &Table, index: usize) -> &str {
    table
        .meta
        .indexes
        .get(index)
        .map_or("write time", |i| &i.name)
}
