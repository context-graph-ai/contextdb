//! A similarity search: the rows of a table that pass the statement's filter and lie nearest a
//! vector. This part settles what is searched for, which partitions the filter selects and
//! which rows may be seen, and works out each row's distance from its stored vector; the vector
//! part finds the rows.

use std::collections::HashMap;

use super::access::{Source, as_key};
use super::expression::{column_index, eval, reads};
use super::{QueryResult, names};
use crate::memory::Budget;
use crate::schema::{Catalog, Table};
use crate::sql::ast::{BinOp, ColumnRef};
use crate::sql::{Expr, Plan};
use crate::store::{Key, Read};
use crate::types::{ColumnType, Error, Result, Value, VectorIndexRef, VectorSearchMode};
use crate::vector::{self, Nearest};

/// The most key tuples a filter may name; past that the search reads every partition.
const MOST_TUPLES: usize = 4096;

/// Answers a planned similarity search, a `VectorSearch` under the select list unless the
/// statement is `SELECT *`: the `k` rows that pass the filter and have a vector nearest the one
/// searched for, nearest first, rows at one distance in key order. `None` for any other plan.
///
/// The column, the mode, the vector and every name of the statement are settled before a row
/// is read, so a refusal does not depend on what the table holds. Then the vector part searches
/// the partitions the filter selects and asks, row by row, for the distance: the row is read,
/// the whole filter applied, and the distance worked out from the row's stored vector. Each row
/// of the answer is read once more for the select list, in which every `<=>` is the distance
/// the row was ordered by. Every one of those reads is counted.
pub(super) fn run(
    plan: &Plan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    read: &dyn Read,
    budget: &Budget,
    now: i64,
) -> Result<Option<QueryResult>> {
    let (plan, list) = match plan {
        Plan::Project { input, columns } => (&**input, Some(columns.as_slice())),
        plan => (plan, None),
    };
    let Plan::VectorSearch {
        table,
        column,
        query,
        k,
        filter,
        mode,
    } = plan
    else {
        return Ok(None);
    };
    let filter = filter.as_ref();
    let table = catalog
        .table(table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;
    let named = ColumnRef {
        table: None,
        column: column.clone(),
    };
    let at = column_index(table, &named)?;
    let index = VectorIndexRef {
        table: table.name.clone(),
        column: named.column,
    };
    let Some(vector) = table.vector(column) else {
        return Err(Error::UnknownVectorIndex { index });
    };
    if mode.unwrap_or_else(|| vector.mode()) == VectorSearchMode::Indexed {
        return Err(Error::VectorIndexedRouteUnavailable { index });
    }
    let near = match eval(query, table, &[], params, now)? {
        Value::Vector(vector) => Some(vector),
        Value::Text(text) => vector_from_text(&text),
        _ => None,
    };
    let Some(near) = near else {
        return Err(Error::PlanError(
            "the right side of <=> is not a vector: write [1.0, 0.0], its text '[1, 0]', or a \
             $name bound to a vector"
                .to_owned(),
        ));
    };
    let actual = near.len();
    if let ColumnType::Vector(expected) = table.meta.columns[at].column_type
        && expected != actual
    {
        return Err(Error::VectorIndexDimensionMismatch {
            index,
            expected,
            actual,
        });
    }
    if let Some(position) = near.iter().position(|number| !number.is_finite()) {
        return Err(Error::InvalidVectorValue { index, position });
    }

    let width = table.meta.columns.len();
    let (mut filtered, mut wanted) = (vec![false; width], vec![false; width]);
    if let Some(filter) = filter {
        reads(filter, table, params, &mut filtered)?;
    }
    match list {
        None => wanted.fill(true),
        Some(list) => {
            for column in list {
                reads(&column.expr, table, params, &mut wanted)?;
            }
        }
    }
    let columns = names(table, list, false);
    let k = usize::try_from(*k).unwrap_or(usize::MAX);
    if k == 0 {
        return Ok(Some(QueryResult::from_rows(columns, Vec::new())));
    }

    let key = vector.partition_key.iter();
    let key = key.filter_map(|name| table.meta.columns.iter().position(|c| c.name == *name));
    let key = key.collect();
    let scope = Scope {
        table,
        key,
        params,
        now,
    }
    .keys(filter)?;
    // A row the vector part asks about is read for the filter and its vector alone; a row of
    // the answer for what the select list reads, so a large value is loaded only then.
    let mut its_vector = vec![false; width];
    its_vector[at] = true;
    let asked = Source::new(table, filter, params, now, read, filtered, &its_vector);
    let returned = Source::new(table, None, params, now, read, vec![false; width], &wanted);
    // What the vector part holds is given back once the rows are built; the rows stay counted
    // with the result.
    let mut searching = budget.reserve(0, "query")?;
    let mut reservation = budget.reserve(0, "query")?;
    let mut examined = 0;

    let nearest = Nearest {
        table: table.id,
        vector: vector.id,
        scope: &scope,
        query: &near,
        k,
    };
    let mut distance_of = |key: &Key| -> Result<Option<f64>> {
        examined += 1;
        let Some((row, bytes)) = asked.row(key, &mut reservation)? else {
            return Ok(None);
        };
        let distance = match &row[at] {
            Value::Vector(stored) => Some(vector::distance(stored, &near)),
            _ => None,
        };
        reservation.shrink(bytes);
        Ok(distance)
    };
    let hits = vector::search(&nearest, read, &mut searching, &mut distance_of)?;

    let moved = returned.moved(list)?;
    let mut rows = Vec::new();
    for hit in &hits {
        examined += 1;
        if let Some((row, bytes)) = returned.row(&hit.key, &mut reservation)? {
            let distance = Some(hit.distance);
            rows.push(returned.output(list, &moved, row, bytes, distance, &mut reservation)?);
        }
    }
    Ok(Some(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        rows_examined: examined,
        _reservation: Some(reservation),
    }))
}

/// The values a filter fixes for the PARTITION_KEY columns, in their order; `None` for a
/// column it leaves open.
type Tuple = Vec<Option<Value>>;

/// Reads a filter for the partitions it selects.
struct Scope<'a> {
    table: &'a Table,
    /// Where each PARTITION_KEY column is in a row, in declared order.
    key: Vec<usize>,
    params: &'a HashMap<String, Value>,
    now: i64,
}

impl Scope<'_> {
    /// The partitions to search: for each key tuple the filter names, the key of the leading
    /// key columns it fixes, which every partition with those values begins with. Empty for
    /// every partition: no filter, a tuple that fixes no leading column, or more than 4,096
    /// tuples. It may select too much and never too little: the whole filter is still applied
    /// to every row.
    fn keys(&self, filter: Option<&Expr>) -> Result<Vec<Key>> {
        let (Some(filter), false) = (filter, self.key.is_empty()) else {
            return Ok(Vec::new());
        };
        let tuples = self.said(filter)?;
        if tuples.len() > MOST_TUPLES {
            return Ok(Vec::new());
        }
        let mut keys = Vec::with_capacity(tuples.len());
        for tuple in tuples {
            let leading: Vec<Value> = tuple.into_iter().map_while(|value| value).collect();
            if leading.is_empty() {
                return Ok(Vec::new());
            }
            keys.push(Key::from_values(&leading, &[])?);
        }
        // A partition is named once, and not again under a key that begins as another does.
        keys.sort();
        keys.dedup_by(|later, kept| later.starts_with(kept));
        Ok(keys)
    }

    /// What a filter says about the key columns: a row passes it only under one of these
    /// tuples. `AND` joins what its two sides say, `OR` gives the tuples of both, and
    /// `column = value`, `column IN (values)` and `(columns) IN (row values)` say which values;
    /// anything else leaves every column open.
    fn said(&self, filter: &Expr) -> Result<Vec<Tuple>> {
        let open = || vec![vec![None; self.key.len()]];
        Ok(match filter {
            Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
                let (left, right) = (self.said(left)?, self.said(right)?);
                if left.len().saturating_mul(right.len()) > MOST_TUPLES {
                    return Ok(open());
                }
                let join = |left: &Tuple, right: &Tuple| -> Tuple {
                    let both = left.iter().zip(right);
                    both.map(|(left, right)| left.clone().or_else(|| right.clone()))
                        .collect()
                };
                let pairs = left.iter().flat_map(|l| right.iter().map(move |r| (l, r)));
                pairs.map(|(left, right)| join(left, right)).collect()
            }
            Expr::BinaryOp { left, op, right } if *op == BinOp::Or => {
                [self.said(left)?, self.said(right)?].concat()
            }
            Expr::BinaryOp { left, op, right } if *op == BinOp::Eq => {
                let (column, value) = match **left {
                    Expr::Column(_) => (left, right),
                    _ => (right, left),
                };
                vec![self.tuple(sides(column), sides(value))?]
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let tuples = list.iter();
                let tuples = tuples.map(|item| self.tuple(sides(expr), sides(item)));
                tuples.collect::<Result<_>>()?
            }
            _ => open(),
        })
    }

    /// The tuple `columns = values` fixes, pair by pair. A pair counts when it is a key column
    /// beside a literal or a `$name` whose value a key of the column's type holds; any other
    /// pair leaves its column open.
    fn tuple(&self, columns: &[Expr], values: &[Expr]) -> Result<Tuple> {
        let mut tuple = vec![None; self.key.len()];
        for pair in columns.iter().zip(values) {
            let (Expr::Column(column), value @ (Expr::Literal(_) | Expr::Parameter(_))) = pair
            else {
                continue;
            };
            let at = column_index(self.table, column)?;
            if let Some(place) = self.key.iter().position(|key| *key == at) {
                let value = eval(value, self.table, &[], self.params, self.now)?;
                tuple[place] = as_key(self.table.meta.columns[at].column_type, value);
            }
        }
        Ok(tuple)
    }
}

/// The expressions of one side of `=` or IN: those of a row value, or the one expression.
fn sides(expr: &Expr) -> &[Expr] {
    match expr {
        Expr::Row(items) => items,
        one => std::slice::from_ref(one),
    }
}

/// Reads the text spelling of a vector, `[0.1, 0.2, 0.3]`: the one reader of it, for a value
/// written into a VECTOR column and for the vector of a search.
pub(crate) fn vector_from_text(text: &str) -> Option<Vec<f32>> {
    let inside = text.trim().strip_prefix('[')?.strip_suffix(']')?;
    inside
        .split(',')
        .map(|number| number.trim().parse().ok())
        .collect()
}
