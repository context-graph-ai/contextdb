//! Reads a SELECT: the SELECTs WITH names, each SELECT of a UNION, and the clauses over the whole.

use super::expr::{expr, exprs, sort_keys, star};
use super::graph::graph_table;
use super::{Pair, Rule, child, first_ident, number, search_mode, where_clause};
use crate::sql::ast::{ColumnRef, Expr, FromItem, Join, Literal, Select, SelectColumn};
use crate::sql::not_supported;
use crate::types::{Error, Result};

pub(super) fn select(pair: Pair<'_>) -> Result<Select> {
    let at = pair.as_span().start();
    let mut with = Vec::new();
    let mut whole: Option<Select> = None;
    let mut all = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::with_clause => {
                if child(part.clone(), Rule::recursive).is_some() {
                    return Err(Error::RecursiveCteNotSupported);
                }
                for cte in part.into_inner().filter(|part| part.as_rule() == Rule::cte) {
                    let named = child(cte.clone(), Rule::select_stmt).expect("a SELECT");
                    with.push((first_ident(cte), select(named)?));
                }
            }
            Rule::set_op => {
                let word = part.as_str().to_ascii_uppercase();
                if word != "UNION" {
                    return Err(not_supported(&word));
                }
                all = false;
            }
            Rule::all => all = true,
            Rule::select_core => {
                let read = core(part)?;
                match &mut whole {
                    Some(first) => first.union.push((all, read)),
                    None => whole = Some(read),
                }
            }
            _ => clause(part, whole.as_mut().expect("a SELECT before its clauses"))?,
        }
    }
    let mut whole = whole.expect("the grammar's SELECT rule holds one SELECT");
    (whole.with, whole.at.0) = (with, at);
    // A number where a key stands is not a column's place here, and is refused as no key.
    let keys = whole.order_by.iter().map(|key| &key.expr);
    let arms = std::iter::once(&whole).chain(whole.union.iter().map(|(_, arm)| arm));
    let mut keys = keys.chain(arms.flat_map(|arm| &arm.group_by));
    if keys.any(|key| matches!(key, Expr::Literal(Literal::Integer(_)))) {
        return Err(not_supported("a column's number in ORDER BY or GROUP BY"));
    }
    // The ORDER BY of a UNION names the columns of the whole, which hold every SELECT's values:
    // there a name is never the first SELECT's expression.
    if whole.union.is_empty() {
        for key in &mut whole.order_by {
            key.expr = resolve_output_alias(&key.expr, &whole.columns)?;
        }
    }
    Ok(whole)
}

/// Reads a bare ORDER BY name as a name of the result first, as Postgres does: the expression
/// of the items the result calls by that name is sorted by in its place, and two items of that
/// name that differ make it ambiguous. A qualified name, and one no item has, is left as written.
fn resolve_output_alias(expr: &Expr, columns: &[SelectColumn]) -> Result<Expr> {
    let Expr::Column(name @ ColumnRef { table: None, .. }) = expr else {
        return Ok(expr.clone());
    };
    let mut items = columns.iter().filter(|item| item.name() == name.column);
    let Some(item) = items.next() else {
        return Ok(expr.clone());
    };
    if items.any(|other| format!("{:?}", other.expr) != format!("{:?}", item.expr)) {
        let why = format!("ORDER BY \"{}\" is ambiguous", name.column);
        return Err(Error::PlanError(why));
    }
    Ok(item.expr.clone())
}

/// One clause of those that stand after the last SELECT of a statement.
fn clause(part: Pair<'_>, select: &mut Select) -> Result<()> {
    let rule = part.as_rule();
    match rule {
        Rule::order_by_clause => select.order_by = sort_keys(part)?,
        Rule::use_vector_clause => {
            let mode = part.into_inner().next().expect("a mode");
            select.use_vector = Some(search_mode(&mode));
        }
        Rule::use_rank_clause => select.use_rank = Some(first_ident(part)),
        Rule::limit_clause | Rule::offset_clause => {
            let count = part.into_inner().next().expect("a count");
            let (what, number_of, name) = match rule {
                Rule::limit_clause => ("LIMIT", &mut select.limit, &mut select.limit_name),
                _ => ("OFFSET", &mut select.offset, &mut select.offset_name),
            };
            match count.as_rule() {
                Rule::parameter => *name = Some(count.as_str()[1..].to_owned()),
                _ => *number_of = Some(number(&count, what)?),
            }
        }
        // What a later version reads, refused by name before this is called.
        _ => {}
    }
    Ok(())
}

/// One SELECT up to its HAVING.
fn core(pair: Pair<'_>) -> Result<Select> {
    let mut select = Select {
        filter: where_clause(pair.clone())?,
        ..Select::default()
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::distinct => select.distinct = true,
            Rule::select_list => {
                for item in part.into_inner() {
                    select.columns.push(match item.as_rule() {
                        Rule::star => SelectColumn {
                            expr: star(),
                            alias: None,
                        },
                        _ => column(item)?,
                    });
                }
            }
            Rule::table_ref => select.from.push(table(part)),
            Rule::graph_table => select.from.push(FromItem::Graph(graph_table(part)?)),
            Rule::more_tables => return Err(not_supported("FROM a, b")),
            Rule::join => {
                let mut parts = part.into_inner();
                let kind = parts.next().expect("the kind of join");
                let kind = kind.as_str().to_ascii_uppercase();
                for other in ["RIGHT", "FULL"] {
                    if kind.starts_with(other) {
                        return Err(not_supported(&format!("{other} JOIN")));
                    }
                }
                select.joins.push(Join {
                    left_outer: kind.starts_with("LEFT"),
                    item: table(parts.next().expect("a table")),
                    on: parts.next().map(expr).transpose()?,
                });
            }
            Rule::group_by_clause => select.group_by = exprs(part)?,
            Rule::having_clause => select.having = exprs(part)?.pop(),
            // The WHERE clause, read above.
            _ => {}
        }
    }
    Ok(select)
}

/// A select item, or a column of a graph pattern, with the name it is given.
pub(super) fn column(item: Pair<'_>) -> Result<SelectColumn> {
    let mut parts = item.into_inner();
    Ok(SelectColumn {
        expr: expr(parts.next().expect("an expression"))?,
        alias: parts.next().map(first_ident),
    })
}

/// A table of FROM or of a JOIN, with the name the statement gives it.
fn table(pair: Pair<'_>) -> FromItem {
    FromItem::Table {
        alias: child(pair.clone(), Rule::alias).map(first_ident),
        name: first_ident(pair),
    }
}
