//! The one reader of a filter: what a statement's `WHERE` fixes of the columns of a row. Every
//! part that chooses how rows are reached takes its answer from here.

use std::cmp::Ordering::{self, Greater, Less};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use super::expression::{EvalContext, Scope, eval, holds, sort_order};
use super::sets::List;
use super::table::as_key;
use crate::schema::{Table, field_key};
use crate::sql::Expr;
use crate::sql::ast::BinOp;
use crate::types::{ColumnType, Result, Value, heap_bytes};

/// The most alternatives a filter is read into; a part that would pass it is left unread.
pub(super) const MOST_CASES: usize = 4096;
const MANY: &str = "more than 4096 alternatives";
const OPEN: &str = "no equality, range or list of values";

/// What a filter fixes, read once for a statement.
pub(super) struct Reading<'f> {
    /// The filter's conjuncts at its top level, in the order written.
    pub(super) parts: Vec<Part<'f>>,
    /// The filter passes a row only where one of these holds.
    pub(super) cases: Vec<Case>,
    /// Each `$name` with no value that an `=` or `IN` compares a column with, and the column.
    pub(super) unbound: Vec<(usize, String)>,
}

pub(super) struct Part<'f> {
    pub(super) expr: &'f Expr,
    /// The columns its facts are on; none when it gave none.
    pub(super) columns: Vec<usize>,
    /// Why every row is still asked it, in EXPLAIN's words; `None` when its facts say all it says.
    pub(super) kept: Option<&'static str>,
}

/// What one alternative fixes: a column once, in column order.
pub(super) type Case = Vec<(usize, Fact)>;

#[derive(Clone, PartialEq)]
pub(super) enum Fact {
    /// One of these values, each of the column's type; `one` when `column = value` said it.
    Among { values: Vec<Value>, one: bool },
    /// Between two bounds; NULL is never inside.
    Within(Bound<Value>, Bound<Value>),
}

/// What the names of a filter are: columns of the statement's scope, and for a table source the
/// table, whose indexes over a JSON path a `json -> 'k'` chain may be read by.
pub(super) struct Columns<'c> {
    pub(super) scope: &'c Scope<'c>,
    pub(super) table: Option<&'c Table>,
}

/// A comparison read: its column and fact, or why it gives none.
type Compared = std::result::Result<(usize, Fact), &'static str>;

pub(super) fn read<'f>(
    filter: Option<&'f Expr>,
    columns: &Columns,
    ctx: &EvalContext,
) -> Result<Reading<'f>> {
    let (mut top, mut parts) = (Vec::from_iter(filter), Vec::new());
    while let Some(expr) = top.pop() {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => top.extend([&**right, &**left]),
            expr => parts.push(expr),
        }
    }
    let (mut cases, mut unbound) = (vec![Vec::new()], Vec::new());
    let mut read = Vec::new();
    for expr in parts {
        let mut kept = None;
        let said = said(expr, columns, ctx, &mut unbound, &mut kept)?;
        let mut on: Vec<usize> = said.iter().flatten().map(|(column, _)| *column).collect();
        on.sort_unstable();
        on.dedup();
        match and(&cases, &said) {
            Some(both) => cases = both,
            None => (kept, on) = (Some(MANY), Vec::new()),
        }
        read.push(Part {
            expr,
            columns: on,
            kept,
        });
    }
    let held = cases.iter().flatten().map(|(_, fact)| match fact {
        Fact::Among { values, .. } => values.iter().map(|value| 32 + heap_bytes(value)).sum(),
        Fact::Within(..) => 80,
    });
    ctx.kept.keep(held.sum::<u64>() + 24 * cases.len() as u64)?;
    Ok(Reading {
        parts: read,
        cases,
        unbound,
    })
}

/// The cases one expression passes a row in: one empty case, and why in `kept`, when unread.
fn said(
    expr: &Expr,
    columns: &Columns,
    ctx: &EvalContext,
    unbound: &mut Vec<(usize, String)>,
    kept: &mut Option<&'static str>,
) -> Result<Vec<Case>> {
    let compare = |unbound: &mut _, of, value, op| compared(of, value, op, columns, ctx, unbound);
    // A NULL in a list of single values equals nothing and is dropped from it.
    let list = match expr {
        Expr::InList { expr: of, .. } => of.sides().len() == 1,
        _ => matches!(expr, Expr::InSubquery { .. }),
    };
    let never = |value: &Value| matches!(value, Value::Null | Value::Bool(false));
    let items: Vec<Vec<Compared>> = match expr {
        _ if is_value(expr, ctx)
            && eval(expr, &Scope::default(), &[], ctx).is_ok_and(|v| never(&v)) =>
        {
            return Ok(Vec::new());
        }
        Expr::BinaryOp {
            left,
            op: op @ (BinOp::And | BinOp::Or),
            right,
        } => {
            let left = said(left, columns, ctx, unbound, kept)?;
            let right = said(right, columns, ctx, unbound, kept)?;
            let both = match op {
                BinOp::And => and(&left, &right),
                _ => Some(or(left, right)),
            };
            return Ok(capped(both, kept));
        }
        Expr::BinaryOp { left, op, right } if turned(*op).is_some() => {
            let (lefts, rights) = (left.sides(), right.sides());
            let whole = lefts.len() == rights.len() && (lefts.len() == 1 || *op == BinOp::Eq);
            let mut item = Vec::new();
            for (left, right) in lefts.iter().zip(rights).filter(|_| whole) {
                item.push(match (left, right, typed(left, columns)) {
                    // Always asked of a row: a field's key is its number as a REAL, which two
                    // integers share. The field is on either side.
                    (of @ Expr::Field { .. }, value, _) | (value, of @ Expr::Field { .. }, _)
                        if *op == BinOp::Eq =>
                    {
                        *kept = kept.or(Some("a JSON field's key is not the field"));
                        field(of, value, columns, ctx)
                    }
                    (.., Some(_)) => compare(unbound, left, right, *op),
                    _ => compare(unbound, right, left, turned(*op).unwrap_or(*op)),
                });
            }
            vec![if whole {
                item
            } else {
                vec![Err(why(left, right))]
            }]
        }
        Expr::IsNull { expr: of, negated } => {
            let null = Fact::Among {
                values: vec![Value::Null],
                one: false,
            };
            let fact = if *negated {
                Fact::Within(Unbounded, Unbounded)
            } else {
                null
            };
            vec![vec![
                typed(of, columns).map(|(at, _)| (at, fact)).ok_or(OPEN),
            ]]
        }
        Expr::InList {
            expr: of,
            list,
            negated: false,
        } => {
            let mut items: Vec<Vec<Compared>> = Vec::new();
            for listed in list {
                let pairs = of.sides().iter().zip(listed.sides());
                items.push(
                    pairs
                        .map(|(of, value)| match of {
                            Expr::Field { .. } => {
                                *kept = kept.or(Some("a JSON field's key is not the field"));
                                field(of, value, columns, ctx)
                            }
                            _ => compare(unbound, of, value, BinOp::Eq),
                        })
                        .collect(),
                );
            }
            // A list never names one row, even a list of one value.
            for fact in items.iter_mut().flatten().flatten() {
                if let (_, Fact::Among { one, .. }) = fact {
                    *one = false;
                }
            }
            items
        }
        Expr::InSubquery {
            expr: of,
            query,
            negated: false,
        } if ctx.said.is_none() => match (typed(of, columns), ctx.subquery(query).ok()) {
            (Some((at, kind)), Some(List::Memory(values, _))) => values
                .iter()
                .map(|value| vec![fact(at, kind, value.clone(), BinOp::Eq)])
                .collect(),
            _ => vec![vec![Err(OPEN)]],
        },
        Expr::Between {
            expr: of,
            low,
            high,
            negated: false,
        } => vec![vec![
            compare(unbound, of, low, BinOp::Gte),
            compare(unbound, of, high, BinOp::Lte),
        ]],
        Expr::Like { .. } => vec![vec![Err("LIKE is residual-only")]],
        Expr::BinaryOp { left, right, .. } => vec![vec![Err(why(left, right))]],
        _ => vec![vec![Err(OPEN)]],
    };
    let mut cases = Vec::new();
    for item in items {
        let mut facts = Vec::new();
        for fact in item {
            match fact {
                // NULL beside `=` or a comparison leaves its part to the row check, as today.
                Ok((_, Fact::Among { values, .. })) if values.is_empty() && !list => {
                    *kept = kept.or(Some("compared with NULL"));
                }
                Ok((at, Fact::Among { values, .. })) if list => {
                    facts.push((at, Fact::Among { values, one: false }));
                }
                Ok(fact) => facts.push(fact),
                Err(why) => *kept = kept.or(Some(why)),
            }
        }
        cases.extend(met(facts));
    }
    // The items of a list are its alternatives.
    Ok(capped(
        Some(if list { or(Vec::new(), cases) } else { cases }),
        kept,
    ))
}

/// The cases, or one empty case and why when there are too many to keep.
fn capped(cases: Option<Vec<Case>>, kept: &mut Option<&'static str>) -> Vec<Case> {
    match cases.filter(|cases| cases.len() <= MOST_CASES) {
        Some(cases) => cases,
        None => {
            *kept = kept.or(Some(MANY));
            vec![Vec::new()]
        }
    }
}

/// `column op value` read as a fact: the value worked out once, before any row. One whose
/// working out fails is left to the row check, which fails only if a row reaches it.
fn compared(
    of: &Expr,
    value: &Expr,
    op: BinOp,
    columns: &Columns,
    ctx: &EvalContext,
    unbound: &mut Vec<(usize, String)>,
) -> Compared {
    let (at, kind) = typed(of, columns)
        .filter(|_| is_value(value, ctx))
        .ok_or(why(of, value))?;
    let found = eval(value, &Scope::default(), &[], ctx);
    if let (Err(_), Expr::Parameter(name), BinOp::Eq) = (&found, value, op) {
        unbound.push((at, name.clone()));
    }
    fact(
        at,
        kind,
        found
            .map_err(|_| "value not worked out before the rows")?
            .into_owned(),
        op,
    )
}

/// The fact `column op value` gives, the value made the column's type; NULL gives no value.
fn fact(at: usize, kind: ColumnType, value: Value, op: BinOp) -> Compared {
    let null = value == Value::Null;
    let Some(value) = as_key(kind, value) else {
        return if null {
            Ok((
                at,
                Fact::Among {
                    values: Vec::new(),
                    one: false,
                },
            ))
        } else {
            Err("value not of the column's type")
        };
    };
    Ok((
        at,
        match op {
            BinOp::Eq => Fact::Among {
                values: vec![value],
                one: true,
            },
            BinOp::Gt => Fact::Within(Excluded(value), Unbounded),
            BinOp::Gte => Fact::Within(Included(value), Unbounded),
            BinOp::Lt => Fact::Within(Unbounded, Excluded(value)),
            _ => Fact::Within(Unbounded, Included(value)),
        },
    ))
}

/// `doc -> 'k' -> 'j' = value` as a fact of the index over that path, if there is one: on the
/// column only that index has, numbered past the table's columns by its place.
fn field(mut json: &Expr, value: &Expr, columns: &Columns, ctx: &EvalContext) -> Compared {
    let mut path = Vec::new();
    while let Expr::Field {
        json: of,
        key,
        as_text: false,
    } = json
        && let Expr::Literal(Value::Text(key)) = &**key
    {
        path.insert(0, key);
        json = of;
    }
    let (Some(table), Some((at, _)), true) =
        (columns.table, typed(json, columns), is_value(value, ctx))
    else {
        return Err(why(json, value));
    };
    let name = &table.meta.columns[at].name;
    let place = table.meta.indexes.iter().position(|index| {
        index.json_path.iter().eq(path.iter().copied()) && index.columns[0].0 == *name
    });
    let none = "no index over that JSON path";
    let (place, found) = (place.ok_or(none)?, eval(value, &Scope::default(), &[], ctx));
    let key = match found.ok().as_deref() {
        Some(Value::Text(text)) => ctx.kept.with_json(Some(value), text, field_key).ok(),
        Some(Value::Json(field)) => Some(field_key(field)),
        _ => None,
    };
    let fact = |key| Fact::Among {
        values: vec![key],
        one: true,
    };
    Ok((
        table.meta.columns.len() + place,
        fact(key.flatten().ok_or(none)?),
    ))
}

/// The column an expression is, and its type.
fn typed(of: &Expr, columns: &Columns) -> Option<(usize, ColumnType)> {
    match of {
        Expr::Column(column) => columns.scope.find(column).ok(),
        _ => None,
    }
}

/// True for an expression that reads no column of a row; a SELECT only when the statement runs.
fn is_value(expr: &Expr, ctx: &EvalContext) -> bool {
    let selects = |expr: &Expr| matches!(expr, Expr::Subquery(_) | Expr::InSubquery { .. });
    !holds(expr, |expr| matches!(expr, Expr::Column(_)))
        && (ctx.said.is_none() || !holds(expr, selects))
}

/// The comparison with its sides swapped, `5 < id` as `id > 5`; `None` for no comparison.
fn turned(op: BinOp) -> Option<BinOp> {
    Some(match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Lte => BinOp::Gte,
        BinOp::Gt => BinOp::Lt,
        BinOp::Gte => BinOp::Lte,
        BinOp::Eq => BinOp::Eq,
        _ => return None,
    })
}

/// Why two sides compared give no fact, in EXPLAIN's words.
fn why(left: &Expr, right: &Expr) -> &'static str {
    let known = |expr: &Expr| matches!(expr, Expr::Literal(_) | Expr::Parameter(_));
    match (left, right) {
        (Expr::FunctionCall { name, .. }, _) | (_, Expr::FunctionCall { name, .. }) => {
            match name.starts_with("__") {
                true => "arithmetic in predicate",
                false => "function call in predicate",
            }
        }
        _ if known(left) || known(right) => OPEN,
        _ => "non-literal rhs",
    }
}

/// Each case of `left` with each of `right`; `None` past the most cases.
fn and(left: &[Case], right: &[Case]) -> Option<Vec<Case>> {
    (left.len().saturating_mul(right.len()) <= MOST_CASES).then(|| {
        let pairs = left
            .iter()
            .flat_map(|l| right.iter().map(move |r| [l.clone(), r.clone()]));
        pairs.filter_map(|pair| met(pair.concat())).collect()
    })
}

/// The cases of both sides, two that differ only in the values of one column made one, its
/// values in the order written. An alternative that fixes nothing leaves one empty case.
fn or(mut cases: Vec<Case>, more: Vec<Case>) -> Vec<Case> {
    'more: for case in more {
        for kept in cases.iter_mut().filter(|kept| kept.len() == case.len()) {
            let mut differ = (0..case.len()).filter(|&at| kept[at] != case[at]);
            let (first, second) = (differ.next(), differ.next());
            if let (Some(at), None) = (first, second)
                && let ((c, Fact::Among { values, one }), (o, Fact::Among { values: more, .. })) =
                    (&mut kept[at], &case[at])
                && c == o
            {
                values.extend(more.iter().cloned());
                *one = false;
            } else if first.is_some() {
                continue;
            }
            continue 'more;
        }
        cases.push(case);
    }
    if cases.iter().any(Vec::is_empty) {
        vec![Vec::new()]
    } else {
        cases
    }
}

/// Each column of the filter's one case, with the one value it is given when it is given one,
/// when the filter has one case and says nothing else.
pub(super) fn pins<'r>(reading: &'r Reading) -> Option<Vec<(usize, Option<&'r Value>)>> {
    let ([case], true) = (
        &reading.cases[..],
        reading.parts.iter().all(|p| p.kept.is_none()),
    ) else {
        return None;
    };
    let one = |fact: &'r Fact| match fact {
        Fact::Among { values, .. } if values.len() == 1 => values.first(),
        _ => None,
    };
    Some(case.iter().map(|(c, f)| (*c, one(f))).collect())
}

/// One case of these facts, two on one column met; `None` when no row can be in it.
fn met(facts: Vec<(usize, Fact)>) -> Option<Case> {
    let mut case: Case = Vec::new();
    for (column, fact) in facts {
        match case.binary_search_by_key(&column, |(at, _)| *at) {
            Ok(at) => case[at].1 = meet(&case[at].1, &fact),
            Err(at) => case.insert(at, (column, fact)),
        }
    }
    let none = |(_, fact): &(usize, Fact)| match fact {
        Fact::Among { values, .. } => values.is_empty(),
        Fact::Within(Included(l), Included(h)) => sort_order(l, h).is_gt(),
        Fact::Within(Included(l) | Excluded(l), Included(h) | Excluded(h)) => {
            sort_order(l, h).is_ge()
        }
        Fact::Within(..) => false,
    };
    (!case.iter().any(none)).then_some(case)
}

/// What two facts on one column both allow.
fn meet(a: &Fact, b: &Fact) -> Fact {
    match (a, b) {
        (Fact::Within(low, high), Fact::Within(other_low, other_high)) => Fact::Within(
            tighter(low, other_low, Greater),
            tighter(high, other_high, Less),
        ),
        (Fact::Among { values, one }, other) | (other, Fact::Among { values, one }) => {
            Fact::Among {
                values: values
                    .iter()
                    .filter(|value| other.allows(value))
                    .cloned()
                    .collect(),
                one: *one || matches!(other, Fact::Among { one: true, .. }),
            }
        }
    }
}

/// Of two bounds of one end, the one `keep` puts first; an excluded value before itself included.
fn tighter(a: &Bound<Value>, b: &Bound<Value>, keep: Ordering) -> Bound<Value> {
    let (Included(x) | Excluded(x), Included(y) | Excluded(y)) = (a, b) else {
        return if *a == Unbounded {
            b.clone()
        } else {
            a.clone()
        };
    };
    let order = sort_order(x, y);
    let first = order == keep || order.is_eq() && matches!(a, Excluded(_));
    if first { a.clone() } else { b.clone() }
}

impl Fact {
    /// True when a row with this value in the column has the fact.
    fn allows(&self, value: &Value) -> bool {
        let (low, high) = match self {
            Fact::Among { values, .. } => {
                return values.iter().any(|v| sort_order(v, value).is_eq());
            }
            Fact::Within(low, high) => (low, high),
        };
        let inside = |bound: &Bound<Value>, past: Ordering| match bound {
            Included(end) => sort_order(value, end) != past,
            Excluded(end) => sort_order(value, end) == past.reverse(),
            Unbounded => true,
        };
        *value != Value::Null && inside(low, Less) && inside(high, Greater)
    }
}
