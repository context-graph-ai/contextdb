//! Expressions. The grammar reads the boolean operators as one flat run and the arithmetic
//! operators as another; the functions here apply their precedence.
//!
//! `expr`, `comparison_expr`, `distance_expr`, `additive_expr` and `primary_expr` call each other
//! once for every level of nesting, to the limit, on the caller's stack. So each is kept small:
//! what is not on that path is in a function of its own, and a list is gathered with a loop.

use super::{MAX_EXPR_NESTING, Pair, Rule, depth_error, has, ident, number, string};
use crate::sql::ast::*;
use crate::types::Result;

pub(super) fn expr(pair: Pair<'_>) -> Result<Expr> {
    // `[NOT] comparison {AND | OR [NOT] comparison}`. AND binds tighter than OR: `all` gathers
    // the comparisons joined by AND, `any` the runs between ORs.
    let mut any = Vec::new();
    let mut all = Vec::new();
    let mut nots = 0;
    let mut parts = pair.into_inner().peekable();
    while let Some(part) = parts.next() {
        match part.as_rule() {
            Rule::not_op => nots += 1,
            Rule::and_op => {}
            Rule::or_op => any.push(balanced(std::mem::take(&mut all), BinOp::And)),
            _ => {
                let suffix =
                    parts.next_if(|next| !matches!(next.as_rule(), Rule::and_op | Rule::or_op));
                let comparison = comparison_expr(part, suffix)?;
                all.push(
                    (0..std::mem::take(&mut nots)).fold(comparison, |operand, _| not(operand)),
                );
            }
        }
    }
    any.push(balanced(all, BinOp::And));
    let built = balanced(any, BinOp::Or);
    let depth = depth_of(&built);
    if depth > MAX_EXPR_NESTING {
        return Err(depth_error(depth));
    }
    Ok(built)
}

/// Every expression directly inside `pair`, in order.
pub(super) fn exprs(pair: Pair<'_>) -> Result<Vec<Expr>> {
    let mut exprs = Vec::new();
    for part in pair.into_inner() {
        if part.as_rule() == Rule::expr {
            exprs.push(expr(part)?);
        }
    }
    Ok(exprs)
}

/// How many expressions deep the tree goes below its root.
fn depth_of(expr: &Expr) -> usize {
    let below = match expr {
        Expr::BinaryOp { left, right, .. } => depth_of(left).max(depth_of(right)),
        Expr::UnaryOp { operand, .. } => depth_of(operand),
        Expr::FunctionCall { args, .. } => args.iter().map(depth_of).max().unwrap_or(0),
        Expr::IsNull { expr, .. } => depth_of(expr),
        Expr::Distance { left, right } => depth_of(left).max(depth_of(right)),
        Expr::Row(items) => items.iter().map(depth_of).max().unwrap_or(0),
        Expr::InList { expr, list, .. } => {
            let items = list.iter().map(depth_of).max().unwrap_or(0);
            depth_of(expr).max(items)
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => return 0,
    };
    below + 1
}

/// Joins the operands of a run of ANDs or ORs into a tree as shallow as it can be, keeping
/// their order, so a long run does not count against the nesting limit.
fn balanced(operands: Vec<Expr>, op: BinOp) -> Expr {
    fn build(operands: &mut std::vec::IntoIter<Expr>, len: usize, op: BinOp) -> Expr {
        if len == 1 {
            return operands.next().expect("an operand");
        }
        let left = build(operands, len / 2, op);
        let right = build(operands, len - len / 2, op);
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
    let len = operands.len();
    build(&mut operands.into_iter(), len, op)
}

fn not(operand: Expr) -> Expr {
    Expr::UnaryOp {
        op: UnaryOp::Not,
        operand: Box::new(operand),
    }
}

/// One comparison: its left side and what the grammar read after it, if anything.
fn comparison_expr(left: Pair<'_>, suffix: Option<Pair<'_>>) -> Result<Expr> {
    let left = Box::new(distance_expr(left)?);
    let Some(suffix) = suffix else {
        return Ok(*left);
    };
    if suffix.as_rule() == Rule::is_null_suffix {
        return Ok(Expr::IsNull {
            expr: left,
            negated: has(&suffix, Rule::not_op),
        });
    }
    if suffix.as_rule() == Rule::in_suffix {
        return Ok(Expr::InList {
            expr: left,
            negated: has(&suffix, Rule::not_op),
            list: exprs(suffix)?,
        });
    }
    let mut parts = suffix.into_inner();
    let op = match parts.next().expect("an operator").as_str() {
        "=" => BinOp::Eq,
        "!=" | "<>" => BinOp::Neq,
        "<" => BinOp::Lt,
        "<=" => BinOp::Lte,
        ">" => BinOp::Gt,
        _ => BinOp::Gte,
    };
    Ok(Expr::BinaryOp {
        left,
        op,
        right: Box::new(distance_expr(parts.next().expect("an operand"))?),
    })
}

/// `sum [<=> sum]`: a sum alone, or the distance between two.
fn distance_expr(pair: Pair<'_>) -> Result<Expr> {
    let mut sides = pair.into_inner();
    let left = additive_expr(sides.next().expect("an operand"))?;
    let Some(right) = sides.next() else {
        return Ok(left);
    };
    Ok(Expr::Distance {
        left: Box::new(left),
        right: Box::new(additive_expr(right)?),
    })
}

/// `left <op> right` as a call of `__add`, `__sub`, `__mul` or `__div`. A chain of operators
/// builds a tree one deeper per operator, so its depth is checked as it grows.
fn arithmetic(op: &str, left: Expr, right: Expr) -> Result<Expr> {
    let depth = depth_of(&left).max(depth_of(&right)) + 1;
    if depth > MAX_EXPR_NESTING {
        return Err(depth_error(depth));
    }
    let name = match op {
        "+" => "__add",
        "-" => "__sub",
        "*" => "__mul",
        _ => "__div",
    };
    Ok(Expr::FunctionCall {
        name: name.to_owned(),
        args: vec![left, right],
    })
}

/// `[-] primary {+ | - | * | / [-] primary}`, read left to right with `*` and `/` binding
/// tighter; a rank formula's sum the same way.
pub(super) fn additive_expr(pair: Pair<'_>) -> Result<Expr> {
    // `sum` is everything before the last `+` or `-`, with that operator; `product` is the run
    // of `*` and `/` being read, with the operator that waits for its right side.
    let mut sum: Option<(Expr, &str)> = None;
    let mut product: Option<(Expr, &str)> = None;
    let mut minuses = 0;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::unary_minus => minuses += 1,
            Rule::math_op | Rule::formula_op => {
                let (operand, _) = product.take().expect("an operand before an operator");
                let op = part.as_str();
                if op == "*" || op == "/" {
                    product = Some((operand, op));
                } else {
                    let total = match sum.take() {
                        Some((left, plus_or_minus)) => arithmetic(plus_or_minus, left, operand)?,
                        None => operand,
                    };
                    sum = Some((total, op));
                }
            }
            _ => {
                let mut operand = primary_expr(part)?;
                for _ in 0..std::mem::take(&mut minuses) {
                    operand = negated(operand);
                }
                product = Some(match product.take() {
                    Some((left, times_or_over)) => (arithmetic(times_or_over, left, operand)?, ""),
                    None => (operand, ""),
                });
            }
        }
    }
    let (operand, _) = product.expect("an operand");
    match sum {
        Some((left, plus_or_minus)) => arithmetic(plus_or_minus, left, operand),
        None => Ok(operand),
    }
}

/// `-operand`. A number written with a minus sign is that number: a literal, as a number
/// written without a sign is, so whatever reads a literal reads it.
fn negated(operand: Expr) -> Expr {
    match operand {
        Expr::Literal(Literal::Integer(whole)) => Expr::Literal(Literal::Integer(-whole)),
        Expr::Literal(Literal::Real(real)) => Expr::Literal(Literal::Real(-real)),
        operand => Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand: Box::new(operand),
        },
    }
}

/// The `*` of `SELECT *` and of `COUNT(*)`: a column of that name.
pub(super) fn star() -> Expr {
    Expr::Column(ColumnRef {
        table: None,
        column: "*".to_owned(),
    })
}

fn primary_expr(primary: Pair<'_>) -> Result<Expr> {
    match primary.as_rule() {
        // One expression in parentheses is that expression; two or more are a row value.
        Rule::parenthesized => {
            let mut items = exprs(primary)?;
            Ok(match items.len() {
                1 => items.swap_remove(0),
                _ => Expr::Row(items),
            })
        }
        Rule::function_call => function_call(primary),
        Rule::column_ref | Rule::braced => Ok(column_ref(primary)),
        // The parts of a rank formula.
        Rule::formula_sum => additive_expr(primary),
        Rule::coalesce => {
            let (name, parts) = ("coalesce".to_owned(), primary.into_inner());
            let args = parts.map(primary_expr).collect::<Result<_>>()?;
            Ok(Expr::FunctionCall { name, args })
        }
        _ => literal(&primary),
    }
}

fn literal(pair: &Pair<'_>) -> Result<Expr> {
    Ok(match pair.as_rule() {
        Rule::parameter => Expr::Parameter(pair.as_str()[1..].to_owned()),
        Rule::null_lit => Expr::Literal(Literal::Null),
        Rule::bool_lit => Expr::Literal(Literal::Bool(pair.as_str().eq_ignore_ascii_case("true"))),
        Rule::float => Expr::Literal(Literal::Real(number(pair, "the number")?)),
        Rule::integer => Expr::Literal(Literal::Integer(number(pair, "the whole number")?)),
        Rule::string => Expr::Literal(Literal::Text(string(pair))),
        // The one literal left: a vector.
        _ => {
            let components = pair
                .clone()
                .into_inner()
                .map(|component| number(&component, "the vector component"))
                .collect::<Result<_>>()?;
            Expr::Literal(Literal::Vector(components))
        }
    })
}

fn column_ref(pair: Pair<'_>) -> Expr {
    let mut names: Vec<String> = pair.into_inner().map(|name| ident(&name)).collect();
    let column = names.pop().expect("a column name");
    Expr::Column(ColumnRef {
        table: names.pop(),
        column,
    })
}

fn function_call(pair: Pair<'_>) -> Result<Expr> {
    let mut parts = pair.into_inner();
    let name = ident(&parts.next().expect("a function name"));
    let mut args = Vec::new();
    for arg in parts {
        args.push(match arg.as_rule() {
            Rule::star => star(),
            _ => expr(arg)?,
        });
    }
    Ok(Expr::FunctionCall { name, args })
}
