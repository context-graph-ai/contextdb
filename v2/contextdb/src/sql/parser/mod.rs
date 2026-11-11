//! Reads SQL text into a [`Statement`]. The grammar is `grammar.pest`; the functions here and in
//! the files beside this one turn what the grammar matched into the statement types.
//!
//! A builder trusts the grammar for the shape of what it is handed (which parts are present and
//! in what order) and checks only what a grammar cannot say: a number's range, a clause written
//! twice, two clauses that contradict each other.

mod expr;

use std::str::FromStr;

use pest::Parser;
use pest::error::{InputLocation, LineColLocation};

use super::ast::*;
use super::not_supported;
use crate::types::{Error, Result};

#[derive(pest_derive::Parser)]
#[grammar = "sql/grammar.pest"]
struct SqlParser;

type Pair<'a> = pest::iterators::Pair<'a, Rule>;

/// The deepest an expression may nest, in the text and in the tree built from it. It bounds the
/// parser's own recursion and the recursion of everything that later walks an expression.
const MAX_EXPR_NESTING: usize = 128;

pub(super) fn parse(text: &str) -> Result<Statement> {
    check_source_nesting(text)?;
    let read =
        SqlParser::parse(Rule::statement, text).map_err(|error| syntax_error(text, &error))?;
    for part in read.clone().flatten() {
        let later = match part.as_rule() {
            Rule::later_name => {
                let words: Vec<&str> = part.as_str().split_whitespace().collect();
                words.join(" ").to_ascii_uppercase()
            }
            Rule::subquery => "IN (SELECT ...)".to_owned(),
            Rule::vector_options => "USE VECTOR with options".to_owned(),
            _ => continue,
        };
        return Err(not_supported(&later));
    }
    let pair = read
        .flatten()
        .nth(1)
        .expect("the grammar's statement rule holds one statement");
    match pair.as_rule() {
        Rule::create_table_stmt => create_table(pair).map(Statement::CreateTable),
        Rule::create_index_stmt => {
            let [name, table] = two_idents(pair.clone());
            Ok(Statement::CreateIndex(CreateIndex {
                name,
                table,
                columns: pair
                    .into_inner()
                    .filter(|part| part.as_rule() == Rule::indexed_column)
                    .map(|column| {
                        let mut parts = column.into_inner();
                        let name = ident(&parts.next().expect("a column"));
                        (name, direction(parts.next()))
                    })
                    .collect(),
            }))
        }
        Rule::insert_stmt => insert(pair).map(Statement::Insert),
        Rule::delete_stmt => Ok(Statement::Delete(Delete {
            table: first_ident(pair.clone()),
            where_clause: where_clause(pair)?,
        })),
        Rule::update_stmt => Ok(Statement::Update(Update {
            table: first_ident(pair.clone()),
            assignments: assignments(pair.clone())?,
            where_clause: where_clause(pair)?,
        })),
        Rule::select_stmt => select(pair).map(Statement::Select),
        Rule::set_memory_limit => memory_limit(pair).map(Statement::SetMemoryLimit),
        Rule::show_vector_indexes => Ok(Statement::ShowVectorIndexes),
        Rule::show_vector_partitions => {
            let mut names = idents(pair.clone()).into_iter();
            let count = |rule, what| {
                child(pair.clone(), rule)
                    .map(|clause| number(&clause.into_inner().next().expect("a count"), what))
                    .transpose()
            };
            Ok(Statement::ShowVectorPartitions {
                column: names.next().zip(names.next()),
                limit: count(Rule::limit_clause, "LIMIT")?,
                offset: count(Rule::offset_clause, "OFFSET")?.unwrap_or(0),
            })
        }
        // The one statement left: SHOW MEMORY_LIMIT.
        _ => Ok(Statement::ShowMemoryLimit),
    }
}

/// The refusal for text the grammar does not read: where it stopped and the word it stopped at.
fn syntax_error(text: &str, error: &pest::error::Error<Rule>) -> Error {
    let (LineColLocation::Pos((line, column)) | LineColLocation::Span((line, column), _)) =
        error.line_col;
    let (InputLocation::Pos(at) | InputLocation::Span((at, _))) = error.location;
    let word: String = text[at..]
        .trim_start()
        .chars()
        .take_while(|ch| !ch.is_whitespace())
        .take(64)
        .collect();
    if word.is_empty() {
        refuse(format!(
            "syntax error at line {line}, column {column}: the statement ends too soon"
        ))
    } else {
        refuse(format!(
            "syntax error at line {line}, column {column}: unexpected '{word}'"
        ))
    }
}

fn depth_error(depth: usize) -> Error {
    refuse(format!(
        "expression nesting of {depth} is deeper than the limit of {MAX_EXPR_NESTING}"
    ))
}

/// The words and marks of a statement, without its comments. A quoted text or name comes as its
/// opening quote alone, so nothing inside quotes is ever taken for a keyword or a parenthesis.
struct Tokens<'a> {
    text: &'a str,
    at: usize,
}

impl<'a> Iterator for Tokens<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        loop {
            let rest = &self.text[self.at..];
            let first = rest.chars().next()?;
            let len = if first.is_whitespace() {
                first.len_utf8()
            } else if rest.starts_with("--") {
                rest.find('\n').unwrap_or(rest.len())
            } else if rest.starts_with("/*") {
                rest.find("*/").map_or(rest.len(), |end| end + 2)
            } else {
                break;
            };
            self.at += len;
        }
        let rest = &self.text[self.at..];
        let first = rest.chars().next()?;
        let (token, consumed) = if first == '\'' || first == '"' {
            // A doubled quote inside is the quote itself, so the text ends at the first quote
            // that is not followed by another.
            let mut end = 1;
            loop {
                match rest[end..].find(first) {
                    None => break (&rest[..1], rest.len()),
                    Some(quote) if rest[end + quote + 1..].starts_with(first) => end += quote + 2,
                    Some(quote) => break (&rest[..1], end + quote + 1),
                }
            }
        } else if first.is_ascii_alphanumeric() || first == '_' {
            let end = rest
                .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .unwrap_or(rest.len());
            (&rest[..end], end)
        } else {
            (&rest[..first.len_utf8()], first.len_utf8())
        };
        self.at += consumed;
        Some(token)
    }
}

/// Refuses text whose expressions nest deeper than the limit, before the grammar recurses into
/// it. A parenthesis, a NOT and a unary minus each count one level.
fn check_source_nesting(text: &str) -> Result<()> {
    // The depth outside each open parenthesis, innermost last.
    let mut frames = vec![0usize];
    // NOT and unary minus read since the last operand.
    let mut prefixes = 0;
    // Whether a minus read now would be unary.
    let mut operand_next = true;
    for token in (Tokens { text, at: 0 }) {
        match token {
            "(" => {
                frames.push(frames[frames.len() - 1] + prefixes + 1);
                prefixes = 0;
                operand_next = true;
            }
            ")" => {
                if frames.len() > 1 {
                    frames.pop();
                }
                prefixes = 0;
                operand_next = false;
            }
            "-" if operand_next => prefixes += 1,
            _ if token.eq_ignore_ascii_case("NOT") => {
                prefixes += 1;
                operand_next = true;
            }
            _ => {
                prefixes = 0;
                operand_next = matches!(token, "=" | "<" | ">" | "+" | "-" | "*" | "/" | ",")
                    || ["AND", "OR", "WHERE", "SELECT", "BY"]
                        .iter()
                        .any(|word| token.eq_ignore_ascii_case(word));
            }
        }
        let depth = frames[frames.len() - 1] + prefixes;
        if depth > MAX_EXPR_NESTING {
            return Err(depth_error(depth));
        }
    }
    Ok(())
}

fn refuse(message: impl Into<String>) -> Error {
    Error::ParseError(message.into())
}

/// A name as written, or what is between the double quotes of a quoted one.
fn ident(pair: &Pair<'_>) -> String {
    let raw = pair.as_str();
    match raw.strip_prefix('"').and_then(|raw| raw.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => raw.to_owned(),
    }
}

/// What is between the single quotes of a quoted text.
fn string(pair: &Pair<'_>) -> String {
    let raw = pair.as_str();
    raw[1..raw.len() - 1].replace("''", "'")
}

/// Every name directly inside `pair`, in order.
fn idents(pair: Pair<'_>) -> Vec<String> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(|part| ident(&part))
        .collect()
}

fn first_ident(pair: Pair<'_>) -> String {
    idents(pair).swap_remove(0)
}

fn two_idents(pair: Pair<'_>) -> [String; 2] {
    let mut names = idents(pair).into_iter();
    [(); 2].map(|()| names.next().expect("the grammar reads two names here"))
}

/// The first part of `pair` that the grammar read with `rule`.
fn child<'a>(pair: Pair<'a>, rule: Rule) -> Option<Pair<'a>> {
    pair.into_inner().find(|part| part.as_rule() == rule)
}

fn has(pair: &Pair<'_>, rule: Rule) -> bool {
    child(pair.clone(), rule).is_some()
}

/// A number the grammar read as digits, refused when it does not fit the type it is for.
fn number<T: FromStr>(pair: &Pair<'_>, what: &str) -> Result<T> {
    pair.as_str()
        .parse()
        .map_err(|_| refuse(format!("{what} {} is out of range", pair.as_str())))
}

fn insert(pair: Pair<'_>) -> Result<Insert> {
    let mut values = Vec::new();
    for row in pair.clone().into_inner() {
        if row.as_rule() == Rule::values_row {
            values.push(expr::exprs(row)?);
        }
    }
    let mut names = idents(pair);
    let columns = names.split_off(1);
    Ok(Insert {
        table: names.swap_remove(0),
        columns,
        values,
    })
}

/// Every `column = value` directly inside `pair`.
fn assignments(pair: Pair<'_>) -> Result<Vec<(String, Expr)>> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::assignment)
        .map(|assignment| {
            let mut parts = assignment.into_inner();
            let column = ident(&parts.next().expect("a column"));
            Ok((column, expr::expr(parts.next().expect("a value"))?))
        })
        .collect()
}

fn where_clause(pair: Pair<'_>) -> Result<Option<Expr>> {
    child(pair, Rule::where_clause)
        .map(|clause| expr::expr(clause.into_inner().next().expect("a condition")))
        .transpose()
}

/// The number of a clause of a VECTOR column, as written.
fn signed(clause: Pair<'_>) -> Option<String> {
    child(clause, Rule::signed).map(|number| number.as_str().to_owned())
}

/// AUTO, EXACT or INDEXED, after SEARCH_MODE and after USE VECTOR.
fn search_mode(pair: &Pair<'_>) -> VectorSearchMode {
    match pair.as_str().to_ascii_uppercase().as_str() {
        "AUTO" => VectorSearchMode::Auto,
        "EXACT" => VectorSearchMode::Exact,
        _ => VectorSearchMode::Indexed,
    }
}

/// ASC when nothing is written.
fn direction(written: Option<Pair<'_>>) -> SortDirection {
    match written {
        Some(written) if written.as_str().eq_ignore_ascii_case("DESC") => SortDirection::Desc,
        _ => SortDirection::Asc,
    }
}

fn select(pair: Pair<'_>) -> Result<Select> {
    let mut select = Select {
        columns: Vec::new(),
        table: first_ident(pair.clone()),
        where_clause: where_clause(pair.clone())?,
        order_by: Vec::new(),
        use_vector: None,
        limit: None,
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::select_list => {
                for item in part.into_inner() {
                    select.columns.push(match item.as_rule() {
                        Rule::star => SelectColumn {
                            expr: expr::star(),
                            alias: None,
                        },
                        _ => {
                            let mut parts = item.into_inner();
                            SelectColumn {
                                expr: expr::expr(parts.next().expect("an expression"))?,
                                alias: parts.next().map(|alias| ident(&alias)),
                            }
                        }
                    });
                }
            }
            Rule::order_by_clause => {
                for item in part.into_inner() {
                    let mut parts = item.into_inner();
                    select.order_by.push(OrderByItem {
                        expr: expr::expr(parts.next().expect("a sort key"))?,
                        direction: direction(parts.next()),
                    });
                }
            }
            Rule::use_vector_clause => {
                let mode = part.into_inner().next().expect("a mode");
                select.use_vector = Some(search_mode(&mode));
            }
            Rule::limit_clause => {
                let count = part.into_inner().next().expect("a count");
                select.limit = Some(number(&count, "LIMIT")?);
            }
            // The table and the WHERE clause, read above.
            _ => {}
        }
    }
    Ok(select)
}

/// The bytes of `SET MEMORY_LIMIT <count>G|M|K`, or no limit for the word NONE.
fn memory_limit(pair: Pair<'_>) -> Result<SetMemoryLimitValue> {
    let value = pair.into_inner().next().expect("a size");
    let Some(size) = value.into_inner().next() else {
        return Ok(SetMemoryLimitValue::None);
    };
    let (digits, unit) = size.as_str().split_at(size.as_str().len() - 1);
    let shift = match unit {
        "G" => 30,
        "M" => 20,
        _ => 10,
    };
    digits
        .parse::<usize>()
        .ok()
        .and_then(|count| count.checked_mul(1 << shift))
        .map(SetMemoryLimitValue::Bytes)
        .ok_or_else(|| refuse("the size is out of range"))
}

fn create_table(pair: Pair<'_>) -> Result<CreateTable> {
    let mut table = CreateTable {
        name: first_ident(pair.clone()),
        columns: Vec::new(),
        unique_constraints: Vec::new(),
        if_not_exists: has(&pair, Rule::if_not_exists),
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::column_def => table.columns.push(column_def(part)?),
            Rule::unique_table_constraint => {
                let columns = idents(part);
                if let Some(twice) = columns
                    .iter()
                    .enumerate()
                    .find_map(|(at, column)| columns[..at].contains(column).then_some(column))
                {
                    return Err(refuse(format!("UNIQUE names '{twice}' twice")));
                }
                table.unique_constraints.push(columns);
            }
            // IF NOT EXISTS and the table's name, read above.
            _ => {}
        }
    }
    let declared = |name: &String| table.columns.iter().any(|column| column.name == *name);
    if let Some(unknown) = table
        .unique_constraints
        .iter()
        .flatten()
        .find(|column| !declared(column))
    {
        return Err(refuse(format!(
            "UNIQUE constraint references unknown column '{unknown}'"
        )));
    }
    Ok(table)
}

fn column_def(pair: Pair<'_>) -> Result<ColumnDef> {
    let mut parts = pair.into_inner();
    let name = ident(&parts.next().expect("a column name"));
    let type_pair = parts.next().expect("a column type");
    let type_name = type_pair.as_str().to_ascii_uppercase();
    let mut vector = VectorClauses::default();
    let data_type = match (type_pair.into_inner().next(), type_name.as_str()) {
        (Some(declared), _) => {
            let mut parts = declared.into_inner();
            let dimension = parts.next().expect("a dimension");
            for clause in parts {
                match clause.as_rule() {
                    Rule::quantization => {
                        let text = clause.into_inner().next().expect("a quoted text");
                        vector.quantization = Some(string(&text));
                    }
                    Rule::partition_key => vector.partition_key = idents(clause),
                    Rule::max_partitions => vector.max_partitions = signed(clause),
                    Rule::search_mode => vector.search_mode = Some(search_mode(&clause)),
                    Rule::auto_index_at => vector.auto_index_at = signed(clause),
                    // The one clause left: CONSOLIDATION, which is NONE when it has no member.
                    _ => {
                        let member = |rule| child(clause.clone(), rule).and_then(signed);
                        let members = (
                            member(Rule::change_percent),
                            member(Rule::tombstone_percent),
                        );
                        vector.consolidation = Some(match members {
                            (None, None) => ConsolidationClause::None,
                            (change, tombstone) => {
                                ConsolidationClause::Percent { change, tombstone }
                            }
                        });
                    }
                }
            }
            DataType::Vector(number(&dimension, "the VECTOR dimension")?)
        }
        (None, "UUID") => DataType::Uuid,
        (None, "TEXT") => DataType::Text,
        (None, "INTEGER" | "INT") => DataType::Integer,
        (None, "REAL" | "FLOAT") => DataType::Real,
        (None, "BOOLEAN" | "BOOL") => DataType::Boolean,
        (None, "TIMESTAMP") => DataType::Timestamp,
        (None, _) => DataType::Json,
    };
    let mut column = ColumnDef {
        name,
        data_type,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
        vector,
    };
    // A constraint written twice is refused.
    let once = |written: bool, clause: &str| match written {
        true => Err(refuse(format!("duplicate {clause} clause"))),
        false => Ok(()),
    };
    for constraint in parts {
        match constraint.as_rule() {
            Rule::not_null => {
                once(!column.nullable, "NOT NULL")?;
                column.nullable = false;
            }
            Rule::nullable_marker => {}
            Rule::primary_key => {
                once(column.primary_key, "PRIMARY KEY")?;
                column.primary_key = true;
            }
            Rule::unique => {
                once(column.unique, "UNIQUE")?;
                column.unique = true;
            }
            // The one constraint left: DEFAULT.
            _ => {
                once(column.default.is_some(), "DEFAULT")?;
                let value = constraint.into_inner().next().expect("a value");
                column.default = Some(expr::expr(value)?);
            }
        }
    }
    Ok(column)
}

#[cfg(test)]
mod tests;
