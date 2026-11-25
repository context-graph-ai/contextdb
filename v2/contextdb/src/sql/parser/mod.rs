//! Reads SQL text into a [`Statement`]. The grammar is `grammar.pest`; the functions here and in
//! the files beside this one turn what the grammar matched into the statement types.
//!
//! A builder trusts the grammar for the shape of what it is handed (which parts are present and
//! in what order) and checks only what a grammar cannot say: a number's range, a clause written
//! twice, two clauses that contradict each other.

mod alter;
mod ddl;
mod expr;
mod graph;
mod select;

use std::str::FromStr;

use pest::Parser;
use pest::error::{InputLocation, LineColLocation};

use super::ast::*;
use super::{not_carried, not_supported};
use crate::types::{Error, Result};
use select::select;

#[derive(pest_derive::Parser)]
#[grammar = "sql/grammar.pest"]
struct SqlParser;

type Pair<'a> = pest::iterators::Pair<'a, Rule>;

/// The deepest an expression may nest, in the text and in the tree built from it. It bounds the
/// parser's own recursion and the recursion of everything that later walks an expression.
pub(crate) const MAX_EXPR_NESTING: usize = 64;

/// Whether a read session answers this statement, read from its words and charging nothing:
/// `SELECT`, `WITH`, `EXPLAIN`, and a `SHOW` of what is not a setting.
pub fn read_session_answers(text: &str) -> bool {
    let mut words = Tokens { text, at: 0 };
    match words.next().map(str::to_ascii_uppercase).as_deref() {
        Some("SELECT" | "WITH" | "EXPLAIN") => true,
        Some("SHOW") => {
            let name = words.next().map(str::to_ascii_uppercase);
            let named = crate::settings::SETTINGS.map(|s| s.0[4..].split(' ').next());
            !named.contains(&name.as_deref())
        }
        _ => false,
    }
}

pub(super) fn parse(text: &str) -> Result<(Statement, String)> {
    check_source_nesting(text)?;
    let read =
        SqlParser::parse(Rule::statement, text).map_err(|error| syntax_error(text, &error))?;
    let (mut form, mut end) = (String::new(), 0);
    for part in read.clone().flatten() {
        let later = match part.as_rule() {
            // A constant, a vector whole, becomes `?` in the statement's form.
            Rule::null_lit
            | Rule::bool_lit
            | Rule::float
            | Rule::integer
            | Rule::string
            | Rule::vector_lit
                if part.as_span().start() >= end =>
            {
                form.extend([&text[end..part.as_span().start()], "?"]);
                end = part.as_span().end();
                continue;
            }
            Rule::later_name => {
                let words: Vec<&str> = part.as_str().split_whitespace().collect();
                words.join(" ").to_ascii_uppercase()
            }
            Rule::subquery => "INSERT ... SELECT".to_owned(),
            Rule::vector_options => "USE VECTOR with options".to_owned(),
            _ => continue,
        };
        if matches!(&*later, "IVF" | "SEARCH") {
            let instead = "the index sizes its own groups and measures how far a search scans; \
                           for the true nearest rows write USE VECTOR EXACT or SEARCH_MODE EXACT, \
                           to move the size from which AUTO uses the index write INDEXING_THRESHOLD, \
                           and to bound how far a search scans write PROBES n";
            return Err(not_carried(&format!("{later} (...)"), instead));
        }
        return Err(not_supported(&later));
    }
    form += &text[end..];
    let first = read.flatten().nth(1);
    let first = first.expect("the statement rule holds one statement");
    Ok((statement(first)?, form))
}

/// The statement one rule of the grammar matched.
fn statement(pair: Pair) -> Result<Statement> {
    match pair.as_rule() {
        Rule::create_trigger_stmt
        | Rule::create_event_type_stmt
        | Rule::create_sink_stmt
        | Rule::create_route_stmt
        | Rule::create_schedule_stmt
        | Rule::drop_trigger_stmt
        | Rule::drop_route_stmt
        | Rule::drop_schedule_stmt => ddl::event_declaration(pair),
        Rule::stored_procedure => Err(Error::StoredProcNotSupported),
        Rule::create_table_stmt => ddl::create_table(pair).map(Statement::CreateTable),
        Rule::create_index_stmt => Ok(Statement::CreateIndex(ddl::create_index(pair))),
        Rule::alter_table_stmt => alter::alter_table(pair).map(Statement::AlterTable),
        Rule::drop_table_stmt => Ok(Statement::DropTable(DropTable {
            if_exists: has(&pair, Rule::if_exists),
            name: first_ident(pair),
        })),
        Rule::drop_index_stmt => Ok(Statement::DropIndex(DropIndex {
            if_exists: has(&pair, Rule::if_exists),
            table: child(pair.clone(), Rule::on_table).map(first_ident),
            name: first_ident(pair),
        })),
        Rule::insert_stmt => insert(pair).map(Statement::Insert),
        Rule::purge_stmt => Ok(Statement::Purge(
            pair.into_inner()
                .map(|part| {
                    Ok(Delete {
                        table: first_ident(part.clone()),
                        where_clause: where_clause(part)?,
                    })
                })
                .collect::<Result<_>>()?,
        )),
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
        Rule::reset_stmt => Ok(Statement::ResetStatements),
        Rule::set_setting => {
            let mut parts = pair.into_inner();
            let at = crate::settings::named("SET", parts.next().unwrap().as_str())?;
            let value = parts.next().unwrap().into_inner();
            let words: Vec<_> = value.map(|p| p.as_str()).collect();
            crate::settings::read(at, &words.join(" ")).map(|n| Statement::SetSetting(at, n))
        }
        Rule::show_setting => {
            let name = pair.clone().into_inner().next().unwrap();
            if let Some(report) = crate::query::REPORTS
                .iter()
                .position(|row| row.0[5..].eq_ignore_ascii_case(name.as_str()))
            {
                let mut names = idents(pair.clone()).into_iter();
                let count = |rule, what| {
                    child(pair.clone(), rule)
                        .map(|clause| number(&clause.into_inner().next().unwrap(), what))
                        .transpose()
                };
                return Ok(Statement::ShowReport(Show {
                    report,
                    column: names.next().zip(names.next()),
                    limit: count(Rule::limit_clause, "LIMIT")?,
                    offset: count(Rule::offset_clause, "OFFSET")?.unwrap_or(0),
                }));
            }
            if pair.clone().into_inner().count() != 1 {
                return Err(refuse("this setting has no report options"));
            }
            let selected = if name.as_str().eq_ignore_ascii_case("STORAGE_COMPACTION") {
                2..5
            } else {
                let at = crate::settings::named("SHOW", name.as_str())?;
                at..at + 1
            };
            Ok(Statement::ShowSetting(selected))
        }
        Rule::explain_stmt => {
            let explained = pair.into_inner().next().expect("the statement explained");
            statement(explained).map(|explained| Statement::Explain(Box::new(explained)))
        }
        _ => unreachable!("every statement has a parser"),
    }
}

/// The refusal for text the grammar does not read: where it stopped and the word it stopped at.
fn syntax_error(text: &str, error: &pest::error::Error<Rule>) -> Error {
    let (LineColLocation::Pos((line, column)) | LineColLocation::Span((line, column), _)) =
        error.line_col;
    let (InputLocation::Pos(at) | InputLocation::Span((at, _))) = error.location;
    let word = crate::types::excerpt(text[at..].split_whitespace().next().unwrap_or_default());
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

pub(crate) fn depth_error(depth: usize) -> Error {
    refuse(format!(
        "nesting of {depth} is deeper than the limit of {MAX_EXPR_NESTING}"
    ))
}

/// The words and marks of a statement, without its comments. A quoted text or name comes as its
/// whole span, so nothing inside quotes is ever taken for a keyword or a parenthesis.
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
                    None => break (rest, rest.len()),
                    Some(quote) if rest[end + quote + 1..].starts_with(first) => end += quote + 2,
                    Some(quote) => break (&rest[..end + quote + 1], end + quote + 1),
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

// What one token can make: two rule pairs of two places each in the parser's queue (pest 2.9.6's
// private token, two markers and positions and a rule, pinned by the exact dependency), counted
// twice for the queue's doubling, and the node built from it.
const QUEUED: usize = size_of::<(usize, usize, Option<Rule>, usize, usize)>();
const TOKEN: u64 = (2 * 2 * 2 * QUEUED + size_of::<Expr>()) as u64;

/// The parse bound: every allocation the parser makes is for a token or a node made from tokens,
/// so it is the tokens the first scan counts times the most one token makes, the text's bytes
/// for the slices kept, and 4 bytes a number of a vector, which is one token. The scan itself
/// never allocates.
pub(super) fn charge(text: &str) -> u64 {
    let mut bytes = text.len() as u64;
    let mut tokens = Tokens { text, at: 0 };
    while let Some(token) = tokens.next() {
        bytes = bytes.saturating_add(TOKEN);
        if token == "[" {
            // One number for each component the grammar reads: each comma starts the next.
            let mut numbers = 1u64;
            let mut vector = Tokens {
                at: tokens.at,
                ..tokens
            };
            while let Some(part) = vector.next() {
                if part == "]" {
                    bytes = bytes.saturating_add(4 * numbers);
                    tokens.at = vector.at;
                    break;
                }
                // An exponent is read as words: `5e`, `-`, `05`, or `1e5` whole.
                if part == "," {
                    numbers += 1;
                } else if !part.starts_with(|ch: char| ch.is_ascii_digit())
                    && !matches!(part, "." | "-" | "+")
                {
                    break;
                }
            }
        }
    }
    bytes
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
            _ if token.eq_ignore_ascii_case("NOT") || token.eq_ignore_ascii_case("EXPLAIN") => {
                prefixes += 1;
                operand_next = true;
            }
            _ => {
                prefixes = 0;
                operand_next = matches!(token, "=" | "<" | ">" | "+" | "-" | "*" | "/" | "%" | ",")
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

/// Reads a rank formula by the grammar's formula rules, into the tree any expression is.
pub(super) fn formula(text: &str) -> Result<Expr> {
    check_source_nesting(text)?;
    let read = SqlParser::parse(Rule::formula, text).map_err(|error| syntax_error(text, &error))?;
    expr::additive_expr(read.flatten().nth(1).expect("a formula is one sum"))
}

fn refuse(message: impl Into<String>) -> Error {
    Error::ParseError(message.into())
}

/// A name in lower case, or what is between the double quotes of a quoted one. What the
/// grammar reads `as_written` keeps its letters without quotes too.
fn ident(pair: &Pair<'_>) -> String {
    let raw = pair.as_str();
    let mut name = match raw.strip_prefix('"').and_then(|raw| raw.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None if pair.as_rule() == Rule::as_written => raw.to_owned(),
        None => raw.to_ascii_lowercase(),
    };
    name.truncate(super::identifier(&name).len());
    name
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
    pair.as_str().parse().map_err(|_| {
        refuse(format!(
            "{what} {} is out of range",
            crate::types::excerpt(pair.as_str())
        ))
    })
}

fn insert(pair: Pair<'_>) -> Result<Insert> {
    let mut values = Vec::new();
    for row in pair.clone().into_inner() {
        if row.as_rule() == Rule::values_row {
            values.push(expr::exprs(row)?);
        }
    }
    let on_conflict = child(pair.clone(), Rule::on_conflict)
        .map(|conflict| {
            Ok(OnConflict {
                columns: idents(child(conflict.clone(), Rule::columns).expect("a list")),
                assignments: assignments(conflict)?,
            })
        })
        .transpose()?;
    let mut names = idents(pair);
    let columns = names.split_off(1);
    Ok(Insert {
        on_conflict,
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

#[cfg(test)]
mod tests;
