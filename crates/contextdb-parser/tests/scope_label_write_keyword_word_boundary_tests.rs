//! `find_keyword_outside_strings` (`crates/contextdb-parser/src/parser.rs`,
//! used by `build_scope_label_split` to locate the `WRITE` keyword that
//! separates a `SCOPE_LABEL_READ (...) WRITE (...)` column constraint's two
//! label lists) is a plain ASCII-case-insensitive SUBSTRING search: it has
//! no word-boundary check, so it matches `WRITE` inside any longer run of
//! unquoted text, not only the literal `WRITE` keyword token.
//!
//! The grammar's implicit `WHITESPACE` rule (`grammar.pest`) includes SQL
//! comments (`-- ...` and `/* ... */`), and comments are legal between any
//! two tokens of `scope_label_split` — including between the `SCOPE_LABEL_READ`
//! label list and the real `WRITE` keyword. A comment containing the
//! substring "write" (e.g. the English word "rewrite") sitting inside the
//! READ label list is matched FIRST by the naive substring search, so the
//! split point lands inside the comment instead of at the real `WRITE`
//! keyword — corrupting which quoted labels end up on the READ side vs the
//! WRITE side, or (when the false match precedes every real label)
//! producing a spurious "requires read and write labels" parse error on
//! otherwise-valid SQL.
//!
//! Every label in the `SCOPE_LABEL_READ (...) WRITE (...)` lists is
//! grammatically a quoted `string` (`grammar.pest`'s `scope_label_split`
//! accepts only the `string` rule inside each parenthesized list, never a
//! bare identifier), so a keyword substring landing INSIDE an unquoted
//! label is grammatically unreachable — `find_keyword_outside_strings`'s
//! `in_string` toggle already skips everything between quotes, so a label
//! such as `'tenant_write_a'` (see the positive control below) can never be
//! mis-split regardless of word-boundary handling. The reachable defect is
//! comment handling: a SQL comment (which the grammar's implicit whitespace
//! rule allows between any two tokens here, including between the READ list
//! and the real `WRITE` keyword) containing the substring "write" is
//! matched by the naive substring search before the real keyword — the
//! adversarial cases below pin exactly that.

use contextdb_core::Error;
use contextdb_parser::ast::{ScopeLabelConstraint, Statement};
use contextdb_parser::parse;

fn scope_label_constraint(sql: &str) -> ScopeLabelConstraint {
    let stmt = parse(sql).unwrap_or_else(|err| panic!("must parse: {sql:?}: {err:?}"));
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CreateTable, got {other:?}"),
    };
    let column = ct
        .columns
        .iter()
        .find(|c| c.name == "scope")
        .expect("column `scope` must be present");
    *column
        .scope_label
        .clone()
        .expect("column `scope` must carry a ScopeLabelConstraint")
}

// ============================================================================
// A block comment containing "write" as a substring ("rewrite"), placed
// BEFORE the real WRITE keyword's opening paren, sits between the last READ
// label and the real WRITE keyword — a comment position standard SQL
// tooling allows anywhere whitespace is legal. Today the false match lands
// inside the comment, before the real WRITE keyword's `(`, so the WRITE
// half of the split still contains the real WRITE list untouched, but this
// pins the exact split point is the KEYWORD, not a coincidental substring —
// see the adversarial variant below for where a wrong split point actually
// corrupts the parsed labels.
// ============================================================================
#[test]
fn comment_containing_write_substring_between_groups_does_not_break_parsing() {
    let constraint = scope_label_constraint(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ ('edge', 'server') /* rewrite pending review */ WRITE ('server'))",
    );
    assert_eq!(
        constraint,
        ScopeLabelConstraint::Split {
            read: vec!["edge".to_string(), "server".to_string()],
            write: vec!["server".to_string()],
        },
        "a comment containing \"write\" as a substring must not corrupt the \
         parsed read/write label lists"
    );
}

// ============================================================================
// Adversarial placement: the "write"-containing comment sits INSIDE the
// READ label list, before its closing paren and before the real WRITE
// keyword. A word-boundary-blind search finds the false match here first;
// the wrong split point then truncates the READ half (losing `'server'`,
// which sits after the comment) and folds it into the WRITE half instead —
// producing a WRONG parse (`read: ['edge']`, `write` containing `'server'`
// twice) rather than a loud error, which is the more dangerous failure
// mode: silently wrong ACL/scope enforcement rather than a refusal.
// ============================================================================
#[test]
fn comment_containing_write_substring_inside_read_list_does_not_corrupt_labels() {
    let constraint = scope_label_constraint(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ ('edge', /* rewrite pending review */ 'server') WRITE ('server'))",
    );
    assert_eq!(
        constraint,
        ScopeLabelConstraint::Split {
            read: vec!["edge".to_string(), "server".to_string()],
            write: vec!["server".to_string()],
        },
        "a comment containing \"write\" as a substring, sitting inside the \
         READ label list, must not move a READ label into the WRITE side"
    );
}

// ============================================================================
// A line comment (`-- ...`) containing "write" as a substring
// ("overwrite"), placed right after the `SCOPE_LABEL_READ` keyword and
// before its label list's opening paren — the false match here precedes
// EVERY real label, so today's split point falls before the parser has
// consumed any quoted string at all: `read` ends up empty, which trips the
// explicit "requires read and write labels" refusal on SQL that is
// otherwise entirely valid.
// ============================================================================
#[test]
fn line_comment_containing_write_substring_before_read_list_does_not_error() {
    let constraint = scope_label_constraint(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ -- overwrite policy, revisit later\n         ('edge', 'server') WRITE ('server'))",
    );
    assert_eq!(
        constraint,
        ScopeLabelConstraint::Split {
            read: vec!["edge".to_string(), "server".to_string()],
            write: vec!["server".to_string()],
        },
        "a line comment containing \"write\" as a substring, placed before \
         the READ label list, must not empty out the parsed READ labels"
    );
}

// ============================================================================
// Same adversarial shape, asserted as an explicit non-error: today this
// exact statement fails to parse at all (`ParseError` from
// `build_scope_label_split`'s explicit empty-read-or-write refusal),
// which is the clearest possible way to show the bug independent of the
// `ScopeLabelConstraint` equality check above.
// ============================================================================
#[test]
fn line_comment_containing_write_substring_before_read_list_parses_successfully() {
    let result = parse(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ -- overwrite policy, revisit later\n         ('edge', 'server') WRITE ('server'))",
    );
    match result {
        Ok(Statement::CreateTable(_)) => {}
        Ok(other) => panic!("expected CreateTable, got {other:?}"),
        Err(Error::ParseError(message)) => panic!(
            "a comment containing \"write\" as a substring must not turn valid \
             SCOPE_LABEL_READ ... WRITE ... SQL into a parse error: {message}"
        ),
        Err(other) => panic!("expected successful parse, got {other:?}"),
    }
}

// ============================================================================
// Positive control (must stay green): the real WRITE keyword is still
// found correctly with no comment present at all — the baseline this file's
// adversarial cases must not regress.
// ============================================================================
#[test]
fn split_scope_label_without_any_comment_still_parses() {
    let constraint = scope_label_constraint(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('server'))",
    );
    assert_eq!(
        constraint,
        ScopeLabelConstraint::Split {
            read: vec!["edge".to_string(), "server".to_string()],
            write: vec!["server".to_string()],
        }
    );
}

// ============================================================================
// Positive control (must stay green): a quoted LABEL containing "write" as
// a substring (`'tenant_write_a'`) is already handled correctly today — the
// `in_string` toggle in `find_keyword_outside_strings` skips quoted content
// — and must remain correct once word-boundary matching is added outside
// strings.
// ============================================================================
#[test]
fn quoted_label_containing_write_substring_is_unaffected() {
    let constraint = scope_label_constraint(
        "CREATE TABLE t (id UUID PRIMARY KEY, \
         scope TEXT SCOPE_LABEL_READ ('tenant_write_a', 'tenant_b') WRITE ('tenant_write_a', 'tenant_b'))",
    );
    assert_eq!(
        constraint,
        ScopeLabelConstraint::Split {
            read: vec!["tenant_write_a".to_string(), "tenant_b".to_string()],
            write: vec!["tenant_write_a".to_string(), "tenant_b".to_string()],
        }
    );
}
