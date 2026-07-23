//! The parser must never crash the process on multi-byte UTF-8 input — it
//! must parse (`Ok`) or reject with a typed `Error`, exactly like any other
//! malformed/unusual input. `parse()` used to panic with a byte-index
//! "not a char boundary" message.
//!
//! Root cause: `parse()` runs several manual pre-checks over the raw SQL
//! text BEFORE handing off to the pest grammar — `contains_token_outside_strings`
//! (checks for a bare `OVER` keyword, to reject window functions) walks the
//! input by `char_indices()` (so `idx` always lands on a char boundary) but
//! then slices a FIXED 4-byte window `input[idx..idx + "OVER".len()]` to
//! compare against the token. That fixed-byte window can end partway through
//! a later multi-byte character (a string's contents, a `--` comment's
//! contents, or bare text) even though `idx` itself was a valid boundary,
//! which panics `str`'s range-index. This reproduces with a `—` (em dash,
//! U+2014, 3 bytes) right after an opening quote, a `—` in the middle of a
//! string literal, and a `★`/`€` inside a `--` comment.
//!
//! Panic site: `contextdb-parser/src/parser.rs`, `contains_token_outside_strings`
//! (the `input[idx..idx + token.len()]` slice).
//!
//! Directed cases alone are insufficient to pin the FAILURE CLASS: a handful
//! of particular characters at particular offsets can be fixed by a
//! position- or code-point-specific workaround without fixing the
//! underlying byte-boundary bug. This file adds: a table-driven,
//! deterministically GENERATED matrix
//! over representative 2-/3-/4-byte scalars at every offset around the
//! 4-byte lookahead window, across every scanner context the grammar
//! supports (string literals, line comments, block comments, quoted
//! identifiers, bare text); a malformed-input matrix (unterminated strings
//! and comments); and a tightened AST assertion that checks the exact
//! `column = 'text'` shape instead of "a text literal appears somewhere in
//! the WHERE tree".

use contextdb_parser::ast::{BinOp, Expr, Literal, Statement};
use contextdb_parser::parse;

fn expect_select(stmt: contextdb_core::Result<Statement>, sql: &str) -> Statement {
    match stmt {
        Ok(s @ Statement::Select(_)) => s,
        other => panic!("expected Ok(Statement::Select(_)) for {sql:?}, got {other:?}"),
    }
}

/// Depth-first search for an EXACT `column = 'expected'` comparison anywhere
/// in the WHERE tree — the left side must be exactly `column` and the right
/// side must be exactly the text literal `expected`. This is deliberately
/// stricter than "some text literal appears somewhere in the tree": a
/// malformed AST that retains the right text under the wrong column, or
/// synthesizes the right shape from unrelated nodes, must NOT satisfy it.
fn contains_exact_text_comparison(expr: &Expr, column: &str, expected: &str) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let is_match = matches!(op, BinOp::Eq)
                && matches!(left.as_ref(), Expr::Column(c) if c.column == column)
                && matches!(right.as_ref(), Expr::Literal(Literal::Text(t)) if t == expected);
            is_match
                || contains_exact_text_comparison(left, column, expected)
                || contains_exact_text_comparison(right, column, expected)
        }
        _ => false,
    }
}

fn assert_where_has_exact_text_comparison(stmt: &Statement, column: &str, expected: &str) {
    let Statement::Select(select) = stmt else {
        panic!("expected a Select statement, got {stmt:?}");
    };
    let where_clause = select
        .body
        .where_clause
        .as_ref()
        .expect("expected a WHERE clause");
    assert!(
        contains_exact_text_comparison(where_clause, column, expected),
        "expected an exact `{column} = {expected:?}` comparison somewhere in the WHERE tree, got {where_clause:?}"
    );
}

// ============================================================================
// The parser must never panic on a multi-byte character positioned so that
// the fixed 4-byte "OVER" lookahead window ends inside it.
// ============================================================================

#[test]
fn parse_never_panics_on_multibyte_immediately_after_an_opening_quote() {
    // '—foo' — the em dash starts one byte after the opening `'`.
    let sql = "SELECT * FROM t WHERE name = '\u{2014}foo'";
    let stmt = expect_select(parse(sql), sql);
    assert_where_has_exact_text_comparison(&stmt, "name", "\u{2014}foo");
}

#[test]
fn parse_never_panics_on_multibyte_in_the_middle_of_a_string_literal() {
    // 'a—b' — the em dash sits between two ASCII characters inside the string.
    let sql = "SELECT * FROM t WHERE name = 'a\u{2014}b' AND x = 1";
    let stmt = expect_select(parse(sql), sql);
    assert_where_has_exact_text_comparison(&stmt, "name", "a\u{2014}b");
}

#[test]
fn parse_never_panics_on_a_leading_comment_containing_a_star_symbol() {
    let sql = "-- \u{2605} star comment\nSELECT 1";
    expect_select(parse(sql), sql);
}

#[test]
fn parse_never_panics_on_a_trailing_comment_containing_a_star_symbol() {
    let sql = "SELECT 1 -- \u{2605} star";
    expect_select(parse(sql), sql);
}

#[test]
fn parse_never_panics_on_a_comment_containing_a_currency_symbol() {
    let sql = "-- \u{20AC}uro sign\nSELECT 1";
    expect_select(parse(sql), sql);
}

// ============================================================================
// Nearby boundary shapes at the same panic site — these do NOT currently
// panic (the fixed-byte window happens to land on a boundary for these
// particular byte offsets), but they exercise the same code path and must
// stay safe under any fix. Kept in this file so a fix can't "solve" the
// failing cases above by re-introducing a narrower version of the same bug.
// ============================================================================

#[test]
fn parse_handles_ellipsis_inside_a_string_literal() {
    let sql = "SELECT * FROM t WHERE name = 'hello\u{2026} world'";
    let stmt = expect_select(parse(sql), sql);
    assert_where_has_exact_text_comparison(&stmt, "name", "hello\u{2026} world");
}

#[test]
fn parse_handles_multibyte_immediately_before_a_closing_quote() {
    let sql = "SELECT * FROM t WHERE name = 'foo\u{2014}'";
    let stmt = expect_select(parse(sql), sql);
    assert_where_has_exact_text_comparison(&stmt, "name", "foo\u{2014}");
}

#[test]
fn parse_handles_a_string_literal_that_is_only_a_multibyte_character() {
    let sql = "SELECT '\u{2014}' FROM t";
    let stmt = expect_select(parse(sql), sql);
    let Statement::Select(select) = &stmt else {
        unreachable!()
    };
    assert!(matches!(
        select.body.columns.first().map(|c| &c.expr),
        Some(Expr::Literal(Literal::Text(t))) if t == "\u{2014}"
    ));
}

#[test]
fn parse_handles_ellipsis_inside_a_comment() {
    let sql = "-- comment with ellipsis \u{2026} here\nSELECT 1";
    expect_select(parse(sql), sql);
}

#[test]
fn parse_handles_emoji_inside_a_comment() {
    let sql = "-- comment with emoji \u{1F600} here\nSELECT 1";
    expect_select(parse(sql), sql);
}

#[test]
fn parse_handles_trailing_multibyte_comment_at_end_of_input() {
    let sql = "SELECT 1 -- trailing \u{2014}\n";
    expect_select(parse(sql), sql);
}

// ============================================================================
// Generated width/offset/context matrix. The
// directed cases above enumerate a handful of particular characters at
// particular offsets — a position- or code-point-specific workaround could
// pass them without fixing the underlying byte-boundary invariant. This test
// instead GENERATES its inputs: every combination of a representative
// 2-/3-/4-byte scalar, an ASCII-prefix length bracketing the 4-byte lookahead
// window used by `contains_token_outside_strings`, and every scanner context
// the grammar supports. It asserts only "no panic" (`Ok` or a typed `Err`
// are both acceptable) via `catch_unwind`, so one panicking case does not
// stop the sweep from reporting every other panicking case in the same run.
// ============================================================================

/// Representative multi-byte scalars: é (2 bytes), € (3 bytes), 😀 (4 bytes).
const SCALARS: [(&str, char); 3] = [
    ("2-byte (e-acute)", '\u{00E9}'),
    ("3-byte (euro sign)", '\u{20AC}'),
    ("4-byte (grinning face emoji)", '\u{1F600}'),
];

/// ASCII-prefix lengths bracketing the 4-byte "OVER" lookahead window: 0
/// through 8 bytes of leading ASCII puts the scalar's first byte at every
/// offset that could align a word-boundary-preceded 4-byte slice to land
/// inside it (window width 4, plus margin for the scalar's own width up to
/// 4 bytes).
const PREFIX_LENS: std::ops::RangeInclusive<usize> = 0..=8;

#[derive(Clone, Copy)]
enum ScanContext {
    StringLiteral,
    LineComment,
    BlockComment,
    QuotedIdentifier,
    BareText,
}

impl ScanContext {
    const ALL: [ScanContext; 5] = [
        ScanContext::StringLiteral,
        ScanContext::LineComment,
        ScanContext::BlockComment,
        ScanContext::QuotedIdentifier,
        ScanContext::BareText,
    ];

    fn label(self) -> &'static str {
        match self {
            ScanContext::StringLiteral => "string literal",
            ScanContext::LineComment => "line comment (--)",
            ScanContext::BlockComment => "block comment (/* */)",
            ScanContext::QuotedIdentifier => "double-quoted identifier",
            ScanContext::BareText => "bare unquoted text",
        }
    }

    fn build(self, prefix: &str, scalar: char) -> String {
        match self {
            ScanContext::StringLiteral => {
                format!("SELECT * FROM t WHERE name = '{prefix}{scalar}suffix'")
            }
            ScanContext::LineComment => format!("-- {prefix}{scalar}suffix\nSELECT 1"),
            ScanContext::BlockComment => format!("/* {prefix}{scalar}suffix */ SELECT 1"),
            ScanContext::QuotedIdentifier => {
                format!("SELECT \"{prefix}{scalar}suffix\" FROM t")
            }
            ScanContext::BareText => format!("SELECT {prefix}{scalar}suffix FROM t"),
        }
    }
}

#[test]
fn parse_never_panics_across_a_generated_utf8_width_and_offset_matrix() {
    // Pre-fix, most of this matrix panics — silence the default panic hook's
    // per-case backtrace spam for the duration of this test only.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let mut panics = Vec::new();
    let mut total = 0usize;
    for (scalar_label, scalar) in SCALARS {
        for prefix_len in PREFIX_LENS {
            let prefix: String = std::iter::repeat_n('x', prefix_len).collect();
            for ctx in ScanContext::ALL {
                let sql = ctx.build(&prefix, scalar);
                total += 1;
                // Only "did it panic" is asserted here — Ok and typed Err are
                // both acceptable outcomes for a generated, possibly-invalid
                // shape (e.g. a bare unquoted multi-byte token).
                if std::panic::catch_unwind(|| parse(&sql)).is_err() {
                    panics.push(format!(
                        "{scalar_label} at ASCII-prefix len {prefix_len} inside {}: {sql:?}",
                        ctx.label()
                    ));
                }
            }
        }
    }

    std::panic::set_hook(default_hook);

    assert!(
        panics.is_empty(),
        "parse() panicked on {}/{} generated width/offset/context cases (Ok or a typed Err are both fine — only a panic fails this test):\n{}",
        panics.len(),
        total,
        panics.join("\n")
    );
}

// ============================================================================
// Malformed input: the "parse or reject, never panic"
// contract must also hold for input that is not expected to parse
// successfully — an unterminated string/identifier/comment containing a
// multi-byte character right at the point truncation occurs. Every case here
// is genuinely malformed, so — unlike the generated matrix above, which
// mixes syntactically valid and invalid shapes — the inner result is
// asserted to be a typed `Err`, not just "no panic".
// ============================================================================

fn assert_never_panics_and_is_err(sql: &str) {
    let outcome = std::panic::catch_unwind(|| parse(sql));
    let result = outcome.unwrap_or_else(|_| panic!("parse() panicked on malformed input {sql:?}"));
    assert!(
        result.is_err(),
        "malformed input {sql:?} must be rejected with a typed Err, got {result:?}"
    );
}

#[test]
fn parse_never_panics_on_an_unterminated_string_literal_containing_multibyte() {
    assert_never_panics_and_is_err("SELECT * FROM t WHERE name = 'unterminated \u{2014} string");
}

#[test]
fn parse_never_panics_on_an_unterminated_string_ending_exactly_at_a_multibyte_char() {
    // The multi-byte character is the very last byte(s) of the whole input —
    // no closing quote follows it at all.
    assert_never_panics_and_is_err("SELECT * FROM t WHERE name = 'x\u{1F600}");
}

#[test]
fn parse_never_panics_on_an_unterminated_quoted_identifier_containing_multibyte() {
    assert_never_panics_and_is_err("SELECT \"unterminated \u{20AC} ident FROM t");
}

#[test]
fn parse_never_panics_on_an_unterminated_block_comment_containing_multibyte() {
    assert_never_panics_and_is_err("/* unterminated \u{1F600} block\nSELECT 1");
}

#[test]
fn parse_never_panics_on_a_dangling_escaped_quote_after_multibyte_at_eof() {
    // Two trailing quotes after the multi-byte content are consumed as one
    // escaped-quote pair (a literal `'` inside the string), so the string is
    // still open with no real closing quote following — genuinely
    // unterminated, must reject cleanly, never panic.
    assert_never_panics_and_is_err("SELECT * FROM t WHERE name = 'a\u{2014}b''");
}
