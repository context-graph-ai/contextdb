//! Journey: paging a result larger than an ordinary ceiling.
//!
//! Contract held here: one cursor per CLI session; `.cursor open` takes exactly
//! one read-only SELECT and returns the first page; `.cursor fetch` returns the
//! next; `has_more: false` means genuine exhaustion and auto-closes the cursor,
//! after which draining or closing again is a clean success rather than an
//! error; a page carries only complete rows; the six cursor refusals each say
//! what to do next; and independent CLI sessions have independent cursors.
//!
//! Cursor idle and lifetime EXPIRY are deliberately absent from this wall: they
//! are proven deterministically against the manual deadline clock, which the CLI
//! does not expose. A test here could only sleep, which this estate forbids.

mod read_cli_support;

use read_cli_support::*;

fn paged_store(rows: usize) -> Store {
    store_with(&create_seeded_table("paged", rows))
}

fn cursor_pages(outcome: &Outcome) -> Vec<serde_json::Value> {
    documents_named(&outcome.stdout_docs(), "cursor")
        .into_iter()
        .filter(|d| d.get("rows").is_some())
        .collect()
}

fn refusal(outcome: &Outcome, kind: &str) -> serde_json::Value {
    outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some(kind))
        .unwrap_or_else(|| panic!("expected a {kind} refusal.\n{}", outcome.describe()))
}

#[test]
fn the_cursor_pages_a_result_the_ordinary_ceiling_refuses() {
    let store = paged_store(5);
    let bounded = [
        "--json",
        "--read-result-rows",
        "2",
        "--read-cursor-page-rows",
        "2",
    ];
    let mut args = vec![store.path_str()];
    args.extend_from_slice(&bounded);

    let refused = run(&args, "SELECT id FROM paged ORDER BY id;\n");
    assert_eq!(
        refused.code,
        Some(1),
        "the whole result is over the declared ceiling.\n{}",
        refused.describe()
    );

    let paged = run(
        &args,
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch\n.cursor fetch\n",
    );
    assert_eq!(
        paged.code,
        Some(0),
        "the escape the refusal names must reach the whole result.\n{}",
        paged.describe()
    );

    let pages = cursor_pages(&paged);
    let shape: Vec<(usize, bool)> = pages
        .iter()
        .map(|p| {
            (
                rows_of(p).len(),
                p.get("has_more").and_then(|h| h.as_bool()).unwrap_or(true),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![(2, true), (2, true), (1, false)],
        "every row arrives exactly once across the pages, and `has_more` is truthful.\n{}",
        paged.describe()
    );

    let seen: Vec<i64> = pages
        .iter()
        .flat_map(rows_of)
        .filter_map(|row| row.get("id").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(
        seen,
        vec![1, 2, 3, 4, 5],
        "paging returns the complete result, in order, with nothing duplicated or dropped.\n{}",
        paged.describe()
    );
}

#[test]
fn an_omitted_fetch_count_uses_the_declared_page_rows() {
    let store = paged_store(9);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "10",
            "--read-cursor-page-rows",
            "4",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch\n",
    );

    let pages = cursor_pages(&outcome);
    let sizes: Vec<usize> = pages.iter().map(|p| rows_of(p).len()).collect();
    assert_eq!(
        sizes,
        vec![4, 4],
        "omitting the row count uses the declared `cursor_page_rows`.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_first_page_carries_its_columns() {
    let store = paged_store(3);
    let outcome = run(
        &[store.path_str(), "--json"],
        ".cursor open SELECT id, label FROM paged ORDER BY id\n",
    );

    let page = cursor_pages(&outcome)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("expected one cursor page.\n{}", outcome.describe()));
    let columns: Vec<String> = page
        .get("columns")
        .and_then(|c| c.as_array())
        .map(|c| {
            c.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        columns,
        vec!["id".to_owned(), "label".to_owned()],
        "a cursor page names its columns, like every other result document: {page}"
    );
}

#[test]
fn draining_and_closing_past_the_end_stay_clean_successes() {
    let store = paged_store(2);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "4",
            "--read-cursor-page-rows",
            "4",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch\n.cursor close\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "a fetch-until-empty loop always exits clean: exhaustion auto-closes the cursor, a \
         further fetch answers an empty success page, and a further close is a no-op.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.errors().is_empty(),
        "draining past the end is never an error.\n{}",
        outcome.describe()
    );

    let pages = cursor_pages(&outcome);
    let shape: Vec<(usize, bool)> = pages
        .iter()
        .map(|p| {
            (
                rows_of(p).len(),
                p.get("has_more").and_then(|h| h.as_bool()).unwrap_or(true),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![(2, false), (0, false)],
        "the first page exhausts the result; the extra fetch answers one empty page.\n{}",
        outcome.describe()
    );

    let closed = documents_named(&outcome.stdout_docs(), "cursor")
        .into_iter()
        .find(|d| d.get("closed").is_some())
        .unwrap_or_else(|| panic!("expected a close document.\n{}", outcome.describe()));
    assert_eq!(
        closed.get("closed").and_then(|c| c.as_bool()),
        Some(true),
        "`.cursor close` answers its own document: {closed}"
    );
}

#[test]
fn fetching_a_cursor_that_never_existed_is_refused() {
    let store = paged_store(3);
    let outcome = run(&[store.path_str(), "--json"], ".cursor fetch\n");

    let error = refusal(&outcome, "cursor_not_found");
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "cursor_not_found is io-class: {error}"
    );
}

#[test]
fn fetching_after_an_explicit_close_is_refused() {
    let store = paged_store(6);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor close\n.cursor fetch\n",
    );

    assert!(
        outcome
            .error_kinds()
            .iter()
            .any(|k| k == "cursor_not_found"),
        "a cursor the session explicitly closed is gone — that is distinct from the drained \
         cursor's empty success page.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_second_cursor_in_one_session_is_refused_until_the_first_closes() {
    let store = paged_store(6);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n\
         .cursor open SELECT label FROM paged ORDER BY id\n\
         .cursor close\n\
         .cursor open SELECT label FROM paged ORDER BY id\n",
    );

    let error = refusal(&outcome, "cursor_already_open");
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("sql"),
        "cursor_already_open is sql-class: {error}"
    );
    assert_eq!(
        outcome.error_kinds().len(),
        1,
        "once the first cursor closes, opening another is ordinary.\n{}",
        outcome.describe()
    );
}

#[test]
fn independent_sessions_hold_independent_cursors() {
    let store = paged_store(6);
    let mut holder = terminal(&[
        store.path_str(),
        "--json",
        "--read-result-rows",
        "2",
        "--read-cursor-page-rows",
        "2",
    ]);
    holder.send_line(".cursor open SELECT id FROM paged ORDER BY id");
    holder.wait_for("\"has_more\"");

    let other = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n",
    );

    assert_eq!(
        other.code,
        Some(0),
        "the one-cursor restriction belongs to a CLI session, not to the store.\n{}",
        other.describe()
    );
    assert!(
        !other
            .error_kinds()
            .iter()
            .any(|k| k == "cursor_already_open"),
        "another session's open cursor must not refuse this one.\n{}",
        other.describe()
    );

    let (_, transcript) = holder.quit();
    assert!(
        !transcript.contains("cursor_already_open"),
        "and the holder is undisturbed.\ntranscript:\n{transcript}"
    );
}

#[test]
fn the_cursor_accepts_exactly_one_read_only_select() {
    let store = paged_store(3);
    let before = store.contents();

    let write_argument = run(
        &[store.path_str(), "--json"],
        ".cursor open INSERT INTO paged (id, label) VALUES (99, 'smuggled')\n",
    );
    assert!(
        write_argument
            .error_kinds()
            .iter()
            .any(|k| k == "write_requires_flag"),
        "a write handed to the cursor is refused as a write, telling the user to add --write.\n{}",
        write_argument.describe()
    );

    let nonsense = run(
        &[store.path_str(), "--json"],
        ".cursor open not a statement\n",
    );
    let error = refusal(&nonsense, "cursor_invalid_statement");
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("usage"),
        "anything that is not one SELECT is command misuse and is never executed: {error}"
    );
    assert_eq!(
        before,
        store.contents(),
        "nothing the cursor refuses ever executes, so the store folder is untouched"
    );
}

#[test]
fn an_explicit_fetch_above_the_effective_row_limit_is_refused() {
    let store = paged_store(20);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch 6\n",
    );

    let error = refusal(&outcome, "owner_limit_exceeded");
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("limit"))
            .and_then(|l| l.as_str()),
        Some("result_rows"),
        "a fetch may not ask for more rows than one bounded read may publish: {error}"
    );
}

#[test]
fn a_page_stops_on_bytes_with_complete_rows_and_a_truthful_has_more() {
    let payload = "x".repeat(512);
    let store = store_with(&format!(
        "CREATE TABLE chunky (id INTEGER PRIMARY KEY, body TEXT);\n\
         INSERT INTO chunky (id, body) VALUES (1, '{payload}'), (2, '{payload}'), (3, '{payload}');\n"
    ));

    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "4194304",
            "--read-cursor-page-bytes",
            "1024",
            "--read-cursor-page-rows",
            "10",
            "--read-result-rows",
            "10",
        ],
        ".cursor open SELECT id, body FROM chunky ORDER BY id\n.cursor fetch\n.cursor fetch\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "a page that fills its byte budget stops there and says there is more.\n{}",
        outcome.describe()
    );
    let pages = cursor_pages(&outcome);
    assert!(
        pages
            .iter()
            .any(|p| p.get("has_more").and_then(|h| h.as_bool()) == Some(true)),
        "the byte budget, not the row budget, ends the first page.\n{}",
        outcome.describe()
    );
    let total: usize = pages.iter().map(|p| rows_of(p).len()).sum();
    assert_eq!(
        total,
        3,
        "no row is split across pages and none is lost.\n{}",
        outcome.describe()
    );
    for page in &pages {
        for row in rows_of(page) {
            assert_eq!(
                row.get("body").and_then(|b| b.as_str()).map(str::len),
                Some(512),
                "a page contains only complete rows — no value is ever cut in half: {row}"
            );
        }
    }
}

#[test]
fn a_single_row_too_wide_for_a_page_refuses_and_says_what_to_do() {
    let payload = "y".repeat(4096);
    let store = store_with(&format!(
        "CREATE TABLE oversized (id INTEGER PRIMARY KEY, body TEXT);\n\
         INSERT INTO oversized (id, body) VALUES (1, '{payload}');\n"
    ));

    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-cursor-page-bytes",
            "512",
            "--read-result-bytes",
            "4194304",
        ],
        ".cursor open SELECT id, body FROM oversized ORDER BY id\n",
    );

    let error = refusal(&outcome, "owner_limit_exceeded");
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("limit"))
            .and_then(|l| l.as_str()),
        Some("cursor_page_bytes"),
        "a row that cannot fit a page even alone names the page-byte ceiling: {error}"
    );
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        message.contains("column"),
        "and it says to select fewer columns rather than cutting a value in half: {error}"
    );
    assert!(
        outcome.stdout_docs().is_empty() || cursor_pages(&outcome).is_empty(),
        "no partial page is published.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_write_session_may_page_outside_a_transaction_but_not_inside_one() {
    let store = store_with(&create_seeded_table("paged", 4));

    let outside = run(
        &[
            store.path_str(),
            "--write",
            "--json",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor close\n",
    );
    assert_eq!(
        outside.code,
        Some(0),
        "a write session with no open transaction may use the cursor.\n{}",
        outside.describe()
    );

    let inside = run(
        &[store.path_str(), "--write", "--json"],
        "BEGIN;\n\
         .cursor open SELECT id FROM paged ORDER BY id\n\
         INSERT INTO paged (id, label) VALUES (500, 'inside-the-transaction');\n\
         COMMIT;\n\
         SELECT id FROM paged WHERE id = 500;\n",
    );

    let error = refusal(&inside, "cursor_transaction_active");
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("sql"),
        "opening a cursor inside a write transaction is refused so cursor state never mixes \
         committed and uncommitted rows: {error}"
    );
    let docs = inside.stdout_docs();
    let result = expect_document(&docs, "result", "the statement after the refused cursor");
    assert_eq!(
        rows_of(&result).len(),
        1,
        "and the refusal leaves the transaction itself untouched — it still commits.\n{}",
        inside.describe()
    );
}

/// A fetch count above the ceiling refuses the REQUEST, not the read: it is
/// answered before anything is pulled, so no continuation position is consumed.
/// The cursor is the one escape the ceiling refusal itself teaches, so a reader
/// who mis-sizes a single page must not lose it — the read is exactly where
/// they left it and a smaller page is served from there.
#[test]
fn a_refused_over_limit_fetch_leaves_the_cursor_usable_for_a_smaller_page() {
    let store = paged_store(20);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch 6\n.cursor fetch 2\n",
    );

    let _ = refusal(&outcome, "owner_limit_exceeded");
    let pages = cursor_pages(&outcome);
    assert_eq!(
        pages.len(),
        2,
        "the open page and the smaller page after the refusal both arrive.\n{}",
        outcome.describe()
    );
    let ids: Vec<i64> = rows_of(&pages[1])
        .iter()
        .filter_map(|row| row.get("id").and_then(serde_json::Value::as_i64))
        .collect();
    assert_eq!(
        ids,
        vec![3, 4],
        "the smaller fetch is served, and it resumes where the first page ended — \
         the refused request consumed nothing.\n{}",
        outcome.describe()
    );
}

/// The other escape the reference promises after a ceiling refusal: close the
/// cursor and open a new one. Both must work in the same session.
#[test]
fn a_refused_over_limit_fetch_leaves_close_and_reopen_working() {
    let store = paged_store(20);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n\
         .cursor fetch 6\n\
         .cursor close\n\
         .cursor open SELECT id FROM paged ORDER BY id LIMIT 3\n",
    );

    let _ = refusal(&outcome, "owner_limit_exceeded");
    let docs = outcome.stdout_docs();
    let closed = documents_named(&docs, "cursor")
        .into_iter()
        .find(|d| d.get("closed").and_then(serde_json::Value::as_bool) == Some(true));
    assert!(
        closed.is_some(),
        "`.cursor close` after a ceiling refusal releases the cursor.\n{}",
        outcome.describe()
    );
    assert!(
        outcome
            .errors()
            .iter()
            .all(|error| detail_kind(error).as_deref() != Some("cursor_already_open")),
        "and the session's cursor slot is free again, so `.cursor open` is not refused.\n{}",
        outcome.describe()
    );
    assert_eq!(
        cursor_pages(&outcome).len(),
        2,
        "the reopened cursor publishes its first page.\n{}",
        outcome.describe()
    );
}

/// `owner_not_running` means Rust `request_owner` was called on a direct-file
/// route — an internal misuse, never a condition a CLI user doing a documented
/// thing may be handed. No cursor journey on the file route may surface it.
#[test]
fn a_cursor_refusal_never_leaks_an_internal_route_condition() {
    let store = paged_store(20);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n\
         .cursor fetch 6\n\
         .cursor fetch 6\n\
         .cursor close\n\
         .cursor close\n",
    );

    let leaked: Vec<String> = outcome
        .errors()
        .iter()
        .filter_map(detail_kind)
        .filter(|kind| kind == "owner_not_running")
        .collect();
    assert!(
        leaked.is_empty(),
        "an internal route-kind label reached the user on the file route.\n{}",
        outcome.describe()
    );
}

/// The refusal must hand back the command that works. `result_rows` is the
/// count this cursor WILL serve, so the refusal carries a copy-ready
/// `.cursor fetch <effective-limit>` — typed in `--json`, printed in human
/// mode — and running exactly that string pages the read on.
#[test]
fn the_over_limit_fetch_refusal_carries_the_fetch_command_that_works() {
    let store = paged_store(20);
    let bounded = [
        "--json",
        "--read-result-rows",
        "5",
        "--read-cursor-page-rows",
        "2",
    ];
    let mut args = vec![store.path_str()];
    args.extend_from_slice(&bounded);

    let outcome = run(
        &args,
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch 6\n",
    );
    let error = refusal(&outcome, "owner_limit_exceeded");
    let detail = error.get("detail").expect("refusal detail");
    assert_eq!(
        detail.get("remedy_command").and_then(|c| c.as_str()),
        Some(".cursor fetch 5"),
        "the refusal names the exact count this cursor will serve: {error}"
    );

    // And the printed string is the one that runs.
    let human = run(
        &[
            store.path_str(),
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch 6\n.cursor fetch 5\n",
    );
    assert!(
        human.stderr.contains(".cursor fetch 5"),
        "human mode prints the copy-ready remedy.\n{}",
        human.describe()
    );
    assert!(
        human.stdout.contains("(5 rows, has_more"),
        "and running exactly that remedy serves a full five-row page.\n{}",
        human.describe()
    );
}
