//! Journey: an ordinary result is complete or it is refused — never truncated.
//!
//! Contract held here: under shipped defaults a `SELECT` succeeds only when it
//! carries at most 500 rows and 4 MiB of canonical encoding. Crossing either
//! ceiling publishes NO rows and refuses with `owner_limit_exceeded`, naming the
//! ceiling it crossed and carrying the statement back to the user with two
//! executable escapes: page it with `.cursor open`, or raise the ceiling on the
//! route that permits it. `--json` changes rendering only, never a ceiling, and
//! a `--write` session's reads are bounded like anyone's.

mod read_cli_support;

use read_cli_support::*;

const SHIPPED_RESULT_ROWS: i64 = 500;

fn store_of(rows: usize) -> Store {
    store_with(&create_seeded_table("bounded", rows))
}

fn limit_error(outcome: &Outcome) -> serde_json::Value {
    outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| {
            panic!(
                "expected an owner_limit_exceeded refusal.\n{}",
                outcome.describe()
            )
        })
}

#[test]
fn a_result_at_the_shipped_row_ceiling_succeeds_complete() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id LIMIT 500;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "the exact shipped ceiling succeeds.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "a successful SELECT under --json");
    assert_eq!(
        rows_of(&result).len(),
        500,
        "a successful result publishes every one of its rows: {}",
        outcome.describe()
    );
}

#[test]
fn one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "crossing a read ceiling is a runtime refusal.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "a refused result publishes NO rows — a partial answer is never emitted.\n{}",
        outcome.describe()
    );

    let error = limit_error(&outcome);
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "a crossed read ceiling is an io-class refusal, not a SQL error: {error}"
    );
    let detail = error.get("detail").expect("refusal detail");
    assert_eq!(
        detail.get("limit").and_then(|l| l.as_str()),
        Some("result_rows"),
        "the refusal names which ceiling was crossed: {error}"
    );
    assert_eq!(
        detail.get("value").and_then(|v| v.as_i64()),
        Some(SHIPPED_RESULT_ROWS),
        "the refusal carries the effective ceiling as an integer: {error}"
    );
}

#[test]
fn the_refusal_carries_the_statement_and_a_copy_ready_cursor_command() {
    let store = store_of(501);
    let machine = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );
    let error = limit_error(&machine);
    let detail = error.get("detail").expect("refusal detail");

    let statement = detail
        .get("statement")
        .and_then(|s| s.as_str())
        .unwrap_or_default();
    assert!(
        statement.contains("SELECT") && statement.contains("bounded"),
        "the refusal carries the refused statement verbatim so an agent can re-issue it: {error}"
    );
    let remedy = detail
        .get("remedy_command")
        .and_then(|r| r.as_str())
        .unwrap_or_default();
    assert!(
        remedy.starts_with(".cursor open") && remedy.contains("bounded"),
        "on the file route the detail carries the copy-ready `.cursor open <same SELECT>` line: {error}"
    );

    let human = run(
        &[store.path_str()],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );
    assert!(
        human.stderr.contains(".cursor open") && human.stderr.contains("bounded"),
        "human mode prints the copy-ready cursor command, so the instruction is executable as printed.\n{}",
        human.describe()
    );
}

#[test]
fn the_file_route_refusal_names_raising_the_result_limits_as_the_export_escape() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str()],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert!(
        outcome.stderr.contains("--read-result-rows")
            || outcome.stderr.contains("--read-result-bytes"),
        "reading a file directly, the second escape is a deliberate one-shot export through raised \
         `--read-result-*` limits — the replacement for the removed row-cap flag.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stderr.contains("--all"),
        "the removed flag is never offered as a remedy.\n{}",
        outcome.describe()
    );
}

#[test]
fn raising_the_row_limit_publishes_the_complete_result() {
    let store = store_of(501);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "600",
            "--read-cursor-page-rows",
            "100",
        ],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "the remedy the refusal named must actually work.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "the raised-limit export");
    assert_eq!(
        rows_of(&result).len(),
        501,
        "the export publishes every row.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_lowered_row_ceiling_moves_the_boundary_exactly() {
    let store = store_of(8);
    let args = [
        "--json",
        "--read-result-rows",
        "3",
        "--read-cursor-page-rows",
        "3",
    ];

    let mut at_ceiling = vec![store.path_str()];
    at_ceiling.extend_from_slice(&args);
    let ok = run(&at_ceiling, "SELECT id FROM bounded ORDER BY id LIMIT 3;\n");
    assert_eq!(
        ok.code,
        Some(0),
        "the exact declared ceiling succeeds.\n{}",
        ok.describe()
    );

    let refused = run(&at_ceiling, "SELECT id FROM bounded ORDER BY id LIMIT 4;\n");
    assert_eq!(
        refused.code,
        Some(1),
        "one row beyond the declared ceiling is refused.\n{}",
        refused.describe()
    );
    let error = limit_error(&refused);
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("value"))
            .and_then(|v| v.as_i64()),
        Some(3),
        "the refusal reports the EFFECTIVE ceiling, which is the one the caller declared: {error}"
    );
}

#[test]
fn a_result_over_the_byte_ceiling_is_refused_naming_result_bytes() {
    let store = store_of(200);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "256",
            "--read-cursor-page-bytes",
            "256",
        ],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "the byte ceiling refuses just like the row ceiling.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "no partial result is published when the encoding will not fit.\n{}",
        outcome.describe()
    );
    let error = limit_error(&outcome);
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("limit"))
            .and_then(|l| l.as_str()),
        Some("result_bytes"),
        "the refusal names the byte ceiling, not the row ceiling: {error}"
    );
}

#[test]
fn human_and_machine_rendering_cross_the_same_boundary() {
    let store = store_of(501);
    let sql = "SELECT id, label FROM bounded ORDER BY id;\n";

    let human = run(&[store.path_str()], sql);
    let machine = run(&[store.path_str(), "--json"], sql);

    assert_eq!(
        human.code,
        machine.code,
        "`--json` changes rendering only; it never bypasses or moves a ceiling.\nhuman:\n{}\nmachine:\n{}",
        human.describe(),
        machine.describe()
    );
    assert!(
        human.stdout.trim().is_empty() && machine.stdout.trim().is_empty(),
        "neither rendering publishes a partial result.\nhuman:\n{}\nmachine:\n{}",
        human.describe(),
        machine.describe()
    );
}

#[test]
fn a_write_session_reads_under_the_same_ceiling() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--write", "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "every CLI SELECT is bounded, including in a `--write` session.\n{}",
        outcome.describe()
    );
    assert!(
        outcome
            .error_kinds()
            .iter()
            .any(|k| k == "owner_limit_exceeded"),
        "the write session's read crosses the same ceiling with the same refusal.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_session_keeps_running_after_a_refusal_and_reports_it_in_the_exit_code() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\nSELECT id FROM bounded ORDER BY id LIMIT 2;\n",
    );

    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "the statement after the refusal");
    assert_eq!(
        rows_of(&result).len(),
        2,
        "a multi-statement session keeps executing after a refused statement.\n{}",
        outcome.describe()
    );
    assert_eq!(
        outcome.code,
        Some(1),
        "a non-interactive run that suffered any refusal exits 1, so automation never reads a \
         half-run as success.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_session_with_no_refusal_exits_zero() {
    let store = store_of(10);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM bounded ORDER BY id;\n.tables\n",
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "a run where every statement succeeded exits 0.\n{}",
        outcome.describe()
    );
}
