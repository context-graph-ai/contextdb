//! Journey: interrupting work without losing the session.
//!
//! Contract held here: in an interactive session, Ctrl-C during a running
//! statement or cursor fetch cancels that statement and returns to the prompt
//! with the session and any open cursor intact; Ctrl-C at an idle prompt clears
//! the typed line; a session that is NOT interactive keeps process termination.
//! On the owner route the promise crosses the process boundary: cancelling stops
//! the owning process's execution of that statement, and the owner here is a
//! genuinely separate process.
//!
//! Nothing in this file waits for a duration. A statement is known to be running
//! because the CLI publishes its `statement_progress` notice — the same notice
//! that exists so a long export is never mistaken for a hang — and the tests act
//! on that event.

mod read_cli_support;

use read_cli_support::*;

const READ_PROMPT: &str = "contextdb(ro)> ";

/// Two tables whose rows all share one join value, so the join examines every
/// pair. The fixture is deliberately larger than one statement's natural work,
/// and the query aggregates so its RESULT is one row — a cancelled statement is
/// therefore distinguishable from a completed one by the absence of a result
/// document, not by timing.
fn expensive_store() -> Store {
    let mut sql = String::from(
        "CREATE TABLE left_side (id INTEGER PRIMARY KEY, label TEXT);\n\
         CREATE TABLE right_side (id INTEGER PRIMARY KEY, label TEXT);\n\
         CREATE TABLE paged (id INTEGER PRIMARY KEY, label TEXT);\n",
    );
    for table in ["left_side", "right_side"] {
        for chunk in 0..9 {
            sql.push_str(&format!("INSERT INTO {table} (id, label) VALUES "));
            for offset in 0..100 {
                let id = chunk * 100 + offset + 1;
                if offset > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("({id}, 'shared-join-value')"));
            }
            sql.push_str(";\n");
        }
    }
    sql.push_str(&seed_rows("paged", 6));
    store_with(&sql)
}

const EXPENSIVE_STATEMENT: &str =
    "SELECT COUNT(*) FROM left_side INNER JOIN right_side ON left_side.label = right_side.label;";

/// Ceilings raised so the expensive statement is genuinely long-running rather
/// than refused, and the progress notice threshold lowered so "it is running" is
/// observable immediately.
fn patient_reader_args(store: &Store) -> Vec<&str> {
    vec![
        store.path_str(),
        "--json",
        "--read-hydration-notice-ms",
        "1",
        "--read-work",
        "100000000",
        "--read-active-ms",
        "600000",
        "--read-memory",
        "1073741824",
        "--read-result-rows",
        "4",
        "--read-cursor-page-rows",
        "2",
    ]
}

fn result_documents(transcript: &str) -> usize {
    lenient_json_lines(transcript)
        .into_iter()
        .filter(|d| d.get("result").is_some())
        .count()
}

#[test]
fn interrupting_a_running_statement_returns_to_the_prompt_with_the_session_alive() {
    let store = expensive_store();
    let mut session = terminal(&patient_reader_args(&store));
    session.wait_for(READ_PROMPT);

    session.send_line(EXPENSIVE_STATEMENT);
    session.wait_for("statement_progress");
    let results_while_running = result_documents(&session.transcript());
    session.interrupt();

    session.wait_for_count(READ_PROMPT, 2);
    assert_eq!(
        result_documents(&session.transcript()),
        results_while_running,
        "the cancelled statement publishes no result.\ntranscript:\n{}",
        session.transcript()
    );

    session.send_line("SELECT id FROM paged ORDER BY id LIMIT 2;");
    session.wait_for("\"result\"");

    let (code, transcript) = session.quit();
    assert_eq!(
        code,
        Some(0),
        "Ctrl-C cancelled the statement, not the session: the session went on to answer another \
         query and exited normally.\ntranscript:\n{transcript}"
    );
}

#[test]
fn an_open_cursor_survives_the_interruption() {
    let store = expensive_store();
    let mut session = terminal(&patient_reader_args(&store));
    session.wait_for(READ_PROMPT);

    session.send_line(".cursor open SELECT id FROM paged ORDER BY id");
    session.wait_for("\"has_more\"");

    session.send_line(EXPENSIVE_STATEMENT);
    session.wait_for("statement_progress");
    session.interrupt();
    session.wait_for_count(READ_PROMPT, 3);

    session.send_line(".cursor fetch");
    session.wait_for_count("\"has_more\"", 2);

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");
    assert!(
        !transcript.contains("cursor_not_found"),
        "cancelling a statement leaves the session's open cursor intact.\ntranscript:\n{transcript}"
    );

    let pages: Vec<serde_json::Value> = lenient_json_lines(&transcript)
        .into_iter()
        .filter_map(|d| d.get("cursor").cloned())
        .filter(|p| p.get("rows").is_some())
        .collect();
    let ids: Vec<i64> = pages
        .iter()
        .flat_map(rows_of)
        .filter_map(|row| row.get("id").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4],
        "and the cursor resumes exactly where it was, with nothing repeated or skipped.\
         \ntranscript:\n{transcript}"
    );
}

#[test]
fn interrupting_at_an_idle_prompt_clears_the_typed_line() {
    let store = expensive_store();
    let mut session = terminal(&patient_reader_args(&store));
    session.wait_for(READ_PROMPT);

    // Typed but never submitted.
    session.send("SELECT id FROM paged ORDER BY id LIMIT 1;");
    session.interrupt();
    session.wait_for_count(READ_PROMPT, 2);
    session.send_line("");

    session.send_line(".tables");
    session.wait_for("\"tables\"");

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");
    assert_eq!(
        result_documents(&transcript),
        0,
        "the cleared line was never executed — Ctrl-C at an idle prompt discards what was typed \
         and ends nothing.\ntranscript:\n{transcript}"
    );
}

#[test]
fn a_non_interactive_session_keeps_process_termination() {
    let store = expensive_store();
    let mut session = background(&patient_reader_args(&store));
    session.send_line(EXPENSIVE_STATEMENT);
    session.wait_for("statement_progress");

    session.interrupt();
    session.send_line("SELECT id FROM paged ORDER BY id LIMIT 1;");

    let (code, transcript) = session.finish();
    assert_ne!(
        code,
        Some(0),
        "a session with no terminal keeps the ordinary meaning of an interrupt: the process \
         ends.\ntranscript:\n{transcript}"
    );
    assert_eq!(
        result_documents(&transcript),
        0,
        "it does not carry on with the next statement.\ntranscript:\n{transcript}"
    );
}

#[test]
fn interrupting_stops_the_statement_inside_a_separate_owning_process() {
    let store = expensive_store();
    // The effective ceiling on the owner route is the stricter of the reader's
    // request and the owner's policy, so the owner is started as patient as the
    // reader: otherwise the shipped work ceiling refuses the expensive statement
    // at once and there is nothing running to interrupt.
    let _owner = live_owner_with(
        &store.path,
        &[
            "--owner-read-work",
            "100000000",
            "--owner-read-active-ms",
            "600000",
            "--owner-read-memory",
            "1073741824",
            "--owner-read-request-ms",
            "600000",
        ],
        "",
    );

    let mut session = terminal(&patient_reader_args(&store));
    session.wait_for(READ_PROMPT);

    session.send_line(".cursor open SELECT id FROM paged ORDER BY id");
    session.wait_for("\"has_more\"");

    session.send_line(EXPENSIVE_STATEMENT);
    session.wait_for("statement_progress");
    let results_while_running = result_documents(&session.transcript());
    session.interrupt();
    session.wait_for_count(READ_PROMPT, 3);

    assert_eq!(
        result_documents(&session.transcript()),
        results_while_running,
        "the interrupted statement published nothing.\ntranscript:\n{}",
        session.transcript()
    );

    session.send_line(".cursor fetch");
    session.wait_for_count("\"has_more\"", 2);
    session.send_line("SELECT id FROM paged ORDER BY id LIMIT 1;");
    session.wait_for("\"result\"");

    let (code, transcript) = session.quit();
    assert_eq!(
        code,
        Some(0),
        "against a live owner in its own process, cancelling stops that statement and leaves the \
         session, its route, and its cursor intact.\ntranscript:\n{transcript}"
    );
    assert!(
        !transcript.contains("owner_disconnected"),
        "cancelling a statement is not losing the owner.\ntranscript:\n{transcript}"
    );
    assert!(
        !transcript.contains("cursor_not_found"),
        "and it is not losing the cursor.\ntranscript:\n{transcript}"
    );
}
