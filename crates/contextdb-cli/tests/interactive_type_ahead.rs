//! Journey: type-ahead survives.
//!
//! Contract held here: two complete statements delivered to the session in
//! ONE write — a paste, or any automation driving the terminal with a single
//! write(2) — are BOTH executed, in order, exactly as if they had arrived one
//! at a time. The second statement must not be lost while the first is still
//! running from the terminal's point of view.
//!
//! Every send in this file uses one `Terminal::send` call carrying both
//! complete statements, never two separate `send_line` calls — two sends
//! could legitimately land as two separate writes and would not exercise the
//! type-ahead path this file exists to pin. The piped control test proves the
//! two statements themselves are unremarkable: the same two lines, delivered
//! the pipe's way, already produce two answers today.

mod read_cli_support;

use read_cli_support::*;

const READ_PROMPT: &str = "contextdb(ro)> ";

/// A table whose two distinguishable answers are the row counts themselves:
/// one row for the first statement, three for the second. No timing is
/// involved — no fixture wait, no slow join — the failure mode this file
/// pins is a discarded read, not a race.
fn two_statement_store() -> Store {
    store_with(&create_seeded_table("rows", 3))
}

const FIRST_STATEMENT: &str = "SELECT id FROM rows WHERE id = 1;";
const SECOND_STATEMENT: &str = "SELECT id FROM rows ORDER BY id;";

fn both_statements_one_write() -> String {
    format!("{FIRST_STATEMENT}\n{SECOND_STATEMENT}\n")
}

#[test]
fn a_terminal_session_executes_both_type_ahead_statements_in_human_mode() {
    let store = two_statement_store();
    let mut session = terminal(&[store.path_str()]);
    session.wait_for(READ_PROMPT);

    session.send(&both_statements_one_write());

    session.wait_for("(1 rows)");
    session.wait_for("(3 rows)");

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");

    assert_eq!(
        transcript.matches("(1 rows)").count(),
        1,
        "the first statement answers exactly once.\ntranscript:\n{transcript}"
    );
    assert_eq!(
        transcript.matches("(3 rows)").count(),
        1,
        "the second statement — typed ahead in the same write as the first — is not lost: it \
         answers exactly once too.\ntranscript:\n{transcript}"
    );
    let one_row_at = transcript
        .find("(1 rows)")
        .expect("already asserted present above");
    let three_rows_at = transcript
        .find("(3 rows)")
        .expect("already asserted present above");
    assert!(
        one_row_at < three_rows_at,
        "the two answers appear in the order the statements were sent, first before second.\n\
         transcript:\n{transcript}"
    );
}

#[test]
fn a_terminal_session_executes_both_type_ahead_statements_under_json() {
    let store = two_statement_store();
    let mut session = terminal(&[store.path_str(), "--json"]);
    session.wait_for(READ_PROMPT);

    session.send(&both_statements_one_write());

    session.wait_for_count("\"result\"", 2);

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");

    let result_docs: Vec<serde_json::Value> = lenient_json_lines(&transcript)
        .into_iter()
        .filter_map(|d| d.get("result").cloned())
        .collect();
    assert_eq!(
        result_docs.len(),
        2,
        "both statements delivered in one write each publish their own `{{\"result\":…}}` \
         document — the second is not silently dropped.\ntranscript:\n{transcript}"
    );
    let row_counts: Vec<usize> = result_docs.iter().map(rows_of).map(|r| r.len()).collect();
    assert_eq!(
        row_counts,
        vec![1, 3],
        "the documents appear in the order the statements were sent: the first statement's \
         single row before the second statement's three.\ntranscript:\n{transcript}"
    );
}

/// Control: the same two statements, delivered the piped way, already produce
/// two answers today. This test is expected to PASS — it establishes that the
/// loss pinned above is specific to the interactive (pty) device, not a
/// property of the two statements themselves.
#[test]
fn a_piped_session_executes_both_statements_from_one_stdin_write() {
    let store = two_statement_store();
    let outcome = run(&[store.path_str(), "--json"], &both_statements_one_write());

    assert_eq!(outcome.code, Some(0), "{}", outcome.describe());

    let docs = outcome.stdout_docs();
    let result_docs = documents_named(&docs, "result");
    assert_eq!(
        result_docs.len(),
        2,
        "the piped route answers both statements from one stdin write — the control this file's \
         interactive pins are measured against.\n{}",
        outcome.describe()
    );
    let row_counts: Vec<usize> = result_docs.iter().map(rows_of).map(|r| r.len()).collect();
    assert_eq!(
        row_counts,
        vec![1, 3],
        "in the order sent, first before second.\n{}",
        outcome.describe()
    );
}
