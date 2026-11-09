//! Journey: `.explain` announces how the reading shell reads, like a query.
//!
//! Contract held here: `.explain <sql>` reads the store, so when it is the
//! first command of a reading shell it reports the route, and when it is the
//! first command after the shell's writer exits it reports the change, on
//! standard error, exactly as a `SELECT` would.
//!
//! The writer is a separate process. Each phase ends with an unknown command,
//! refused on standard error after any notice for that phase and without
//! reading the store, so the transcript holds the phase's notices before
//! they are counted.

mod read_cli_support;

use read_cli_support::*;
use serde_json::Value;

fn routes_announced(session: &Background) -> Vec<String> {
    session
        .documents()
        .into_iter()
        .filter_map(|document| document.get("notice").cloned())
        .filter_map(|notice| {
            let detail = notice.get("detail")?;
            (detail.get("kind").and_then(Value::as_str) == Some("read_route")).then(|| {
                detail
                    .get("route")
                    .and_then(Value::as_str)
                    .expect("route word")
                    .to_owned()
            })
        })
        .collect()
}

fn explain_phase(session: &mut Background, phase: &str) {
    session.send_line(".explain SELECT id FROM shared ORDER BY id");
    session.send_line(&format!(".end_of_{phase}"));
    session.wait_for(&format!(".end_of_{phase}"));
}

#[test]
fn explain_reports_the_route_first_and_again_after_the_writer_exits() {
    let store = store_with(&create_seeded_table("shared", 2));
    let writer = live_owner(&store.path, "");
    let mut session = background(&[store.path_str(), "--json"]);

    explain_phase(&mut session, "first");
    assert_eq!(
        routes_announced(&session),
        vec!["owner".to_owned()],
        "an `.explain` that is the shell's first command reports how it reads. transcript:\n{}",
        session.transcript()
    );

    let (code, transcript) = writer.finish();
    assert_eq!(code, Some(0), "the writer exits cleanly:\n{transcript}");
    explain_phase(&mut session, "after_exit");
    assert_eq!(
        routes_announced(&session),
        vec!["owner".to_owned(), "file".to_owned()],
        "an `.explain` that is the first command after the writer exits reports that the shell \
         now reads the committed file. transcript:\n{}",
        session.transcript()
    );
}
