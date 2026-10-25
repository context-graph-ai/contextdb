//! Journey: a reading shell says again how it reads whenever that changes.
//!
//! Contract held here: the `--read` shell reports its route and snapshot when
//! it first reads, and reports them again every time either changes during
//! the session: when its writer exits and it reads the committed file, when a
//! writer returns, and when it reads a newer file image after that writer
//! exits. A statement that reads the same way as the one before it adds no
//! notice. A file image read after a writer committed is never reported with
//! an earlier snapshot moment than the image read before that commit.
//!
//! Every writer is a separate process. Each phase ends with an unknown
//! command, refused on standard error after any notice for that phase and
//! without reading the store, so the transcript holds that phase's notices
//! before they are counted.

mod read_cli_support;

use read_cli_support::*;
use serde_json::Value;

fn route_notices(session: &Background) -> Vec<Value> {
    session
        .documents()
        .into_iter()
        .filter_map(|document| document.get("notice").cloned())
        .filter(|notice| {
            notice
                .get("detail")
                .and_then(|detail| detail.get("kind"))
                .and_then(Value::as_str)
                == Some("read_route")
        })
        .collect()
}

fn route_of(notice: &Value) -> (String, Option<String>) {
    let detail = notice.get("detail").expect("route notice detail");
    (
        detail
            .get("route")
            .and_then(Value::as_str)
            .expect("route word")
            .to_owned(),
        detail
            .get("snapshot_at")
            .and_then(Value::as_str)
            .map(str::to_owned),
    )
}

/// Read the table, then end the phase with a refusal on standard error that
/// does not read the store.
fn read_phase(session: &mut Background, phase: &str) {
    session.send_line(&format!(
        "SELECT '{phase}' AS phase, id FROM shared ORDER BY id;"
    ));
    session.send_line(&format!(".end_of_{phase}"));
    session.wait_for(&format!(".end_of_{phase}"));
}

/// The ids the phase's statement returned.
fn phase_ids(session: &Background, phase: &str) -> Vec<i64> {
    let documents = session.documents();
    let result = documents
        .iter()
        .filter_map(|document| document.get("result"))
        .find(|result| result.to_string().contains(&format!("\"{phase}\"")))
        .unwrap_or_else(|| {
            panic!(
                "the {phase} statement must be answered. transcript:\n{}",
                session.transcript()
            )
        });
    rows_of(result)
        .iter()
        .map(|row| {
            row.as_array()
                .and_then(|cells| cells.get(1))
                .or_else(|| row.get("id"))
                .and_then(Value::as_i64)
                .unwrap_or_else(|| panic!("a row with an id: {row}"))
        })
        .collect()
}

#[test]
fn a_reading_shell_reports_its_route_again_each_time_its_writer_leaves_or_returns() {
    let store = store_with(&create_seeded_table("shared", 2));
    let first_writer = live_owner(&store.path, "");
    let mut session = background(&[store.path_str(), "--json"]);

    read_phase(&mut session, "p1");
    let notices = route_notices(&session);
    assert_eq!(
        notices.len(),
        1,
        "the first read reports how the shell reads. transcript:\n{}",
        session.transcript()
    );
    assert_eq!(route_of(&notices[0]), ("owner".to_owned(), None));
    read_phase(&mut session, "p1again");
    assert_eq!(
        route_notices(&session).len(),
        1,
        "a statement that reads the same way adds no notice. transcript:\n{}",
        session.transcript()
    );

    let (code, transcript) = first_writer.finish();
    assert_eq!(
        code,
        Some(0),
        "the first writer exits cleanly:\n{transcript}"
    );
    read_phase(&mut session, "p2");
    let notices = route_notices(&session);
    assert_eq!(
        notices.len(),
        2,
        "when its writer exits, the shell says it now reads the committed file. transcript:\n{}",
        session.transcript()
    );
    let (route, first_image) = route_of(&notices[1]);
    assert_eq!(route, "file");
    let first_image = first_image.expect("a file image reports its snapshot moment");
    assert_eq!(phase_ids(&session, "p2"), vec![1, 2]);

    let second_writer = live_owner(
        &store.path,
        "INSERT INTO shared (id, label) VALUES (3, 'row-000003');\n",
    );
    read_phase(&mut session, "p3");
    let notices = route_notices(&session);
    assert_eq!(
        notices.len(),
        3,
        "when a writer returns, the shell says how it reads now. transcript:\n{}",
        session.transcript()
    );
    match route_of(&notices[2]) {
        (route, None) if route == "owner" => {}
        (route, Some(image)) if route == "file" => assert!(
            image >= first_image,
            "a newer file image is never reported earlier than the one before it: {image} < \
             {first_image}"
        ),
        other => panic!("an unexpected route notice: {other:?}"),
    }
    assert_eq!(phase_ids(&session, "p3"), vec![1, 2, 3]);

    let (code, transcript) = second_writer.finish();
    assert_eq!(
        code,
        Some(0),
        "the second writer exits cleanly:\n{transcript}"
    );
    read_phase(&mut session, "p4");
    let notices = route_notices(&session);
    assert_eq!(
        notices.len(),
        4,
        "when that writer exits, the shell says it reads the newer committed file. \
         transcript:\n{}",
        session.transcript()
    );
    let (route, last_image) = route_of(&notices[3]);
    assert_eq!(route, "file");
    let last_image = last_image.expect("a file image reports its snapshot moment");
    assert!(
        last_image >= first_image,
        "the file image read after a commit is never reported earlier than the one before it: \
         {last_image} < {first_image}"
    );
    assert_eq!(phase_ids(&session, "p4"), vec![1, 2, 3]);
}
