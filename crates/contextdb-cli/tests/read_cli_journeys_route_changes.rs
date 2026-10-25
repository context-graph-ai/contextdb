//! Journey: a reading shell says again how it reads whenever that changes.
//!
//! Contract held here: the `--read` shell reports its route and snapshot when
//! it first reads, and reports them again every time either changes during
//! the session: when its writer exits and it reads the committed file, when a
//! writer returns, and when it reads a newer file image after that writer
//! exits. A statement that reads the same way as the one before it adds no
//! notice. A file notice names its snapshot by the committed LSN, its
//! identity, and by its moment, for people: a file image read after a writer
//! committed has a strictly greater LSN than the image read before that
//! commit, and a moment that is never earlier.
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

/// What a route notice says: the route, the snapshot's committed LSN (its
/// identity) and the snapshot moment (for people).
#[derive(Debug, Clone, PartialEq)]
struct RouteNotice {
    route: String,
    snapshot_lsn: Option<u64>,
    snapshot_at: Option<String>,
}

fn route_of(notice: &Value) -> RouteNotice {
    let detail = notice.get("detail").expect("route notice detail");
    RouteNotice {
        route: detail
            .get("route")
            .and_then(Value::as_str)
            .expect("route word")
            .to_owned(),
        snapshot_lsn: detail.get("snapshot_lsn").and_then(Value::as_u64),
        snapshot_at: detail
            .get("snapshot_at")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

/// A file notice names its snapshot by committed LSN and moment.
fn file_image(notice: &RouteNotice, when: &str) -> (u64, String) {
    assert_eq!(
        notice.route, "file",
        "{when}, the shell reads the committed file"
    );
    (
        notice.snapshot_lsn.unwrap_or_else(|| {
            panic!("{when}, a file image reports its committed LSN: {notice:?}")
        }),
        notice
            .snapshot_at
            .clone()
            .unwrap_or_else(|| panic!("{when}, a file image reports its moment: {notice:?}")),
    )
}

/// The owner route reads live state: no snapshot identity or moment.
fn assert_owner(notice: &RouteNotice, when: &str) {
    assert_eq!(
        (
            notice.route.as_str(),
            notice.snapshot_lsn,
            notice.snapshot_at.as_deref()
        ),
        ("owner", None, None),
        "{when}, the shell reads through the live owner, with no snapshot"
    );
}

/// A later file image has a strictly greater committed LSN and a moment that
/// is never earlier.
fn assert_newer_image(earlier: &(u64, String), later: &(u64, String), when: &str) {
    assert!(
        later.0 > earlier.0,
        "{when}, the newer file image's committed LSN strictly increases: {} after {}",
        later.0,
        earlier.0
    );
    assert!(
        later.1 >= earlier.1,
        "{when}, the newer file image is never reported earlier: {} after {}",
        later.1,
        earlier.1
    );
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
    assert_owner(&route_of(&notices[0]), "beside the first writer");
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
    let first_image = file_image(&route_of(&notices[1]), "after the first writer exits");
    assert_eq!(phase_ids(&session, "p2"), vec![1, 2]);
    read_phase(&mut session, "p2again");
    assert_eq!(
        route_notices(&session).len(),
        2,
        "a second statement on the same file image adds no notice. transcript:\n{}",
        session.transcript()
    );

    // A second writer opens and commits row 3. `live_owner` sends its setup
    // SQL before asking for the owner's status and returns only on that reply,
    // so row 3 is committed before p3 reads.
    let mut second_writer = live_owner(
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
    let third = route_of(&notices[2]);
    let latest_image = if third.route == "owner" {
        assert_owner(&third, "with the second writer serving");
        first_image.clone()
    } else {
        let image = file_image(&third, "with the second writer serving");
        assert_newer_image(
            &first_image,
            &image,
            "after the second writer committed row 3",
        );
        image
    };
    assert_eq!(phase_ids(&session, "p3"), vec![1, 2, 3]);

    // The second writer commits row 4, then exits: the insert is sent before
    // its input closes, so it is committed before the writer is gone and p4
    // reads it.
    second_writer.send_line("INSERT INTO shared (id, label) VALUES (4, 'row-000004');");
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
    let last_image = file_image(&route_of(&notices[3]), "after the second writer exits");
    assert_newer_image(
        &latest_image,
        &last_image,
        "after the second writer committed row 4",
    );
    assert_eq!(phase_ids(&session, "p4"), vec![1, 2, 3, 4]);
    read_phase(&mut session, "p4again");
    assert_eq!(
        route_notices(&session).len(),
        4,
        "a second statement on the same newer file image adds no notice. transcript:\n{}",
        session.transcript()
    );
}
