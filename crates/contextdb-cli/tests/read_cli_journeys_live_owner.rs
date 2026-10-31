//! Journey: reading a store that a live process already owns.
//!
//! Contract held here: the same `contextdb <path>` command works whether the
//! store is idle or owned — when a writer holds it, the reading session goes
//! over that owner's authenticated local channel and says so once; a second
//! writer is refused with `held_by_writer` and told to drop `--write`; the
//! owner's ceilings are the ones that apply and a caller cannot raise them; the
//! refusal on that route names the writer-side change and the cursor; and a
//! reader outlives its writer: when the owner disappears the reader answers
//! from what was last saved, says which route it took, and goes back to the
//! writer on its own when one returns (owner ruling 2026-10-26, reader
//! lifetime).
//!
//! Every owner here is a genuinely separate process. An in-process stand-in
//! cannot prove any of this.

mod read_cli_support;

use read_cli_support::*;

#[test]
fn a_reading_session_routes_through_the_live_owner_and_says_so_once() {
    let store = store_with(&create_seeded_table("shared", 4));
    let _owner = live_owner(&store.path, "");

    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n.tables\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "the same command reads a live-owned store.\n{}",
        outcome.describe()
    );
    let notices = outcome.route_notices();
    assert_eq!(
        notices.len(),
        1,
        "routing happens once and is reported once.\n{}",
        outcome.describe()
    );
    let detail = notices[0].get("detail").expect("route notice detail");
    assert_eq!(
        detail.get("route").and_then(|r| r.as_str()),
        Some("owner"),
        "a live owner serves the session: {}",
        notices[0]
    );
    assert!(
        detail
            .get("snapshot_at")
            .map(|s| s.is_null())
            .unwrap_or(false),
        "an owner serves live committed state, so there is no snapshot moment to report: {}",
        notices[0]
    );
}

#[test]
fn a_second_writer_is_refused_and_told_how_to_read_instead() {
    let store = store_with(&create_seeded_table("shared", 2));
    let _owner = live_owner(&store.path, "");

    let outcome = run(&[store.path_str(), "--write", "--json"], "SELECT 1;\n");

    assert_eq!(
        outcome.code,
        Some(1),
        "a second writer is refused.\n{}",
        outcome.describe()
    );
    let error = outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("held_by_writer"))
        .unwrap_or_else(|| {
            panic!(
                "a store held by a live writer refuses a second writer with held_by_writer.\n{}",
                outcome.describe()
            )
        });
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "held_by_writer is an io-class refusal, not a SQL error: {error}"
    );
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        message.contains("--write"),
        "the refusal names the executable next action — drop --write and read through the \
         owner's channel: {error}"
    );
}

#[test]
fn owner_status_reports_the_serving_owner_as_control_data() {
    let store = store_with(&create_seeded_table("shared", 2));
    let _owner = live_owner(&store.path, "");

    let outcome = run(&[store.path_str(), "--json"], ".owner status\n");
    assert_eq!(outcome.code, Some(0), "{}", outcome.describe());

    let docs = outcome.stdout_docs();
    let owner = expect_document(&docs, "owner", "`.owner status` against a live owner");
    assert_eq!(
        owner.get("state").and_then(|s| s.as_str()),
        Some("serving"),
        "a file-backed writer serves bounded same-machine reads by default: {owner}"
    );

    let limits = owner
        .get("limits")
        .unwrap_or_else(|| panic!("the status reports the owner's declared policy: {owner}"));
    for field in [
        "result_rows",
        "result_bytes",
        "work",
        "active_ms",
        "memory",
        "cursor_page_rows",
        "cursor_page_bytes",
        "cursor_idle_ms",
        "cursor_lifetime_ms",
        "concurrency",
    ] {
        let declared = limits
            .get(field)
            .unwrap_or_else(|| panic!("the owner reports {field}: {owner}"));
        assert!(
            declared.get("value").and_then(|v| v.as_i64()).is_some(),
            "{field} reports its effective value: {owner}"
        );
        let source = declared.get("source").and_then(|s| s.as_str());
        assert!(
            source == Some("default") || source == Some("override"),
            "{field} says whether it is a shipped default or an operator override: {owner}"
        );
    }
    assert_eq!(
        limits
            .get("concurrency")
            .and_then(|c| c.get("value"))
            .and_then(|v| v.as_i64()),
        Some(4),
        "the shipped reader-slot policy is four: {owner}"
    );
    assert!(
        owner
            .get("active_readers")
            .and_then(|a| a.as_i64())
            .is_some(),
        "the status reports how many readers are active: {owner}"
    );
    assert!(
        owner
            .get("database_memory")
            .and_then(|m| m.get("used_bytes"))
            .is_some(),
        "and the database-wide memory picture: {owner}"
    );
    assert!(
        outcome.route_notices().is_empty(),
        "`.owner status` still resolves no row-reading route.\n{}",
        outcome.describe()
    );
}

#[test]
#[cfg(unix)]
fn owner_status_uses_the_live_store_behind_a_symlink_alias() {
    let store = store_with(&create_seeded_table("shared", 2));
    let _owner = live_owner(&store.path, "");
    let alias = store.folder().join("live-owner-alias.db");
    std::os::unix::fs::symlink(&store.path, &alias).expect("name the live store through an alias");

    let outcome = run(
        &[alias.to_str().expect("utf-8 alias path"), "--json"],
        ".owner status\n",
    );
    assert_eq!(outcome.code, Some(0), "{}", outcome.describe());
    let owner = expect_document(
        &outcome.stdout_docs(),
        "owner",
        "`.owner status` through a live-store alias",
    );
    assert_eq!(
        owner.get("state").and_then(|state| state.as_str()),
        Some("serving"),
        "an alias of the live store identifies the same owner: {owner}"
    );
    assert!(
        outcome.route_notices().is_empty(),
        "owner status remains control data rather than selecting a row route"
    );
}

#[test]
fn the_owners_ceiling_applies_and_a_caller_cannot_raise_it() {
    let store = store_with(&create_seeded_table("shared", 6));
    let _owner = live_owner_with(
        &store.path,
        &[
            "--owner-read-result-rows",
            "2",
            "--owner-read-cursor-page-rows",
            "2",
        ],
        "",
    );

    let raised = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "500",
            "--read-cursor-page-rows",
            "100",
        ],
        "SELECT id FROM shared ORDER BY id;\n",
    );

    assert_eq!(
        raised.code,
        Some(1),
        "on the owner route the effective ceiling is the stricter of the two, field by field — \
         a caller can lower owner policy, never raise it.\n{}",
        raised.describe()
    );
    let error = raised
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| {
            panic!(
                "expected the owner's ceiling to refuse.\n{}",
                raised.describe()
            )
        });
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("value"))
            .and_then(|v| v.as_i64()),
        Some(2),
        "the effective ceiling reported is the owner's, not the caller's request: {error}"
    );

    let lowered = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "1",
            "--read-cursor-page-rows",
            "1",
        ],
        "SELECT id FROM shared ORDER BY id LIMIT 1;\n",
    );
    assert_eq!(
        lowered.code,
        Some(0),
        "lowering below owner policy is always allowed.\n{}",
        lowered.describe()
    );
}

#[test]
fn the_owner_route_refusal_names_the_writer_side_change_and_the_cursor() {
    let store = store_with(&create_seeded_table("shared", 6));
    let _owner = live_owner_with(
        &store.path,
        &[
            "--owner-read-result-rows",
            "2",
            "--owner-read-cursor-page-rows",
            "2",
        ],
        "",
    );

    let outcome = run(&[store.path_str()], "SELECT id FROM shared ORDER BY id;\n");

    assert!(
        outcome.stderr.contains("--owner-read-result-rows")
            || outcome.stderr.contains("--owner-read-result-bytes"),
        "on the owner route the ceiling is owner-imposed, so the refusal names the writer-side \
         change rather than a flag the reader cannot use.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stderr.contains("--read-result-rows"),
        "offering the reader a flag that cannot raise owner policy would be a lie.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stderr.contains(".cursor open"),
        "and it offers the cursor as the in-session escape.\n{}",
        outcome.describe()
    );
}

#[test]
fn an_owner_reader_sees_committed_state_only() {
    let store = store_with(&create_seeded_table("shared", 3));
    let mut owner = live_owner(&store.path, "");

    owner.send_line("BEGIN;");
    owner.send_line("INSERT INTO shared (id, label) VALUES (900, 'uncommitted');");
    owner.send_line("SELECT id FROM shared WHERE id = 900;");
    owner.wait_for("\"result\"");

    let reader = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared WHERE id = 900;\n",
    );
    let docs = reader.stdout_docs();
    let result = expect_document(&docs, "result", "the reader's view of the owner's store");
    assert!(
        rows_of(&result).is_empty(),
        "a holder's open write transaction is invisible to an inspecting reader.\n{}",
        reader.describe()
    );

    owner.send_line("ROLLBACK;");
}

/// The route each `read_route` notice in a transcript announced, in order.
fn routes_announced(transcript: &str) -> Vec<String> {
    lenient_json_lines(transcript)
        .into_iter()
        .filter_map(|d| d.get("notice").cloned())
        .filter(|n| detail_kind(n).as_deref() == Some("read_route"))
        .filter_map(|n| {
            n.get("detail")
                .and_then(|d| d.get("route"))
                .and_then(|r| r.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// Reader lifetime: the session outlives its writer. The owner dies between two
/// commands; the next command is answered completely from the last saved file
/// and the route notice says so, with the point in the data it reads from; when
/// a writer returns, the same session switches to it on its own.
#[test]
fn a_session_outlives_its_owner_reads_the_saved_file_and_returns_to_a_new_owner() {
    let store = store_with(&create_seeded_table("shared", 3));
    let mut owner = live_owner(&store.path, "");

    let mut session = background(&[store.path_str(), "--json"]);
    session.send_line("SELECT id, 'before-death' AS phase FROM shared ORDER BY id;");
    session.wait_for("before-death");
    assert_eq!(
        routes_announced(&session.transcript()),
        vec!["owner".to_owned()],
        "the session routed once, through the owner.\ntranscript:\n{}",
        session.transcript()
    );

    owner.kill();

    session.send_line("SELECT id, 'saved-file' AS phase FROM shared ORDER BY id;");
    session.wait_for("saved-file");
    let after_death = session.transcript();
    assert_eq!(
        routes_announced(&after_death),
        vec!["owner".to_owned(), "file".to_owned()],
        "with the writer down the reader answers from what was last saved, and says so.\
         \ntranscript:\n{after_death}"
    );
    let file_notice = lenient_json_lines(&after_death)
        .into_iter()
        .filter_map(|d| d.get("notice").cloned())
        .filter(|n| detail_kind(n).as_deref() == Some("read_route"))
        .nth(1)
        .expect("the file-route notice");
    assert!(
        file_notice
            .get("detail")
            .and_then(|d| d.get("snapshot_lsn"))
            .is_some_and(|lsn| lsn.is_u64()),
        "the notice names the point in the data the reader is now reading from: {file_notice}"
    );
    let docs = lenient_json_lines(&after_death);
    let results = documents_named(&docs, "result");
    assert_eq!(results.len(), 2, "transcript:\n{after_death}");
    assert_eq!(
        rows_of(&results[1])
            .iter()
            .filter_map(|row| row.get("id").and_then(serde_json::Value::as_i64))
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the saved file answers completely.\ntranscript:\n{after_death}"
    );
    assert!(
        docs.iter().all(|d| d.get("error").is_none()),
        "a writer that went away between commands is not an error for the reader.\
         \ntranscript:\n{after_death}"
    );

    // A writer returns over the same store; nobody reopens the reader.
    let returned = live_owner(&store.path, "");
    session.send_line("SELECT id, 'returned-owner' AS phase FROM shared ORDER BY id;");
    session.wait_for("returned-owner");
    let after_return = session.transcript();
    assert_eq!(
        routes_announced(&after_return),
        vec!["owner".to_owned(), "file".to_owned(), "owner".to_owned()],
        "when the writer is back the reader switches to it on its own.\
         \ntranscript:\n{after_return}"
    );
    let docs = lenient_json_lines(&after_return);
    let results = documents_named(&docs, "result");
    assert_eq!(results.len(), 3, "transcript:\n{after_return}");
    assert_eq!(
        rows_of(&results[2]).len(),
        3,
        "and the returned owner answers completely.\ntranscript:\n{after_return}"
    );

    let (code, transcript) = session.finish();
    assert_eq!(
        code,
        Some(0),
        "a writer that came and went never fails the reader's run.\ntranscript:\n{transcript}"
    );
    drop(returned);
}

#[test]
fn a_fresh_invocation_after_the_owner_leaves_reads_the_idle_file() {
    let store = store_with(&create_seeded_table("shared", 3));
    let owner = live_owner(&store.path, "");
    let owned = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n",
    );
    assert_eq!(
        owned
            .route_notices()
            .first()
            .and_then(|n| n.get("detail"))
            .and_then(|d| d.get("route"))
            .and_then(|r| r.as_str()),
        Some("owner"),
        "first invocation reached the owner.\n{}",
        owned.describe()
    );

    // The owner LEAVES: end of input, a clean close. The store it leaves
    // behind is an idle file.
    let (owner_code, owner_transcript) = owner.finish();
    assert_eq!(
        owner_code,
        Some(0),
        "the owner left cleanly.\ntranscript:\n{owner_transcript}"
    );

    let idle = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n",
    );
    assert_eq!(
        idle.code,
        Some(0),
        "with the owner gone, a new invocation reads the file directly.\n{}",
        idle.describe()
    );
    assert_eq!(
        idle.route_notices()
            .first()
            .and_then(|n| n.get("detail"))
            .and_then(|d| d.get("route"))
            .and_then(|r| r.as_str()),
        Some("file"),
        "routing is per invocation, and it is honest about which route it took.\n{}",
        idle.describe()
    );
}

#[test]
fn a_fresh_invocation_after_the_owner_dies_reads_the_saved_file_and_routes_to_a_replacement() {
    let store = store_with(&create_seeded_table("shared", 3));
    let mut owner = live_owner(&store.path, "");
    owner.kill();

    // The store was never closed: its committed image is intact, but the file
    // says a writer was inside it. A reader never repairs and never guesses;
    // it answers from what was last saved and says that it did.
    let saved = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n",
    );
    assert_eq!(
        saved.code,
        Some(0),
        "after an abrupt owner death, a bare-path read is served from the saved file.\n{}",
        saved.describe()
    );
    let docs = saved.stdout_docs();
    let result = expect_document(&docs, "result", "the saved-state read after owner death");
    assert_eq!(
        rows_of(&result)
            .iter()
            .filter_map(|row| row.get("id").and_then(serde_json::Value::as_i64))
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the saved file answers completely.\n{}",
        saved.describe()
    );
    assert_eq!(
        saved
            .route_notices()
            .first()
            .and_then(|n| n.get("detail"))
            .and_then(|d| d.get("route"))
            .and_then(|r| r.as_str()),
        Some("file"),
        "and says it read the file.\n{}",
        saved.describe()
    );
    assert!(
        saved.errors().is_empty(),
        "an ordinary writer disappearance is not a refusal.\n{}",
        saved.describe()
    );

    // One writable open closes the file cleanly; the idle file reads again.
    let reopened = run(&[store.path_str(), "--write", "--json"], "");
    assert_eq!(
        reopened.code,
        Some(0),
        "a writer opens and closes the store.\n{}",
        reopened.describe()
    );
    let idle = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n",
    );
    assert_eq!(
        idle.code,
        Some(0),
        "the file route reads the store once a writer has closed it.\n{}",
        idle.describe()
    );
    assert_eq!(
        idle.route_notices()
            .first()
            .and_then(|n| n.get("detail"))
            .and_then(|d| d.get("route"))
            .and_then(|r| r.as_str()),
        Some("file"),
        "and says which route it took.\n{}",
        idle.describe()
    );

    // A replacement writer is routed to, per invocation.
    let _replacement = live_owner(&store.path, "");
    let routed = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM shared ORDER BY id;\n",
    );
    assert_eq!(routed.code, Some(0), "{}", routed.describe());
    assert_eq!(
        routed
            .route_notices()
            .first()
            .and_then(|n| n.get("detail"))
            .and_then(|d| d.get("route"))
            .and_then(|r| r.as_str()),
        Some("owner"),
        "with a writer back, a new invocation reaches it.\n{}",
        routed.describe()
    );
}

/// The nonterminal over-count refusal on the OWNER route. A mis-sized fetch is
/// a request-shape refusal wherever the read is served from: the owner keeps
/// the cursor, the refusal names the count that works, and the read pages on
/// from exactly where it stopped.
#[test]
fn an_over_limit_fetch_through_the_owner_keeps_the_cursor_and_names_the_count() {
    let store = store_with(&create_seeded_table("shared", 6));
    let _owner = live_owner_with(
        &store.path,
        &[
            "--owner-read-result-rows",
            "2",
            "--owner-read-cursor-page-rows",
            "2",
        ],
        "",
    );

    let outcome = run(
        &[store.path_str(), "--json"],
        ".cursor open SELECT id FROM shared ORDER BY id\n\
         .cursor fetch 3\n\
         .cursor fetch 2\n\
         .cursor close\n\
         .cursor open SELECT id FROM shared ORDER BY id LIMIT 1\n",
    );

    let error = outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| panic!("the owner's row ceiling refuses.\n{}", outcome.describe()));
    let detail = error.get("detail").expect("refusal detail");
    assert_eq!(
        detail.get("value").and_then(|v| v.as_i64()),
        Some(2),
        "the ceiling reported is the owner's: {error}"
    );
    assert_eq!(
        detail.get("remedy_command").and_then(|c| c.as_str()),
        Some(".cursor fetch 2"),
        "and the refusal names the exact count the owner will serve: {error}"
    );

    let pages: Vec<serde_json::Value> = documents_named(&outcome.stdout_docs(), "cursor")
        .into_iter()
        .filter(|d| d.get("rows").is_some())
        .collect();
    assert_eq!(
        pages.len(),
        3,
        "the open page, the page after the refusal, and the reopened page all arrive.\n{}",
        outcome.describe()
    );
    let ids = |page: &serde_json::Value| -> Vec<i64> {
        rows_of(page)
            .iter()
            .filter_map(|row| row.get("id").and_then(serde_json::Value::as_i64))
            .collect()
    };
    assert_eq!(ids(&pages[0]), vec![1, 2]);
    assert_eq!(
        ids(&pages[1]),
        vec![3, 4],
        "the owner consumed nothing for the refused request — no row is skipped or repeated.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.errors().iter().all(|e| !matches!(
            detail_kind(e).as_deref(),
            Some("cursor_not_found") | Some("cursor_already_open")
        )),
        "the client's slot stays in step with the owner's registry.\n{}",
        outcome.describe()
    );
    assert!(
        documents_named(&outcome.stdout_docs(), "cursor")
            .iter()
            .any(|d| d.get("closed").and_then(serde_json::Value::as_bool) == Some(true)),
        "and close still releases the cursor afterwards.\n{}",
        outcome.describe()
    );
}
