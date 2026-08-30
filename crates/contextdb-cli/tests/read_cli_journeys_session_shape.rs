//! Journey: the session shows its mode, decorates nothing else, and behaves the
//! same whether a person or a pipe is driving it.
//!
//! Contract held here: an interactive session prints one start banner naming its
//! mode and prompts `contextdb(ro)> ` or `contextdb> `; neither decoration is
//! emitted when the session is not interactive; every successful ordinary SELECT
//! in human mode ends with exactly one `(N rows)` footer and nothing else; the
//! first store-reading command emits exactly one route notice, on stderr; and a
//! piped invocation is a full session, not a reduced one.

mod read_cli_support;

use read_cli_support::*;

const READ_BANNER: &str = "read-only session — pass --write to mutate";
const WRITE_BANNER: &str = "write session — mutations are enabled";
const READ_PROMPT: &str = "contextdb(ro)> ";
const WRITE_PROMPT: &str = "contextdb> ";

#[test]
fn a_piped_session_prints_no_prompt_and_no_banner() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(&[store.path_str()], "SELECT id FROM rows ORDER BY id;\n");
    let everything = format!("{}{}", outcome.stdout, outcome.stderr);

    for decoration in [READ_BANNER, WRITE_BANNER, READ_PROMPT, WRITE_PROMPT] {
        assert!(
            !everything.contains(decoration),
            "a non-interactive session emits no {decoration:?}, so piped output stays clean.\n{}",
            outcome.describe()
        );
    }
}

#[test]
fn an_interactive_reading_session_announces_its_mode_and_prompts_read_only() {
    let store = store_with(&create_seeded_table("rows", 2));
    let mut session = terminal(&[store.path_str()]);

    session.wait_for(READ_BANNER);
    session.wait_for(READ_PROMPT);
    session.send_line("SELECT id FROM rows ORDER BY id;");
    session.wait_for("(2 rows)");

    let (code, transcript) = session.quit();
    assert_eq!(
        code,
        Some(0),
        "an interactive session exits 0 on quit.\ntranscript:\n{transcript}"
    );
    assert!(
        !transcript.contains(WRITE_BANNER),
        "a reading session must never announce write mode.\n{transcript}"
    );
}

#[test]
fn an_interactive_write_session_announces_write_mode_and_prompts_plainly() {
    let store = store_with(&create_seeded_table("rows", 1));
    let session = terminal(&[store.path_str(), "--write"]);

    session.wait_for(WRITE_BANNER);
    session.wait_for(WRITE_PROMPT);

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");
    assert!(
        !transcript.contains(READ_PROMPT),
        "a write session must not wear the read-only prompt.\n{transcript}"
    );
}

#[test]
fn an_interactive_memory_session_is_a_write_session_with_no_route_to_report() {
    let mut session = terminal(&[":memory:"]);

    session.wait_for(WRITE_BANNER);
    session.wait_for(WRITE_PROMPT);
    session.send_line("CREATE TABLE scratch (id INTEGER PRIMARY KEY);");
    session.send_line("SELECT id FROM scratch;");
    session.wait_for("(0 rows)");

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");
    assert!(
        !transcript.contains("read_route"),
        "`:memory:` has no route, so no route notice exists to print.\n{transcript}"
    );
}

#[test]
fn a_successful_human_select_ends_with_exactly_one_row_count_footer() {
    let store = store_with(&create_seeded_table("rows", 3));
    let outcome = run(
        &[store.path_str()],
        "SELECT id, label FROM rows ORDER BY id;\n",
    );

    assert_eq!(outcome.code, Some(0), "{}", outcome.describe());
    assert_eq!(
        outcome.stdout.matches("(3 rows)").count(),
        1,
        "human mode ends a successful ordinary SELECT with exactly one `(N rows)` footer.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stdout.contains("showing"),
        "no truncation footer exists: a result is complete or it is refused.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stdout.contains("--all"),
        "no output advertises a flag the contract removed.\n{}",
        outcome.describe()
    );
    let trailing = outcome
        .stdout
        .lines()
        .rfind(|l| !l.trim().is_empty())
        .unwrap_or_default()
        .trim()
        .to_owned();
    assert_eq!(
        trailing,
        "(3 rows)",
        "the footer is the last thing a successful SELECT prints.\n{}",
        outcome.describe()
    );
}

#[test]
fn each_successful_select_carries_its_own_single_footer() {
    let store = store_with(&create_seeded_table("rows", 4));
    let outcome = run(
        &[store.path_str()],
        "SELECT id FROM rows WHERE id <= 2 ORDER BY id;\nSELECT id FROM rows ORDER BY id;\n",
    );

    assert_eq!(
        outcome.stdout.matches("(2 rows)").count(),
        1,
        "{}",
        outcome.describe()
    );
    assert_eq!(
        outcome.stdout.matches("(4 rows)").count(),
        1,
        "{}",
        outcome.describe()
    );
}

#[test]
fn the_route_notice_is_emitted_once_on_stderr_at_the_first_store_reading_command() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM rows ORDER BY id;\n.tables\nSELECT label FROM rows ORDER BY id;\n",
    );

    let notices = outcome.route_notices();
    assert_eq!(
        notices.len(),
        1,
        "routing happens once per session and is reported once.\n{}",
        outcome.describe()
    );
    let notice = &notices[0];
    assert_eq!(
        notice.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "the route notice is io-class: {notice}"
    );
    let detail = notice.get("detail").expect("route notice detail");
    assert_eq!(
        detail.get("route").and_then(|r| r.as_str()),
        Some("file"),
        "an idle store is read directly, so the route is `file`: {notice}"
    );
    assert!(
        detail
            .get("snapshot_at")
            .map(|s| s.is_string())
            .unwrap_or(false),
        "the file route states the committed-snapshot moment it serves, so a long-open \
         terminal is never mistaken for a live view: {notice}"
    );
    assert!(
        !outcome.stdout.contains("read_route"),
        "notices go to stderr so JSON Lines on stdout stay pure results.\n{}",
        outcome.describe()
    );
}

#[test]
fn session_only_commands_never_resolve_a_route() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(
        &[store.path_str(), "--json"],
        ".help\n.trace on\n.trace off\n.sync status\n.owner status\n",
    );

    assert!(
        outcome.route_notices().is_empty(),
        "`.help`, `.trace`, `.sync status`, and `.owner status` read no rows, so no route \
         is resolved and no route notice is emitted.\n{}",
        outcome.describe()
    );
}

#[test]
fn read_mode_sync_status_reports_the_session_and_disclaims_the_store() {
    let store = store_with(&create_seeded_table("rows", 1));

    let human = run(&[store.path_str()], ".sync status\n");
    assert!(
        human.stdout.contains("no sync in this session")
            && human.stdout.contains("not the store")
            && human.stdout.contains("belongs to that owner process"),
        "read-mode `.sync status` reports the CLI session only and says so, so nobody reads it \
         as the store's sync health.\n{}",
        human.describe()
    );

    let machine = run(&[store.path_str(), "--json"], ".sync status\n");
    let docs = machine.stdout_docs();
    let document = expect_document(&docs, "sync_status", "`.sync status` under --json");
    let message = document
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        message.starts_with("no sync in this session"),
        "the machine document carries the same session-only wording: {document}"
    );
}

#[test]
fn removed_aliases_are_not_accepted_spellings() {
    let store = store_with(&create_seeded_table("rows", 1));
    let outcome = run(&[store.path_str(), "--json"], "\\trace on\n\\sync status\n");

    assert!(
        !outcome.stdout.contains("\"trace\""),
        "`\\trace` does not exist.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stdout.contains("\"sync_status\"") && !outcome.stdout.contains("\"sync\""),
        "`\\sync` does not exist.\n{}",
        outcome.describe()
    );
    assert_ne!(
        outcome.code,
        Some(0),
        "a spelling the contract removed is never a silent success.\n{}",
        outcome.describe()
    );
}

#[test]
fn conventional_aliases_keep_the_classification_of_the_commands_they_spell() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(&[store.path_str(), "--json"], "\\dt\n\\d rows\n");

    assert_eq!(
        outcome.code,
        Some(0),
        "`\\dt` and `\\d` are read commands, exactly like `.tables` and `.schema`.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    expect_document(&docs, "tables", "`\\dt` under --json");
    expect_document(&docs, "schema", "`\\d rows` under --json");
}

#[test]
fn a_piped_session_is_a_full_session_including_the_cursor() {
    let store = store_with(&create_seeded_table("paged", 5));
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        ".cursor open SELECT id FROM paged ORDER BY id\n.cursor fetch\n.cursor fetch\n.cursor close\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "a piped invocation is a full session: dot-commands and the single session cursor \
         work exactly as they do at a terminal.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let pages = documents_named(&docs, "cursor");
    let has_more: Vec<bool> = pages
        .iter()
        .filter(|p| p.get("rows").is_some())
        .map(|p| p.get("has_more").and_then(|h| h.as_bool()).unwrap_or(true))
        .collect();
    assert_eq!(
        has_more,
        vec![true, true, false],
        "paging a five-row table two rows at a time ends with a truthful `has_more: false`.\n{}",
        outcome.describe()
    );
    assert_eq!(
        outcome.route_notices().len(),
        1,
        "a piped session emits the one route notice, like any other session.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_terminal_session_pages_the_cursor_exactly_as_the_pipe_did() {
    let store = store_with(&create_seeded_table("paged", 5));
    let mut session = terminal(&[
        store.path_str(),
        "--json",
        "--read-result-rows",
        "2",
        "--read-cursor-page-rows",
        "2",
    ]);

    session.send_line(".cursor open SELECT id FROM paged ORDER BY id");
    session.wait_for("\"has_more\"");
    session.send_line(".cursor fetch");
    session.wait_for_count("\"has_more\"", 2);
    session.send_line(".cursor fetch");
    session.wait_for_count("\"has_more\"", 3);

    let (code, transcript) = session.quit();
    assert_eq!(code, Some(0), "transcript:\n{transcript}");

    let pages: Vec<serde_json::Value> = lenient_json_lines(&transcript)
        .into_iter()
        .filter_map(|d| d.get("cursor").cloned())
        .filter(|p| p.get("rows").is_some())
        .collect();
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
        "the terminal session produces the same pages as the piped one — one session contract, \
         two input devices.\ntranscript:\n{transcript}"
    );
}
