//! Journey: the machine surface is the primary contract.
//!
//! Contract held here: under `--json`, stdout is JSON Lines — one complete
//! document per statement or meta-command and nothing else — while every error,
//! notice, trace, and help line is a JSON document on stderr. Errors carry a
//! stable `class` and `detail.kind` that scripts branch on, and `.owner status`
//! is control data: it resolves no route, works whatever the store's state, and
//! reports its own exit.

mod read_cli_support;

use read_cli_support::*;

#[test]
fn a_successful_select_is_one_namespaced_column_carrying_document() {
    let store = store_with(&create_seeded_table("rows", 3));
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM rows ORDER BY id;\n",
    );

    let docs = outcome.stdout_docs();
    assert_eq!(
        docs.len(),
        1,
        "one statement produces exactly one stdout document.\n{}",
        outcome.describe()
    );
    let result = expect_document(&docs, "result", "a successful SELECT");
    let columns: Vec<String> = result
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
        "the result names its columns: {result}"
    );
    let rows = rows_of(&result);
    assert_eq!(
        rows.len(),
        3,
        "and carries every row as an object: {result}"
    );
    assert!(
        rows[0].get("label").and_then(|l| l.as_str()).is_some(),
        "rows are objects keyed by column name, not positional arrays: {result}"
    );
    assert!(
        docs.iter().all(|d| d.is_object()),
        "the bare-array rendering is removed with no deprecation layer: every document is a \
         namespaced object.\n{}",
        outcome.describe()
    );
}

#[test]
fn each_statement_gets_its_own_document_on_one_line() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM rows ORDER BY id;\nSELECT label FROM rows ORDER BY id;\n.tables\n",
    );

    let docs = outcome.stdout_docs();
    assert_eq!(
        documents_named(&docs, "result").len(),
        2,
        "one result document per SELECT.\n{}",
        outcome.describe()
    );
    assert_eq!(
        documents_named(&docs, "tables").len(),
        1,
        "and one document for the meta-command.\n{}",
        outcome.describe()
    );
}

#[test]
fn stdout_carries_results_only_while_notices_traces_and_help_go_to_stderr() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(
        &[store.path_str(), "--json"],
        ".trace on\nSELECT id FROM rows ORDER BY id;\n.help\n",
    );

    // Parsing every stdout line IS the JSON Lines purity assertion.
    let docs = outcome.stdout_docs();
    for doc in &docs {
        assert!(
            doc.get("error").is_none() && doc.get("notice").is_none() && doc.get("help").is_none(),
            "an error, notice, or help document must never land on stdout: {doc}"
        );
        assert!(
            doc.get("trace")
                .map(|t| t.get("rows_examined").is_none())
                .unwrap_or(true),
            "an execution trace is diagnostic output and belongs on stderr: {doc}"
        );
    }
    assert!(
        outcome.stderr.contains("help") || outcome.stderr.contains("\"trace\""),
        "guidance and diagnostics are on stderr, as documents.\n{}",
        outcome.describe()
    );
}

#[test]
fn every_refusal_carries_its_ratified_class_and_kind() {
    let store = store_with(&create_seeded_table("rows", 3));
    let absent = absent_store();

    // (invocation, script, expected detail.kind, expected class)
    let cases: Vec<(Vec<&str>, String, &str, &str)> = vec![
        (
            vec![absent.path.to_str().expect("utf-8"), "--json"],
            "SELECT 1;\n".to_owned(),
            "store_not_found",
            "io",
        ),
        (
            vec![store.path_str(), "--json"],
            "INSERT INTO rows (id, label) VALUES (77, 'nope');\n".to_owned(),
            "write_requires_flag",
            "sql",
        ),
        (
            vec![
                store.path_str(),
                "--json",
                "--read-result-rows",
                "1",
                "--read-cursor-page-rows",
                "1",
            ],
            "SELECT id FROM rows ORDER BY id;\n".to_owned(),
            "owner_limit_exceeded",
            "io",
        ),
        (
            vec![store.path_str(), "--json"],
            ".cursor open definitely not a select\n".to_owned(),
            "cursor_invalid_statement",
            "usage",
        ),
        (
            vec![store.path_str(), "--json"],
            ".cursor fetch\n".to_owned(),
            "cursor_not_found",
            "io",
        ),
        (
            vec![store.path_str(), "--json"],
            ".tables --continue not-a-real-continuation\n".to_owned(),
            "invalid_continuation",
            "usage",
        ),
    ];

    for (args, script, kind, class) in cases {
        let outcome = run(&args, &script);
        let error = outcome
            .errors()
            .into_iter()
            .find(|e| detail_kind(e).as_deref() == Some(kind))
            .unwrap_or_else(|| {
                panic!(
                    "expected detail.kind {kind}, which is what a script branches on.\n{}",
                    outcome.describe()
                )
            });
        assert_eq!(
            error.get("class").and_then(|c| c.as_str()),
            Some(class),
            "{kind} is {class}-class in the ratified table; a read refusal rendered as a SQL \
             error would send every script down the wrong branch: {error}"
        );
        assert!(
            error.get("message").and_then(|m| m.as_str()).is_some(),
            "{kind} carries human prose alongside the stable fields: {error}"
        );
    }
}

#[test]
fn a_parse_failure_is_an_ordinary_sql_error_in_a_reading_session() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(&[store.path_str(), "--json"], "SELCT id FROM rows;\n");

    let error =
        outcome.errors().into_iter().next().unwrap_or_else(|| {
            panic!("unparseable input must be refused.\n{}", outcome.describe())
        });
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("sql"),
        "input that parses as nothing is a sql-class parse error, identical in read and write \
         sessions — not a write refusal: {error}"
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "nothing executes.\n{}",
        outcome.describe()
    );
}

#[test]
fn owner_status_is_control_data_that_resolves_no_route() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(&[store.path_str(), "--json"], ".owner status\n");

    assert_eq!(
        outcome.code,
        Some(0),
        "no owner is a successful report, not a failure.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let owner = expect_document(&docs, "owner", "`.owner status` under --json");
    assert_eq!(
        owner.get("state").and_then(|s| s.as_str()),
        Some("not_running"),
        "an idle store has no process owner: {owner}"
    );
    assert!(
        outcome.route_notices().is_empty(),
        "`.owner status` describes the owner, it does not establish a row-reading route.\n{}",
        outcome.describe()
    );
}

#[test]
fn owner_status_describes_the_process_owner_and_not_sync_health() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(&[store.path_str(), "--json"], ".owner status\n");
    let docs = outcome.stdout_docs();
    let owner = expect_document(&docs, "owner", "`.owner status`");

    assert!(
        owner.get("endpoint").is_none() && owner.get("tenant_id").is_none(),
        "`.owner status` is about the file-backed process owner, never sync: {owner}"
    );
}

#[test]
fn the_argument_parser_owns_everything_before_the_json_promise_starts() {
    let store = store_with(&create_seeded_table("rows", 2));
    let outcome = run(
        &[store.path_str(), "--json", "--no-such-flag"],
        "SELECT 1;\n",
    );

    assert_eq!(
        outcome.code,
        Some(2),
        "a malformed command line is rejected before any work, with its own rendering.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "and publishes no machine document at all.\n{}",
        outcome.describe()
    );
}

/// An owner that was asked to serve and could not is a FAILED report. A script
/// that branches on the state word must be able to branch on the exit code
/// too, so the state and the exit have to agree.
#[test]
fn owner_status_reports_its_own_exit_when_the_owner_cannot_serve() {
    let store = absent_store();
    // A runtime root the channel cannot live in: it exists, so nothing about
    // the command line is wrong, but it is not owner-only, so serving cannot
    // start. Owner-channel startup failure never fails the store open — it
    // becomes the state `.owner status` reports.
    let root = store.folder().join("world-readable-runtime");
    std::fs::create_dir(&root).expect("create the runtime root the writer is pointed at");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("leave the runtime root readable by everyone");
    }

    let outcome = run(
        &[
            store.path_str(),
            "--write",
            "--owner-read-runtime-dir",
            root.to_str().expect("utf-8 runtime root"),
            "--json",
        ],
        "CREATE TABLE rows (id INTEGER PRIMARY KEY);\n.owner status\n",
    );

    let docs = outcome.stdout_docs();
    let owner = expect_document(&docs, "owner", "`.owner status` under --json");
    assert_eq!(
        owner.get("state").and_then(|state| state.as_str()),
        Some("not_serving"),
        "an owner that was expected to serve and could not says so: {owner}"
    );
    let reason = owner
        .get("reason")
        .and_then(|reason| reason.as_str())
        .unwrap_or_else(|| panic!("a state that is not plain serving carries a reason: {owner}"));
    assert!(
        !reason.trim().is_empty(),
        "the reason is a word a script can branch on: {owner}"
    );
    assert_eq!(
        outcome.code,
        Some(1),
        "an owner expected to serve and unable to is a failed report, and the exit says so.\n{}",
        outcome.describe()
    );
}
