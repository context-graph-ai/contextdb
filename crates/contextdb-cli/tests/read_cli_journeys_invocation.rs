//! Journey: how the command is invoked decides whether anything can be written.
//!
//! The promise these tests hold the CLI to: `contextdb <path>` is a bounded
//! reading session that never creates or changes a byte, and `--write` is the
//! only way to authorize creation and mutation. Everything else in this file is
//! the surface that promise implies — the removed spellings, the operational
//! commands that must be visible, the two bootstrap environment names, the
//! `:memory:` exception, and the configuration that is refused before open.

mod read_cli_support;

use read_cli_support::*;

#[test]
fn bare_path_on_a_missing_store_refuses_and_creates_nothing() {
    let store = absent_store();
    let before = folder_contents(store.folder());

    let outcome = run(&[store.path_str(), "--json"], "SELECT 1;\n");

    assert_eq!(
        outcome.code,
        Some(1),
        "reading a store that does not exist is a runtime refusal, which exits 1.\n{}",
        outcome.describe()
    );
    assert!(
        !store.path.exists(),
        "a reading invocation must never create the store it was pointed at"
    );
    assert_eq!(
        before,
        folder_contents(store.folder()),
        "a refused reading invocation must leave the folder exactly as it found it"
    );
    assert!(
        outcome.error_kinds().iter().any(|k| k == "store_not_found"),
        "the refusal must carry detail.kind store_not_found so a script can branch on it.\n{}",
        outcome.describe()
    );
    let error = outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("store_not_found"))
        .expect("a store_not_found error document");
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "store_not_found is an io-class refusal: {error}"
    );
}

#[test]
fn bare_path_beneath_a_missing_parent_keeps_the_store_not_found_answer() {
    let store = absent_store();
    let path = store
        .folder()
        .join("parent-that-does-not-exist")
        .join("journey.db");
    let before = folder_contents(store.folder());

    let outcome = run(
        &[path.to_str().expect("utf-8 missing store path"), "--json"],
        "SELECT 1;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "a missing parent does not change the missing-store runtime refusal.\n{}",
        outcome.describe()
    );
    assert_eq!(
        before,
        folder_contents(store.folder()),
        "a refused reading invocation must not create the missing parent or any store bytes"
    );
    let error = outcome
        .errors()
        .into_iter()
        .find(|error| detail_kind(error).as_deref() == Some("store_not_found"))
        .expect("a store_not_found error document");
    assert_eq!(
        error.get("class").and_then(|class| class.as_str()),
        Some("io"),
        "a missing parent must keep the stable io-class store_not_found answer: {error}"
    );
}

#[test]
fn write_flag_creates_the_store_a_read_refused_to_create() {
    let store = absent_store();
    let outcome = run(
        &[store.path_str(), "--write"],
        "CREATE TABLE t (id INTEGER PRIMARY KEY);\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "`--write` is what authorizes creation.\n{}",
        outcome.describe()
    );
    assert!(
        store.path.exists(),
        "`--write` must create the store the bare path refused to create"
    );
}

#[test]
fn a_reading_session_refuses_every_mutating_statement_before_it_executes() {
    let store = store_with(&create_seeded_table("kept", 3));
    let before = store.contents();

    let mutations = [
        "INSERT INTO kept (id, label) VALUES (9001, 'smuggled');",
        "CREATE TABLE late (id INTEGER PRIMARY KEY);",
        "DELETE FROM kept WHERE id = 1;",
        "BEGIN;",
        "COMMIT;",
        "ROLLBACK;",
        "SET MEMORY_LIMIT '512M';",
    ];

    for statement in mutations {
        let outcome = run(&[store.path_str(), "--json"], &format!("{statement}\n"));
        assert_eq!(
            outcome.code,
            Some(1),
            "a refused statement makes a non-interactive session exit 1: {statement}\n{}",
            outcome.describe()
        );
        let kinds = outcome.error_kinds();
        assert!(
            kinds.iter().any(|k| k == "write_requires_flag"),
            "{statement} must be refused with write_requires_flag, telling the user to add --write.\n{}",
            outcome.describe()
        );
        let error = outcome
            .errors()
            .into_iter()
            .find(|e| detail_kind(e).as_deref() == Some("write_requires_flag"))
            .expect("a write_requires_flag error document");
        assert_eq!(
            error.get("class").and_then(|c| c.as_str()),
            Some("sql"),
            "write_requires_flag is sql-class: {error}"
        );
        assert!(
            outcome.stdout.trim().is_empty(),
            "a refused mutation publishes nothing on stdout: {statement}\n{}",
            outcome.describe()
        );
    }

    assert_eq!(
        before,
        store.contents(),
        "no refused mutation may change a single byte in the store folder"
    );
}

#[test]
fn writer_only_meta_commands_are_refused_in_a_reading_session() {
    let store = store_with(&create_seeded_table("kept", 2));
    let before = store.contents();

    for command in [
        ".maintenance run",
        ".maintenance compact",
        ".sync push",
        ".sync pull",
    ] {
        let outcome = run(&[store.path_str(), "--json"], &format!("{command}\n"));
        assert!(
            outcome
                .error_kinds()
                .iter()
                .any(|k| k == "write_requires_flag"),
            "{command} is a store write and must be refused with write_requires_flag.\n{}",
            outcome.describe()
        );
    }

    assert_eq!(
        before,
        store.contents(),
        "a refused writer-only meta-command must change nothing"
    );
}

#[test]
fn reading_an_idle_store_leaves_every_byte_and_the_folder_listing_unchanged() {
    let store = store_with(&create_seeded_table("evidence", 12));
    let before = store.contents();

    let script = "\
SELECT id, label FROM evidence ORDER BY id;
.tables
.schema evidence
.explain SELECT id FROM evidence;
.events status
.maintenance status
";
    let outcome = run(&[store.path_str(), "--json"], script);

    assert_eq!(
        outcome.code,
        Some(0),
        "a complete reading session over an idle store succeeds.\n{}",
        outcome.describe()
    );
    assert_eq!(
        before,
        store.contents(),
        "reading must leave every byte in the store folder unchanged"
    );
}

#[test]
fn the_removed_row_cap_flag_is_rejected_as_an_invalid_invocation() {
    let store = store_with(&create_seeded_table("rows", 3));
    let outcome = run(&[store.path_str(), "--all"], "SELECT id FROM rows;\n");
    assert_eq!(
        outcome.code,
        Some(2),
        "`--all` is removed with no deprecation layer, so it is an invalid invocation.\n{}",
        outcome.describe()
    );
}

#[test]
fn operational_commands_are_visible_in_ordinary_help_and_repair_is_gone() {
    let outcome = run(&["--help"], "");
    let help = format!("{}{}", outcome.stdout, outcome.stderr);

    for command in [
        "migrate", "reset", "diagnose", "snapshot", "inspect", "purge",
    ] {
        assert!(
            help.contains(command),
            "`{command}` is part of the one command tree and must appear in ordinary --help.\n{help}"
        );
    }
    assert!(
        !help.contains("repair"),
        "`repair` is replaced by `diagnose` and must not be offered.\n{help}"
    );

    let store = store_with(&create_seeded_table("rows", 2));
    let refused = run(&["repair", store.path_str()], "");
    assert_eq!(
        refused.code,
        Some(2),
        "`repair` is rejected by name.\n{}",
        refused.describe()
    );
}

#[test]
fn a_former_behavior_environment_alias_changes_nothing_and_says_nothing() {
    let store = store_with(&create_seeded_table("rows", 4));
    let query = "SELECT id, label FROM rows ORDER BY id;\n";

    let plain = run(&[store.path_str(), "--json"], query);
    let aliased = run_with_env(
        &[store.path_str(), "--json"],
        &[
            ("CONTEXTDB_MEMORY_LIMIT", "17M"),
            ("CONTEXTDB_DISK_LIMIT", "23M"),
            ("CONTEXTDB_SYNC_DEBOUNCE_MS", "31"),
            ("CONTEXTDB_TENANT_ID", "not-a-tenant"),
        ],
        query,
    );

    assert_eq!(
        plain.code, aliased.code,
        "a former behavior alias must not change the outcome"
    );
    assert_eq!(
        plain.stdout, aliased.stdout,
        "a former behavior alias must not change results"
    );
    assert_eq!(
        plain.stderr.lines().count(),
        aliased.stderr.lines().count(),
        "setting a former behavior alias must produce no additional output: it is simply not a surface.\nplain:\n{}\naliased:\n{}",
        plain.describe(),
        aliased.describe()
    );
}

#[test]
fn an_explicit_path_wins_over_the_bootstrap_database_path_variable() {
    let named = store_with(&create_seeded_table("named_store", 2));
    let other = store_with(&create_seeded_table("other_store", 2));

    let outcome = run_with_env(
        &[named.path_str(), "--json"],
        &[("CONTEXTDB_DB_PATH", other.path_str())],
        ".tables\n",
    );

    let docs = outcome.stdout_docs();
    let tables = expect_document(&docs, "tables", "`.tables` under --json");
    let items = tables
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();
    let names: Vec<String> = items
        .iter()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    assert!(
        names.iter().any(|n| n == "named_store"),
        "the path on the command line wins over CONTEXTDB_DB_PATH.\n{}",
        outcome.describe()
    );
    assert!(
        !names.iter().any(|n| n == "other_store"),
        "the environment must not redirect an explicitly named store.\n{}",
        outcome.describe()
    );
}

#[test]
fn memory_store_is_writable_without_the_flag_and_accepts_the_flag_as_a_no_op() {
    let script = "CREATE TABLE scratch (id INTEGER PRIMARY KEY);\n\
                  INSERT INTO scratch (id) VALUES (7);\n\
                  SELECT id FROM scratch;\n";

    let implicit = run(&[":memory:", "--json"], script);
    assert_eq!(
        implicit.code,
        Some(0),
        "`:memory:` is always writable; mutation needs no flag.\n{}",
        implicit.describe()
    );

    let explicit = run(&[":memory:", "--write", "--json"], script);
    assert_eq!(
        explicit.code,
        Some(0),
        "an explicit `--write` on `:memory:` is an accepted no-op.\n{}",
        explicit.describe()
    );
}

#[test]
fn memory_store_emits_no_route_notice_and_reports_owner_state_not_applicable() {
    let outcome = run(&[":memory:", "--json"], "SELECT 1;\n.owner status\n");

    assert!(
        outcome.route_notices().is_empty(),
        "`:memory:` has no route to report, so it emits no read_route notice.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let owner = expect_document(&docs, "owner", "`.owner status` on :memory:");
    assert_eq!(
        owner.get("state").and_then(|s| s.as_str()),
        Some("not_applicable"),
        "an in-memory database has no file-backed owner: {owner}"
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "`not_applicable` is a successful report.\n{}",
        outcome.describe()
    );
}

#[test]
fn read_and_owner_read_flags_are_invalid_with_a_memory_store() {
    for flag in [
        "--read-result-rows",
        "--read-result-bytes",
        "--read-cursor-page-rows",
        "--owner-read-result-rows",
        "--owner-read-concurrency",
    ] {
        let outcome = run(&[":memory:", flag, "4"], "SELECT 1;\n");
        assert_eq!(
            outcome.code,
            Some(2),
            "{flag} is invalid with `:memory:` and is refused before any work.\n{}",
            outcome.describe()
        );
    }

    let disabled = run(&[":memory:", "--no-owner-reads"], "SELECT 1;\n");
    assert_eq!(
        disabled.code,
        Some(2),
        "`--no-owner-reads` is invalid with `:memory:`.\n{}",
        disabled.describe()
    );
}

#[test]
fn an_invalid_limit_relationship_is_refused_before_the_store_is_opened() {
    let store = store_with(&create_seeded_table("rows", 3));
    let before = store.contents();

    let cases: [&[&str]; 4] = [
        &[
            "--read-cursor-page-rows",
            "600",
            "--read-result-rows",
            "500",
        ],
        &[
            "--read-cursor-page-bytes",
            "8388608",
            "--read-result-bytes",
            "4194304",
        ],
        &[
            "--read-cursor-idle-ms",
            "1800000",
            "--read-cursor-lifetime-ms",
            "900000",
        ],
        &["--read-result-rows", "0"],
    ];

    for extra in cases {
        let mut args = vec![store.path_str()];
        args.extend_from_slice(extra);
        let outcome = run(&args, "SELECT id FROM rows;\n");
        assert_eq!(
            outcome.code,
            Some(2),
            "an invalid limit relationship is an invalid invocation, refused before open: {extra:?}\n{}",
            outcome.describe()
        );
        assert!(
            outcome.stdout.trim().is_empty(),
            "nothing runs when the configuration itself is refused: {extra:?}\n{}",
            outcome.describe()
        );
    }

    assert_eq!(
        before,
        store.contents(),
        "a refused configuration must not have opened, let alone touched, the store"
    );
}

#[test]
fn a_valid_lowered_configuration_is_accepted() {
    let store = store_with(&create_seeded_table("rows", 3));
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "10",
            "--read-cursor-page-rows",
            "5",
            "--read-result-bytes",
            "1048576",
            "--read-cursor-page-bytes",
            "65536",
        ],
        "SELECT id FROM rows ORDER BY id;\n",
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "lowering one's own ceilings is ordinary declared configuration.\n{}",
        outcome.describe()
    );
}
