use super::common::*;
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::Database;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

/// I piped a SQL script into the CLI, and it ran every command and showed me results.
#[test]
fn f28_scripted_usage_via_stdin_pipe() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f28.db");
    let script_path = tmp.path().join("commands.sql");
    fs::write(
        &script_path,
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'hello');\nSELECT * FROM t;\n.quit\n",
    )
    .expect("write commands.sql");
    let output = run_cli_script_from_file(&db_path, &["--write"], &script_path);
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("hello"));
}

/// I asked for sync status while connected, and it showed me the tenant, endpoint, connection state, and LSN — not a cryptic blob.
#[tokio::test]
async fn f29_sync_status_shows_meaningful_info_when_connected() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f29.db");
    let server_path = temp_db_file(&tmp, "f29-server.db");
    let sync = start_sync_fixture().await;
    let mut server = spawn_server(&server_path, "f29", &sync.bind_spec);
    let output = run_cli_script(
        &db_path,
        &[
            "--write",
            "--tenant-id",
            "f29",
            "--sync-endpoint",
            &sync.ticket,
        ],
        ".sync status\n.quit\n",
    );
    stop_child(&mut server);
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("tenant=f29"));
    assert!(stdout.contains(&sync.ticket));
    assert!(stdout.contains("connected"));
    assert!(stdout.contains("LSN"));
}

/// I asked for sync status when the server was down, and it told me "unreachable" instead of crashing.
#[test]
fn f30_sync_status_when_endpoint_is_unreachable() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f30.db");
    let output = run_cli_script(
        &db_path,
        &[
            "--write",
            "--tenant-id",
            "f30",
            "--sync-endpoint",
            "iroh:invalid-test-ticket",
        ],
        ".sync status\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("unreachable"));
}

/// I typed a nonsense dot-command, and the CLI told me it was unknown on stderr and failed the run —
/// not a silent success a script would trust.
///
/// This used to print to stdout and exit 0; every error now fails the run
/// and goes to stderr.
#[test]
fn f31_unknown_commands_produce_helpful_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31.db"),
        &["--write"],
        ".bogus\n.quit\n",
    );
    assert!(
        !output.status.success(),
        "an unknown command must fail the run"
    );
    let stdout = output_string(&output.stdout);
    let stderr = output_string(&output.stderr);
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got: {stdout}"
    );
    assert!(
        stderr.contains("Unknown command"),
        "stderr must name the unknown command, got: {stderr}"
    );
}

/// I launched the CLI without any sync flags, and it still let me create tables, insert, and query locally.
#[test]
fn f31b_cli_works_without_sync_flags_graceful_degradation() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31b.db"),
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok');\nSELECT * FROM t;\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("ok"));
}

/// I tried to sync push without configuring a tenant, and it told me what flags I was missing on
/// stderr and failed the run — an action that never happened must not read as success to a script.
///
/// Every `.sync` subcommand (action or query) used to return `ok: true` when
/// unconfigured, printing to stdout and exiting 0. The family now splits:
/// ACTIONS (push/pull/reconnect/direction/policy) fail; QUERIES (status/auto)
/// keep answering and exiting 0 (see the sibling test below).
#[test]
fn f31c_sync_push_without_sync_config_gives_helpful_error() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31c.db"),
        &["--write"],
        ".sync push\n.quit\n",
    );
    assert!(
        !output.status.success(),
        "a sync ACTION with no sync configured must fail the run"
    );
    let stderr = output_string(&output.stderr);
    assert!(
        stderr.contains("Sync not configured"),
        "got stderr: {stderr}"
    );
    assert!(stderr.contains("--tenant-id"), "got stderr: {stderr}");
}

/// I asked for sync status without configuring a tenant, and it answered the question (no sync
/// configured) and exited cleanly — a QUERY, unlike an ACTION, isn't refused by missing config.
#[test]
fn f31c_sync_status_without_sync_config_still_succeeds() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31c-status.db"),
        &["--write"],
        ".sync status\n.quit\n",
    );
    assert!(
        output.status.success(),
        "a sync QUERY with no sync configured must still exit 0"
    );
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("Sync not configured"),
        "got stdout: {stdout}"
    );
}

/// I ran valid and invalid SQL in scripts, and the exit code was 0 for success and non-zero for errors, so my shell scripts can trust it.
#[test]
fn f31d_cli_exit_codes_are_reliable_for_scripting() {
    let tmp = TempDir::new().expect("tempdir");
    let good = run_cli_script(
        &temp_db_file(&tmp, "f31d-good.db"),
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY);\nSELECT * FROM t;\n.quit\n",
    );
    let parse_error = run_cli_script(
        &temp_db_file(&tmp, "f31d-parse.db"),
        &["--write"],
        "SELET * FROM t;\n.quit\n",
    );
    let missing_table = run_cli_script(
        &temp_db_file(&tmp, "f31d-missing.db"),
        &["--write"],
        "SELECT * FROM nonexistent;\n.quit\n",
    );
    assert!(good.status.success());
    assert!(!parse_error.status.success());
    assert!(!missing_table.status.success());
}

/// I ran bad SQL and good SQL, and errors went to stderr while results went to stdout, so piping works correctly.
#[test]
fn f31e_errors_go_to_stderr_results_to_stdout() {
    let tmp = TempDir::new().expect("tempdir");
    let invalid = run_cli_script(
        &temp_db_file(&tmp, "f31e-invalid.db"),
        &["--write"],
        "SELET * FROM t;\n.quit\n",
    );
    assert!(output_string(&invalid.stdout).trim().is_empty());
    assert!(!output_string(&invalid.stderr).trim().is_empty());

    let valid = run_cli_script(
        &temp_db_file(&tmp, "f31e-valid.db"),
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok');\nSELECT * FROM t;\n.quit\n",
    );
    assert!(output_string(&valid.stderr).trim().is_empty());
    assert!(output_string(&valid.stdout).contains("ok"));
}

/// I pointed the CLI at a directory I can't write to, and it told me "permission denied" instead of panicking.
#[test]
fn f31f_permission_denied_on_db_path_gives_clear_error() {
    let tmp = TempDir::new().expect("tempdir");
    let denied_dir = tmp.path().join("denied");
    fs::create_dir_all(&denied_dir).expect("create denied dir");
    fs::set_permissions(&denied_dir, fs::Permissions::from_mode(0o555)).expect("chmod denied dir");
    let db_path = denied_dir.join("db.sqlite");
    let output = run_cli_script(&db_path, &["--write"], ".quit\n");
    assert!(!output.status.success());
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(stderr.contains("permission denied") || stderr.contains("failed to open database"));
}

/// I ran a SELECT, and the output came back in a pipe-delimited table I can parse with standard tools.
#[test]
fn f31g_select_output_format_is_parseable() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31g.db"),
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT, val REAL);\nINSERT INTO t (id, name, val) VALUES ('00000000-0000-0000-0000-000000000001', 'alpha', 1.5);\nSELECT * FROM t;\n.quit\n",
    );
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("| id "));
    assert!(stdout.contains("| name "));
    assert!(stdout.contains("| val "));
    assert!(stdout.contains("| alpha "));
}

/// I asked for an over-deep graph traversal, and the CLI treated it as a real error on stderr instead of a successful run.
#[test]
fn f31h_bfs_depth_exceeded_routes_to_stderr_and_nonzero_exit() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31h.db"),
        &["--write"],
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,11}(b) COLUMNS (b.id AS b_id));\n.quit\n",
    );
    assert!(
        !output.status.success(),
        "BfsDepthExceeded must fail the CLI script so shell automation can detect it"
    );
    let stdout = output_string(&output.stdout);
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(
        stdout.trim().is_empty(),
        "BfsDepthExceeded should not be reported as successful stdout output: {stdout}"
    );
    assert!(
        stderr.contains("depth") || stderr.contains("bfs"),
        "BfsDepthExceeded should be reported on stderr with a depth-related message: {stderr}"
    );
}

fn graph_trace_fixture_script(query: &str) -> String {
    format!(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT);\n\
         CREATE TABLE edges (source_id UUID, target_id UUID, edge_type TEXT);\n\
         INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'root');\n\
         INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'a');\n\
         INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'b');\n\
         INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'other');\n\
         INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'c');\n\
         INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS');\n\
         INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'LINKS');\n\
         INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005', 'LINKS');\n\
         {query};\n\
         .quit\n"
    )
}

fn graph_trace_default_query() -> &'static str {
    "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS t))"
}

fn graph_trace_default_ordered_query() -> &'static str {
    "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS t)) ORDER BY t"
}

fn graph_trace_unpinned_query() -> &'static str {
    "SELECT s, t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS s, b.id AS t))"
}

fn cli_output(output: &std::process::Output) -> String {
    let mut combined = output_string(&output.stdout);
    combined.push_str(&output_string(&output.stderr));
    combined
}

/// ct01: default CLI query output stays byte-stable and does not leak trace lines.
#[test]
fn ct01_default_cli_output_for_graph_query_has_no_trace_line() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct01.db");
    let output = run_cli_script(
        &db_path,
        &["--write"],
        &graph_trace_fixture_script(graph_trace_default_ordered_query()),
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    let expected = "\
ok (rows_affected=0)\n\
ok (rows_affected=0)\n\
INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'root');\n\
ok (rows_affected=1)\n\
INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'a');\n\
ok (rows_affected=1)\n\
INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'b');\n\
ok (rows_affected=1)\n\
INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000004', 'other');\n\
ok (rows_affected=1)\n\
INSERT INTO nodes (id, name) VALUES ('00000000-0000-0000-0000-000000000005', 'c');\n\
ok (rows_affected=1)\n\
INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS');\n\
ok (rows_affected=1)\n\
INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'LINKS');\n\
ok (rows_affected=1)\n\
INSERT INTO edges (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005', 'LINKS');\n\
ok (rows_affected=1)\n\
+--------------------------------------+\n\
| t                                    |\n\
+--------------------------------------+\n\
| 00000000-0000-0000-0000-000000000002 |\n\
| 00000000-0000-0000-0000-000000000003 |\n\
+--------------------------------------+\n\
(2 rows)\n";
    assert_eq!(stdout, expected);
    assert!(
        !stdout.lines().any(|line| line.starts_with("trace: ")),
        "default output must not contain trace lines: {stdout}"
    );
}

/// ct02: `.trace on` exposes the single-hop adjacency probe and exact degree work.
#[test]
fn ct02_trace_on_graph_pinned_match_reports_adjacency_probe_rows_examined() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct02.db");
    let script = graph_trace_fixture_script(&format!(".trace on\n{}", graph_trace_default_query()));
    let output = run_cli_script(&db_path, &["--write"], &script);
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("trace: AdjacencyProbe index=forward_adj pushed=[a.id] rows_examined=2"),
        "expected pinned graph probe trace with exact degree rows; got: {stdout}"
    );
}

/// ct03: `.trace on` shows an honest edges scan when no vertex is pinned.
#[test]
fn ct03_trace_on_unpinned_graph_match_reports_edges_scan() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct03.db");
    let script =
        graph_trace_fixture_script(&format!(".trace on\n{}", graph_trace_unpinned_query()));
    let output = run_cli_script(&db_path, &["--write"], &script);
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("trace: EdgesScan rows_examined=3"),
        "expected unpinned graph query to report EdgesScan with full edge count; got: {stdout}"
    );
}

/// ct04: `.trace on` is general, so relational indexed queries report IndexScan too.
#[test]
fn ct04_trace_on_relational_indexed_query_reports_index_scan() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct04.db");
    let script = "\
CREATE TABLE things (id UUID PRIMARY KEY, name TEXT);\n\
CREATE INDEX idx_things_name ON things (name);\n\
INSERT INTO things (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'hit');\n\
INSERT INTO things (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'miss');\n\
.trace on\n\
SELECT id FROM things WHERE name = 'hit';\n\
.quit\n";
    let output = run_cli_script(&db_path, &["--write"], script);
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("trace: IndexScan index=idx_things_name pushed=[name] rows_examined=1"),
        "expected relational index trace; got: {stdout}"
    );
}

/// ct05: `.trace off` suppresses trace lines after they were enabled.
#[test]
fn ct05_trace_off_disables_subsequent_trace_lines() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct05.db");
    let script = graph_trace_fixture_script(&format!(
        ".trace on\n{};\n.trace off\n{}",
        graph_trace_default_query(),
        graph_trace_default_query()
    ));
    let output = run_cli_script(&db_path, &["--write"], &script);
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert_eq!(
        stdout
            .lines()
            .filter(|line| line.starts_with("trace: "))
            .count(),
        1,
        "only the query executed while trace is on should print a trace line: {stdout}"
    );
}

/// ct06: help output lists the trace toggle for discoverability.
#[test]
fn ct06_help_lists_trace_command() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct06.db");
    let output = run_cli_script(&db_path, &["--write"], ".help\n.quit\n");
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains(".trace on|off"),
        ".help must list the trace toggle; got: {stdout}"
    );
}

/// ct07: scripted plan errors must produce a non-zero process exit, not a false-success shell status.
#[test]
fn ct07_scripted_plan_error_exits_nonzero_without_panic() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct07.db");
    let bad_query = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.bogus AS t))";
    let output = run_cli_script(
        &db_path,
        &["--write"],
        &graph_trace_fixture_script(bad_query),
    );
    assert!(
        !output.status.success(),
        "plan errors must fail scripted CLI sessions"
    );
    let combined = cli_output(&output);
    assert!(
        combined.contains("plan error") && combined.contains("project column not found"),
        "plan error must be rendered without panic output; got: {combined}"
    );
    assert!(
        !combined.to_lowercase().contains("panic"),
        "plan error path must not panic; got: {combined}"
    );
}

/// ct08: scripted parse errors already exited non-zero; keep that contract pinned.
#[test]
fn ct08_scripted_parse_error_still_exits_nonzero() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct08.db");
    let output = run_cli_script(&db_path, &["--write"], "SELET * FROM things\n.quit\n");
    assert!(!output.status.success());
    let combined = cli_output(&output).to_lowercase();
    assert!(
        combined.contains("parse error") || combined.contains("syntax"),
        "parse errors must remain operator-visible; got: {combined}"
    );
}

/// ct09: scripted `.explain` parse errors must also fail the process.
#[test]
fn ct09_scripted_explain_parse_error_exits_nonzero() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct09.db");
    let output = run_cli_script(
        &db_path,
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY);\n.explain SELET * FROM t\n.quit\n",
    );
    assert!(
        !output.status.success(),
        ".explain errors must fail scripted CLI sessions"
    );
    let combined = cli_output(&output).to_lowercase();
    assert!(
        combined.contains("parse error") || combined.contains("syntax"),
        ".explain parse errors must remain operator-visible; got: {combined}"
    );
    assert!(
        !combined.contains("panic"),
        ".explain error path must not panic; got: {combined}"
    );
}

/// ct10: sorted graph query traces must still expose the graph access strategy.
#[test]
fn ct10_trace_on_sorted_graph_query_reports_adjacency_probe_not_sort_only() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct10.db");
    let script = graph_trace_fixture_script(&format!(
        ".trace on\n{}",
        graph_trace_default_ordered_query()
    ));
    let output = run_cli_script(&db_path, &["--write"], &script);
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("trace: AdjacencyProbe index=forward_adj pushed=[a.id] rows_examined=2"),
        "sorted graph query trace must expose the adjacency probe; got: {stdout}"
    );
    assert!(
        !stdout.contains("trace: Sort index=forward_adj"),
        "sorted graph query trace must not hide the graph route behind Sort: {stdout}"
    );
}

/// ct11: scripted `.explain` usage errors must also fail the process.
#[test]
fn ct11_scripted_explain_without_sql_exits_nonzero() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct11.db");
    let output = run_cli_script(&db_path, &["--write"], ".explain\n.quit\n");
    assert!(
        !output.status.success(),
        ".explain without SQL must fail scripted CLI sessions"
    );
    let combined = cli_output(&output).to_lowercase();
    assert!(
        combined.contains("usage: .explain <sql>"),
        ".explain usage error must remain operator-visible; got: {combined}"
    );
}

#[test]
fn ct12_scripted_explain_shows_runtime_index_trace() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "ct12.db");
    let output = run_cli_script(
        &db_path,
        &["--write"],
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER);\n\
         CREATE INDEX idx_a ON t (a);\n\
         INSERT INTO t (id, a) VALUES ('00000000-0000-0000-0000-000000000001', 7);\n\
         .explain SELECT id FROM t WHERE a = 7\n\
         .quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("IndexScan { index: idx_a }"),
        ".explain must show runtime index routing, got: {stdout}"
    );
    assert!(
        stdout.contains("predicates_pushed: [a]"),
        ".explain must show pushed predicates, got: {stdout}"
    );
}

// Named vector index tests: naming, isolation, and lifecycle coverage.

fn single_cli_error_line<'a>(stderr: &'a str, context: &str) -> &'a str {
    let error_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(
        error_lines.len(),
        1,
        "{context} must render as exactly one non-empty stderr line; got: {stderr}"
    );
    assert!(
        error_lines[0].trim_start().starts_with("Error:"),
        "{context} stderr line must start with `Error:`; got: {stderr}"
    );
    error_lines[0]
}

/// I pointed contextdb-cli at a 0.3.4 fixture file; the CLI printed a single Error: line that contained "sync" and
/// "reimport" and exited non-zero, so an operator can act on the message without reading source.
#[test]
fn f116_cli_renders_legacy_vector_store_error_with_actionable_guidance() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let legacy_path = tmp.path().join("legacy.db");
    let fixture = workspace_root()
        .join("tests")
        .join("fixtures")
        .join("legacy_vector_store_v0_3_4.db");
    std::fs::copy(&fixture, &legacy_path).expect("copy fixture");

    let output = run_cli_script(&legacy_path, &["--write"], ".quit\n");
    assert!(
        !output.status.success(),
        "CLI must exit non-zero on legacy detection"
    );
    let stderr = output_string(&output.stderr);
    let error_line = single_cli_error_line(&stderr, "legacy detection");
    assert!(
        error_line.contains("legacy vector store")
            || error_line.contains("LegacyVectorStoreDetected"),
        "stderr must name the error; got: {stderr}"
    );
    assert!(
        error_line.contains("sync"),
        "stderr must point at sync-from-peer recovery; got: {stderr}"
    );
    assert!(
        error_line.contains("recreate") && error_line.contains("reimport"),
        "stderr must point at recreate-and-reimport recovery; got: {stderr}"
    );
}

/// I tried inserting a wrong-dim vector through the CLI; the Error: line on stderr carried (table, column) and
/// "expected 4" / "got 5" — readable identity that an operator can act on.
#[test]
fn f117_cli_renders_dimension_mismatch_with_index_identity() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("db.db");
    // Use distinctive table/column names so they cannot be confused with input-echo digits.
    // (CLI scripted mode echoes INSERT lines to stdout via println!; errors go to stderr only.)
    let script = "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4));\n\
                  INSERT INTO evidence VALUES ('11111111-1111-1111-1111-111111111111', [0.1, 0.2, 0.3, 0.4, 0.5]);\n\
                  .quit\n";
    let output = run_cli_script(&db_path, &["--write"], script);
    assert!(
        !output.status.success(),
        "CLI must exit non-zero for vector dimension mismatch"
    );
    // Errors are printed to stderr. Stdout contains the echoed INSERT line — vacuous for assertions.
    let stderr = output_string(&output.stderr);
    // Find the operator-readable error line: a single line prefixed with `Error:` (the existing CLI
    // error rendering convention from is_fatal_cli_error path). All identity tokens must appear on
    // this one line so an operator scanning logs sees the context together — not scattered across a
    // multi-line panic dump.
    let error_line = single_cli_error_line(&stderr, "vector dimension mismatch");
    assert!(
        error_line.contains("evidence") && error_line.contains("vector_text"),
        "Error line must name the offending (table, column); got: {error_line}"
    );
    let lower = error_line.to_lowercase();
    assert!(
        lower.contains("expected") && lower.contains("4"),
        "Error line must label expected dimension 4; got: {error_line}"
    );
    assert!(
        (lower.contains("actual") || lower.contains("got")) && lower.contains("5"),
        "Error line must label actual/got dimension 5; got: {error_line}"
    );
    assert!(
        lower.contains("dimension") || lower.contains("dim "),
        "Error line must include a 'dimension' context word so the digits 4 and 5 are clearly \
             the dim pair, not incidental tokens; got: {error_line}"
    );
}

/// I tried searching by an unregistered vector column through the CLI; the stderr Error: line carried the unregistered (table, column).
#[test]
fn f118_cli_renders_unknown_vector_index_error() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("db.db");
    let script = "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4));\n\
                  SELECT id FROM evidence ORDER BY vector_unknown <=> '[0,0,0,0]' LIMIT 1;\n\
                  .quit\n";
    let output = run_cli_script(&db_path, &["--write"], script);
    assert!(
        !output.status.success(),
        "CLI must exit non-zero for unknown vector index"
    );
    // Only the INSERT path is echoed in scripted mode; SELECT errors go to stderr cleanly.
    let stderr = output_string(&output.stderr);
    // Single `Error:` line; both identity tokens on it.
    let error_line = single_cli_error_line(&stderr, "unknown vector index");
    assert!(
        error_line.contains("evidence") && error_line.contains("vector_unknown"),
        "Error line must name the unregistered (table, column); got: {error_line}"
    );
}

/// I drove the CLI to MEMORY_LIMIT exhaustion on a vector insert; stderr's Error: line carried the
/// offending (table, column) tag so the operator could attribute the budget exhaustion.
#[test]
fn f117b_cli_renders_memory_budget_exceeded_with_index_tag() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("budget.db");
    // SET MEMORY_LIMIT before insert. The exact CLI / SQL surface for setting MEMORY_LIMIT is
    // `SET MEMORY_LIMIT '4K'` per query-surface-spec; if that surface differs at Step 3, adjust.
    let vector_literal = std::iter::repeat_n("1.0", 2048)
        .collect::<Vec<_>>()
        .join(",");
    let script = format!(
        "SET MEMORY_LIMIT '4K';\n\
         CREATE TABLE audio_clips (id UUID PRIMARY KEY, vector_audio VECTOR(2048));\n\
         INSERT INTO audio_clips (id, vector_audio) VALUES \
         ('11111111-1111-1111-1111-111111111111', [{vector_literal}]);\n\
         .quit\n"
    );
    let output = run_cli_script(&db_path, &["--write"], &script);
    assert!(
        !output.status.success(),
        "CLI must exit non-zero for vector memory budget exhaustion"
    );
    let stderr = output_string(&output.stderr);
    let error_line = single_cli_error_line(&stderr, "memory budget exhaustion");
    assert!(
        error_line.to_lowercase().contains("memory"),
        "Error line must be memory-related; got: {error_line}"
    );
    assert!(
        error_line.contains("@")
            && error_line.contains("audio_clips")
            && error_line.contains("vector_audio"),
        "Error line must render `@audio_clips.vector_audio` operation tag for budget attribution; got: {error_line}"
    );
}

/// I fed a script with a typo on the WITH clause; stderr's Error: line surfaced a positional hint
/// (line number or column) so a 200-line script doesn't force the operator to guess.
#[test]
fn f117c_cli_renders_parse_error_with_positional_hint() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("parse.db");
    // Line 3 has the typo. Lines 1-2 and 4-5 are valid filler.
    let script = "-- line 1: valid comment\n\
                  CREATE TABLE good (id UUID PRIMARY KEY);\n\
                  CREATE TABLE bad (id UUID PRIMARY KEY, v VECTOR(4) WITH (quantization = 'NOPE'));\n\
                  CREATE TABLE good2 (id UUID PRIMARY KEY);\n\
                  .quit\n";
    let output = run_cli_script(&db_path, &["--write"], script);
    assert!(
        !output.status.success(),
        "CLI must exit non-zero for parse errors"
    );
    let stderr = output_string(&output.stderr);
    let error_line = single_cli_error_line(&stderr, "parse error");
    // Pest-driven errors include line/column positions. Accept either `line 3`, `:3:`, or
    // `(line 3, col N)` — each is a real operator-actionable hint.
    let has_position =
        error_line.contains("line 3") || error_line.contains(":3:") || error_line.contains("3:");
    assert!(
        has_position,
        "Parse error must include a positional hint (line/col) so multi-line scripts are debuggable; \
             got: {error_line}"
    );
}
/// I started contextdb-server with an Iroh endpoint, parked an authenticated push after the server
/// accepted its apply task, then sent SIGTERM. The server stopped admission, drained and acknowledged
/// that accepted work, and exited 0. On reopen every accepted row was durable. A no-drain `exit(0)`
/// implementation loses the acknowledgement or the accepted work and fails this test.
/// Unix-only: contextdb has no Windows CI target.
#[cfg(unix)]
#[tokio::test]
async fn f121_contextdb_server_drains_on_sigterm_and_persists_in_flight_commits() {
    use std::process::Command;
    use std::time::Duration;

    async fn wait_for_fixture_path(
        path: std::path::PathBuf,
        timeout: Duration,
        contract: &'static str,
    ) {
        let observed_path = path.clone();
        let observed =
            tokio::task::spawn_blocking(move || wait_until(timeout, || observed_path.exists()))
                .await
                .expect("fixture path observer must not panic");
        assert!(observed, "{contract}: {}", path.display());
    }

    let sync = start_sync_fixture().await;
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("drain.db");
    let barrier_path = tmp.path().join("push-started.barrier");
    let release_path = tmp.path().join("push-release.barrier");
    let shutdown_quiesced_path = tmp.path().join("shutdown-quiesced.barrier");
    assert!(
        Command::new("mkfifo")
            .arg(&release_path)
            .status()
            .expect("mkfifo must execute")
            .success(),
        "create deterministic push-release channel"
    );

    // Pre-create the schema; the server will open this database when spawned.
    {
        let db = Database::open(&db_path).expect("seed open");
        db.execute(
            "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
            &empty_params(),
        )
        .expect("create");
        drop(db);
    }

    ensure_release_binaries();
    let stderr_path = tmp.path().join("server.stderr");
    let stderr_file = std::fs::File::create(&stderr_path).expect("server stderr");
    let mut child = Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            "f121",
            "--sync-endpoint",
            &sync.bind_spec,
        ])
        .env("CONTEXTDB_TEST_PUSH_BARRIER_MIN_ROWS", "100")
        .env("CONTEXTDB_TEST_PUSH_BARRIER_FILE", &barrier_path)
        .env("CONTEXTDB_TEST_PUSH_RELEASE_FILE", &release_path)
        .env(
            "CONTEXTDB_TEST_SHUTDOWN_QUIESCED_FILE",
            &shutdown_quiesced_path,
        )
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::from(stderr_file))
        .spawn()
        .expect("server should spawn");

    struct ServerStderrDump<'a> {
        path: &'a std::path::Path,
    }
    impl<'a> Drop for ServerStderrDump<'a> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                let s = std::fs::read_to_string(self.path).unwrap_or_default();
                eprintln!("--- server.stderr ---\n{s}\n--- end ---");
            }
        }
    }
    let _stderr_dump = ServerStderrDump { path: &stderr_path };

    let ready =
        wait_for_child_stdout_contains(&mut child, "enrollment ticket:", Duration::from_secs(15));
    assert!(
        ready.contains("enrollment ticket:"),
        "sync server must publish its enrollment ticket before SIGTERM drain starts; stdout={ready}"
    );

    // Start with one blank, stable file-backed edge. Its authenticated pull installs the hub's
    // pre-created schema; this same database and client author both pushes below.
    let edge_path = tmp.path().join("drain-edge.db");
    let edge_db = Arc::new(Database::open(&edge_path).expect("open blank edge db"));
    let identity_path = tmp.path().join("drain-edge-identity.key");
    let dial_spec = contextdb_server::peer_dial_spec(&sync.ticket, &identity_path);
    let client = Arc::new(contextdb_server::SyncClient::new(
        edge_db.clone(),
        &dial_spec,
        contextdb_core::TenantId::from("f121"),
    ));
    client
        .pull_default()
        .await
        .expect("authenticated pull must install the hub schema on the blank edge");

    // Push a baseline row through the running server via an authenticated sync client. It proves the
    // edge can write through this exact hub before the drain boundary is exercised below.
    let in_flight_id = Uuid::new_v4();
    let in_flight_vec = vec![0.5_f32, 0.5, 0.5, 0.5];
    let baseline_tx = edge_db.begin_or_panic();
    let baseline_row = edge_db
        .insert_row(
            baseline_tx,
            "evidence",
            values(vec![("id", Value::Uuid(in_flight_id))]),
        )
        .expect("insert edge baseline row");
    edge_db
        .insert_vector(
            baseline_tx,
            VectorIndexRef::new("evidence", "vector_text"),
            baseline_row,
            in_flight_vec.clone(),
        )
        .expect("insert edge baseline vector");
    edge_db
        .commit(baseline_tx)
        .expect("commit edge baseline row");
    client
        .push()
        .await
        .expect("push baseline change through server");

    // Start a larger follow-up push and send SIGTERM only after the client has begun the server push.
    // A server that exits 0 without draining active handlers can lose this batch while still passing a
    // quiescent-shutdown test.
    let batch_ids: Vec<Uuid> = (0..128).map(|_| Uuid::new_v4()).collect();
    let batch_tx = edge_db.begin_or_panic();
    for (offset, id) in batch_ids.iter().copied().enumerate() {
        let row_id = edge_db
            .insert_row(batch_tx, "evidence", values(vec![("id", Value::Uuid(id))]))
            .expect("insert edge batch row");
        let mut vector = vec![0.0_f32; 4];
        vector[offset % 4] = 1.0;
        edge_db
            .insert_vector(
                batch_tx,
                VectorIndexRef::new("evidence", "vector_text"),
                row_id,
                vector,
            )
            .expect("insert edge batch vector");
    }
    edge_db.commit(batch_tx).expect("commit edge batch rows");

    let client_for_task = client.clone();
    let task_barrier_path = barrier_path.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let pending_push = tokio::spawn(async move {
        let push_task = tokio::spawn(async move { client_for_task.push().await });
        wait_for_fixture_path(
            task_barrier_path,
            Duration::from_secs(10),
            "in-flight push must reach the server-side SIGTERM barrier",
        )
        .await;
        let _ = started_tx.send(());

        tokio::time::timeout(Duration::from_secs(45), push_task)
            .await
            .expect("direct push must respond after graceful drain")
            .expect("direct push task must not panic")
    });
    started_rx
        .await
        .expect("in-flight push task must reach the server-side SIGTERM barrier");

    let pid = child.id().to_string();
    let kill_status = Command::new("kill")
        .args(["-TERM", pid.as_str()])
        .status()
        .expect("kill -TERM must execute");
    assert!(
        kill_status.success(),
        "kill -TERM must signal the server; status: {kill_status:?}"
    );
    wait_for_fixture_path(
        shutdown_quiesced_path.clone(),
        Duration::from_secs(10),
        "server must observe SIGTERM and close sync admission before the accepted apply is released",
    )
    .await;
    let shutdown_state =
        std::fs::read_to_string(&shutdown_quiesced_path).expect("read shutdown admission marker");
    assert!(
        !shutdown_state.contains("active_requests=0"),
        "the accepted push reply must still hold a drain lease when admission closes: {shutdown_state}"
    );
    std::fs::write(&release_path, b"release").expect("release in-flight push barrier");

    let push_result = tokio::time::timeout(Duration::from_secs(30), pending_push)
        .await
        .expect("in-flight push must complete during graceful drain")
        .expect("in-flight push task must not panic");
    let exit = wait_for_child_output(child, Duration::from_secs(30), "graceful drain").status;
    let completed_shutdown_state =
        std::fs::read_to_string(&shutdown_quiesced_path).expect("read completed shutdown marker");
    let reply_position = completed_shutdown_state
        .find("reply-acknowledged active_requests=1")
        .unwrap_or_else(|| {
            panic!("accepted push reply was not acknowledged under its drain lease:\n{completed_shutdown_state}")
        });
    let drained_position = completed_shutdown_state
        .find("drain-complete active_requests=0")
        .unwrap_or_else(|| {
            panic!("server did not reach a zero-work drain frontier:\n{completed_shutdown_state}")
        });
    assert!(
        reply_position < drained_position,
        "reply acknowledgement must precede endpoint drain completion:\n{completed_shutdown_state}"
    );
    push_result.unwrap_or_else(|err| {
        panic!(
            "direct push response must include apply result: {err}; shutdown state:\n{completed_shutdown_state}"
        )
    });
    assert!(
        exit.success(),
        "contextdb-server must exit 0 on SIGTERM; status: {exit:?}"
    );

    // Reopen and assert the baseline plus every row from the accepted apply are durable. A no-drain
    // `exit(0)` implementation loses work or its acknowledgement at this boundary.
    let reopened = Database::open(&db_path).expect("reopen after the server process exits");

    let r = reopened
        .execute("SELECT id FROM evidence", &empty_params())
        .expect("select");
    let id_idx = r.columns.iter().position(|c| c == "id").unwrap();
    let returned: Vec<Value> = r.rows.iter().map(|row| row[id_idx].clone()).collect();
    assert!(
        returned.contains(&Value::Uuid(in_flight_id)),
        "first in-flight commit must survive graceful drain"
    );
    for id in &batch_ids {
        assert!(
            returned.contains(&Value::Uuid(*id)),
            "every row from the in-flight batch must survive graceful drain; missing {id}"
        );
    }
    let vector_hits = reopened
        .query_vector(
            VectorIndexRef::new("evidence", "vector_text"),
            &in_flight_vec,
            batch_ids.len() + 1,
            None,
            reopened.snapshot(),
        )
        .expect("search vectors after graceful drain reopen");
    assert_eq!(
        vector_hits.len(),
        batch_ids.len() + 1,
        "the baseline and every accepted batch vector must remain searchable after reopen"
    );
}

/// I ran `contextdb-server --version` and got binary version + supported protocol version on stdout, exit 0 — so an
/// operator deciding whether to roll forward a fleet has a single command that answers "what wire version does this
/// node speak?"
#[test]
fn f120_contextdb_server_version_prints_binary_and_protocol_version() {
    ensure_release_binaries();
    let output = std::process::Command::new(server_bin())
        .arg("--version")
        .output()
        .expect("spawn contextdb-server --version");
    assert!(output.status.success(), "--version must exit 0");
    let stdout = output_string(&output.stdout);
    // Binary version line: clap's default `--version` format is "<binary-name> <semver>". Require both
    // tokens so a stripped version emitting only the binary name does not pass.
    assert!(
        stdout.contains("contextdb-server"),
        "stdout must include the binary name; got: {stdout}"
    );
    let semver_present = stdout.split_whitespace().any(|tok| {
        tok.split('.').count() == 3
            && tok
                .split('.')
                .all(|p| p.chars().any(|c| c.is_ascii_digit()))
    });
    assert!(
        semver_present,
        "stdout must include a SemVer-shaped version token; got: {stdout}"
    );
    // Protocol version is operator-discoverable; require the literal numeric `6` adjacent to the protocol token.
    assert!(
        stdout.contains("protocol_version=6")
            || stdout.contains("protocol_version 6")
            || stdout.contains("PROTOCOL_VERSION 6"),
        "stdout must include `protocol_version=6` (or equivalent) so operators can detect asymmetric upgrades; got: {stdout}"
    );
}
/// I ran the README's two-vector walkthrough through contextdb-cli scripted mode. SHOW VECTOR_INDEXES rendered
/// both columns at their declared dimensions, and the ORDER BY query returned the inserted row.
/// Catches doc-vs-binary drift. Robust to CLI input-echo (INSERT lines are echoed; this test discards those
/// before asserting on real output).
#[test]
fn f119_readme_two_vector_walkthrough_through_cli() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("walkthrough.db");
    let script = "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4), vector_vision VECTOR(8));\n\
                  INSERT INTO evidence (id, vector_text, vector_vision) VALUES \
                    ('11111111-1111-1111-1111-111111111111', [1,0,0,0], [0,1,0,0,0,0,0,0]);\n\
                  SHOW VECTOR_INDEXES;\n\
                  SELECT id FROM evidence ORDER BY vector_text <=> '[1,0,0,0]' LIMIT 1;\n\
                  .quit\n";
    let output = run_cli_script(&db_path, &["--write"], script);
    assert!(
        output.status.success(),
        "CLI must succeed on README walkthrough; stderr: {}",
        output_string(&output.stderr)
    );
    let stdout = output_string(&output.stdout);

    // Strip the echoed INSERT line(s) — scripted mode echoes INSERT lines (repl.rs:117). Anything left
    // is real CLI output (rendered tables, query results, prompts).
    let real_output: String = stdout
        .lines()
        .filter(|line| !line.trim_start().to_uppercase().starts_with("INSERT"))
        .collect::<Vec<_>>()
        .join("\n");

    // SHOW VECTOR_INDEXES rendering: column header `dimension` is unique to this output (does not appear in
    // the rest of the script's echoed text or in error messages).
    assert!(
        real_output.contains("dimension"),
        "SHOW VECTOR_INDEXES must render a 'dimension' column header; got: {real_output}"
    );
    let row_for = |column: &str| -> Vec<String> {
        real_output
            .lines()
            .find(|line| line.contains(column))
            .unwrap_or_else(|| panic!("SHOW VECTOR_INDEXES must list {column}; got: {real_output}"))
            .trim_matches('|')
            .split('|')
            .map(|cell| cell.trim().to_string())
            .collect()
    };
    let text_row = row_for("vector_text");
    let vision_row = row_for("vector_vision");
    assert_eq!(
        text_row[0], "evidence",
        "vector_text row must name evidence table"
    );
    assert_eq!(text_row[1], "vector_text");
    assert_eq!(text_row[2], "4");
    assert_eq!(text_row[3], "F32");
    assert_eq!(text_row[4], "1");
    assert!(
        text_row[5].parse::<i64>().expect("vector_text bytes") >= 16,
        "vector_text bytes must reflect stored f32 vector; row={text_row:?}"
    );
    assert_eq!(
        vision_row[0], "evidence",
        "vector_vision row must name evidence table"
    );
    assert_eq!(vision_row[1], "vector_vision");
    assert_eq!(vision_row[2], "8");
    assert_eq!(vision_row[3], "F32");
    assert_eq!(vision_row[4], "1");
    assert!(
        vision_row[5].parse::<i64>().expect("vector_vision bytes") >= 32,
        "vector_vision bytes must reflect stored f32 vector; row={vision_row:?}"
    );
    // The SELECT result includes the row id; the INSERT echo also did, but we filtered echoed INSERT lines.
    assert!(
        real_output.contains("11111111-1111-1111-1111-111111111111"),
        "ORDER BY vector_text must return the inserted row; got: {real_output}"
    );
}
