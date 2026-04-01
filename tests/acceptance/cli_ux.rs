use super::common::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

/// I piped a SQL script into the CLI, and it ran every command and showed me results.
#[test]
fn f28_scripted_usage_via_stdin_pipe() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f28.db");
    let script_path = tmp.path().join("commands.sql");
    fs::write(
        &script_path,
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'hello')\nSELECT * FROM t\n.quit\n",
    )
    .expect("write commands.sql");
    let output = run_cli_script_from_file(&db_path, &[], &script_path);
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("hello"));
}

/// I asked for sync status while connected, and it showed me the tenant, URL, connection state, and LSN — not a cryptic blob.
#[tokio::test]
async fn f29_sync_status_shows_meaningful_info_when_connected() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f29.db");
    let server_path = temp_db_file(&tmp, "f29-server.db");
    let nats = start_nats().await;
    let mut server = spawn_server(&server_path, "f29", &nats.nats_url);
    let output = run_cli_script(
        &db_path,
        &["--tenant-id", "f29", "--nats-url", &nats.ws_url],
        ".sync status\n.quit\n",
    );
    stop_child(&mut server);
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("tenant=f29"));
    assert!(stdout.contains(&nats.ws_url));
    assert!(stdout.contains("connected"));
    assert!(stdout.contains("LSN"));
}

/// I asked for sync status when the server was down, and it told me "unreachable" instead of crashing.
#[test]
fn f30_sync_status_when_nats_is_unreachable() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f30.db");
    let output = run_cli_script(
        &db_path,
        &["--tenant-id", "f30", "--nats-url", "nats://127.0.0.1:65532"],
        ".sync status\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("unreachable"));
}

/// I typed a nonsense dot-command, and the CLI told me it was unknown instead of silently ignoring it.
#[test]
fn f31_unknown_commands_produce_helpful_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(&temp_db_file(&tmp, "f31.db"), &[], ".bogus\n.quit\n");
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("Unknown command"));
}

/// I launched the CLI without any sync flags, and it still let me create tables, insert, and query locally.
#[test]
fn f31b_cli_works_without_sync_flags_graceful_degradation() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f31b.db"),
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok')\nSELECT * FROM t\n.quit\n",
    );
    assert!(output.status.success());
    assert!(output_string(&output.stdout).contains("ok"));
}

/// I tried to sync push without configuring a tenant, and it told me what flags I was missing.
#[test]
fn f31c_sync_push_without_sync_config_gives_helpful_error() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(&temp_db_file(&tmp, "f31c.db"), &[], ".sync push\n.quit\n");
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("Sync not configured"));
    assert!(stdout.contains("--tenant-id"));
}

/// I ran valid and invalid SQL in scripts, and the exit code was 0 for success and non-zero for errors, so my shell scripts can trust it.
#[test]
fn f31d_cli_exit_codes_are_reliable_for_scripting() {
    let tmp = TempDir::new().expect("tempdir");
    let good = run_cli_script(
        &temp_db_file(&tmp, "f31d-good.db"),
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY)\nSELECT * FROM t\n.quit\n",
    );
    let parse_error = run_cli_script(
        &temp_db_file(&tmp, "f31d-parse.db"),
        &[],
        "SELET * FROM t\n.quit\n",
    );
    let missing_table = run_cli_script(
        &temp_db_file(&tmp, "f31d-missing.db"),
        &[],
        "SELECT * FROM nonexistent\n.quit\n",
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
        &[],
        "SELET * FROM t\n.quit\n",
    );
    assert!(output_string(&invalid.stdout).trim().is_empty());
    assert!(!output_string(&invalid.stderr).trim().is_empty());

    let valid = run_cli_script(
        &temp_db_file(&tmp, "f31e-valid.db"),
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok')\nSELECT * FROM t\n.quit\n",
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
    let output = run_cli_script(&db_path, &[], ".quit\n");
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
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT, val REAL)\nINSERT INTO t (id, name, val) VALUES ('00000000-0000-0000-0000-000000000001', 'alpha', 1.5)\nSELECT * FROM t\n.quit\n",
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
        &[],
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,11}(b) COLUMNS (b.id AS b_id))\n.quit\n",
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
