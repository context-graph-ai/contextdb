use super::common::*;
use contextdb_core::Value;
use contextdb_engine::Database;
use std::fs;
use std::os::unix::fs::PermissionsExt;
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

// Named vector index RED tests from named-vector-indexes-tests.md.

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

    let output = run_cli_script(&legacy_path, &[], ".quit\n");
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
    let output = run_cli_script(&db_path, &[], script);
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
    let output = run_cli_script(&db_path, &[], script);
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
    let output = run_cli_script(&db_path, &[], &script);
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
    let output = run_cli_script(&db_path, &[], script);
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
/// I started contextdb-server with a NATS testcontainer, pushed an IN-FLIGHT commit through a sync client,
/// then sent SIGTERM. The server drained, committed the in-flight write, and exited 0. On reopen the
/// in-flight row was durable. A no-drain `exit(0)` impl loses the in-flight commit and fails this test.
/// Unix-only: contextdb has no Windows CI target.
#[cfg(unix)]
#[tokio::test]
async fn f121_contextdb_server_drains_on_sigterm_and_persists_in_flight_commits() {
    use std::process::Command;
    use std::time::Duration;

    let nats = start_nats().await;
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("drain.db");
    let barrier_path = tmp.path().join("push-started.barrier");
    let release_path = tmp.path().join("push-release.barrier");

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
    let child = Command::new(server_bin())
        .args([
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--tenant-id",
            "f121",
            "--nats-url",
            &nats.nats_url,
        ])
        .env("CONTEXTDB_TEST_PUSH_BARRIER_MIN_ROWS", "100")
        .env("CONTEXTDB_TEST_PUSH_BARRIER_FILE", &barrier_path)
        .env("CONTEXTDB_TEST_PUSH_RELEASE_FILE", &release_path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("server should spawn");
    assert!(
        wait_for_sync_server_ready(&nats.nats_url, "f121", Duration::from_secs(15)).await,
        "sync server must respond before SIGTERM drain test starts"
    );

    // Push an IN-FLIGHT row through the running server via a sync client. Use the same path other
    // acceptance tests use (NatsFixture-driven; an edge client opens its own Database, writes, and
    // pushes the changeset to the server's NATS subject). The server applies the change against the
    // db file. This is the "in-flight commit" that the drain must persist.
    let in_flight_id = Uuid::new_v4();
    let in_flight_vec = vec![0.5_f32, 0.5, 0.5, 0.5];
    push_change_through_server(
        &nats.nats_url,
        "f121",
        "evidence",
        in_flight_id,
        in_flight_vec.clone(),
    )
    .await;
    // Start a larger follow-up push and send SIGTERM only after the client has begun the server push.
    // A server that exits 0 without draining active handlers can lose this batch while still passing a
    // quiescent-shutdown test.
    let batch_ids: Vec<Uuid> = (0..4096).map(|_| Uuid::new_v4()).collect();
    let batch_for_task = batch_ids.clone();
    let nats_url = nats.nats_url.clone();
    let task_barrier_path = barrier_path.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let pending_push = tokio::spawn(async move {
        push_many_changes_through_server(
            &nats_url,
            "f121",
            "evidence",
            batch_for_task,
            4,
            Some(started_tx),
            Some(task_barrier_path),
        )
        .await;
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
    std::fs::write(&release_path, b"release").expect("release in-flight push barrier");

    let exit = wait_for_child_output(child, Duration::from_secs(10), "graceful drain").status;
    assert!(
        exit.success(),
        "contextdb-server must exit 0 on SIGTERM; status: {exit:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), pending_push)
        .await
        .expect("in-flight push must complete during graceful drain")
        .expect("in-flight push task must not panic");

    // Reopen and assert BOTH in-flight commits are durable. A no-drain `exit(0)` impl loses any
    // in-flight apply that hadn't fsynced yet — typically the most recent push.
    let reopened = (0..5)
        .find_map(|attempt| match Database::open(&db_path) {
            Ok(db) => Some(db),
            Err(_) if attempt < 4 => {
                std::thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
                None
            }
            Err(e) => panic!("reopen after drain failed: {e:?}"),
        })
        .expect("reopen after drain succeeds within 5 attempts");

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
}

// Helper: push a single-row INSERT through the server via NATS, wait for ack. Lives in
// tests/acceptance/common.rs as `pub(crate) async fn push_change_through_server(...)`. The test plan's
// mechanical-call-site list adds this helper to common.rs alongside the existing spawn_server and
// start_nats helpers — it wraps the existing wire-protocol primitives (encode/decode + push subject)
// already used by the sync acceptance tests.
/// I ran `contextdb-server --version` and got binary version + supported protocol version on stdout, exit 0 — so an
/// operator deciding whether to roll forward a fleet has a single command that answers "what wire version does this
/// node speak?"
#[test]
fn f120_contextdb_server_version_prints_binary_and_protocol_version() {
    let server_bin = workspace_root()
        .join("target")
        .join("release")
        .join("contextdb-server");
    // The existing acceptance harness exposes `ensure_release_binaries()` (tests/acceptance/common.rs:62),
    // which gates the workspace `cargo build --release` behind a `Once` so all CLI-binary tests share one build.
    ensure_release_binaries();
    let output = std::process::Command::new(&server_bin)
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
    // Protocol version is operator-discoverable; require the literal numeric `3` adjacent to the protocol token.
    assert!(
        stdout.contains("protocol_version=3")
            || stdout.contains("protocol_version 3")
            || stdout.contains("PROTOCOL_VERSION 3"),
        "stdout must include `protocol_version=3` (or equivalent) so operators can detect asymmetric upgrades; got: {stdout}"
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
    let output = run_cli_script(&db_path, &[], script);
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
