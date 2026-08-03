//! Tests for the CLI's exit-code contract: results go to stdout, every
//! error/warning/diagnostic goes to stderr, and a run that hit ANY error
//! exits non-zero, from a four-row table (0 ok / 1 error / 2 usage /
//! 3 unconfirmed-push). A whole class of runtime errors (UNIQUE violations,
//! IMMUTABLE-column writes, unknown meta-commands, unconfigured `.sync`
//! actions) used to print `Error: ...` to STDOUT and exit 0 — indistinguishable
//! from success to a script branching on `$?`. Some assertions below already
//! held before this fix (clap's own usage-error exit code, `.explain` in
//! scripted mode, `.sync status`/`.sync auto` with no config, a corrupt-open
//! failure) — those are noted inline as baseline pins; everything else is
//! pinned for the reason stated in its assertion message.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

/// Spawn `contextdb-cli` against `db_path` with `extra_args`, feeding
/// `stdin_sql` on stdin (scripted, non-interactive). Returns (exit code,
/// stdout, stderr).
fn run_cli_at(
    db_path: &str,
    extra_args: &[&str],
    stdin_sql: &str,
) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(db_path);
    for a in extra_args {
        cmd.arg(a);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn contextdb-cli");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin_sql.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn run_cli(extra_args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
    run_cli_at(":memory:", extra_args, stdin_sql)
}

/// A fresh, unique temp directory under the OS temp root — built from the
/// process id plus a per-process atomic counter (never a wall-clock read, per
/// the test-estate ratchet audit).
fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("cdb-exit-code-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create unique temp dir");
    dir
}

// 1. Baseline pin — a clean run exits 0 with empty stderr;
// kept as the positive control for the rest of this file's error-path assertions.
#[test]
fn success_exits_zero_with_empty_stderr() {
    let (code, stdout, stderr) = run_cli(
        &[],
        "CREATE TABLE t (id UUID PRIMARY KEY);\nSELECT * FROM t;\n",
    );
    assert_eq!(
        code,
        Some(0),
        "a clean run must exit 0. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.trim().is_empty(),
        "a clean run must have empty stderr, got:\n{stderr}"
    );
    assert!(
        !stdout.trim().is_empty(),
        "a clean run must produce stdout output"
    );
}

// 2. Baseline pin — a parse error is classified fatal
// (`is_fatal_cli_error`), so it routes to stderr and exits 1.
#[test]
fn parse_error_exits_one_on_stderr() {
    let (code, stdout, stderr) = run_cli(&[], "SELET * FROM t;\n");
    assert_eq!(
        code,
        Some(1),
        "a parse error must exit exactly 1. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty on a parse error, got:\n{stdout}"
    );
    assert!(
        stderr.to_lowercase().contains("parse error"),
        "stderr must name the parse error, got:\n{stderr}"
    );
}

// 3. A `UniqueViolation` must never be classified NON-fatal: the CLI must
// never print `Error: ...` to STDOUT and exit 0, which would make it
// indistinguishable from success to a script branching on `$?`.
#[test]
fn runtime_sql_error_exits_one_on_stderr() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES (1, 'a');\n\
               INSERT INTO t (id, name) VALUES (1, 'dup');\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert_eq!(
        code,
        Some(1),
        "a duplicate primary key must exit 1, never the 0 a non-fatal classification would give it. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("constraint") || stderr.to_lowercase().contains("unique"),
        "stderr must name the constraint violation, got:\n{stderr}"
    );
    assert!(
        !stdout.contains("Error:"),
        "no error text may reach stdout under the exit-code contract, got:\n{stdout}"
    );
}

// 4. A run that hit a non-fatal-classified error must still exit 1 even
// though the session visibly continued past it.
#[test]
fn run_continues_after_an_error_and_still_exits_one() {
    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES (1, 'survivor_row');\n\
               INSERT INTO t (id, name) VALUES (1, 'dup');\n\
               SELECT name FROM t WHERE id = 1;\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert!(
        stdout.contains("survivor_row"),
        "the surviving SELECT must still produce its result. stdout:\n{stdout}"
    );
    assert_eq!(
        code,
        Some(1),
        "a run that hit any error must exit 1 even though it continued. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

// 5. Baseline pin — clap itself raises exit 2 for an unrecognized flag,
// before any of the binary's own code runs. Pins that nothing renumbers it.
#[test]
fn unknown_flag_exits_two() {
    let (code, stdout, stderr) = run_cli(&["--bogus"], "");
    assert_eq!(
        code,
        Some(2),
        "clap usage errors must exit 2. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.trim().is_empty(), "got stdout:\n{stdout}");
    assert!(
        !stderr.trim().is_empty(),
        "clap must explain itself on stderr"
    );
}

// 6. An invalid --memory-limit is a usage error (nothing ran), not a
// runtime error, and must exit 2, not a bare 1.
#[test]
fn invalid_memory_limit_value_exits_two() {
    let (code, stdout, stderr) = run_cli(&["--memory-limit", "12Q"], "");
    assert_eq!(
        code,
        Some(2),
        "an invalid --memory-limit is a usage error, not a runtime error. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--memory-limit"),
        "stderr must name the offending flag, got:\n{stderr}"
    );
}

// 7. `--tenant-id` with no sync endpoint is an incomplete flag
// combination (a usage error), not a runtime error, and must exit 2.
#[test]
fn tenant_id_without_sync_endpoint_exits_two() {
    let (code, stdout, stderr) = run_cli(&["--tenant-id", "acme"], "");
    assert_eq!(
        code,
        Some(2),
        "an incomplete flag combination is a usage error, not a runtime error. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--sync-endpoint"),
        "stderr must name the missing flag, got:\n{stderr}"
    );
}

// 8. An unknown meta-command must never print to STDOUT and succeed —
// it must fail the run.
#[test]
fn unknown_meta_command_exits_one_on_stderr() {
    let (code, stdout, stderr) = run_cli(&[], ".bogus\n");
    assert_eq!(
        code,
        Some(1),
        "an unknown meta-command must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("Unknown command"),
        "stderr must name the unknown command, got:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
}

// 9. An unconfigured `.sync` ACTION subcommand must never return `ok: true`
// the way a QUERY subcommand legitimately does — an action that never ran
// must fail, not read as success.
#[test]
fn sync_action_without_configuration_exits_one() {
    for subcmd in [
        ".sync push",
        ".sync pull",
        ".sync reconnect",
        ".sync destination replacement-hub",
    ] {
        let script = format!("{subcmd}\n");
        let (code, stdout, stderr) = run_cli(&[], &script);
        assert_eq!(
            code,
            Some(1),
            "'{subcmd}' with no sync configured must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.trim().is_empty(),
            "'{subcmd}': stdout must be empty, got:\n{stdout}"
        );
        assert!(
            stderr.contains("Sync not configured"),
            "'{subcmd}': stderr must carry the message, got:\n{stderr}"
        );
    }
}

// 10. Baseline pin — `.sync status` / `.sync auto` answer the
// question and exit 0 (the unconfigured early-return is uniform across
// every subcommand); this pins that QUERIES must keep doing so even though
// ACTIONS (test 9, above) fail.
#[test]
fn sync_query_without_configuration_exits_zero() {
    for subcmd in [".sync status", ".sync auto"] {
        let script = format!("{subcmd}\n");
        let (code, stdout, stderr) = run_cli(&[], &script);
        assert_eq!(
            code,
            Some(0),
            "'{subcmd}' answers the question asked and must not fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains("Sync not configured"),
            "'{subcmd}': the message must be on stdout, got:\n{stdout}"
        );
        assert!(
            stderr.trim().is_empty(),
            "'{subcmd}': stderr must be empty, got:\n{stderr}"
        );
    }
}

// 11. `.schema` of an unknown table must fail the run, not merely print to
// stderr while still exiting 0.
#[test]
fn schema_of_unknown_table_exits_one() {
    let (code, stdout, stderr) = run_cli(&[], ".schema nope\n");
    assert_eq!(
        code,
        Some(1),
        ".schema of an unknown table must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("nope"),
        "stderr must name the missing table, got:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
}

// 12. Baseline pin — `.explain` with no SQL, in SCRIPTED mode, sets
// `session.had_error` (`handle_explain_command` returns
// `input.interactive`, which is `false` when scripted) and exits 1.
#[test]
fn explain_without_argument_exits_one() {
    let (code, stdout, stderr) = run_cli(&[], ".explain\n");
    assert_eq!(
        code,
        Some(1),
        ".explain with no SQL must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("Usage: .explain"),
        "stderr must show the usage line, got:\n{stderr}"
    );
}

// 13. Baseline pin — opening a database under a read-only directory exits 1
// via the named `EXIT_ERROR` constant; this pins that value.
#[test]
fn database_open_failure_exits_one() {
    let dir = unique_temp_dir("db-open-failure");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
        .expect("chmod denied dir");
    let db_path = dir.join("db.sqlite");
    let (code, _stdout, stderr) = run_cli_at(db_path.to_str().expect("utf8 path"), &[], ".quit\n");
    let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755));
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        code,
        Some(1),
        "a database open failure must exit 1. stderr:\n{stderr}"
    );
}
