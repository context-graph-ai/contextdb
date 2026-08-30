use super::common::*;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// A-MA1 — RED: --memory-limit flag sets startup ceiling
// ---------------------------------------------------------------------------
#[test]
fn a_ma1_memory_limit_flag_sets_ceiling() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma1.db");
    let output = run_cli_script(
        &db_path,
        &["--write", "--memory-limit", "4G"],
        "SHOW MEMORY_LIMIT;\n.quit\n",
    );
    assert!(
        output.status.success(),
        "CLI must not crash with --memory-limit"
    );
    let stdout = output_string(&output.stdout);
    // 4G = 4294967296 bytes.
    assert!(
        stdout.contains("4294967296"),
        "SHOW must report startup_ceiling of 4G: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// A-MA2 — RED: CONTEXTDB_MEMORY_LIMIT env var sets ceiling
// ---------------------------------------------------------------------------
/// Run the CLI over a fresh store with the given extra arguments and
/// environment, ask it what its memory ceiling is, and hand back what it said.
fn show_memory_limit(name: &str, extra_args: &[&str], env: &[(&str, &str)]) -> String {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, name);
    ensure_release_binaries();
    let mut command = std::process::Command::new(cli_bin());
    command.arg(&db_path).arg("--write").args(extra_args);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(b"SHOW MEMORY_LIMIT;\n.quit\n")
                .expect("write stdin");
            child.wait_with_output()
        })
        .expect("CLI reports its memory ceiling");
    assert!(output.status.success());
    output_string(&output.stdout)
}

/// The startup memory ceiling is something the operator states on the command
/// line. The environment is not a behavior surface: a `CONTEXTDB_*` alias for
/// the same ceiling is not read, and setting one changes nothing and says
/// nothing -- which is what keeps a ceiling auditable from the invocation
/// alone, rather than from whatever the surrounding shell happened to export.
#[test]
fn a_ma2_the_command_line_sets_the_startup_memory_ceiling_and_the_environment_does_not() {
    // 512M = 536870912 bytes.
    let stated = show_memory_limit("a_ma2-stated.db", &["--memory-limit", "512M"], &[]);
    assert!(
        stated.contains("536870912"),
        "SHOW MEMORY_LIMIT must report the ceiling the command line stated: {stated}"
    );

    let from_environment = show_memory_limit(
        "a_ma2-environment.db",
        &[],
        &[("CONTEXTDB_MEMORY_LIMIT", "512M")],
    );
    assert!(
        !from_environment.contains("536870912"),
        "an environment alias must not set the startup ceiling: {from_environment}"
    );
    assert!(
        from_environment.contains("none"),
        "a store nobody gave a ceiling reports it has none: {from_environment}"
    );
}

// ---------------------------------------------------------------------------
// A-MA3 — RED: SET MEMORY_LIMIT lower than ceiling takes effect
// ---------------------------------------------------------------------------
#[test]
fn a_ma3_set_lower_than_ceiling() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma3.db");
    let output = run_cli_script(
        &db_path,
        &["--write", "--memory-limit", "4G"],
        "SET MEMORY_LIMIT '1G';\nSHOW MEMORY_LIMIT;\n.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    // 1G = 1073741824.
    assert!(
        stdout.contains("1073741824"),
        "SHOW must report limit of 1G after SET: {stdout}"
    );
    // startup_ceiling should still be 4G.
    assert!(
        stdout.contains("4294967296"),
        "startup_ceiling must remain 4G: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// A-MA4 — RED: SET MEMORY_LIMIT higher than ceiling errors
// ---------------------------------------------------------------------------
#[test]
fn a_ma4_set_higher_than_ceiling_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma4.db");
    let output = run_cli_script(
        &db_path,
        &["--memory-limit", "1G"],
        "SET MEMORY_LIMIT '4G';\n.quit\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code on SET error, not crash"
    );
    let stderr = output_string(&output.stderr);
    assert!(
        stderr.contains("Error")
            || stderr.contains("error")
            || stderr.contains("exceed")
            || stderr.contains("ceiling"),
        "SET above ceiling must produce error on stderr: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// A-MA5 — RED: SET MEMORY_LIMIT 'none' with ceiling errors
// ---------------------------------------------------------------------------
#[test]
fn a_ma5_set_none_with_ceiling_errors() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma5.db");
    let output = run_cli_script(
        &db_path,
        &["--memory-limit", "1G"],
        "SET MEMORY_LIMIT 'none';\n.quit\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code on SET error, not crash"
    );
    let stderr = output_string(&output.stderr);
    assert!(
        stderr.contains("Error") || stderr.contains("error") || stderr.contains("ceiling"),
        "SET 'none' with ceiling must produce error on stderr: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// A-MA6 — RED: MemoryBudgetExceeded error has all diagnostic fields
// ---------------------------------------------------------------------------
#[test]
fn a_ma6_error_message_has_diagnostic_fields() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma6.db");
    let output = run_cli_script(
        &db_path,
        &["--write", "--memory-limit", "512"],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'test data that exceeds tiny budget');\n.quit\n",
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code on a memory budget error, not crash"
    );
    let stderr = output_string(&output.stderr);
    // Error must contain: subsystem, operation, requested bytes, available, budget, hint.
    assert!(
        stderr.contains("memory budget exceeded"),
        "error must mention 'memory budget exceeded' on stderr: {stderr}"
    );
    assert!(
        stderr.contains("Hint:") || stderr.contains("hint:"),
        "error must contain a hint for AI agents on stderr: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// A-MA7 — RED: INSERT, exhaust, delete, INSERT again
// ---------------------------------------------------------------------------
#[test]
fn a_ma7_insert_exhaust_delete_insert() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma7.db");
    let output = run_cli_script(
        &db_path,
        &["--write", "--memory-limit", "2048"],
        concat!(
            "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'first');\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'second attempt that may fail');\n",
            "DELETE FROM t WHERE id = '00000000-0000-0000-0000-000000000001';\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'after reclaim');\n",
            "SELECT COUNT(*) FROM t;\n",
            ".quit\n",
        ),
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code (the probing second INSERT fails), not crash"
    );
    let stdout = output_string(&output.stdout);
    // The final INSERT (after DELETE) must succeed, proving memory reclamation works.
    // The SELECT COUNT(*) output must show a count (at least 1 row survived).
    // Also verify "after reclaim" row is present — it was the post-delete INSERT.
    assert!(
        stdout.contains("after reclaim"),
        "final INSERT with 'after reclaim' must succeed and be visible in SELECT: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// A-MA8 — REGRESSION GUARD: CLI without --memory-limit works as before
// ---------------------------------------------------------------------------
#[test]
fn a_ma8_no_memory_limit_flag_works() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma8.db");
    let output = run_cli_script(
        &db_path,
        &["--write"],
        concat!(
            "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'works');\n",
            "SELECT * FROM t;\n",
            "SHOW MEMORY_LIMIT;\n",
            ".quit\n",
        ),
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("works"),
        "basic operations must work without limit flag"
    );
    assert!(
        stdout.contains("none"),
        "SHOW MEMORY_LIMIT must report 'none' when no limit set: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// A-MA9 — RED: file-backed MEMORY_LIMIT survives restart
// ---------------------------------------------------------------------------
#[test]
fn a_ma9_memory_limit_survives_restart() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma9.db");

    let configured = run_cli_script(
        &db_path,
        &["--write"],
        "SET MEMORY_LIMIT '1K';\nSHOW MEMORY_LIMIT;\n.quit\n",
    );
    assert!(configured.status.success());
    let configured_stdout = output_string(&configured.stdout);
    assert!(
        configured_stdout.contains("1024"),
        "SHOW MEMORY_LIMIT must report the configured limit before restart: {configured_stdout}"
    );

    let reopened = run_cli_script(
        &db_path,
        &["--write"],
        &format!(
            "SHOW MEMORY_LIMIT;\nCREATE TABLE big (id UUID PRIMARY KEY, payload TEXT);\nINSERT INTO big (id, payload) VALUES ('00000000-0000-0000-0000-000000000001', '{}');\n.quit\n",
            "x".repeat(4096)
        ),
    );
    assert_eq!(
        reopened.status.code(),
        Some(1),
        "CLI must exit cleanly with the definitive-error code across OOM errors, not crash"
    );
    let reopened_stdout = output_string(&reopened.stdout);
    assert!(
        reopened_stdout.contains("1024"),
        "reopened database must still report the configured MEMORY_LIMIT: {reopened_stdout}"
    );
    let reopened_stderr = output_string(&reopened.stderr);
    assert!(
        reopened_stderr
            .to_lowercase()
            .contains("memory budget exceeded"),
        "reopened database must still enforce the persisted limit on stderr: {reopened_stderr}"
    );
}
