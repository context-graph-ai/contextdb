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
        &["--memory-limit", "4G"],
        "SHOW MEMORY_LIMIT\n.quit\n",
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
#[test]
fn a_ma2_env_var_sets_ceiling() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "a_ma2.db");
    ensure_release_binaries();
    let output = std::process::Command::new(cli_bin())
        .arg(&db_path)
        .env("CONTEXTDB_MEMORY_LIMIT", "512M")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"SHOW MEMORY_LIMIT\n.quit\n")
                .unwrap();
            child.wait_with_output()
        })
        .expect("CLI with env var");
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    // 512M = 536870912 bytes.
    assert!(
        stdout.contains("536870912"),
        "SHOW must report startup_ceiling of 512M: {stdout}"
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
        &["--memory-limit", "4G"],
        "SET MEMORY_LIMIT '1G'\nSHOW MEMORY_LIMIT\n.quit\n",
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
        "SET MEMORY_LIMIT '4G'\n.quit\n",
    );
    assert!(output.status.success(), "CLI must not crash on SET error");
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("Error")
            || stdout.contains("error")
            || stdout.contains("exceed")
            || stdout.contains("ceiling"),
        "SET above ceiling must produce error: {stdout}"
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
        "SET MEMORY_LIMIT 'none'\n.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(
        stdout.contains("Error") || stdout.contains("error") || stdout.contains("ceiling"),
        "SET 'none' with ceiling must produce error: {stdout}"
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
        &["--memory-limit", "256"],
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'test data that exceeds tiny budget')\n.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    // Error must contain: subsystem, operation, requested bytes, available, budget, hint.
    assert!(
        stdout.contains("memory budget exceeded"),
        "error must mention 'memory budget exceeded': {stdout}"
    );
    assert!(
        stdout.contains("Hint:") || stdout.contains("hint:"),
        "error must contain a hint for AI agents: {stdout}"
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
        &["--memory-limit", "2048"],
        concat!(
            "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'first')\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'second attempt that may fail')\n",
            "DELETE FROM t WHERE id = '00000000-0000-0000-0000-000000000001'\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'after reclaim')\n",
            "SELECT COUNT(*) FROM t\n",
            ".quit\n",
        ),
    );
    assert!(output.status.success());
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
        &[],
        concat!(
            "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)\n",
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'works')\n",
            "SELECT * FROM t\n",
            "SHOW MEMORY_LIMIT\n",
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
