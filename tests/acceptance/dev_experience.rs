use super::common::*;
use std::fs;
use std::process::Command;

/// I copy-pasted every SQL example from the README into the CLI, and they all ran without errors.
#[test]
fn f37_readme_quick_start_actually_works() {
    let readme = fs::read_to_string(workspace_root().join("README.md")).expect("read README");
    let sql_blocks: Vec<String> = readme
        .split("```sql")
        .skip(1)
        .filter_map(|block| block.split("```").next())
        .map(|block| {
            block
                .lines()
                .filter_map(|line| line.trim().strip_prefix("contextdb> "))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect();
    assert!(
        !sql_blocks.is_empty(),
        "README should contain SQL quick-start blocks"
    );
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let db_path = temp_db_file(&tmp, "f37.db");
    for block in sql_blocks {
        let output = run_cli_script(&db_path, &[], &(block + "\n.quit\n"));
        assert!(
            output.status.success(),
            "README example should execute successfully: {}",
            output_string(&output.stderr)
        );
    }
}

/// I ran --help on both the CLI and server binaries, and each one listed all the flags I need (tenant-id, nats-url, db-path).
#[test]
fn f38_help_on_both_binaries_explains_all_options_clearly() {
    ensure_release_binaries();
    let cli = Command::new(cli_bin())
        .arg("--help")
        .output()
        .expect("CLI --help");
    let server = Command::new(server_bin())
        .arg("--help")
        .output()
        .expect("server --help");
    let cli_stdout = output_string(&cli.stdout);
    let server_stdout = output_string(&server.stdout);
    assert!(cli.status.success());
    assert!(server.status.success());
    assert!(cli_stdout.contains("--tenant-id"));
    assert!(cli_stdout.contains("--nats-url"));
    assert!(server_stdout.contains("--db-path"));
    assert!(server_stdout.contains("--tenant-id"));
    assert!(server_stdout.contains("--nats-url"));
}

/// I typed a misspelled SQL keyword, and the error said "parse error" or "syntax" — not a panic backtrace.
#[test]
fn f39_error_messages_on_bad_sql_are_actionable() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f39.db"),
        &[],
        "SELET * FROM t\n.quit\n",
    );
    let stderr = output_string(&output.stderr).to_lowercase();
    assert!(!output.status.success());
    assert!(stderr.contains("parse error") || stderr.contains("syntax"));
    assert!(!stderr.contains("panicked"));
}
