use super::common::*;
use tempfile::TempDir;

#[test]
fn f52_alter_table_add_column() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f52.db"),
        &[],
        "CREATE TABLE sensors (id UUID PRIMARY KEY, name TEXT)\nINSERT INTO sensors (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')\nALTER TABLE sensors ADD COLUMN reading REAL\nSELECT * FROM sensors\n.quit\n",
    );
    assert!(output.status.success());
    let stdout = output_string(&output.stdout);
    assert!(stdout.contains("NULL"));
}

#[test]
fn f53_rename_a_column() {
    let tmp = TempDir::new().expect("tempdir");
    let output = run_cli_script(
        &temp_db_file(&tmp, "f53.db"),
        &[],
        "CREATE TABLE readings (id UUID PRIMARY KEY, temp REAL)\nALTER TABLE readings RENAME COLUMN temp TO temperature\nSELECT temperature FROM readings\n.quit\n",
    );
    assert!(output.status.success());
    assert!(!output_string(&output.stdout).contains("temp"));
}
