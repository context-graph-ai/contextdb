use crate::common::{output_string, run_cli_script};
use tempfile::TempDir;

#[test]
fn cli_happy_path_composite_fk_violation_renders() {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("cfk-cli.db");
    let output = run_cli_script(
        &db_path,
        &[],
        "CREATE TABLE parent (a INTEGER, b INTEGER, UNIQUE(a, b));\n\
         CREATE TABLE child (id INTEGER PRIMARY KEY, c1 INTEGER, c2 INTEGER, FOREIGN KEY (c1, c2) REFERENCES parent(a, b));\n\
         INSERT INTO child (id, c1, c2) VALUES (1, 9, 9);\n\
         .quit\n",
    );

    let stdout = output_string(&output.stdout);
    let stderr = output_string(&output.stderr);
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        combined.contains("child(c1, c2)") && combined.contains("parent(a, b)"),
        "CLI must render tuple-aware FK error without stack trace; got:\n{combined}"
    );
    assert!(
        !combined.to_ascii_lowercase().contains("stack backtrace"),
        "CLI FK error must not print a stack trace; got:\n{combined}"
    );
}
