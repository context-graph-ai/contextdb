//! `.schema`'s `sync_direction` and `conflict_policy` fields under `--json`
//! are a machine wire contract — a consumer parses the string value to
//! decide how a table behaves under sync. That contract must not be coupled
//! to Rust's `Debug` spelling of the underlying enum: renaming a variant, or
//! reordering how it derives `Debug`, would silently change the wire value
//! with no product intent behind the change. The values must instead be an
//! explicit, declared vocabulary that only changes when someone deliberately
//! decides to change it.
//!
//! The chosen vocabulary is the DDL's own words, translated to stable
//! lowercase snake_case (the DDL clause is authoritative — see
//! `SyncDirection::sql` and `ConflictPolicy::declared_clause` in
//! `contextdb-core`): `SYNC OFF` → `sync_off`, `SYNC PUSH ONLY` →
//! `push_only`, `SYNC PULL ONLY` → `pull_only`, `SYNC TWO WAY` → `two_way`;
//! `SYNC CONFLICT KEEP FIRST` → `keep_first`, `SYNC CONFLICT KEEP LATEST` →
//! `keep_latest`. `ServerWins`/`EdgeWins` are not declarable through the DDL
//! surface (they are role-relative policies used only by the engine's own
//! built-in distributed-contract tables), so no table in this file can
//! declare them; a table whose policy is one of those two still needs a wire
//! word — `server_wins` / `edge_wins` — covered separately wherever the
//! mapping is unit-reachable.

use std::io::Write;
use std::process::{Command, Stdio};

fn run_cli(stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg("--json");
    cmd.arg(":memory:");
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

fn schema_document(stdout: &str) -> serde_json::Value {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .find(|d| d.get("table").is_some())
        .unwrap_or_else(|| panic!(".schema under --json must emit a document with a \"table\" key, got stdout:\n{stdout}"))
}

/// Every `sync_direction` DDL clause the engine accepts, and the wire word
/// this contract declares for it.
const SYNC_DIRECTION_CASES: &[(&str, &str)] = &[
    ("SYNC OFF", "sync_off"),
    ("SYNC PUSH ONLY", "push_only"),
    ("SYNC PULL ONLY", "pull_only"),
    ("SYNC TWO WAY", "two_way"),
];

/// Every `conflict_policy` DDL clause an application can declare, and the
/// wire word this contract declares for it. `ServerWins`/`EdgeWins` are
/// deliberately absent — see the file header.
const CONFLICT_POLICY_CASES: &[(&str, &str)] = &[
    ("SYNC CONFLICT KEEP FIRST", "keep_first"),
    ("SYNC CONFLICT KEEP LATEST", "keep_latest"),
];

#[test]
fn json_schema_emits_the_declared_sync_direction_vocabulary() {
    for (clause, expected) in SYNC_DIRECTION_CASES {
        let sql = format!("CREATE TABLE t (id UUID PRIMARY KEY) {clause};\n.schema t\n");
        let (code, stdout, stderr) = run_cli(&sql);
        assert_eq!(
            code,
            Some(0),
            "clause {clause:?}: stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let doc = schema_document(&stdout);
        assert_eq!(
            doc["sync_direction"],
            serde_json::json!(expected),
            "clause {clause:?} must render sync_direction as {expected:?}, the declared wire \
             word — never the Rust Debug spelling of the enum variant. got: {doc}"
        );
    }
}

#[test]
fn json_schema_emits_the_declared_conflict_policy_vocabulary() {
    for (clause, expected) in CONFLICT_POLICY_CASES {
        let sql = format!("CREATE TABLE t (id UUID PRIMARY KEY) {clause};\n.schema t\n");
        let (code, stdout, stderr) = run_cli(&sql);
        assert_eq!(
            code,
            Some(0),
            "clause {clause:?}: stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let doc = schema_document(&stdout);
        assert_eq!(
            doc["conflict_policy"],
            serde_json::json!(expected),
            "clause {clause:?} must render conflict_policy as {expected:?}, the declared wire \
             word — never the Rust Debug spelling of the enum variant. got: {doc}"
        );
    }
}

// Both fields declared on one table together, to prove the fix is not
// accidentally scoped to only one of the two.
#[test]
fn json_schema_emits_both_vocabularies_together() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY) SYNC PULL ONLY SYNC CONFLICT KEEP LATEST;\n.schema t\n";
    let (code, stdout, stderr) = run_cli(sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let doc = schema_document(&stdout);
    assert_eq!(
        doc["sync_direction"],
        serde_json::json!("pull_only"),
        "got: {doc}"
    );
    assert_eq!(
        doc["conflict_policy"],
        serde_json::json!("keep_latest"),
        "got: {doc}"
    );
}
