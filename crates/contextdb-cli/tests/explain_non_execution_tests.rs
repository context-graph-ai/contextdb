//! `.explain <sql>` keeps three paths distinct in every output mode. Ordinary
//! relational SELECT/WITH runs through the bounded reader and reports its
//! real trace. A vector-similarity SELECT is passive so explanation cannot
//! fault its search state into memory. Writes are planned and never applied:
//! DELETE must not delete rows, UPDATE must not mutate them, and INSERT must
//! not insert one.

use std::io::Write;
use std::process::{Command, Stdio};

fn run_cli(extra_args: &[&str], stdin_sql: &str) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_contextdb"));
    cmd.arg(":memory:");
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

fn stdout_docs(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect()
}

// (a) `.explain DELETE FROM t` under --json must not delete anything.
#[test]
fn explain_json_delete_leaves_rows_intact() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a');\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b');\n\
               .explain DELETE FROM t\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    // The follow-up SELECT publishes one namespaced result document; the bare
    // row array it used to publish is gone with no deprecation layer.
    let rows = docs
        .last()
        .and_then(|d| d.get("result")?.get("rows")?.as_array().cloned())
        .unwrap_or_else(|| {
            panic!("the follow-up SELECT must emit a result document, got:\n{stdout}")
        });
    assert_eq!(
        rows.len(),
        2,
        "`.explain DELETE FROM t` under --json must NOT delete the rows — the \
         JSON branch must never execute the statement to get a trace. Rows after explain:\n{rows:?}"
    );
}

// (b) `.explain UPDATE ...` under --json must not mutate anything.
#[test]
fn explain_json_update_leaves_rows_intact() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'original');\n\
               .explain UPDATE t SET name = 'mutated' WHERE id = '00000000-0000-0000-0000-000000000001'\n\
               SELECT name FROM t WHERE id = '00000000-0000-0000-0000-000000000001';\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    // The follow-up SELECT publishes one namespaced result document; the bare
    // row array it used to publish is gone with no deprecation layer.
    let rows = docs
        .last()
        .and_then(|d| d.get("result")?.get("rows")?.as_array().cloned())
        .unwrap_or_else(|| {
            panic!("the follow-up SELECT must emit a result document, got:\n{stdout}")
        });
    assert_eq!(
        rows.len(),
        1,
        "the row must still exist, got:\n{rows:?}\nfull stdout:\n{stdout}"
    );
    assert_eq!(
        rows[0]["name"],
        serde_json::json!("original"),
        "`.explain UPDATE ...` under --json must NOT change the row, got:\n{rows:?}"
    );
}

// (c) `.explain INSERT ...` under --json must not add a row. Cheapest
// possible observation of non-execution: presence, not content, changes.
#[test]
fn explain_json_insert_does_not_add_a_row() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               .explain INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000009', 'ghost')\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let docs = stdout_docs(&stdout);
    assert!(
        docs.iter().any(|d| d.get("explain").is_some()),
        "an explain document must be emitted, got docs:\n{docs:?}"
    );
    // The follow-up SELECT publishes one namespaced result document; the bare
    // row array it used to publish is gone with no deprecation layer.
    let rows = docs
        .last()
        .and_then(|d| d.get("result")?.get("rows")?.as_array().cloned())
        .unwrap_or_else(|| {
            panic!("the follow-up SELECT must emit a result document, got:\n{stdout}")
        });
    assert_eq!(
        rows.len(),
        0,
        "`.explain INSERT ...` under --json must NOT add a row, got:\n{rows:?}"
    );
}

// (d) Ordinary relational explain executes and publishes the trace it really
// observed, including the statement-local rows-examined count.
#[test]
fn explain_json_select_emits_runtime_trace() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, marker TEXT);\n\
               CREATE INDEX idx_marker ON t (marker);\n\
               INSERT INTO t (id, marker) VALUES ('00000000-0000-0000-0000-000000000001', '<=>');\n\
               .explain SELECT id FROM t WHERE marker = '<=>'\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = stdout_docs(&stdout);
    let explain = docs
        .iter()
        .find(|d| d.get("explain").is_some())
        .unwrap_or_else(|| {
            panic!(".explain SELECT under --json must emit an explain document, got:\n{stdout}")
        });
    assert!(
        explain["explain"]["physical_plan"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "explain.physical_plan must be a non-empty string, got: {explain}"
    );
    assert_eq!(explain["explain"]["runtime_trace"], true);
    assert_eq!(explain["explain"]["index_used"], "idx_marker");
    assert_eq!(
        explain["explain"]["predicates_pushed"],
        serde_json::json!(["marker"])
    );
    assert_eq!(explain["explain"]["rows_examined"], 1);
}

// (e) Human mode keeps the same non-execution guarantee for writes.
#[test]
fn explain_human_mode_still_does_not_execute() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a');\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'b');\n\
               .explain DELETE FROM t\n\
               SELECT id FROM t;\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    // Each id appears twice if the row survives: once in the scripted INSERT
    // echo, once in the final SELECT's rendered table row. Once (the echo
    // only) would mean the row was actually deleted.
    for id in [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    ] {
        assert_eq!(
            stdout.matches(id).count(),
            2,
            "human-mode `.explain DELETE FROM t` must leave {id} intact (echo + SELECT row), got stdout:\n{stdout}"
        );
    }
}

#[test]
fn vector_select_and_with_explain_publish_passive_route_facts_even_when_indexed_is_unavailable() {
    let setup = "CREATE TABLE vector_docs (\
                     id INTEGER PRIMARY KEY, \
                     embedding VECTOR(3) SEARCH_MODE INDEXED\
                 );\n\
                 INSERT INTO vector_docs (id, embedding) VALUES (1, '[1,0,0]');\n";
    let select = ".explain SELECT id FROM vector_docs \
                  ORDER BY embedding <=> '[1,0,0]' USE VECTOR INDEXED LIMIT 1\n";
    let with = ".explain WITH chosen AS (SELECT id, embedding FROM vector_docs) \
                SELECT id FROM chosen ORDER BY embedding <=> '[1,0,0]' \
                USE VECTOR INDEXED LIMIT 1\n";
    let (code, stdout, stderr) = run_cli(&["--json"], &format!("{setup}{select}{with}"));
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let explained = stdout_docs(&stdout)
        .into_iter()
        .filter_map(|document| document.get("explain").cloned())
        .collect::<Vec<_>>();
    assert_eq!(explained.len(), 2, "both passive explanations are emitted");
    for body in &explained {
        assert_eq!(body["runtime_trace"], serde_json::json!(false));
        let vector = &body["vector_search"];
        assert_eq!(vector["requested_mode"], "INDEXED");
        assert_eq!(vector["resolved_mode"], "INDEXED");
        assert_eq!(vector["aggregate_allowed_vectors"], 1);
        assert_eq!(vector["effective_auto_index_at"], 1_000);
        assert_eq!(vector["scope"], "all");
        assert_eq!(vector["merge"], "global");
        assert_eq!(vector["query_source"], "<vector>");
        assert!(vector["refusal"].as_str().is_some());
        assert!(vector["recovery"].as_str().is_some());
        assert_eq!(vector["layers"]["base"], "absent");
        assert_eq!(vector["layers"]["change"], "absent");
        assert_eq!(vector["layers"]["tail"], "present");
        let partitions = vector["partitions"]
            .as_array()
            .expect("effective HNSW policy is structured per redacted partition");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0]["partition"], "<redacted:1>");
        assert!(partitions[0]["hnsw_ef_search"].as_u64().is_some());
        assert!(partitions[0]["ef_search_source"].as_str().is_some());
    }

    let (human_code, human_stdout, human_stderr) = run_cli(&[], &format!("{setup}{select}"));
    assert_eq!(
        human_code,
        Some(0),
        "stdout:\n{human_stdout}\nstderr:\n{human_stderr}"
    );
    for fact in [
        "requested_mode=INDEXED",
        "resolved_mode=INDEXED",
        "aggregate_allowed_vectors=1",
        "route=indexed",
        "refusal=indexed_route_unavailable",
        "recovery=run vector maintenance and retry",
        "tail=present",
        "partition=<redacted:1>",
        "hnsw_ef_search=",
    ] {
        assert!(
            human_stdout.contains(fact),
            "human explain must expose {fact}:\n{human_stdout}"
        );
    }
}

#[test]
fn trace_on_publishes_the_vector_route_that_actually_executed() {
    let sql = "CREATE TABLE vector_docs (\
                   id INTEGER PRIMARY KEY, \
                   embedding VECTOR(3) SEARCH_MODE AUTO\
               );\n\
               INSERT INTO vector_docs (id, embedding) VALUES (1, '[1,0,0]');\n\
               .trace on\n\
               SELECT id FROM vector_docs \
               ORDER BY embedding <=> '[1,0,0]' USE VECTOR AUTO LIMIT 1\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    let traces = stdout_docs(&stderr)
        .into_iter()
        .filter_map(|document| document.get("trace").cloned())
        .collect::<Vec<_>>();
    let trace = traces
        .iter()
        .find(|trace| trace["vector_search"].is_object())
        .unwrap_or_else(|| panic!("the executed vector query must publish its trace:\n{stderr}"));
    let vector = &trace["vector_search"];
    assert_eq!(vector["requested_mode"], "AUTO");
    assert_eq!(vector["resolved_mode"], "EXACT");
    assert_eq!(vector["aggregate_allowed_vectors"], 1);
    assert_eq!(vector["route"], "exact");
    assert_eq!(vector["fallback"], "aggregate_below_auto_index_at");
    assert!(trace["rows_examined"].as_u64().is_some());
}
