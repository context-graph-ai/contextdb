//! Tests for `--json` machine-output coverage across every CLI meta-command.
//!
//! `--json` used to be honored by exactly one path (`execute_sql`): a JSON
//! array for a query, `{"rows_affected":N}` for a non-query, errors to
//! stderr as plain text. Every meta-command (`.tables`, `.schema`,
//! `.explain`, `.trace`, `.help`, the whole `.sync` family) printed human
//! text to STDOUT regardless of `--json` — so an agent parsing the pipe got
//! DDL text or a "Trace enabled" line where it expected a JSON document, and
//! `.trace on` corrupted the JSON Lines stream outright (a trace line landed
//! on stdout unconditionally). Tests 16 and 17 below are baseline pins
//! (already-correct behavior); everything else is pinned for the reason
//! stated in its assertion message.

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

fn nonempty_lines(text: &str) -> Vec<&str> {
    text.lines().filter(|l| !l.trim().is_empty()).collect()
}

/// Every non-empty line of `text` must parse as JSON; panics with the
/// offending line otherwise. Returns the parsed documents in order.
fn assert_all_lines_are_json(text: &str, context: &str) -> Vec<serde_json::Value> {
    nonempty_lines(text)
        .into_iter()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line.trim()).unwrap_or_else(|e| {
                panic!("{context}: every line must be JSON, but {line:?} failed: {e}\nfull text:\n{text}")
            })
        })
        .collect()
}

/// The first document among `docs` carrying a top-level `key`.
fn find_doc<'a>(docs: &'a [serde_json::Value], key: &str) -> Option<&'a serde_json::Value> {
    docs.iter().find(|d| d.get(key).is_some())
}

/// The payload of the `.schema` document.
///
/// `.schema` publishes one NAMESPACED document — `{"schema":{…}}` — like every
/// other meta-command; the un-namespaced form, whose fields sat at the top
/// level beside nothing that named them, is gone. Asserting through this
/// helper keeps every schema pin reading the payload rather than the envelope.
fn schema_payload(docs: &[serde_json::Value]) -> Option<serde_json::Value> {
    docs.iter().find_map(|d| d.get("schema").cloned())
}

// 1. `.tables` under --json must emit a `{"tables": [...]}` document,
// never a bare name list (one `println!` per table).
#[test]
fn json_tables_emits_sorted_table_array() {
    let sql = "CREATE TABLE b (id UUID PRIMARY KEY);\n\
               CREATE TABLE a (id UUID PRIMARY KEY);\n\
               CREATE TABLE c (id UUID PRIMARY KEY);\n\
               .tables\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stderr.trim().is_empty(), "got stderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".tables under --json");
    let tables_doc = find_doc(&docs, "tables").unwrap_or_else(|| {
        panic!(
            ".tables under --json must emit a namespaced page document, never a bare \
             name list one per line. stdout:\n{stdout}"
        )
    });
    // A page document, not a bare array: `.tables` is resumable, so it states
    // whether another page exists and carries the continuation that fetches
    // one exactly when there is one.
    assert_eq!(
        tables_doc["tables"]["items"],
        serde_json::json!(["a", "b", "c"])
    );
    assert_eq!(tables_doc["tables"]["has_more"], serde_json::json!(false));
    assert!(
        tables_doc["tables"]["continuation"].is_null(),
        "continuation is null exactly when has_more is false: {tables_doc}"
    );
}

// 2. `.schema` under --json must emit a structured document, never raw
// DDL text. Exercises the declared-policy surface across two
// fixtures (IMMUTABLE + STATE MACHINE cannot combine with DAG in one table —
// an engine constraint, not a test simplification — so DAG is exercised on a
// second, minimal table).
#[test]
fn json_schema_emits_every_declared_policy() {
    let sql = "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT);\n\
               CREATE TABLE widgets (\n\
               \x20 part_id TEXT,\n\
               \x20 rev INTEGER,\n\
               \x20 name TEXT NOT NULL UNIQUE,\n\
               \x20 frozen TEXT IMMUTABLE,\n\
               \x20 embedding VECTOR(384) WITH (quantization = 'SQ8'),\n\
               \x20 intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated,\n\
               \x20 status TEXT NOT NULL,\n\
               \x20 PRIMARY KEY (part_id, rev)\n\
               ) STATE MACHINE (status: active -> [invalidated, superseded])\n\
               \x20 RETAIN 30 DAYS SYNC SAFE\n\
               \x20 PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated\n\
               \x20 PROPAGATE ON STATE invalidated EXCLUDE VECTOR;\n\
               CREATE INDEX idx_widgets_name ON widgets (name);\n\
               .schema widgets\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".schema widgets under --json");
    let schema = schema_payload(&docs).unwrap_or_else(|| {
        panic!(
            ".schema under --json must emit a structured document with a \
             \"table\" key, never raw DDL text. stdout:\n{stdout}"
        )
    });

    assert_eq!(schema["table"], serde_json::json!("widgets"));
    assert_eq!(schema["primary_key"], serde_json::json!(["part_id", "rev"]));
    assert_eq!(schema["retain"]["unit"], serde_json::json!("DAYS"));
    assert_eq!(schema["retain"]["window"], serde_json::json!(30));
    assert_eq!(schema["retain"]["sync_safe"], serde_json::json!(true));
    assert_eq!(
        schema["state_machine"]["column"],
        serde_json::json!("status")
    );

    let columns = schema["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("schema.columns must be an array, got: {schema}"));
    let name_col = columns
        .iter()
        .find(|c| c["name"] == serde_json::json!("name"))
        .expect("name column must be present");
    assert_eq!(name_col["nullable"], serde_json::json!(false));
    assert_eq!(name_col["unique"], serde_json::json!(true));
    let frozen_col = columns
        .iter()
        .find(|c| c["name"] == serde_json::json!("frozen"))
        .expect("frozen column must be present");
    assert_eq!(frozen_col["immutable"], serde_json::json!(true));
    let intention_col = columns
        .iter()
        .find(|c| c["name"] == serde_json::json!("intention_id"))
        .expect("intention_id column must be present");
    assert_eq!(
        intention_col["references"]["table"],
        serde_json::json!("intentions")
    );
    assert_eq!(
        intention_col["references"]["propagate"]["on_state"],
        serde_json::json!("archived")
    );

    let indexes = schema["indexes"]
        .as_array()
        .unwrap_or_else(|| panic!("schema.indexes must be an array, got: {schema}"));
    assert!(
        indexes
            .iter()
            .any(|i| i["name"] == serde_json::json!("idx_widgets_name")),
        "the user CREATE INDEX must be listed, got: {indexes:?}"
    );

    let propagate = schema["propagate"]
        .as_array()
        .unwrap_or_else(|| panic!("schema.propagate must be an array, got: {schema}"));
    let kinds: std::collections::BTreeSet<&str> = propagate
        .iter()
        .filter_map(|p| p["kind"].as_str())
        .collect();
    assert!(
        kinds.contains("edge")
            && kinds.contains("vector_exclusion")
            && kinds.contains("foreign_key"),
        "propagate must carry all three PropagationRule kinds (edge, \
         vector_exclusion, foreign_key), got: {propagate:?}"
    );

    assert!(
        schema["ddl"]
            .as_str()
            .is_some_and(|s| s.contains("CREATE TABLE widgets")),
        "ddl must carry the rendered DDL, got: {schema}"
    );

    // dag_edge_types, exercised separately (see the doc comment above).
    let (code2, stdout2, stderr2) = run_cli(
        &["--json"],
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES');\n.schema edges\n",
    );
    assert_eq!(code2, Some(0), "stdout:\n{stdout2}\nstderr:\n{stderr2}");
    let docs2 = assert_all_lines_are_json(&stdout2, ".schema edges under --json");
    let edges_schema = schema_payload(&docs2).unwrap_or_else(|| {
        panic!(".schema edges under --json must emit a structured document. stdout:\n{stdout2}")
    });
    assert_eq!(
        edges_schema["dag_edge_types"],
        serde_json::json!(["CITES"]),
        "dag_edge_types must reflect the declared DAG('CITES'), got: {edges_schema}"
    );
}

// 3. Single-source guard against DDL drift: `.schema`'s `ddl` field
// must equal the human-mode `.schema` stdout exactly.
#[test]
fn json_schema_ddl_field_equals_human_schema_output() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n.schema t\n";
    let (human_code, human_stdout, human_stderr) = run_cli(&[], sql);
    assert_eq!(human_code, Some(0), "stderr:\n{human_stderr}");

    let (json_code, json_stdout, json_stderr) = run_cli(&["--json"], sql);
    assert_eq!(json_code, Some(0), "stderr:\n{json_stderr}");
    let docs = assert_all_lines_are_json(&json_stdout, ".schema t under --json");
    let schema = schema_payload(&docs).unwrap_or_else(|| {
        panic!(".schema under --json must emit a structured document. stdout:\n{json_stdout}")
    });
    let ddl = schema["ddl"]
        .as_str()
        .unwrap_or_else(|| panic!("schema.ddl must be a string, got: {schema}"));
    // The human-mode session also carries the CREATE TABLE's own status line
    // ("ok (rows_affected=0)") ahead of the `.schema` output; strip it so the
    // comparison is against the `.schema` command's own rendering only — no
    // correct implementation folds a session status line into the schema
    // document. The fixture above is exactly one CREATE followed by one
    // `.schema`, so splitting off the first line isolates the DDL cleanly.
    let human_schema_output = human_stdout
        .split_once('\n')
        .map(|(_, rest)| rest)
        .unwrap_or(human_stdout.as_str());
    assert_eq!(
        ddl, human_schema_output,
        "the JSON ddl field must equal the human-mode .schema rendering exactly \
         (single-source guard against drift)"
    );
}

// 4. A bare table declares no policy: undeclared keys must be absent
// (or empty arrays for list-shaped fields); table/columns/primary_key/indexes/ddl
// must always be present.
#[test]
fn json_schema_omits_undeclared_policy_keys() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY);\n.schema t\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".schema t under --json");
    let schema = schema_payload(&docs).unwrap_or_else(|| {
        panic!(".schema under --json must emit a structured document. stdout:\n{stdout}")
    });

    for always_present in ["table", "columns", "primary_key", "indexes", "ddl"] {
        assert!(
            schema.get(always_present).is_some(),
            "{always_present} must always be present, got: {schema}"
        );
    }
    for undeclared in [
        "retain",
        "state_machine",
        "sync_direction",
        "conflict_policy",
        "propagate",
        "dag_edge_types",
    ] {
        let value = schema.get(undeclared);
        let omitted_or_empty = match value {
            None => true,
            Some(v) => v.is_null() || v.as_array().is_some_and(|a| a.is_empty()),
        };
        assert!(
            omitted_or_empty,
            "{undeclared} must be absent or an empty array when undeclared, got: {schema}"
        );
    }
}

// 5. An auto-created PK index must not appear in `indexes`, matching
// human `.schema` (`cli_render.rs` suppresses IndexKind::Auto there too).
#[test]
fn json_schema_suppresses_auto_indexes() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY);\n.schema t\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".schema t under --json");
    let schema = schema_payload(&docs).unwrap_or_else(|| {
        panic!(".schema under --json must emit a structured document. stdout:\n{stdout}")
    });
    let indexes = schema["indexes"].as_array().unwrap_or_else(|| {
        panic!("schema.indexes must be an array (possibly empty), got: {schema}")
    });
    assert!(
        !indexes
            .iter()
            .any(|i| i["name"].as_str().is_some_and(|n| n.contains("id"))
                || i["kind"] == serde_json::json!("auto")),
        "the PK's auto-created index must not be listed, got: {indexes:?}"
    );
}

// 6. `.explain` under --json must emit a JSON document, never the human
// plan text (`cli_render::render_explain`).
#[test]
fn json_explain_emits_plan_object() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER);\n\
               CREATE INDEX idx_a ON t (a);\n\
               INSERT INTO t (id, a) VALUES ('00000000-0000-0000-0000-000000000001', 7);\n\
               .explain SELECT id FROM t WHERE a = 7\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".explain under --json");
    let explain = find_doc(&docs, "explain").unwrap_or_else(|| {
        panic!(
            ".explain under --json must emit an {{\"explain\": {{...}}}} document, \
             never the human plan text. stdout:\n{stdout}"
        )
    });
    assert!(
        explain["explain"]["physical_plan"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "explain.physical_plan must be a non-empty string, got: {explain}"
    );
    assert!(
        explain["explain"]["predicates_pushed"].is_array(),
        "explain.predicates_pushed must be an array, got: {explain}"
    );
    assert!(
        explain["explain"]["indexes_considered"].is_array(),
        "explain.indexes_considered must be an array, got: {explain}"
    );
}

// 7. Per-statement trace lines must never land on stdout unconditionally
// (repl.rs must not print them regardless of --json) — that would corrupt
// the JSON Lines stream.
#[test]
fn json_trace_lines_never_reach_stdout() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER);\n\
               CREATE INDEX idx_a ON t (a);\n\
               INSERT INTO t (id, a) VALUES ('00000000-0000-0000-0000-000000000001', 7);\n\
               .trace on\n\
               SELECT id FROM t WHERE a = 7;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    for line in nonempty_lines(&stdout) {
        assert!(
            !line.contains("trace: "),
            "no human trace text may reach stdout under --json, got line: {line:?}\nfull stdout:\n{stdout}"
        );
        serde_json::from_str::<serde_json::Value>(line.trim()).unwrap_or_else(|e| {
            panic!("every stdout line must be JSON under --json, but {line:?} failed: {e}\nfull stdout:\n{stdout}")
        });
    }

    let stderr_docs: Vec<serde_json::Value> = nonempty_lines(&stderr)
        .into_iter()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect();
    let trace_doc = stderr_docs.iter().find(|d| d.get("trace").is_some());
    assert!(
        trace_doc.is_some_and(|d| d["trace"].get("rows_examined").is_some()),
        "the per-statement trace must be emitted on stderr as a \
         {{\"trace\": {{..., \"rows_examined\": N}}}} document under --json. \
         stderr:\n{stderr}"
    );
}

// 8. `.trace on` under --json must emit a `{"trace":"on"}` result
// document, never the human "Trace enabled" text.
#[test]
fn json_trace_toggle_emits_a_result_document() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".trace on\n");
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".trace on under --json");
    assert!(
        docs.iter().any(|d| d["trace"] == serde_json::json!("on")),
        "`.trace on` under --json must emit {{\"trace\":\"on\"}}, never the \
         human \"Trace enabled\" text. stdout:\n{stdout}"
    );
}

// 9. `.help` under --json must never print the full human help text to
// STDOUT unconditionally.
#[test]
fn json_help_keeps_stdout_pure() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".help\n.help vector\n");
    assert_eq!(code, Some(0), "stderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must stay empty under --json; help text must move to stderr. \
         stdout:\n{stdout}"
    );
    assert!(
        stderr.contains(".tables"),
        "the help text (on stderr) must still be there, got stderr:\n{stderr}"
    );
}

// 10. `.sync status` under --json must emit
// `{"sync":{"configured":false}}`, never the plain "Sync not
// configured..." text.
#[test]
fn json_sync_status_without_configuration() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".sync status\n");
    assert_eq!(
        code,
        Some(0),
        "a sync QUERY with no config must still exit 0. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let docs = assert_all_lines_are_json(&stdout, ".sync status under --json");
    assert!(
        docs.iter()
            .any(|d| d["sync"]["configured"] == serde_json::json!(false)),
        "must emit {{\"sync\":{{\"configured\":false}}}}, never plain \
         text. stdout:\n{stdout}"
    );
}

// 11. `.sync auto` under --json holds the same contract as test 10.
#[test]
fn json_sync_auto_without_configuration() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".sync auto\n");
    assert_eq!(
        code,
        Some(0),
        "a sync QUERY with no config must still exit 0. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let docs = assert_all_lines_are_json(&stdout, ".sync auto under --json");
    assert!(
        docs.iter()
            .any(|d| d["sync_auto"]["configured"] == serde_json::json!(false)),
        "must emit {{\"sync_auto\":{{\"configured\":false,...}}}}, never \
         plain text. stdout:\n{stdout}"
    );
}

// 12. `.sync push` under --json with no config must put the error
// envelope on stderr and fail the run — never land the message on STDOUT
// with `ok:true` (exit 0).
#[test]
fn json_sync_action_without_configuration_emits_error_on_stderr() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".sync push\n");
    assert_eq!(
        code,
        Some(1),
        "a sync ACTION with no config must fail the run under --json too. \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty. stdout:\n{stdout}"
    );
    let stderr_docs: Vec<serde_json::Value> = nonempty_lines(&stderr)
        .into_iter()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect();
    assert!(
        stderr_docs
            .iter()
            .any(|d| d["error"]["class"] == serde_json::json!("usage")),
        "stderr must carry a {{\"error\":{{\"class\":\"usage\",...}}}} \
         envelope, never plain text with no envelope. stderr:\n{stderr}"
    );
}

// 13. A SQL parse error under --json must be the `{"error":{...}}`
// envelope, never a plain `eprintln!`'d text message.
#[test]
fn json_sql_error_emits_error_envelope_on_stderr() {
    let (code, stdout, stderr) = run_cli(&["--json"], "SELET * FROM t;\n");
    assert_eq!(code, Some(1), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.trim().is_empty(),
        "stdout must be empty, got:\n{stdout}"
    );
    let stderr_docs: Vec<serde_json::Value> = nonempty_lines(&stderr)
        .into_iter()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect();
    let error_doc = stderr_docs.iter().find(|d| d.get("error").is_some());
    let error_doc = error_doc.unwrap_or_else(|| {
        panic!(
            "stderr must carry a {{\"error\":{{...}}}} JSON envelope, never \
             plain text \"Error: ...\". stderr:\n{stderr}"
        )
    });
    assert_eq!(error_doc["error"]["class"], serde_json::json!("sql"));
    let message = error_doc["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("error.message must be a string, got: {error_doc}"));
    assert!(!message.is_empty());
    assert!(
        !message.starts_with("line "),
        "the line prefix must not be folded into the message under --json, got: {message:?}"
    );
    assert!(
        error_doc["error"]["line"].is_i64() || error_doc["error"]["line"].is_u64(),
        "error.line must be an integer, got: {error_doc}"
    );
}

// 14. `.bogus` under --json must never print plain text to stdout and
// exit 0.
#[test]
fn json_unknown_meta_command_emits_usage_envelope() {
    let (code, stdout, stderr) = run_cli(&["--json"], ".bogus\n");
    assert_eq!(
        code,
        Some(1),
        "must fail the run under --json too. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.trim().is_empty(), "got stdout:\n{stdout}");
    let stderr_docs: Vec<serde_json::Value> = nonempty_lines(&stderr)
        .into_iter()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .collect();
    assert!(
        stderr_docs
            .iter()
            .any(|d| d["error"]["class"] == serde_json::json!("usage")),
        "stderr must carry a {{\"error\":{{\"class\":\"usage\",...}}}} \
         envelope, never plain text on stdout. stderr:\n{stderr}"
    );
}

// 15. A mixed session must be pure JSON Lines on stdout end to end.
#[test]
fn json_mixed_session_stdout_is_pure_json_lines() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok');\n\
               SELECT * FROM t;\n\
               .tables\n\
               .schema t\n\
               .explain SELECT * FROM t\n\
               .help\n\
               SELET bad syntax;\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(
        code,
        Some(1),
        "the bad statement must fail the run. stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let docs = assert_all_lines_are_json(&stdout, "mixed session under --json");
    // rows_affected (CREATE) + rows_affected (INSERT) + array (SELECT) +
    // tables + schema + explain — .help produces nothing on stdout.
    assert_eq!(
        docs.len(),
        6,
        "expected exactly 6 stdout documents (create, insert, select, tables, \
         schema, explain — .help emits nothing on stdout), got {}: {docs:?}",
        docs.len()
    );
    // Probe for one of the known document keys directly (`Value::get`, order
    // independent) rather than reading `keys().next()`: serde_json's default
    // `Map` is a `BTreeMap`, so its keys iterate ALPHABETICALLY, not by
    // insertion order. The `.schema` document's sibling keys (columns/ddl/
    // indexes/primary_key) all sort before "table" — `keys().next()` would
    // return "columns" there, which is a real defect in this probe, not a
    // property of a correct implementation. `"tables"` is checked before
    // `"table"` so the two don't collide (neither document carries both).
    // Every document on stdout names its own payload — there is no bare array
    // and no bare field set left in this stream.
    const KNOWN_KEYS: &[&str] = &["rows_affected", "result", "tables", "schema", "explain"];
    let top_level_keys: Vec<&str> = docs
        .iter()
        .map(|d| {
            d.as_object()
                .and_then(|o| KNOWN_KEYS.iter().find(|k| o.contains_key(**k)))
                .copied()
                .unwrap_or("<non-object>")
        })
        .collect();
    assert_eq!(
        top_level_keys,
        vec![
            "rows_affected",
            "rows_affected",
            "result",
            "tables",
            "schema",
            "explain",
        ]
    );
    let stderr_docs: Vec<serde_json::Value> = nonempty_lines(&stderr)
        .into_iter()
        .map(|l| {
            serde_json::from_str(l.trim()).unwrap_or_else(|e| {
                panic!("every stderr line must be JSON, {l:?}: {e}\nfull stderr:\n{stderr}")
            })
        })
        .collect();
    assert!(
        stderr_docs
            .iter()
            .any(|d| d["error"]["class"] == serde_json::json!("sql")),
        "stderr must carry the bad statement's error envelope, got:\n{stderr}"
    );
}

// 16. Baseline pin — the scripted INSERT echo is suppressed under
// --json (repl.rs:411-413's condition checks `output.json`).
#[test]
fn insert_echo_stays_suppressed_under_json() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ok');\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        !stdout.to_uppercase().contains("INSERT INTO"),
        "no raw INSERT line may reach stdout under --json, got:\n{stdout}"
    );
}

// 17. Baseline pin — human mode (no --json) is untouched: `.tables` prints
// bare names, `.schema` prints DDL, `.explain` prints plan text, `.help`
// prints to stdout.
#[test]
fn human_mode_meta_output_is_unchanged() {
    let sql = "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT);\n\
               .tables\n\
               .schema t\n\
               .explain SELECT * FROM t\n\
               .help\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.lines().any(|l| l.trim() == "t"),
        "got stdout:\n{stdout}"
    );
    assert!(stdout.contains("CREATE TABLE t"), "got stdout:\n{stdout}");
    assert!(
        stdout.to_lowercase().contains("scan") || stdout.contains("plan"),
        "got stdout:\n{stdout}"
    );
    assert!(
        stdout.contains(".tables"),
        "help text must be on stdout, got stdout:\n{stdout}"
    );
    assert!(
        !stdout.trim_start().starts_with('{') && !stdout.trim_start().starts_with('['),
        "human mode must never emit a JSON document, got stdout:\n{stdout}"
    );
}

// Access control on the readback surfaces. `docs/cli.md` promises `.schema`
// "reflects the table's full *enforced* policy" and that "its printed DDL
// re-parses to a table with the same policy," listing only column `DEFAULT`
// clauses and `STATE MACHINE` from-state ordering as literal exceptions. A
// column declared `ACL REFERENCES acl_grants(acl_id)` is reported on neither
// surface: the human DDL prints a bare `acl_id UUID` and the `--json` column
// carries no reference at all, so an operator replaying the printed DDL
// rebuilds the table with row-level authorization removed and no warning.
//
// The `--json` shape mirrors the foreign-key one already published:
// `acl_references: {"table": …, "column": …}` beside `references`.
#[test]
fn json_schema_reports_a_declared_acl_reference_on_both_surfaces() {
    let sql = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID);\n\
               CREATE TABLE notes (id INTEGER PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT);\n\
               .schema notes\n";
    let (code, stdout, stderr) = run_cli(&["--json"], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");
    let docs = assert_all_lines_are_json(&stdout, ".schema notes under --json");
    let schema = schema_payload(&docs).unwrap_or_else(|| {
        panic!(".schema must publish its namespaced document. stdout:\n{stdout}")
    });

    let columns = schema["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("schema.columns must be an array, got: {schema}"));
    let acl_col = columns
        .iter()
        .find(|c| c["name"] == serde_json::json!("acl_id"))
        .expect("acl_id column must be present");
    assert_eq!(
        acl_col["acl_references"]["table"],
        serde_json::json!("acl_grants"),
        "the --json schema answer must name the table an access-controlled column grants \
         against; today the column carries no reference at all, so an agent testing for the \
         presence of a policy concludes the table declares none. Column: {acl_col}"
    );
    assert_eq!(
        acl_col["acl_references"]["column"],
        serde_json::json!("acl_id"),
        "the --json schema answer must name the grant column as well. Column: {acl_col}"
    );

    let ddl = schema["ddl"]
        .as_str()
        .unwrap_or_else(|| panic!("schema.ddl must be a string, got: {schema}"));
    assert!(
        ddl.contains("ACL REFERENCES acl_grants(acl_id)"),
        "the published DDL must carry the ACL clause -- docs/cli.md promises the printed DDL \
         re-parses to a table with the same policy, and names only DEFAULT and STATE MACHINE \
         ordering as exceptions. DDL:\n{ddl}"
    );
}

#[test]
fn human_schema_prints_the_acl_clause_and_leaves_a_plain_foreign_key_alone() {
    let sql = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID);\n\
               CREATE TABLE parent (id UUID PRIMARY KEY);\n\
               CREATE TABLE notes (id INTEGER PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT);\n\
               CREATE TABLE child (id UUID PRIMARY KEY, p UUID REFERENCES parent(id));\n\
               .schema child\n\
               .schema notes\n";
    let (code, stdout, stderr) = run_cli(&[], sql);
    assert_eq!(code, Some(0), "stdout:\n{stdout}\nstderr:\n{stderr}");

    assert!(
        stdout.contains("p UUID REFERENCES parent(id)"),
        "control: an ordinary foreign key already prints its REFERENCES clause and must keep \
         doing so. stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("ACL REFERENCES acl_grants(acl_id)"),
        "`.schema notes` prints `acl_id UUID` today, dropping the access control the table \
         enforces; an operator who snapshots and replays that DDL rebuilds the table without \
         row-level authorization. stdout:\n{stdout}"
    );
}
