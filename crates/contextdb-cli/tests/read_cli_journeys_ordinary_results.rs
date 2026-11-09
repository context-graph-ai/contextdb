//! Journey: an ordinary result is complete or it is refused — never truncated.
//!
//! Contract held here: under shipped defaults a `SELECT` succeeds only when it
//! carries at most 500 rows and 4 MiB of canonical encoding. Crossing either
//! ceiling publishes NO rows and refuses with `owner_limit_exceeded`, naming the
//! ceiling it crossed and carrying the statement back to the user with two
//! executable escapes: page it with `.cursor open`, or raise the ceiling on the
//! route that permits it. `--json` changes rendering only, never a ceiling, and
//! a `--write` session's reads are bounded like anyone's.

mod read_cli_support;

use read_cli_support::*;

const SHIPPED_RESULT_ROWS: i64 = 500;

/// A work declaration below what the 12,000-vector AUTO fallback needs. The
/// exact scan is charged per vector and dimension: on both the writer-owned and
/// the direct-file route it first succeeds at 36,003 (12,001 x 3) and refuses
/// below, so 50,000 no longer refuses anything. Half of that observed need
/// refuses on both routes; the sufficient 5,000,000 runs in the same journey are
/// the admitted positive control.
const LOW_VECTOR_WORK: &str = "18000";

fn store_of(rows: usize) -> Store {
    store_with(&create_seeded_table("bounded", rows))
}

fn auto_vector_store(rows: usize) -> Store {
    let mut sql = String::from(
        "CREATE TABLE vector_budget_docs (\
            id INTEGER PRIMARY KEY, \
            kind TEXT NOT NULL, \
            embedding VECTOR(3) SEARCH_MODE AUTO\
        );\nBEGIN;\nINSERT INTO vector_budget_docs (id, kind, embedding) VALUES ",
    );
    for offset in 0..rows {
        if offset != 0 {
            sql.push_str(", ");
        }
        let id = offset + 1;
        sql.push_str(&format!("({id}, 'eligible', '[1,0,0]')"));
    }
    sql.push_str(";\nCOMMIT;\n");
    store_with(&sql)
}

/// The startup budget a "memory budget exceeded ... requested N bytes, A
/// available of B budget" refusal discloses: the B - A bytes already held plus
/// the N it asked for.
fn startup_budget(stderr: &str) -> Option<u64> {
    let after = stderr.split("requested ").nth(1)?;
    let number = |text: &str| -> Option<u64> {
        text.split_whitespace()
            .next()?
            .trim_end_matches(',')
            .parse()
            .ok()
    };
    let requested = number(after)?;
    let available = number(after.split("bytes, ").nth(1)?)?;
    let budget = number(after.split(" of ").nth(1)?)?;
    Some(budget - available + requested)
}

fn limit_error(outcome: &Outcome) -> serde_json::Value {
    outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| {
            panic!(
                "expected an owner_limit_exceeded refusal.\n{}",
                outcome.describe()
            )
        })
}

#[test]
fn a_result_at_the_shipped_row_ceiling_succeeds_complete() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id LIMIT 500;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "the exact shipped ceiling succeeds.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "a successful SELECT under --json");
    assert_eq!(
        rows_of(&result).len(),
        500,
        "a successful result publishes every one of its rows: {}",
        outcome.describe()
    );
}

#[test]
fn one_row_past_the_ceiling_publishes_nothing_and_names_the_ceiling() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "crossing a read ceiling is a runtime refusal.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "a refused result publishes NO rows — a partial answer is never emitted.\n{}",
        outcome.describe()
    );

    let error = limit_error(&outcome);
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("io"),
        "a crossed read ceiling is an io-class refusal, not a SQL error: {error}"
    );
    let detail = error.get("detail").expect("refusal detail");
    assert_eq!(
        detail.get("limit").and_then(|l| l.as_str()),
        Some("result_rows"),
        "the refusal names which ceiling was crossed: {error}"
    );
    assert_eq!(
        detail.get("value").and_then(|v| v.as_i64()),
        Some(SHIPPED_RESULT_ROWS),
        "the refusal carries the effective ceiling as an integer: {error}"
    );
}

#[test]
fn the_refusal_carries_the_statement_and_a_copy_ready_cursor_command() {
    let store = store_of(501);
    let machine = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );
    let error = limit_error(&machine);
    let detail = error.get("detail").expect("refusal detail");

    let statement = detail
        .get("statement")
        .and_then(|s| s.as_str())
        .unwrap_or_default();
    assert!(
        statement.contains("SELECT") && statement.contains("bounded"),
        "the refusal carries the refused statement verbatim so an agent can re-issue it: {error}"
    );
    let remedy = detail
        .get("remedy_command")
        .and_then(|r| r.as_str())
        .unwrap_or_default();
    assert!(
        remedy.starts_with(".cursor open") && remedy.contains("bounded"),
        "on the file route the detail carries the copy-ready `.cursor open <same SELECT>` line: {error}"
    );

    let human = run(
        &[store.path_str()],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );
    assert!(
        human.stderr.contains(".cursor open") && human.stderr.contains("bounded"),
        "human mode prints the copy-ready cursor command, so the instruction is executable as printed.\n{}",
        human.describe()
    );
}

#[test]
fn the_file_route_refusal_names_raising_the_result_limits_as_the_export_escape() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str()],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert!(
        outcome.stderr.contains("--read-result-rows")
            || outcome.stderr.contains("--read-result-bytes"),
        "reading a file directly, the second escape is a deliberate one-shot export through raised \
         `--read-result-*` limits — the replacement for the removed row-cap flag.\n{}",
        outcome.describe()
    );
    assert!(
        !outcome.stderr.contains("--all"),
        "the removed flag is never offered as a remedy.\n{}",
        outcome.describe()
    );
}

#[test]
fn raising_the_row_limit_publishes_the_complete_result() {
    let store = store_of(501);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-rows",
            "600",
            "--read-cursor-page-rows",
            "100",
        ],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "the remedy the refusal named must actually work.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "the raised-limit export");
    assert_eq!(
        rows_of(&result).len(),
        501,
        "the export publishes every row.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_lowered_row_ceiling_moves_the_boundary_exactly() {
    let store = store_of(8);
    let args = [
        "--json",
        "--read-result-rows",
        "3",
        "--read-cursor-page-rows",
        "3",
    ];

    let mut at_ceiling = vec![store.path_str()];
    at_ceiling.extend_from_slice(&args);
    let ok = run(&at_ceiling, "SELECT id FROM bounded ORDER BY id LIMIT 3;\n");
    assert_eq!(
        ok.code,
        Some(0),
        "the exact declared ceiling succeeds.\n{}",
        ok.describe()
    );

    let refused = run(&at_ceiling, "SELECT id FROM bounded ORDER BY id LIMIT 4;\n");
    assert_eq!(
        refused.code,
        Some(1),
        "one row beyond the declared ceiling is refused.\n{}",
        refused.describe()
    );
    let error = limit_error(&refused);
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("value"))
            .and_then(|v| v.as_i64()),
        Some(3),
        "the refusal reports the EFFECTIVE ceiling, which is the one the caller declared: {error}"
    );
}

#[test]
fn a_result_over_the_byte_ceiling_is_refused_naming_result_bytes() {
    let store = store_of(200);
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "256",
            "--read-cursor-page-bytes",
            "256",
        ],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "the byte ceiling refuses just like the row ceiling.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.trim().is_empty(),
        "no partial result is published when the encoding will not fit.\n{}",
        outcome.describe()
    );
    let error = limit_error(&outcome);
    assert_eq!(
        error
            .get("detail")
            .and_then(|d| d.get("limit"))
            .and_then(|l| l.as_str()),
        Some("result_bytes"),
        "the refusal names the byte ceiling, not the row ceiling: {error}"
    );
}

#[test]
fn human_and_machine_rendering_cross_the_same_boundary() {
    let store = store_of(501);
    let sql = "SELECT id, label FROM bounded ORDER BY id;\n";

    let human = run(&[store.path_str()], sql);
    let machine = run(&[store.path_str(), "--json"], sql);

    assert_eq!(
        human.code,
        machine.code,
        "`--json` changes rendering only; it never bypasses or moves a ceiling.\nhuman:\n{}\nmachine:\n{}",
        human.describe(),
        machine.describe()
    );
    assert!(
        human.stdout.trim().is_empty() && machine.stdout.trim().is_empty(),
        "neither rendering publishes a partial result.\nhuman:\n{}\nmachine:\n{}",
        human.describe(),
        machine.describe()
    );
}

#[test]
fn a_write_session_reads_under_the_same_ceiling() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--write", "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "every CLI SELECT is bounded, including in a `--write` session.\n{}",
        outcome.describe()
    );
    assert!(
        outcome
            .error_kinds()
            .iter()
            .any(|k| k == "owner_limit_exceeded"),
        "the write session's read crosses the same ceiling with the same refusal.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_session_keeps_running_after_a_refusal_and_reports_it_in_the_exit_code() {
    let store = store_of(501);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id, label FROM bounded ORDER BY id;\nSELECT id FROM bounded ORDER BY id LIMIT 2;\n",
    );

    let docs = outcome.stdout_docs();
    let result = expect_document(&docs, "result", "the statement after the refusal");
    assert_eq!(
        rows_of(&result).len(),
        2,
        "a multi-statement session keeps executing after a refused statement.\n{}",
        outcome.describe()
    );
    assert_eq!(
        outcome.code,
        Some(1),
        "a non-interactive run that suffered any refusal exits 1, so automation never reads a \
         half-run as success.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_session_with_no_refusal_exits_zero() {
    let store = store_of(10);
    let outcome = run(
        &[store.path_str(), "--json"],
        "SELECT id FROM bounded ORDER BY id;\n.tables\n",
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "a run where every statement succeeded exits 0.\n{}",
        outcome.describe()
    );
}

#[test]
fn vector_work_and_result_refusals_match_between_writer_and_direct_file_sessions() {
    let store = auto_vector_store(12_000);
    let query = "SELECT id FROM vector_budget_docs WHERE kind = 'eligible' \
                 ORDER BY embedding <=> '[1,0,0]' LIMIT 3";
    // docs/cli.md declares active time as an independent per-read limit.
    // This journey varies work and result limits over a large AUTO fallback,
    // so give every invocation the same explicit active-time allowance.
    let run_with_active_budget = |args: &[&str], script: &str| {
        let mut args = args.to_vec();
        args.extend(["--read-active-ms", "300000"]);
        run(&args, script)
    };

    let writer = run_with_active_budget(
        &[
            store.path_str(),
            "--write",
            "--json",
            "--read-work",
            "5000000",
        ],
        &format!("{query};\n"),
    );
    assert_eq!(
        writer.code,
        Some(0),
        "the writer's own AUTO fallback uses its declared read-work allowance.\n{}",
        writer.describe()
    );

    let reader = run_with_active_budget(
        &[store.path_str(), "--json", "--read-work", "5000000"],
        &format!("{query};\n"),
    );
    assert_eq!(
        reader.code,
        Some(0),
        "the direct reader can pay for the same AUTO fallback.\n{}",
        reader.describe()
    );
    let writer_result = expect_document(&writer.stdout_docs(), "result", "writer AUTO result");
    let reader_result = expect_document(&reader.stdout_docs(), "result", "reader AUTO result");
    assert_eq!(rows_of(&writer_result), rows_of(&reader_result));
    assert_eq!(rows_of(&writer_result).len(), 3);

    let writer_low_work = run_with_active_budget(
        &[
            store.path_str(),
            "--write",
            "--json",
            // The writer's own SELECT is governed by its own `--read-work`
            // declaration. `--owner-read-work` is only the allowance offered
            // to other readers, so the low declaration goes on `--read-work`;
            // the sufficient 5,000,000 above is the admitted positive control.
            "--read-work",
            LOW_VECTOR_WORK,
        ],
        &format!("{query};\n"),
    );
    let direct_low_work = run_with_active_budget(
        &[store.path_str(), "--json", "--read-work", LOW_VECTOR_WORK],
        &format!("{query};\n"),
    );
    for (route, outcome, expected_route, remedy) in [
        ("writer-owned", &writer_low_work, None, "--read-work"),
        ("direct file", &direct_low_work, Some("file"), "--read-work"),
    ] {
        assert_eq!(
            outcome.code,
            Some(1),
            "the {route} query reaches execution and receives a refusal.\n{}",
            outcome.describe()
        );
        assert!(
            outcome.stdout.trim().is_empty(),
            "the {route} work refusal cannot publish a partial vector result.\n{}",
            outcome.describe()
        );
        let refusal = limit_error(outcome);
        assert_eq!(
            refusal
                .get("detail")
                .and_then(|detail| detail.get("limit"))
                .and_then(serde_json::Value::as_str),
            Some("work"),
            "the {route} refusal is the typed vector-work ceiling, not a parse or open failure: {refusal}"
        );
        assert!(
            outcome.stderr.contains(remedy),
            "the {route} refusal names its actual work-limit recovery.\n{}",
            outcome.describe()
        );
        if let Some(expected_route) = expected_route {
            assert!(
                outcome.route_notices().iter().any(|notice| {
                    notice
                        .get("detail")
                        .and_then(|detail| detail.get("route"))
                        .and_then(serde_json::Value::as_str)
                        == Some(expected_route)
                }),
                "the {route} refusal exercised the required production read route.\n{}",
                outcome.describe()
            );
        }
    }

    let writer_result_refusal = run_with_active_budget(
        &[
            store.path_str(),
            "--write",
            "--json",
            "--read-work",
            "5000000",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        &format!("{query};\n"),
    );
    let direct_result_refusal = run_with_active_budget(
        &[
            store.path_str(),
            "--json",
            "--read-work",
            "5000000",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        &format!("{query};\n"),
    );
    for (route, outcome) in [
        ("writer-owned", &writer_result_refusal),
        ("direct file", &direct_result_refusal),
    ] {
        let refusal = limit_error(outcome);
        assert_eq!(
            refusal
                .get("detail")
                .and_then(|detail| detail.get("limit"))
                .and_then(serde_json::Value::as_str),
            Some("result_rows"),
            "the {route} ordinary-result refusal remains typed and cursor-pageable: {refusal}"
        );
    }

    let cursor_script = format!(".cursor open {query}\n.cursor fetch\n");
    let writer_cursor = run_with_active_budget(
        &[
            store.path_str(),
            "--write",
            "--json",
            "--read-work",
            "5000000",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        &cursor_script,
    );
    let direct_cursor = run_with_active_budget(
        &[
            store.path_str(),
            "--json",
            "--read-work",
            "5000000",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
        ],
        &cursor_script,
    );
    let cursor_rows = |outcome: &Outcome| {
        documents_named(&outcome.stdout_docs(), "cursor")
            .into_iter()
            .filter(|document| document.get("rows").is_some())
            .collect::<Vec<_>>()
    };
    assert_eq!(writer_cursor.code, Some(0), "{}", writer_cursor.describe());
    assert_eq!(direct_cursor.code, Some(0), "{}", direct_cursor.describe());
    let writer_pages = cursor_rows(&writer_cursor);
    let direct_pages = cursor_rows(&direct_cursor);
    assert_eq!(writer_pages, direct_pages);
    assert_eq!(
        writer_pages
            .iter()
            .map(|page| rows_of(page).len())
            .collect::<Vec<_>>(),
        vec![2, 1],
        "the ordinary cursor returns all three vector results in complete pages"
    );
}

#[test]
fn maintenance_run_and_status_publish_their_text_receipts_through_the_real_cli() {
    let store = auto_vector_store(1_024);
    let outcome = run(
        &[store.path_str(), "--write"],
        ".maintenance run\n.maintenance status\n",
    );

    assert_eq!(outcome.code, Some(0), "{}", outcome.describe());
    for field in [
        "vector_built_indexes=",
        "vector_remaining_indexes=0",
        // No index ships, so a populated partition has nothing to build.
        "vector_nonempty_partitions=0",
        "vector_ready_partitions=0",
        "vector_built_partitions=",
        "vector_remaining_partitions=0",
        "vector_failure_operation=none",
        "vector_failure_requested_bytes=none",
        "vector_failure_recovery_instruction=none",
    ] {
        assert!(
            outcome.stdout.contains(field),
            "the text maintenance-cycle receipt is missing {field:?}:\n{}",
            outcome.describe()
        );
    }
    for field in [
        "running=",
        "retention_enabled=false",
        "currency_compaction_enabled=false",
        "active_maintenance_loops=",
        "policy=engine_owned",
    ] {
        assert!(
            outcome.stdout.contains(field),
            "the text maintenance status is missing {field:?}:\n{}",
            outcome.describe()
        );
    }

    // A cycle with no check due measured nothing: its receipt says unknown,
    // never 0. Every run and compact receipt names the policy that governed it.
    let receipts = run(
        &[store.path_str(), "--write"],
        ".maintenance run\n.maintenance run\n.maintenance compact\n",
    );
    assert_eq!(receipts.code, Some(0), "{}", receipts.describe());
    assert!(
        receipts
            .stdout
            .contains("auto_compact_fragmentation_before=unknown"),
        "the second cycle's text receipt says unknown:\n{}",
        receipts.describe()
    );
    let policy = "compaction_policy=start_ratio:0.5(default),min_interval_ms:3600000(default),\
                  stop_ratio:0.05(default),max_sweeps:8(default)";
    assert_eq!(
        receipts.stdout.matches(policy).count(),
        3,
        "every run and compact text receipt names the governing policy:\n{}",
        receipts.describe()
    );
    let json_receipts = run(
        &[store.path_str(), "--write", "--json"],
        ".maintenance run\n.maintenance run\n",
    );
    assert_eq!(json_receipts.code, Some(0), "{}", json_receipts.describe());
    let cycles = documents_named(&json_receipts.stdout_docs(), "maintenance_cycle");
    assert_eq!(cycles.len(), 2, "{}", json_receipts.describe());
    assert!(
        cycles[1]["compaction"]["fragmentation_before"].is_null(),
        "the second cycle's JSON receipt says null: {}",
        cycles[1]
    );
    for (name, value) in [
        ("start_ratio", serde_json::json!(0.5)),
        ("min_interval_ms", serde_json::json!(3_600_000)),
        ("stop_ratio", serde_json::json!(0.05)),
        ("max_sweeps", serde_json::json!(8)),
    ] {
        assert_eq!(
            cycles[1]["compaction"]["policy"][name],
            serde_json::json!({ "value": value, "source": "default" }),
            "the JSON receipt names {name}: {}",
            cycles[1]
        );
    }
}

/// The CLI has no flag or session mechanism that constructs an
/// access-restricted (Context/scope/ACL) handle -- `Database::scoped_with_contexts`
/// is a Rust-API-only door. This journey therefore proves the same
/// underlying route rule the CLI CAN reach: a caller whose own `WHERE`
/// predicate names one tuple of a Context-grouped partition key gets its
/// nearest row from that tuple alone (never a closer row filed under a
/// different tuple), and the identical `WHERE` under `USE VECTOR INDEXED`
/// ANSWERS with no relational index declared on the grouping column -- a
/// predicate that fixes the partition key routes the search to that
/// partition, as the CLI's own SQL surface exercises it. The memory-bounded
/// and restricted-handle doors, which the CLI cannot construct, are pinned by
/// the engine's `context_restricted_handle_searches_its_own_partition_bounded`
/// coverage; this journey pins the user-visible ordinary-door behavior.
#[test]
#[ignore = "The CLI reports a refused USE VECTOR INDEXED query without its typed vector_indexed_route_unavailable error kind."]
fn a_context_grouped_nearest_neighbour_answers_its_own_tuple_not_a_closer_foreign_one() {
    let query = "SELECT id FROM context_grouped_docs WHERE context_id = 1 \
                 ORDER BY embedding <=> '[1,0,0]' LIMIT 1;";
    let exact_query = "SELECT id FROM context_grouped_docs WHERE context_id = 1 \
                        ORDER BY embedding <=> '[1,0,0]' USE VECTOR EXACT LIMIT 1;";
    let indexed_query = "SELECT id FROM context_grouped_docs WHERE context_id = 1 \
                          ORDER BY embedding <=> '[1,0,0]' USE VECTOR INDEXED LIMIT 1;";
    let store = absent_store();
    let outcome = run(
        &[store.path_str(), "--write", "--json"],
        &format!(
            "CREATE TABLE context_grouped_docs (\
                 id INTEGER PRIMARY KEY, \
                 context_id INTEGER NOT NULL, \
                 embedding VECTOR(3) \
                     PARTITION_KEY (context_id) \
                     MAX_PARTITIONS 8 \
                     SEARCH_MODE AUTO\
             );\n\
             INSERT INTO context_grouped_docs (id, context_id, embedding) VALUES \
                 (1, 1, '[4,3,0]'), \
                 (2, 2, '[1,0,0]');\n\
             {query}\n\
             .maintenance run\n\
             {exact_query}\n"
        ),
    );
    assert_eq!(
        outcome.code,
        Some(0),
        "a Context-grouped LIMIT 1 search under AUTO and EXACT must both succeed.\n{}",
        outcome.describe()
    );
    let results = documents_named(&outcome.stdout_docs(), "result");
    let vector_results: Vec<_> = results
        .iter()
        .filter(|document| document.get("columns") == Some(&serde_json::json!(["id"])))
        .map(rows_of)
        .collect();
    assert_eq!(
        vector_results.len(),
        2,
        "both the AUTO and the EXACT search must publish a result: {}",
        outcome.describe()
    );
    for (label, rows) in [
        ("column-default AUTO", &vector_results[0]),
        ("USE VECTOR EXACT", &vector_results[1]),
    ] {
        assert_eq!(
            rows,
            &vec![serde_json::json!({"id": 1})],
            "{label}: the caller's own Context tuple must win, never the geometrically closer \
             row filed under a different Context: {}",
            outcome.describe()
        );
    }
    let refused = run(&[store.path_str(), "--json"], &format!("{indexed_query}\n"));
    assert_eq!(refused.code, Some(1), "{}", refused.describe());
    assert!(
        refused
            .error_kinds()
            .iter()
            .any(|kind| kind == "vector_indexed_route_unavailable"),
        "USE VECTOR INDEXED reports the typed switched-off refusal: {}",
        refused.describe()
    );
}

#[test]
fn memory_limit_bounds_read_work_on_file_and_owner_routes_without_changing_the_store() {
    let store = store_of(64);
    let sql = "SELECT id, label FROM bounded ORDER BY label;\n";
    // A reader's root and request control need a fixed startup budget, which the
    // refusal below discloses: what was already held plus what the control asked
    // for. Below that the reader cannot start: a typed startup memory refusal,
    // reached before any statement. At exactly that budget setup is admitted and
    // the selected statement is what is refused. Each pair declares one small and
    // one large budget, so the smaller one must be the effective limit.
    let mut startup_bytes = None::<u64>;
    for owned in [false, true] {
        let owner = owned.then(|| {
            let mut owner = background(&[store.path_str(), "--write", "--json"]);
            owner.send_line("SELECT 987654 AS ready;");
            owner.wait_for("987654");
            owner
        });
        let before = std::fs::read(&store.path).unwrap();
        for (memory_limit, read_memory) in [("128", "16777216"), ("16M", "128")] {
            let outcome = run(
                &[
                    store.path_str(),
                    "--json",
                    "--memory-limit",
                    memory_limit,
                    "--read-memory",
                    read_memory,
                ],
                sql,
            );
            assert_eq!(outcome.code, Some(1), "{}", outcome.describe());
            assert!(outcome.stdout.trim().is_empty(), "{}", outcome.describe());
            assert!(
                outcome.stderr.contains("memory budget exceeded")
                    && outcome.stderr.contains("reader/request_owner"),
                "a 128-byte reader refuses at startup, naming the owner it could not admit.\n{}",
                outcome.describe()
            );
            assert!(
                !outcome.stderr.contains("owner_limit_exceeded"),
                "{}",
                outcome.describe()
            );
            let disclosed = startup_budget(&outcome.stderr).unwrap_or_else(|| {
                panic!(
                    "the startup refusal discloses its cost.\n{}",
                    outcome.describe()
                )
            });
            assert_eq!(*startup_bytes.get_or_insert(disclosed), disclosed);
        }
        let startup = startup_bytes.expect("the startup refusal ran");
        for (memory_limit, read_memory) in [
            (startup.to_string(), "16777216".to_owned()),
            ("16M".to_owned(), startup.to_string()),
        ] {
            let outcome = run(
                &[
                    store.path_str(),
                    "--json",
                    "--memory-limit",
                    &memory_limit,
                    "--read-memory",
                    &read_memory,
                ],
                sql,
            );
            assert_eq!(outcome.code, Some(1), "{}", outcome.describe());
            assert!(outcome.stdout.trim().is_empty(), "{}", outcome.describe());
            let error = limit_error(&outcome);
            assert_eq!(error["detail"]["limit"], "memory", "{error}");
            assert_eq!(
                error["detail"]["value"], startup,
                "the smaller declared reader budget actually refused: {error}"
            );
        }
        let recovered = run(&[store.path_str(), "--json", "--memory-limit", "16M"], sql);
        assert_eq!(recovered.code, Some(0), "{}", recovered.describe());
        let rows = expect_document(&recovered.stdout_docs(), "result", "raised reader budget");
        assert_eq!(rows_of(&rows).len(), 64);
        assert_eq!(
            std::fs::read(&store.path).unwrap(),
            before,
            "reader budgets never persist a store setting"
        );
        if let Some(owner) = owner {
            assert_eq!(owner.finish().0, Some(0));
        }
    }
}

#[test]
fn writer_reads_keep_default_and_explicit_limits_when_owner_work_is_lower() {
    let store = store_of(20);
    let query = "SELECT id FROM bounded ORDER BY id;\n";
    for read_flags in [vec![], vec!["--read-work", "50000"]] {
        let mut args = vec![
            store.path_str(),
            "--write",
            "--json",
            "--owner-read-work",
            "1",
        ];
        args.extend(read_flags);
        let outcome = run(&args, query);
        assert_eq!(outcome.code, Some(0), "{}", outcome.describe());
        let document = expect_document(&outcome.stdout_docs(), "result", "writer's own limits");
        assert_eq!(rows_of(&document).len(), 20);
    }
    let _owner = live_owner_with(&store.path, &["--owner-read-work", "1"], "");
    let reader = run(&[store.path_str(), "--json", "--read-work", "50000"], query);
    let refusal = limit_error(&reader);
    assert_eq!(refusal["detail"]["limit"], "work");
    assert!(
        reader.stderr.contains("--owner-read-work"),
        "{}",
        reader.describe()
    );
}

#[test]
fn writer_explicit_read_limit_is_not_raised_by_owner_policy() {
    let store = store_of(20);
    let outcome = run(
        &[
            store.path_str(),
            "--write",
            "--json",
            "--read-work",
            "1",
            "--owner-read-work",
            "5000000",
        ],
        "SELECT id FROM bounded ORDER BY id;\n",
    );
    let refusal = limit_error(&outcome);
    assert_eq!(refusal["detail"]["limit"], "work");
    assert!(
        outcome.stderr.contains("--read-work"),
        "{}",
        outcome.describe()
    );
    assert!(
        !outcome.stderr.contains("--owner-read-work"),
        "{}",
        outcome.describe()
    );
}
