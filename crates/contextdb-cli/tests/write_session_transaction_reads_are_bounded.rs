//! Journey: a `--write` session's own reads stay bounded WHILE they also see
//! its own open transaction.
//!
//! Contract held here (cli.md, "the same bounds apply to a `--write`
//! session's SELECTs"): a writer's reads are bounded exactly like anyone
//! else's, including while a transaction it opened itself is still open —
//! read-your-writes and the ceiling are not in tension. A refusal names the
//! CEILING that was crossed, never the row count the statement actually
//! produced; after `ROLLBACK` the same session's SELECT sees nothing.

mod read_cli_support;

use read_cli_support::*;

fn owner_limit_error(outcome: &Outcome) -> serde_json::Value {
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

fn result_row_ids(document: &serde_json::Value) -> Vec<i64> {
    rows_of(document)
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_i64()))
        .collect()
}

#[test]
fn a_write_sessions_select_inside_its_own_open_transaction_refuses_naming_the_ceiling_not_the_count()
 {
    let store = absent_store();
    let outcome = run(
        &[
            store.path_str(),
            "--write",
            "--read-result-rows",
            "2",
            "--read-cursor-page-rows",
            "2",
            "--json",
        ],
        "CREATE TABLE tx_rows (id INTEGER PRIMARY KEY, label TEXT);\n\
         BEGIN;\n\
         INSERT INTO tx_rows (id, label) VALUES (1, 'a');\n\
         INSERT INTO tx_rows (id, label) VALUES (2, 'b');\n\
         INSERT INTO tx_rows (id, label) VALUES (3, 'c');\n\
         SELECT id FROM tx_rows ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "a runtime refusal makes a non-interactive session exit 1.\n{}",
        outcome.describe()
    );
    let error = owner_limit_error(&outcome);
    let detail = error.get("detail").expect("refusal detail");
    assert_eq!(
        detail.get("limit").and_then(|l| l.as_str()),
        Some("result_rows"),
        "the crossed ceiling is result_rows: {error}"
    );
    assert_eq!(
        detail.get("value").and_then(|v| v.as_i64()),
        Some(2),
        "the refusal names the CEILING that was crossed (2), never the row count the three \
         uncommitted inserts actually produced (3): {error}"
    );
}

#[test]
fn a_write_sessions_select_inside_its_own_open_transaction_succeeds_under_a_roomier_ceiling_then_rollback_empties_it()
 {
    let store = absent_store();
    let outcome = run(
        &[
            store.path_str(),
            "--write",
            "--read-result-rows",
            "5",
            "--read-cursor-page-rows",
            "5",
            "--json",
        ],
        "CREATE TABLE tx_rows (id INTEGER PRIMARY KEY, label TEXT);\n\
         BEGIN;\n\
         INSERT INTO tx_rows (id, label) VALUES (1, 'a');\n\
         INSERT INTO tx_rows (id, label) VALUES (2, 'b');\n\
         INSERT INTO tx_rows (id, label) VALUES (3, 'c');\n\
         SELECT id FROM tx_rows ORDER BY id;\n\
         ROLLBACK;\n\
         SELECT id FROM tx_rows ORDER BY id;\n",
    );

    assert_eq!(
        outcome.code,
        Some(0),
        "every statement here succeeds under a five-row ceiling, so the session exits 0.\n{}",
        outcome.describe()
    );
    let docs = outcome.stdout_docs();
    let results = documents_named(&docs, "result");
    assert_eq!(
        results.len(),
        2,
        "two ordinary SELECTs, so two result documents.\n{}",
        outcome.describe()
    );
    assert_eq!(
        result_row_ids(&results[0]),
        vec![1, 2, 3],
        "inside the open transaction the write session's own bounded read sees its own three \
         uncommitted inserts (read-your-writes), staying under the five-row ceiling.\n{}",
        outcome.describe()
    );
    assert!(
        result_row_ids(&results[1]).is_empty(),
        "after ROLLBACK the same session's SELECT sees nothing — the uncommitted inserts are \
         gone.\n{}",
        outcome.describe()
    );
}
