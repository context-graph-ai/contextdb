//! Journey: metadata reads are bounded and resumable, or refused whole.
//!
//! Contract held here: `.tables` and `.events status` return as many complete
//! items as fit the byte ceiling plus their own continuation — never the SQL
//! cursor — and human output prints the exact follow-up command; a continuation
//! is accepted only by the command that issued it; `.schema` and
//! `.maintenance status` are single complete responses that refuse rather than
//! publish partial metadata, and the refusal says exactly what setting would let
//! the response through.

mod read_cli_support;

use read_cli_support::*;
use serde_json::Value;

/// Enough tables, named distinctly, that a small byte ceiling forces several
/// pages. The names carry no hint of how many pages that turns out to be.
fn many_tables() -> Store {
    let mut sql = String::new();
    for index in 0..12 {
        sql.push_str(&format!(
            "CREATE TABLE catalog_subject_{index:02}_records (id INTEGER PRIMARY KEY, label TEXT);\n"
        ));
    }
    store_with(&sql)
}

fn documents_of(transcript: &str, key: &str) -> Vec<Value> {
    lenient_json_lines(transcript)
        .into_iter()
        .filter_map(|d| d.get(key).cloned())
        .collect()
}

fn page_items(page: &Value) -> Vec<Value> {
    page.get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default()
}

fn continuation_of(page: &Value) -> Option<String> {
    page.get("continuation")
        .and_then(|c| c.as_str())
        .map(str::to_owned)
}

/// Drive `command` to exhaustion inside ONE session, following the continuation
/// the CLI hands back. Each step waits for the next document — never for time.
fn page_to_exhaustion(store: &Store, command: &str, key: &str, byte_ceiling: &str) -> Vec<Value> {
    let mut session = terminal(&[
        store.path_str(),
        "--json",
        "--read-result-bytes",
        byte_ceiling,
        "--read-cursor-page-bytes",
        byte_ceiling,
    ]);

    session.send_line(command);
    let mut seen = 1usize;
    let mut pages: Vec<Value> = Vec::new();

    for _ in 0..32 {
        let transcript = session.wait_for_count(&format!("\"{key}\""), seen);
        pages = documents_of(&transcript, key);
        let last = pages
            .last()
            .unwrap_or_else(|| panic!("expected a {key} page.\ntranscript:\n{transcript}"));
        let has_more = last.get("has_more").and_then(|h| h.as_bool());
        match (has_more, continuation_of(last)) {
            (Some(true), Some(token)) => {
                session.send_line(&format!("{command} --continue {token}"));
                seen += 1;
            }
            (Some(true), None) => {
                panic!("`has_more: true` must always come with a continuation string: {last}")
            }
            _ => break,
        }
    }

    let (code, transcript) = session.quit();
    assert_eq!(
        code,
        Some(0),
        "paging metadata to exhaustion is an ordinary success.\ntranscript:\n{transcript}"
    );
    pages
}

#[test]
fn tables_under_json_is_a_namespaced_page_document() {
    let store = many_tables();
    let outcome = run(&[store.path_str(), "--json"], ".tables\n");

    let docs = outcome.stdout_docs();
    let page = expect_document(&docs, "tables", "`.tables` under --json");
    assert!(
        page.get("items").and_then(|i| i.as_array()).is_some(),
        "`.tables` answers a page document carrying `items`, not a bare name array: {page}"
    );
    assert_eq!(
        page.get("has_more").and_then(|h| h.as_bool()),
        Some(false),
        "everything fits under shipped defaults, so there is no next page: {page}"
    );
    assert!(
        page.get("continuation")
            .map(|c| c.is_null())
            .unwrap_or(false),
        "`continuation` is null exactly when `has_more` is false: {page}"
    );
}

#[test]
fn tables_resumes_through_its_own_continuation_until_exhausted() {
    let store = many_tables();
    let pages = page_to_exhaustion(&store, ".tables", "tables", "256");

    assert!(
        pages.len() > 1,
        "a small byte ceiling must actually force more than one page, or this journey proves \
         nothing: {pages:?}"
    );
    let last = pages.last().expect("a final page");
    assert_eq!(
        last.get("has_more").and_then(|h| h.as_bool()),
        Some(false),
        "resuming ends with a truthful `has_more: false`: {last}"
    );
    assert!(
        last.get("continuation")
            .map(|c| c.is_null())
            .unwrap_or(false),
        "the final page carries a null continuation: {last}"
    );

    let mut names: Vec<String> = pages
        .iter()
        .flat_map(page_items)
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect();
    let total = names.len();
    names.sort();
    names.dedup();
    assert_eq!(
        total,
        names.len(),
        "an item never appears on two pages: {names:?}"
    );
    for index in 0..12 {
        let expected = format!("catalog_subject_{index:02}_records");
        assert!(
            names.contains(&expected),
            "every table that existed throughout the paging appears exactly once: {expected} \
             missing from {names:?}"
        );
    }
}

#[test]
fn a_human_metadata_page_prints_the_exact_follow_up_command() {
    let store = many_tables();
    let outcome = run(
        &[
            store.path_str(),
            "--read-result-bytes",
            "256",
            "--read-cursor-page-bytes",
            "256",
        ],
        ".tables\n",
    );

    assert!(
        outcome.stdout.contains("has_more"),
        "a human metadata page states whether another page exists.\n{}",
        outcome.describe()
    );
    assert!(
        outcome.stdout.contains(".tables --continue "),
        "and prints the exact command that fetches it, so the instruction is executable as \
         printed.\n{}",
        outcome.describe()
    );
}

#[test]
fn events_status_pages_under_its_own_namespaced_key() {
    let store = many_tables();
    let outcome = run(&[store.path_str(), "--json"], ".events status\n");

    let docs = outcome.stdout_docs();
    let page = expect_document(&docs, "events_status", "`.events status` under --json");
    assert!(
        page.get("items").is_some() && page.get("has_more").is_some(),
        "`.events status` answers the same page shape as `.tables`, under its own key: {page}"
    );
    assert!(
        document_named(&docs, "events").is_none(),
        "the document key is `events_status`, matching the command.\n{}",
        outcome.describe()
    );
}

#[test]
fn a_continuation_is_refused_by_a_command_that_did_not_issue_it() {
    let store = many_tables();
    let first = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "256",
            "--read-cursor-page-bytes",
            "256",
        ],
        ".tables\n",
    );
    let docs = first.stdout_docs();
    let page = expect_document(&docs, "tables", "the first `.tables` page");
    let token = continuation_of(&page)
        .unwrap_or_else(|| panic!("this journey needs a real continuation to misuse: {page}"));

    let misused = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "256",
            "--read-cursor-page-bytes",
            "256",
        ],
        &format!(".events status --continue {token}\n"),
    );

    let error = misused
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("invalid_continuation"))
        .unwrap_or_else(|| {
            panic!(
                "a continuation belongs to the command that issued it.\n{}",
                misused.describe()
            )
        });
    assert_eq!(
        error.get("class").and_then(|c| c.as_str()),
        Some("usage"),
        "misusing a continuation is caller error, not an io failure: {error}"
    );

    let garbage = run(
        &[store.path_str(), "--json"],
        ".tables --continue not-a-real-continuation\n",
    );
    assert!(
        garbage
            .error_kinds()
            .iter()
            .any(|k| k == "invalid_continuation"),
        "a malformed continuation is refused the same way.\n{}",
        garbage.describe()
    );
}

#[test]
fn schema_is_one_complete_namespaced_document() {
    let store = store_with(&create_seeded_table("subject", 2));
    let outcome = run(&[store.path_str(), "--json"], ".schema subject\n");

    let docs = outcome.stdout_docs();
    let schema = expect_document(&docs, "schema", "`.schema` under --json");
    assert!(
        schema.get("columns").is_some() && schema.get("ddl").is_some(),
        "`.schema` keeps its declared-contract payload, namespaced: {schema}"
    );
}

#[test]
fn maintenance_status_and_explain_keep_their_own_document_keys() {
    let store = store_with(&create_seeded_table("subject", 2));
    let outcome = run(
        &[store.path_str(), "--json"],
        ".maintenance status\n.explain SELECT id FROM subject;\n",
    );

    let docs = outcome.stdout_docs();
    expect_document(&docs, "maintenance", "`.maintenance status` under --json");
    expect_document(&docs, "explain", "`.explain` under --json");
}

#[test]
fn a_complete_metadata_response_that_does_not_fit_refuses_with_the_setting_that_would() {
    let store = store_with(&create_seeded_table("subject", 2));

    for command in [".schema subject", ".maintenance status"] {
        let outcome = run(
            &[
                store.path_str(),
                "--json",
                "--read-result-bytes",
                "24",
                "--read-cursor-page-bytes",
                "24",
            ],
            &format!("{command}\n"),
        );

        assert_eq!(
            outcome.code,
            Some(1),
            "{command} publishes no partial metadata; it refuses.\n{}",
            outcome.describe()
        );
        assert!(
            outcome.stdout.trim().is_empty(),
            "{command} must publish nothing at all when the complete response will not fit.\n{}",
            outcome.describe()
        );

        let error = outcome
            .errors()
            .into_iter()
            .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
            .unwrap_or_else(|| {
                panic!(
                    "{command} must refuse with owner_limit_exceeded.\n{}",
                    outcome.describe()
                )
            });
        let detail = error.get("detail").expect("refusal detail");
        assert_eq!(
            detail.get("limit").and_then(|l| l.as_str()),
            Some("result_bytes"),
            "{command}: the refusal names the byte ceiling: {error}"
        );
        let required = detail
            .get("required_bytes")
            .and_then(|r| r.as_i64())
            .unwrap_or_else(|| {
                panic!("{command}: the refusal states how many bytes it needs: {error}")
            });
        assert_eq!(
            detail.get("required_setting").and_then(|s| s.as_str()),
            Some(format!("effective result_bytes >= {required}").as_str()),
            "{command}: the refusal renders the setting that would let it through: {error}"
        );
    }
}

#[test]
fn a_metadata_item_that_cannot_fit_even_an_empty_page_refuses_the_same_way() {
    let store = many_tables();
    let outcome = run(
        &[
            store.path_str(),
            "--json",
            "--read-result-bytes",
            "8",
            "--read-cursor-page-bytes",
            "8",
        ],
        ".tables\n",
    );

    assert!(
        outcome.stdout.trim().is_empty(),
        "no oversized or partial page is ever published.\n{}",
        outcome.describe()
    );
    let error = outcome
        .errors()
        .into_iter()
        .find(|e| detail_kind(e).as_deref() == Some("owner_limit_exceeded"))
        .unwrap_or_else(|| {
            panic!(
                "an item too large for even an empty page refuses like any oversized response.\n{}",
                outcome.describe()
            )
        });
    let detail = error.get("detail").expect("refusal detail");
    assert!(
        detail
            .get("required_bytes")
            .and_then(|r| r.as_i64())
            .is_some()
            && detail
                .get("required_setting")
                .and_then(|s| s.as_str())
                .is_some(),
        "with the identical required_bytes / required_setting detail: {error}"
    );
}
