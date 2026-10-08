//! Journey: a refused statement is reported, the script continues, the
//! process exits failed.
//!
//! A refused statement is reported clearly; the session continues with
//! subsequent independent and recovery commands (including inspecting or
//! adjusting the exhausted limit), and the process exits with a failure
//! status. A refusal is never an empty success. The translated `.sync`
//! inspection route (`repl.rs` around `custody_inspection_sql` /
//! `process_meta_line`) follows the same continuation rule.

mod read_cli_support;

use read_cli_support::*;

fn vector_insert_refusal_limit(extra_table: bool) -> usize {
    use contextdb_engine::memory_accounting::{
        MemoryAccountant, take_budget_ledger_for_test, watch_budget_ledger_for_test,
    };
    let root = tempfile::tempdir().expect("task-scoped owner profile");
    let accountant = std::sync::Arc::new(MemoryAccountant::no_limit());
    let db = contextdb_engine::Database::open_with_config(
        root.path().join("profile.db"),
        std::sync::Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant,
    )
    .unwrap();
    db.execute(
        "CREATE TABLE audio_clips (id UUID PRIMARY KEY, vector_audio VECTOR(2048))",
        &std::collections::HashMap::new(),
    )
    .unwrap();
    if extra_table {
        db.execute(
            "CREATE TABLE empty_t (id UUID PRIMARY KEY)",
            &std::collections::HashMap::new(),
        )
        .unwrap();
    }
    watch_budget_ledger_for_test();
    let literal = std::iter::repeat_n("1.0", 2048)
        .collect::<Vec<_>>()
        .join(",");
    db.execute(
        &format!(
            "INSERT INTO audio_clips VALUES ('11111111-1111-1111-1111-111111111111', [{literal}])"
        ),
        &std::collections::HashMap::new(),
    )
    .unwrap();
    let ledger = take_budget_ledger_for_test();
    let position = ledger
        .iter()
        .position(|entry| {
            entry
                .what
                .contains("vector_insert@audio_clips.vector_audio")
        })
        .unwrap_or_else(|| panic!("vector owner must be present: {ledger:?}"));
    let owner = &ledger[position];
    // SQL exposes whole KiB units; use the largest expressible ceiling below this owner.
    let limit = (owner.used_after - 1) / 1024 * 1024;
    assert!(
        ledger[..position]
            .iter()
            .all(|entry| entry.used_after <= limit),
        "prior real owners fit before the vector body"
    );
    db.close().unwrap();
    limit
}

fn attributed_vector_memory_refusal(message: &str) -> bool {
    message.to_lowercase().contains("memory")
        && message.contains("vector_insert@audio_clips.vector_audio")
}

fn vector_literal(len: usize) -> String {
    std::iter::repeat_n("1.0", len)
        .collect::<Vec<_>>()
        .join(",")
}

/// A memory refusal is followed by an independent SELECT that succeeds, then
/// `SHOW MEMORY_LIMIT` / `SET MEMORY_LIMIT` recovery, then a statement that
/// now succeeds under the raised limit — and the overall exit status is
/// still failure, because a refusal happened earlier in the same run.
#[test]
fn a_memory_refusal_is_followed_by_independent_work_and_recovery() {
    let store = absent_store();
    let vec_body = vector_literal(2048);
    let limit = vector_insert_refusal_limit(false);
    let limit_kib = limit / 1024;
    let script = format!(
        "SET MEMORY_LIMIT '{limit_kib}K';\n\
         CREATE TABLE audio_clips (id UUID PRIMARY KEY, vector_audio VECTOR(2048));\n\
         INSERT INTO audio_clips (id, vector_audio) VALUES \
         ('11111111-1111-1111-1111-111111111111', [{vec_body}]);\n\
         SELECT 1 + 1;\n\
         SHOW MEMORY_LIMIT;\n\
         SET MEMORY_LIMIT '10M';\n\
         INSERT INTO audio_clips (id, vector_audio) VALUES \
         ('22222222-2222-2222-2222-222222222222', [{vec_body}]);\n\
         SELECT id FROM audio_clips;\n"
    );
    let outcome = run(&[store.path_str(), "--write", "--json"], &script);

    let docs = outcome.stdout_docs();
    // The two `CREATE TABLE`/`SET MEMORY_LIMIT` setup statements and the
    // refused INSERT never publish a `result` document with rows; the
    // independent SELECT that follows the refusal does.
    let independent_select = documents_named(&docs, "result")
        .into_iter()
        .find(|d| rows_of(d).first().and_then(|r| r.get("expr")).is_some())
        .expect("the independent SELECT after the refusal must still publish its result");
    assert_eq!(
        rows_of(&independent_select)[0]["expr"],
        2,
        "the independent SELECT must have actually run and computed 1 + 1.\n{}",
        outcome.describe()
    );

    let show_limit = documents_named(&docs, "result")
        .into_iter()
        .find(|d| rows_of(d).first().and_then(|r| r.get("limit")).is_some())
        .expect("SHOW MEMORY_LIMIT must publish its recovery-inspection result");
    assert_eq!(
        rows_of(&show_limit)[0]["limit"],
        limit,
        "SHOW MEMORY_LIMIT must report the limit set before the refusal.\n{}",
        outcome.describe()
    );

    let errors = outcome.errors();
    assert_eq!(
        errors.len(),
        1,
        "exactly one refusal (the INSERT under the derived vector-owner budget), not the recovery statements.\n{}",
        outcome.describe()
    );
    assert!(
        errors[0]["message"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("memory"),
        "the refusal must name the resource it exhausted.\n{}",
        outcome.describe()
    );

    let message = errors[0]["message"].as_str().unwrap();
    assert!(attributed_vector_memory_refusal(message), "{message}");
    assert!(!attributed_vector_memory_refusal(
        &message.replace("audio_clips.vector_audio", "other.vector_audio")
    ));
    let recovered = documents_named(&docs, "result")
        .into_iter()
        .find(|doc| rows_of(doc).first().and_then(|row| row.get("id")).is_some())
        .expect("recovery publishes the fitting row");
    assert_eq!(rows_of(&recovered).len(), 1);
    assert_eq!(
        rows_of(&recovered)[0]["id"],
        "22222222-2222-2222-2222-222222222222"
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "a run that suffered a refusal exits failed even though every later statement, \
         including the one that raised the limit and retried, succeeded.\n{}",
        outcome.describe()
    );
}

/// The same shape for a disk refusal: `SHOW DISK_LIMIT` is the recovery
/// inspection, and the retried write under the raised limit succeeds.
#[test]
fn a_disk_refusal_is_followed_by_independent_work_and_recovery() {
    let store = absent_store();
    let payload = "x".repeat(4000);
    let script = format!(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT);\n\
         SET DISK_LIMIT '1K';\n\
         INSERT INTO big (id, payload) VALUES \
         ('11111111-1111-1111-1111-111111111111', '{payload}');\n\
         SELECT 1 + 1;\n\
         SHOW DISK_LIMIT;\n\
         SET DISK_LIMIT '10M';\n\
         INSERT INTO big (id, payload) VALUES \
         ('22222222-2222-2222-2222-222222222222', '{payload}');\n"
    );
    let outcome = run(&[store.path_str(), "--write", "--json"], &script);

    let docs = outcome.stdout_docs();
    let independent_select = documents_named(&docs, "result")
        .into_iter()
        .find(|d| rows_of(d).first().and_then(|r| r.get("expr")).is_some())
        .expect("the independent SELECT after the refusal must still publish its result");
    assert_eq!(
        rows_of(&independent_select)[0]["expr"],
        2,
        "the independent SELECT must have actually run.\n{}",
        outcome.describe()
    );

    let show_limit = documents_named(&docs, "result")
        .into_iter()
        .find(|d| rows_of(d).first().and_then(|r| r.get("limit")).is_some())
        .expect("SHOW DISK_LIMIT must publish its recovery-inspection result");
    assert_eq!(
        rows_of(&show_limit)[0]["limit"],
        1024,
        "SHOW DISK_LIMIT must report the limit set before the refusal.\n{}",
        outcome.describe()
    );

    let errors = outcome.errors();
    assert_eq!(
        errors.len(),
        1,
        "exactly one refusal (the first INSERT under the 1K budget).\n{}",
        outcome.describe()
    );
    assert!(
        errors[0]["message"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("disk"),
        "the refusal must name the resource it exhausted.\n{}",
        outcome.describe()
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "a run that suffered a disk refusal exits failed even though the retried write under \
         the raised limit succeeded.\n{}",
        outcome.describe()
    );
}

/// The translated `.sync` inspection route (`custody_inspection_sql` in
/// `repl.rs`, dispatched from `process_meta_line`) is a second entry point
/// into statement execution, distinct from the ordinary SQL path. A refusal arriving
/// through that route still lets the session continue and still fails the
/// exit status, exactly like a refusal on the ordinary path.
#[test]
fn a_refusal_through_the_translated_sync_inspection_route_still_continues() {
    let store = absent_store();
    let script = "\
        .sync outcomes nonexistent_table\n\
        SELECT 1 + 1;\n\
        .sync policy\n";
    let outcome = run(&[store.path_str(), "--write", "--json"], script);

    let docs = outcome.stdout_docs();
    let independent_select = documents_named(&docs, "result")
        .into_iter()
        .find(|d| rows_of(d).first().and_then(|r| r.get("expr")).is_some())
        .expect(
            "the SELECT after the translated-route refusal must still run and publish its \
             result",
        );
    assert_eq!(
        rows_of(&independent_select)[0]["expr"],
        2,
        "the session must keep executing after the translated-route refusal.\n{}",
        outcome.describe()
    );

    let final_policy_result = documents_named(&docs, "result")
        .into_iter()
        .find(|d| {
            d.get("columns")
                .and_then(|c| c.as_array())
                .is_some_and(|c| c.iter().any(|v| v == "table"))
        })
        .expect("the later `.sync policy` translated command must still run and publish");
    assert_eq!(
        rows_of(&final_policy_result).len(),
        0,
        "no tenant table policy is declared, so this is an empty success, not a refusal.\n{}",
        outcome.describe()
    );

    assert_eq!(
        outcome.errors().len(),
        1,
        "exactly one refusal: the malformed `.sync outcomes` translated statement.\n{}",
        outcome.describe()
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "a refusal through the translated `.sync` inspection route still fails the exit status.\n{}",
        outcome.describe()
    );
}

/// A refused query must be distinguishable from a successful empty result:
/// the refusal names the resource and the operation on stderr, and an empty
/// `SELECT` renders as an empty `result` document with success, never as an
/// error and never silently indistinguishable from one.
#[test]
fn a_refusal_is_distinguishable_from_a_successful_empty_result() {
    let store = absent_store();
    let vec_body = vector_literal(2048);
    let limit = vector_insert_refusal_limit(true);
    let limit_kib = limit / 1024;
    let script = format!(
        "SET MEMORY_LIMIT '{limit_kib}K';\n\
         CREATE TABLE audio_clips (id UUID PRIMARY KEY, vector_audio VECTOR(2048));\n\
         CREATE TABLE empty_t (id UUID PRIMARY KEY);\n\
         SELECT * FROM empty_t;\n\
         INSERT INTO audio_clips (id, vector_audio) VALUES \
         ('11111111-1111-1111-1111-111111111111', [{vec_body}]);\n"
    );
    let outcome = run(&[store.path_str(), "--write", "--json"], &script);

    let docs = outcome.stdout_docs();
    let empty_select = documents_named(&docs, "result")
        .into_iter()
        .find(|d| {
            d.get("columns")
                .and_then(|c| c.as_array())
                .is_some_and(|c| c.iter().any(|v| v == "id"))
        })
        .expect("the empty SELECT must publish a `result` document, not an error");
    assert_eq!(
        rows_of(&empty_select).len(),
        0,
        "an empty result has zero rows, rendered as ordinary success.\n{}",
        outcome.describe()
    );
    assert!(
        empty_select.get("error").is_none(),
        "an empty result must never be rendered as, or alongside, an error document.\n{}",
        outcome.describe()
    );

    let errors = outcome.errors();
    assert_eq!(
        errors.len(),
        1,
        "exactly one refusal: the vector INSERT over budget.\n{}",
        outcome.describe()
    );
    let message = errors[0]["message"].as_str().unwrap_or_default();
    assert!(attributed_vector_memory_refusal(message), "{message}");
    assert!(!attributed_vector_memory_refusal(
        &message.replace("audio_clips.vector_audio", "audio_clips.other")
    ));
    assert!(
        message.to_lowercase().contains("memory"),
        "the refusal must name the resource (memory) it exhausted; got: {message}"
    );
    assert!(
        message.contains("insert") || message.contains("vector_insert"),
        "the refusal must name the operation that was refused; got: {message}"
    );
    assert!(
        message.contains("audio_clips") && message.contains("vector_audio"),
        "the refusal must name the (table, column) it was attributed to; got: {message}"
    );

    assert_eq!(
        outcome.code,
        Some(1),
        "the run failed because of the refusal, distinct from the empty-but-successful SELECT.\n{}",
        outcome.describe()
    );
}
