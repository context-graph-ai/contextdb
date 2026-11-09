//! Production-door proofs for passive vector inspection and truthful explain.
//!
//! This deliberately uses the SQL inspection/explain surfaces and the existing
//! read-only test counters.  The counters establish that inspection did not
//! construct a graph or change the store charge; they are not another way to
//! obtain lifecycle facts. `QueryTrace::vector_search` is an intentional
//! production compile wall for the accepted typed, per-result vector
//! disclosure; the current label-only `physical_plan` is not treated as that
//! promised surface. `__vector_passive_activity_counters_for_test` is the
//! second narrow observation wall: it counts raw/graph loads, replay, build,
//! repair, compaction, and eviction without performing any of them.

use contextdb_core::{ContextId, Value, VectorSearchMode};
use contextdb_engine::{
    Database, ExplainOutput, MaintenancePolicy, QueryResult, VectorSearchScopeShape,
};
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;
use uuid::Uuid;

const SCOPE_ALPHA: &str = "layout-key-alpha-that-must-not-leak";
const SCOPE_BRAVO: &str = "layout-key-bravo-that-must-not-leak";
const SCOPE_HIDDEN: &str = "unauthorized-layout-key-that-must-not-leak";
const HIDDEN_CONTEXT: u128 = 0xA11C_E000_0000_0000_0000_0000_0000_0102;
const HIDDEN_ROW: u128 = 0xA11C_E000_0000_0000_0000_0000_0000_0014;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns()
        .iter()
        .position(|candidate| candidate == name)
        .unwrap_or_else(|| panic!("inspection contains {name}: {:?}", result.columns()))
}

fn row_for(result: &QueryResult, table: &str, vector_column: &str) -> usize {
    let table_column = column(result, "table");
    let vector_column_index = column(result, "column");
    result
        .rows()
        .iter()
        .position(|row| {
            row[table_column] == Value::Text(table.to_owned())
                && row[vector_column_index] == Value::Text(vector_column.to_owned())
        })
        .unwrap_or_else(|| panic!("inspection describes {table}.{vector_column}: {result:?}"))
}

fn partition_row_for_scope(result: &QueryResult, scope: &str) -> usize {
    let key_column = column(result, "partition_key");
    result
        .rows()
        .iter()
        .position(|row| {
            matches!(
                row.get(key_column),
                Some(Value::Json(key))
                    if key.get("scope_id").and_then(serde_json::Value::as_str) == Some(scope)
            )
        })
        .unwrap_or_else(|| panic!("inspection contains partition {scope:?}: {result:?}"))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows()
        .get(row)
        .and_then(|values| values.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection has row {row} and {name}: {result:?}"))
}

fn int(result: &QueryResult, row: usize, name: &str) -> i64 {
    match value(result, row, name) {
        Value::Int64(value) => *value,
        other => panic!("inspection {name} is an integer, got {other:?}"),
    }
}

fn seed_fixture(db: &Database) -> Uuid {
    db.execute(
        "CREATE TABLE vector_inspection_docs (\
            id UUID PRIMARY KEY, \
            context_id UUID CONTEXT_ID, \
            scope_id TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (scope_id) \
                MAX_PARTITIONS 8 SEARCH_MODE EXACT\
        )",
        &empty(),
    )
    .expect("declare a partitioned vector column");

    let source_id = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0001);
    let context = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101);
    for (offset, scope, vector) in [
        (0_u128, SCOPE_ALPHA, vec![1.0, 0.0, 0.0]),
        (1_u128, SCOPE_ALPHA, vec![0.9, 0.1, 0.0]),
        (2_u128, SCOPE_BRAVO, vec![0.0, 1.0, 0.0]),
        (3_u128, SCOPE_BRAVO, vec![0.0, 0.9, 0.1]),
    ] {
        let id = if offset == 0 {
            source_id
        } else {
            Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0010 + offset)
        };
        db.execute(
            "INSERT INTO vector_inspection_docs \
             (id, context_id, scope_id, kind, embedding) \
             VALUES ($id, $context, $scope, $kind, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("context", Value::Uuid(context)),
                ("scope", Value::Text(scope.to_owned())),
                (
                    "kind",
                    Value::Text("filter-value-that-must-not-leak".to_owned()),
                ),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("commit one small deterministic partition member");
    }
    source_id
}

fn fixture() -> (Database, Uuid) {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let source_id = seed_fixture(&db);
    (db, source_id)
}

fn show_indexes(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_INDEXES", &empty())
        .expect("SHOW VECTOR_INDEXES is an admin inspection surface")
}

fn show_partitions(db: &Database) -> QueryResult {
    db.execute(
        "SHOW VECTOR_PARTITIONS FOR vector_inspection_docs.embedding",
        &empty(),
    )
    .expect("SHOW VECTOR_PARTITIONS is an admin inspection surface")
}

fn assert_no_secrets(explain: &str, secrets: &[String]) {
    for secret in secrets {
        assert!(
            !explain.contains(secret),
            "explain disclosed caller or authorization data {secret:?}: {explain}"
        );
    }
    for forbidden_count in [
        "authorized_count=",
        "candidate_count=",
        "partition_count=",
        "selected_count=",
        "selected_partitions=",
        "vector_count=",
        "live_rows=",
    ] {
        assert!(
            !explain.contains(forbidden_count),
            "explain disclosed an authorization-sensitive count through {forbidden_count:?}: {explain}"
        );
    }
}

fn fixture_row_ids() -> [Uuid; 5] {
    [
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0001),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0011),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0012),
        Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0013),
        Uuid::from_u128(HIDDEN_ROW),
    ]
}

fn secret_strings(extra: impl IntoIterator<Item = String>) -> Vec<String> {
    fixture_row_ids()
        .into_iter()
        .map(|id| id.to_string())
        .chain([
            SCOPE_HIDDEN.to_owned(),
            Uuid::from_u128(HIDDEN_CONTEXT).to_string(),
        ])
        .chain(extra)
        .collect()
}

fn assert_explain_fact(explain: &str, name: &str, expected: &str) {
    let fact = format!("{name}={expected}");
    assert!(
        explain.contains(&fact),
        "explain must report the structural fact {fact:?}: {explain}"
    );
}

fn runtime_vector_explain(result: &QueryResult) -> String {
    result
        .trace()
        .vector_search
        .as_ref()
        .expect("a runtime vector query returns its own typed vector disclosure")
        .to_string()
}

/// With no index built, every stored vector is in the tail layer.
fn assert_exact_layer_facts(explain: &str) {
    assert_explain_fact(explain, "partition_key", "scope_id");
    assert_explain_fact(explain, "base", "absent");
    assert_explain_fact(explain, "change", "absent");
    assert_explain_fact(explain, "tail", "present");
}

// A saved base with a nonempty suffix must inspect without replaying that suffix.
// Each arm has N surviving inserts and N insert/delete pairs after the same base.
fn inspect_reopened_backlog_without_maintenance(backlog: usize) -> usize {
    use contextdb_engine::{DatabaseOpenOptions, writer_query_test_seams};
    let root = TempDir::new().expect("temporary pending inspection store");
    let path = root.path().join("vector-inspection-pending.redb");
    let open = || {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .expect("open caller-driven inspection store")
    };
    let db = open();
    seed_fixture(&db);
    let context = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101);
    for ordinal in 0..backlog {
        for deleted in [false, true] {
            let id = Uuid::from_u128(0xBACC_0000 + ordinal as u128 * 2 + u128::from(deleted));
            db.execute("INSERT INTO vector_inspection_docs (id,context_id,scope_id,kind,embedding) VALUES ($id,$context,$scope,'pending','[0.8,0.2,0.0]')",
                &params([("id",Value::Uuid(id)),("context",Value::Uuid(context)),
                         ("scope",Value::Text(SCOPE_ALPHA.to_owned()))])).unwrap();
            if deleted {
                db.execute(
                    "DELETE FROM vector_inspection_docs WHERE id = $id",
                    &params([("id", Value::Uuid(id))]),
                )
                .unwrap();
            }
        }
    }
    // Do not consolidate the suffix or inspect it before closing.
    db.close().expect("seal a nonempty insert/delete suffix");
    drop(db);
    let db = open();
    let cache_before = db.__writer_cache_stats_for_test().unwrap();
    let activity_before = db.__vector_passive_activity_counters_for_test();
    let charged_before = db.accountant().usage().used;
    db.__reset_vector_membership_records_decoded_for_test();
    let ((indexes, partitions), work) =
        writer_query_test_seams::observe(|| (show_indexes(&db), show_partitions(&db)));
    let cache_after = db.__writer_cache_stats_for_test().unwrap();
    let row = row_for(&indexes, "vector_inspection_docs", "embedding");
    // With no index shipped every live vector is pending: the backlog and the
    // four seeded vectors.
    assert_eq!(int(&indexes, row, "pending_inserts"), backlog as i64 + 4);
    assert_eq!(int(&indexes, row, "tombstones"), backlog as i64);
    assert_eq!(int(&indexes, row, "vector_count"), backlog as i64 + 4);
    let alpha = partition_row_for_scope(&partitions, SCOPE_ALPHA);
    let bravo = partition_row_for_scope(&partitions, SCOPE_BRAVO);
    assert_eq!(
        int(&partitions, alpha, "pending_inserts"),
        backlog as i64 + 2
    );
    assert_eq!(int(&partitions, alpha, "tombstones"), backlog as i64);
    assert_eq!(int(&partitions, alpha, "live_rows"), backlog as i64 + 2);
    assert_eq!(int(&partitions, bravo, "pending_inserts"), 2);
    assert_eq!(int(&partitions, bravo, "tombstones"), 0);
    assert_eq!(int(&partitions, bravo, "live_rows"), 2);
    assert_eq!(
        work.history.vector_memberships, 0,
        "inspection never traverses durable membership history"
    );
    assert_eq!(
        db.__vector_membership_records_decoded_for_test(),
        0,
        "truthful pending counts never decode the membership population"
    );
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        activity_before,
        "pending inspection cannot load, replay, build, repair, compact or evict vector state"
    );
    assert_eq!(db.accountant().usage().used, charged_before);
    let indexes_again = show_indexes(&db);
    let partitions_again = show_partitions(&db);
    assert_eq!(indexes_again.rows(), indexes.rows());
    assert_eq!(partitions_again.rows(), partitions.rows());
    assert_eq!(
        db.__vector_passive_activity_counters_for_test(),
        activity_before
    );
    let growth = cache_after.3.checked_sub(cache_before.3).unwrap();
    db.close().unwrap();
    growth
}

#[test]
fn reopened_nonempty_vector_backlog_inspection_has_fixed_cache_work_and_truthful_counts() {
    let small = inspect_reopened_backlog_without_maintenance(8);
    let grown = inspect_reopened_backlog_without_maintenance(256);
    assert_eq!(
        grown, small,
        "metadata-only inspection has the same cache residency increase with a grown insert/delete backlog"
    );
}

#[test]
fn vector_explain_names_mode_scope_route_and_merge_without_secrets() {
    let (db, source_id) = fixture();
    db.execute(
        "CREATE INDEX inspection_context ON vector_inspection_docs (context_id)",
        &empty(),
    )
    .unwrap();
    let hidden_context = Uuid::from_u128(HIDDEN_CONTEXT);
    db.execute(
        "INSERT INTO vector_inspection_docs \
         (id, context_id, scope_id, kind, embedding) \
         VALUES ($id, $context, $scope, $kind, $embedding)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(HIDDEN_ROW))),
            ("context", Value::Uuid(hidden_context)),
            ("scope", Value::Text(SCOPE_HIDDEN.to_owned())),
            (
                "kind",
                Value::Text("filter-value-that-must-not-leak".to_owned()),
            ),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("seed one partition the scoped caller is not authorized to inspect");
    let context = Uuid::from_u128(0xA11C_E000_0000_0000_0000_0000_0000_0101);
    let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(context)]));
    let source = source_id.to_string();
    let literal_vector = "[0.1234567,0.2345678,0.3456789]".to_owned();
    let explain_passively = |sql: &str| {
        let charge_before = db.accountant().usage().used;
        let activity_before = db.__vector_passive_activity_counters_for_test();
        let scan_before = db.__relational_scan_rows_touched();
        let explain = scoped
            .explain(sql)
            .expect("the scoped explain is metadata-only");
        // A passive explain on a Context-restricted handle must never
        // derive its candidates by scanning the whole table: neither
        // `vector_policy_facts_for_explain` nor `vector_search_disclosure`
        // may walk `effective_read_candidates` for a restricted `.explain`.
        assert_eq!(
            db.__relational_scan_rows_touched(),
            scan_before,
            "a passive explain must never scan the table to derive a restricted \
             reader's candidates: {sql}"
        );
        assert_eq!(
            db.accountant().usage().used,
            charge_before,
            "explain cannot load a graph or vector body"
        );
        assert_eq!(
            db.__vector_passive_activity_counters_for_test(),
            activity_before,
            "explain cannot load and evict, replay, repair, or compact behind an unchanged net receipt"
        );
        explain
    };

    for (mode_clause, requested_mode, resolved_mode, expected_route) in [
        ("USE VECTOR EXACT", "EXACT", "EXACT", "exact"),
        ("USE VECTOR AUTO", "AUTO", "EXACT", "exact"),
        ("", "EXACT", "EXACT", "exact"),
    ] {
        let sql = format!(
            "SELECT id FROM vector_inspection_docs \
             WHERE scope_id = '{SCOPE_ALPHA}' \
             ORDER BY embedding <=> ROW_VECTOR('vector_inspection_docs','embedding','{source}') \
             {mode_clause} LIMIT 2"
        );
        let explain = explain_passively(&sql);
        assert_explain_fact(&explain, "requested_mode", requested_mode);
        assert_explain_fact(&explain, "resolved_mode", resolved_mode);
        assert_explain_fact(&explain, "scope", "one");
        assert_explain_fact(&explain, "route", expected_route);
        assert_explain_fact(&explain, "merge", "global");
        assert_explain_fact(
            &explain,
            "residual",
            if resolved_mode == "EXACT" {
                "none"
            } else {
                "bounded"
            },
        );
        assert_explain_fact(&explain, "refusal", "none");
        assert_exact_layer_facts(&explain);
        if requested_mode == "EXACT" {
            assert!(
                !explain.contains("strategy=Indexed"),
                "explicit EXACT must never be described as an indexed route: {explain}"
            );
        }
        assert!(
            explain.contains("<redacted>"),
            "a literal row-source key and partition value retain only structural redaction markers: {explain}"
        );
        assert_no_secrets(
            &explain,
            &secret_strings([source.clone(), SCOPE_ALPHA.to_owned(), context.to_string()]),
        );

        let runtime = scoped
            .execute(
                &format!(
                    "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
                     ORDER BY embedding <=> $query {mode_clause} LIMIT 2"
                ),
                &params([
                    ("scope", Value::Text(SCOPE_ALPHA.to_owned())),
                    ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
                ]),
            )
            .expect("the ready maintained route executes through the ordinary production door");
        let runtime_explain = runtime_vector_explain(&runtime);
        assert_explain_fact(&runtime_explain, "requested_mode", requested_mode);
        assert_explain_fact(&runtime_explain, "resolved_mode", resolved_mode);
        assert_explain_fact(&runtime_explain, "scope", "one");
        assert_explain_fact(&runtime_explain, "route", expected_route);
        assert_explain_fact(&runtime_explain, "merge", "global");
        assert_explain_fact(
            &runtime_explain,
            "residual",
            if resolved_mode == "EXACT" {
                "none"
            } else {
                "bounded"
            },
        );
        assert_explain_fact(&runtime_explain, "refusal", "none");
        assert_exact_layer_facts(&runtime_explain);
        if requested_mode == "EXACT" {
            assert!(
                !runtime_explain.contains("strategy=Indexed"),
                "explicit EXACT runtime explain must never describe an indexed route: {runtime_explain}"
            );
        }
        assert!(
            runtime_explain.contains("<vector>"),
            "a bound query vector keeps only its structural vector placeholder: {runtime_explain}"
        );
        assert_no_secrets(
            &runtime_explain,
            &secret_strings([
                SCOPE_ALPHA.to_owned(),
                "[1.0, 0.0, 0.0]".to_owned(),
                context.to_string(),
            ]),
        );
    }

    for (predicate, scope_shape) in [
        (format!("scope_id = '{SCOPE_ALPHA}'"), "one"),
        (
            format!("scope_id = '{SCOPE_ALPHA}' OR scope_id = '{SCOPE_BRAVO}'"),
            "several",
        ),
        ("TRUE".to_owned(), "all"),
    ] {
        let explain = explain_passively(&format!(
            "SELECT id FROM vector_inspection_docs WHERE {predicate} \
                 ORDER BY embedding <=> {literal_vector} USE VECTOR EXACT LIMIT 2"
        ));
        assert_explain_fact(&explain, "scope", scope_shape);
        assert_explain_fact(&explain, "requested_mode", "EXACT");
        assert_explain_fact(&explain, "resolved_mode", "EXACT");
        assert_explain_fact(&explain, "route", "exact");
        assert_explain_fact(&explain, "merge", "global");
        assert_explain_fact(&explain, "residual", "none");
        assert_explain_fact(&explain, "refusal", "none");
        assert_exact_layer_facts(&explain);
        assert!(
            !explain.contains("strategy=Indexed"),
            "every explicit EXACT scope shape remains an exact route: {explain}"
        );
        assert!(
            explain.contains("<vector>"),
            "a literal query vector keeps only its structural vector placeholder: {explain}"
        );
        assert_no_secrets(
            &explain,
            &secret_strings([
                literal_vector.clone(),
                "0.1234567".to_owned(),
                "0.2345678".to_owned(),
                "0.3456789".to_owned(),
                SCOPE_ALPHA.to_owned(),
                SCOPE_BRAVO.to_owned(),
                context.to_string(),
            ]),
        );
    }

    let bound_row_source = scoped
        .execute(
            "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
             ORDER BY embedding <=> ROW_VECTOR('vector_inspection_docs','embedding',$source) \
             USE VECTOR EXACT LIMIT 2",
            &params([
                ("scope", Value::Text(SCOPE_ALPHA.to_owned())),
                ("source", Value::Uuid(source_id)),
            ]),
        )
        .expect("the runtime row-source path resolves a bound key without disclosing it");
    let bound_row_source_explain = runtime_vector_explain(&bound_row_source);
    assert_explain_fact(&bound_row_source_explain, "requested_mode", "EXACT");
    assert_explain_fact(&bound_row_source_explain, "resolved_mode", "EXACT");
    assert_explain_fact(&bound_row_source_explain, "route", "exact");
    assert_explain_fact(&bound_row_source_explain, "scope", "one");
    assert_explain_fact(&bound_row_source_explain, "merge", "global");
    assert_explain_fact(&bound_row_source_explain, "residual", "none");
    assert_explain_fact(&bound_row_source_explain, "refusal", "none");
    assert_exact_layer_facts(&bound_row_source_explain);
    assert!(
        bound_row_source_explain.contains("<redacted>"),
        "the runtime row-source key leaves only a structural redaction marker: {bound_row_source_explain}"
    );
    assert_no_secrets(
        &bound_row_source_explain,
        &secret_strings([source.clone(), SCOPE_ALPHA.to_owned(), context.to_string()]),
    );
}

/// A passive `.explain` runs nothing and binds nothing, so it must accept
/// placeholders in a partition predicate: neither the vector disclosure nor
/// the policy facts of the same explain may fail with
/// `Error::NotFound("missing parameter: scope")` or silently drop that
/// failure. Legs that need no unbound placeholder (the no-predicate "all"
/// shape, and execution of the same statement) are controls.
#[test]
fn explaining_a_prepared_partition_predicate_binds_nothing_and_names_the_shape() {
    const VEC: &str = "[0.1234567,0.2345678,0.3456789]";
    let (db, _source_id) = fixture();

    let named_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let second_named_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = $scope_alt \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let literal_sql = format!(
        "SELECT id FROM vector_inspection_docs WHERE scope_id = '{SCOPE_ALPHA}' \
         ORDER BY embedding <=> {VEC} LIMIT 2"
    );
    let in_sql = "SELECT id FROM vector_inspection_docs WHERE scope_id IN ($a, $b) \
         ORDER BY embedding <=> [0.1234567,0.2345678,0.3456789] LIMIT 2"
        .to_owned();
    let no_predicate_sql =
        format!("SELECT id FROM vector_inspection_docs ORDER BY embedding <=> {VEC} LIMIT 2");

    let explain_passively = |sql: &str| -> String {
        let charge_before = db.accountant().usage().used;
        let activity_before = db.__vector_passive_activity_counters_for_test();
        let explain = db
            .explain(sql)
            .unwrap_or_else(|error| panic!("explain must bind nothing and never error on an unbound partition predicate: {error}\nSQL: {sql}"));
        assert_eq!(
            db.accountant().usage().used,
            charge_before,
            "a prepared-predicate explain cannot load a graph or vector body"
        );
        assert_eq!(
            db.__vector_passive_activity_counters_for_test(),
            activity_before,
            "a prepared-predicate explain cannot load, replay, repair, or compact behind an unchanged net receipt"
        );
        explain
    };

    // Leg 1 -- named placeholder.
    let named_explain = explain_passively(&named_sql);
    assert_explain_fact(&named_explain, "scope", "one");
    assert_explain_fact(&named_explain, "refusal", "none");
    assert_explain_fact(&named_explain, "residual", "none");

    // Leg 2 -- a second named placeholder spelling; identical facts.
    let second_named_explain = explain_passively(&second_named_sql);
    assert_explain_fact(&second_named_explain, "scope", "one");
    assert_explain_fact(&second_named_explain, "refusal", "none");
    assert_explain_fact(&second_named_explain, "residual", "none");

    // Leg 3 -- the structured disclosure through the unscoped admin handle.
    for sql in [&named_sql, &second_named_sql] {
        let explained: ExplainOutput = db.explain_output(sql).unwrap_or_else(|error| {
            panic!("explain_output must bind nothing: {error}\nSQL: {sql}")
        });
        let disclosure = explained
            .vector_search
            .expect("a vector-similarity SELECT reports a vector disclosure");
        assert_eq!(disclosure.scope, VectorSearchScopeShape::One);
        assert_eq!(
            disclosure.partition_key_columns,
            vec!["scope_id".to_owned()]
        );
    }

    // Grammar order is `ORDER BY ... USE VECTOR ... LIMIT`, so the mode
    // clause is inserted before the trailing `LIMIT 2`, not appended after it.
    let insert_mode_before_limit = |sql: &str, mode_clause: &str| -> String {
        sql.replacen(" LIMIT 2", &format!(" {mode_clause} LIMIT 2"), 1)
    };

    // Leg 4 -- route parity under an explicit mode: a placeholder explain and
    // the same statement with the literal inlined must report the same
    // scope, route, residual, and refusal. AUTO and the empty clause are
    // deliberately excluded: for an unbound key their allowed-vector count is
    // reported as unresolved until bind rather than counted (leg 5), so a
    // placeholder explain has no literal-inlined twin to match.
    for mode_clause in ["USE VECTOR EXACT"] {
        let prepared = explain_passively(&insert_mode_before_limit(&named_sql, mode_clause));
        let literal = explain_passively(&insert_mode_before_limit(&literal_sql, mode_clause));
        for fact in ["scope", "route", "residual", "refusal"] {
            let prepared_value = extract_fact(&prepared, fact);
            let literal_value = extract_fact(&literal, fact);
            assert_eq!(
                prepared_value, literal_value,
                "{fact} must agree between a prepared and a literal-inlined explain under {mode_clause}"
            );
        }
    }

    // Leg 5 -- resolved-mode correctness for an unbound key. With no `USE VECTOR`
    // clause, the query takes the column's own declared mode (`SEARCH_MODE EXACT`,
    // fixture above) regardless of the key being unbound -- this bound case is
    // unchanged. Under an explicit `USE VECTOR AUTO` clause, the planner resolves
    // partition scope only after parameters are bound (partition selection
    // produces one global answer), so an unbound key leaves AUTO
    // unresolved-until-bind rather than resolving it from a count taken over a scope
    // the caller never named -- there is no trustworthy count to derive a route from,
    // so the old count-vs-threshold derivation no longer applies here.
    let declared_mode_explained = db.explain_output(&named_sql).unwrap_or_else(|error| {
        panic!("explain_output must bind nothing: {error}\nSQL: {named_sql}")
    });
    let declared_mode_disclosure = declared_mode_explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        declared_mode_disclosure.resolved_mode,
        VectorSearchMode::Exact,
        "with no USE VECTOR clause, resolved mode follows the column's own declared mode even \
         with an unbound key: {declared_mode_disclosure:?}"
    );

    let explicit_auto_sql = insert_mode_before_limit(&named_sql, "USE VECTOR AUTO");
    let explicit_auto_explained = db
        .explain_output(&explicit_auto_sql)
        .unwrap_or_else(|error| {
            panic!("explain_output must bind nothing: {error}\nSQL: {explicit_auto_sql}")
        });
    let explicit_auto_disclosure = explicit_auto_explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        explicit_auto_disclosure.resolved_mode,
        VectorSearchMode::Auto,
        "an explicit USE VECTOR AUTO over an unbound key must leave the route \
         unresolved-until-bind rather than resolving it to EXACT or INDEXED from a count taken \
         over a scope the caller never named: {explicit_auto_disclosure:?}"
    );
    let explicit_auto_reason = explicit_auto_disclosure
        .fallback
        .as_deref()
        .or(explicit_auto_disclosure.refusal.as_deref())
        .unwrap_or("");
    assert!(
        ["unresolved", "unbound", "unbind", "bind"]
            .iter()
            .any(|needle| explicit_auto_reason.to_lowercase().contains(needle)),
        "an unresolved AUTO route for an unbound partition key must carry a typed reason \
         explaining why: fallback={:?} refusal={:?}",
        explicit_auto_disclosure.fallback,
        explicit_auto_disclosure.refusal
    );

    // Leg 6 -- layer facts are the whole-column facts: an unbound placeholder
    // reports what the whole-column explain reports.
    let unpartitioned_explain = explain_passively(&no_predicate_sql);
    assert!(
        unpartitioned_explain.contains("base=present")
            || unpartitioned_explain.contains("base=absent"),
        "control: the unpartitioned explain reports a base fact: {unpartitioned_explain}"
    );
    for fact in ["base", "change", "tail"] {
        let prepared_value = extract_fact(&named_explain, fact);
        let unpartitioned_value = extract_fact(&unpartitioned_explain, fact);
        assert_eq!(
            prepared_value, unpartitioned_value,
            "{fact} must match the whole-column explain, not render as the empty-prefix facts"
        );
    }

    // Leg 7 -- shape, not identity.
    let in_explain = explain_passively(&in_sql);
    assert_explain_fact(&in_explain, "scope", "several");
    let all_explain = explain_passively(&no_predicate_sql);
    assert_explain_fact(&all_explain, "scope", "all");

    // Leg 8 -- nothing leaks.
    assert_no_secrets(
        &named_explain,
        &secret_strings([SCOPE_ALPHA.to_owned(), SCOPE_BRAVO.to_owned()]),
    );
    assert_no_secrets(
        &second_named_explain,
        &secret_strings([SCOPE_ALPHA.to_owned(), SCOPE_BRAVO.to_owned()]),
    );

    // Leg 9 -- control: execution of the same unbound statement is unchanged.
    let execution_error = db
        .execute(&named_sql, &empty())
        .expect_err("an unbound partition predicate must still fail to execute");
    assert!(
        execution_error
            .to_string()
            .contains("missing parameter: scope"),
        "execution keeps the ordinary missing-parameter refusal: {execution_error}"
    );
    let second_named_execution_error = db
        .execute(&second_named_sql, &empty())
        .expect_err("an unbound second named partition predicate must still fail to execute");
    assert!(
        second_named_execution_error
            .to_string()
            .contains("missing parameter"),
        "second named execution keeps the ordinary missing-parameter refusal: {second_named_execution_error}"
    );
}

/// Pull one `name=value` structural fact out of a rendered `.explain` block.
fn extract_fact<'a>(explain: &'a str, name: &str) -> &'a str {
    let needle = format!("{name}=");
    let start = explain
        .find(&needle)
        .unwrap_or_else(|| panic!("explain must report the structural fact {name:?}: {explain}"))
        + needle.len();
    explain[start..]
        .split(|c: char| c.is_whitespace())
        .next()
        .unwrap_or("")
}

fn restricted_db(indexed: bool, hidden: i64) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE private_vectors (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, embedding VECTOR(3))", &empty()).unwrap();
    if indexed {
        db.execute(
            "CREATE INDEX by_context ON private_vectors(context_id)",
            &empty(),
        )
        .unwrap();
    }
    let visible = Uuid::from_u128(1);
    let tx = db.begin().unwrap();
    for id in 0..=hidden {
        db.execute_in_tx(
            tx,
            "INSERT INTO private_vectors VALUES ($id, $context, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "context",
                    Value::Uuid(if id == 0 { visible } else { Uuid::from_u128(2) }),
                ),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db
}

const RESTRICTED_EXPLAIN: &str =
    "SELECT id FROM private_vectors ORDER BY embedding <=> [1,0,0] USE VECTOR EXACT LIMIT 1";

/// The requested size of the refusal a restricted explain meets with the given
/// headroom above what the store already holds.
fn refused_request(headroom: usize) -> usize {
    let db = restricted_db(false, 0);
    let scoped = db.scoped_with_contexts(BTreeSet::from([ContextId::new(Uuid::from_u128(1))]));
    let resident = db.accountant().usage().used;
    db.set_memory_limit(Some(resident + headroom)).unwrap();
    match scoped.explain_output(RESTRICTED_EXPLAIN) {
        Err(contextdb_core::Error::MemoryBudgetExceeded {
            requested_bytes, ..
        }) => requested_bytes,
        other => panic!("headroom {headroom} must refuse the explain: {other:?}"),
    }
}

/// A restricted explain makes two admissions before it answers: the writer
/// wait control, then the room for the identities its caller may see. Their
/// sizes are read off the refusals they produce, not written down here.
#[test]
#[ignore = "A restricted explain still reveals the real memory limit instead of the caller's headroom."]
fn restricted_explain_admits_only_visible_identities_at_equal_headroom() {
    let control = refused_request(0);
    let reservation = refused_request(control);
    assert!(control > 0 && reservation > control);
    let enough = control + reservation;
    for indexed in [false, true] {
        let mut expected = None;
        for hidden in [0, 64, 2048] {
            let db = restricted_db(indexed, hidden);
            let scoped =
                db.scoped_with_contexts(BTreeSet::from([ContextId::new(Uuid::from_u128(1))]));
            let resident = db.accountant().usage().used;
            let activity = db.__vector_passive_activity_counters_for_test();
            let scans = db.__relational_scan_rows_touched();
            let mut outcomes = Vec::new();
            for headroom in [0, control - 1, control, enough - 1, enough, 1024, 4096] {
                db.set_memory_limit(Some(resident + headroom)).unwrap();
                let result = scoped.explain_output(RESTRICTED_EXPLAIN);
                match result {
                    Ok(explain) => {
                        assert!(headroom >= enough);
                        assert_eq!(
                            explain
                                .vector_search
                                .as_ref()
                                .unwrap()
                                .aggregate_allowed_vectors,
                            Some(1)
                        );
                        outcomes.push(format!("{explain:?}"));
                    }
                    Err(error) => {
                        assert!(
                            headroom < enough,
                            "authorized-only allocation must fit: {error:?}"
                        );
                        // Whichever admission refuses, the caller is told its
                        // own headroom and nothing about the store.
                        let wanted = if headroom < control {
                            control
                        } else {
                            reservation
                        };
                        assert!(
                            matches!(error, contextdb_core::Error::MemoryBudgetExceeded {
                                requested_bytes, available_bytes, budget_limit_bytes, ..
                            } if requested_bytes == wanted
                                && available_bytes == headroom
                                && budget_limit_bytes == headroom),
                            "headroom {headroom}: {error:?}"
                        );
                        outcomes.push(format!("{error:?}"));
                    }
                }
                assert_eq!(db.accountant().usage().used, resident);
            }
            assert_eq!(db.__vector_passive_activity_counters_for_test(), activity);
            assert_eq!(db.__relational_scan_rows_touched(), scans);
            if let Some(expected) = &expected {
                assert_eq!(&outcomes, expected);
            } else {
                expected = Some(outcomes);
            }
        }
    }
}
