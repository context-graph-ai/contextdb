//! Production-SQL coverage for partition-scoped vector queries.
//!
//! The controls use the already-shipped unpartitioned surface. The remaining
//! checks describe the approved partition and per-query mode syntax entirely
//! through SQL, so this test binary continues to compile before the matching
//! Rust mode API is available.

use contextdb_core::{ContextId, Error, Principal, ScopeLabel, TxId, Value};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(0x0067_0000_0000_0000_0000_0000_0000_0000 + value)
}

fn query_vector() -> Vec<f32> {
    vec![1.0, 0.0]
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == name || candidate.rsplit('.').next() == Some(name))
        .unwrap_or_else(|| panic!("column {name} not found in {:?}", result.columns))
}

fn result_ids(result: &QueryResult) -> Vec<Uuid> {
    let position = column(result, "id");
    result
        .rows
        .iter()
        .map(|row| match row.get(position) {
            Some(Value::Uuid(value)) => *value,
            other => panic!("expected UUID id, got {other:?}"),
        })
        .collect()
}

fn result_scores(result: &QueryResult) -> Vec<f64> {
    let position = column(result, "score");
    result
        .rows
        .iter()
        .map(|row| match row.get(position) {
            Some(Value::Float64(value)) => *value,
            other => panic!("expected vector score, got {other:?}"),
        })
        .collect()
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1.0e-6,
        "expected score {expected}, got {actual}"
    );
}

fn insert_unpartitioned(database: &Database, row_id: Uuid, is_current: bool, embedding: Vec<f32>) {
    database
        .execute(
            "INSERT INTO unpartitioned_docs (id, is_current, embedding) \
             VALUES ($id, $is_current, $embedding)",
            &params([
                ("id", Value::Uuid(row_id)),
                ("is_current", Value::Bool(is_current)),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("insert an unpartitioned vector row");
}

#[test]
fn unpartitioned_search_keeps_global_scores_row_id_ties_and_filters() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE unpartitioned_docs (\
             id UUID PRIMARY KEY, \
             is_current BOOL NOT NULL, \
             embedding VECTOR(2))",
            &empty(),
        )
        .expect("declare the unpartitioned control table");

    let tied_inserted_first = id(0x300);
    let winner = id(0x500);
    let tied_inserted_later = id(0x100);
    let far = id(0x200);
    insert_unpartitioned(&database, tied_inserted_first, true, vec![4.0, 3.0]);
    insert_unpartitioned(&database, winner, true, vec![1.0, 0.0]);
    insert_unpartitioned(&database, tied_inserted_later, true, vec![4.0, 3.0]);
    insert_unpartitioned(&database, far, true, vec![0.0, 1.0]);

    let global = database
        .execute(
            "SELECT id, score FROM unpartitioned_docs \
             ORDER BY embedding <=> $query LIMIT 3",
            &params([("query", Value::Vector(query_vector()))]),
        )
        .expect("the unpartitioned global search succeeds");
    assert_eq!(
        result_ids(&global),
        vec![winner, tied_inserted_first, tied_inserted_later],
        "equal vector scores use ascending internal row id, not UUID value"
    );
    let scores = result_scores(&global);
    assert_close(scores[0], 1.0);
    assert_close(scores[1], 0.8);
    assert_eq!(scores[1], scores[2], "identical vectors have equal scores");

    database
        .execute(
            "UPDATE unpartitioned_docs SET is_current = FALSE WHERE id = $id",
            &params([("id", Value::Uuid(winner))]),
        )
        .expect("make the closest row ineligible");
    let filtered = database
        .execute(
            "SELECT id, score FROM unpartitioned_docs WHERE is_current = TRUE \
             ORDER BY embedding <=> $query LIMIT 2",
            &params([("query", Value::Vector(query_vector()))]),
        )
        .expect("the unpartitioned filtered search succeeds");
    assert_eq!(
        result_ids(&filtered),
        vec![tied_inserted_first, tied_inserted_later],
        "the residual filter excludes the closer ineligible row before LIMIT"
    );
}

#[test]
fn semantic_query_new_searches_an_unpartitioned_vector_column() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE semantic_docs (id UUID PRIMARY KEY, embedding VECTOR(2))",
            &empty(),
        )
        .expect("declare the semantic-query control table");
    let nearest = id(0x610);
    for (row_id, embedding) in [(nearest, vec![1.0, 0.0]), (id(0x611), vec![0.0, 1.0])] {
        database
            .execute(
                "INSERT INTO semantic_docs (id, embedding) VALUES ($id, $embedding)",
                &params([
                    ("id", Value::Uuid(row_id)),
                    ("embedding", Value::Vector(embedding)),
                ]),
            )
            .expect("insert a semantic-query control row");
    }

    let query = SemanticQuery::new("semantic_docs", "embedding", query_vector(), 1);
    let results = database
        .semantic_search(query)
        .expect("SemanticQuery::new remains a working unpartitioned call");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.get("id"), Some(&Value::Uuid(nearest)));
    assert_close(f64::from(results[0].vector_score), 1.0);
    assert_eq!(results[0].rank, results[0].vector_score);
}

struct SecureFixture {
    admin: Database,
    allowed_context: Uuid,
    hidden_context: Uuid,
    allowed_scope: ScopeLabel,
    principal: Principal,
    visible: Uuid,
}

fn secure_fixture(vector_declaration: &str) -> SecureFixture {
    let admin = Database::open_memory();
    let allowed_context = id(0x701);
    let hidden_context = id(0x702);
    let allowed_acl = id(0x703);
    let hidden_acl = id(0x704);
    let principal_name = "partition-query-reader";
    let principal = Principal::Agent(principal_name.to_owned());
    let allowed_scope = ScopeLabel::new("visible");

    admin
        .execute(
            "CREATE TABLE acl_grants (\
             id UUID PRIMARY KEY, \
             principal_kind TEXT, \
             principal_id TEXT, \
             acl_id UUID)",
            &empty(),
        )
        .expect("declare the ACL grant table");
    admin
        .execute(
            &format!(
                "CREATE TABLE secure_docs (\
                 id UUID PRIMARY KEY, \
                 context_id UUID NOT NULL CONTEXT_ID, \
                 scope TEXT NOT NULL SCOPE_LABEL_READ ('visible','hidden') \
                     WRITE ('visible','hidden'), \
                 acl_id UUID NOT NULL ACL REFERENCES acl_grants(acl_id), \
                 bucket TEXT NOT NULL, \
                 embedding {vector_declaration})"
            ),
            &empty(),
        )
        .expect("declare the access-controlled vector table");
    admin
        .execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
             VALUES ($id, 'Agent', $principal, $acl)",
            &params([
                ("id", Value::Uuid(id(0x705))),
                ("principal", Value::Text(principal_name.to_owned())),
                ("acl", Value::Uuid(allowed_acl)),
            ]),
        )
        .expect("grant the reader one ACL identity");

    let visible = id(0x710);
    for (row_id, context, scope, acl, embedding) in [
        (
            visible,
            allowed_context,
            "visible",
            allowed_acl,
            vec![4.0, 3.0],
        ),
        (
            id(0x711),
            hidden_context,
            "visible",
            allowed_acl,
            vec![1.0, 0.0],
        ),
        (
            id(0x712),
            allowed_context,
            "hidden",
            allowed_acl,
            vec![1.0, 0.0],
        ),
        (
            id(0x713),
            allowed_context,
            "visible",
            hidden_acl,
            vec![1.0, 0.0],
        ),
    ] {
        admin
            .execute(
                "INSERT INTO secure_docs (id, context_id, scope, acl_id, bucket, embedding) \
                 VALUES ($id, $context, $scope, $acl, $bucket, $embedding)",
                &params([
                    ("id", Value::Uuid(row_id)),
                    ("context", Value::Uuid(context)),
                    ("scope", Value::Text(scope.to_owned())),
                    ("acl", Value::Uuid(acl)),
                    ("bucket", Value::Text("shared".to_owned())),
                    ("embedding", Value::Vector(embedding)),
                ]),
            )
            .expect("insert an access-controlled vector row");
    }

    SecureFixture {
        admin,
        allowed_context,
        hidden_context,
        allowed_scope,
        principal,
        visible,
    }
}

fn secure_search(fixture: &SecureFixture) -> QueryResult {
    let scoped = fixture.admin.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(fixture.allowed_context)])),
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        Some(fixture.principal.clone()),
    );
    scoped
        .execute(
            "SELECT id FROM secure_docs \
             WHERE context_id IN ($allowed_context, $hidden_context) \
               AND bucket = $bucket \
             ORDER BY embedding <=> $query LIMIT 10",
            &params([
                ("allowed_context", Value::Uuid(fixture.allowed_context)),
                ("hidden_context", Value::Uuid(fixture.hidden_context)),
                ("bucket", Value::Text("shared".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("the constrained vector query succeeds")
}

#[test]
fn unpartitioned_search_keeps_context_scope_and_principal_filters() {
    let fixture = secure_fixture("VECTOR(2)");
    assert_eq!(
        result_ids(&secure_search(&fixture)),
        vec![fixture.visible],
        "rows hidden by Context, scope, or principal stay outside the answer"
    );
}

struct PartitionFixture {
    database: Database,
    context_a: Uuid,
    context_b: Uuid,
    context_c: Uuid,
    a_note: Uuid,
    a_code: Uuid,
    b_note: Uuid,
    c_note: Uuid,
}

fn insert_partitioned(
    database: &Database,
    row_id: Uuid,
    context: Uuid,
    kind: &str,
    is_current: bool,
    embedding: Vec<f32>,
) {
    database
        .execute(
            "INSERT INTO partitioned_docs \
             (id, context_id, kind, is_current, embedding) \
             VALUES ($id, $context, $kind, $is_current, $embedding)",
            &params([
                ("id", Value::Uuid(row_id)),
                ("context", Value::Uuid(context)),
                ("kind", Value::Text(kind.to_owned())),
                ("is_current", Value::Bool(is_current)),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("insert a partitioned vector row");
}

fn partition_fixture() -> PartitionFixture {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE partitioned_docs (\
             id UUID PRIMARY KEY, \
             context_id UUID NOT NULL, \
             kind TEXT NOT NULL, \
             is_current BOOL NOT NULL, \
             embedding VECTOR(2) \
                 PARTITION_KEY (context_id, kind) \
                 MAX_PARTITIONS 8 \
                 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare a composite partitioned vector column");

    let context_a = id(0x801);
    let context_b = id(0x802);
    let context_c = id(0x803);
    let c_note = id(0x830);
    let b_note = id(0x810);
    let a_note = id(0x850);
    let a_code = id(0x820);

    // The equal-score C row is inserted before the B row even though its UUID
    // sorts later. Their answer order therefore proves the row-id tie rule.
    insert_partitioned(&database, c_note, context_c, "note", true, vec![4.0, 3.0]);
    insert_partitioned(&database, b_note, context_b, "note", true, vec![4.0, 3.0]);
    insert_partitioned(&database, a_note, context_a, "note", true, vec![1.0, 0.0]);
    insert_partitioned(&database, a_code, context_a, "code", true, vec![3.0, 4.0]);
    insert_partitioned(
        &database,
        id(0x840),
        context_a,
        "note",
        false,
        vec![1.0, 0.0],
    );

    PartitionFixture {
        database,
        context_a,
        context_b,
        context_c,
        a_note,
        a_code,
        b_note,
        c_note,
    }
}

#[test]
fn equality_on_every_partition_component_selects_one_named_tuple() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE context_id = $context AND kind = $kind AND is_current = TRUE \
             ORDER BY embedding <=> $query LIMIT 2",
            &params([
                ("context", Value::Uuid(fixture.context_a)),
                ("kind", Value::Text("note".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("search one named partition tuple");
    assert_eq!(result_ids(&result), vec![fixture.a_note]);
}

#[cfg(feature = "test-seams")]
#[test]
fn a_covering_partition_scope_skips_a_redundant_relational_candidate_index() {
    let fixture = partition_fixture();
    fixture
        .database
        .execute(
            "CREATE INDEX partitioned_scope_idx ON partitioned_docs(context_id, kind)",
            &empty(),
        )
        .expect("declare the otherwise usable relational candidate index");
    fixture.database.__reset_relational_scan_rows_touched();
    fixture.database.__reset_relational_index_entries_touched();

    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE context_id = $context AND kind = $kind \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10",
            &params([
                ("context", Value::Uuid(fixture.context_a)),
                ("kind", Value::Text("note".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("search the fully bound partition without a redundant candidate walk");

    assert_eq!(
        result_ids(&result).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([fixture.a_note, id(0x840)]),
        "the selected partition still supplies every matching row"
    );
    assert_eq!(
        fixture.database.__relational_scan_rows_touched(),
        0,
        "a covering partition scope must not scan the table"
    );
    assert_eq!(
        fixture.database.__relational_index_entries_touched(),
        0,
        "a covering partition scope must not walk a redundant relational candidate index"
    );
}

#[test]
fn finite_in_partition_scope_merges_before_one_limit() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE context_id IN ($context_a, $context_b) \
               AND kind = $kind AND is_current = TRUE \
             ORDER BY embedding <=> $query LIMIT 2",
            &params([
                ("context_a", Value::Uuid(fixture.context_a)),
                ("context_b", Value::Uuid(fixture.context_b)),
                ("kind", Value::Text("note".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("search a finite partition set");
    assert_eq!(
        result_ids(&result),
        vec![fixture.a_note, fixture.b_note],
        "LIMIT applies after candidates from both named tuples compete"
    );
}

#[test]
fn equivalent_or_partition_scope_keeps_cross_partition_row_id_ties() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE is_current = TRUE AND (\
                 (context_id = $context_b AND kind = $kind) OR \
                 (context_id = $context_c AND kind = $kind)) \
             ORDER BY embedding <=> $query LIMIT 2",
            &params([
                ("context_b", Value::Uuid(fixture.context_b)),
                ("context_c", Value::Uuid(fixture.context_c)),
                ("kind", Value::Text("note".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("search the equivalent finite OR scope");
    assert_eq!(
        result_ids(&result),
        vec![fixture.c_note, fixture.b_note],
        "partition iteration and UUID order do not replace the row-id tie rule"
    );
}

#[test]
fn composite_partition_prefix_selects_every_tuple_below_the_prefix() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE context_id = $context AND is_current = TRUE \
             ORDER BY embedding <=> $query LIMIT 2",
            &params([
                ("context", Value::Uuid(fixture.context_a)),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("search every tuple under a composite-key prefix");
    assert_eq!(
        result_ids(&result),
        vec![fixture.a_note, fixture.a_code],
        "the prefix includes both kind tuples and ranks them together"
    );
}

#[test]
fn no_partition_key_predicate_returns_one_global_limited_answer() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs WHERE is_current = TRUE \
             ORDER BY embedding <=> $query LIMIT 3",
            &params([("query", Value::Vector(query_vector()))]),
        )
        .expect("search all local partition tuples");
    assert_eq!(
        result_ids(&result),
        vec![fixture.a_note, fixture.c_note, fixture.b_note],
        "all tuples contribute to one score order and one final LIMIT"
    );
}

#[test]
fn missing_named_partition_tuple_is_an_empty_answer() {
    let fixture = partition_fixture();
    let result = fixture
        .database
        .execute(
            "SELECT id FROM partitioned_docs \
             WHERE context_id = $context AND kind = $kind \
             ORDER BY embedding <=> $query LIMIT 5",
            &params([
                ("context", Value::Uuid(id(0x8ff))),
                ("kind", Value::Text("note".to_owned())),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("a missing named tuple is a complete empty search");
    assert!(
        result.rows.is_empty(),
        "a missing tuple never broadens scope"
    );
}

#[test]
fn partition_scope_does_not_weaken_context_scope_or_principal_filters() {
    let fixture = secure_fixture(
        "VECTOR(2) \
         PARTITION_KEY (context_id, bucket) \
         MAX_PARTITIONS 8 \
         SEARCH_MODE AUTO",
    );
    assert_eq!(
        result_ids(&secure_search(&fixture)),
        vec![fixture.visible],
        "physical tuple selection never admits a row hidden by Context, scope, or principal"
    );
}

/// Which restriction leg hides the closer row: the reader's Context set, the
/// reader's read-labelled scope set, or the reader's ACL grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HiddenBy {
    Context,
    Scope,
    Principal,
}

struct HiddenRowFixture {
    admin: Database,
    allowed_context: Uuid,
    hidden_context: Uuid,
    allowed_scope: ScopeLabel,
    principal: Principal,
    authorized_ids: Vec<Uuid>,
}

impl HiddenRowFixture {
    fn scoped(&self) -> Database {
        self.admin.scoped_with_constraints(
            Some(BTreeSet::from([ContextId::new(self.allowed_context)])),
            Some(BTreeSet::from([self.allowed_scope.clone()])),
            Some(self.principal.clone()),
        )
    }

    fn sql(&self) -> &'static str {
        "SELECT id FROM hidden_row_docs \
         WHERE context_id IN ($allowed_context, $hidden_context) AND bucket = $bucket \
         ORDER BY embedding <=> $query LIMIT 1"
    }

    fn query_params(&self) -> HashMap<String, Value> {
        params([
            ("allowed_context", Value::Uuid(self.allowed_context)),
            ("hidden_context", Value::Uuid(self.hidden_context)),
            ("bucket", Value::Text("shared".to_owned())),
            ("query", Value::Vector(query_vector())),
        ])
    }
}

/// Two rows: the VISIBLE row is farther from the query than the row hidden by
/// `hidden_by`, so `LIMIT 1` is a genuine contest between the visible row and
/// a row that must never be returned. A reader that applies its restriction
/// only after selecting the top-k drops the visible row's own slot to the
/// hidden row and answers empty; a reader that applies the restriction first
/// answers with the visible row.
fn secure_fixture_hidden_by(vector_declaration: &str, hidden_by: HiddenBy) -> HiddenRowFixture {
    secure_fixture_with_restrictions(vector_declaration, hidden_by, true)
}

fn secure_context_fixture(vector_declaration: &str) -> HiddenRowFixture {
    secure_fixture_with_restrictions(vector_declaration, HiddenBy::Context, false)
}

fn secure_fixture_with_restrictions(
    vector_declaration: &str,
    hidden_by: HiddenBy,
    residual: bool,
) -> HiddenRowFixture {
    secure_fixture_in_database(
        Database::open_memory(),
        vector_declaration,
        hidden_by,
        residual,
    )
}

fn secure_fixture_in_database(
    admin: Database,
    vector_declaration: &str,
    hidden_by: HiddenBy,
    residual: bool,
) -> HiddenRowFixture {
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let allowed_context = id(0x901);
    let hidden_context = id(0x902);
    let allowed_acl = id(0x903);
    let hidden_acl = id(0x904);
    let principal_name = "hidden-row-reader";
    let principal = Principal::Agent(principal_name.to_owned());
    let allowed_scope = ScopeLabel::new("visible");

    admin
        .execute(
            "CREATE TABLE hidden_acl_grants (\
             id UUID PRIMARY KEY, \
             principal_kind TEXT, \
             principal_id TEXT, \
             acl_id UUID)",
            &empty(),
        )
        .expect("declare the ACL grant table");
    let residual_declarations = if residual {
        "scope TEXT NOT NULL SCOPE_LABEL_READ ('visible','hidden') WRITE ('visible','hidden'), acl_id UUID NOT NULL ACL REFERENCES hidden_acl_grants(acl_id),"
    } else {
        "scope TEXT NOT NULL, acl_id UUID NOT NULL,"
    };
    admin
        .execute(
            &format!(
                "CREATE TABLE hidden_row_docs (\
                 id UUID PRIMARY KEY, \
                 context_id UUID NOT NULL CONTEXT_ID, \
                 {residual_declarations} \
                 bucket TEXT NOT NULL, \
                 embedding {vector_declaration})"
            ),
            &empty(),
        )
        .expect("declare the hidden-row vector table");
    admin
        .execute(
            "INSERT INTO hidden_acl_grants (id, principal_kind, principal_id, acl_id) \
             VALUES ($id, 'Agent', $principal, $acl)",
            &params([
                ("id", Value::Uuid(id(0x905))),
                ("principal", Value::Text(principal_name.to_owned())),
                ("acl", Value::Uuid(allowed_acl)),
            ]),
        )
        .expect("grant the reader one ACL identity");

    let visible = id(0x910);
    let hidden = id(0x911);
    let (hidden_context_value, hidden_scope_value, hidden_acl_value) = match hidden_by {
        HiddenBy::Context => (hidden_context, "visible", allowed_acl),
        HiddenBy::Scope => (allowed_context, "hidden", allowed_acl),
        HiddenBy::Principal => (allowed_context, "visible", hidden_acl),
    };
    for (row_id, context, scope, acl, embedding) in [
        (
            visible,
            allowed_context,
            "visible",
            allowed_acl,
            vec![4.0, 3.0],
        ),
        (
            hidden,
            hidden_context_value,
            hidden_scope_value,
            hidden_acl_value,
            vec![1.0, 0.0],
        ),
    ] {
        admin
            .execute(
                "INSERT INTO hidden_row_docs (id, context_id, scope, acl_id, bucket, embedding) \
                 VALUES ($id, $context, $scope, $acl, $bucket, $embedding)",
                &params([
                    ("id", Value::Uuid(row_id)),
                    ("context", Value::Uuid(context)),
                    ("scope", Value::Text(scope.to_owned())),
                    ("acl", Value::Uuid(acl)),
                    ("bucket", Value::Text("shared".to_owned())),
                    ("embedding", Value::Vector(embedding)),
                ]),
            )
            .expect("insert a hidden-row fixture row");
    }
    // Finite, caller-driven maintenance so an `INDEXED` declaration has a
    // ready graph; this is not a timing wait (`MaintenancePolicy::CallerDriven`
    // above disables the background maintenance thread `Database::open_memory`
    // otherwise defaults to).
    for _ in 0..32 {
        admin
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance batch succeeds");
    }

    HiddenRowFixture {
        admin,
        allowed_context,
        hidden_context,
        allowed_scope,
        principal,
        authorized_ids: vec![visible],
    }
}

fn empty_mode_fixture() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE mode_docs (id UUID PRIMARY KEY, embedding VECTOR(2))",
            &empty(),
        )
        .expect("declare the empty mode-syntax fixture");
    database
}

fn assert_empty_mode_query(sql: &str) {
    let database = empty_mode_fixture();
    let result = database
        .execute(sql, &params([("query", Value::Vector(query_vector()))]))
        .expect("the query-level vector mode parses and executes");
    assert!(
        result.rows.is_empty(),
        "an empty vector column is a complete empty answer under every mode"
    );
}

#[test]
fn use_vector_auto_parses_between_vector_ordering_and_limit() {
    assert_empty_mode_query(
        "SELECT id FROM mode_docs \
         ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 1",
    );
}

#[test]
fn use_vector_exact_parses_between_vector_ordering_and_limit() {
    assert_empty_mode_query(
        "SELECT id FROM mode_docs \
         ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
    );
}

#[test]
fn use_vector_indexed_parses_between_vector_ordering_and_limit() {
    assert_empty_mode_query(
        "SELECT id FROM mode_docs \
         ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
    );
}

#[test]
fn every_use_vector_mode_refuses_a_non_vector_ordering_query() {
    let database = empty_mode_fixture();
    for mode in ["AUTO", "EXACT", "INDEXED"] {
        let sql = format!("SELECT id FROM mode_docs USE VECTOR {mode} LIMIT 1");
        let refusal = database
            .execute(&sql, &empty())
            .expect_err("USE VECTOR without nearest-neighbour ordering must refuse");
        assert!(
            matches!(refusal, Error::UseVectorRequiresVectorOrder),
            "{mode} must use the exact non-vector-query refusal, got {refusal:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Partition-key literals follow SQL equality coercion.
//
// A partition-scoped vector search must return the same rows as the same
// `WHERE key = <literal>` without the vector ordering, for every literal
// spelling ordinary `=` accepts at the declared key type. A literal fed to
// the typed key encoder exactly as written would select an empty scope while
// the covered row filter is skipped, returning zero rows silently. Legs
// that describe a provably empty scope, and the write-side refusal legs whose
// value type the encoder cannot represent at all, are noted individually.
// ---------------------------------------------------------------------------

const TYPED_KEY_QUERY_VECTOR: &str = "[1,0]";

fn tid(offset: u128) -> Uuid {
    id(0x0900_0000 + offset)
}

struct TypedKeyFixture {
    database: Database,
    uuid_alpha: Uuid,
    ts_alpha_typed: Uuid,
    ts_alpha_int: Uuid,
    ts_alpha_ms: i64,
    int_zero: Uuid,
    int_two_pow_53: Uuid,
    int_two_pow_53_plus_one: Uuid,
    tx_first_value: TxId,
}

fn insert_typed_key_row(
    database: &Database,
    table: &str,
    key_column: &str,
    row_id: Uuid,
    key_value: Value,
    embedding: Vec<f32>,
) {
    database
        .execute(
            &format!(
                "INSERT INTO {table} (id, {key_column}, embedding) \
                 VALUES ($id, $key, $embedding)"
            ),
            &params([
                ("id", Value::Uuid(row_id)),
                ("key", key_value),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .unwrap_or_else(|error| panic!("insert into {table}.{key_column} must succeed: {error}"));
}

/// Drives `committed_watermark()` past `at_least` through ordinary commits on
/// a disposable control table, so a bound `Value::TxId` below the watermark
/// is admissible on the TXID-keyed fixture table.
fn bump_committed_watermark_past(database: &Database, at_least: u64) {
    database
        .execute(
            "CREATE TABLE typed_key_watermark_bump (id UUID PRIMARY KEY, n INTEGER)",
            &empty(),
        )
        .expect("declare the watermark-bump control table");
    let mut n: i64 = 0;
    while database.committed_watermark().0 < at_least {
        database
            .execute(
                "INSERT INTO typed_key_watermark_bump (id, n) VALUES ($id, $n)",
                &params([("id", Value::Uuid(Uuid::new_v4())), ("n", Value::Int64(n))]),
            )
            .expect("watermark-bump insert must succeed");
        n += 1;
    }
}

/// One `Database::open_memory()` with four partitioned tables, one per
/// declared key type, so each type's coercion is exercised independently.
fn typed_key_fixture() -> TypedKeyFixture {
    let database = Database::open_memory();
    bump_committed_watermark_past(&database, 10);

    database
        .execute(
            "CREATE TABLE uuid_keyed (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare the UUID-keyed partitioned table");
    database
        .execute(
            "CREATE TABLE int_keyed (\
             id UUID PRIMARY KEY, \
             bucket INTEGER NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (bucket) MAX_PARTITIONS 16 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare the INTEGER-keyed partitioned table");
    database
        .execute(
            "CREATE TABLE ts_keyed (\
             id UUID PRIMARY KEY, \
             stamped TIMESTAMP NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (stamped) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare the TIMESTAMP-keyed partitioned table");
    database
        .execute(
            "CREATE TABLE txid_keyed (\
             id UUID PRIMARY KEY, \
             at_tx TXID NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (at_tx) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare the TXID-keyed partitioned table");

    let uuid_alpha = tid(0x101);
    let uuid_bravo = tid(0x102);
    insert_typed_key_row(
        &database,
        "uuid_keyed",
        "scope_id",
        tid(0x110),
        Value::Uuid(uuid_alpha),
        vec![1.0, 0.0],
    );
    insert_typed_key_row(
        &database,
        "uuid_keyed",
        "scope_id",
        tid(0x111),
        Value::Uuid(uuid_alpha),
        vec![0.9, 0.1],
    );
    insert_typed_key_row(
        &database,
        "uuid_keyed",
        "scope_id",
        tid(0x120),
        Value::Uuid(uuid_bravo),
        vec![0.0, 1.0],
    );
    insert_typed_key_row(
        &database,
        "uuid_keyed",
        "scope_id",
        tid(0x121),
        Value::Uuid(uuid_bravo),
        vec![0.1, 0.9],
    );

    let int_zero = tid(0x212);
    let int_two_pow_53 = tid(0x213);
    let int_two_pow_53_plus_one = tid(0x214);
    insert_typed_key_row(
        &database,
        "int_keyed",
        "bucket",
        tid(0x210),
        Value::Int64(7),
        vec![1.0, 0.0],
    );
    insert_typed_key_row(
        &database,
        "int_keyed",
        "bucket",
        tid(0x211),
        Value::Int64(8),
        vec![0.0, 1.0],
    );
    insert_typed_key_row(
        &database,
        "int_keyed",
        "bucket",
        int_zero,
        Value::Int64(0),
        vec![1.0, 1.0],
    );
    insert_typed_key_row(
        &database,
        "int_keyed",
        "bucket",
        int_two_pow_53,
        Value::Int64(9_007_199_254_740_992),
        vec![1.0, 0.0],
    );
    insert_typed_key_row(
        &database,
        "int_keyed",
        "bucket",
        int_two_pow_53_plus_one,
        Value::Int64(9_007_199_254_740_993),
        vec![0.9, 0.1],
    );

    let ts_alpha_ms: i64 = 1_700_000_000_000;
    let ts_bravo_ms: i64 = 1_700_000_100_000;
    let ts_alpha_typed = tid(0x310);
    let ts_alpha_int = tid(0x311);
    insert_typed_key_row(
        &database,
        "ts_keyed",
        "stamped",
        ts_alpha_typed,
        Value::Timestamp(ts_alpha_ms),
        vec![1.0, 0.0],
    );
    insert_typed_key_row(
        &database,
        "ts_keyed",
        "stamped",
        ts_alpha_int,
        Value::Int64(ts_alpha_ms),
        vec![0.9, 0.1],
    );
    insert_typed_key_row(
        &database,
        "ts_keyed",
        "stamped",
        tid(0x312),
        Value::Timestamp(ts_bravo_ms),
        vec![0.0, 1.0],
    );

    let tx_first_value = TxId(2);
    let tx_second_value = TxId(4);
    insert_typed_key_row(
        &database,
        "txid_keyed",
        "at_tx",
        tid(0x410),
        Value::TxId(tx_first_value),
        vec![1.0, 0.0],
    );
    insert_typed_key_row(
        &database,
        "txid_keyed",
        "at_tx",
        tid(0x411),
        Value::TxId(tx_second_value),
        vec![0.0, 1.0],
    );

    TypedKeyFixture {
        database,
        uuid_alpha,
        ts_alpha_typed,
        ts_alpha_int,
        ts_alpha_ms,
        int_zero,
        int_two_pow_53,
        int_two_pow_53_plus_one,
        tx_first_value,
    }
}

fn sorted(mut ids: Vec<Uuid>) -> Vec<Uuid> {
    ids.sort();
    ids
}

fn plain_equality_ids(database: &Database, table: &str, predicate: &str) -> Vec<Uuid> {
    let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
    let result = database
        .execute(&sql, &empty())
        .unwrap_or_else(|error| panic!("the plain `=` control must succeed: {sql}: {error}"));
    sorted(result_ids(&result))
}

fn semantic_search_ids(database: &Database, table: &str, predicate: &str) -> Vec<Uuid> {
    let query = SemanticQuery {
        where_clause: Some(predicate.to_owned()),
        ..SemanticQuery::new(table, "embedding", vec![1.0, 0.0], 8)
    };
    let results = database.semantic_search(query).unwrap_or_else(|error| {
        panic!("SemanticQuery scoped search must succeed for {predicate} on {table}: {error}")
    });
    sorted(
        results
            .into_iter()
            .map(|result| match result.values.get("id") {
                Some(Value::Uuid(id)) => *id,
                other => panic!("SemanticQuery result id is UUID, got {other:?}"),
            })
            .collect(),
    )
}

/// Asserts the ordinary reader and the `SemanticQuery` door both agree with
/// the plain, non-vector `=` answer for one partition-key predicate spelling.
/// The bounded-reader door for the same predicates is proved separately in
/// `bounded_door` below (that door needs the `test-seams` feature; this file
/// must keep compiling without it, per the proof plan).
fn assert_ordinary_and_semantic_doors_match_plain_equality(
    database: &Database,
    table: &str,
    predicate: &str,
) {
    let expected = plain_equality_ids(database, table, predicate);
    let vector_sql = format!(
        "SELECT id FROM {table} WHERE {predicate} \
         ORDER BY embedding <=> {TYPED_KEY_QUERY_VECTOR} LIMIT 8"
    );
    let ordinary = database
        .execute(&vector_sql, &empty())
        .unwrap_or_else(|error| {
            panic!("the ordinary scoped vector search must succeed: {vector_sql}: {error}")
        });
    assert_eq!(
        sorted(result_ids(&ordinary)),
        expected,
        "ordinary reader must equal the plain `=` control: {vector_sql}"
    );
    assert_eq!(
        semantic_search_ids(database, table, predicate),
        expected,
        "SemanticQuery must equal the plain `=` control for predicate {predicate:?} on {table}"
    );
}

#[test]
fn typed_partition_key_literals_match_plain_equality_across_declared_key_types() {
    let fixture = typed_key_fixture();
    let db = &fixture.database;

    // UUID key <- quoted text literal, the documented cg spelling.
    assert_ordinary_and_semantic_doors_match_plain_equality(
        db,
        "uuid_keyed",
        &format!("scope_id = '{}'", fixture.uuid_alpha),
    );

    // UUID key <- a bound $scope carrying Value::Text (ordinary door only:
    // SemanticQuery's where_clause is resolved with an empty params map, so
    // it cannot exercise a bound spelling).
    let bound_sql = format!(
        "SELECT id FROM uuid_keyed WHERE scope_id = $scope \
         ORDER BY embedding <=> {TYPED_KEY_QUERY_VECTOR} LIMIT 8"
    );
    let expected = plain_equality_ids(
        db,
        "uuid_keyed",
        &format!("scope_id = '{}'", fixture.uuid_alpha),
    );
    let bound_result = db
        .execute(
            &bound_sql,
            &params([("scope", Value::Text(fixture.uuid_alpha.to_string()))]),
        )
        .expect("a bound Value::Text scope must be coerced to the declared UUID key type");
    assert_eq!(
        sorted(result_ids(&bound_result)),
        expected,
        "a bound Value::Text UUID spelling must equal the plain `=` control"
    );

    // INTEGER key <- a REAL literal.
    assert_ordinary_and_semantic_doors_match_plain_equality(db, "int_keyed", "bucket = 7.0");
    // TIMESTAMP key <- an integer literal.
    assert_ordinary_and_semantic_doors_match_plain_equality(
        db,
        "ts_keyed",
        &format!("stamped = {}", fixture.ts_alpha_ms),
    );
    // TXID key <- an integer literal.
    assert_ordinary_and_semantic_doors_match_plain_equality(
        db,
        "txid_keyed",
        &format!("at_tx = {}", fixture.tx_first_value.0),
    );
}

#[test]
fn explain_names_the_same_one_partition_a_typed_literal_selects() {
    let fixture = typed_key_fixture();
    let db = &fixture.database;
    let sql = format!(
        "SELECT id FROM uuid_keyed WHERE scope_id = '{}' \
         ORDER BY embedding <=> {TYPED_KEY_QUERY_VECTOR} LIMIT 8",
        fixture.uuid_alpha
    );
    let explained = db
        .explain_output(&sql)
        .unwrap_or_else(|error| panic!("explain must succeed: {sql}: {error}"));
    let disclosure = explained
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    assert_eq!(
        disclosure.scope,
        contextdb_engine::VectorSearchScopeShape::One
    );
    assert_eq!(
        disclosure.partition_hnsw.len(),
        1,
        "a UUID-typed literal scope must select the one stored partition it names, \
         not zero: {disclosure:?}"
    );
}

#[test]
fn a_literal_no_key_of_the_declared_type_can_equal_is_an_empty_answer_not_an_error() {
    let fixture = typed_key_fixture();
    let db = &fixture.database;

    for (table, predicate) in [
        ("uuid_keyed", "scope_id = 'not-a-uuid'"),
        ("ts_keyed", "stamped = '2024-01-01T00:00:00Z'"),
        ("txid_keyed", "at_tx = -1"),
        ("int_keyed", "bucket = 7.5"),
    ] {
        let expected = plain_equality_ids(db, table, predicate);
        assert!(
            expected.is_empty(),
            "control premise: {table} {predicate} must be empty under plain `=`"
        );
        assert_ordinary_and_semantic_doors_match_plain_equality(db, table, predicate);
        let explain_sql = format!(
            "SELECT id FROM {table} WHERE {predicate} \
             ORDER BY embedding <=> {TYPED_KEY_QUERY_VECTOR} LIMIT 8"
        );
        db.explain(&explain_sql).unwrap_or_else(|error| {
            panic!("a provably empty scope must explain without error: {explain_sql}: {error}")
        });
    }
}

#[test]
fn negative_zero_does_not_over_match_and_two_pow_53_does_not_under_match() {
    let fixture = typed_key_fixture();
    let db = &fixture.database;

    // -0.0 must not match the stored INTEGER 0: `total_cmp` orders -0.0
    // below +0.0, so a scoped search that returned the 0 row would
    // contradict the plain `=` control.
    let expected_zero = plain_equality_ids(db, "int_keyed", "bucket = -0.0");
    assert!(
        expected_zero.is_empty(),
        "control premise: plain `=` must exclude bucket 0 for a -0.0 literal"
    );
    assert_ordinary_and_semantic_doors_match_plain_equality(db, "int_keyed", "bucket = -0.0");

    // A float literal in the >= 2^53 band names two possible INTEGER
    // partitions; no narrowing is safe there, so the ordinary row filter
    // must decide and both rows must come back, matching plain `=`.
    let expected_band = plain_equality_ids(db, "int_keyed", "bucket = 9007199254740992.0");
    assert_eq!(
        expected_band,
        sorted(vec![
            fixture.int_two_pow_53,
            fixture.int_two_pow_53_plus_one
        ]),
        "control premise: both 2^53 neighbours are `=`-equal to that float literal"
    );
    assert_ordinary_and_semantic_doors_match_plain_equality(
        db,
        "int_keyed",
        "bucket = 9007199254740992.0",
    );
}

#[test]
fn a_text_partition_key_predicate_stays_case_exact() {
    let fixture = partition_fixture();
    let db = &fixture.database;
    let upper = db
        .execute(
            "SELECT id FROM partitioned_docs WHERE context_id = $context AND kind = 'NOTE' \
             ORDER BY embedding <=> $query LIMIT 5",
            &params([
                ("context", Value::Uuid(fixture.context_a)),
                ("query", Value::Vector(query_vector())),
            ]),
        )
        .expect("an uppercase spelling must explain as a provably empty scope, not an error");
    assert!(
        upper.rows.is_empty(),
        "the TEXT partition encoder must not lowercase: 'NOTE' must not reach the stored 'note' partition"
    );
    db.explain(
        "SELECT id FROM partitioned_docs WHERE kind = 'NOTE' \
         ORDER BY embedding <=> [1,0] LIMIT 5",
    )
    .expect("a case-mismatched TEXT literal must explain as an empty scope, not error");
}

#[test]
fn both_write_spellings_of_one_instant_land_in_one_partition() {
    let fixture = typed_key_fixture();
    let db = &fixture.database;
    let partitions = db
        .execute("SHOW VECTOR_PARTITIONS FOR ts_keyed.embedding", &empty())
        .expect("SHOW VECTOR_PARTITIONS is an admin inspection surface");
    assert_eq!(
        partitions.rows.len(),
        2,
        "the bound-Timestamp row and the integer-written row at the same instant must \
         collapse into ONE stored partition (alongside the second, distinct instant), \
         not split the instant into two: {partitions:?}"
    );
    let both = plain_equality_ids(
        db,
        "ts_keyed",
        &format!("stamped = {}", fixture.ts_alpha_ms),
    );
    assert_eq!(
        both,
        sorted(vec![fixture.ts_alpha_typed, fixture.ts_alpha_int]),
        "control premise: both write spellings answer to the same plain `=` predicate"
    );
    assert_ordinary_and_semantic_doors_match_plain_equality(
        db,
        "ts_keyed",
        &format!("stamped = {}", fixture.ts_alpha_ms),
    );
}

#[test]
fn a_key_value_of_the_wrong_declared_type_is_refused_not_silently_partitioned() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE text_keyed_docs (\
         id UUID PRIMARY KEY, \
         label TEXT NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (label) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &empty(),
    )
    .expect("declare the TEXT-keyed table");
    let stray_uuid = Uuid::from_u128(0x0900_0000_0000_0000_0000_0000_0000_0500);
    let row_id = Uuid::from_u128(0x0900_0000_0000_0000_0000_0000_0000_0501);
    let error = db
        .execute(
            "INSERT INTO text_keyed_docs (id, label, embedding) VALUES ($id, $label, $embedding)",
            &params([
                ("id", Value::Uuid(row_id)),
                ("label", Value::Uuid(stray_uuid)),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect_err("a UUID value into a declared TEXT partition key must be refused");
    match &error {
        Error::VectorPartitionKeyValueTypeMismatch { column, .. } => {
            assert_eq!(
                column, "label",
                "the refusal must name the key COLUMN, not the declaration: {error:?}"
            );
        }
        other => panic!("expected VectorPartitionKeyValueTypeMismatch, got {other:?}"),
    }
    let message = error.to_string();
    assert!(
        !message.contains(&stray_uuid.to_string()),
        "the message must never echo the caller's offending VALUE (error-secrecy rule): {message}"
    );
    let after = db
        .execute("SELECT id FROM text_keyed_docs", &empty())
        .expect("select must still succeed");
    assert!(after.rows.is_empty(), "the refused row must be absent");
    let partitions = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR text_keyed_docs.embedding",
            &empty(),
        )
        .expect("partition inspection must still succeed");
    assert!(
        partitions.rows.is_empty(),
        "no stray partition of the wrong component type may exist: {partitions:?}"
    );

    db.execute(
        "CREATE TABLE int_id_keyed_docs (\
         id UUID PRIMARY KEY, \
         bucket_id INTEGER NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (bucket_id) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &empty(),
    )
    .expect("declare the INTEGER-keyed table with an `_id`-suffixed column");
    let row_id_2 = Uuid::from_u128(0x0900_0000_0000_0000_0000_0000_0000_0502);
    let error_2 = db
        .execute(
            "INSERT INTO int_id_keyed_docs (id, bucket_id, embedding) \
             VALUES ($id, $bucket_id, $embedding)",
            &params([
                ("id", Value::Uuid(row_id_2)),
                ("bucket_id", Value::Text(stray_uuid.to_string())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect_err(
            "a text UUID that `coerce_uuid_if_needed` turns into a Uuid component for an \
             `_id`-suffixed INTEGER-declared key must also be refused, not silently partitioned",
        );
    match &error_2 {
        Error::VectorPartitionKeyValueTypeMismatch { column, .. } => {
            assert_eq!(
                column, "bucket_id",
                "the refusal must name the key COLUMN, not the declaration: {error_2:?}"
            );
        }
        other => panic!("expected VectorPartitionKeyValueTypeMismatch, got {other:?}"),
    }
    let message_2 = error_2.to_string();
    assert!(
        !message_2.contains(&stray_uuid.to_string()),
        "the message must never echo the caller's offending VALUE (error-secrecy rule): \
         {message_2}"
    );
    let partitions_2 = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR int_id_keyed_docs.embedding",
            &empty(),
        )
        .expect("partition inspection must still succeed");
    assert!(
        partitions_2.rows.is_empty(),
        "no stray Uuid-typed partition may exist under a declared INTEGER key: {partitions_2:?}"
    );
}

/// A developer's PARTITION_KEY declaration on `bucket_code` is fine; only the
/// VALUE a caller inserted is wrong (a TEXT literal for a column declared
/// INTEGER). The refusal must say so with its own typed shape distinct from
/// `InvalidVectorPartitionDeclaration` -- the declaration error names a
/// structural problem with the CREATE TABLE the developer would search in
/// vain -- and, per the error-secrecy rule, the message names the column and
/// never echoes the caller's offending value.
#[test]
fn a_partition_key_value_of_the_wrong_type_gets_its_own_typed_refusal_naming_the_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE bucket_typed_docs (\
         id UUID PRIMARY KEY, \
         bucket_code INTEGER NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (bucket_code) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &empty(),
    )
    .expect("declare the INTEGER-keyed table; the declaration itself is valid");
    let row_id = Uuid::from_u128(0x0900_0000_0000_0000_0000_0000_0000_0600);
    let offending_value = "forty-two-not-an-integer";
    let error = db
        .execute(
            "INSERT INTO bucket_typed_docs (id, bucket_code, embedding) \
             VALUES ($id, $bucket_code, $embedding)",
            &params([
                ("id", Value::Uuid(row_id)),
                ("bucket_code", Value::Text(offending_value.to_owned())),
                ("embedding", Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect_err("a TEXT value into a declared INTEGER partition key must be refused");
    assert!(
        !matches!(error, Error::InvalidVectorPartitionDeclaration { .. }),
        "the column's PARTITION_KEY declaration is fine; only the inserted VALUE is wrong, so \
         this refusal must not be reported as an invalid declaration (that sends the developer \
         to inspect a CREATE TABLE where there is nothing to find): {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("bucket_code"),
        "the message must name the key COLUMN so the developer knows where to look: {message}"
    );
    assert!(
        !message.contains(offending_value),
        "the message must never echo the caller's offending VALUE (error-secrecy rule): {message}"
    );
    let after = db
        .execute("SELECT id FROM bucket_typed_docs", &empty())
        .expect("select must still succeed");
    assert!(after.rows.is_empty(), "the refused row must be absent");
    let partitions = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR bucket_typed_docs.embedding",
            &empty(),
        )
        .expect("partition inspection must still succeed");
    assert!(
        partitions.rows.is_empty(),
        "no stray partition of the wrong component type may exist: {partitions:?}"
    );
}

/// The identical refusal, read the same way, for a row arriving through sync
/// rather than through ordinary SQL. `RowChange.values` carries a `Value` per
/// column directly (no SQL layer to reject the mismatch earlier), so a peer
/// that ships a wrongly-typed key value reaches the exact same write-side
/// check `vector_partition_key_for_row` runs for a local write.
#[test]
fn a_partition_key_value_of_the_wrong_type_arriving_through_sync_gets_the_same_typed_refusal() {
    use contextdb_core::{RowId, VectorIndexRef};
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange, VectorChange,
    };

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE synced_bucket_typed_docs (\
             id UUID PRIMARY KEY, \
             bucket_code INTEGER NOT NULL, \
             embedding VECTOR(2) PARTITION_KEY (bucket_code) MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
            &empty(),
        )
        .expect("declare the INTEGER-keyed table on the receiver; the declaration is valid");

    let row_id = Uuid::from_u128(0x0900_0000_0000_0000_0000_0000_0000_0601);
    let offending_value = "forty-two-not-an-integer";
    let mut values: HashMap<String, Value> = HashMap::new();
    values.insert("id".to_owned(), Value::Uuid(row_id));
    values.insert(
        "bucket_code".to_owned(),
        Value::Text(offending_value.to_owned()),
    );
    let lsn = contextdb_core::Lsn(1);
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "synced_bucket_typed_docs".to_owned(),
            natural_key: NaturalKey::single("id".to_owned(), Value::Uuid(row_id)),
            values,
            deleted: false,
            lsn,
            created_at: None,
        }],
        edges: vec![],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("synced_bucket_typed_docs", "embedding"),
            row_id: RowId(1),
            vector: vec![1.0, 0.0],
            lsn,
        }],
        ddl: vec![],
        ddl_lsn: Vec::new(),
    };

    let error = receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err(
            "a row arriving through sync with a key value not of the declared type must be \
             refused, exactly as the local write path refuses it",
        );
    assert!(
        !matches!(error, Error::InvalidVectorPartitionDeclaration { .. }),
        "a wrong VALUE arriving over sync must not be reported as an invalid declaration \
         either: {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("bucket_code"),
        "the message must name the key COLUMN on the sync path too: {message}"
    );
    assert!(
        !message.contains(offending_value),
        "the message must never echo the offending VALUE on the sync path either: {message}"
    );

    let after = receiver
        .execute("SELECT id FROM synced_bucket_typed_docs", &empty())
        .expect("select must still succeed");
    assert!(
        after.rows.is_empty(),
        "the refused synced row must be absent"
    );
}

/// The bounded-reader door for the coercion legs. Gated on `test-seams`
/// because `bounded_read_test_support` only exists under that feature; the
/// rest of this file (including every existing control test) must keep
/// compiling without it.
#[cfg(feature = "test-seams")]
mod bounded_door {
    use super::{
        Database, HiddenBy, SemanticQuery, TYPED_KEY_QUERY_VECTOR, Value, empty, params,
        query_vector, result_ids, secure_fixture_hidden_by, sorted, typed_key_fixture,
    };
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
    use contextdb_engine::executor::bounded_read_test_support as bounded;
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    struct FrozenClock;

    impl DeadlineClock for FrozenClock {
        fn now_ms(&self) -> u64 {
            0
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    fn roomy_limits() -> ReadLimits {
        ReadLimits {
            result_rows: 2_048,
            result_bytes: 16 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 1_000_000,
            memory: 64 * 1024 * 1024,
            cursor_page_rows: 128,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 100_000,
        }
    }

    fn bounded_request(
        sql: &str,
        bound: std::collections::HashMap<String, Value>,
    ) -> bounded::BoundedReadRequest {
        bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
    }

    fn assert_bounded_matches_plain_equality(database: &Database, table: &str, predicate: &str) {
        let plain_sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
        let expected = sorted(result_ids(
            &database
                .execute(&plain_sql, &empty())
                .unwrap_or_else(|error| panic!("plain `=` control must succeed: {error}")),
        ));
        let vector_sql = format!(
            "SELECT id FROM {table} WHERE {predicate} \
             ORDER BY embedding <=> {TYPED_KEY_QUERY_VECTOR} LIMIT 8"
        );
        let request = bounded_request(&vector_sql, empty());
        let bounded_result = bounded::execute(database, &request).unwrap_or_else(|error| {
            panic!("bounded scoped vector search must succeed: {vector_sql}: {error:?}")
        });
        assert_eq!(
            sorted(result_ids(&bounded_result.result)),
            expected,
            "bounded reader must equal the plain `=` control: {vector_sql}"
        );
    }

    #[test]
    fn bounded_reader_matches_plain_equality_across_declared_key_types() {
        let fixture = typed_key_fixture();
        let db = &fixture.database;
        assert_bounded_matches_plain_equality(
            db,
            "uuid_keyed",
            &format!("scope_id = '{}'", fixture.uuid_alpha),
        );
        assert_bounded_matches_plain_equality(db, "int_keyed", "bucket = 7.0");
        assert_bounded_matches_plain_equality(
            db,
            "ts_keyed",
            &format!("stamped = {}", fixture.ts_alpha_ms),
        );
        assert_bounded_matches_plain_equality(
            db,
            "txid_keyed",
            &format!("at_tx = {}", fixture.tx_first_value.0),
        );
    }

    #[test]
    fn bounded_reader_reports_an_empty_answer_not_an_error_for_an_unrepresentable_literal() {
        let fixture = typed_key_fixture();
        let db = &fixture.database;
        assert_bounded_matches_plain_equality(db, "uuid_keyed", "scope_id = 'not-a-uuid'");
        assert_bounded_matches_plain_equality(db, "txid_keyed", "at_tx = -1");
    }

    #[test]
    fn bounded_reader_does_not_over_match_negative_zero_or_under_match_above_two_pow_53() {
        let fixture = typed_key_fixture();
        let db = &fixture.database;
        let _ = fixture.int_zero;
        assert_bounded_matches_plain_equality(db, "int_keyed", "bucket = -0.0");
        assert_bounded_matches_plain_equality(db, "int_keyed", "bucket = 9007199254740992.0");
    }

    fn hidden_row_bounded_request(
        sql: &str,
        bound: std::collections::HashMap<String, Value>,
    ) -> bounded::BoundedReadRequest {
        bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
    }

    /// A reader restricted by Context, scope, or an ACL grant
    /// asks for its nearest row and must get its nearest VISIBLE row, never
    /// an empty answer because a closer hidden row won the single `LIMIT`
    /// slot before the restriction was applied. Proved across the three
    /// restriction legs and across the unpartitioned, `SEARCH_MODE AUTO`, and
    /// `SEARCH_MODE EXACT` partitioned declarations, on all three doors
    /// production actually serves reads through.
    #[test]
    fn a_restricted_reader_gets_its_nearest_visible_row_when_a_hidden_row_is_closer() {
        for hidden_by in [HiddenBy::Context, HiddenBy::Scope, HiddenBy::Principal] {
            for declaration in [
                "VECTOR(2)",
                "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE AUTO",
                "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE EXACT",
                "VECTOR(2) PARTITION_KEY (bucket) MAX_PARTITIONS 8 SEARCH_MODE EXACT",
                "VECTOR(2) PARTITION_KEY (context_id, bucket, scope) MAX_PARTITIONS 8 SEARCH_MODE INDEXED",
            ] {
                let fixture = secure_fixture_hidden_by(declaration, hidden_by);
                if declaration.contains("INDEXED") {
                    fixture
                        .admin
                        .execute(
                            "CREATE INDEX readable_rows ON hidden_row_docs(context_id)",
                            &empty(),
                        )
                        .unwrap();
                }
                let scoped = fixture.scoped();
                let case = format!("hidden_by={hidden_by:?} declaration={declaration}");

                // CONTROL — ordinary door: `effective_read_candidates`
                // merges the readable bitmap before `search_with_mode` runs.
                let ordinary = scoped
                    .execute(fixture.sql(), &fixture.query_params())
                    .unwrap_or_else(|error| {
                        panic!("CONTROL ordinary door failed ({case}): {error}")
                    });
                assert_eq!(
                    result_ids(&ordinary),
                    fixture.authorized_ids,
                    "CONTROL ordinary door must answer the nearest VISIBLE row ({case})"
                );

                // CONTROL — Rust `SemanticQuery` door. Same production merge
                // point as the ordinary door
                // (`query_vector_strict_in_tx_with_strategy`).
                let mut semantic_query =
                    SemanticQuery::new("hidden_row_docs", "embedding", query_vector(), 1);
                semantic_query.where_clause = Some("bucket = 'shared'".to_owned());
                let semantic_results = scoped
                    .semantic_search(semantic_query)
                    .unwrap_or_else(|error| panic!("CONTROL Rust door failed ({case}): {error}"));
                let semantic_ids: Vec<uuid::Uuid> = semantic_results
                    .iter()
                    .map(|result| match result.values.get("id") {
                        Some(Value::Uuid(value)) => *value,
                        other => panic!("expected UUID id from the Rust door, got {other:?}"),
                    })
                    .collect();
                assert_eq!(
                    semantic_ids, fixture.authorized_ids,
                    "CONTROL Rust door must answer the nearest VISIBLE row ({case})"
                );

                // The memory-bounded door. Access must apply before the
                // candidate subtree is built, not per published row, or the
                // hidden row wins the one `LIMIT` slot and is dropped with
                // nothing to backfill it.
                let request = hidden_row_bounded_request(fixture.sql(), fixture.query_params());
                let bounded_result = bounded::execute(&scoped, &request).unwrap_or_else(|error| {
                    panic!("the bounded door must answer, not error ({case}): {error:?}")
                });
                assert_eq!(
                    result_ids(&bounded_result.result),
                    fixture.authorized_ids,
                    "the bounded door must answer the nearest VISIBLE row, not drop it \
                     for the hidden row that briefly occupied its LIMIT slot ({case})"
                );
            }
        }
    }

    /// S1a: narrowing must never widen. A reader restricted to one Context
    /// who names a DIFFERENT Context in the statement gets an empty answer on
    /// every door, never that Context's rows and never an error.
    #[test]
    fn a_restricted_reader_naming_a_foreign_context_gets_an_empty_answer_not_that_contexts_rows() {
        let fixture = secure_fixture_hidden_by(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE AUTO",
            HiddenBy::Context,
        );
        let scoped = fixture.scoped();
        let foreign_only_sql = "SELECT id FROM hidden_row_docs \
             WHERE context_id = $hidden_context AND bucket = $bucket \
             ORDER BY embedding <=> $query LIMIT 10";
        let foreign_params = params([
            ("hidden_context", Value::Uuid(fixture.hidden_context)),
            ("bucket", Value::Text("shared".to_owned())),
            ("query", Value::Vector(query_vector())),
        ]);

        let ordinary = scoped
            .execute(foreign_only_sql, &foreign_params)
            .expect("naming a foreign Context is a legal, merely unanswerable query");
        assert!(
            result_ids(&ordinary).is_empty(),
            "ordinary door: a foreign Context in the WHERE clause must never widen the \
             reader's scope"
        );

        let request = hidden_row_bounded_request(foreign_only_sql, foreign_params);
        let bounded_result = bounded::execute(&scoped, &request)
            .expect("naming a foreign Context must not error the bounded door");
        assert!(
            result_ids(&bounded_result.result).is_empty(),
            "bounded door: a foreign Context in the WHERE clause must never widen the \
             reader's scope"
        );
    }

    /// For the partition-key-aligned declaration, on the partition-key-aligned
    /// declaration, a predicate that fully specifies the key answers under
    /// `SEARCH_MODE INDEXED` with NO relational index declared on the
    /// restriction column anywhere — the restriction is discharged by
    /// partition alignment, not by an index. Its answer is identical to the
    /// same query run by an unrestricted admin handle with the restriction
    /// added to the WHERE clause: a restriction never changes the answer
    /// the same question would get with the restriction written as a filter.
    #[test]
    fn restricted_indexed_reads_admit_their_own_staged_vectors_and_rollback_removes_them() {
        let fixture = super::secure_context_fixture(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE INDEXED",
        );
        let scoped = fixture.scoped();
        let staged = super::id(0x912);
        let tx = scoped.begin().expect("begin scoped transaction");
        scoped.execute_in_tx(tx,
            "INSERT INTO hidden_row_docs (id, context_id, scope, acl_id, bucket, embedding) VALUES ($id, $context, 'visible', $acl, 'shared', $vector)",
            &params([
                ("id", Value::Uuid(staged)),
                ("context", Value::Uuid(fixture.allowed_context)),
                ("acl", Value::Uuid(super::id(0x903))),
                ("vector", Value::Vector(query_vector())),
            ]),
        ).expect("stage an authorized nearest vector");
        let answer = scoped
            .execute_in_tx(tx, fixture.sql(), &fixture.query_params())
            .expect("INDEXED merges the authorized staged delta with the maintained source");
        assert_eq!(result_ids(&answer), vec![staged]);
        assert!(matches!(
            answer.trace.vector_search.as_ref().unwrap().route,
            Some(
                contextdb_engine::VectorSearchRoute::Indexed
                    | contextdb_engine::VectorSearchRoute::FilteredIndexed
            )
        ));
        scoped.rollback(tx).expect("rollback scoped transaction");
        assert_eq!(
            result_ids(
                &scoped
                    .execute(fixture.sql(), &fixture.query_params())
                    .unwrap()
            ),
            fixture.authorized_ids
        );
    }

    #[test]
    fn a_key_aligned_restricted_indexed_query_answers_with_no_relational_index() {
        let fixture = super::secure_context_fixture(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE INDEXED",
        );
        let scoped = fixture.scoped();

        let ordinary = scoped
            .execute(fixture.sql(), &fixture.query_params())
            .unwrap_or_else(|error| {
                panic!("a key-aligned restricted INDEXED query must answer, not refuse: {error}")
            });
        assert_eq!(
            result_ids(&ordinary),
            fixture.authorized_ids,
            "ordinary door: key-aligned restricted INDEXED must answer the nearest VISIBLE row"
        );

        let request = hidden_row_bounded_request(fixture.sql(), fixture.query_params());
        let bounded_result = bounded::execute(&scoped, &request).unwrap_or_else(|error| {
            panic!("a key-aligned restricted INDEXED query must answer, not error: {error:?}")
        });
        assert_eq!(
            result_ids(&bounded_result.result),
            fixture.authorized_ids,
            "bounded door: key-aligned restricted INDEXED must answer the nearest VISIBLE row"
        );

        // The restriction equivalence: an unrestricted admin handle asking
        // the same question with the restriction folded into the WHERE
        // clause gets the identical answer a restricted reader gets for
        // free.
        let admin_equivalent_sql = "SELECT id FROM hidden_row_docs \
             WHERE context_id = $allowed_context AND bucket = $bucket \
             ORDER BY embedding <=> $query LIMIT 1";
        let admin_params = params([
            ("allowed_context", Value::Uuid(fixture.allowed_context)),
            ("bucket", Value::Text("shared".to_owned())),
            ("query", Value::Vector(query_vector())),
        ]);
        let admin_result = fixture
            .admin
            .execute(admin_equivalent_sql, &admin_params)
            .expect("the admin equivalence query must succeed");
        assert_eq!(
            result_ids(&admin_result),
            result_ids(&ordinary),
            "a restricted reader's answer must equal an admin handle's answer to the same \
             question with the restriction folded into the WHERE clause"
        );
    }
}

// ---------------------------------------------------------------------------
// The bounded reader applies access restrictions before top-k, and a
// Context-restricted handle searches its own partition through it.
//
// `bounded_door` above (`secure_fixture_hidden_by`, `HiddenRowFixture`)
// already proves the headline promise -- a restricted reader gets its
// nearest VISIBLE row on every door, including the aligned-`INDEXED`-answers
// arm and the narrowing-never-widens arm. This module adds the distinct
// proof that the bounded door reaches the right
// answer WITHOUT a relational table scan, for both the key-aligned route
// (arm A) and the unaligned-but-statement-bounded route (arm B), plus the
// unpartitioned refuse-then-recover-via-index arm (arm C then D) that
// `bounded_door` does not cover. Reuses `bounded_door`'s fixture rather than
// a second copy of it.
//
// Gated on `test-seams` for the same reason as `bounded_door`: only the
// bounded reader needs the test-seam surface.
// ---------------------------------------------------------------------------
#[cfg(feature = "test-seams")]
mod restricted_access_scan_accounting {
    use super::{
        Error, HiddenBy, Value, empty, params, query_vector, result_ids, secure_fixture_hidden_by,
    };
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
    use contextdb_engine::executor::bounded_read_test_support::{self as bounded, TestWorkSource};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    struct FrozenClock;

    impl DeadlineClock for FrozenClock {
        fn now_ms(&self) -> u64 {
            0
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    fn roomy_limits() -> ReadLimits {
        ReadLimits {
            result_rows: 2_048,
            result_bytes: 16 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 1_000_000,
            memory: 64 * 1024 * 1024,
            cursor_page_rows: 128,
            cursor_page_bytes: 4 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 100_000,
        }
    }

    fn bounded_request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
        bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
    }

    /// Leg 1a -- key-aligned: the restriction column (`context_id`) IS a
    /// partition-key component. The bounded door must answer boundedly, with
    /// no relational table scan, and the access-control charge must be the
    /// publish-time count only (the prefixes already discharged the
    /// restriction), not one gate check per candidate in an unrelated
    /// partition.
    #[test]
    fn key_aligned_restriction_narrows_prefixes_with_no_relational_scan() {
        let fixture = secure_fixture_hidden_by(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE AUTO",
            HiddenBy::Context,
        );
        let scoped = fixture.scoped();
        scoped.__reset_relational_scan_rows_touched();
        let outcome = bounded::execute(
            &scoped,
            &bounded_request(fixture.sql(), fixture.query_params()),
        )
        .expect("the key-aligned restricted read succeeds");
        assert_eq!(
            result_ids(&outcome.result),
            fixture.authorized_ids,
            "a Context-aligned partition narrows to the reader's own partition and answers"
        );
        assert_eq!(
            scoped.__relational_scan_rows_touched(),
            0,
            "a key-aligned restriction must never fall back to a relational table scan"
        );
        let access_control_charge = outcome
            .telemetry
            .source_work
            .get(&TestWorkSource::AccessControl)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            access_control_charge, 3,
            "the selected row needs one candidate access check, one ACL grant check, and one publication check; unrelated partitions contribute no access work"
        );
    }

    /// Leg 1b -- unaligned but inside the statement's own bounded candidate
    /// set: the ACL leg is not a key component, but the statement's own
    /// predicate (`context_id` + `bucket`, both key components) already
    /// covers the partition key. The authorization invariant: no relational subtree
    /// may be built; the door must derive its candidates from the vector
    /// directory under the ambient prefix scope, charged per entry, and
    /// never scan the table (a `Scan` with no index pick otherwise falls to
    /// `ScanMode::Physical` over the whole table).
    #[test]
    fn unaligned_restriction_inside_a_bounded_candidate_set_never_scans() {
        let fixture = secure_fixture_hidden_by(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE AUTO",
            HiddenBy::Principal,
        );
        let scoped = fixture.scoped();
        scoped.__reset_relational_scan_rows_touched();
        let outcome = bounded::execute(
            &scoped,
            &bounded_request(fixture.sql(), fixture.query_params()),
        )
        .expect("the ACL-restricted read succeeds");
        assert_eq!(
            result_ids(&outcome.result),
            fixture.authorized_ids,
            "the ACL leg is a residual over the statement's own bounded candidate set and must \
             not drop the visible row"
        );
        assert_eq!(
            scoped.__relational_scan_rows_touched(),
            0,
            "an unaligned restriction inside a bounded candidate set must \
             derive its candidates from the vector directory, never from a relational table scan"
        );
        let access_control_charge = outcome
            .telemetry
            .source_work
            .get(&TestWorkSource::AccessControl)
            .copied()
            .unwrap_or(0);
        assert!(
            access_control_charge > 0,
            "the ACL gate must run before scoring, charged per candidate id, not only on the \
             published row: {access_control_charge}"
        );
        let candidate_charge = outcome
            .telemetry
            .source_work
            .get(&TestWorkSource::VectorCandidates)
            .copied()
            .unwrap_or(0);
        assert!(
            candidate_charge > 0,
            "the directory-derived candidate source must be charged per entry, never free: \
             {candidate_charge}"
        );
    }

    #[test]
    fn restricted_rust_indexed_without_predicate_refuses_before_sources_and_recovers() {
        let fixture = secure_fixture_hidden_by("VECTOR(2)", HiddenBy::Context);
        let scoped = fixture.scoped();
        let query = || {
            let mut query = contextdb_engine::SemanticQuery::new(
                "hidden_row_docs",
                "embedding",
                query_vector(),
                1,
            );
            query.search_mode = Some(contextdb_core::VectorSearchMode::Indexed);
            query
        };
        scoped.__reset_relational_scan_rows_touched();
        let error = scoped.semantic_search(query()).unwrap_err();
        assert!(
            matches!(error, Error::VectorFilteredRouteUnavailable { ref predicate_columns, .. }
            if predicate_columns.iter().any(|column| column == "context_id")),
            "{error:?}"
        );
        assert_eq!(
            scoped.__relational_scan_rows_touched(),
            0,
            "INDEXED must refuse before authorization scans any table row"
        );
        fixture
            .admin
            .execute(
                "CREATE INDEX readable_contexts ON hidden_row_docs(context_id)",
                &empty(),
            )
            .unwrap();
        for _ in 0..32 {
            fixture.admin.run_maintenance_cycle().unwrap();
        }
        let rows = scoped.semantic_search(query()).unwrap();
        let ids = rows
            .iter()
            .map(|row| match row.values.get("id") {
                Some(Value::Uuid(id)) => *id,
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, fixture.authorized_ids);
        assert_eq!(scoped.__relational_scan_rows_touched(), 0);
        assert!(
            scoped.__debug_last_query_vector_used_hnsw_for_test(),
            "recovery must use the maintained graph"
        );
    }

    /// Arm C then D: on an UNPARTITIONED declaration with neither alignment
    /// nor statement coverage, `INDEXED` must REFUSE naming the restriction
    /// column, and then answer once a relational index on that column
    /// exists. `bounded_door::a_key_aligned_restricted_indexed_query_answers_with_no_relational_index`
    /// proves the aligned-answers arm; this proves the opposite starting
    /// point.
    #[test]
    fn indexed_refuses_naming_the_restriction_column_then_recovers_via_index() {
        let fixture = secure_fixture_hidden_by("VECTOR(2)", HiddenBy::Context);
        let scoped = fixture.scoped();
        let sql = "SELECT id FROM hidden_row_docs \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1";
        let bound = params([("query", Value::Vector(query_vector()))]);

        let refusal = scoped.execute(sql, &bound).expect_err(
            "a restricted INDEXED read with neither alignment, coverage, nor a restriction \
                 index must refuse rather than silently scan the whole table",
        );
        assert!(
            matches!(&refusal, Error::VectorFilteredRouteUnavailable { .. }),
            "the refusal must be the typed filtered-route-unavailable identity, distinct from \
             the store's unready-graph refusal: {refusal:?}"
        );
        assert!(
            refusal.to_string().contains("context_id"),
            "the refusal must name the restriction column that made the route incomplete: {refusal}"
        );

        fixture
            .admin
            .execute(
                "CREATE INDEX hidden_row_docs_context_idx ON hidden_row_docs(context_id)",
                &empty(),
            )
            .expect("declare the recovery index");
        for _ in 0..32 {
            fixture
                .admin
                .run_maintenance_cycle()
                .expect("one caller-driven maintenance batch succeeds");
        }
        let recovered = scoped.execute(sql, &bound).expect(
            "once the restriction column is indexed, the same INDEXED restricted read must \
             answer",
        );
        assert_eq!(
            result_ids(&recovered),
            fixture.authorized_ids,
            "the recovered restriction-index route must answer with the visible row"
        );
    }
}

#[cfg(feature = "test-seams")]
#[test]
fn residual_acl_without_an_index_refuses_before_selected_partition_enumeration() {
    use contextdb_core::VectorSearchMode;
    let fixture = secure_fixture_hidden_by(
        "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8 SEARCH_MODE INDEXED",
        HiddenBy::Principal,
    );
    let scoped = fixture.scoped();
    let sql = fixture.sql();
    assert!(matches!(
        scoped.execute(sql, &fixture.query_params()),
        Err(Error::VectorFilteredRouteUnavailable { .. })
    ));
    let mut query = SemanticQuery::new("hidden_row_docs", "embedding", query_vector(), 1);
    query.search_mode = Some(VectorSearchMode::Indexed);
    query.where_clause = Some(format!(
        "context_id = '{}' AND bucket = 'shared'",
        fixture.allowed_context
    ));
    assert!(matches!(
        scoped.semantic_search(query),
        Err(Error::VectorFilteredRouteUnavailable { .. })
    ));
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
    use contextdb_engine::executor::bounded_read_test_support as bounded;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Clock;
    impl DeadlineClock for Clock {
        fn now_ms(&self) -> u64 {
            0
        }
        fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }
    struct Probe(AtomicUsize);
    impl bounded::ExecutionProbe for Probe {
        fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
        fn cancellation_observed(&self, _: u64) {}
        fn before_source_touch(&self, _: bounded::TestSourceTouch, _: u64) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let probe = Arc::new(Probe(AtomicUsize::new(0)));
    let mut request = bounded::BoundedReadRequest::new(
        sql,
        fixture.query_params(),
        ReadLimits::default(),
        Arc::new(Clock),
    );
    request.probe = Some(probe.clone());
    let error = bounded::execute(&scoped, &request).unwrap_err();
    let expected = scoped
        .execute(sql, &fixture.query_params())
        .unwrap_err()
        .to_string();
    assert!(matches!(error, bounded::TestError::Engine(message) if message == expected));
    assert_eq!(
        probe.0.load(Ordering::SeqCst),
        0,
        "INDEXED refusal precedes every candidate source touch"
    );
}

#[cfg(feature = "test-seams")]
#[test]
fn candidate_intersection_matches_row_visibility_and_transaction_grant_changes() {
    use contextdb_core::VectorIndexRef;
    use roaring::RoaringTreemap;
    for hidden_by in [HiddenBy::Context, HiddenBy::Scope, HiddenBy::Principal] {
        let fixture = secure_fixture_hidden_by("VECTOR(2) SEARCH_MODE EXACT", hidden_by);
        let scoped = fixture.scoped();
        let snapshot = fixture.admin.snapshot();
        let all = fixture.admin.scan("hidden_row_docs", snapshot).unwrap();
        let expected = scoped
            .scan("hidden_row_docs", snapshot)
            .unwrap()
            .into_iter()
            .map(|r| r.row_id)
            .collect::<BTreeSet<_>>();
        for subset in [
            vec![],
            vec![all[0].row_id],
            vec![all[1].row_id],
            all.iter().map(|r| r.row_id).collect(),
        ] {
            let candidates = subset
                .iter()
                .map(|id| id.0)
                .chain([u64::MAX - 1])
                .collect::<RoaringTreemap>();
            let actual = scoped
                .query_vector(
                    VectorIndexRef::new("hidden_row_docs", "embedding"),
                    &query_vector(),
                    10,
                    Some(&candidates),
                    snapshot,
                )
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<BTreeSet<_>>();
            let reference = subset
                .into_iter()
                .filter(|id| expected.contains(id))
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, reference, "{hidden_by:?}: candidate intersection");
        }
        let tx = fixture.admin.begin().unwrap();
        fixture
            .admin
            .execute_in_tx(tx, "DELETE FROM hidden_acl_grants", &empty())
            .unwrap();
        let sql = "SELECT id FROM hidden_row_docs ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
        let bound = params([("query", Value::Vector(query_vector()))]);
        assert!(
            scoped
                .execute_in_tx(tx, sql, &bound)
                .unwrap()
                .rows
                .is_empty()
        );
        let query = SemanticQuery::new("hidden_row_docs", "embedding", query_vector(), 10);
        assert!(
            scoped
                .__semantic_search_in_tx_for_test(tx, query)
                .unwrap()
                .is_empty()
        );
        fixture.admin.rollback(tx).unwrap();
        assert_eq!(
            result_ids(&scoped.execute(sql, &bound).unwrap()),
            fixture.authorized_ids
        );
    }
}

#[cfg(feature = "test-seams")]
#[test]
fn restricted_inspection_keeps_adaptive_partition_populations_private() {
    for hidden_by in [HiddenBy::Context, HiddenBy::Scope, HiddenBy::Principal] {
        let fixture = secure_fixture_hidden_by(
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8",
            hidden_by,
        );
        let scoped = fixture.scoped();
        for sql in [
            "SHOW VECTOR_INDEXES",
            "SHOW VECTOR_PARTITIONS FOR hidden_row_docs.embedding",
        ] {
            let refusal = scoped
                .execute(sql, &empty())
                .expect_err("restricted whole-index inspection must refuse");
            assert!(
                matches!(refusal, Error::VectorWholeIndexInspectionDenied),
                "{hidden_by:?}: inspection must use the exact typed privacy refusal, got {refusal:?}"
            );
        }
        let bound_sql = fixture
            .sql()
            .replace(
                "$allowed_context",
                &format!("'{}'", fixture.allowed_context),
            )
            .replace("$hidden_context", &format!("'{}'", fixture.hidden_context))
            .replace("$bucket", "'shared'")
            .replace("$query", "[1.0,0.0]");
        for sql in [fixture.sql(), bound_sql.as_str()] {
            let explain = scoped.explain_output(sql).unwrap();
            let disclosure = explain.vector_search.unwrap();
            assert!(
                disclosure.partition_hnsw.is_empty(),
                "{hidden_by:?}: {disclosure:?}"
            );
        }
    }
}

#[cfg(all(feature = "test-seams", unix))]
#[test]
fn restricted_vector_access_and_route_refusal_match_file_and_owner_service() {
    use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits, ReadRoute};
    use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
    use std::os::unix::fs::PermissionsExt;
    for hidden_by in [HiddenBy::Context, HiddenBy::Scope, HiddenBy::Principal] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("restricted.db");
        let runtime = directory.path().join("runtime");
        std::fs::create_dir(&runtime).unwrap();
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
        let open_options = || DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        };
        let admin = Database::open_with_options(&path, open_options()).unwrap();
        let fixture = secure_fixture_in_database(
            admin,
            "VECTOR(2) PARTITION_KEY (context_id, bucket) MAX_PARTITIONS 8",
            hidden_by,
            true,
        );
        let options = || ReadSessionOptions {
            contexts: Some(BTreeSet::from([ContextId::new(fixture.allowed_context)])),
            scope_labels: Some(BTreeSet::from([fixture.allowed_scope.clone()])),
            principal: Some(fixture.principal.clone()),
            limits: ReadLimits::default(),
            ..ReadSessionOptions::default()
        };
        let check = |reader: &ReadSession, indexed: bool| {
            for mode in ["AUTO", "EXACT", "INDEXED"] {
                let sql = fixture
                    .sql()
                    .replace("LIMIT 1", &format!("USE VECTOR {mode} LIMIT 1"));
                let answer = reader.execute(&sql, &fixture.query_params());
                if mode == "INDEXED" && !indexed {
                    assert!(
                        matches!(answer, Err(Error::VectorFilteredRouteUnavailable { .. })),
                        "{hidden_by:?} {:?}: {answer:?}",
                        reader.route()
                    );
                } else {
                    assert_eq!(
                        result_ids(&answer.unwrap()),
                        fixture.authorized_ids,
                        "{hidden_by:?} {:?} {mode}",
                        reader.route()
                    );
                }
            }
            let cancellation = OwnerReadCancellation::new();
            cancellation.cancel();
            assert!(matches!(
                reader.execute_with_cancellation(
                    fixture.sql(),
                    &fixture.query_params(),
                    &cancellation
                ),
                Err(Error::ReadCancelled)
            ));
        };
        // Each image is queried over both native read routes before changing
        // its schema. A supporting index changes eligibility, never visibility.
        for indexed in [false, true] {
            let reopened =
                indexed.then(|| Database::open_with_options(&path, open_options()).unwrap());
            let admin = reopened.as_ref().unwrap_or(&fixture.admin);
            if indexed {
                admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                admin
                    .execute(
                        "CREATE INDEX readable_context ON hidden_row_docs(context_id)",
                        &empty(),
                    )
                    .unwrap();
            }
            let owner = ReadSession::open_owner_only_in_runtime_dir(
                &path,
                options(),
                Some(runtime.clone()),
            )
            .unwrap();
            assert_eq!(owner.route(), ReadRoute::Owner);
            check(&owner, indexed);
            drop(owner);
            admin.close().unwrap();
            let file = ReadSession::with_runtime_directory_for_test(&runtime, || {
                ReadSession::open_with_options(&path, options())
            })
            .unwrap();
            assert_eq!(file.route(), ReadRoute::File);
            check(&file, indexed);
        }
    }
}
