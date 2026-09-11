//! Sync and operator-inspection contracts for vector partitions.
//!
//! The sync cases use the same `changes_since`/`apply_changes` path as the
//! existing engine sync fixtures. The only payload assumptions pinned here
//! are the shipped positional owner pairing and the absence of a vector
//! payload when an update changes only a partition-key column.

use contextdb_core::{ContextId, Error, Lsn, Value, VectorIndexRef};
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy};
use contextdb_engine::{Database, QueryResult};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

// The real two-node sync door needs the `test-seams`-only in-process broker
// and test constructors; the hand-cut `apply_changes` door and the other
// tests in this file do not, so only these imports are gated -- gating the
// whole file would hide the other tests from a default build.
#[cfg(feature = "test-seams")]
use contextdb_core::TenantId;
#[cfg(feature = "test-seams")]
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
#[cfg(feature = "test-seams")]
use contextdb_engine::executor::bounded_read_test_support::{
    self as bounded, TestSourceTouch, TestWorkSource,
};
#[cfg(feature = "test-seams")]
#[cfg(feature = "test-seams")]
use contextdb_server::subjects::{pull_subject, push_subject};
#[cfg(feature = "test-seams")]
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
#[cfg(feature = "test-seams")]
use std::sync::Arc;
#[cfg(feature = "test-seams")]
use std::sync::atomic::{AtomicBool, Ordering};

const UNPARTITIONED_SCHEMA: &str = "CREATE TABLE vector_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3)
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

const PARTITIONED_SCHEMA: &str = "CREATE TABLE vector_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3)
        PARTITION_KEY (scope_id)
        MAX_PARTITIONS 8
        SEARCH_MODE AUTO
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

const VECTOR_PARTITION_COLUMNS: &[&str] = &[
    "table",
    "column",
    "partition_key",
    "live_rows",
    "retained_rows",
    "base_generation",
    "base_tx",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "charged_vector_bytes",
    "durable_index_bytes",
    "charged_index_bytes",
    "query_state",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "maintenance_checkpoint_tx",
    "desired_hnsw_m",
    "desired_hnsw_ef_construction",
    "desired_hnsw_ef_search",
    "serving_hnsw_m",
    "serving_hnsw_ef_construction",
    "serving_hnsw_ef_search",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "desired_policy_revision",
    "serving_policy_revision",
    "recovery_action",
];

#[derive(Clone, Debug)]
struct FixtureItem {
    id: Uuid,
    scope_id: Uuid,
    vector: Vec<f32>,
}

fn fixture_items() -> Vec<FixtureItem> {
    vec![
        FixtureItem {
            id: Uuid::from_u128(0x1101),
            scope_id: Uuid::from_u128(0x2201),
            vector: vec![1.0, 0.0, 0.0],
        },
        FixtureItem {
            id: Uuid::from_u128(0x1102),
            scope_id: Uuid::from_u128(0x2202),
            vector: vec![0.0, 1.0, 0.0],
        },
    ]
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn inspection_refusal_violation(
    statement: &str,
    result: Result<QueryResult, Error>,
) -> Option<String> {
    match result {
        Ok(answer) => Some(format!(
            "{statement} exposed {} whole-index row(s) through a constrained handle",
            answer.rows.len()
        )),
        Err(Error::ParseError(message)) => Some(format!(
            "{statement} returned a parse error instead of a typed inspection refusal: {message}"
        )),
        Err(Error::PlanError(message)) => Some(format!(
            "{statement} returned a planning error instead of a typed inspection refusal: {message}"
        )),
        Err(Error::Other(message)) => Some(format!(
            "{statement} returned a generic error instead of a typed inspection refusal: {message}"
        )),
        Err(_) => None,
    }
}

fn column_index(result: &QueryResult, column: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == column)
        .unwrap_or_else(|| panic!("result contains column {column}: {:?}", result.columns))
}

fn value_at<'a>(result: &'a QueryResult, row: usize, column: &str) -> &'a Value {
    let column = column_index(result, column);
    result
        .rows
        .get(row)
        .and_then(|values| values.get(column))
        .unwrap_or_else(|| panic!("result contains row {row} and column {column}: {result:?}"))
}

fn vector_index_row(result: &QueryResult, table: &str, column: &str) -> usize {
    let table_column = column_index(result, "table");
    let vector_column = column_index(result, "column");
    result
        .rows
        .iter()
        .position(|row| {
            row[table_column] == Value::Text(table.to_owned())
                && row[vector_column] == Value::Text(column.to_owned())
        })
        .unwrap_or_else(|| panic!("result describes {table}.{column}: {result:?}"))
}

fn replicate_schema(sender: &Database, receiver: &Database, schema: &str) {
    sender
        .execute(schema, &empty())
        .expect("the sender accepts the table declaration");
    let changes = sender.changes_since(Lsn(0));
    assert!(
        !changes.ddl.is_empty(),
        "the schema snapshot carries the table declaration"
    );
    assert!(changes.rows.is_empty());
    assert!(changes.vectors.is_empty());
    receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the receiver applies the sender's table declaration");
}

fn commit_fixture_items(sender: &Database, items: &[FixtureItem]) -> ChangeSet {
    let watermark = sender.current_lsn();
    let tx = sender.begin().expect("begin the two-row transaction");
    for item in items {
        sender
            .execute_in_tx(
                tx,
                "INSERT INTO vector_items (id, scope_id, embedding) \
                 VALUES ($id, $scope_id, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(item.id)),
                    ("scope_id", Value::Uuid(item.scope_id)),
                    ("embedding", Value::Vector(item.vector.clone())),
                ]),
            )
            .expect("stage one vector-bearing row");
    }
    sender.commit(tx).expect("commit both rows together");

    let commit_lsn = sender.current_lsn();
    let changes = sender.changes_since(watermark);
    assert_eq!(changes.rows.len(), items.len());
    assert_eq!(changes.vectors.len(), items.len());
    assert!(changes.rows.iter().all(|row| row.lsn == commit_lsn));
    assert!(
        changes
            .vectors
            .iter()
            .all(|vector| vector.lsn == commit_lsn)
    );

    let row_keys = changes
        .rows
        .iter()
        .map(|row| row.natural_key.value.clone())
        .collect::<Vec<_>>();
    let vectors = changes
        .vectors
        .iter()
        .map(|vector| vector.vector.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        row_keys,
        items
            .iter()
            .map(|item| Value::Uuid(item.id))
            .collect::<Vec<_>>(),
        "the row side of the shipped owner pairing keeps transaction order"
    );
    assert_eq!(
        vectors,
        items
            .iter()
            .map(|item| item.vector.clone())
            .collect::<Vec<_>>(),
        "the vector side of the shipped owner pairing keeps the same order"
    );
    changes
}

fn apply_fixture_changes(receiver: &Database, changes: ChangeSet) {
    receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the receiver applies the row and vector changes together");
}

fn nearest_id(database: &Database, query: Vec<f32>) -> Uuid {
    let result = database
        .execute(
            "SELECT id FROM vector_items \
             ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![("query", Value::Vector(query))]),
        )
        .expect("vector search returns the nearest row");
    assert_eq!(result.rows.len(), 1);
    match value_at(&result, 0, "id") {
        Value::Uuid(id) => *id,
        other => panic!("nearest row has a UUID id, got {other:?}"),
    }
}

fn search_in_scope(database: &Database, scope_id: Uuid, query: Vec<f32>) -> QueryResult {
    database
        .execute(
            "SELECT id FROM vector_items WHERE scope_id = $scope_id \
             ORDER BY embedding <=> $query LIMIT 1",
            &params(vec![
                ("scope_id", Value::Uuid(scope_id)),
                ("query", Value::Vector(query)),
            ]),
        )
        .expect("scoped vector search succeeds")
}

fn partition_key(scope_id: Uuid) -> Value {
    Value::Json(serde_json::json!({ "scope_id": scope_id.to_string() }))
}

fn assert_partition_membership(database: &Database, scopes: &[Uuid]) {
    let result = database
        .execute(
            "SHOW VECTOR_PARTITIONS FOR vector_items.embedding",
            &empty(),
        )
        .expect("the receiver describes the vector partitions");
    assert_eq!(result.rows.len(), scopes.len());
    for (row, scope_id) in scopes.iter().copied().enumerate() {
        assert_eq!(
            value_at(&result, row, "table"),
            &Value::Text("vector_items".into())
        );
        assert_eq!(
            value_at(&result, row, "column"),
            &Value::Text("embedding".into())
        );
        assert_eq!(
            value_at(&result, row, "partition_key"),
            &partition_key(scope_id)
        );
        assert_eq!(value_at(&result, row, "live_rows"), &Value::Int64(1));
        assert_eq!(value_at(&result, row, "retained_rows"), &Value::Int64(0));
    }
}

fn assert_partitioned_summary(database: &Database, live_partitions: i64) {
    let result = database
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the receiver describes its vector index");
    let row = vector_index_row(&result, "vector_items", "embedding");
    assert_eq!(
        value_at(&result, row, "partition_key_columns"),
        &Value::Json(serde_json::json!(["scope_id"]))
    );
    assert_eq!(value_at(&result, row, "max_partitions"), &Value::Int64(8));
    assert_eq!(
        value_at(&result, row, "live_partitions"),
        &Value::Int64(live_partitions)
    );
    assert_eq!(
        value_at(&result, row, "retained_partitions"),
        &Value::Int64(0)
    );
    assert_eq!(
        value_at(&result, row, "search_mode"),
        &Value::Text("AUTO".to_owned())
    );
}

fn seeded_partitioned_pair() -> (Database, Database, Vec<FixtureItem>) {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    replicate_schema(&sender, &receiver, PARTITIONED_SCHEMA);
    let items = fixture_items();
    let changes = commit_fixture_items(&sender, &items);
    apply_fixture_changes(&receiver, changes);
    (sender, receiver, items)
}

#[test]
fn every_consolidation_form_keeps_schema_identity_across_sync() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    replicate_schema(
        &sender,
        &receiver,
        "CREATE TABLE maintained_items (
            id UUID PRIMARY KEY,
            scope_id UUID NOT NULL,
            embedding VECTOR(3) PARTITION_KEY (scope_id)
         ) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
    );

    let forms = [
        (
            "ALTER TABLE maintained_items ALTER COLUMN embedding SET \
             CONSOLIDATION (CHANGE_PERCENT = 7, TOMBSTONE_PERCENT = 31)",
            Some(7),
            Some(31),
            false,
            "CONSOLIDATION (CHANGE_PERCENT = 7, TOMBSTONE_PERCENT = 31)",
        ),
        (
            "ALTER TABLE maintained_items ALTER COLUMN embedding SET CONSOLIDATION NONE",
            None,
            None,
            true,
            "CONSOLIDATION NONE",
        ),
        (
            "ALTER TABLE maintained_items ALTER COLUMN embedding SET CONSOLIDATION DEFAULT",
            None,
            None,
            false,
            "",
        ),
    ];

    for (statement, change_percent, tombstone_percent, disabled, rendered_clause) in forms {
        let watermark = sender.current_lsn();
        sender
            .execute(statement, &empty())
            .expect("the sender accepts the consolidation form");
        let changes = sender.changes_since(watermark);
        assert_eq!(changes.ddl.len(), 1, "the form emits one synced DDL change");
        receiver
            .apply_changes(
                changes,
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .expect("the receiver applies the consolidation form");

        let sender_meta = sender.table_meta("maintained_items").unwrap();
        let receiver_meta = receiver.table_meta("maintained_items").unwrap();
        assert_eq!(
            receiver_meta, sender_meta,
            "synced schema identity is exact"
        );
        let vector = receiver_meta
            .columns
            .iter()
            .find(|column| column.name == "embedding")
            .unwrap();
        assert_eq!(vector.consolidation_change_percent, change_percent);
        assert_eq!(vector.consolidation_tombstone_percent, tombstone_percent);
        assert_eq!(vector.consolidation_disabled, disabled);
        let rendered = render_table_meta("maintained_items", &receiver_meta);
        if rendered_clause.is_empty() {
            assert!(!rendered.contains("CONSOLIDATION"));
        } else {
            assert!(
                rendered.contains(rendered_clause),
                "rendered schema: {rendered}"
            );
        }
    }
}

#[test]
fn existing_two_row_sync_keeps_each_vector_with_its_row_and_shows_summary() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    replicate_schema(&sender, &receiver, UNPARTITIONED_SCHEMA);
    let items = fixture_items();
    let changes = commit_fixture_items(&sender, &items);
    apply_fixture_changes(&receiver, changes);

    assert_eq!(nearest_id(&receiver, items[0].vector.clone()), items[0].id);
    assert_eq!(nearest_id(&receiver, items[1].vector.clone()), items[1].id);

    let summary = receiver
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the existing vector summary remains queryable");
    let row = vector_index_row(&summary, "vector_items", "embedding");
    assert_eq!(value_at(&summary, row, "dimension"), &Value::Int64(3));
    assert_eq!(value_at(&summary, row, "vector_count"), &Value::Int64(2));
}

#[test]
fn partitioned_sync_derives_receiver_membership_without_changing_owner_pairing() {
    let (_sender, receiver, items) = seeded_partitioned_pair();

    assert_eq!(nearest_id(&receiver, items[0].vector.clone()), items[0].id);
    assert_eq!(nearest_id(&receiver, items[1].vector.clone()), items[1].id);
    assert_partition_membership(&receiver, &[items[0].scope_id, items[1].scope_id]);
    assert_partitioned_summary(&receiver, 2);
}

#[test]
fn received_key_only_update_moves_the_unchanged_vector() {
    let (sender, receiver, items) = seeded_partitioned_pair();
    let moved = &items[0];
    let untouched = &items[1];
    let old_scope = moved.scope_id;
    let new_scope = Uuid::from_u128(0x2203);
    let watermark = sender.current_lsn();

    sender
        .execute(
            "UPDATE vector_items SET scope_id = $scope_id WHERE id = $id",
            &params(vec![
                ("scope_id", Value::Uuid(new_scope)),
                ("id", Value::Uuid(moved.id)),
            ]),
        )
        .expect("change only the declared partition key");
    let changes = sender.changes_since(watermark);
    assert_eq!(changes.rows.len(), 1);
    assert!(
        changes.vectors.is_empty(),
        "a key-only update carries no replacement vector payload"
    );
    assert_eq!(
        changes.rows[0].values.get("scope_id"),
        Some(&Value::Uuid(new_scope))
    );
    apply_fixture_changes(&receiver, changes);

    assert_partition_membership(&receiver, &[untouched.scope_id, new_scope]);
    assert_partitioned_summary(&receiver, 2);
    assert!(
        search_in_scope(&receiver, old_scope, moved.vector.clone())
            .rows
            .is_empty(),
        "the old partition no longer contains the moved row"
    );
    let moved_result = search_in_scope(&receiver, new_scope, moved.vector.clone());
    assert_eq!(moved_result.rows.len(), 1);
    assert_eq!(value_at(&moved_result, 0, "id"), &Value::Uuid(moved.id));
    assert_eq!(nearest_id(&receiver, moved.vector.clone()), moved.id);
}

#[test]
fn show_vector_partitions_supports_all_sql_forms() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE vector_items (
                id UUID PRIMARY KEY,
                embedding VECTOR(3),
                secondary VECTOR(3)
            )",
            &empty(),
        )
        .expect("declare two empty unpartitioned vector columns");

    let all = database
        .execute("SHOW VECTOR_PARTITIONS", &empty())
        .expect("list every vector partition");
    assert_eq!(
        all.columns,
        VECTOR_PARTITION_COLUMNS
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(all.rows.len(), 2);
    assert_eq!(
        value_at(&all, 0, "column"),
        &Value::Text("embedding".into())
    );
    assert_eq!(
        value_at(&all, 1, "column"),
        &Value::Text("secondary".into())
    );
    for row in 0..all.rows.len() {
        assert_eq!(
            value_at(&all, row, "partition_key"),
            &Value::Json(serde_json::json!({}))
        );
        assert_eq!(value_at(&all, row, "live_rows"), &Value::Int64(0));
        assert_eq!(value_at(&all, row, "retained_rows"), &Value::Int64(0));
        assert_eq!(value_at(&all, row, "base_generation"), &Value::Null);
        assert_eq!(value_at(&all, row, "base_tx"), &Value::Null);
        assert_eq!(
            value_at(&all, row, "query_state"),
            &Value::Text("empty".to_owned())
        );
    }

    let selected = database
        .execute(
            "SHOW VECTOR_PARTITIONS FOR vector_items.embedding",
            &empty(),
        )
        .expect("list one named vector index");
    assert_eq!(selected.columns, all.columns);
    assert_eq!(selected.rows.len(), 1);
    assert_eq!(
        value_at(&selected, 0, "column"),
        &Value::Text("embedding".to_owned())
    );

    let paged = database
        .execute(
            "SHOW VECTOR_PARTITIONS FOR vector_items.embedding LIMIT 1 OFFSET 1",
            &empty(),
        )
        .expect("apply LIMIT and OFFSET after selecting the vector index");
    assert_eq!(paged.columns, all.columns);
    assert!(paged.rows.is_empty());
}

#[test]
fn constrained_handle_refuses_whole_vector_inspection() {
    let database = Database::open_memory();
    let context_id = Uuid::from_u128(0x3301);
    database
        .execute(
            "CREATE TABLE inspection_vectors (
                id UUID PRIMARY KEY,
                context_id UUID NOT NULL CONTEXT_ID,
                embedding VECTOR(3)
            )",
            &empty(),
        )
        .expect("create a context-aware vector table through the unrestricted handle");
    database
        .execute(
            "INSERT INTO inspection_vectors (id, context_id, embedding)
             VALUES ($id, $context_id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(0x4401))),
                ("context_id", Value::Uuid(context_id)),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("insert one harmless vector through the unrestricted handle");

    let mut violations = Vec::new();
    match database.execute("SHOW VECTOR_INDEXES", &empty()) {
        Ok(summary) => {
            let row = vector_index_row(&summary, "inspection_vectors", "embedding");
            assert_eq!(value_at(&summary, row, "vector_count"), &Value::Int64(1));
        }
        Err(error) => violations.push(format!(
            "the unrestricted SHOW VECTOR_INDEXES control failed: {error}"
        )),
    }
    match database.execute("SHOW VECTOR_PARTITIONS", &empty()) {
        Ok(partitions) => {
            let row = vector_index_row(&partitions, "inspection_vectors", "embedding");
            assert_eq!(value_at(&partitions, row, "live_rows"), &Value::Int64(1));
        }
        Err(error) => violations.push(format!(
            "the unrestricted SHOW VECTOR_PARTITIONS control failed: {error}"
        )),
    }

    let constrained = database.scoped_with_contexts(BTreeSet::from([ContextId::new(context_id)]));
    for statement in ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS"] {
        if let Some(violation) =
            inspection_refusal_violation(statement, constrained.execute(statement, &empty()))
        {
            violations.push(violation);
        }
    }

    assert!(
        violations.is_empty(),
        "whole-index inspection is available only through an unrestricted handle:\n{}",
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// A synced partition limit below use is refused and recoverable.
//
// A receiving node refuses a synced `MAX_PARTITIONS` below its live-or-
// retained use with the existing typed sync-DDL refusal, before any state
// write, keeps its declaration and every partition, and resumes without
// manual repair once its own use falls to or below the incoming limit.
// Proved on both the hand-cut `apply_changes` door (the mechanism:
// `a_lowered_partition_limit_arriving_over_sync_is_refused_and_changes_nothing`)
// and the real `pull_default` sync door (the journey:
// `a_refused_lowered_partition_limit_holds_the_tenants_pull_until_its_use_falls`).
// The journey's legs 3-6 pin the stalled-tenant behavior: while the refused
// DDL leads the page, later rows for that tenant do not arrive; this pins
// the observed behavior without deciding whether a refused DDL should stall
// the tenant.
// ---------------------------------------------------------------------------

const PARTITIONED_SCHEMA_CURRENT_ONLY: &str = "CREATE TABLE vector_items (
    id UUID PRIMARY KEY,
    scope_id UUID NOT NULL,
    embedding VECTOR(3)
        PARTITION_KEY (scope_id)
        MAX_PARTITIONS 8
        SEARCH_MODE AUTO
) HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

fn seeded_partitioned_pair_with_schema(schema: &str) -> (Database, Database, Vec<FixtureItem>) {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    replicate_schema(&sender, &receiver, schema);
    let items = fixture_items();
    let changes = commit_fixture_items(&sender, &items);
    apply_fixture_changes(&receiver, changes);
    (sender, receiver, items)
}

#[test]
fn a_lowered_partition_limit_arriving_over_sync_is_refused_and_changes_nothing() {
    let (sender, receiver, items) =
        seeded_partitioned_pair_with_schema(PARTITIONED_SCHEMA_CURRENT_ONLY);
    assert_partitioned_summary(&receiver, 2);

    // The sender's own local door validates a lowering ALTER against its own
    // live-or-retained use just as strictly as the receiver's synced-ALTER
    // arm does, so the sender must first bring its own use down to the new
    // limit -- WITHOUT yet delivering that reduction to the receiver, so the
    // receiver still genuinely holds more partitions than the new value.
    // The watermark for the
    // coming page is captured strictly after this local deletion.
    let delete_watermark = sender.current_lsn();
    sender
        .execute(
            "DELETE FROM vector_items WHERE id = $id",
            &params(vec![("id", Value::Uuid(items[1].id))]),
        )
        .expect("delete one scope's row on the sender to bring its own use to one");
    let delete_changes = sender.changes_since(delete_watermark);
    assert!(
        delete_changes.ddl.is_empty(),
        "the recovery page must carry only the row deletion"
    );

    // Leg 2 -- cut a page whose only DDL entry is the lowering ALTER.
    let watermark = sender.current_lsn();
    sender
        .execute(
            "ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 1",
            &empty(),
        )
        .expect("the sender accepts the lowered limit once its own use is one");
    let changes = sender.changes_since(watermark);
    assert_eq!(
        changes.ddl.len(),
        1,
        "the page's only DDL entry must be the lowering ALTER"
    );
    let changes_for_recovery = changes.clone();

    // Leg 3 -- the typed refusal.
    let refusal = receiver
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("a synced limit below the receiver's live use must be refused");
    match refusal {
        Error::VectorPartitionLimitExceeded {
            index,
            max_partitions,
        } => {
            assert_eq!(index, VectorIndexRef::new("vector_items", "embedding"));
            assert_eq!(max_partitions, 1);
        }
        other => panic!("expected VectorPartitionLimitExceeded, got {other:?}"),
    }

    // Leg 4 -- nothing moved.
    let schema_text = render_table_meta(
        "vector_items",
        &receiver
            .table_meta("vector_items")
            .expect("vector_items metadata exists on the receiver"),
    );
    assert!(
        schema_text.contains("MAX_PARTITIONS 8"),
        "the receiver's declaration must keep its prior limit: {schema_text}"
    );
    assert_partitioned_summary(&receiver, 2);
    assert_partition_membership(&receiver, &[items[0].scope_id, items[1].scope_id]);
    assert_eq!(
        search_in_scope(&receiver, items[0].scope_id, items[0].vector.clone())
            .rows
            .len(),
        1,
        "a scoped search into an untouched partition must still succeed"
    );
    assert_eq!(
        search_in_scope(&receiver, items[1].scope_id, items[1].vector.clone())
            .rows
            .len(),
        1
    );

    // Leg 5 -- the same typed refusal from a purely local door, on a third,
    // independently-seeded store of the same shape.
    let (_third_sender, third, _third_items) = seeded_partitioned_pair();
    let local_error = third
        .execute(
            "ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 1",
            &empty(),
        )
        .expect_err("the local door refuses the same lowered limit");
    match local_error {
        Error::VectorPartitionLimitExceeded {
            index,
            max_partitions,
        } => {
            assert_eq!(index, VectorIndexRef::new("vector_items", "embedding"));
            assert_eq!(max_partitions, 1);
        }
        other => panic!("expected VectorPartitionLimitExceeded, got {other:?}"),
    }

    // Leg 6 -- recovery once this same receiver's use falls, replaying the
    // SAME lowered-limit page from the same sender. `HISTORY CURRENT ONLY`
    // avoids retained history keeping the deleted scope's partition alive;
    // the count is read, never assumed.
    receiver
        .apply_changes(
            delete_changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("the deletion replicates to the receiver");
    let after_delete = receiver
        .execute(
            "SHOW VECTOR_PARTITIONS FOR vector_items.embedding",
            &empty(),
        )
        .expect("the receiver still describes its vector partitions");
    assert_eq!(
        after_delete.rows.len(),
        1,
        "HISTORY CURRENT ONLY must drop the deleted scope's partition rather than retain it: {after_delete:?}"
    );

    receiver
        .apply_changes(
            changes_for_recovery,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect("once the receiver's own use falls to one, the same lowered-limit page applies");
    let recovered_schema = render_table_meta(
        "vector_items",
        &receiver
            .table_meta("vector_items")
            .expect("vector_items metadata exists on the receiver"),
    );
    assert!(
        recovered_schema.contains("MAX_PARTITIONS 1"),
        "the receiver must render the recovered lowered limit: {recovered_schema}"
    );
}

#[cfg(feature = "test-seams")]
struct RunningPartitionLimitServer {
    #[allow(dead_code)]
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "test-seams")]
impl RunningPartitionLimitServer {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("server task stops");
    }
}

#[cfg(feature = "test-seams")]
async fn start_partition_limit_server(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningPartitionLimitServer {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&pull_subject(tenant))
        .await;
    broker
        .wait_for_registered_route_for_test(&push_subject(tenant))
        .await;
    RunningPartitionLimitServer {
        server,
        shutdown,
        task,
    }
}

#[cfg(feature = "test-seams")]
const PARTITION_LIMIT_SYNC_TENANT: &str = "vector-partition-limit-sync";

#[cfg(feature = "test-seams")]
#[tokio::test]
async fn a_refused_lowered_partition_limit_holds_the_tenants_pull_until_its_use_falls() {
    let root = tempfile::TempDir::new().expect("temporary partition-limit-sync stores");
    let hub =
        Arc::new(Database::open(root.path().join("limit-hub.redb")).expect("open file-backed hub"));
    let edge = Arc::new(
        Database::open(root.path().join("limit-edge.redb")).expect("open file-backed edge"),
    );
    hub.execute(PARTITIONED_SCHEMA_CURRENT_ONLY, &empty())
        .expect("declare the partitioned vector table on the hub");
    for item in fixture_items() {
        hub.execute(
            "INSERT INTO vector_items (id, scope_id, embedding) VALUES ($id, $scope_id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(item.id)),
                ("scope_id", Value::Uuid(item.scope_id)),
                ("embedding", Value::Vector(item.vector.clone())),
            ]),
        )
        .expect("seed one row per scope on the hub");
    }

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_partition_limit_server(
        &broker,
        PARTITION_LIMIT_SYNC_TENANT,
        hub.clone(),
        hub_identity,
    )
    .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&edge_node),
        TenantId::from(PARTITION_LIMIT_SYNC_TENANT),
        edge_identity,
    );
    client
        .pull_default()
        .await
        .expect("the initial pull succeeds");
    assert_partitioned_summary(&edge, 2);

    // Leg 2 -- capture the watermark before the lowering ALTER.
    let watermark = client.pull_watermark();

    // The hub's own local door validates a lowering ALTER against its own
    // live-or-retained use just as strictly as the receiver's synced-ALTER
    // arm does, so the hub must first bring its own use down to one --
    // without yet delivering that reduction to the edge, so the edge still
    // genuinely holds more partitions than the new value. A hand-cut
    // snapshot of just this deletion is captured now, for the leg 7 recovery
    // below (the same door the `apply_changes` test's leg 6 uses to force
    // reclamation).
    hub.execute(
        "DELETE FROM vector_items WHERE id = $id",
        &params(vec![("id", Value::Uuid(fixture_items()[1].id))]),
    )
    .expect("delete one scope's row on the hub to bring its own use to one");
    let delete_only_changes = hub.changes_since(watermark);
    assert!(
        delete_only_changes.ddl.is_empty(),
        "the hand-cut recovery page must carry only the row deletion, no DDL"
    );

    // Leg 3 -- the typed refusal, through the real pull door. The edge has
    // not yet received the hub's deletion, so its own live-plus-retained use
    // is still two.
    hub.execute(
        "ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 1",
        &empty(),
    )
    .expect("the hub accepts the lowered limit once its own use is one");
    let refusal = client
        .pull_default()
        .await
        .expect_err("a synced lowered limit below the edge's use must be refused");
    assert!(
        matches!(refusal, Error::VectorPartitionLimitExceeded { .. }),
        "expected VectorPartitionLimitExceeded, got {refusal:?}"
    );

    // Leg 4 -- the watermark did not move.
    assert_eq!(
        client.pull_watermark(),
        watermark,
        "a refused DDL page must not advance the durable pull watermark"
    );
    assert_partitioned_summary(&edge, 2);

    // Leg 5 -- the tenant is stalled: an ordinary row committed after the
    // refusal into an existing scope does not reach the edge, and the same
    // typed refusal repeats. This pins the observed stalled-tenant journey;
    // it does not declare it the desired one.
    let stalled_row_id = Uuid::from_u128(0x9909);
    hub.execute(
        "INSERT INTO vector_items (id, scope_id, embedding) VALUES ($id, $scope_id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(stalled_row_id)),
            ("scope_id", Value::Uuid(fixture_items()[0].scope_id)),
            ("embedding", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .expect("an ordinary insert into an existing scope must succeed on the hub");
    let second_refusal = client
        .pull_default()
        .await
        .expect_err("the stalled tenant keeps refusing the same page");
    assert!(matches!(
        second_refusal,
        Error::VectorPartitionLimitExceeded { .. }
    ));
    assert!(
        edge.execute(
            "SELECT id FROM vector_items WHERE id = $id",
            &params(vec![("id", Value::Uuid(stalled_row_id))]),
        )
        .expect("select must still succeed on the edge")
        .rows
        .is_empty(),
        "the stalled-tenant journey: an unrelated row committed after the \
         refusal must not reach the edge while the refused DDL still leads the page"
    );

    // Leg 6 -- a hub raise does not clear it, because `changes_since`
    // replays the superseded lowering ALTER first.
    hub.execute(
        "ALTER TABLE vector_items ALTER COLUMN embedding SET MAX_PARTITIONS 16",
        &empty(),
    )
    .expect("the hub raises the limit again");
    let third_refusal = client.pull_default().await.expect_err(
        "a hub raise does not clear the earlier refusal: changes_since replays it first",
    );
    assert!(matches!(
        third_refusal,
        Error::VectorPartitionLimitExceeded { .. }
    ));
    assert_partitioned_summary(&edge, 2);
    let unrecovered_schema = render_table_meta(
        "vector_items",
        &edge
            .table_meta("vector_items")
            .expect("vector_items metadata exists on the edge"),
    );
    assert!(
        unrecovered_schema.contains("MAX_PARTITIONS 8"),
        "the edge must still render its prior limit, not the hub's raise: {unrecovered_schema}"
    );

    // Leg 7 -- the one real exit: drive the edge's own use down to one scope
    // through the hand-cut `apply_changes` snapshot captured above, bypassing
    // the stuck pull page entirely, then the stuck pull applies both
    // superseded ALTERs in order.
    edge.apply_changes(
        delete_only_changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .expect("the hand-cut deletion reduces the edge's own live-plus-retained use");
    assert_partitioned_summary(&edge, 1);

    client
        .pull_default()
        .await
        .expect("once the edge's own use falls to one, the stuck pull applies cleanly");
    assert_eq!(
        client.pull_watermark(),
        hub.current_lsn(),
        "the watermark must advance past both superseded ALTERs"
    );
    let recovered_schema = render_table_meta(
        "vector_items",
        &edge
            .table_meta("vector_items")
            .expect("vector_items metadata exists on the edge"),
    );
    assert!(
        recovered_schema.contains("MAX_PARTITIONS 16"),
        "both superseded ALTERs must apply in order once unblocked: {recovered_schema}"
    );
    assert!(
        !edge
            .execute(
                "SELECT id FROM vector_items WHERE id = $id",
                &params(vec![("id", Value::Uuid(stalled_row_id))]),
            )
            .expect("select must succeed")
            .rows
            .is_empty(),
        "the row from leg 5 must now be present"
    );

    running.stop().await;
    edge.close().expect("close partition-limit-sync edge");
}

// ---------------------------------------------------------------------------
// The generic recognition-reference journey: a shared reference table searched
// per Context and embedding space. It uses no Context Graph or Vigil model semantics: the table is
// deliberately generic, while Context is the engine's access boundary.
// ---------------------------------------------------------------------------
#[cfg(feature = "test-seams")]
const RECOGNITION_REFERENCE_SCHEMA: &str = "CREATE TABLE recognition_references (
    id UUID PRIMARY KEY,
    context_id UUID NOT NULL CONTEXT_ID,
    embedding_space_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    eligible BOOL NOT NULL,
    recognition_scope TEXT NOT NULL,
    embedding VECTOR(3)
        PARTITION_KEY (embedding_space_id, entity_type)
        MAX_PARTITIONS 8
        SEARCH_MODE AUTO
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

#[cfg(feature = "test-seams")]
const RECOGNITION_REFERENCE_RESIDUAL_INDEX: &str = "CREATE INDEX \
    recognition_references_eligible_scope_idx \
    ON recognition_references (eligible, recognition_scope)";

#[cfg(feature = "test-seams")]
const RECOGNITION_REFERENCE_TENANT: &str = "recognition-reference-tenant";
#[cfg(feature = "test-seams")]
const OTHER_RECOGNITION_REFERENCE_TENANT: &str = "other-recognition-reference-tenant";

#[cfg(feature = "test-seams")]
const RECOGNITION_SEARCH_SQL: &str = "SELECT id FROM recognition_references \
    WHERE eligible = $eligible AND recognition_scope = $recognition_scope \
    ORDER BY embedding <=> $query LIMIT 1";

#[cfg(feature = "test-seams")]
fn recognition_search_params(scope: &str) -> HashMap<String, Value> {
    params(vec![
        ("eligible", Value::Bool(true)),
        ("recognition_scope", Value::Text(scope.to_owned())),
        ("query", Value::Vector(vec![1.0, 0.0, 0.0])),
    ])
}

#[cfg(feature = "test-seams")]
fn recognition_result_ids(result: &QueryResult) -> Vec<Uuid> {
    let id = column_index(result, "id");
    result
        .rows
        .iter()
        .map(|row| match &row[id] {
            Value::Uuid(id) => *id,
            other => panic!("recognition result id is UUID, got {other:?}"),
        })
        .collect()
}

#[cfg(feature = "test-seams")]
fn assert_recognition_partition(database: &Database) {
    let partitions = database
        .execute(
            "SHOW VECTOR_PARTITIONS FOR recognition_references.embedding",
            &empty(),
        )
        .expect("the generic recognition table has one inspectable partition");
    assert_eq!(partitions.rows.len(), 1);
    assert_eq!(
        value_at(&partitions, 0, "partition_key"),
        &Value::Json(serde_json::json!({
            "embedding_space_id": "vision-face-v1",
            "entity_type": "person",
        })),
        "the stable embedding-space/entity-type tuple, rather than Context or recognition scope, owns the partition"
    );
    assert_eq!(value_at(&partitions, 0, "live_rows"), &Value::Int64(1));
}

#[cfg(feature = "test-seams")]
#[derive(Default)]
struct AuthorizationBeforeCandidates {
    access_seen: AtomicBool,
}

#[cfg(feature = "test-seams")]
impl bounded::ExecutionProbe for AuthorizationBeforeCandidates {
    fn before_work(&self, _source: TestWorkSource, _completed_work: u64) {}

    fn before_source_touch(&self, touch: TestSourceTouch, _completed_items: u64) {
        match touch {
            TestSourceTouch::AccessRow => self.access_seen.store(true, Ordering::SeqCst),
            TestSourceTouch::BruteForceVectorCandidate | TestSourceTouch::HnswCandidate => {
                assert!(
                    self.access_seen.load(Ordering::SeqCst),
                    "the Context authorization gate must run before any vector candidate is scored"
                );
            }
            _ => {}
        }
    }

    fn cancellation_observed(&self, _completed_work: u64) {}
}

#[cfg(feature = "test-seams")]
#[derive(Clone, Copy)]
struct RecognitionFrozenClock;

#[cfg(feature = "test-seams")]
impl DeadlineClock for RecognitionFrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

#[cfg(feature = "test-seams")]
fn recognition_bounded_request(params: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(
        RECOGNITION_SEARCH_SQL,
        params,
        ReadLimits {
            result_rows: 64,
            result_bytes: 64 * 1024,
            work: 10_000,
            active_ms: 10_000,
            memory: 1024 * 1024,
            cursor_page_rows: 64,
            cursor_page_bytes: 64 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 10_000,
        },
        Arc::new(RecognitionFrozenClock),
    )
}

#[cfg(feature = "test-seams")]
#[tokio::test]
async fn recognition_reference_sync_scope_delete_and_tenant_isolation_journey() {
    let root = tempfile::TempDir::new().expect("temporary recognition-reference stores");
    let hub = Arc::new(
        Database::open(root.path().join("recognition-hub.redb"))
            .expect("open the tenant recognition hub"),
    );
    let kerala = Arc::new(
        Database::open(root.path().join("recognition-kerala.redb")).expect("open the Kerala node"),
    );
    let bangalore = Arc::new(
        Database::open(root.path().join("recognition-bangalore.redb"))
            .expect("open the Bangalore node"),
    );
    let other_tenant_hub = Arc::new(
        Database::open(root.path().join("other-recognition-hub.redb"))
            .expect("open the other tenant hub"),
    );
    let other_tenant_node = Arc::new(
        Database::open(root.path().join("other-recognition-node.redb"))
            .expect("open the other tenant node"),
    );

    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let recognition_server = start_partition_limit_server(
        &broker,
        RECOGNITION_REFERENCE_TENANT,
        hub.clone(),
        hub_identity.clone(),
    )
    .await;
    let other_tenant_server = start_partition_limit_server(
        &broker,
        OTHER_RECOGNITION_REFERENCE_TENANT,
        other_tenant_hub.clone(),
        hub_identity,
    )
    .await;

    let kerala_identity = Arc::new(FabricIdentity::generate());
    let kerala_node_id = kerala_identity.node_id();
    let kerala_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        kerala.clone(),
        broker.client_as(&kerala_node_id),
        TenantId::from(RECOGNITION_REFERENCE_TENANT),
        kerala_identity,
    );
    let bangalore_identity = Arc::new(FabricIdentity::generate());
    let bangalore_node_id = bangalore_identity.node_id();
    let bangalore_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        bangalore.clone(),
        broker.client_as(&bangalore_node_id),
        TenantId::from(RECOGNITION_REFERENCE_TENANT),
        bangalore_identity,
    );
    let other_identity = Arc::new(FabricIdentity::generate());
    let other_node_id = other_identity.node_id();
    let other_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        other_tenant_node.clone(),
        broker.client_as(&other_node_id),
        TenantId::from(OTHER_RECOGNITION_REFERENCE_TENANT),
        other_identity,
    );

    let tenant_context = Uuid::from_u128(0x8101);
    let kerala_context = Uuid::from_u128(0x8102);
    let other_tenant_context = Uuid::from_u128(0x8103);
    let reference_id = Uuid::from_u128(0x8201);
    kerala
        .execute(RECOGNITION_REFERENCE_SCHEMA, &empty())
        .expect("declare the generic recognition-reference table on Kerala");
    kerala
        .execute(RECOGNITION_REFERENCE_RESIDUAL_INDEX, &empty())
        .expect("index the residual eligibility and scope columns");
    kerala
        .execute(
            "INSERT INTO recognition_references \
             (id, context_id, embedding_space_id, entity_type, eligible, recognition_scope, embedding) \
             VALUES ($id, $context_id, $embedding_space_id, $entity_type, $eligible, \
                     $recognition_scope, $embedding)",
            &params(vec![
                ("id", Value::Uuid(reference_id)),
                ("context_id", Value::Uuid(tenant_context)),
                (
                    "embedding_space_id",
                    Value::Text("vision-face-v1".to_owned()),
                ),
                ("entity_type", Value::Text("person".to_owned())),
                ("eligible", Value::Bool(true)),
                ("recognition_scope", Value::Text("tenant".to_owned())),
                ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .expect("enrol the deterministic Kerala reference through ordinary DML");
    kerala_client
        .push()
        .await
        .expect("sync the Kerala enrolment through the authenticated hub");
    bangalore_client
        .pull_default()
        .await
        .expect("sync the enrolled reference to Bangalore");

    let bangalore_authorized =
        bangalore.scoped_with_contexts(BTreeSet::from([ContextId::new(tenant_context)]));
    let found = bangalore_authorized
        .execute(RECOGNITION_SEARCH_SQL, &recognition_search_params("tenant"))
        .expect("the authorized Bangalore node searches its tenant context");
    assert_eq!(recognition_result_ids(&found), vec![reference_id]);
    assert_recognition_partition(&bangalore);

    // Move the reference's authorization and residual recognition scope to
    // Kerala. Neither mutable field belongs to the declared partition key.
    kerala
        .execute(
            "UPDATE recognition_references \
             SET context_id = $context_id, recognition_scope = $recognition_scope \
             WHERE id = $id",
            &params(vec![
                ("context_id", Value::Uuid(kerala_context)),
                ("recognition_scope", Value::Text("kerala".to_owned())),
                ("id", Value::Uuid(reference_id)),
            ]),
        )
        .expect("restrict the generic reference to Kerala through ordinary DML");
    kerala_client
        .push()
        .await
        .expect("sync the Kerala-only restriction through the hub");
    bangalore_client
        .pull_default()
        .await
        .expect("Bangalore receives the scope restriction");

    let probe = Arc::new(AuthorizationBeforeCandidates::default());
    let mut request = recognition_bounded_request(recognition_search_params("kerala"));
    request.probe = Some(probe.clone());
    let hidden = bounded::execute(&bangalore_authorized, &request)
        .expect("the restricted Bangalore search completes without disclosing Kerala data");
    assert!(
        recognition_result_ids(&hidden.result).is_empty(),
        "Bangalore must no longer consider a Kerala-restricted reference"
    );
    assert!(
        probe.access_seen.load(Ordering::SeqCst),
        "the bounded production kernel must check Context authorization for the restricted row"
    );
    let admin_control = bangalore
        .execute(RECOGNITION_SEARCH_SQL, &recognition_search_params("kerala"))
        .expect("the unrestricted control confirms the received Kerala row still exists");
    assert_eq!(recognition_result_ids(&admin_control), vec![reference_id]);
    assert_recognition_partition(&bangalore);

    kerala
        .execute(
            "UPDATE recognition_references \
             SET context_id = $context_id, recognition_scope = $recognition_scope \
             WHERE id = $id",
            &params(vec![
                ("context_id", Value::Uuid(tenant_context)),
                ("recognition_scope", Value::Text("tenant".to_owned())),
                ("id", Value::Uuid(reference_id)),
            ]),
        )
        .expect("restore the reference to the tenant scope through ordinary DML");
    kerala_client
        .push()
        .await
        .expect("sync the restored tenant scope through the hub");
    bangalore_client
        .pull_default()
        .await
        .expect("Bangalore receives the restored tenant scope");
    let restored = bangalore_authorized
        .execute(RECOGNITION_SEARCH_SQL, &recognition_search_params("tenant"))
        .expect("Bangalore considers the restored tenant reference");
    assert_eq!(recognition_result_ids(&restored), vec![reference_id]);
    assert_recognition_partition(&bangalore);

    // A second tenant uses the same authenticated in-process transport but a
    // distinct tenant route. It receives only its empty copy of the generic
    // schema, so it can neither inspect this reference nor match it.
    other_tenant_hub
        .execute(RECOGNITION_REFERENCE_SCHEMA, &empty())
        .expect("declare the generic table for the other tenant");
    other_tenant_hub
        .execute(RECOGNITION_REFERENCE_RESIDUAL_INDEX, &empty())
        .expect("declare the other tenant residual index");
    other_client
        .pull_default()
        .await
        .expect("pull the other tenant's empty generic table");
    let other_authorized = other_tenant_node
        .scoped_with_contexts(BTreeSet::from([ContextId::new(other_tenant_context)]));
    assert!(
        inspection_refusal_violation(
            "SHOW VECTOR_INDEXES",
            other_authorized.execute("SHOW VECTOR_INDEXES", &empty()),
        )
        .is_none(),
        "a constrained other-tenant handle cannot inspect the source tenant's vector index"
    );
    let other_summary = other_tenant_node
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("the other tenant admin may inspect only its own empty table");
    let other_row = vector_index_row(&other_summary, "recognition_references", "embedding");
    assert_eq!(
        value_at(&other_summary, other_row, "vector_count"),
        &Value::Int64(0)
    );
    let other_match = other_authorized
        .execute(RECOGNITION_SEARCH_SQL, &recognition_search_params("tenant"))
        .expect("the other tenant's generic search completes");
    assert!(
        recognition_result_ids(&other_match).is_empty(),
        "the other tenant cannot match the source tenant's reference"
    );

    kerala
        .execute(
            "DELETE FROM recognition_references WHERE id = $id",
            &params(vec![("id", Value::Uuid(reference_id))]),
        )
        .expect("delete the generic reference through ordinary DML");
    kerala_client
        .push()
        .await
        .expect("sync the ordinary reference delete through the hub");
    bangalore_client
        .pull_default()
        .await
        .expect("Bangalore receives the ordinary reference delete");
    let deleted = bangalore_authorized
        .execute(RECOGNITION_SEARCH_SQL, &recognition_search_params("tenant"))
        .expect("Bangalore search after the delete completes");
    assert!(
        recognition_result_ids(&deleted).is_empty(),
        "the ordinary deleted reference must not match after resync"
    );

    other_tenant_server.stop().await;
    recognition_server.stop().await;
    kerala.close().expect("close the Kerala node");
    bangalore.close().expect("close the Bangalore node");
    other_tenant_node
        .close()
        .expect("close the other tenant node");
}

// ---------------------------------------------------------------------------
// The bounded reader applies access restrictions before top-k on a receiver.
//
// After a synced partitioned vector table is received, a Context-restricted
// reader on the receiver must have its disclosure list only its OWN
// authorized ids and report a nonzero aggregate count -- never the whole
// synced index's membership. Uses the hand-cut `apply_changes` door, like
// the partition-limit refusal test above, so it needs no `test-seams`
// server plumbing.
// ---------------------------------------------------------------------------
const CONTEXT_PARTITIONED_SCHEMA: &str = "CREATE TABLE context_vector_items (
    id UUID PRIMARY KEY,
    context_id UUID NOT NULL CONTEXT_ID,
    scope_id UUID NOT NULL,
    embedding VECTOR(3)
        PARTITION_KEY (scope_id)
        MAX_PARTITIONS 8
        SEARCH_MODE AUTO
) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

#[test]
fn a_synced_restricted_reads_disclosure_lists_only_its_own_authorized_ids() {
    let sender = Database::open_memory();
    let receiver = Database::open_memory();
    replicate_schema(&sender, &receiver, CONTEXT_PARTITIONED_SCHEMA);
    let displayed = render_table_meta(
        "context_vector_items",
        &receiver
            .table_meta("context_vector_items")
            .expect("received table"),
    );
    assert!(displayed.contains("CONTEXT_ID"), "{displayed}");
    let replayed = Database::open_memory();
    replayed
        .execute(&displayed, &empty())
        .expect("replay displayed Context declaration");
    assert!(
        replayed
            .table_meta("context_vector_items")
            .unwrap()
            .columns
            .iter()
            .find(|column| column.name == "context_id")
            .unwrap()
            .context_id
    );

    let allowed_context = Uuid::from_u128(0x5501);
    let foreign_context = Uuid::from_u128(0x5502);
    let allowed_item = Uuid::from_u128(0x5601);
    let foreign_item = Uuid::from_u128(0x5602);
    let watermark = sender.current_lsn();
    let tx = sender.begin().expect("begin the two-row transaction");
    for (row_id, context_id, scope_id, vector) in [
        (
            allowed_item,
            allowed_context,
            Uuid::from_u128(0x6601),
            vec![1.0, 0.0, 0.0],
        ),
        (
            foreign_item,
            foreign_context,
            Uuid::from_u128(0x6602),
            vec![0.9, 0.1, 0.0],
        ),
    ] {
        sender
            .execute_in_tx(
                tx,
                "INSERT INTO context_vector_items (id, context_id, scope_id, embedding) \
                 VALUES ($id, $context_id, $scope_id, $embedding)",
                &params(vec![
                    ("id", Value::Uuid(row_id)),
                    ("context_id", Value::Uuid(context_id)),
                    ("scope_id", Value::Uuid(scope_id)),
                    ("embedding", Value::Vector(vector)),
                ]),
            )
            .expect("stage one context-restricted vector row");
    }
    sender.commit(tx).expect("commit both rows together");
    apply_fixture_changes(&receiver, sender.changes_since(watermark));

    let scoped = receiver.scoped_with_contexts(BTreeSet::from([ContextId::new(allowed_context)]));
    let sql = "SELECT id FROM context_vector_items \
               ORDER BY embedding <=> $query LIMIT 10";
    let bound = params(vec![("query", Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let runtime = scoped
        .execute(sql, &bound)
        .expect("the restricted read on the synced receiver succeeds");
    let ids: Vec<Uuid> = {
        let column = column_index(&runtime, "id");
        runtime
            .rows
            .iter()
            .map(|row| match &row[column] {
                Value::Uuid(value) => *value,
                other => panic!("expected UUID id, got {other:?}"),
            })
            .collect()
    };
    assert_eq!(
        ids,
        vec![allowed_item],
        "the synced restricted read must list only the reader's own authorized row, never the \
         foreign Context's row that arrived over the same sync"
    );
    let disclosure = runtime
        .trace
        .vector_search
        .expect("the executed synced restricted read publishes a vector disclosure");
    assert_eq!(
        disclosure.aggregate_allowed_vectors,
        Some(1),
        "the disclosed aggregate must count only the reader's own authorized partition, never \
         the whole synced index's membership: {disclosure:?}"
    );
}
