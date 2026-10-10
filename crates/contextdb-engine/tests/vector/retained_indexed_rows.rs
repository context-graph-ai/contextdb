//! An indexed writer must rebuild from the complete committed row.
//!
//! A file-backed writer deliberately keeps a compact directory in memory.
//! The directory names a row and its indexed fields, but it is not the row
//! body: a long unindexed text stays in Redb and a quantized vector stays in
//! the vector store. An indexed UPDATE that clones the directory row writes a
//! replacement with those fields missing. This test takes the whole path a
//! caller takes: an indexed read, an indexed update, a restart, a key read,
//! and a ranked read.

use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
#[cfg(feature = "test-seams")]
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_vector::stored_vector_value;
#[cfg(feature = "test-seams")]
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSIONS: usize = 8;

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

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn stored_vector() -> Vec<f32> {
    vec![0.91, 0.73, 0.55, 0.37, 0.19, 0.01, -0.17, -0.35]
}

fn expected_vector() -> Value {
    expected_stored_vector(&stored_vector())
}

fn expected_stored_vector(vector: &[f32]) -> Value {
    Value::Vector(stored_vector_value(
        vector,
        contextdb_core::table_meta::VectorQuantization::SQ8,
    ))
}

fn bounded_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 4,
        result_bytes: 512 * 1024,
        work: 24,
        active_ms: 10_000,
        memory: 512 * 1024,
        cursor_page_rows: 4,
        cursor_page_bytes: 512 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 20_000,
    }
}

#[test]
fn an_indexed_update_keeps_the_complete_disk_row_after_reopen() {
    let directory = TempDir::new().expect("task-scoped retained-row directory");
    let path = directory.path().join("retained-indexed-row.db");
    let id = Uuid::from_u128(0x00E9_0000_0000_0000_0000_0000_0000_0001);
    let group = "retain-this";
    let body = format!("complete disk body:{}", "long-text-".repeat(4_096));

    {
        let database = Database::open(&path).expect("create file-backed store");
        database
            .execute(
                &format!(
                    "CREATE TABLE retained_rows (\
                     id UUID PRIMARY KEY, \
                     group_name TEXT NOT NULL, \
                     body TEXT NOT NULL, \
                     embedding VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'), \
                     published BOOLEAN NOT NULL)"
                ),
                &empty(),
            )
            .expect("declare a row whose body and vector are outside its index");
        database
            .execute(
                "CREATE INDEX retained_rows_group_idx ON retained_rows(group_name)",
                &empty(),
            )
            .expect("declare the indexed update predicate");
        database
            .execute(
                "INSERT INTO retained_rows (id, group_name, body, embedding, published) \
                 VALUES ($id, $group, $body, $embedding, false)",
                &params([
                    ("id", Value::Uuid(id)),
                    ("group", Value::Text(group.to_owned())),
                    ("body", Value::Text(body.clone())),
                    ("embedding", Value::Vector(stored_vector())),
                ]),
            )
            .expect("insert the complete committed row");

        let selected = database
            .execute(
                "SELECT body, embedding FROM retained_rows WHERE group_name = $group",
                &params([("group", Value::Text(group.to_owned()))]),
            )
            .expect("an index scan returns the complete row before the update");
        assert_eq!(
            selected.rows,
            vec![vec![Value::Text(body.clone()), expected_vector()]],
            "the index posting resolves to the durable text and quantized vector, not its compact directory"
        );

        database
            .execute(
                "UPDATE retained_rows SET published = true WHERE group_name = $group",
                &params([("group", Value::Text(group.to_owned()))]),
            )
            .expect("an indexed update changes only its named field");
        database.close().expect("close the writer before reopening");
    }

    let reopened = Database::open(&path).expect("reopen the committed store");
    let by_key = reopened
        .execute(
            "SELECT body, embedding, published FROM retained_rows WHERE id = $id",
            &params([("id", Value::Uuid(id))]),
        )
        .expect("a primary-key read returns the replacement");
    assert_eq!(
        by_key.rows,
        vec![vec![
            Value::Text(body.clone()),
            expected_vector(),
            Value::Bool(true)
        ]],
        "an unrelated indexed update preserves both retained fields after restart"
    );

    let ranked = reopened
        .execute(
            "SELECT id, body, embedding FROM retained_rows ORDER BY embedding <=> $query LIMIT 1",
            &params([("query", Value::Vector(stored_vector()))]),
        )
        .expect("the retained vector remains rankable after the replacement");
    assert_eq!(
        ranked.rows,
        vec![vec![
            Value::Uuid(id),
            Value::Text(body.clone()),
            expected_vector()
        ]],
        "the ranked row carries the same retained text and stored vector"
    );

    let bounded = bounded::execute(
        &reopened,
        &bounded::BoundedReadRequest::new(
            "SELECT body, embedding FROM retained_rows WHERE group_name = $group",
            params([("group", Value::Text(group.to_owned()))]),
            bounded_limits(),
            Arc::new(FrozenClock),
        ),
    )
    .expect("the complete indexed row fits the statement's existing bounded read limits");
    assert_eq!(
        bounded.result.rows,
        vec![vec![Value::Text(body), expected_vector()]],
        "the bounded read agrees with the eager index scan"
    );
    assert_eq!(
        bounded.result.trace.physical_plan, "IndexScan",
        "the bounded read keeps using the declared index"
    );
    assert!(
        bounded.result.trace.rows_examined <= 1,
        "the one-row indexed read examined {} postings, so it must not hide a table scan",
        bounded.result.trace.rows_examined
    );
    reopened.close().expect("close the reopened writer");
}

#[cfg(feature = "test-seams")]
#[test]
#[serial]
fn eager_indexed_projection_loads_only_selected_vector_bodies() {
    let directory = TempDir::new().expect("task-scoped indexed projection directory");
    let path = directory.path().join("indexed-projection.db");
    let matching = 3_u128;
    let wanted_scope = Uuid::from_u128(0xE900);
    let other_scope = Uuid::from_u128(0xE901);
    {
        let database = Database::open(&path).expect("create the projection fixture");
        database
            .execute(
                &format!(
                    "CREATE TABLE projected_rows (\
                     id UUID PRIMARY KEY, \
                     scope_id UUID NOT NULL, \
                     group_name TEXT NOT NULL, \
                     embedding VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8') \
                     PARTITION_KEY (scope_id) MAX_PARTITIONS 4)"
                ),
                &empty(),
            )
            .expect("declare the projection fixture");
        database
            .execute(
                "CREATE INDEX projected_rows_group_idx ON projected_rows(group_name)",
                &empty(),
            )
            .expect("declare the projection-driving index");
        for ordinal in 0_u128..8 {
            database
                .execute(
                    "INSERT INTO projected_rows (id, scope_id, group_name, embedding) \
                     VALUES ($id, $scope, $group, $embedding)",
                    &params([
                        ("id", Value::Uuid(Uuid::from_u128(0xE910 + ordinal))),
                        (
                            "scope",
                            Value::Uuid(if ordinal < matching {
                                wanted_scope
                            } else {
                                other_scope
                            }),
                        ),
                        (
                            "group",
                            Value::Text(
                                if ordinal < matching {
                                    "wanted"
                                } else {
                                    "other"
                                }
                                .into(),
                            ),
                        ),
                        ("embedding", Value::Vector(stored_vector())),
                    ]),
                )
                .expect("insert one projection fixture row");
        }
        database
            .close()
            .expect("close before the physical read proof");
    }

    let database = Database::open(&path).expect("reopen the projection fixture");
    let activity_before = database.__vector_passive_activity_counters_for_test();
    let unselected_work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let ids = database
        .execute(
            "SELECT id FROM projected_rows WHERE group_name = 'wanted'",
            &empty(),
        )
        .expect("the eager index scan projects only ids");
    assert_eq!(ids.rows.len(), matching as usize);
    assert_eq!(ids.trace.physical_plan, "IndexScan");
    assert_eq!(
        unselected_work.counters().decoded_vectors,
        0,
        "an eager indexed projection that omits the vector performs no physical vector decode"
    );
    drop(unselected_work);
    let activity_after_unselected = database.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_unselected.raw_partition_loads, activity_before.raw_partition_loads,
        "an unselected vector partition stays dormant"
    );

    let selected_work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let vectors = database
        .execute(
            "SELECT embedding FROM projected_rows WHERE group_name = 'wanted'",
            &empty(),
        )
        .expect("the eager index scan projects the selected vectors");
    assert_eq!(vectors.rows.len(), matching as usize);
    assert_eq!(vectors.trace.physical_plan, "IndexScan");
    let activity_after_selected = database.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_selected.raw_partition_loads - activity_after_unselected.raw_partition_loads,
        0,
        "projecting selected bodies does not hydrate a complete partition"
    );
    assert_eq!(selected_work.counters().decoded_vectors, matching as u64);
    assert!(vectors.rows.iter().all(|row| row == &vec![expected_vector()]));
    database.close().expect("close the projection fixture");
}

#[cfg(feature = "test-seams")]
#[test]
#[serial]
fn indexed_graph_anchor_resolution_does_not_load_unselected_vectors() {
    let directory = TempDir::new().expect("task-scoped graph-anchor directory");
    let path = directory.path().join("graph-anchor.db");
    let source = Uuid::from_u128(0xE950);
    let target = Uuid::from_u128(0xE951);
    let scope = Uuid::from_u128(0xE952);
    {
        let database = Database::open(&path).expect("create the graph-anchor fixture");
        database
            .execute(
                &format!(
                    "CREATE TABLE nodes (\
                     id UUID PRIMARY KEY, \
                     scope_id UUID NOT NULL, \
                     name TEXT NOT NULL, \
                     embedding VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8') \
                     PARTITION_KEY (scope_id) MAX_PARTITIONS 4)"
                ),
                &empty(),
            )
            .expect("declare graph nodes with a dormant vector body");
        database
            .execute("CREATE INDEX nodes_name_idx ON nodes(name)", &empty())
            .expect("declare the graph anchor index");
        database
            .execute(
                "CREATE TABLE edges (\
                 id UUID PRIMARY KEY, \
                 source_id UUID, \
                 target_id UUID, \
                 edge_type TEXT)",
                &empty(),
            )
            .expect("declare the graph edge table");
        for (id, name) in [(source, "seed"), (target, "target")] {
            database
                .execute(
                    "INSERT INTO nodes (id, scope_id, name, embedding) \
                     VALUES ($id, $scope, $name, $embedding)",
                    &params([
                        ("id", Value::Uuid(id)),
                        ("scope", Value::Uuid(scope)),
                        ("name", Value::Text(name.to_string())),
                        ("embedding", Value::Vector(stored_vector())),
                    ]),
                )
                .expect("insert one graph node");
        }
        database
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
                 VALUES ($id, $source, $target, 'LINKS')",
                &params([
                    ("id", Value::Uuid(Uuid::from_u128(0xE953))),
                    ("source", Value::Uuid(source)),
                    ("target", Value::Uuid(target)),
                ]),
            )
            .expect("insert the graph edge");
        database
            .close()
            .expect("close before graph anchor resolution");
    }

    let database = Database::open(&path).expect("reopen the graph-anchor fixture");
    let before = database.__vector_passive_activity_counters_for_test();
    let result = database
        .execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             WHERE a.name = 'seed' COLUMNS (b.id AS t))",
            &empty(),
        )
        .expect("resolve the graph anchor through its relational index");
    assert_eq!(result.rows, vec![vec![Value::Uuid(target)]]);
    let after = database.__vector_passive_activity_counters_for_test();
    assert_eq!(
        after.raw_partition_loads, before.raw_partition_loads,
        "graph anchor resolution reads id and name only, leaving its vector partition dormant"
    );
    database.close().expect("close the graph-anchor fixture");
}

#[cfg(feature = "test-seams")]
fn missing_vector_membership_work(unrelated_rows: u128) -> u64 {
    let directory = TempDir::new().expect("task-scoped missing-vector directory");
    let path = directory.path().join("missing-vector.db");
    let missing_id = Uuid::from_u128(0xEA00);
    {
        let database = Database::open(&path).expect("create the missing-vector fixture");
        database
            .execute(
                &format!(
                    "CREATE TABLE missing_vectors (\
                     id UUID PRIMARY KEY, \
                     group_name TEXT NOT NULL, \
                     embedding VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'))"
                ),
                &empty(),
            )
            .expect("declare the missing-vector fixture");
        database
            .execute(
                "CREATE INDEX missing_vectors_group_idx ON missing_vectors(group_name)",
                &empty(),
            )
            .expect("declare the missing-vector index");
        database
            .execute(
                "INSERT INTO missing_vectors (id, group_name) VALUES ($id, 'missing')",
                &params([("id", Value::Uuid(missing_id))]),
            )
            .expect("insert the row whose vector is absent");
        for ordinal in 0..unrelated_rows {
            database
                .execute(
                    "INSERT INTO missing_vectors (id, group_name, embedding) \
                     VALUES ($id, 'unrelated', $embedding)",
                    &params([
                        ("id", Value::Uuid(Uuid::from_u128(0xEB00 + ordinal))),
                        ("embedding", Value::Vector(stored_vector())),
                    ]),
                )
                .expect("insert one unrelated vector membership");
        }
        database.close().expect("close the missing-vector fixture");
    }
    let database = Database::open(&path).expect("reopen the missing-vector fixture");
    database.__reset_vector_membership_records_decoded_for_test();
    let result = database
        .execute(
            "SELECT embedding FROM missing_vectors WHERE group_name = 'missing'",
            &empty(),
        )
        .expect("the indexed read preserves an actually missing vector as NULL");
    assert_eq!(result.rows, vec![vec![Value::Null]]);
    assert_eq!(result.trace.physical_plan, "IndexScan");
    let decoded = database.__vector_membership_records_decoded_for_test();
    database.close().expect("close the missing-vector fixture");
    decoded
}

#[cfg(feature = "test-seams")]
#[test]
#[serial]
fn missing_vector_lookup_does_not_decode_unrelated_memberships() {
    let without_growth = missing_vector_membership_work(0);
    let with_growth = missing_vector_membership_work(96);
    assert_eq!(without_growth, 0);
    assert_eq!(
        with_growth, without_growth,
        "a missing vector point lookup stays flat when unrelated memberships grow"
    );
}

#[test]
fn repeated_indexed_updates_see_staged_quantized_vectors_and_rollback_together() {
    let database = Database::open_memory();
    let id = Uuid::from_u128(0xEC00);
    let replacement = vec![-0.93, -0.71, -0.49, -0.27, -0.05, 0.17, 0.39, 0.61];
    database
        .execute(
            &format!(
                "CREATE TABLE transactional_vectors (\
                 id UUID PRIMARY KEY, \
                 group_name TEXT NOT NULL, \
                 body TEXT NOT NULL, \
                 embedding VECTOR({DIMENSIONS}) WITH (quantization = 'SQ8'))"
            ),
            &empty(),
        )
        .expect("declare the transactional vector fixture");
    database
        .execute(
            "CREATE INDEX transactional_vectors_group_idx ON transactional_vectors(group_name)",
            &empty(),
        )
        .expect("declare the transaction-driving index");
    database
        .execute(
            "INSERT INTO transactional_vectors (id, group_name, body, embedding) \
             VALUES ($id, 'one', 'original', $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(stored_vector())),
            ]),
        )
        .expect("insert the transactional row");

    let rollback = database.begin().expect("begin the rollback transaction");
    database
        .execute_in_tx(
            rollback,
            "UPDATE transactional_vectors SET embedding = $embedding WHERE group_name = 'one'",
            &params([("embedding", Value::Vector(replacement.clone()))]),
        )
        .expect("stage the quantized replacement through the relational index");
    database
        .execute_in_tx(
            rollback,
            "UPDATE transactional_vectors SET body = 'staged' WHERE group_name = 'one'",
            &empty(),
        )
        .expect("a second indexed replacement retains the staged vector");
    let staged = database
        .execute_in_tx(
            rollback,
            "SELECT embedding, body FROM transactional_vectors WHERE group_name = 'one'",
            &empty(),
        )
        .expect("the indexed read sees the transaction overlay");
    assert_eq!(
        staged.rows,
        vec![vec![
            expected_stored_vector(&replacement),
            Value::Text("staged".to_string()),
        ]]
    );
    database
        .rollback(rollback)
        .expect("roll back both replacements");
    let committed = database
        .execute(
            "SELECT embedding, body FROM transactional_vectors WHERE group_name = 'one'",
            &empty(),
        )
        .expect("read the committed row after rollback");
    assert_eq!(
        committed.rows,
        vec![vec![expected_vector(), Value::Text("original".to_string())]],
        "rollback removes both staged replacements and restores the committed vector"
    );
}

#[cfg(feature = "test-seams")]
#[test]
#[serial]
fn schema_rewrite_builds_index_postings_from_rows_it_already_holds() {
    const ROWS: u64 = 6;
    let directory = TempDir::new().expect("task-scoped index-rebuild directory");
    let path = directory.path().join("index-rebuild.db");
    let database = Database::open(&path).expect("create the index-rebuild fixture");
    database
        .execute(
            "CREATE TABLE rebuild_rows (\
             id INTEGER PRIMARY KEY, \
             group_name TEXT NOT NULL, \
             body TEXT NOT NULL)",
            &empty(),
        )
        .expect("declare the index-rebuild fixture");
    database
        .execute(
            "CREATE INDEX rebuild_rows_group_idx ON rebuild_rows(group_name)",
            &empty(),
        )
        .expect("declare the rebuilt index");
    for ordinal in 0..ROWS {
        database
            .execute(
                "INSERT INTO rebuild_rows (id, group_name, body) VALUES ($id, 'one', $body)",
                &params([
                    ("id", Value::Int64(ordinal as i64)),
                    (
                        "body",
                        Value::Text(format!("row-{ordinal}:{}", "body-".repeat(1024))),
                    ),
                ]),
            )
            .expect("insert one rebuild row");
    }

    database.__reset_relational_index_rebuild_rows_decoded_for_test();
    database
        .__rebuild_relational_indexes_from_durable_rows_for_test("rebuild_rows")
        .expect("exercise the fallback rebuild over the durable relation");
    assert_eq!(
        database.__relational_index_rebuild_rows_decoded_for_test(),
        ROWS * 2,
        "the positive control must observe one full durable-row walk per declared index"
    );

    database.__reset_relational_index_rebuild_rows_decoded_for_test();
    database
        .execute("ALTER TABLE rebuild_rows ADD COLUMN note TEXT", &empty())
        .expect("rewrite the table and its relational postings atomically");
    assert_eq!(
        database.__relational_index_rebuild_rows_decoded_for_test(),
        0,
        "posting rebuild consumes the schema projection's supplied rows instead of decoding another full table"
    );

    let indexed = database
        .execute(
            "SELECT id FROM rebuild_rows WHERE group_name = 'one'",
            &empty(),
        )
        .expect("the rebuilt posting table answers after publication");
    assert_eq!(indexed.rows.len(), ROWS as usize);
    assert_eq!(indexed.trace.physical_plan, "IndexScan");
    database.close().expect("close the index-rebuild fixture");
}
