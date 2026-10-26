use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits,
};
use contextdb_core::{CompositeForeignKey, IndexKind, TxId, Value};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use uuid::Uuid;

const PASSAGE_DDL: &str = "CREATE TABLE event_search_passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), generation_id UUID NOT NULL REFERENCES event_search_generations(id), connector_id TEXT NOT NULL, session_ref TEXT NOT NULL, source_instance_ref TEXT NOT NULL, passage_ordinal INTEGER NOT NULL, first_event_sequence INTEGER NOT NULL, last_event_sequence INTEGER NOT NULL, rendered_text TEXT NOT NULL, content_sha256 TEXT NOT NULL, vector_text VECTOR(768) PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4096 SEARCH_MODE AUTO, dimension INTEGER NOT NULL, is_current BOOLEAN NOT NULL, created_at TXID NOT NULL, UNIQUE (context_id, id), UNIQUE (context_id, generation_id, id), UNIQUE (context_id, generation_id, connector_id, source_instance_ref, passage_ordinal), FOREIGN KEY (context_id, generation_id) REFERENCES event_search_generations(context_id, id)) SYNC OFF";

const PASSAGE_INDEX_DDLS: [&str; 5] = [
    "CREATE INDEX event_search_passages_active_idx ON event_search_passages (context_id, generation_id, is_current)",
    "CREATE INDEX event_search_passages_generation_ordinal_idx ON event_search_passages (generation_id, passage_ordinal)",
    "CREATE INDEX event_search_passages_connector_idx ON event_search_passages (context_id, generation_id, is_current, connector_id)",
    "CREATE INDEX event_search_passages_session_idx ON event_search_passages (context_id, generation_id, is_current, session_ref)",
    "CREATE INDEX event_search_passages_source_idx ON event_search_passages (context_id, generation_id, is_current, source_instance_ref)",
];

const ACTIVE_INDEX: &str = "event_search_passages_active_idx";
const CONTEXTS: usize = 8;
const GENERATIONS_PER_CONTEXT: usize = 2;
const PASSAGES_PER_GENERATION: usize = 10;
const RUN_PRODUCT_SIDE: usize = 192;
const RUN_PRODUCT_ROWS: usize = RUN_PRODUCT_SIDE * RUN_PRODUCT_SIDE;
const RUN_PRODUCT_INDEX: &str = "run_product_left_right_idx";

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

fn context_id(ordinal: usize) -> Uuid {
    Uuid::from_u128(0xC044_0000_0000_0000_0000_0000_0000_0000 + ordinal as u128)
}

fn generation_id(context: usize, generation: usize) -> Uuid {
    Uuid::from_u128(
        0xC044_1000_0000_0000_0000_0000_0000_0000
            + (context * GENERATIONS_PER_CONTEXT + generation) as u128,
    )
}

fn passage_id(context: usize, generation: usize, ordinal: usize) -> Uuid {
    Uuid::from_u128(
        0xC044_2000_0000_0000_0000_0000_0000_0000
            + (context * GENERATIONS_PER_CONTEXT * PASSAGES_PER_GENERATION
                + generation * PASSAGES_PER_GENERATION
                + ordinal) as u128,
    )
}

fn bind_uuid_values(
    params: &mut HashMap<String, Value>,
    column: &str,
    prefix: &str,
    values: impl IntoIterator<Item = Uuid>,
) -> String {
    let values = values.into_iter().collect::<BTreeSet<_>>();
    assert!(!values.is_empty());
    let mut placeholders = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let name = format!("{prefix}_{index}");
        params.insert(name.clone(), Value::Uuid(value));
        placeholders.push(format!("${name}"));
    }
    if placeholders.len() == 1 {
        format!("{column} = {}", placeholders[0])
    } else {
        format!("{column} IN ({})", placeholders.join(", "))
    }
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

fn search_statement(
    contexts: impl IntoIterator<Item = Uuid>,
    generations: impl IntoIterator<Item = Uuid>,
) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([("is_current".into(), Value::Bool(true))]);
    let context_filter = bind_uuid_values(&mut params, "context_id", "search_context", contexts);
    let generation_filter = bind_uuid_values(
        &mut params,
        "generation_id",
        "search_generation",
        generations,
    );
    (
        format!(
            "SELECT id FROM event_search_passages WHERE {context_filter} AND \
             {generation_filter} AND is_current = $is_current"
        ),
        params,
    )
}

fn seed() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE contexts (id UUID PRIMARY KEY CONTEXT_ID) SYNC OFF",
        &empty(),
    )
    .expect("create the cg Context identity parent");
    db.execute(
        "CREATE TABLE event_search_generations (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID \
         NOT NULL REFERENCES contexts(id), UNIQUE (context_id, id)) SYNC OFF",
        &empty(),
    )
    .expect("create the cg generation identity parent");
    db.execute(PASSAGE_DDL, &empty())
        .expect("create the exact pinned cg passage declaration");
    for ddl in PASSAGE_INDEX_DDLS {
        db.execute(ddl, &empty())
            .expect("create a pinned cg passage index in declaration order");
    }

    for context in 0..CONTEXTS {
        db.execute(
            "INSERT INTO contexts (id) VALUES ($id)",
            &HashMap::from([("id".into(), Value::Uuid(context_id(context)))]),
        )
        .expect("insert a Context identity");
        for generation in 0..GENERATIONS_PER_CONTEXT {
            db.execute(
                "INSERT INTO event_search_generations (id, context_id) VALUES ($id, $context)",
                &HashMap::from([
                    ("id".into(), Value::Uuid(generation_id(context, generation))),
                    ("context".into(), Value::Uuid(context_id(context))),
                ]),
            )
            .expect("insert a globally unique generation belonging to its Context");
            for ordinal in 0..PASSAGES_PER_GENERATION {
                db.execute(
                    "INSERT INTO event_search_passages \
                     (id, context_id, generation_id, connector_id, session_ref, \
                      source_instance_ref, passage_ordinal, first_event_sequence, \
                      last_event_sequence, rendered_text, content_sha256, dimension, \
                      is_current, created_at) \
                     VALUES ($id, $context, $generation, $connector, $session, $source, \
                             $ordinal, $first_sequence, $last_sequence, $text, $sha, 768, \
                             $current, $created_at)",
                    &HashMap::from([
                        (
                            "id".into(),
                            Value::Uuid(passage_id(context, generation, ordinal)),
                        ),
                        ("context".into(), Value::Uuid(context_id(context))),
                        (
                            "generation".into(),
                            Value::Uuid(generation_id(context, generation)),
                        ),
                        ("connector".into(), Value::Text("fixture".into())),
                        (
                            "session".into(),
                            Value::Text(format!("session-{context}-{generation}")),
                        ),
                        ("source".into(), Value::Text("source".into())),
                        ("ordinal".into(), Value::Int64(ordinal as i64)),
                        ("first_sequence".into(), Value::Int64((ordinal * 10) as i64)),
                        (
                            "last_sequence".into(),
                            Value::Int64((ordinal * 10 + 9) as i64),
                        ),
                        (
                            "text".into(),
                            Value::Text(format!("passage {context}/{generation}/{ordinal}")),
                        ),
                        (
                            "sha".into(),
                            Value::Text(format!("sha-{context}-{generation}-{ordinal}")),
                        ),
                        ("current".into(), Value::Bool(generation == 1)),
                        ("created_at".into(), Value::TxId(TxId(1))),
                    ]),
                )
                .expect("insert a constrained cg passage row");
            }
        }
    }
    db
}

fn assert_pinned_schema_competition(db: &Database) {
    let meta = db
        .table_meta("event_search_passages")
        .expect("the passage metadata exists");
    assert_eq!(
        meta.unique_constraints,
        vec![
            columns(&["context_id", "id"]),
            columns(&["context_id", "generation_id", "id"]),
            columns(&[
                "context_id",
                "generation_id",
                "connector_id",
                "source_instance_ref",
                "passage_ordinal",
            ]),
        ],
        "the fixture keeps every pinned passage uniqueness declaration"
    );
    assert_eq!(
        meta.composite_foreign_keys,
        vec![CompositeForeignKey {
            child_columns: columns(&["context_id", "generation_id"]),
            parent_table: "event_search_generations".into(),
            parent_columns: columns(&["context_id", "id"]),
        }],
        "the fixture keeps the cg Context/generation ownership foreign key"
    );

    let auto_columns = meta
        .indexes
        .iter()
        .filter(|index| index.kind == IndexKind::Auto)
        .map(|index| {
            index
                .columns
                .iter()
                .map(|(column, _)| column.clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        auto_columns,
        vec![
            columns(&["id"]),
            columns(&["context_id", "id"]),
            columns(&["context_id", "generation_id", "id"]),
            columns(&[
                "context_id",
                "generation_id",
                "connector_id",
                "source_instance_ref",
                "passage_ordinal",
            ]),
            columns(&["context_id"]),
            columns(&["generation_id"]),
            columns(&["context_id", "generation_id"]),
        ],
        "constraint-backed indexes compete in the exact pinned declaration order"
    );
    let user_names = meta
        .indexes
        .iter()
        .filter(|index| index.kind == IndexKind::UserDeclared)
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        user_names,
        vec![
            "event_search_passages_active_idx",
            "event_search_passages_generation_ordinal_idx",
            "event_search_passages_connector_idx",
            "event_search_passages_session_idx",
            "event_search_passages_source_idx",
        ],
        "the five explicit cg indexes retain their pinned order"
    );
}

fn selected_statement() -> (String, HashMap<String, Value>) {
    search_statement(
        (0..CONTEXTS).map(context_id),
        (0..CONTEXTS).map(|context| generation_id(context, 1)),
    )
}

fn run_product_id(side: &str, ordinal: usize) -> Uuid {
    let base = match side {
        "left" => 0xC044_3000_0000_0000_0000_0000_0000_0000,
        "right" => 0xC044_4000_0000_0000_0000_0000_0000_0000,
        other => panic!("unknown run-product side {other}"),
    };
    Uuid::from_u128(base + ordinal as u128)
}

fn seeded_run_product() -> Arc<Database> {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE run_product (id INTEGER PRIMARY KEY, left_id UUID NOT NULL, \
         right_id UUID NOT NULL)",
        &empty(),
    )
    .expect("create the tuple-run fixture table");
    db.execute(
        &format!("CREATE INDEX {RUN_PRODUCT_INDEX} ON run_product (left_id, right_id)"),
        &empty(),
    )
    .expect("create the tuple-run fixture index");

    let tx = db.begin_or_panic();
    for left in 0..RUN_PRODUCT_SIDE {
        for right in 0..RUN_PRODUCT_SIDE {
            let id = left * RUN_PRODUCT_SIDE + right;
            db.insert_row(
                tx,
                "run_product",
                HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("left_id".into(), Value::Uuid(run_product_id("left", left))),
                    (
                        "right_id".into(),
                        Value::Uuid(run_product_id("right", right)),
                    ),
                ]),
            )
            .expect("stage one real tuple-run row");
        }
    }
    db.commit(tx).expect("publish every real tuple-run row");
    Arc::new(db)
}

fn run_product_statement() -> (String, HashMap<String, Value>) {
    let mut params = HashMap::new();
    let left = bind_uuid_values(
        &mut params,
        "left_id",
        "run_left",
        (0..RUN_PRODUCT_SIDE).map(|ordinal| run_product_id("left", ordinal)),
    );
    let right = bind_uuid_values(
        &mut params,
        "right_id",
        "run_right",
        (0..RUN_PRODUCT_SIDE).map(|ordinal| run_product_id("right", ordinal)),
    );
    (
        format!("SELECT id FROM run_product WHERE {left} AND {right}"),
        params,
    )
}

fn run_product_limits(memory: u64) -> ReadLimits {
    ReadLimits {
        result_rows: (RUN_PRODUCT_ROWS + 1) as u64,
        cursor_page_rows: 1,
        ..limits(memory)
    }
}

fn expected_ids() -> BTreeSet<Uuid> {
    (0..CONTEXTS)
        .flat_map(|context| {
            (0..PASSAGES_PER_GENERATION).map(move |ordinal| passage_id(context, 1, ordinal))
        })
        .collect()
}

fn returned_ids(rows: &[Vec<Value>]) -> BTreeSet<Uuid> {
    rows.iter()
        .map(|row| match row.as_slice() {
            [Value::Uuid(id)] => *id,
            other => panic!("expected one UUID passage id, got {other:?}"),
        })
        .collect()
}

fn assert_active_plan(result: &contextdb_engine::QueryResult) {
    assert_eq!(returned_ids(&result.rows()), expected_ids());
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some(ACTIVE_INDEX),
        "the real cg predicate must choose the exact three-column active index: {:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().predicates_pushed.len(),
        3,
        "both lists and the current-state equality are pushed: {:?}",
        result.trace()
    );
    assert_eq!(
        result.trace().rows_examined,
        (CONTEXTS * PASSAGES_PER_GENERATION) as u64,
        "the index reads only current rows of the selected generation pairs"
    );
    for longer in [
        "event_search_passages_connector_idx",
        "event_search_passages_session_idx",
        "event_search_passages_source_idx",
    ] {
        let candidate = result
            .trace()
            .indexes_considered
            .iter()
            .find(|candidate| candidate.name == longer)
            .unwrap_or_else(|| {
                panic!("trace omitted competing index {longer}: {:?}", result.trace())
            });
        assert!(
            candidate.rejected_reason.contains("whole-key coverage"),
            "the exposed tie receipt must name whole-key coverage, got {candidate:?}"
        );
    }
}

fn limits(memory: u64) -> ReadLimits {
    ReadLimits {
        result_rows: 4_096,
        result_bytes: 64 * 1024 * 1024,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory,
        cursor_page_rows: 500,
        cursor_page_bytes: 16 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

#[test]
fn in_list_scope_uses_the_declared_composite_index() {
    let db = seed();
    assert_pinned_schema_competition(&db);
    let (sql, params) = selected_statement();
    assert_eq!(
        sql,
        "SELECT id FROM event_search_passages WHERE context_id IN ($search_context_0, \
         $search_context_1, $search_context_2, $search_context_3, $search_context_4, \
         $search_context_5, $search_context_6, $search_context_7) AND generation_id IN \
         ($search_generation_0, $search_generation_1, $search_generation_2, \
         $search_generation_3, $search_generation_4, $search_generation_5, \
         $search_generation_6, $search_generation_7) AND is_current = $is_current",
        "the proof executes cg's two-IN-list relational predicate verbatim"
    );

    let ordinary = db
        .execute(&sql, &params)
        .expect("ordinary execution answers the cg predicate");
    assert_active_plan(&ordinary);

    let bounded = bounded::execute(
        &db,
        &bounded::BoundedReadRequest::new(
            sql,
            params,
            limits(64 * 1024 * 1024),
            Arc::new(FrozenClock),
        ),
    )
    .expect("the bounded production kernel answers the same cg predicate");
    assert_active_plan(&bounded.result);
    assert_eq!(bounded.result.rows(), ordinary.rows());
    println!(
        "wall_search_sql ordinary={{rows:{},index:{:?},pushed:{},examined:{},trace:{:?}}} bounded={{rows:{},index:{:?},pushed:{},examined:{},trace:{:?}}}",
        ordinary.rows().len(),
        ordinary.trace().index_used.as_deref(),
        ordinary.trace().predicates_pushed.len(),
        ordinary.trace().rows_examined,
        ordinary.trace(),
        bounded.result.rows().len(),
        bounded.result.trace().index_used.as_deref(),
        bounded.result.trace().predicates_pushed.len(),
        bounded.result.trace().rows_examined,
        bounded.result.trace(),
    );
}

#[test]
fn oversized_two_list_product_is_refused_by_the_declared_memory_budget() {
    let db = seeded_run_product();
    let (sql, params) = run_product_statement();
    let (control_sql, control_params) = selected_statement();
    let ceiling = 2 * 1024 * 1024;
    bounded::execute(
        &seed(),
        &bounded::BoundedReadRequest::new(
            control_sql,
            control_params,
            limits(ceiling),
            Arc::new(FrozenClock),
        ),
    )
    .expect("the real eight-by-eight product fits under the comparison ceiling");

    let roomy_request = bounded::BoundedReadRequest::new(
        sql.clone(),
        params.clone(),
        run_product_limits(512 * 1024 * 1024),
        Arc::new(FrozenClock),
    );
    let mut opened = bounded::open_cursor(Arc::clone(&db), &roomy_request)
        .expect("the real tuple-run product opens under a roomy ceiling");
    assert_eq!(
        opened.first_page.rows().len(),
        1,
        "the open cursor reaches a stored tuple and remains suspended with the other runs owed"
    );
    let pending = opened
        .cursor
        .continuation_bytes()
        .expect("inspect the production continuation")
        .part(bounded::PENDING_INDEX_RUNS);
    assert!(
        pending > ceiling as usize,
        "{RUN_PRODUCT_ROWS} stored tuple runs retain {pending} bytes, which must cross the unchanged {ceiling}-byte admission ceiling"
    );
    opened.cursor.close().expect("close the roomy proof cursor");

    let refused = bounded::open_cursor(
        Arc::clone(&db),
        &bounded::BoundedReadRequest::new(
            sql,
            params,
            run_product_limits(ceiling),
            Arc::new(FrozenClock),
        ),
    )
    .err()
    .expect("the real tuple-run product must be admitted before allocation");
    let bounded::TestError::Refused(failure) = refused else {
        panic!("a crossed read ceiling must be a typed refusal, got {refused:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail().clone() else {
        panic!("the refusal must retain its typed ceiling detail: {failure:?}");
    };
    assert_eq!(detail.limit, ReadFailureLimit::Memory);
    assert_eq!(detail.value, ceiling);
    println!(
        "wall_search_sql_oversized runs={RUN_PRODUCT_ROWS} pending={pending} refusal={{kind:{:?},limit:{:?},value:{}}}",
        failure.kind(),
        detail.limit,
        detail.value,
    );
}
