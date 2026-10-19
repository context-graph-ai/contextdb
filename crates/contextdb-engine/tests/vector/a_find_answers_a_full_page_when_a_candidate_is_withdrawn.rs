//! A find that can still see LIMIT eligible rows answers LIMIT rows.
//!
//! Two routes can withdraw a candidate after the search has scored it. On a
//! grant-lookup table the candidate source reads and judges every candidate
//! row itself, so the row that reaches publication has already passed — there
//! is nothing left to withdraw. Inside the reader's own open transaction the
//! committed index has not seen a staged change yet, so a row the transaction
//! retired is scored and then withdrawn where it is published.
//!
//! Both must answer a full page while a full page of eligible rows exists.

use contextdb_core::Value;
use contextdb_core::VectorIndexRef;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::types::Principal;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const ROWS: usize = 400;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

fn context_id() -> Uuid {
    Uuid::from_u128(0xC04A_0000_0000_0000_0000_0000_0000_0000)
}

fn generation_id() -> Uuid {
    Uuid::from_u128(0xC04A_1000_0000_0000_0000_0000_0000_0000)
}

fn acl_id() -> Uuid {
    Uuid::from_u128(0xC04A_2000_0000_0000_0000_0000_0000_0000)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn query_vector() -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (axis.wrapping_mul(0x27d4_eb2f)) % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn answer(db: &Database, sql: &str, params: &HashMap<String, Value>) -> Vec<i64> {
    let request = bounded::BoundedReadRequest::new(
        sql.to_owned(),
        params.clone(),
        limits(),
        Arc::new(FrozenClock),
    );
    bounded::execute(db, &request)
        .expect("the bounded read succeeds")
        .result
        .rows
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("the projection returns one integer id, got {other:?}"),
        })
        .collect()
}

fn maintain(db: &Database, index: &VectorIndexRef) {
    for _ in 0..512 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = db
            .vector_store_for_test()
            .partition_infos(index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if !ready.is_empty() && ready.iter().all(|partition| partition.graph_available) {
            return;
        }
    }
    panic!("maintenance did not publish a graph before the read is measured");
}

/// A grant-lookup table keeps the row route: the candidate source reads and
/// judges every candidate row, so publication has nothing left to withdraw and
/// the answer cannot come back short for that reason.
#[test]
fn a_grant_gated_find_answers_a_full_page() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let db = Database::open(directory.path().join("granted.db")).expect("open the fixture store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
         principal_id TEXT, acl_id UUID)",
        &HashMap::new(),
    )
    .expect("declare the ACL grant table");
    db.execute(
        "CREATE TABLE granted_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         generation_id UUID NOT NULL, acl_id UUID NOT NULL ACL REFERENCES acl_grants(acl_id), \
         is_current BOOL NOT NULL, embedding VECTOR(16) \
         PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the grant-gated vector table");
    db.execute(
        "CREATE INDEX granted_rows_active_idx ON granted_rows \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the index the candidate identities would come from");
    db.execute(
        "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
         VALUES ($id, 'Agent', $principal, $acl)",
        &HashMap::from([
            ("id".into(), Value::Uuid(Uuid::from_u128(0x705))),
            ("principal".into(), Value::Text("reader".to_owned())),
            ("acl".into(), Value::Uuid(acl_id())),
        ]),
    )
    .expect("grant the reader one ACL identity");
    for ordinal in 0..ROWS {
        db.execute(
            "INSERT INTO granted_rows VALUES ($id, $context, $generation, $acl, TRUE, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64 + 1)),
                ("context".into(), Value::Uuid(context_id())),
                ("generation".into(), Value::Uuid(generation_id())),
                ("acl".into(), Value::Uuid(acl_id())),
                ("embedding".into(), Value::Vector(embedding(ordinal))),
            ]),
        )
        .expect("seed one grant-gated row");
    }
    maintain(&db, &VectorIndexRef::new("granted_rows", "embedding"));

    let reader = db.scoped_with_constraints(
        Some(
            [contextdb_core::types::ContextId::new(context_id())]
                .into_iter()
                .collect(),
        ),
        None,
        Some(Principal::Agent("reader".to_owned())),
    );
    let params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ]);
    let rows = answer(
        &reader,
        &format!(
            "SELECT id FROM granted_rows WHERE context_id IN ($context) AND \
             generation_id IN ($generation) AND is_current = $is_current \
             ORDER BY embedding <=> $query LIMIT {LIMIT}"
        ),
        &params,
    );
    assert_eq!(
        rows.len(),
        LIMIT,
        "a grant-gated find over {ROWS} eligible rows answers a full page, got {rows:?}"
    );
}

/// A row this reader's own transaction retired is scored from the committed
/// index and then withdrawn where it is published. The next candidate has to
/// take its place, or the answer is short by exactly the rows withdrawn.
#[test]
fn a_find_inside_a_transaction_that_retired_a_row_answers_a_full_page() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let db = Database::open(directory.path().join("withdrawn.db")).expect("open the fixture store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE staged_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, embedding VECTOR(16) \
         PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    db.execute(
        "CREATE INDEX staged_rows_active_idx ON staged_rows \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the index the candidate identities come from");
    for ordinal in 0..ROWS {
        db.execute(
            "INSERT INTO staged_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64 + 1)),
                ("context".into(), Value::Uuid(context_id())),
                ("generation".into(), Value::Uuid(generation_id())),
                ("embedding".into(), Value::Vector(embedding(ordinal))),
            ]),
        )
        .expect("seed one row");
    }
    maintain(&db, &VectorIndexRef::new("staged_rows", "embedding"));

    let reader = db.scoped_with_contexts(
        [contextdb_core::types::ContextId::new(context_id())]
            .into_iter()
            .collect(),
    );
    let params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
        ("context".into(), Value::Uuid(context_id())),
        ("generation".into(), Value::Uuid(generation_id())),
    ]);
    let sql = format!(
        "SELECT id FROM staged_rows WHERE context_id IN ($context) AND \
         generation_id IN ($generation) AND is_current = $is_current \
         ORDER BY embedding <=> $query LIMIT {LIMIT}"
    );
    let before = answer(&reader, &sql, &params);
    assert_eq!(
        before.len(),
        LIMIT,
        "the untouched store answers a full page"
    );
    let retired = before[0];

    db.execute("BEGIN", &HashMap::new())
        .expect("open a transaction");
    db.execute(
        "UPDATE staged_rows SET is_current = FALSE WHERE id = $id",
        &HashMap::from([("id".into(), Value::Int64(retired))]),
    )
    .expect("stage a retirement without committing it");
    let during = answer(&db, &sql, &params);
    db.execute("COMMIT", &HashMap::new())
        .expect("commit the staged retirement");

    assert!(
        !during.contains(&retired),
        "the row this transaction retired is gone from its own answer: {during:?}"
    );
    assert_eq!(
        during.len(),
        LIMIT,
        "{} eligible rows remain, so the answer is still a full page: {during:?}",
        ROWS - 1,
    );
}
