use super::execute_plan;
use crate::database::Database;
use contextdb_core::{ContextId, Error, Value};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

fn params(membership: u128) -> HashMap<String, Value> {
    HashMap::from([("m".into(), Value::Uuid(Uuid::from_u128(membership)))])
}

fn fixture() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE members (id INTEGER PRIMARY KEY, membership_id UUID NOT NULL UNIQUE, context_id UUID CONTEXT_ID NOT NULL, ordinal INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE residual_values (value INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    for value in [1_i64, 2] {
        db.execute(
            "INSERT INTO residual_values VALUES ($value)",
            &HashMap::from([("value".into(), Value::Int64(value))]),
        )
        .unwrap();
    }
    for (id, membership, context, ordinal) in [
        (1_i64, 0x2000_u128, 0xA_u128, 1_i64),
        (2_i64, 0x3000_u128, 0xB_u128, 2_i64),
    ] {
        db.execute(
            "INSERT INTO members (id, membership_id, context_id, ordinal) VALUES ($id, $m, $c, $o)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("m".into(), Value::Uuid(Uuid::from_u128(membership))),
                ("c".into(), Value::Uuid(Uuid::from_u128(context))),
                ("o".into(), Value::Int64(ordinal)),
            ]),
        )
        .unwrap();
    }
    db
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn eager(db: &Database, sql: &str, params: &HashMap<String, Value>) -> Result<usize, Error> {
    let statement = contextdb_parser::parse(sql)?;
    let plan = contextdb_planner::plan(&statement)?;
    let read_memory = db.declared_reader_memory()?;
    let mut vector_copy = crate::vector_read_memory::admission(read_memory.clone());
    execute_plan(
        db,
        &plan,
        params,
        None,
        &mut vector_copy,
        read_memory.clone(),
    )
    .and_then(|result| {
        result
            .into_public_with_owner(read_memory)
            .map(|result| result.rows().len())
    })
}

#[test]
fn eager_missing_unique_storage_preserves_hide_refuse_and_residual_contracts() {
    const UNIQUE: &str = "__unique_membership_id";
    let base = fixture();
    let db = base.scoped_with_contexts(BTreeSet::from([ContextId::new(Uuid::from_u128(0xA))]));
    assert!(db.__remove_physical_index_storage_for_test("members", UNIQUE));
    let sql = "SELECT id FROM members WHERE membership_id = $m";
    assert!(matches!(
        eager(&db, sql, &params(0x3000)),
        Err(Error::ContextScopeViolation { .. })
    ));
    assert_eq!(eager(&db, sql, &params(0x2000)).unwrap(), 1);
    assert_eq!(
        eager(
            &db,
            "SELECT id FROM members WHERE membership_id = $m AND ordinal = 99",
            &params(0x3000),
        )
        .unwrap(),
        0,
        "a complete residual mismatch is hidden before the refusal gate"
    );
}
