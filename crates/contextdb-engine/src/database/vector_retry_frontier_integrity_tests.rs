//! A refused covered-journal retry defers only its own build. A later search
//! must still prove every partition's frontier before publishing an answer.

use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::atomic::AtomicUsize;

const CATALOGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_generation_catalog");
const JOURNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_partition_journal");
const REFUSING_EXTENT_BYTES: usize = 9 * 1024 * 1024;
const INITIAL_ROWS: i64 = 256;
const SIBLING_START: i64 = 1000;
const VISIBLE_ROWS: usize = (INITIAL_ROWS * 2 + 3) as usize;
const FRESH_MUTATION_COUNT: usize = 4 + 1;

fn route(scope: i64) -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("retry_frontier_items", "v"),
        VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
    )
}

fn insert(db: &Database, id: i64, scope: i64) {
    db.execute(
        "INSERT INTO retry_frontier_items VALUES ($id, $scope, $v)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("scope".into(), Value::Int64(scope)),
            ("v".into(), Value::Vector(vec![1.0, id as f32 + 1.0, 0.0])),
        ]),
    )
    .unwrap();
}
