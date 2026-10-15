//! Writing one row to a partitioned vector column is admitted for what the
//! batch that prepares it holds: the batch does not keep, or reserve for, a
//! copy of the row's partition key per bookkeeping map.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn fixture(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the partition-key fixture");
    db.execute(
        "CREATE TABLE keyed (id INT PRIMARY KEY, scope TEXT NOT NULL, \
         v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 4 SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .expect("declare the partitioned table");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn insert(db: &Database, id: i64, key: &str) -> contextdb_core::Result<()> {
    db.execute(
        "INSERT INTO keyed VALUES ($id, $scope, $v)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("scope".into(), Value::Text(key.to_owned())),
            ("v".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .map(|_| ())
}

#[test]
fn a_row_with_a_one_mebibyte_partition_key_is_admitted_under_the_default_reader() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let db = fixture(&root.path().join("wide-key.db"));
    insert(&db, 1, &"k".repeat(1024 * 1024))
        .expect("one row whose partition key is 1 MiB fits the default 16 MiB reader");
}
