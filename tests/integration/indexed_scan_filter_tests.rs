//! Indexed Scan Filter — planner, executor, MVCC, DDL, ordering, persistence, sync, trace tests.

use contextdb_core::table_meta::{ColumnType, IndexDecl, SortDirection, TableMeta};
use contextdb_core::{Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::cli_render::render_table_meta;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn seed_t_with_index(db: &Database) {
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER, tag TEXT)",
        &empty(),
    )
    .expect("CREATE TABLE");
    db.execute("CREATE INDEX idx_a ON t (a)", &empty())
        .expect("CREATE INDEX idx_a");
}

fn insert_row_t(db: &Database, a: i64, b: i64, tag: &str) -> Uuid {
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, a, b, tag) VALUES ($id, $a, $b, $tag)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("a", Value::Int64(a)),
            ("b", Value::Int64(b)),
            ("tag", Value::Text(tag.into())),
        ]),
    )
    .expect("INSERT t");
    id
}

// ============================================================================
// EP01 — RED — equality on indexed column picks IndexScan. [I1, I15]
// ============================================================================
#[test]
fn ep01_equality_on_indexed_column_picks_index_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    // Seed 50 rows so a Scan impl examines >> the expected result cardinality.
    for i in 0..50i64 {
        insert_row_t(&db, i, 100 + i, &format!("r{i}"));
    }
    insert_row_t(&db, 7, 100, "x"); // row that matches; duplicates a=7 row above at i=7 distinct row_id
    let r = db
        .execute(
            "SELECT a, b, tag FROM t WHERE a = 7 ORDER BY tag ASC",
            &empty(),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    // Trace-liar gate: a full-scan impl that writes "IndexScan" into trace
    // examines every row (>= 51). A real IndexScan examines at most the
    // matching postings (2 rows sharing a=7).
    assert!(
        db.__rows_examined() <= 4,
        "IndexScan must not examine more than a small multiple of result cardinality; got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(7), Value::Int64(107), Value::Text("r7".into())],
            vec![Value::Int64(7), Value::Int64(100), Value::Text("x".into())],
        ]
    );
}

// ============================================================================
// EP02 — RED — BETWEEN on indexed column picks IndexScan; predicates_pushed
// contains "a". [I1, I15]
// ============================================================================
#[test]
fn ep02_between_on_indexed_column_picks_index_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    // 50 distinct a values; only 2 fall in [3, 9].
    insert_row_t(&db, 2, 0, "lo");
    insert_row_t(&db, 5, 0, "mid");
    insert_row_t(&db, 9, 0, "hi");
    insert_row_t(&db, 10, 0, "over");
    for i in 20..66i64 {
        insert_row_t(&db, i, 0, &format!("f{i}"));
    }
    let r = db
        .execute(
            "SELECT a FROM t WHERE a BETWEEN 3 AND 9 ORDER BY a ASC",
            &empty(),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    let pushed: Vec<&str> = r
        .trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref())
        .collect();
    assert_eq!(pushed, vec!["a"]);
    // Trace-liar gate: table size = 50; real IndexScan touches ~2.
    assert!(db.__rows_examined() <= 4, "got {}", db.__rows_examined());
    assert_eq!(r.rows, vec![vec![Value::Int64(5)], vec![Value::Int64(9)]]);
}

// ============================================================================
// EP03 — RED — IN (const_list) on indexed column returns the exact rows. [I2]
// ============================================================================
#[test]
fn ep03_in_const_list_on_indexed_column_returns_exact_rows() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 100, "one");
    insert_row_t(&db, 2, 200, "two");
    insert_row_t(&db, 3, 300, "three");
    insert_row_t(&db, 4, 400, "four");
    let r = db
        .execute(
            "SELECT a, b, tag FROM t WHERE a IN (1, 2, 3) ORDER BY a ASC",
            &empty(),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(
        r.rows,
        vec![
            vec![
                Value::Int64(1),
                Value::Int64(100),
                Value::Text("one".into())
            ],
            vec![
                Value::Int64(2),
                Value::Int64(200),
                Value::Text("two".into())
            ],
            vec![
                Value::Int64(3),
                Value::Int64(300),
                Value::Text("three".into())
            ],
        ]
    );
}

// ============================================================================
// EP04 — RED — `!=` on indexed column still picks IndexScan; residual applied.
// [I1, I2]
// ============================================================================
#[test]
fn ep04_not_equal_on_indexed_column_picks_index_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 0, "a");
    insert_row_t(&db, 5, 0, "b");
    insert_row_t(&db, 9, 0, "c");
    let r = db
        .execute("SELECT a FROM t WHERE a != 5 ORDER BY a ASC", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, vec![vec![Value::Int64(1)], vec![Value::Int64(9)]]);
}

// ============================================================================
// EP05 — RED — function call disqualifies IndexScan; falls back to Scan.
// `indexes_considered` names candidate with a structured reason. [I1, I15]
// ============================================================================
#[test]
fn ep05_function_call_in_predicate_disqualifies_index() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty())
        .unwrap();
    for i in 0..20i64 {
        let name = if i == 0 {
            "alpha".to_string()
        } else {
            format!("beta{i}")
        };
        db.execute(
            "INSERT INTO t (id, name) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("n", Value::Text(name)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT name FROM t WHERE UPPER(name) = 'ALPHA'", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "Scan");
    assert!(r.trace.index_used.is_none());
    let considered: Vec<&str> = r
        .trace
        .indexes_considered
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(considered, vec!["idx_name"]);
    assert_eq!(
        r.trace.indexes_considered[0].rejected_reason.as_ref(),
        "function call in predicate"
    );
    // Positive gate: a Scan must examine the whole table — confirms the
    // rows_examined counter is live for the paired IndexScan tests.
    assert!(
        db.__rows_examined() >= 20,
        "Scan must examine all rows; got {}",
        db.__rows_examined()
    );
}

// ============================================================================
// EP06 — RED — arithmetic in predicate disqualifies. [I1]
// ============================================================================
#[test]
fn ep06_arithmetic_in_predicate_disqualifies_index() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 4, 0, "x");
    let r = db
        .execute("SELECT a FROM t WHERE a + 1 = 5", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "Scan");
    assert!(r.trace.index_used.is_none());
    assert_eq!(
        r.trace.indexes_considered[0].rejected_reason.as_ref(),
        "arithmetic in predicate"
    );
}

// ============================================================================
// EP07 — RED — column-to-column ref RHS disqualifies. [I1]
// ============================================================================
#[test]
fn ep07_column_ref_rhs_disqualifies_index() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 3, 3, "eq");
    insert_row_t(&db, 1, 2, "neq");
    let r = db
        .execute("SELECT a, b FROM t WHERE a = b", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "Scan");
    assert_eq!(
        r.trace.indexes_considered[0].rejected_reason.as_ref(),
        "non-literal rhs"
    );
    assert_eq!(r.rows, vec![vec![Value::Int64(3), Value::Int64(3)]]);
}

// ============================================================================
// EP08 — RED — subquery RHS disqualifies the OUTER IndexScan. [I1]
// ============================================================================
#[test]
fn ep08_subquery_rhs_disqualifies_outer_index() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    db.execute("CREATE TABLE sub (x INTEGER)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO sub (x) VALUES ($v)",
        &params(vec![("v", Value::Int64(5))]),
    )
    .unwrap();
    insert_row_t(&db, 5, 0, "match");
    let r = db
        .execute("SELECT a FROM t WHERE a IN (SELECT x FROM sub)", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "Scan");
    assert!(r.trace.index_used.is_none());
    assert_eq!(r.rows, vec![vec![Value::Int64(5)]]);
}

// ============================================================================
// EP09 — RED — parameterized equality with non-NULL bind picks IndexScan.
// ============================================================================
#[test]
fn ep09_parameterized_equality_picks_index_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 42, 0, "answer");
    let r = db
        .execute(
            "SELECT a FROM t WHERE a = $p",
            &params(vec![("p", Value::Int64(42))]),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, vec![vec![Value::Int64(42)]]);
}

// ============================================================================
// EP10 — RED — parameterized equality with NULL bind returns zero rows.
// Pair with EP09. I15.
// ============================================================================
#[test]
fn ep10_parameterized_equality_with_null_bind_returns_no_rows() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 42, 0, "answer");
    // Seed additional rows so Scan would examine many; IndexScan must still
    // short-circuit the NULL bind to zero.
    for i in 0..20i64 {
        insert_row_t(&db, 100 + i, 0, &format!("n{i}"));
    }
    let r = db
        .execute(
            "SELECT a FROM t WHERE a = $p",
            &params(vec![("p", Value::Null)]),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
    // Trace-liar gate: NULL-bind short-circuit examines zero postings.
    assert!(db.__rows_examined() <= 1, "got {}", db.__rows_examined());
}

// ============================================================================
// EP11 — RED — IS NULL on indexed column uses IndexScan; returns exactly the
// NULL partition rows.
// ============================================================================
#[test]
fn ep11_is_null_on_indexed_column_uses_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER NULL, tag TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    // Insert one non-NULL and two NULLs.
    db.execute(
        "INSERT INTO t (id, a, tag) VALUES ($id, $a, $t)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
            ("t", Value::Text("nonnull".into())),
        ]),
    )
    .unwrap();
    for tag in ["null1", "null2"] {
        db.execute(
            "INSERT INTO t (id, a, tag) VALUES ($id, $a, $t)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Null),
                ("t", Value::Text(tag.into())),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT tag FROM t WHERE a IS NULL ORDER BY tag ASC",
            &empty(),
        )
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Text("null1".into())],
            vec![Value::Text("null2".into())],
        ]
    );
}

// ============================================================================
// EP12 — RED — IS NOT NULL returns non-NULL partition. Pair with EP11.
// ============================================================================
#[test]
fn ep12_is_not_null_on_indexed_column_uses_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER NULL, tag TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    db.execute(
        "INSERT INTO t (id, a, tag) VALUES ($id, $a, $t)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
            ("t", Value::Text("nonnull".into())),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, a, tag) VALUES ($id, $a, $t)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Null),
            ("t", Value::Text("null1".into())),
        ]),
    )
    .unwrap();
    let r = db
        .execute("SELECT tag FROM t WHERE a IS NOT NULL", &empty())
        .expect("SELECT");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, vec![vec![Value::Text("nonnull".into())]]);
}

// ============================================================================
// EP13 — RED — tie-break between two equality-capable indexes on same column
// picks creation-order-earlier.
// ============================================================================
#[test]
fn ep13_tie_break_by_creation_order() {
    let db = Database::open_memory();
    seed_t_with_index(&db); // creates idx_a first
    db.execute("CREATE INDEX idx_a2 ON t (a)", &empty())
        .unwrap();
    insert_row_t(&db, 1, 100, "x");
    for i in 2..22i64 {
        insert_row_t(&db, i, 0, &format!("r{i}"));
    }
    let r = db
        .execute("SELECT a, b, tag FROM t WHERE a = 1", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    assert!(db.__rows_examined() <= 4, "got {}", db.__rows_examined());
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int64(1),
            Value::Int64(100),
            Value::Text("x".into())
        ]]
    );
}

// ============================================================================
// EP14 — RED — picker prefers equality > range when both available.
// ============================================================================
#[test]
fn ep14_picker_prefers_equality_over_range() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    // Range-capable only (composite starting with b).
    db.execute("CREATE INDEX idx_ba ON t (b, a)", &empty())
        .unwrap();
    // Equality-capable (simple index on a), created LATER.
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    db.execute(
        "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(5)),
            ("b", Value::Int64(100)),
        ]),
    )
    .unwrap();
    for i in 0..20i64 {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(100 + i)),
                ("b", Value::Int64(i + 1)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT a, b FROM t WHERE a = 5 AND b > 0", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
    assert!(db.__rows_examined() <= 4, "got {}", db.__rows_examined());
    assert_eq!(r.rows, vec![vec![Value::Int64(5), Value::Int64(100)]]);
}

// ============================================================================
// EP15 — RED — composite index (a, b): col1=X walks range, col2=Y is residual.
// `predicates_pushed` lists only the pushed column (a), not b. [I2, I15]
// ============================================================================
#[test]
fn ep15_composite_index_pushes_only_leading_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a, b)", &empty())
        .unwrap();
    for (a, b) in &[(1i64, 10i64), (1, 20), (2, 10)] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(*a)),
                ("b", Value::Int64(*b)),
            ]),
        )
        .unwrap();
    }
    // Seed additional rows so table size > 10.
    for i in 10..30i64 {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(i)),
                ("b", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT a, b FROM t WHERE a = 1 AND b = 20", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    let pushed: Vec<&str> = r
        .trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref())
        .collect();
    assert_eq!(pushed, vec!["a"]);
    // Trace-liar gate: composite walks range a=1 (2 rows), residual b=20 filters.
    assert!(db.__rows_examined() <= 4, "got {}", db.__rows_examined());
    assert_eq!(r.rows, vec![vec![Value::Int64(1), Value::Int64(20)]]);
}

// ============================================================================
// EP16 — RED — LIKE residual only; planner emits Scan; considered
// rejected with reason.
// ============================================================================
#[test]
fn ep16_like_is_residual_only() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $n)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("n", Value::Text("pay-1".into())),
        ]),
    )
    .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name LIKE 'pay%'", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "Scan");
    assert_eq!(
        r.trace.indexes_considered[0].rejected_reason.as_ref(),
        "LIKE is residual-only"
    );
}

// ============================================================================
// EP17 — RED — IndexScan plus residual LIKE returns rows matching BOTH.
// [I2, I10]
// ============================================================================
#[test]
fn ep17_index_scan_with_residual_like_returns_intersection() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, tag TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    for (a, tag) in &[(3i64, "xavier"), (3, "yoda"), (4, "xavier")] {
        db.execute(
            "INSERT INTO t (id, a, tag) VALUES ($id, $a, $t)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(*a)),
                ("t", Value::Text((*tag).into())),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT a, tag FROM t WHERE a = 3 AND tag LIKE 'x%'",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(
        r.rows,
        vec![vec![Value::Int64(3), Value::Text("xavier".into())]]
    );
}

// ============================================================================
// MV01 — RED — snapshot isolation: reader opened before INSERT sees zero.
// [I5]
// ============================================================================
#[test]
fn mv01_reader_before_insert_sees_zero_rows_via_index_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let snap_before = db.snapshot();
    insert_row_t(&db, 7, 0, "late");
    let r = db
        .execute_at_snapshot("SELECT a FROM t WHERE a = 7", &empty(), snap_before)
        .expect("SELECT at snapshot");
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// MV02 — RED — soft-delete tombstone: pre-DELETE reader still sees row.
// [I5, I7]
// ============================================================================
#[test]
fn mv02_pre_delete_reader_still_sees_row() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 9, 99, "keep");
    let snap_before_delete = db.snapshot();
    db.execute(
        "DELETE FROM t WHERE a = $a",
        &params(vec![("a", Value::Int64(9))]),
    )
    .unwrap();

    let r_before = db
        .execute_at_snapshot(
            "SELECT a, b FROM t WHERE a = 9",
            &empty(),
            snap_before_delete,
        )
        .unwrap();
    assert_eq!(r_before.trace.physical_plan, "IndexScan");
    assert_eq!(r_before.rows, vec![vec![Value::Int64(9), Value::Int64(99)]]);

    let r_now = db
        .execute("SELECT a, b FROM t WHERE a = 9", &empty())
        .unwrap();
    assert_eq!(r_now.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// MV03 — RED — UPDATE changes indexed column; pre-UPDATE snapshot sees old key,
// post-UPDATE sees new. Each exactly once. [I5]
// ============================================================================
#[test]
fn mv03_update_changes_indexed_column_visibility() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let id = insert_row_t(&db, 1, 0, "row");
    let snap_before_update = db.snapshot();
    db.execute(
        "UPDATE t SET a = $new WHERE id = $id",
        &params(vec![("new", Value::Int64(99)), ("id", Value::Uuid(id))]),
    )
    .unwrap();

    let r_old_snap_old_key = db
        .execute_at_snapshot("SELECT a FROM t WHERE a = 1", &empty(), snap_before_update)
        .unwrap();
    assert_eq!(r_old_snap_old_key.rows, vec![vec![Value::Int64(1)]]);

    let r_new_snap_new_key = db
        .execute("SELECT a FROM t WHERE a = 99", &empty())
        .unwrap();
    assert_eq!(r_new_snap_new_key.rows, vec![vec![Value::Int64(99)]]);

    let r_new_snap_old_key = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    assert_eq!(r_new_snap_old_key.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// MV04 — RED — WriteSet rollback leaves index unchanged. [I6]
// ============================================================================
#[test]
fn mv04_rollback_leaves_index_unchanged() {
    let db = Database::open_memory();
    seed_t_with_index(&db);

    db.execute("BEGIN", &empty()).unwrap();
    let _mid_id = insert_row_t(&db, 55, 0, "will-roll");
    db.execute("ROLLBACK", &empty()).unwrap();

    let r = db
        .execute("SELECT a FROM t WHERE a = 55", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// MV05 — RED — sync-apply batch holds indexes.write() exactly once. [I14]
// Counter-instrumented via test-only probe on Database.
// ============================================================================
#[test]
fn mv05_sync_apply_batch_single_write_lock() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let before = db.__index_write_lock_count();

    let mut rows = Vec::new();
    for i in 0..100i64 {
        let mut vals = HashMap::new();
        vals.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
        vals.insert("a".to_string(), Value::Int64(i));
        vals.insert("b".to_string(), Value::Int64(i * 10));
        vals.insert("tag".to_string(), Value::Text(format!("r{i}")));
        rows.push(RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: vals["id"].clone(),
            },
            values: vals,
            deleted: false,
            lsn: Lsn(0),
        });
    }
    let cs = ChangeSet {
        rows,
        ..ChangeSet::default()
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();
    let after = db.__index_write_lock_count();
    assert_eq!(
        after - before,
        1,
        "expected exactly 1 indexes.write() for batch"
    );
}

// ============================================================================
// MV06 — RED — cross-open: IndexScan selected after reopen; rows byte-identical.
// [I4]
// ============================================================================
#[test]
fn mv06_cross_open_index_scan_preserved() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    {
        let db = Database::open(&path).unwrap();
        seed_t_with_index(&db);
        insert_row_t(&db, 11, 111, "persist");
    }
    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT a, b, tag FROM t WHERE a = 11", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int64(11),
            Value::Int64(111),
            Value::Text("persist".into())
        ]]
    );
}

// ============================================================================
// MV07 — RED — concurrent writer + reader: reader at snapshot S sees no
// post-commit row via IndexScan while writer is committing. [I5 under
// contention]
// ============================================================================
#[test]
fn indexed_scan_concurrent_writer_reader_no_ghost_rows() {
    use std::sync::{Arc, Barrier};
    let db = Arc::new(Database::open_memory());
    seed_t_with_index(&db);
    // Pre-existing row visible to every snapshot.
    insert_row_t(&db, 100, 0, "pre");

    let snap = db.snapshot();
    let barrier = Arc::new(Barrier::new(2));

    let writer_db = Arc::clone(&db);
    let writer_barrier = Arc::clone(&barrier);
    let writer = std::thread::spawn(move || {
        writer_barrier.wait();
        // Writer inserts a NEW row after the reader's snapshot was captured.
        let id = Uuid::new_v4();
        writer_db
            .execute(
                "INSERT INTO t (id, a, b, tag) VALUES ($id, $a, $b, $t)",
                &params(vec![
                    ("id", Value::Uuid(id)),
                    ("a", Value::Int64(200)),
                    ("b", Value::Int64(0)),
                    ("t", Value::Text("post".into())),
                ]),
            )
            .unwrap();
        id
    });

    let reader_db = Arc::clone(&db);
    let reader_barrier = Arc::clone(&barrier);
    let reader = std::thread::spawn(move || {
        reader_barrier.wait();
        // Wait briefly so the writer has a chance to commit before the read.
        // The correctness gate is semantic: snapshot must isolate, regardless
        // of timing.
        std::thread::sleep(std::time::Duration::from_millis(20));
        reader_db
            .execute_at_snapshot("SELECT a, tag FROM t WHERE a = 200", &empty(), snap)
            .unwrap()
    });

    let _new_id = writer.join().expect("writer");
    let r_reader = reader.join().expect("reader");
    assert_eq!(r_reader.trace.physical_plan, "IndexScan");
    assert_eq!(
        r_reader.rows,
        Vec::<Vec<Value>>::new(),
        "snapshot reader must not see the post-snapshot insert"
    );

    // Final read at the current snapshot sees the new row.
    let r_now = db
        .execute("SELECT a, tag FROM t WHERE a = 200", &empty())
        .unwrap();
    assert_eq!(r_now.trace.physical_plan, "IndexScan");
    assert_eq!(
        r_now.rows,
        vec![vec![Value::Int64(200), Value::Text("post".into())]]
    );
}

// ============================================================================
// DDL01 — RED — CREATE INDEX writes IndexDecl into TableMeta.indexes.
// ============================================================================
#[test]
fn ddl01_create_index_records_index_decl() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx ON t (a)", &empty()).unwrap();
    let meta = db.table_meta("t").expect("table meta");
    assert_eq!(
        meta.indexes,
        vec![IndexDecl {
            name: "idx".to_string(),
            columns: vec![("a".to_string(), SortDirection::Asc)],
        }]
    );
}

// ============================================================================
// DDL02 — RED — CREATE INDEX with explicit DESC preserves direction.
// ============================================================================
#[test]
fn ddl02_create_index_desc_preserves_direction() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx ON t (a DESC)", &empty())
        .unwrap();
    let meta = db.table_meta("t").unwrap();
    assert_eq!(
        meta.indexes[0].columns,
        vec![("a".to_string(), SortDirection::Desc)]
    );
}

// ============================================================================
// DDL02neg — RED — bogus direction token returns ParseError naming the token.
// Negative with positive control DDL02.
// ============================================================================
#[test]
fn ddl02neg_create_index_bogus_direction_rejected() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t (a BOGUS)", &empty())
        .expect_err("bogus direction must parse-error");
    match err {
        Error::ParseError(msg) => assert!(msg.contains("BOGUS"), "msg={msg}"),
        other => panic!("expected ParseError, got {other:?}"),
    }
}

// ============================================================================
// DDL03 — RED — composite index with mixed directions preserves per-column.
// ============================================================================
#[test]
fn ddl03_create_index_composite_mixed_directions() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx ON t (a ASC, b DESC, c)", &empty())
        .unwrap();
    let meta = db.table_meta("t").unwrap();
    assert_eq!(
        meta.indexes[0].columns,
        vec![
            ("a".to_string(), SortDirection::Asc),
            ("b".to_string(), SortDirection::Desc),
            ("c".to_string(), SortDirection::Asc),
        ]
    );
}

// ============================================================================
// DDL04 — RED — CREATE INDEX on nonexistent column → ColumnNotFound.
// ============================================================================
#[test]
fn ddl04_create_index_on_missing_column_returns_column_not_found() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t (nope)", &empty())
        .expect_err("must fail");
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "nope");
        }
        other => panic!("expected ColumnNotFound, got {other:?}"),
    }
}

// ============================================================================
// DDL05 — RED — CREATE INDEX on Json column → ColumnNotIndexable. [I16]
// ============================================================================
#[test]
fn ddl05_create_index_on_json_returns_column_not_indexable() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, payload JSON)",
        &empty(),
    )
    .unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t (payload)", &empty())
        .expect_err("must fail");
    match err {
        Error::ColumnNotIndexable {
            table,
            column,
            column_type,
        } => {
            assert_eq!(table, "t");
            assert_eq!(column, "payload");
            assert_eq!(column_type, ColumnType::Json);
        }
        other => panic!("expected ColumnNotIndexable, got {other:?}"),
    }
}

// ============================================================================
// DDL06 — RED — CREATE INDEX on Vector column → ColumnNotIndexable. [I16]
// ============================================================================
#[test]
fn ddl06_create_index_on_vector_returns_column_not_indexable() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, emb VECTOR(4))",
        &empty(),
    )
    .unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t (emb)", &empty())
        .expect_err("must fail");
    match err {
        Error::ColumnNotIndexable {
            table,
            column,
            column_type,
        } => {
            assert_eq!(table, "t");
            assert_eq!(column, "emb");
            assert_eq!(column_type, ColumnType::Vector(4));
        }
        other => panic!("expected ColumnNotIndexable, got {other:?}"),
    }
}

// ============================================================================
// DDL07 — RED — duplicate index name → DuplicateIndex.
// ============================================================================
#[test]
fn ddl07_duplicate_index_name_returns_duplicate_index() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx ON t (a)", &empty()).unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t (a)", &empty())
        .expect_err("duplicate must fail");
    match err {
        Error::DuplicateIndex { table, index } => {
            assert_eq!(table, "t");
            assert_eq!(index, "idx");
        }
        other => panic!("expected DuplicateIndex, got {other:?}"),
    }
}

// ============================================================================
// DDL07a — RED — CREATE INDEX on nonexistent table → TableNotFound.
// Pins error-precedence position 1: TableNotFound beats every other error.
// ============================================================================
#[test]
fn ddl07a_create_index_on_nonexistent_table_returns_table_not_found() {
    let db = Database::open_memory();
    // No CREATE TABLE — the target table does not exist.
    let err = db
        .execute("CREATE INDEX idx ON nope (col)", &empty())
        .expect_err("CREATE INDEX on missing table must fail");
    match err {
        Error::TableNotFound(name) => {
            assert_eq!(name, "nope");
        }
        other => panic!("expected TableNotFound(\"nope\"), got {other:?}"),
    }
}

// ============================================================================
// DDL07b — RED — CREATE INDEX with simultaneous ColumnNotFound + DuplicateIndex:
// ColumnNotFound wins. Pins error-precedence position 2 > 4.
// ============================================================================
#[test]
fn ddl07b_create_index_column_not_found_beats_duplicate_index() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx ON t (a)", &empty()).unwrap();
    // Re-declaring `idx` (name collision) AND referencing a nonexistent column `bogus`.
    let err = db
        .execute("CREATE INDEX idx ON t (bogus)", &empty())
        .expect_err("simultaneous ColumnNotFound + DuplicateIndex must fail");
    match err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "bogus");
        }
        other => {
            panic!("expected ColumnNotFound for the typo to win over DuplicateIndex, got {other:?}")
        }
    }
}

// ============================================================================
// DDL07c — RED — CREATE INDEX with simultaneous ColumnNotIndexable + DuplicateIndex:
// ColumnNotIndexable wins. Pins error-precedence position 3 > 4.
// ============================================================================
#[test]
fn ddl07c_create_index_column_not_indexable_beats_duplicate_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, meta JSON)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx ON t (a)", &empty()).unwrap();
    // Re-declaring `idx` AND indexing the Json `meta` column (not B-tree indexable).
    let err = db
        .execute("CREATE INDEX idx ON t (meta)", &empty())
        .expect_err("simultaneous ColumnNotIndexable + DuplicateIndex must fail");
    match err {
        Error::ColumnNotIndexable { table, column, .. } => {
            assert_eq!(table, "t");
            assert_eq!(column, "meta");
        }
        other => panic!("expected ColumnNotIndexable to win over DuplicateIndex, got {other:?}"),
    }
}

// ============================================================================
// DDL08 — RED — two indexes same column, different names: both allowed.
// ============================================================================
#[test]
fn ddl08_two_indexes_same_column_different_names_succeed() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx1 ON t (a)", &empty()).unwrap();
    db.execute("CREATE INDEX idx2 ON t (a)", &empty()).unwrap();
    let meta = db.table_meta("t").unwrap();
    let names: Vec<&str> = meta.indexes.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, vec!["idx1", "idx2"]);
}

// ============================================================================
// DDL09 — RED — same index name on two tables: both succeed (table-scoped).
// ============================================================================
#[test]
fn ddl09_same_index_name_across_tables_succeeds() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t1 (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE TABLE t2 (id UUID PRIMARY KEY, b INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx ON t1 (a)", &empty()).unwrap();
    db.execute("CREATE INDEX idx ON t2 (b)", &empty()).unwrap();
    let t1 = db.table_meta("t1").unwrap();
    let t2 = db.table_meta("t2").unwrap();
    assert_eq!(
        t1.indexes,
        vec![IndexDecl {
            name: "idx".to_string(),
            columns: vec![("a".to_string(), SortDirection::Asc)],
        }]
    );
    assert_eq!(
        t2.indexes,
        vec![IndexDecl {
            name: "idx".to_string(),
            columns: vec![("b".to_string(), SortDirection::Asc)],
        }]
    );
}

// ============================================================================
// DDL10 — RED — DROP INDEX removes; subsequent SELECT falls back to Scan.
// ============================================================================
#[test]
fn ddl10_drop_index_falls_back_to_scan() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 0, "x");
    db.execute("DROP INDEX idx_a ON t", &empty()).unwrap();
    let r = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    assert_eq!(r.trace.physical_plan, "Scan");
    assert!(r.trace.index_used.is_none());
}

// ============================================================================
// DDL11 — RED — DROP INDEX missing index → IndexNotFound.
// ============================================================================
#[test]
fn ddl11_drop_index_missing_returns_index_not_found() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let err = db
        .execute("DROP INDEX no_such ON t", &empty())
        .expect_err("must fail");
    match err {
        Error::IndexNotFound { table, index } => {
            assert_eq!(table, "t");
            assert_eq!(index, "no_such");
        }
        other => panic!("expected IndexNotFound, got {other:?}"),
    }
}

// ============================================================================
// DDL12 — RED — DROP INDEX IF EXISTS is idempotent.
// ============================================================================
#[test]
fn ddl12_drop_index_if_exists_is_idempotent() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let r1 = db
        .execute("DROP INDEX IF EXISTS no_such ON t", &empty())
        .expect("IF EXISTS first call must succeed");
    assert_eq!(r1.rows_affected, 0);
    assert!(db.table_meta("t").unwrap().indexes.is_empty());
    let r2 = db
        .execute("DROP INDEX IF EXISTS no_such ON t", &empty())
        .expect("IF EXISTS second call must succeed (idempotent)");
    assert_eq!(r2.rows_affected, 0);
    assert!(db.table_meta("t").unwrap().indexes.is_empty());
}

// ============================================================================
// DDL13 — RED — DROP COLUMN RESTRICT with indexed column → ColumnInIndex. [I17]
// ============================================================================
#[test]
fn ddl13_drop_column_restrict_on_indexed_rejects() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let err = db
        .execute("ALTER TABLE t DROP COLUMN a RESTRICT", &empty())
        .expect_err("must fail");
    match err {
        Error::ColumnInIndex {
            table,
            column,
            index,
        } => {
            assert_eq!(table, "t");
            assert_eq!(column, "a");
            assert_eq!(index, "idx_a");
        }
        other => panic!("expected ColumnInIndex, got {other:?}"),
    }
    // Row-readback: column still exists (schema unchanged).
    let meta = db.table_meta("t").unwrap();
    assert!(meta.columns.iter().any(|c| c.name == "a"));
}

// ============================================================================
// DDL14 — RED — default modifier (omitted) is RESTRICT.
// ============================================================================
#[test]
fn ddl14_drop_column_default_is_restrict() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let err = db
        .execute("ALTER TABLE t DROP COLUMN a", &empty())
        .expect_err("must fail");
    if let Error::ColumnInIndex {
        table,
        column,
        index,
    } = err
    {
        assert_eq!(table, "t");
        assert_eq!(column, "a");
        assert_eq!(index, "idx_a");
    } else {
        panic!("expected ColumnInIndex with all fields pinned, got different variant");
    }
}

// ============================================================================
// DDL15 — RED — CASCADE drops indexes referencing the column; cascade report
// populated. [I11]
// ============================================================================
#[test]
fn ddl15_drop_column_cascade_drops_indexes_and_reports() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a, b)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_b ON t (b)", &empty()).unwrap();
    let r = db
        .execute("ALTER TABLE t DROP COLUMN a CASCADE", &empty())
        .expect("CASCADE must succeed");
    let cascade = r.cascade.expect("cascade must be populated");
    let mut dropped = cascade.dropped_indexes.clone();
    dropped.sort();
    assert_eq!(dropped, vec!["idx_a".to_string(), "idx_ab".to_string()]);
    let meta = db.table_meta("t").unwrap();
    let remaining: Vec<&str> = meta.indexes.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(remaining, vec!["idx_b"]);
}

// ============================================================================
// DDL16 — RED — CASCADE with no dependent indexes: Some, empty vec.
// ============================================================================
#[test]
fn ddl16_drop_column_cascade_empty_vec_when_unindexed() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    let r = db
        .execute("ALTER TABLE t DROP COLUMN b CASCADE", &empty())
        .unwrap();
    let cascade = r.cascade.expect("cascade Some");
    assert!(cascade.dropped_indexes.is_empty());
    // Column actually removed from the schema.
    let meta = db.table_meta("t").unwrap();
    let names: Vec<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !names.contains(&"b"),
        "column b must be removed; got {names:?}"
    );
    // And idx_a still exists since it did not reference b.
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "idx_a");
}

// ============================================================================
// DDL17 — REGRESSION GUARD — DROP COLUMN on unindexed column succeeds either
// modifier. Pair with DDL13.
// ============================================================================
#[test]
fn ddl17_drop_column_unindexed_succeeds_both_modifiers() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("ALTER TABLE t DROP COLUMN b RESTRICT", &empty())
        .expect("unindexed RESTRICT must succeed");
    let t_meta = db.table_meta("t").unwrap();
    let t_cols: Vec<&str> = t_meta.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !t_cols.contains(&"b"),
        "b must be removed from t; got {t_cols:?}"
    );

    db.execute(
        "CREATE TABLE t2 (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("ALTER TABLE t2 DROP COLUMN b CASCADE", &empty())
        .expect("unindexed CASCADE must succeed");
    let t2_meta = db.table_meta("t2").unwrap();
    let t2_cols: Vec<&str> = t2_meta.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !t2_cols.contains(&"b"),
        "b must be removed from t2; got {t2_cols:?}"
    );
}

// ============================================================================
// DDL18neg — RED — bad modifier token rejected by parser.
// ============================================================================
#[test]
fn ddl18neg_drop_column_bad_modifier_rejected() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let err = db
        .execute("ALTER TABLE t DROP COLUMN a UNKNOWN", &empty())
        .expect_err("bad modifier must fail");
    match err {
        Error::ParseError(msg) => assert!(
            msg.contains("UNKNOWN"),
            "error must name the offending token; got {msg}"
        ),
        other => panic!("expected ParseError, got {other:?}"),
    }
}

// ============================================================================
// DDL22 — RED — DROP TABLE cascades all indexes; re-creating the table shows
// empty indexes; no stale storage leaks from the dropped table's indexes.
// ============================================================================
#[test]
fn drop_table_unregisters_all_indexes_and_releases_storage() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    db.execute("CREATE INDEX idx_b ON t (b)", &empty()).unwrap();
    // Populate for storage footprint — a leak would keep BTreeMap entries live.
    for i in 0..50i64 {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(i)),
                ("b", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let before = db.__introspect_indexes_total_entries();
    assert!(
        before >= 100,
        "two indexes with 50 rows must have >= 100 entries; got {before}"
    );

    db.execute("DROP TABLE t", &empty()).unwrap();
    let after_drop = db.__introspect_indexes_total_entries();
    assert_eq!(
        after_drop, 0,
        "DROP TABLE must release every index entry; got {after_drop}"
    );

    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    let meta = db.table_meta("t").unwrap();
    assert!(
        meta.indexes.is_empty(),
        "re-created t must have empty indexes; got {:?}",
        meta.indexes
    );
}

// ============================================================================
// PK01 — RED — 10K INSERTs routed through PK index, not O(n²). Asserts the
// check_row_constraints probe shows IndexScan and total wall-clock budget.
// [I13]
// ============================================================================
#[test]
fn pk01_pk_constraint_check_routes_through_index() {
    use std::time::Instant;
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let started = Instant::now();
    for i in 0..10_000i64 {
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let elapsed = started.elapsed();
    // A full-scan check_row_constraints is O(n) per insert, O(n²) total — at
    // n=10K that is >> 2 seconds on any hardware. An index-routed check stays
    // well under this budget. The upper bound is generous to avoid CI flakes
    // while still being a hard contradictor of O(n²).
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "10K PK inserts must finish in <2s, took {elapsed:?}"
    );
    // Direct probe: a duplicate-id INSERT returns UniqueViolation and the
    // constraint probe trace says IndexScan.
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![("id", Value::Uuid(id)), ("a", Value::Int64(0))]),
    )
    .unwrap();
    let probe = db
        .__probe_constraint_check("t", "id", Value::Uuid(id))
        .expect("probe result");
    assert_eq!(probe.trace.physical_plan, "IndexScan");
}

// ============================================================================
// PK02 — RED — UNIQUE constraint check routes through index. [I13]
// ============================================================================
#[test]
fn pk02_unique_constraint_check_routes_through_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    let id1 = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, email) VALUES ($id, $e)",
        &params(vec![
            ("id", Value::Uuid(id1)),
            ("e", Value::Text("a@b.c".into())),
        ]),
    )
    .unwrap();
    let err = db
        .execute(
            "INSERT INTO t (id, email) VALUES ($id, $e)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("e", Value::Text("a@b.c".into())),
            ]),
        )
        .expect_err("duplicate must fail");
    match err {
        Error::UniqueViolation { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "email");
        }
        other => panic!("expected UniqueViolation, got {other:?}"),
    }
    let probe = db
        .__probe_constraint_check("t", "email", Value::Text("a@b.c".into()))
        .unwrap();
    assert_eq!(probe.trace.physical_plan, "IndexScan");
}

// ============================================================================
// PK03 — RED — composite UNIQUE (a, b) constraint check routes through index.
// ============================================================================
#[test]
fn pk03_composite_unique_routes_through_index() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
            ("b", Value::Int64(2)),
        ]),
    )
    .unwrap();
    let err = db
        .execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(1)),
                ("b", Value::Int64(2)),
            ]),
        )
        .expect_err("duplicate composite must fail");
    match err {
        Error::UniqueViolation { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "a");
        }
        other => panic!("expected UniqueViolation, got {other:?}"),
    }
    let probe = db
        .__probe_constraint_check("t", "a", Value::Int64(1))
        .unwrap();
    assert_eq!(probe.trace.physical_plan, "IndexScan");
}

// ============================================================================
// ORD05 — RED — deterministic ordering across identical SELECTs. [I3]
// ============================================================================
#[test]
fn ord05_two_identical_selects_return_identical_order() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    for (a, tag) in [(3i64, "c"), (1, "a"), (2, "b"), (1, "a2")] {
        insert_row_t(&db, a, 0, tag);
    }
    let r1 = db
        .execute(
            "SELECT a, tag FROM t WHERE a != 999 ORDER BY a ASC",
            &empty(),
        )
        .unwrap();
    let r2 = db
        .execute(
            "SELECT a, tag FROM t WHERE a != 999 ORDER BY a ASC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r1.trace.physical_plan, "IndexScan");
    assert_eq!(r2.trace.physical_plan, "IndexScan");
    assert_eq!(r1.rows.len(), 4, "must return all four rows");
    assert_eq!(r1.rows, r2.rows);
}

// ============================================================================
// ORD06 — RED — non-unique index tie-break by row_id ascending. [I18]
// Two rows sharing the same key: they appear in row_id-ascending order.
// ============================================================================
#[test]
fn ord06_non_unique_index_tie_break_by_row_id_ascending() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let id_first = insert_row_t(&db, 5, 100, "first");
    let id_second = insert_row_t(&db, 5, 200, "second");
    let r = db
        .execute("SELECT id, b FROM t WHERE a = 5", &empty())
        .unwrap();
    // The first-inserted row has the smaller row_id, so appears first.
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Uuid(id_first), Value::Int64(100)],
            vec![Value::Uuid(id_second), Value::Int64(200)],
        ]
    );
}

// ============================================================================
// NUL01 — RED — ASC index: NULL sorts LAST. [I12]
// ============================================================================
#[test]
fn nul01_asc_null_sorts_last() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER NULL)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a ASC)", &empty())
        .unwrap();
    for v in [Some(1i64), None, Some(2)] {
        let val = match v {
            Some(n) => Value::Int64(n),
            None => Value::Null,
        };
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4())), ("a", val)]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT a FROM t ORDER BY a ASC", &empty())
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Null],
        ]
    );
}

// ============================================================================
// NUL02 — RED — DESC index: NULL sorts FIRST. [I12]
// ============================================================================
#[test]
fn nul02_desc_null_sorts_first() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER NULL)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a DESC)", &empty())
        .unwrap();
    for v in [Some(1i64), None, Some(2)] {
        let val = match v {
            Some(n) => Value::Int64(n),
            None => Value::Null,
        };
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4())), ("a", val)]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT a FROM t ORDER BY a DESC", &empty())
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Null],
            vec![Value::Int64(2)],
            vec![Value::Int64(1)],
        ]
    );
}

// ============================================================================
// NAN01 — RED — Float64 ASC: NaN sorts greater than finite values. [I12]
// `f64::total_cmp` semantics.
// ============================================================================
#[test]
fn nan01_asc_nan_sorts_greater_than_finite() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, f REAL)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_f ON t (f ASC)", &empty())
        .unwrap();
    for v in [1.0_f64, f64::NAN, 2.0] {
        db.execute(
            "INSERT INTO t (id, f) VALUES ($id, $f)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("f", Value::Float64(v)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT f FROM t ORDER BY f ASC", &empty())
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    // First two are finite 1.0, 2.0.
    match (&r.rows[0][0], &r.rows[1][0]) {
        (Value::Float64(a), Value::Float64(b)) => {
            assert_eq!(*a, 1.0);
            assert_eq!(*b, 2.0);
        }
        other => panic!("expected finite floats first, got {other:?}"),
    }
    // Last is NaN (Float64 NaN; identity via is_nan).
    match &r.rows[2][0] {
        Value::Float64(x) => assert!(x.is_nan(), "expected NaN last, got {x}"),
        other => panic!("expected Float64 NaN, got {other:?}"),
    }
}

// ============================================================================
// NAN02 — RED — WHERE col = NaN returns zero rows. [I19]
// ============================================================================
#[test]
fn nan02_where_equal_nan_returns_zero_rows() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, f REAL)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_f ON t (f)", &empty()).unwrap();
    let id_nan = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, f) VALUES ($id, $f)",
        &params(vec![
            ("id", Value::Uuid(id_nan)),
            ("f", Value::Float64(f64::NAN)),
        ]),
    )
    .unwrap();
    let id_finite = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, f) VALUES ($id, $f)",
        &params(vec![
            ("id", Value::Uuid(id_finite)),
            ("f", Value::Float64(1.0)),
        ]),
    )
    .unwrap();
    // Positive control: WHERE f = 1.0 returns the finite row via IndexScan.
    let r_pos = db
        .execute(
            "SELECT id FROM t WHERE f = $v",
            &params(vec![("v", Value::Float64(1.0))]),
        )
        .unwrap();
    assert_eq!(r_pos.trace.physical_plan, "IndexScan");
    assert_eq!(r_pos.rows, vec![vec![Value::Uuid(id_finite)]]);
    // Negative: NaN equality short-circuits.
    let r = db
        .execute("SELECT f FROM t WHERE f = 0.0/0.0", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// NAN03 — RED — parameterized NaN bind returns zero rows. [I19]
// ============================================================================
#[test]
fn nan03_param_nan_bind_returns_zero_rows() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, f REAL)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_f ON t (f)", &empty()).unwrap();
    let id_nan = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, f) VALUES ($id, $f)",
        &params(vec![
            ("id", Value::Uuid(id_nan)),
            ("f", Value::Float64(f64::NAN)),
        ]),
    )
    .unwrap();
    let id_finite = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, f) VALUES ($id, $f)",
        &params(vec![
            ("id", Value::Uuid(id_finite)),
            ("f", Value::Float64(2.5)),
        ]),
    )
    .unwrap();
    // Positive control: bound-param finite equality works.
    let r_pos = db
        .execute(
            "SELECT id FROM t WHERE f = $p",
            &params(vec![("p", Value::Float64(2.5))]),
        )
        .unwrap();
    assert_eq!(r_pos.trace.physical_plan, "IndexScan");
    assert_eq!(r_pos.rows, vec![vec![Value::Uuid(id_finite)]]);
    // Negative: NaN-bind short-circuits.
    let r = db
        .execute(
            "SELECT f FROM t WHERE f = $p",
            &params(vec![("p", Value::Float64(f64::NAN))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
}

// ============================================================================
// NAN04 — RED — IS NULL does NOT match NaN. Pair with NAN02 for I19 coverage.
// ============================================================================
#[test]
fn nan04_is_null_does_not_match_nan() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, f REAL NULL)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_f ON t (f)", &empty()).unwrap();
    let id_nan = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, f) VALUES ($id, $f)",
        &params(vec![
            ("id", Value::Uuid(id_nan)),
            ("f", Value::Float64(f64::NAN)),
        ]),
    )
    .unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE f IS NULL", &empty())
        .unwrap();
    assert_eq!(r.rows, Vec::<Vec<Value>>::new());
    let r2 = db
        .execute("SELECT id FROM t WHERE f IS NOT NULL", &empty())
        .unwrap();
    assert_eq!(r2.rows, vec![vec![Value::Uuid(id_nan)]]);
}

// ============================================================================
// NAN05 — RED — Float64 DESC: NaN ordering mirrors f64::total_cmp reversed.
// Under total_cmp NaN > all finite, so DESC (reverse of total_cmp order)
// places NaN FIRST, then 2.0, then 1.0. Pins I12's DESC half.
// ============================================================================
#[test]
fn indexed_scan_float64_nan_ordering_desc_mirrors_total_cmp() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, f REAL)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_f ON t (f DESC)", &empty())
        .unwrap();
    for v in [1.0_f64, f64::NAN, 2.0] {
        db.execute(
            "INSERT INTO t (id, f) VALUES ($id, $f)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("f", Value::Float64(v)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT f FROM t ORDER BY f DESC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(r.rows.len(), 3);
    match &r.rows[0][0] {
        Value::Float64(x) => assert!(x.is_nan(), "expected NaN first under DESC; got {x}"),
        other => panic!("expected Float64 NaN, got {other:?}"),
    }
    match (&r.rows[1][0], &r.rows[2][0]) {
        (Value::Float64(a), Value::Float64(b)) => {
            assert_eq!(*a, 2.0);
            assert_eq!(*b, 1.0);
        }
        other => panic!("expected 2.0 then 1.0, got {other:?}"),
    }
}

// ============================================================================
// ORD01 — RED — prefix-compatible ORDER BY elides Sort.
// ============================================================================
#[test]
fn ord01_prefix_compatible_order_by_elides_sort() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_ct ON t (created_at DESC, id DESC)",
        &empty(),
    )
    .unwrap();
    let r = db
        .execute(
            "SELECT id FROM t ORDER BY created_at DESC, id DESC",
            &empty(),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
}

// ============================================================================
// ORD02 — RED — direction mismatch keeps Sort node.
// ============================================================================
#[test]
fn ord02_direction_mismatch_keeps_sort() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, created_at TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_ct ON t (created_at DESC, id DESC)",
        &empty(),
    )
    .unwrap();
    let r = db
        .execute("SELECT id FROM t ORDER BY created_at ASC, id ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "Sort");
    assert!(!r.trace.sort_elided);
}

// ============================================================================
// ORD03 — RED — ORDER BY prefix of composite index elides Sort.
// ============================================================================
#[test]
fn ord03_prefix_of_composite_elides_sort() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a, b)", &empty())
        .unwrap();
    for (a, b) in &[(3i64, 10i64), (1, 20), (2, 30)] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(*a)),
                ("b", Value::Int64(*b)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT a FROM t ORDER BY a ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)],
        ]
    );
}

// ============================================================================
// ORD04 — RED — ORDER BY non-prefix keeps Sort.
// ============================================================================
#[test]
fn ord04_non_prefix_keeps_sort() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a, b)", &empty())
        .unwrap();
    for (a, b) in &[(3i64, 10i64), (1, 20), (2, 30)] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(*a)),
                ("b", Value::Int64(*b)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT b FROM t ORDER BY b ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "Sort");
    assert!(!r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(10)],
            vec![Value::Int64(20)],
            vec![Value::Int64(30)],
        ]
    );
}

// ============================================================================
// ORD09 — RED — composite mixed-direction elision requires exact per-column
// direction match. Index `(a ASC, b DESC)`:
//   - ORDER BY a ASC, b DESC  → sort_elided == true
//   - ORDER BY a ASC, b ASC   → sort_elided == false + Sort node retained
// Pins the direction-per-column equality rule.
// ============================================================================
#[test]
fn order_by_composite_mixed_direction_elision_requires_exact_match() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a ASC, b DESC)", &empty())
        .unwrap();
    for (a, b) in &[(1i64, 30i64), (1, 20), (1, 10), (2, 50), (2, 40)] {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(*a)),
                ("b", Value::Int64(*b)),
            ]),
        )
        .unwrap();
    }
    let r_match = db
        .execute("SELECT a, b FROM t ORDER BY a ASC, b DESC", &empty())
        .unwrap();
    assert_eq!(r_match.trace.physical_plan, "IndexScan");
    assert!(r_match.trace.sort_elided);
    assert_eq!(
        r_match.rows,
        vec![
            vec![Value::Int64(1), Value::Int64(30)],
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(50)],
            vec![Value::Int64(2), Value::Int64(40)],
        ]
    );

    let r_mismatch = db
        .execute("SELECT a, b FROM t ORDER BY a ASC, b ASC", &empty())
        .unwrap();
    assert_eq!(r_mismatch.trace.physical_plan, "Sort");
    assert!(!r_mismatch.trace.sort_elided);
    assert_eq!(
        r_mismatch.rows,
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(1), Value::Int64(30)],
            vec![Value::Int64(2), Value::Int64(40)],
            vec![Value::Int64(2), Value::Int64(50)],
        ]
    );
}

// ============================================================================
// PR01 — RED — CREATE INDEX survives reopen; IndexScan still selected. [I4]
// ============================================================================
#[test]
fn pr01_index_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
            .unwrap();
        db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
        db.execute(
            "INSERT INTO t (id, a) VALUES ($id, $a)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(42)),
            ]),
        )
        .unwrap();
    }
    let db = Database::open(&path).unwrap();
    let meta = db.table_meta("t").unwrap();
    assert_eq!(meta.indexes.len(), 1);
    assert_eq!(meta.indexes[0].name, "idx_a");
    let r = db
        .execute("SELECT a FROM t WHERE a = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.rows, vec![vec![Value::Int64(42)]]);
}

// ============================================================================
// PR02 — REGRESSION GUARD — legacy TableMeta without `indexes` field decodes
// cleanly via `#[serde(default)]` on the new field. Once §4.2 stubs land,
// this test passes; it guards against a future removal of the serde default.
// ============================================================================
#[test]
fn pr02_legacy_table_meta_decodes_with_empty_indexes() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct LegacyColumnDef {
        name: String,
        column_type: ColumnType,
        nullable: bool,
        primary_key: bool,
        unique: bool,
        default: Option<String>,
        references: Option<contextdb_core::table_meta::ForeignKeyReference>,
        expires: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct LegacyTableMeta {
        columns: Vec<LegacyColumnDef>,
        immutable: bool,
        state_machine: Option<contextdb_core::table_meta::StateMachineConstraint>,
        dag_edge_types: Vec<String>,
        unique_constraints: Vec<Vec<String>>,
        natural_key_column: Option<String>,
        propagation_rules: Vec<contextdb_core::table_meta::PropagationRule>,
        default_ttl_seconds: Option<u64>,
        sync_safe: bool,
        expires_column: Option<String>,
    }

    let legacy = LegacyTableMeta {
        columns: vec![LegacyColumnDef {
            name: "id".to_string(),
            column_type: ColumnType::Uuid,
            nullable: false,
            primary_key: true,
            unique: false,
            default: None,
            references: None,
            expires: false,
        }],
        immutable: false,
        state_machine: None,
        dag_edge_types: vec![],
        unique_constraints: vec![],
        natural_key_column: None,
        propagation_rules: vec![],
        default_ttl_seconds: None,
        sync_safe: false,
        expires_column: None,
    };
    let bytes =
        bincode::serde::encode_to_vec(&legacy, bincode::config::standard()).expect("legacy encode");
    let (decoded, _len) =
        bincode::serde::decode_from_slice::<TableMeta, _>(&bytes, bincode::config::standard())
            .expect("legacy bytes must decode into TableMeta");
    assert!(decoded.indexes.is_empty());
    assert_eq!(decoded.columns.len(), 1);
    assert_eq!(decoded.columns[0].name, "id");
}

// ============================================================================
// PR03 — RED — CREATE INDEX / DROP INDEX emit DdlChange entries in ddl_log.
// ============================================================================
#[test]
fn pr03_index_ddl_emits_ddl_log_entries() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let before = db.current_lsn();
    db.execute("CREATE INDEX idx ON t (a ASC)", &empty())
        .unwrap();
    db.execute("DROP INDEX idx ON t", &empty()).unwrap();
    let entries = db.ddl_log_since(before);
    let has_create = entries.iter().any(|d| matches!(
        d,
        DdlChange::CreateIndex { table, name, columns }
        if table == "t" && name == "idx" && columns == &vec![("a".to_string(), SortDirection::Asc)]
    ));
    let has_drop = entries.iter().any(|d| {
        matches!(
            d,
            DdlChange::DropIndex { table, name } if table == "t" && name == "idx"
        )
    });
    assert!(has_create, "expected CreateIndex entry, got {entries:?}");
    assert!(has_drop, "expected DropIndex entry, got {entries:?}");
}

// ============================================================================
// PR04 — RED — rebuild-scale guard: 100K rows + 1 index, close, reopen must
// complete under 10s on Jetson-class hardware. Binds the upper edge of the
// "on-disk index rebuild on open" non-goal (>500K rows file a follow-up).
// ============================================================================
#[test]
fn database_open_with_100k_rows_and_index_completes_under_upper_bound() {
    use std::time::Instant;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, bucket INTEGER)",
            &empty(),
        )
        .unwrap();
        db.execute("CREATE INDEX idx_bucket ON t (bucket)", &empty())
            .unwrap();
        for i in 0..100_000i64 {
            db.execute(
                "INSERT INTO t (id, bucket) VALUES ($id, $b)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("b", Value::Int64(i % 10_000)),
                ]),
            )
            .unwrap();
        }
    }
    let started = Instant::now();
    let db = Database::open(&path).unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "100K-row reopen must complete in <10s; took {elapsed:?}"
    );
    // Index functional after reopen.
    let r = db
        .execute("SELECT id FROM t WHERE bucket = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    // 100_000 / 10_000 = 10 rows per bucket.
    assert_eq!(r.rows.len(), 10);
    assert!(db.__rows_examined() <= 50, "got {}", db.__rows_examined());
}

// ============================================================================
// SY01 — RED — DdlChange::CreateIndex replicates across sync. [I8]
// ============================================================================
#[test]
fn sy01_create_index_replicates_through_sync() {
    let origin = Database::open_memory();
    origin
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let before = origin.current_lsn();
    origin
        .execute("CREATE INDEX idx ON t (a)", &empty())
        .unwrap();
    let ddl = origin.ddl_log_since(before);

    let receiver = Database::open_memory();
    receiver
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let cs = ChangeSet {
        ddl: ddl.clone(),
        ..ChangeSet::default()
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    let meta = receiver.table_meta("t").unwrap();
    assert_eq!(
        meta.indexes,
        vec![IndexDecl {
            name: "idx".to_string(),
            columns: vec![("a".to_string(), SortDirection::Asc)],
        }]
    );
}

// ============================================================================
// SY02 — RED — DdlChange::DropIndex replicates. [I8]
// ============================================================================
#[test]
fn sy02_drop_index_replicates_through_sync() {
    let origin = Database::open_memory();
    origin
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    origin
        .execute("CREATE INDEX idx ON t (a)", &empty())
        .unwrap();
    let before = origin.current_lsn();
    origin.execute("DROP INDEX idx ON t", &empty()).unwrap();
    // Origin-side readback: drop must actually remove the index locally.
    assert!(
        origin.table_meta("t").unwrap().indexes.is_empty(),
        "origin must show empty indexes after DROP"
    );
    let ddl = origin.ddl_log_since(before);

    let receiver = Database::open_memory();
    receiver
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    receiver
        .execute("CREATE INDEX idx ON t (a)", &empty())
        .unwrap();
    let cs = ChangeSet {
        ddl,
        ..ChangeSet::default()
    };
    receiver
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    let meta = receiver.table_meta("t").unwrap();
    assert!(meta.indexes.is_empty());
}

// ============================================================================
// SY03 — RED — sync-apply batch of 100 row inserts acquires indexes.write()
// exactly once. [I14] Named distinctly from MV05 to differentiate pathway.
// ============================================================================
#[test]
fn sy03_sync_apply_batch_single_index_write_lock() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    let before = db.__index_write_lock_count();
    let mut rows = Vec::new();
    for i in 0..100i64 {
        let mut vals = HashMap::new();
        vals.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        vals.insert("a".into(), Value::Int64(i));
        vals.insert("b".into(), Value::Int64(0));
        vals.insert("tag".into(), Value::Text("x".into()));
        rows.push(RowChange {
            table: "t".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: vals["id"].clone(),
            },
            values: vals,
            deleted: false,
            lsn: Lsn(0),
        });
    }
    let cs = ChangeSet {
        rows,
        ..ChangeSet::default()
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();
    let after = db.__index_write_lock_count();
    assert_eq!(after - before, 1);
}

// ============================================================================
// TR01 — RED — QueryResult.trace.physical_plan is always populated with a
// non-empty string. [I15]
// ============================================================================
#[test]
fn tr01_physical_plan_always_populated() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let r1 = db.execute("SELECT a FROM t", &empty()).unwrap();
    assert_eq!(r1.trace.physical_plan, "Scan");
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
        ]),
    )
    .unwrap();
    let r2 = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    assert_eq!(r2.trace.physical_plan, "IndexScan");
}

// ============================================================================
// TR02 — RED — predicates_pushed for equality is exactly [column]. [I15]
// ============================================================================
#[test]
fn tr02_predicates_pushed_is_exact_singleton() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 0, "x");
    let r = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    let pushed: Vec<String> = r
        .trace
        .predicates_pushed
        .iter()
        .map(|c| c.as_ref().to_string())
        .collect();
    assert_eq!(pushed, vec!["a".to_string()]);
}

// ============================================================================
// TR03 — RED — indexes_considered populated with rejected candidates. [I15]
// ============================================================================
#[test]
fn tr03_indexes_considered_populated_with_reasons() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty())
        .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE UPPER(name) = 'X'", &empty())
        .unwrap();
    assert_eq!(r.trace.indexes_considered.len(), 1);
    assert_eq!(r.trace.indexes_considered[0].name, "idx_name");
    assert_eq!(
        r.trace.indexes_considered[0].rejected_reason.as_ref(),
        "function call in predicate"
    );
}

// ============================================================================
// TR04 — RED — sort_elided populated in isolation.
// ============================================================================
#[test]
fn tr04_sort_elided_populated_independently() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a ASC)", &empty())
        .unwrap();
    let r_elided = db
        .execute("SELECT a FROM t ORDER BY a ASC", &empty())
        .unwrap();
    assert!(r_elided.trace.sort_elided);
    let r_not = db
        .execute("SELECT a FROM t ORDER BY a DESC", &empty())
        .unwrap();
    assert!(!r_not.trace.sort_elided);
}

// ============================================================================
// SCL01 — RED — 10K-row scale gate: IndexScan examines <<100 rows on a filter
// returning ~10 matches. A full-scan impl that lies about the trace examines
// 10K and fails this test. [I15 + non-fakeable performance gate]
// ============================================================================
#[test]
fn indexed_scan_select_at_10k_rows_sublinear_rows_examined() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, bucket INTEGER, v INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_bucket ON t (bucket)", &empty())
        .unwrap();
    // 10,000 rows across 1000 buckets (10 rows per bucket).
    for i in 0..10_000i64 {
        db.execute(
            "INSERT INTO t (id, bucket, v) VALUES ($id, $b, $v)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("b", Value::Int64(i % 1000)),
                ("v", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT v FROM t WHERE bucket = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_bucket"));
    assert_eq!(r.rows.len(), 10, "bucket=42 has exactly 10 rows");
    // Non-fakeable gate: a Scan touches 10,000; IndexScan must stay under 100.
    assert!(
        db.__rows_examined() < 100,
        "IndexScan must be sublinear at 10K rows; got {}",
        db.__rows_examined()
    );
}

// ============================================================================
// SCL02 — REGRESSION GUARD — paired positive control for SCL01. Same data,
// no index — confirms Scan examines the full 10K so the counter distinguishes
// the two paths.
// ============================================================================
#[test]
fn scan_select_at_10k_rows_examines_full_table() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, bucket INTEGER, v INTEGER)",
        &empty(),
    )
    .unwrap();
    for i in 0..10_000i64 {
        db.execute(
            "INSERT INTO t (id, bucket, v) VALUES ($id, $b, $v)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("b", Value::Int64(i % 1000)),
                ("v", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT v FROM t WHERE bucket = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "Scan");
    assert_eq!(r.rows.len(), 10);
    assert!(
        db.__rows_examined() >= 10_000,
        "Scan must examine the full table; got {}",
        db.__rows_examined()
    );
}

// ============================================================================
// EX01 — RED — programmatic trace carries "IndexScan" on an indexed query.
// (`.explain` is a REPL command, not a SQL prefix; machine-readable access
// is QueryResult.trace per the behavior contract.)
// ============================================================================
#[test]
fn ex01_programmatic_trace_indexed_query() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 0, "x");
    let r = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert_eq!(r.trace.index_used.as_deref(), Some("idx_a"));
}

// ============================================================================
// EX02 — RED — programmatic trace carries "Scan" on an unindexed query.
// Pair with EX01.
// ============================================================================
#[test]
fn ex02_programmatic_trace_unindexed_query() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let r = db.execute("SELECT a FROM t WHERE a = 1", &empty()).unwrap();
    assert_eq!(r.trace.physical_plan, "Scan");
    assert!(r.trace.index_used.is_none());
}

// ============================================================================
// EX03 — RED — CLI `.explain <sql>` rendering helper produces human-readable
// text containing "IndexScan" and the index name. The CLI binary (binary-only
// crate `contextdb-cli`) delegates its `.explain` REPL command to a public
// renderer in `contextdb_engine::cli_render::render_explain`, which tests can
// call directly. This keeps the CLI contract testable without spawning a
// subprocess.
// ============================================================================
#[test]
fn ex03_cli_explain_renders_index_scan_text() {
    let db = Database::open_memory();
    seed_t_with_index(&db);
    insert_row_t(&db, 1, 0, "x");
    let output =
        contextdb_engine::cli_render::render_explain(&db, "SELECT a FROM t WHERE a = 1", &empty())
            .expect("render_explain must succeed on valid SQL");
    assert!(output.contains("IndexScan"), "got: {output}");
    assert!(output.contains("idx_a"), "got: {output}");
}

// ============================================================================
// SC01 — RED — render_table_meta emits CREATE INDEX lines with directions.
// [I9]
// ============================================================================
#[test]
fn sc01_render_table_meta_emits_create_index_lines() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx ON t (a ASC, b DESC)", &empty())
        .unwrap();
    let meta = db.table_meta("t").unwrap();
    let rendered = render_table_meta("t", &meta);
    assert!(rendered.contains("CREATE TABLE t"), "got: {rendered}");
    assert!(
        rendered.contains("CREATE INDEX idx ON t (a ASC, b DESC)"),
        "got: {rendered}"
    );
}

// ============================================================================
// SC02 — RED — round-trip render → parse → apply yields identical indexes.
// [I9]
// ============================================================================
#[test]
fn sc02_round_trip_rendered_schema_matches() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx ON t (a ASC, b DESC)", &empty())
        .unwrap();
    let meta_a = db.table_meta("t").unwrap();
    let rendered = render_table_meta("t", &meta_a);

    let db2 = Database::open_memory();
    for stmt in rendered.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        db2.execute(stmt, &empty())
            .unwrap_or_else(|e| panic!("parse/apply failed for {stmt}: {e}"));
    }
    let meta_b = db2.table_meta("t").unwrap();
    assert_eq!(meta_a.indexes, meta_b.indexes);
}

// ============================================================================
// BA01 — RED — ChangeSet with 50 rows + 1 CreateIndex: index applied BEFORE
// rows so rows populate the new structure. Post-apply IndexScan returns all 50.
// ============================================================================
#[test]
fn ba01_batch_apply_create_index_before_rows() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER, tag TEXT)",
        &empty(),
    )
    .unwrap();

    let mut rows = Vec::new();
    for i in 0..50i64 {
        let mut vals = HashMap::new();
        vals.insert("id".into(), Value::Uuid(Uuid::new_v4()));
        vals.insert("a".into(), Value::Int64(42));
        vals.insert("b".into(), Value::Int64(i));
        vals.insert("tag".into(), Value::Text(format!("r{i}")));
        rows.push(RowChange {
            table: "t".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: vals["id"].clone(),
            },
            values: vals,
            deleted: false,
            lsn: Lsn(0),
        });
    }
    let cs = ChangeSet {
        rows,
        ddl: vec![DdlChange::CreateIndex {
            table: "t".into(),
            name: "idx_a".into(),
            columns: vec![("a".into(), SortDirection::Asc)],
        }],
        ..ChangeSet::default()
    };
    db.apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE a = 42", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    // COUNT(*) path: assert exactly one row with Int64(50).
    assert_eq!(r.rows, vec![vec![Value::Int64(50)]]);
}

// ============================================================================
// CG02 — RED — engine-side parallel of the cg acceptance test: at 5K rows
// filtered by indexed `entity_type` + residual `name_contains`, p95 under 100ms
// AND trace populated. Proves the cg-layer call path without cg dependency.
// ============================================================================
#[test]
fn cg02_engine_side_entity_list_filter_under_budget() {
    use std::time::Instant;
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, entity_type TEXT, name TEXT, created_at TIMESTAMP)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_entity_type ON entities (entity_type, created_at DESC, id DESC)",
        &empty(),
    )
    .unwrap();
    for i in 0..5_000i64 {
        let entity_type = if i % 2 == 0 { "Service" } else { "Database" };
        let name = if i % 5 == 0 {
            format!("pay-entity-{i}")
        } else {
            format!("misc-entity-{i}")
        };
        db.execute(
            "INSERT INTO entities (id, entity_type, name, created_at) VALUES ($id, $et, $n, CURRENT_TIMESTAMP)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("et", Value::Text(entity_type.into())),
                ("n", Value::Text(name)),
            ]),
        )
        .unwrap();
    }
    let mut samples = Vec::new();
    for _ in 0..20 {
        let started = Instant::now();
        let _ = db
            .execute(
                "SELECT id, entity_type, name FROM entities \
                 WHERE entity_type = $et AND name LIKE $pat \
                 ORDER BY created_at DESC, id DESC",
                &params(vec![
                    ("et", Value::Text("Service".into())),
                    ("pat", Value::Text("%pay%".into())),
                ]),
            )
            .unwrap();
        samples.push(started.elapsed());
    }
    samples.sort();
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    assert!(
        p95 <= std::time::Duration::from_millis(100),
        "p95 {p95:?} exceeds 100ms budget"
    );
    let r = db
        .execute(
            "SELECT id FROM entities WHERE entity_type = $et ORDER BY created_at DESC, id DESC",
            &params(vec![("et", Value::Text("Service".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    // Trace-liar gate: ~2500 rows match (entity_type = "Service"). A full
    // scan examines 5000. IndexScan must examine close to the match count.
    assert!(
        db.__rows_examined() <= 3000,
        "IndexScan must not examine the full 5K table; got {}",
        db.__rows_examined()
    );
}

// ============================================================================
// CO01 — RED — IMMUTABLE column + index: INSERT + IndexScan works identically.
// ============================================================================
#[test]
fn co01_index_on_immutable_column_succeeds_and_filters() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, label TEXT NOT NULL IMMUTABLE)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_label ON t (label)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, label) VALUES ($id, $l)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("l", Value::Text("frozen".into())),
        ]),
    )
    .unwrap();
    // Seed 30 noise rows so a full-scan impl would be visible to __rows_examined.
    for i in 0..30i64 {
        db.execute(
            "INSERT INTO t (id, label) VALUES ($id, $l)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("l", Value::Text(format!("noise{i}"))),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM t WHERE label = $l",
            &params(vec![("l", Value::Text("frozen".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
}

// ============================================================================
// CO02 — RED — STATE MACHINE status column + index: query by status uses
// IndexScan; state-machine rules still enforced on UPDATE.
// ============================================================================
#[test]
fn co02_index_on_state_machine_column_succeeds_and_filters() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (\
            id UUID PRIMARY KEY, \
            status TEXT NOT NULL DEFAULT 'active'\
         ) STATE MACHINE (status: active -> [superseded, archived])",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_status ON t (status)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, status) VALUES ($id, $s)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("s", Value::Text("active".into())),
        ]),
    )
    .unwrap();
    // 20 rows in 'archived'; only the target row in 'active'.
    for _ in 0..20 {
        let other = Uuid::new_v4();
        db.execute(
            "INSERT INTO t (id, status) VALUES ($id, $s)",
            &params(vec![
                ("id", Value::Uuid(other)),
                ("s", Value::Text("archived".into())),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM t WHERE status = $s",
            &params(vec![("s", Value::Text("active".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
}

// ============================================================================
// CO03 — RED — DAG table + index: INSERT + IndexScan works; DAG constraint
// still governs edges.
// ============================================================================
#[test]
fn co03_index_on_dag_table_succeeds() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (\
            id UUID PRIMARY KEY, \
            name TEXT\
         ) DAG ('supersedes')",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $n)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("n", Value::Text("root".into())),
        ]),
    )
    .unwrap();
    for i in 0..20i64 {
        db.execute(
            "INSERT INTO t (id, name) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("n", Value::Text(format!("other{i}"))),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM t WHERE name = $n",
            &params(vec![("n", Value::Text("root".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
}

// ============================================================================
// CO04 — RED — RETAIN table + index: IndexScan works during the retain window.
// ============================================================================
#[test]
fn co04_index_on_retain_table_succeeds() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (\
            id UUID PRIMARY KEY, \
            name TEXT\
         ) RETAIN 1 HOUR",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_name ON t (name)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $n)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("n", Value::Text("kept".into())),
        ]),
    )
    .unwrap();
    for i in 0..20i64 {
        db.execute(
            "INSERT INTO t (id, name) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("n", Value::Text(format!("other{i}"))),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM t WHERE name = $n",
            &params(vec![("n", Value::Text("kept".into()))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
}

// ============================================================================
// CO05 — RED — REFERENCES column + index: FK still enforced, IndexScan used.
// ============================================================================
#[test]
fn co05_index_on_references_column_succeeds() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE parent (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child (\
            id UUID PRIMARY KEY, \
            parent_id UUID REFERENCES parent(id)\
         )",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_parent ON child (parent_id)", &empty())
        .unwrap();
    let parent_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO parent (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(parent_id))]),
    )
    .unwrap();
    // Additional parents for noise children.
    let mut other_parents = Vec::new();
    for _ in 0..5 {
        let pid = Uuid::new_v4();
        db.execute(
            "INSERT INTO parent (id) VALUES ($id)",
            &params(vec![("id", Value::Uuid(pid))]),
        )
        .unwrap();
        other_parents.push(pid);
    }
    let child_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO child (id, parent_id) VALUES ($id, $p)",
        &params(vec![
            ("id", Value::Uuid(child_id)),
            ("p", Value::Uuid(parent_id)),
        ]),
    )
    .unwrap();
    // 20 noise child rows pointing at other parents.
    for pid in other_parents.iter().cycle().take(20) {
        db.execute(
            "INSERT INTO child (id, parent_id) VALUES ($id, $p)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("p", Value::Uuid(*pid)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM child WHERE parent_id = $p",
            &params(vec![("p", Value::Uuid(parent_id))]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(child_id)]]);
}

// ============================================================================
// CO06 — RED — EXPIRES column + index: IndexScan works on the expires column.
//
// NOTE: The original test body referenced `chrono::DateTime::from_timestamp`,
// which is not a dependency of this workspace, and wrapped the result in
// `Value::Timestamp(...)` — but `Value::Timestamp` is an `i64` Unix timestamp.
// The test's setup would not have compiled; this is an IC14-style test-design
// bug in the plan. The body below preserves every assertion and the row-noise
// design; only the Value constructors are adapted to `Value::Timestamp(i64)`.
// See the execute-step report's "Test-design issues" section.
// ============================================================================
#[test]
fn co06_index_on_expires_column_succeeds() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (\
            id UUID PRIMARY KEY, \
            expires_at TIMESTAMP EXPIRES\
         )",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_expires ON t (expires_at)", &empty())
        .unwrap();
    let id = Uuid::new_v4();
    let ts = Value::Timestamp(2_000_000_000);
    db.execute(
        "INSERT INTO t (id, expires_at) VALUES ($id, $e)",
        &params(vec![("id", Value::Uuid(id)), ("e", ts.clone())]),
    )
    .unwrap();
    for i in 0..20i64 {
        let other_ts = Value::Timestamp(2_000_000_000 + 100 + i);
        db.execute(
            "INSERT INTO t (id, expires_at) VALUES ($id, $e)",
            &params(vec![("id", Value::Uuid(Uuid::new_v4())), ("e", other_ts)]),
        )
        .unwrap();
    }
    let r = db
        .execute(
            "SELECT id FROM t WHERE expires_at = $e",
            &params(vec![("e", ts)]),
        )
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(
        db.__rows_examined() <= 4,
        "trace-liar gate: got {}",
        db.__rows_examined()
    );
    assert_eq!(r.rows, vec![vec![Value::Uuid(id)]]);
}

// ============================================================================
// DDL19 — RED — CREATE INDEX on UUID column succeeds and walks sorted.
// ============================================================================
#[test]
fn ddl19_create_index_on_uuid_column_succeeds_and_walks_sorted() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, tag TEXT, ref_id UUID)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ref ON t (ref_id ASC)", &empty())
        .unwrap();
    // Three UUIDs whose byte-order is known: Uuid::from_u128 preserves ordering.
    let u1 = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0001);
    let u2 = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0002);
    let u3 = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_0003);
    // Insert out of order.
    for (tag, r) in &[("b", u2), ("c", u3), ("a", u1)] {
        db.execute(
            "INSERT INTO t (id, tag, ref_id) VALUES ($id, $t, $r)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("t", Value::Text((*tag).into())),
                ("r", Value::Uuid(*r)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT tag FROM t ORDER BY ref_id ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Text("a".into())],
            vec![Value::Text("b".into())],
            vec![Value::Text("c".into())],
        ]
    );
}

// ============================================================================
// DDL20 — RED — CREATE INDEX on TXID column succeeds and walks sorted.
// ============================================================================
#[test]
fn ddl20_create_index_on_txid_column_succeeds_and_walks_sorted() {
    use contextdb_core::TxId;
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, tag TEXT, seen_tx TXID)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_tx ON t (seen_tx ASC)", &empty())
        .unwrap();
    for (tag, tx) in &[("mid", TxId(20)), ("hi", TxId(30)), ("lo", TxId(10))] {
        db.execute(
            "INSERT INTO t (id, tag, seen_tx) VALUES ($id, $t, $x)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("t", Value::Text((*tag).into())),
                ("x", Value::TxId(*tx)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT tag FROM t ORDER BY seen_tx ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Text("lo".into())],
            vec![Value::Text("mid".into())],
            vec![Value::Text("hi".into())],
        ]
    );
}

// ============================================================================
// DDL21 — RED — CREATE INDEX on BOOLEAN column succeeds; equality walks the
// true / false partitions.
// ============================================================================
#[test]
fn ddl21_create_index_on_bool_column_succeeds_and_walks_sorted() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, tag TEXT, active BOOLEAN)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_active ON t (active ASC)", &empty())
        .unwrap();
    for (tag, b) in &[("t1", true), ("f1", false), ("t2", true), ("f2", false)] {
        db.execute(
            "INSERT INTO t (id, tag, active) VALUES ($id, $t, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("t", Value::Text((*tag).into())),
                ("b", Value::Bool(*b)),
            ]),
        )
        .unwrap();
    }
    let r_true = db
        .execute(
            "SELECT tag FROM t WHERE active = $b ORDER BY tag ASC",
            &params(vec![("b", Value::Bool(true))]),
        )
        .unwrap();
    assert_eq!(r_true.trace.physical_plan, "IndexScan");
    assert_eq!(
        r_true.rows,
        vec![
            vec![Value::Text("t1".into())],
            vec![Value::Text("t2".into())],
        ]
    );
    let r_false = db
        .execute(
            "SELECT tag FROM t WHERE active = $b ORDER BY tag ASC",
            &params(vec![("b", Value::Bool(false))]),
        )
        .unwrap();
    assert_eq!(r_false.trace.physical_plan, "IndexScan");
    assert_eq!(
        r_false.rows,
        vec![
            vec![Value::Text("f1".into())],
            vec![Value::Text("f2".into())],
        ]
    );
}

// ============================================================================
// ORD07 — RED — non-unique tie-break: row_id ordering survives reopen. [I18]
// ============================================================================
#[test]
fn ord07_tie_break_row_id_ascending_cross_reopen_stable() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let mut ids_in_insert_order = Vec::new();
    {
        let db = Database::open(&path).unwrap();
        seed_t_with_index(&db);
        // Five rows sharing the same indexed key (a = 7); distinct ids + tags.
        for tag in ["p", "q", "r", "s", "t"] {
            ids_in_insert_order.push(insert_row_t(&db, 7, 0, tag));
        }
    }
    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT id, tag FROM t WHERE a = 7", &empty())
        .unwrap();
    // Extract the observed row_id sequence via id field.
    let ids: Vec<Value> = r.rows.iter().map(|row| row[0].clone()).collect();
    // row_id is internal; the observable tie-break is "same sequence every
    // time." The pre-reopen sequence is the insert order; post-reopen must
    // match. Assert full row-vec identity (id + tag) across all 5 rows.
    let expected_tags = vec!["p", "q", "r", "s", "t"];
    let got_tags: Vec<String> = r
        .rows
        .iter()
        .map(|row| match &row[1] {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text tag, got {other:?}"),
        })
        .collect();
    assert_eq!(got_tags, expected_tags);
    // And id sequence must match insertion order.
    let expected_ids: Vec<Value> = ids_in_insert_order
        .iter()
        .map(|u| Value::Uuid(*u))
        .collect();
    assert_eq!(ids, expected_ids);
}

// ============================================================================
// ORD08 — RED — non-unique tie-break: row_id ordering is byte-identical across
// peers via sync. [I18 "byte-identical across peers"]
// ============================================================================
#[test]
fn ord08_tie_break_row_id_ascending_cross_peer_byte_identical() {
    let edge_a = Database::open_memory();
    seed_t_with_index(&edge_a);
    let before = edge_a.current_lsn();
    let mut ids_in_insert_order = Vec::new();
    for tag in ["p", "q", "r", "s", "t"] {
        ids_in_insert_order.push(insert_row_t(&edge_a, 7, 0, tag));
    }
    let ddl = edge_a.ddl_log_since(Lsn(0));
    let rows_changes: Vec<RowChange> = edge_a
        .change_log_rows_since(before)
        .expect("change log rows");

    let edge_b = Database::open_memory();
    // Replicate schema + index + rows to edge B.
    let cs = ChangeSet {
        ddl,
        rows: rows_changes,
        ..ChangeSet::default()
    };
    edge_b
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap();

    let r_a = edge_a
        .execute("SELECT id, tag FROM t WHERE a = 7", &empty())
        .unwrap();
    let r_b = edge_b
        .execute("SELECT id, tag FROM t WHERE a = 7", &empty())
        .unwrap();
    assert_eq!(
        r_a.rows, r_b.rows,
        "edge A and edge B must agree row-by-row"
    );
    // And both must match the insertion-order id sequence.
    let expected_ids: Vec<Value> = ids_in_insert_order
        .iter()
        .map(|u| Value::Uuid(*u))
        .collect();
    let a_ids: Vec<Value> = r_a.rows.iter().map(|row| row[0].clone()).collect();
    assert_eq!(a_ids, expected_ids);
}

// ============================================================================
// SY04 — RED — DESC direction replicates; peer's IndexScan returns rows in
// DESC order. Augments I8.
// ============================================================================
#[test]
fn sy04_desc_direction_replicates_through_sync_ddl() {
    let origin = Database::open_memory();
    origin
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    let before = origin.current_lsn();
    origin
        .execute("CREATE INDEX idx_a ON t (a DESC)", &empty())
        .unwrap();
    let ddl = origin.ddl_log_since(before);

    let receiver = Database::open_memory();
    receiver
        .execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    receiver
        .apply_changes(
            ChangeSet {
                ddl: ddl.clone(),
                ..ChangeSet::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let meta = receiver.table_meta("t").unwrap();
    assert_eq!(
        meta.indexes,
        vec![IndexDecl {
            name: "idx_a".to_string(),
            columns: vec![("a".to_string(), SortDirection::Desc)],
        }]
    );

    for v in [1i64, 3, 2] {
        receiver
            .execute(
                "INSERT INTO t (id, a) VALUES ($id, $a)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("a", Value::Int64(v)),
                ]),
            )
            .unwrap();
    }
    let r = receiver
        .execute("SELECT a FROM t ORDER BY a DESC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(3)],
            vec![Value::Int64(2)],
            vec![Value::Int64(1)],
        ]
    );
}

// ============================================================================
// SY05 — RED — mixed-direction composite replicates; peer IndexScan respects
// both directions. Augments I8.
// ============================================================================
#[test]
fn sy05_mixed_direction_composite_replicates_through_sync_ddl() {
    let origin = Database::open_memory();
    origin
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
            &empty(),
        )
        .unwrap();
    let before = origin.current_lsn();
    origin
        .execute("CREATE INDEX idx_ab ON t (a ASC, b DESC)", &empty())
        .unwrap();
    let ddl = origin.ddl_log_since(before);

    let receiver = Database::open_memory();
    receiver
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
            &empty(),
        )
        .unwrap();
    receiver
        .apply_changes(
            ChangeSet {
                ddl,
                ..ChangeSet::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    let meta = receiver.table_meta("t").unwrap();
    assert_eq!(
        meta.indexes[0].columns,
        vec![
            ("a".to_string(), SortDirection::Asc),
            ("b".to_string(), SortDirection::Desc),
        ]
    );

    for (a, b) in &[(1i64, 10i64), (1, 30), (1, 20), (2, 5)] {
        receiver
            .execute(
                "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("a", Value::Int64(*a)),
                    ("b", Value::Int64(*b)),
                ]),
            )
            .unwrap();
    }
    let r = receiver
        .execute("SELECT a, b FROM t ORDER BY a ASC, b DESC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided);
    assert_eq!(
        r.rows,
        vec![
            vec![Value::Int64(1), Value::Int64(30)],
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(5)],
        ]
    );
}

// Doc-scan helpers — live in the same test file.

fn read_doc(rel_path: &str) -> String {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|p| p.join("docs").is_dir())
        .expect("doc root must exist")
        .to_path_buf();
    let full = root.join(rel_path);
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {full:?}: {e}"))
}

fn section_body<'a>(doc: &'a str, heading: &str) -> &'a str {
    let heading_line = doc
        .lines()
        .position(|l| {
            l.trim_start_matches('#').trim() == heading.trim_start_matches('#').trim()
                && l.starts_with('#')
        })
        .unwrap_or_else(|| panic!("heading {heading:?} not found"));
    let mut end = doc.len();
    let level = heading.chars().take_while(|c| *c == '#').count();
    let prefix = "#".repeat(level);
    let mut cursor = 0usize;
    for (idx, line) in doc.lines().enumerate() {
        cursor += line.len() + 1;
        if idx <= heading_line {
            continue;
        }
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            let next_level = trimmed.chars().take_while(|c| *c == '#').count();
            if next_level <= level && !trimmed.starts_with(&format!("{prefix}#")) {
                end = cursor - line.len() - 1;
                break;
            }
        }
    }
    let start = {
        let mut c = 0usize;
        for (idx, line) in doc.lines().enumerate() {
            if idx == heading_line {
                break;
            }
            c += line.len() + 1;
        }
        c
    };
    &doc[start..end.min(doc.len())]
}

fn extract_fenced_sql(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut lines = body.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed == "```sql" {
            let mut block = String::new();
            for l in &mut lines {
                if l.trim() == "```" {
                    break;
                }
                block.push_str(l);
                block.push('\n');
            }
            out.push(block);
        }
    }
    out
}

// ============================================================================
// DO01 — RED — docs/query-language.md has a section titled "Indexes" that
// mentions CREATE INDEX, per-column ASC/DESC, and the ColumnNotIndexable error.
// ============================================================================
#[test]
fn do01_query_language_md_indexes_section_present() {
    let doc = read_doc("docs/query-language.md");
    let body = section_body(&doc, "## Indexes");
    assert!(
        body.contains("CREATE INDEX"),
        "missing CREATE INDEX in: {body}"
    );
    assert!(
        body.contains("ASC") && body.contains("DESC"),
        "missing per-column ASC/DESC direction in: {body}"
    );
    assert!(body.contains("DROP INDEX"), "missing DROP INDEX in: {body}");
    assert!(
        body.contains("ColumnNotIndexable"),
        "missing ColumnNotIndexable error ref in: {body}"
    );
}

// ============================================================================
// DO02 — RED — docs/cli.md contains "Trace vs Explain" subsection or
// equivalent section distinguishing the programmatic trace from CLI explain.
// ============================================================================
#[test]
fn do02_cli_md_trace_vs_explain_subsection() {
    let doc = read_doc("docs/cli.md");
    let has_subsection = doc.lines().any(|l| {
        l.starts_with('#')
            && l.to_ascii_lowercase().contains("trace")
            && l.to_ascii_lowercase().contains("explain")
    });
    assert!(
        has_subsection,
        "missing Trace vs Explain heading in docs/cli.md"
    );
    assert!(
        doc.contains(".explain"),
        "missing .explain REPL mention in docs/cli.md"
    );
    assert!(
        doc.contains("QueryResult.trace"),
        "missing QueryResult.trace mention in docs/cli.md"
    );
}

// ============================================================================
// DO03 — RED — docs/getting-started.md contains a CREATE INDEX + filtered
// SELECT example; extracted SQL runs against a fresh DB and produces
// trace.physical_plan == "IndexScan" on the filtered SELECT.
// ============================================================================
#[allow(clippy::collapsible_if)]
#[test]
fn do03_getting_started_md_filtered_select_with_explain() {
    let doc = read_doc("docs/getting-started.md");
    assert!(
        doc.contains("CREATE INDEX"),
        "getting-started.md must include a CREATE INDEX example"
    );
    assert!(
        doc.contains("IndexScan") || doc.contains(".explain"),
        "getting-started.md must demonstrate IndexScan or .explain output"
    );
    let sql_blocks = extract_fenced_sql(&doc);
    let db = Database::open_memory();
    let mut observed_index_scan = false;
    for block in sql_blocks {
        for stmt in block.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            if let Ok(r) = db.execute(stmt, &empty()) {
                if r.trace.physical_plan == "IndexScan" {
                    observed_index_scan = true;
                }
            }
        }
    }
    assert!(
        observed_index_scan,
        "getting-started.md fenced SQL must produce at least one IndexScan trace"
    );
}

// ============================================================================
// DO04 — RED — docs/usage-scenarios.md contains a composite CREATE INDEX on a
// decisions- or entities-style table.
// ============================================================================
#[allow(clippy::collapsible_if)]
#[test]
fn do04_usage_scenarios_md_composite_index_in_decisions_scenario() {
    let doc = read_doc("docs/usage-scenarios.md");
    // Extract fenced SQL and require that at least one CREATE INDEX statement
    // is composite (contains `,` between the `(` and `)`) AND the surrounding
    // doc body mentions a decisions- or entities-shaped table.
    let sql_blocks = extract_fenced_sql(&doc);
    let mut composite_seen = false;
    for block in &sql_blocks {
        for stmt in block.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let upper = stmt.to_ascii_uppercase();
            if upper.starts_with("CREATE INDEX") {
                if let (Some(open), Some(close)) = (stmt.find('('), stmt.rfind(')')) {
                    if stmt[open..close].contains(',') {
                        composite_seen = true;
                    }
                }
            }
        }
    }
    assert!(
        composite_seen,
        "usage-scenarios.md fenced SQL must include a composite CREATE INDEX"
    );
    let has_target_table = doc.contains("decisions") || doc.contains("entities");
    assert!(
        has_target_table,
        "usage-scenarios.md composite index scenario must reference decisions or entities"
    );
    // And the fenced statements must at least parse against the engine.
    let db = Database::open_memory();
    for block in sql_blocks {
        for stmt in block.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let _ = db.execute(stmt, &empty());
        }
    }
}

// ============================================================================
// Step-6 fix #2 — user CREATE INDEX with reserved prefix is rejected.
// ============================================================================
#[test]
fn user_index_with_reserved_prefix_rejected_pk() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    let err = db
        .execute("CREATE INDEX __pk_id ON t (id)", &empty())
        .unwrap_err();
    match err {
        Error::ReservedIndexName {
            table,
            name,
            prefix,
        } => {
            assert_eq!(table, "t");
            assert_eq!(name, "__pk_id");
            assert_eq!(prefix, "__pk_");
        }
        other => panic!("expected ReservedIndexName, got {other:?}"),
    }
}

#[test]
fn user_index_with_reserved_prefix_rejected_unique() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, email TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    let err = db
        .execute("CREATE INDEX __unique_email ON t (email)", &empty())
        .unwrap_err();
    match err {
        Error::ReservedIndexName {
            table,
            name,
            prefix,
        } => {
            assert_eq!(table, "t");
            assert_eq!(name, "__unique_email");
            assert_eq!(prefix, "__unique_");
        }
        other => panic!("expected ReservedIndexName, got {other:?}"),
    }
}

// ============================================================================
// Step-6 fix #3 — non-id PK column probe routes through the auto-index.
// ============================================================================
#[test]
fn non_id_pk_column_probe_routes_through_index() {
    use std::time::Instant;
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (user_id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    let started = Instant::now();
    let mut sample_user: Option<Uuid> = None;
    for i in 0..10_000i64 {
        let uid = Uuid::new_v4();
        if i == 5_000 {
            sample_user = Some(uid);
        }
        db.execute(
            "INSERT INTO t (user_id, name) VALUES ($id, $n)",
            &params(vec![
                ("id", Value::Uuid(uid)),
                ("n", Value::Text(format!("u{i}"))),
            ]),
        )
        .unwrap();
    }
    let elapsed = started.elapsed();
    // If the non-id-PK code path fell through to full-scan constraint checks,
    // 10K inserts under O(n^2) would grossly exceed 4s.
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "non-id PK inserts must finish in <4s, took {elapsed:?}"
    );
    // Probe trace says IndexScan.
    let probe = db
        .__probe_constraint_check("t", "user_id", Value::Uuid(sample_user.unwrap()))
        .expect("probe result");
    assert_eq!(probe.trace.physical_plan, "IndexScan");

    // Full-row identity: the sampled row is still readable back.
    let r = db
        .execute(
            "SELECT user_id, name FROM t WHERE user_id = $id",
            &params(vec![("id", Value::Uuid(sample_user.unwrap()))]),
        )
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    match (&r.rows[0][0], &r.rows[0][1]) {
        (Value::Uuid(u), Value::Text(n)) => {
            assert_eq!(*u, sample_user.unwrap());
            assert_eq!(n, "u5000");
        }
        other => panic!("expected (Uuid, Text), got {other:?}"),
    }
}

// ============================================================================
// Step-6 fix #8 — sync DDL CreateIndex on missing table surfaces as
// Err(TableNotFound) instead of silently skipping.
// ============================================================================
#[test]
fn sync_createindex_missing_table_returns_tablenotfound() {
    let peer = Database::open_memory();
    // Peer has no table "bogus" — the sender's CreateIndex must not silently
    // skip (that would hide divergence). It must surface TableNotFound.
    let cs = ChangeSet {
        ddl: vec![DdlChange::CreateIndex {
            table: "bogus".into(),
            name: "idx".into(),
            columns: vec![("a".into(), SortDirection::Asc)],
        }],
        ..ChangeSet::default()
    };
    let err = peer
        .apply_changes(cs, &ConflictPolicies::uniform(ConflictPolicy::LatestWins))
        .unwrap_err();
    match err {
        Error::TableNotFound(name) => assert_eq!(name, "bogus"),
        other => panic!("expected TableNotFound, got {other:?}"),
    }
}

// ============================================================================
// Step-6 fix #9 — sort elision refused when the leading IndexScan shape is
// InList (fragmented posting walks do not emit globally sorted output).
// ============================================================================
#[test]
fn sort_elision_refused_for_inlist_indexscan() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)", &empty())
        .unwrap();
    db.execute("CREATE INDEX idx_a ON t (a)", &empty()).unwrap();
    // Seed rows with a IN {1,2,3,4,5}, multiple rows per value, reverse
    // insertion order so natural scan is NOT sorted by row_id either.
    for a in (1..=5i64).rev() {
        for dup in 0..3 {
            let _ = dup;
            db.execute(
                "INSERT INTO t (id, a) VALUES ($id, $a)",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("a", Value::Int64(a)),
                ]),
            )
            .unwrap();
        }
    }
    let r = db
        .execute(
            "SELECT a FROM t WHERE a IN (5, 1, 3) ORDER BY a ASC",
            &empty(),
        )
        .unwrap();
    // IndexScan is still picked (faster than full scan for an IN-list)...
    assert_eq!(r.trace.physical_plan, "IndexScan");
    // ...but the ORDER BY Sort node MUST run; elision is refused for InList.
    assert!(
        !r.trace.sort_elided,
        "sort_elided must be false for InList IndexScan"
    );
    // Rows ARE in the requested order — the real Sort node ran.
    let actual: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(v) => *v,
            other => panic!("expected Int64, got {other:?}"),
        })
        .collect();
    let mut sorted = actual.clone();
    sorted.sort();
    assert_eq!(actual, sorted, "rows must be globally sorted");
    assert!(
        actual.iter().all(|v| [1, 3, 5].contains(v)),
        "only IN-list values should be returned"
    );
}

#[test]
fn sort_elision_fires_for_equality_indexscan() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER, b INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE INDEX idx_ab ON t (a, b)", &empty())
        .unwrap();
    for i in 0..20i64 {
        db.execute(
            "INSERT INTO t (id, a, b) VALUES ($id, $a, $b)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("a", Value::Int64(7)),
                ("b", Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    let r = db
        .execute("SELECT b FROM t WHERE a = 7 ORDER BY b ASC", &empty())
        .unwrap();
    assert_eq!(r.trace.physical_plan, "IndexScan");
    assert!(r.trace.sort_elided, "equality IndexScan must permit elision");
}

// ============================================================================
// Step-6 fix #4 — snapshot override is honoured by user-visible readers.
// This pins the behavior that upsert_row, a writer-side reader, sees the
// override when pinned via execute_at_snapshot.
// ============================================================================
#[test]
fn snapshot_override_honoured_by_all_readers() {
    use contextdb_core::SnapshotId;
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, a INTEGER)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(1)),
        ]),
    )
    .unwrap();
    let snap_after_one: SnapshotId = db.snapshot();
    db.execute(
        "INSERT INTO t (id, a) VALUES ($id, $a)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("a", Value::Int64(2)),
        ]),
    )
    .unwrap();
    // At the pinned snapshot, SELECT must see only the first row.
    let r = db
        .execute_at_snapshot("SELECT COUNT(*) FROM t", &empty(), snap_after_one)
        .unwrap();
    match &r.rows[0][0] {
        Value::Int64(n) => assert_eq!(*n, 1, "pinned snapshot must see exactly 1 row"),
        other => panic!("expected Int64, got {other:?}"),
    }
    // But unpinned reads see the committed state (2 rows).
    let r2 = db.execute("SELECT COUNT(*) FROM t", &empty()).unwrap();
    match &r2.rows[0][0] {
        Value::Int64(n) => assert_eq!(*n, 2),
        other => panic!("expected Int64, got {other:?}"),
    }
}

// ============================================================================
// Step-6 fix #14 — rows_examined accumulates across sub-plans, not resets.
// Unit test against the counter itself plus a targeted behavioral test.
// ============================================================================
#[test]
fn rows_examined_counter_accumulates_across_bumps() {
    let db = Database::open_memory();
    db.__reset_rows_examined();
    assert_eq!(db.__rows_examined(), 0);
    db.__bump_rows_examined(10);
    db.__bump_rows_examined(25);
    assert_eq!(
        db.__rows_examined(),
        35,
        "accumulation must sum bumps, not overwrite"
    );
    // A subsequent query resets at the entry point.
    db.execute("CREATE TABLE accum (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    // Non-DDL query resets the counter.
    let _ = db.execute("SELECT * FROM accum", &empty()).unwrap();
    // The scan returned 0 rows; counter reflects that, not 35.
    assert_eq!(
        db.__rows_examined(),
        0,
        "top-of-query reset must zero the counter for a fresh SELECT"
    );
}
