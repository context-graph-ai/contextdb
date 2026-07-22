//! tests/integration/commit_write_volume_scaling_tests.rs
//!
//! Commit cost must be close to proportional to staged writes (small constant
//! term) and flat in committed history H, with byte-identical constraint
//! semantics. Counter-bound tests pair a floor (kills the zero stub) with a
//! ceiling and/or a history-invariance delta (kills the current superlinear /
//! history-proportional behavior).

use std::collections::HashMap;

use contextdb_core::{Error, SortDirection, Value};
use contextdb_engine::Database;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(items: Vec<(&str, Value)>) -> HashMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn assert_column_not_indexable(err: Error, expected_table: &str, expected_column: &str) {
    match err {
        Error::ColumnNotIndexable { table, column, .. } => {
            assert_eq!(table, expected_table);
            assert_eq!(column, expected_column);
        }
        other => panic!(
            "expected ColumnNotIndexable for {expected_table}.{expected_column}, got {other:?}"
        ),
    }
}

/// Mixed-shape schema: a state-machine parent, a child under composite UNIQUE +
/// composite FK, and a relational edge table whose (source_id, target_id,
/// edge_type) columns earn the `__graph_edge_source_target_type` auto-index.
/// Generic schema-agnostic names only.
fn declare_mixed_schema(db: &Database) {
    db.execute(
        "CREATE TABLE parent (a UUID NOT NULL, b INTEGER NOT NULL, \
         status TEXT NOT NULL DEFAULT 'active', UNIQUE (a, b)) \
         STATE MACHINE (status: active -> [invalidated, superseded])",
        &empty(),
    )
    .expect("create parent");
    db.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, c1 UUID NOT NULL, c2 INTEGER NOT NULL, \
         tag TEXT NOT NULL, UNIQUE (c1, c2, tag), FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &empty(),
    )
    .expect("create child");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, \
         target_id UUID NOT NULL, edge_type TEXT NOT NULL, \
         UNIQUE (source_id, target_id, edge_type))",
        &empty(),
    )
    .expect("create edges");
}

/// Insert one committed parent tuple (a, b) and return its (a, b) so children
/// can reference it. Each parent row starts in 'active'.
fn seed_parent(db: &Database, b: i64) -> (Uuid, i64) {
    let a = Uuid::new_v4();
    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "parent",
        HashMap::from([
            ("a".to_string(), Value::Uuid(a)),
            ("b".to_string(), Value::Int64(b)),
            ("status".to_string(), Value::Text("active".to_string())),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    (a, b)
}

/// Stage one mixed cascade of size N into a single tx and commit it:
/// - N parent status flips active -> invalidated
/// - N child inserts under composite UNIQUE + composite FK
/// - N edge inserts under composite UNIQUE on the auto-indexed shape
fn stage_mixed_cascade(db: &Database, parents: &[(Uuid, i64)]) {
    let tx = db.begin().unwrap();
    for (a, b) in parents {
        db.execute_in_tx(
            tx,
            "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(*a)), ("b", Value::Int64(*b))]),
        )
        .unwrap();
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("c1".to_string(), Value::Uuid(*a)),
                ("c2".to_string(), Value::Int64(*b)),
                ("tag".to_string(), Value::Text(format!("tag-{b}"))),
            ]),
        )
        .unwrap();
        db.insert_row(
            tx,
            "edges",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("source_id".to_string(), Value::Uuid(*a)),
                ("target_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("edge_type".to_string(), Value::Text("cascades".to_string())),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}

fn seed_mixed_history(db: &Database, rows: i64, base_b: i64) {
    let mut parents = Vec::with_capacity(rows as usize);

    let tx = db.begin().unwrap();
    for h in 0..rows {
        let a = Uuid::new_v4();
        let b = base_b + h;
        db.insert_row(
            tx,
            "parent",
            HashMap::from([
                ("a".to_string(), Value::Uuid(a)),
                ("b".to_string(), Value::Int64(b)),
                ("status".to_string(), Value::Text("active".to_string())),
            ]),
        )
        .unwrap();
        parents.push((a, b));
    }
    db.commit(tx).unwrap();

    let tx = db.begin().unwrap();
    for (i, (a, b)) in parents.iter().enumerate() {
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("c1".to_string(), Value::Uuid(*a)),
                ("c2".to_string(), Value::Int64(*b)),
                ("tag".to_string(), Value::Text(format!("hist-tag-{b}"))),
            ]),
        )
        .unwrap();
        db.insert_row(
            tx,
            "edges",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("source_id".to_string(), Value::Uuid(*a)),
                ("target_id".to_string(), Value::Uuid(Uuid::new_v4())),
                (
                    "edge_type".to_string(),
                    Value::Text(format!("hist-edge-{i}")),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}

fn run_measured_mixed_cascade(n: usize) -> contextdb_engine::CommitStageStats {
    let db = Database::open_memory();
    declare_mixed_schema(&db);

    let mut parents = Vec::with_capacity(n);
    for b in 0..n as i64 {
        parents.push(seed_parent(&db, b));
    }

    db.__reset_commit_stage_stats();
    stage_mixed_cascade(&db, &parents);
    let s = db.__commit_stage_stats();

    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        n
    );
    assert_eq!(
        db.execute("SELECT id FROM edges", &empty())
            .unwrap()
            .rows
            .len(),
        n
    );
    let invalidated = db
        .execute(
            "SELECT a FROM parent WHERE status = 'invalidated'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        invalidated.rows.len(),
        n,
        "all N parents must read 'invalidated'"
    );

    s
}

#[test]
fn cwv_01_mixed_shape_work_counters_bounded() {
    const N: usize = 30;
    const SMALL_N: usize = 10;

    let small = run_measured_mixed_cascade(SMALL_N);
    let s = run_measured_mixed_cascade(N);
    let staged_inserts = (2 * N) as u64;
    let small_staged_inserts = (2 * SMALL_N) as u64;
    let n = N as u64;
    let small_n = SMALL_N as u64;

    assert!(
        small.staged_vs_staged_comparisons >= small_n - 1,
        "small-N floor: expected >= {}, got {}",
        small_n - 1,
        small.staged_vs_staged_comparisons
    );
    assert!(
        small.rows_validated >= small_staged_inserts,
        "small-N floor: expected >= {small_staged_inserts} rows validated, got {}",
        small.rows_validated
    );
    assert!(
        small.indexed_probes >= small_staged_inserts,
        "small-N floor: expected >= {small_staged_inserts} indexed probes, got {}",
        small.indexed_probes
    );
    assert!(
        small.index_maintenance_visits >= small_staged_inserts,
        "small-N floor: expected >= {small_staged_inserts} index visits, got {}",
        small.index_maintenance_visits
    );
    assert!(
        s.staged_vs_staged_comparisons >= n - 1,
        "expected >= {} staged-vs-staged comparisons, got {} (zero stub?)",
        n - 1,
        s.staged_vs_staged_comparisons
    );
    assert!(
        s.rows_validated >= staged_inserts,
        "expected >= {staged_inserts} rows validated, got {}",
        s.rows_validated
    );
    assert!(
        s.indexed_probes >= staged_inserts,
        "expected >= {staged_inserts} indexed probes, got {}",
        s.indexed_probes
    );
    assert!(
        s.index_maintenance_visits >= staged_inserts,
        "expected >= {staged_inserts} index-maintenance visits, got {}",
        s.index_maintenance_visits
    );

    assert!(
        s.staged_vs_staged_comparisons <= 12 * staged_inserts,
        "comparisons {} exceed linear ceiling {} - superlinear staged validation",
        s.staged_vs_staged_comparisons,
        12 * staged_inserts
    );
    assert!(
        s.index_maintenance_visits <= staged_inserts * 4 + 4 * n,
        "index-maintenance visits {} exceed bound - touching more than written rows' indexes",
        s.index_maintenance_visits
    );

    assert!(
        s.staged_vs_staged_comparisons > small.staged_vs_staged_comparisons,
        "comparison counter did not grow from N={SMALL_N} to N={N}: {} vs {}",
        small.staged_vs_staged_comparisons,
        s.staged_vs_staged_comparisons
    );
    assert!(
        s.rows_validated > small.rows_validated,
        "rows_validated did not grow from N={SMALL_N} to N={N}: {} vs {}",
        small.rows_validated,
        s.rows_validated
    );
    assert!(
        s.indexed_probes > small.indexed_probes,
        "indexed_probes did not grow from N={SMALL_N} to N={N}: {} vs {}",
        small.indexed_probes,
        s.indexed_probes
    );
    assert!(
        s.index_maintenance_visits > small.index_maintenance_visits,
        "index_maintenance_visits did not grow from N={SMALL_N} to N={N}: {} vs {}",
        small.index_maintenance_visits,
        s.index_maintenance_visits
    );

    assert_eq!(
        s.scan_rows_touched, 0,
        "commit validation must not full-scan; touched {} scan rows",
        s.scan_rows_touched
    );
    assert_eq!(
        small.scan_rows_touched, 0,
        "small-N commit validation must not full-scan; touched {} scan rows",
        small.scan_rows_touched
    );
}

#[test]
fn cwv_02_mixed_shape_flat_in_history() {
    const N: usize = 20;

    fn run_cascade_with_history(history_multiplier: i64) -> contextdb_engine::CommitStageStats {
        let db = Database::open_memory();
        declare_mixed_schema(&db);

        let history_rows = history_multiplier * 250;
        seed_mixed_history(&db, history_rows, 1_000_000);

        let mut parents = Vec::with_capacity(N);
        for b in 0..N as i64 {
            parents.push(seed_parent(&db, b));
        }
        db.__reset_commit_stage_stats();
        stage_mixed_cascade(&db, &parents);
        db.__commit_stage_stats()
    }

    let small = run_cascade_with_history(1);
    let large = run_cascade_with_history(10);

    let n = N as u64;
    assert!(small.staged_vs_staged_comparisons >= n - 1);
    assert!(large.staged_vs_staged_comparisons >= n - 1);

    assert_eq!(
        small.staged_vs_staged_comparisons, large.staged_vs_staged_comparisons,
        "staged comparisons grew with committed history"
    );
    assert_eq!(
        small.rows_validated, large.rows_validated,
        "rows-validated grew with committed history"
    );
    assert_eq!(
        small.indexed_probes, large.indexed_probes,
        "indexed-probe count grew with committed history"
    );
    assert_eq!(small.scan_rows_touched, 0, "small-H commit must not scan");
    assert_eq!(
        large.scan_rows_touched, 0,
        "10x-H commit must not scan (history-proportional fallback fired)"
    );
    assert_eq!(
        small.index_maintenance_visits, large.index_maintenance_visits,
        "index-maintenance visits grew with committed history"
    );
}

#[test]
fn cwv_03_index_maintenance_visits_bounded() {
    fn stats_for_edge_history(history_edges: i64) -> contextdb_engine::CommitStageStats {
        let db = Database::open_memory();
        declare_mixed_schema(&db);

        let tx = db.begin().unwrap();
        for h in 0..history_edges {
            db.insert_row(
                tx,
                "edges",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("source_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("target_id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("edge_type".to_string(), Value::Text(format!("hist-{h}"))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();

        const N: usize = 25;
        let mut parents = Vec::with_capacity(N);
        for b in 0..N as i64 {
            parents.push(seed_parent(&db, b));
        }
        db.__reset_commit_stage_stats();
        stage_mixed_cascade(&db, &parents);
        db.__commit_stage_stats()
    }

    let small = stats_for_edge_history(1_000);
    let large = stats_for_edge_history(10_000);

    let n = 25_u64;
    let staged_inserts = (2 * 25) as u64;
    assert!(
        small.index_maintenance_visits >= staged_inserts,
        "floor: at least one index slot per staged insert, got {}",
        small.index_maintenance_visits
    );
    assert!(
        small.index_maintenance_visits <= staged_inserts * 4 + 4 * n,
        "ceiling: index-maintenance visits touched too much work: {}",
        small.index_maintenance_visits
    );
    assert_eq!(
        small.index_maintenance_visits, large.index_maintenance_visits,
        "index-maintenance visits must be flat in committed edge-table size: {} vs {}",
        small.index_maintenance_visits, large.index_maintenance_visits
    );
    assert_eq!(
        small.staged_vs_staged_comparisons, large.staged_vs_staged_comparisons,
        "staged comparisons grew with committed edge-table size"
    );
    assert_eq!(
        small.rows_validated, large.rows_validated,
        "rows-validated grew with committed edge-table size"
    );
    assert_eq!(
        small.indexed_probes, large.indexed_probes,
        "indexed probes grew with committed edge-table size"
    );
    assert_eq!(
        small.scan_rows_touched, 0,
        "small edge-history commit must not scan"
    );
    assert_eq!(
        large.scan_rows_touched, 0,
        "large edge-history commit must not scan"
    );
}

#[test]
fn cwv_04_generality_plain_unique_fk() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER NOT NULL REFERENCES p(id), \
         label TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();

    const N: i64 = 40;
    let tx0 = db.begin().unwrap();
    for i in 0..N {
        db.insert_row(
            tx0,
            "p",
            HashMap::from([
                ("id".to_string(), Value::Int64(i)),
                ("name".to_string(), Value::Text(format!("p-{i}"))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx0).unwrap();

    db.__reset_commit_stage_stats();
    let tx = db.begin().unwrap();
    for i in 0..N {
        db.insert_row(
            tx,
            "c",
            HashMap::from([
                ("id".to_string(), Value::Int64(i)),
                ("p_id".to_string(), Value::Int64(i)),
                ("label".to_string(), Value::Text(format!("c-{i}"))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let s = db.__commit_stage_stats();

    let n = N as u64;
    assert!(
        s.staged_vs_staged_comparisons >= n - 1,
        "floor: real UNIQUE staged comparison work, got {}",
        s.staged_vs_staged_comparisons
    );
    assert!(
        s.staged_vs_staged_comparisons <= 12 * n,
        "ceiling: linear in staged inserts, got {}",
        s.staged_vs_staged_comparisons
    );
    assert!(
        s.index_maintenance_visits >= n,
        "floor: apply happened, got {}",
        s.index_maintenance_visits
    );
    assert!(
        s.indexed_probes >= n,
        "floor: indexed FK/UNIQUE probes happened, got {}",
        s.indexed_probes
    );
    assert_eq!(
        s.scan_rows_touched, 0,
        "plain UNIQUE+FK commit must not full-scan"
    );

    assert_eq!(
        db.execute("SELECT id FROM c", &empty()).unwrap().rows.len(),
        N as usize
    );
}

#[test]
fn cwv_04b_single_column_fk_parent_delete_uses_child_index() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER NOT NULL REFERENCES p(id))",
        &empty(),
    )
    .unwrap();

    const N: i64 = 400;
    let tx = db.begin().unwrap();
    for i in 0..=N {
        db.insert_row(
            tx,
            "p",
            HashMap::from([("id".to_string(), Value::Int64(i))]),
        )
        .unwrap();
    }
    for i in 0..N {
        db.insert_row(
            tx,
            "c",
            HashMap::from([
                ("id".to_string(), Value::Int64(i)),
                ("p_id".to_string(), Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_commit_stage_stats();
    let deleted = db
        .execute(
            "DELETE FROM p WHERE id = $id",
            &params(vec![("id", Value::Int64(N))]),
        )
        .unwrap();
    assert_eq!(deleted.rows_affected, 1);

    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 1,
        "reverse FK validation should probe the child FK index, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "deleting an unreferenced parent must not scan committed child history"
    );
}

#[test]
fn cwv_04c_single_column_fk_name_collision_keeps_both_child_indexes() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE c (d INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TABLE b_to_c (d INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, \
         a_to_b INTEGER NOT NULL REFERENCES c(d), \
         a INTEGER NOT NULL REFERENCES b_to_c(d))",
        &empty(),
    )
    .unwrap();

    const N: i64 = 400;
    let tx = db.begin().unwrap();
    for i in 0..=N {
        db.insert_row(tx, "c", HashMap::from([("d".to_string(), Value::Int64(i))]))
            .unwrap();
        db.insert_row(
            tx,
            "b_to_c",
            HashMap::from([("d".to_string(), Value::Int64(i))]),
        )
        .unwrap();
    }
    for i in 0..N {
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Int64(i)),
                ("a_to_b".to_string(), Value::Int64(i)),
                ("a".to_string(), Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_commit_stage_stats();
    let deleted = db
        .execute(
            "DELETE FROM b_to_c WHERE d = $d",
            &params(vec![("d", Value::Int64(N))]),
        )
        .unwrap();
    assert_eq!(deleted.rows_affected, 1);

    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 1,
        "colliding single-column FK names must still leave a child index, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "single-column FK name collision must not force a committed child scan"
    );
}

#[test]
fn cwv_04d_composite_fk_name_collision_keeps_both_child_indexes() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parent_x (d INTEGER NOT NULL, e INTEGER NOT NULL, UNIQUE (d, e))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, \
         a_b INTEGER NOT NULL, c INTEGER NOT NULL, \
         a INTEGER NOT NULL, b_c INTEGER NOT NULL, \
         FOREIGN KEY (a_b, c) REFERENCES parent_x(d, e), \
         FOREIGN KEY (a, b_c) REFERENCES parent_x(d, e))",
        &empty(),
    )
    .unwrap();

    const N: i64 = 400;
    let tx = db.begin().unwrap();
    for i in 0..=N {
        db.insert_row(
            tx,
            "parent_x",
            HashMap::from([
                ("d".to_string(), Value::Int64(i)),
                ("e".to_string(), Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    for i in 0..N {
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Int64(i)),
                ("a_b".to_string(), Value::Int64(i)),
                ("c".to_string(), Value::Int64(i)),
                ("a".to_string(), Value::Int64(i)),
                ("b_c".to_string(), Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();

    db.__reset_commit_stage_stats();
    let deleted = db
        .execute(
            "DELETE FROM parent_x WHERE d = $d AND e = $e",
            &params(vec![("d", Value::Int64(N)), ("e", Value::Int64(N))]),
        )
        .unwrap();
    assert_eq!(deleted.rows_affected, 1);

    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 2,
        "colliding composite FK names must still leave both child indexes, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "composite FK name collision must not force a committed child scan"
    );
}

#[test]
fn cwv_04e_single_column_fk_rejects_unindexable_scan_shapes() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty())
        .unwrap();

    let child_err = db
        .execute(
            "CREATE TABLE child_json (id INTEGER PRIMARY KEY, p_doc JSON REFERENCES p(id))",
            &empty(),
        )
        .expect_err("non-indexable child FK columns must be rejected at DDL time");
    match child_err {
        Error::ColumnNotIndexable { table, column, .. } => {
            assert_eq!(table, "child_json");
            assert_eq!(column, "p_doc");
        }
        other => panic!("expected child ColumnNotIndexable, got {other:?}"),
    }

    db.execute("CREATE TABLE loose_parent (id INTEGER)", &empty())
        .unwrap();
    let loose_parent_err = db
        .execute(
            "CREATE TABLE child_loose (id INTEGER PRIMARY KEY, p_id INTEGER REFERENCES loose_parent(id))",
            &empty(),
        )
        .expect_err("FK parent column must be PRIMARY KEY or UNIQUE covered");
    match loose_parent_err {
        Error::SchemaInvalid { reason } => assert!(
            reason.contains("without a PRIMARY KEY or UNIQUE constraint"),
            "unexpected parent-key error: {reason}"
        ),
        other => panic!("expected SchemaInvalid for unkeyed FK parent, got {other:?}"),
    }

    db.execute(
        "CREATE TABLE alter_child (id INTEGER PRIMARY KEY)",
        &empty(),
    )
    .unwrap();
    let alter_err = db
        .execute(
            "ALTER TABLE alter_child ADD COLUMN p_doc JSON REFERENCES p(id)",
            &empty(),
        )
        .expect_err("ALTER ADD COLUMN must not introduce a non-indexable FK");
    match alter_err {
        Error::ColumnNotIndexable { table, column, .. } => {
            assert_eq!(table, "alter_child");
            assert_eq!(column, "p_doc");
        }
        other => panic!("expected ALTER child ColumnNotIndexable, got {other:?}"),
    }
}

#[test]
fn cwv_04k_exact_constraints_reject_real_and_scan_only_unique_shapes() {
    let db = Database::open_memory();

    let err = db
        .execute("CREATE TABLE real_pk (id REAL PRIMARY KEY)", &empty())
        .expect_err("REAL primary keys must not rely on ordered index equality");
    assert_column_not_indexable(err, "real_pk", "id");

    let err = db
        .execute(
            "CREATE TABLE real_uq (id INTEGER PRIMARY KEY, f REAL UNIQUE)",
            &empty(),
        )
        .expect_err("REAL UNIQUE columns must not rely on ordered index equality");
    assert_column_not_indexable(err, "real_uq", "f");

    let err = db
        .execute(
            "CREATE TABLE json_uq (id INTEGER PRIMARY KEY, doc JSON UNIQUE)",
            &empty(),
        )
        .expect_err("JSON UNIQUE columns must be rejected instead of scan-backed");
    assert_column_not_indexable(err, "json_uq", "doc");

    let err = db
        .execute(
            "CREATE TABLE real_composite (id INTEGER PRIMARY KEY, f REAL, tag TEXT, UNIQUE (f, tag))",
            &empty(),
        )
        .expect_err("composite UNIQUE must reject non-exact key columns");
    assert_column_not_indexable(err, "real_composite", "f");

    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    let err = db
        .execute(
            "CREATE TABLE real_child (id INTEGER PRIMARY KEY, p_id REAL REFERENCES p(id))",
            &empty(),
        )
        .expect_err("REAL child FK columns must not rely on ordered index equality");
    assert_column_not_indexable(err, "real_child", "p_id");

    db.execute("CREATE TABLE alter_real (id INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    let err = db
        .execute("ALTER TABLE alter_real ADD COLUMN f REAL UNIQUE", &empty())
        .expect_err("ALTER ADD COLUMN must reject scan-backed exact constraints");
    assert_column_not_indexable(err, "alter_real", "f");

    db.execute(
        "CREATE TABLE real_plain (id INTEGER PRIMARY KEY, score REAL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX real_plain_score ON real_plain (score)",
        &empty(),
    )
    .expect("ordinary REAL indexes remain available for query planning");
}

#[test]
fn cwv_04f_alter_parent_fk_target_rejects_invalidating_ddl() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT REFERENCES p(code))",
        &empty(),
    )
    .unwrap();

    let drop_table_err = db
        .execute("DROP TABLE p", &empty())
        .expect_err("dropping a referenced parent table must be rejected");
    match drop_table_err {
        Error::TableNotFound(table) => assert_eq!(table, "p"),
        other => panic!("expected parent TableNotFound, got {other:?}"),
    }

    let drop_col_err = db
        .execute("ALTER TABLE p DROP COLUMN code CASCADE", &empty())
        .expect_err("dropping a referenced parent column must be rejected");
    match drop_col_err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "p");
            assert_eq!(column, "code");
        }
        other => panic!("expected parent ColumnNotFound, got {other:?}"),
    }

    let rename_col_err = db
        .execute("ALTER TABLE p RENAME COLUMN code TO new_code", &empty())
        .expect_err("renaming a referenced parent column must be rejected");
    match rename_col_err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "p");
            assert_eq!(column, "code");
        }
        other => panic!("expected parent ColumnNotFound, got {other:?}"),
    }

    db.execute("INSERT INTO p (id, code) VALUES (1, 'kept')", &empty())
        .unwrap();
    db.execute("INSERT INTO c (id, p_code) VALUES (1, 'kept')", &empty())
        .unwrap();
}

#[test]
fn cwv_04g_alter_composite_fk_child_rejects_invalidating_ddl() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (a INTEGER NOT NULL, b INTEGER NOT NULL, UNIQUE (a, b))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, x INTEGER NOT NULL, y INTEGER NOT NULL, \
         FOREIGN KEY (x, y) REFERENCES p(a, b))",
        &empty(),
    )
    .unwrap();

    let drop_col_err = db
        .execute("ALTER TABLE c DROP COLUMN x CASCADE", &empty())
        .expect_err("dropping a composite FK child column must be rejected");
    match drop_col_err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "c");
            assert_eq!(column, "x");
        }
        other => panic!("expected child ColumnNotFound, got {other:?}"),
    }

    let rename_col_err = db
        .execute("ALTER TABLE c RENAME COLUMN x TO x2", &empty())
        .expect_err("renaming a composite FK child column must be rejected");
    match rename_col_err {
        Error::ColumnNotFound { table, column } => {
            assert_eq!(table, "c");
            assert_eq!(column, "x");
        }
        other => panic!("expected child ColumnNotFound, got {other:?}"),
    }

    db.execute("INSERT INTO p (a, b) VALUES (1, 2)", &empty())
        .unwrap();
    db.execute("INSERT INTO c (id, x, y) VALUES (1, 1, 2)", &empty())
        .unwrap();
}

#[test]
fn cwv_04h_single_column_fk_nulls_do_not_block_parent_delete_or_update() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_code TEXT REFERENCES p(code))",
        &empty(),
    )
    .unwrap();

    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "p",
        HashMap::from([
            ("id".to_string(), Value::Int64(1)),
            ("code".to_string(), Value::Null),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "p",
        HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("code".to_string(), Value::Null),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "c",
        HashMap::from([
            ("id".to_string(), Value::Int64(1)),
            ("p_code".to_string(), Value::Null),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();

    db.__reset_commit_stage_stats();
    assert_eq!(
        db.execute("DELETE FROM p WHERE id = 1", &empty())
            .unwrap()
            .rows_affected,
        1
    );
    assert_eq!(
        db.execute("UPDATE p SET code = 'assigned' WHERE id = 2", &empty())
            .unwrap()
            .rows_affected,
        1
    );
    assert_eq!(
        db.__commit_stage_stats().scan_rows_touched,
        0,
        "NULL parent FK keys must not trigger committed child scans"
    );
}

#[test]
fn cwv_04i_unique_auto_index_name_collision_keeps_fk_parent_probe_correct() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, a_b TEXT UNIQUE, \
         a TEXT NOT NULL, b TEXT NOT NULL, UNIQUE (a, b))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_key TEXT REFERENCES p(a_b))",
        &empty(),
    )
    .unwrap();

    let meta = db.table_meta("p").unwrap();
    let single = meta
        .indexes
        .iter()
        .find(|index| index.columns == vec![("a_b".to_string(), SortDirection::Asc)])
        .expect("single-column UNIQUE auto index must exist");
    let composite = meta
        .indexes
        .iter()
        .find(|index| {
            index.columns
                == vec![
                    ("a".to_string(), SortDirection::Asc),
                    ("b".to_string(), SortDirection::Asc),
                ]
        })
        .expect("composite UNIQUE auto index must exist");
    assert_ne!(
        single.name, composite.name,
        "ambiguous UNIQUE auto indexes must not share storage names"
    );

    db.execute(
        "INSERT INTO p (id, a_b, a, b) VALUES (1, 'key-1', 'a-1', 'b-1')",
        &empty(),
    )
    .unwrap();
    db.__reset_commit_stage_stats();
    db.execute("INSERT INTO c (id, p_key) VALUES (1, 'key-1')", &empty())
        .unwrap();

    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 1,
        "child FK insert should use the a_b parent auto index, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "ambiguous UNIQUE names must not force a committed parent scan"
    );
}

#[test]
fn cwv_04j_alter_add_unique_collision_rebuilds_moved_auto_index_storage() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL, UNIQUE (a, b))",
        &empty(),
    )
    .unwrap();
    db.execute("ALTER TABLE p ADD COLUMN a_b TEXT UNIQUE", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_key TEXT REFERENCES p(a_b))",
        &empty(),
    )
    .unwrap();

    let meta = db.table_meta("p").unwrap();
    let single = meta
        .indexes
        .iter()
        .find(|index| index.columns == vec![("a_b".to_string(), SortDirection::Asc)])
        .expect("single-column UNIQUE auto index must exist after ALTER");
    let composite = meta
        .indexes
        .iter()
        .find(|index| {
            index.columns
                == vec![
                    ("a".to_string(), SortDirection::Asc),
                    ("b".to_string(), SortDirection::Asc),
                ]
        })
        .expect("composite UNIQUE auto index must survive ALTER");
    assert_ne!(
        single.name, composite.name,
        "ALTER-added UNIQUE column must not share the composite auto-index name"
    );

    db.execute(
        "INSERT INTO p (id, a, b, a_b) VALUES (1, 'a-1', 'b-1', 'key-1')",
        &empty(),
    )
    .unwrap();
    db.__reset_commit_stage_stats();
    db.execute("INSERT INTO c (id, p_key) VALUES (1, 'key-1')", &empty())
        .unwrap();

    let stats = db.__commit_stage_stats();
    assert!(
        stats.indexed_probes >= 1,
        "ALTER-added UNIQUE storage should back the FK parent probe, got {}",
        stats.indexed_probes
    );
    assert_eq!(
        stats.scan_rows_touched, 0,
        "ALTER-added UNIQUE name collision must not force a committed parent scan"
    );
}

#[test]
fn cwv_05_trigger_staged_counter_parity() {
    fn child_insert(db: &Database, tx: contextdb_core::TxId, i: i64) {
        db.insert_row(
            tx,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                (
                    "c1".to_string(),
                    Value::Uuid(Uuid::from_u128(i as u128 + 1)),
                ),
                ("c2".to_string(), Value::Int64(i)),
                ("tag".to_string(), Value::Text(format!("t-{i}"))),
            ]),
        )
        .unwrap();
    }

    const N: i64 = 15;

    let plain = Database::open_memory();
    plain
        .execute(
            "CREATE TABLE parent (a UUID NOT NULL, b INTEGER NOT NULL, UNIQUE (a, b))",
            &empty(),
        )
        .unwrap();
    plain
        .execute(
            "CREATE TABLE child (id UUID PRIMARY KEY, c1 UUID NOT NULL, c2 INTEGER NOT NULL, \
             tag TEXT NOT NULL, UNIQUE (c1, c2, tag), FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
            &empty(),
        )
        .unwrap();
    plain
        .execute("CREATE TABLE fire (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    let seed = plain.begin().unwrap();
    for i in 0..N {
        plain
            .insert_row(
                seed,
                "parent",
                HashMap::from([
                    ("a".to_string(), Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                    ("b".to_string(), Value::Int64(i)),
                ]),
            )
            .unwrap();
    }
    plain.commit(seed).unwrap();

    plain.__reset_commit_stage_stats();
    let tx = plain.begin().unwrap();
    plain
        .insert_row(
            tx,
            "fire",
            HashMap::from([("id".to_string(), Value::Uuid(Uuid::new_v4()))]),
        )
        .unwrap();
    for i in 0..N {
        child_insert(&plain, tx, i);
    }
    plain.commit(tx).unwrap();
    let plain_stats = plain.__commit_stage_stats();

    let trig = Database::open_memory();
    trig.execute(
        "CREATE TABLE parent (a UUID NOT NULL, b INTEGER NOT NULL, UNIQUE (a, b))",
        &empty(),
    )
    .unwrap();
    trig.execute(
        "CREATE TABLE child (id UUID PRIMARY KEY, c1 UUID NOT NULL, c2 INTEGER NOT NULL, \
         tag TEXT NOT NULL, UNIQUE (c1, c2, tag), FOREIGN KEY (c1, c2) REFERENCES parent(a, b))",
        &empty(),
    )
    .unwrap();
    trig.execute("CREATE TABLE fire (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    trig.execute("CREATE TRIGGER tr ON fire WHEN INSERT", &empty())
        .unwrap();
    trig.register_trigger_callback("tr", move |db_handle, ctx| {
        for i in 0..N {
            db_handle.insert_row(
                ctx.tx,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    (
                        "c1".to_string(),
                        Value::Uuid(Uuid::from_u128(i as u128 + 1)),
                    ),
                    ("c2".to_string(), Value::Int64(i)),
                    ("tag".to_string(), Value::Text(format!("t-{i}"))),
                ]),
            )?;
        }
        Ok(())
    })
    .unwrap();
    trig.complete_initialization().unwrap();
    let seed2 = trig.begin().unwrap();
    for i in 0..N {
        trig.insert_row(
            seed2,
            "parent",
            HashMap::from([
                ("a".to_string(), Value::Uuid(Uuid::from_u128(i as u128 + 1))),
                ("b".to_string(), Value::Int64(i)),
            ]),
        )
        .unwrap();
    }
    trig.commit(seed2).unwrap();

    trig.__reset_commit_stage_stats();
    trig.execute(
        "INSERT INTO fire (id) VALUES ($id)",
        &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
    )
    .unwrap();
    let trig_stats = trig.__commit_stage_stats();

    let n = N as u64;
    let staged_inserts = n + 1;
    assert!(plain_stats.staged_vs_staged_comparisons >= n - 1);
    assert!(trig_stats.staged_vs_staged_comparisons >= n - 1);
    assert!(
        plain_stats.staged_vs_staged_comparisons <= 12 * staged_inserts,
        "plain trigger-parity fixture exceeded linear comparison ceiling: {}",
        plain_stats.staged_vs_staged_comparisons
    );
    assert!(
        trig_stats.staged_vs_staged_comparisons <= 12 * staged_inserts,
        "trigger fixture exceeded linear comparison ceiling: {}",
        trig_stats.staged_vs_staged_comparisons
    );

    assert_eq!(
        trig_stats.staged_vs_staged_comparisons, plain_stats.staged_vs_staged_comparisons,
        "trigger-staged comparison work diverged from plain staging"
    );
    assert_eq!(
        trig_stats.rows_validated, plain_stats.rows_validated,
        "trigger-staged rows-validated diverged from plain staging"
    );
    assert_eq!(
        trig_stats.indexed_probes, plain_stats.indexed_probes,
        "trigger-staged indexed probes diverged from plain staging"
    );
    assert!(
        plain_stats.index_maintenance_visits >= staged_inserts,
        "plain fixture did not count index maintenance"
    );
    assert!(
        trig_stats.index_maintenance_visits >= staged_inserts,
        "trigger fixture did not count index maintenance"
    );
    assert_eq!(
        trig_stats.index_maintenance_visits, plain_stats.index_maintenance_visits,
        "trigger-staged index maintenance diverged from plain staging"
    );
    assert_eq!(trig_stats.scan_rows_touched, 0);
    assert_eq!(plain_stats.scan_rows_touched, 0);

    assert_eq!(
        plain
            .execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        N as usize
    );
    assert_eq!(
        trig.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        N as usize
    );
}

#[test]
fn cwv_06_unique_rejection_wins_over_fk() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INTEGER NOT NULL REFERENCES p(id), \
         label TEXT NOT NULL UNIQUE)",
        &empty(),
    )
    .unwrap();
    db.execute("INSERT INTO p (id) VALUES (1)", &empty())
        .unwrap();
    db.execute(
        "INSERT INTO c (id, p_id, label) VALUES (1, 1, 'dup')",
        &empty(),
    )
    .unwrap();

    let tx = db.begin().unwrap();
    let err = db
        .insert_row(
            tx,
            "c",
            HashMap::from([
                ("id".to_string(), Value::Int64(2)),
                ("p_id".to_string(), Value::Int64(999)),
                ("label".to_string(), Value::Text("dup".to_string())),
            ]),
        )
        .expect_err("must reject");
    db.rollback(tx).unwrap();
    assert!(
        matches!(err, Error::UniqueViolation { .. }),
        "UNIQUE must surface before FK; got {err:?}"
    );
    let rows = db.execute("SELECT id FROM c", &empty()).unwrap();
    assert_eq!(rows.rows.len(), 1, "only the original child survives");
}

#[test]
fn cwv_07_state_machine_illegal_transition_rejected() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);
    let (a, b) = seed_parent(&db, 0);
    db.execute(
        "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
        &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
    )
    .unwrap();

    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "child",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("c1".to_string(), Value::Uuid(a)),
            ("c2".to_string(), Value::Int64(b)),
            ("tag".to_string(), Value::Text("t".to_string())),
        ]),
    )
    .unwrap();
    let illegal = db.execute_in_tx(
        tx,
        "UPDATE parent SET status = 'active' WHERE a = $a AND b = $b",
        &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
    );
    match illegal {
        Ok(_) => {
            let err = db
                .commit(tx)
                .expect_err("illegal transition must reject at commit");
            assert!(
                matches!(err, Error::InvalidStateTransition(_)),
                "got {err:?}"
            );
        }
        Err(err) => {
            assert!(
                matches!(err, Error::InvalidStateTransition(_)),
                "got {err:?}"
            );
            db.rollback(tx).unwrap();
        }
    }
    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        0,
        "no child from the rejected tx may survive"
    );
    let parent = db
        .execute(
            "SELECT status FROM parent WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
        )
        .unwrap();
    assert_eq!(parent.rows.len(), 1, "the exact parent row must survive");
    assert_eq!(
        parent.rows[0][0],
        Value::Text("invalidated".to_string()),
        "parent must remain at the exact pre-tx status after rollback"
    );
}

#[test]
fn cwv_07b_upsert_illegal_state_transition_rejected() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE jobs (id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued') \
         STATE MACHINE (status: queued -> [running], running -> [done])",
        &empty(),
    )
    .unwrap();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO jobs (id, status) VALUES ($id, 'queued')",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();

    let outcome = db.execute(
        "INSERT INTO jobs (id, status) VALUES ($id, 'queued') ON CONFLICT (id) DO UPDATE SET status = 'done'",
        &params(vec![("id", Value::Uuid(id))]),
    );
    assert!(
        matches!(outcome, Err(Error::InvalidStateTransition(_))),
        "illegal upsert transition must reject; got {outcome:?}"
    );
    let row = db
        .execute(
            "SELECT status FROM jobs WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(
        row.rows[0][0],
        Value::Text("queued".to_string()),
        "conflict row must keep its prior value after a rejected upsert"
    );

    let ok = db
        .execute(
            "INSERT INTO jobs (id, status) VALUES ($id, 'queued') ON CONFLICT (id) DO UPDATE SET status = 'running'",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .expect("legal upsert transition must succeed");
    assert_eq!(ok.rows_affected, 1);
    let row = db
        .execute(
            "SELECT status FROM jobs WHERE id = $id",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(
        row.rows[0][0],
        Value::Text("running".to_string()),
        "legal upsert hit must apply the post-image, not report success as a no-op"
    );
}

#[test]
fn cwv_08_conditional_stale_update_downgrades_to_noop() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);
    let (a, b) = seed_parent(&db, 0);

    let stale_tx = db.begin().unwrap();
    let staged = db
        .execute_in_tx(
            stale_tx,
            "UPDATE parent SET status = 'superseded' WHERE a = $a AND b = $b AND status = 'active'",
            &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
        )
        .unwrap();
    assert_eq!(
        staged.rows_affected, 1,
        "positive case: the stale writer matched at execution time"
    );

    let winner = db
        .execute(
            "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b AND status = 'active'",
            &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
        )
        .unwrap();
    assert_eq!(
        winner.rows_affected, 1,
        "positive case: concurrent winner updates the committed row"
    );

    db.commit(stale_tx)
        .expect("stale guarded update must commit as a no-op, not error");

    let row = db
        .execute(
            "SELECT status FROM parent WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(a)), ("b", Value::Int64(b))]),
        )
        .unwrap();
    assert_eq!(row.rows[0][0], Value::Text("invalidated".to_string()));
}

#[test]
fn cwv_09_composite_unique_duplicate_is_silent_noop() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);
    let (a, b) = seed_parent(&db, 0);

    let first = db
        .execute(
            "INSERT INTO child (id, c1, c2, tag) VALUES ($id, $c1, $c2, 'dup')",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("c1", Value::Uuid(a)),
                ("c2", Value::Int64(b)),
            ]),
        )
        .unwrap();
    assert_eq!(first.rows_affected, 1, "positive: first tuple inserts");

    let dup = db
        .execute(
            "INSERT INTO child (id, c1, c2, tag) VALUES ($id, $c1, $c2, 'dup')",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("c1", Value::Uuid(a)),
                ("c2", Value::Int64(b)),
            ]),
        )
        .expect("duplicate composite-UNIQUE tuple must be Ok, not Err");
    assert_eq!(dup.rows_affected, 0, "duplicate composite tuple is a no-op");

    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        1,
        "no second row for the duplicate tuple"
    );
}

#[test]
fn cwv_10_midset_violation_all_or_nothing() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);
    let (a, b) = seed_parent(&db, 0);

    let tx = db.begin().unwrap();
    db.insert_row(
        tx,
        "child",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("c1".to_string(), Value::Uuid(a)),
            ("c2".to_string(), Value::Int64(b)),
            ("tag".to_string(), Value::Text("ok".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "edges",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("source_id".to_string(), Value::Uuid(a)),
            ("target_id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("edge_type".to_string(), Value::Text("e".to_string())),
        ]),
    )
    .unwrap();
    let staged = db.insert_row(
        tx,
        "child",
        HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::new_v4())),
            ("c1".to_string(), Value::Uuid(Uuid::new_v4())),
            ("c2".to_string(), Value::Int64(999)),
            ("tag".to_string(), Value::Text("bad".to_string())),
        ]),
    );
    let err = match staged {
        Ok(_) => db
            .commit(tx)
            .expect_err("composite-FK violation must reject the tx"),
        Err(err) => {
            db.rollback(tx).unwrap();
            err
        }
    };
    assert!(
        matches!(err, Error::ForeignKeyViolation { .. }),
        "composite-FK violation must keep its typed error; got {err:?}"
    );

    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        0,
        "no child from the rolled-back tx survives (not even the good one)"
    );
    assert_eq!(
        db.execute("SELECT id FROM edges", &empty())
            .unwrap()
            .rows
            .len(),
        0,
        "no edge from the rolled-back tx survives"
    );
}

#[test]
fn cwv_11_upsert_transformation_path_bounded() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);

    const N: usize = 30;
    const K: usize = 10;
    let mut parents = Vec::with_capacity(N);
    for b in 0..N as i64 {
        parents.push(seed_parent(&db, b));
    }

    let pre = db.begin().unwrap();
    for (a, b) in parents.iter().take(K) {
        db.insert_row(
            pre,
            "child",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("c1".to_string(), Value::Uuid(*a)),
                ("c2".to_string(), Value::Int64(*b)),
                ("tag".to_string(), Value::Text(format!("tag-{b}"))),
            ]),
        )
        .unwrap();
    }
    db.commit(pre).unwrap();

    db.__reset_commit_stage_stats();
    let tx = db.begin().unwrap();
    for (i, (a, b)) in parents.iter().enumerate() {
        db.execute_in_tx(
            tx,
            "UPDATE parent SET status = 'invalidated' WHERE a = $a AND b = $b",
            &params(vec![("a", Value::Uuid(*a)), ("b", Value::Int64(*b))]),
        )
        .unwrap();
        if i < K {
            db.execute_in_tx(
                tx,
                "INSERT INTO child (id, c1, c2, tag) VALUES ($id, $c1, $c2, $tag) \
                 ON CONFLICT (c1, c2, tag) DO UPDATE SET tag = $new_tag",
                &params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("c1", Value::Uuid(*a)),
                    ("c2", Value::Int64(*b)),
                    ("tag", Value::Text(format!("tag-{b}"))),
                    ("new_tag", Value::Text(format!("updated-{b}"))),
                ]),
            )
            .unwrap();
        } else {
            db.insert_row(
                tx,
                "child",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                    ("c1".to_string(), Value::Uuid(*a)),
                    ("c2".to_string(), Value::Int64(*b)),
                    ("tag".to_string(), Value::Text(format!("tag-{b}"))),
                ]),
            )
            .unwrap();
        }
        db.insert_row(
            tx,
            "edges",
            HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("source_id".to_string(), Value::Uuid(*a)),
                ("target_id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("edge_type".to_string(), Value::Text("cascades".to_string())),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let s = db.__commit_stage_stats();

    let staged_inserts = (2 * N) as u64;
    let n = N as u64;
    let upsert_hits = K as u64;

    assert!(
        s.staged_vs_staged_comparisons >= n - 1,
        "floor: real staged comparison work under upsert transforms, got {}",
        s.staged_vs_staged_comparisons
    );
    assert!(
        s.staged_vs_staged_comparisons <= 12 * staged_inserts,
        "comparisons {} exceed linear ceiling {} - validator restarts on upsert transform",
        s.staged_vs_staged_comparisons,
        12 * staged_inserts
    );
    assert_eq!(
        s.scan_rows_touched, 0,
        "upsert-bearing commit must not full-scan"
    );
    assert!(
        s.indexed_probes >= staged_inserts + upsert_hits,
        "upsert-bearing commit did not count indexed probes for inserts plus hits: {}",
        s.indexed_probes
    );
    assert!(
        s.indexed_probes <= 12 * staged_inserts,
        "upsert-bearing indexed probes exceeded linear ceiling: {}",
        s.indexed_probes
    );
    assert!(
        s.index_maintenance_visits >= staged_inserts,
        "upsert-bearing commit did not count index maintenance: {}",
        s.index_maintenance_visits
    );
    assert!(
        s.index_maintenance_visits <= staged_inserts * 4 + 4 * n,
        "upsert-bearing index maintenance exceeded written-row/index bound: {}",
        s.index_maintenance_visits
    );

    assert_eq!(
        db.execute("SELECT id FROM child", &empty())
            .unwrap()
            .rows
            .len(),
        N,
        "K upsert hits update in place; remaining inserts add rows - N child rows total"
    );
    for (_a, b) in parents.iter().take(K) {
        assert_eq!(
            db.execute(
                "SELECT id FROM child WHERE tag = $tag",
                &params(vec![("tag", Value::Text(format!("updated-{b}")))]),
            )
            .unwrap()
            .rows
            .len(),
            1,
            "upsert hit must update the conflict row to its post-image"
        );
        assert_eq!(
            db.execute(
                "SELECT id FROM child WHERE tag = $tag",
                &params(vec![("tag", Value::Text(format!("tag-{b}")))]),
            )
            .unwrap()
            .rows
            .len(),
            0,
            "old conflict tuple must not remain visible after update"
        );
    }
}

#[test]
fn cwv_12_reset_and_accumulation_semantics() {
    let db = Database::open_memory();
    declare_mixed_schema(&db);

    const FIRST_N: usize = 12;
    const SECOND_N: usize = 18;
    let mut first = Vec::with_capacity(FIRST_N);
    for b in 0..FIRST_N as i64 {
        first.push(seed_parent(&db, b));
    }

    db.__reset_commit_stage_stats();
    stage_mixed_cascade(&db, &first);
    let s1 = db.__commit_stage_stats();
    let n = FIRST_N as u64;
    let staged_inserts = (2 * FIRST_N) as u64;
    assert!(
        s1.staged_vs_staged_comparisons >= n - 1,
        "floor (first commit), got {}",
        s1.staged_vs_staged_comparisons
    );
    assert!(
        s1.staged_vs_staged_comparisons <= 12 * staged_inserts,
        "first commit exceeded linear comparison ceiling: {}",
        s1.staged_vs_staged_comparisons
    );
    assert!(
        s1.rows_validated >= n,
        "first commit did not count validated rows"
    );
    assert!(
        s1.indexed_probes >= staged_inserts,
        "first commit did not count indexed probes"
    );
    assert!(
        s1.index_maintenance_visits >= staged_inserts,
        "first commit did not count index maintenance"
    );
    assert_eq!(s1.scan_rows_touched, 0, "first commit must not full-scan");

    db.__reset_commit_stage_stats();
    let z = db.__commit_stage_stats();
    assert_eq!(
        z,
        contextdb_engine::CommitStageStats::default(),
        "reset must zero every counter"
    );

    let mut second = Vec::with_capacity(SECOND_N);
    for b in 100..100 + SECOND_N as i64 {
        second.push(seed_parent(&db, b));
    }
    db.__reset_commit_stage_stats();
    stage_mixed_cascade(&db, &second);
    let s2 = db.__commit_stage_stats();
    let second_n = SECOND_N as u64;
    let second_staged_inserts = (2 * SECOND_N) as u64;
    assert!(
        s2.staged_vs_staged_comparisons >= second_n - 1,
        "floor (second commit) - accumulation restarts from zero, got {}",
        s2.staged_vs_staged_comparisons
    );
    assert!(
        s2.staged_vs_staged_comparisons <= 12 * second_staged_inserts,
        "second commit exceeded linear comparison ceiling after reset: {}",
        s2.staged_vs_staged_comparisons
    );
    assert!(
        s2.rows_validated >= second_n,
        "rows_validated did not resume after reset"
    );
    assert!(
        s2.indexed_probes >= second_staged_inserts,
        "indexed_probes did not resume after reset"
    );
    assert!(
        s2.index_maintenance_visits >= second_staged_inserts,
        "index_maintenance_visits did not resume after reset"
    );
    assert!(
        s2.staged_vs_staged_comparisons > s1.staged_vs_staged_comparisons,
        "post-reset comparison counter did not reflect larger second commit: {} vs {}",
        s1.staged_vs_staged_comparisons,
        s2.staged_vs_staged_comparisons
    );
    assert!(
        s2.rows_validated > s1.rows_validated,
        "post-reset rows_validated did not reflect larger second commit: {} vs {}",
        s1.rows_validated,
        s2.rows_validated
    );
    assert!(
        s2.indexed_probes > s1.indexed_probes,
        "post-reset indexed_probes did not reflect larger second commit: {} vs {}",
        s1.indexed_probes,
        s2.indexed_probes
    );
    assert!(
        s2.index_maintenance_visits > s1.index_maintenance_visits,
        "post-reset index_maintenance_visits did not reflect larger second commit: {} vs {}",
        s1.index_maintenance_visits,
        s2.index_maintenance_visits
    );
    assert_eq!(s2.scan_rows_touched, 0, "second commit must not full-scan");
}
