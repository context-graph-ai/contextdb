//! An UPDATE or DELETE whose WHERE binds a full declared index prefix finds
//! its rows through that index, exactly as the same WHERE does in a SELECT.
//!
//! The shape is a consumer's passage table: a UUID key, a partitioned vector
//! column, three composite UNIQUE constraints and six composite indexes that
//! share the `(context_id, generation_id, is_current)` prefix. Setting one
//! BOOLEAN on one session's ten rows, or deleting them, must examine the same
//! number of entries whether the table holds 200 rows or 2,000: a write whose
//! cost grows with the table makes every new session dearer than the last.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const INDEXES: &[&str] = &[
    "CREATE INDEX p_generation_ordinal_idx ON p (generation_id, passage_ordinal)",
    "CREATE INDEX p_current_page ON p (generation_id, is_current, passage_ordinal, id)",
    "CREATE INDEX p_connector_idx ON p (context_id, generation_id, is_current, connector_id)",
    "CREATE INDEX p_session_idx ON p (context_id, generation_id, is_current, session_ref)",
    "CREATE INDEX p_source_idx ON p (context_id, generation_id, is_current, source_instance_ref)",
    "CREATE INDEX p_active_idx ON p (context_id, generation_id, is_current, is_searchable)",
];
const ROWS_PER_SESSION: i64 = 10;
const WHERE: &str = "WHERE context_id = $context_id AND generation_id = $generation_id \
     AND is_current = $current AND session_ref = $session AND connector_id = $connector_id";

fn context() -> Uuid {
    Uuid::from_u128(1)
}

fn generation() -> Uuid {
    Uuid::from_u128(2)
}

fn passages(rows: i64) -> (tempfile::TempDir, Database) {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = Database::open(root.path().join("passages.db")).expect("open the store");
    db.execute(
        "CREATE TABLE p (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
         generation_id UUID NOT NULL, connector_id TEXT NOT NULL, session_ref TEXT NOT NULL, \
         source_instance_ref TEXT NOT NULL, passage_ordinal INTEGER NOT NULL, \
         vector_text VECTOR(8) PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4096 \
         SEARCH_MODE AUTO, is_current BOOLEAN NOT NULL, \
         is_searchable BOOLEAN NOT NULL DEFAULT true, UNIQUE (context_id, id), \
         UNIQUE (context_id, generation_id, id), \
         UNIQUE (context_id, generation_id, connector_id, source_instance_ref, passage_ordinal)\
         ) HISTORY CURRENT ONLY SYNC OFF",
        &HashMap::new(),
    )
    .expect("declare the passage table");
    for ddl in INDEXES {
        db.execute(ddl, &HashMap::new())
            .expect("declare a passage index");
    }
    for chunk in (1..=rows).collect::<Vec<_>>().chunks(100) {
        let tx = db.begin().expect("begin a seed transaction");
        for ordinal in chunk {
            let session = format!("s{:04}", ordinal / ROWS_PER_SESSION);
            db.insert_row(
                tx,
                "p",
                HashMap::from([
                    (
                        "id".to_string(),
                        Value::Uuid(Uuid::from_u128(0x5eed << 64 | *ordinal as u128)),
                    ),
                    ("context_id".to_string(), Value::Uuid(context())),
                    ("generation_id".to_string(), Value::Uuid(generation())),
                    ("connector_id".to_string(), Value::Text("c".into())),
                    ("session_ref".to_string(), Value::Text(session.clone())),
                    (
                        "source_instance_ref".to_string(),
                        Value::Text(format!("{session}-src")),
                    ),
                    ("passage_ordinal".to_string(), Value::Int64(*ordinal)),
                    (
                        "vector_text".to_string(),
                        Value::Vector(
                            (0..8)
                                .map(|i| ((*ordinal * 7 + i) % 13) as f32 / 13.0)
                                .collect(),
                        ),
                    ),
                    ("is_current".to_string(), Value::Bool(true)),
                    ("is_searchable".to_string(), Value::Bool(false)),
                ]),
            )
            .expect("seed one passage");
        }
        db.commit(tx).expect("commit the seed chunk");
    }
    (root, db)
}

fn params() -> HashMap<String, Value> {
    HashMap::from([
        ("context_id".to_string(), Value::Uuid(context())),
        ("generation_id".to_string(), Value::Uuid(generation())),
        ("current".to_string(), Value::Bool(true)),
        ("session".to_string(), Value::Text("s0001".into())),
        ("connector_id".to_string(), Value::Text("c".into())),
        ("searchable".to_string(), Value::Bool(true)),
    ])
}

/// (examined, affected) for one statement run in its own transaction.
fn run(db: &Database, sql: &str) -> (u64, u64) {
    let tx = db.begin().expect("begin the measured transaction");
    let result = db
        .execute_in_tx(tx, sql, &params())
        .expect("run the measured write");
    db.commit(tx).expect("commit the measured write");
    eprintln!(
        "write_through_index sql={sql:?} plan={} index={:?} examined={} affected={} \
         considered={:?}",
        result.trace.physical_plan,
        result.trace.index_used,
        result.trace.rows_examined,
        result.rows_affected,
        result.trace.indexes_considered,
    );
    (result.trace.rows_examined, result.rows_affected)
}

fn examined_by_size(sql: &str) -> Vec<(i64, u64)> {
    [200, 2_000]
        .into_iter()
        .map(|rows| {
            let (_root, db) = passages(rows);
            let (examined, affected) = run(&db, sql);
            assert_eq!(
                affected, ROWS_PER_SESSION as u64,
                "{sql}: the write reaches exactly one session's rows"
            );
            (rows, examined)
        })
        .collect()
}

fn assert_flat(sql: &str) {
    let examined = examined_by_size(sql);
    assert_eq!(
        examined[0].1, examined[1].1,
        "{sql}: entries examined grew with the table ({examined:?}); the same WHERE in a \
         SELECT finds its rows through p_session_idx"
    );
}

#[test]
fn an_update_bound_through_a_full_index_prefix_examines_only_its_rows() {
    assert_flat(&format!("UPDATE p SET is_searchable = $searchable {WHERE}"));
}

#[test]
fn a_delete_bound_through_a_full_index_prefix_examines_only_its_rows() {
    assert_flat(&format!("DELETE FROM p {WHERE}"));
}

/// Two indexes bind all four equality columns of this WHERE. Every row shares
/// one connector, so `p_connector_idx` holds the whole context under the bound
/// prefix and `p_session_idx` holds ten rows. The planner chooses by which
/// bound range is smaller, never by which index was declared first.
#[test]
fn the_same_where_in_a_select_examines_only_its_rows() {
    let mut examined = Vec::new();
    for rows in [200, 2_000] {
        let (_root, db) = passages(rows);
        let result = db
            .execute(&format!("SELECT id FROM p {WHERE}"), &params())
            .expect("select one session");
        eprintln!(
            "write_through_index select rows={rows} plan={} index={:?} examined={}",
            result.trace.physical_plan, result.trace.index_used, result.trace.rows_examined
        );
        assert_eq!(result.rows.len(), ROWS_PER_SESSION as usize);
        assert_eq!(result.trace.index_used.as_deref(), Some("p_session_idx"));
        examined.push((rows, result.trace.rows_examined));
    }
    assert_eq!(
        examined[0].1, examined[1].1,
        "the SELECT's entries examined grew with the table: {examined:?}"
    );
}

/// The entries examined are entries on every route: the same WHERE reports the
/// same count whether it selects, updates or deletes the session's rows.
#[test]
fn the_same_where_examines_the_same_entries_on_every_route() {
    let (_root, db) = passages(200);
    let select = db
        .execute(&format!("SELECT id FROM p {WHERE}"), &params())
        .expect("select one session")
        .trace
        .rows_examined;
    let (update, _) = run(
        &db,
        &format!("UPDATE p SET is_searchable = $searchable {WHERE}"),
    );
    let (delete, _) = run(&db, &format!("DELETE FROM p {WHERE}"));
    assert_eq!(
        (update, delete),
        (select, select),
        "UPDATE and DELETE count entries examined as SELECT does (select={select})"
    );
}
