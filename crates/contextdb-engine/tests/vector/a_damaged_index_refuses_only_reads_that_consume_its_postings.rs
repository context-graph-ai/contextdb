//! A relational index whose physical table no longer matches the root its
//! writer published is damage. A read that consumes that index's physical
//! postings -- an ordered window, a batch probe, a writer cursor, a
//! maintained group -- refuses with `StoreCorrupted`. A read that asks only
//! the canonical tree, and so never reads a physical posting, still answers
//! and discloses the damaged index. Row-table damage and writes into the
//! damaged index still refuse.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    IndexPostingDamage, damage_relational_index_posting_for_test,
    damage_relational_row_table_for_test, misfile_relational_index_posting_for_test,
};
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const PARTITIONS: usize = 3;
const ROWS_PER_PARTITION: usize = 40;
const DIMENSIONS: usize = 8;
const LIMIT: usize = 5;
const INDEX: &str = "scoped_rows_active_idx";

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xD048_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xD048_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| (((ordinal * 31 + axis * 17) % 101) as f32 / 101.0) - 0.5)
        .collect()
}

fn row_id(partition: usize, local: usize) -> i64 {
    (partition * ROWS_PER_PARTITION + local) as i64 + 1
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("open the fixture store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, label TEXT NOT NULL, \
         embedding VECTOR(8) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned table");
    db.execute(
        "CREATE INDEX scoped_rows_active_idx ON scoped_rows (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the damaged index");
    db.execute(
        "CREATE INDEX scoped_rows_label_idx ON scoped_rows (label)",
        &HashMap::new(),
    )
    .expect("declare an undamaged sibling index");
    db.execute(
        "CREATE TABLE notes (id INT PRIMARY KEY, label TEXT NOT NULL)",
        &HashMap::new(),
    )
    .expect("declare an unrelated table");
    db.execute(
        "CREATE INDEX notes_label_idx ON notes (label)",
        &HashMap::new(),
    )
    .expect("declare an index on the unrelated table");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            db.execute(
                "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $label, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(row_id(partition, local))),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    ("label".into(), Value::Text(format!("label-{}", ordinal % 4))),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one row");
        }
    }
    for id in 0..8_i64 {
        db.execute(
            "INSERT INTO notes VALUES ($id, $label)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("label".into(), Value::Text(format!("note-{}", id % 2))),
            ]),
        )
        .expect("seed one note");
    }
    for _ in 0..256 {
        db.run_maintenance_cycle().expect("advance maintenance");
        let mut owner = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the observation reader"),
        );
        let ready = db
            .vector_store_for_test()
            .partition_infos(&VectorIndexRef::new("scoped_rows", "embedding"), &mut owner)
            .expect("inspect readiness");
        if ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available) {
            db.close().expect("close the sealed fixture");
            return;
        }
    }
    panic!("every partition publishes a maintained route");
}

fn reader(path: &std::path::Path) -> ReadSession {
    ReadSession::open_with_options(
        path,
        ReadSessionOptions {
            contexts: Some(
                (0..PARTITIONS)
                    .map(|partition| ContextId::new(context_id(partition)))
                    .collect::<BTreeSet<_>>(),
            ),
            limits: ReadLimits {
                work: 5_000_000,
                memory: 512 * 1024 * 1024,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
    )
    .expect("the reader opens over index damage")
}

fn ids(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
) -> contextdb_core::Result<Vec<i64>> {
    result.map(|result| {
        result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("one id column: {other:?}"),
            })
            .collect()
    })
}

/// The find whose candidates are proved by canonical descents only: it names
/// whole partition pairs and never reads a physical posting.
fn whole_pair(reader: &ReadSession, partition: usize) -> contextdb_core::Result<Vec<i64>> {
    ids(reader.execute(
        &format!(
            "SELECT id FROM scoped_rows WHERE context_id IN ($context) AND generation_id IN ($generation) \
             AND is_current = TRUE ORDER BY embedding <=> $query LIMIT {LIMIT}"
        ),
        &HashMap::from([
            ("context".into(), Value::Uuid(context_id(partition))),
            ("generation".into(), Value::Uuid(generation_id(partition))),
            ("query".into(), Value::Vector(embedding(partition * ROWS_PER_PARTITION))),
        ]),
    ))
}

/// The find that proves candidates through a window over the index's
/// physical postings.
fn window(
    reader: &ReadSession,
    partition: usize,
    limit: usize,
) -> contextdb_core::Result<Vec<i64>> {
    ids(reader.execute(
        &format!(
            "SELECT id FROM scoped_rows WHERE context_id = $context \
             ORDER BY embedding <=> $query LIMIT {limit}"
        ),
        &HashMap::from([
            ("context".into(), Value::Uuid(context_id(partition))),
            (
                "query".into(),
                Value::Vector(embedding(partition * ROWS_PER_PARTITION)),
            ),
        ]),
    ))
}

fn assert_corrupt(outcome: &contextdb_core::Result<Vec<i64>>, what: &str) {
    assert!(
        matches!(outcome, Err(Error::StoreCorrupted { .. })),
        "{what}: a read that consumes the damaged index's postings refuses, got {outcome:?}"
    );
}

fn assert_own_rows(outcome: contextdb_core::Result<Vec<i64>>, partition: usize, what: &str) {
    let rows = outcome.unwrap_or_else(|error| panic!("{what}: answers: {error:?}"));
    let own = row_id(partition, 0)..=row_id(partition, ROWS_PER_PARTITION - 1);
    assert!(
        rows.len() == LIMIT && rows.iter().all(|id| own.contains(id)),
        "{what}: answers only its own Context's rows: {rows:?}"
    );
}

fn damaged_store(
    name: &str,
    damage: impl FnOnce(&std::path::Path),
) -> (tempfile::TempDir, std::path::PathBuf) {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join(name);
    seed(&path);
    damage(&path);
    (root, path)
}

fn misfile(path: &std::path::Path) {
    // One row of Context 1 filed under Context 0's key.
    misfile_relational_index_posting_for_test(
        path,
        "scoped_rows",
        INDEX,
        contextdb_core::RowId(row_id(1, 0) as u64),
        contextdb_core::RowId(row_id(0, 0) as u64),
    )
    .expect("misfile one posting");
}

/// Rows 1 and 8: the moved posting. The whole-pair find answers through the
/// canonical tree without one strict index check (so without a physical
/// cursor), discloses the index, and the window find refuses.
#[test]
fn a_canonical_find_answers_over_a_moved_posting_and_a_window_refuses() {
    let (_root, path) = damaged_store("moved.db", misfile);
    let session = reader(&path);

    let strict_before = Database::__strict_index_root_checks_for_test();
    assert_own_rows(whole_pair(&session, 0), 0, "whole-pair find over Context 0");
    assert_eq!(
        Database::__strict_index_root_checks_for_test(),
        strict_before,
        "the whole-pair find makes no strict index check, so it opens no physical index cursor"
    );
    assert_eq!(
        Database::observed_index_root_damage(&path).expect("read the disclosure"),
        vec![("scoped_rows".to_owned(), INDEX.to_owned())],
        "the canonical read discloses the index it tolerated"
    );
    assert_corrupt(&window(&session, 0, LIMIT), "window find over Context 0");
}

/// Row 3: missing and hidden postings. Canonical-only reads answer; reads over
/// the physical postings refuse.
#[test]
fn missing_and_hidden_postings_refuse_every_physical_read() {
    for damage in [IndexPostingDamage::Remove, IndexPostingDamage::Hide] {
        let (_root, path) = damaged_store("missing.db", |path| {
            damage_relational_index_posting_for_test(
                path,
                "scoped_rows",
                INDEX,
                contextdb_core::RowId(row_id(0, 3) as u64),
                damage,
            )
            .expect("damage one posting");
        });
        let session = reader(&path);
        assert_own_rows(
            whole_pair(&session, 0),
            0,
            &format!("{damage:?}: whole-pair find"),
        );
        assert_corrupt(
            &window(&session, 0, LIMIT),
            &format!("{damage:?}: window find"),
        );
        assert_corrupt(
            &ids(session.execute(
                "SELECT id FROM scoped_rows WHERE context_id = $context AND generation_id = $generation \
                 AND is_current = TRUE",
                &HashMap::from([
                    ("context".into(), Value::Uuid(context_id(0))),
                    ("generation".into(), Value::Uuid(generation_id(0))),
                ]),
            )),
            &format!("{damage:?}: batch probe through the damaged index"),
        );
    }
}

/// Row 4: an empty canonical prefix answers empty; a TopN prefix walk over the
/// damaged postings refuses.
#[test]
fn an_empty_prefix_answers_and_a_top_n_prefix_walk_refuses() {
    let (_root, path) = damaged_store("prefix.db", misfile);
    let session = reader(&path);
    assert_eq!(
        ids(session.execute(
            &format!(
                "SELECT id FROM scoped_rows WHERE context_id IN ($context) AND generation_id IN ($generation) \
                 AND is_current = TRUE ORDER BY embedding <=> $query LIMIT {LIMIT}"
            ),
            &HashMap::from([
                ("context".into(), Value::Uuid(context_id(0))),
                ("generation".into(), Value::Uuid(generation_id(1))),
                ("query".into(), Value::Vector(embedding(0))),
            ]),
        ))
        .expect("a pair with no rows answers from its canonical proof"),
        Vec::<i64>::new(),
        "the canonical tree proves the mismatched pair empty"
    );
    assert_corrupt(&window(&session, 0, 1), "TopN-1 prefix walk over Context 0");
}

/// Rows 5 and 6: point and identity reads answer; a grouped count over the
/// damaged index refuses; reads that never touch the damaged index answer.
#[test]
fn reads_that_do_not_consume_the_damaged_postings_answer() {
    let (_root, path) = damaged_store("doors.db", misfile);
    let session = reader(&path);
    assert_eq!(
        ids(session.execute(
            "SELECT id FROM scoped_rows WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(row_id(1, 0)))]),
        ))
        .expect("a point read by primary key answers"),
        vec![row_id(1, 0)],
        "the misfiled row's own identity read answers from the row table"
    );
    assert_eq!(
        ids(session.execute(
            "SELECT id FROM scoped_rows WHERE label = $label ORDER BY id LIMIT 3",
            &HashMap::from([("label".into(), Value::Text("label-1".into()))]),
        ))
        .expect("a read through the undamaged sibling index answers"),
        vec![row_id(0, 1), row_id(0, 5), row_id(0, 9)],
        "the sibling index serves its own exact answer"
    );
    assert_eq!(
        ids(session.execute(
            "SELECT id FROM notes WHERE label = $label ORDER BY id",
            &HashMap::from([("label".into(), Value::Text("note-1".into()))]),
        ))
        .expect("a read of another table answers"),
        vec![1, 3, 5, 7]
    );
    assert_own_rows(
        whole_pair(&session, 2),
        2,
        "a whole-pair find over an untouched Context",
    );
    let grouped = session.execute(
        "SELECT COUNT(*) FROM scoped_rows WHERE context_id = $context AND generation_id = $generation \
         AND is_current = TRUE",
        &HashMap::from([
            ("context".into(), Value::Uuid(context_id(0))),
            ("generation".into(), Value::Uuid(generation_id(0))),
        ]),
    );
    assert!(
        matches!(grouped, Err(Error::StoreCorrupted { .. })),
        "a grouped count over the damaged index refuses, got {grouped:?}"
    );
}

/// Row 7: row-table damage refuses every read of the table, the canonical
/// find included; a write into the damaged index refuses and changes no
/// witness.
#[test]
fn row_table_damage_and_writes_into_a_damaged_index_still_refuse() {
    let (_root, rows_damaged) = damaged_store("rows.db", |path| {
        damage_relational_row_table_for_test(path, "scoped_rows").expect("damage the row table");
    });
    let session = reader(&rows_damaged);
    assert_corrupt(
        &whole_pair(&session, 0),
        "whole-pair find over a damaged row table",
    );
    drop(session);

    let (_root, path) = damaged_store("write.db", misfile);
    let outcome = Database::open(&path).and_then(|db| {
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let insert = db.execute(
            "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, 'late', $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(10_000)),
                ("context".into(), Value::Uuid(context_id(0))),
                ("generation".into(), Value::Uuid(generation_id(0))),
                ("embedding".into(), Value::Vector(embedding(0))),
            ]),
        );
        let damage = db.index_root_damage();
        db.close()?;
        Ok((insert, damage))
    });
    let (insert, damage) =
        outcome.expect("the writer opens: only the index is damaged, not the store");
    assert!(
        matches!(insert, Err(Error::StoreCorrupted { .. })),
        "a write into the damaged index refuses, got {insert:?}"
    );
    assert_eq!(
        damage.expect("the verifier answers"),
        vec![("scoped_rows".to_owned(), INDEX.to_owned())],
        "the refused write left the index's witness unchanged: the verifier still lists it"
    );
}

/// Row 8: an undamaged store discloses nothing.
#[test]
fn an_undamaged_store_discloses_no_index_damage() {
    let (_root, path) = damaged_store("healthy.db", |_| {});
    let session = reader(&path);
    assert_own_rows(whole_pair(&session, 0), 0, "a healthy whole-pair find");
    assert_eq!(
        window(&session, 0, LIMIT)
            .expect("a healthy window answers")
            .len(),
        LIMIT
    );
    assert!(
        Database::observed_index_root_damage(&path)
            .expect("read the disclosure")
            .is_empty(),
        "nothing is disclosed for a healthy store"
    );
    let db = Database::open(&path).expect("open the healthy store");
    assert!(
        db.index_root_damage()
            .expect("the verifier answers")
            .is_empty(),
        "the verifier lists no index of a healthy store"
    );
    db.close().expect("close");
}
