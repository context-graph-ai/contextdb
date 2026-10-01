//! Acceptance: "Durable rows, indexes and their metadata must load on demand rather than remain
//! wholly resident." "Writable open needs measured work/memory evidence, without a new numeric
//! latency threshold."

use contextdb_core::{Value, Wallclock};
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn writable_open_never_touches_history_families() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut measurements = Vec::new();
    for rows in [32_i64, 64] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.redb");
        let database = Database::open(&path).unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        database.execute("CREATE TABLE history_items (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, payload TEXT, embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE EXACT) HISTORY ALL", &HashMap::new()).unwrap();
        database
            .execute(
                "CREATE INDEX history_scope ON history_items (scope, id)",
                &HashMap::new(),
            )
            .unwrap();
        for id in 1..=rows {
            database
                .execute(
                    "INSERT INTO history_items VALUES ($id, 1, 'original', $vector)",
                    &HashMap::from([
                        ("id".to_owned(), Value::Int64(id)),
                        ("vector".to_owned(), Value::Vector(vec![id as f32, 1.0])),
                    ]),
                )
                .unwrap();
            database
                .execute(
                    "UPDATE history_items SET payload = 'current' WHERE id = $id",
                    &HashMap::from([("id".to_owned(), Value::Int64(id))]),
                )
                .unwrap();
            let tx = database.begin_or_panic();
            database
                .insert_edge(
                    tx,
                    Uuid::from_u128(id as u128 * 2),
                    Uuid::from_u128(id as u128 * 2 + 1),
                    "links".to_owned(),
                    HashMap::new(),
                )
                .unwrap();
            database.commit(tx).unwrap();
        }
        let prior_snapshot = database.snapshot();
        let prior_lsn = database.current_lsn();
        database.close().unwrap();
        drop(database);
        let (opened, work) = observe(|| Database::open(&path));
        let database = opened.unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        assert_eq!(
            database.snapshot(),
            prior_snapshot,
            "open restores the exact committed transaction bound"
        );
        assert_eq!(
            database.current_lsn(),
            prior_lsn,
            "open restores the exact committed log bound"
        );
        // Positive access oracles exclude an empty store masquerading as a cheap open.
        let current = database
            .execute(
                "SELECT id, payload FROM history_items WHERE scope = 1 ORDER BY id",
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            current.rows,
            (1..=rows)
                .map(|id| vec![Value::Int64(id), Value::Text("current".to_owned())])
                .collect::<Vec<_>>()
        );
        let vectors = database.execute("SELECT id FROM history_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 64", &HashMap::from([("query".to_owned(), Value::Vector(vec![0.0, 1.0]))])).unwrap();
        assert_eq!(
            vectors.rows,
            (1..=rows)
                .map(|id| vec![Value::Int64(id)])
                .collect::<Vec<_>>(),
            "all saved memberships remain searchable after lazy open"
        );
        assert_eq!(
            database
                .edge_count(Uuid::from_u128(2), "links", database.snapshot())
                .unwrap(),
            1
        );
        let tx = database.begin_or_panic();
        assert!(
            tx.0 > prior_snapshot.0,
            "the next writer never reuses a committed transaction identity"
        );
        database.rollback(tx).unwrap();
        database.close().unwrap();
        measurements.push(work.history);
    }
    for (arm, work) in measurements.iter().enumerate() {
        assert_eq!(
            (
                work.row_versions,
                work.index_rebuild_inputs,
                work.graph_versions,
                work.change_log_entries,
                work.commit_index_entries,
                work.vector_memberships
            ),
            (0, 0, 0, 0, 0, 0),
            "writable open must decode/iterate zero history entries in arm {arm}; observed={work:?}"
        );
    }
    assert_eq!(
        measurements[0].commit_bound_keys, measurements[1].commit_bound_keys,
        "open reads a constant number of commit bounds keys as history doubles"
    );
    assert!(
        measurements[0].commit_bound_keys > 0,
        "the bounds observer must witness the physical committed-index bound lookup"
    );
}
