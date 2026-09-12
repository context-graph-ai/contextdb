use contextdb_core::Value;
use contextdb_engine::vector_observations::{
    VectorJournalReplayWork, observe_base_publications, observe_journal_replay,
};
use contextdb_engine::{Database, MaintenancePolicy};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[test]
fn replay_events_are_operation_local_and_base_backlog_is_observed_at_publication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observed.redb");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO items VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0])",
        &HashMap::new(),
    )
    .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let observation = observe_base_publications(Arc::new(move |event| {
        sink.lock().unwrap().push(event.clone())
    }));
    db.run_maintenance_cycle().unwrap();
    let first = events.lock().unwrap()[0].clone();
    assert_eq!(first.pending_before.inserts, 2);
    assert_eq!(first.pending_after.inserts, 0);
    db.execute("INSERT INTO items VALUES (3, [0.5, 0.5])", &HashMap::new())
        .unwrap();
    db.execute("DELETE FROM items WHERE id = 2", &HashMap::new())
        .unwrap();
    db.run_maintenance_cycle().unwrap();
    db.execute("INSERT INTO items VALUES (4, [0.2, 0.8])", &HashMap::new())
        .unwrap();
    db.run_maintenance_cycle().unwrap();
    let last = events.lock().unwrap().last().unwrap().clone();
    assert!(last.generation > first.generation);
    assert_eq!(last.previous_generation, first.generation);
    assert_eq!(last.pending_before.inserts, 2);
    assert_eq!(last.pending_before.tombstones, 1);
    assert_eq!(last.previous_generation_high_water.inserts, 2);
    assert_eq!(last.previous_generation_high_water.tombstones, 1);
    assert_eq!(last.previous_generation_total_high_water, 3);
    assert_eq!(
        last.pending_after.inserts + last.pending_after.tombstones,
        0
    );
    drop(observation);
    db.execute(
        "UPDATE items SET embedding = [0.7, 0.3] WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);

    let replay = Arc::new(Mutex::new(Vec::new()));
    let sink = replay.clone();
    let observation = observe_journal_replay(Arc::new(move |event| {
        sink.lock().unwrap().push(event.clone())
    }));
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let sql = "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    reopened
        .execute(
            sql,
            &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    let events = replay.lock().unwrap().clone();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.work, VectorJournalReplayWork::Record { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.work, VectorJournalReplayWork::Vector { .. }))
            .count(),
        1
    );
    drop(observation);
    reopened
        .execute(
            sql,
            &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(
        replay.lock().unwrap().len(),
        events.len(),
        "ending the operation detaches its observer"
    );
}

#[test]
fn publication_keeps_commits_after_its_snapshot_in_the_observed_durable_tail() {
    use contextdb_core::{VectorIndexRef, VectorPartitionKey};
    use contextdb_engine::VectorMaintenancePreparationPhaseForTest;
    use contextdb_vector::VectorPartitionRef;
    use std::{sync::mpsc, thread, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.redb");
    let db = Arc::new(Database::open(&path).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO items VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0])",
        &HashMap::new(),
    )
    .unwrap();
    db.run_maintenance_cycle().unwrap();
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (M = 24)",
        &HashMap::new(),
    )
    .unwrap();
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "embedding"),
        VectorPartitionKey::unpartitioned(),
    );
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &partition,
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let maintenance_db = db.clone();
    let maintenance = thread::spawn(move || {
        let _observation = observe_base_publications(Arc::new(move |event| {
            sink.lock().unwrap().push(event.clone())
        }));
        maintenance_db.run_maintenance_cycle().unwrap();
    });
    pause
        .wait_until_reached_timeout(Duration::from_secs(10))
        .unwrap();
    let writer_db = db.clone();
    let (sender, receiver) = mpsc::channel();
    let writer = thread::spawn(move || {
        writer_db
            .execute("INSERT INTO items VALUES (3, [0.5, 0.5])", &HashMap::new())
            .unwrap();
        writer_db
            .execute("DELETE FROM items WHERE id = 2", &HashMap::new())
            .unwrap();
        let rows = writer_db
            .read_session(contextdb_core::read_contract::ReadLimits::default())
            .unwrap()
            .execute(
                "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
                &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]),
            )
            .unwrap();
        sender.send(rows.rows.len()).unwrap();
    });
    let completed = receiver.recv_timeout(Duration::from_secs(5));
    pause.release();
    writer.join().unwrap();
    maintenance.join().unwrap();
    assert_eq!(
        completed.unwrap(),
        2,
        "recording and reads finish before preparation resumes"
    );
    let observed = events.lock().unwrap();
    assert_eq!(
        observed.len(),
        1,
        "one finite cycle publishes its captured snapshot once"
    );
    let event = &observed[0];
    assert_eq!(event.pending_before.inserts, 1);
    assert_eq!(event.pending_before.tombstones, 1);
    assert_eq!(
        event.pending_after.inserts, 1,
        "newer insertion stays in the durable tail"
    );
    assert_eq!(
        event.pending_after.tombstones, 1,
        "newer deletion stays in the durable tail"
    );
    assert_eq!(event.generation_high_water.inserts, 1);
    assert_eq!(event.generation_high_water.tombstones, 1);
    drop(observed);
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let answer = reopened
        .execute(
            "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0]))]),
        )
        .unwrap();
    assert_eq!(
        answer.rows,
        vec![vec![Value::Int64(1)], vec![Value::Int64(3)]]
    );
}
