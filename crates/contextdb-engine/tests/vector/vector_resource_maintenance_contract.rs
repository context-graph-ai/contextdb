use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::VectorPartitionRef;
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

fn seed(db: &Database, scopes: usize, _m: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let max_partitions = scopes.max(3);
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS {max_partitions} SEARCH_MODE INDEXED )"), &HashMap::new()).unwrap();
    for scope in 0..scopes {
        for row in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, $scope, $v)",
                &HashMap::from([
                    ("id".into(), Value::Int64((scope * 16 + row) as i64)),
                    ("scope".into(), Value::Int64(scope as i64)),
                    (
                        "v".into(),
                        Value::Vector(vec![1.0, row as f32 / 16.0, scope as f32 / 16.0]),
                    ),
                ]),
            )
            .unwrap();
        }
    }
}
fn partition(scope: i64) -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
    )
}
#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn in_memory_construction_leaves_same_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let target = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&target, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the production in-memory build reaches its third inserted vector"
    );

    let publication_is_free = db
        .vector_store_for_test()
        .try_partition_maintenance_lock_for_test(&target);
    if !publication_is_free {
        pause.release();
        worker
            .join()
            .expect("join the blocked maintenance proof")
            .expect("the maintenance build itself succeeds");
        panic!("full graph construction must not hold the mutex a same-partition commit needs");
    }

    db.execute(
        "INSERT INTO items VALUES (1000, 0, '[0,0,1]')",
        &HashMap::new(),
    )
    .expect("a same-partition vector commit completes while construction remains paused");
    let visible = db
        .execute(
            "SELECT id FROM items WHERE scope = 0 ORDER BY v <=> [0,0,1] USE VECTOR EXACT LIMIT 1",
            &HashMap::new(),
        )
        .expect("the committed vector is exact-searchable before construction resumes");
    assert_eq!(visible.rows(), vec![vec![Value::Int64(1000)]]);

    pause.release();
    let first = worker
        .join()
        .expect("join the in-memory maintenance worker")
        .expect("a concurrently stale build returns normally");
    assert_eq!(
        first.vector.built_partitions, 1,
        "the same finite wake discards the stale sample and rebuilds from the committed write"
    );
    let indexed = db
        .execute(
            "SELECT id FROM items WHERE scope = 0 ORDER BY v <=> [0,0,1] USE VECTOR INDEXED LIMIT 1",
            &HashMap::new(),
        )
        .expect("the replacement route includes the concurrent vector");
    assert_eq!(indexed.rows(), vec![vec![Value::Int64(1000)]]);
}

#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn in_memory_construction_leaves_new_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let building = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&building, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the production in-memory build reaches its third inserted vector"
    );

    let structural_publication_is_free = db
        .vector_store_for_test()
        .try_bulk_maintenance_lock_for_test();
    if !structural_publication_is_free {
        pause.release();
        worker
            .join()
            .expect("join the blocked maintenance proof")
            .expect("the maintenance build itself succeeds");
        panic!("full graph construction must not hold the gate a new-partition commit needs");
    }

    db.execute(
        "INSERT INTO items VALUES (1000, 1, '[0,0,1]')",
        &HashMap::new(),
    )
    .expect("a new-partition vector commit completes while construction remains paused");
    let visible = db
        .execute(
            "SELECT id FROM items WHERE scope = 1 ORDER BY v <=> [0,0,1] USE VECTOR EXACT LIMIT 1",
            &HashMap::new(),
        )
        .expect("the new partition is exact-searchable before construction resumes");
    assert_eq!(visible.rows(), vec![vec![Value::Int64(1000)]]);

    pause.release();
    assert_eq!(
        worker
            .join()
            .expect("join the in-memory maintenance worker")
            .expect("the sampled maintenance cycle succeeds")
            .vector
            .built_partitions,
        1,
        "the fixed candidate snapshot completes while the new partition waits for the next wake"
    );
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("the next finite cycle advances the new partition")
            .vector
            .built_partitions,
        1
    );
    let indexed = db
        .execute(
            "SELECT id FROM items WHERE scope = 1 ORDER BY v <=> [0,0,1] USE VECTOR INDEXED LIMIT 1",
            &HashMap::new(),
        )
        .expect("the next maintenance wake publishes the new partition's route");
    assert_eq!(indexed.rows(), vec![vec![Value::Int64(1000)]]);
}

#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn changing_the_poll_interval_returns_while_engine_maintenance_is_active() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE items (
            id INT PRIMARY KEY,
            scope INT NOT NULL,
            v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED
                
        )",
        &HashMap::new(),
    )
    .expect("declare engine-owned vector maintenance");
    for row in 0..16 {
        db.execute(
            "INSERT INTO items VALUES ($id, 0, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(row))]),
        )
        .expect("seed active maintenance work");
    }

    let clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("hold the real engine-owned maintenance clock");
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition(0), 1);
    let cycle = thread::spawn(move || clock.wake_once());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the controlled wake must reach the index-construction progress point"
    );

    let (returned_tx, returned_rx) = mpsc::channel();
    let setter_db = db.clone();
    let setter = thread::spawn(move || {
        let result = setter_db.set_maintenance_poll_interval(Duration::from_millis(37));
        returned_tx
            .send(())
            .expect("report the setter's completion event");
        result
    });
    returned_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the interval setter returns while graph construction is still paused");
    assert_eq!(
        db.maintenance_poll_interval(),
        Duration::from_millis(37),
        "the setter publishes its value while graph construction is still paused"
    );
    setter
        .join()
        .expect("join the interval setter")
        .expect("the interval setter succeeds");

    pause.release();
    cycle
        .join()
        .expect("join the controlled maintenance wake")
        .expect("the controlled maintenance wake completes");
}
