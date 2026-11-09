//! A search on either live-writer reader route while six transactions are staged.
use super::support::{
    drive_until_serving, empty, int_ids, ivf, observe_file_search, open_caller_driven, params,
    serving, unit_vector,
};
use contextdb_core::{Value, read_contract::ReadLimits};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::{Database, ReadRoute, ReadSession, ReadSessionOptions, VectorSearchRoute};
use std::sync::{Arc, mpsc};
use uuid::Uuid;

fn context(n: u128) -> Uuid {
    Uuid::from_u128(0xc100 + n)
}
fn insert(db: &Database, tx: contextdb_core::TxId, id: i64, partition: u128) {
    db.execute_in_tx(
        tx,
        "INSERT INTO live_items (id, context_id, embedding) VALUES ($id, $context, $v)",
        &params([
            ("id", Value::Int64(id)),
            ("context", Value::Uuid(context(partition))),
            ("v", Value::Vector(unit_vector(id as u64, 16))),
        ]),
    )
    .unwrap();
}

fn exercise(owner_channel: bool) {
    let _wallclock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("live.db");
    let db = open_caller_driven(&path);
    db.execute(&format!("CREATE TABLE live_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR(16) PARTITION_KEY (context_id) SEARCH_MODE INDEXED {})", ivf(8, 2, 32)), &empty()).unwrap();
    let tx = db.begin_or_panic();
    for partition in 0..2 {
        for i in 0..150 {
            insert(&db, tx, partition as i64 * 1000 + i, partition);
        }
    }
    db.commit(tx).unwrap();
    drive_until_serving(&db, 300, 1, 10);
    let tuple = [Value::Uuid(context(0))];
    let generation = serving(&db, "live_items", "embedding", &tuple).expect("target generation");
    let reader = if owner_channel {
        ReadSession::open_with_clock_for_test(
            &path,
            ReadSessionOptions::default(),
            Arc::new(ManualDeadlineClock::at(0)),
        )
        .unwrap()
    } else {
        db.read_session_with_clock_for_test(
            ReadLimits::default(),
            Arc::new(ManualDeadlineClock::at(0)),
        )
        .unwrap()
    };
    assert_eq!(reader.route(), ReadRoute::Owner);
    let bound = params([
        ("context", Value::Uuid(context(0))),
        ("v", Value::Vector(unit_vector(3, 16))),
    ]);
    let indexed = "SELECT id FROM live_items WHERE context_id = $context ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 10";
    let auto = "SELECT id FROM live_items WHERE context_id = $context ORDER BY embedding <=> $v USE VECTOR AUTO LIMIT 10";
    let (baseline, before) =
        observe_file_search(&reader, || reader.execute(indexed, &bound).unwrap());
    let ids = int_ids(&baseline);
    assert_eq!(ids.len(), 10);
    assert!(before.clustered_route);
    std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut release = Vec::new();
        for worker in 0..6 {
            let (go_tx, go_rx) = mpsc::channel();
            release.push(go_tx);
            let ready = ready_tx.clone();
            let db = &db;
            scope.spawn(move || {
                let tx = db.begin_or_panic();
                insert(db, tx, 10_000 + worker, 1);
                ready.send(()).unwrap();
                if go_rx.recv().is_ok() {
                    db.commit(tx).unwrap();
                } else {
                    db.rollback(tx).unwrap();
                }
            });
        }
        drop(ready_tx);
        for _ in 0..6 {
            ready_rx.recv().expect("six staged writers");
        }
        let (during, work) =
            observe_file_search(&reader, || reader.execute(indexed, &bound).unwrap());
        assert_eq!(int_ids(&during), ids);
        assert_eq!(
            work, before,
            "unrelated staged writes leave target work unchanged"
        );
        assert_eq!(
            serving(&db, "live_items", "embedding", &tuple),
            Some(generation)
        );
        let automatic = reader.execute(auto, &bound).unwrap();
        assert_eq!(automatic.rows().len(), 10);
        let fraction = automatic
            .trace()
            .cache_resident_fraction
            .expect("live route reports actual query residency");
        assert!((0.0..=1.0).contains(&fraction));
        let route = automatic
            .trace()
            .vector_search
            .as_ref()
            .expect("executed vector trace")
            .route;
        assert!(
            matches!(
                route,
                Some(VectorSearchRoute::Indexed | VectorSearchRoute::Exact)
            ),
            "AUTO reports its executed route: {route:?}"
        );
        for go in release {
            go.send(()).unwrap();
        }
    });
    let (after, work) = observe_file_search(&reader, || reader.execute(indexed, &bound).unwrap());
    assert_eq!(int_ids(&after), ids);
    assert_eq!(
        work, before,
        "unrelated committed writes leave target work unchanged"
    );
    assert_eq!(
        serving(&db, "live_items", "embedding", &tuple),
        Some(generation)
    );
}

#[test]
#[ignore = "The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run."]
fn owner_channel_search_during_six_staged_writers() {
    exercise(true);
}
#[test]
#[ignore = "The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run."]
fn live_database_search_during_six_staged_writers() {
    exercise(false);
}
