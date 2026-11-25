//! What a handle and a transaction promise the compiler, and what a lookup does with a damaged
//! index.

use std::collections::HashMap;

use super::{Database, DatabaseOpenOptions, Transaction};
use crate::types::{Error, Value};

fn shared<T: Send + Sync>() {}
fn sent<T: Send>() {}

/// No test may start a thread, so what "calls may be made from many threads" rests on is
/// checked here by the compiler.
#[test]
fn a_handle_is_shared_between_threads_and_a_transaction_moves_to_one() {
    shared::<Database>();
    shared::<crate::Snapshot>();
    shared::<crate::ReadSession>();
    sent::<Transaction<'_>>();
}

/// An index entry whose row is not in the table is damage of the store, never an empty answer:
/// a lookup through the index reads the row it names, and a row that is not there means the
/// store disagrees with itself. No public call makes such an entry, so it is staged through
/// the store's own write, as a commit of a store that was damaged would have left it.
#[test]
fn an_index_entry_with_no_row_is_refused_as_damage_by_a_lookup_through_it() {
    use crate::memory::Budget;
    use crate::schema::Catalog;
    use crate::store::{ChangeSet, Key, Store};

    let folder = crate::vector::tests::fresh("orphan-entry");
    let db = Database::open(&folder).expect("open a fresh store");
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, tag INTEGER)",
        "CREATE INDEX t_tag ON t (tag)",
        "INSERT INTO t (id, tag) VALUES (1, 7)",
    ] {
        db.execute(sql, &HashMap::new()).expect(sql);
    }
    drop(db);

    {
        let budget = Budget::new(None).expect("a budget");
        let store = Store::open_for_writing(&folder, &budget, &DatabaseOpenOptions::default())
            .expect("open the store");
        let view = store.view().expect("a view");
        let catalog = Catalog::load(&view, &budget).expect("the tables");
        let table = catalog.table("t").expect("table t");
        // Place 0 is the index behind the primary key; `t_tag` is place 1, the one `tag = 9` walks.
        let entry = table
            .entry_shape(1)
            .entry(&[Value::Int64(9)])
            .expect("an entry for tag 9");
        let absent = Key::from_values(&[Value::Int64(99)], &[]).expect("a key");
        let mut changes = ChangeSet::new(view, &budget).expect("begin");
        changes
            .put_index(table.index_ids[1], entry, absent)
            .expect("stage the entry");
        store.commit(changes).expect("commit");
    }

    let db = Database::open(&folder).expect("open the store again");
    let found = db
        .execute("SELECT id FROM t WHERE tag = 7", &HashMap::new())
        .expect("a lookup through an entry with its row");
    assert_eq!(
        found.rows(),
        [vec![Value::Int64(1)]],
        "an entry with its row is found"
    );
    let found = db.execute("SELECT id FROM t WHERE tag = 9", &HashMap::new());
    assert!(
        matches!(found, Err(Error::StoreCorrupted { .. })),
        "an index entry naming a row that is not there must be refused as damage, not skipped \
         into an empty answer: {found:?}"
    );
    // An ordered walk of the index meets the entry too, forward and backward (CDB-141).
    for order in ["", " DESC"] {
        let sql = format!("SELECT id FROM t WHERE tag >= 7 ORDER BY tag{order}");
        let found = db.execute(&sql, &HashMap::new());
        let damage = matches!(found, Err(Error::StoreCorrupted { .. }));
        assert!(damage, "{sql} gave {found:?}, not the damage");
    }
    drop(db);
    std::fs::remove_dir_all(&folder).expect("remove the test's folder");
}

use crate::vector::tests::report;

#[test]
fn expiry_uses_current_declarations_and_keeps_every_derived_answer_atomic() {
    use crate::{
        Clock, Maintenance, params,
        store::{Key, Read},
    };
    use std::{
        ops::Bound::Included,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering::SeqCst},
            mpsc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    struct At(
        AtomicU64,
        Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    );
    impl Clock for At {
        fn now(&self) -> SystemTime {
            let gate = self.1.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
            UNIX_EPOCH + Duration::from_secs(self.0.load(SeqCst))
        }
    }
    let folder = crate::vector::tests::fresh("expiry-identity");
    std::fs::create_dir_all(&folder).unwrap();
    let clock = Arc::new(At(AtomicU64::new(100), Mutex::new(None)));
    let open = |storage| {
        Database::open_with_options(
            &folder,
            DatabaseOpenOptions {
                clock: Some(clock.clone()),
                storage,
                maintenance: Maintenance::Caller,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let run = |db: &Database, sql: &str| db.execute(sql, &params! {}).unwrap();
    let edge = "CREATE TABLE edges (id INT PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, expires TIMESTAMP EXPIRES) RETAIN 1 SECONDS";
    let graph = "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS b_id))";
    let graph_answers = |db: &Database, present: bool| {
        let wanted = if present {
            vec![vec![Value::Uuid(crate::Uuid::from_u128(2))]]
        } else {
            vec![]
        };
        assert_eq!(run(db, graph).rows(), wanted);
        let wanted = if present {
            vec![vec![Value::Int64(1)]]
        } else {
            vec![]
        };
        assert_eq!(
            run(db, "SELECT id FROM edges WHERE edge_type = 'E'").rows(),
            wanted
        );
    };
    let parents = |db: &Database, ids: &[i64]| {
        let wanted: Vec<_> = ids.iter().map(|id| vec![Value::Int64(*id)]).collect();
        assert_eq!(
            run(db, "SELECT id FROM a_parent ORDER BY id").rows(),
            wanted
        );
        assert_eq!(
            run(db, "SELECT id FROM a_parent WHERE id >= 0 ORDER BY id").rows(),
            wanted
        );
    };
    let children = |db: &Database, first: bool| {
        let mut keys: Vec<_> = (2..=601).map(|id| vec![Value::Int64(id)]).collect();
        if first {
            keys.insert(0, vec![Value::Int64(1)]);
        }
        keys.push(vec![Value::Int64(10000)]);
        assert_eq!(run(db, "SELECT id FROM b_child ORDER BY id").rows(), keys);
        let mut wanted = if first {
            vec![vec![Value::Int64(1), Value::Int64(1)]]
        } else {
            vec![]
        };
        wanted.extend([
            vec![Value::Int64(5000), Value::Int64(600)],
            vec![Value::Int64(10000), Value::Int64(1)],
        ]);
        assert_eq!(
            run(db, "SELECT p, COUNT(*) FROM b_child GROUP BY p ORDER BY p").rows(),
            wanted
        );
    };
    let db = open(None);
    run(
        &db,
        "CREATE TABLE a_parent (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
    );
    run(
        &db,
        "CREATE TABLE b_child (id INT PRIMARY KEY, p INT REFERENCES a_parent(id), expires TIMESTAMP EXPIRES, UNIQUE(p, id), UNIQUE(id, p)) RETAIN 1 SECONDS MAINTAIN COUNT(p)",
    );
    db.execute(
        "INSERT INTO a_parent VALUES (1, NULL), (2, NULL), (5000, $pin), (10000, NULL)",
        &params! {"pin"=>Value::Timestamp(i64::MAX)},
    )
    .unwrap();
    db.execute(
        "INSERT INTO b_child VALUES (1, 1, NULL), (10000, 10000, $pin)",
        &params! {"pin"=>Value::Timestamp(i64::MAX)},
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    for id in 2..=601 {
        tx.execute(
            "INSERT INTO b_child VALUES ($id, 5000, $pin)",
            &params! {"id"=>id, "pin"=>Value::Timestamp(i64::MAX)},
        )
        .unwrap();
    }
    tx.commit().unwrap();
    run(&db, "CREATE INDEX parent_id ON a_parent(id)");
    run(&db, edge);
    run(&db, "CREATE INDEX edges_type ON edges(edge_type)");
    run(
        &db,
        "INSERT INTO edges VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E', NULL)",
    );
    run(
        &db,
        "CREATE TABLE docs (id INT PRIMARY KEY, ctx TEXT NOT NULL, expires TIMESTAMP EXPIRES, v VECTOR(4) PARTITION_KEY(ctx) INDEXING_THRESHOLD 2000) RETAIN 1 SECONDS",
    );
    let load = |ids: std::ops::RangeInclusive<i64>, ctx: &str, due: bool| {
        let tx = db.transaction().unwrap();
        for id in ids {
            tx.execute("INSERT INTO docs VALUES ($id, $ctx, $e, $v)", &params! {"id"=>id, "ctx"=>ctx, "e"=>Value::Timestamp(if due && id <= 1010 {101_000} else {i64::MAX}), "v"=>vec![1.0_f32, (id % 7) as f32, (id % 11) as f32, 0.5]}).unwrap();
        }
        tx.commit().unwrap();
    };
    let drain = |db: &Database| {
        for ran in 1..=100 {
            let v = db.run_maintenance_cycle().unwrap().vector;
            if v.remaining_partitions == Some(0) && v.maintenance_vectors_remaining == Some(0) {
                break;
            }
            assert!(ran < 100, "the fixture IVF drains");
        }
    };
    load(1..=10, "a", false);
    drain(&db);
    run(&db, "DELETE FROM docs WHERE id <= 2");
    load(1001..=3000, "b", true);
    drain(&db);
    parents(&db, &[1, 2, 5000, 10000]);
    children(&db, true);
    graph_answers(&db, true);
    db.verify().unwrap();
    db.close().unwrap();
    drop(db);
    let saved = std::fs::read(folder.join("data")).unwrap();
    let restore = || std::fs::write(folder.join("data"), &saved).unwrap();
    clock.0.store(101, SeqCst);
    for action in ["extend", "drop", "replace"] {
        restore();
        let db = Arc::new(open(None));
        let opened = db.opened().unwrap();
        let expiry = super::locked(&opened.expiry);
        let (_, catalog) = opened.newest().unwrap();
        let (entered, seen) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        *clock.1.lock().unwrap() = Some((entered, wait));
        let (answer, result) = mpsc::channel();
        let other = db.clone();
        let thread = crate::system::launch(move || {
            answer.send(other.run_maintenance_cycle()).unwrap();
        })
        .unwrap();
        seen.recv().unwrap();
        let owners = Arc::strong_count(&catalog);
        release.send(()).unwrap();
        while Arc::strong_count(&catalog) <= owners {
            std::thread::yield_now();
        }
        match action {
            "extend" => {
                run(&db, "ALTER TABLE edges SET RETAIN 100 SECONDS");
            }
            "drop" => {
                run(&db, "DROP TABLE edges");
            }
            _ => {
                run(&db, "DROP TABLE edges");
                run(&db, edge);
                run(&db, "CREATE INDEX edges_type ON edges(edge_type)");
                db.execute("INSERT INTO edges VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E', $past)", &params! {"past"=>Value::Timestamp(99_000)}).unwrap();
            }
        }
        drop(expiry);
        let cycle = result.recv().unwrap();
        thread.join().unwrap();
        let report = cycle.unwrap();
        if action == "drop" {
            assert!(matches!(
                db.table_meta("edges"),
                Err(Error::TableNotFound(_))
            ));
        } else {
            graph_answers(&db, true);
        }
        parents(&db, &[1, 5000, 10000]);
        children(&db, false);
        assert_eq!(
            (
                report.vector.remaining_partitions,
                report.vector.maintenance_vectors_remaining
            ),
            (Some(1), Some(10)),
            "expiry exposes untouched partition a"
        );
        // The cycle that expires rows starts work on the partition they expose (9a13449e1).
        let shown = run(&db, "SHOW VECTOR_PARTITIONS FOR docs.v");
        assert!(
            shown
                .rows()
                .iter()
                .any(|r| r.iter().any(|v| v == &Value::Text("building".into())))
        );
        db.verify().unwrap();
        db.close().unwrap();
    }
    // Isolate the dependent lookup before replaying its exact cold page in public expiry.
    restore();
    let fault = report::Fault::new("dependent-validation-sentinel");
    let db = open(fault.options(Default::default()).storage);
    let opened = db.opened().unwrap();
    let (view, catalog) = opened.newest().unwrap();
    let child = catalog.table("b_child").unwrap();
    let reference = &child.rules.references[0];
    let key = Key::from_values(&[Value::Int64(10000)], &[]).unwrap();
    let needle = [
        child.index_ids[reference.index].0.to_be_bytes().as_slice(),
        key.as_bytes(),
    ]
    .concat();
    fault.clear();
    assert!(
        view.index(
            child.index_ids[reference.index],
            Included(&key),
            Included(&key),
            false
        )
        .unwrap()
        .next()
        .transpose()
        .unwrap()
        .is_some()
    );
    fault.identify(&needle);
    drop((view, catalog, opened));
    db.close().unwrap();
    drop(db);
    restore();
    let db = open(fault.options(Default::default()).storage);
    let db = fault.refuse_cycle(db, || open(fault.options(Default::default()).storage));
    parents(&db, &[1, 2, 5000, 10000]);
    children(&db, true);
    graph_answers(&db, true);
    db.verify().unwrap();
    let report = db.run_maintenance_cycle().unwrap();
    assert_eq!(report.deferred, Some(2));
    graph_answers(&db, false);
    parents(&db, &[1, 5000, 10000]);
    children(&db, false);
    assert_eq!(report::count(report.expired_rows), 13);
    assert_eq!(db.run_maintenance_cycle().unwrap().expired_rows, Some(1));
    parents(&db, &[5000, 10000]);
    assert_eq!(db.run_maintenance_cycle().unwrap().expired_rows, Some(0));
    db.verify().unwrap();
    db.close().unwrap();
}
#[test]
fn a_kept_plan_is_returned_without_parsing_its_text_again() {
    let budget = crate::memory::Budget::new(None).unwrap();
    let plans = super::KeptPlans::new(&budget, None).unwrap();
    plans.planned("SELECT 1", &budget).unwrap();
    // A sentinel key cannot parse: only the existing hit can return the stored SELECT.
    super::locked(&plans.0).0[0].0 = "this cannot parse".into();
    assert!(matches!(
        plans.planned("this cannot parse", &budget).unwrap().0,
        crate::sql::Plan::Select(_)
    ));
}
