use super::*;
use contextdb::Maintenance;
use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
use std::collections::BTreeMap;
use support::records::{DIM, assert_derived, grouped, id_of, key_of, number, slots, under};
use support::*;

type Records = BTreeMap<Vec<u8>, Vec<u8>>;

fn records(folder: &Path) -> Records {
    let db = redb::Builder::new()
        .open_read_only(folder.join("data"))
        .unwrap();
    let read = db.begin_read().unwrap();
    let beside = read
        .open_table(TableDefinition::<u64, &[u8]>::new("beside"))
        .unwrap();
    assert!(beside.is_empty().unwrap(), "erased large values remain");
    support::vector_records(folder)
}

fn snapshot(db: &Database, dest: &Path) -> Records {
    db.verify().expect("all records agree after erasure");
    db.export_snapshot(dest).expect("export committed records");
    records(dest)
}

#[test]
fn a_purge_leaves_no_copy_in_either_commit_slot() {
    if let Some(role) = role() {
        let db = open(&role.folder);
        run(&db, "PURGE FROM docs");
        say("purged");
        hear(); // killed without a close that could replace the older commit slot
        return;
    }
    // Recovery must select the older slot after the newest slot's checksum is damaged.
    let test = "purge::a_purge_leaves_no_copy_in_either_commit_slot";
    let folder = store_with_vectors("purge-two-slots", 20, 21..=25);
    let mut child = Child::start(test, "purger", &folder);
    child.expect("purged");
    child.kill();
    let file = folder.join("data");
    let mut bytes = fs::read(&file).unwrap();
    let newest = 64 + usize::from(bytes[9] & 1) * 128;
    bytes[newest + 112] ^= 1;
    fs::write(&file, bytes).unwrap();
    let db = open(&folder);
    assert_eq!(int(&db, "SELECT COUNT(*) FROM docs"), 0);
    let copy = folder.join("export");
    assert!(
        snapshot(&db, &copy).is_empty(),
        "vector records survive recovery"
    );
    assert_eq!(int(&open(&copy), "SELECT COUNT(*) FROM docs"), 0);

    for (mode, zero) in [
        (Maintenance::Caller, true),
        (Maintenance::Caller, false),
        (Maintenance::Engine, false),
    ] {
        let folder = support::folder(&format!("purge-derived-{mode:?}-{zero}"));
        let mut db = open(&folder);
        declare_docs(&db, DIM, "PARTITION_KEY(ctx) INDEXING_THRESHOLD 1000");
        load_docs(&db, 1..=2000, "a", |n| {
            let mut v = gathered(n, DIM);
            if zero && n == 1 {
                v.fill(0.0);
            }
            v
        });
        load_docs(&db, 20000..=21999, "d", |n| gathered(n, DIM));
        for p in 0..130 {
            load_docs(
                &db,
                30000 + 2 * p..=30001 + 2 * p,
                &format!("z{p:03}"),
                |n| gathered(n, DIM),
            );
        }
        maintenance(&db);
        let original = snapshot(&db, &folder.join("original"));
        let (group_blocks, centres, members) = grouped(&original);
        assert!(centres[0].iter().any(|x| *x != 0.0));
        // The zero-vector row survives; its centre must lose the erased direction.
        let mut erased: BTreeSet<i64> = members
            .iter()
            .filter(|(k, _, g)| *g == 0 && (!zero || id_of(k) != 1))
            .map(|(k, _, _)| id_of(k))
            .collect();
        erased.insert(id_of(&members.iter().find(|(_, _, g)| *g == 1).unwrap().0));
        let changed = *erased.first().unwrap();
        db.execute(
            "UPDATE docs SET v = $v WHERE id = $id",
            &params! { "id" => changed, "v" => vector(9000, DIM) },
        )
        .unwrap();
        let neighbours: Vec<_> = members
            .iter()
            .map(|(k, _, _)| id_of(k))
            .filter(|id| *id != 1 && !erased.contains(id))
            .take(2)
            .collect();
        for (id, ctx) in [
            (neighbours[0], "d"),
            (neighbours[0], "e"),
            (neighbours[1], "d"),
            (changed, "b"),
            (changed, "c"),
        ] {
            run(&db, &format!("UPDATE docs SET ctx='{ctx}' WHERE id={id}"));
            cycle(&db);
        }
        run(&db, &format!("UPDATE docs SET v=NULL WHERE id={changed}"));
        load_docs(&db, 2001..=11000, "a", |n| gathered(n, DIM));
        load_docs(&db, 40000..=48999, "d", |n| gathered(n, DIM));
        erased.extend([2001, 30258]);
        let mut before = Records::new();
        for cycle in 0..80 {
            db.run_maintenance_cycle().unwrap();
            before = snapshot(&db, &folder.join(format!("cycle-{cycle}")));
            if before
                .keys()
                .filter(|k| k[0] == 6)
                .map(|k| under(k))
                .collect::<BTreeSet<_>>()
                .len()
                == 2
            {
                break;
            }
        }
        assert!(before.keys().any(|k| k[0] == 5), "a build is under way");
        let part =
            |ctx| &before[&[vec![1], group_blocks[0][1..5].to_vec(), vec![4, ctx, 0, 0]].concat()];
        let build = number(part(b'a'), 69) as u32;
        assert!(before.keys().any(|k| k[0] == 6 && under(k) == build));
        let (a, d) = (number(part(b'a'), 0) as u32, part(b'd'));
        assert!(before.keys().filter(|k| k[0] == 0 && under(k) == a).count() > 1);
        let (d, building) = (number(d, 0) as u32, number(d, 69) as u32);
        let kept: Records = before
            .iter()
            .filter(|(k, _)| {
                ([5, 9].contains(&k[0]) && under(k) == d)
                    || ([0, 4, 6].contains(&k[0]) && under(k) == building)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert!(kept.keys().any(|k| k[0] == 9) && kept.keys().any(|k| k[0] == 6));
        run(
            &db,
            "CREATE TABLE secrets (id TEXT PRIMARY KEY, body TEXT, v VECTOR(384))",
        );
        run(&db, "CREATE EVENT TYPE added WHEN INSERT ON secrets");
        run(&db, "CREATE SINK absent TYPE callback");
        run(&db, "CREATE ROUTE notify EVENT added TO absent");
        let marker = letters(987, 100);
        let vector = vector(987, 384);
        db.execute("INSERT INTO secrets VALUES ($key, $body, $v)", &params! { "key" => marker.as_str(), "body" => letters(1234, 4000), "v" => vector.clone() }).unwrap();
        if mode == Maintenance::Engine {
            close(db);
            let mut options = DatabaseOpenOptions::default();
            options.maintenance = mode;
            options.clock = Some(Arc::new(At(Arc::new(AtomicU64::new(100)))));
            db = Database::open_with_options(&folder, options).unwrap();
        }
        let ids = erased
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        run(
            &db,
            &format!("PURGE FROM docs WHERE id IN ({ids}), secrets"),
        );
        close(db);
        let db = open(&folder);
        let after = snapshot(&db, &folder.join("after"));
        if mode == Maintenance::Caller {
            assert!(
                kept.iter().all(|(k, v)| after.get(k) == Some(v)),
                "unselected build or Left changed"
            );
            for (k, v) in before
                .iter()
                .filter(|(k, _)| [3, 8].contains(&k[0]) && under(k) == under(&group_blocks[0]))
            {
                if k[0] == 8 {
                    assert_ne!(after[k], *v, "the mean changed");
                    continue;
                }
                assert!(
                    after.get(k).is_some_and(|now| now.len() == v.len()
                        && (0..(v.len() - 20) / 8).all(|g| {
                            let empty = |at| {
                                members
                                    .iter()
                                    .filter(|(_, _, g)| *g == at)
                                    .all(|(row, _, _)| erased.contains(&id_of(row)))
                            };
                            &now[12 + 8 * g..20 + 8 * g]
                                == if g == 1 && empty(0) {
                                    &[0; 8]
                                } else if g > 0 && empty(g) {
                                    &now[20 + 8 * g..28 + 8 * g]
                                } else {
                                    &v[12 + 8 * g..20 + 8 * g]
                                }
                        })),
                    "group count, live starts and emptied boundaries"
                );
            }
        }
        assert_eq!(
            int(&db, "SELECT COUNT(*) FROM docs"),
            22260 - erased.len() as i64
        );
        assert_eq!(int(&db, "SELECT COUNT(*) FROM secrets"), 0);
        for (key, value) in &after {
            if [0, 4, 6, 7, 8].contains(&key[0]) {
                assert_ne!(under(key), build, "abandoned build record remains");
            }
            for id in &erased {
                let row = key_of(*id);
                assert!(
                    !([2, 9].contains(&key[0]) && key.ends_with(&row)),
                    "pending or Left retains erased key"
                );
                if key[0] == 0 {
                    assert!(slots(value).iter().all(|(k, _)| *k != row));
                }
            }
        }
        for (key, block) in before
            .iter()
            .filter(|(k, _)| k[0] == 0 && under(k) != build)
        {
            let kept = slots(
                after
                    .get(key)
                    .expect("a surviving partition keeps its blocks"),
            );
            let old = slots(block);
            assert_eq!(old.len(), kept.len());
            for ((row, slot), kept) in old.into_iter().zip(kept) {
                if !row.is_empty() && !erased.contains(&id_of(row)) {
                    assert_eq!((row, slot), kept, "neighbour's vector slot changed");
                }
            }
        }
        assert_derived(&after);
        let exported = fs::read(folder.join("after/data")).unwrap();
        for needle in [
            marker.as_bytes().to_vec(),
            vector.iter().flat_map(|x| x.to_le_bytes()).collect(),
        ] {
            assert!(
                !exported.windows(needle.len()).any(|w| w == needle),
                "export retains erased bytes"
            );
        }
        let delivered = Arc::new(Mutex::new(0));
        let seen = delivered.clone();
        db.register_sink("absent", None, move |_| {
            *seen.lock().unwrap() += 1;
            Ok(())
        })
        .unwrap();
        db.complete_initialization().unwrap();
        maintenance(&db);
        assert_eq!(*delivered.lock().unwrap(), 0, "erased event was delivered");
        assert_derived(&snapshot(&db, &folder.join("regrouped")));
        run(&db, "PURGE FROM docs");
        let empty = snapshot(&db, &folder.join("empty"));
        assert!(
            empty.is_empty(),
            "empty partitions retain records: {empty:?}"
        );
    }
}

pub(super) fn purging(role: Role) {
    let (db, calls) = open_stopping(&role);
    say(format!("purging {}", calls.load(Ordering::SeqCst)));
    run(&db, "PURGE FROM t WHERE id <= 3, other WHERE id >= 3");
    say("purged");
    close(db);
    say(format!("calls {}", calls.load(Ordering::SeqCst)));
}

pub(super) fn kill_workload(test: &str) {
    let store = |k| {
        let folder = store_with_rows(&format!("purge-kill-{k}"), 5);
        let db = open(&folder);
        run(&db, "CREATE TABLE other (id INTEGER PRIMARY KEY)");
        run(&db, "INSERT INTO other VALUES (1), (2), (3), (4), (5)");
        close(db);
        folder
    };
    let (begins, calls, _) = learned(test, "purger", &store(0), Some("purging"));
    assert!(calls > begins);
    for_each_stop(
        test,
        "purger",
        begins + 1..=calls,
        store(1),
        |_, folder, before| {
            let db = open(folder);
            let rows = int(&db, "SELECT COUNT(*) FROM t");
            let other = int(&db, "SELECT COUNT(*) FROM other");
            assert!(
                (rows, other) == (2, 2)
                    || ((rows, other) == (5, 5) && !before.iter().any(|s| s == "purged"))
            );
            assert_rows_are(&db, if rows == 2 { 4..=5 } else { 1..=5 });
            db.verify().unwrap();
            close(db);
            let data = folder.join("data");
            let mut bytes = fs::read(&data).unwrap();
            let newest = 64 + usize::from(bytes[9] & 1) * 128;
            bytes[newest + 112] ^= 1;
            fs::write(&data, bytes).unwrap();
            assert!(
                matches!(
                    open_with(folder, DatabaseOpenOptions::default()),
                    Err(Error::StoreCorrupted { .. })
                ),
                "checksum damage must return StoreCorrupted and leave no handle to read purged rows"
            );
        },
    );
}
