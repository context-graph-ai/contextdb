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

/// A PURGE erases what rows deleted earlier from a table that keeps no copy of them left
/// behind, with compaction off so no build drops it: a queued delivery when the row it holds
/// passes the condition and the reader's sight, and then that row's sealed vector; with no
/// condition, every such delivery and vector. Row 1 was written with tag B, deleted, written
/// with tag A and deleted; row 2 (B) and row 3 (A, in a context the narrowed reader is not in)
/// were deleted A search code holds no value, so a reader who sees every row clears every one
/// whose row is gone; a commit subscriber is told of each. A list too long to hold in memory is
/// read from the store for each delivery.
#[test]
fn a_purge_erases_what_rows_deleted_earlier_left_behind_that_it_selects() {
    // Each case: its name, its PURGEs (narrowed or not), the rows they take, whether rows 1
    // to 3 still name a vector, and the deliveries of rows 1 to 3 still sent.
    type Case<'a> = (
        &'a str,
        &'a [(bool, &'a str)],
        i64,
        [bool; 3],
        &'a [(i64, &'a str)],
    );
    let cases: [Case; 4] = [
        ("all", &[(false, "PURGE FROM docs")], 40, [false; 3], &[]),
        (
            "tag",
            &[(false, "PURGE FROM docs WHERE tag = 'A'")],
            20,
            [false; 3],
            &[(1, "B"), (2, "B")],
        ),
        (
            "spilled",
            &[(false, "PURGE FROM docs WHERE id IN (SELECT n FROM many)")],
            40,
            [false; 3],
            &[],
        ),
        (
            "key-and-tag",
            &[
                (true, "PURGE FROM docs WHERE id = 1 AND tag = 'A'"),
                (true, "PURGE FROM docs WHERE id = 3 AND tag = 'A'"),
            ],
            1,
            [false, true, true],
            &[(1, "B"), (2, "B"), (3, "A")],
        ),
    ];
    for (name, purges, rows, kept, gone_sent) in cases {
        let folder = support::folder(&format!("purge-earlier-{name}"));
        let db = open(&folder);
        run(
            &db,
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, tag TEXT NOT NULL, cx UUID CONTEXT_ID, \
             v VECTOR(16) INDEXING_THRESHOLD 1000 COMPACTION NONE) HISTORY CURRENT ONLY SYNC OFF",
        );
        run(&db, "CREATE EVENT TYPE added WHEN INSERT ON docs");
        run(&db, "CREATE SINK absent TYPE callback");
        run(&db, "CREATE ROUTE notify EVENT added TO absent");
        run(&db, "CREATE TABLE many (n INTEGER PRIMARY KEY)");
        let many: Vec<_> = (1..=10_000).map(|n| format!("({n})")).collect();
        run(&db, &format!("INSERT INTO many VALUES {}", many.join(", ")));
        let _told = db.subscribe().unwrap();
        let put = |id: i64, tag: &str| {
            let cx = context(if id == 3 { 2 } else { 1 });
            let given = params! { "id" => id, "tag" => tag, "cx" => cx, "v" => gathered(id, DIM) };
            db.execute("INSERT INTO docs VALUES ($id, $tag, $cx, $v)", &given)
                .unwrap();
        };
        put(1, "B");
        run(&db, "DELETE FROM docs WHERE id = 1");
        for id in 1..=40 {
            put(id, if id % 2 == 1 { "A" } else { "B" });
        }
        seal_all(&db);
        run(&db, "DELETE FROM docs WHERE id IN (1, 2, 3)");
        let narrowed = in_contexts(&db, &[1]);
        let affected = purges.iter().map(|(narrow, sql)| {
            run(if *narrow { &narrowed } else { &db }, sql).rows_affected() as i64
        });
        assert_eq!(affected.sum::<i64>(), rows, "{name}: each key taken, once");
        let live = int(&db, "SELECT COUNT(*) FROM docs") as usize;
        drop(narrowed);
        let db = reopen(db, &folder);
        let all = snapshot(&db, &folder.join("export"));
        let names = |id| {
            let left = all
                .keys()
                .any(|k| [9, 37].contains(&k[0]) && k.ends_with(&key_of(id)));
            let blocks = all.iter().filter(|(k, _)| k[0] == 0);
            left || blocks
                .flat_map(|(_, v)| slots(v))
                .any(|(k, _)| k == key_of(id))
        };
        assert_eq!([1, 2, 3].map(names), kept, "{name}: vectors left behind");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let seen = sent.clone();
        db.register_sink("absent", None, move |event| {
            let Value::Int64(id) = event.row_values["id"] else {
                panic!("an id")
            };
            let tag = format!("{:?}", event.row_values["tag"]);
            seen.lock().unwrap().push((id, tag));
            Ok(())
        })
        .unwrap();
        while db.run_maintenance_cycle().unwrap().work_remaining == Some(true) {}
        let sent = sent.lock().unwrap();
        let gone: Vec<_> = sent.iter().filter(|(id, _)| *id <= 3).cloned().collect();
        let wanted: Vec<_> = gone_sent
            .iter()
            .map(|(id, tag)| (*id, format!("{:?}", Value::Text((*tag).into()))))
            .collect();
        assert_eq!(gone, wanted, "{name}: deliveries of rows deleted earlier");
        assert_eq!(
            sent.len() - gone.len(),
            live,
            "{name}: deliveries of rows there"
        );
    }
}

/// A PURGE follows every generation that follows a parent's erase, through live rows, kept
/// versions and queued deliveries, as a union with a clause that names a follower table:
/// grandchild 100 of child 10, both deleted earlier, goes with parent 1, and 200 of parent 2
/// stays. A parent deleted earlier is decided by its kept version and takes its children's.
/// Each key is counted once. A table named twice, or with no declared key, is refused first.
#[test]
fn a_purge_follows_every_generation_and_every_clause() {
    for (name, history) in [("delivered", "HISTORY CURRENT ONLY"), ("kept", "")] {
        let folder = support::folder(&format!("purge-generations-{name}"));
        let db = open(&folder);
        let table = |name: &str, columns: &str| {
            format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY{columns}) {history} SYNC OFF")
        };
        let follows = |parent| format!(", up INTEGER REFERENCES {parent}(id) FOLLOWS PARENT PURGE");
        run(&db, &table("parents", ""));
        run(&db, &table("children", &follows("parents")));
        run(&db, &table("grandchildren", &follows("children")));
        run(&db, "CREATE SINK absent TYPE callback");
        for (event, table) in [("born", "children"), ("hatched", "grandchildren")] {
            run(
                &db,
                &format!("CREATE EVENT TYPE {event} WHEN INSERT ON {table}"),
            );
            run(
                &db,
                &format!("CREATE ROUTE {event}_to EVENT {event} TO absent"),
            );
        }
        for sql in [
            "INSERT INTO parents VALUES (1), (2), (3)",
            "INSERT INTO children VALUES (10, 1), (11, 1), (20, 2), (30, 3)",
            "INSERT INTO grandchildren VALUES (100, 10), (110, 11), (200, 20)",
            "DELETE FROM grandchildren WHERE id IN (100, 200)",
            "DELETE FROM children WHERE id IN (10, 20, 30)",
            "DELETE FROM parents WHERE id = 3",
        ] {
            run(&db, sql);
        }
        let purge = "PURGE FROM parents WHERE id = 1, children WHERE id = 999";
        assert_eq!(
            run(&db, purge).rows_affected(),
            5,
            "{name}: 1, 10, 11, 100, 110"
        );
        // Parent 3 has a copy only where versions are kept.
        let taken = run(&db, "PURGE FROM parents WHERE id = 3").rows_affected();
        let kept = history.is_empty();
        assert_eq!(taken, if kept { 2 } else { 0 }, "{name}: 3 and 30");
        let child = db.row_versions("children", &[Value::Int64(30)], 4).unwrap();
        assert!(
            child.rows.rows().is_empty(),
            "{name}: child 30 keeps no version"
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let seen = sent.clone();
        db.register_sink("absent", None, move |event| {
            let Value::Int64(id) = event.row_values["id"] else {
                panic!("an id")
            };
            seen.lock().unwrap().push(id);
            Ok(())
        })
        .unwrap();
        while db.run_maintenance_cycle().unwrap().work_remaining == Some(true) {}
        let mut sent = sent.lock().unwrap().clone();
        sent.sort();
        let left: &[i64] = if kept { &[20, 200] } else { &[20, 30, 200] };
        assert_eq!(sent, left, "{name}: deliveries left");
        run(&db, "CREATE TABLE loose (n INTEGER)");
        for (sql, reason) in [
            (
                "PURGE FROM parents, parents",
                "table parents is listed twice",
            ),
            (
                "PURGE FROM loose",
                "table loose has no declared erasure key",
            ),
        ] {
            let refused = db.execute(sql, &params! {}).unwrap_err().to_string();
            assert_eq!(refused, format!("schema invalid: {reason}"), "{name}");
        }
        assert_eq!(
            int(&db, "SELECT COUNT(*) FROM parents"),
            1,
            "{name}: refused whole"
        );
    }
}

/// A parent deleted earlier follows only values no row holds now in any context, and every
/// value its copies name: a 500-byte name reused by a parent the reader cannot see keeps that
/// parent's child, and names A and B of one parent's two queued deliveries both take their
/// children's. A condition that names a key reads no other key's queued body: an 8 MiB one of
/// key 2 is passed by within 16 MiB.
#[test]
fn a_purge_follows_each_value_no_row_holds_and_reads_only_the_keys_it_names() {
    let folder = support::folder("purge-follow-values");
    let db = open(&folder);
    let long = letters(500, 500);
    for sql in [
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT UNIQUE, cx UUID CONTEXT_ID) \
         HISTORY CURRENT ONLY SYNC OFF",
        "CREATE TABLE children (id INTEGER PRIMARY KEY, \
         up TEXT REFERENCES parents(name) FOLLOWS PARENT PURGE) HISTORY CURRENT ONLY SYNC OFF",
        "CREATE TABLE bodies (id INTEGER PRIMARY KEY, up TEXT REFERENCES parents(name) FOLLOWS PARENT PURGE, body TEXT) HISTORY CURRENT ONLY SYNC OFF",
        "CREATE SINK absent TYPE callback",
    ] {
        run(&db, sql);
    }
    for (event, when) in [
        ("named", "INSERT ON parents"),
        ("renamed", "UPDATE ON parents"),
        ("born", "INSERT ON children"),
        ("written", "INSERT ON bodies"),
    ] {
        run(&db, &format!("CREATE EVENT TYPE {event} WHEN {when}"));
        run(
            &db,
            &format!("CREATE ROUTE {event}_to EVENT {event} TO absent"),
        );
    }
    let parent = |id: i64, name: &str, cx: u128| {
        let given = params! { "id" => id, "name" => name, "cx" => context(cx) };
        db.execute("INSERT INTO parents VALUES ($id, $name, $cx)", &given)
            .unwrap();
    };
    let child = |id: i64, up: &str| {
        let given = params! { "id" => id, "up" => up };
        db.execute("INSERT INTO children VALUES ($id, $up)", &given)
            .unwrap();
    };
    parent(1, &long, 1);
    run(&db, "DELETE FROM parents WHERE id = 1");
    parent(2, &long, 2);
    child(20, &long);
    parent(3, "A", 1);
    child(30, "A");
    run(&db, "DELETE FROM children WHERE id = 30");
    run(&db, "UPDATE parents SET name = 'B' WHERE id = 3");
    child(31, "B");
    run(&db, "DELETE FROM children WHERE id = 31");
    run(&db, "DELETE FROM parents WHERE id = 3");
    let body = support::large_text(support::Kind::Scattered, 2, 8 << 20);
    db.execute(
        "INSERT INTO bodies VALUES (2, NULL, $body)",
        &params! { "body" => body },
    )
    .unwrap();
    run(&db, "DELETE FROM bodies WHERE id = 2");
    let narrowed = in_contexts(&db, &[1]);
    let taken = run(&narrowed, "PURGE FROM parents WHERE id IN (1, 3)").rows_affected();
    assert_eq!(taken, 4, "parents 1 and 3, and children 30 and 31");
    assert_eq!(int(&db, "SELECT COUNT(*) FROM children WHERE id = 20"), 1);
    drop(narrowed);
    db.set_memory_limit(Some(16 << 20)).unwrap();
    for purge in [
        "PURGE FROM bodies WHERE id = 1",
        "PURGE FROM bodies WHERE id = 999",
    ] {
        assert_eq!(run(&db, purge).rows_affected(), 0, "{purge}");
    }
    db.set_memory_limit(None).unwrap();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let seen = sent.clone();
    db.register_sink("absent", None, move |event| {
        let Value::Int64(id) = event.row_values["id"] else {
            panic!("an id")
        };
        seen.lock().unwrap().push(id);
        Ok(())
    })
    .unwrap();
    while db.run_maintenance_cycle().unwrap().work_remaining == Some(true) {}
    let mut sent = sent.lock().unwrap().clone();
    sent.sort();
    assert_eq!(
        sent,
        [2, 2, 20],
        "deliveries left: parent 2, body 2, child 20"
    );
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
