//! Retention cycles examine a fixed candidate window and selected-node edges;
//! unrelated history cannot enlarge one cycle or hide future-stamped rows.
use crate::resident_history_support as support;
use contextdb_core::{Direction, Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

const NOW: u64 = 1_800_000_000_000;
fn open(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            memory_limit: Some(64 << 20),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn node(db: &Database, id: u128) {
    db.execute(
        "INSERT INTO retained_nodes VALUES ($id)",
        &HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(id)))]),
    )
    .unwrap();
}
fn assert_edges(db: &Database, unrelated: u128, expired: bool) {
    let snapshot = db.snapshot();
    for (source, target, edge_type, present) in
        [(1, 2, "LINKS", !expired), (3, 1, "LINKS", !expired)]
            .into_iter()
            .chain((100..100 + unrelated).map(|id| (id, 1_000_000 + id, "UNRELATED", true)))
    {
        let source = Uuid::from_u128(source);
        let target = Uuid::from_u128(target);
        assert_eq!(
            db.get_edge_properties(source, target, edge_type, snapshot)
                .unwrap(),
            present.then(HashMap::new),
            "exact durable edge {source}->{target}"
        );
        assert_eq!(
            db.edge_count(source, edge_type, snapshot).unwrap(),
            usize::from(present)
        );
        let types = [edge_type.to_owned()];
        for (node, direction, opposite) in [
            (source, Direction::Outgoing, target),
            (target, Direction::Incoming, source),
        ] {
            let mut ids = db
                .query_bfs(node, Some(&types), direction, 1, snapshot)
                .unwrap()
                .nodes
                .into_iter()
                .map(|node| node.id)
                .collect::<Vec<_>>();
            ids.sort();
            assert_eq!(
                ids,
                if present { vec![opposite] } else { Vec::new() },
                "exact visible adjacency {direction:?} for {node}"
            );
        }
    }
}
#[test]
fn retention_expiry_and_prune_examine_fixed_work_as_unrelated_edges_and_rows_grow() {
    let _clock = Wallclock::test_clock_guard(|| NOW);
    for expire in [false, true] {
        for future in [false, true] {
            let mut first_cycles = Vec::new();
            let mut later_graph_cycles = Vec::new();
            for unrelated in [64_u128, 256] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("retention.redb");
                let db = open(&path);
                db.execute(
                    "CREATE TABLE retained_nodes (id UUID PRIMARY KEY) RETAIN 1 HOURS SYNC OFF",
                    &HashMap::new(),
                )
                .unwrap();
                {
                    let _selected_clock =
                        Wallclock::test_clock_guard(
                            move || if expire { NOW - 7_200_000 } else { NOW },
                        );
                    node(&db, 1);
                }
                node(&db, 2);
                node(&db, 3);
                let tx = db.begin_or_panic();
                db.insert_edge(
                    tx,
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                    "LINKS".into(),
                    HashMap::new(),
                )
                .unwrap();
                db.insert_edge(
                    tx,
                    Uuid::from_u128(3),
                    Uuid::from_u128(1),
                    "LINKS".into(),
                    HashMap::new(),
                )
                .unwrap();
                db.commit(tx).unwrap();
                // Both populations exceed the finite expiry window; fixed live
                // controls keep a short table from masquerading as growth.
                for id in 10_000..10_064 {
                    node(&db, id);
                }
                for id in 100..100 + unrelated {
                    node(&db, id);
                    let tx = db.begin_or_panic();
                    db.insert_edge(
                        tx,
                        Uuid::from_u128(id),
                        Uuid::from_u128(1_000_000 + id),
                        "UNRELATED".into(),
                        HashMap::new(),
                    )
                    .unwrap();
                    db.commit(tx).unwrap();
                }
                if future {
                    let _future_clock = Wallclock::test_clock_guard(|| NOW + 3_600_000);
                    node(&db, 9_000_000);
                }
                db.close().unwrap();
                drop(db);
                let db = open(&path);
                // This fixture has no foreign-key dependency chain. Each
                // phase admits one candidate window, scans it for candidates,
                // performs at most one protection pass, and mutates it. The
                // aggregate also includes two frozen-tail probes and two row
                // lifetime probes for the one selected node. Future recount
                // adds its independent 32-row window and frozen-tail probe.
                let cycle_bound = 4 * (128 + 32) + 2 + 2 + if future { 32 + 1 } else { 0 };
                let (report, work) = observe(|| db.run_pruning_cycle_checked());
                let mut report = report.unwrap();
                first_cycles.push((work.retention_keys_examined, work.graph_keys_examined));
                assert!(
                    work.retention_keys_examined > 0 && work.retention_keys_examined <= cycle_bound,
                    "actual finite work: {work:?}"
                );
                if expire {
                    assert!(
                        work.graph_keys_examined > 0,
                        "selected node probes its real edges"
                    );
                } else {
                    assert_eq!(
                        work.graph_keys_examined, 0,
                        "empty expiry selection does no graph work"
                    );
                }
                // Complete the finite future-stamp recount before requiring its
                // exact completed tally. Each cycle still uses the same cap.
                let mut graph_cycles = Vec::new();
                for cycle in 0..32 {
                    let (next, work) = observe(|| db.run_pruning_cycle_checked());
                    report = next.unwrap();
                    graph_cycles.push(work.graph_keys_examined);
                    assert!(
                        work.retention_keys_examined <= cycle_bound,
                        "finite resumed work: {work:?}"
                    );
                }
                later_graph_cycles.push(graph_cycles);
                assert_edges(&db, unrelated, expire);
                assert_eq!(report.future_dated_rows, u64::from(future));
                assert_eq!(
                    report.future_dated_tables,
                    if future {
                        vec!["retained_nodes".to_string()]
                    } else {
                        Vec::new()
                    }
                );
                let selected = db
                    .execute(
                        "SELECT id FROM retained_nodes WHERE id = $id",
                        &HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(1)))]),
                    )
                    .unwrap();
                assert_eq!(selected.rows.len(), usize::from(!expire));
                assert_eq!(
                    db.execute("SELECT COUNT(*) FROM retained_nodes", &HashMap::new())
                        .unwrap()
                        .rows,
                    vec![vec![Value::Int64(
                        (unrelated + 64 + 2 + u128::from(!expire) + u128::from(future)) as i64
                    )]]
                );
                db.close().unwrap();
                drop(db);
                let db = open(&path);
                let report = db.run_pruning_cycle_checked().unwrap();
                assert_edges(&db, unrelated, expire);
                assert_eq!(
                    report.future_dated_rows,
                    u64::from(future),
                    "completed exact recount persists through reopen"
                );
                db.close().unwrap();
            }
            assert_eq!(
                later_graph_cycles[0], later_graph_cycles[1],
                "every later cycle has fixed graph work: expire={expire} future={future}"
            );
            assert_eq!(
                first_cycles[0], first_cycles[1],
                "expiry/prune work must not grow with unrelated rows or edges: expire={expire} future={future}"
            );
        }
    }
}

fn ids(db: &contextdb_engine::Database, table: &str) -> BTreeSet<i64> {
    support::exec(db, &format!("SELECT id FROM {table} ORDER BY id"))
        .into_iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref value => panic!("expected integer identity, got {value:?}"),
        })
        .collect()
}
#[test]
fn retention_reclaims_both_expired_foreign_key_ends_across_finite_windows() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("expiry-cohorts.redb");
    let db = support::open(&path, None);
    support::exec(
        &db,
        "CREATE TABLE cohort_parents (id INT PRIMARY KEY, expiry TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    support::exec(
        &db,
        "CREATE TABLE cohort_children (id INT PRIMARY KEY, parent INT REFERENCES cohort_parents(id), expiry TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    for id in 0..130 {
        db.execute(
            "INSERT INTO cohort_parents VALUES ($id,$expiry)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("expiry".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO cohort_children VALUES ($id,$id,$expiry)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("expiry".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    // The same declared TTL applies to this child, but its future expiry keeps
    // the already-expired parent protected throughout every candidate window.
    db.execute(
        "INSERT INTO cohort_parents VALUES (1000,$expiry)",
        &HashMap::from([("expiry".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO cohort_children VALUES (1000,1000,$expiry)",
        &HashMap::from([("expiry".into(), Value::Timestamp(1_800_000_000_000))]),
    )
    .unwrap();
    let mut removed = 0;
    let mut saw_positive_work = false;
    for cycle in 0..64 {
        let (report, work) = observe(|| db.run_pruning_cycle_checked());
        let report = report.unwrap();
        assert!(
            report.pruned_rows <= 64,
            "finite deletion bound: {report:?}"
        );
        assert!(
            work.retention_keys_examined <= 512,
            "finite candidate bound: {work:?}"
        );
        saw_positive_work |= work.retention_keys_examined > 0;
        removed += report.pruned_rows;
        let parents = ids(&db, "cohort_parents");
        let children = ids(&db, "cohort_children");
        assert!(
            children.is_subset(&parents),
            "no surviving child loses its parent at cycle {cycle}"
        );
        assert!(
            parents.contains(&1000) && children.contains(&1000),
            "future child protects its expired parent"
        );
        if removed == 260 {
            break;
        }
    }
    assert!(saw_positive_work);
    assert_eq!(
        removed, 260,
        "both expired ends are eventually physically reclaimed"
    );
    for table in ["cohort_parents", "cohort_children"] {
        assert_eq!(ids(&db, table), BTreeSet::from([1000]));
        assert_eq!(db.__durable_version_count_for_test(table).unwrap(), 1);
    }
    db.close().unwrap();
    drop(db);
    let db = support::open(&path, None);
    for table in ["cohort_parents", "cohort_children"] {
        assert_eq!(ids(&db, table), BTreeSet::from([1000]));
        assert_eq!(db.__durable_version_count_for_test(table).unwrap(), 1);
    }
    db.close().unwrap();

    // Protection must propagate through expired C to P while unrelated U is reclaimed in the same window.
    let protected_path = dir.path().join("expiry-protected-chain-window.redb");
    let db = support::open(&protected_path, None);
    support::exec(
        &db,
        "CREATE TABLE protected_chain (id INT PRIMARY KEY, parent INT REFERENCES protected_chain(id), expiry TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    for (id, parent, expiry) in [
        (1, Value::Null, 100),
        (2, Value::Int64(1), 100),
        (3, Value::Int64(2), 1_800_000_000_000),
        (4, Value::Null, 100),
    ] {
        db.execute(
            "INSERT INTO protected_chain VALUES ($id, $parent, $expiry)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("parent".into(), parent),
                ("expiry".into(), Value::Timestamp(expiry)),
            ]),
        )
        .unwrap();
    }
    let before = ids(&db, "protected_chain");
    let physical_before = db
        .__durable_version_count_for_test("protected_chain")
        .unwrap();
    assert_eq!(before, BTreeSet::from([1, 2, 3, 4]));
    assert_eq!(physical_before, before.len());
    assert!(
        physical_before < 32,
        "the entire chain and unrelated row fit one existing candidate window"
    );
    let (report, work) = observe(|| db.run_pruning_cycle_checked());
    let report = report.expect("one protected chain must not refuse unrelated expired work");
    let after = ids(&db, "protected_chain");
    assert_eq!(after, BTreeSet::from([1, 2, 3]));
    assert_eq!(
        support::exec(&db, "SELECT parent FROM protected_chain WHERE id = 2"),
        vec![vec![Value::Int64(1)]],
        "the surviving expired child still resolves to its surviving parent"
    );
    assert_eq!(
        support::exec(&db, "SELECT parent FROM protected_chain WHERE id = 3"),
        vec![vec![Value::Int64(2)]],
        "the live grandchild still resolves to its surviving child"
    );
    assert_eq!(report.pruned_rows, (before.len() - after.len()) as u64);
    assert!(
        work.retention_keys_examined > 0,
        "the real candidate window was examined"
    );
    assert!(
        work.retention_keys_examined <= 512,
        "the inherited finite candidate bound holds: {work:?}"
    );
    assert_eq!(
        db.__durable_version_count_for_test("protected_chain")
            .unwrap(),
        after.len()
    );
    db.close().unwrap();

    // A mutual expired pair needs cohort carry when live physical fillers keep its ends in different windows.
    let split_path = dir.path().join("expiry-split-mutual-cycle.redb");
    let db = support::open(&split_path, None);
    support::exec(
        &db,
        "CREATE TABLE split_cycle (id INT PRIMARY KEY, peer INT REFERENCES split_cycle(id), expiry TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF",
    );
    db.execute(
        "INSERT INTO split_cycle VALUES (1, NULL, $expiry)",
        &HashMap::from([("expiry".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    let filler_ids: BTreeSet<i64> = (2..=34).collect();
    for id in &filler_ids {
        db.execute(
            "INSERT INTO split_cycle VALUES ($id, NULL, $expiry)",
            &HashMap::from([
                ("id".into(), Value::Int64(*id)),
                ("expiry".into(), Value::Timestamp(1_800_000_000_000)),
            ]),
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO split_cycle VALUES (35, 1, $expiry)",
        &HashMap::from([("expiry".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    support::exec(&db, "UPDATE split_cycle SET peer = 35 WHERE id = 1");
    let first = db
        .point_lookup("split_cycle", "id", &Value::Int64(1), db.snapshot())
        .unwrap()
        .unwrap();
    let last = db
        .point_lookup("split_cycle", "id", &Value::Int64(35), db.snapshot())
        .unwrap()
        .unwrap();
    assert!(filler_ids.len() > 32);
    for id in &filler_ids {
        let filler = db
            .point_lookup("split_cycle", "id", &Value::Int64(*id), db.snapshot())
            .unwrap()
            .unwrap();
        assert!(first.row_id < filler.row_id && filler.row_id < last.row_id);
        assert_eq!(filler.values["expiry"], Value::Timestamp(1_800_000_000_000));
    }
    for _ in 0..64 {
        db.run_pruning_cycle_checked().unwrap();
        let alive = ids(&db, "split_cycle");
        assert!(
            filler_ids.is_subset(&alive),
            "live fillers must keep the physical boundary separated"
        );
        assert_eq!(
            alive.contains(&1),
            alive.contains(&35),
            "neither live endpoint loses its foreign-key parent"
        );
        if alive == filler_ids {
            break;
        }
    }
    assert_eq!(
        ids(&db, "split_cycle"),
        filler_ids,
        "cohort carry reclaims both separated expired ends in finite windows"
    );
    assert_eq!(
        db.__durable_version_count_for_test("split_cycle").unwrap(),
        filler_ids.len()
    );
    db.close().unwrap();
}
