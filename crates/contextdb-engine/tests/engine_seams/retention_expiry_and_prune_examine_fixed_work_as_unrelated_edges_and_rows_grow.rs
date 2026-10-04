//! Retention cycles examine a fixed candidate window and selected-node edges;
//! unrelated history cannot enlarge one cycle or hide future-stamped rows.
use contextdb_core::{Direction, Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
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
                let (report, work) = observe(|| db.run_pruning_cycle_checked());
                let mut report = report.unwrap();
                eprintln!(
                    "retention unrelated={unrelated} expire={expire} future={future} first_work={work:?} report={report:?}"
                );
                first_cycles.push((work.retention_keys_examined, work.graph_keys_examined));
                assert!(
                    work.retention_keys_examined > 0 && work.retention_keys_examined <= 512,
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
                    eprintln!(
                        "retention unrelated={unrelated} expire={expire} future={future} later_cycle={cycle} work={work:?}"
                    );
                    assert!(
                        work.retention_keys_examined <= 512,
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
