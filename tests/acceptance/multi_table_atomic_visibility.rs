use contextdb_core::{Direction, SnapshotId, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// RED — t21_01
#[test]
fn t21_01_concurrent_reader_never_sees_torn_three_paradigm_commit() {
    use contextdb_core::RowId;
    use std::sync::mpsc;
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    // Pre-fixture: source entity AND target entity exist with row_ids the
    // reader will read directly. The writer's tx then UPDATEs the target's
    // body, INSERTs the edge, and inserts a NEW vector for the target row.
    // This decouples the three torn-state probes (relational column update
    // vs edge presence vs new-vector indexing): all three can be checked
    // independently using the pre-stamped target_row_id, so a torn read is
    // not gated on the relational visibility of the target row.
    let src_id = Uuid::from_u128(1);
    let target_uuid = Uuid::from_u128(99);
    let mut p = HashMap::new();
    p.insert("sid".into(), Value::Uuid(src_id));
    p.insert("tid".into(), Value::Uuid(target_uuid));
    p.insert("v0".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($sid, 'source', $v0)",
        &p,
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($tid, 'pre', $v0)",
        &p,
    )
    .unwrap();
    let scan = db.scan("entities", db.snapshot()).unwrap();
    let target_row_id: RowId = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_uuid))
        .unwrap()
        .row_id;

    let pause = db.pause_after_relational_apply_for_test();

    // Writer: single tx touching all three subsystems.
    let writer_db = db.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let mut writer = Some(thread::spawn(move || {
        let mut p = HashMap::new();
        p.insert("tid".into(), Value::Uuid(target_uuid));
        p.insert("eid".into(), Value::Uuid(Uuid::from_u128(50)));
        p.insert("src".into(), Value::Uuid(src_id));
        p.insert("vnew".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
        writer_db.execute("BEGIN", &empty()).unwrap();
        writer_db
            .execute(
                "UPDATE entities SET name = 'target', embedding = $vnew WHERE id = $tid",
                &p,
            )
            .unwrap();
        writer_db
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) \
             VALUES ($eid, $src, $tid, 'LINKS')",
                &p,
            )
            .unwrap();
        writer_db.execute("COMMIT", &empty()).unwrap();
        done_tx.send(()).unwrap();
    }));

    let pause_reached = pause.wait_until_reached(Duration::from_secs(2));
    if !pause_reached {
        pause.release();
        writer.take().unwrap().join().unwrap();
    }
    assert!(
        pause_reached,
        "test pause must stop the writer after relational apply and before graph/vector apply"
    );
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        done_rx.try_recv().is_err(),
        "writer must remain blocked while apply-phase pause guard is held"
    );

    // White-box phase proof for the deterministic pause seam: the relational
    // physical row has been applied, but graph/vector have not. The public
    // snapshot below must still hide all three until the tx is fully applied.
    let raw_phase_snapshot = SnapshotId::from_raw_wire(u64::MAX);
    let raw_phase_entities = db.scan("entities", raw_phase_snapshot).unwrap();
    let raw_phase_has_renamed = raw_phase_entities.iter().any(|r| {
        matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_uuid)
            && matches!(r.values.get("name"), Some(Value::Text(s)) if s == "target")
    });
    let raw_phase_has_edge = db
        .edge_count(src_id, "LINKS", raw_phase_snapshot)
        .unwrap_or(0)
        > 0;
    let raw_phase_has_new_vec = db
        .live_vector_entry(target_row_id, raw_phase_snapshot)
        .is_some_and(|entry| entry.vector == vec![0.0, 1.0, 0.0]);
    assert_eq!(
        (
            raw_phase_has_renamed,
            raw_phase_has_edge,
            raw_phase_has_new_vec
        ),
        (true, false, false),
        "pause seam must be after relational apply and before graph/vector apply"
    );

    // Deterministic mid-commit probe: while the writer is paused after the
    // relational apply, the public snapshot frontier must still expose NONE of
    // the tx. A bad implementation that advances visibility per paradigm will
    // expose the relational rename here.
    let snap = db.snapshot();
    let entities = db.scan("entities", snap).unwrap();
    let has_renamed = entities.iter().any(|r| {
        matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_uuid)
            && matches!(r.values.get("name"), Some(Value::Text(s)) if s == "target")
    });
    let has_edge = db.edge_count(src_id, "LINKS", snap).unwrap_or(0) > 0;
    let has_new_vec = db
        .live_vector_entry(target_row_id, snap)
        .is_some_and(|entry| entry.vector == vec![0.0, 1.0, 0.0]);
    assert_eq!(
        (has_renamed, has_edge, has_new_vec),
        (false, false, false),
        "mid-commit snapshot must see none of the paused tx"
    );

    pause.release();
    writer.take().unwrap().join().unwrap();
    done_rx
        .try_recv()
        .expect("writer must complete after pause release");

    let snap = db.snapshot();
    let entities = db.scan("entities", snap).unwrap();
    assert!(
        entities.iter().any(|r| {
            matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_uuid)
                && matches!(r.values.get("name"), Some(Value::Text(s)) if s == "target")
        }),
        "relational rename must be visible after the full tx applies"
    );
    assert_eq!(
        db.edge_count(src_id, "LINKS", snap).unwrap_or(0),
        1,
        "graph edge must be visible after the full tx applies"
    );
    assert_eq!(
        db.live_vector_entry(target_row_id, snap)
            .expect("target row has live vector after commit")
            .vector,
        vec![0.0, 1.0, 0.0],
        "new vector must be visible after the full tx applies"
    );

    use contextdb_core::Lsn;
    use contextdb_engine::sync_types::{
        ChangeSet, ConflictPolicies, ConflictPolicy, EdgeChange, NaturalKey, RowChange,
        VectorChange,
    };
    let sync_db = Arc::new(Database::open_memory());
    sync_db
        .execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
    sync_db
        .execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        )
        .unwrap();
    let sync_source = Uuid::from_u128(0x2101);
    let sync_target = Uuid::from_u128(0x2102);
    let remote_row_id = RowId(0x2102);
    let mut sync_values = HashMap::new();
    sync_values.insert("id".into(), Value::Uuid(sync_target));
    sync_values.insert("name".into(), Value::Text("sync-target".into()));
    sync_values.insert("embedding".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    let sync_changes = ChangeSet {
        rows: vec![RowChange {
            table: "entities".into(),
            natural_key: NaturalKey {
                column: "id".into(),
                value: Value::Uuid(sync_target),
            },
            values: sync_values,
            deleted: false,
            lsn: Lsn(10),
        }],
        edges: vec![EdgeChange {
            source: sync_source,
            target: sync_target,
            edge_type: "LINKS".into(),
            properties: HashMap::new(),
            lsn: Lsn(10),
        }],
        vectors: vec![VectorChange {
            index: VectorIndexRef::new("entities", "embedding"),
            row_id: remote_row_id,
            vector: vec![0.0, 1.0, 0.0],
            lsn: Lsn(10),
        }],
        ddl: Vec::new(),
    };
    let sync_pause = sync_db.pause_after_relational_apply_for_test();
    let mut sync_writer = Some({
        let sync_db = sync_db.clone();
        thread::spawn(move || {
            sync_db
                .apply_changes(
                    sync_changes,
                    &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
                )
                .unwrap();
        })
    });
    let sync_pause_reached = sync_pause.wait_until_reached(Duration::from_secs(2));
    if !sync_pause_reached {
        sync_pause.release();
        sync_writer.take().unwrap().join().unwrap();
    }
    assert!(
        sync_pause_reached,
        "sync apply must use the same atomic apply boundary and pause seam as local commits"
    );
    let sync_mid_snapshot = sync_db.snapshot();
    assert_eq!(
        sync_db.scan("entities", sync_mid_snapshot).unwrap().len(),
        0,
        "sync apply mid-commit snapshot must hide the relational row"
    );
    assert_eq!(
        sync_db
            .edge_count(sync_source, "LINKS", sync_mid_snapshot)
            .unwrap_or(0),
        0,
        "sync apply mid-commit snapshot must hide the graph edge"
    );
    let sync_mid_hits = sync_db
        .query_vector(
            VectorIndexRef::new("entities", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            sync_mid_snapshot,
        )
        .unwrap();
    assert!(
        sync_mid_hits.is_empty(),
        "sync apply mid-commit snapshot must hide the vector entry; hits={sync_mid_hits:?}"
    );
    sync_pause.release();
    sync_writer.take().unwrap().join().unwrap();
    let sync_snap = sync_db.snapshot();
    let sync_rows = sync_db.scan("entities", sync_snap).unwrap();
    let sync_row = sync_rows
        .iter()
        .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == sync_target))
        .expect("sync-applied relational row must be visible after release");
    assert_eq!(
        sync_db
            .edge_count(sync_source, "LINKS", sync_snap)
            .unwrap_or(0),
        1,
        "sync-applied graph edge must become visible with the row"
    );
    let sync_hits = sync_db
        .query_vector(
            VectorIndexRef::new("entities", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            sync_snap,
        )
        .unwrap();
    assert_eq!(
        sync_hits.first().map(|(row_id, _)| *row_id),
        Some(sync_row.row_id),
        "sync-applied vector must become visible with the row and graph edge; hits={sync_hits:?}"
    );
}

/// REGRESSION GUARD — t21_02
#[test]
fn t21_02_cross_table_relational_atomic() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE a (id UUID PRIMARY KEY, v TEXT)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE b (id UUID PRIMARY KEY, ref_id UUID, v TEXT)",
        &empty(),
    )
    .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let probes = Arc::new(AtomicU64::new(0));

    let r_db = db.clone();
    let r_stop = stop.clone();
    let r_torn = torn.clone();
    let r_probes = probes.clone();
    let reader = thread::spawn(move || {
        while !r_stop.load(Ordering::SeqCst) {
            let snap = r_db.snapshot();
            let a_count = r_db.scan("a", snap).map(|r| r.len()).unwrap_or(0);
            let b_count = r_db.scan("b", snap).map(|r| r.len()).unwrap_or(0);
            // Either both empty, or both filled to 5/5. Anything else is torn.
            if !(a_count == 0 && b_count == 0 || a_count == 5 && b_count == 5) {
                r_torn.fetch_add(1, Ordering::SeqCst);
            }
            r_probes.fetch_add(1, Ordering::SeqCst);
        }
    });

    thread::sleep(Duration::from_millis(5));
    db.execute("BEGIN", &empty()).unwrap();
    for i in 0..5 {
        let mut p = HashMap::new();
        p.insert("aid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
        p.insert("bid".into(), Value::Uuid(Uuid::from_u128(100 + i + 1)));
        p.insert("ref".into(), Value::Uuid(Uuid::from_u128(i + 1)));
        p.insert("v".into(), Value::Text(format!("row-{i}")));
        db.execute("INSERT INTO a (id, v) VALUES ($aid, $v)", &p)
            .unwrap();
        db.execute("INSERT INTO b (id, ref_id, v) VALUES ($bid, $ref, $v)", &p)
            .unwrap();
    }
    db.execute("COMMIT", &empty()).unwrap();

    thread::sleep(Duration::from_millis(20));
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "torn cross-relational reads observed; probes={}",
        probes.load(Ordering::SeqCst)
    );
}

/// REGRESSION GUARD — t21_03
#[test]
fn t21_03_stress_no_torn_under_concurrent_commits() {
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE a (id UUID PRIMARY KEY, t TEXT)", &empty())
        .unwrap();
    db.execute("CREATE TABLE b (id UUID PRIMARY KEY, t TEXT)", &empty())
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));

    let r_db = db.clone();
    let r_stop = stop.clone();
    let r_torn = torn.clone();
    let reader = thread::spawn(move || {
        while !r_stop.load(Ordering::SeqCst) {
            let snap = r_db.snapshot();
            let a = r_db.scan("a", snap).map(|r| r.len()).unwrap_or(0);
            let b = r_db.scan("b", snap).map(|r| r.len()).unwrap_or(0);
            if a != b {
                r_torn.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let w_db = db.clone();
    let writer = thread::spawn(move || {
        for i in 0..200u128 {
            let mut p = HashMap::new();
            p.insert("aid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
            p.insert("bid".into(), Value::Uuid(Uuid::from_u128(10_000 + i + 1)));
            p.insert("t".into(), Value::Text(format!("r{i}")));
            w_db.execute("BEGIN", &empty()).unwrap();
            w_db.execute("INSERT INTO a (id, t) VALUES ($aid, $t)", &p)
                .unwrap();
            w_db.execute("INSERT INTO b (id, t) VALUES ($bid, $t)", &p)
                .unwrap();
            w_db.execute("COMMIT", &empty()).unwrap();
        }
    });

    writer.join().unwrap();
    thread::sleep(Duration::from_millis(10));
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "torn read observed under stress: a.len() != b.len() at some snapshot"
    );
}

/// REGRESSION GUARD — t21_04
#[test]
fn t21_04_single_table_atomic_still_works() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE a (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("BEGIN", &empty()).unwrap();
    for i in 0..3 {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(i + 1)));
        db.execute("INSERT INTO a (id) VALUES ($id)", &p).unwrap();
    }
    db.execute("COMMIT", &empty()).unwrap();
    let snap = db.snapshot();
    assert_eq!(db.scan("a", snap).unwrap().len(), 3);
}

/// RED — t21_05
#[test]
fn t21_05_subscriber_observes_consistent_snapshot_after_event() {
    use contextdb_core::RowId;
    let db = Arc::new(Database::open_memory());
    db.execute("CREATE TABLE a (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("CREATE TABLE b (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let source_id = Uuid::from_u128(700);
    let target_id = Uuid::from_u128(701);
    let old_anchor_id = Uuid::from_u128(703);
    let mut fixture = HashMap::new();
    fixture.insert("src".into(), Value::Uuid(source_id));
    fixture.insert("target".into(), Value::Uuid(target_id));
    fixture.insert("old_anchor".into(), Value::Uuid(old_anchor_id));
    fixture.insert("v0".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    fixture.insert("vanchor".into(), Value::Vector(vec![0.5, 0.5, 0.0]));
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($src, 'source', $v0)",
        &fixture,
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($target, 'pre', $v0)",
        &fixture,
    )
    .unwrap();
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($old_anchor, 'old-anchor', $vanchor)",
        &fixture,
    )
    .unwrap();
    let rx = db.subscribe();

    db.execute("BEGIN", &empty()).unwrap();
    for i in 0..4u128 {
        let mut p = HashMap::new();
        p.insert("aid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
        p.insert("bid".into(), Value::Uuid(Uuid::from_u128(100 + i + 1)));
        db.execute("INSERT INTO a (id) VALUES ($aid)", &p).unwrap();
        db.execute("INSERT INTO b (id) VALUES ($bid)", &p).unwrap();
    }
    let mut graph_vector = HashMap::new();
    graph_vector.insert("eid".into(), Value::Uuid(Uuid::from_u128(750)));
    graph_vector.insert("src".into(), Value::Uuid(source_id));
    graph_vector.insert("target".into(), Value::Uuid(target_id));
    graph_vector.insert("vnew".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    db.execute(
        "UPDATE entities SET name = 'target', embedding = $vnew WHERE id = $target",
        &graph_vector,
    )
    .unwrap();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($eid, $src, $target, 'LINKS')",
        &graph_vector,
    )
    .unwrap();
    db.execute("COMMIT", &empty()).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("commit event must arrive");
    assert!(event.tables_changed.contains(&"a".to_string()));
    assert!(event.tables_changed.contains(&"b".to_string()));
    assert!(event.tables_changed.contains(&"entities".to_string()));
    assert!(event.tables_changed.contains(&"edges".to_string()));
    assert!(
        rx.try_recv().is_err(),
        "exactly one CommitEvent expected for the multi-table tx"
    );
    let target_row_id: RowId = db
        .scan("entities", db.snapshot())
        .unwrap()
        .into_iter()
        .find(|r| {
            matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_id)
                && matches!(r.values.get("name"), Some(Value::Text(s)) if s == "target")
        })
        .expect("event-time target row must be visible after first commit")
        .row_id;

    let mut later = HashMap::new();
    let later_target_id = Uuid::from_u128(702);
    later.insert("aid".into(), Value::Uuid(Uuid::from_u128(999)));
    later.insert("eid".into(), Value::Uuid(Uuid::from_u128(751)));
    later.insert("src".into(), Value::Uuid(source_id));
    later.insert("later_target".into(), Value::Uuid(later_target_id));
    later.insert("vlater".into(), Value::Vector(vec![0.0, 0.0, 1.0]));
    db.execute("BEGIN", &empty()).unwrap();
    db.execute("INSERT INTO a (id) VALUES ($aid)", &later)
        .unwrap();
    db.execute(
        "INSERT INTO entities (id, name, embedding) VALUES ($later_target, 'later-target', $vlater)",
        &later,
    )
    .unwrap();
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) \
         VALUES ($eid, $src, $later_target, 'LINKS')",
        &later,
    )
    .unwrap();
    db.execute("COMMIT", &empty()).unwrap();
    assert_eq!(
        db.scan("a", db.snapshot()).unwrap().len(),
        5,
        "current snapshot should include the later commit"
    );

    let index = VectorIndexRef::new("entities", "embedding");
    let mutate_existing = db.begin();
    db.delete_edge(mutate_existing, source_id, target_id, "LINKS")
        .unwrap();
    db.commit(mutate_existing).unwrap();
    let mut mutate_target = HashMap::new();
    mutate_target.insert("target".into(), Value::Uuid(target_id));
    mutate_target.insert("vcurrent".into(), Value::Vector(vec![0.0, 0.0, 1.0]));
    db.execute(
        "UPDATE entities SET embedding = $vcurrent WHERE id = $target",
        &mutate_target,
    )
    .unwrap();

    assert_eq!(
        db.edge_count(source_id, "LINKS", db.snapshot())
            .unwrap_or(0),
        1,
        "current snapshot should include only the later graph edge after the target edge is deleted"
    );
    let current_graph = db
        .query_bfs(source_id, None, Direction::Outgoing, 1, db.snapshot())
        .unwrap();
    let current_graph_ids: Vec<Uuid> = current_graph.nodes.iter().map(|node| node.id).collect();
    assert!(
        !current_graph_ids.contains(&target_id),
        "current graph should no longer expose the original target edge; nodes={current_graph_ids:?}"
    );
    assert!(
        current_graph_ids.contains(&later_target_id),
        "current graph should expose the later target edge; nodes={current_graph_ids:?}"
    );
    let later_target_row_id: RowId = db
        .scan("entities", db.snapshot())
        .unwrap()
        .into_iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == later_target_id))
        .expect("current snapshot should include the later vector-bearing row")
        .row_id;
    let current_live = db
        .live_vector_entry(later_target_row_id, db.snapshot())
        .expect("current snapshot must have the later vector entry");
    assert_eq!(
        current_live.vector,
        vec![0.0, 0.0, 1.0],
        "current snapshot should include the later vector entry"
    );
    let current_target_row_id: RowId = db
        .scan("entities", db.snapshot())
        .unwrap()
        .into_iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_id))
        .expect("current snapshot should include the target row after vector mutation")
        .row_id;
    let current_mutated_target = db
        .live_vector_entry(current_target_row_id, db.snapshot())
        .expect("current snapshot must have the target's later vector");
    assert_eq!(
        current_mutated_target.vector,
        vec![0.0, 0.0, 1.0],
        "current snapshot should include the target row's later vector mutation"
    );

    // Snapshot rooted at the event's LSN must see the first multi-table commit,
    // but not the later relational, graph, or vector commit. A `snapshot_at`
    // implementation that aliases to current snapshot or lets graph/vector
    // helpers ignore the historical snapshot fails here.
    let snap = db.snapshot_at(event.lsn);
    assert_eq!(
        db.scan("a", snap).unwrap().len(),
        4,
        "a not fully visible at event LSN"
    );
    assert_eq!(
        db.scan("b", snap).unwrap().len(),
        4,
        "b not fully visible at event LSN"
    );
    let entities = db.scan("entities", snap).unwrap();
    assert!(
        entities.iter().any(|r| {
            matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == target_id)
                && matches!(r.values.get("name"), Some(Value::Text(s)) if s == "target")
        }),
        "updated entity row not visible at event LSN"
    );
    assert_eq!(
        db.edge_count(source_id, "LINKS", snap).unwrap_or(0),
        1,
        "graph edge not visible at event LSN"
    );
    let historical_graph = db
        .query_bfs(source_id, None, Direction::Outgoing, 1, snap)
        .unwrap();
    let historical_graph_ids: Vec<Uuid> =
        historical_graph.nodes.iter().map(|node| node.id).collect();
    assert!(
        historical_graph_ids.contains(&target_id),
        "historical query_bfs must expose the first event's graph edge; nodes={historical_graph_ids:?}"
    );
    assert!(
        !historical_graph_ids.contains(&later_target_id),
        "historical query_bfs leaked the later graph edge; nodes={historical_graph_ids:?}"
    );
    let live = db
        .live_vector_entry(target_row_id, snap)
        .expect("target row must have a live vector at event LSN");
    assert_eq!(
        live.vector,
        vec![0.0, 1.0, 0.0],
        "vector index not visible at event LSN"
    );
    assert!(
        db.live_vector_entry(later_target_row_id, snap).is_none(),
        "later vector entry leaked into the event-rooted snapshot"
    );
    let historical_vector = db
        .query_vector(index, &[0.0, 1.0, 0.0], 1, None, snap)
        .unwrap();
    assert_eq!(
        historical_vector.first().map(|(row_id, _)| *row_id),
        Some(target_row_id),
        "historical query_vector must rank the event-time target vector first, not the later-mutated current vector; result={historical_vector:?}"
    );
    assert!(
        historical_vector
            .iter()
            .all(|(row_id, _)| *row_id != later_target_row_id),
        "historical query_vector leaked the later vector entry; result={historical_vector:?}"
    );
}

/// REGRESSION GUARD — t21_06: relational + graph atomic (the Plan-7 cascade shape).
#[test]
fn t21_06_relational_plus_graph_atomic_no_torn_reads() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, decision_id UUID, reason TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute("CREATE TABLE inv_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)", &empty()).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));

    let r_db = db.clone();
    let r_stop = stop.clone();
    let r_torn = torn.clone();
    let reader = thread::spawn(move || {
        while !r_stop.load(Ordering::SeqCst) {
            let snap = r_db.snapshot();
            let inv_count = r_db
                .scan("invalidations", snap)
                .map(|r| r.len())
                .unwrap_or(0);
            let edge_count = r_db.scan("inv_edges", snap).map(|r| r.len()).unwrap_or(0);
            // Either both 0 or both 200 at any time.
            if inv_count != edge_count {
                r_torn.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let w_db = db.clone();
    let writer = thread::spawn(move || {
        for i in 0..200u128 {
            let mut p = HashMap::new();
            p.insert("iid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
            p.insert("did".into(), Value::Uuid(Uuid::from_u128(10_000 + i + 1)));
            p.insert("eid".into(), Value::Uuid(Uuid::from_u128(20_000 + i + 1)));
            p.insert("rsn".into(), Value::Text("basis-divergence".into()));
            w_db.execute("BEGIN", &empty()).unwrap();
            w_db.execute(
                "INSERT INTO invalidations (id, decision_id, reason) VALUES ($iid, $did, $rsn)",
                &p,
            )
            .unwrap();
            w_db.execute(
                "INSERT INTO inv_edges (id, source_id, target_id, edge_type) VALUES ($eid, $iid, $did, 'INVALIDATES')",
                &p,
            ).unwrap();
            w_db.execute("COMMIT", &empty()).unwrap();
        }
    });

    writer.join().unwrap();
    thread::sleep(Duration::from_millis(10));
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "relational+graph torn read observed"
    );
}

/// REGRESSION GUARD — t21_07: relational + vector atomic.
#[test]
fn t21_07_relational_plus_vector_atomic_no_torn_reads() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE memo_vecs (id UUID PRIMARY KEY, ref_id UUID, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let r_db = db.clone();
    let r_stop = stop.clone();
    let r_torn = torn.clone();
    let reader = thread::spawn(move || {
        while !r_stop.load(Ordering::SeqCst) {
            let snap = r_db.snapshot();
            let m = r_db.scan("memos", snap).map(|r| r.len()).unwrap_or(0);
            let v = r_db.scan("memo_vecs", snap).map(|r| r.len()).unwrap_or(0);
            if m != v {
                r_torn.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let w_db = db.clone();
    let writer = thread::spawn(move || {
        for i in 0..200u128 {
            let mut p = HashMap::new();
            p.insert("mid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
            p.insert("vid".into(), Value::Uuid(Uuid::from_u128(50_000 + i + 1)));
            p.insert("body".into(), Value::Text(format!("m-{i}")));
            p.insert(
                "vec".into(),
                Value::Vector(vec![i as f32 / 1000.0, 0.0, 1.0]),
            );
            w_db.execute("BEGIN", &empty()).unwrap();
            w_db.execute("INSERT INTO memos (id, body) VALUES ($mid, $body)", &p)
                .unwrap();
            w_db.execute(
                "INSERT INTO memo_vecs (id, ref_id, embedding) VALUES ($vid, $mid, $vec)",
                &p,
            )
            .unwrap();
            w_db.execute("COMMIT", &empty()).unwrap();
        }
    });

    writer.join().unwrap();
    thread::sleep(Duration::from_millis(10));
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "relational+vector torn read observed"
    );
}

/// REGRESSION GUARD — t21_08: graph + vector atomic.
#[test]
fn t21_08_graph_plus_vector_atomic_no_torn_reads() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE g_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    ).unwrap();
    db.execute(
        "CREATE TABLE g_vecs (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let r_db = db.clone();
    let r_stop = stop.clone();
    let r_torn = torn.clone();
    let reader = thread::spawn(move || {
        while !r_stop.load(Ordering::SeqCst) {
            let snap = r_db.snapshot();
            let e = r_db.scan("g_edges", snap).map(|r| r.len()).unwrap_or(0);
            let v = r_db.scan("g_vecs", snap).map(|r| r.len()).unwrap_or(0);
            if e != v {
                r_torn.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let w_db = db.clone();
    let writer = thread::spawn(move || {
        for i in 0..200u128 {
            let mut p = HashMap::new();
            p.insert("eid".into(), Value::Uuid(Uuid::from_u128(i + 1)));
            p.insert("src".into(), Value::Uuid(Uuid::from_u128(70_000 + i)));
            p.insert("tgt".into(), Value::Uuid(Uuid::from_u128(80_000 + i)));
            p.insert("vid".into(), Value::Uuid(Uuid::from_u128(90_000 + i + 1)));
            p.insert(
                "vec".into(),
                Value::Vector(vec![i as f32 / 1000.0, 0.0, 1.0]),
            );
            w_db.execute("BEGIN", &empty()).unwrap();
            w_db.execute(
                "INSERT INTO g_edges (id, source_id, target_id, edge_type) VALUES ($eid, $src, $tgt, 'LINKS')",
                &p,
            ).unwrap();
            w_db.execute("INSERT INTO g_vecs (id, embedding) VALUES ($vid, $vec)", &p)
                .unwrap();
            w_db.execute("COMMIT", &empty()).unwrap();
        }
    });

    writer.join().unwrap();
    thread::sleep(Duration::from_millis(10));
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        torn.load(Ordering::SeqCst),
        0,
        "graph+vector torn read observed"
    );
}
