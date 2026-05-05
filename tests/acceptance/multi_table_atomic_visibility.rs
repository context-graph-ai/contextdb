use contextdb_core::{Direction, MemoryAccountant, RowId, SnapshotId, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange, NaturalKey, RowChange,
    VectorChange,
};
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

        ddl_lsn: Vec::new(),
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

#[test]
fn t21_09_insert_then_update_same_tx_keeps_final_row() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(0x2109);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("v1".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
    p.insert("v2".into(), Value::Vector(vec![0.0, 1.0, 0.0]));

    db.execute("BEGIN", &empty()).unwrap();
    db.execute(
        "INSERT INTO items (id, name, embedding) VALUES ($id, 'draft', $v1)",
        &p,
    )
    .unwrap();
    db.execute(
        "UPDATE items SET name = 'final', embedding = $v2 WHERE id = $id",
        &p,
    )
    .unwrap();
    db.execute("COMMIT", &empty()).unwrap();

    let rows = db.scan("items", db.snapshot()).unwrap();
    assert_eq!(rows.len(), 1, "insert-then-update must commit one row");
    assert_eq!(
        rows[0].values.get("name"),
        Some(&Value::Text("final".to_string()))
    );
    let hits = db
        .query_vector(
            VectorIndexRef::new("items", "embedding"),
            &[0.0, 1.0, 0.0],
            1,
            None,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        hits.first().map(|(row_id, _)| *row_id),
        Some(rows[0].row_id)
    );
}

#[test]
fn t21_10_snapshot_at_lsn_survives_reopen_after_later_mutations() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("atomic-visible.db");
    let source_id = Uuid::from_u128(0x2110);
    let target_id = Uuid::from_u128(0x2111);
    let later_id = Uuid::from_u128(0x2112);

    let (event_lsn, target_row_id) = {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(3) WITH (quantization = 'SQ8'))",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        )
        .unwrap();
        let mut fixture = HashMap::new();
        fixture.insert("src".into(), Value::Uuid(source_id));
        fixture.insert("target".into(), Value::Uuid(target_id));
        fixture.insert("v0".into(), Value::Vector(vec![1.0, 0.0, 0.0]));
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

        let rx = db.subscribe();
        let mut event_values = HashMap::new();
        event_values.insert("eid".into(), Value::Uuid(Uuid::from_u128(0x2113)));
        event_values.insert("src".into(), Value::Uuid(source_id));
        event_values.insert("target".into(), Value::Uuid(target_id));
        event_values.insert("v_event".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
        db.execute("BEGIN", &empty()).unwrap();
        db.execute(
            "UPDATE entities SET name = 'event-target', embedding = $v_event WHERE id = $target",
            &event_values,
        )
        .unwrap();
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($eid, $src, $target, 'LINKS')",
            &event_values,
        )
        .unwrap();
        db.execute("COMMIT", &empty()).unwrap();
        let event = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("event commit must publish");
        let target_row_id = db
            .scan("entities", db.snapshot())
            .unwrap()
            .into_iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == target_id))
            .unwrap()
            .row_id;

        let mut later = HashMap::new();
        later.insert("later".into(), Value::Uuid(later_id));
        later.insert("target".into(), Value::Uuid(target_id));
        later.insert("v_later".into(), Value::Vector(vec![0.0, 0.0, 1.0]));
        db.execute(
            "INSERT INTO entities (id, name, embedding) VALUES ($later, 'later', $v_later)",
            &later,
        )
        .unwrap();
        let tx = db.begin();
        db.delete_edge(tx, source_id, target_id, "LINKS").unwrap();
        db.commit(tx).unwrap();
        db.execute(
            "UPDATE entities SET name = 'current-target', embedding = $v_later WHERE id = $target",
            &later,
        )
        .unwrap();
        db.close().unwrap();
        (event.lsn, target_row_id)
    };

    let reopened = Database::open(&path).unwrap();
    let snap = reopened.snapshot_at(event_lsn);
    let rows = reopened.scan("entities", snap).unwrap();
    let event_target = rows
        .iter()
        .find(|row| {
            matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == target_id)
                && matches!(row.values.get("name"), Some(Value::Text(name)) if name == "event-target")
        })
        .expect("event snapshot after reopen must see the event-time target row");
    assert_eq!(
        event_target.values.get("embedding"),
        Some(&Value::Vector(vec![0.0, 1.0, 0.0])),
        "SQ8 relational vector hydration must target the event-time row version"
    );
    let mut indexed = HashMap::new();
    indexed.insert("target".into(), Value::Uuid(target_id));
    let indexed_historical = reopened
        .execute_at_snapshot(
            "SELECT name, embedding FROM entities WHERE id = $target",
            &indexed,
            snap,
        )
        .unwrap();
    assert_eq!(indexed_historical.trace.physical_plan, "IndexScan");
    assert_eq!(
        indexed_historical.rows,
        vec![vec![
            Value::Text("event-target".to_string()),
            Value::Vector(vec![0.0, 1.0, 0.0])
        ]],
        "historical IndexScan must materialize the row version visible at the event LSN"
    );
    assert!(
        !rows
            .iter()
            .any(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == later_id)),
        "event snapshot after reopen must exclude later rows"
    );
    assert_eq!(
        reopened.edge_count(source_id, "LINKS", snap).unwrap_or(0),
        1,
        "event snapshot after reopen must see event-time edge"
    );
    assert_eq!(
        reopened
            .live_vector_entry(target_row_id, snap)
            .expect("event-time vector must survive reopen")
            .vector,
        vec![0.0, 1.0, 0.0]
    );
    reopened.close().unwrap();
}

#[test]
fn t21_11_snapshot_at_lsn_preserves_reinserted_graph_edge_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("graph-history.db");
    let source_id = Uuid::from_u128(0x2114);
    let target_id = Uuid::from_u128(0x2115);

    let (insert_lsn, delete_lsn, reinsert_lsn) = {
        let db = Database::open(&path).unwrap();
        let rx = db.subscribe();

        let tx = db.begin();
        db.insert_edge(
            tx,
            source_id,
            target_id,
            "LINKS".to_string(),
            HashMap::new(),
        )
        .unwrap();
        db.commit(tx).unwrap();
        let insert_lsn = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("insert edge event must publish")
            .lsn;

        let tx = db.begin();
        db.delete_edge(tx, source_id, target_id, "LINKS").unwrap();
        db.commit(tx).unwrap();
        let delete_lsn = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("delete edge event must publish")
            .lsn;

        let tx = db.begin();
        db.insert_edge(
            tx,
            source_id,
            target_id,
            "LINKS".to_string(),
            HashMap::new(),
        )
        .unwrap();
        db.commit(tx).unwrap();
        let reinsert_lsn = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("reinsert edge event must publish")
            .lsn;

        db.close().unwrap();
        (insert_lsn, delete_lsn, reinsert_lsn)
    };

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .edge_count(source_id, "LINKS", reopened.snapshot_at(insert_lsn))
            .unwrap_or(0),
        1,
        "reopened snapshot at the original insert LSN must retain the first edge version"
    );
    assert_eq!(
        reopened
            .edge_count(source_id, "LINKS", reopened.snapshot_at(delete_lsn))
            .unwrap_or(0),
        0,
        "reopened snapshot at the delete LSN must hide the deleted edge"
    );
    assert_eq!(
        reopened
            .edge_count(source_id, "LINKS", reopened.snapshot_at(reinsert_lsn))
            .unwrap_or(0),
        1,
        "reopened snapshot at the reinsert LSN must expose the new edge version"
    );
    reopened.close().unwrap();
}

#[test]
fn t21_12_reconstructed_commit_index_is_backfilled_before_new_commits() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("commit-index-backfill.db");
    let old_id = Uuid::from_u128(0x2116);
    let new_id = Uuid::from_u128(0x2117);

    let old_lsn = {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT)",
            &empty(),
        )
        .unwrap();
        let rx = db.subscribe();
        db.execute(
            "INSERT INTO items (id, label) VALUES ($id, 'old')",
            &HashMap::from([("id".to_string(), Value::Uuid(old_id))]),
        )
        .unwrap();
        let lsn = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("old commit event must publish")
            .lsn;
        db.close().unwrap();
        lsn
    };

    let redb_db = redb::Database::open(&path).unwrap();
    let write_txn = redb_db.begin_write().unwrap();
    let commit_index: redb::TableDefinition<u64, u64> = redb::TableDefinition::new("commit_index");
    let _ = write_txn.delete_table(commit_index);
    write_txn.commit().unwrap();
    drop(redb_db);

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "INSERT INTO items (id, label) VALUES ($id, 'new')",
            &HashMap::from([("id".to_string(), Value::Uuid(new_id))]),
        )
        .unwrap();
        db.close().unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    let old_rows = reopened
        .scan("items", reopened.snapshot_at(old_lsn))
        .unwrap();
    assert!(
        old_rows
            .iter()
            .any(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == old_id)),
        "reopened snapshot_at(old_lsn) must still resolve through the backfilled commit index"
    );
    assert!(
        !old_rows
            .iter()
            .any(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == new_id)),
        "old snapshot must not floor to the later commit-index entry after backfill"
    );
    reopened.close().unwrap();
}

#[test]
fn t21_13_same_tx_graph_delete_then_reinsert_keeps_new_edge() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("graph-delete-reinsert.db");
    let source_id = Uuid::from_u128(0x2118);
    let target_id = Uuid::from_u128(0x2119);

    {
        let db = Database::open(&path).unwrap();
        let tx = db.begin();
        db.insert_edge(
            tx,
            source_id,
            target_id,
            "LINKS".to_string(),
            HashMap::new(),
        )
        .unwrap();
        db.commit(tx).unwrap();

        let tx = db.begin();
        db.delete_edge(tx, source_id, target_id, "LINKS").unwrap();
        db.insert_edge(
            tx,
            source_id,
            target_id,
            "LINKS".to_string(),
            HashMap::new(),
        )
        .unwrap();
        assert!(
            !db.insert_edge(
                tx,
                source_id,
                target_id,
                "LINKS".to_string(),
                HashMap::new(),
            )
            .unwrap(),
            "a replayed same-tx insert after delete+reinsert must dedupe to final state"
        );
        db.commit(tx).unwrap();
        assert_eq!(db.edge_count(source_id, "LINKS", db.snapshot()).unwrap(), 1);
        db.close().unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .edge_count(source_id, "LINKS", reopened.snapshot())
            .unwrap(),
        1,
        "same-tx graph delete+reinsert must survive persistence"
    );
    reopened.close().unwrap();
}

#[test]
fn t21_14_mixed_sync_edge_error_rolls_back_rows() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS')",
        &empty(),
    )
    .unwrap();
    let id = Uuid::from_u128(0x2120);
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                (
                    "label".to_string(),
                    Value::Text("should-rollback".to_string()),
                ),
            ]),
            deleted: false,
            lsn: contextdb_core::Lsn(1),
        }],
        edges: vec![EdgeChange {
            source: id,
            target: id,
            edge_type: "DEPENDS".to_string(),
            properties: HashMap::new(),
            lsn: contextdb_core::Lsn(1),
        }],
        vectors: Vec::new(),
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    let err = db
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("self-cycle sync edge must reject the mixed batch");
    assert!(
        matches!(err, contextdb_core::Error::CycleDetected { .. }),
        "expected cycle rejection, got {err:?}"
    );
    assert!(
        db.scan("items", db.snapshot()).unwrap().is_empty(),
        "mixed sync batch must not commit rows when graph apply fails"
    );
}

#[test]
fn t21_16_mixed_sync_vector_error_rolls_back_rows() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT, embedding VECTOR(3))",
        &empty(),
    )
    .unwrap();
    let id = Uuid::from_u128(0x2123);
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "items".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                (
                    "label".to_string(),
                    Value::Text("should-rollback".to_string()),
                ),
            ]),
            deleted: false,
            lsn: contextdb_core::Lsn(1),
        }],
        edges: Vec::new(),
        vectors: vec![contextdb_engine::sync_types::VectorChange {
            index: VectorIndexRef::new("items", "embedding"),
            row_id: contextdb_core::RowId(1),
            vector: vec![1.0, 0.0],
            lsn: contextdb_core::Lsn(1),
        }],
        ddl: Vec::new(),

        ddl_lsn: Vec::new(),
    };

    let err = db
        .apply_changes(
            changes,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .expect_err("bad vector dimension must reject the mixed batch");
    assert!(
        matches!(
            err,
            contextdb_core::Error::VectorIndexDimensionMismatch { .. }
        ),
        "expected vector dimension rejection, got {err:?}"
    );
    assert!(
        db.scan("items", db.snapshot()).unwrap().is_empty(),
        "mixed sync batch must not commit rows when vector apply fails"
    );
}

#[test]
fn t21_15_legacy_nonmonotonic_txids_are_repaired_on_reopen() {
    use redb::ReadableTable;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PersistedVersionedRow {
        row_id: contextdb_core::RowId,
        values: HashMap<String, PersistedValue>,
        created_tx: contextdb_core::TxId,
        deleted_tx: Option<contextdb_core::TxId>,
        lsn: contextdb_core::Lsn,
        created_at: Option<contextdb_core::Wallclock>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum PersistedValue {
        Plain(Value),
        Vector(PersistedVector),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum PersistedVector {
        F32(Vec<f32>),
        SQ8 {
            min: f32,
            max: f32,
            len: u32,
            payload: Vec<u8>,
        },
        SQ4 {
            min: f32,
            max: f32,
            len: u32,
            payload: Vec<u8>,
        },
    }

    fn encode<T: Serialize>(value: &T) -> Vec<u8> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .unwrap()
            .0
    }

    fn row_key(row: &PersistedVersionedRow) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&row.row_id.0.to_be_bytes());
        key.extend_from_slice(&row.created_tx.0.to_be_bytes());
        key.extend_from_slice(&row.lsn.0.to_be_bytes());
        key
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("legacy-nonmonotonic.db");
    let first_id = Uuid::from_u128(0x2121);
    let second_id = Uuid::from_u128(0x2122);
    let (first_lsn, second_lsn) = {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT)",
            &empty(),
        )
        .unwrap();
        let rx = db.subscribe();
        db.execute(
            "INSERT INTO items (id, label) VALUES ($id, 'first')",
            &HashMap::from([("id".to_string(), Value::Uuid(first_id))]),
        )
        .unwrap();
        let first_lsn = rx.recv_timeout(Duration::from_secs(2)).unwrap().lsn;
        db.execute(
            "INSERT INTO items (id, label) VALUES ($id, 'second')",
            &HashMap::from([("id".to_string(), Value::Uuid(second_id))]),
        )
        .unwrap();
        let second_lsn = rx.recv_timeout(Duration::from_secs(2)).unwrap().lsn;
        db.close().unwrap();
        (first_lsn, second_lsn)
    };

    let redb_db = redb::Database::open(&path).unwrap();
    let write_txn = redb_db.begin_write().unwrap();
    let rel_items: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("rel_items");
    let mut rows = Vec::new();
    {
        let table = write_txn.open_table(rel_items).unwrap();
        for entry in table.iter().unwrap() {
            let (_, value) = entry.unwrap();
            let mut row: PersistedVersionedRow = decode(value.value());
            match row.values.get("label") {
                Some(PersistedValue::Plain(Value::Text(label))) if label == "first" => {
                    row.created_tx = contextdb_core::TxId(2);
                }
                Some(PersistedValue::Plain(Value::Text(label))) if label == "second" => {
                    row.created_tx = contextdb_core::TxId(1);
                }
                _ => {}
            }
            rows.push(row);
        }
    }
    write_txn.delete_table(rel_items).unwrap();
    {
        let mut table = write_txn.open_table(rel_items).unwrap();
        for row in &rows {
            table
                .insert(row_key(row).as_slice(), encode(row).as_slice())
                .unwrap();
        }
    }
    let commit_index: redb::TableDefinition<u64, u64> = redb::TableDefinition::new("commit_index");
    let _ = write_txn.delete_table(commit_index);
    write_txn.commit().unwrap();
    drop(redb_db);

    let repaired = Database::open(&path).unwrap();
    let first_rows = repaired
        .scan("items", repaired.snapshot_at(first_lsn))
        .unwrap();
    assert!(
        first_rows
            .iter()
            .any(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == first_id)),
        "first LSN snapshot must still see the first row after TxId repair"
    );
    assert!(
        !first_rows
            .iter()
            .any(|row| matches!(row.values.get("id"), Some(Value::Uuid(id)) if *id == second_id)),
        "first LSN snapshot must not leak the later lower-Tx row"
    );
    let second_rows = repaired
        .scan("items", repaired.snapshot_at(second_lsn))
        .unwrap();
    assert_eq!(second_rows.len(), 2);
    repaired.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .scan("items", reopened.snapshot_at(second_lsn))
            .unwrap()
            .len(),
        2,
        "TxId repair must be durable"
    );
    reopened.close().unwrap();
}

#[test]
fn t21_17_fk_validation_uses_same_tx_final_state() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id))",
        &empty(),
    )
    .unwrap();

    let parent_id = Uuid::from_u128(0x2117);
    let child_id = Uuid::from_u128(0x2118);
    let params = HashMap::from([
        ("parent_id".to_string(), Value::Uuid(parent_id)),
        ("child_id".to_string(), Value::Uuid(child_id)),
    ]);
    db.execute(
        "INSERT INTO parents (id, name) VALUES ($parent_id, 'before')",
        &params,
    )
    .unwrap();

    db.execute("BEGIN", &empty()).unwrap();
    db.execute(
        "UPDATE parents SET name = 'after' WHERE id = $parent_id",
        &params,
    )
    .unwrap();
    db.execute(
        "INSERT INTO children (id, parent_id) VALUES ($child_id, $parent_id)",
        &params,
    )
    .unwrap();
    db.execute("COMMIT", &empty())
        .expect("FK validation must accept parent final state after same-tx update");

    assert_eq!(db.scan("children", db.snapshot()).unwrap().len(), 1);
}

#[test]
fn t21_18_mixed_sync_ddl_vector_error_does_not_apply_ddl() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, vector_text VECTOR(4))",
        &empty(),
    )
    .unwrap();

    let err = db
        .apply_changes(
            ChangeSet {
                rows: vec![],
                edges: vec![],
                vectors: vec![VectorChange {
                    index: VectorIndexRef::new("evidence", "vector_vision"),
                    row_id: contextdb_core::RowId(1),
                    vector: vec![1.0, 0.0, 0.0],
                    lsn: contextdb_core::Lsn(10),
                }],
                ddl: vec![DdlChange::AlterTable {
                    name: "evidence".to_string(),
                    columns: vec![("vector_vision".to_string(), "VECTOR(8)".to_string())],
                    constraints: vec![],
                }],

                ddl_lsn: vec![contextdb_core::Lsn(10)],
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("invalid mixed DDL+vector batch must fail before DDL side effects");
    assert!(
        matches!(
            err,
            contextdb_core::Error::VectorIndexDimensionMismatch { .. }
        ),
        "unexpected error for invalid mixed DDL+vector batch: {err}"
    );
    assert!(
        db.table_meta("evidence")
            .unwrap()
            .columns
            .iter()
            .all(|column| column.name != "vector_vision"),
        "failed mixed DDL+vector batch must not leave the new vector column registered"
    );
}

#[test]
fn t21_19_mixed_sync_ddl_edge_error_does_not_apply_ddl() {
    let db = Database::open_memory();
    let id = Uuid::from_u128(0x2119);

    let err = db
        .apply_changes(
            ChangeSet {
                rows: vec![],
                edges: vec![EdgeChange {
                    source: id,
                    target: id,
                    edge_type: "DEPENDS".to_string(),
                    properties: HashMap::new(),
                    lsn: contextdb_core::Lsn(10),
                }],
                vectors: vec![],
                ddl: vec![DdlChange::CreateTable {
                    name: "edges".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("source_id".to_string(), "UUID".to_string()),
                        ("target_id".to_string(), "UUID".to_string()),
                        ("edge_type".to_string(), "TEXT".to_string()),
                    ],
                    constraints: vec!["DAG('DEPENDS')".to_string()],
                }],

                ddl_lsn: vec![contextdb_core::Lsn(10)],
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("invalid mixed DDL+edge batch must fail before DDL side effects");

    assert!(
        matches!(err, contextdb_core::Error::CycleDetected { .. }),
        "unexpected error for invalid mixed DDL+edge batch: {err}"
    );
    assert!(
        db.table_meta("edges").is_none(),
        "failed mixed DDL+edge batch must not leave the new edge table registered"
    );
}

#[test]
fn t21_20_sync_drop_table_removes_vector_state_and_persists() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("sync-drop-vector.db");
    let id = Uuid::from_u128(0x2120);
    let source = Uuid::from_u128(0x2121);
    let target = Uuid::from_u128(0x2122);
    let index = VectorIndexRef::new("items", "embedding");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, embedding VECTOR(3))",
            &empty(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('DEPENDS')",
            &empty(),
        )
        .unwrap();
        let params = HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("v".to_string(), Value::Vector(vec![1.0, 0.0, 0.0])),
            ("edge_id".to_string(), Value::Uuid(Uuid::from_u128(0x2123))),
            ("source".to_string(), Value::Uuid(source)),
            ("target".to_string(), Value::Uuid(target)),
        ]);
        db.execute(
            "INSERT INTO items (id, embedding) VALUES ($id, $v)",
            &params,
        )
        .unwrap();
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($edge_id, $source, $target, 'DEPENDS')",
            &params,
        )
        .unwrap();
        assert!(
            !db.query_vector(index.clone(), &[1.0, 0.0, 0.0], 1, None, db.snapshot())
                .unwrap()
                .is_empty(),
            "guard: vector must exist before sync DropTable"
        );
        assert_eq!(
            db.edge_count(source, "DEPENDS", db.snapshot()).unwrap(),
            1,
            "guard: graph edge must exist before sync DropTable"
        );

        db.apply_changes(
            ChangeSet {
                rows: vec![],
                edges: vec![],
                vectors: vec![],
                ddl: vec![
                    DdlChange::DropTable {
                        name: "items".to_string(),
                    },
                    DdlChange::DropTable {
                        name: "edges".to_string(),
                    },
                ],

                ddl_lsn: vec![contextdb_core::Lsn(10), contextdb_core::Lsn(10)],
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("sync DropTable should apply");

        assert!(db.table_meta("items").is_none());
        assert!(db.table_meta("edges").is_none());
        assert!(matches!(
            db.query_vector(index.clone(), &[1.0, 0.0, 0.0], 1, None, db.snapshot()),
            Err(contextdb_core::Error::UnknownVectorIndex { .. })
        ));
        assert_eq!(
            db.edge_count(source, "DEPENDS", db.snapshot()).unwrap(),
            0,
            "sync DropTable must remove graph edges in memory"
        );
        db.close().unwrap();
    }

    let reopened = Database::open(&path).unwrap();
    assert!(reopened.table_meta("items").is_none());
    assert!(reopened.table_meta("edges").is_none());
    assert!(matches!(
        reopened.query_vector(index, &[1.0, 0.0, 0.0], 1, None, reopened.snapshot()),
        Err(contextdb_core::Error::UnknownVectorIndex { .. })
    ));
    assert_eq!(
        reopened
            .edge_count(source, "DEPENDS", reopened.snapshot())
            .unwrap(),
        0,
        "sync DropTable must durably remove graph edges"
    );
    reopened.close().unwrap();
}

#[test]
fn t21_21_mixed_sync_ddl_vector_memory_error_does_not_apply_ddl() {
    let accountant = Arc::new(MemoryAccountant::with_budget(1300));
    let db = Database::open_memory_with_accountant(accountant);
    let index = VectorIndexRef::new("evidence", "embedding");

    let err = db
        .apply_changes(
            ChangeSet {
                rows: vec![],
                edges: vec![],
                vectors: vec![VectorChange {
                    index: index.clone(),
                    row_id: RowId(1),
                    vector: vec![1.0; 256],
                    lsn: contextdb_core::Lsn(1),
                }],
                ddl: vec![DdlChange::CreateTable {
                    name: "evidence".to_string(),
                    columns: vec![
                        ("id".to_string(), "UUID PRIMARY KEY".to_string()),
                        ("embedding".to_string(), "VECTOR(256)".to_string()),
                    ],
                    constraints: vec![],
                }],

                ddl_lsn: vec![contextdb_core::Lsn(1)],
            },
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect_err("projected DDL+vector memory must fail before DDL side effects");

    assert!(
        matches!(err, contextdb_core::Error::MemoryBudgetExceeded { .. }),
        "unexpected error for projected memory failure: {err}"
    );
    assert!(
        db.table_meta("evidence").is_none(),
        "failed mixed DDL+vector memory batch must not register the table"
    );
    assert!(matches!(
        db.query_vector(index, &[1.0; 256], 1, None, db.snapshot()),
        Err(contextdb_core::Error::UnknownVectorIndex { .. })
    ));
}
