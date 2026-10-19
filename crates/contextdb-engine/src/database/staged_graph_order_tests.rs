//! The engine must hand graph traversal one grouped physical staged stream.
//! Interleaved accepted rows remain separate bindings at depth one, while
//! reachability visits each visible logical triad once across both lanes.
use super::*;

#[test]
fn interleaved_staged_row_owners_and_standalone_edges_share_logical_reachability() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE links (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, subtype TEXT NOT NULL, UNIQUE(source_id, target_id, edge_type, subtype))", &HashMap::new()).unwrap();
    let source = NodeId::from_u128(101);
    let a = NodeId::from_u128(201);
    let b = NodeId::from_u128(301);
    let insert = |id: u128, target: NodeId, subtype: &str| {
        db.execute(
            "INSERT INTO links VALUES ($id, $source, $target, 'LINKS', $subtype)",
            &HashMap::from([
                ("id".into(), Value::Uuid(NodeId::from_u128(id))),
                ("source".into(), Value::Uuid(source)),
                ("target".into(), Value::Uuid(target)),
                ("subtype".into(), Value::Text(subtype.into())),
            ]),
        )
        .unwrap();
    };
    insert(1, a, "committed-a");
    insert(2, b, "committed-b");
    let tx = db.next_tx();
    db.execute("BEGIN", &HashMap::new()).unwrap();
    insert(3, b, "staged-b-one");
    insert(4, a, "staged-a-one");
    insert(5, b, "staged-b-two");
    insert(6, a, "staged-a-two");
    assert!(
        db.insert_edge(tx, source, b, "LINKS".into(), HashMap::new())
            .unwrap()
    );
    assert_eq!(db.execute("SELECT t FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &HashMap::from([("source".into(), Value::Uuid(source))])).unwrap().rows.len(), 7,
        "physical depth-one matching retains every declared owner plus standalone");

    let memory = db.declared_reader_memory().unwrap();
    let mut cursor = db
        .bounded_graph_bfs_cursor(
            Some(tx),
            source,
            Some(&["LINKS".into()]),
            Direction::Outgoing,
            1,
            2,
            db.snapshot_for_read(),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| {},
            memory.clone(),
        )
        .unwrap()
        .unwrap();
    let mut reached = Vec::new();
    while let Some(node) = db
        .bounded_graph_bfs_next(
            &mut cursor,
            &mut || Ok::<_, Error>(()),
            &mut || Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| {},
            &mut || Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| {},
            &mut || {},
            None,
            Some(tx),
            memory.clone(),
        )
        .unwrap()
    {
        reached.push(node.node.id);
        assert!(
            reached.len() <= 2,
            "physical witnesses must coalesce before reachability publication"
        );
    }
    reached.sort_unstable();
    assert_eq!(reached, vec![a, b]);
    db.execute("ROLLBACK", &HashMap::new()).unwrap();
    assert_eq!(db.execute("SELECT t FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &HashMap::from([("source".into(), Value::Uuid(source))])).unwrap().rows.len(), 2);
}

#[test]
fn detached_standalone_graph_copies_return_exactly_their_admitted_bytes() {
    for versions in [1usize, 4] {
        let accountant = Arc::new(MemoryAccountant::with_budget(1024 * 1024));
        let db = Database::open_memory_with_accountant(accountant.clone());
        for version in 0..versions {
            let edge = AdjEntry {
                source: NodeId::from_u128(100 + version as u128),
                target: NodeId::from_u128(200 + version as u128),
                edge_type: "selected".to_owned(),
                properties: HashMap::new(),
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            };
            let retained = contextdb_graph::GraphAdjEntry::from(edge.clone()).estimated_bytes();
            accountant.try_allocate(retained).unwrap();
            db.graph_store.insert_loaded_edge(edge);
        }
        let baseline = accountant.usage().used;
        for _ in 0..2 {
            let memory = db.declared_reader_memory().unwrap();
            let mut copies = crate::vector_read_memory::admission(memory.clone());
            let (metadata, working) = db
                .detached_received_schema_working_database(
                    &HashMap::new(),
                    &HashSet::new(),
                    &HashSet::from(["selected".to_owned()]),
                    false,
                    false,
                    None,
                    None,
                    None,
                    &HashSet::new(),
                    &HashSet::new(),
                    &mut copies,
                    memory,
                )
                .unwrap();
            assert_eq!(
                working
                    .graph_store
                    .forward_adj
                    .read()
                    .values()
                    .map(Vec::len)
                    .sum::<usize>(),
                versions
            );
            drop(working);
            drop(metadata);
            drop(copies);
            assert_eq!(
                accountant.usage().used,
                baseline,
                "dropping {versions} copied standalone versions must preserve the receiver balance"
            );
        }
    }
}

// Policy 22: an open captured view cannot prohibit otherwise valid writes.
// The rows exercise insert, same-owner changed endpoints, and delete separately:
// otherwise an insert refusal masks the update/delete contract.
fn staged_cursor_keeps_old_and_new_views_after_write(operation: &str) {
    let db = Database::open_memory();
    let params = HashMap::new();
    db.execute("CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)", &params).unwrap();
    let tx = db.next_tx();
    db.execute("BEGIN", &params).unwrap();
    let values = HashMap::from([
        ("source".into(), Value::Uuid(NodeId::from_u128(101))),
        ("target".into(), Value::Uuid(NodeId::from_u128(201))),
        ("changed".into(), Value::Uuid(NodeId::from_u128(301))),
    ]);
    db.execute(
        "INSERT INTO links VALUES (1, $source, $target, 'LINKS')",
        &values,
    )
    .unwrap();
    let memory = db.declared_reader_memory().unwrap();
    let open = || {
        db.bounded_graph_bfs_cursor(
            Some(tx),
            NodeId::from_u128(101),
            Some(&["LINKS".into()]),
            Direction::Outgoing,
            1,
            8,
            db.snapshot_for_read(),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| {},
            memory.clone(),
        )
        .unwrap()
        .unwrap()
    };
    let mut old = open();
    let statement = match operation {
        "insert" => "INSERT INTO links VALUES (2, $source, $changed, 'LINKS')",
        "update" => "UPDATE links SET target_id = $changed WHERE id = 1",
        "delete" => "DELETE FROM links WHERE id = 1",
        _ => unreachable!(),
    };
    db.execute(statement, &values)
        .expect("valid staged write must succeed while its earlier cursor is open");
    let mut new = open();
    let mut collect = |cursor: &mut _| {
        let mut nodes = Vec::new();
        while let Some(node) = db
            .bounded_graph_bfs_next(
                cursor,
                &mut || Ok::<_, Error>(()),
                &mut || Ok::<_, Error>(()),
                &mut |_| Ok::<_, Error>(()),
                &mut |_| Ok::<_, Error>(()),
                &mut |_| {},
                &mut || Ok::<_, Error>(()),
                &mut |_| Ok::<_, Error>(()),
                &mut |_| {},
                &mut || {},
                None,
                Some(tx),
                memory.clone(),
            )
            .unwrap()
        {
            nodes.push(node.node.id);
            assert!(nodes.len() <= 2);
        }
        nodes.sort_unstable();
        nodes
    };
    assert_eq!(
        collect(&mut old),
        vec![NodeId::from_u128(201)],
        "old cursor retains its exact original endpoints"
    );
    let expected = match operation {
        "insert" => vec![NodeId::from_u128(201), NodeId::from_u128(301)],
        "update" => vec![NodeId::from_u128(301)],
        "delete" => vec![],
        _ => unreachable!(),
    };
    assert_eq!(
        collect(&mut new),
        expected,
        "new cursor sees the complete changed staged view"
    );
    drop(old);
    drop(new);
    db.execute("ROLLBACK", &params).unwrap();
}
#[test]
fn an_open_graph_cursor_allows_staged_insert_and_keeps_its_view() {
    staged_cursor_keeps_old_and_new_views_after_write("insert");
}
#[test]
fn an_open_graph_cursor_allows_same_owner_endpoint_update_and_keeps_its_view() {
    staged_cursor_keeps_old_and_new_views_after_write("update");
}
#[test]
fn an_open_graph_cursor_allows_staged_delete_and_keeps_its_view() {
    staged_cursor_keeps_old_and_new_views_after_write("delete");
}

// Policy 22: insertion may move physical slots but cannot change a captured answer.
// Thin rows ensure an early failing anchor case does not mask tail truncation.
fn memory_edge_cursor_keeps_snapshot(direction: Direction, inserted: u128) {
    let db = Database::open_memory();
    db.execute("CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)", &HashMap::new()).unwrap();
    let insert = |id: i64, endpoint: u128| {
        let (source, target) = if direction != Direction::Incoming {
            (NodeId::from_u128(101), NodeId::from_u128(endpoint))
        } else {
            (NodeId::from_u128(endpoint), NodeId::from_u128(101))
        };
        db.execute(
            "INSERT INTO links VALUES ($id, $source, $target, $kind)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("source".into(), Value::Uuid(source)),
                ("target".into(), Value::Uuid(target)),
                ("kind".into(), Value::Text("LINKS".into())),
            ]),
        )
        .unwrap();
    };
    insert(1, 201);
    insert(2, 301);
    let memory = db.declared_reader_memory().unwrap();
    let mut cursor = db
        .bounded_graph_edge_cursor(
            direction,
            db.snapshot_for_read(),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
        )
        .unwrap();
    let next = |cursor: &mut gate::BoundedGraphEdgeCursor| {
        db.bounded_graph_edge_next(
            cursor,
            Some(&["LINKS".into()]),
            None,
            &mut || Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut || {},
            memory.clone(),
        )
    };
    assert_eq!(
        next(&mut cursor).unwrap(),
        Some((NodeId::from_u128(101), NodeId::from_u128(201)))
    );
    insert(3, inserted);
    assert_eq!(
        next(&mut cursor).unwrap(),
        Some((NodeId::from_u128(101), NodeId::from_u128(301))),
        "new rows sorting before the anchor or unread tail cannot displace a visible snapshot binding"
    );
    if direction == Direction::Both {
        assert_eq!(
            next(&mut cursor).unwrap(),
            Some((NodeId::from_u128(201), NodeId::from_u128(101)))
        );
        assert_eq!(
            next(&mut cursor).unwrap(),
            Some((NodeId::from_u128(301), NodeId::from_u128(101)))
        );
    }
    assert_eq!(
        next(&mut cursor).unwrap(),
        None,
        "the later committed row is outside the captured snapshot"
    );
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_outgoing_before_anchor() {
    memory_edge_cursor_keeps_snapshot(Direction::Outgoing, 151);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_outgoing_between_anchor_and_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Outgoing, 251);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_outgoing_after_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Outgoing, 401);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_incoming_before_anchor() {
    memory_edge_cursor_keeps_snapshot(Direction::Incoming, 151);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_incoming_between_anchor_and_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Incoming, 251);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_incoming_after_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Incoming, 401);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_both_before_anchor() {
    memory_edge_cursor_keeps_snapshot(Direction::Both, 151);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_both_between_anchor_and_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Both, 251);
}
#[test]
fn a_memory_edge_cursor_keeps_its_snapshot_both_after_tail() {
    memory_edge_cursor_keeps_snapshot(Direction::Both, 401);
}

// Captured authorization decisions must use the same revision as staged bodies.
// A live grant/node/edge lookup mutant passes the endpoint-only frozen rows.
fn captured_graph_authorization_survives_mutation(operation: &str) {
    let db = Database::open_memory();
    let p = HashMap::from([
        ("source".into(), Value::Uuid(NodeId::from_u128(101))),
        ("target".into(), Value::Uuid(NodeId::from_u128(201))),
        ("acl".into(), Value::Uuid(NodeId::from_u128(501))),
        ("other".into(), Value::Uuid(NodeId::from_u128(502))),
    ]);
    db.execute("CREATE TABLE acl_grants (id INTEGER PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)", &p).unwrap();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id))", &p).unwrap();
    db.execute("CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))", &p).unwrap();
    let tx = db.begin_or_panic();
    db.execute_in_tx(tx, "INSERT INTO acl_grants VALUES (1, 'Agent', 'reader', $acl)", &p).unwrap();
    if operation == "grant" {
        db.execute_in_tx(tx, "INSERT INTO acl_grants VALUES (2, 'Agent', 'reader', $other)", &p).unwrap();
    }
    db.execute_in_tx(tx, "INSERT INTO nodes VALUES ($source, $acl)", &p).unwrap();
    db.execute_in_tx(tx, if operation == "grant" { "INSERT INTO nodes VALUES ($target, $other)" } else { "INSERT INTO nodes VALUES ($target, $acl)" }, &p).unwrap();
    db.execute_in_tx(tx, "INSERT INTO edges VALUES (1, $source, $target, 'LINKS', $acl)", &p).unwrap();
    let scoped = db.scoped_with_constraints(None, None, Some(contextdb_core::Principal::Agent("reader".into())));
    let memory = scoped.declared_reader_memory().unwrap();
    let open = || scoped.bounded_graph_bfs_cursor(Some(tx), NodeId::from_u128(101), Some(&["LINKS".into()]), Direction::Outgoing, 1, 8,
        scoped.snapshot_for_read(), &mut |_| Ok::<_, Error>(()), &mut |_| Ok::<_, Error>(()), &mut |_| {}, memory.clone()).unwrap().unwrap();
    let mut old = open();
    db.execute_in_tx(tx, match operation {
        "grant" => "DELETE FROM acl_grants WHERE id = 2",
        "node" => "UPDATE nodes SET acl_id = $other WHERE id = $target",
        "edge" => "UPDATE edges SET acl_id = $other WHERE id = 1",
        _ => unreachable!(),
    }, &p).unwrap();
    let mut new = open();
    let collect = |cursor: &mut _| {
        let mut result = Vec::new();
        while let Some(node) = scoped.bounded_graph_bfs_next(cursor,
            &mut || Ok::<_, Error>(()), &mut || Ok::<_, Error>(()), &mut |_| Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()), &mut |_| {}, &mut || Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()), &mut |_| {}, &mut || {}, None, Some(tx), memory.clone()).unwrap() {
            result.push(node.node.id); assert!(result.len() <= 1);
        }
        result
    };
    assert_eq!(collect(&mut old), vec![NodeId::from_u128(201)], "captured authorization retains the originally granted body");
    assert_eq!(collect(&mut new), Vec::<NodeId>::new(), "the new capture enforces the changed authorization revision");
    drop(old); drop(new); db.rollback(tx).unwrap();
}
#[test]
fn graph_capture_keeps_grant_revision_after_revocation() { captured_graph_authorization_survives_mutation("grant"); }
#[test]
fn graph_capture_keeps_node_acl_revision_after_mutation() { captured_graph_authorization_survives_mutation("node"); }
#[test]
fn graph_capture_keeps_edge_acl_revision_after_mutation() { captured_graph_authorization_survives_mutation("edge"); }
