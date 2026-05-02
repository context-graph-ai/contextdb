use contextdb_core::types::ScopeLabel;
use contextdb_core::{Direction, Error, Value, VectorIndexRef};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn assert_error<T>(result: contextdb_core::Result<T>, message: &str) -> Error {
    let err = result.err();
    assert!(err.is_some(), "{message}: expected Err, got Ok");
    err.unwrap()
}

fn assert_scope_violation(err: Error, requested: &str, allowed: &[&str], message: &str) {
    match err {
        Error::ScopeLabelViolation {
            requested: got_requested,
            allowed: got_allowed,
        } => {
            assert_eq!(
                got_requested,
                ScopeLabel::new(requested),
                "{message}: requested scope-label mismatch"
            );
            let expected: BTreeSet<_> = allowed.iter().copied().map(ScopeLabel::new).collect();
            assert_eq!(got_allowed, expected, "{message}: allowed set mismatch");
        }
        other => panic!("{message}: expected ScopeLabelViolation, got {other:?}"),
    }
}

fn seed_signatures(path: &std::path::Path) {
    let admin = Database::open(path).unwrap();
    admin
        .execute(
            "CREATE TABLE signatures (id UUID PRIMARY KEY, name TEXT, \
         scope_label TEXT SCOPE_LABEL ('server'))",
            &empty(),
        )
        .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("name".into(), Value::Text("face-cluster-1".into()));
    p.insert("sl".into(), Value::Text("server".into()));
    admin
        .execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        )
        .unwrap();
    admin.close().unwrap();
}

/// RED — t6_01
#[test]
fn t6_01_edge_handle_cannot_write_server_only_table() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scope.redb");
    seed_signatures(&path);

    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();

    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(2)));
    p.insert("name".into(), Value::Text("from-edge".into()));
    p.insert("sl".into(), Value::Text("server".into()));
    let err = assert_error(
        edge.execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        ),
        "edge cannot write to server-only table",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "edge cannot write to server-only table",
    );
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(3)));
    p.insert("name".into(), Value::Text("schema-denied-edge".into()));
    p.insert("sl".into(), Value::Text("edge".into()));
    let err = assert_error(
        edge.execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        ),
        "edge handle cannot write edge-labelled row to server-only schema",
    );
    assert_scope_violation(
        err,
        "edge",
        &[],
        "server-only schema write allow-list must still reject edge-labelled rows",
    );
    drop(edge);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute("SELECT name FROM signatures", &empty())
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Text("face-cluster-1".into())]],
        "denied edge write must not persist a server-labelled row"
    );
}

/// RED — t6_02
#[test]
fn t6_02_split_read_gate_excludes_server_rows_for_edge_handle() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("read.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE signatures (id UUID PRIMARY KEY, name TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('server'))",
                &empty(),
            )
            .unwrap();
        for (id, name, label) in [
            (Uuid::from_u128(1), "edge-readable", "edge"),
            (Uuid::from_u128(2), "server-hidden", "server"),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("name".into(), Value::Text(name.into()));
            p.insert("sl".into(), Value::Text(label.into()));
            admin
                .execute(
                    "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
                    &p,
                )
                .unwrap();
        }
        admin.close().unwrap();
    }
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let r = edge
        .execute("SELECT name FROM signatures", &empty())
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("edge-readable".into()));
    let direct = edge.scan("signatures", edge.snapshot()).unwrap();
    assert_eq!(
        direct.len(),
        1,
        "direct scan on a scope-labelled handle must also exclude server rows"
    );
    assert!(
        matches!(direct[0].values.get("name"), Some(Value::Text(s)) if s == "edge-readable"),
        "direct scan must return the edge-readable row; rows={direct:?}"
    );
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(3)));
    p.insert("name".into(), Value::Text("server-from-edge".into()));
    p.insert("sl".into(), Value::Text("server".into()));
    let err = assert_error(
        edge.execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        ),
        "edge handle must not INSERT server-labelled rows into split-form table",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "split-form write gate must reject server-labelled INSERT from edge handle",
    );
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(4)));
    p.insert(
        "name".into(),
        Value::Text("edge-write-denied-by-schema".into()),
    );
    p.insert("sl".into(), Value::Text("edge".into()));
    let err = assert_error(
        edge.execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        ),
        "edge handle must not INSERT edge-labelled rows when schema WRITE only allows server",
    );
    assert_scope_violation(
        err,
        "edge",
        &[],
        "split-form write gate must intersect schema WRITE labels with handle labels",
    );
    edge.execute(
        "CREATE TABLE server_read_only (id UUID PRIMARY KEY, name TEXT, \
         scope_label TEXT SCOPE_LABEL_READ ('server') WRITE ('server'))",
        &empty(),
    )
    .unwrap();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(5)));
    p.insert(
        "name".into(),
        Value::Text("edge-hidden-by-schema-read".into()),
    );
    p.insert("sl".into(), Value::Text("edge".into()));
    let err = assert_error(
        edge.execute(
            "INSERT INTO server_read_only (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        ),
        "edge handle must not seed edge row into server-read/server-write schema",
    );
    assert_scope_violation(
        err,
        "edge",
        &[],
        "schema WRITE allow-list must be enforced even when handle allows edge",
    );
    drop(edge);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute(
            "SELECT name FROM signatures WHERE id = '00000000-0000-0000-0000-000000000003'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        r.rows.len(),
        0,
        "denied split-form INSERT must not persist a server-labelled row"
    );
    admin
        .execute(
            "CREATE TABLE admin_seeded_server_read (id UUID PRIMARY KEY, name TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('server') WRITE ('server'))",
            &empty(),
        )
        .unwrap();
    let mut seed = HashMap::new();
    seed.insert("id".into(), Value::Uuid(Uuid::from_u128(6)));
    seed.insert(
        "name".into(),
        Value::Text("edge-hidden-by-read-schema".into()),
    );
    seed.insert("sl".into(), Value::Text("edge".into()));
    admin
        .execute(
            "INSERT INTO admin_seeded_server_read (id, name, scope_label) VALUES ($id, $name, $sl)",
            &seed,
        )
        .unwrap();
    drop(admin);
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let r = edge
        .execute("SELECT name FROM admin_seeded_server_read", &empty())
        .unwrap();
    assert_eq!(
        r.rows.len(),
        0,
        "schema READ allow-list must hide edge-labelled rows when READ only allows server"
    );
    let direct = edge
        .scan("admin_seeded_server_read", edge.snapshot())
        .unwrap();
    assert_eq!(
        direct.len(),
        0,
        "direct scan must also intersect schema READ labels with handle labels"
    );
}

/// REGRESSION GUARD — t6_03
#[test]
fn t6_03_server_handle_writes_server_table() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("server.redb");
    seed_signatures(&path);
    let server =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("server")]))
            .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(2)));
    p.insert("name".into(), Value::Text("svr".into()));
    p.insert("sl".into(), Value::Text("server".into()));
    server
        .execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        )
        .unwrap();
    let r = server
        .execute(
            "SELECT name FROM signatures WHERE id = '00000000-0000-0000-0000-000000000002'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        r.rows.len(),
        1,
        "server handle must read its own write back"
    );
    assert_eq!(r.rows[0][0], Value::Text("svr".into()));
}

/// RED — t6_04
#[test]
fn t6_04_edge_graph_traversal_excludes_server_only_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE nodes (id UUID PRIMARY KEY, label TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('server'))",
                &empty(),
            )
            .unwrap();
        admin.execute(
            "CREATE TABLE nodes_edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        ).unwrap();
        let n_edge = Uuid::from_u128(1);
        let n_server = Uuid::from_u128(2);
        let n_edge_behind_server = Uuid::from_u128(3);
        let n_edge_allowed_neighbor = Uuid::from_u128(4);
        for (id, label, sl) in [
            (n_edge, "n_edge", "edge"),
            (n_server, "n_server", "server"),
            (n_edge_behind_server, "n_edge_behind_server", "edge"),
            (n_edge_allowed_neighbor, "n_edge_allowed_neighbor", "edge"),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("label".into(), Value::Text(label.into()));
            p.insert("sl".into(), Value::Text(sl.into()));
            admin
                .execute(
                    "INSERT INTO nodes (id, label, scope_label) VALUES ($id, $label, $sl)",
                    &p,
                )
                .unwrap();
        }
        for (eid, src, tgt) in [
            (Uuid::from_u128(50), n_edge, n_edge),
            (Uuid::from_u128(51), n_edge, n_server),
            (Uuid::from_u128(52), n_server, n_edge_behind_server),
            (Uuid::from_u128(53), n_edge, n_edge_allowed_neighbor),
        ] {
            let mut e = HashMap::new();
            e.insert("eid".into(), Value::Uuid(eid));
            e.insert("src".into(), Value::Uuid(src));
            e.insert("tgt".into(), Value::Uuid(tgt));
            admin.execute(
                "INSERT INTO nodes_edges (id, source_id, target_id, edge_type) VALUES ($eid, $src, $tgt, 'LINKS')",
                &e,
            ).unwrap();
        }
        admin.close().unwrap();
    }
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    // Issue traversal from the edge node itself so a non-vacuous result is reachable.
    let r = edge.execute(
        "SELECT b_id FROM GRAPH_TABLE(nodes_edges MATCH (a)-[:LINKS]->{1,2}(b) COLUMNS (b.id AS b_id))",
        &empty(),
    ).unwrap();
    let n_edge = Uuid::from_u128(1);
    let n_server = Uuid::from_u128(2);
    let n_edge_behind_server = Uuid::from_u128(3);
    let n_edge_allowed_neighbor = Uuid::from_u128(4);
    let saw_edge = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == n_edge));
    assert!(saw_edge, "edge node must be visible to edge handle");
    let saw_allowed_neighbor = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == n_edge_allowed_neighbor));
    assert!(
        saw_allowed_neighbor,
        "edge handle must traverse at least one allowed edge; rows={:?}",
        r.rows
    );
    for row in &r.rows {
        if let Value::Uuid(u) = &row[0] {
            assert_ne!(*u, n_server, "server-only node leaked into edge traversal");
            assert_ne!(
                *u, n_edge_behind_server,
                "edge-visible node reachable only through a server-only bridge leaked into traversal"
            );
        }
    }
    let direct = edge
        .query_bfs(n_edge, None, Direction::Outgoing, 2, edge.snapshot())
        .unwrap();
    let direct_ids: Vec<Uuid> = direct.nodes.iter().map(|node| node.id).collect();
    assert!(
        direct_ids.contains(&n_edge),
        "direct graph helper must still return visible edge node; nodes={direct_ids:?}"
    );
    assert!(
        direct_ids.contains(&n_edge_allowed_neighbor),
        "direct graph helper must traverse allowed edge-visible neighbor; nodes={direct_ids:?}"
    );
    assert!(
        !direct_ids.contains(&n_server) && !direct_ids.contains(&n_edge_behind_server),
        "direct graph helper leaked scope-denied nodes; nodes={direct_ids:?}"
    );

    let denied_insert_source = n_edge_allowed_neighbor;
    let denied_insert_target = n_server;
    let tx = edge.begin();
    let err = assert_error(
        edge.insert_edge(
            tx,
            denied_insert_source,
            denied_insert_target,
            "DENIED_LINK".into(),
            HashMap::new(),
        ),
        "direct insert_edge from edge handle to server-scoped node must be rejected",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "direct insert_edge from edge handle to server-scoped node",
    );
    edge.commit(tx).unwrap();

    let tx = edge.begin();
    let err = assert_error(
        edge.delete_edge(tx, n_edge, n_server, "LINKS"),
        "direct delete_edge from edge handle touching server-scoped node must be rejected",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "direct delete_edge from edge handle touching server-scoped node",
    );
    edge.commit(tx).unwrap();
    drop(edge);

    let admin = Database::open(&path).unwrap();
    assert!(
        admin
            .get_edge_properties(
                denied_insert_source,
                denied_insert_target,
                "DENIED_LINK",
                admin.snapshot(),
            )
            .unwrap()
            .is_none(),
        "failed direct insert_edge must not leave a denied server-scoped edge"
    );
    assert!(
        admin
            .get_edge_properties(n_edge, n_server, "LINKS", admin.snapshot())
            .unwrap()
            .is_some(),
        "failed direct delete_edge must not remove the existing server-scoped edge after commit"
    );
}

/// RED — t6_05
#[test]
fn t6_05_edge_vector_query_excludes_server_only_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("vec.redb");
    let (edge_far_row_id, edge_nearest_row_id, server_row_ids) = {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE memos (id UUID PRIMARY KEY, embedding VECTOR(3), \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('server'))",
                &empty(),
            )
            .unwrap();
        let rows = std::iter::once((1u128, "edge", vec![1.0, 0.0, 0.0]))
            .chain((2u128..12).map(|i| (i, "server", vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((12u128, "edge", vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((13u128, "edge", vec![0.5, 0.5, 0.0])));
        for (i, sl, vector) in rows {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("v".into(), Value::Vector(vector));
            p.insert("sl".into(), Value::Text(sl.into()));
            admin
                .execute(
                    "INSERT INTO memos (id, embedding, scope_label) VALUES ($id, $v, $sl)",
                    &p,
                )
                .unwrap();
        }
        let rows = admin.scan("memos", admin.snapshot()).unwrap();
        let edge_far_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(1)))
            .expect("edge far vector fixture row missing")
            .row_id;
        let edge_nearest_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(12)))
            .expect("edge nearest vector fixture row missing")
            .row_id;
        let server_row_ids: BTreeSet<_> = rows
            .iter()
            .filter(
                |row| matches!(row.values.get("scope_label"), Some(Value::Text(label)) if label == "server"),
            )
            .map(|row| row.row_id)
            .collect();
        admin.close().unwrap();
        (edge_far_row_id, edge_nearest_row_id, server_row_ids)
    };
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let mut p = HashMap::new();
    p.insert("q".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    let r = edge
        .execute("SELECT id FROM memos ORDER BY embedding <=> $q LIMIT 1", &p)
        .unwrap();
    let edge_id = Uuid::from_u128(12);
    assert_eq!(
        r.rows.len(),
        1,
        "scope-label vector gating must happen before LIMIT"
    );
    let saw_edge = r
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == edge_id));
    assert!(
        saw_edge,
        "nearest visible edge-labeled row must be present in edge handle's vector query"
    );
    for row in &r.rows {
        if let Value::Uuid(u) = &row[0] {
            assert!(
                !(2..12).contains(&u.as_u128()),
                "server-only row leaked into edge vector query"
            );
        }
    }
    let direct = edge
        .query_vector(
            VectorIndexRef::new("memos", "embedding"),
            &[0.0, 1.0, 0.0],
            10,
            None,
            edge.snapshot(),
        )
        .unwrap();
    assert!(
        matches!(direct.first(), Some((row_id, _)) if *row_id == edge_nearest_row_id),
        "direct vector API must rank the nearest visible edge row first; far={edge_far_row_id:?} result={direct:?}"
    );
    assert!(
        direct
            .iter()
            .all(|(row_id, _)| !server_row_ids.contains(row_id)),
        "direct vector API leaked server-labelled rows; result={direct:?}"
    );

    let mut denied_rows = server_row_ids.iter();
    let denied_insert_row_id = *denied_rows
        .next()
        .expect("server vector fixture row missing for insert");
    let denied_delete_row_id = *denied_rows
        .next()
        .expect("server vector fixture row missing for delete");
    let tx = edge.begin();
    let err = assert_error(
        edge.insert_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_insert_row_id,
            vec![1.0, 0.0, 0.0],
        ),
        "direct insert_vector from edge handle into server-scoped row must be rejected",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "direct insert_vector from edge handle into server-scoped row",
    );
    edge.commit(tx).unwrap();

    let tx = edge.begin();
    let err = assert_error(
        edge.delete_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_delete_row_id,
        ),
        "direct delete_vector from edge handle on server-scoped row must be rejected",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "direct delete_vector from edge handle on server-scoped row",
    );
    edge.commit(tx).unwrap();
    drop(edge);

    let admin = Database::open(&path).unwrap();
    let inserted_query = admin
        .query_vector(
            VectorIndexRef::new("memos", "embedding"),
            &[1.0, 0.0, 0.0],
            20,
            None,
            admin.snapshot(),
        )
        .unwrap();
    assert!(
        !inserted_query
            .iter()
            .any(|(row_id, score)| *row_id == denied_insert_row_id && *score > 0.9),
        "failed direct insert_vector must not re-vector a server-scoped row; result={inserted_query:?}"
    );
    let deleted_query = admin
        .query_vector(
            VectorIndexRef::new("memos", "embedding"),
            &[0.0, 1.0, 0.0],
            20,
            None,
            admin.snapshot(),
        )
        .unwrap();
    assert!(
        deleted_query
            .iter()
            .any(|(row_id, _)| *row_id == denied_delete_row_id),
        "failed direct delete_vector must not remove the server-scoped row; result={deleted_query:?}"
    );
}

/// RED — t6_06
#[test]
fn t6_06_arbitrary_tenant_label_strings_enforced() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("tenant.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, body TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('tenant_write_a', 'tenant_b') WRITE ('tenant_write_a', 'tenant_b'))",
            &empty(),
        ).unwrap();
        for (i, sl) in [(1u128, "tenant_write_a"), (2u128, "tenant_b")] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("b".into(), Value::Text(format!("body-{i}")));
            p.insert("sl".into(), Value::Text(sl.into()));
            admin
                .execute(
                    "INSERT INTO t (id, body, scope_label) VALUES ($id, $b, $sl)",
                    &p,
                )
                .unwrap();
        }
        admin.close().unwrap();
    }
    let h = Database::open_with_scope_labels(
        &path,
        BTreeSet::from([ScopeLabel::new("tenant_write_a")]),
    )
    .unwrap();
    let r = h.execute("SELECT id FROM t", &empty()).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Uuid(Uuid::from_u128(1)));
}

/// REGRESSION GUARD — t6_07
#[test]
fn t6_07_admin_handle_writes_anywhere() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("admin.redb");
    seed_signatures(&path);
    let admin = Database::open(&path).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(2)));
    p.insert("name".into(), Value::Text("admin-write".into()));
    p.insert("sl".into(), Value::Text("server".into()));
    admin
        .execute(
            "INSERT INTO signatures (id, name, scope_label) VALUES ($id, $name, $sl)",
            &p,
        )
        .unwrap();
    let r = admin
        .execute("SELECT name FROM signatures", &empty())
        .unwrap();
    assert_eq!(
        r.rows.len(),
        2,
        "admin handle reads all rows including its own write"
    );
}

/// REGRESSION GUARD — t6_08: edge-labeled row write succeeds from edge handle.
#[test]
fn t6_08_edge_handle_writes_edge_row() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("edgewrite.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE rows (id UUID PRIMARY KEY, body TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'))",
                &empty(),
            )
            .unwrap();
        admin.close().unwrap();
    }
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("from-edge".into()));
    p.insert("sl".into(), Value::Text("edge".into()));
    edge.execute(
        "INSERT INTO rows (id, body, scope_label) VALUES ($id, $body, $sl)",
        &p,
    )
    .unwrap();
    let r = edge.execute("SELECT body FROM rows", &empty()).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Text("from-edge".into()));
}

/// RED — t6_09: label-flip-attack rejected.
#[test]
fn t6_09_edge_cannot_flip_label_to_server() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("flip.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE rows (id UUID PRIMARY KEY, body TEXT, \
             scope_label TEXT SCOPE_LABEL_READ ('edge', 'server') WRITE ('edge', 'server'))",
                &empty(),
            )
            .unwrap();
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
        p.insert("body".into(), Value::Text("legit".into()));
        p.insert("sl".into(), Value::Text("edge".into()));
        admin
            .execute(
                "INSERT INTO rows (id, body, scope_label) VALUES ($id, $body, $sl)",
                &p,
            )
            .unwrap();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(2)));
        p.insert("body".into(), Value::Text("server-only".into()));
        p.insert("sl".into(), Value::Text("server".into()));
        admin
            .execute(
                "INSERT INTO rows (id, body, scope_label) VALUES ($id, $body, $sl)",
                &p,
            )
            .unwrap();
        admin.close().unwrap();
    }
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("sl".into(), Value::Text("server".into()));
    let err = assert_error(
        edge.execute("UPDATE rows SET scope_label = $sl WHERE id = $id", &p),
        "edge handle must not flip a row's label to 'server'",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "edge handle must not flip a row's label to server",
    );
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(2)));
    let err = assert_error(
        edge.execute("DELETE FROM rows WHERE id = $id", &p),
        "edge handle must not delete a server-labelled row",
    );
    assert_scope_violation(
        err,
        "server",
        &["edge"],
        "edge handle must not delete a server-labelled row",
    );
    drop(edge);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute("SELECT id, scope_label FROM rows", &empty())
        .unwrap();
    assert!(
        r.rows.iter().any(|row| {
            row[0] == Value::Uuid(Uuid::from_u128(1)) && row[1] == Value::Text("edge".into())
        }),
        "denied scope-label flip must leave row 1 labelled edge; rows={:?}",
        r.rows
    );
    assert!(
        r.rows.iter().any(|row| {
            row[0] == Value::Uuid(Uuid::from_u128(2)) && row[1] == Value::Text("server".into())
        }),
        "denied DELETE must leave server-labelled row 2 intact; rows={:?}",
        r.rows
    );
}

/// REGRESSION GUARD — t6_10: simple-form `SCOPE_LABEL ('server')` is write-only-gated; reads are unrestricted.
#[test]
fn t6_10_simple_form_is_write_only_gate() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("simple.redb");
    seed_signatures(&path);
    // The simple form `SCOPE_LABEL ('server')` (used by seed_signatures) gates
    // WRITES to require the 'server' label. Reads are NOT gated by this form;
    // an edge handle SELECTs and sees the row. (Schemas wanting read-side
    // gating use the split form `SCOPE_LABEL_READ (...) WRITE (...)`.)
    let edge =
        Database::open_with_scope_labels(&path, BTreeSet::from([ScopeLabel::new("edge")])).unwrap();
    let r = edge
        .execute("SELECT name FROM signatures", &empty())
        .unwrap();
    assert_eq!(
        r.rows.len(),
        1,
        "simple-form scope label must not gate reads"
    );

    edge.execute(
        "CREATE TABLE unconstrained (id UUID PRIMARY KEY, scope_label TEXT)",
        &empty(),
    )
    .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(99)));
    p.insert("sl".into(), Value::Text("server".into()));
    edge.execute(
        "INSERT INTO unconstrained (id, scope_label) VALUES ($id, $sl)",
        &p,
    )
    .unwrap();
    let r = edge
        .execute("SELECT scope_label FROM unconstrained", &empty())
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Text("server".into())]],
        "column name alone must not activate scope-label gating"
    );
}
