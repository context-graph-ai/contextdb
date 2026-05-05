use contextdb_core::types::ContextId;
use contextdb_core::{Direction, Error, Lsn, Value, VectorIndexRef};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange, NaturalKey, RowChange,
};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
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

fn assert_context_violation(err: Error, requested: Uuid, allowed: &[Uuid], message: &str) {
    match err {
        Error::ContextScopeViolation {
            requested: got_requested,
            allowed: got_allowed,
        } => {
            assert_eq!(
                got_requested,
                ContextId::new(requested),
                "{message}: requested context mismatch"
            );
            let expected: BTreeSet<_> = allowed.iter().copied().map(ContextId::new).collect();
            assert_eq!(got_allowed, expected, "{message}: allowed set mismatch");
        }
        other => panic!("{message}: expected ContextScopeViolation, got {other:?}"),
    }
}

/// Seed a file-backed store via admin handle with rows in the listed contexts.
fn seed_memos(path: &Path, rows: &[(u128, &str, Uuid)]) {
    let admin = Database::open(path).unwrap();
    admin
        .execute(
            "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT, context_id UUID CONTEXT_ID)",
            &empty(),
        )
        .unwrap();
    for (id, body, ctx) in rows {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(*id)));
        p.insert("body".into(), Value::Text((*body).into()));
        p.insert("ctx".into(), Value::Uuid(*ctx));
        admin
            .execute(
                "INSERT INTO memos (id, body, context_id) VALUES ($id, $body, $ctx)",
                &p,
            )
            .unwrap();
    }
    admin.close().unwrap();
}

/// RED — t4_01
#[test]
fn t4_01_select_excludes_other_contexts() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ctx.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "in-a", ctx_a), (2, "in-b", ctx_b)]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let result = scoped
        .execute("SELECT id, body FROM memos", &empty())
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "scoped handle must see exactly the ctx_a row"
    );
    let body = match &result.rows[0][1] {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text body, got {other:?}"),
    };
    assert_eq!(body, "in-a", "ctx-a row must be visible");
    // Negative: ctx-b row must not appear.
    for row in &result.rows {
        if let Value::Text(s) = &row[1] {
            assert_ne!(s, "in-b", "ctx-b row leaked into ctx-a-scoped handle");
        }
    }
    let direct = scoped.scan("memos", scoped.snapshot()).unwrap();
    assert_eq!(
        direct.len(),
        1,
        "direct scan on a scoped handle must also enforce context"
    );
    assert!(
        matches!(direct[0].values.get("body"), Some(Value::Text(s)) if s == "in-a"),
        "direct scan must return the ctx-a row; rows={direct:?}"
    );
}

/// RED — t4_02
#[test]
fn t4_02_insert_into_other_context_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ins.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[]);
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("payload".into()));
    p.insert("ctx".into(), Value::Uuid(ctx_b));
    let err = assert_error(
        scoped.execute(
            "INSERT INTO memos (id, body, context_id) VALUES ($id, $body, $ctx)",
            &p,
        ),
        "INSERT into ctx-b from ctx-a handle must be rejected",
    );
    assert_context_violation(err, ctx_b, &[ctx_a], "INSERT into ctx-b from ctx-a handle");
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        0,
        "denied INSERT must not persist a ctx-b row before returning the typed error"
    );
}

/// RED — t4_03
#[test]
fn t4_03_update_other_context_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("upd.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "original", ctx_b)]);
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("body".into(), Value::Text("hijacked".into()));
    let result = scoped
        .execute("UPDATE memos SET body = $body WHERE id = $id", &p)
        .unwrap();
    assert_eq!(
        result.rows_affected, 0,
        "UPDATE predicate targeting a hidden ctx-b row must behave as no visible match"
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute(
            "SELECT body FROM memos WHERE id = '00000000-0000-0000-0000-000000000001'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Text("original".into())]],
        "denied UPDATE must leave the ctx-b row unchanged"
    );
}

/// RED — t4_04
#[test]
fn t4_04_delete_other_context_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("del.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "body", ctx_b)]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let result = scoped
        .execute(
            "DELETE FROM memos WHERE id = '00000000-0000-0000-0000-000000000001'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        result.rows_affected, 0,
        "DELETE predicate targeting a hidden ctx-b row must behave as no visible match"
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "denied DELETE must leave the ctx-b row durable"
    );
}

/// RED — t4_05
#[test]
fn t4_05_graph_traversal_excludes_other_contexts() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);

    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE nodes (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin.execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &empty(),
        ).unwrap();
        let n1 = Uuid::from_u128(1);
        let n2 = Uuid::from_u128(2);
        let n3 = Uuid::from_u128(3);
        let n4 = Uuid::from_u128(4);
        for (id, label, ctx) in [
            (n1, "a-node", ctx_a),
            (n2, "b-bridge", ctx_b),
            (n3, "a-behind-hidden-bridge", ctx_a),
            (n4, "a-visible-neighbor", ctx_a),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("label".into(), Value::Text(label.into()));
            p.insert("ctx".into(), Value::Uuid(ctx));
            admin
                .execute(
                    "INSERT INTO nodes (id, label, context_id) VALUES ($id, $label, $ctx)",
                    &p,
                )
                .unwrap();
        }
        for (eid, src, tgt) in [
            (Uuid::from_u128(50), n1, n1),
            (Uuid::from_u128(51), n1, n2),
            (Uuid::from_u128(52), n2, n3),
            (Uuid::from_u128(53), n1, n4),
        ] {
            let mut e = HashMap::new();
            e.insert("eid".into(), Value::Uuid(eid));
            e.insert("src".into(), Value::Uuid(src));
            e.insert("tgt".into(), Value::Uuid(tgt));
            admin.execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($eid, $src, $tgt, 'LINKS')",
                &e,
            ).unwrap();
        }
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let result = scoped.execute(
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,2}(b) COLUMNS (b.id AS b_id))",
        &empty(),
    ).unwrap();
    let n1 = Uuid::from_u128(1);
    let n2 = Uuid::from_u128(2);
    let n3 = Uuid::from_u128(3);
    let n4 = Uuid::from_u128(4);
    // Positive: ctx-a node IS visible to the scoped handle (rules out vacuous "empty result" pass).
    let saw_a = result
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == n1));
    assert!(saw_a, "ctx-a node must be visible to ctx-a-scoped handle");
    let saw_allowed_neighbor = result
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == n4));
    assert!(
        saw_allowed_neighbor,
        "GRAPH_TABLE must traverse to the visible ctx-a neighbor, not only return the start node"
    );
    // Negative: ctx-b node never appears.
    for row in &result.rows {
        if let Value::Uuid(u) = &row[0] {
            assert_ne!(*u, n2, "ctx-b node leaked into ctx-a graph traversal");
            assert_ne!(
                *u, n3,
                "ctx-a node reachable only through a ctx-b bridge leaked into traversal"
            );
        }
    }
    let direct = scoped
        .query_bfs(n1, None, Direction::Outgoing, 2, scoped.snapshot())
        .unwrap();
    let direct_ids: Vec<Uuid> = direct.nodes.iter().map(|node| node.id).collect();
    assert!(
        direct_ids.contains(&n1),
        "direct graph helper must still return visible ctx-a node; nodes={direct_ids:?}"
    );
    assert!(
        direct_ids.contains(&n4),
        "direct graph helper must traverse to the visible ctx-a neighbor; nodes={direct_ids:?}"
    );
    assert!(
        !direct_ids.contains(&n2) && !direct_ids.contains(&n3),
        "direct graph helper leaked out-of-context nodes; nodes={direct_ids:?}"
    );

    let denied_insert_source = n4;
    let denied_insert_target = n2;
    let tx = scoped.begin();
    let err = assert_error(
        scoped.insert_edge(
            tx,
            denied_insert_source,
            denied_insert_target,
            "DENIED_LINK".into(),
            HashMap::new(),
        ),
        "direct insert_edge from ctx-a handle to ctx-b node must be rejected",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "direct insert_edge from ctx-a handle to ctx-b node",
    );
    scoped.commit(tx).unwrap();

    let tx = scoped.begin();
    let err = assert_error(
        scoped.delete_edge(tx, n1, n2, "LINKS"),
        "direct delete_edge from ctx-a handle touching ctx-b node must be rejected",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "direct delete_edge from ctx-a handle touching ctx-b node",
    );
    scoped.commit(tx).unwrap();
    drop(scoped);

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
        "failed direct insert_edge must not leave a denied edge in the committed graph"
    );
    assert!(
        admin
            .get_edge_properties(n1, n2, "LINKS", admin.snapshot())
            .unwrap()
            .is_some(),
        "failed direct delete_edge must not remove the existing denied edge after commit"
    );
}

/// RED — t4_06
#[test]
fn t4_06_vector_query_excludes_other_contexts() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("vec.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);

    let (ctx_a_far_row_id, ctx_a_nearest_row_id, ctx_b_row_ids) = {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE memos (id UUID PRIMARY KEY, embedding VECTOR(3), context_id UUID CONTEXT_ID)",
            &empty(),
        ).unwrap();
        let rows = std::iter::once((1u128, ctx_a, vec![1.0, 0.0, 0.0]))
            .chain((2u128..12).map(|i| (i, ctx_b, vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((12u128, ctx_a, vec![0.0, 1.0, 0.0])))
            .chain(std::iter::once((13u128, ctx_a, vec![0.5, 0.5, 0.0])));
        for (i, ctx, vector) in rows {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("v".into(), Value::Vector(vector));
            p.insert("ctx".into(), Value::Uuid(ctx));
            admin
                .execute(
                    "INSERT INTO memos (id, embedding, context_id) VALUES ($id, $v, $ctx)",
                    &p,
                )
                .unwrap();
        }
        let rows = admin.scan("memos", admin.snapshot()).unwrap();
        let ctx_a_far_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(1)))
            .expect("ctx-a far vector fixture row missing")
            .row_id;
        let ctx_a_nearest_row_id = rows
            .iter()
            .find(|row| matches!(row.values.get("id"), Some(Value::Uuid(u)) if *u == Uuid::from_u128(12)))
            .expect("ctx-a nearest vector fixture row missing")
            .row_id;
        let ctx_b_row_ids: BTreeSet<_> = rows
            .iter()
            .filter(
                |row| matches!(row.values.get("context_id"), Some(Value::Uuid(u)) if *u == ctx_b),
            )
            .map(|row| row.row_id)
            .collect();
        admin.close().unwrap();
        (ctx_a_far_row_id, ctx_a_nearest_row_id, ctx_b_row_ids)
    };

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let mut p = HashMap::new();
    p.insert("q".into(), Value::Vector(vec![0.0, 1.0, 0.0]));
    let result = scoped
        .execute("SELECT id FROM memos ORDER BY embedding <=> $q LIMIT 1", &p)
        .unwrap();
    let ctx_a_nearest = Uuid::from_u128(12);
    assert_eq!(
        result.rows.len(),
        1,
        "vector gating must happen before LIMIT so the nearest visible row is still returned"
    );
    let saw_nearest_a = result
        .rows
        .iter()
        .any(|r| matches!(&r[0], Value::Uuid(u) if *u == ctx_a_nearest));
    assert!(
        saw_nearest_a,
        "nearest visible ctx-a row must be present in ctx-a-scoped vector query result"
    );
    // Negative: ctx-b row never appears.
    for row in &result.rows {
        if let Value::Uuid(u) = &row[0] {
            assert!(
                !(2..12).contains(&u.as_u128()),
                "ctx-b row leaked into vector query"
            );
        }
    }
    let direct = scoped
        .query_vector(
            VectorIndexRef::new("memos", "embedding"),
            &[0.0, 1.0, 0.0],
            10,
            None,
            scoped.snapshot(),
        )
        .unwrap();
    assert!(
        matches!(direct.first(), Some((row_id, _)) if *row_id == ctx_a_nearest_row_id),
        "direct vector API must rank the nearest visible ctx-a row first; far={ctx_a_far_row_id:?} result={direct:?}"
    );
    assert!(
        direct
            .iter()
            .all(|(row_id, _)| !ctx_b_row_ids.contains(row_id)),
        "direct vector API leaked ctx-b rows; result={direct:?}"
    );

    let mut denied_rows = ctx_b_row_ids.iter();
    let denied_insert_row_id = *denied_rows
        .next()
        .expect("ctx-b vector fixture row missing for insert");
    let denied_delete_row_id = *denied_rows
        .next()
        .expect("ctx-b vector fixture row missing for delete");
    let tx = scoped.begin();
    let err = assert_error(
        scoped.insert_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_insert_row_id,
            vec![1.0, 0.0, 0.0],
        ),
        "direct insert_vector from ctx-a handle into ctx-b row must be rejected",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "direct insert_vector from ctx-a handle into ctx-b row",
    );
    scoped.commit(tx).unwrap();

    let tx = scoped.begin();
    let err = assert_error(
        scoped.delete_vector(
            tx,
            VectorIndexRef::new("memos", "embedding"),
            denied_delete_row_id,
        ),
        "direct delete_vector from ctx-a handle on ctx-b row must be rejected",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "direct delete_vector from ctx-a handle on ctx-b row",
    );
    scoped.commit(tx).unwrap();
    drop(scoped);

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
        "failed direct insert_vector must not re-vector a denied ctx-b row; result={inserted_query:?}"
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
        "failed direct delete_vector must not remove the denied ctx-b row; result={deleted_query:?}"
    );
}

/// RED — t4_07
#[test]
fn t4_07_multi_context_set_sees_subset_excludes_others() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("multi.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    let ctx_c = Uuid::from_u128(0xC);
    seed_memos(
        &path,
        &[(1, "in-a", ctx_a), (2, "in-b", ctx_b), (3, "in-c", ctx_c)],
    );
    let scoped = Database::open_with_contexts(
        &path,
        BTreeSet::from([ContextId::new(ctx_a), ContextId::new(ctx_b)]),
    )
    .unwrap();
    let result = scoped
        .execute("SELECT id, body FROM memos", &empty())
        .unwrap();
    assert_eq!(
        result.rows.len(),
        2,
        "handle scoped to {{a,b}} must see exactly 2 rows; got {}",
        result.rows.len()
    );
    let bodies: std::collections::HashSet<String> = result
        .rows
        .iter()
        .map(|r| match &r[1] {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text body, got {other:?}"),
        })
        .collect();
    assert!(bodies.contains("in-a"), "ctx-a row must appear");
    assert!(bodies.contains("in-b"), "ctx-b row must appear");
    assert!(
        !bodies.contains("in-c"),
        "ctx-c row must NOT appear under {{a,b}} scope"
    );
}

/// REGRESSION GUARD — t4_08
#[test]
fn t4_08_admin_handle_sees_all_contexts() {
    use tempfile::TempDir;
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("admin.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        for (i, ctx) in [(1u128, ctx_a), (2u128, ctx_b)] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
            p.insert("body".into(), Value::Text(format!("r-{i}")));
            p.insert("ctx".into(), Value::Uuid(ctx));
            admin
                .execute(
                    "INSERT INTO memos (id, body, context_id) VALUES ($id, $body, $ctx)",
                    &p,
                )
                .unwrap();
        }
        admin.close().unwrap();
    }
    let admin = Database::open(&path).unwrap();
    let r = admin.execute("SELECT id FROM memos", &empty()).unwrap();
    assert_eq!(r.rows.len(), 2);
}

/// REGRESSION GUARD — t4_09
#[test]
fn t4_09_table_without_context_id_unaffected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("plain.redb");
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE plain (id UUID PRIMARY KEY, v TEXT, context_id UUID)",
                &empty(),
            )
            .unwrap();
        admin.close().unwrap();
    }
    let scoped = Database::open_with_contexts(
        &path,
        BTreeSet::from([ContextId::new(Uuid::from_u128(0xA))]),
    )
    .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("v".into(), Value::Text("anything".into()));
    p.insert("ctx".into(), Value::Uuid(Uuid::from_u128(0xB)));
    scoped
        .execute(
            "INSERT INTO plain (id, v, context_id) VALUES ($id, $v, $ctx)",
            &p,
        )
        .unwrap();
    let r = scoped
        .execute("SELECT v, context_id FROM plain", &empty())
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Uuid(Uuid::from_u128(0xB)));
}

/// RED — t4_10
#[test]
fn t4_10_insert_context_id_omitted_handle_one_context_auto_stamps() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("auto_one.redb");
    let ctx_a = Uuid::from_u128(0xA);
    seed_memos(&path, &[]);
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(42)));
    p.insert("body".into(), Value::Text("auto".into()));
    // INSERT omits context_id from the column list; handle has exactly 1 ctx.
    scoped
        .execute("INSERT INTO memos (id, body) VALUES ($id, $body)", &p)
        .expect("auto-stamp must allow INSERT to succeed when handle has exactly 1 context");
    let r = scoped
        .execute("SELECT id, context_id FROM memos", &empty())
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0][1],
        Value::Uuid(ctx_a),
        "auto-stamped context_id must equal the handle's single context"
    );
}

/// RED — t4_11
#[test]
fn t4_11_insert_context_id_omitted_handle_multi_or_zero_context_rejects() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("auto_multi.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[]);
    let scoped = Database::open_with_contexts(
        &path,
        BTreeSet::from([ContextId::new(ctx_a), ContextId::new(ctx_b)]),
    )
    .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(42)));
    p.insert("body".into(), Value::Text("ambiguous".into()));
    let err = assert_error(
        scoped.execute("INSERT INTO memos (id, body) VALUES ($id, $body)", &p),
        "INSERT without context_id from a multi-context handle must be rejected",
    );
    assert_context_violation(
        err,
        Uuid::from_u128(u128::MAX),
        &[ctx_a, ctx_b],
        "ambiguous multi-context auto-stamp rejection",
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        0,
        "ambiguous auto-stamp rejection must not insert a row with NULL or fake context_id"
    );

    let path = tmp.path().join("auto_zero.redb");
    seed_memos(&path, &[]);
    let scoped = Database::open_with_contexts(&path, BTreeSet::new()).unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(43)));
    p.insert("body".into(), Value::Text("unscoped".into()));
    let err = assert_error(
        scoped.execute("INSERT INTO memos (id, body) VALUES ($id, $body)", &p),
        "INSERT without context_id from a zero-context handle must be rejected",
    );
    assert_context_violation(
        err,
        Uuid::from_u128(u128::MAX),
        &[],
        "zero-context auto-stamp rejection",
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    assert_eq!(
        rows.len(),
        0,
        "zero-context auto-stamp rejection must not insert an unscoped row"
    );
}

/// RED — t4_12: UPDATE label-flip post-image rejected (pre-image is allowed).
#[test]
fn t4_12_update_post_image_label_flip_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("flip.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "in-a", ctx_a)]);
    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let row_a = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(row_a));
    p.insert("ctx".into(), Value::Uuid(ctx_b));
    // Pre-image: row's current context_id is ctx_a → IN the handle's set.
    // Post-image: SET context_id = ctx_b → NOT IN the handle's set.
    // The gate must check the post-image, not just the pre-image.
    let err = assert_error(
        scoped.execute("UPDATE memos SET context_id = $ctx WHERE id = $id", &p),
        "post-image label-flip to ctx-b must be rejected",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "UPDATE post-image context flip rejection",
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let r = admin
        .execute(
            "SELECT context_id FROM memos WHERE id = '00000000-0000-0000-0000-000000000001'",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![Value::Uuid(ctx_a)]],
        "denied context flip must leave the row in ctx-a"
    );
}

/// RED — t4_13: direct scan_filter predicates must not observe denied rows.
#[test]
fn t4_13_scan_filter_predicate_sees_only_context_visible_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("scan_filter.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "visible", ctx_a), (2, "hidden", ctx_b)]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let observed = std::cell::RefCell::new(Vec::new());
    let rows = scoped
        .scan_filter("memos", scoped.snapshot(), &|row| {
            if let Some(Value::Text(body)) = row.values.get("body") {
                observed.borrow_mut().push(body.clone());
            }
            true
        })
        .unwrap();

    assert_eq!(rows.len(), 1, "scan_filter must return only visible rows");
    assert_eq!(
        observed.into_inner(),
        vec!["visible".to_string()],
        "scan_filter predicate must not be invoked with hidden row values"
    );
}

/// RED — t4_14: sync apply's raw insert helper must not bypass context gates.
#[test]
fn t4_14_sync_apply_does_not_insert_other_context_rows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("sync_apply.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let row_id = Uuid::from_u128(20);
    let result = scoped
        .apply_changes(
            ChangeSet {
                rows: vec![RowChange {
                    table: "memos".into(),
                    natural_key: NaturalKey {
                        column: "id".into(),
                        value: Value::Uuid(row_id),
                    },
                    values: HashMap::from([
                        ("id".into(), Value::Uuid(row_id)),
                        ("body".into(), Value::Text("from-sync".into())),
                        ("context_id".into(), Value::Uuid(ctx_b)),
                    ]),
                    deleted: false,
                    lsn: Lsn(1),
                }],
                ..ChangeSet::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();

    assert_eq!(result.applied_rows, 0, "denied sync row must not apply");
    assert_eq!(result.skipped_rows, 1, "denied sync row must be skipped");
    assert_eq!(
        result.conflicts.len(),
        1,
        "denied sync row should surface as a conflict"
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    let rows = admin.scan("memos", admin.snapshot()).unwrap();
    assert_eq!(rows.len(), 0, "sync apply must not persist ctx-b row");
}

/// RED — t4_15: state propagation must not mutate out-of-context child rows.
#[test]
fn t4_15_state_propagation_respects_context_on_children() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("propagate.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    let parent = Uuid::from_u128(1);
    let child = Uuid::from_u128(2);

    {
        let admin = Database::open(&path).unwrap();
        admin.execute(
            "CREATE TABLE parents (id UUID PRIMARY KEY, status TEXT, context_id UUID CONTEXT_ID) \
             STATE MACHINE (status: active -> [archived])",
            &empty(),
        )
        .unwrap();
        admin
            .execute(
                "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) \
             ON STATE archived PROPAGATE SET archived, status TEXT, context_id UUID CONTEXT_ID) \
             STATE MACHINE (status: active -> [archived])",
                &empty(),
            )
            .unwrap();
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(parent));
        p.insert("status".into(), Value::Text("active".into()));
        p.insert("ctx".into(), Value::Uuid(ctx_a));
        admin
            .execute(
                "INSERT INTO parents (id, status, context_id) VALUES ($id, $status, $ctx)",
                &p,
            )
            .unwrap();
        let mut c = HashMap::new();
        c.insert("id".into(), Value::Uuid(child));
        c.insert("parent".into(), Value::Uuid(parent));
        c.insert("status".into(), Value::Text("active".into()));
        c.insert("ctx".into(), Value::Uuid(ctx_b));
        admin
            .execute(
                "INSERT INTO children (id, parent_id, status, context_id) \
                 VALUES ($id, $parent, $status, $ctx)",
                &c,
            )
            .unwrap();
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let mut params = HashMap::new();
    params.insert("id".into(), Value::Uuid(parent));
    params.insert("status".into(), Value::Text("archived".into()));
    let err = assert_error(
        scoped.execute(
            "UPDATE parents SET status = $status WHERE id = $id",
            &params,
        ),
        "ctx-a update must not propagate into ctx-b child",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "propagation into ctx-b child must be rejected",
    );
    drop(scoped);

    let admin = Database::open(&path).unwrap();
    let parent_status = admin
        .execute("SELECT status FROM parents", &empty())
        .unwrap();
    let child_status = admin
        .execute("SELECT status FROM children", &empty())
        .unwrap();
    assert_eq!(
        parent_status.rows,
        vec![vec![Value::Text("active".into())]],
        "failed propagation must roll back the parent transition"
    );
    assert_eq!(
        child_status.rows,
        vec![vec![Value::Text("active".into())]],
        "failed propagation must leave the ctx-b child unchanged"
    );
}

/// RED — t4_16: protected edge-row metadata hides edges even when endpoints are visible.
#[test]
fn t4_16_graph_edge_metadata_context_gate_hides_visible_endpoint_edge() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("edge_metadata.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    let n1 = Uuid::from_u128(1);
    let n2 = Uuid::from_u128(2);

    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE nodes (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        admin
            .execute(
                "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, \
                 edge_type TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        for (id, label) in [(n1, "source"), (n2, "target")] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("label".into(), Value::Text(label.into()));
            p.insert("ctx".into(), Value::Uuid(ctx_a));
            admin
                .execute(
                    "INSERT INTO nodes (id, label, context_id) VALUES ($id, $label, $ctx)",
                    &p,
                )
                .unwrap();
        }
        let mut e = HashMap::new();
        e.insert("id".into(), Value::Uuid(Uuid::from_u128(50)));
        e.insert("source".into(), Value::Uuid(n1));
        e.insert("target".into(), Value::Uuid(n2));
        e.insert("ctx".into(), Value::Uuid(ctx_b));
        admin
            .execute(
                "INSERT INTO edges (id, source_id, target_id, edge_type, context_id) \
                 VALUES ($id, $source, $target, 'LINKS', $ctx)",
                &e,
            )
            .unwrap();
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let graph_rows = scoped
        .execute(
            "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
             WHERE a.id = '00000000-0000-0000-0000-000000000001' COLUMNS (b.id AS b_id))",
            &empty(),
        )
        .unwrap();
    assert_eq!(
        graph_rows.rows.len(),
        0,
        "GRAPH_TABLE must hide edge whose metadata row is ctx-b"
    );
    let direct = scoped
        .query_bfs(
            n1,
            Some(&["LINKS".to_string()]),
            Direction::Outgoing,
            1,
            scoped.snapshot(),
        )
        .unwrap();
    assert!(
        direct.nodes.is_empty(),
        "direct graph traversal must hide metadata-denied edge; nodes={:?}",
        direct.nodes
    );
    assert_eq!(
        scoped.edge_count(n1, "LINKS", scoped.snapshot()).unwrap(),
        0,
        "edge_count must hide metadata-denied edge"
    );
    assert!(
        scoped
            .get_edge_properties(n1, n2, "LINKS", scoped.snapshot())
            .unwrap()
            .is_none(),
        "get_edge_properties must hide metadata-denied edge"
    );
    let tx = scoped.begin();
    let err = assert_error(
        scoped.delete_edge(tx, n1, n2, "LINKS"),
        "delete_edge must not delete metadata-denied edge",
    );
    assert_context_violation(
        err,
        ctx_b,
        &[ctx_a],
        "delete_edge must gate protected edge metadata row",
    );
    scoped.commit(tx).unwrap();
}

/// RED — t4_17: sync extraction from a scoped handle must not leak hidden rows or DDL.
#[test]
fn t4_17_changes_since_filters_context_rows_and_schema() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("changes_since.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "visible", ctx_a), (2, "hidden", ctx_b)]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let changes = scoped.changes_since(Lsn(0));
    assert!(
        changes.ddl.is_empty(),
        "constrained changes_since must not leak schema DDL"
    );
    assert_eq!(
        changes.rows.len(),
        1,
        "constrained changes_since must include only visible row changes"
    );
    assert_eq!(
        changes.rows[0].values.get("body"),
        Some(&Value::Text("visible".into())),
        "hidden ctx-b row must not appear in changes_since"
    );
}

/// REGRESSION GUARD — t4_17b: a visible row moved out of scope must emit a scoped delete.
#[test]
fn t4_17b_changes_since_emits_delete_when_row_moves_out_of_context_after_ddl_lsn_gap() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("changes_move.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    seed_memos(&path, &[(1, "visible", ctx_a)]);

    let admin = Database::open(&path).unwrap();
    let since = admin.current_lsn();
    admin
        .execute("CREATE TABLE unrelated (id UUID PRIMARY KEY)", &empty())
        .unwrap();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("ctx".into(), Value::Uuid(ctx_b));
    admin
        .execute("UPDATE memos SET context_id = $ctx WHERE id = $id", &p)
        .unwrap();
    admin.close().unwrap();

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let changes = scoped.changes_since(since);
    assert!(
        changes.ddl.is_empty(),
        "scoped change streams must not expose schema DDL"
    );
    assert_eq!(
        changes.rows.len(),
        1,
        "moving a previously visible row out of context must emit one scoped delete"
    );
    assert!(changes.rows[0].deleted);
    assert_eq!(
        changes.rows[0].natural_key.value,
        Value::Uuid(Uuid::from_u128(1))
    );
}

/// RED — t4_18: sync DDL apply requires an admin handle.
#[test]
fn t4_18_scoped_apply_changes_rejects_ddl() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("sync_ddl.redb");
    let ctx_a = Uuid::from_u128(0xA);
    seed_memos(&path, &[]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let err = assert_error(
        scoped.apply_changes(
            ChangeSet {
                ddl: vec![DdlChange::DropTable {
                    name: "memos".into(),
                }],
                ddl_lsn: vec![Lsn(1)],
                ..ChangeSet::default()
            },
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        ),
        "scoped sync apply must reject DDL changes",
    );
    assert!(
        matches!(err, Error::Other(ref message) if message.contains("admin database handle")),
        "expected admin-handle DDL rejection, got {err:?}"
    );
    drop(scoped);

    let admin = Database::open(&path).unwrap();
    assert!(
        admin.table_meta("memos").is_some(),
        "rejected scoped DDL apply must leave schema intact"
    );
}

/// RED — t4_19: scoped handles must not expose raw DDL/log or vector payload bypasses.
#[test]
fn t4_19_scoped_raw_helpers_respect_context_boundary() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("raw_helpers.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let ctx_b = Uuid::from_u128(0xB);
    let hidden_row_id = {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE memos (id UUID PRIMARY KEY, embedding VECTOR(3), context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        for (id, ctx, vector) in [
            (Uuid::from_u128(1), ctx_a, vec![1.0, 0.0, 0.0]),
            (Uuid::from_u128(2), ctx_b, vec![0.0, 1.0, 0.0]),
        ] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("v".into(), Value::Vector(vector));
            p.insert("ctx".into(), Value::Uuid(ctx));
            admin
                .execute(
                    "INSERT INTO memos (id, embedding, context_id) VALUES ($id, $v, $ctx)",
                    &p,
                )
                .unwrap();
        }
        let hidden = admin
            .scan("memos", admin.snapshot())
            .unwrap()
            .into_iter()
            .find(|row| matches!(row.values.get("context_id"), Some(Value::Uuid(ctx)) if *ctx == ctx_b))
            .expect("hidden vector fixture row missing")
            .row_id;
        admin.close().unwrap();
        hidden
    };

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    assert!(
        scoped.ddl_log_since(Lsn(0)).is_empty(),
        "scoped raw DDL log helper must not expose schema"
    );
    let row_changes = scoped.change_log_rows_since(Lsn(0)).unwrap();
    assert_eq!(
        row_changes.len(),
        1,
        "scoped raw row log helper must include only visible row changes"
    );
    assert_eq!(
        row_changes[0].values.get("context_id"),
        Some(&Value::Uuid(ctx_a)),
        "raw row log helper leaked hidden context row"
    );
    assert!(
        !scoped.has_live_vector(hidden_row_id, scoped.snapshot()),
        "has_live_vector must hide vectors attached to hidden rows"
    );
    assert!(
        scoped
            .live_vector_entry(hidden_row_id, scoped.snapshot())
            .is_none(),
        "live_vector_entry must not return hidden vector payloads"
    );
}

/// RED — t4_20: scoped handles must not run normal SQL DDL.
#[test]
fn t4_20_scoped_sql_ddl_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("sql_ddl.redb");
    let ctx_a = Uuid::from_u128(0xA);
    seed_memos(&path, &[]);

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let err = assert_error(
        scoped.execute("DROP TABLE memos", &empty()),
        "scoped handle must not run SQL DDL",
    );
    assert!(
        matches!(err, Error::Other(ref message) if message.contains("admin database handle")),
        "expected admin DDL rejection, got {err:?}"
    );
    drop(scoped);
    let admin = Database::open(&path).unwrap();
    assert!(admin.table_meta("memos").is_some());
}

/// REGRESSION GUARD — t4_21: constrained handles fail closed on graph-only edges.
#[test]
fn t4_21_scoped_graph_only_edges_require_relational_metadata() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("graph_only.redb");
    let ctx_a = Uuid::from_u128(0xA);
    let n1 = Uuid::from_u128(1);
    let n2 = Uuid::from_u128(2);
    {
        let admin = Database::open(&path).unwrap();
        admin
            .execute(
                "CREATE TABLE nodes (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
                &empty(),
            )
            .unwrap();
        for (id, label) in [(n1, "a"), (n2, "b")] {
            let mut p = HashMap::new();
            p.insert("id".into(), Value::Uuid(id));
            p.insert("label".into(), Value::Text(label.into()));
            p.insert("ctx".into(), Value::Uuid(ctx_a));
            admin
                .execute(
                    "INSERT INTO nodes (id, label, context_id) VALUES ($id, $label, $ctx)",
                    &p,
                )
                .unwrap();
        }
        let tx = admin.begin();
        admin
            .insert_edge(tx, n1, n2, "LOOSE".into(), HashMap::new())
            .unwrap();
        admin.commit(tx).unwrap();
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let edge_types = vec!["LOOSE".to_string()];
    let traversal = scoped
        .query_bfs(
            n1,
            Some(edge_types.as_slice()),
            Direction::Outgoing,
            1,
            scoped.snapshot(),
        )
        .unwrap();
    assert!(
        traversal.nodes.is_empty(),
        "scoped traversal must not expose graph-only edges with no gated metadata row"
    );
    assert!(
        scoped
            .get_edge_properties(n1, n2, "LOOSE", scoped.snapshot())
            .unwrap()
            .is_none(),
        "scoped edge property lookup must hide graph-only edges with no gated metadata row"
    );

    let tx = scoped.begin();
    let err = assert_error(
        scoped.insert_edge(tx, n1, n2, "NEW".into(), HashMap::new()),
        "scoped direct graph-only insert must be rejected without relational metadata",
    );
    assert!(
        matches!(err, Error::ContextScopeViolation { .. }),
        "expected ContextScopeViolation for metadata-less direct graph write, got {err:?}"
    );
    scoped.rollback(tx).unwrap();
}
